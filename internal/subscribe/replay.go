package subscribe

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/segment"
)

// WalkInput is the parameter bundle for WalkFromCursor.
type WalkInput struct {
	// StartSeq is the smallest seq the walker will emit. Events with
	// Seq < StartSeq are skipped silently.
	StartSeq uint64

	// StopSeq, when non-zero, is the exclusive upper bound for cold replay.
	// Production passes the writer readable-log floor so WalkFromCursor only
	// serves durable data below that boundary.
	StopSeq uint64

	// Manifest is the in-memory segment manifest. May be nil; callers
	// without sealed segments still walk the active segment's flushed region.
	Manifest *manifest.Manifest

	// Writer is the ingest writer; the walker reads its active segment's
	// flushed blocks to extend past the sealed-segment region. Required.
	Writer *ingest.Writer

	// BlockCache, when non-nil, serves sealed-block decodes through the shared
	// cache instead of decoding directly. Optional; nil preserves direct decode.
	BlockCache *blockCache

	// OnSeamRetry, when non-nil, is invoked once per rotation-seam convergence
	// retry (each time a sealed+active pass ends below StopSeq and the walk
	// re-enters the sealed sweep to fill the gap), carrying the seq at which the
	// gap was observed. Optional; used by tests to drive the seam
	// deterministically and a natural hook point for a future operator metric
	// counting how often the cold-read seam is hit.
	OnSeamRetry func(holeSeq uint64)
}

// WalkFromCursor invokes emit for every durable event with
// Seq >= input.StartSeq, in seq order, across:
//
//  1. the sealed-segment region from the manifest,
//  2. the active segment's flushed blocks.
//
// Halts when emit returns a non-nil error and surfaces the error
// (errors.Is is honored).
//
// Pure-function design: WalkFromCursor holds no subscriber state. The
// bounded cold reader (NewColdReader) composes it with a batch limit and
// the shared block cache to serve Tail's cold-path reads.
//
// Cold replay is bounded by StopSeq, the writer readable-log floor. Every seq
// below StopSeq is durable and file-visible by the readable-log invariant, so
// the walk must serve the whole [StartSeq, StopSeq) range gap-free.
//
// # Rotation-seam convergence
//
// The sealed region (manifest) and the active region (writer's active file)
// are read non-atomically: the walk snapshots the manifest, then reads
// ActiveIndex. A segment rotation completing BETWEEN the two reads is the
// hazard. ingest.Writer.rotateLocked does, all under the writer lock:
//
//	seal(N) -> publish N to the manifest -> activeIdx = N+1
//
// A walker can snapshot the manifest BEFORE N is published (its sealed sweep
// stops below N's range), then read the active region AFTER activeIdx is
// bumped to N+1 (an empty or higher-seq successor). N is then reachable via
// neither source in that pass. A single-pass walk would let the cold reader
// jump its cursor to StopSeq and silently drop N's events (issue #190).
//
// Two properties close the seam:
//
//   - No silent loss: walkActiveRegion emits only contiguous seqs from
//     `current` and stops the instant it sees an event above `current` (a
//     hole), leaving `current` exactly at the hole. It never jumps past a gap.
//
//   - Convergence (no spurious disconnect): a pass that ends below StopSeq
//     means the floor's data is not yet all served — a seam gap. We re-enter
//     the sealed sweep, which by the publish-before-bump happens-before now
//     sees the freshly-published segment(s), so `current` strictly advances.
//     Events are only ever MOVED active->sealed (compaction preserves each
//     segment's historical seq envelope), so a below-StopSeq gap is always
//     fillable. A retry that fails to advance is an invariant violation
//     (e.g. publish-before-bump broke, or a genuine hole below the floor):
//     we surface it loudly rather than spin or silently skip.
func WalkFromCursor(ctx context.Context, input WalkInput, emit func(*segment.Event) error) error {
	current := input.StartSeq

	if input.Manifest == nil {
		// No sealed segments to race against and no StopSeq boundary that could
		// later be filled: read the active region once, leniently. Stopping at
		// a hole here would just wedge the caller.
		_, err := walkActiveRegion(input, current, emit)
		return err
	}

	if err := input.Manifest.Wait(ctx); err != nil {
		return err
	}

	// noProgress counts consecutive passes that neither advanced current nor
	// reached StopSeq. ONE such pass is expected at a live seam: the pass read
	// the manifest BEFORE the just-sealed segment N was published, then observed
	// the bumped activeIdx (empty/higher successor) — so it could serve N from
	// neither source. The publish-before-bump happens-before then guarantees the
	// NEXT pass's fresh manifest read contains N, so a second consecutive
	// no-progress pass means the gap is not a transient seam: a real hole below
	// a floor that promised durable data, or the ordering invariant broke. Fail
	// loud rather than spin or silently jump the cursor past durable events.
	noProgress := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		passStart := current

		next, err := walkSealedRegion(ctx, input, current, emit)
		if err != nil {
			return err
		}
		current = next

		// activeIdx is read fresh inside walkActiveRegion each pass, so a
		// rotation since the last pass moves us onto the new active file and
		// the just-sealed file is recovered by the next sealed sweep.
		next, err = walkActiveRegion(input, current, emit)
		if err != nil {
			return err
		}
		current = next

		// With no StopSeq boundary the walk is unbounded (lenient), so one
		// sealed+active pass is the whole contract.
		if input.StopSeq == 0 || current >= input.StopSeq {
			return nil
		}

		// The pass ended below StopSeq: re-enter the sealed sweep to fill the
		// gap the active sweep could not serve.
		if input.OnSeamRetry != nil {
			input.OnSeamRetry(current)
		}
		if current == passStart {
			noProgress++
			if noProgress >= 2 {
				return fmt.Errorf("subscribe: cold replay made no progress at seq %d "+
					"before readable-log floor %d (rotation seam invariant violated)",
					current, input.StopSeq)
			}
		} else {
			noProgress = 0
		}
	}
}

// walkSealedRegion emits every event with Seq >= start that resides in the
// manifest's sealed segments, in seq order, and returns the next unemitted
// seq. Only invoked when input.Manifest != nil.
func walkSealedRegion(ctx context.Context, input WalkInput, start uint64, emit func(*segment.Event) error) (uint64, error) {
	current := start
	for {
		if err := ctx.Err(); err != nil {
			return current, err
		}
		if input.StopSeq != 0 && current >= input.StopSeq {
			return current, nil
		}
		bounds, ok := input.Manifest.SegmentForSeq(current)
		if !ok {
			return current, nil
		}
		next, err := walkSealedSegment(input.Manifest, bounds, current, input.StopSeq, input.BlockCache, emit)
		if err != nil {
			return current, err
		}
		if next <= bounds.MaxSeq {
			// The segment was fully scanned and emitted nothing at or
			// above next. Compaction preserves a segment's historical seq
			// envelope while dropping rows, so the trailing seqs up to
			// bounds.MaxSeq may simply no longer exist; without this bump
			// SegmentForSeq would return the same segment forever and the
			// walk would spin.
			next = bounds.MaxSeq + 1
		}
		current = next
	}
}

// walkActiveRegion emits events with Seq >= start from the active segment's
// flushed blocks, in seq order, and returns the next unemitted seq.
//
// A concurrent seal of the active file is benign: segment.Seal only appends
// the footer and patches the fixed header, never rewriting the frame region,
// so the flushed frames stay byte-intact. If WalkActive runs past them into
// footer bytes it fails loud (the footer's leading bytes don't decode as a
// zstd frame) and emits nothing partial; that error propagates here and the
// subscriber reconnects, by which point the file is a normal sealed segment
// the sealed sweep will serve. See segment/walkactive_seal_test.go.
func walkActiveRegion(input WalkInput, start uint64, emit func(*segment.Event) error) (next uint64, err error) {
	current := start

	// emitErr captures an error returned by the CALLER's emit (including the
	// cold reader's errBatchFull control signal). It is kept distinct from a
	// WalkActive I/O/decode error so the former propagates verbatim while only
	// the latter is wrapped as "walk active".
	var emitErr error

	// emitOne applies the skip/emit decision for a single event.
	emitOne := func(ev *segment.Event) (stop bool) {
		switch {
		case ev.Seq < current:
			return false // already emitted or below start
		case input.StopSeq != 0 && current >= input.StopSeq:
			return true // served up to the floor
		case input.StopSeq != 0 && ev.Seq >= input.StopSeq:
			return true // reached the floor boundary
		case input.StopSeq != 0 && ev.Seq > current:
			// A hole below the floor: the active file's next visible event
			// skips past `current`. This is the rotation seam — a just-sealed
			// segment holds [current, ev.Seq) but is not in our manifest
			// snapshot yet. Stop cleanly with current UNCHANGED (never jump the
			// gap); WalkFromCursor re-sweeps the sealed region to fill it, or
			// fails loud if a full pass cannot advance. Checked AFTER the
			// boundary cases above so a hole whose next event sits at/above the
			// floor is not misclassified — it stops at the floor either way,
			// and the below-floor gap is still caught by WalkFromCursor's
			// no-progress guard.
			return true
		default:
			cp := *ev
			if e := emit(&cp); e != nil {
				emitErr = e
				return true
			}
			current = ev.Seq + 1
			return false
		}
	}

	activeIdx := input.Writer.ActiveIndex()
	activePath := filepath.Join(input.Writer.SegmentsDir(), ingest.SegmentFilename(activeIdx))
	walkErr := segment.WalkActive(activePath, func(events []segment.Event) error {
		for i := range events {
			if emitOne(&events[i]) {
				return errStopWalk
			}
		}
		return nil
	})
	switch {
	case emitErr != nil:
		// Caller's emit error/control signal: propagate verbatim.
		return current, emitErr
	case errors.Is(walkErr, errStopWalk):
		return current, nil
	case walkErr != nil && !errors.Is(walkErr, os.ErrNotExist):
		return current, fmt.Errorf("walk active: %w", walkErr)
	}

	return current, nil
}

// errStopWalk is an internal sentinel used to halt segment.WalkActive. It never
// escapes walkActiveRegion.
var errStopWalk = errors.New("subscribe: stop active walk")

func decodeSealedBlock(cache *blockCache, segIdx uint64, blockIdx int, r *segment.Reader) ([]segment.Event, error) {
	if cache == nil {
		return r.DecodeBlock(blockIdx)
	}
	return cache.getOrDecode(
		cache.keyForBlock(segIdx, r.Header().Checksum, blockIdx),
		func() ([]segment.Event, error) { return r.DecodeBlock(blockIdx) },
	)
}

// walkSealedSegment mixes the MANIFEST's block index (iteration order,
// seq-bound skip decisions) with offsets from the freshly-opened file.
// That mixing is safe across compaction rewrites ONLY because Rewrite
// preserves block topology and historical seq envelopes (block numbers
// stable, manifest bounds valid supersets). Block repacking — merging
// thinned blocks — must migrate this call site (or generation-check
// the manifest entry) first.
func walkSealedSegment(m *manifest.Manifest, bounds manifest.SegmentBounds, current uint64, stopSeq uint64, cache *blockCache, emit func(*segment.Event) error) (uint64, error) {
	blocks, err := m.BlockIndex(bounds.Idx)
	if err != nil {
		return current, fmt.Errorf("block index for seg %d: %w", bounds.Idx, err)
	}

	r, err := segment.Open(segment.ReaderConfig{Path: bounds.Path, SkipChecksum: true})
	if err != nil {
		return current, fmt.Errorf("open seg %d: %w", bounds.Idx, err)
	}
	defer func() { _ = r.Close() }()

	for i, block := range blocks {
		if stopSeq != 0 && current >= stopSeq {
			return current, nil
		}
		if block.MaxSeq < current {
			continue
		}
		events, err := decodeSealedBlock(cache, bounds.Idx, i, r)
		if err != nil {
			return current, fmt.Errorf("decode seg %d block %d: %w", bounds.Idx, i, err)
		}
		for j := range events {
			if events[j].Seq < current {
				continue
			}
			if stopSeq != 0 && events[j].Seq >= stopSeq {
				return current, nil
			}
			ev := events[j]
			if err := emit(&ev); err != nil {
				return current, err
			}
			current = events[j].Seq + 1
		}
	}
	return current, nil
}

// DefaultBlockCacheBytes bounds the shared decoded-block cache for the cold
// (disk replay) path. Operator-tunable via --subscribe-block-cache-bytes.
const DefaultBlockCacheBytes = 64 << 20

// ColdReaderConfig wires the cold (disk) read path. The writer is held by
// reference (atomic.Pointer) because cmd/jetstream publishes it after
// steady-state begins; before then a cold read returns errColdUnavailable.
type ColdReaderConfig struct {
	Manifest        *manifest.Manifest
	WriterRef       *atomic.Pointer[ingest.Writer]
	BlockCacheBytes int // 0 -> DefaultBlockCacheBytes
}

// errBatchFull is the sentinel the bounded collector returns to stop the
// walk once max entries are gathered. Never escapes ColdReader.Read.
var errBatchFull = errors.New("subscribe: cold batch full")

// ColdReader serves bounded cold-path reads from disk and owns the decoded
// block cache shared by those reads.
type ColdReader struct {
	manifest  *manifest.Manifest
	writerRef *atomic.Pointer[ingest.Writer]
	cache     *blockCache
}

// NewColdReader returns a ColdReader that serves bounded batches from disk
// via WalkFromCursor, routing sealed-block decodes through a shared, byte-
// bounded block cache. Read stops after max events and reports the next cursor
// so the subscriber loop resumes contiguously.
func NewColdReader(cfg ColdReaderConfig) *ColdReader {
	bytes := cfg.BlockCacheBytes
	if bytes <= 0 {
		bytes = DefaultBlockCacheBytes
	}
	return &ColdReader{
		manifest:  cfg.Manifest,
		writerRef: cfg.WriterRef,
		cache:     newBlockCache(bytes),
	}
}

// InvalidateSegment purges decoded blocks for segIdx from the cold read cache.
func (r *ColdReader) InvalidateSegment(idx uint64) {
	if r == nil || r.cache == nil {
		return
	}
	r.cache.invalidateSegment(idx)
}

// Read serves a bounded batch from disk, stopping after max entries and
// returning the next cursor so the subscriber loop resumes contiguously.
func (r *ColdReader) Read(ctx context.Context, cursor uint64, max int) ([]*Entry, uint64, error) {
	if r == nil || r.writerRef == nil {
		return nil, cursor, errColdUnavailable
	}
	w := r.writerRef.Load()
	if w == nil {
		return nil, cursor, errColdUnavailable
	}
	batch := make([]*Entry, 0, max)
	next := cursor
	floor := w.ReadLog().FloorSeq()
	err := WalkFromCursor(ctx, WalkInput{
		StartSeq:   cursor,
		StopSeq:    floor,
		Manifest:   r.manifest,
		Writer:     w,
		BlockCache: r.cache,
	}, func(ev *segment.Event) error {
		if floor > 0 && ev.Seq >= floor {
			return errBatchFull
		}
		cp := *ev
		batch = append(batch, newEntry(&cp))
		next = ev.Seq + 1
		if len(batch) >= max {
			return errBatchFull
		}
		return nil
	})
	if err != nil && !errors.Is(err, errBatchFull) {
		return nil, cursor, err
	}
	if len(batch) == 0 && floor > cursor {
		next = floor
	}
	return batch, next, nil
}
