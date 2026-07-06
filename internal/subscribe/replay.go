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
	// without sealed segments still walk the active segment + pending.
	Manifest *manifest.Manifest

	// Writer is the ingest writer; the walker reads its active segment's
	// flushed blocks to extend past the sealed-segment region. Required.
	Writer *ingest.Writer

	// BlockCache, when non-nil, serves sealed-block decodes through the shared
	// cache instead of decoding directly. Optional; nil preserves direct decode.
	BlockCache *blockCache

	// onSeamRetry, when non-nil, is invoked once for each rotation-seam
	// convergence retry (i.e. each time the active region reports a hole — or
	// an empty active successor leaves a sub-tip gap — and the walk re-enters
	// the sealed sweep to fill it). Optional; used by
	// tests to assert the retry path is exercised, and a natural hook point
	// for a future operator metric counting how often the cold-read seam is
	// hit. It carries the seq at which the hole was observed.
	onSeamRetry func(holeSeq uint64)
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
// # Rotation-seam convergence
//
// The three sources are read non-atomically: sealed segments come from the
// manifest (under the manifest lock), the active region from the writer
// (under the writer lock). A segment rotation completing BETWEEN the sealed
// read and the active read used to drop a whole segment's seq range. The
// rotation (ingest.Writer.rotateLocked) does, all under the writer lock:
//
//	seal(N) -> publish N to the manifest -> activeIdx = N+1
//
// So a walker could snapshot the manifest BEFORE N was published (its sealed
// loop sees nothing at/after the cursor and stops), then read the active
// region AFTER the rotation (activeIdx already N+1) — leaving N reachable via
// neither source. The old code then emitted N+1's events (all above the
// cursor), jumping PAST N's seq range and silently dropping it. See issue #190.
//
// Two properties close the seam:
//
//   - Strict contiguity (correctness / no silent loss): the active region
//     never emits an event whose Seq is ABOVE the running cursor. The instant
//     it sees a hole (the next available event is past `current`), it stops.
//     A walk therefore emits a gap-free prefix and the caller's cursor lands
//     exactly at the hole — the skipped range is re-fetched on the next read,
//     never jumped over. This holds even if the loop below did nothing.
//
//   - Convergence (seamlessness / no spurious disconnect): on a detected hole
//     we re-enter the sealed sweep from `current` and retry. A hole is either
//     an in-band gap (the active region saw an event above `current`) or a
//     sub-tip gap with NO in-band signal: rotation sealed+published N, bumped
//     activeIdx to N+1, and opened an EMPTY N+1, so the active sweep emits
//     nothing yet `current < Writer.NextSeq()`. Both retry. This fills the
//     hole within the same call instead of forcing a caller re-invocation
//     (which, at a hot-ring miss, would surface as a disconnect). It is sound
//     and bounded because:
//
//   - rotateLocked publishes N to the manifest BEFORE bumping activeIdx,
//     so once this goroutine has observed activeIdx >= N+1 (which a hole
//     implies — the active file sits above the hole), a SUBSEQUENT
//     manifest read in the same goroutine is guaranteed to contain N.
//     The retry's sealed sweep therefore fills exactly the missed
//     segment(s) and `current` strictly advances.
//
//   - events are only ever MOVED active->sealed, never deleted from under
//     the cursor (compaction preserves each segment's historical seq
//     envelope), so a hole is always fillable, never a true void.
//     Because each retry that observed a hole must strictly advance `current`
//     on its successor pass, two consecutive holes with no advance is an
//     invariant violation (e.g. the publish-before-bump ordering broke); we
//     surface that loudly rather than spin.
func WalkFromCursor(ctx context.Context, input WalkInput, emit func(*segment.Event) error) error {
	current := input.StartSeq

	if input.Manifest == nil {
		// No sealed segments to race against: read the active region once,
		// leniently (there is no manifest that could later fill a hole, so
		// stopping at one would just wedge the caller). This preserves the
		// documented nil-manifest contract.
		_, _, err := walkActiveRegion(input, current, false, emit)
		return err
	}

	if err := input.Manifest.Wait(ctx); err != nil {
		return err
	}

	noAdvanceHoles := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		passStart := current

		// 1. Sealed segments.
		next, err := walkSealedRegion(ctx, input, current, emit)
		if err != nil {
			return err
		}
		current = next

		// 2. Active segment's flushed blocks + 3. pending in-memory block.
		// activeIdx is read fresh inside walkActiveRegion each pass, so a
		// rotation since the last pass moves us onto the new active file and
		// the just-sealed file is recovered by the next sealed sweep.
		next, hole, err := walkActiveRegion(input, current, true, emit)
		if err != nil {
			return err
		}
		current = next

		if !hole {
			// The active region reported no in-band hole, but a rotation can
			// still have left an UNFILLED gap below the live tip that the
			// active sweep cannot see: rotateLocked seals+publishes segment N,
			// bumps activeIdx to N+1, and opens an EMPTY N+1. A walk that read
			// the manifest before N was published then sees an empty active
			// successor — it emits nothing and reports hole=false, yet
			// current still sits at the start of N's range. Returning here
			// would hand the caller a non-advancing batch (next==cursor) and
			// disconnect the subscriber even though N is now recoverable from
			// the manifest. So: if the writer has durably allocated seqs at or
			// above current, the gap is fillable — treat it as a hole and
			// retry the sealed sweep, which (per the happens-before below)
			// now sees the freshly-published segment(s). Only when current has
			// reached the live tip is the walk genuinely complete.
			tip := input.Writer.NextSeq()
			if input.StopSeq != 0 {
				tip = input.StopSeq
			}
			if current >= tip {
				return nil
			}
			// Otherwise fall through to the retry: a sub-tip gap the empty
			// active successor could not serve. (We do not set hole=true —
			// it is not read again before the next pass reassigns it.)
		}

		// A hole was detected (either the active region saw an event above
		// current, or the convergence check above found durable seqs the
		// active successor could not serve). Retry the sealed sweep, which
		// (per the happens-before above) now sees the freshly-published
		// segment(s).
		if input.onSeamRetry != nil {
			input.onSeamRetry(current)
		}
		if current == passStart {
			noAdvanceHoles++
			if noAdvanceHoles >= 2 {
				// We retried after observing a hole and STILL neither swept
				// past it nor advanced. The publish-before-bump invariant
				// guarantees a post-hole manifest read contains the missing
				// segment, so this should be unreachable; crash loud rather
				// than spin (CLAUDE.md: crashing > corruption/hangs).
				return fmt.Errorf("subscribe: walk did not converge at seq %d "+
					"(rotation seam invariant violated)", current)
			}
		} else {
			noAdvanceHoles = 0
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
		bounds, ok := input.Manifest.SegmentForSeq(current)
		if !ok {
			return current, nil
		}
		next, err := walkSealedSegment(input.Manifest, bounds, current, input.BlockCache, emit)
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
// flushed blocks and then its in-memory pending block, in seq order, and
// returns the next unemitted seq.
//
// When strict is true it enforces contiguity: it emits only events whose Seq
// equals the running cursor, and stops at the first event ABOVE the cursor
// (a hole), reporting hole=true. The caller fills the hole from the manifest
// and retries. When strict is false (nil-manifest callers, who have no
// manifest that could ever fill a hole) it emits every event >= the cursor,
// matching the legacy lenient contract; hole is always false.
//
// A concurrent seal of the active file is benign: segment.Seal only appends
// the footer and patches the fixed header, never rewriting the frame region,
// so the flushed frames stay byte-intact. If WalkActive runs past them into
// footer bytes it fails loud (the footer's leading bytes don't decode as a
// zstd frame) and emits nothing partial; that error propagates here and the
// subscriber reconnects, by which point the file is a normal sealed segment
// the sealed sweep will serve. See segment/walkactive_seal_test.go.
func walkActiveRegion(input WalkInput, start uint64, strict bool, emit func(*segment.Event) error) (next uint64, hole bool, err error) {
	current := start

	// emitErr captures an error returned by the CALLER's emit (including the
	// cold reader's errBatchFull control signal). It is kept distinct from a
	// WalkActive I/O/decode error so the former propagates verbatim while only
	// the latter is wrapped as "walk active". WalkActive's callback signals
	// "stop now" via errStopWalk regardless of which case fired.
	var emitErr error

	// emitOne applies the skip/emit/hole decision for a single event, setting
	// emitErr (and returning stop=true) on a caller emit error, or setting
	// hole (and stop=true) at a strict-contiguity gap.
	emitOne := func(ev *segment.Event) (stop bool) {
		switch {
		case ev.Seq < current:
			return false // already emitted or below start
		case strict && ev.Seq > current:
			hole = true
			return true // halt at the hole; caller fills it
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
		return current, hole, emitErr
	case errors.Is(walkErr, errStopWalk):
		// Strict hole inside the flushed region: do not read pending — it
		// only sits ABOVE the flushed tail, so it cannot fill a hole below.
		return current, hole, nil
	case walkErr != nil && !errors.Is(walkErr, os.ErrNotExist):
		return current, hole, fmt.Errorf("walk active: %w", walkErr)
	}

	if strict && input.StopSeq != 0 {
		return current, hole, nil
	}

	// Pending in-memory block for nil-manifest test callers. Production cold
	// replay runs in strict mode behind the writer read log and must not read
	// not-yet-durable memory here.
	pending := input.Writer.SnapshotPending()
	for i := range pending {
		if emitOne(&pending[i]) {
			// stop: either a caller emit error (propagate) or a strict hole
			// (current/hole already set). Either way, halt the pending scan.
			if emitErr != nil {
				return current, hole, emitErr
			}
			break
		}
	}
	return current, hole, nil
}

// errStopWalk is an internal sentinel used to halt segment.WalkActive at a
// strict-contiguity hole. It never escapes walkActiveRegion.
var errStopWalk = errors.New("subscribe: stop active walk at hole")

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
func walkSealedSegment(m *manifest.Manifest, bounds manifest.SegmentBounds, current uint64, cache *blockCache, emit func(*segment.Event) error) (uint64, error) {
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
// (disk replay) path. Smaller than the hot ring: the cold path is the
// less-common case. Operator-tunable via --subscribe-block-cache-bytes.
const DefaultBlockCacheBytes = 64 << 20

// ColdReaderConfig wires the cold (disk) read path. The writer is held by
// reference (atomic.Pointer) because cmd/jetstream publishes it after
// steady-state begins; before then a cold read returns errColdUnavailable.
type ColdReaderConfig struct {
	Manifest        *manifest.Manifest
	WriterRef       *atomic.Pointer[ingest.Writer]
	BlockCacheBytes int // 0 -> DefaultBlockCacheBytes
	// StopAtReadLogFloor bounds cold replay at Writer.ReadLog().FloorSeq().
	// Enable this when Tail is wired to the writer read log; leave false for
	// tests that model hot delivery through Tail.Append without a writer log.
	StopAtReadLogFloor bool
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
	stopFloor bool
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
		stopFloor: cfg.StopAtReadLogFloor,
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
	var floor uint64
	if r.stopFloor {
		floor = w.ReadLog().FloorSeq()
	}
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
