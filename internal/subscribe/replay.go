package subscribe

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/seqspace"
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

	// Catalog supplies the views the walk reads main-namespace blocks from.
	// Required.
	Catalog catalog.Catalog

	// Fetcher reads the blocks the catalog's refs name. Required.
	Fetcher catalog.Fetcher

	// BlockCache, when non-nil, serves sealed-block decodes through the shared
	// cache instead of decoding directly. Optional; nil preserves direct decode.
	BlockCache *blockCache

	// Gaps authorizes explicit cursor jumps across durable seq vacancies.
	Gaps *seqspace.Gaps

	// OnGapJump observes a registered-gap jump. Optional.
	OnGapJump func(start, end uint64)

	// OnStaleRetry, when non-nil, is invoked each time a pass hits a stale
	// ref (a compaction rewrite or a seal of the active segment landed
	// mid-walk) and the walk resumes on a fresh view, carrying the seq it
	// resumes from. Optional; tests use it to observe the retry.
	OnStaleRetry func(seq uint64)
}

// maxStaleRetries bounds consecutive stale-ref retries that make no
// progress. One is expected when a seal or rewrite lands mid-walk; the next
// view already holds the new generation (see catalog.ErrStaleRef), so
// repeated staleness at one seq means the catalog is not converging.
const maxStaleRetries = 8

// WalkFromCursor emits durable events with Seq >= input.StartSeq in seq
// order, reading main-namespace blocks through catalog views. It stops on an
// emit error and preserves errors.Is.
//
// Cached sealed events share read-only Entry values and memoized encodings
// across subscribers. Active and uncached events get per-walk entries.
// NewColdReader adds a batch limit and shared cache; WalkFromCursor holds no
// subscriber state.
//
// StopSeq is the readable-log floor: all seqs below it must be durable, and
// so in any view taken after the floor was read. The walk must cover
// [StartSeq, StopSeq), crossing only registered vacancies, and fails loud on
// any other hole rather than skipping events.
//
// # Rotation and compaction
//
// One pass reads one view, and a view is coherent across rotation: the
// writer publishes a seal before it moves to the next segment, so a view
// never misses a just-sealed segment (#190). What can change under a pass is
// the bytes a ref names, when the active segment is sealed or a sealed one
// is rewritten by compaction. The fetch then fails with catalog.ErrStaleRef
// and the walk resumes from the seq it reached on a fresh view. Compaction
// preserves historical seq envelopes, so resuming never re-emits or skips.
func WalkFromCursor(ctx context.Context, input WalkInput, emit func(*Entry) error) error {
	w := walker{in: input, current: input.StartSeq}
	w.jumpGap()

	stale, staleAt := 0, w.current
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := w.pass(ctx, input.Catalog.Snapshot(), emit)
		if !errors.Is(err, catalog.ErrStaleRef) {
			return err
		}
		if w.current != staleAt {
			stale, staleAt = 0, w.current
		}
		stale++
		if stale > maxStaleRetries {
			return fmt.Errorf("subscribe: cold replay at seq %d: %d stale views in a row: %w", w.current, stale, err)
		}
		if input.OnStaleRetry != nil {
			input.OnStaleRetry(w.current)
		}
	}
}

// walker is one WalkFromCursor's cursor state across passes.
type walker struct {
	in      WalkInput
	current uint64
}

// jumpGap moves current past a registered vacancy containing it.
func (w *walker) jumpGap() bool {
	end, ok := w.in.Gaps.EndContaining(w.current)
	if !ok {
		return false
	}
	start := w.current
	w.current = end
	if w.in.OnGapJump != nil {
		w.in.OnGapJump(start, end)
	}
	return true
}

func (w *walker) done() bool {
	return w.in.StopSeq != 0 && w.current >= w.in.StopSeq
}

// pass walks view's refs from current. It returns catalog.ErrStaleRef
// (wrapped) when a ref's generation is gone, with current at the first
// unemitted seq.
func (w *walker) pass(ctx context.Context, view catalog.CatalogView, emit func(*Entry) error) error {
	stopSeq := w.in.StopSeq
	for ref := range view.RefsFrom(catalog.Main, w.current) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if w.done() {
			return nil
		}
		if ref.MaxSeq < w.current {
			continue
		}
		// Compaction may remove rows inside a block, but it preserves that
		// block's historical [MinSeq, MaxSeq] envelope. It cannot explain a
		// hole between two block envelopes. Crash-abandoned leases occur exactly
		// at such block boundaries and may be crossed only when the durable gap
		// registry authorizes the jump.
		if w.jumpGap() {
			if w.done() {
				return nil
			}
			if ref.MaxSeq < w.current {
				continue
			}
		}
		if ref.MinSeq > w.current {
			if stopSeq != 0 && w.current < stopSeq {
				return fmt.Errorf("subscribe: unregistered sequence hole [%d,%d) before segment %d block %d", w.current, ref.MinSeq, ref.Segment, ref.Block)
			}
			// Unbounded callers retain the historical compaction/replay behavior:
			// emit every extant row without asserting complete coverage to a floor.
			w.current = ref.MinSeq
		}
		entries, err := decodeRef(ctx, w.in.BlockCache, w.in.Fetcher, ref)
		if err != nil {
			return fmt.Errorf("decode seg %d block %d: %w", ref.Segment, ref.Block, err)
		}
		for _, e := range entries {
			seq := e.Event.Seq
			if seq < w.current {
				continue
			}
			if stopSeq != 0 && seq >= stopSeq {
				return nil
			}
			if err := emit(e); err != nil {
				return err
			}
			w.current = seq + 1
		}
		if w.current <= ref.MaxSeq {
			if ref.MaxSeq == ^uint64(0) {
				return fmt.Errorf("subscribe: segment %d block %d max seq overflows replay cursor", ref.Segment, ref.Block)
			}
			w.current = ref.MaxSeq + 1
			if stopSeq != 0 && w.current > stopSeq {
				w.current = stopSeq
			}
		}
	}
	// The view ends below the floor. Only a registered vacancy explains
	// that: the floor promised durable data, and the view was taken after
	// the floor was read.
	w.jumpGap()
	if stopSeq != 0 && w.current < stopSeq {
		return fmt.Errorf("subscribe: cold replay reached the end of the catalog at seq %d before readable-log floor %d (unregistered sequence hole)", w.current, stopSeq)
	}
	return nil
}

// decodeRef returns the block's events wrapped as entries. Sealed blocks go
// through the cache when there is one, and come back as the block's SHARED
// entries (memoized encodes and compressed frames are reused across every
// cold subscriber). Active blocks, and every block without a cache, get
// fresh per-call wrappers over a private decode: the active tail is thin
// relative to a deep replay, and it is only immutable until its segment is
// sealed.
func decodeRef(ctx context.Context, cache *blockCache, f catalog.Fetcher, ref catalog.BlockRef) ([]*Entry, error) {
	if cache == nil || ref.Generation == 0 {
		events, err := catalog.DecodeRef(ctx, f, ref)
		if err != nil {
			return nil, err
		}
		entries := make([]*Entry, len(events))
		for i := range events {
			entries[i] = newEntry(&events[i])
		}
		return entries, nil
	}
	return cache.getOrDecode(cache.keyForRef(ref), func() ([]segment.Event, error) {
		return catalog.DecodeRef(ctx, f, ref)
	})
}

// DefaultBlockCacheBytes bounds the shared decoded-block cache for the cold
// (disk replay) path. Operator-tunable via --subscribe-block-cache-bytes.
const DefaultBlockCacheBytes = 64 << 20

// ColdReaderConfig wires the cold read path. The writer is held by
// reference (atomic.Pointer) because cmd/jetstream publishes it after
// steady-state begins; before then a cold read returns errColdUnavailable.
// The writer supplies the readable-log floor and the durable gap registry;
// the catalog and fetcher supply the blocks.
type ColdReaderConfig struct {
	Catalog catalog.Catalog
	Fetcher catalog.Fetcher
	// Ready, when non-nil, gates reads on the catalog's initial load: a view
	// taken before it would be missing sealed segments. Read blocks on it.
	Ready           func(context.Context) error
	WriterRef       *atomic.Pointer[ingest.Writer]
	BlockCacheBytes int // 0 -> DefaultBlockCacheBytes
	Metrics         *Metrics
}

// errBatchFull is the sentinel the bounded collector returns to stop the
// walk once max entries are gathered. Never escapes ColdReader.Read.
var errBatchFull = errors.New("subscribe: cold batch full")

// ColdReader serves bounded cold-path reads from the archive and owns the
// decoded block cache shared by those reads.
type ColdReader struct {
	catalog   catalog.Catalog
	fetcher   catalog.Fetcher
	ready     func(context.Context) error
	writerRef *atomic.Pointer[ingest.Writer]
	cache     *blockCache
	metrics   *Metrics
}

// NewColdReader returns a ColdReader that serves bounded batches from the
// archive via WalkFromCursor, routing sealed-block decodes through a shared,
// byte-bounded block cache. Read stops after max events and reports the next
// cursor so the subscriber loop resumes contiguously.
func NewColdReader(cfg ColdReaderConfig) *ColdReader {
	bytes := cfg.BlockCacheBytes
	if bytes <= 0 {
		bytes = DefaultBlockCacheBytes
	}
	return &ColdReader{
		catalog:   cfg.Catalog,
		fetcher:   cfg.Fetcher,
		ready:     cfg.Ready,
		writerRef: cfg.WriterRef,
		cache:     newBlockCache(bytes),
		metrics:   cfg.Metrics,
	}
}

// InvalidateSegment purges decoded blocks for segIdx from the cold read
// cache. Cache keys already pin the segment generation, so this only frees
// memory early after a compaction; it is local-only.
func (r *ColdReader) InvalidateSegment(idx uint64) {
	if r == nil || r.cache == nil {
		return
	}
	r.cache.invalidateSegment(idx)
}

// Read serves a bounded batch from the archive, stopping after max entries
// and returning the next cursor so the subscriber loop resumes contiguously.
func (r *ColdReader) Read(ctx context.Context, cursor uint64, max int) ([]*Entry, uint64, error) {
	if r == nil || r.writerRef == nil || r.catalog == nil {
		return nil, cursor, errColdUnavailable
	}
	w := r.writerRef.Load()
	if w == nil {
		return nil, cursor, errColdUnavailable
	}
	if r.ready != nil {
		if err := r.ready(ctx); err != nil {
			return nil, cursor, fmt.Errorf("subscribe: catalog load: %w", err)
		}
	}
	batch := make([]*Entry, 0, max)
	next := cursor
	// The floor is read before WalkFromCursor takes its view, so every seq
	// below it is in that view.
	floor := w.ReadLog().FloorSeq()
	var onGapJump func(start, end uint64)
	if r.metrics != nil {
		onGapJump = r.metrics.incGapJump
	}
	err := WalkFromCursor(ctx, WalkInput{
		StartSeq:   cursor,
		StopSeq:    floor,
		Catalog:    r.catalog,
		Fetcher:    r.fetcher,
		BlockCache: r.cache,
		Gaps:       w.SeqGaps(),
		OnGapJump:  onGapJump,
	}, func(e *Entry) error {
		// Sealed-region entries arrive SHARED from the block cache (the #295
		// fix: concurrent cold subscribers reuse one memoized encode and one
		// compressed frame per event, like the hot path); active-region
		// entries are per-walk. Either way they are appended as-is.
		if floor > 0 && e.Event.Seq >= floor {
			return errBatchFull
		}
		batch = append(batch, e)
		next = e.Event.Seq + 1
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
