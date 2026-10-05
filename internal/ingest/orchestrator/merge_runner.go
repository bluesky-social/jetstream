// merge_runner.go owns the per-source-segment
// drain loop. One goroutine, serial, no fan-out. Spec:
// specs/notes/2026-05-27-merge-phase-design.md §4.2.

package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/segment"
)

type mergeRunner struct {
	dst           *ingest.Writer
	store         metastore.Store
	src           mergeSource
	logger        *slog.Logger
	metrics       *Metrics
	crashInjector crashpoint.Injector
	now           func() time.Time // overridable for tests
	cache         *repoStatusLookup
	// readAhead is how many source blocks are fetched and decoded ahead
	// of the one being drained. Zero reads one block at a time, which
	// keeps local-mode fault and crash tests deterministic; disaggregated
	// merge sets mergeReadAhead, since each read is an object GET.
	readAhead int
}

// mergeReadAhead is the disaggregated drain's source read-ahead.
const mergeReadAhead = 8

// newMergeRunner builds a drain runner. injector is the test-only crash
// simulator (crashpoint.Injector); production callers thread
// o.cfg.CrashInjector, which is nil in production, making every
// simulateCrash checkpoint a no-op. Pass nil to disable injection.
//
// Sources are src's catalog.BootstrapLive segments.
func newMergeRunner(dst *ingest.Writer, st metastore.Store, src mergeSource, logger *slog.Logger, m *Metrics, injector crashpoint.Injector) *mergeRunner {
	r := &mergeRunner{
		dst:           dst,
		store:         st,
		src:           src,
		logger:        logger.With(slog.String("component", "orchestrator/merge")),
		metrics:       m,
		crashInjector: injector,
		now:           func() time.Time { return time.Now().UTC() },
	}
	r.cache = newRepoStatusLookup(st, m.incMergeDIDLookups)
	return r
}

// run drains every source seg whose index >= the persisted cursor,
// committing per-source. Returns nil on full drain; returns
// ctx.Err() if the context is cancelled mid-drain so the
// orchestrator can distinguish a clean stop from real failure.
func (r *mergeRunner) run(ctx context.Context) error {
	return obs.Span(ctx, func(ctx context.Context) error {
		fromIdx, err := loadMergeCursor(r.store)
		if err != nil {
			return err
		}

		// Nothing writes the sources during merge, so one read serves the
		// whole drain.
		all, err := r.src.segments(ctx)
		if err != nil {
			return err
		}

		// Skip already-drained sources; verify contiguity from fromIdx.
		var todo []sourceSegment
		expectIdx := fromIdx
		for i, sf := range all {
			if sf.index < fromIdx {
				continue
			}
			if sf.index != expectIdx {
				return fmt.Errorf("orchestrator: merge: source index gap: expected %d, got %d", expectIdx, sf.index)
			}
			if sf.state != catalog.Sealed {
				// A disaggregated seal opens the next segment (design
				// §10.8), so the trailing segment is active and empty.
				if i == len(all)-1 && sf.blocks == 0 {
					break
				}
				return fmt.Errorf("orchestrator: merge: source segment %d is %s", sf.index, sf.state)
			}
			todo = append(todo, sf)
			expectIdx++
		}

		for _, sf := range todo {
			if err := ctx.Err(); err != nil {
				return err
			}
			perDID, err := r.processSourceSegment(ctx, sf)
			if err != nil {
				return err
			}
			if err := r.simulateCrash(ctx, crashpoint.AfterMergeDstFlushBeforeSourceCommit); err != nil {
				return err
			}
			if err := commitSourceComplete(r.store, r.cache, sf.index+1, perDID, r.now()); err != nil {
				return err
			}
			r.metrics.incMergeSegmentsConsumed()
			r.metrics.addMergeRepoRevsUpdated(len(perDID))
		}
		return nil
	})
}

func (r *mergeRunner) simulateCrash(ctx context.Context, point crashpoint.Point) error {
	if r.crashInjector == nil {
		return nil
	}
	return r.crashInjector.SimulateCrash(ctx, point)
}

// processSourceSegment reads one source seg's blocks,
// applies the keep/drop predicate, appends survivors with re-stamped
// WitnessedAt, returns the per-DID last-seen rev map. dst.Flush is
// called before returning so the cursor commit that follows is
// ordered after a fsync (§5.2).
//
// Each block's repo rows are read in one batch before its events are
// filtered, and only for the event kinds shouldKeep checks against the
// row; a remote store would otherwise pay a round trip per DID.
func (r *mergeRunner) processSourceSegment(ctx context.Context, sf sourceSegment) (map[string]string, error) {
	return obs.Span2(ctx, func(ctx context.Context) (map[string]string, error) {
		perDID := make(map[string]string)
		blocks, stop := r.readBlocks(ctx, sf)
		defer stop()

		var dids []string
		for i, ref := range sf.refs {
			blk, ok := <-blocks
			if !ok {
				// readBlocks closes early only on cancellation.
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				return nil, fmt.Errorf("orchestrator: merge: source segment %d ended after %d of %d blocks", sf.index, i, len(sf.refs))
			}
			if blk.err != nil {
				return nil, fmt.Errorf("orchestrator: merge: decode source segment %d block %d: %w", sf.index, ref.Block, blk.err)
			}
			if blk.index != i {
				return nil, fmt.Errorf("orchestrator: merge: source segment %d read block %d out of order (want %d)", sf.index, blk.index, i)
			}
			events := blk.events

			dids = dids[:0]
			for j := range events {
				if isBackfillRevFilteredKind(events[j].Kind) {
					dids = append(dids, events[j].DID)
				}
			}
			if err := r.cache.prefill(ctx, dids); err != nil {
				return nil, err
			}

			for j := range events {
				ev := &events[j]
				var rs *backfill.RepoStatus
				if isBackfillRevFilteredKind(ev.Kind) {
					var lerr error
					if rs, lerr = r.cache.get(ev.DID); lerr != nil {
						return nil, lerr
					}
				}
				if !shouldKeep(ev, rs) {
					r.metrics.incMergeEventsDropped()
					continue
				}
				ev.WitnessedAt = r.now().UnixMicro() // §3.4 re-stamp
				if err := r.dst.Append(ctx, ev); err != nil {
					return nil, fmt.Errorf("orchestrator: merge: append: %w", err)
				}
				r.metrics.incMergeEventsKept()
				if isBackfillRevFilteredKind(ev.Kind) && ev.Rev != "" {
					perDID[ev.DID] = ev.Rev
				}
			}
		}

		// Force any pending dst block to fsync before the cursor
		// commit (durability ordering, spec §5.2).
		if err := r.dst.Flush(ctx); err != nil {
			return nil, fmt.Errorf("orchestrator: merge: flush dst: %w", err)
		}
		return perDID, nil
	})
}

// sourceBlock is one decoded source block, or the error reading it.
type sourceBlock struct {
	index  int
	events []segment.Event
	err    error
}

// readBlocks streams sf's blocks, decoded, in order. With readAhead > 0 it
// keeps up to readAhead blocks fetching concurrently ahead of the
// consumer. The stream stops after the first error. stop cancels any
// outstanding reads and waits for them, and must be called.
func (r *mergeRunner) readBlocks(ctx context.Context, sf sourceSegment) (<-chan sourceBlock, func()) {
	ctx, cancel := context.WithCancel(ctx)
	fetcher := r.src.fetcher()
	out := make(chan sourceBlock)
	var wg sync.WaitGroup

	if r.readAhead <= 0 {
		wg.Go(func() {
			defer close(out)
			for i, ref := range sf.refs {
				events, err := catalog.DecodeRef(ctx, fetcher, ref)
				select {
				case out <- sourceBlock{index: i, events: events, err: err}:
				case <-ctx.Done():
					return
				}
				if err != nil {
					return
				}
			}
		})
		return out, func() { cancel(); wg.Wait() }
	}

	// Each block gets a one-slot result channel, queued in order; the
	// queue's capacity bounds the reads in flight.
	pending := make(chan chan sourceBlock, r.readAhead)
	wg.Go(func() {
		defer close(pending)
		for i, ref := range sf.refs {
			res := make(chan sourceBlock, 1)
			select {
			case pending <- res:
			case <-ctx.Done():
				return
			}
			wg.Go(func() {
				events, err := catalog.DecodeRef(ctx, fetcher, ref)
				res <- sourceBlock{index: i, events: events, err: err}
			})
		}
	})
	wg.Go(func() {
		defer close(out)
		for res := range pending {
			var blk sourceBlock
			select {
			case blk = <-res:
			case <-ctx.Done():
				return
			}
			select {
			case out <- blk:
			case <-ctx.Done():
				return
			}
			if blk.err != nil {
				return
			}
		}
	})
	return out, func() { cancel(); wg.Wait() }
}
