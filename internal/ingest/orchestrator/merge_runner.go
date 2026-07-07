// merge_runner.go owns the per-source-segment
// drain loop. One goroutine, serial, no fan-out. Spec:
// specs/notes/2026-05-27-merge-phase-design.md §4.2.

package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
)

type mergeRunner struct {
	dst           *ingest.Writer
	store         *store.Store
	sourceDir     string
	logger        *slog.Logger
	metrics       *Metrics
	crashInjector crashpoint.Injector
	now           func() time.Time // overridable for tests
	cache         *repoStatusLookup
}

// newMergeRunner builds a drain runner. injector is the test-only crash
// simulator (crashpoint.Injector); production callers thread
// o.cfg.CrashInjector, which is nil in production, making every
// simulateCrash checkpoint a no-op. Pass nil to disable injection.
func newMergeRunner(dst *ingest.Writer, st *store.Store, sourceDir string, logger *slog.Logger, m *Metrics, injector crashpoint.Injector) *mergeRunner {
	r := &mergeRunner{
		dst:           dst,
		store:         st,
		sourceDir:     sourceDir,
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

		all, err := ingest.SegmentFiles(r.sourceDir)
		if err != nil {
			return fmt.Errorf("orchestrator: merge: list source segments: %w", err)
		}

		// Skip already-drained sources; verify contiguity from fromIdx.
		var todo []ingest.SegmentFile
		expectIdx := fromIdx
		for _, sf := range all {
			if sf.Idx < fromIdx {
				continue
			}
			if sf.Idx != expectIdx {
				return fmt.Errorf("orchestrator: merge: source index gap: expected %d, got %d", expectIdx, sf.Idx)
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
			if err := commitSourceComplete(r.store, r.cache, sf.Idx+1, perDID, r.now()); err != nil {
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

// processSourceSegment opens one source seg, iterates its blocks,
// applies the keep/drop predicate, appends survivors with re-stamped
// WitnessedAt, returns the per-DID last-seen rev map. dst.Flush is
// called before returning so the cursor commit that follows is
// ordered after a fsync (§5.2).
func (r *mergeRunner) processSourceSegment(ctx context.Context, sf ingest.SegmentFile) (map[string]string, error) {
	return obs.Span2(ctx, func(ctx context.Context) (map[string]string, error) {
		rd, err := segment.Open(segment.ReaderConfig{Path: sf.Path})
		if err != nil {
			return nil, fmt.Errorf("orchestrator: merge: open %s: %w", sf.Path, err)
		}
		defer func() { _ = rd.Close() }()

		blockCount := int(rd.Header().BlockCount)
		perDID := make(map[string]string)

		for i := range blockCount {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			events, err := rd.DecodeBlock(i)
			if err != nil {
				return nil, fmt.Errorf("orchestrator: merge: decode %s block %d: %w", sf.Path, i, err)
			}
			for j := range events {
				ev := &events[j]
				rs, lerr := r.cache.get(ev.DID)
				if lerr != nil {
					return nil, lerr
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
