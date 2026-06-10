package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/crashpoint"
	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream-v2/internal/ingest/live"
	"github.com/bluesky-social/jetstream-v2/internal/obs"
	"golang.org/x/sync/errgroup"
)

// runBootstrap is the orchestrator's State 0. It builds:
//
//   - a shared ingest.Writer pointed at <DataDir>/segments (used by
//     the backfill engine; closed in State 4 of the cutover),
//   - the backfill engine itself,
//   - a live.Consumer pointed at <DataDir>/backfill/live_segments
//     with the throwaway "live_segments/seq/next" seq counter and
//     the shared "relay/cursor" upstream cursor.
//
// It runs the backfill engine and the live consumer as siblings
// under an internal errgroup, with the live consumer attached to a
// derived context the orchestrator can cancel independently. When
// backfill drains (returns nil), runBootstrap walks the cutover:
//
//  1. State 1: WritePhase(merging) inside the backfill goroutine.
//  2. State 2: cancel the live consumer's derived context, await
//     its Run return via g.Wait().
//  3. State 3: Close the live consumer (persists cursor + flushes),
//     then re-open the live_segments dir and SealActiveAndClose its
//     trailing active segment so the tree is fully sealed.
//  4. State 4: Close the backfill writer (flush only — steady-state
//     will reopen the same directory).
//
// On success, runBootstrap returns nil and the caller falls through
// to the merge case. On any subsystem error before backfill drains,
// the errgroup cancels both and the error is returned without
// touching the phase.
func (o *Orchestrator) runBootstrap(ctx context.Context) error {
	return obs.Span(ctx, func(ctx context.Context) error {
		segmentsDir := filepath.Join(o.cfg.DataDir, "segments")
		liveSegmentsDir := filepath.Join(o.cfg.DataDir, "backfill", "live_segments")

		// Backfill writer (shared with the backfill engine).
		bw, err := ingest.Open(ingest.Config{
			SegmentsDir:    segmentsDir,
			Store:          o.cfg.Store,
			Logger:         o.cfg.Logger,
			Metrics:        o.cfg.IngestMetrics,
			SegmentMetrics: o.cfg.SegmentMetrics,
			OnAfterSeal:    o.cfg.IngestOnAfterSeal,
		})
		if err != nil {
			return fmt.Errorf("orchestrator: open backfill ingest writer: %w", err)
		}

		// Bootstrap-time live consumer.
		bootstrapLive, err := live.Open(live.Config{
			SegmentsDir:      liveSegmentsDir,
			Store:            o.cfg.Store,
			SeqKey:           live.BootstrapSeqKey,
			CursorKey:        live.CursorKey,
			RelayURL:         o.cfg.RelayURL,
			Logger:           o.cfg.Logger,
			Metrics:          o.cfg.LiveMetrics,
			Verifier:         o.cfg.Verifier,
			SyncStateStore:   o.cfg.SyncStateStore,
			SegmentMetrics:   o.cfg.SegmentMetrics,
			OnEvent:          o.cfg.OnBootstrapLiveEvent,
			ReconnectBackoff: o.cfg.LiveReconnectBackoff,
		})
		if err != nil {
			if cerr := bw.Close(); cerr != nil {
				o.logger.WarnContext(ctx, "backfill writer close after bootstrap-live open failure", "err", cerr)
			}
			return fmt.Errorf("orchestrator: open bootstrap-live consumer: %w", err)
		}

		g, gctx := errgroup.WithContext(ctx)

		// Derived context the orchestrator cancels at cutover-time.
		// Wrapping gctx means errgroup-driven cancellation (e.g. from a
		// backfill error) also propagates to the live consumer, while
		// still letting us call cancelLive() to stop ONLY the live
		// consumer when backfill drains successfully — gctx remains
		// uncancelled so the backfill goroutine can return nil normally.
		liveCtx, cancelLive := context.WithCancel(gctx)
		defer cancelLive()

		// drainStartUnixNano is set by the backfill goroutine just before
		// cancelLive(); the live goroutine reads it after observing
		// liveCtx.Done(). Atomic rather than a plain time.Time because the
		// happens-before via context.cancel is implicit and a future
		// refactor that moves the read off the cancel chain would race.
		// Zero means cutover was not initiated cleanly (error path).
		var drainStartUnixNano atomic.Int64

		g.Go(func() error {
			err := backfill.Run(gctx, backfill.Config{
				Store:             o.cfg.Store,
				HTTPClient:        o.cfg.HTTPClient,
				Directory:         o.cfg.Directory,
				Writer:            bw,
				RelayURL:          o.cfg.RelayURL,
				Logger:            o.cfg.Logger,
				Metrics:           o.cfg.BackfillMetrics,
				MaxRepos:          o.cfg.MaxBackfillRepos,
				BackfillRepos:     o.cfg.BackfillRepos,
				RetryBaseDelay:    o.cfg.BackfillRetryBaseDelay,
				AfterRepoComplete: o.cfg.AfterRepoComplete,
				CrashInjector:     o.cfg.CrashInjector,
			})
			if err != nil {
				return err
			}

			// Backfill drained cleanly. Trigger the cutover by writing
			// phase=merging FIRST (commit point #1), THEN cancelling the
			// live consumer. The order matters: the phase write is the
			// only durable signal that backfill has finished, and a crash
			// after the phase write recovers via PhaseMerging restart.
			o.logger.InfoContext(ctx, "cutover begin")
			if err := o.writeMergingPhase(); err != nil {
				return err
			}

			// Cancel the bootstrap-live consumer's context. This signals
			// state 2. The live consumer's Run goroutine returns shortly,
			// and its return value is the second errgroup goroutine's
			// result.
			drainStartUnixNano.Store(time.Now().UnixNano())
			cancelLive()
			return nil
		})

		g.Go(func() error {
			err := bootstrapLive.Run(liveCtx)

			// If the only thing that happened is the orchestrator's own
			// cancelLive() call (after backfill drained successfully),
			// liveCtx is cancelled but ctx (the outer process ctx) is
			// still healthy. Treat that as a clean stop.
			if err != nil && errors.Is(err, context.Canceled) && liveCtx.Err() != nil && ctx.Err() == nil {
				if startNs := drainStartUnixNano.Load(); startNs != 0 {
					o.cfg.Metrics.observeState("drain_bootstrap", time.Since(time.Unix(0, startNs)).Seconds())
				}
				return nil
			}
			return err
		})

		if err := g.Wait(); err != nil {
			// Best-effort cleanup. Close errors are logged, not returned,
			// because the underlying error is what we want surfaced.
			if cerr := bootstrapLive.Close(); cerr != nil {
				o.logger.WarnContext(ctx, "bootstrap-live close after error", "err", cerr)
			}
			if cerr := bw.Close(); cerr != nil {
				o.logger.WarnContext(ctx, "backfill writer close after error", "err", cerr)
			}
			return err
		}

		// Success path. Cleanup ordering is: try to seal/close everything
		// even if an intermediate step fails, then return the first error.
		// We must reach bw.Close() so its Flush+saveNextSeq run; a leaked
		// bw also leaks fd/locking on data/segments.
		return o.finishBootstrap(ctx, bootstrapLive, bw, liveSegmentsDir)
	})
}

// finishBootstrap drives States 3 and 4. Split out so the success
// path's cleanup pattern is clear and uniform: every resource gets a
// best-effort termination, the first error is reported, subsequent
// errors are logged.
//
// ctx is the parent runBootstrap context (NOT a fresh
// context.Background); a fresh ctx would orphan finishBootstrap's
// span from the runBootstrap span tree and break trace lineage.
func (o *Orchestrator) finishBootstrap(ctx context.Context, bootstrapLive *live.Consumer, bw *ingest.Writer, liveSegmentsDir string) error {
	return obs.Span(ctx, func(ctx context.Context) (retErr error) {
		// State 4 cleanup runs last (LIFO). bw.Close persists nextSeq for
		// the data/segments directory.
		defer func() {
			closeStart := time.Now()
			err := bw.Close()
			o.cfg.Metrics.observeState("close_backfill", time.Since(closeStart).Seconds())
			if err != nil {
				if retErr == nil {
					retErr = fmt.Errorf("orchestrator: close backfill ingest writer: %w", err)
				} else {
					o.logger.WarnContext(ctx, "backfill writer close failed after earlier error", "err", err)
				}
				return
			}
		}()

		// State 3: flush the bootstrap-live consumer and seal its trailing
		// active segment. Close() persists the upstream cursor and the
		// throwaway seq counter; the seal-via-reopen below finalizes the
		// active file's footer + header so the live_segments tree is
		// fully sealed once steady-state begins.
		//
		// If the bootstrap consumer's underlying writer happened to seal
		// its active file during normal rotation just before we got here,
		// ingest.Open rolls forward to a fresh empty seg_<N+1>.jss and
		// SealActiveAndClose seals that empty file. The future compactor
		// reads zero events from such a file and ignores it.
		start := time.Now()
		if err := bootstrapLive.Close(); err != nil {
			return fmt.Errorf("orchestrator: close bootstrap-live consumer: %w", err)
		}
		if err := o.simulateCrash(ctx, crashpoint.AfterBootstrapLiveCloseBeforeSeal); err != nil {
			return err
		}

		sealW, err := ingest.Open(ingest.Config{
			SegmentsDir: liveSegmentsDir,
			Store:       o.cfg.Store,
			SeqKey:      live.BootstrapSeqKey,
			// Bare cfg.Logger; ingest.Open sets its own component.
			Logger: o.cfg.Logger,
			// Metrics nil to match the bootstrap-live consumer convention
			// (live/consumer.go Open): bootstrap-time live writes are not
			// counted in steady-state ingest counters, and the trailing
			// seal is a continuation of that lifetime.
			Metrics: nil,
			// SegmentMetrics IS shared though — the seal_duration
			// histogram is a global concern and we want every
			// segment.Writer in the process recording into the same
			// series.
			SegmentMetrics: o.cfg.SegmentMetrics,
		})
		if err != nil {
			return fmt.Errorf("orchestrator: re-open bootstrap-live writer for seal: %w", err)
		}
		if err := sealW.SealActiveAndClose(); err != nil {
			return fmt.Errorf("orchestrator: seal bootstrap-live segment: %w", err)
		}

		o.cfg.Metrics.observeState("seal_bootstrap", time.Since(start).Seconds())
		return nil
	})
}
