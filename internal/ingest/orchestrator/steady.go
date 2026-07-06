package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/ingest/live"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/segment"
	"golang.org/x/sync/errgroup"
)

// runSteadyState opens a live.Consumer pointed at <DataDir>/segments
// and runs it until ctx is cancelled. Returns ctx.Err() on a clean
// shutdown; the underlying error otherwise.
//
// live.Consumer constructs and owns its own *ingest.Writer pointed
// at the same SegmentsDir. We deliberately do NOT open a second
// writer here: two writers sharing one active segment would race on
// the file offset, hand out duplicate seq numbers, and clobber each
// other's "seq/next" on close.
//
// The consumer's internal ingest.Open runs ScanMaxSeq against the
// active segment in <DataDir>/segments and reconciles the in-memory
// nextSeq against pebble's "seq/next", so steady-state continues
// exactly where the backfill writer left off.
//
// Pebble keys "seq/next" and "relay/cursor" are the steady-state
// defaults; the upstream firehose resumes from the bootstrap-time
// consumer's last persisted cursor and at-least-once delivery
// covers the at-most-one-block overlap.
func (o *Orchestrator) runSteadyState(ctx context.Context) error {
	return obs.Span(ctx, func(ctx context.Context) error {
		// Set the phase gauge before any I/O. On the resume-from-
		// PhaseSteadyState path Run dispatches here directly without
		// going through writeSteadyStatePhase, so this is the only place
		// the gauge gets set on that path.
		o.cfg.Metrics.setPhase(PhaseGaugeSteadyState)

		segmentsDir := filepath.Join(o.cfg.DataDir, "segments")
		if err := o.rebuildLiveTombstones(ctx); err != nil {
			return err
		}

		// With compaction disabled nothing ever evicts the live set, so
		// feeding it would leak memory without bound; leave it detached.
		tombstones := o.cfg.Tombstones
		tombstoneCap := o.cfg.CompactionTombstoneCap
		if o.cfg.CompactionInterval == 0 {
			tombstones = nil
			tombstoneCap = 0
		}

		// Net-new DID backfill (issue #188). A DID can first appear on the
		// firehose in steady state that we never backfilled — e.g. a PDS that
		// was unreachable during the bootstrap listRepos sweep becomes
		// reachable and replays its backlog. We enqueue a StatusPending repo
		// row for it so the failed-repo retry loop performs a full getRepo.
		//
		// This is wired ONLY in steady state, and only when that retry loop is
		// enabled (it is what drains pending rows). During bootstrap the gap
		// does not exist: a DID that first appears mid-sweep is still
		// enumerated later in the same listRepos pagination, so the backfill
		// engine discovers it correctly there.
		//
		// The enqueuer and the retry runner share ONE *backfill.Store so their
		// counts/host-aggregate read-modify-writes serialize on a single
		// countsMu. Two Stores over the same pebble db would race those RMWs
		// and corrupt the aggregates.
		var enqueuer *backfill.LiveEnqueuer
		var backfillStore *backfill.Store
		onEvent := o.cfg.OnEvent
		if o.cfg.FailedRepoRetryInterval > 0 {
			backfillStore = backfill.NewStore(o.cfg.Store, o.cfg.BackfillMetrics)
			enqueuer = backfill.NewLiveEnqueuer(backfill.LiveEnqueuerConfig{
				Store:   backfillStore,
				Metrics: o.cfg.BackfillMetrics,
				Logger:  o.cfg.Logger,
			})
			downstream := o.cfg.OnEvent
			onEvent = func(ev *segment.Event) {
				// Observe is non-blocking and pebble-free; safe to run inline on
				// the live consumer's per-event hot path before fan-out.
				enqueuer.Observe(ev.DID)
				if downstream != nil {
					downstream(ev)
				}
			}
		}

		c, err := live.Open(live.Config{
			DataDir:     o.cfg.DataDir,
			SegmentsDir: segmentsDir,
			Store:       o.cfg.Store,
			SeqKey:      live.SteadySeqKey,
			CursorKey:   live.CursorKey,
			RelayURL:    o.cfg.RelayURL,
			// Bare cfg.Logger; live.Open sets its own component.
			Logger:                o.cfg.Logger,
			Metrics:               o.cfg.LiveMetrics,
			DropMetrics:           o.cfg.DropMetrics,
			WriterMetrics:         o.cfg.IngestMetrics,
			Verifier:              o.cfg.Verifier,
			SyncStateStore:        o.cfg.SyncStateStore,
			Tombstones:            tombstones,
			TombstoneCap:          tombstoneCap,
			CompactionTrigger:     o.compactionTrigger,
			SegmentMetrics:        o.cfg.SegmentMetrics,
			ReadLogRetentionBytes: o.cfg.ReadLogRetentionBytes,
			OnEvent:               onEvent,
			OnAfterSeal:           o.cfg.IngestOnAfterSeal,
			ReconnectBackoff:      o.cfg.LiveReconnectBackoff,
			Dial:                  o.cfg.LiveDial,

			SegmentIOFaultInjector: o.cfg.SegmentIOFaultInjector,
		})
		if err != nil {
			return fmt.Errorf("orchestrator: open steady-state live consumer: %w", err)
		}
		defer func() {
			if cerr := c.Close(); cerr != nil {
				o.logger.ErrorContext(ctx, "close steady-state live consumer", "err", cerr)
			}
		}()

		if o.cfg.OnSteadyStateWriter != nil {
			o.cfg.OnSteadyStateWriter(c.Writer())
		}

		o.logger.InfoContext(ctx, "steady-state consumer running")

		g, gctx := errgroup.WithContext(ctx)

		// Run the main loop
		g.Go(func() error { return c.Run(gctx) })

		// Run the compactor loop
		g.Go(func() error {
			err := o.runSteadyCompactor(gctx, c.Writer())
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		})

		if o.cfg.FailedRepoRetryInterval > 0 {
			g.Go(func() error {
				err := backfill.RunFailedRepoRetry(gctx, backfill.RetryConfig{
					Store:         o.cfg.Store,
					BackfillStore: backfillStore,
					Writer:        c.Writer(),
					HTTPClient:    o.cfg.HTTPClient,
					RelayURL:      o.cfg.RelayURL,
					Logger:        o.cfg.Logger,
					Metrics:       o.cfg.BackfillMetrics,
					DropMetrics:   o.cfg.DropMetrics,
					Interval:      o.cfg.FailedRepoRetryInterval,
					Workers:       o.cfg.FailedRepoRetryWorkers,
					HostWorkers:   o.cfg.FailedRepoRetryHostWorkers,
					MaxDelay:      o.cfg.FailedRepoRetryMaxDelay,
				})
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return err
			})

			// Background worker that drains net-new DID candidates from the
			// firehose hot path and durably enqueues them (issue #188).
			g.Go(func() error {
				err := enqueuer.Run(gctx)
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return err
			})
		}

		return g.Wait()
	})
}
