package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/bluesky-social/jetstream-v2/internal/ingest/live"
	"github.com/bluesky-social/jetstream-v2/internal/obs"
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

		c, err := live.Open(live.Config{
			SegmentsDir: segmentsDir,
			Store:       o.cfg.Store,
			SeqKey:      live.SteadySeqKey,
			CursorKey:   live.CursorKey,
			RelayURL:    o.cfg.RelayURL,
			// Bare cfg.Logger; live.Open sets its own component.
			Logger:            o.cfg.Logger,
			Metrics:           o.cfg.LiveMetrics,
			Verifier:          o.cfg.Verifier,
			SyncStateStore:    o.cfg.SyncStateStore,
			Tombstones:        o.cfg.Tombstones,
			TombstoneCap:      o.cfg.CompactionTombstoneCap,
			CompactionTrigger: o.compactionTrigger,
			SegmentMetrics:    o.cfg.SegmentMetrics,
			OnEvent:           o.cfg.OnEvent,
			OnAfterSeal:       o.cfg.IngestOnAfterSeal,
			ReconnectBackoff:  o.cfg.LiveReconnectBackoff,
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
		g.Go(func() error { return c.Run(gctx) })
		g.Go(func() error {
			err := o.runSteadyCompactor(gctx)
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		})
		return g.Wait()
	})
}
