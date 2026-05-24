package orchestrator

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/bluesky-social/jetstream-v2/internal/ingest/live"
	"github.com/bluesky-social/jetstream-v2/internal/obs"
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
func (o *Orchestrator) runSteadyState(ctx context.Context) (retErr error) {
	ctx, _, done := obs.Observe(ctx)
	defer func() { done(retErr) }()

	// Set the phase gauge before any I/O. On the resume-from-
	// PhaseSteadyState path Run dispatches here directly without
	// going through writeSteadyStatePhase, so this is the only place
	// the gauge gets set on that path.
	o.cfg.Metrics.setPhase(PhaseGaugeSteadyState)

	segmentsDir := filepath.Join(o.cfg.DataDir, "segments")

	c, err := live.Open(live.Config{
		SegmentsDir: segmentsDir,
		Store:       o.cfg.Store,
		SeqKey:      live.SteadySeqKey,
		CursorKey:   live.CursorKey,
		RelayURL:    o.cfg.RelayURL,
		// Bare cfg.Logger; live.Open sets its own component.
		Logger:         o.cfg.Logger,
		Metrics:        o.cfg.LiveMetrics,
		Verifier:       o.cfg.Verifier,
		SegmentMetrics: o.cfg.SegmentMetrics,
	})
	if err != nil {
		return fmt.Errorf("orchestrator: open steady-state live consumer: %w", err)
	}
	defer func() {
		if cerr := c.Close(); cerr != nil {
			o.logger.ErrorContext(ctx, "close steady-state live consumer", "err", cerr)
		}
	}()

	o.logger.InfoContext(ctx, "steady-state consumer running")

	return c.Run(ctx)
}
