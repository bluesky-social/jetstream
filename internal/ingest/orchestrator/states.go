package orchestrator

import (
	"context"
	"fmt"
	"time"

	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/metastore/pebblestore"
)

// writeMergingPhase is commit point #1. After this call returns nil,
// the data dir is durably in PhaseMerging and a restart will resume
// at the merge step.
func (o *Orchestrator) writeMergingPhase() error {
	start := time.Now()
	completedAt := start.UTC()
	bootstrapStartedAt, err := lifecycle.ReadPhaseEnteredAt(context.Background(), pebblestore.New(o.cfg.Store, o.cfg.DataDir))
	if err != nil {
		return fmt.Errorf("orchestrator: read bootstrap phase entered_at: %w", err)
	}
	if bootstrapStartedAt.IsZero() {
		if err := lifecycle.WritePhase(context.Background(), pebblestore.New(o.cfg.Store, o.cfg.DataDir), lifecycle.PhaseMerging, completedAt); err != nil {
			return fmt.Errorf("orchestrator: write phase=merging: %w", err)
		}
	} else {
		if err := lifecycle.WritePhaseWithBackfillTiming(context.Background(), pebblestore.New(o.cfg.Store, o.cfg.DataDir), lifecycle.PhaseMerging, completedAt, bootstrapStartedAt, completedAt); err != nil {
			return fmt.Errorf("orchestrator: write phase=merging with backfill timing: %w", err)
		}
	}
	o.cfg.Metrics.observeState("write_phase_merging", time.Since(start).Seconds())
	o.cfg.Metrics.incTransition(lifecycle.PhaseBootstrap, lifecycle.PhaseMerging)
	o.cfg.Metrics.setPhase(PhaseGaugeMerging)
	return nil
}

// writeSteadyStatePhase is commit point #2. After this call returns
// nil, the data dir is durably in PhaseSteadyState.
func (o *Orchestrator) writeSteadyStatePhase() error {
	start := time.Now()
	if err := lifecycle.WritePhase(context.Background(), pebblestore.New(o.cfg.Store, o.cfg.DataDir), lifecycle.PhaseSteadyState, start.UTC()); err != nil {
		return fmt.Errorf("orchestrator: write phase=steady_state: %w", err)
	}
	o.cfg.Metrics.observeState("write_phase_steady", time.Since(start).Seconds())
	o.cfg.Metrics.incTransition(lifecycle.PhaseMerging, lifecycle.PhaseSteadyState)
	o.cfg.Metrics.setPhase(PhaseGaugeSteadyState)
	return nil
}
