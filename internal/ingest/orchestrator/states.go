package orchestrator

import (
	"fmt"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
)

// writeMergingPhase is commit point #1. After this call returns nil,
// the data dir is durably in PhaseMerging and a restart will resume
// at the merge step.
func (o *Orchestrator) writeMergingPhase() error {
	start := time.Now()
	if err := lifecycle.WritePhase(o.cfg.Store, lifecycle.PhaseMerging, start.UTC()); err != nil {
		return fmt.Errorf("orchestrator: write phase=merging: %w", err)
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
	if err := lifecycle.WritePhase(o.cfg.Store, lifecycle.PhaseSteadyState, start.UTC()); err != nil {
		return fmt.Errorf("orchestrator: write phase=steady_state: %w", err)
	}
	o.cfg.Metrics.observeState("write_phase_steady", time.Since(start).Seconds())
	o.cfg.Metrics.incTransition(lifecycle.PhaseMerging, lifecycle.PhaseSteadyState)
	o.cfg.Metrics.setPhase(PhaseGaugeSteadyState)
	return nil
}
