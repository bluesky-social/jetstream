package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
	"github.com/bluesky-social/jetstream-v2/internal/obs"
)

// Orchestrator owns the ingestion-lifecycle state machine. Construct
// via New; call Run exactly once.
type Orchestrator struct {
	cfg Config
	// logger is cfg.Logger pre-attributed with component=orchestrator
	// for the orchestrator's own log lines. cfg.Logger itself is left
	// bare (no component attribute) so child constructors (live.Open,
	// ingest.Open, backfill.Run) can set their own `component`
	// without slog stacking duplicate keys (slog appends; it does not
	// replace).
	logger *slog.Logger
}

// New validates cfg and returns an Orchestrator ready to Run.
func New(cfg Config) (*Orchestrator, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Orchestrator{
		cfg:    cfg,
		logger: cfg.Logger.With(slog.String("component", "orchestrator")),
	}, nil
}

// Run reads the persisted lifecycle phase and dispatches to the
// matching entry path. Phase transitions during a single Run are
// internal — callers see one Run that returns when ctx is cancelled
// or the steady-state consumer exits.
//
// On a fresh data dir (no phase key), Run treats the data dir as
// PhaseBootstrap and writes that value before starting any
// subsystems. This matches the previous cmd/jetstream behavior.
func (o *Orchestrator) Run(ctx context.Context) error {
	return obs.Span(ctx, func(ctx context.Context) error {
		phase, err := lifecycle.ReadPhase(o.cfg.Store)
		if err != nil {
			return fmt.Errorf("orchestrator: read phase: %w", err)
		}
		if phase == "" {
			phase = lifecycle.PhaseBootstrap
			if err := lifecycle.WritePhase(o.cfg.Store, phase, time.Now().UTC()); err != nil {
				return fmt.Errorf("orchestrator: write initial phase: %w", err)
			}
		}

		o.logger.InfoContext(ctx, "starting", "phase", phase)

		switch phase {
		case lifecycle.PhaseBootstrap:
			o.cfg.Metrics.setPhase(PhaseGaugeBootstrap)
			if err := o.runBootstrap(ctx); err != nil {
				return err
			}

			// runBootstrap returned cleanly: phase=merging is durably
			// written and the bootstrap-time subsystems are torn down.
			// Merge has NOT run and PhaseSteadyState has NOT been written;
			// fall through to do both.
			fallthrough
		case lifecycle.PhaseMerging:
			// Either we just got here from bootstrap (fallthrough — in
			// which case writeMergingPhase already set the gauge) or we
			// are resuming after a crash, where the gauge starts at zero
			// from prometheus default. Set it here for the resume case;
			// the bootstrap-fallthrough case is a harmless idempotent
			// re-set.
			o.cfg.Metrics.setPhase(PhaseGaugeMerging)

			// Re-run merge. Idempotent under partial completion: the
			// restart-after-cleanup guard, per-source cursor, and
			// idempotent discovery-row writes ensure a crash at any
			// point in runMerge leaves the next start in a recoverable
			// state. Spec §5.3.
			if err := o.runMerge(ctx); err != nil {
				return fmt.Errorf("orchestrator: merge: %w", err)
			}

			if err := o.writeSteadyStatePhase(); err != nil {
				return err
			}

			fallthrough
		case lifecycle.PhaseSteadyState:
			return o.runSteadyState(ctx)

		default:
			return fmt.Errorf("orchestrator: unrecognized phase %q", phase)
		}
	})
}
