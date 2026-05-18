package backfill

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
)

// Config controls a single bootstrap pipeline run.
//
// The Store is owned by the caller — Run does not Close it on the
// way out. That lets the same Store flow into the steady-state
// ingest path without a re-open after Run returns. The caller
// is responsible for closing both Store and any clients it created.
type Config struct {
	// Store is the metadata store to drive bootstrap state through.
	// Must be open. Required.
	Store *store.Store

	// RelayURL is the base URL of the upstream relay (e.g.
	// "https://relay1.us-east.bsky.network"). Required.
	RelayURL string

	// Metrics for tracking initial backfill progress. Required.
	Metrics *Metrics

	// Logger receives lifecycle events. nil falls back to
	// slog.Default().
	Logger gt.Option[*slog.Logger]
}

// Run drives the bootstrap pipeline (DESIGN.md §4.1) to completion
// for the data directory backed by cfg.Store. It is the single
// entrypoint the serve command uses, and is designed to be safe to
// call on every startup:
//
//   - First boot (PhaseUnset)        → run seed → mark PhaseComplete.
//   - Crashed mid-seed (PhaseSeed)   → re-run seed → mark PhaseComplete.
//     The seed step is idempotent so existing rows are skipped.
//   - Already complete (PhaseComplete) → no-op, returns immediately.
//
// In this slice the only phase Run drives is the listRepos seed.
// Future iterations will chain PhaseDownload and PhaseMerge in the
// same state machine.
func Run(ctx context.Context, cfg Config) error {
	logger := cfg.Logger.ValOr(slog.Default())

	if cfg.Store == nil {
		return fmt.Errorf("backfill: Run: Store is required")
	}
	if cfg.RelayURL == "" {
		return fmt.Errorf("backfill: Run: RelayURL is required")
	}
	if cfg.Metrics == nil {
		return fmt.Errorf("backfill: Run: Metrics is required")
	}

	state, err := GetBootstrapState(cfg.Store)
	if err != nil {
		return err
	}

	if state.IsComplete() {
		logger.Info("backfill: bootstrap already complete, skipping",
			"completed_at", state.CompletedAt,
		)
		return nil
	}

	// Update the stored phase and timestamps
	now := time.Now().UTC()
	if state.StartedAt.IsZero() {
		state.StartedAt = now
	}
	state.UpdatedAt = now
	state.Phase = PhaseSeed
	if err := PutBootstrapState(cfg.Store, state); err != nil {
		return err
	}

	logger.Info("backfill: starting bootstrap",
		"phase", state.Phase,
		"relay_url", cfg.RelayURL,
		"resuming", !state.StartedAt.Equal(now),
	)

	xc := &xrpc.Client{Host: cfg.RelayURL}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	res, err := SeedRepos(ctx, cfg.Store, sc, cfg.Metrics, logger)
	if err != nil {
		return fmt.Errorf("backfill: seed phase: %w", err)
	}

	completedAt := time.Now().UTC()
	state.Phase = PhaseComplete
	state.UpdatedAt = completedAt
	state.CompletedAt = completedAt
	if err := PutBootstrapState(cfg.Store, state); err != nil {
		return err
	}

	logger.Info("backfill: bootstrap complete",
		"enumerated", res.Enumerated,
		"seeded", res.Seeded,
		"skipped_existing", res.SkippedExisting,
		"duration", completedAt.Sub(state.StartedAt),
	)

	return nil
}
