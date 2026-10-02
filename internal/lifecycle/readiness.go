package lifecycle

import (
	"context"
	"errors"

	"github.com/bluesky-social/jetstream/internal/metastore"
)

// Readiness gates the public read surfaces (/subscribe, subscribeEvents, and
// the archive XRPCs). Ready returns nil when the archive is safe to expose,
// or an error whose text is shown to the client after "service not ready: ".
// It runs on every request, so it must be cheap.
//
// The local implementation is SteadyState, composed with manifest warmup for
// the archive endpoints. Disaggregated serving (S2) answers from the shared
// catalog instead of this process's own metastore, which is why the serve
// paths take the interface rather than a metastore.Store.
type Readiness interface {
	Ready(ctx context.Context) error
}

// ReadinessFunc adapts a function to Readiness.
type ReadinessFunc func(context.Context) error

func (f ReadinessFunc) Ready(ctx context.Context) error { return f(ctx) }

// ErrBootstrapInProgress is SteadyState's error before the persisted phase
// reaches steady state. Its text is part of the 503 body clients see.
var ErrBootstrapInProgress = errors.New("bootstrap in progress")

// SteadyState is ready once s's persisted phase is PhaseSteadyState. It fails
// closed like IsSteadyState: an empty, earlier, or corrupt phase all report
// ErrBootstrapInProgress.
func SteadyState(s metastore.Store) Readiness {
	return ReadinessFunc(func(ctx context.Context) error {
		if !IsSteadyState(ctx, s) {
			return ErrBootstrapInProgress
		}
		return nil
	})
}

// AllReady is ready when every r is, reporting the first error in order.
func AllReady(rs ...Readiness) Readiness {
	return ReadinessFunc(func(ctx context.Context) error {
		for _, r := range rs {
			if err := r.Ready(ctx); err != nil {
				return err
			}
		}
		return nil
	})
}
