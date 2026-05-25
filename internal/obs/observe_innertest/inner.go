// Package observe_innertest exists solely to provide an obs.Span call
// site in a different package from internal/obs's own tests, so the
// tracer-scoping test can exercise cross-package behavior.
package observe_innertest

import (
	"context"

	"github.com/bluesky-social/jetstream-v2/internal/obs"
)

// Inner runs obs.Span and returns its result. The surrounding test
// asserts that the recorded span's InstrumentationScope.Name reflects
// this package, not the test's.
func Inner(ctx context.Context) error {
	return obs.Span(ctx, func(_ context.Context) error {
		return nil
	})
}
