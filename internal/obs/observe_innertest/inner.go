// Package observe_innertest exists solely to provide an Observe call
// site in a different package from internal/obs's own tests, so the
// tracer-scoping test can exercise cross-package behavior.
package observe_innertest

import (
	"context"

	"github.com/bluesky-social/jetstream-v2/internal/obs"
)

// Inner runs obs.Observe and immediately ends the span. The
// surrounding test asserts that the recorded span's
// InstrumentationScope.Name reflects this package, not the test's.
func Inner(ctx context.Context) {
	_, _, done := obs.Observe(ctx)
	done(nil)
}
