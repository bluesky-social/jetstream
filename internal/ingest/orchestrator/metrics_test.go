package orchestrator

import (
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
)

// TestMetrics_NilSafe confirms every metric helper is nil-safe so
// callers can pass *Metrics(nil) in tests without registering.
func TestMetrics_NilSafe(t *testing.T) {
	t.Parallel()
	var m *Metrics
	m.setPhase(0)
	m.incTransition(lifecycle.PhaseBootstrap, lifecycle.PhaseMerging)
	m.observeState("drain", 0.1)
}
