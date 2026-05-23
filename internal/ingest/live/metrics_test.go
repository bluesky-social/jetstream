package live

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestNewMetrics_RegistersAllSeries pins that the constructor
// registers every series on the provided registry exactly once.
// We catch double-registration via reg.Register, which returns
// AlreadyRegisteredError on collision.
func TestNewMetrics_RegistersAllSeries(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	require.NotNil(t, m)

	// Re-registering the same collectors must collide.
	require.Panics(t, func() { _ = NewMetrics(reg) })
}

// TestMetrics_NilSafe pins that a nil *Metrics receiver tolerates
// every increment / set helper. This lets tests skip metric
// registration entirely by passing nil.
func TestMetrics_NilSafe(t *testing.T) {
	t.Parallel()
	var m *Metrics
	m.incEventsReceived()
	m.incEventsConverted()
	m.incReconnects()
	m.incDecodeErrors()
	m.incUnknownEvents()
	m.setUpstreamCursor(42)
}
