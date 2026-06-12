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

	gathered, err := reg.Gather()
	require.NoError(t, err)
	names := make(map[string]struct{}, len(gathered))
	for _, mf := range gathered {
		names[mf.GetName()] = struct{}{}
	}
	for _, want := range []string{
		"jetstream_livestream_events_received_total",
		"jetstream_livestream_reconnects_total",
		"jetstream_livestream_decode_errors_total",
		"jetstream_livestream_unknown_events_total",
		"jetstream_livestream_dropped_ops_missing_block_total",
		"jetstream_livestream_dropped_events_total",
		"jetstream_livestream_stale_resyncs_dropped_total",
		"jetstream_livestream_upstream_cursor",
	} {
		_, ok := names[want]
		require.True(t, ok, "missing metric %s", want)
	}
	_, ok := names["jetstream_livestream_events_converted_total"]
	require.False(t, ok, "events converted duplicates jetstream_ingest_events_appended_total")

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
	m.incReconnects()
	m.incDecodeErrors()
	m.incUnknownEvents()
	m.setUpstreamCursor(42)
}
