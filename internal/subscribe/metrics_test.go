package subscribe

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestNewMetrics_RegistersAllSeries(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	require.NotNil(t, m)

	// Tickle every method to ensure they don't panic on a real Metrics.
	m.incSubscribers()
	m.decSubscribers()
	m.incCleanDisconnects()
	m.incSlowDrops()
	m.incEventsPublished()
	m.incEventsSent()
	m.incEventsSkippedSync()
	m.incEncodeErrors()
	m.observeQueueDepth(42)
}

func TestMetrics_NilReceiverIsNoop(t *testing.T) {
	t.Parallel()
	var m *Metrics
	// A nil *Metrics must be a valid zero-value: every method a no-op.
	// This mirrors live.Metrics so tests can pass nil.
	m.incSubscribers()
	m.decSubscribers()
	m.incCleanDisconnects()
	m.incSlowDrops()
	m.incEventsPublished()
	m.incEventsSent()
	m.incEventsSkippedSync()
	m.incEncodeErrors()
	m.observeQueueDepth(7)
}
