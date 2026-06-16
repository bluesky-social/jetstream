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
	m.incEventsSent()
	m.incEventsSkippedSync()
	m.incEventsSkippedResync()
	m.incEncodeErrors()
	m.incEventsFiltered()
	m.incEventsOversize()
	m.incOptionsUpdates()
	m.incOptionsUpdateError(optionsUpdateErrorReasonOversize)
	m.incOptionsUpdateError(optionsUpdateErrorReasonBadEnvelopeJSON)
	m.incOptionsUpdateError(optionsUpdateErrorReasonBadPayloadJSON)
	m.incOptionsUpdateError(optionsUpdateErrorReasonInvalidOptions)

	m.incCursorRequests("live")
	m.observeCursorResolveSeconds(0.001)

	m.incEventsAppended()
	m.incHotReads()
	m.incColdReads()
	m.incAdversarialDrops()
	m.setHotRingBytes(4096)
}

func TestMetrics_NilReceiverIsNoop(t *testing.T) {
	t.Parallel()
	var m *Metrics
	// A nil *Metrics must be a valid zero-value: every method a no-op.
	// This mirrors live.Metrics so tests can pass nil.
	m.incSubscribers()
	m.decSubscribers()
	m.incCleanDisconnects()
	m.incEventsSent()
	m.incEventsSkippedSync()
	m.incEventsSkippedResync()
	m.incEncodeErrors()
	m.incEventsFiltered()
	m.incEventsOversize()
	m.incOptionsUpdates()
	m.incOptionsUpdateError(optionsUpdateErrorReasonOversize)

	m.incCursorRequests("live")
	m.observeCursorResolveSeconds(0.001)

	m.incEventsAppended()
	m.incHotReads()
	m.incColdReads()
	m.incAdversarialDrops()
	m.setHotRingBytes(7)
}

func TestMetrics_PullSeriesRegistered(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	require.NotNil(t, m.CursorRequests)
	require.NotNil(t, m.CursorResolveSeconds)
	require.NotNil(t, m.EventsAppended)
	require.NotNil(t, m.HotReads)
	require.NotNil(t, m.ColdReads)
	require.NotNil(t, m.AdversarialDrops)
	require.NotNil(t, m.HotRingBytes)
}
