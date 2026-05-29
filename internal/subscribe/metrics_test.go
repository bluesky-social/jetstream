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
	m.incEventsFiltered()
	m.incEventsOversize()
	m.incOptionsUpdates()
	m.incOptionsUpdateError(optionsUpdateErrorReasonOversize)
	m.incOptionsUpdateError(optionsUpdateErrorReasonBadEnvelopeJSON)
	m.incOptionsUpdateError(optionsUpdateErrorReasonBadPayloadJSON)
	m.incOptionsUpdateError(optionsUpdateErrorReasonInvalidOptions)

	m.incCursorRequests("live")
	m.observeCursorResolveSeconds(0.001)
	m.incLookbackSubscribers()
	m.decLookbackSubscribers()
	m.incLookbackIterations()
	m.incRingOverflows()
	m.observeLookbackSeconds(0.5)
	m.incLookbackEvents()
	m.incLookbackTerminated("too_slow")
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
	m.incEventsFiltered()
	m.incEventsOversize()
	m.incOptionsUpdates()
	m.incOptionsUpdateError(optionsUpdateErrorReasonOversize)

	m.incCursorRequests("live")
	m.observeCursorResolveSeconds(0.001)
	m.incLookbackSubscribers()
	m.decLookbackSubscribers()
	m.incLookbackIterations()
	m.incRingOverflows()
	m.observeLookbackSeconds(0.5)
	m.incLookbackEvents()
	m.incLookbackTerminated("too_slow")
}

func TestMetrics_CursorAndLookbackSeriesRegistered(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	require.NotNil(t, m.CursorRequests)
	require.NotNil(t, m.CursorResolveSeconds)
	require.NotNil(t, m.LookbackSubscribers)
	require.NotNil(t, m.LookbackIterations)
	require.NotNil(t, m.RingOverflows)
	require.NotNil(t, m.LookbackSeconds)
	require.NotNil(t, m.LookbackEvents)
	require.NotNil(t, m.LookbackTerminated)
}
