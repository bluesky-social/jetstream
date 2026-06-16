package ingest

import (
	"reflect"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// TestNewMetrics_RegistersCounters confirms NewMetrics registers the
// expected counter and gauge series against the supplied registry
// and that every helper round-trips through testutil.ToFloat64.
func TestNewMetrics_RegistersCounters(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	require.NotNil(t, m)

	m.incEventsAppended()
	m.incBlocksFlushed()
	m.incSegmentsRotated()
	m.incAppendErrors()
	m.setActiveSegBytes(123)
	m.setNextSeq(456)

	require.InDelta(t, 1.0, testutil.ToFloat64(m.EventsAppended), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.BlocksFlushed), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.SegmentsRotated), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.AppendErrors), 0)
	require.InDelta(t, 123.0, testutil.ToFloat64(m.ActiveSegBytes), 0)
	require.InDelta(t, 456.0, testutil.ToFloat64(m.NextSeq), 0)
	requireNoDebugMetricFields(t, m)
	requireNoDebugMetrics(t, reg)
}

// TestNewMetrics_NilSafe pins that every inc/set helper tolerates a
// nil receiver. The tests in writer_test.go pass nil to skip
// registration.
func TestNewMetrics_NilSafe(t *testing.T) {
	t.Parallel()
	var m *Metrics
	require.NotPanics(t, func() {
		m.incEventsAppended()
		m.incBlocksFlushed()
		m.incSegmentsRotated()
		m.incAppendErrors()
		m.setActiveSegBytes(1)
		m.setNextSeq(1)
	})
}

func requireNoDebugMetrics(t *testing.T, reg *prometheus.Registry) {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		require.NotContains(t, mf.GetName(), "_debug_")
	}
}

func requireNoDebugMetricFields(t *testing.T, m *Metrics) {
	t.Helper()
	typ := reflect.TypeOf(*m)
	for i := range typ.NumField() {
		require.NotContains(t, typ.Field(i).Name, "Debug")
	}
}
