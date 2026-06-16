package backfill

import (
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestNewMetrics_RegistersStableMetrics(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.incDiscovered()
	m.incCompleted()
	m.incFailed()
	m.incActiveFlips()
	m.incOnFailErrors()
	m.observeHandleRepo(time.Now().Add(-time.Millisecond), nil)
	m.setProgressCompleted(42)
	m.incDroppedRecords()

	require.InDelta(t, 1.0, testutil.ToFloat64(m.Discovered), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.Completed), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.Failed), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.ActiveFlips), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.OnFailErrors), 0)
	require.Equal(t, 1, testutil.CollectAndCount(m.HandleRepoDuration))
	require.InDelta(t, 42.0, testutil.ToFloat64(m.ProgressCompleted), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.DroppedRecords), 0)
	requireNoDebugMetricFields(t, m)
	requireNoDebugMetrics(t, reg)
}

func TestNewMetrics_NilSafe(t *testing.T) {
	t.Parallel()

	var m *Metrics
	require.NotPanics(t, func() {
		m.incDiscovered()
		m.incCompleted()
		m.incFailed()
		m.incActiveFlips()
		m.incOnFailErrors()
		m.observeHandleRepo(time.Now(), nil)
		m.setProgressCompleted(1)
		m.incDroppedRecords()
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
