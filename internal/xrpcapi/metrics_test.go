package xrpcapi

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestMetrics_NilSafe(t *testing.T) {
	t.Parallel()

	var m *Metrics
	require.NotPanics(t, func() { m.observeServe(resultOK, 123, 0.001) })
}

func TestMetrics_ObserveServe(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	m.observeServe(resultOK, 100, 0.002)
	m.observeServe(resultNotFound, 0, 0.001)

	mfs, err := reg.Gather()
	require.NoError(t, err)

	got := map[string]bool{}
	for _, mf := range mfs {
		got[mf.GetName()] = true
	}
	require.True(t, got["jetstream_getblock_requests_total"])
	require.True(t, got["jetstream_getblock_served_bytes_total"])
	require.True(t, got["jetstream_getblock_duration_seconds"])
}
