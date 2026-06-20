package store_test

import (
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestMetrics_NilSafe pins the codebase-wide nil-receiver convention.
func TestMetrics_NilSafe(t *testing.T) {
	t.Parallel()
	var m *store.Metrics
	var zero time.Time
	m.ObserveGet(zero, nil)
	m.ObserveSet(zero, nil)
	m.ObserveDelete(zero, nil)
	m.ObserveBatchCommit(zero, nil)
}

// TestMetrics_StatusLabels exercises the four observe methods with
// every status branch and confirms the sample counts land on the
// correct {op,status} series.
func TestMetrics_StatusLabels(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := store.NewMetrics(reg)

	var zero time.Time
	m.ObserveGet(zero, nil)
	m.ObserveGet(zero, store.ErrNotFound)
	m.ObserveGet(zero, errSomeIO())
	m.ObserveSet(zero, nil)
	m.ObserveSet(zero, errSomeIO())
	m.ObserveDelete(zero, nil)
	m.ObserveBatchCommit(zero, nil)

	require.Equal(t, uint64(1), histCount(t, reg, "get", "ok"))
	require.Equal(t, uint64(1), histCount(t, reg, "get", "notfound"))
	require.Equal(t, uint64(1), histCount(t, reg, "get", "error"))
	require.Equal(t, uint64(1), histCount(t, reg, "set", "ok"))
	require.Equal(t, uint64(1), histCount(t, reg, "set", "error"))
	require.Equal(t, uint64(1), histCount(t, reg, "delete", "ok"))
	require.Equal(t, uint64(1), histCount(t, reg, "batch_commit", "ok"))

	// Sanity: a series we did not touch must remain at zero.
	require.Equal(t, uint64(0), histCount(t, reg, "delete", "error"))
}

func errSomeIO() error { return &simpleErr{"io fail"} }

type simpleErr struct{ s string }

func (e *simpleErr) Error() string { return e.s }

// histCount returns the cumulative observation count for the
// {op, status} histogram series, or 0 if the series isn't present.
func histCount(t *testing.T, reg *prometheus.Registry, op, status string) uint64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "jetstream_store_op_duration_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if labels["op"] == op && labels["status"] == status {
				return m.GetHistogram().GetSampleCount()
			}
		}
	}
	return 0
}
