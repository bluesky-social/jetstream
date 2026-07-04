package ingest

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// TestNewDropMetrics_CountsPerSourceAndReason pins the shared
// dropped-events counter family: one series per (source, reason)
// pair, pre-bound at construction so hot-path increments skip the
// CounterVec label lookup.
func TestNewDropMetrics_CountsPerSourceAndReason(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDropMetrics(reg)
	require.NotNil(t, m)

	sources := []DropSource{DropSourceLive, DropSourceBackfill}
	reasons := []DropReason{
		DropReasonInvalidRev,
		DropReasonInvalidCollection,
		DropReasonInvalidRkey,
		DropReasonFieldTooLong,
		DropReasonMissingBlock,
	}

	// Every pair increments its own series and only its own series.
	want := 0.0
	for _, s := range sources {
		for _, r := range reasons {
			m.IncDropped(s, r)
			want++
		}
	}
	m.AddDropped(DropSourceLive, DropReasonMissingBlock, 3)
	want += 3

	got := testutil.ToFloat64(m.dropped.WithLabelValues(string(DropSourceLive), string(DropReasonMissingBlock)))
	require.InDelta(t, 4.0, got, 0)

	total := 0.0
	for _, s := range sources {
		for _, r := range reasons {
			total += testutil.ToFloat64(m.dropped.WithLabelValues(string(s), string(r)))
		}
	}
	require.InDelta(t, want, total, 0)

	// AddDropped with n<=0 is a no-op, not a negative-count panic.
	require.NotPanics(t, func() { m.AddDropped(DropSourceLive, DropReasonMissingBlock, 0) })
	require.NotPanics(t, func() { m.AddDropped(DropSourceLive, DropReasonMissingBlock, -1) })
	got = testutil.ToFloat64(m.dropped.WithLabelValues(string(DropSourceLive), string(DropReasonMissingBlock)))
	require.InDelta(t, 4.0, got, 0)
}

// TestNewDropMetrics_MetricName pins the exported series name so a
// rename cannot slip through silently — dashboards select on it.
func TestNewDropMetrics_MetricName(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDropMetrics(reg)
	m.IncDropped(DropSourceLive, DropReasonInvalidRev)

	mfs, err := reg.Gather()
	require.NoError(t, err)
	require.Len(t, mfs, 1)
	require.Equal(t, "jetstream_ingest_dropped_events_total", mfs[0].GetName())
}

// TestNewDropMetrics_NilSafe pins that a nil *DropMetrics is a valid
// zero-value, matching the convention of every other metrics type in
// the codebase.
func TestNewDropMetrics_NilSafe(t *testing.T) {
	t.Parallel()
	var m *DropMetrics
	require.NotPanics(t, func() {
		m.IncDropped(DropSourceLive, DropReasonInvalidRev)
		m.AddDropped(DropSourceBackfill, DropReasonMissingBlock, 2)
	})
}

// TestNewDropMetrics_UnknownPairPanics pins that an unregistered
// (source, reason) pair fails loud at IncDropped rather than silently
// minting an unbounded label set: the pairs are a closed enum and a
// typo'd constant is programmer error.
func TestNewDropMetrics_UnknownPairPanics(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDropMetrics(reg)
	require.Panics(t, func() { m.IncDropped("nope", DropReasonInvalidRev) })
	require.Panics(t, func() { m.IncDropped(DropSourceLive, "nope") })
}
