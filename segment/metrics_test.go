package segment_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestMetrics_NilSafe pins the codebase nil-receiver convention.
func TestMetrics_NilSafe(t *testing.T) {
	t.Parallel()
	var m *segment.Metrics
	var zero time.Time
	m.ObserveSeal(zero, nil)
}

// TestSeal_RecordsHistogram drives a real Writer through Seal with a
// configured Metrics and confirms the histogram landed exactly one
// observation.
func TestSeal_RecordsHistogram(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := segment.NewMetrics(reg)

	path := filepath.Join(t.TempDir(), "seg_0.jss")
	w, err := segment.New(segment.Config{
		Path:    path,
		Metrics: m,
	})
	require.NoError(t, err)

	_, err = w.Append(segment.Event{
		IndexedAt: 1,
		Kind:      segment.KindCreate,
		DID:       "did:plc:test",
		Seq:       1,
	})
	require.NoError(t, err)

	_, err = w.Seal()
	require.NoError(t, err)

	mfs, err := reg.Gather()
	require.NoError(t, err)
	var count uint64
	for _, mf := range mfs {
		if mf.GetName() == "jetstream_segment_seal_duration_seconds" {
			for _, mm := range mf.GetMetric() {
				count += mm.GetHistogram().GetSampleCount()
			}
		}
	}
	require.Equal(t, uint64(1), count)
}
