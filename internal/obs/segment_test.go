package obs_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestSegmentMetrics_RecordsHistogram drives a real Writer through Seal with
// a registered *SegmentMetrics and confirms the histogram landed exactly one
// observation.
func TestSegmentMetrics_RecordsHistogram(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := obs.NewSegmentMetrics(reg)

	path := filepath.Join(t.TempDir(), "seg_0.jss")
	w, err := segment.New(segment.Config{
		Path:    path,
		Metrics: m,
	})
	require.NoError(t, err)

	_, err = w.Append(segment.Event{
		WitnessedAt: 1,
		Kind:        segment.KindCreate,
		DID:         "did:plc:test",
		Seq:         1,
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

// TestSegmentMetrics_IgnoresFailedSeal confirms a non-nil err is not recorded.
func TestSegmentMetrics_IgnoresFailedSeal(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := obs.NewSegmentMetrics(reg)
	m.ObserveSeal(time.Now(), assertErr{})

	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == "jetstream_segment_seal_duration_seconds" {
			for _, mm := range mf.GetMetric() {
				require.Zero(t, mm.GetHistogram().GetSampleCount())
			}
		}
	}
}

type assertErr struct{}

func (assertErr) Error() string { return "seal failed" }
