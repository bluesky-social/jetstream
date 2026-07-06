package obs_test

import (
	"testing"

	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestRegisterDataDirFreeBytesCollectsGauge(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	obs.RegisterDataDirFreeBytes(reg, t.TempDir())

	mfs, err := reg.Gather()
	require.NoError(t, err)

	var found *dto.MetricFamily
	for _, mf := range mfs {
		if mf.GetName() == "jetstream_data_dir_free_bytes" {
			found = mf
			break
		}
	}
	require.NotNil(t, found, "data-dir free-space gauge must be registered")
	require.Equal(t, dto.MetricType_GAUGE, found.GetType())
	require.Len(t, found.GetMetric(), 1)
	require.Greater(t, found.GetMetric()[0].GetGauge().GetValue(), float64(0))
}
