package obs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewMetrics_RegistersCurrentTimestampGauge(t *testing.T) {
	t.Parallel()

	metrics := NewMetrics()
	gathered, err := metrics.Registry.Gather()
	require.NoError(t, err)

	var got float64
	found := false
	for _, mf := range gathered {
		if mf.GetName() != "jetstream_current_timestamp_seconds" {
			continue
		}
		require.Len(t, mf.GetMetric(), 1)
		got = mf.GetMetric()[0].GetGauge().GetValue()
		found = true
	}
	require.True(t, found, "missing jetstream_current_timestamp_seconds")
	require.InDelta(t, float64(time.Now().Unix()), got, 2)
}
