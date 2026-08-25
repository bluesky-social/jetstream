package subscribe

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestMetricsGapJump(t *testing.T) {
	t.Parallel()
	m := NewMetrics(prometheus.NewRegistry())
	m.incGapJump(10, 14)
	m.incGapJump(20, 25)
	require.Equal(t, 2.0, testutil.ToFloat64(m.GapJumps))
	require.Equal(t, 9.0, testutil.ToFloat64(m.GapValuesSkipped))

	var nilMetrics *Metrics
	require.NotPanics(t, func() { nilMetrics.incGapJump(1, 2) })
}
