package orchestrator

import (
	"testing"

	"github.com/bluesky-social/jetstream/internal/tombstone"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestMetrics_CompactionTombstoneGaugesReadLiveSet(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	set := tombstone.New()
	NewMetrics(reg, set)

	require.InDelta(t, 0.0, gaugeValue(t, reg, "jetstream_compaction_tombstone_set_entries"), 0)
	require.InDelta(t, 0.0, gaugeValue(t, reg, "jetstream_compaction_tombstone_set_bytes"), 0)

	require.NoError(t, set.Observe(&segment.Event{
		Seq:        1,
		Kind:       segment.KindDelete,
		DID:        "did:plc:a",
		Collection: "app.bsky.feed.post",
		Rkey:       "abc",
	}))

	require.InDelta(t, 1.0, gaugeValue(t, reg, "jetstream_compaction_tombstone_set_entries"), 0)
	require.Greater(t, gaugeValue(t, reg, "jetstream_compaction_tombstone_set_bytes"), 0.0)
}

func gaugeValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	gathered, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range gathered {
		if mf.GetName() == name {
			require.NotEmpty(t, mf.GetMetric(), "metric %s has no samples", name)
			return mf.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatalf("missing metric %s", name)
	return 0
}
