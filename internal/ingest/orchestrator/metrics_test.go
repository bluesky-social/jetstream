package orchestrator

import (
	"testing"

	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/tombstone"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestMetrics_NilSafe confirms every metric helper is nil-safe so
// callers can pass *Metrics(nil) in tests without registering.
func TestMetrics_NilSafe(t *testing.T) {
	t.Parallel()
	var m *Metrics
	m.setPhase(0)
	m.incTransition(lifecycle.PhaseBootstrap, lifecycle.PhaseMerging)
	m.observeState("drain", 0.1)
}

func TestMetrics_RegistersMergeCounters(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.incMergeEventsKept()
	m.incMergeEventsDropped()
	m.incMergeSegmentsConsumed()
	m.incMergeDIDLookups()
	m.addMergeRepoRevsUpdated(3)
	m.incMergeDIDsDiscoveredPostBootstrap()

	gathered, err := reg.Gather()
	require.NoError(t, err)
	names := make(map[string]struct{}, len(gathered))
	for _, mf := range gathered {
		names[mf.GetName()] = struct{}{}
	}
	for _, want := range []string{
		"jetstream_orchestrator_merge_events_kept_total",
		"jetstream_orchestrator_merge_events_dropped_total",
		"jetstream_orchestrator_merge_segments_consumed_total",
		"jetstream_orchestrator_merge_did_lookups_total",
		"jetstream_orchestrator_merge_repo_revs_updated_total",
		"jetstream_orchestrator_merge_dids_discovered_post_bootstrap_total",
	} {
		_, ok := names[want]
		require.True(t, ok, "missing metric %s", want)
	}
}

func TestMetrics_RegistersCompactionWatermarkLag(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.setCompactionWatermarkLag(12.5)

	gathered, err := reg.Gather()
	require.NoError(t, err)
	names := make(map[string]struct{}, len(gathered))
	for _, mf := range gathered {
		names[mf.GetName()] = struct{}{}
	}
	_, ok := names["jetstream_compaction_watermark_lag_seconds"]
	require.True(t, ok, "missing compaction watermark lag metric")
	_, ok = names["jetstream_compaction_passes_skipped_ticks_total"]
	require.False(t, ok, "skipped ticks metric is intentionally not exported")
}

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
