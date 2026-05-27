package orchestrator

import (
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
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
