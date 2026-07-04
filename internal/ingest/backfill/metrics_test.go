package backfill

import (
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestNewMetrics_RegistersStableMetrics(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.incDiscovered()
	m.incCompleted()
	m.incFailed()
	m.incActiveFlips()
	m.incOnFailErrors()
	m.observeHandleRepo(time.Now().Add(-time.Millisecond), nil)
	m.setProgressCompleted(42)
	m.incCompletionQueued()
	m.setCompletionQueueDepth(3)
	m.observeCompletionDurableBatch(2)
	m.incCompletionStageErrors()
	m.observeCompletionQueueWait(time.Millisecond)
	m.incForcedCheckpointFlushes()
	m.incRetryPasses()
	m.incRetryCandidates()
	m.incRetryAttempts()
	m.incRetrySucceeded()
	m.incRetryFailed()
	m.incRetrySkippedHostParked()
	m.incEnqueuedNetNew()
	m.incEnqueueAlreadyKnown()
	m.incEnqueueSeenCacheHit()
	m.incEnqueueDropped()
	m.incEnqueueInvalidDID()
	m.setEnqueueQueueDepth(7)

	require.InDelta(t, 1.0, testutil.ToFloat64(m.Discovered), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.Completed), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.Failed), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.ActiveFlips), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.OnFailErrors), 0)
	require.Equal(t, 1, testutil.CollectAndCount(m.HandleRepoDuration))
	require.InDelta(t, 42.0, testutil.ToFloat64(m.ProgressCompleted), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.CompletionQueued), 0)
	require.InDelta(t, 3.0, testutil.ToFloat64(m.CompletionQueueDepth), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.CompletionDurableBatches), 0)
	require.InDelta(t, 2.0, testutil.ToFloat64(m.CompletionDurableRepos), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.CompletionStageErrors), 0)
	require.Equal(t, 1, testutil.CollectAndCount(m.CompletionQueueWait))
	require.InDelta(t, 1.0, testutil.ToFloat64(m.ForcedCheckpointFlushes), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.RetryPasses), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.RetryCandidates), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.RetryAttempts), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.RetrySucceeded), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.RetryFailed), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.RetrySkippedHostParked), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.EnqueuedNetNew), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.EnqueueAlreadyKnown), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.EnqueueSeenCacheHit), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.EnqueueDropped), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.EnqueueInvalidDID), 0)
	require.InDelta(t, 7.0, testutil.ToFloat64(m.EnqueueQueueDepth), 0)
	requireNoDebugMetricFields(t, m)
	requireNoDebugMetrics(t, reg)
}

func TestNewMetrics_NilSafe(t *testing.T) {
	t.Parallel()

	var m *Metrics
	require.NotPanics(t, func() {
		m.incDiscovered()
		m.incCompleted()
		m.incFailed()
		m.incActiveFlips()
		m.incOnFailErrors()
		m.observeHandleRepo(time.Now(), nil)
		m.setProgressCompleted(1)
		m.incCompletionQueued()
		m.setCompletionQueueDepth(1)
		m.observeCompletionDurableBatch(1)
		m.incCompletionStageErrors()
		m.observeCompletionQueueWait(time.Millisecond)
		m.incForcedCheckpointFlushes()
		m.incRetryPasses()
		m.incRetryCandidates()
		m.incRetryAttempts()
		m.incRetrySucceeded()
		m.incRetryFailed()
		m.incRetrySkippedHostParked()
		m.incEnqueuedNetNew()
		m.incEnqueueAlreadyKnown()
		m.incEnqueueSeenCacheHit()
		m.incEnqueueDropped()
		m.incEnqueueInvalidDID()
		m.setEnqueueQueueDepth(1)
	})
}

func requireNoDebugMetrics(t *testing.T, reg *prometheus.Registry) {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		require.NotContains(t, mf.GetName(), "_debug_")
	}
}

func requireNoDebugMetricFields(t *testing.T, m *Metrics) {
	t.Helper()
	typ := reflect.TypeOf(*m)
	for i := range typ.NumField() {
		require.NotContains(t, typ.Field(i).Name, "Debug")
	}
}
