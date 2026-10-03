package backfill

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/metastore/memstore"
	"github.com/bluesky-social/jetstream/internal/metastore/pebblestore"
	"github.com/jcalabro/atmos"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	"github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestCompletionBatcherStagesCompletionAtDurableSeq(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := pebblestore.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	bs := newSeededStore(t, st, nil)
	did := atmos.DID("did:plc:completebatch")
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(did)))

	cb := NewCompletionBatcher(bs, nil)
	cb.RecordWatermark(did, 41, true)
	require.NoError(t, cb.QueueComplete(t.Context(), did, "", &repo.Commit{DID: string(did), Rev: "rev1"}))

	b := st.NewBatch()
	afterCommit, afterDone, err := cb.StageDurable(t.Context(), b, 42, false, nil)
	require.NoError(t, err)
	require.NotNil(t, afterCommit)
	require.NotNil(t, afterDone)
	requireLookupState(t, bs, did, atmosbackfill.StateDiscovered)

	commitErr := b.Commit(context.Background())
	if commitErr != nil {
		afterDone(commitErr)
		require.NoError(t, commitErr)
	}
	requireLookupState(t, bs, did, atmosbackfill.StateComplete)

	require.Len(t, cb.queued, 1)
	require.Equal(t, did, cb.queued[0].did)

	afterCommit()
	afterDone(nil)
	require.Empty(t, cb.queued)
}

func TestCompletionBatcherHostCursorNeverLeadsCoveredCompletion(t *testing.T) {
	t.Parallel()
	st, err := pebblestore.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	bs := newSeededStore(t, st, nil)
	hostname := "pds.cursor.test"
	require.NoError(t, bs.OnHost(t.Context(), atmosbackfill.HostInfo{Hostname: hostname, RelayStatus: "active"}))
	did := atmos.DID("did:plc:cursor-order")
	require.NoError(t, bs.onDiscover(t.Context(), hostname, testListReposEntry(did)))
	cb := NewCompletionBatcher(bs, nil)
	cb.RecordWatermark(did, 10, true)
	require.NoError(t, cb.QueueComplete(t.Context(), did, hostname, &repo.Commit{DID: string(did), Rev: "rev1"}))
	require.NoError(t, cb.QueueHostCursor(t.Context(), hostname, "cursor-after-repo"))

	batch := st.NewBatch()
	afterCommit, afterDone, err := cb.StageDurable(t.Context(), batch, 10, false, nil)
	require.NoError(t, err)
	require.Nil(t, afterCommit)
	require.Nil(t, afterDone)
	requireLookupState(t, bs, did, atmosbackfill.StateDiscovered)
	host, _, err := bs.loadPDSHost(hostname)
	require.NoError(t, err)
	require.Empty(t, host.ListReposCursor)

	batch = st.NewBatch()
	afterCommit, afterDone, err = cb.StageDurable(t.Context(), batch, 11, false, nil)
	require.NoError(t, err)
	require.NotNil(t, afterCommit)
	require.NotNil(t, afterDone)
	commitErr := batch.Commit(context.Background())
	afterDone(commitErr)
	require.NoError(t, commitErr)
	afterCommit()
	requireLookupState(t, bs, did, atmosbackfill.StateComplete)
	host, _, err = bs.loadPDSHost(hostname)
	require.NoError(t, err)
	require.Equal(t, "cursor-after-repo", host.ListReposCursor)
}

func TestCompletionBatcherDoesNotStageCompletionAtEqualDurableSeq(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := pebblestore.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	bs := newSeededStore(t, st, nil)
	did := atmos.DID("did:plc:completebatch-equal")
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(did)))

	cb := NewCompletionBatcher(bs, nil)
	cb.RecordWatermark(did, 42, true)
	require.NoError(t, cb.QueueComplete(t.Context(), did, "", &repo.Commit{DID: string(did), Rev: "rev-equal"}))

	b := st.NewBatch()
	afterCommit, afterDone, err := cb.StageDurable(t.Context(), b, 42, false, nil)
	require.NoError(t, err)
	require.Nil(t, afterCommit)
	require.Nil(t, afterDone)
	requireLookupState(t, bs, did, atmosbackfill.StateDiscovered)
	require.Len(t, cb.queued, 1)

	b = st.NewBatch()
	afterCommit, afterDone, err = cb.StageDurable(t.Context(), b, 43, false, nil)
	require.NoError(t, err)
	require.NotNil(t, afterCommit)
	require.NotNil(t, afterDone)

	commitErr := b.Commit(context.Background())
	if commitErr != nil {
		afterDone(commitErr)
		require.NoError(t, commitErr)
	}
	requireLookupState(t, bs, did, atmosbackfill.StateComplete)

	afterCommit()
	afterDone(nil)
	require.Empty(t, cb.queued)
}

func TestCompletionBatcherQueueCompleteRequiresWatermark(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := pebblestore.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	bs := newSeededStore(t, st, nil)
	did := atmos.DID("did:plc:completebatch-missing-watermark")
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(did)))

	cb := NewCompletionBatcher(bs, nil)
	err = cb.QueueComplete(t.Context(), did, "", &repo.Commit{DID: string(did), Rev: "rev-missing"})
	require.ErrorContains(t, err, "missing watermark")
	require.Empty(t, cb.queued)
}

func TestCompletionBatcherStagesExplicitEmptyRepoCompletion(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := pebblestore.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	bs := newSeededStore(t, st, nil)
	did := atmos.DID("did:plc:completebatch-empty")
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(did)))

	cb := NewCompletionBatcher(bs, nil)
	cb.RecordWatermark(did, 0, false)
	require.NoError(t, cb.QueueComplete(t.Context(), did, "", &repo.Commit{DID: string(did), Rev: "rev-empty"}))

	b := st.NewBatch()
	afterCommit, afterDone, err := cb.StageDurable(t.Context(), b, 0, false, nil)
	require.NoError(t, err)
	require.NotNil(t, afterCommit)
	require.NotNil(t, afterDone)

	commitErr := b.Commit(context.Background())
	if commitErr != nil {
		afterDone(commitErr)
		require.NoError(t, commitErr)
	}
	requireLookupState(t, bs, did, atmosbackfill.StateComplete)

	afterCommit()
	afterDone(nil)
	require.Empty(t, cb.queued)
}

// TestCompletionBatcherCommitsMultipleCompletionsInOneBatch covers the spec
// testing checklist item "multiple repos completed in one block are committed
// in one durability batch" — the core amortization the change exists for. Two
// repos whose final events are both durable below nextSeq must stage together
// and land their repo rows + the shared counts row in a single commit.
func TestCompletionBatcherCommitsMultipleCompletionsInOneBatch(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := pebblestore.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	metrics := NewMetrics(prometheus.NewRegistry())
	bs := newSeededStore(t, st, metrics)
	first := atmos.DID("did:plc:completebatch-multi-first")
	second := atmos.DID("did:plc:completebatch-multi-second")
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(first)))
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(second)))

	cb := NewCompletionBatcher(bs, metrics)
	cb.RecordWatermark(first, 40, true)
	require.NoError(t, cb.QueueComplete(t.Context(), first, "", &repo.Commit{DID: string(first), Rev: "rev-first"}))
	cb.RecordWatermark(second, 41, true)
	require.NoError(t, cb.QueueComplete(t.Context(), second, "", &repo.Commit{DID: string(second), Rev: "rev-second"}))

	// nextSeq=42 means events through seq 41 are durable, so both completions
	// are eligible and must stage into the same batch.
	b := st.NewBatch()
	afterCommit, afterDone, err := cb.StageDurable(t.Context(), b, 42, false, nil)
	require.NoError(t, err)
	require.NotNil(t, afterCommit)
	require.NotNil(t, afterDone)
	requireLookupState(t, bs, first, atmosbackfill.StateDiscovered)
	requireLookupState(t, bs, second, atmosbackfill.StateDiscovered)

	commitErr := b.Commit(context.Background())
	if commitErr != nil {
		afterDone(commitErr)
		require.NoError(t, commitErr)
	}
	// One synced commit flips both repos complete.
	requireLookupState(t, bs, first, atmosbackfill.StateComplete)
	requireLookupState(t, bs, second, atmosbackfill.StateComplete)

	afterCommit()
	afterDone(nil)
	require.Empty(t, cb.queued)

	require.InDelta(t, 1.0, testutil.ToFloat64(metrics.CompletionDurableBatches), 0,
		"two completions must coalesce into a single durable batch")
	require.InDelta(t, 2.0, testutil.ToFloat64(metrics.CompletionDurableRepos), 0)

	counts, ok, err := LoadCounts(st)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, Counts{Total: 2, Complete: 2}, counts,
		"the shared counts row must reflect both completions from the single batch")
}

// TestCompletionBatcherForcedStageRejectsNonDurableAppendedCompletion pins the
// crash-over-corruption guard: a force=true (drain/terminal) commit must never
// stage an appended completion whose final event is not yet durable (lastSeq >=
// nextSeq). If it did, saveBatchCursor could advance the listRepos cursor past a
// non-durable completion. The forced path must surface a hard error instead.
func TestCompletionBatcherForcedStageRejectsNonDurableAppendedCompletion(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := pebblestore.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	metrics := NewMetrics(prometheus.NewRegistry())
	bs := newSeededStore(t, st, metrics)
	did := atmos.DID("did:plc:completebatch-forced-nondurable")
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(did)))

	cb := NewCompletionBatcher(bs, metrics)
	// lastSeq=42 with nextSeq=42 means the final event is NOT yet durable.
	cb.RecordWatermark(did, 42, true)
	require.NoError(t, cb.QueueComplete(t.Context(), did, "", &repo.Commit{DID: string(did), Rev: "rev-forced"}))

	b := st.NewBatch()
	afterCommit, afterDone, err := cb.StageDurable(t.Context(), b, 42, true, nil)
	require.Error(t, err, "forced commit must reject an appended completion whose events are not durable")
	require.ErrorContains(t, err, "events not durable")
	require.Nil(t, afterCommit)
	require.Nil(t, afterDone)

	requireLookupState(t, bs, did, atmosbackfill.StateDiscovered)
	require.Len(t, cb.queued, 1, "the completion stays queued, not silently dropped")
	require.InDelta(t, 1.0, testutil.ToFloat64(metrics.CompletionStageErrors), 0)

	// A non-forced commit at the same seq must NOT crash — it simply leaves the
	// completion queued until its block becomes durable. This proves the guard
	// is force-only and does not disturb the steady per-block path.
	b2 := st.NewBatch()
	afterCommit, afterDone, err = cb.StageDurable(t.Context(), b2, 42, false, nil)
	require.NoError(t, err)
	require.Nil(t, afterCommit)
	require.Nil(t, afterDone)
	require.Len(t, cb.queued, 1)
}

func TestCompletionBatcherQueueCompleteReplacesDuplicateDID(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := pebblestore.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	bs := newSeededStore(t, st, nil)
	did := atmos.DID("did:plc:completebatch-duplicate")
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(did)))

	cb := NewCompletionBatcher(bs, nil)
	cb.RecordWatermark(did, 41, true)
	require.NoError(t, cb.QueueComplete(t.Context(), did, "", &repo.Commit{DID: string(did), Rev: "rev-old"}))
	cb.RecordWatermark(did, 42, true)
	require.NoError(t, cb.QueueComplete(t.Context(), did, "", &repo.Commit{DID: string(did), Rev: "rev-new"}))
	require.Len(t, cb.queued, 1)
	require.Equal(t, "rev-new", cb.queued[0].commit.Rev)
	require.Equal(t, completionWatermark{lastSeq: 42, appended: true}, cb.queued[0].watermark)

	b := st.NewBatch()
	afterCommit, afterDone, err := cb.StageDurable(t.Context(), b, 43, false, nil)
	require.NoError(t, err)
	require.NotNil(t, afterCommit)
	require.NotNil(t, afterDone)

	commitErr := b.Commit(context.Background())
	if commitErr != nil {
		afterDone(commitErr)
		require.NoError(t, commitErr)
	}
	afterCommit()
	afterDone(nil)
	require.Empty(t, cb.queued)

	rs, err := bs.readRepoStatus(did)
	require.NoError(t, err)
	require.NotNil(t, rs)
	require.Equal(t, "rev-new", rs.Backfill.Rev)
	require.Equal(t, "rev-new", rs.Rev)
}

// A write that follows the hook's staging stages without waiting (countsMu
// is free once the hook has staged) but does not commit, or return, until
// the hook's batch is done: it builds on rows that batch may yet fail to
// commit (commitPipe).
func TestCompletionBatcherOrdersLaterWritesAfterItsCommit(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := pebblestore.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	bs := newSeededStore(t, st, nil)
	ready := atmos.DID("did:plc:completebatch-lock-ready")
	discovered := atmos.DID("did:plc:completebatch-lock-discovered")
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(ready)))

	cb := NewCompletionBatcher(bs, nil)
	cb.RecordWatermark(ready, 41, true)
	require.NoError(t, cb.QueueComplete(t.Context(), ready, "", &repo.Commit{DID: string(ready), Rev: "rev-ready"}))

	b := st.NewBatch()
	afterCommit, afterDone, err := cb.StageDurable(t.Context(), b, 42, false, nil)
	require.NoError(t, err)
	require.NotNil(t, afterCommit)
	require.NotNil(t, afterDone)

	discoverDone := make(chan error, 1)
	go func() {
		discoverDone <- bs.OnDiscover(t.Context(), testListReposEntry(discovered))
	}()
	requireStaged(t, bs, repoKey(discovered))

	select {
	case err := <-discoverDone:
		afterDone(err)
		require.Failf(t, "OnDiscover completed before afterDone", "err: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	commitErr := b.Commit(context.Background())
	if commitErr != nil {
		afterDone(commitErr)
		require.NoError(t, commitErr)
	}
	afterCommit()
	afterDone(nil)

	select {
	case err := <-discoverDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		require.Fail(t, "OnDiscover remained blocked after afterDone")
	}
	requireLookupState(t, bs, ready, atmosbackfill.StateComplete)
	requireLookupState(t, bs, discovered, atmosbackfill.StateDiscovered)
}

func TestCompletionBatcherAfterCommitRemovesOnlyStagedCompletions(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := pebblestore.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	bs := newSeededStore(t, st, nil)
	ready := atmos.DID("did:plc:completebatch-ready")
	pending := atmos.DID("did:plc:completebatch-pending")
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(ready)))
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(pending)))

	cb := NewCompletionBatcher(bs, nil)
	cb.RecordWatermark(ready, 41, true)
	require.NoError(t, cb.QueueComplete(t.Context(), ready, "", &repo.Commit{DID: string(ready), Rev: "rev-ready"}))
	cb.RecordWatermark(pending, 50, true)
	require.NoError(t, cb.QueueComplete(t.Context(), pending, "", &repo.Commit{DID: string(pending), Rev: "rev-pending"}))

	b := st.NewBatch()
	afterCommit, afterDone, err := cb.StageDurable(t.Context(), b, 42, false, nil)
	require.NoError(t, err)
	require.NotNil(t, afterCommit)
	require.NotNil(t, afterDone)
	require.Len(t, cb.queued, 2)
	requireLookupState(t, bs, ready, atmosbackfill.StateDiscovered)
	requireLookupState(t, bs, pending, atmosbackfill.StateDiscovered)

	commitErr := b.Commit(context.Background())
	if commitErr != nil {
		afterDone(commitErr)
		require.NoError(t, commitErr)
	}
	requireLookupState(t, bs, ready, atmosbackfill.StateComplete)
	requireLookupState(t, bs, pending, atmosbackfill.StateDiscovered)
	require.Len(t, cb.queued, 2)

	afterCommit()
	afterDone(nil)
	require.Len(t, cb.queued, 1)
	require.Equal(t, pending, cb.queued[0].did)

	b = st.NewBatch()
	afterCommit, afterDone, err = cb.StageDurable(t.Context(), b, 51, false, nil)
	require.NoError(t, err)
	require.NotNil(t, afterCommit)
	require.NotNil(t, afterDone)
	require.Len(t, cb.queued, 1)

	commitErr = b.Commit(context.Background())
	if commitErr != nil {
		afterDone(commitErr)
		require.NoError(t, commitErr)
	}
	requireLookupState(t, bs, pending, atmosbackfill.StateComplete)
	require.Len(t, cb.queued, 1)

	afterCommit()
	afterDone(nil)
	require.Empty(t, cb.queued)
}

func TestCompletionBatcherRecordsQueueAndDurableBatchMetrics(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := pebblestore.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	metrics := NewMetrics(prometheus.NewRegistry())
	bs := newSeededStore(t, st, metrics)
	ready := atmos.DID("did:plc:completebatch-metrics-ready")
	pending := atmos.DID("did:plc:completebatch-metrics-pending")
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(ready)))
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(pending)))

	cb := NewCompletionBatcher(bs, metrics)
	cb.RecordWatermark(ready, 41, true)
	require.NoError(t, cb.QueueComplete(t.Context(), ready, "", &repo.Commit{DID: string(ready), Rev: "rev-ready"}))
	cb.RecordWatermark(pending, 50, true)
	require.NoError(t, cb.QueueComplete(t.Context(), pending, "", &repo.Commit{DID: string(pending), Rev: "rev-pending"}))
	require.InDelta(t, 2.0, testutil.ToFloat64(metrics.CompletionQueued), 0)
	require.InDelta(t, 2.0, testutil.ToFloat64(metrics.CompletionQueueDepth), 0)

	b := st.NewBatch()
	afterCommit, afterDone, err := cb.StageDurable(t.Context(), b, 42, false, nil)
	require.NoError(t, err)
	require.NotNil(t, afterCommit)
	require.NotNil(t, afterDone)

	commitErr := b.Commit(context.Background())
	if commitErr != nil {
		afterDone(commitErr)
		require.NoError(t, commitErr)
	}
	afterCommit()
	afterDone(nil)

	require.InDelta(t, 1.0, testutil.ToFloat64(metrics.CompletionDurableBatches), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(metrics.CompletionDurableRepos), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(metrics.CompletionQueueDepth), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(metrics.Completed), 0)
	require.InDelta(t, 0.0, testutil.ToFloat64(metrics.CompletionStageErrors), 0)
}

func TestCompletionBatcherCommitFailureKeepsStagedCompletionQueued(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := pebblestore.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	bs := newSeededStore(t, st, nil)
	did := atmos.DID("did:plc:completebatch-retry")
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(did)))

	cb := NewCompletionBatcher(bs, nil)
	cb.RecordWatermark(did, 41, true)
	require.NoError(t, cb.QueueComplete(t.Context(), did, "", &repo.Commit{DID: string(did), Rev: "rev-retry"}))

	b := st.NewBatch()
	afterCommit, afterDone, err := cb.StageDurable(t.Context(), b, 42, false, nil)
	require.NoError(t, err)
	require.NotNil(t, afterCommit)
	require.NotNil(t, afterDone)
	require.Len(t, cb.queued, 1)

	afterDone(errors.New("synthetic commit failure"))
	require.Len(t, cb.queued, 1)
	require.Equal(t, did, cb.queued[0].did)
	requireLookupState(t, bs, did, atmosbackfill.StateDiscovered)

	b = st.NewBatch()
	afterCommit, afterDone, err = cb.StageDurable(t.Context(), b, 42, false, nil)
	require.NoError(t, err)
	require.NotNil(t, afterCommit)
	require.NotNil(t, afterDone)

	commitErr := b.Commit(context.Background())
	if commitErr != nil {
		afterDone(commitErr)
		require.NoError(t, commitErr)
	}
	requireLookupState(t, bs, did, atmosbackfill.StateComplete)
	require.Len(t, cb.queued, 1)

	afterCommit()
	afterDone(nil)
	require.Empty(t, cb.queued)
}

func TestCompletionBatcherCommitFailureKeepsHostCursorQueued(t *testing.T) {
	t.Parallel()
	st, err := pebblestore.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	bs := newSeededStore(t, st, nil)
	const hostname = "pds-cursor-retry.example.com"
	require.NoError(t, bs.OnHost(t.Context(), atmosbackfill.HostInfo{Hostname: hostname, RelayStatus: "active"}))
	cb := NewCompletionBatcher(bs, nil)
	require.NoError(t, cb.QueueHostCursor(t.Context(), hostname, "cursor-retry"))

	b := st.NewBatch()
	afterCommit, afterDone, err := cb.StageDurable(t.Context(), b, 0, false, nil)
	require.NoError(t, err)
	require.NotNil(t, afterCommit)
	require.NotNil(t, afterDone)
	afterDone(errors.New("synthetic commit failure"))
	require.Contains(t, cb.cursors, hostname)
	host, _, err := bs.loadPDSHost(hostname)
	require.NoError(t, err)
	require.Empty(t, host.ListReposCursor)

	b = st.NewBatch()
	afterCommit, afterDone, err = cb.StageDurable(t.Context(), b, 0, false, nil)
	require.NoError(t, err)
	commitErr := b.Commit(context.Background())
	afterDone(commitErr)
	require.NoError(t, commitErr)
	afterCommit()
	require.NotContains(t, cb.cursors, hostname)
	host, _, err = bs.loadPDSHost(hostname)
	require.NoError(t, err)
	require.Equal(t, "cursor-retry", host.ListReposCursor)
}

// TestCompletionBatcherExhaustedPreservesPendingCheckpoint pins the
// same-batch transition: a Running checkpoint queued for a host and then an
// Exhausted record before either staged must not lose the checkpoint — the
// exhausted host resumes from it on the next run.
func TestCompletionBatcherExhaustedPreservesPendingCheckpoint(t *testing.T) {
	t.Parallel()
	st, err := pebblestore.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	bs := newSeededStore(t, st, nil)
	const hostname = "pds-exhaust-mid-batch.example.com"
	require.NoError(t, bs.OnHost(t.Context(), atmosbackfill.HostInfo{Hostname: hostname, RelayStatus: "active"}))
	cb := NewCompletionBatcher(bs, nil)
	require.NoError(t, cb.QueueHostCursor(t.Context(), hostname, "cursor-4000"))
	require.NoError(t, cb.QueueHostExhausted(t.Context(), hostname, errors.New("listRepos 503"), 8))

	b := st.NewBatch()
	afterCommit, afterDone, err := cb.StageDurable(t.Context(), b, 0, false, nil)
	require.NoError(t, err)
	commitErr := b.Commit(context.Background())
	afterDone(commitErr)
	require.NoError(t, commitErr)
	afterCommit()

	host, _, err := bs.loadPDSHost(hostname)
	require.NoError(t, err)
	require.Equal(t, string(atmosbackfill.HostStateExhausted), host.State)
	require.Equal(t, "cursor-4000", host.ListReposCursor,
		"an exhausted transition must not discard the same-batch checkpoint it superseded")
	require.False(t, host.Enumerated)
	require.Equal(t, 8, host.Attempts)
}

func TestCompletionBatcherOldAfterCommitDoesNotRemoveNewerQueuedCompletion(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := pebblestore.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	bs := newSeededStore(t, st, nil)
	did := atmos.DID("did:plc:completebatch-replaced-after-stage")
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(did)))

	cb := NewCompletionBatcher(bs, nil)
	cb.RecordWatermark(did, 41, true)
	require.NoError(t, cb.QueueComplete(t.Context(), did, "", &repo.Commit{DID: string(did), Rev: "rev-old"}))

	b := st.NewBatch()
	afterCommit, afterDone, err := cb.StageDurable(t.Context(), b, 42, false, nil)
	require.NoError(t, err)
	require.NotNil(t, afterCommit)
	require.NotNil(t, afterDone)

	cb.RecordWatermark(did, 42, true)
	require.NoError(t, cb.QueueComplete(t.Context(), did, "", &repo.Commit{DID: string(did), Rev: "rev-new"}))
	require.Len(t, cb.queued, 1)
	require.Equal(t, "rev-new", cb.queued[0].commit.Rev)

	commitErr := b.Commit(context.Background())
	if commitErr != nil {
		afterDone(commitErr)
		require.NoError(t, commitErr)
	}
	afterCommit()
	afterDone(nil)
	require.Len(t, cb.queued, 1)
	require.Equal(t, "rev-new", cb.queued[0].commit.Rev)

	b = st.NewBatch()
	afterCommit, afterDone, err = cb.StageDurable(t.Context(), b, 43, false, nil)
	require.NoError(t, err)
	require.NotNil(t, afterCommit)
	require.NotNil(t, afterDone)
	commitErr = b.Commit(context.Background())
	if commitErr != nil {
		afterDone(commitErr)
		require.NoError(t, commitErr)
	}
	afterCommit()
	afterDone(nil)
	require.Empty(t, cb.queued)

	rs, err := bs.readRepoStatus(did)
	require.NoError(t, err)
	require.NotNil(t, rs)
	require.Equal(t, "rev-new", rs.Backfill.Rev)
}

// newPipelinedBatcher is a batcher over a store whose segment writer stages
// a durable batch while the one before it commits.
func newPipelinedBatcher(t *testing.T) (metastore.Store, *Store, *completionBatcher) {
	t.Helper()
	db := memstore.New()
	bs := newSeededStore(t, db, nil)
	bs.hookAhead = true
	cb := NewCompletionBatcher(bs, nil)
	bs.SetCompletionBatcher(cb)
	return db, bs, cb
}

// A pipelined writer stages a host cursor behind the open batch that
// carries the completion it covers: that batch commits first, or the writer
// fails and neither commits.
func TestCompletionBatcherPipelinedCursorFollowsOpenCompletion(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db, bs, cb := newPipelinedBatcher(t)
	const hostname = "pds.pipelined-cursor.test"
	require.NoError(t, bs.OnHost(ctx, atmosbackfill.HostInfo{Hostname: hostname, RelayStatus: "active"}))
	did := atmos.DID("did:plc:pipelined-cursor")
	require.NoError(t, bs.onDiscover(ctx, hostname, testListReposEntry(did)))
	cb.RecordWatermark(did, 10, true)
	require.NoError(t, cb.QueueComplete(ctx, did, hostname, &repo.Commit{DID: string(did), Rev: "rev1"}))

	first := stageOne(cb, db)
	require.NoError(t, first.err)
	require.NoError(t, cb.QueueHostCursor(ctx, hostname, "cursor-after-repo"))
	ahead := stageAhead(cb, db)
	requireStageBlocked(t, ahead)
	require.NoError(t, first.finish(nil))
	second := <-ahead
	require.NoError(t, second.err)
	require.ElementsMatch(t, []string{countsKey, string(pdsHostKey(hostname))}, second.keys(),
		"the cursor alone: its completion commits in the batch before")
	require.NoError(t, second.finish(nil))

	requireLookupState(t, bs, did, atmosbackfill.StateComplete)
	host, _, err := bs.loadPDSHost(hostname)
	require.NoError(t, err)
	require.Equal(t, "cursor-after-repo", host.ListReposCursor)
	require.Empty(t, cb.queued)
	require.Empty(t, cb.cursors)
	require.Empty(t, cb.inflight)
}

// A host cursor in an open batch is not staged again behind it: a write to
// the host's row between the two would be undone by the stale cursor.
func TestCompletionBatcherPipelinedDoesNotRestageOpenCursor(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db, bs, cb := newPipelinedBatcher(t)
	const hostname = "pds.pipelined-open-cursor.test"
	require.NoError(t, bs.OnHost(ctx, atmosbackfill.HostInfo{Hostname: hostname, RelayStatus: "active"}))
	require.NoError(t, cb.QueueHostDrained(ctx, hostname, "last-page"))

	first := stageOne(cb, db)
	require.NoError(t, first.err)
	require.Contains(t, first.keys(), string(pdsHostKey(hostname)))
	var second pipelinedStage
	select {
	case second = <-stageAhead(cb, db):
	case <-time.After(5 * time.Second):
		t.Fatal("the batch behind waits on the open one: it staged the cursor again")
	}
	require.NoError(t, second.err)
	require.Empty(t, second.keys(), "the open batch carries the cursor")
	require.Nil(t, second.afterDone)
	require.NoError(t, first.finish(nil))
	host, _, err := bs.loadPDSHost(hostname)
	require.NoError(t, err)
	require.Equal(t, string(atmosbackfill.HostStateDrained), host.State)
	require.Empty(t, cb.cursors)
	requireAggregatesMatchRows(t, db)
}

// When the open batch fails, the writer fails the batch staged behind it,
// and what both carried stays queued for the next writer.
func TestCompletionBatcherPipelinedFailureKeepsBothBatchesQueued(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db, bs, cb := newPipelinedBatcher(t)
	const hostname = "pds.pipelined-failure.test"
	require.NoError(t, bs.OnHost(ctx, atmosbackfill.HostInfo{Hostname: hostname, RelayStatus: "active"}))
	did := atmos.DID("did:plc:pipelined-failure")
	require.NoError(t, bs.onDiscover(ctx, hostname, testListReposEntry(did)))
	cb.RecordWatermark(did, 10, true)
	require.NoError(t, cb.QueueComplete(ctx, did, hostname, &repo.Commit{DID: string(did), Rev: "rev1"}))

	first := stageOne(cb, db)
	require.NoError(t, first.err)
	require.NoError(t, cb.QueueHostCursor(ctx, hostname, "cursor-after-repo"))
	ahead := stageAhead(cb, db)
	requireStageBlocked(t, ahead)
	failed := errors.New("block commit failed")
	require.ErrorIs(t, first.finish(failed), failed)
	second := <-ahead
	require.NoError(t, second.err, "the hook staged again on the store")
	require.ErrorIs(t, second.finish(failed), failed)
	requireLookupState(t, bs, did, atmosbackfill.StateDiscovered)
	require.Len(t, cb.queued, 1)
	require.Contains(t, cb.cursors, hostname)
	require.Empty(t, cb.inflight)

	third := stageOne(cb, db)
	require.NoError(t, third.err)
	require.Contains(t, third.keys(), string(repoKey(did)))
	require.Contains(t, third.keys(), string(pdsHostKey(hostname)))
	require.NoError(t, third.finish(nil))
	requireLookupState(t, bs, did, atmosbackfill.StateComplete)
	host, _, err := bs.loadPDSHost(hostname)
	require.NoError(t, err)
	require.Equal(t, "cursor-after-repo", host.ListReposCursor)
	requireAggregatesMatchRows(t, db)
}

// A completion queued again while its earlier one is in an open batch is a
// new completion: the batch staged ahead carries it, on top of the open one.
func TestCompletionBatcherPipelinedStagesNewerCompletionBehindOpenOne(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db, bs, cb := newPipelinedBatcher(t)
	did := atmos.DID("did:plc:pipelined-requeued")
	require.NoError(t, bs.OnDiscover(ctx, testListReposEntry(did)))
	cb.RecordWatermark(did, 41, true)
	require.NoError(t, cb.QueueComplete(ctx, did, "", &repo.Commit{DID: string(did), Rev: "rev-old"}))

	first := stageOne(cb, db)
	require.NoError(t, first.err)
	cb.RecordWatermark(did, 42, true)
	require.NoError(t, cb.QueueComplete(ctx, did, "", &repo.Commit{DID: string(did), Rev: "rev-new"}))
	ahead := stageAhead(cb, db)
	requireStageBlocked(t, ahead)
	require.NoError(t, first.finish(nil))
	second := <-ahead
	require.NoError(t, second.err)
	require.Contains(t, second.keys(), string(repoKey(did)))
	require.NoError(t, second.finish(nil))

	rs, err := bs.readRepoStatus(did)
	require.NoError(t, err)
	require.Equal(t, "rev-new", rs.Backfill.Rev)
	require.Empty(t, cb.queued)
	require.Empty(t, cb.inflight)
	counts, _, err := LoadCounts(db)
	require.NoError(t, err)
	require.Equal(t, uint64(1), counts.Complete)
	requireAggregatesMatchRows(t, db)
}

func testListReposEntry(did atmos.DID) atmossync.ListReposEntry {
	return atmossync.ListReposEntry{DID: did, Active: true}
}

func requireLookupState(t *testing.T, bs *Store, did atmos.DID, want atmosbackfill.State) {
	t.Helper()

	got, err := bs.Lookup(t.Context(), did)
	require.NoError(t, err)
	require.Equal(t, want, got.State)
}
