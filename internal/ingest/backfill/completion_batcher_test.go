package backfill

import (
	"errors"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/jcalabro/atmos"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	"github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/stretchr/testify/require"
)

func TestCompletionBatcherStagesCompletionAtDurableSeq(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	bs := NewStore(st, nil)
	did := atmos.DID("did:plc:completebatch")
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(did)))

	cb := NewCompletionBatcher(bs, nil)
	cb.RecordWatermark(did, 41, true)
	require.NoError(t, cb.QueueComplete(t.Context(), did, &repo.Commit{DID: string(did), Rev: "rev1"}))

	b := st.NewBatch()
	afterCommit, afterDone, err := cb.StageDurable(t.Context(), b, 42, false)
	require.NoError(t, err)
	require.NotNil(t, afterCommit)
	require.NotNil(t, afterDone)
	requireLookupState(t, bs, did, atmosbackfill.StateDiscovered)

	commitErr := st.Commit(b, store.SyncWrites)
	if commitErr != nil {
		afterDone(commitErr)
		require.NoError(t, commitErr)
	}
	requireLookupState(t, bs, did, atmosbackfill.StateComplete)

	require.Len(t, cb.queued, 1)
	require.Equal(t, did, cb.queued[0].did)

	afterCommit()
	afterDone(nil)
	require.NoError(t, b.Close())
	require.Empty(t, cb.queued)
}

func TestCompletionBatcherDoesNotStageCompletionAtEqualDurableSeq(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	bs := NewStore(st, nil)
	did := atmos.DID("did:plc:completebatch-equal")
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(did)))

	cb := NewCompletionBatcher(bs, nil)
	cb.RecordWatermark(did, 42, true)
	require.NoError(t, cb.QueueComplete(t.Context(), did, &repo.Commit{DID: string(did), Rev: "rev-equal"}))

	b := st.NewBatch()
	afterCommit, afterDone, err := cb.StageDurable(t.Context(), b, 42, false)
	require.NoError(t, err)
	require.Nil(t, afterCommit)
	require.Nil(t, afterDone)
	requireLookupState(t, bs, did, atmosbackfill.StateDiscovered)
	require.Len(t, cb.queued, 1)
	require.NoError(t, b.Close())

	b = st.NewBatch()
	afterCommit, afterDone, err = cb.StageDurable(t.Context(), b, 43, false)
	require.NoError(t, err)
	require.NotNil(t, afterCommit)
	require.NotNil(t, afterDone)

	commitErr := st.Commit(b, store.SyncWrites)
	if commitErr != nil {
		afterDone(commitErr)
		require.NoError(t, commitErr)
	}
	requireLookupState(t, bs, did, atmosbackfill.StateComplete)

	afterCommit()
	afterDone(nil)
	require.NoError(t, b.Close())
	require.Empty(t, cb.queued)
}

func TestCompletionBatcherQueueCompleteRequiresWatermark(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	bs := NewStore(st, nil)
	did := atmos.DID("did:plc:completebatch-missing-watermark")
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(did)))

	cb := NewCompletionBatcher(bs, nil)
	err = cb.QueueComplete(t.Context(), did, &repo.Commit{DID: string(did), Rev: "rev-missing"})
	require.ErrorContains(t, err, "missing watermark")
	require.Empty(t, cb.queued)
}

func TestCompletionBatcherStagesExplicitEmptyRepoCompletion(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	bs := NewStore(st, nil)
	did := atmos.DID("did:plc:completebatch-empty")
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(did)))

	cb := NewCompletionBatcher(bs, nil)
	cb.RecordWatermark(did, 0, false)
	require.NoError(t, cb.QueueComplete(t.Context(), did, &repo.Commit{DID: string(did), Rev: "rev-empty"}))

	b := st.NewBatch()
	afterCommit, afterDone, err := cb.StageDurable(t.Context(), b, 0, false)
	require.NoError(t, err)
	require.NotNil(t, afterCommit)
	require.NotNil(t, afterDone)

	commitErr := st.Commit(b, store.SyncWrites)
	if commitErr != nil {
		afterDone(commitErr)
		require.NoError(t, commitErr)
	}
	requireLookupState(t, bs, did, atmosbackfill.StateComplete)

	afterCommit()
	afterDone(nil)
	require.NoError(t, b.Close())
	require.Empty(t, cb.queued)
}

func TestCompletionBatcherQueueCompleteReplacesDuplicateDID(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	bs := NewStore(st, nil)
	did := atmos.DID("did:plc:completebatch-duplicate")
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(did)))

	cb := NewCompletionBatcher(bs, nil)
	cb.RecordWatermark(did, 41, true)
	require.NoError(t, cb.QueueComplete(t.Context(), did, &repo.Commit{DID: string(did), Rev: "rev-old"}))
	cb.RecordWatermark(did, 42, true)
	require.NoError(t, cb.QueueComplete(t.Context(), did, &repo.Commit{DID: string(did), Rev: "rev-new"}))
	require.Len(t, cb.queued, 1)
	require.Equal(t, "rev-new", cb.queued[0].commit.Rev)
	require.Equal(t, completionWatermark{lastSeq: 42, appended: true}, cb.queued[0].watermark)

	b := st.NewBatch()
	afterCommit, afterDone, err := cb.StageDurable(t.Context(), b, 43, false)
	require.NoError(t, err)
	require.NotNil(t, afterCommit)
	require.NotNil(t, afterDone)

	commitErr := st.Commit(b, store.SyncWrites)
	if commitErr != nil {
		afterDone(commitErr)
		require.NoError(t, commitErr)
	}
	afterCommit()
	afterDone(nil)
	require.NoError(t, b.Close())
	require.Empty(t, cb.queued)

	rs, err := bs.readRepoStatus(did)
	require.NoError(t, err)
	require.NotNil(t, rs)
	require.Equal(t, "rev-new", rs.Backfill.Rev)
	require.Equal(t, "rev-new", rs.Rev)
}

func TestCompletionBatcherHoldsCountsLockUntilAfterDone(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	bs := NewStore(st, nil)
	ready := atmos.DID("did:plc:completebatch-lock-ready")
	discovered := atmos.DID("did:plc:completebatch-lock-discovered")
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(ready)))

	cb := NewCompletionBatcher(bs, nil)
	cb.RecordWatermark(ready, 41, true)
	require.NoError(t, cb.QueueComplete(t.Context(), ready, &repo.Commit{DID: string(ready), Rev: "rev-ready"}))

	b := st.NewBatch()
	defer func() { _ = b.Close() }()
	afterCommit, afterDone, err := cb.StageDurable(t.Context(), b, 42, false)
	require.NoError(t, err)
	require.NotNil(t, afterCommit)
	require.NotNil(t, afterDone)

	discoverDone := make(chan error, 1)
	go func() {
		discoverDone <- bs.OnDiscover(t.Context(), testListReposEntry(discovered))
	}()

	select {
	case err := <-discoverDone:
		afterDone(err)
		require.Failf(t, "OnDiscover completed before afterDone", "err: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	commitErr := st.Commit(b, store.SyncWrites)
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
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	bs := NewStore(st, nil)
	ready := atmos.DID("did:plc:completebatch-ready")
	pending := atmos.DID("did:plc:completebatch-pending")
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(ready)))
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(pending)))

	cb := NewCompletionBatcher(bs, nil)
	cb.RecordWatermark(ready, 41, true)
	require.NoError(t, cb.QueueComplete(t.Context(), ready, &repo.Commit{DID: string(ready), Rev: "rev-ready"}))
	cb.RecordWatermark(pending, 50, true)
	require.NoError(t, cb.QueueComplete(t.Context(), pending, &repo.Commit{DID: string(pending), Rev: "rev-pending"}))

	b := st.NewBatch()
	afterCommit, afterDone, err := cb.StageDurable(t.Context(), b, 42, false)
	require.NoError(t, err)
	require.NotNil(t, afterCommit)
	require.NotNil(t, afterDone)
	require.Len(t, cb.queued, 2)
	requireLookupState(t, bs, ready, atmosbackfill.StateDiscovered)
	requireLookupState(t, bs, pending, atmosbackfill.StateDiscovered)

	commitErr := st.Commit(b, store.SyncWrites)
	if commitErr != nil {
		afterDone(commitErr)
		require.NoError(t, commitErr)
	}
	requireLookupState(t, bs, ready, atmosbackfill.StateComplete)
	requireLookupState(t, bs, pending, atmosbackfill.StateDiscovered)
	require.Len(t, cb.queued, 2)

	afterCommit()
	afterDone(nil)
	require.NoError(t, b.Close())
	require.Len(t, cb.queued, 1)
	require.Equal(t, pending, cb.queued[0].did)

	b = st.NewBatch()
	afterCommit, afterDone, err = cb.StageDurable(t.Context(), b, 51, false)
	require.NoError(t, err)
	require.NotNil(t, afterCommit)
	require.NotNil(t, afterDone)
	require.Len(t, cb.queued, 1)

	commitErr = st.Commit(b, store.SyncWrites)
	if commitErr != nil {
		afterDone(commitErr)
		require.NoError(t, commitErr)
	}
	requireLookupState(t, bs, pending, atmosbackfill.StateComplete)
	require.Len(t, cb.queued, 1)

	afterCommit()
	afterDone(nil)
	require.NoError(t, b.Close())
	require.Empty(t, cb.queued)
}

func TestCompletionBatcherCommitFailureKeepsStagedCompletionQueued(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	bs := NewStore(st, nil)
	did := atmos.DID("did:plc:completebatch-retry")
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(did)))

	cb := NewCompletionBatcher(bs, nil)
	cb.RecordWatermark(did, 41, true)
	require.NoError(t, cb.QueueComplete(t.Context(), did, &repo.Commit{DID: string(did), Rev: "rev-retry"}))

	b := st.NewBatch()
	afterCommit, afterDone, err := cb.StageDurable(t.Context(), b, 42, false)
	require.NoError(t, err)
	require.NotNil(t, afterCommit)
	require.NotNil(t, afterDone)
	require.Len(t, cb.queued, 1)

	afterDone(errors.New("synthetic commit failure"))
	require.NoError(t, b.Close())
	require.Len(t, cb.queued, 1)
	require.Equal(t, did, cb.queued[0].did)
	requireLookupState(t, bs, did, atmosbackfill.StateDiscovered)

	b = st.NewBatch()
	afterCommit, afterDone, err = cb.StageDurable(t.Context(), b, 42, false)
	require.NoError(t, err)
	require.NotNil(t, afterCommit)
	require.NotNil(t, afterDone)

	commitErr := st.Commit(b, store.SyncWrites)
	if commitErr != nil {
		afterDone(commitErr)
		require.NoError(t, commitErr)
	}
	requireLookupState(t, bs, did, atmosbackfill.StateComplete)
	require.Len(t, cb.queued, 1)

	afterCommit()
	afterDone(nil)
	require.NoError(t, b.Close())
	require.Empty(t, cb.queued)
}

func TestCompletionBatcherOldAfterCommitDoesNotRemoveNewerQueuedCompletion(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	bs := NewStore(st, nil)
	did := atmos.DID("did:plc:completebatch-replaced-after-stage")
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(did)))

	cb := NewCompletionBatcher(bs, nil)
	cb.RecordWatermark(did, 41, true)
	require.NoError(t, cb.QueueComplete(t.Context(), did, &repo.Commit{DID: string(did), Rev: "rev-old"}))

	b := st.NewBatch()
	afterCommit, afterDone, err := cb.StageDurable(t.Context(), b, 42, false)
	require.NoError(t, err)
	require.NotNil(t, afterCommit)
	require.NotNil(t, afterDone)

	cb.RecordWatermark(did, 42, true)
	require.NoError(t, cb.QueueComplete(t.Context(), did, &repo.Commit{DID: string(did), Rev: "rev-new"}))
	require.Len(t, cb.queued, 1)
	require.Equal(t, "rev-new", cb.queued[0].commit.Rev)

	commitErr := st.Commit(b, store.SyncWrites)
	if commitErr != nil {
		afterDone(commitErr)
		require.NoError(t, commitErr)
	}
	afterCommit()
	afterDone(nil)
	require.NoError(t, b.Close())
	require.Len(t, cb.queued, 1)
	require.Equal(t, "rev-new", cb.queued[0].commit.Rev)

	b = st.NewBatch()
	afterCommit, afterDone, err = cb.StageDurable(t.Context(), b, 43, false)
	require.NoError(t, err)
	require.NotNil(t, afterCommit)
	require.NotNil(t, afterDone)
	commitErr = st.Commit(b, store.SyncWrites)
	if commitErr != nil {
		afterDone(commitErr)
		require.NoError(t, commitErr)
	}
	afterCommit()
	afterDone(nil)
	require.NoError(t, b.Close())
	require.Empty(t, cb.queued)

	rs, err := bs.readRepoStatus(did)
	require.NoError(t, err)
	require.NotNil(t, rs)
	require.Equal(t, "rev-new", rs.Backfill.Rev)
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
