package backfill

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	"github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// newTestStore is the shared fixture for Store unit tests: open a
// fresh pebble in t.TempDir(), register cleanup, return the wrapped
// Store with no metrics.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return NewStore(db, nil)
}

// TestStore_Lookup_Missing covers the StateUnknown path: a fresh
// pebble has no repo/<did> rows, and atmos uses StateUnknown to
// trigger OnDiscover.
func TestStore_Lookup_Missing(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	got, err := s.Lookup(context.Background(), atmos.DID("did:plc:abc"))
	require.NoError(t, err)
	require.Equal(t, atmosbackfill.StateUnknown, got.State)
	require.False(t, got.Active)
}

// TestStore_Lookup_StatusMapping pins the disk-status -> atmos.State
// projection. atmos uses these values to decide whether to dispatch
// the DID for download or skip it.
func TestStore_Lookup_StatusMapping(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		status   Status
		expected atmosbackfill.State
	}{
		{"not_started -> Discovered", StatusNotStarted, atmosbackfill.StateDiscovered},
		{"legacy pending -> Complete", StatusPending, atmosbackfill.StateComplete},
		{"complete -> Complete", StatusComplete, atmosbackfill.StateComplete},
		{"failed -> Failed", StatusFailed, atmosbackfill.StateFailed},
		{"unavailable -> Complete", StatusUnavailable, atmosbackfill.StateComplete},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestStore(t)
			did := atmos.DID("did:plc:abc")
			rs := &RepoStatus{
				Backfill: RepoBackfillStatus{Status: tc.status},
				Active:   true,
			}
			require.NoError(t, s.putRepoStatus(did, rs))

			got, err := s.Lookup(context.Background(), did)
			require.NoError(t, err)
			require.Equal(t, tc.expected, got.State)
			require.True(t, got.Active)
		})
	}
}

// TestStore_Lookup_CorruptRow asserts decode failures are surfaced as
// errors, not silently mapped to StateUnknown — the latter would let
// the engine fire OnDiscover on a "corrupt but present" row and
// clobber it. We want a Run failure instead.
func TestStore_Lookup_CorruptRow(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	did := atmos.DID("did:plc:abc")
	require.NoError(t, s.db.Set(repoKey(did), []byte("not json"), pebble.Sync))

	_, err := s.Lookup(context.Background(), did)
	require.ErrorContains(t, err, "decode RepoStatus")
}

// TestStore_Lookup_UnknownStatus pins the forward-compat trap: if
// pebble holds a status string the current binary doesn't recognize
// (e.g. a future StatusInProgress added by a newer version),
// Lookup must abort the Run rather than silently mapping it to
// StateUnknown — that would let the engine clobber the unfamiliar row
// via OnDiscover.
func TestStore_Lookup_UnknownStatus(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	did := atmos.DID("did:plc:abc")

	rs := &RepoStatus{
		Backfill: RepoBackfillStatus{Status: Status("future")},
		Active:   true,
	}
	require.NoError(t, s.putRepoStatus(did, rs))

	_, err := s.Lookup(context.Background(), did)
	require.ErrorContains(t, err, "unknown status")
	require.ErrorContains(t, err, "future")
}

// TestStore_OnDiscover_WritesNotStarted is the producer-side hot
// path: every DID returned by listRepos for the first time gets a
// fresh row at not_started with the listRepos.Active flag preserved.
func TestStore_OnDiscover_WritesNotStarted(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	did := atmos.DID("did:plc:abc")

	err := s.OnDiscover(context.Background(), atmossync.ListReposEntry{
		DID:    did,
		Active: true,
	})
	require.NoError(t, err)

	got, err := s.Lookup(context.Background(), did)
	require.NoError(t, err)
	require.Equal(t, atmosbackfill.StateDiscovered, got.State)
	require.True(t, got.Active)

	rs, err := s.readRepoStatus(did)
	require.NoError(t, err)
	require.NotNil(t, rs)
	require.Equal(t, StatusNotStarted, rs.Backfill.Status)
	require.False(t, rs.Backfill.StartedAt.IsZero(), "StartedAt must be set on first discovery")
}

// TestStore_OnDiscover_InactiveDID confirms an inactive DID still
// gets a row written so we can re-attempt later if it flips active.
// Active flips are tracked by Store, not by absence of a row.
func TestStore_OnDiscover_InactiveDID(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	did := atmos.DID("did:plc:tomb")

	err := s.OnDiscover(context.Background(), atmossync.ListReposEntry{
		DID:    did,
		Active: false,
	})
	require.NoError(t, err)

	got, err := s.Lookup(context.Background(), did)
	require.NoError(t, err)
	require.Equal(t, atmosbackfill.StateDiscovered, got.State)
	require.False(t, got.Active)
}

// TestStore_OnUpdate_FlipsActive_PreservesStatus exercises the
// active-flip path: an account flipping inactive must update Active
// in pebble without clobbering the lifecycle Status. atmos uses this
// callback to tell us "the relay's view of activeness changed".
func TestStore_OnUpdate_FlipsActive_PreservesStatus(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	did := atmos.DID("did:plc:flip")

	require.NoError(t, s.OnDiscover(context.Background(), atmossync.ListReposEntry{
		DID: did, Active: true,
	}))

	require.NoError(t, s.OnUpdate(context.Background(), atmossync.ListReposEntry{
		DID: did, Active: false,
	}))

	got, err := s.Lookup(context.Background(), did)
	require.NoError(t, err)
	require.Equal(t, atmosbackfill.StateDiscovered, got.State, "Status must not flip on OnUpdate")
	require.False(t, got.Active, "Active must be updated to false")
}

// TestStore_OnUpdate_MissingRow is a sanity check: atmos only fires
// OnUpdate for DIDs whose Lookup found a row, so the row should
// always exist. If somehow it doesn't, we want a hard error rather
// than a silent recreate.
func TestStore_OnUpdate_MissingRow(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	err := s.OnUpdate(context.Background(), atmossync.ListReposEntry{
		DID: atmos.DID("did:plc:nobody"), Active: true,
	})
	require.ErrorContains(t, err, "missing row")
}

// TestStore_OnComplete_WritesComplete is the success path: a
// successful download lands the row at Complete with the commit rev
// recorded both at top-level Rev and in Backfill.Rev (per
// docs/README.md §3.5 — both fields exist; Rev is the latest, Backfill.Rev
// is the rev at end of initial download).
func TestStore_OnComplete_WritesComplete(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	metrics := NewMetrics(prometheus.NewRegistry())
	s.metrics = metrics
	did := atmos.DID("did:plc:done")

	require.NoError(t, s.OnDiscover(context.Background(), atmossync.ListReposEntry{
		DID: did, Active: true,
	}))

	commit := &repo.Commit{DID: string(did), Rev: "rev-final"}
	require.NoError(t, s.OnComplete(context.Background(), did, "", commit))

	got, err := s.Lookup(context.Background(), did)
	require.NoError(t, err)
	require.Equal(t, atmosbackfill.StateComplete, got.State)

	rs, err := s.readRepoStatus(did)
	require.NoError(t, err)
	require.Equal(t, "rev-final", rs.Backfill.Rev)
	require.Equal(t, "rev-final", rs.Rev)
	require.False(t, rs.Backfill.CompletedAt.IsZero())
	require.False(t, rs.UpdatedAt.IsZero())
	require.InDelta(t, 1.0, testutil.ToFloat64(metrics.Completed), 0)
}

func TestStore_OnCompleteQueuesWhenBatcherConfigured(t *testing.T) {
	t.Parallel()

	st, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	metrics := NewMetrics(prometheus.NewRegistry())
	s := NewStore(st, metrics)
	cb := NewCompletionBatcher(s, metrics)
	s.SetCompletionBatcher(cb)

	ctx := t.Context()
	did := atmos.DID("did:plc:queued-complete")
	require.NoError(t, s.OnDiscover(ctx, atmossync.ListReposEntry{DID: did, Active: true}))

	var hookRan bool
	s.afterComplete = func(ctx context.Context, got atmos.DID) error {
		require.Equal(t, did, got)
		requireLookupState(t, s, got, atmosbackfill.StateComplete)
		hookRan = true
		return nil
	}

	cb.RecordWatermark(did, 41, true)
	require.NoError(t, s.OnComplete(ctx, did, "", &repo.Commit{DID: string(did), Rev: "rev-queued"}))
	requireLookupState(t, s, did, atmosbackfill.StateDiscovered)
	require.False(t, hookRan, "afterComplete must wait for durable completion commit")
	require.InDelta(t, 0.0, testutil.ToFloat64(metrics.Completed), 0)
	require.Len(t, cb.queued, 1)

	b := st.NewBatch()
	afterCommit, afterDone, err := cb.StageDurable(ctx, b, 42, false, nil)
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

	requireLookupState(t, s, did, atmosbackfill.StateComplete)
	require.True(t, hookRan)
	require.Empty(t, cb.queued)
	require.InDelta(t, 1.0, testutil.ToFloat64(metrics.Completed), 0)
}

func TestStore_OnCompleteWithBatcherRequiresWatermark(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	cb := NewCompletionBatcher(s, nil)
	s.SetCompletionBatcher(cb)

	ctx := t.Context()
	did := atmos.DID("did:plc:queued-missing-watermark")
	require.NoError(t, s.OnDiscover(ctx, atmossync.ListReposEntry{DID: did, Active: true}))

	err := s.OnComplete(ctx, did, "", &repo.Commit{DID: string(did), Rev: "rev-missing"})
	require.ErrorContains(t, err, "missing watermark")
	requireLookupState(t, s, did, atmosbackfill.StateDiscovered)
	require.Empty(t, cb.queued)
}

func TestStore_OnCompleteRunsHookAfterDurableRow(t *testing.T) {
	t.Parallel()

	db := newTestStore(t).db
	s := NewStore(db, nil)

	ctx := context.Background()
	did := atmos.DID("did:plc:hooked")
	require.NoError(t, s.OnDiscover(ctx, atmossync.ListReposEntry{DID: did, Active: true}))

	var hookSawComplete bool
	s.afterComplete = func(ctx context.Context, got atmos.DID) error {
		require.Equal(t, did, got)
		val, closer, err := db.Get(RepoKey(string(got)))
		require.NoError(t, err)
		defer func() { _ = closer.Close() }()

		rs, err := DecodeRepoStatus(val)
		require.NoError(t, err)
		hookSawComplete = rs.Backfill.Status == StatusComplete && rs.Backfill.Rev == "rev-hooked"
		return nil
	}

	require.NoError(t, s.OnComplete(ctx, did, "", &repo.Commit{DID: string(did), Rev: "rev-hooked"}))
	require.True(t, hookSawComplete, "completion hook must run after the Complete row is durable and readable")
}

// TestStore_OnComplete_PreservesExtraFields locks in the RMW
// guarantee: a future PR may add fields like RecordCount; OnComplete
// must not clobber them. We simulate this by writing a row with a
// non-zero RecordCount directly, then calling OnComplete.
func TestStore_OnComplete_PreservesExtraFields(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	did := atmos.DID("did:plc:rmv")

	require.NoError(t, s.putRepoStatus(did, &RepoStatus{
		Backfill:    RepoBackfillStatus{Status: StatusNotStarted},
		RecordCount: 42,
		Active:      true,
	}))

	commit := &repo.Commit{DID: string(did), Rev: "rev-z"}
	require.NoError(t, s.OnComplete(context.Background(), did, "", commit))

	rs, err := s.readRepoStatus(did)
	require.NoError(t, err)
	require.Equal(t, int64(42), rs.RecordCount, "RMW must preserve RecordCount")
	require.Equal(t, StatusComplete, rs.Backfill.Status)
}

// TestStore_OnFail_RecordsFailure pins the failure path. attempts is
// the count for the current Run only — atmos passes initial+retries
// from processRepo, and we overwrite rather than accumulate across
// Runs (this cosmetic regression is intentional).
func TestStore_OnFail_RecordsFailure(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	did := atmos.DID("did:plc:bad")

	require.NoError(t, s.OnDiscover(context.Background(), atmossync.ListReposEntry{
		DID: did, Active: true,
	}))

	failErr := errors.New("upstream 500")
	require.NoError(t, s.OnFail(context.Background(), did, "", failErr, 6))

	got, err := s.Lookup(context.Background(), did)
	require.NoError(t, err)
	require.Equal(t, atmosbackfill.StateFailed, got.State)

	rs, err := s.readRepoStatus(did)
	require.NoError(t, err)
	require.Equal(t, "upstream 500", rs.Backfill.LastError)
	require.Equal(t, 6, rs.Backfill.Attempts)
	require.False(t, rs.Backfill.StartedAt.IsZero(), "StartedAt set by OnDiscover must survive")
	require.True(t, rs.Backfill.CompletedAt.IsZero(), "OnFail must not stamp CompletedAt")
}

// TestStore_OnFail_AfterPriorComplete documents (and locks in) the
// defensive behavior: a Run never re-attempts a Complete row in this
// PR, but if it ever did, OnFail keeps Backfill.CompletedAt and Rev
// from the prior run rather than zeroing them.
func TestStore_OnFail_AfterPriorComplete(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	did := atmos.DID("did:plc:flake")

	require.NoError(t, s.OnDiscover(context.Background(), atmossync.ListReposEntry{
		DID: did, Active: true,
	}))
	require.NoError(t, s.OnComplete(context.Background(), did, "", &repo.Commit{DID: string(did), Rev: "rev-good"}))

	require.NoError(t, s.OnFail(context.Background(), did, "", errors.New("boom"), 3))

	rs, err := s.readRepoStatus(did)
	require.NoError(t, err)
	require.Equal(t, StatusFailed, rs.Backfill.Status)
	require.Equal(t, "rev-good", rs.Backfill.Rev)
	require.False(t, rs.Backfill.CompletedAt.IsZero(), "prior CompletedAt preserved")
}

func TestStore_OnFail_RepoNotFoundCompletesWithoutError(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	ctx := context.Background()
	did := atmos.DID("did:plc:deleted")

	require.NoError(t, s.OnDiscover(ctx, atmossync.ListReposEntry{
		DID: did, Active: true,
	}))
	require.NoError(t, s.recordIdentityResolution(ctx, did, IdentityResolution{
		PDS:  "https://pds.example.com",
		Host: "pds.example.com",
	}))

	failErr := &xrpc.Error{
		StatusCode: 400,
		Name:       "RepoNotFound",
		Message:    "Could not find repo for DID: did:plc:deleted",
	}
	require.NoError(t, s.OnFail(ctx, did, "", failErr, 1))

	got, err := s.Lookup(ctx, did)
	require.NoError(t, err)
	require.Equal(t, atmosbackfill.StateComplete, got.State)

	rs, err := s.readRepoStatus(did)
	require.NoError(t, err)
	require.Equal(t, StatusComplete, rs.Backfill.Status)
	require.Empty(t, rs.Backfill.LastError)
	require.Equal(t, 0, rs.Backfill.Attempts)
	require.False(t, rs.Backfill.CompletedAt.IsZero())

	counts, ok, err := LoadCounts(s.db)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, Counts{Total: 1, Complete: 1}, counts)

	hs, ok, err := loadHostStatus(s.db, "pds.example.com")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), hs.Total)
	require.Equal(t, uint64(1), hs.Complete)
	require.Equal(t, uint64(0), hs.Failed)
	require.Empty(t, hs.LatestError)
	require.Empty(t, hs.ErrorClassCounts)
	require.Empty(t, hs.RecentErrors)
}

func TestStore_OnFail_RepoUnavailableIsTerminalNotFailed(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"RepoDeactivated", "RepoSuspended", "RepoTakendown"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := newTestStore(t)
			ctx := context.Background()
			did := atmos.DID("did:plc:gone")

			require.NoError(t, s.OnDiscover(ctx, atmossync.ListReposEntry{
				DID: did, Active: true,
			}))
			require.NoError(t, s.recordIdentityResolution(ctx, did, IdentityResolution{
				PDS:  "https://pds.example.com",
				Host: "pds.example.com",
			}))

			failErr := &xrpc.Error{
				StatusCode: 400,
				Name:       name,
				Message:    "Repo has been " + name + ": did:plc:gone",
			}
			require.NoError(t, s.OnFail(ctx, did, "", failErr, 1))

			// atmos must skip re-dispatch: unavailable projects to StateComplete.
			got, err := s.Lookup(ctx, did)
			require.NoError(t, err)
			require.Equal(t, atmosbackfill.StateComplete, got.State)

			rs, err := s.readRepoStatus(did)
			require.NoError(t, err)
			require.Equal(t, StatusUnavailable, rs.Backfill.Status)
			require.Empty(t, rs.Backfill.LastError)
			require.Equal(t, 0, rs.Backfill.Attempts)

			// Counts: tracked under Unavailable, never Failed or Complete.
			counts, ok, err := LoadCounts(s.db)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, Counts{Total: 1, Unavailable: 1}, counts)

			// Host buckets: not a failed host, no error sample pollution.
			hs, ok, err := loadHostStatus(s.db, "pds.example.com")
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, uint64(1), hs.Total)
			require.Equal(t, uint64(1), hs.Unavailable)
			require.Equal(t, uint64(0), hs.Failed)
			require.Equal(t, uint64(0), hs.Complete)
			require.Empty(t, hs.LatestError)
			require.Empty(t, hs.ErrorClassCounts)
			require.Empty(t, hs.RecentErrors)
		})
	}
}

// TestStore_OnComplete_ClearsLastError is the symmetric partner to
// TestStore_OnFail_AfterPriorComplete: a Failed -> Complete transition
// must scrub the stale LastError so observers don't see a zombie
// diagnostic on a healthy row. Without an explicit test, a future
// refactor of OnComplete could silently drop the LastError = "" line.
func TestStore_OnComplete_ClearsLastError(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	did := atmos.DID("did:plc:recovered")

	require.NoError(t, s.OnDiscover(context.Background(), atmossync.ListReposEntry{
		DID: did, Active: true,
	}))
	require.NoError(t, s.OnFail(context.Background(), did, "", errors.New("transient"), 4))

	rs, err := s.readRepoStatus(did)
	require.NoError(t, err)
	require.Equal(t, "transient", rs.Backfill.LastError, "precondition: failure left a LastError")

	require.NoError(t, s.OnComplete(context.Background(), did, "", &repo.Commit{DID: string(did), Rev: "rev-recovered"}))

	rs, err = s.readRepoStatus(did)
	require.NoError(t, err)
	require.Equal(t, "", rs.Backfill.LastError, "OnComplete must clear LastError")
	require.Equal(t, StatusComplete, rs.Backfill.Status)
}

func TestStore_RecordRetryFailure_SchedulesNextAttempt(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	ctx := context.Background()
	did := atmos.DID("did:plc:retryfailure")
	next := time.Date(2026, 6, 23, 16, 0, 0, 0, time.UTC)

	require.NoError(t, s.OnDiscover(ctx, atmossync.ListReposEntry{DID: did, Active: true}))
	require.NoError(t, s.OnFail(ctx, did, "pds.example.com", errors.New("xrpc: HTTP 503: bootstrap unavailable"), 1))
	require.NoError(t, s.RecordRetryFailure(ctx, did, "pds.example.com", errors.New("xrpc: HTTP 503: unavailable"), next))

	rs, err := s.readRepoStatus(did)
	require.NoError(t, err)
	require.Equal(t, StatusFailed, rs.Backfill.Status)
	require.Equal(t, "xrpc: HTTP 503: unavailable", rs.Backfill.LastError)
	require.Equal(t, 2, rs.Backfill.Attempts)
	require.Equal(t, 1, rs.Backfill.RetryCount)
	require.Equal(t, next, rs.Backfill.NextAttemptAt)
	require.Equal(t, "pds.example.com", rs.Host)

	counts, ok, err := LoadCounts(s.db)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, Counts{Total: 1, Failed: 1}, counts)

	hs, ok, err := loadHostStatus(s.db, "pds.example.com")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), hs.Total)
	require.Equal(t, uint64(1), hs.Active)
	require.Equal(t, uint64(1), hs.Failed)
	require.Equal(t, uint64(2), hs.ErrorClassCounts[ErrorClassHTTP5xx])
	require.Len(t, hs.RecentErrors, 2)
}

func TestStore_OnComplete_ClearsRetryMetadata(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	ctx := context.Background()
	did := atmos.DID("did:plc:retryrecovered")
	next := time.Date(2026, 6, 23, 16, 0, 0, 0, time.UTC)

	require.NoError(t, s.OnDiscover(ctx, atmossync.ListReposEntry{DID: did, Active: true}))
	require.NoError(t, s.OnFail(ctx, did, "pds.example.com", errors.New("xrpc: HTTP 503: bootstrap unavailable"), 1))
	require.NoError(t, s.RecordRetryFailure(ctx, did, "pds.example.com", errors.New("xrpc: HTTP 503: unavailable"), next))
	require.NoError(t, s.OnComplete(ctx, did, "pds.example.com", &repo.Commit{DID: string(did), Rev: "rev-recovered"}))

	rs, err := s.readRepoStatus(did)
	require.NoError(t, err)
	require.Equal(t, StatusComplete, rs.Backfill.Status)
	require.Equal(t, 0, rs.Backfill.Attempts)
	require.Equal(t, 0, rs.Backfill.RetryCount)
	require.True(t, rs.Backfill.NextAttemptAt.IsZero())
	require.Empty(t, rs.Backfill.LastError)
}

// TestStore_RetryHostMove_AdjustsAggregates locks in the fix for the
// per-host double-count: a repo that failed on host A and is then
// re-attributed to host B by a later retry (the relay 302'd to a
// different PDS) must decrement A's aggregates, not leave the DID
// counted under both hosts.
func TestStore_RetryHostMove_AdjustsAggregates(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	ctx := context.Background()
	did := atmos.DID("did:plc:retryhostmove")
	next := time.Date(2026, 6, 23, 16, 0, 0, 0, time.UTC)

	require.NoError(t, s.OnDiscover(ctx, atmossync.ListReposEntry{DID: did, Active: true}))
	// Initial failure attributes the DID to host A.
	require.NoError(t, s.OnFail(ctx, did, "pds-a.example.com", errors.New("xrpc: HTTP 503: unavailable"), 1))

	hsA, ok, err := loadHostStatus(s.db, "pds-a.example.com")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), hsA.Total)
	require.Equal(t, uint64(1), hsA.Failed)

	// A retry fails again, but the relay redirected to host B this time.
	require.NoError(t, s.RecordRetryFailure(ctx, did, "pds-b.example.com", errors.New("xrpc: HTTP 503: still unavailable"), next))

	// Host A must have been decremented; the DID is no longer counted there.
	hsA, ok, err = loadHostStatus(s.db, "pds-a.example.com")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(0), hsA.Total, "stale host bucket must be decremented on host move")
	require.Equal(t, uint64(0), hsA.Active)
	require.Equal(t, uint64(0), hsA.Failed)

	// Host B now owns the DID's failed aggregate.
	hsB, ok, err := loadHostStatus(s.db, "pds-b.example.com")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), hsB.Total)
	require.Equal(t, uint64(1), hsB.Active)
	require.Equal(t, uint64(1), hsB.Failed)

	// Now a successful retry on host C moves it complete.
	require.NoError(t, s.OnComplete(ctx, did, "pds-c.example.com", &repo.Commit{DID: string(did), Rev: "rev-ok"}))

	hsB, ok, err = loadHostStatus(s.db, "pds-b.example.com")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(0), hsB.Total, "host B decremented when retry succeeds on host C")
	require.Equal(t, uint64(0), hsB.Failed)

	hsC, ok, err := loadHostStatus(s.db, "pds-c.example.com")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), hsC.Total)
	require.Equal(t, uint64(1), hsC.Complete)
	require.Equal(t, uint64(0), hsC.Failed)

	// Global counts stay correct: exactly one repo, now complete.
	counts, ok, err := LoadCounts(s.db)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, Counts{Total: 1, Complete: 1}, counts)
}

func TestStore_MaintainsCounts(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	ctx := context.Background()

	done := atmos.DID("did:plc:done")
	recovered := atmos.DID("did:plc:recovered")
	failed := atmos.DID("did:plc:failed")

	require.NoError(t, s.OnDiscover(ctx, atmossync.ListReposEntry{DID: done, Active: true}))
	require.NoError(t, s.OnDiscover(ctx, atmossync.ListReposEntry{DID: recovered, Active: true}))
	require.NoError(t, s.OnDiscover(ctx, atmossync.ListReposEntry{DID: failed, Active: true}))

	counts, ok, err := LoadCounts(s.db)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, Counts{Total: 3, Discovered: 3}, counts)

	require.NoError(t, s.OnComplete(ctx, done, "", &repo.Commit{DID: string(done), Rev: "rev-done"}))
	require.NoError(t, s.OnFail(ctx, recovered, "", errors.New("temporary"), 1))
	require.NoError(t, s.OnFail(ctx, failed, "", errors.New("permanent"), 1))

	counts, ok, err = LoadCounts(s.db)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, Counts{Total: 3, Complete: 1, Failed: 2}, counts)

	require.NoError(t, s.OnComplete(ctx, recovered, "", &repo.Commit{DID: string(recovered), Rev: "rev-recovered"}))

	counts, ok, err = LoadCounts(s.db)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, Counts{Total: 3, Complete: 2, Failed: 1}, counts)
}

func TestStore_SeedsMissingCountsFromRows(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	ctx := context.Background()

	existing := atmos.DID("did:plc:existing")
	require.NoError(t, s.putRepoStatus(existing, &RepoStatus{
		Backfill: RepoBackfillStatus{Status: StatusComplete},
		Active:   true,
	}))

	next := atmos.DID("did:plc:next")
	require.NoError(t, s.OnDiscover(ctx, atmossync.ListReposEntry{DID: next, Active: true}))

	counts, ok, err := LoadCounts(s.db)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, Counts{Total: 2, Discovered: 1, Complete: 1}, counts)
}

func TestStore_HostAggregates_FailThenComplete(t *testing.T) {
	t.Parallel()
	st, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	bs := NewStore(st, nil)
	ctx := context.Background()
	did := atmos.DID("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")
	require.NoError(t, bs.OnDiscover(ctx, atmossync.ListReposEntry{DID: did, Active: true}))
	require.NoError(t, bs.recordIdentityResolution(ctx, did, IdentityResolution{
		Handle: "alice.test",
		PDS:    "https://pds.example.com",
		Host:   "pds.example.com",
	}))

	require.NoError(t, bs.OnFail(ctx, did, "", errors.New("xrpc: HTTP 503: unavailable"), 3))
	hs, ok, err := loadHostStatus(st, "pds.example.com")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), hs.Total)
	require.Equal(t, uint64(1), hs.Active)
	require.Equal(t, uint64(0), hs.NotStarted)
	require.Equal(t, uint64(0), hs.Complete)
	require.Equal(t, uint64(1), hs.Failed)
	require.Equal(t, ErrorClassHTTP5xx, hs.LatestErrorClass)
	require.Len(t, hs.RecentErrors, 1)

	require.NoError(t, bs.OnComplete(ctx, did, "", &repo.Commit{Rev: "rev1"}))
	hs, ok, err = loadHostStatus(st, "pds.example.com")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), hs.Total)
	require.Equal(t, uint64(1), hs.Active)
	require.Equal(t, uint64(0), hs.Failed)
	require.Equal(t, uint64(1), hs.Complete)
}

func TestStore_HostAggregates_ActiveFlip(t *testing.T) {
	t.Parallel()
	st, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	bs := NewStore(st, nil)
	ctx := context.Background()
	did := atmos.DID("did:plc:bbbbbbbbbbbbbbbbbbbbbbbb")
	require.NoError(t, bs.OnDiscover(ctx, atmossync.ListReposEntry{DID: did, Active: true}))
	require.NoError(t, bs.recordIdentityResolution(ctx, did, IdentityResolution{
		PDS:  "https://pds.example.com",
		Host: "pds.example.com",
	}))

	require.NoError(t, bs.OnUpdate(ctx, atmossync.ListReposEntry{DID: did, Active: false}))
	hs, ok, err := loadHostStatus(st, "pds.example.com")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), hs.Total)
	require.Equal(t, uint64(0), hs.Active)
}

func TestStore_StaleActiveFlipCannotRegressCompletedStatus(t *testing.T) {
	t.Parallel()

	st, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	bs := NewStore(st, nil)
	ctx := context.Background()
	did := atmos.DID("did:plc:staleactiveflip")
	require.NoError(t, bs.OnDiscover(ctx, atmossync.ListReposEntry{DID: did, Active: true}))

	cb := NewCompletionBatcher(bs, nil)
	cb.RecordWatermark(did, 41, true)
	require.NoError(t, cb.QueueComplete(ctx, did, "", &repo.Commit{DID: string(did), Rev: "rev-complete"}))

	b := st.NewBatch()
	afterCommit, afterDone, err := cb.StageDurable(ctx, b, 42, false, nil)
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

	require.NoError(t, bs.updateRepoActive(did, false))

	rs, err := bs.readRepoStatus(did)
	require.NoError(t, err)
	require.Equal(t, StatusComplete, rs.Backfill.Status)
	require.Equal(t, "rev-complete", rs.Backfill.Rev)
	require.False(t, rs.Active)

	counts, ok, err := LoadCounts(st)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, Counts{Total: 1, Complete: 1}, counts)
}

func TestStore_HandleIndex_DeclaredHandleChange(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	ctx := context.Background()
	did := atmos.DID("did:plc:handlechange")

	require.NoError(t, s.OnDiscover(ctx, atmossync.ListReposEntry{DID: did, Active: true}))
	require.NoError(t, s.recordIdentityResolution(ctx, did, IdentityResolution{
		Handle: "alice.test",
		PDS:    "https://pds.example.com",
		Host:   "pds.example.com",
	}))

	got, ok, err := lookupDIDByHandle(s.db, "ALICE.TEST")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, did, got)

	require.NoError(t, s.recordIdentityResolution(ctx, did, IdentityResolution{
		Handle: "bob.test",
		PDS:    "https://pds.example.com",
		Host:   "pds.example.com",
	}))

	_, ok, err = lookupDIDByHandle(s.db, "alice.test")
	require.NoError(t, err)
	require.False(t, ok)

	got, ok, err = lookupDIDByHandle(s.db, "bob.test")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, did, got)
}

func TestStore_HandleIndex_HandleChangePreservesLaterOwner(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	ctx := context.Background()
	didA := atmos.DID("did:plc:handlechangea")
	didB := atmos.DID("did:plc:handlechangeb")

	require.NoError(t, s.OnDiscover(ctx, atmossync.ListReposEntry{DID: didA, Active: true}))
	require.NoError(t, s.OnDiscover(ctx, atmossync.ListReposEntry{DID: didB, Active: true}))

	require.NoError(t, s.recordIdentityResolution(ctx, didA, IdentityResolution{
		Handle: "alice.test",
		PDS:    "https://pds-a.example.com",
		Host:   "pds-a.example.com",
	}))
	require.NoError(t, s.recordIdentityResolution(ctx, didB, IdentityResolution{
		Handle: "alice.test",
		PDS:    "https://pds-b.example.com",
		Host:   "pds-b.example.com",
	}))
	require.NoError(t, s.recordIdentityResolution(ctx, didA, IdentityResolution{
		Handle: "bob.test",
		PDS:    "https://pds-a.example.com",
		Host:   "pds-a.example.com",
	}))

	got, ok, err := lookupDIDByHandle(s.db, "alice.test")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, didB, got)

	got, ok, err = lookupDIDByHandle(s.db, "bob.test")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, didA, got)
}

func TestStore_HostMove_AdjustsOldAndNewAggregates(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	ctx := context.Background()
	did := atmos.DID("did:plc:hostmove")

	require.NoError(t, s.OnDiscover(ctx, atmossync.ListReposEntry{DID: did, Active: true}))
	require.NoError(t, s.recordIdentityResolution(ctx, did, IdentityResolution{
		PDS:  "https://pds-a.example.com",
		Host: "pds-a.example.com",
	}))
	require.NoError(t, s.recordIdentityResolution(ctx, did, IdentityResolution{
		PDS:  "https://pds-b.example.com",
		Host: "pds-b.example.com",
	}))

	oldHost, ok, err := loadHostStatus(s.db, "pds-a.example.com")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(0), oldHost.Total)
	require.Equal(t, uint64(0), oldHost.Active)
	require.Equal(t, uint64(0), oldHost.NotStarted)

	newHost, ok, err := loadHostStatus(s.db, "pds-b.example.com")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), newHost.Total)
	require.Equal(t, uint64(1), newHost.Active)
	require.Equal(t, uint64(1), newHost.NotStarted)
}

func TestStore_HostAggregates_LatestFiveFailureSamples(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	ctx := context.Background()
	did := atmos.DID("did:plc:samplefailures")

	require.NoError(t, s.OnDiscover(ctx, atmossync.ListReposEntry{DID: did, Active: true}))
	require.NoError(t, s.recordIdentityResolution(ctx, did, IdentityResolution{
		PDS:  "https://pds.example.com",
		Host: "pds.example.com",
	}))

	for i := range 7 {
		require.NoError(t, s.OnFail(ctx, did, "", errors.New("xrpc: HTTP 503: unavailable sample "+string(rune('0'+i))), i+1))
	}

	hs, ok, err := loadHostStatus(s.db, "pds.example.com")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), hs.Total)
	require.Equal(t, uint64(1), hs.Failed)
	require.Equal(t, uint64(7), hs.ErrorClassCounts[ErrorClassHTTP5xx])
	require.Equal(t, ErrorClassHTTP5xx, hs.LatestErrorClass)
	require.Contains(t, hs.LatestError, "sample 6")
	require.Len(t, hs.RecentErrors, 5)
	require.Contains(t, hs.RecentErrors[0].Error, "sample 6")
	require.Contains(t, hs.RecentErrors[4].Error, "sample 2")
}
