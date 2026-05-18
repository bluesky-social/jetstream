package backfill

import (
	"context"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	"github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/stretchr/testify/require"
)

// newTestStore is the shared fixture for Store unit tests: open a
// fresh pebble in t.TempDir(), register cleanup, return the wrapped
// Store with no metrics.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	db, err := store.Open(t.TempDir())
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
		{"complete -> Complete", StatusComplete, atmosbackfill.StateComplete},
		{"failed -> Failed", StatusFailed, atmosbackfill.StateFailed},
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
// DESIGN.md §3.5 — both fields exist; Rev is the latest, Backfill.Rev
// is the rev at end of initial download).
func TestStore_OnComplete_WritesComplete(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	did := atmos.DID("did:plc:done")

	require.NoError(t, s.OnDiscover(context.Background(), atmossync.ListReposEntry{
		DID: did, Active: true,
	}))

	commit := &repo.Commit{DID: string(did), Rev: "rev-final"}
	require.NoError(t, s.OnComplete(context.Background(), did, commit))

	got, err := s.Lookup(context.Background(), did)
	require.NoError(t, err)
	require.Equal(t, atmosbackfill.StateComplete, got.State)

	rs, err := s.readRepoStatus(did)
	require.NoError(t, err)
	require.Equal(t, "rev-final", rs.Backfill.Rev)
	require.Equal(t, "rev-final", rs.Rev)
	require.False(t, rs.Backfill.CompletedAt.IsZero())
	require.False(t, rs.UpdatedAt.IsZero())
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
	require.NoError(t, s.OnComplete(context.Background(), did, commit))

	rs, err := s.readRepoStatus(did)
	require.NoError(t, err)
	require.Equal(t, int64(42), rs.RecordCount, "RMW must preserve RecordCount")
	require.Equal(t, StatusComplete, rs.Backfill.Status)
}
