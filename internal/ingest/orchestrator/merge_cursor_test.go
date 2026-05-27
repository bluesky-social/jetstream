package orchestrator

import (
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/stretchr/testify/require"
)

func TestMergeCursor_AbsentReadsZero(t *testing.T) {
	t.Parallel()
	st := newOrchestratorTestStore(t)
	got, err := loadMergeCursor(st)
	require.NoError(t, err)
	require.Equal(t, uint64(0), got)
}

func TestMergeCursor_RoundTripViaCommit(t *testing.T) {
	t.Parallel()
	st := newOrchestratorTestStore(t)
	require.NoError(t, st.Set(backfill.RepoKey("did:plc:a"), mustEncodeStatus(t, &backfill.RepoStatus{
		Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete, Rev: "rev-old"},
		Rev:      "rev-old",
	}), store.SyncWrites))

	cache := newRepoStatusLookup(st, nil)
	_, err := cache.get("did:plc:a")
	require.NoError(t, err)

	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	require.NoError(t, commitSourceComplete(st, cache, 5, map[string]string{"did:plc:a": "rev-new"}, now))

	got, err := loadMergeCursor(st)
	require.NoError(t, err)
	require.Equal(t, uint64(5), got)

	val, closer, err := st.Get(backfill.RepoKey("did:plc:a"))
	require.NoError(t, err)
	defer func() { _ = closer.Close() }()
	rs2, err := backfill.DecodeRepoStatus(val)
	require.NoError(t, err)
	require.Equal(t, "rev-new", rs2.Rev)
	require.Equal(t, "rev-old", rs2.Backfill.Rev) // immutable
	require.Equal(t, now, rs2.UpdatedAt)

	cached, err := cache.get("did:plc:a")
	require.NoError(t, err)
	require.Equal(t, "rev-new", cached.Rev)
}

func TestMergeCursor_NoRevsCommitsCursorOnly(t *testing.T) {
	t.Parallel()
	st := newOrchestratorTestStore(t)
	require.NoError(t, commitSourceComplete(st, newRepoStatusLookup(st, nil), 1, nil, time.Now()))
	got, err := loadMergeCursor(st)
	require.NoError(t, err)
	require.Equal(t, uint64(1), got)
}

func TestMergeCursor_Delete(t *testing.T) {
	t.Parallel()
	st := newOrchestratorTestStore(t)
	require.NoError(t, commitSourceComplete(st, newRepoStatusLookup(st, nil), 7, nil, time.Now()))
	require.NoError(t, deleteMergeCursor(st))
	got, err := loadMergeCursor(st)
	require.NoError(t, err)
	require.Equal(t, uint64(0), got)
}

// TestMergeCursor_SkipsRevUpdateForUnknownDID verifies that
// commitSourceComplete does NOT write a repo/<did> row for a DID
// where the cache returned nil (no pre-existing pebble row).
//
// Writing such a row would produce an invalid RepoStatus with
// Backfill.Status="" (zero value, not in the Status enum).
// backfill.Store.Lookup would then error on the row, and the
// steady-state retry path would never pick the DID up. The post-
// merge discovery step (§4.7) is the correct path for these DIDs.
func TestMergeCursor_SkipsRevUpdateForUnknownDID(t *testing.T) {
	t.Parallel()
	st := newOrchestratorTestStore(t)

	cache := newRepoStatusLookup(st, nil)
	// Cache hasn't seen this DID; get returns nil.
	rs, err := cache.get("did:plc:unknown")
	require.NoError(t, err)
	require.Nil(t, rs)

	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	require.NoError(t, commitSourceComplete(st, cache, 1,
		map[string]string{"did:plc:unknown": "rev-1"}, now))

	// Cursor advanced.
	got, err := loadMergeCursor(st)
	require.NoError(t, err)
	require.Equal(t, uint64(1), got)

	// repo/<did> row was NOT written for the unknown DID. (Writing
	// would have produced Backfill.Status="" — a corrupt row.)
	_, _, err = st.Get(backfill.RepoKey("did:plc:unknown"))
	require.ErrorIs(t, err, store.ErrNotFound)
}
