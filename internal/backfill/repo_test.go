package backfill

import (
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/jcalabro/atmos"
	"github.com/stretchr/testify/require"
)

// newTestStore returns a *store.Store rooted in t.TempDir(). Closing
// is scheduled via t.Cleanup so individual tests don't have to
// remember to defer it.
func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, s.Close())
	})
	return s
}

// TestRepoStatus_RoundTrip covers the basic Put -> Get -> Has path
// for the per-DID rows. We deliberately exercise every field so a
// future spec tweak (e.g. renaming a JSON tag) surfaces as a test
// failure rather than as a silent on-disk format drift.
func TestRepoStatus_RoundTrip(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	did := atmos.DID("did:plc:abc123")
	now := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	want := RepoStatus{
		Backfill: RepoBackfillStatus{
			Status:      StatusComplete,
			Rev:         "3l3qo2vutsw2b",
			Attempts:    2,
			LastError:   "",
			StartedAt:   now.Add(-time.Hour),
			CompletedAt: now,
		},
		PDS:         "https://shimeji.us-east.host.bsky.network",
		Rev:         "3l3qo2vuyyy2b",
		UpdatedAt:   now,
		RecordCount: 42,
		TotalBytes:  4242,
	}

	require.NoError(t, PutRepoStatus(s, did, want))

	got, ok, err := GetRepoStatus(s, did)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, want, got)

	exists, err := HasRepo(s, did)
	require.NoError(t, err)
	require.True(t, exists)
}

// TestRepoStatus_GetMissingReturnsNotFound pins the (zero, false,
// nil) contract documented on GetRepoStatus. The seed step relies
// on this being a non-error path.
func TestRepoStatus_GetMissingReturnsNotFound(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	got, ok, err := GetRepoStatus(s, atmos.DID("did:plc:missing"))
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, RepoStatus{}, got)

	exists, err := HasRepo(s, atmos.DID("did:plc:missing"))
	require.NoError(t, err)
	require.False(t, exists)
}

// TestCountRepos_IgnoresOtherKeyspaces guards against future
// keyspace additions (account/, sync/, …) accidentally bleeding
// into the repo/<did> count. We poke a sibling key directly via
// the embedded pebble db and verify CountRepos still answers
// correctly.
func TestCountRepos_IgnoresOtherKeyspaces(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	for _, raw := range []string{"did:plc:aaa", "did:plc:bbb", "did:plc:ccc"} {
		require.NoError(t, PutRepoStatus(s, atmos.DID(raw), RepoStatus{
			Backfill: RepoBackfillStatus{Status: StatusNotStarted},
		}))
	}

	// A sibling keyspace key that must be ignored. We poke the
	// underlying pebble db directly because backfill's API only
	// covers repo/<did>; this is the same kind of cross-keyspace
	// write the live ingest path will eventually do.
	require.NoError(t, s.Set([]byte("relay/cursor"), []byte("123"), nil))

	n, err := CountRepos(s)
	require.NoError(t, err)
	require.Equal(t, int64(3), n)
}
