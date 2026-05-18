package backfill

import (
	"context"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
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
