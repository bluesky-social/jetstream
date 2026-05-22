package livestream

import (
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestLoadUpstreamCursor_Empty(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)

	got, err := LoadUpstreamCursor(st, "relay/cursor")
	require.NoError(t, err)
	require.Equal(t, int64(0), got)
}

func TestUpstreamCursor_RoundTrip(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)

	require.NoError(t, SaveUpstreamCursor(st, "relay/cursor", 12345))
	got, err := LoadUpstreamCursor(st, "relay/cursor")
	require.NoError(t, err)
	require.Equal(t, int64(12345), got)
}

func TestUpstreamCursor_DistinctKeys(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)

	require.NoError(t, SaveUpstreamCursor(st, "relay/cursor", 10))
	require.NoError(t, SaveUpstreamCursor(st, "replica/upstream_cursor", 20))

	a, err := LoadUpstreamCursor(st, "relay/cursor")
	require.NoError(t, err)
	require.Equal(t, int64(10), a)

	b, err := LoadUpstreamCursor(st, "replica/upstream_cursor")
	require.NoError(t, err)
	require.Equal(t, int64(20), b)
}

func TestLoadUpstreamCursor_RejectsCorruptValue(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	require.NoError(t, st.Set([]byte("relay/cursor"), []byte{0x01, 0x02, 0x03}, store.SyncWrites))

	_, err := LoadUpstreamCursor(st, "relay/cursor")
	require.Error(t, err)
	require.Contains(t, err.Error(), "wrong length")
}

// TestLoadUpstreamCursor_RejectsHighBitSet pins that a stored value
// whose high bit is set (corruption, or a future writer writing the
// full uint64 range — the cursor's on-disk shape mirrors uint64 seq
// counters elsewhere in the codebase) is rejected rather than
// silently casting to a negative int64.
//
// atmos's dial guards on `cursor > 0`, so a negative cursor would
// silently degrade to "start from live tail" and lose every
// historical event between the corrupt-but-meaningful seq and now.
// PRACTICES.md prefers a crash to a silent fallback.
func TestLoadUpstreamCursor_RejectsHighBitSet(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)

	// Write 0xFFFFFFFFFFFFFFFF — the maximally-corrupt uint64 — under
	// the cursor key. Reading as int64 would silently produce -1.
	corrupt := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	require.NoError(t, st.Set([]byte("relay/cursor"), corrupt, store.SyncWrites))

	_, err := LoadUpstreamCursor(st, "relay/cursor")
	require.Error(t, err)
	require.Contains(t, err.Error(), "negative")
}

// TestSaveUpstreamCursor_RejectsNegative pins the symmetric write-side
// guarantee: callers cannot persist a negative cursor that LoadUpstreamCursor
// would later refuse. This makes the invariant "stored cursor >= 0"
// hold by construction at every write site.
func TestSaveUpstreamCursor_RejectsNegative(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	err := SaveUpstreamCursor(st, "relay/cursor", -1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "negative")
}
