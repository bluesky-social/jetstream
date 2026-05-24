package live

import (
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(t.TempDir(), nil)
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

// TestLoadUpstreamCursor_RejectsUnknownVersion pins the strict version
// check on read. A future writer that bumps the version (e.g. to
// embed a relay generation) must surface as an explicit error here
// rather than be misinterpreted as a v1 payload — the byte layout
// after the version byte is by definition undefined for v != 1.
func TestLoadUpstreamCursor_RejectsUnknownVersion(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)

	// Correct length (9 bytes), unknown version byte. Without the
	// strict check the seven payload bytes after the version would be
	// silently casted as a uint64.
	bogus := []byte{0xFF, 0, 0, 0, 0, 0, 0, 0, 0}
	require.NoError(t, st.Set([]byte("relay/cursor"), bogus, store.SyncWrites))

	_, err := LoadUpstreamCursor(st, "relay/cursor")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown version")
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

	// Valid v1 prefix + maximally-corrupt uint64. Reading the payload
	// as int64 would silently produce -1.
	corrupt := []byte{cursorV1, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	require.NoError(t, st.Set([]byte("relay/cursor"), corrupt, store.SyncWrites))

	_, err := LoadUpstreamCursor(st, "relay/cursor")
	require.Error(t, err)
	require.Contains(t, err.Error(), "negative")
}

// TestSaveUpstreamCursor_WritesV1Format pins the on-disk shape: a
// successful Save followed by a raw pebble Get must yield exactly
// [version=0x01][8B LE uint64]. This is a format-stability assertion
// — if anyone changes the layout, this test forces them to look at
// it.
func TestSaveUpstreamCursor_WritesV1Format(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	require.NoError(t, SaveUpstreamCursor(st, "relay/cursor", 0x0102030405060708))

	val, closer, err := st.Get([]byte("relay/cursor"))
	require.NoError(t, err)
	defer func() { _ = closer.Close() }()

	want := []byte{cursorV1, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}
	require.Equal(t, want, val)
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
