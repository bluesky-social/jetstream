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
