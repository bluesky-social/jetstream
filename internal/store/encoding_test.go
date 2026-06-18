package store_test

import (
	"encoding/binary"
	"testing"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestGetUint64LE_Missing(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	got, ok, err := st.GetUint64LE("k/missing")
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, uint64(0), got)
}

func TestGetUint64LE_RoundTrip(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)

	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], 0xDEADBEEFCAFEBABE)
	require.NoError(t, st.Set([]byte("k/v"), buf[:], store.SyncWrites))

	got, ok, err := st.GetUint64LE("k/v")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(0xDEADBEEFCAFEBABE), got)
}

func TestGetUint64LE_WrongLength(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	require.NoError(t, st.Set([]byte("k/v"), []byte{0x01, 0x02}, store.SyncWrites))

	_, _, err := st.GetUint64LE("k/v")
	require.Error(t, err)
}

func TestPrefixUpperBound(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want string
		nil  bool
	}{
		{"repo/", "repo0", false}, // '/'+1 == '0' in ASCII
		{"a", "b", false},
		{"foo\xff", "fop", false}, // carry across one byte
		{"\xff\xff", "", true},    // all-0xFF → nil
		{"", "", true},            // empty prefix → nil (no byte to increment)
	}
	for _, c := range cases {
		got := store.PrefixUpperBound([]byte(c.in))
		if c.nil {
			require.Nil(t, got, "input %q expected nil", c.in)
			continue
		}
		require.Equal(t, c.want, string(got), "input %q", c.in)
	}
}

func TestVersionedUint64LE_RoundTrip(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	require.NoError(t, s.SetVersionedUint64LE("merge/test", 0x42, 12345))

	got, ok, err := s.GetVersionedUint64LE("merge/test", 0x42)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(12345), got)
}

func TestVersionedUint64LE_AbsentKey(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	got, ok, err := s.GetVersionedUint64LE("merge/missing", 0x42)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, uint64(0), got)
}

func TestVersionedUint64LE_VersionMismatch(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	require.NoError(t, s.SetVersionedUint64LE("merge/test", 0x01, 7))

	_, _, err := s.GetVersionedUint64LE("merge/test", 0x02)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown version")
}

func TestVersionedUint64LE_WrongLength(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	// Stash a too-short value directly.
	require.NoError(t, s.Set([]byte("merge/test"), []byte{0x01, 0x00, 0x00}, store.SyncWrites))

	_, _, err := s.GetVersionedUint64LE("merge/test", 0x01)
	require.Error(t, err)
	require.Contains(t, err.Error(), "wrong length")
}
