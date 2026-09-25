package objstore_test

import (
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream/internal/objstore"
	"github.com/bluesky-social/jetstream/internal/objstore/memblob"
)

func TestVerify(t *testing.T) {
	t.Parallel()
	data := []byte("block bytes")
	sum := sha256.Sum256(data)

	require.NoError(t, objstore.Verify(data, int64(len(data)), sum))

	flipped := []byte("block bytez")
	require.ErrorIs(t, objstore.Verify(flipped, int64(len(data)), sum), objstore.ErrCorrupt, "same length, wrong hash")
	require.ErrorIs(t, objstore.Verify(data[:5], int64(len(data)), sum), objstore.ErrCorrupt, "truncated")
	require.ErrorIs(t, objstore.Verify(append(data, 0), int64(len(data)), sum), objstore.ErrCorrupt, "extended")
	require.ErrorIs(t, objstore.Verify(nil, int64(len(data)), sum), objstore.ErrCorrupt, "empty")
}

func TestRangeLen(t *testing.T) {
	t.Parallel()
	ok := []struct {
		size, off, n, want int64
	}{
		{100, 0, 100, 100},
		{100, 0, 1, 1},
		{100, 99, 1, 1},
		{100, 90, 50, 10},
		{100, 0, 1 << 62, 100},
	}
	for _, c := range ok {
		got, err := objstore.RangeLen(c.size, c.off, c.n)
		require.NoError(t, err, "%+v", c)
		require.Equal(t, c.want, got, "%+v", c)
	}

	bad := []struct{ size, off, n int64 }{
		{100, 100, 1},
		{100, -1, 1},
		{100, 0, 0},
		{100, 0, -1},
		{0, 0, 1},
	}
	for _, c := range bad {
		_, err := objstore.RangeLen(c.size, c.off, c.n)
		require.ErrorIs(t, err, objstore.ErrInvalidRange, "%+v", c)
	}
}

// TestKey pins the design §7.1 layout: <archive_id>/objects/<uuid>, both
// canonical UUIDs, with a fresh v4 UUID per upload attempt.
func TestKey(t *testing.T) {
	t.Parallel()
	archive := [16]byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10}
	key := [16]byte{15: 0xff}
	require.Equal(t,
		"01234567-89ab-cdef-fedc-ba9876543210/objects/00000000-0000-0000-0000-0000000000ff",
		objstore.Key(archive, key))

	a, b := objstore.NewUUID(), objstore.NewUUID()
	require.NotEqual(t, a, b)
	s := objstore.FormatUUID(a)
	require.Len(t, s, 36)
	require.Equal(t, byte('4'), s[14], "version nibble")
	require.Contains(t, "89ab", string(s[19]), "variant nibble")
}

// The probe succeeds on a working store, leaves nothing behind, and names
// the operation that failed on a broken one.
func TestProbe(t *testing.T) {
	t.Parallel()
	archive := objstore.NewUUID()
	prefix := objstore.FormatUUID(archive) + "/probe/"

	b := memblob.New()
	require.NoError(t, objstore.Probe(t.Context(), b, archive))
	require.Empty(t, b.Keys(), "the probe object is deleted")

	for _, tc := range []struct {
		op   memblob.Op
		kind memblob.FaultKind
		want string
	}{
		{memblob.OpPut, memblob.FaultError, "probe put"},
		{memblob.OpPut, memblob.FaultDropPut, "probe get"},
		{memblob.OpGet, memblob.FaultError, "probe get"},
		{memblob.OpGet, memblob.FaultWrongBytes, "differ"},
		{memblob.OpDelete, memblob.FaultError, "probe delete"},
	} {
		b := memblob.New(memblob.WithFaultInjector(&memblob.KeyPrefixFault{Prefix: prefix, Op: tc.op, Kind: tc.kind, Ordinal: 1}))
		err := objstore.Probe(t.Context(), b, archive)
		require.ErrorContains(t, err, tc.want, "%s %s", tc.op, tc.kind)
		require.Contains(t, err.Error(), prefix, "the error names the key")
	}
}
