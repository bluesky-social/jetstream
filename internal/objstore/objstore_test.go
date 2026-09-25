package objstore_test

import (
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream/internal/objstore"
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
