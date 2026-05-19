package segment

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodecollectionIndexRoundtrip(t *testing.T) {
	t.Parallel()

	idx := collectionIndex{
		stringTable: []string{
			"app.bsky.feed.post",
			"app.bsky.feed.like",
			"app.bsky.graph.follow",
		},
		blockBitmasks: [][]uint32{
			{0, 1}, // block 0 has post + like
			{0, 2}, // block 1 has post + follow
			{1, 2}, // block 2 has like + follow
		},
	}

	buf, err := encodeCollectionIndex(idx)
	require.NoError(t, err)

	got, err := decodeCollectionIndex(buf)
	require.NoError(t, err)
	require.Equal(t, idx.stringTable, got.stringTable)
	require.Len(t, got.blockBitmasks, len(idx.blockBitmasks))
	for i := range idx.blockBitmasks {
		require.ElementsMatch(t, idx.blockBitmasks[i], got.blockBitmasks[i],
			"block %d", i)
	}
}

func TestEncodecollectionIndexEmpty(t *testing.T) {
	t.Parallel()

	// Empty segment is meaningless for sealing in production, but the
	// encoder must not panic on it. Round-trips to an equivalent empty
	// index.
	idx := collectionIndex{stringTable: nil, blockBitmasks: nil}
	buf, err := encodeCollectionIndex(idx)
	require.NoError(t, err)
	got, err := decodeCollectionIndex(buf)
	require.NoError(t, err)
	require.Empty(t, got.stringTable)
	require.Empty(t, got.blockBitmasks)
}

func TestEncodecollectionIndexBitmaskBoundary(t *testing.T) {
	t.Parallel()

	// Test the bitmask byte boundary: 8 collections produce
	// bitmask_len == 1; 9 produce bitmask_len == 2.
	idx := collectionIndex{
		stringTable: []string{"a", "b", "c", "d", "e", "f", "g", "h"},
		blockBitmasks: [][]uint32{
			{0, 7},
			{1, 6},
		},
	}
	buf, err := encodeCollectionIndex(idx)
	require.NoError(t, err)

	got, err := decodeCollectionIndex(buf)
	require.NoError(t, err)
	require.Equal(t, idx.stringTable, got.stringTable)
	require.ElementsMatch(t, idx.blockBitmasks[0], got.blockBitmasks[0])
	require.ElementsMatch(t, idx.blockBitmasks[1], got.blockBitmasks[1])

	idx2 := collectionIndex{
		stringTable:   append([]string(nil), idx.stringTable...),
		blockBitmasks: [][]uint32{{8}},
	}
	idx2.stringTable = append(idx2.stringTable, "i")
	buf2, err := encodeCollectionIndex(idx2)
	require.NoError(t, err)
	got2, err := decodeCollectionIndex(buf2)
	require.NoError(t, err)
	require.ElementsMatch(t, []uint32{8}, got2.blockBitmasks[0])
}

func TestDecodecollectionIndexRejectsShortHeader(t *testing.T) {
	t.Parallel()

	_, err := decodeCollectionIndex([]byte{0x01, 0x02})
	require.True(t, errors.Is(err, ErrInvalidFooter))
}

func TestEncodecollectionIndexRejectsOversizedNSID(t *testing.T) {
	t.Parallel()

	long := make([]byte, 256)
	for i := range long {
		long[i] = 'a'
	}
	idx := collectionIndex{
		stringTable:   []string{string(long)},
		blockBitmasks: [][]uint32{{0}},
	}
	_, err := encodeCollectionIndex(idx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds")
}
