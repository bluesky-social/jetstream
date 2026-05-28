package segment

import (
	"encoding/binary"
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
		eventCounts: []uint32{100, 50, 25},
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
	require.Equal(t, idx.eventCounts, got.eventCounts)
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
	idx := collectionIndex{stringTable: nil, eventCounts: nil, blockBitmasks: nil}
	buf, err := encodeCollectionIndex(idx)
	require.NoError(t, err)
	got, err := decodeCollectionIndex(buf)
	require.NoError(t, err)
	require.Empty(t, got.stringTable)
	require.Empty(t, got.eventCounts)
	require.Empty(t, got.blockBitmasks)
}

func TestEncodecollectionIndexBitmaskBoundary(t *testing.T) {
	t.Parallel()

	// Test the bitmask byte boundary: 8 collections produce
	// bitmask_len == 1; 9 produce bitmask_len == 2.
	idx := collectionIndex{
		stringTable: []string{"a", "b", "c", "d", "e", "f", "g", "h"},
		eventCounts: []uint32{1, 2, 3, 4, 5, 6, 7, 8},
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
		eventCounts:   append([]uint32(nil), idx.eventCounts...),
		blockBitmasks: [][]uint32{{8}},
	}
	idx2.stringTable = append(idx2.stringTable, "i")
	idx2.eventCounts = append(idx2.eventCounts, 9)
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
		eventCounts:   []uint32{1},
		blockBitmasks: [][]uint32{{0}},
	}
	_, err := encodeCollectionIndex(idx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds")
}

// TestDecodecollectionIndexRejectsTruncatedEventCount exercises the
// decoder branch that fires when a row has its NSID length byte but
// the body ends before the 4-byte event count can be read. The
// happy-path encoder cannot produce this state (lockstep emit); we
// build a corrupt body by hand and re-pack it through the same zstd
// header path the decoder uses.
func TestDecodecollectionIndexRejectsTruncatedEventCount(t *testing.T) {
	t.Parallel()

	// One collection with len=1, but no count or NSID bytes follow.
	// bitmask_len = ceil(1/8) = 1; we still emit zero bitmask bytes
	// since the row read fails before bitmask iteration.
	body := []byte{0x01}

	bodyZstd := blockEncoder.EncodeAll(body, nil)
	header := make([]byte, collectionIndexHeaderSize)
	le := binary.LittleEndian
	le.PutUint32(header[0:4], 1)  // collection_count
	le.PutUint32(header[4:8], 0)  // block_count
	le.PutUint32(header[8:12], 1) // bitmask_len = ceil(1/8)
	le.PutUint32(header[12:16], uint32(len(body)))

	_, err := decodeCollectionIndex(append(header, bodyZstd...))
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidFooter))
	require.Contains(t, err.Error(), "truncated event count")
}

func TestEncodecollectionIndexRejectsLengthMismatch(t *testing.T) {
	t.Parallel()

	idx := collectionIndex{
		stringTable:   []string{"a", "b"},
		eventCounts:   []uint32{1}, // intentionally one short
		blockBitmasks: [][]uint32{{0, 1}},
	}
	_, err := encodeCollectionIndex(idx)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidFooter))
}
