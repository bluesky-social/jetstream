package segment

import (
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeBlockIndexRoundtrip(t *testing.T) {
	t.Parallel()

	infos := []BlockInfo{
		{
			Offset:           ReservedHeaderBytes,
			CompressedSize:   1024,
			UncompressedSize: 4096,
			EventCount:       16,
			MinSeq:           1,
			MaxSeq:           16,
			MinIndexedAt:     100,
			MaxIndexedAt:     1600,
		},
		{
			Offset:           ReservedHeaderBytes + 1024 + 8,
			CompressedSize:   2048,
			UncompressedSize: 8192,
			EventCount:       32,
			MinSeq:           17,
			MaxSeq:           48,
			MinIndexedAt:     1601,
			MaxIndexedAt:     4800,
		},
	}

	buf := encodeBlockIndex(infos)
	require.Len(t, buf, len(infos)*blockIndexEntrySize)

	got, err := decodeBlockIndex(buf, uint32(len(infos)))
	require.NoError(t, err)
	require.Equal(t, infos, got)
}

func TestEncodeBlockIndexEntryFieldOffsets(t *testing.T) {
	t.Parallel()

	// Pin field offsets within a single 52-byte entry. Same rationale
	// as the header field-offset test: silent reorder = silent file
	// incompatibility.
	info := BlockInfo{
		Offset:           0x0102030405060708,
		CompressedSize:   0x11121314,
		UncompressedSize: 0x21222324,
		EventCount:       0x31323334,
		MinSeq:           0x4142434445464748,
		MaxSeq:           0x5152535455565758,
		MinIndexedAt:     0x6162636465666768,
		MaxIndexedAt:     0x7172737475767778,
	}
	buf := encodeBlockIndex([]BlockInfo{info})
	require.Len(t, buf, blockIndexEntrySize)

	require.Equal(t, uint64(0x0102030405060708),
		binary.LittleEndian.Uint64(buf[0:8]))
	require.Equal(t, uint32(0x11121314),
		binary.LittleEndian.Uint32(buf[8:12]))
	require.Equal(t, uint32(0x21222324),
		binary.LittleEndian.Uint32(buf[12:16]))
	require.Equal(t, uint32(0x31323334),
		binary.LittleEndian.Uint32(buf[16:20]))
	require.Equal(t, uint64(0x4142434445464748),
		binary.LittleEndian.Uint64(buf[20:28]))
	require.Equal(t, uint64(0x5152535455565758),
		binary.LittleEndian.Uint64(buf[28:36]))
	require.Equal(t, uint64(0x6162636465666768),
		binary.LittleEndian.Uint64(buf[36:44]))
	require.Equal(t, uint64(0x7172737475767778),
		binary.LittleEndian.Uint64(buf[44:52]))
}

func TestDecodeBlockIndexRejectsLengthMismatch(t *testing.T) {
	t.Parallel()

	// One entry's worth of bytes, but caller asks for two.
	buf := make([]byte, blockIndexEntrySize)
	_, err := decodeBlockIndex(buf, 2)
	require.True(t, errors.Is(err, ErrInvalidBlockIndex))
}

func TestDecodeBlockIndexZeroEntries(t *testing.T) {
	t.Parallel()

	got, err := decodeBlockIndex(nil, 0)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestEncodeBlockIndexProperty(t *testing.T) {
	t.Parallel()

	r := rand.New(rand.NewPCG(1, 2))
	for range 200 {
		n := 1 + r.IntN(64)
		infos := make([]BlockInfo, n)
		for i := range infos {
			infos[i] = BlockInfo{
				Offset:           r.Uint64(),
				CompressedSize:   r.Uint32(),
				UncompressedSize: r.Uint32(),
				EventCount:       r.Uint32(),
				MinSeq:           r.Uint64(),
				MaxSeq:           r.Uint64(),
				MinIndexedAt:     int64(r.Uint64()),
				MaxIndexedAt:     int64(r.Uint64()),
			}
		}
		buf := encodeBlockIndex(infos)
		got, err := decodeBlockIndex(buf, uint32(n))
		require.NoError(t, err)
		require.Equal(t, infos, got)
	}
}
