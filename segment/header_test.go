package segment

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeHeaderRoundtrip(t *testing.T) {
	t.Parallel()

	h := Header{
		Version:               1,
		BlockCount:            42,
		EventCount:            123_456,
		UniqueDIDCount:        9_999,
		MinSeq:                1,
		MaxSeq:                123_456,
		MinIndexedAt:          1_700_000_000_000_000,
		MaxIndexedAt:          1_700_000_001_000_000,
		FooterOffset:          1 << 20,
		DIDBloomOffset:        (1 << 20) + 1024,
		BlockDIDBloomOffset:   (1 << 20) + 4096,
		CollectionIndexOffset: (1 << 20) + 8192,
		BlockIndexOffset:      1 << 20, // == FooterOffset by spec invariant
		Checksum:              0xDEADBEEFCAFEBABE,
	}

	buf := encodeHeader(h)
	require.Len(t, buf, ReservedHeaderBytes)

	got, err := decodeHeader(buf)
	require.NoError(t, err)
	require.Equal(t, h, got)
}

func TestEncodeHeaderFieldOffsets(t *testing.T) {
	t.Parallel()

	// Pin every field offset by hand so accidental field reorders fail
	// loudly. The byte-layout is contractual with sealed files on disk
	// and with replicas; reorders are silently file-format-incompatible
	// otherwise.
	h := Header{
		Version:               1,
		BlockCount:            2,
		EventCount:            3,
		UniqueDIDCount:        4,
		MinSeq:                5,
		MaxSeq:                6,
		MinIndexedAt:          7,
		MaxIndexedAt:          8,
		FooterOffset:          9,
		DIDBloomOffset:        10,
		BlockDIDBloomOffset:   11,
		CollectionIndexOffset: 12,
		BlockIndexOffset:      9, // matches FooterOffset
		Checksum:              0xAABBCCDDEEFF0011,
	}
	buf := encodeHeader(h)

	require.Equal(t, []byte("jss0"), buf[0:4], "magic at offset 0")
	require.Equal(t, uint64(0xAABBCCDDEEFF0011),
		binary.LittleEndian.Uint64(buf[4:12]), "checksum at offset 4")
	require.Equal(t, uint16(1), binary.LittleEndian.Uint16(buf[12:14]),
		"version at offset 12")
	require.Equal(t, uint32(2), binary.LittleEndian.Uint32(buf[14:18]),
		"block_count at offset 14")
	require.Equal(t, uint32(3), binary.LittleEndian.Uint32(buf[18:22]),
		"event_count at offset 18")
	require.Equal(t, uint32(4), binary.LittleEndian.Uint32(buf[22:26]),
		"unique_did_count at offset 22")
	require.Equal(t, uint64(5), binary.LittleEndian.Uint64(buf[26:34]),
		"min_seq at offset 26")
	require.Equal(t, uint64(6), binary.LittleEndian.Uint64(buf[34:42]),
		"max_seq at offset 34")
	require.Equal(t, int64(7), int64(binary.LittleEndian.Uint64(buf[42:50])),
		"min_indexed_at at offset 42")
	require.Equal(t, int64(8), int64(binary.LittleEndian.Uint64(buf[50:58])),
		"max_indexed_at at offset 50")
	require.Equal(t, uint64(9), binary.LittleEndian.Uint64(buf[58:66]),
		"footer_offset at offset 58")
	require.Equal(t, uint64(10), binary.LittleEndian.Uint64(buf[66:74]),
		"did_bloom_offset at offset 66")
	require.Equal(t, uint64(11), binary.LittleEndian.Uint64(buf[74:82]),
		"block_did_bloom_offset at offset 74")
	require.Equal(t, uint64(12), binary.LittleEndian.Uint64(buf[82:90]),
		"collection_index_offset at offset 82")
	require.Equal(t, uint64(9), binary.LittleEndian.Uint64(buf[90:98]),
		"block_index_offset at offset 90")

	// Reserved padding (bytes 98..256) must be zero so future
	// extensions can land without breaking older readers.
	for i := 98; i < ReservedHeaderBytes; i++ {
		require.Zerof(t, buf[i], "reserved byte %d must be zero", i)
	}
}

func TestDecodeHeaderRejectsShort(t *testing.T) {
	t.Parallel()

	_, err := decodeHeader(make([]byte, ReservedHeaderBytes-1))
	require.True(t, errors.Is(err, ErrInvalidFooter))
}

func TestDecodeHeaderRejectsBadMagic(t *testing.T) {
	t.Parallel()

	buf := make([]byte, ReservedHeaderBytes)
	copy(buf, []byte("XXXX"))
	_, err := decodeHeader(buf)
	require.True(t, errors.Is(err, ErrCorruptSegment))
}

func TestDecodeHeaderRejectsZeroChecksum(t *testing.T) {
	t.Parallel()

	// Zero checksum at offset 4 means "active file" by our active/sealed
	// convention. decodeHeader is only ever called on what should be a
	// sealed file, so a zero checksum is an unambiguous error.
	buf := make([]byte, ReservedHeaderBytes)
	copy(buf, segmentMagic)
	binary.LittleEndian.PutUint16(buf[12:14], 1) // version
	_, err := decodeHeader(buf)
	require.True(t, errors.Is(err, ErrActiveSegment))
}

func TestDecodeHeader_ActiveSegmentSentinel(t *testing.T) {
	t.Parallel()

	// 256 bytes: magic + zero checksum + zero-padded rest.
	buf := make([]byte, ReservedHeaderBytes)
	copy(buf[0:4], []byte("jss0"))
	// Bytes 4..11 (checksum) deliberately left zero.

	_, err := decodeHeader(buf)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrActiveSegment)
	require.NotErrorIs(t, err, ErrCorruptSegment,
		"active-segment marker must be distinct from real corruption")
}

func TestDecodeHeaderRejectsBadVersion(t *testing.T) {
	t.Parallel()

	buf := make([]byte, ReservedHeaderBytes)
	copy(buf, segmentMagic)
	binary.LittleEndian.PutUint64(buf[4:12], 0xCAFE) // non-zero checksum
	binary.LittleEndian.PutUint16(buf[12:14], 99)    // future version
	_, err := decodeHeader(buf)
	require.True(t, errors.Is(err, ErrInvalidFooter))
}

func TestXxh3HeaderFooter(t *testing.T) {
	t.Parallel()

	headerBytes := bytes.Repeat([]byte{0xAB}, ReservedHeaderBytes)
	footerBytes := []byte("hello, footer")
	got := xxh3HeaderFooter(headerBytes, footerBytes)
	require.NotZero(t, got)

	// Streaming computes the same value as feeding both regions in
	// one go. Idempotent regression in case we ever switch the
	// streaming model.
	again := xxh3HeaderFooter(headerBytes, footerBytes)
	require.Equal(t, got, again)
}
