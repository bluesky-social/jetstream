package segment

import (
	"encoding/binary"
	"fmt"
)

// BlockInfo is one entry of the block index (DESIGN.md §3.1.2). It
// describes a block's location and bounds within a sealed segment
// file.
type BlockInfo struct {
	// Offset is the absolute file offset of the block's 8-byte
	// length prefix. The compressed frame begins at Offset + 8.
	Offset uint64

	// CompressedSize is the number of bytes in the zstd frame,
	// excluding the 8-byte length prefix.
	CompressedSize uint32

	// UncompressedSize is the size of the columnar block body
	// before compression.
	UncompressedSize uint32

	// EventCount is the number of events in this block.
	EventCount uint32

	// MinSeq, MaxSeq bound the seq column. MaxSeq >= MinSeq.
	MinSeq, MaxSeq uint64

	// MinWitnessedAt, MaxWitnessedAt bound the witnessed_at column in
	// unix microseconds. MaxWitnessedAt >= MinWitnessedAt. The type
	// matches Header.MinWitnessedAt/MaxWitnessedAt and the per-event
	// witnessed_at column in §3.2 of DESIGN.md.
	MinWitnessedAt, MaxWitnessedAt int64
}

// blockIndexEntrySize is the wire-format size of one block index
// entry: 8 + 4 + 4 + 4 + 8 + 8 + 8 + 8 = 52 bytes.
const blockIndexEntrySize = 52

// encodeBlockIndex serializes the given infos into a freshly-allocated
// slice of length len(infos) * blockIndexEntrySize. Entries are
// emitted in argument order (which is also the on-disk block order).
func encodeBlockIndex(infos []BlockInfo) []byte {
	buf := make([]byte, len(infos)*blockIndexEntrySize)
	le := binary.LittleEndian
	for i, info := range infos {
		off := i * blockIndexEntrySize
		le.PutUint64(buf[off+0:off+8], info.Offset)
		le.PutUint32(buf[off+8:off+12], info.CompressedSize)
		le.PutUint32(buf[off+12:off+16], info.UncompressedSize)
		le.PutUint32(buf[off+16:off+20], info.EventCount)
		le.PutUint64(buf[off+20:off+28], info.MinSeq)
		le.PutUint64(buf[off+28:off+36], info.MaxSeq)
		le.PutUint64(buf[off+36:off+44], uint64(info.MinWitnessedAt))
		le.PutUint64(buf[off+44:off+52], uint64(info.MaxWitnessedAt))
	}
	return buf
}

// decodeBlockIndex parses count entries out of buf. buf must be
// exactly count * blockIndexEntrySize bytes; mismatch is reported
// as ErrInvalidBlockIndex.
func decodeBlockIndex(buf []byte, count uint32) ([]BlockInfo, error) {
	want := int(count) * blockIndexEntrySize
	if len(buf) != want {
		return nil, fmt.Errorf("%w: have %d bytes, want %d (count=%d)",
			ErrInvalidBlockIndex, len(buf), want, count)
	}
	if count == 0 {
		return nil, nil
	}
	infos := make([]BlockInfo, count)
	le := binary.LittleEndian
	for i := range infos {
		off := i * blockIndexEntrySize
		infos[i] = BlockInfo{
			Offset:           le.Uint64(buf[off+0 : off+8]),
			CompressedSize:   le.Uint32(buf[off+8 : off+12]),
			UncompressedSize: le.Uint32(buf[off+12 : off+16]),
			EventCount:       le.Uint32(buf[off+16 : off+20]),
			MinSeq:           le.Uint64(buf[off+20 : off+28]),
			MaxSeq:           le.Uint64(buf[off+28 : off+36]),
			MinWitnessedAt:   int64(le.Uint64(buf[off+36 : off+44])),
			MaxWitnessedAt:   int64(le.Uint64(buf[off+44 : off+52])),
		}
	}
	return infos, nil
}
