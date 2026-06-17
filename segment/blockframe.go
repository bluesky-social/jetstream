package segment

import (
	"encoding/binary"
	"fmt"
	"io"
)

// ReadBlockFrame reads the raw, stored zstd frame for block idx using only the
// already-read fixed header — no footer/bloom/collection parsing and no
// decompression. The returned bytes exclude the 8-byte length prefix, i.e. they
// are exactly the [block_len]byte frame the writer appended.
//
// r is the fd for the sealed segment file; hdr is its fixed header as returned
// by ReadSealedHeader. Returns ErrBlockOutOfRange when idx is out of range.
//
// All offsets are validated against hdr.FooterOffset before any read keyed off
// them, so a corrupt/hostile block-index entry cannot drive an out-of-bounds or
// oversized allocation/read.
func ReadBlockFrame(r io.ReaderAt, hdr Header, idx int) ([]byte, error) {
	if idx < 0 || idx >= int(hdr.BlockCount) {
		return nil, fmt.Errorf("%w: idx %d, block_count %d",
			ErrBlockOutOfRange, idx, hdr.BlockCount)
	}
	if hdr.FooterOffset < uint64(ReservedHeaderBytes) {
		return nil, fmt.Errorf("%w: footer_offset %d < reserved header",
			ErrInvalidFooter, hdr.FooterOffset)
	}
	if hdr.BlockIndexOffset != hdr.FooterOffset {
		return nil, fmt.Errorf("%w: block_index_offset %d != footer_offset %d",
			ErrInvalidFooter, hdr.BlockIndexOffset, hdr.FooterOffset)
	}

	const maxInt64 = uint64(1<<63 - 1)
	entryDelta := uint64(idx) * blockIndexEntrySize
	if hdr.BlockIndexOffset > maxInt64 || entryDelta > maxInt64-hdr.BlockIndexOffset {
		return nil, fmt.Errorf("%w: block %d index entry offset overflows int64",
			ErrInvalidFooter, idx)
	}

	entry := make([]byte, blockIndexEntrySize)
	entryOff := int64(hdr.BlockIndexOffset + entryDelta)
	if _, err := r.ReadAt(entry, entryOff); err != nil {
		return nil, fmt.Errorf("segment: read block %d index entry: %w", idx, err)
	}
	le := binary.LittleEndian
	offset := le.Uint64(entry[0:8])
	compressedSize := le.Uint32(entry[8:12])

	// Validate the frame range lies within [ReservedHeaderBytes, FooterOffset),
	// mirroring validateBlockOffsets. end = offset + 8 (length prefix) + size.
	if uint64(compressedSize) > hdr.FooterOffset {
		return nil, fmt.Errorf("%w: block %d compressed_size %d > footer_offset %d",
			ErrInvalidBlockIndex, idx, compressedSize, hdr.FooterOffset)
	}
	if offset > hdr.FooterOffset-8 || uint64(compressedSize) > hdr.FooterOffset-offset-8 {
		return nil, fmt.Errorf("%w: block %d range overflows or exceeds footer",
			ErrInvalidBlockIndex, idx)
	}
	end := offset + 8 + uint64(compressedSize)
	if offset < uint64(ReservedHeaderBytes) || end > hdr.FooterOffset {
		return nil, fmt.Errorf("%w: block %d range [%d, %d) outside [%d, %d)",
			ErrInvalidBlockIndex, idx, offset, end, ReservedHeaderBytes, hdr.FooterOffset)
	}

	frame := make([]byte, compressedSize)
	if _, err := r.ReadAt(frame, int64(offset)+8); err != nil {
		return nil, fmt.Errorf("segment: read block %d frame: %w", idx, err)
	}
	return frame, nil
}
