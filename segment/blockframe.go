package segment

import (
	"encoding/binary"
	"fmt"
	"io"
)

// BlockFrameSection returns a reader for the raw, stored zstd frame for block
// idx. It reads and validates only that block's 52-byte footer index entry;
// the returned section excludes the 8-byte length prefix and does not allocate
// space proportional to the compressed frame.
//
// r is the fd for the sealed segment file; hdr is its fixed header as returned
// by ReadSealedHeader. Returns ErrBlockOutOfRange when idx is out of range.
// All offsets are validated against hdr.FooterOffset before constructing the
// section, so a corrupt/hostile block-index entry cannot drive an out-of-bounds
// read.
func BlockFrameSection(r io.ReaderAt, hdr Header, idx int) (*io.SectionReader, error) {
	frameStart, compressedSize, err := blockFrameRange(r, hdr, idx)
	if err != nil {
		return nil, err
	}
	return io.NewSectionReader(r, int64(frameStart), int64(compressedSize)), nil
}

// ReadBlockFrame reads the raw, stored zstd frame for block idx using only the
// already-read fixed header — no footer/bloom/collection parsing and no
// decompression. The returned bytes exclude the 8-byte length prefix, i.e. they
// are exactly the [block_len]byte frame the writer appended.
//
// r is the fd for the sealed segment file; hdr is its fixed header as returned
// by ReadSealedHeader. Returns ErrBlockOutOfRange when idx is out of range.
// All offsets are validated against hdr.FooterOffset before any read keyed off
// them, so a corrupt/hostile block-index entry cannot drive an out-of-bounds or
// oversized allocation/read.
func ReadBlockFrame(r io.ReaderAt, hdr Header, idx int) ([]byte, error) {
	frameStart, compressedSize, err := blockFrameRange(r, hdr, idx)
	if err != nil {
		return nil, err
	}

	frame := make([]byte, compressedSize)
	if _, err := r.ReadAt(frame, int64(frameStart)); err != nil {
		return nil, fmt.Errorf("segment: read block %d frame: %w", idx, err)
	}
	return frame, nil
}

func blockFrameRange(r io.ReaderAt, hdr Header, idx int) (uint64, uint32, error) {
	if idx < 0 || uint64(idx) >= uint64(hdr.BlockCount) {
		return 0, 0, fmt.Errorf("%w: idx %d, block_count %d",
			ErrBlockOutOfRange, idx, hdr.BlockCount)
	}
	if hdr.FooterOffset < uint64(ReservedHeaderBytes) {
		return 0, 0, fmt.Errorf("%w: footer_offset %d < reserved header",
			ErrInvalidFooter, hdr.FooterOffset)
	}
	if hdr.BlockIndexOffset != hdr.FooterOffset {
		return 0, 0, fmt.Errorf("%w: block_index_offset %d != footer_offset %d",
			ErrInvalidFooter, hdr.BlockIndexOffset, hdr.FooterOffset)
	}

	const maxInt64 = uint64(1<<63 - 1)
	if hdr.FooterOffset > maxInt64 {
		return 0, 0, fmt.Errorf("%w: footer_offset %d overflows int64",
			ErrInvalidFooter, hdr.FooterOffset)
	}
	entryDelta := uint64(idx) * blockIndexEntrySize
	if hdr.BlockIndexOffset > maxInt64 || entryDelta > maxInt64-hdr.BlockIndexOffset {
		return 0, 0, fmt.Errorf("%w: block %d index entry offset overflows int64",
			ErrInvalidFooter, idx)
	}

	entry := make([]byte, blockIndexEntrySize)
	entryOff := int64(hdr.BlockIndexOffset + entryDelta)
	if _, err := r.ReadAt(entry, entryOff); err != nil {
		return 0, 0, fmt.Errorf("segment: read block %d index entry: %w", idx, err)
	}
	le := binary.LittleEndian
	offset := le.Uint64(entry[0:8])
	compressedSize := le.Uint32(entry[8:12])

	// Validate the frame range lies within [ReservedHeaderBytes, FooterOffset),
	// mirroring validateBlockOffsets. end = offset + 8 (length prefix) + size.
	if offset > hdr.FooterOffset-8 || uint64(compressedSize) > hdr.FooterOffset-offset-8 {
		return 0, 0, fmt.Errorf("%w: block %d range overflows or exceeds footer",
			ErrInvalidBlockIndex, idx)
	}
	end := offset + 8 + uint64(compressedSize)
	if offset < uint64(ReservedHeaderBytes) || end > hdr.FooterOffset {
		return 0, 0, fmt.Errorf("%w: block %d range [%d, %d) outside [%d, %d)",
			ErrInvalidBlockIndex, idx, offset, end, ReservedHeaderBytes, hdr.FooterOffset)
	}
	frameStart := offset + 8
	if frameStart > maxInt64 || uint64(compressedSize) > maxInt64-frameStart {
		return 0, 0, fmt.Errorf("%w: block %d frame range overflows int64",
			ErrInvalidBlockIndex, idx)
	}
	return frameStart, compressedSize, nil
}

// DecodeBlockFrame decompresses and decodes a single raw block frame into its
// events. frame is exactly the zstd frame returned by ReadBlockFrame or by the
// getBlock XRPC endpoint (no 8-byte length prefix). This is the standard block
// decoder remote clients use on the bytes named by a snapshot plan.
//
// The decoder bounds decompressed size to guard against zstd bombs and
// validates every column length, so a corrupt or hostile frame yields an error
// rather than a panic or oversized allocation.
//
// Buffer-aliasing contract: the returned events alias an internal decompressed
// buffer for their string and payload columns. The buffer is private to this
// call (not shared across frames), but callers that retain fields beyond the
// events' lifetime should clone them.
func DecodeBlockFrame(frame []byte) ([]Event, error) {
	return decodeBlockCompressed(frame)
}
