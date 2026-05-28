package segment

import (
	"encoding/binary"
	"fmt"
	"math"
)

// collectionIndex is the parsed form of the collection block index
// (DESIGN.md §3.1.4, spec §5.5). stringTable is the unique NSIDs in
// first-seen order; the index of each NSID in stringTable is its
// "collection ID". eventCounts[i] is the total number of events with
// collection-id i across the whole segment (parallel-indexed with
// stringTable). blockBitmasks[i] is the sorted, deduplicated set of
// collection IDs present in block i.
//
// On disk the per-collection rows are interleaved as
// (len:uint8, count:uint32 LE, nsid:[len]byte); we keep them as two
// parallel slices in memory because every consumer reads them by
// collection-id index. The bitmasks are stored as packed-byte bit
// arrays of size ceil(len(stringTable) / 8); we decode them into
// []uint32 of IDs here so callers don't have to do bit math.
type collectionIndex struct {
	stringTable   []string
	eventCounts   []uint32
	blockBitmasks [][]uint32
}

// collectionIndexHeaderSize is the uncompressed 16-byte header that
// precedes the zstd-compressed body.
const collectionIndexHeaderSize = 16

// encodeCollectionIndex serializes idx with the spec §5.5 wire layout.
// The body is zstd-compressed; the 16-byte header is uncompressed.
//
// Returns ErrFieldTooLong wrapped if any NSID exceeds the on-disk
// uint8 length column.
func encodeCollectionIndex(idx collectionIndex) ([]byte, error) {
	collectionCount := len(idx.stringTable)
	blockCount := len(idx.blockBitmasks)
	if collectionCount > math.MaxUint32 || blockCount > math.MaxUint32 {
		return nil, fmt.Errorf("%w: collection or block count overflows uint32",
			ErrInvalidFooter)
	}
	bitmaskLen := (collectionCount + 7) / 8

	// Build the uncompressed body: collection table (interleaved
	// len/count/nsid) + per-block bitmasks. The two parallel slices on
	// the in-memory struct are emitted in lockstep so a length mismatch
	// can't be expressed on the wire.
	if len(idx.eventCounts) != len(idx.stringTable) {
		return nil, fmt.Errorf(
			"%w: eventCounts len %d != stringTable len %d",
			ErrInvalidFooter, len(idx.eventCounts), len(idx.stringTable))
	}
	var body []byte
	var countBuf [4]byte
	le := binary.LittleEndian
	for i, nsid := range idx.stringTable {
		if len(nsid) > math.MaxUint8 {
			return nil, fmt.Errorf("%w: NSID %d exceeds %d bytes",
				ErrFieldTooLong, i, math.MaxUint8)
		}
		body = append(body, uint8(len(nsid)))
		le.PutUint32(countBuf[:], idx.eventCounts[i])
		body = append(body, countBuf[:]...)
		body = append(body, nsid...)
	}
	for blockIdx, ids := range idx.blockBitmasks {
		mask := make([]byte, bitmaskLen)
		for _, id := range ids {
			if int(id) >= collectionCount {
				return nil, fmt.Errorf(
					"%w: block %d references collection id %d (table has %d)",
					ErrInvalidFooter, blockIdx, id, collectionCount)
			}
			mask[id/8] |= 1 << (id % 8)
		}
		body = append(body, mask...)
	}

	bodyZstd := blockEncoder.EncodeAll(body, nil)

	header := make([]byte, collectionIndexHeaderSize)
	le.PutUint32(header[0:4], uint32(collectionCount))
	le.PutUint32(header[4:8], uint32(blockCount))
	le.PutUint32(header[8:12], uint32(bitmaskLen))
	le.PutUint32(header[12:16], uint32(len(body)))

	return append(header, bodyZstd...), nil
}

// decodeCollectionIndex parses the on-disk bytes into a collectionIndex.
func decodeCollectionIndex(buf []byte) (collectionIndex, error) {
	if len(buf) < collectionIndexHeaderSize {
		return collectionIndex{}, fmt.Errorf(
			"%w: collection index header is %d bytes, want >=%d",
			ErrInvalidFooter, len(buf), collectionIndexHeaderSize)
	}

	le := binary.LittleEndian
	collectionCount := le.Uint32(buf[0:4])
	blockCount := le.Uint32(buf[4:8])
	bitmaskLen := le.Uint32(buf[8:12])
	uncompressedSize := le.Uint32(buf[12:16])

	// Bound counts up front so the make() calls below cannot be driven
	// to multi-GB allocations by a hostile or corrupt header.
	if collectionCount > maxCollectionCountLimit {
		return collectionIndex{}, fmt.Errorf(
			"%w: collection_count %d exceeds limit %d",
			ErrInvalidFooter, collectionCount, maxCollectionCountLimit)
	}
	if blockCount > maxBlockCountLimit {
		return collectionIndex{}, fmt.Errorf(
			"%w: block_count %d exceeds limit %d",
			ErrInvalidFooter, blockCount, maxBlockCountLimit)
	}
	if uint64(uncompressedSize) > maxDecodedBlockBytes {
		return collectionIndex{}, fmt.Errorf(
			"%w: uncompressed_size %d exceeds limit %d",
			ErrInvalidFooter, uncompressedSize, maxDecodedBlockBytes)
	}

	wantBitmaskLen := (collectionCount + 7) / 8
	if bitmaskLen != wantBitmaskLen {
		return collectionIndex{}, fmt.Errorf(
			"%w: bitmask_len %d, want %d for collection_count %d",
			ErrInvalidFooter, bitmaskLen, wantBitmaskLen, collectionCount)
	}

	body, err := blockDecoder.DecodeAll(buf[collectionIndexHeaderSize:], nil)
	if err != nil {
		return collectionIndex{}, fmt.Errorf("segment: collection index decompress: %w", err)
	}
	if uint32(len(body)) != uncompressedSize {
		return collectionIndex{}, fmt.Errorf(
			"%w: collection body decompressed to %d bytes, header claimed %d",
			ErrInvalidFooter, len(body), uncompressedSize)
	}

	off := 0
	stringTable := make([]string, collectionCount)
	eventCounts := make([]uint32, collectionCount)
	for i := range stringTable {
		if off+1 > len(body) {
			return collectionIndex{}, fmt.Errorf(
				"%w: truncated NSID length at i=%d", ErrInvalidFooter, i)
		}
		strLen := int(body[off])
		off++
		if off+4 > len(body) {
			return collectionIndex{}, fmt.Errorf(
				"%w: truncated event count at i=%d", ErrInvalidFooter, i)
		}
		eventCounts[i] = le.Uint32(body[off : off+4])
		off += 4
		if off+strLen > len(body) {
			return collectionIndex{}, fmt.Errorf(
				"%w: truncated NSID body at i=%d", ErrInvalidFooter, i)
		}
		// Copy: the body buffer is private to this call but the result
		// outlives it; we can't alias.
		stringTable[i] = string(body[off : off+strLen])
		off += strLen
	}

	bitmasks := make([][]uint32, blockCount)
	for i := range bitmasks {
		if off+int(bitmaskLen) > len(body) {
			return collectionIndex{}, fmt.Errorf(
				"%w: truncated bitmask at block %d", ErrInvalidFooter, i)
		}
		mask := body[off : off+int(bitmaskLen)]
		off += int(bitmaskLen)
		var ids []uint32
		for byteIdx, b := range mask {
			for bit := 0; bit < 8 && b != 0; bit++ {
				if b&(1<<bit) != 0 {
					id := uint32(byteIdx*8 + bit)
					if id >= collectionCount {
						return collectionIndex{}, fmt.Errorf(
							"%w: block %d bitmask references collection id %d (table has %d)",
							ErrInvalidFooter, i, id, collectionCount)
					}
					ids = append(ids, id)
				}
			}
		}
		bitmasks[i] = ids
	}

	if off != len(body) {
		return collectionIndex{}, fmt.Errorf(
			"%w: trailing bytes in collection body (off=%d, len=%d)",
			ErrInvalidFooter, off, len(body))
	}

	return collectionIndex{
		stringTable:   stringTable,
		eventCounts:   eventCounts,
		blockBitmasks: bitmasks,
	}, nil
}
