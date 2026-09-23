package segment

import (
	"encoding/binary"
	"fmt"

	"github.com/jcalabro/gloom"
)

// DID blooms target a 0.1% false-positive rate: each false positive costs a
// block decode and scan.
//
// Per-block capacity is the segment's maximum unique-DID count in any block.
// Equal sizes allow direct indexing without an offset table; smaller blocks
// get a lower false-positive rate. bloom_size_bytes in the region header
// supports both this sizing and legacy fixed-capacity blooms.
//
// Real blocks typically contain 1–3 DIDs. Sizing every bloom for 4096 wasted
// most server heap; see specs/notes/2026-07-10-bloom-memory-exploration.md.
// Compaction recalculates capacity from surviving rows, reducing legacy
// allocations as files are rewritten.
const (
	perBlockBloomFPRate = 0.001

	// segmentBloomFPRate matches the per-block rate. Memory cost
	// across all segments is bounded; FP cost is one extra pread to
	// load per-block-blooms, which is also small but measurable. Same
	// rate keeps the trade-off symmetric.
	segmentBloomFPRate = 0.001
)

// blockBloomsRegionHeaderSize is the 8-byte uncompressed header that
// precedes the packed per-block filters: block_count (uint32 LE) +
// bloom_size_bytes (uint32 LE).
const blockBloomsRegionHeaderSize = 8

// encodeBlockBloomsRegion serializes the per-block blooms region
// (docs/README.md §3.1.3, spec §5.4). Every filter must marshal to an
// identical length; this is the invariant that lets the reader index
// blooms by multiplication. We assert it here so a violation is loud
// rather than a silent on-disk corruption.
//
// Returns the encoded region and the per-bloom size in bytes (which
// the caller stores in the header for cross-region offset math).
func encodeBlockBloomsRegion(filters []*gloom.Filter) ([]byte, uint32, error) {
	header := make([]byte, blockBloomsRegionHeaderSize)
	if len(filters) == 0 {
		// Header carries (block_count=0, bloom_size_bytes=0) and no body.
		return header, 0, nil
	}

	first, err := filters[0].MarshalBinary()
	if err != nil {
		return nil, 0, fmt.Errorf("segment: marshal block bloom 0: %w", err)
	}
	sizeBytes := uint32(len(first))

	body := make([]byte, len(filters)*int(sizeBytes))
	copy(body[:sizeBytes], first)

	for i := 1; i < len(filters); i++ {
		marshaled, err := filters[i].MarshalBinary()
		if err != nil {
			return nil, 0, fmt.Errorf("segment: marshal block bloom %d: %w", i, err)
		}
		if uint32(len(marshaled)) != sizeBytes {
			return nil, 0, fmt.Errorf(
				"segment: per-block bloom size mismatch at block %d: got %d, want %d",
				i, len(marshaled), sizeBytes)
		}
		copy(body[i*int(sizeBytes):(i+1)*int(sizeBytes)], marshaled)
	}

	le := binary.LittleEndian
	le.PutUint32(header[0:4], uint32(len(filters)))
	le.PutUint32(header[4:8], sizeBytes)

	return append(header, body...), sizeBytes, nil
}

// decodeBlockBloomsRegionHeader reads the 8-byte region header and
// returns (block_count, bloom_size_bytes). The full region body
// follows on disk and is read by the caller via pread on demand.
func decodeBlockBloomsRegionHeader(buf []byte) (uint32, uint32, error) {
	if len(buf) < blockBloomsRegionHeaderSize {
		return 0, 0, fmt.Errorf("%w: bloom region header is %d bytes, want %d",
			ErrInvalidFooter, len(buf), blockBloomsRegionHeaderSize)
	}
	le := binary.LittleEndian
	count := le.Uint32(buf[0:4])
	size := le.Uint32(buf[4:8])
	return count, size, nil
}

// decodeBlockBloomFromRegion returns the bloom for block idx given a
// region body (everything after the 8-byte header) and the per-bloom
// size. Most callers will not have the whole body in memory; they use
// the (offset, size) math directly via pread. This helper exists for
// the in-memory roundtrip path used by tests.
func decodeBlockBloomFromRegion(region []byte, sizeBytes uint32, idx int) (*gloom.Filter, error) {
	if idx < 0 {
		return nil, fmt.Errorf("%w: idx %d < 0", ErrBlockOutOfRange, idx)
	}

	body := region[blockBloomsRegionHeaderSize:]
	start := idx * int(sizeBytes)
	end := start + int(sizeBytes)
	if end > len(body) {
		return nil, fmt.Errorf("%w: idx %d past region (size %d)",
			ErrBlockOutOfRange, idx, len(body)/int(sizeBytes))
	}

	f, err := gloom.UnmarshalBinary(body[start:end])
	if err != nil {
		return nil, fmt.Errorf("segment: unmarshal block bloom %d: %w", idx, err)
	}
	return f, nil
}
