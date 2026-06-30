package segment

import (
	"fmt"
	"os"
	"sync/atomic"

	"github.com/jcalabro/gloom"
)

// ReaderConfig controls Reader.Open behavior. Path is required.
type ReaderConfig struct {
	// Path is the sealed segment file. Required.
	Path string

	// SkipChecksum disables the xxh3 verification performed by Open.
	// The default (false) computes xxh3 over (version..end-of-footer)
	// and compares against the value in the fixed header, returning
	// ErrChecksumMismatch on mismatch.
	//
	// Operators that have already verified the file via an out-of-
	// band mechanism (e.g., a checked SHA-256 from a CDN download)
	// may opt out to save the cost of re-hashing the metadata region.
	SkipChecksum bool
}

// maxBlockCountLimit caps header.BlockCount before any allocation
// keyed off it (the block-index slice and the collection-index
// bitmask slice). A 256 MB target segment with 4096 events per block
// has ~64k blocks; an order of magnitude beyond that is extravagant.
// 1<<20 lets us absorb future block-size knob changes without
// rebuilding the cap, while still bounding the worst-case allocation
// a hostile/corrupt header can drive: at blockIndexEntrySize bytes
// per entry, that's ~52 MB rather than 4 GB.
const maxBlockCountLimit = 1 << 20

// maxCollectionCountLimit caps the unique-collections-per-segment
// count parsed from the collection-index header. Real atproto
// segments observe well under a thousand distinct NSIDs network-
// wide; a million is a hostile-input ceiling, not a steady-state
// target. Without this, decodeCollectionIndex's stringTable
// allocation is driven directly by an on-disk uint32.
const maxCollectionCountLimit = 1 << 20

// Reader provides goroutine-safe read access to a sealed segment
// file. After Open, the file's metadata (header, block index,
// segment bloom, collection index) is parsed and held in memory;
// per-block decode and per-block-bloom load happen on demand via
// pread, so multiple goroutines may call DecodeBlock and BlockBloom
// concurrently with no shared mutable state.
//
// Close releases the file handle. It is idempotent.
type Reader struct {
	path string
	file *os.File

	header            Header
	blocks            []BlockInfo
	segmentBloom      *gloom.Filter
	parsedCollections collectionIndex
	perBlockBloomSize uint32

	closed atomic.Bool
}

// Open parses the sealed segment file at cfg.Path. On success, the
// returned Reader holds the parsed metadata in memory and an
// O_RDONLY file handle for on-demand block decode.
//
// Open performs at most five pread calls on the metadata region of
// the file: the fixed header, the block index, the segment-level
// bloom, the footer (for checksum), and the collection-index
// header+body. The per-block-blooms region is not read at Open;
// BlockBloom preads on demand.
func Open(cfg ReaderConfig) (*Reader, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("%w: ReaderConfig.Path is required", ErrInvalidConfig)
	}

	f, err := os.OpenFile(cfg.Path, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("segment: open %s: %w", cfg.Path, err)
	}
	success := false
	defer func() {
		if !success {
			_ = f.Close()
		}
	}()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("segment: stat %s: %w", cfg.Path, err)
	}
	fileSize := info.Size()
	if fileSize < int64(ReservedHeaderBytes) {
		return nil, fmt.Errorf("%w: %s is %d bytes",
			ErrCorruptSegment, cfg.Path, fileSize)
	}

	// 1. Fixed header.
	headerBytes := make([]byte, ReservedHeaderBytes)
	if _, err := f.ReadAt(headerBytes, 0); err != nil {
		return nil, fmt.Errorf("segment: read header: %w", err)
	}
	header, err := decodeHeader(headerBytes)
	if err != nil {
		return nil, err
	}
	if err := validateHeaderOffsets(header, uint64(fileSize)); err != nil {
		return nil, err
	}
	if header.BlockCount > maxBlockCountLimit {
		return nil, fmt.Errorf("%w: block_count %d exceeds limit %d",
			ErrInvalidFooter, header.BlockCount, maxBlockCountLimit)
	}

	// 2. Block index.
	blockIndexLen := int64(header.BlockCount) * blockIndexEntrySize
	blockIndexBytes := make([]byte, blockIndexLen)
	if blockIndexLen > 0 {
		if _, err := f.ReadAt(blockIndexBytes, int64(header.BlockIndexOffset)); err != nil {
			return nil, fmt.Errorf("segment: read block index: %w", err)
		}
	}
	blocks, err := decodeBlockIndex(blockIndexBytes, header.BlockCount)
	if err != nil {
		return nil, err
	}
	if err := validateBlockOffsets(blocks, header.FooterOffset); err != nil {
		return nil, err
	}

	// 3. Segment-level DID bloom.
	segmentBloomLen := int64(header.BlockDIDBloomOffset - header.DIDBloomOffset)
	segmentBloomBytes := make([]byte, segmentBloomLen)
	if segmentBloomLen > 0 {
		if _, err := f.ReadAt(segmentBloomBytes, int64(header.DIDBloomOffset)); err != nil {
			return nil, fmt.Errorf("segment: read segment bloom: %w", err)
		}
	}
	var segmentBloom *gloom.Filter
	if segmentBloomLen > 0 {
		segmentBloom, err = gloom.UnmarshalBinary(segmentBloomBytes)
		if err != nil {
			return nil, fmt.Errorf("segment: unmarshal segment bloom: %w", err)
		}
	}

	// 4. Per-block blooms region header (8 bytes).
	bloomRegionHeader := make([]byte, blockBloomsRegionHeaderSize)
	if _, err := f.ReadAt(bloomRegionHeader, int64(header.BlockDIDBloomOffset)); err != nil {
		return nil, fmt.Errorf("segment: read bloom region header: %w", err)
	}
	regionCount, perBlockSize, err := decodeBlockBloomsRegionHeader(bloomRegionHeader)
	if err != nil {
		return nil, err
	}
	if regionCount != header.BlockCount {
		return nil, fmt.Errorf("%w: bloom region count %d, header block_count %d",
			ErrInvalidFooter, regionCount, header.BlockCount)
	}
	// The per-block-blooms region must exactly fill the gap between
	// BlockDIDBloomOffset and CollectionIndexOffset; otherwise BlockBloom
	// preads could overrun into the collection index (or stop short and
	// silently truncate a real bloom).
	wantBloomRegionLen := uint64(blockBloomsRegionHeaderSize) +
		uint64(regionCount)*uint64(perBlockSize)
	gotBloomRegionLen := header.CollectionIndexOffset - header.BlockDIDBloomOffset
	if wantBloomRegionLen != gotBloomRegionLen {
		return nil, fmt.Errorf(
			"%w: per-block-blooms region is %d bytes, want %d (header=%d, body=%d×%d)",
			ErrInvalidFooter, gotBloomRegionLen, wantBloomRegionLen,
			blockBloomsRegionHeaderSize, regionCount, perBlockSize)
	}

	// 5. Optional checksum verification (before collection index decode).
	// We verify early so that corruption is detected before attempting to
	// decompress zstd bodies, which may fail in opaque ways.
	if !cfg.SkipChecksum {
		footerLen := fileSize - int64(header.FooterOffset)
		footerBytes := make([]byte, footerLen)
		if _, err := f.ReadAt(footerBytes, int64(header.FooterOffset)); err != nil {
			return nil, fmt.Errorf("segment: read footer for checksum: %w", err)
		}
		// Header bytes with the checksum field zeroed: that's how it
		// looked when seal computed xxh3.
		headerForHash := make([]byte, ReservedHeaderBytes)
		copy(headerForHash, headerBytes)
		for i := 4; i < 12; i++ {
			headerForHash[i] = 0
		}
		got := xxh3HeaderFooter(headerForHash, footerBytes)
		if got != header.Checksum {
			return nil, fmt.Errorf("%w: computed=%x, header=%x",
				ErrChecksumMismatch, got, header.Checksum)
		}
	}

	// 6. Collection index.
	collectionLen := fileSize - int64(header.CollectionIndexOffset)
	if collectionLen < int64(collectionIndexHeaderSize) {
		return nil, fmt.Errorf("%w: collection index region too small", ErrInvalidFooter)
	}
	collectionBytes := make([]byte, collectionLen)
	if _, err := f.ReadAt(collectionBytes, int64(header.CollectionIndexOffset)); err != nil {
		return nil, fmt.Errorf("segment: read collection index: %w", err)
	}
	colIdx, err := decodeCollectionIndex(collectionBytes)
	if err != nil {
		return nil, err
	}
	if uint32(len(colIdx.blockBitmasks)) != header.BlockCount {
		return nil, fmt.Errorf(
			"%w: collection index block_count %d != header block_count %d",
			ErrInvalidFooter, len(colIdx.blockBitmasks), header.BlockCount)
	}

	r := &Reader{
		path:              cfg.Path,
		file:              f,
		header:            header,
		blocks:            blocks,
		segmentBloom:      segmentBloom,
		parsedCollections: colIdx,
		perBlockBloomSize: perBlockSize,
	}
	success = true
	return r, nil
}

// Close releases the underlying file handle. Idempotent.
func (r *Reader) Close() error {
	if !r.closed.CompareAndSwap(false, true) {
		return nil
	}
	return r.file.Close()
}

// Header returns a copy of the parsed fixed header.
func (r *Reader) Header() Header { return r.header }

// Blocks returns a copy of the parsed block index. len == BlockCount.
func (r *Reader) Blocks() []BlockInfo {
	out := make([]BlockInfo, len(r.blocks))
	copy(out, r.blocks)
	return out
}

// SegmentBloom returns the segment-level DID bloom filter. Read-only;
// callers must not mutate the returned filter.
//
// Always non-nil for files produced by this package's Seal: gloom.New
// allocates a single block even when expectedItems is zero, so even
// an empty segment carries a (small) serialized filter. Callers
// should still defensively nil-check if they consume sealed files
// from arbitrary sources.
func (r *Reader) SegmentBloom() *gloom.Filter { return r.segmentBloom }

// Collections returns the collection string table, indexed by NSID
// id. The returned slice is a fresh copy; mutation is harmless.
func (r *Reader) Collections() []string {
	out := make([]string, len(r.parsedCollections.stringTable))
	copy(out, r.parsedCollections.stringTable)
	return out
}

// CollectionEventCounts returns the per-collection event counts,
// indexed the same way as Collections(). For collection id i,
// CollectionEventCounts()[i] is the total number of events with
// Collection == Collections()[i] across the whole segment. Events
// with an empty Collection (e.g. Identity, Account) are not counted,
// so sum(CollectionEventCounts()) <= Header().EventCount.
//
// The returned slice is a fresh copy; mutation is harmless.
func (r *Reader) CollectionEventCounts() []uint32 {
	out := make([]uint32, len(r.parsedCollections.eventCounts))
	copy(out, r.parsedCollections.eventCounts)
	return out
}

// BlockCollections returns the NSID ids present in the given block,
// sorted ascending.
func (r *Reader) BlockCollections(idx int) ([]uint32, error) {
	if idx < 0 || idx >= len(r.parsedCollections.blockBitmasks) {
		return nil, fmt.Errorf("%w: idx %d, BlockCount %d",
			ErrBlockOutOfRange, idx, len(r.parsedCollections.blockBitmasks))
	}
	src := r.parsedCollections.blockBitmasks[idx]
	out := make([]uint32, len(src))
	copy(out, src)
	return out, nil
}

// DecodeBlock reads, decompresses, and decodes the block at the
// given index. Returns the decoded events in their stored order.
//
// Multiple goroutines may call DecodeBlock concurrently; each call
// is a fresh pread + decompress + decode with no shared mutable
// state.
func (r *Reader) DecodeBlock(idx int) ([]Event, error) {
	if idx < 0 || idx >= len(r.blocks) {
		return nil, fmt.Errorf("%w: idx %d, BlockCount %d",
			ErrBlockOutOfRange, idx, len(r.blocks))
	}
	b := r.blocks[idx]
	frame := make([]byte, b.CompressedSize)
	// The block index records the offset of the 8-byte length prefix;
	// the frame body starts 8 bytes later.
	if _, err := r.file.ReadAt(frame, int64(b.Offset)+8); err != nil {
		return nil, fmt.Errorf("segment: read block %d frame: %w", idx, err)
	}
	events, _, err := decodeBlockCompressedSized(frame)
	if err != nil {
		return nil, fmt.Errorf("segment: decode block %d: %w", idx, err)
	}
	return events, nil
}

// validateHeaderOffsets checks that every offset in the parsed header
// fits within the file and that the section ordering matches the
// spec. Returns ErrInvalidFooter on any violation.
func validateHeaderOffsets(h Header, fileSize uint64) error {
	if h.FooterOffset < uint64(ReservedHeaderBytes) {
		return fmt.Errorf("%w: footer_offset %d < reserved header",
			ErrInvalidFooter, h.FooterOffset)
	}
	if h.FooterOffset > fileSize {
		return fmt.Errorf("%w: footer_offset %d > file size %d",
			ErrInvalidFooter, h.FooterOffset, fileSize)
	}
	if h.BlockIndexOffset != h.FooterOffset {
		return fmt.Errorf("%w: block_index_offset %d != footer_offset %d",
			ErrInvalidFooter, h.BlockIndexOffset, h.FooterOffset)
	}
	if h.DIDBloomOffset < h.BlockIndexOffset ||
		h.BlockDIDBloomOffset < h.DIDBloomOffset ||
		h.CollectionIndexOffset < h.BlockDIDBloomOffset ||
		h.CollectionIndexOffset > fileSize {
		return fmt.Errorf("%w: footer section offsets out of order", ErrInvalidFooter)
	}
	return nil
}

// BlockBloom reads and unmarshals the per-block DID bloom for the
// given block index. Each call performs one pread; there is no
// internal cache. Callers that want every block's bloom should
// prefer LoadAllBlockBlooms.
//
// Multiple goroutines may call BlockBloom concurrently.
func (r *Reader) BlockBloom(idx int) (*gloom.Filter, error) {
	if idx < 0 || idx >= len(r.blocks) {
		return nil, fmt.Errorf("%w: idx %d, BlockCount %d",
			ErrBlockOutOfRange, idx, len(r.blocks))
	}
	if r.perBlockBloomSize == 0 {
		// Empty segment (no blocks ever appended). The bounds check
		// above already short-circuits in that case, but keep the
		// guard explicit.
		return nil, fmt.Errorf("%w: no per-block blooms in this segment",
			ErrBlockOutOfRange)
	}
	off := int64(r.header.BlockDIDBloomOffset) +
		int64(blockBloomsRegionHeaderSize) +
		int64(idx)*int64(r.perBlockBloomSize)
	buf := make([]byte, r.perBlockBloomSize)
	if _, err := r.file.ReadAt(buf, off); err != nil {
		return nil, fmt.Errorf("segment: read block %d bloom: %w", idx, err)
	}
	f, err := gloom.UnmarshalBinary(buf)
	if err != nil {
		return nil, fmt.Errorf("segment: unmarshal block %d bloom: %w", idx, err)
	}
	return f, nil
}

// LoadAllBlockBlooms reads and unmarshals every per-block DID bloom in order.
// The on-disk bloom body is contiguous, so this performs one pread instead of
// one pread per block.
func (r *Reader) LoadAllBlockBlooms() ([]*gloom.Filter, error) {
	out := make([]*gloom.Filter, len(r.blocks))
	if len(out) == 0 {
		return out, nil
	}
	if r.perBlockBloomSize == 0 {
		return nil, fmt.Errorf("%w: no per-block blooms in this segment",
			ErrBlockOutOfRange)
	}

	bodyLen := uint64(len(out)) * uint64(r.perBlockBloomSize)
	if bodyLen > uint64(int(^uint(0)>>1)) {
		return nil, fmt.Errorf("%w: per-block bloom body too large: %d bytes",
			ErrInvalidFooter, bodyLen)
	}
	buf := make([]byte, int(bodyLen))
	off := int64(r.header.BlockDIDBloomOffset) + int64(blockBloomsRegionHeaderSize)
	if _, err := r.file.ReadAt(buf, off); err != nil {
		return nil, fmt.Errorf("segment: read block blooms: %w", err)
	}

	size := int(r.perBlockBloomSize)
	for i := range out {
		start := i * size
		f, err := gloom.UnmarshalBinary(buf[start : start+size])
		if err != nil {
			return nil, fmt.Errorf("segment: unmarshal block %d bloom: %w", i, err)
		}
		out[i] = f
	}
	return out, nil
}

// BlocksContainingDID returns the ascending indices of blocks that may
// contain an event for did, reading the segment's per-block DID blooms
// from disk and applying SelectBlocksForDID. It is the file-backed
// counterpart to the manifest's in-memory selection; both share the
// same SelectBlocksForDID decision so they can never diverge.
//
// See SelectBlocksForDID for the one-sided (no-false-negative) contract.
func (r *Reader) BlocksContainingDID(did string) ([]int, error) {
	if len(r.blocks) == 0 {
		return nil, nil
	}
	// Segment-level short-circuit before paying for the per-block bloom
	// pread: if the whole-segment bloom says the DID is absent, no block
	// can contain it.
	if r.segmentBloom != nil && !r.segmentBloom.TestString(did) {
		return nil, nil
	}

	blooms, err := r.LoadAllBlockBlooms()
	if err != nil {
		// Without per-block blooms we cannot prune safely; fall back to
		// every block rather than risk a false negative.
		all := make([]int, len(r.blocks))
		for i := range all {
			all[i] = i
		}
		return all, nil
	}

	// segBloom is nil here so SelectBlocksForDID does not re-test the
	// segment bloom (already checked above); pass nil to skip it.
	return SelectBlocksForDID(nil, blooms, did), nil
}

// validateBlockOffsets verifies every block's [offset, offset+8+size]
// range fits before the footer, that ranges are strictly ascending and
// non-overlapping (since that's how Seal writes them), that
// MaxSeq >= MinSeq within each entry, and that consecutive non-empty
// blocks are seq-disjoint and index-monotonic (block[i].MaxSeq <
// block[i+1].MinSeq). A malformed block index that passes per-entry
// bounds checks but is internally inconsistent could otherwise surface
// as confusing decode errors at DecodeBlock time.
//
// The cross-block seq-monotonicity check is load-bearing for the backfill
// planner: PlanBackfill's truncation continuation cursor is the last
// included block's MaxSeq, and the next page's exclusive afterSeq drops
// every block with MaxSeq <= that cursor (internal/manifest/plan.go). That
// is gap-free ONLY if a later block never carries a smaller MaxSeq; a
// segment that reached disk with out-of-order per-block seq bounds would
// otherwise make the planner silently skip a block (silent data loss). The
// single-writer ingest path holds this invariant (seqs assigned under one
// lock, seal walks frames in ascending offset), so this check fails loud
// on a corrupt/foreign/regressed segment rather than letting it serve.
func validateBlockOffsets(blocks []BlockInfo, footerOffset uint64) error {
	var prevEnd uint64 = ReservedHeaderBytes
	// prevMaxSeq tracks the MaxSeq of the most recent NON-EMPTY block.
	// Empty (EventCount==0) blocks are skipped: a compaction-to-empty
	// rewrite preserves the block's original MinSeq/MaxSeq envelope
	// (segment/rewrite.go) even though it now holds no rows, so its stale
	// bounds must not gate the monotonicity comparison. hasPrevSeq guards
	// the first non-empty block.
	var prevMaxSeq uint64
	hasPrevSeq := false
	for i, b := range blocks {
		// Reject an offset at or past the footer BEFORE computing end. This is
		// both a real bound (a block's 8-byte length prefix must start strictly
		// before the footer) and the overflow guard for the addition below:
		// footerOffset is validated <= fileSize <= MaxInt64 by
		// validateHeaderOffsets, so once b.Offset < footerOffset the sum
		// b.Offset + 8 + CompressedSize (CompressedSize is a uint32) cannot wrap
		// uint64. Without this a hostile b.Offset near MaxUint64 would wrap end
		// to a small value and slip past the `end > footerOffset` range check.
		if b.Offset >= footerOffset {
			return fmt.Errorf("%w: block %d offset %d at or past footer_offset %d",
				ErrInvalidBlockIndex, i, b.Offset, footerOffset)
		}
		end := b.Offset + 8 + uint64(b.CompressedSize)
		if b.Offset < uint64(ReservedHeaderBytes) || end > footerOffset {
			return fmt.Errorf(
				"%w: block %d range [%d, %d) outside [%d, %d)",
				ErrInvalidBlockIndex, i, b.Offset, end,
				ReservedHeaderBytes, footerOffset)
		}
		if b.Offset < prevEnd {
			return fmt.Errorf(
				"%w: block %d offset %d overlaps prior block ending at %d",
				ErrInvalidBlockIndex, i, b.Offset, prevEnd)
		}
		if b.MaxSeq < b.MinSeq {
			return fmt.Errorf(
				"%w: block %d has max_seq %d < min_seq %d",
				ErrInvalidBlockIndex, i, b.MaxSeq, b.MinSeq)
		}
		if b.MaxIndexedAt < b.MinIndexedAt {
			return fmt.Errorf(
				"%w: block %d has max_indexed_at %d < min_indexed_at %d",
				ErrInvalidBlockIndex, i, b.MaxIndexedAt, b.MinIndexedAt)
		}
		if b.EventCount > 0 {
			if hasPrevSeq && b.MinSeq <= prevMaxSeq {
				return fmt.Errorf(
					"%w: block %d min_seq %d not greater than prior non-empty block max_seq %d (blocks must be seq-disjoint and index-monotonic; the backfill planner cursor depends on it)",
					ErrInvalidBlockIndex, i, b.MinSeq, prevMaxSeq)
			}
			prevMaxSeq = b.MaxSeq
			hasPrevSeq = true
		}
		prevEnd = end
	}
	return nil
}
