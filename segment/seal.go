package segment

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/jcalabro/gloom"
)

// SealResult is returned by Writer.Seal so the caller (orchestrator)
// can log/emit metrics without re-reading the file.
type SealResult struct {
	BlockCount     uint32
	EventCount     uint32
	UniqueDIDCount uint32
	MinSeq         uint64
	MaxSeq         uint64
	MinWitnessedAt int64
	MaxWitnessedAt int64
	Checksum       uint64
	FooterOffset   uint64
	FileSize       int64
}

// Seal finalizes the active segment file: flushes any pending events,
// walks the on-disk frames to gather per-block stats, writes the
// variable-length footer at end-of-file, patches the finalized
// 256-byte fixed header at offset 0, fsyncs, and closes the file.
//
// Seal consumes the Writer. After a successful Seal, the writer is
// closed and any subsequent Append/Flush/Seal/Close call returns
// ErrClosed (Close is idempotent and returns nil).
//
// On failure, the file is left in a state from which the caller can
// recover by opening a fresh Writer at the same path:
//   - Failure before the footer is durable: the file is untouched.
//   - Failure after the footer is durable but before the header is
//     patched: Seal explicitly truncates the partial footer back off
//     before returning, restoring the active-state "last byte is the
//     last good frame" invariant. The detail is documented in
//     sealAfterFlush.
//
// Seal performs no goroutine work. It is safe to call from any
// goroutine that already serializes access to this Writer.
//
// Seal does not open its own tracing span — it has no ctx parameter
// to attach to a parent, and a context.Background() span would
// orphan and lie about parentage. Callers that want a span around
// the seal (ingest.Writer.rotateLocked, orchestrator.finishBootstrap)
// already wrap with obs.Span one frame up. Seal's contribution
// to observability is the seal_duration_seconds histogram below.
func (w *Writer) Seal() (SealResult, error) {
	if w.closed {
		return SealResult{}, ErrClosed
	}
	if w.stickyErr != nil {
		return SealResult{}, w.stickyErr
	}
	if w.preparedOutstanding > 0 {
		return SealResult{}, fmt.Errorf("segment: seal with %d uncommitted prepared block(s)", w.preparedOutstanding)
	}
	// Capture start *after* the closed/stickyErr early-returns so the
	// histogram measures real seal work, not no-op rejections. Per
	// metrics.go: failed seals are not recorded — operators chase
	// failures through error logs and trace status.
	start := time.Now()
	// Flush pending. flushLocked is a no-op if pending is empty.
	if err := w.flushLocked(); err != nil {
		return SealResult{}, err
	}
	r, err := w.sealAfterFlush()
	if err != nil {
		return SealResult{}, err
	}
	if w.cfg.Metrics != nil {
		w.cfg.Metrics.ObserveSeal(start, nil)
	}
	// Mark the writer closed: Seal is terminal. The underlying file
	// has already been Close()d by sealAfterFlush.
	w.closed = true
	return r, nil
}

// sealAfterFlush walks the active file's frames to gather per-block
// stats, builds the variable-length footer in memory, writes it at
// EOF, patches the finalized fixed header at offset 0, fsyncs, and
// closes the file.
//
// Failure paths and recovery:
//   - Walk fails: file is untouched, error is returned. The next
//     Writer.New() resumes cleanly.
//   - Footer Write fails: a partial footer past the last good frame
//     is written. Those bytes do not parse as a frame length prefix
//     (because the first 8 bytes of the footer are the block index,
//     which is a uint64 file offset, which when interpreted as a
//     length always overruns the file). lastGoodOffset truncates them
//     on the next Writer.New(). Seal latches stickyErr and returns.
//   - Footer Sync fails: same as above; the footer bytes may or may
//     not be durable, but the recovery path is identical.
//   - Header WriteAt fails: the footer is durable but the header is
//     still zero. We explicitly truncate the footer back off here so
//     the file is restored to the active-state invariant. stickyErr
//     latched.
//   - Header Sync fails: both writes are durable per the kernel; we
//     leave the file as-is and rely on the Reader.Open checksum check
//     at next open to detect any corruption. stickyErr latched.
func (w *Writer) sealAfterFlush() (SealResult, error) {
	footerOffset, err := w.activeFileSize()
	if err != nil {
		return SealResult{}, err
	}

	walk, err := w.walkBlocks(footerOffset)
	if err != nil {
		return SealResult{}, err
	}

	footerBytes, header, err := buildFooter(walk, footerOffset)
	if err != nil {
		return SealResult{}, err
	}

	headerBytes := encodeHeader(header)
	checksum := xxh3HeaderFooter(headerBytes, footerBytes)
	header.Checksum = checksum
	binary.LittleEndian.PutUint64(headerBytes[4:12], checksum)

	// Footer write.
	if err := w.cfg.beforeIO(IOOpWrite); err != nil {
		w.stickyErr = fmt.Errorf("segment: write footer: %w", err)
		return SealResult{}, w.stickyErr
	}
	if _, err := w.file.WriteAt(footerBytes, footerOffset); err != nil {
		w.stickyErr = fmt.Errorf("segment: write footer: %w", err)
		return SealResult{}, w.stickyErr
	}
	if err := w.cfg.beforeIO(IOOpSync); err != nil {
		w.stickyErr = fmt.Errorf("segment: fsync footer: %w", err)
		return SealResult{}, w.stickyErr
	}
	if err := syncSegmentFile(w.cfg.FS, w.file); err != nil {
		w.stickyErr = fmt.Errorf("segment: fsync footer: %w", err)
		return SealResult{}, w.stickyErr
	}

	// Header pwrite.
	if err := w.cfg.beforeIO(IOOpWrite); err != nil {
		writeErr := fmt.Errorf("segment: write header: %w", err)
		if truncErr := w.truncateFooterTail(footerOffset); truncErr != nil {
			w.stickyErr = fmt.Errorf("%w (also: %w)", writeErr, truncErr)
		} else {
			w.stickyErr = writeErr
		}
		return SealResult{}, w.stickyErr
	}
	if _, err := w.file.WriteAt(headerBytes, 0); err != nil {
		// Footer is durable but header is zero. Truncate the footer
		// back off so the file is restored to active-state invariants.
		// We swallow the truncate error in favor of surfacing the
		// original WriteAt failure, but log it via the wrapped error.
		writeErr := fmt.Errorf("segment: write header: %w", err)
		if truncErr := w.truncateFooterTail(footerOffset); truncErr != nil {
			w.stickyErr = fmt.Errorf("%w (also: %w)", writeErr, truncErr)
		} else {
			w.stickyErr = writeErr
		}
		return SealResult{}, w.stickyErr
	}
	if err := w.cfg.beforeIO(IOOpSync); err != nil {
		w.stickyErr = fmt.Errorf("segment: fsync sealed file: %w", err)
		return SealResult{}, w.stickyErr
	}
	if err := syncSegmentFile(w.cfg.FS, w.file); err != nil {
		// Both writes happened but we couldn't confirm durability of
		// the header. Don't truncate: the bytes may already be durable
		// and truncating could destroy a valid sealed file. The Reader
		// will detect corruption on Open via the xxh3 check.
		w.stickyErr = fmt.Errorf("segment: fsync sealed file: %w", err)
		return SealResult{}, w.stickyErr
	}

	if err := w.closeFile(); err != nil {
		w.stickyErr = err
		return SealResult{}, err
	}

	stat, _ := w.fileStatAfterClose() // best-effort for FileSize reporting
	return SealResult{
		BlockCount:     header.BlockCount,
		EventCount:     header.EventCount,
		UniqueDIDCount: header.UniqueDIDCount,
		MinSeq:         header.MinSeq,
		MaxSeq:         header.MaxSeq,
		MinWitnessedAt: header.MinWitnessedAt,
		MaxWitnessedAt: header.MaxWitnessedAt,
		Checksum:       checksum,
		FooterOffset:   header.FooterOffset,
		FileSize:       stat,
	}, nil
}

// fileStatAfterClose stats the sealed file by path so SealResult can
// report FileSize. It's "best-effort": a stat error is silently
// reported as 0 because the seal itself already succeeded.
func (w *Writer) fileStatAfterClose() (int64, error) {
	info, err := statSegmentFile(w.cfg.FS, w.cfg.Path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// blockWalkResult holds the per-block info and segment-wide
// accumulators gathered during the seal walk.
type blockWalkResult struct {
	infos []BlockInfo

	// Segment-wide accumulators.
	totalEventCount uint32
	uniqueDIDs      map[string]struct{}
	minSeq          uint64
	maxSeq          uint64
	minWitnessedAt  int64
	maxWitnessedAt  int64

	// Per-block bloom inputs and collection IDs.
	perBlockDIDs        []map[string]struct{}
	perBlockCollections [][]uint32

	// Global collection string table in first-seen order.
	collectionStringTable []string
	collectionEventCounts []uint32 // parallel-indexed with collectionStringTable
	collectionIDByName    map[string]uint32

	// Did we see any events at all? minSeq/minWitnessedAt are only
	// meaningful when this is true.
	sawAny bool
}

// internCollection records collection under the segment-wide string
// table (assigning a stable id on first sight, bumping its event count)
// and adds that id to blockCollections. It is the shared collection-index
// step for both the seal walk (walkActiveFrames) and the compaction
// rewrite walk (accumulateRewriteBlock), so the two index paths cannot
// drift. countEvent is false for synthetic sentinel ids (DID-level marker
// sentinels are a selection hint, not real per-collection traffic, so they
// must not inflate collectionEventCounts).
func (res *blockWalkResult) internCollection(collection string, countEvent bool, blockCollections map[uint32]struct{}) error {
	id, ok := res.collectionIDByName[collection]
	if !ok {
		if uint64(len(res.collectionStringTable)) >= math.MaxUint32 {
			return fmt.Errorf("%w: too many distinct collections", ErrInvalidFooter)
		}
		col := string([]byte(collection))
		id = uint32(len(res.collectionStringTable))
		res.collectionStringTable = append(res.collectionStringTable, col)
		res.collectionEventCounts = append(res.collectionEventCounts, 0)
		res.collectionIDByName[col] = id
	}
	if countEvent {
		res.collectionEventCounts[id]++
	}
	blockCollections[id] = struct{}{}
	return nil
}

// indexEventCollection adds ev's collection coordinates to blockCollections
// via internCollection: the real collection for commit rows that carry one,
// and a reserved DID-level marker sentinel ($account/$identity/$sync) for
// marker kinds, which carry no collection on the wire. Indexing the sentinel
// is what lets a collection-filtered backfill select marker-bearing blocks
// (see segment/sentinel.go); the sentinel does not count as a collection
// event so it stays invisible in per-collection stats.
func (res *blockWalkResult) indexEventCollection(ev *Event, blockCollections map[uint32]struct{}) error {
	if ev.Collection != "" {
		if err := res.internCollection(ev.Collection, true, blockCollections); err != nil {
			return err
		}
	}
	if sentinel := didMarkerSentinel(ev.Kind); sentinel != "" {
		if err := res.internCollection(sentinel, false, blockCollections); err != nil {
			return err
		}
	}
	return nil
}

// walkBlocks walks the framed-block region of the active file from
// ReservedHeaderBytes to footerOffset. Wrapper around walkActiveFrames
// so the seal path keeps its existing call shape.
func (w *Writer) walkBlocks(footerOffset int64) (blockWalkResult, error) {
	return walkActiveFrames(w.file, footerOffset)
}

// walkActiveFrames walks the framed-block region of a segment file
// from ReservedHeaderBytes to maxOffset, decompressing each frame
// and gathering per-block stats. Used by Writer.Seal during sealing
// and by Inspect when reporting on an active (unsealed) file.
//
// On a torn tail (a frame whose length prefix points past maxOffset)
// the partial blockWalkResult accumulated up to that frame is
// returned alongside an ErrCorruptSegment-wrapped error so callers
// that want the partial result can recover it.
func walkActiveFrames(f io.ReaderAt, maxOffset int64) (blockWalkResult, error) {
	res := blockWalkResult{
		uniqueDIDs:         map[string]struct{}{},
		collectionIDByName: map[string]uint32{},
	}
	off := int64(ReservedHeaderBytes)
	for off < maxOffset {
		frame, frameSize, err := readFrameAt(f, off, maxOffset)
		if err != nil {
			return res, err
		}

		events, uncompressedSize, err := decodeBlockCompressedSized(frame)
		if err != nil {
			return res, fmt.Errorf("segment: decode block at %d: %w",
				off, err)
		}
		if len(events) == 0 {
			break
		}

		info := BlockInfo{
			Offset:           uint64(off),
			CompressedSize:   uint32(len(frame)),
			UncompressedSize: uint32(uncompressedSize),
			EventCount:       uint32(len(events)),
		}
		blockDIDs := map[string]struct{}{}
		blockCollections := map[uint32]struct{}{}

		for i, ev := range events {
			if i == 0 {
				info.MinSeq = ev.Seq
				info.MaxSeq = ev.Seq
				info.MinWitnessedAt = ev.WitnessedAt
				info.MaxWitnessedAt = ev.WitnessedAt
			}
			if ev.Seq < info.MinSeq {
				info.MinSeq = ev.Seq
			}
			if ev.Seq > info.MaxSeq {
				info.MaxSeq = ev.Seq
			}
			if ev.WitnessedAt < info.MinWitnessedAt {
				info.MinWitnessedAt = ev.WitnessedAt
			}
			if ev.WitnessedAt > info.MaxWitnessedAt {
				info.MaxWitnessedAt = ev.WitnessedAt
			}

			if !res.sawAny {
				res.minSeq = ev.Seq
				res.maxSeq = ev.Seq
				res.minWitnessedAt = ev.WitnessedAt
				res.maxWitnessedAt = ev.WitnessedAt
				res.sawAny = true
			} else {
				if ev.Seq < res.minSeq {
					res.minSeq = ev.Seq
				}
				if ev.Seq > res.maxSeq {
					res.maxSeq = ev.Seq
				}
				if ev.WitnessedAt < res.minWitnessedAt {
					res.minWitnessedAt = ev.WitnessedAt
				}
				if ev.WitnessedAt > res.maxWitnessedAt {
					res.maxWitnessedAt = ev.WitnessedAt
				}
			}

			// DIDs may be empty for some kinds (Identity, Account,
			// Sync events sometimes carry no DID per atproto spec).
			// Skip empty DIDs from both bloom filters and the unique-
			// DID count: an empty bloom hit is meaningless, and the
			// uniqueDIDCount in the header should reflect real DIDs.
			if ev.DID != "" {
				if _, ok := res.uniqueDIDs[ev.DID]; !ok {
					// Clone the string: ev.DID aliases the decompressed
					// frame, which goes out of scope at the end of this
					// loop iteration.
					did := string([]byte(ev.DID))
					res.uniqueDIDs[did] = struct{}{}
					blockDIDs[did] = struct{}{}
				} else if _, ok := blockDIDs[ev.DID]; !ok {
					blockDIDs[string([]byte(ev.DID))] = struct{}{}
				}
			}
			if err := res.indexEventCollection(&ev, blockCollections); err != nil {
				return res, err
			}
		}

		res.infos = append(res.infos, info)
		res.perBlockDIDs = append(res.perBlockDIDs, blockDIDs)

		ids := make([]uint32, 0, len(blockCollections))
		for id := range blockCollections {
			ids = append(ids, id)
		}
		sortUint32(ids)
		res.perBlockCollections = append(res.perBlockCollections, ids)

		res.totalEventCount += uint32(len(events))
		off += int64(frameSize)
	}
	return res, nil
}

// buildFooter assembles the four footer sections in the layout
// specified in docs/README.md §3.1.2 and spec §5.6 and returns them
// concatenated, plus the partially-populated Header (Checksum left
// zero; the caller fills it in after computing xxh3). Shared by the
// seal and compaction-rewrite paths; both derive per-block bloom
// sizing from the walk itself.
func buildFooter(walk blockWalkResult, footerOffset int64) ([]byte, Header, error) {
	// 1. Block index.
	blockIndexBytes := encodeBlockIndex(walk.infos)

	// 2. Segment-level DID bloom.
	segmentBloom := gloom.New(uint64(len(walk.uniqueDIDs)), segmentBloomFPRate)
	for did := range walk.uniqueDIDs {
		segmentBloom.AddString(did)
	}
	segmentBloomBytes, err := segmentBloom.MarshalBinary()
	if err != nil {
		return nil, Header{}, fmt.Errorf("segment: marshal segment bloom: %w", err)
	}

	// 3. Per-block DID blooms, right-sized to the segment's actual max
	// per-block unique-DID cardinality (see the sizing rationale in
	// bloom.go). Sizing every filter for the max — rather than each
	// block's own count — is what preserves the equal-size region
	// invariant; the FP target is realized exactly on the max block
	// and beaten on the rest. Capacity clamps to >= 1 so a segment of
	// DID-less events (identity/account markers) still produces a
	// valid minimal filter.
	perBlockCapacity := uint64(1)
	for _, dids := range walk.perBlockDIDs {
		if n := uint64(len(dids)); n > perBlockCapacity {
			perBlockCapacity = n
		}
	}
	perBlockFilters := make([]*gloom.Filter, len(walk.perBlockDIDs))
	for i, dids := range walk.perBlockDIDs {
		f := gloom.New(perBlockCapacity, perBlockBloomFPRate)
		for did := range dids {
			f.AddString(did)
		}
		perBlockFilters[i] = f
	}
	perBlockBloomsRegion, _, err := encodeBlockBloomsRegion(perBlockFilters)
	if err != nil {
		return nil, Header{}, err
	}

	// 4. Collection block index.
	colIdx := collectionIndex{
		stringTable:   walk.collectionStringTable,
		eventCounts:   walk.collectionEventCounts,
		blockBitmasks: walk.perBlockCollections,
	}
	collectionIndexBytes, err := encodeCollectionIndex(colIdx)
	if err != nil {
		return nil, Header{}, err
	}

	// Concatenate in spec order. Track section offsets so the header
	// can record absolute file positions for each.
	footer := make([]byte, 0,
		len(blockIndexBytes)+len(segmentBloomBytes)+
			len(perBlockBloomsRegion)+len(collectionIndexBytes))

	blockIndexOffset := uint64(footerOffset)
	footer = append(footer, blockIndexBytes...)
	didBloomOffset := blockIndexOffset + uint64(len(blockIndexBytes))
	footer = append(footer, segmentBloomBytes...)
	blockDIDBloomOffset := didBloomOffset + uint64(len(segmentBloomBytes))
	footer = append(footer, perBlockBloomsRegion...)
	collectionIndexOffset := blockDIDBloomOffset + uint64(len(perBlockBloomsRegion))
	footer = append(footer, collectionIndexBytes...)

	header := Header{
		Version:               currentHeaderVersion,
		BlockCount:            uint32(len(walk.infos)),
		EventCount:            walk.totalEventCount,
		UniqueDIDCount:        uint32(len(walk.uniqueDIDs)),
		MinSeq:                walk.minSeq,
		MaxSeq:                walk.maxSeq,
		MinWitnessedAt:        walk.minWitnessedAt,
		MaxWitnessedAt:        walk.maxWitnessedAt,
		FooterOffset:          uint64(footerOffset),
		DIDBloomOffset:        didBloomOffset,
		BlockDIDBloomOffset:   blockDIDBloomOffset,
		CollectionIndexOffset: collectionIndexOffset,
		BlockIndexOffset:      blockIndexOffset,
		// Checksum: filled in by caller after xxh3.
	}
	return footer, header, nil
}

// sortUint32 sorts in ascending order. Pulled out as a tiny helper
// because the std slices.Sort generic instantiation is overkill for a
// single use here, and a hand-coded insertion sort is fine for the
// small N we see (~tens of collections per block).
func sortUint32(s []uint32) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}

// activeFileSize returns the current on-disk size of the writer's
// file. Used by sealAfterFlush to compute footer_offset.
func (w *Writer) activeFileSize() (int64, error) {
	info, err := w.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("segment: stat for seal: %w", err)
	}
	return info.Size(), nil
}

// truncateFooterTail truncates the file back to footerOffset and
// fsyncs. Used by sealAfterFlush when the header pwrite fails: the
// footer is on disk but unreferenced by a finalized header, leaving
// the file in an ambiguous state. Truncating restores the active-
// state invariant ("last byte is the last good frame"), so the next
// Writer.New() can resume cleanly and the operator can re-call Seal.
//
// We use file.Truncate (which acts on the inode regardless of file
// position) and fsync immediately so a second crash before any
// further writes cannot resurrect the truncated bytes. We don't fsync
// the parent directory: only the file's contents and size are
// changing, both inode-attached.
func (w *Writer) truncateFooterTail(footerOffset int64) error {
	if err := truncateSegmentFile(w.cfg.FS, w.file, footerOffset); err != nil {
		return fmt.Errorf("segment: truncate footer: %w", err)
	}
	if err := syncSegmentFile(w.cfg.FS, w.file); err != nil {
		return fmt.Errorf("segment: fsync truncated file: %w", err)
	}
	return nil
}

// readFrameAt reads a single [uint64 LE compressed_len][zstd frame]
// pair starting at fileOffset, bounded by maxOffset so a torn or
// hostile length prefix can't drive an unbounded allocation.
// Used by the seal walk and by Inspect's active-file path.
func readFrameAt(f io.ReaderAt, fileOffset, maxOffset int64) (frame []byte, frameLen int, err error) {
	var lenBuf [8]byte
	if _, err := f.ReadAt(lenBuf[:], fileOffset); err != nil {
		return nil, 0, fmt.Errorf("segment: read frame length at %d: %w",
			fileOffset, err)
	}
	frameSize := binary.LittleEndian.Uint64(lenBuf[:])
	// Reject frames that would extend past the active-state end of
	// the framed-block region. The writer's own resumeExistingSegment
	// path already truncates torn tails, so a hit on a well-behaved
	// file means corruption.
	remaining := uint64(maxOffset - fileOffset - int64(len(lenBuf)))
	if frameSize > remaining {
		return nil, 0, fmt.Errorf(
			"%w: frame at %d claims %d bytes, only %d remain before footer",
			ErrCorruptSegment, fileOffset, frameSize, remaining)
	}
	frame = make([]byte, frameSize)
	if _, err := f.ReadAt(frame, fileOffset+8); err != nil {
		return nil, 0, fmt.Errorf("segment: read frame body at %d: %w",
			fileOffset+8, err)
	}
	return frame, len(lenBuf) + int(frameSize), nil
}

// closeFile closes the writer's underlying file. Used by sealAfterFlush
// at the end of the happy path. The standalone helper exists so
// sealAfterFlush can call it once at end-of-success without polluting
// the error-path control flow.
func (w *Writer) closeFile() error {
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("segment: close after seal: %w", err)
	}
	return nil
}
