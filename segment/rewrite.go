package segment

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble/vfs"
)

type RowDecision uint8

const (
	RowKeep RowDecision = iota
	RowDrop
)

type RewriteOptions struct {
	// FS is the filesystem used for segment I/O. Nil uses the host OS
	// filesystem.
	FS            vfs.FS
	CrashInjector CrashInjector
	// IOFaultInjector is a test-only seam consulted before every tmp-file
	// write, fsync, and the commit rename. Nil in production.
	IOFaultInjector IOFaultInjector
	CandidateDIDs   []string
}

type RewriteResult struct {
	Rewritten     bool
	RowsDropped   uint64
	BlocksTouched uint32
	NewChecksum   uint64
	Header        Header
}

func Rewrite(path string, decide func(*Event) RowDecision, opts RewriteOptions) (RewriteResult, error) {
	if path == "" {
		return RewriteResult{}, fmt.Errorf("%w: Rewrite path is required", ErrInvalidConfig)
	}
	if decide == nil {
		return RewriteResult{}, fmt.Errorf("%w: Rewrite decide is required", ErrInvalidConfig)
	}

	r, err := Open(ReaderConfig{Path: path, FS: opts.FS})
	if err != nil {
		return RewriteResult{}, err
	}
	defer func() { _ = r.Close() }()

	header := r.Header()
	if len(opts.CandidateDIDs) > 0 && !segmentBloomMayContainAny(r, opts.CandidateDIDs) {
		return RewriteResult{Header: header}, nil
	}

	plan, err := computeRewrite(r, decide)
	if err != nil {
		return RewriteResult{}, err
	}
	if plan.rowsDropped == 0 {
		return RewriteResult{Header: header}, nil
	}
	if err := writeRewriteFile(path, plan, opts); err != nil {
		return RewriteResult{}, err
	}
	return RewriteResult{
		Rewritten:     true,
		RowsDropped:   plan.rowsDropped,
		BlocksTouched: plan.blocksTouched,
		NewChecksum:   plan.header.Checksum,
		Header:        plan.header,
	}, nil
}

// rewritePlan is a rewritten segment laid out in memory: its block
// frames (without length prefixes) in order, and its finalized header
// and footer. Writing the header at 0, each frame behind its 8-byte
// length prefix from ReservedHeaderBytes on, and the footer at
// header.FooterOffset yields the rewritten segment file.
type rewritePlan struct {
	frames        [][]byte
	headerBytes   []byte
	footerBytes   []byte
	header        Header
	rowsDropped   uint64
	blocksTouched uint32
}

// computeRewrite applies decide to every row of r and builds the
// rewritten segment in memory. It does no I/O beyond reading r's
// blocks, so it works for a Reader over any source. When no row is
// dropped it returns a plan with rowsDropped == 0 and nothing else
// filled in: the segment stays as it is.
//
// Unlike BuildSealed it keeps blocks whose rows were all dropped (as
// empty blocks) and every block's original seq and witnessed bounds, so
// block indices and the snapshot planner's seq envelope survive
// compaction.
func computeRewrite(r *Reader, decide func(*Event) RowDecision) (rewritePlan, error) {
	header := r.Header()
	blocks := r.Blocks()
	frames := make([][]byte, 0, len(blocks))
	walk := newBlockWalkResult()

	var rowsDropped uint64
	var blocksTouched uint32
	nextOffset := uint64(ReservedHeaderBytes)
	for i, orig := range blocks {
		frame, err := r.readFrame(i)
		if err != nil {
			return rewritePlan{}, fmt.Errorf("segment: rewrite read block %d: %w", i, err)
		}
		events, uncompressedSize, err := decodeBlockCompressedSized(frame)
		if err != nil {
			return rewritePlan{}, fmt.Errorf("segment: rewrite decode block %d: %w", i, err)
		}
		kept := events[:0]
		var droppedInBlock bool
		for j := range events {
			ev := &events[j]
			if decide(ev) == RowDrop {
				rowsDropped++
				droppedInBlock = true
				continue
			}
			kept = append(kept, *ev)
		}

		outFrame := frame
		outUncompressedSize := uncompressedSize
		if droppedInBlock {
			blocksTouched++
			if len(kept) == 0 {
				outFrame = encodeEmptyBlockCompressed()
				outUncompressedSize = len(encodeEmptyBlock())
			} else {
				outFrame, outUncompressedSize, err = encodeBlockCompressedSized(kept)
				if err != nil {
					return rewritePlan{}, fmt.Errorf("segment: rewrite encode block %d: %w", i, err)
				}
			}
		}

		info := BlockInfo{
			Offset:           nextOffset,
			CompressedSize:   uint32(len(outFrame)),
			UncompressedSize: uint32(outUncompressedSize),
			EventCount:       uint32(len(kept)),
			MinSeq:           orig.MinSeq,
			MaxSeq:           orig.MaxSeq,
			MinWitnessedAt:   orig.MinWitnessedAt,
			MaxWitnessedAt:   orig.MaxWitnessedAt,
		}
		nextOffset += uint64(8 + len(outFrame))
		frames = append(frames, outFrame)
		if err := accumulateRewriteBlock(&walk, kept); err != nil {
			return rewritePlan{}, err
		}
		walk.infos = append(walk.infos, info)
	}

	if rowsDropped == 0 {
		return rewritePlan{}, nil
	}

	// The footer is rebuilt from the surviving rows, which right-sizes
	// the per-block blooms (issue #303): a rewrite of a legacy segment
	// sheds its oversized pre-#302 blooms, and dropped rows can lower
	// the surviving cardinality below the source's in any case.
	footerBytes, newHeader, err := buildFooter(walk, int64(nextOffset))
	if err != nil {
		return rewritePlan{}, err
	}
	newHeader.MinSeq = header.MinSeq
	newHeader.MaxSeq = header.MaxSeq
	newHeader.MinWitnessedAt = header.MinWitnessedAt
	newHeader.MaxWitnessedAt = header.MaxWitnessedAt

	headerBytes := encodeHeader(newHeader)
	checksum := xxh3HeaderFooter(headerBytes, footerBytes)
	newHeader.Checksum = checksum
	binary.LittleEndian.PutUint64(headerBytes[4:12], checksum)

	return rewritePlan{
		frames:        frames,
		headerBytes:   headerBytes,
		footerBytes:   footerBytes,
		header:        newHeader,
		rowsDropped:   rowsDropped,
		blocksTouched: blocksTouched,
	}, nil
}

// writeRewriteFile is the file sink for a rewrite: it writes plan to
// path+".tmp", fsyncs it, renames it over path, and fsyncs the parent
// directory, consulting the IO fault and crash seams along the way.
func writeRewriteFile(path string, plan rewritePlan, opts RewriteOptions) error {
	tmp := path + ".tmp"
	if err := removeSegmentFile(opts.FS, tmp); err != nil && !isSegmentNotExist(err) {
		return fmt.Errorf("segment: rewrite remove stale tmp: %w", err)
	}
	f, err := createSegmentFileExclusive(opts.FS, tmp)
	if err != nil {
		return fmt.Errorf("segment: rewrite create tmp: %w", err)
	}
	success := false
	defer func() {
		if f != nil {
			_ = f.Close()
		}
		if !success {
			_ = removeSegmentFile(opts.FS, tmp)
		}
	}()

	if err := initializeNewSegment(f, Config{Path: tmp, FS: opts.FS, IOFaultInjector: opts.IOFaultInjector}); err != nil {
		return err
	}
	writeOffset := int64(ReservedHeaderBytes)
	for _, frame := range plan.frames {
		var lenBuf [8]byte
		binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(frame)))
		if err := beforeSegmentIO(opts.IOFaultInjector, tmp, IOOpWrite); err != nil {
			return fmt.Errorf("segment: rewrite write frame len: %w", err)
		}
		if _, err := f.WriteAt(lenBuf[:], writeOffset); err != nil {
			return fmt.Errorf("segment: rewrite write frame len: %w", err)
		}
		writeOffset += int64(len(lenBuf))
		if err := beforeSegmentIO(opts.IOFaultInjector, tmp, IOOpWrite); err != nil {
			return fmt.Errorf("segment: rewrite write frame: %w", err)
		}
		if _, err := f.WriteAt(frame, writeOffset); err != nil {
			return fmt.Errorf("segment: rewrite write frame: %w", err)
		}
		writeOffset += int64(len(frame))
	}
	if uint64(writeOffset) != plan.header.FooterOffset {
		return fmt.Errorf("%w: rewrite wrote blocks to %d, footer_offset %d",
			ErrCorruptSegment, writeOffset, plan.header.FooterOffset)
	}

	if err := beforeSegmentIO(opts.IOFaultInjector, tmp, IOOpWrite); err != nil {
		return fmt.Errorf("segment: rewrite write footer: %w", err)
	}
	if _, err := f.WriteAt(plan.footerBytes, writeOffset); err != nil {
		return fmt.Errorf("segment: rewrite write footer: %w", err)
	}
	if err := beforeSegmentIO(opts.IOFaultInjector, tmp, IOOpWrite); err != nil {
		return fmt.Errorf("segment: rewrite write header: %w", err)
	}
	if _, err := f.WriteAt(plan.headerBytes, 0); err != nil {
		return fmt.Errorf("segment: rewrite write header: %w", err)
	}
	if err := simulateRewriteCrash(opts, CrashPointRewriteTempWritten); err != nil {
		return err
	}
	if err := beforeSegmentIO(opts.IOFaultInjector, tmp, IOOpSync); err != nil {
		return fmt.Errorf("segment: rewrite fsync tmp: %w", err)
	}
	if err := syncSegmentFile(opts.FS, f); err != nil {
		return fmt.Errorf("segment: rewrite fsync tmp: %w", err)
	}
	if err := simulateRewriteCrash(opts, CrashPointRewriteTempSynced); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("segment: rewrite close tmp: %w", err)
	}
	f = nil
	if err := beforeSegmentIO(opts.IOFaultInjector, path, IOOpRename); err != nil {
		return fmt.Errorf("segment: rewrite rename: %w", err)
	}
	if err := renameSegmentFile(opts.FS, tmp, path); err != nil {
		return fmt.Errorf("segment: rewrite rename: %w", err)
	}
	if err := simulateRewriteCrash(opts, CrashPointRewriteRenamed); err != nil {
		return err
	}
	if err := syncParentDirFS(opts.FS, path, opts.IOFaultInjector); err != nil {
		return err
	}
	if err := simulateRewriteCrash(opts, CrashPointRewriteDirSynced); err != nil {
		return err
	}
	success = true
	return nil
}

func segmentBloomMayContainAny(r *Reader, dids []string) bool {
	bloom := r.SegmentBloom()
	if bloom == nil {
		return true
	}
	for _, did := range dids {
		if did != "" && bloom.TestString(did) {
			return true
		}
	}
	return false
}

func simulateRewriteCrash(opts RewriteOptions, point string) error {
	if opts.CrashInjector == nil {
		return nil
	}
	return opts.CrashInjector.SimulateCrash(context.Background(), point)
}

func readSealedFrame(f io.ReaderAt, b BlockInfo) ([]byte, error) {
	frame := make([]byte, b.CompressedSize)
	if _, err := f.ReadAt(frame, int64(b.Offset)+8); err != nil {
		return nil, err
	}
	return frame, nil
}

func accumulateRewriteBlock(walk *blockWalkResult, events []Event) error {
	blockDIDs := map[string]struct{}{}
	blockCollections := map[uint32]struct{}{}
	for i := range events {
		ev := &events[i]
		if ev.DID != "" {
			if _, ok := walk.uniqueDIDs[ev.DID]; !ok {
				did := string([]byte(ev.DID))
				walk.uniqueDIDs[did] = struct{}{}
				blockDIDs[did] = struct{}{}
			} else if _, ok := blockDIDs[ev.DID]; !ok {
				blockDIDs[string([]byte(ev.DID))] = struct{}{}
			}
		}
		if err := walk.indexEventCollection(ev, blockCollections); err != nil {
			return err
		}
	}
	ids := make([]uint32, 0, len(blockCollections))
	for id := range blockCollections {
		ids = append(ids, id)
	}
	sortUint32(ids)
	walk.perBlockDIDs = append(walk.perBlockDIDs, blockDIDs)
	walk.perBlockCollections = append(walk.perBlockCollections, ids)
	walk.totalEventCount += uint32(len(events))
	return nil
}
