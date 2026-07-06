package segment

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type RowDecision uint8

const (
	RowKeep RowDecision = iota
	RowDrop
)

type RewriteOptions struct {
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

	r, err := Open(ReaderConfig{Path: path})
	if err != nil {
		return RewriteResult{}, err
	}
	defer func() { _ = r.Close() }()

	src := r.file
	header := r.Header()
	blocks := r.Blocks()
	if len(opts.CandidateDIDs) > 0 && !segmentBloomMayContainAny(r, opts.CandidateDIDs) {
		return RewriteResult{Header: header}, nil
	}
	var perBlockParams *bloomParams
	if len(blocks) > 0 {
		firstBloom, err := r.BlockBloom(0)
		if err != nil {
			return RewriteResult{}, err
		}
		params := bloomParamsFromFilter(firstBloom)
		perBlockParams = &params
	}

	type outBlock struct {
		frame []byte
		info  BlockInfo
	}
	outBlocks := make([]outBlock, 0, len(blocks))
	walk := blockWalkResult{
		uniqueDIDs:         map[string]struct{}{},
		collectionIDByName: map[string]uint32{},
	}

	var rowsDropped uint64
	var blocksTouched uint32
	nextOffset := uint64(ReservedHeaderBytes)
	for i, orig := range blocks {
		frame, err := readSealedFrame(src, orig)
		if err != nil {
			return RewriteResult{}, fmt.Errorf("segment: rewrite read block %d: %w", i, err)
		}
		events, uncompressedSize, err := decodeBlockCompressedSized(frame)
		if err != nil {
			return RewriteResult{}, fmt.Errorf("segment: rewrite decode block %d: %w", i, err)
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
					return RewriteResult{}, fmt.Errorf("segment: rewrite encode block %d: %w", i, err)
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
		outBlocks = append(outBlocks, outBlock{frame: outFrame, info: info})
		if err := accumulateRewriteBlock(&walk, kept); err != nil {
			return RewriteResult{}, err
		}
		walk.infos = append(walk.infos, info)
	}

	if rowsDropped == 0 {
		return RewriteResult{Header: header}, nil
	}

	tmp := path + ".tmp"
	if err := os.Remove(tmp); err != nil && !os.IsNotExist(err) {
		return RewriteResult{}, fmt.Errorf("segment: rewrite remove stale tmp: %w", err)
	}
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return RewriteResult{}, fmt.Errorf("segment: rewrite create tmp: %w", err)
	}
	success := false
	defer func() {
		_ = f.Close()
		if !success {
			_ = os.Remove(tmp)
		}
	}()

	if err := initializeNewSegment(f, Config{Path: tmp, IOFaultInjector: opts.IOFaultInjector}); err != nil {
		return RewriteResult{}, err
	}
	for _, b := range outBlocks {
		var lenBuf [8]byte
		binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(b.frame)))
		if err := beforeSegmentIO(opts.IOFaultInjector, tmp, IOOpWrite); err != nil {
			return RewriteResult{}, fmt.Errorf("segment: rewrite write frame len: %w", err)
		}
		if _, err := f.Write(lenBuf[:]); err != nil {
			return RewriteResult{}, fmt.Errorf("segment: rewrite write frame len: %w", err)
		}
		if err := beforeSegmentIO(opts.IOFaultInjector, tmp, IOOpWrite); err != nil {
			return RewriteResult{}, fmt.Errorf("segment: rewrite write frame: %w", err)
		}
		if _, err := f.Write(b.frame); err != nil {
			return RewriteResult{}, fmt.Errorf("segment: rewrite write frame: %w", err)
		}
	}

	footerOffset := int64(nextOffset)
	footerBytes, newHeader, err := buildFooterWithBloomParams(walk, DefaultMaxEventsPerBlock, footerOffset, perBlockParams)
	if err != nil {
		return RewriteResult{}, err
	}
	newHeader.MinSeq = header.MinSeq
	newHeader.MaxSeq = header.MaxSeq
	newHeader.MinWitnessedAt = header.MinWitnessedAt
	newHeader.MaxWitnessedAt = header.MaxWitnessedAt

	headerBytes := encodeHeader(newHeader)
	checksum := xxh3HeaderFooter(headerBytes, footerBytes)
	newHeader.Checksum = checksum
	binary.LittleEndian.PutUint64(headerBytes[4:12], checksum)

	if err := beforeSegmentIO(opts.IOFaultInjector, tmp, IOOpWrite); err != nil {
		return RewriteResult{}, fmt.Errorf("segment: rewrite write footer: %w", err)
	}
	if _, err := f.WriteAt(footerBytes, footerOffset); err != nil {
		return RewriteResult{}, fmt.Errorf("segment: rewrite write footer: %w", err)
	}
	if err := beforeSegmentIO(opts.IOFaultInjector, tmp, IOOpWrite); err != nil {
		return RewriteResult{}, fmt.Errorf("segment: rewrite write header: %w", err)
	}
	if _, err := f.WriteAt(headerBytes, 0); err != nil {
		return RewriteResult{}, fmt.Errorf("segment: rewrite write header: %w", err)
	}
	if err := simulateRewriteCrash(opts, CrashPointRewriteTempWritten); err != nil {
		return RewriteResult{}, err
	}
	if err := beforeSegmentIO(opts.IOFaultInjector, tmp, IOOpSync); err != nil {
		return RewriteResult{}, fmt.Errorf("segment: rewrite fsync tmp: %w", err)
	}
	if err := syncFile(f); err != nil {
		return RewriteResult{}, fmt.Errorf("segment: rewrite fsync tmp: %w", err)
	}
	if err := simulateRewriteCrash(opts, CrashPointRewriteTempSynced); err != nil {
		return RewriteResult{}, err
	}
	if err := f.Close(); err != nil {
		return RewriteResult{}, fmt.Errorf("segment: rewrite close tmp: %w", err)
	}
	if err := beforeSegmentIO(opts.IOFaultInjector, path, IOOpRename); err != nil {
		return RewriteResult{}, fmt.Errorf("segment: rewrite rename: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return RewriteResult{}, fmt.Errorf("segment: rewrite rename: %w", err)
	}
	if err := simulateRewriteCrash(opts, CrashPointRewriteRenamed); err != nil {
		return RewriteResult{}, err
	}
	if err := syncParentDir(path, opts.IOFaultInjector); err != nil {
		return RewriteResult{}, err
	}
	if err := simulateRewriteCrash(opts, CrashPointRewriteDirSynced); err != nil {
		return RewriteResult{}, err
	}
	success = true

	return RewriteResult{
		Rewritten:     true,
		RowsDropped:   rowsDropped,
		BlocksTouched: blocksTouched,
		NewChecksum:   checksum,
		Header:        newHeader,
	}, nil
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
