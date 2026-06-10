package segment

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
)

type RowDecision uint8

const (
	RowKeep RowDecision = iota
	RowDrop
)

type RewriteOptions struct{}

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
	var perBlockParams *bloomParams
	if len(blocks) > 0 {
		firstBloom, err := r.BlockBloom(0)
		if err != nil {
			return RewriteResult{}, err
		}
		encodedBloom, err := firstBloom.MarshalBinary()
		if err != nil {
			return RewriteResult{}, fmt.Errorf("segment: rewrite marshal source bloom params: %w", err)
		}
		params, err := parseBloomParams(encodedBloom)
		if err != nil {
			return RewriteResult{}, err
		}
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
				outFrame, err = encodeBlockCompressed(kept)
				if err != nil {
					return RewriteResult{}, fmt.Errorf("segment: rewrite encode block %d: %w", i, err)
				}
				body, err := encodeBlock(kept)
				if err != nil {
					return RewriteResult{}, fmt.Errorf("segment: rewrite size block %d: %w", i, err)
				}
				outUncompressedSize = len(body)
			}
		}

		info := BlockInfo{
			Offset:           nextOffset,
			CompressedSize:   uint32(len(outFrame)),
			UncompressedSize: uint32(outUncompressedSize),
			EventCount:       uint32(len(kept)),
			MinSeq:           orig.MinSeq,
			MaxSeq:           orig.MaxSeq,
			MinIndexedAt:     orig.MinIndexedAt,
			MaxIndexedAt:     orig.MaxIndexedAt,
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

	if err := initializeNewSegment(f); err != nil {
		return RewriteResult{}, err
	}
	for _, b := range outBlocks {
		var lenBuf [8]byte
		binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(b.frame)))
		if _, err := f.Write(lenBuf[:]); err != nil {
			return RewriteResult{}, fmt.Errorf("segment: rewrite write frame len: %w", err)
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
	newHeader.MinIndexedAt = header.MinIndexedAt
	newHeader.MaxIndexedAt = header.MaxIndexedAt

	headerBytes := encodeHeader(newHeader)
	checksum := xxh3HeaderFooter(headerBytes, footerBytes)
	newHeader.Checksum = checksum
	binary.LittleEndian.PutUint64(headerBytes[4:12], checksum)

	if _, err := f.WriteAt(footerBytes, footerOffset); err != nil {
		return RewriteResult{}, fmt.Errorf("segment: rewrite write footer: %w", err)
	}
	if _, err := f.WriteAt(headerBytes, 0); err != nil {
		return RewriteResult{}, fmt.Errorf("segment: rewrite write header: %w", err)
	}
	if err := syncFile(f); err != nil {
		return RewriteResult{}, fmt.Errorf("segment: rewrite fsync tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		return RewriteResult{}, fmt.Errorf("segment: rewrite close tmp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return RewriteResult{}, fmt.Errorf("segment: rewrite rename: %w", err)
	}
	if err := syncParentDir(path); err != nil {
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
	for _, ev := range events {
		if ev.DID != "" {
			if _, ok := walk.uniqueDIDs[ev.DID]; !ok {
				did := string([]byte(ev.DID))
				walk.uniqueDIDs[did] = struct{}{}
				blockDIDs[did] = struct{}{}
			} else if _, ok := blockDIDs[ev.DID]; !ok {
				blockDIDs[string([]byte(ev.DID))] = struct{}{}
			}
		}
		if ev.Collection != "" {
			id, ok := walk.collectionIDByName[ev.Collection]
			if !ok {
				if uint64(len(walk.collectionStringTable)) >= math.MaxUint32 {
					return fmt.Errorf("%w: too many distinct collections", ErrInvalidFooter)
				}
				col := string([]byte(ev.Collection))
				id = uint32(len(walk.collectionStringTable))
				walk.collectionStringTable = append(walk.collectionStringTable, col)
				walk.collectionEventCounts = append(walk.collectionEventCounts, 0)
				walk.collectionIDByName[col] = id
			}
			walk.collectionEventCounts[id]++
			blockCollections[id] = struct{}{}
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
