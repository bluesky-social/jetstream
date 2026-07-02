package segment

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/zeebo/xxh3"
)

// PatchOptions configures a Patch call. It mirrors RewriteOptions: a nil
// CrashInjector makes every durability seam a no-op (production), and a
// non-empty CandidateDIDs lets Patch skip a segment whose DID bloom proves
// none of the targeted repos are present.
type PatchOptions struct {
	CrashInjector CrashInjector
	CandidateDIDs []string
}

// PatchResult reports what a Patch call did so the caller (the import
// compaction pass) can log and emit metrics without re-reading the file.
type PatchResult struct {
	// Patched is true iff the file was rewritten in place. A zero-mutation
	// Patch leaves the file byte-for-byte untouched and reports false.
	Patched bool

	// RowsMutated is the number of rows whose IndexedAt column changed.
	RowsMutated uint64

	// BlocksTouched is the number of blocks that had at least one mutated
	// row and were therefore re-encoded. Unmutated blocks are copied
	// verbatim.
	BlocksTouched uint32

	// NewChecksum is the xxh3 of the patched file's header+footer. Zero
	// when Patched is false.
	NewChecksum uint64

	// Header is the patched file's fixed header (or the source header,
	// unchanged, when Patched is false).
	Header Header
}

// Patch rewrites a sealed segment in place, applying mutate to every event
// to update the display (indexed_at) column only. It is the mutate-mode
// sibling of Rewrite (which is drop-only): Patch never adds, drops, or
// reorders rows and never changes any column other than IndexedAt.
//
// mutate is called once per event, in stored order, with a pointer to a
// decoded Event. It must set only ev.IndexedAt (the operator-imported
// display timestamp) and return true iff it changed the event. Mutating any
// other field is a contract violation: Patch snapshots the immutable fields
// before the call and returns an error (leaving the source file untouched)
// rather than persist a file whose verbatim-copied blooms, collection index,
// or witnessed/seq envelope no longer describe its contents. Crashing beats
// corruption.
//
// Why the footer is mostly copyable: IndexedAt is a fixed-width 8-byte
// column, so mutating it changes neither the block count/order nor any
// block's uncompressed size, and it never touches DID or Collection. The
// segment DID bloom, per-block DID blooms, and collection block index are
// keyed only on DID/Collection/counts and embed no absolute file offsets, so
// they are byte-identical after a patch and are copied verbatim from the
// source. Only the block index (whose per-block Offset and CompressedSize
// shift when a re-compressed block's frame size changes), the header's
// section offsets, and the checksum are rebuilt.
//
// Durability matches Rewrite: write a sibling .tmp, fsync it, rename over the
// original, fsync the parent dir. A crash at or before the rename leaves the
// original intact; a crash after it means the patched file is already the
// durable one (see the CrashPointPatch* seams).
//
// A zero-mutation Patch (every mutate returns false / leaves IndexedAt
// unchanged) skips the rename entirely and leaves the file byte-for-byte
// untouched, so a re-run over already-imported data is a genuine no-op —
// Patch is idempotent for a fixed mutate.
func Patch(path string, mutate func(*Event) bool, opts PatchOptions) (PatchResult, error) {
	if path == "" {
		return PatchResult{}, fmt.Errorf("%w: Patch path is required", ErrInvalidConfig)
	}
	if mutate == nil {
		return PatchResult{}, fmt.Errorf("%w: Patch mutate is required", ErrInvalidConfig)
	}

	r, err := Open(ReaderConfig{Path: path})
	if err != nil {
		return PatchResult{}, err
	}
	defer func() { _ = r.Close() }()

	header := r.Header()
	blocks := r.Blocks()
	if len(opts.CandidateDIDs) > 0 && !segmentBloomMayContainAny(r, opts.CandidateDIDs) {
		return PatchResult{Header: header}, nil
	}

	// The verbatim footer tail: everything after the block index (segment
	// DID bloom + per-block DID blooms + collection index). Read it in one
	// pread and copy it unchanged into the patched file. Open() already
	// validated that BlockIndexOffset == FooterOffset and that the section
	// offsets are internally consistent, so blockIndexLen is trustworthy.
	fi, err := r.file.Stat()
	if err != nil {
		return PatchResult{}, fmt.Errorf("segment: patch stat: %w", err)
	}
	fileSize := fi.Size()
	footerLen := fileSize - int64(header.FooterOffset)
	if footerLen < 0 {
		return PatchResult{}, fmt.Errorf("%w: footer_offset %d past file size %d",
			ErrInvalidFooter, header.FooterOffset, fileSize)
	}
	srcFooter := make([]byte, footerLen)
	if _, err := r.file.ReadAt(srcFooter, int64(header.FooterOffset)); err != nil {
		return PatchResult{}, fmt.Errorf("segment: patch read footer: %w", err)
	}
	blockIndexLen := int(header.BlockCount) * blockIndexEntrySize
	if blockIndexLen > len(srcFooter) {
		return PatchResult{}, fmt.Errorf("%w: block index %d bytes exceeds footer %d",
			ErrInvalidFooter, blockIndexLen, len(srcFooter))
	}
	// Open only checks that the footer section offsets are monotonic, not that
	// the DID bloom begins immediately after the block index. Patch copies
	// srcFooter[blockIndexLen:] verbatim as the bloom-and-onward tail and
	// re-derives DIDBloomOffset as FooterOffset+blockIndexLen, so any padding
	// between the block index and the bloom in the source would be silently
	// reinterpreted as bloom bytes under a freshly-valid checksum. Reject that
	// rather than persist a desynced footer.
	wantDIDBloomOffset := header.FooterOffset + uint64(blockIndexLen)
	if header.DIDBloomOffset != wantDIDBloomOffset {
		return PatchResult{}, fmt.Errorf(
			"%w: did_bloom_offset %d != footer_offset+block_index_len %d",
			ErrInvalidFooter, header.DIDBloomOffset, wantDIDBloomOffset)
	}
	footerTail := srcFooter[blockIndexLen:]

	type outBlock struct {
		frame []byte
		info  BlockInfo
	}
	outBlocks := make([]outBlock, 0, len(blocks))

	var rowsMutated uint64
	var blocksTouched uint32
	nextOffset := uint64(ReservedHeaderBytes)
	for i, orig := range blocks {
		frame, err := readSealedFrame(r.file, orig)
		if err != nil {
			return PatchResult{}, fmt.Errorf("segment: patch read block %d: %w", i, err)
		}
		events, uncompressedSize, err := decodeBlockCompressedSized(frame)
		if err != nil {
			return PatchResult{}, fmt.Errorf("segment: patch decode block %d: %w", i, err)
		}
		if uint32(len(events)) != orig.EventCount {
			return PatchResult{}, fmt.Errorf(
				"%w: block %d decoded %d events, index claims %d",
				ErrInvalidBlockIndex, i, len(events), orig.EventCount)
		}

		// Snapshot every row before running any callback, then verify every
		// row after all callbacks have run. A single interleaved
		// mutate-then-verify loop would let a callback that retains a pointer
		// (or Payload slice) to an earlier row mutate a forbidden field during
		// a later row's callback, after that earlier row already passed its
		// guard. Splitting the passes closes that cross-row window.
		guards := make([]eventGuard, len(events))
		oldIndexed := make([]int64, len(events))
		for j := range events {
			ev := &events[j]
			guards[j] = guardSnapshot(ev)
			oldIndexed[j] = ev.IndexedAt
		}
		for j := range events {
			mutate(&events[j])
		}

		var dirty bool
		for j := range events {
			ev := &events[j]
			if err := guards[j].verify(ev); err != nil {
				return PatchResult{}, fmt.Errorf("segment: patch block %d row %d: %w", i, j, err)
			}
			if ev.IndexedAt != oldIndexed[j] {
				dirty = true
				rowsMutated++
			}
		}

		outFrame := frame
		outUncompressed := uncompressedSize
		if dirty {
			blocksTouched++
			outFrame, outUncompressed, err = encodeBlockCompressedSized(events)
			if err != nil {
				return PatchResult{}, fmt.Errorf("segment: patch encode block %d: %w", i, err)
			}
			// IndexedAt is fixed-width, so a patched block's uncompressed
			// body must be exactly as large as the source's. A mismatch means
			// mutate changed a variable-length field past the guard (or a
			// codec bug) — refuse to persist a desynced file.
			if outUncompressed != uncompressedSize {
				return PatchResult{}, fmt.Errorf(
					"%w: block %d uncompressed size changed %d->%d under patch",
					ErrInvalidBlockIndex, i, uncompressedSize, outUncompressed)
			}
		}

		// Every per-block bound is preserved: seq and witnessed_at columns
		// are untouched, and the row count is invariant. Only Offset and
		// CompressedSize can move (a re-compressed frame differs in size).
		info := BlockInfo{
			Offset:           nextOffset,
			CompressedSize:   uint32(len(outFrame)),
			UncompressedSize: uint32(outUncompressed),
			EventCount:       orig.EventCount,
			MinSeq:           orig.MinSeq,
			MaxSeq:           orig.MaxSeq,
			MinWitnessedAt:   orig.MinWitnessedAt,
			MaxWitnessedAt:   orig.MaxWitnessedAt,
		}
		nextOffset += uint64(8 + len(outFrame))
		outBlocks = append(outBlocks, outBlock{frame: outFrame, info: info})
	}

	if rowsMutated == 0 {
		return PatchResult{Header: header}, nil
	}

	newInfos := make([]BlockInfo, len(outBlocks))
	for i := range outBlocks {
		newInfos[i] = outBlocks[i].info
	}
	blockIndexBytes := encodeBlockIndex(newInfos)
	if len(blockIndexBytes) != blockIndexLen {
		// Block count is invariant under patch, so the freshly-encoded index
		// must be the same length as the source's — otherwise the verbatim
		// footer tail would land at the wrong offset.
		return PatchResult{}, fmt.Errorf(
			"%w: patched block index %d bytes != source %d",
			ErrInvalidFooter, len(blockIndexBytes), blockIndexLen)
	}

	footerOffset := int64(nextOffset)
	footerBytes := make([]byte, 0, len(blockIndexBytes)+len(footerTail))
	footerBytes = append(footerBytes, blockIndexBytes...)
	footerBytes = append(footerBytes, footerTail...)

	// Rebuild the header: everything but the section offsets is carried over
	// from the source verbatim (the witnessed/seq envelope, counts, version).
	segmentBloomLen := header.BlockDIDBloomOffset - header.DIDBloomOffset
	perBlockBloomsLen := header.CollectionIndexOffset - header.BlockDIDBloomOffset
	newHeader := Header{
		Version:               header.Version,
		BlockCount:            header.BlockCount,
		EventCount:            header.EventCount,
		UniqueDIDCount:        header.UniqueDIDCount,
		MinSeq:                header.MinSeq,
		MaxSeq:                header.MaxSeq,
		MinWitnessedAt:        header.MinWitnessedAt,
		MaxWitnessedAt:        header.MaxWitnessedAt,
		FooterOffset:          uint64(footerOffset),
		BlockIndexOffset:      uint64(footerOffset),
		DIDBloomOffset:        uint64(footerOffset) + uint64(len(blockIndexBytes)),
		BlockDIDBloomOffset:   uint64(footerOffset) + uint64(len(blockIndexBytes)) + segmentBloomLen,
		CollectionIndexOffset: uint64(footerOffset) + uint64(len(blockIndexBytes)) + segmentBloomLen + perBlockBloomsLen,
	}

	headerBytes := encodeHeader(newHeader)
	checksum := xxh3HeaderFooter(headerBytes, footerBytes)
	newHeader.Checksum = checksum
	binary.LittleEndian.PutUint64(headerBytes[4:12], checksum)

	tmp := path + ".tmp"
	if err := os.Remove(tmp); err != nil && !os.IsNotExist(err) {
		return PatchResult{}, fmt.Errorf("segment: patch remove stale tmp: %w", err)
	}
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return PatchResult{}, fmt.Errorf("segment: patch create tmp: %w", err)
	}
	success := false
	defer func() {
		_ = f.Close()
		if !success {
			_ = os.Remove(tmp)
		}
	}()

	if err := initializeNewSegment(f); err != nil {
		return PatchResult{}, err
	}
	for _, b := range outBlocks {
		var lenBuf [8]byte
		binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(b.frame)))
		if _, err := f.Write(lenBuf[:]); err != nil {
			return PatchResult{}, fmt.Errorf("segment: patch write frame len: %w", err)
		}
		if _, err := f.Write(b.frame); err != nil {
			return PatchResult{}, fmt.Errorf("segment: patch write frame: %w", err)
		}
	}

	if _, err := f.WriteAt(footerBytes, footerOffset); err != nil {
		return PatchResult{}, fmt.Errorf("segment: patch write footer: %w", err)
	}
	if _, err := f.WriteAt(headerBytes, 0); err != nil {
		return PatchResult{}, fmt.Errorf("segment: patch write header: %w", err)
	}
	if err := simulatePatchCrash(opts, CrashPointPatchTempWritten); err != nil {
		return PatchResult{}, err
	}
	if err := syncFile(f); err != nil {
		return PatchResult{}, fmt.Errorf("segment: patch fsync tmp: %w", err)
	}
	if err := simulatePatchCrash(opts, CrashPointPatchTempSynced); err != nil {
		return PatchResult{}, err
	}
	if err := f.Close(); err != nil {
		return PatchResult{}, fmt.Errorf("segment: patch close tmp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return PatchResult{}, fmt.Errorf("segment: patch rename: %w", err)
	}
	if err := simulatePatchCrash(opts, CrashPointPatchRenamed); err != nil {
		return PatchResult{}, err
	}
	if err := syncParentDir(path); err != nil {
		return PatchResult{}, err
	}
	if err := simulatePatchCrash(opts, CrashPointPatchDirSynced); err != nil {
		return PatchResult{}, err
	}
	success = true

	return PatchResult{
		Patched:       true,
		RowsMutated:   rowsMutated,
		BlocksTouched: blocksTouched,
		NewChecksum:   checksum,
		Header:        newHeader,
	}, nil
}

// eventGuard is a snapshot of every Event field a Patch mutate is forbidden
// to touch. Comparing it before/after the mutate call is the tripwire that
// keeps the verbatim-copied footer (blooms, collection index) and the
// carried-over witnessed/seq envelope honest. UpstreamRelayCursor is
// deliberately unguarded: the block format does not persist it (event.go), so
// no mutation of it can reach disk.
type eventGuard struct {
	seq         uint64
	witnessedAt int64
	kind        Kind
	did         string
	collection  string
	rkey        string
	rev         string
	payloadLen  int
	payloadHash uint64
}

func guardSnapshot(ev *Event) eventGuard {
	return eventGuard{
		seq:         ev.Seq,
		witnessedAt: ev.WitnessedAt,
		kind:        ev.Kind,
		did:         ev.DID,
		collection:  ev.Collection,
		rkey:        ev.Rkey,
		rev:         ev.Rev,
		payloadLen:  len(ev.Payload),
		payloadHash: xxh3.Hash(ev.Payload),
	}
}

// verify reports the first forbidden field mutate changed. Strings compare
// cheaply (they are short repo/collection identifiers). Payload is checked by
// length and an allocation-free xxh3 of its bytes: it is documented read-only
// DAG-CBOR (event.go) that Patch re-encodes verbatim, so an equal-length
// content mutation must still be rejected — the uncompressed-size invariant
// only catches length changes, and hashing avoids a per-row string copy on the
// bulk-rewrite path.
func (g eventGuard) verify(ev *Event) error {
	switch {
	case ev.Seq != g.seq:
		return fmt.Errorf("%w: mutate changed Seq %d->%d", ErrInvalidConfig, g.seq, ev.Seq)
	case ev.WitnessedAt != g.witnessedAt:
		return fmt.Errorf("%w: mutate changed WitnessedAt %d->%d", ErrInvalidConfig, g.witnessedAt, ev.WitnessedAt)
	case ev.Kind != g.kind:
		return fmt.Errorf("%w: mutate changed Kind %d->%d", ErrInvalidConfig, g.kind, ev.Kind)
	case ev.DID != g.did:
		return fmt.Errorf("%w: mutate changed DID", ErrInvalidConfig)
	case ev.Collection != g.collection:
		return fmt.Errorf("%w: mutate changed Collection", ErrInvalidConfig)
	case ev.Rkey != g.rkey:
		return fmt.Errorf("%w: mutate changed Rkey", ErrInvalidConfig)
	case ev.Rev != g.rev:
		return fmt.Errorf("%w: mutate changed Rev", ErrInvalidConfig)
	case len(ev.Payload) != g.payloadLen:
		return fmt.Errorf("%w: mutate changed Payload length %d->%d", ErrInvalidConfig, g.payloadLen, len(ev.Payload))
	case xxh3.Hash(ev.Payload) != g.payloadHash:
		return fmt.Errorf("%w: mutate changed Payload content", ErrInvalidConfig)
	}
	return nil
}

func simulatePatchCrash(opts PatchOptions, point string) error {
	if opts.CrashInjector == nil {
		return nil
	}
	return opts.CrashInjector.SimulateCrash(context.Background(), point)
}
