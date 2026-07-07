package segment

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	// The default number of events to store in a single segment block. This
	// value is just the default, and is configurable by the user.
	DefaultMaxEventsPerBlock = 4096

	// ReservedHeaderBytes is the size of the fixed header at the start
	// of every segment file (docs/README.md §3.1.2). The first len(segmentMagic)
	// bytes are the magic; the remainder is zero-filled placeholder until
	// Seal populates the finalized header. ReservedHeaderBytes is also
	// the byte offset at which the framed-block region begins.
	//
	// Exported so callers that own the active-segment lifecycle (e.g.
	// internal/ingest) can compute byte offsets without duplicating the
	// constant.
	ReservedHeaderBytes = 256
)

// segmentMagic is written at offset 0 of every segment file at
// creation time; it identifies the file as a jetstream segment.
var segmentMagic = []byte("jss0")

// Config controls writer behavior. Path is required.
type Config struct {
	// Path is the segment file to write. Required.
	Path string

	// MaxEventsPerBlock triggers a "block full" signal from Append.
	// Default DefaultMaxEventsPerBlock. Must be >= 1.
	MaxEventsPerBlock int

	// Metrics is optional; nil disables segment-package metrics
	// (e.g. seal duration).
	Metrics SealObserver

	// IOFaultInjector is a test-only seam for deterministic write/fsync
	// failures. Nil in production.
	IOFaultInjector IOFaultInjector
}

func (c Config) validate() error {
	if c.Path == "" {
		return fmt.Errorf("%w: Path is required", ErrInvalidConfig)
	}
	if c.MaxEventsPerBlock < 0 {
		return fmt.Errorf("%w: MaxEventsPerBlock must be >= 0", ErrInvalidConfig)
	}
	// The decoder enforces maxBlockEventsLimit on read; refuse a writer
	// that could produce blocks the same package cannot read back.
	if c.MaxEventsPerBlock > maxBlockEventsLimit {
		return fmt.Errorf("%w: MaxEventsPerBlock %d exceeds decoder cap %d",
			ErrInvalidConfig, c.MaxEventsPerBlock, maxBlockEventsLimit)
	}
	return nil
}

// Writer encodes events into the active segment file. It is not
// safe for concurrent use; the caller serializes access.
type Writer struct {
	cfg     Config
	file    *os.File
	pending pendingBlock
	closed  bool

	// Reusable per-flush scratch buffers. Sizing them once on the
	// first flush avoids n-allocations-per-block on the hot path:
	//
	//   bodyScratch : uncompressed columnar block body
	//   wireScratch : the bytes handed to file.Write — laid out as
	//                 [8-byte LE compressed_len placeholder][zstd frame].
	//                 We encode zstd directly into wireScratch[8:] and
	//                 patch the length prefix in place once known,
	//                 avoiding a second buffer + memcpy of the frame.
	//
	// Each is reset to zero-length before reuse; capacity is
	// retained. They never escape the writer goroutine.
	bodyScratch []byte
	wireScratch []byte

	// stickyErr is latched the first time a flush write or fsync
	// fails. Once set, every subsequent Append/Flush returns it so
	// the caller cannot accidentally retry into a partially-durable
	// frame and produce duplicate rows on disk. The caller must
	// Close the writer and start over.
	stickyErr error

	// flushedBlocks carries one BlockInfo per block already written
	// and fsynced. Appended in flushLocked after the Write succeeds.
	// Cleared on Seal (the writer is consumed). Rebuilt at New()
	// time when resuming an existing active segment.
	flushedBlocks []BlockInfo

	// nextBlockOffset is the file offset at which the *next* flush
	// will write its 8-byte length prefix. Seeded from the file size
	// at New() time (after any torn-tail truncate); updated after
	// each successful flush by adding 8 + len(zstd frame).
	nextBlockOffset uint64

	nextPreparedBlockID  uint64
	nextPreparedCommitID uint64
	preparedOutstanding  uint64
}

// PreparedBlock is an encoded-but-not-yet-durable segment block.
// Callers may compress Body outside the Writer critical section, then
// call CommitPreparedFlush in original block order. A PreparedBlock
// is single-use and must not be modified after PrepareFlush returns.
type PreparedBlock struct {
	Body      []byte
	info      BlockInfo
	owner     *Writer
	id        uint64
	committed bool
}

// MaxSeq returns the highest seq in this prepared block.
func (p *PreparedBlock) MaxSeq() uint64 {
	if p == nil {
		return 0
	}
	return p.info.MaxSeq
}

// New opens or creates the active segment at cfg.Path. See package
// godoc for resumption and rejection semantics.
func New(cfg Config) (*Writer, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	if cfg.MaxEventsPerBlock == 0 {
		cfg.MaxEventsPerBlock = DefaultMaxEventsPerBlock
	}

	// Open-or-create. We want O_RDWR because we both read the magic
	// from offset 0 (when the file pre-existed) and append new
	// blocks at end-of-file.
	f, err := os.OpenFile(cfg.Path, os.O_RDWR|os.O_CREATE, 0o644)
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

	if info.Size() == 0 {
		if err := initializeNewSegment(f, cfg); err != nil {
			// Unlink the still-empty file: a surviving 0-byte segment is a
			// permanent recovery wedge (resume and the manifest loader both
			// reject it as corrupt on every subsequent boot). Nothing durable
			// is lost — the file never held a byte of data.
			_ = os.Remove(cfg.Path)
			return nil, err
		}
		// On POSIX filesystems, the directory entry that names a freshly
		// created file is not durable until the parent directory itself
		// is fsynced. Without this, a crash immediately after creation
		// can drop the entire segment file even though we fsynced its
		// contents — violating the "no data loss" invariant in §2 of
		// the spec.
		if err := syncParentDir(cfg.Path, cfg.IOFaultInjector); err != nil {
			return nil, err
		}
	} else {
		if err := resumeExistingSegment(f, info.Size(), cfg.Path); err != nil {
			return nil, err
		}
	}

	w := &Writer{cfg: cfg, file: f}
	w.pending.preallocate(cfg.MaxEventsPerBlock)

	endInfo, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("segment: stat after open: %w", err)
	}
	w.nextBlockOffset = uint64(endInfo.Size())
	if endInfo.Size() > int64(ReservedHeaderBytes) {
		walk, walkErr := walkActiveFrames(f, endInfo.Size())
		if walkErr != nil {
			return nil, walkErr
		}
		// walk.infos already carries every field we care about
		// because the seal walk populates the witnessed_at bounds.
		w.flushedBlocks = walk.infos
	}

	success = true
	return w, nil
}

// syncParentDir opens and fsyncs the parent directory of path so
// the dirent for a freshly-created or truncated file is durable.
// On filesystems where this is a no-op (e.g. some Windows configs)
// the call is still cheap.
func syncParentDir(path string, faults IOFaultInjector) error {
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("segment: open parent dir: %w", err)
	}
	defer func() { _ = dir.Close() }()
	if faults != nil {
		if err := faults.BeforeSegmentIO(path, IOOpSync); err != nil {
			return fmt.Errorf("segment: fsync parent dir: %w", err)
		}
	}
	if err := syncFile(dir); err != nil {
		return fmt.Errorf("segment: fsync parent dir: %w", err)
	}
	return nil
}

// initializeNewSegment writes the fixed reserved header to a
// brand-new (zero-length) segment file and fsyncs it. The header
// is segmentMagic followed by enough zero-filled padding to reach
// ReservedHeaderBytes total. The returned error is already wrapped
// for the caller; the caller is responsible for closing f on
// failure.
func initializeNewSegment(f *os.File, cfg Config) error {
	header := make([]byte, ReservedHeaderBytes)
	copy(header, segmentMagic)

	if err := cfg.beforeIO(IOOpWrite); err != nil {
		return fmt.Errorf("segment: write header: %w", err)
	}
	if _, err := f.Write(header); err != nil {
		return fmt.Errorf("segment: write header: %w", err)
	}

	if err := cfg.beforeIO(IOOpSync); err != nil {
		return fmt.Errorf("segment: fsync header: %w", err)
	}
	if err := syncFile(f); err != nil {
		return fmt.Errorf("segment: fsync header: %w", err)
	}

	return nil
}

func (c Config) beforeIO(op IOOp) error {
	return beforeSegmentIO(c.IOFaultInjector, c.Path, op)
}

// resumeExistingSegment validates that f is a well-formed segment
// file, truncates any torn tail past the last fully-durable block,
// and positions the file offset at end-of-data so the next Write
// extends the segment in place. Without the truncate, a crash
// mid-Write leaves bytes past the last good frame that a future
// recovery walker would interpret as a malformed block, masking
// everything after it. The caller is responsible for closing f on
// failure.
func resumeExistingSegment(f *os.File, size int64, path string) error {
	if size < ReservedHeaderBytes {
		return fmt.Errorf("%w: %s is %d bytes",
			ErrCorruptSegment, path, size)
	}

	head := make([]byte, len(segmentMagic))
	if _, err := f.ReadAt(head, 0); err != nil {
		return fmt.Errorf("segment: read magic: %w", err)
	}
	if !bytes.Equal(head, segmentMagic) {
		return fmt.Errorf("%w: %s: bad magic %q", ErrCorruptSegment, path, head)
	}

	// Sealed-vs-active detection: bytes 4..11 are zero on an active
	// file (initializeNewSegment writes only the magic into the
	// reserved 256-byte header) and non-zero on a sealed file (Seal
	// patches in the xxh3 checksum). docs/README.md §3.1.2 names this the
	// "checksum at offset 4" signal; spec §8 documents the convention.
	var checksumBuf [8]byte
	if _, err := f.ReadAt(checksumBuf[:], 4); err != nil {
		return fmt.Errorf("segment: read checksum: %w", err)
	}
	if binary.LittleEndian.Uint64(checksumBuf[:]) != 0 {
		return fmt.Errorf("%w: %s", ErrSegmentSealed, path)
	}

	end, err := lastGoodOffset(f, size)
	if err != nil {
		return err
	}
	if end < size {
		if err := f.Truncate(end); err != nil {
			return fmt.Errorf("segment: truncate torn tail: %w", err)
		}
		// fsync the truncate so a second crash before any further
		// writes cannot resurrect the torn bytes. The file's metadata
		// (size) lives in the inode, so file Sync is the right scope
		// here; we don't need a directory fsync because the dirent
		// already exists.
		if err := syncFile(f); err != nil {
			return fmt.Errorf("segment: fsync truncate: %w", err)
		}
	}
	if _, err := f.Seek(end, io.SeekStart); err != nil {
		return fmt.Errorf("segment: seek end: %w", err)
	}
	return nil
}

// lastGoodOffset walks the framed-block region of an active segment
// file (everything after the 256-byte reserved header) and returns
// the byte offset of the end of the last fully-readable
// [uint64 LE compressed_len][zstd frame] pair. If the tail is torn
// (length prefix promises more bytes than the file holds, or the
// length prefix itself is truncated), that tail is reported as
// recoverable bytes-to-discard via the difference between the
// returned offset and size.
//
// We only check framing here; we do not decompress or decode the
// frames themselves. A frame whose bytes are all present but whose
// zstd payload was corrupted in flight is left in place — the
// reader path is responsible for surfacing that as decode errors.
// This keeps recovery O(blocks) rather than O(uncompressed bytes).
func lastGoodOffset(f *os.File, size int64) (int64, error) {
	off := int64(ReservedHeaderBytes)
	var lenBuf [8]byte
	for off < size {
		if size-off < int64(len(lenBuf)) {
			// Torn length prefix.
			return off, nil
		}
		if _, err := f.ReadAt(lenBuf[:], off); err != nil {
			return 0, fmt.Errorf("segment: read frame length at %d: %w", off, err)
		}
		frameLen := binary.LittleEndian.Uint64(lenBuf[:])
		next := off + int64(len(lenBuf)) + int64(frameLen)
		if frameLen > uint64(size-off-int64(len(lenBuf))) || next < off {
			// frame_len overruns the file (torn frame body) or
			// integer-overflows on extremely hostile input.
			return off, nil
		}
		off = next
	}
	return off, nil
}

// pendingBlock is the in-memory accumulator for the active block.
// Per the spec §3.2 columnar layout: parallel column slices, not a
// []Event, so steady-state Append has zero allocations once the
// underlying arrays grow once. Every slice is reset via s = s[:0]
// on flush to retain capacity.
type pendingBlock struct {
	seq         []uint64
	witnessedAt []int64
	indexedAt   []int64
	kind        []uint8
	collLen     []uint8
	didLen      []uint16
	rkeyLen     []uint8
	revLen      []uint8
	eventLen    []uint32

	collections []byte
	dids        []byte
	rkeys       []byte
	revs        []byte
	payloads    []byte

	// pendingBounds is the running min/max of seq and witnessed_at
	// across the events currently buffered. Reset on flushLocked
	// after the BlockInfo for this block is finalized.
	pendingBounds blockBounds
	sawAny        bool
}

// blockBounds is the running per-block summary tracked incrementally
// by Append so flushLocked can finalize a BlockInfo without
// re-walking the events.
type blockBounds struct {
	minSeq, maxSeq                 uint64
	minWitnessedAt, maxWitnessedAt int64
}

// count returns the number of events currently buffered. All column
// slices share this length by construction (Append updates them
// together).
func (p *pendingBlock) count() int { return len(p.seq) }

// preallocate sizes every column slice up front so steady-state
// Append never reallocates a column. Capacity for the variable-
// length blob buffers is sized from typical atproto event shapes
// (collection ~24 B, did ~32 B, rkey/rev ~13 B, payload ~512 B);
// over- or under-shooting only changes the first few Append calls'
// growth pattern — append still amortizes cleanly.
func (p *pendingBlock) preallocate(cap int) {
	p.seq = make([]uint64, 0, cap)
	p.witnessedAt = make([]int64, 0, cap)
	p.indexedAt = make([]int64, 0, cap)
	p.kind = make([]uint8, 0, cap)
	p.collLen = make([]uint8, 0, cap)
	p.didLen = make([]uint16, 0, cap)
	p.rkeyLen = make([]uint8, 0, cap)
	p.revLen = make([]uint8, 0, cap)
	p.eventLen = make([]uint32, 0, cap)
	p.collections = make([]byte, 0, cap*24)
	p.dids = make([]byte, 0, cap*32)
	p.rkeys = make([]byte, 0, cap*13)
	p.revs = make([]byte, 0, cap*13)
	p.payloads = make([]byte, 0, cap*512)
}

// reset truncates every column slice to zero length while retaining
// capacity. Callers use this after a successful flush.
func (p *pendingBlock) reset() {
	p.seq = p.seq[:0]
	p.witnessedAt = p.witnessedAt[:0]
	p.indexedAt = p.indexedAt[:0]
	p.kind = p.kind[:0]
	p.collLen = p.collLen[:0]
	p.didLen = p.didLen[:0]
	p.rkeyLen = p.rkeyLen[:0]
	p.revLen = p.revLen[:0]
	p.eventLen = p.eventLen[:0]
	p.collections = p.collections[:0]
	p.dids = p.dids[:0]
	p.rkeys = p.rkeys[:0]
	p.revs = p.revs[:0]
	p.payloads = p.payloads[:0]
	p.pendingBounds = blockBounds{}
	p.sawAny = false
}

// Close flushes any pending block and closes the file. Idempotent.
func (w *Writer) Close() error {
	if w.closed {
		return nil
	}
	if w.stickyErr != nil {
		return w.stickyErr
	}
	if w.preparedOutstanding > 0 {
		return fmt.Errorf("segment: close with %d uncommitted prepared block(s)", w.preparedOutstanding)
	}
	w.closed = true
	// Flush is a no-op while pending is empty; the unconditional call
	// keeps the implementation honest about durability when Close is
	// called with buffered events.
	flushErr := w.flushLocked()
	closeErr := w.file.Close()
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

// pendingBlock satisfies the columns interface (defined in block.go)
// so flushLocked can encode without materializing []Event.
//
// The variable-length blob accessors are AppendXxx — they copy the
// writer's contiguous buffer wholesale. Per-event Collection(i)/DID(i)/
// etc. accessors would have to walk the length column 0..i to compute
// each row's offset, making a single encode O(n²); appending the entire
// variable region per column keeps it O(n).

func (p *pendingBlock) Len() int                { return p.count() }
func (p *pendingBlock) Seq(i int) uint64        { return p.seq[i] }
func (p *pendingBlock) WitnessedAt(i int) int64 { return p.witnessedAt[i] }
func (p *pendingBlock) IndexedAt(i int) int64   { return p.indexedAt[i] }
func (p *pendingBlock) Kind(i int) uint8        { return p.kind[i] }

func (p *pendingBlock) CollectionLen(i int) uint8 { return p.collLen[i] }
func (p *pendingBlock) DIDLen(i int) uint16       { return p.didLen[i] }
func (p *pendingBlock) RkeyLen(i int) uint8       { return p.rkeyLen[i] }
func (p *pendingBlock) RevLen(i int) uint8        { return p.revLen[i] }
func (p *pendingBlock) PayloadLen(i int) uint32   { return p.eventLen[i] }

func (p *pendingBlock) AppendCollections(dst []byte) []byte { return append(dst, p.collections...) }
func (p *pendingBlock) AppendDIDs(dst []byte) []byte        { return append(dst, p.dids...) }
func (p *pendingBlock) AppendRkeys(dst []byte) []byte       { return append(dst, p.rkeys...) }
func (p *pendingBlock) AppendRevs(dst []byte) []byte        { return append(dst, p.revs...) }
func (p *pendingBlock) AppendPayloads(dst []byte) []byte    { return append(dst, p.payloads...) }

func (p *pendingBlock) TotalCollectionsLen() int { return len(p.collections) }
func (p *pendingBlock) TotalDIDsLen() int        { return len(p.dids) }
func (p *pendingBlock) TotalRkeysLen() int       { return len(p.rkeys) }
func (p *pendingBlock) TotalRevsLen() int        { return len(p.revs) }
func (p *pendingBlock) TotalPayloadsLen() int    { return len(p.payloads) }

// Flush encodes the pending block, writes it to the file as
// [uint64 LE compressed_len][zstd frame], and fsyncs before
// returning. No-op if the pending buffer is empty.
func (w *Writer) Flush() error {
	if w.closed {
		return ErrClosed
	}
	if w.stickyErr != nil {
		return w.stickyErr
	}
	return w.flushLocked()
}

// flushLocked is the flush body shared by Flush and Close.
//
// Durability contract: once Write returns success, the bytes are
// in the kernel's page cache and a subsequent recovery walker will
// see the frame on the file (per lastGoodOffset). It is therefore
// not safe to "retry" the same pending buffer on a Sync failure —
// doing so would write the frame twice. We instead reset pending
// immediately after Write succeeds, then attempt Sync; if Sync
// fails we latch stickyErr so the caller cannot Append further
// without observing the failure.
//
// We must also refuse to do anything once stickyErr is set: a
// previous Write failure may have partially written a torn frame
// to disk, in which case re-encoding the (still-buffered) events
// here would append duplicate bytes after the torn tail. Close
// reaches us via flushLocked too, so without this guard a
// Close-after-Flush-failure would silently corrupt the file.
func (w *Writer) flushLocked() error {
	if w.stickyErr != nil {
		return w.stickyErr
	}
	if w.pending.count() == 0 {
		return nil
	}

	// Reuse the scratch buffers across flushes. encodeBlockInto and
	// zstd.EncodeAll both grow their dst slice as needed; we only
	// need to reset length to zero between calls.
	w.bodyScratch = encodeBlockInto(w.bodyScratch[:0], &w.pending)

	// Lay out the wire frame as [uint64 LE compressed_len][frame] in
	// a single buffer so we issue one Write — a partial-write tear
	// then leaves us at most one torn frame at the tail, which the
	// next New() call will truncate via lastGoodOffset.
	//
	// We encode the zstd frame directly into wireScratch[8:] and
	// patch the length prefix in place once it's known, which saves
	// the second-buffer-plus-memcpy the previous design needed.
	w.wireScratch = append(w.wireScratch[:0], 0, 0, 0, 0, 0, 0, 0, 0)
	w.wireScratch = blockEncoder.EncodeAll(w.bodyScratch, w.wireScratch)
	binary.LittleEndian.PutUint64(w.wireScratch[:8], uint64(len(w.wireScratch)-8))

	if err := w.cfg.beforeIO(IOOpWrite); err != nil {
		w.stickyErr = fmt.Errorf("segment: write block: %w", err)
		return w.stickyErr
	}
	if _, err := w.file.Write(w.wireScratch); err != nil {
		w.stickyErr = fmt.Errorf("segment: write block: %w", err)
		return w.stickyErr
	}
	// Write succeeded: the bytes are owned by the file. Snapshot the
	// BlockInfo before resetting the pending buffer; reset() zeroes
	// pendingBounds.
	info := BlockInfo{
		Offset:           w.nextBlockOffset,
		CompressedSize:   uint32(len(w.wireScratch) - 8),
		UncompressedSize: uint32(len(w.bodyScratch)),
		EventCount:       uint32(w.pending.count()),
		MinSeq:           w.pending.pendingBounds.minSeq,
		MaxSeq:           w.pending.pendingBounds.maxSeq,
		MinWitnessedAt:   w.pending.pendingBounds.minWitnessedAt,
		MaxWitnessedAt:   w.pending.pendingBounds.maxWitnessedAt,
	}
	w.flushedBlocks = append(w.flushedBlocks, info)
	w.nextBlockOffset += uint64(len(w.wireScratch))

	// Drop the pending buffer so a Sync failure cannot re-encode the
	// same rows on a retry.
	w.pending.reset()

	if err := w.cfg.beforeIO(IOOpSync); err != nil {
		w.stickyErr = fmt.Errorf("segment: fsync block: %w", err)
		return w.stickyErr
	}
	if err := syncFile(w.file); err != nil {
		w.stickyErr = fmt.Errorf("segment: fsync block: %w", err)
		return w.stickyErr
	}
	return nil
}

func (w *Writer) prepareFlushLocked(dst []byte) (*PreparedBlock, error) {
	if w.stickyErr != nil {
		return nil, w.stickyErr
	}
	if w.pending.count() == 0 {
		return nil, nil
	}

	body := encodeBlockInto(dst, &w.pending)
	info := BlockInfo{
		UncompressedSize: uint32(len(body)),
		EventCount:       uint32(w.pending.count()),
		MinSeq:           w.pending.pendingBounds.minSeq,
		MaxSeq:           w.pending.pendingBounds.maxSeq,
		MinWitnessedAt:   w.pending.pendingBounds.minWitnessedAt,
		MaxWitnessedAt:   w.pending.pendingBounds.maxWitnessedAt,
	}
	w.pending.reset()
	prepared := &PreparedBlock{
		Body:  body,
		info:  info,
		owner: w,
		id:    w.nextPreparedBlockID,
	}
	w.nextPreparedBlockID++
	w.preparedOutstanding++
	return prepared, nil
}

// PrepareFlush detaches the current pending block for out-of-band
// compression. It is a no-op when no events are pending.
//
// The caller must later call CommitPreparedFlush for every non-nil
// PreparedBlock, in the same order PrepareFlush returned them, before
// closing or sealing the writer. Dropping a prepared block would drop
// memory-only rows whose metadata has not yet become durable.
func (w *Writer) PrepareFlush() (*PreparedBlock, error) {
	return w.prepareFlushLocked(nil)
}

// CompressPreparedBlock zstd-compresses a prepared block body. The returned
// frame does not include the segment file's 8-byte length prefix.
func CompressPreparedBlock(prepared *PreparedBlock) []byte {
	if prepared == nil {
		return nil
	}
	return blockEncoder.EncodeAll(prepared.Body, nil)
}

// CommitPreparedFlush writes a previously prepared and compressed block frame
// to the active segment, fsyncs it, and updates the writer's flushed block
// index. Prepared blocks must be committed in their original prepare order.
func (w *Writer) CommitPreparedFlush(prepared *PreparedBlock, frame []byte) error {
	if prepared == nil {
		return nil
	}
	wire := make([]byte, 8, 8+len(frame))
	wire = append(wire, frame...)
	return w.commitPreparedFlushLocked(prepared, wire)
}

func (w *Writer) commitPreparedFlushLocked(prepared *PreparedBlock, wire []byte) error {
	if w.stickyErr != nil {
		return w.stickyErr
	}
	if prepared == nil {
		return nil
	}
	if prepared.owner != w {
		return fmt.Errorf("segment: prepared block belongs to a different writer")
	}
	if prepared.committed {
		return fmt.Errorf("segment: prepared block already committed")
	}
	if prepared.id != w.nextPreparedCommitID {
		return fmt.Errorf("segment: prepared block order violation: got %d, want %d",
			prepared.id, w.nextPreparedCommitID)
	}
	binary.LittleEndian.PutUint64(wire[:8], uint64(len(wire)-8))
	if err := w.cfg.beforeIO(IOOpWrite); err != nil {
		w.stickyErr = fmt.Errorf("segment: write block: %w", err)
		return w.stickyErr
	}
	if _, err := w.file.Write(wire); err != nil {
		w.stickyErr = fmt.Errorf("segment: write block: %w", err)
		return w.stickyErr
	}
	info := prepared.info
	info.Offset = w.nextBlockOffset
	info.CompressedSize = uint32(len(wire) - 8)
	w.flushedBlocks = append(w.flushedBlocks, info)
	w.nextBlockOffset += uint64(len(wire))

	if err := w.cfg.beforeIO(IOOpSync); err != nil {
		w.stickyErr = fmt.Errorf("segment: fsync block: %w", err)
		return w.stickyErr
	}
	if err := syncFile(w.file); err != nil {
		w.stickyErr = fmt.Errorf("segment: fsync block: %w", err)
		return w.stickyErr
	}
	prepared.committed = true
	w.nextPreparedCommitID++
	w.preparedOutstanding--
	return nil
}

// Append validates ev and splits it into the pending block's column
// slices. The returned bool is true when the pending block has
// reached MaxEventsPerBlock and the caller must Flush before the
// next Append. Calling Append past Cap() returns ErrBufferFull and
// leaves the buffer unchanged.
func (w *Writer) Append(ev Event) (full bool, err error) {
	if w.closed {
		return false, ErrClosed
	}
	if w.stickyErr != nil {
		return false, w.stickyErr
	}
	if w.pending.count() >= w.cfg.MaxEventsPerBlock {
		return false, ErrBufferFull
	}
	if err := ValidateEvent(ev); err != nil {
		return false, err
	}

	p := &w.pending
	p.seq = append(p.seq, ev.Seq)
	p.witnessedAt = append(p.witnessedAt, ev.WitnessedAt)
	p.indexedAt = append(p.indexedAt, ev.IndexedAt)
	p.kind = append(p.kind, uint8(ev.Kind))
	p.collLen = append(p.collLen, uint8(len(ev.Collection)))
	p.didLen = append(p.didLen, uint16(len(ev.DID)))
	p.rkeyLen = append(p.rkeyLen, uint8(len(ev.Rkey)))
	p.revLen = append(p.revLen, uint8(len(ev.Rev)))
	p.eventLen = append(p.eventLen, uint32(len(ev.Payload)))
	p.collections = append(p.collections, ev.Collection...)
	p.dids = append(p.dids, ev.DID...)
	p.rkeys = append(p.rkeys, ev.Rkey...)
	p.revs = append(p.revs, ev.Rev...)
	p.payloads = append(p.payloads, ev.Payload...)

	if !p.sawAny {
		p.pendingBounds.minSeq = ev.Seq
		p.pendingBounds.maxSeq = ev.Seq
		p.pendingBounds.minWitnessedAt = ev.WitnessedAt
		p.pendingBounds.maxWitnessedAt = ev.WitnessedAt
		p.sawAny = true
	} else {
		if ev.Seq < p.pendingBounds.minSeq {
			p.pendingBounds.minSeq = ev.Seq
		}
		if ev.Seq > p.pendingBounds.maxSeq {
			p.pendingBounds.maxSeq = ev.Seq
		}
		if ev.WitnessedAt < p.pendingBounds.minWitnessedAt {
			p.pendingBounds.minWitnessedAt = ev.WitnessedAt
		}
		if ev.WitnessedAt > p.pendingBounds.maxWitnessedAt {
			p.pendingBounds.maxWitnessedAt = ev.WitnessedAt
		}
	}

	return p.count() >= w.cfg.MaxEventsPerBlock, nil
}

// Pending returns the number of events buffered but not yet flushed.
func (w *Writer) Pending() int { return w.pending.count() }

// SnapshotPending returns a copy of every event currently buffered in
// the active block (not yet flushed to disk). Used by the lookback
// replay engine in internal/subscribe to bridge from on-disk events
// to live events without forcing a flush (which would create user-driven
// fsync pressure on every cursor connection).
//
// Each returned Event has its variable-length fields (DID, Collection,
// Rkey, Rev, Payload) copied out of the writer's column buffers so
// the snapshot is safe to retain across subsequent Append calls — a
// later Append may grow and reslice the underlying buffer, leaving
// any aliased pointers dangling.
//
// Like every Writer method, SnapshotPending is not safe for concurrent
// use with Append/Flush/Seal/Close. The caller already serializes
// access (in production via internal/ingest.Writer.mu).
func (w *Writer) SnapshotPending() []Event {
	n := w.pending.count()
	if n == 0 {
		return nil
	}
	out := make([]Event, n)
	p := &w.pending

	// Walk the variable-length blob columns alongside the per-event
	// length columns so we can slice each event's bytes out by running
	// offset rather than re-summing lengths from 0..i for every event.
	var collOff, didOff, rkeyOff, revOff int
	var payloadOff uint64
	for i := range n {
		collN := int(p.collLen[i])
		didN := int(p.didLen[i])
		rkeyN := int(p.rkeyLen[i])
		revN := int(p.revLen[i])
		payloadN := uint64(p.eventLen[i])

		out[i] = Event{
			Seq:         p.seq[i],
			WitnessedAt: p.witnessedAt[i],
			IndexedAt:   p.indexedAt[i],
			Kind:        Kind(p.kind[i]),
			DID:         string(p.dids[didOff : didOff+didN]),
			Collection:  string(p.collections[collOff : collOff+collN]),
			Rkey:        string(p.rkeys[rkeyOff : rkeyOff+rkeyN]),
			Rev:         string(p.revs[revOff : revOff+revN]),
			Payload:     append([]byte(nil), p.payloads[payloadOff:payloadOff+payloadN]...),
		}

		collOff += collN
		didOff += didN
		rkeyOff += rkeyN
		revOff += revN
		payloadOff += payloadN
	}
	return out
}

// Cap returns Config.MaxEventsPerBlock.
func (w *Writer) Cap() int { return w.cfg.MaxEventsPerBlock }

// Blocks returns a snapshot of the blocks already flushed to disk.
// The pending in-memory block is deliberately excluded — its bytes
// are not yet on disk, so its Offset would be a lie. Callers that
// need bounds for in-flight events can wait for the next Flush.
//
// On a writer that has been Sealed (or has not yet flushed any
// block), Blocks returns nil. The caller can range over a nil
// slice safely; it never needs an explicit nil check.
//
// Like every Writer method, Blocks is not safe for concurrent use
// with Append / Flush / Seal / Close. The caller already serializes
// access to the writer.
func (w *Writer) Blocks() []BlockInfo {
	if w.closed {
		return nil
	}
	if len(w.flushedBlocks) == 0 {
		return nil
	}
	out := make([]BlockInfo, len(w.flushedBlocks))
	copy(out, w.flushedBlocks)
	return out
}
