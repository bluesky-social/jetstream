package segment

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"

	"github.com/cockroachdb/pebble/vfs"
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

	// FS is the filesystem used for segment I/O. Nil uses the host OS
	// filesystem. Tests may pass a strict in-memory vfs.FS to model
	// written-vs-synced state.
	FS vfs.FS

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
	cfg    Config
	file   vfs.File
	closed bool

	// pending builds the active block in memory. It owns the reusable
	// uncompressed-body scratch buffer.
	pending BlockBuilder

	// wireScratch is the reusable buffer handed to file.Write, laid out
	// as [8-byte LE compressed_len placeholder][zstd frame]. We encode
	// zstd directly into wireScratch[8:] and patch the length prefix in
	// place once known, avoiding a second buffer + memcpy of the frame.
	// It is reset to zero-length before reuse; capacity is retained. It
	// never escapes the writer goroutine.
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
	f, err := openSegmentReadWrite(cfg.FS, cfg.Path)
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
			_ = removeSegmentFile(cfg.FS, cfg.Path)
			return nil, err
		}
		// On POSIX filesystems, the directory entry that names a freshly
		// created file is not durable until the parent directory itself
		// is fsynced. Without this, a crash immediately after creation
		// can drop the entire segment file even though we fsynced its
		// contents — violating the "no data loss" invariant in §2 of
		// the spec.
		if err := syncParentDirFS(cfg.FS, cfg.Path, cfg.IOFaultInjector); err != nil {
			return nil, err
		}
	} else {
		if err := resumeExistingSegment(cfg.FS, f, info.Size(), cfg.Path); err != nil {
			return nil, err
		}
	}

	w := &Writer{cfg: cfg, file: f}
	w.pending.init(cfg.MaxEventsPerBlock)

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

// initializeNewSegment writes the fixed reserved header to a
// brand-new (zero-length) segment file and fsyncs it. The header
// is segmentMagic followed by enough zero-filled padding to reach
// ReservedHeaderBytes total. The returned error is already wrapped
// for the caller; the caller is responsible for closing f on
// failure.
func initializeNewSegment(f vfs.File, cfg Config) error {
	header := make([]byte, ReservedHeaderBytes)
	copy(header, segmentMagic)

	if err := cfg.beforeIO(IOOpWrite); err != nil {
		return fmt.Errorf("segment: write header: %w", err)
	}
	if _, err := f.WriteAt(header, 0); err != nil {
		return fmt.Errorf("segment: write header: %w", err)
	}

	if err := cfg.beforeIO(IOOpSync); err != nil {
		return fmt.Errorf("segment: fsync header: %w", err)
	}
	if err := syncSegmentFile(cfg.FS, f); err != nil {
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
func resumeExistingSegment(fs vfs.FS, f vfs.File, size int64, path string) error {
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
		if err := truncateSegmentFile(fs, f, end); err != nil {
			return fmt.Errorf("segment: truncate torn tail: %w", err)
		}
		// fsync the truncate so a second crash before any further
		// writes cannot resurrect the torn bytes. The file's metadata
		// (size) lives in the inode, so file Sync is the right scope
		// here; we don't need a directory fsync because the dirent
		// already exists.
		if err := syncSegmentFile(fs, f); err != nil {
			return fmt.Errorf("segment: fsync truncate: %w", err)
		}
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
func lastGoodOffset(f io.ReaderAt, size int64) (int64, error) {
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
	// Flush buffered events before closing; an empty buffer is a no-op.
	flushErr := w.flushLocked()
	closeErr := w.file.Close()
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

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

// flushLocked serves Flush and Close. Reset pending after Write succeeds,
// before Sync: the frame is already in the page cache, so retrying after a
// sync failure would duplicate it. Latch sync errors to prevent later
// appends.
//
// A prior write failure may leave a torn frame. Once stickyErr is set, even
// Close must not append the still-buffered events after that tail.
func (w *Writer) flushLocked() error {
	if w.stickyErr != nil {
		return w.stickyErr
	}
	if w.pending.Len() == 0 {
		return nil
	}

	// Lay out the wire frame as [uint64 LE compressed_len][frame] in
	// a single buffer so we issue one Write — a partial-write tear
	// then leaves us at most one torn frame at the tail, which the
	// next New() call will truncate via lastGoodOffset.
	//
	// We encode the zstd frame directly into wireScratch[8:] and
	// patch the length prefix in place once it's known, which saves
	// the second-buffer-plus-memcpy the previous design needed.
	w.wireScratch = append(w.wireScratch[:0], 0, 0, 0, 0, 0, 0, 0, 0)
	var info BlockInfo
	w.wireScratch, info = w.pending.appendFrame(w.wireScratch)
	binary.LittleEndian.PutUint64(w.wireScratch[:8], uint64(len(w.wireScratch)-8))

	if err := w.cfg.beforeIO(IOOpWrite); err != nil {
		w.stickyErr = fmt.Errorf("segment: write block: %w", err)
		return w.stickyErr
	}
	if _, err := w.file.WriteAt(w.wireScratch, int64(w.nextBlockOffset)); err != nil {
		w.stickyErr = fmt.Errorf("segment: write block: %w", err)
		return w.stickyErr
	}
	// Write succeeded: the bytes are owned by the file.
	info.Offset = w.nextBlockOffset
	w.flushedBlocks = append(w.flushedBlocks, info)
	w.nextBlockOffset += uint64(len(w.wireScratch))

	// Drop the pending buffer so a Sync failure cannot re-encode the
	// same rows on a retry.
	w.pending.reset()

	if err := w.cfg.beforeIO(IOOpSync); err != nil {
		w.stickyErr = fmt.Errorf("segment: fsync block: %w", err)
		return w.stickyErr
	}
	if err := syncSegmentFile(w.cfg.FS, w.file); err != nil {
		w.stickyErr = fmt.Errorf("segment: fsync block: %w", err)
		return w.stickyErr
	}
	return nil
}

func (w *Writer) prepareFlushLocked(dst []byte) (*PreparedBlock, error) {
	if w.stickyErr != nil {
		return nil, w.stickyErr
	}
	if w.pending.Len() == 0 {
		return nil, nil
	}

	body, info := w.pending.prepare(dst)
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
	if _, err := w.file.WriteAt(wire, int64(w.nextBlockOffset)); err != nil {
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
	if err := syncSegmentFile(w.cfg.FS, w.file); err != nil {
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
	return w.pending.appendEvent(&ev)
}

// Pending returns the number of events buffered but not yet flushed.
func (w *Writer) Pending() int { return w.pending.Len() }

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
	return w.pending.Snapshot()
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

// TimeFloorSeq returns the first seq worth scanning for the earliest event
// whose witnessed_at is at least timeUS. For flushed data this is the MinSeq
// of the first candidate block; for the pending block it is that block's
// MinSeq. The caller must still inspect rows within the candidate block to
// find the exact boundary.
//
// The bool is false when no event currently buffered by this writer reaches
// timeUS. Like every Writer method, the caller must serialize this query
// against append, flush, seal, and close.
func (w *Writer) TimeFloorSeq(timeUS int64) (uint64, bool) {
	if w.closed {
		return 0, false
	}
	i := sort.Search(len(w.flushedBlocks), func(i int) bool {
		return w.flushedBlocks[i].MaxWitnessedAt >= timeUS
	})
	if i < len(w.flushedBlocks) {
		return w.flushedBlocks[i].MinSeq, true
	}
	if pending, ok := w.pending.PendingBounds(); ok && pending.MaxWitnessedAt >= timeUS {
		return pending.MinSeq, true
	}
	return 0, false
}

// FlushedRangeFromSeq returns the frame-aligned byte range covering flushed
// blocks from the first block whose MaxSeq is at least seq through the current
// flushed end. Pending in-memory rows are excluded. Like every Writer method,
// the caller must serialize this query against append, flush, seal, and close.
func (w *Writer) FlushedRangeFromSeq(seq uint64) (startOffset, endOffset uint64, ok bool) {
	if w.closed || len(w.flushedBlocks) == 0 {
		return 0, 0, false
	}
	i := sort.Search(len(w.flushedBlocks), func(i int) bool {
		return w.flushedBlocks[i].MaxSeq >= seq
	})
	if i == len(w.flushedBlocks) {
		return 0, 0, false
	}
	return w.flushedBlocks[i].Offset, w.nextBlockOffset, true
}
