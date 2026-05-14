package segment

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// DefaultMaxEventsPerBlock matches DESIGN.md §3.2.
const DefaultMaxEventsPerBlock = 4096

// reservedHeaderBytes is the 256-byte placeholder region at the
// start of an active segment file (DESIGN.md §3.1.2). It stays
// zero in this slice; the future Seal step writes the real header.
const reservedHeaderBytes = 256

// sealedMagic marks a sealed segment file. New rejects sealed files.
var sealedMagic = []byte("jss0")

// Config controls writer behavior. Path is required.
type Config struct {
	// Path is the segment file to write. Required.
	Path string

	// MaxEventsPerBlock triggers a "block full" signal from Append.
	// Default DefaultMaxEventsPerBlock. Must be >= 1.
	MaxEventsPerBlock int
}

func (c Config) validate() error {
	if c.Path == "" {
		return fmt.Errorf("%w: Path is required", ErrInvalidConfig)
	}
	if c.MaxEventsPerBlock < 0 {
		return fmt.Errorf("%w: MaxEventsPerBlock must be >= 0", ErrInvalidConfig)
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

	// stickyErr is latched the first time a flush write or fsync
	// fails. Once set, every subsequent Append/Flush returns it so
	// the caller cannot accidentally retry into a partially-durable
	// frame and produce duplicate rows on disk. The caller must
	// Close the writer and start over.
	stickyErr error
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

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("segment: stat %s: %w", cfg.Path, err)
	}

	if info.Size() == 0 {
		// Brand-new file: write 256 zero bytes for the reserved header.
		if _, err := f.Write(make([]byte, reservedHeaderBytes)); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("segment: write header: %w", err)
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("segment: fsync header: %w", err)
		}
	} else {
		if info.Size() < reservedHeaderBytes {
			_ = f.Close()
			return nil, fmt.Errorf("%w: %s is %d bytes",
				ErrCorruptSegment, cfg.Path, info.Size())
		}
		// Read the first 4 bytes to check for the sealed magic.
		head := make([]byte, len(sealedMagic))
		if _, err := f.ReadAt(head, 0); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("segment: read magic: %w", err)
		}
		if string(head) == string(sealedMagic) {
			_ = f.Close()
			return nil, fmt.Errorf("%w: %s", ErrSegmentSealed, cfg.Path)
		}
		// Active file: walk the framed-block region from offset
		// reservedHeaderBytes forward, find the last fully-durable
		// block boundary, and truncate any torn tail before
		// appending. Without this, a crash mid-Write leaves bytes
		// past the last good frame that a future recovery walker
		// would interpret as a malformed block, masking everything
		// after it.
		end, err := lastGoodOffset(f, info.Size())
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		if end < info.Size() {
			if err := f.Truncate(end); err != nil {
				_ = f.Close()
				return nil, fmt.Errorf("segment: truncate torn tail: %w", err)
			}
		}
		if _, err := f.Seek(end, io.SeekStart); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("segment: seek end: %w", err)
		}
	}

	return &Writer{cfg: cfg, file: f}, nil
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
	off := int64(reservedHeaderBytes)
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
// Per the spec §5.1: parallel column slices, not a []Event, so
// steady-state Append has zero allocations once the underlying
// arrays grow once. Every slice is reset via s = s[:0] on flush
// to retain capacity.
type pendingBlock struct {
	seq        []uint64
	indexedAt  []int64
	renderedAt []int64
	kind       []uint8
	collLen    []uint8
	didLen     []uint16
	rkeyLen    []uint8
	revLen     []uint8
	eventLen   []uint32

	collections []byte
	dids        []byte
	rkeys       []byte
	revs        []byte
	payloads    []byte
}

// count returns the number of events currently buffered. All column
// slices share this length by construction (Append updates them
// together).
func (p *pendingBlock) count() int { return len(p.seq) }

// reset truncates every column slice to zero length while retaining
// capacity. Callers use this after a successful flush.
func (p *pendingBlock) reset() {
	p.seq = p.seq[:0]
	p.indexedAt = p.indexedAt[:0]
	p.renderedAt = p.renderedAt[:0]
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
}

// Close flushes any pending block and closes the file. Idempotent.
func (w *Writer) Close() error {
	if w.closed {
		return nil
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

func (p *pendingBlock) Len() int               { return p.count() }
func (p *pendingBlock) Seq(i int) uint64       { return p.seq[i] }
func (p *pendingBlock) IndexedAt(i int) int64  { return p.indexedAt[i] }
func (p *pendingBlock) RenderedAt(i int) int64 { return p.renderedAt[i] }
func (p *pendingBlock) Kind(i int) uint8       { return p.kind[i] }

// The variable-length accessors slice into the contiguous byte
// buffers using the per-row length columns. Each accessor walks the
// length column from 0 to i to compute the offset of row i; this is
// O(i) per call, and encodeBlockColumns calls each accessor exactly
// once per row in column-major order, so a full encode is O(n²) in
// the lengths-summing cost. For 4096 events × 5 variable columns the
// cost is ~10⁵ trivial integer adds, dwarfed by the zstd compression
// that follows. Task 17's benchmarks measure this; if it's ever a
// real cost we replace the per-call sum with a prefix-sum maintained
// during Append.

func (p *pendingBlock) Collection(i int) string {
	var off int
	for j := 0; j < i; j++ {
		off += int(p.collLen[j])
	}
	return string(p.collections[off : off+int(p.collLen[i])])
}

func (p *pendingBlock) DID(i int) string {
	var off int
	for j := 0; j < i; j++ {
		off += int(p.didLen[j])
	}
	return string(p.dids[off : off+int(p.didLen[i])])
}

func (p *pendingBlock) Rkey(i int) string {
	var off int
	for j := 0; j < i; j++ {
		off += int(p.rkeyLen[j])
	}
	return string(p.rkeys[off : off+int(p.rkeyLen[i])])
}

func (p *pendingBlock) Rev(i int) string {
	var off int
	for j := 0; j < i; j++ {
		off += int(p.revLen[j])
	}
	return string(p.revs[off : off+int(p.revLen[i])])
}

func (p *pendingBlock) Payload(i int) []byte {
	var off int
	for j := 0; j < i; j++ {
		off += int(p.eventLen[j])
	}
	return p.payloads[off : off+int(p.eventLen[i])]
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
func (w *Writer) flushLocked() error {
	if w.pending.count() == 0 {
		return nil
	}

	body := encodeBlockColumns(&w.pending)
	frame := blockEncoder.EncodeAll(body, nil)

	// Frame the block as [uint64 LE compressed_len][frame] and
	// concatenate so we issue a single Write — a partial-write tear
	// then leaves us at most one torn frame at the tail, which the
	// next New() call will truncate via lastGoodOffset.
	combined := make([]byte, 8+len(frame))
	binary.LittleEndian.PutUint64(combined[:8], uint64(len(frame)))
	copy(combined[8:], frame)

	if _, err := w.file.Write(combined); err != nil {
		w.stickyErr = fmt.Errorf("segment: write block: %w", err)
		return w.stickyErr
	}
	// Write succeeded: the bytes are owned by the file. Drop the
	// pending buffer so a Sync failure cannot re-encode the same
	// rows on a retry.
	w.pending.reset()

	if err := w.file.Sync(); err != nil {
		w.stickyErr = fmt.Errorf("segment: fsync block: %w", err)
		return w.stickyErr
	}
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
	if err := validate(ev); err != nil {
		return false, err
	}

	p := &w.pending
	p.seq = append(p.seq, ev.Seq)
	p.indexedAt = append(p.indexedAt, ev.IndexedAt)
	p.renderedAt = append(p.renderedAt, ev.RenderedAt)
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

	return p.count() >= w.cfg.MaxEventsPerBlock, nil
}

// Pending returns the number of events buffered but not yet flushed.
func (w *Writer) Pending() int { return w.pending.count() }

// Cap returns Config.MaxEventsPerBlock.
func (w *Writer) Cap() int { return w.cfg.MaxEventsPerBlock }
