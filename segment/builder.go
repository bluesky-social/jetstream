package segment

import "fmt"

// BlockBuilder accumulates events into one in-memory columnar block
// (docs/README.md §3.2) and encodes it into a compressed block frame. It
// performs no I/O: Writer uses one to build the blocks it appends to the
// active file, and callers that persist blocks somewhere other than a
// local file use one directly.
//
// A BlockBuilder is not safe for concurrent use.
type BlockBuilder struct {
	maxEvents int
	pending   pendingBlock

	// bodyScratch holds the uncompressed columnar body between encode
	// and compress. Reused across blocks so steady-state encoding does
	// not allocate one body per block; it never escapes the builder.
	bodyScratch []byte
}

// NewBlockBuilder returns an empty builder that reports a block full at
// maxEventsPerBlock events. Zero selects DefaultMaxEventsPerBlock. The
// limit must not exceed what the block decoder accepts, so every block
// a builder produces can be read back.
func NewBlockBuilder(maxEventsPerBlock int) (*BlockBuilder, error) {
	if maxEventsPerBlock < 0 {
		return nil, fmt.Errorf("%w: maxEventsPerBlock must be >= 0", ErrInvalidConfig)
	}
	if maxEventsPerBlock > maxBlockEventsLimit {
		return nil, fmt.Errorf("%w: maxEventsPerBlock %d exceeds decoder cap %d",
			ErrInvalidConfig, maxEventsPerBlock, maxBlockEventsLimit)
	}
	if maxEventsPerBlock == 0 {
		maxEventsPerBlock = DefaultMaxEventsPerBlock
	}
	b := &BlockBuilder{}
	b.init(maxEventsPerBlock)
	return b, nil
}

// init sizes an already-validated builder in place. Writer embeds its
// builder by value and calls this instead of NewBlockBuilder.
func (b *BlockBuilder) init(maxEvents int) {
	b.maxEvents = maxEvents
	b.pending.preallocate(maxEvents)
}

// Append validates ev and splits it into the pending block's column
// slices. The returned bool is true when the block has reached its
// event limit and the caller must encode it before the next Append.
// Calling Append on a full builder returns ErrBufferFull and leaves the
// block unchanged. An invalid event (see ValidateEvent) is rejected
// without being buffered.
func (b *BlockBuilder) Append(ev Event) (full bool, err error) {
	return b.appendEvent(&ev)
}

// appendEvent is Append by pointer, so Writer.Append does not copy the
// Event a second time on the hot path.
func (b *BlockBuilder) appendEvent(ev *Event) (full bool, err error) {
	if b.pending.count() >= b.maxEvents {
		return false, ErrBufferFull
	}
	if err := validateEvent(ev); err != nil {
		return false, err
	}

	p := &b.pending
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

	return p.count() >= b.maxEvents, nil
}

// Len returns the number of events buffered in the pending block.
func (b *BlockBuilder) Len() int { return b.pending.count() }

// Cap returns the event count at which Append reports the block full.
func (b *BlockBuilder) Cap() int { return b.maxEvents }

// PendingBounds returns the pending block's event count and its seq and
// witnessed_at bounds. Offset and the size fields are zero because the
// block has not been encoded. ok is false when nothing is buffered.
func (b *BlockBuilder) PendingBounds() (info BlockInfo, ok bool) {
	if !b.pending.sawAny {
		return BlockInfo{}, false
	}
	return b.pendingInfo(), true
}

// pendingInfo is the BlockInfo fields known before encoding.
func (b *BlockBuilder) pendingInfo() BlockInfo {
	return BlockInfo{
		EventCount:     uint32(b.pending.count()),
		MinSeq:         b.pending.pendingBounds.minSeq,
		MaxSeq:         b.pending.pendingBounds.maxSeq,
		MinWitnessedAt: b.pending.pendingBounds.minWitnessedAt,
		MaxWitnessedAt: b.pending.pendingBounds.maxWitnessedAt,
	}
}

// Snapshot returns a copy of every buffered event. Each Event's
// variable-length fields are copied out of the builder's column
// buffers, so the snapshot stays valid across later Append calls (which
// may grow and reslice those buffers) and across Encode.
func (b *BlockBuilder) Snapshot() []Event {
	n := b.pending.count()
	if n == 0 {
		return nil
	}
	out := make([]Event, n)
	p := &b.pending

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

// Encode encodes and zstd-compresses the pending block, resets the
// builder, and returns the block frame with its BlockInfo. The frame is
// a single zstd frame without the segment file's 8-byte length prefix,
// and it is freshly allocated, so the caller owns it. info.Offset is
// zero: only the caller knows where the frame will live. Encode returns
// a nil frame when nothing is buffered.
func (b *BlockBuilder) Encode() (frame []byte, info BlockInfo) {
	if b.pending.count() == 0 {
		return nil, BlockInfo{}
	}
	frame, info = b.appendFrame(nil)
	b.reset()
	return frame, info
}

// appendFrame compresses the pending block and appends the zstd frame to
// dst without resetting the builder, so Writer can lay the frame out
// behind its own length prefix in a reused buffer and reset only once
// the write lands. info.CompressedSize is the appended frame's length.
// The caller must ensure Len() > 0.
func (b *BlockBuilder) appendFrame(dst []byte) ([]byte, BlockInfo) {
	// Reuse the scratch body across blocks. encodeBlockInto and
	// zstd.EncodeAll both grow their dst slice as needed; we only need
	// to reset length to zero between calls.
	b.bodyScratch = encodeBlockInto(b.bodyScratch[:0], &b.pending)
	start := len(dst)
	dst = blockEncoder.EncodeAll(b.bodyScratch, dst)
	info := b.pendingInfo()
	info.CompressedSize = uint32(len(dst) - start)
	info.UncompressedSize = uint32(len(b.bodyScratch))
	return dst, info
}

// prepare encodes the pending block's uncompressed body into dst and
// resets the builder. Unlike appendFrame the body is not scratch: it
// escapes into a PreparedBlock that is compressed later, outside the
// caller's critical section. The caller must ensure Len() > 0.
func (b *BlockBuilder) prepare(dst []byte) ([]byte, BlockInfo) {
	body := encodeBlockInto(dst, &b.pending)
	info := b.pendingInfo()
	info.UncompressedSize = uint32(len(body))
	b.reset()
	return body, info
}

// reset drops the pending block while retaining column capacity.
func (b *BlockBuilder) reset() { b.pending.reset() }

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
	// across the events currently buffered. Reset by BlockBuilder.reset
	// after the BlockInfo for this block is finalized.
	pendingBounds blockBounds
	sawAny        bool
}

// blockBounds is the running per-block summary tracked incrementally
// by Append so the builder can finalize a BlockInfo without
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

// pendingBlock satisfies the columns interface (defined in block.go)
// so the builder can encode without materializing []Event.
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
