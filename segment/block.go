package segment

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"unsafe"
)

// ValidateEvent checks that ev's fields fit the on-disk column widths
// and that Kind is in range. It performs no I/O and never panics.
//
// External-ingest callers use this before Append so malformed
// upstream data can be counted and skipped without touching the
// durable writer. Append still validates internally because segment
// is the final invariant boundary for on-disk data.
func ValidateEvent(ev Event) error {
	if !ev.Kind.Valid() {
		return fmt.Errorf("%w: %d", ErrInvalidKind, ev.Kind)
	}
	if len(ev.DID) > math.MaxUint16 {
		return fmt.Errorf("%w: did is %d bytes (max %d)", ErrFieldTooLong, len(ev.DID), math.MaxUint16)
	}
	if len(ev.Collection) > math.MaxUint8 {
		return fmt.Errorf("%w: collection is %d bytes (max %d)", ErrFieldTooLong, len(ev.Collection), math.MaxUint8)
	}
	if len(ev.Rkey) > math.MaxUint8 {
		return fmt.Errorf("%w: rkey is %d bytes (max %d)", ErrFieldTooLong, len(ev.Rkey), math.MaxUint8)
	}
	if len(ev.Rev) > math.MaxUint8 {
		return fmt.Errorf("%w: rev is %d bytes (max %d)", ErrFieldTooLong, len(ev.Rev), math.MaxUint8)
	}
	if len(ev.Payload) > math.MaxUint32 {
		return fmt.Errorf("%w: payload is %d bytes (max %d)", ErrFieldTooLong, len(ev.Payload), math.MaxUint32)
	}
	return nil
}

// columns is the small interface encodeBlockColumns reads through.
// It exists so the writer's pendingBlock (parallel slices) and the
// test/golden/fuzz path's []Event share one byte-layout
// implementation. Callers must guarantee Len() > 0 and that the
// per-event accessors return values within the on-disk column widths.
//
// The interface separates length accessors (O(1) for both
// implementations) from blob-append methods (O(total_bytes) for both,
// and crucially O(1) overhead per event for pendingBlock since its
// blobs are already contiguous []byte buffers — no prefix-sum walk).
type columns interface {
	Len() int
	Seq(i int) uint64
	IndexedAt(i int) int64
	RenderedAt(i int) int64
	Kind(i int) uint8

	CollectionLen(i int) uint8
	DIDLen(i int) uint16
	RkeyLen(i int) uint8
	RevLen(i int) uint8
	PayloadLen(i int) uint32

	AppendCollections(dst []byte) []byte
	AppendDIDs(dst []byte) []byte
	AppendRkeys(dst []byte) []byte
	AppendRevs(dst []byte) []byte
	AppendPayloads(dst []byte) []byte

	TotalCollectionsLen() int
	TotalDIDsLen() int
	TotalRkeysLen() int
	TotalRevsLen() int
	TotalPayloadsLen() int
}

// encodeBlockInto writes the uncompressed columnar body per
// DESIGN.md §3.2 by appending to dst, returning the grown buffer.
// Reusing a scratch dst across flushes is the writer's allocation
// avoidance strategy.
func encodeBlockInto(dst []byte, c columns) []byte {
	n := c.Len()

	const fixedPerEvent = 8 + 8 + 8 + 1 + 1 + 2 + 1 + 1 + 4
	totalSize := 4 + n*fixedPerEvent +
		c.TotalCollectionsLen() + c.TotalDIDsLen() +
		c.TotalRkeysLen() + c.TotalRevsLen() +
		c.TotalPayloadsLen()

	if cap(dst)-len(dst) < totalSize {
		grown := make([]byte, len(dst), len(dst)+totalSize)
		copy(grown, dst)
		dst = grown
	}

	le := binary.LittleEndian
	dst = le.AppendUint32(dst, uint32(n))

	// Fixed-size columns, in spec order.
	for i := range n {
		dst = le.AppendUint64(dst, c.Seq(i))
	}
	for i := range n {
		dst = le.AppendUint64(dst, uint64(c.IndexedAt(i)))
	}
	for i := range n {
		dst = le.AppendUint64(dst, uint64(c.RenderedAt(i)))
	}
	for i := range n {
		dst = append(dst, c.Kind(i))
	}
	for i := range n {
		dst = append(dst, c.CollectionLen(i))
	}
	for i := range n {
		dst = le.AppendUint16(dst, c.DIDLen(i))
	}
	for i := range n {
		dst = append(dst, c.RkeyLen(i))
	}
	for i := range n {
		dst = append(dst, c.RevLen(i))
	}
	for i := range n {
		dst = le.AppendUint32(dst, c.PayloadLen(i))
	}

	// Variable-length blobs, in spec order.
	dst = c.AppendCollections(dst)
	dst = c.AppendDIDs(dst)
	dst = c.AppendRkeys(dst)
	dst = c.AppendRevs(dst)
	dst = c.AppendPayloads(dst)

	return dst
}

// encodeBlockColumns is the dst-less convenience kept for the
// encodeBlock entry point used by tests/golden/fuzz where allocation
// rate isn't relevant. The hot path (writer.flushLocked) uses
// encodeBlockInto directly with a writer-owned scratch buffer.
func encodeBlockColumns(c columns) []byte {
	return encodeBlockInto(nil, c)
}

// eventColumns adapts []Event to the columns interface so encodeBlock
// shares one layout implementation with the writer's column path.
//
// All length accessors are O(1) (string len is a header field). Blob
// appenders iterate per-row, which for a 4096-event block costs n
// trivial calls — far below the prefix-sum cost the previous design
// incurred for the writer's pendingBlock path.
type eventColumns []Event

func (e eventColumns) Len() int                  { return len(e) }
func (e eventColumns) Seq(i int) uint64          { return e[i].Seq }
func (e eventColumns) IndexedAt(i int) int64     { return e[i].IndexedAt }
func (e eventColumns) RenderedAt(i int) int64    { return e[i].RenderedAt }
func (e eventColumns) Kind(i int) uint8          { return uint8(e[i].Kind) }
func (e eventColumns) CollectionLen(i int) uint8 { return uint8(len(e[i].Collection)) }
func (e eventColumns) DIDLen(i int) uint16       { return uint16(len(e[i].DID)) }
func (e eventColumns) RkeyLen(i int) uint8       { return uint8(len(e[i].Rkey)) }
func (e eventColumns) RevLen(i int) uint8        { return uint8(len(e[i].Rev)) }
func (e eventColumns) PayloadLen(i int) uint32   { return uint32(len(e[i].Payload)) }

func (e eventColumns) AppendCollections(dst []byte) []byte {
	for i := range e {
		dst = append(dst, e[i].Collection...)
	}
	return dst
}
func (e eventColumns) AppendDIDs(dst []byte) []byte {
	for i := range e {
		dst = append(dst, e[i].DID...)
	}
	return dst
}
func (e eventColumns) AppendRkeys(dst []byte) []byte {
	for i := range e {
		dst = append(dst, e[i].Rkey...)
	}
	return dst
}
func (e eventColumns) AppendRevs(dst []byte) []byte {
	for i := range e {
		dst = append(dst, e[i].Rev...)
	}
	return dst
}
func (e eventColumns) AppendPayloads(dst []byte) []byte {
	for i := range e {
		dst = append(dst, e[i].Payload...)
	}
	return dst
}

func (e eventColumns) TotalCollectionsLen() int {
	var t int
	for i := range e {
		t += len(e[i].Collection)
	}
	return t
}
func (e eventColumns) TotalDIDsLen() int {
	var t int
	for i := range e {
		t += len(e[i].DID)
	}
	return t
}
func (e eventColumns) TotalRkeysLen() int {
	var t int
	for i := range e {
		t += len(e[i].Rkey)
	}
	return t
}
func (e eventColumns) TotalRevsLen() int {
	var t int
	for i := range e {
		t += len(e[i].Rev)
	}
	return t
}
func (e eventColumns) TotalPayloadsLen() int {
	var t int
	for i := range e {
		t += len(e[i].Payload)
	}
	return t
}

// encodeBlock writes the uncompressed columnar body for the given
// events. Callers must pass at least one event; the compaction rewrite
// path uses encodeEmptyBlock for the explicit zero-event-block format.
// Events must be validated before this call.
func encodeBlock(events []Event) ([]byte, error) {
	if len(events) == 0 {
		return nil, fmt.Errorf("segment: encodeBlock called with zero events")
	}

	return encodeBlockColumns(eventColumns(events)), nil
}

func encodeEmptyBlock() []byte {
	return []byte{0, 0, 0, 0}
}

// errTruncatedBlock is the sentinel for any malformed uncompressed
// block body: short reads, length-column overflows, anything that
// would cause the decoder to read past the input. It stays
// unexported because the decoder itself stays unexported in this
// slice; the future Reader type will promote it to a public sentinel.
var errTruncatedBlock = errors.New("segment: truncated or malformed block")

// maxBlockEventsLimit is a hard ceiling on the event_count header.
// It is well above the configurable default (4096) but well below
// the int32 ceiling (2_147_483_647) that would otherwise trip
// int(nEvents64) on a 32-bit build. It also caps the up-front
// allocation a hostile header can force.
const maxBlockEventsLimit = 1 << 18 // 262,144

// decodeBlock is the inverse of encodeBlock. It validates input
// length at every step so a malicious header cannot provoke an
// unbounded allocation.
//
// Buffer-aliasing contract (callers MUST honor):
// the returned events alias buf for their string columns (DID,
// Collection, Rkey, Rev) and for Payload. Strings are immutable in
// Go; Payload is []byte by API necessity but is documented (event.go)
// as read-only DAG-CBOR record bytes — callers that need to mutate
// must clone first. If a caller later writes through buf they will
// observe the same write through the events. Both production call
// sites pass a freshly-allocated zstd output buffer that they never
// touch again, which makes the aliasing safe and saves five
// allocations plus a full copy of the variable region per block.
// On a 4096-event production block this is ~3 MB of garbage avoided
// per decoded block, which is meaningful at firehose throughput.
func decodeBlock(buf []byte) ([]Event, error) {
	const fixedPerEvent = 8 + 8 + 8 + 1 + 1 + 2 + 1 + 1 + 4

	if len(buf) < 4 {
		return nil, errTruncatedBlock
	}

	le := binary.LittleEndian
	nEvents64 := uint64(le.Uint32(buf[:4]))
	off := 4

	// Reject impossible event counts up front. We need at least
	// fixedPerEvent bytes per event before any variable-length
	// data; if the remaining input can't cover that, the header is
	// lying.
	if nEvents64 > maxBlockEventsLimit {
		return nil, errTruncatedBlock
	}
	if uint64(len(buf)-off) < nEvents64*fixedPerEvent {
		return nil, errTruncatedBlock
	}
	nEvents := int(nEvents64)

	if nEvents == 0 {
		if off != len(buf) {
			return nil, errTruncatedBlock
		}
		return []Event{}, nil
	}

	events := make([]Event, nEvents)

	// Helper: read N bytes starting at off, advance off, return
	// errTruncatedBlock if the input is too short.
	read := func(n int) ([]byte, error) {
		if off+n > len(buf) {
			return nil, errTruncatedBlock
		}
		s := buf[off : off+n]
		off += n
		return s, nil
	}

	// seq[]
	chunk, err := read(nEvents * 8)
	if err != nil {
		return nil, err
	}
	for i := range nEvents {
		events[i].Seq = le.Uint64(chunk[i*8 : i*8+8])
	}

	// indexed_at[]
	chunk, err = read(nEvents * 8)
	if err != nil {
		return nil, err
	}
	for i := range nEvents {
		events[i].IndexedAt = int64(le.Uint64(chunk[i*8 : i*8+8]))
	}

	// rendered_at[]
	chunk, err = read(nEvents * 8)
	if err != nil {
		return nil, err
	}
	for i := range nEvents {
		events[i].RenderedAt = int64(le.Uint64(chunk[i*8 : i*8+8]))
	}

	// kind[]
	chunk, err = read(nEvents)
	if err != nil {
		return nil, err
	}
	for i := range nEvents {
		k := Kind(chunk[i])
		if !k.Valid() {
			return nil, errTruncatedBlock
		}
		events[i].Kind = k
	}

	// collection_len[]
	collLenBytes, err := read(nEvents)
	if err != nil {
		return nil, err
	}

	// did_len[]
	didLenBytes, err := read(nEvents * 2)
	if err != nil {
		return nil, err
	}

	// rkey_len[]
	rkeyLenBytes, err := read(nEvents)
	if err != nil {
		return nil, err
	}

	// rev_len[]
	revLenBytes, err := read(nEvents)
	if err != nil {
		return nil, err
	}

	// event_len[]
	eventLenBytes, err := read(nEvents * 4)
	if err != nil {
		return nil, err
	}

	// Sum the variable-length blobs in a way that traps int overflow.
	// On 32-bit builds a hostile u32 length column could otherwise sum
	// past math.MaxInt and silently wrap before we feed it to read().
	totalCollLen, err := sumU8(collLenBytes)
	if err != nil {
		return nil, err
	}
	totalDIDLen, err := sumU16(didLenBytes)
	if err != nil {
		return nil, err
	}
	totalRkeyLen, err := sumU8(rkeyLenBytes)
	if err != nil {
		return nil, err
	}
	totalRevLen, err := sumU8(revLenBytes)
	if err != nil {
		return nil, err
	}
	totalPayloadLen, err := sumU32(eventLenBytes)
	if err != nil {
		return nil, err
	}

	collBlob, err := read(totalCollLen)
	if err != nil {
		return nil, err
	}
	didBlob, err := read(totalDIDLen)
	if err != nil {
		return nil, err
	}
	rkeyBlob, err := read(totalRkeyLen)
	if err != nil {
		return nil, err
	}
	revBlob, err := read(totalRevLen)
	if err != nil {
		return nil, err
	}
	payloadBlob, err := read(totalPayloadLen)
	if err != nil {
		return nil, err
	}

	// Refuse trailing bytes. encodeBlock produces an exact-length
	// buffer; anything left is corruption.
	if off != len(buf) {
		return nil, errTruncatedBlock
	}

	// Alias buf for the four immutable string columns. Per the
	// buffer-aliasing contract above, the returned events keep buf
	// alive but never copy from it for the string columns. Each
	// per-event Collection/DID/Rkey/Rev is a substring of the column
	// blob, which is itself a sub-string view of buf via unsafeBytesToString.
	collStr := unsafeBytesToString(collBlob)
	didStr := unsafeBytesToString(didBlob)
	rkeyStr := unsafeBytesToString(rkeyBlob)
	revStr := unsafeBytesToString(revBlob)

	// Decoded payloads alias the input buffer per the
	// buffer-aliasing contract above. Saves a memcpy of every
	// payload byte and one allocation of size sum(payload_len).
	payloadBacking := payloadBlob

	var collOff, didOff, rkeyOff, revOff, pOff int
	for i := range nEvents {
		cl := int(collLenBytes[i])
		dl := int(le.Uint16(didLenBytes[i*2 : i*2+2]))
		kl := int(rkeyLenBytes[i])
		vl := int(revLenBytes[i])
		pl := int(le.Uint32(eventLenBytes[i*4 : i*4+4]))

		events[i].Collection = collStr[collOff : collOff+cl]
		events[i].DID = didStr[didOff : didOff+dl]
		events[i].Rkey = rkeyStr[rkeyOff : rkeyOff+kl]
		events[i].Rev = revStr[revOff : revOff+vl]
		if pl > 0 {
			events[i].Payload = payloadBacking[pOff : pOff+pl : pOff+pl]
		} else {
			events[i].Payload = nil
		}

		collOff += cl
		didOff += dl
		rkeyOff += kl
		revOff += vl
		pOff += pl
	}

	return events, nil
}

// sumU8/sumU16/sumU32 sum the integer values packed in a []byte and
// return the result as an int. They reject inputs whose sum would
// overflow int (relevant on 32-bit builds where the u32 column for
// payload lengths could otherwise wrap silently). On any overflow
// they return errTruncatedBlock — the canonical "this block can't
// possibly be valid" sentinel.
func sumU8(b []byte) (int, error) {
	const max = int(^uint(0) >> 1)
	var t int
	for _, v := range b {
		nt := t + int(v)
		if nt < t || nt > max {
			return 0, errTruncatedBlock
		}
		t = nt
	}
	return t, nil
}

func sumU16(b []byte) (int, error) {
	const max = int(^uint(0) >> 1)
	le := binary.LittleEndian
	var t int
	for i := 0; i+2 <= len(b); i += 2 {
		nt := t + int(le.Uint16(b[i:i+2]))
		if nt < t || nt > max {
			return 0, errTruncatedBlock
		}
		t = nt
	}
	return t, nil
}

// unsafeBytesToString returns a string header that aliases b's
// backing array. The Go runtime treats the result as an immutable
// string for GC and == purposes — it is the standard idiom for
// avoiding a memcpy when the caller can prove b is never mutated
// after the call. decodeBlock's contract (see its docstring)
// ensures both production call sites satisfy that.
func unsafeBytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

func sumU32(b []byte) (int, error) {
	const max = int(^uint(0) >> 1)
	le := binary.LittleEndian
	var t int
	for i := 0; i+4 <= len(b); i += 4 {
		v := int64(le.Uint32(b[i : i+4]))
		nt64 := int64(t) + v
		if nt64 > int64(max) {
			return 0, errTruncatedBlock
		}
		t = int(nt64)
	}
	return t, nil
}
