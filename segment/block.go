package segment

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/klauspost/compress/zstd"
)

// validate checks that ev's fields fit the on-disk column widths and
// that Kind is in range. It performs no I/O and never panics.
func validate(ev Event) error {
	if ev.Kind < KindCreate || ev.Kind > KindSync {
		return fmt.Errorf("%w: %d", ErrInvalidKind, ev.Kind)
	}
	if len(ev.DID) > math.MaxUint16 {
		return fmt.Errorf("%w: did is %d bytes (max %d)",
			ErrFieldTooLong, len(ev.DID), math.MaxUint16)
	}
	if len(ev.Collection) > math.MaxUint8 {
		return fmt.Errorf("%w: collection is %d bytes (max %d)",
			ErrFieldTooLong, len(ev.Collection), math.MaxUint8)
	}
	if len(ev.Rkey) > math.MaxUint8 {
		return fmt.Errorf("%w: rkey is %d bytes (max %d)",
			ErrFieldTooLong, len(ev.Rkey), math.MaxUint8)
	}
	if len(ev.Rev) > math.MaxUint8 {
		return fmt.Errorf("%w: rev is %d bytes (max %d)",
			ErrFieldTooLong, len(ev.Rev), math.MaxUint8)
	}
	if len(ev.Payload) > math.MaxUint32 {
		return fmt.Errorf("%w: payload is %d bytes (max %d)",
			ErrFieldTooLong, len(ev.Payload), math.MaxUint32)
	}
	return nil
}

// columns is the small interface encodeBlockColumns reads through.
// It exists so the writer's pendingBlock (parallel slices) and the
// test/golden/fuzz path's []Event share one byte-layout
// implementation. Callers must guarantee Len() > 0 and that the
// per-event accessors return values within the on-disk column widths.
type columns interface {
	Len() int
	Seq(i int) uint64
	IndexedAt(i int) int64
	RenderedAt(i int) int64
	Kind(i int) uint8
	Collection(i int) string
	DID(i int) string
	Rkey(i int) string
	Rev(i int) string
	Payload(i int) []byte
}

// encodeBlockColumns writes the uncompressed columnar body for the
// given columns per DESIGN.md §3.2.
func encodeBlockColumns(c columns) []byte {
	n := c.Len()

	var totalCollLen, totalDIDLen, totalRkeyLen, totalRevLen, totalPayloadLen int
	for i := range n {
		totalCollLen += len(c.Collection(i))
		totalDIDLen += len(c.DID(i))
		totalRkeyLen += len(c.Rkey(i))
		totalRevLen += len(c.Rev(i))
		totalPayloadLen += len(c.Payload(i))
	}

	const fixedPerEvent = 8 + 8 + 8 + 1 + 1 + 2 + 1 + 1 + 4
	totalSize := 4 + n*fixedPerEvent +
		totalCollLen + totalDIDLen + totalRkeyLen + totalRevLen + totalPayloadLen

	buf := make([]byte, 0, totalSize)
	le := binary.LittleEndian

	buf = le.AppendUint32(buf, uint32(n))

	// Fixed-size columns, in spec order. One loop per column gives
	// the CPU a clean prefetch stride.
	for i := range n {
		buf = le.AppendUint64(buf, c.Seq(i))
	}
	for i := range n {
		buf = le.AppendUint64(buf, uint64(c.IndexedAt(i)))
	}
	for i := range n {
		buf = le.AppendUint64(buf, uint64(c.RenderedAt(i)))
	}
	for i := range n {
		buf = append(buf, c.Kind(i))
	}
	for i := range n {
		buf = append(buf, uint8(len(c.Collection(i))))
	}
	for i := range n {
		buf = le.AppendUint16(buf, uint16(len(c.DID(i))))
	}
	for i := range n {
		buf = append(buf, uint8(len(c.Rkey(i))))
	}
	for i := range n {
		buf = append(buf, uint8(len(c.Rev(i))))
	}
	for i := range n {
		buf = le.AppendUint32(buf, uint32(len(c.Payload(i))))
	}

	// Variable-length blobs, in spec order.
	for i := range n {
		buf = append(buf, c.Collection(i)...)
	}
	for i := range n {
		buf = append(buf, c.DID(i)...)
	}
	for i := range n {
		buf = append(buf, c.Rkey(i)...)
	}
	for i := range n {
		buf = append(buf, c.Rev(i)...)
	}
	for i := range n {
		buf = append(buf, c.Payload(i)...)
	}

	return buf
}

// eventColumns adapts []Event to the columns interface so encodeBlock
// shares one layout implementation with the writer's column path.
type eventColumns []Event

func (e eventColumns) Len() int                { return len(e) }
func (e eventColumns) Seq(i int) uint64        { return e[i].Seq }
func (e eventColumns) IndexedAt(i int) int64   { return e[i].IndexedAt }
func (e eventColumns) RenderedAt(i int) int64  { return e[i].RenderedAt }
func (e eventColumns) Kind(i int) uint8        { return uint8(e[i].Kind) }
func (e eventColumns) Collection(i int) string { return e[i].Collection }
func (e eventColumns) DID(i int) string        { return e[i].DID }
func (e eventColumns) Rkey(i int) string       { return e[i].Rkey }
func (e eventColumns) Rev(i int) string        { return e[i].Rev }
func (e eventColumns) Payload(i int) []byte    { return e[i].Payload }

// encodeBlock writes the uncompressed columnar body for the given
// events. Callers must pass at least one event.
func encodeBlock(events []Event) ([]byte, error) {
	if len(events) == 0 {
		return nil, fmt.Errorf("segment: encodeBlock called with zero events")
	}
	for i := range events {
		if err := validate(events[i]); err != nil {
			return nil, fmt.Errorf("event %d: %w", i, err)
		}
	}
	return encodeBlockColumns(eventColumns(events)), nil
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
const maxBlockEventsLimit = 1 << 24 // 16,777,216

// decodeBlock is the inverse of encodeBlock. It validates input
// length at every step so a malicious header cannot provoke an
// unbounded allocation.
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
		// encodeBlock refuses empty input; a zero-event block on the
		// wire is corruption.
		return nil, errTruncatedBlock
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
		if k < KindCreate || k > KindSync {
			return nil, errTruncatedBlock
		}
		events[i].Kind = k
	}

	// collection_len[]
	collLen, err := read(nEvents)
	if err != nil {
		return nil, err
	}

	// did_len[]
	chunk, err = read(nEvents * 2)
	if err != nil {
		return nil, err
	}
	didLen := make([]uint16, nEvents)
	for i := range nEvents {
		didLen[i] = le.Uint16(chunk[i*2 : i*2+2])
	}

	// rkey_len[]
	rkeyLen, err := read(nEvents)
	if err != nil {
		return nil, err
	}

	// rev_len[]
	revLen, err := read(nEvents)
	if err != nil {
		return nil, err
	}

	// event_len[]
	chunk, err = read(nEvents * 4)
	if err != nil {
		return nil, err
	}
	eventLen := make([]uint32, nEvents)
	for i := range nEvents {
		eventLen[i] = le.Uint32(chunk[i*4 : i*4+4])
	}

	// Variable-length blobs. Each is a single contiguous run; we
	// slice it into per-event substrings.
	readStringField := func(lengths func(i int) int) ([]string, error) {
		out := make([]string, nEvents)
		var total int
		for i := range nEvents {
			total += lengths(i)
		}
		blob, err := read(total)
		if err != nil {
			return nil, err
		}
		var cur int
		for i := range nEvents {
			n := lengths(i)
			out[i] = string(blob[cur : cur+n])
			cur += n
		}
		return out, nil
	}

	collections, err := readStringField(func(i int) int { return int(collLen[i]) })
	if err != nil {
		return nil, err
	}
	dids, err := readStringField(func(i int) int { return int(didLen[i]) })
	if err != nil {
		return nil, err
	}
	rkeys, err := readStringField(func(i int) int { return int(rkeyLen[i]) })
	if err != nil {
		return nil, err
	}
	revs, err := readStringField(func(i int) int { return int(revLen[i]) })
	if err != nil {
		return nil, err
	}

	// payloads[]: same shape but []byte, and Payload == nil for zero-length.
	var totalPayload int
	for i := range nEvents {
		totalPayload += int(eventLen[i])
	}
	payloadBlob, err := read(totalPayload)
	if err != nil {
		return nil, err
	}

	// Refuse trailing bytes. encodeBlock produces an exact-length
	// buffer; anything left is corruption.
	if off != len(buf) {
		return nil, errTruncatedBlock
	}

	var pcur int
	for i := range nEvents {
		events[i].Collection = collections[i]
		events[i].DID = dids[i]
		events[i].Rkey = rkeys[i]
		events[i].Rev = revs[i]
		n := int(eventLen[i])
		if n > 0 {
			// Copy so callers can't mutate the input by writing into
			// Payload, and so the decoded events outlive buf.
			p := make([]byte, n)
			copy(p, payloadBlob[pcur:pcur+n])
			events[i].Payload = p
		} else {
			events[i].Payload = nil
		}
		pcur += n
	}

	return events, nil
}

// blockEncoder is a process-wide reusable zstd encoder at the
// default level (klauspost's SpeedDefault, which is zstd level 3 —
// the format default). zstd.Encoder.EncodeAll is documented as
// safe for concurrent calls, which makes a single instance fine
// despite the package being single-writer.
//
// Construction takes a static, known-good option set; failure
// indicates a programming error (or a broken klauspost build) so
// we panic in init rather than make every caller check a sentinel.
var (
	blockEncoder *zstd.Encoder
	blockDecoder *zstd.Decoder
)

func init() {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderCRC(true),
	)
	if err != nil {
		panic(fmt.Sprintf("segment: zstd encoder init failed: %v", err))
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		panic(fmt.Sprintf("segment: zstd decoder init failed: %v", err))
	}
	blockEncoder = enc
	blockDecoder = dec
}

// encodeBlockCompressed encodes events with encodeBlock, then wraps
// the result in a single zstd frame with content checksums enabled.
func encodeBlockCompressed(events []Event) ([]byte, error) {
	body, err := encodeBlock(events)
	if err != nil {
		return nil, err
	}
	return blockEncoder.EncodeAll(body, nil), nil
}

// decodeBlockCompressed is the inverse: decompress, then decodeBlock.
func decodeBlockCompressed(frame []byte) ([]Event, error) {
	body, err := blockDecoder.DecodeAll(frame, nil)
	if err != nil {
		return nil, fmt.Errorf("segment: zstd decompress: %w", err)
	}
	return decodeBlock(body)
}
