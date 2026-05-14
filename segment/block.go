package segment

import (
	"encoding/binary"
	"fmt"
	"math"
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
	for i := 0; i < n; i++ {
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
	for i := 0; i < n; i++ {
		buf = le.AppendUint64(buf, c.Seq(i))
	}
	for i := 0; i < n; i++ {
		buf = le.AppendUint64(buf, uint64(c.IndexedAt(i)))
	}
	for i := 0; i < n; i++ {
		buf = le.AppendUint64(buf, uint64(c.RenderedAt(i)))
	}
	for i := 0; i < n; i++ {
		buf = append(buf, c.Kind(i))
	}
	for i := 0; i < n; i++ {
		buf = append(buf, uint8(len(c.Collection(i))))
	}
	for i := 0; i < n; i++ {
		buf = le.AppendUint16(buf, uint16(len(c.DID(i))))
	}
	for i := 0; i < n; i++ {
		buf = append(buf, uint8(len(c.Rkey(i))))
	}
	for i := 0; i < n; i++ {
		buf = append(buf, uint8(len(c.Rev(i))))
	}
	for i := 0; i < n; i++ {
		buf = le.AppendUint32(buf, uint32(len(c.Payload(i))))
	}

	// Variable-length blobs, in spec order.
	for i := 0; i < n; i++ {
		buf = append(buf, c.Collection(i)...)
	}
	for i := 0; i < n; i++ {
		buf = append(buf, c.DID(i)...)
	}
	for i := 0; i < n; i++ {
		buf = append(buf, c.Rkey(i)...)
	}
	for i := 0; i < n; i++ {
		buf = append(buf, c.Rev(i)...)
	}
	for i := 0; i < n; i++ {
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
