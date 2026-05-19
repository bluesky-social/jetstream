package segment

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
)

// ScanMaxSeq returns the maximum Seq value across all fully-durable
// blocks of an active segment file. The bool reports whether any
// events were observed; on an empty active segment (zero blocks) it
// returns (0, false, nil). The bool disambiguates "max is 0" from
// "no events" — seq=0 is a valid first-event value, so callers must
// gate forward-correction on found=true.
//
// Intended for crash recovery in callers that own the active-segment
// lifecycle (e.g. internal/ingest). The walk is bounded by
// lastGoodOffset semantics: torn tails are ignored. Returns
// ErrSegmentSealed if the file is sealed; sealed-file readers should
// use Reader instead.
func ScanMaxSeq(path string) (maxSeq uint64, found bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, false, fmt.Errorf("segment: scan open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return 0, false, fmt.Errorf("segment: scan stat: %w", err)
	}
	size := info.Size()
	if size < ReservedHeaderBytes {
		return 0, false, fmt.Errorf("%w: %s is %d bytes", ErrCorruptSegment, path, size)
	}

	var checksumBuf [8]byte
	if _, err := f.ReadAt(checksumBuf[:], 4); err != nil {
		return 0, false, fmt.Errorf("segment: scan read checksum: %w", err)
	}
	if binary.LittleEndian.Uint64(checksumBuf[:]) != 0 {
		return 0, false, fmt.Errorf("%w: %s", ErrSegmentSealed, path)
	}

	off := int64(ReservedHeaderBytes)
	var lenBuf [8]byte
	for off < size {
		if size-off < int64(len(lenBuf)) {
			return maxSeq, found, nil
		}
		if _, err := f.ReadAt(lenBuf[:], off); err != nil {
			return 0, false, fmt.Errorf("segment: scan read frame length at %d: %w", off, err)
		}
		frameLen := binary.LittleEndian.Uint64(lenBuf[:])
		// Reject frames that would extend past EOF (torn tail) or
		// that overflow int on the make() below. lastGoodOffset uses
		// the same torn-tail short-circuit semantics.
		remaining := uint64(size - off - int64(len(lenBuf)))
		if frameLen > remaining || frameLen > math.MaxInt {
			return maxSeq, found, nil
		}
		next := off + int64(len(lenBuf)) + int64(frameLen)

		frame := make([]byte, frameLen)
		if _, err := f.ReadAt(frame, off+int64(len(lenBuf))); err != nil {
			return 0, false, fmt.Errorf("segment: scan read frame body at %d: %w", off, err)
		}

		events, err := decodeBlockCompressed(frame)
		if err != nil {
			return 0, false, fmt.Errorf("segment: scan decode block at %d: %w", off, err)
		}
		for _, ev := range events {
			if !found || ev.Seq > maxSeq {
				maxSeq = ev.Seq
				found = true
			}
		}
		off = next
	}
	return maxSeq, found, nil
}
