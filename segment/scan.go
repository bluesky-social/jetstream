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
// returns (0, false, nil).
//
// Under the 1-based seq design (§R8) the first real event is seq 1 and
// seq 0 is the reserved "nothing yet" sentinel, never a stored event
// value. The bool therefore disambiguates an empty active segment
// (0, false) from a real seq envelope; callers must still gate
// forward-correction (e.g. flooring nextSeq to maxSeq+1) on found=true
// rather than on maxSeq>0, since a recovered segment's max is always >=1.
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
		if len(events) == 0 {
			return maxSeq, found, nil
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

// WalkActive opens path as an active (unsealed) segment file and
// decodes every flushed block in order, calling fn with the events
// in each block. Used by the lookback replay engine to read the
// unsealed tail without forcing a flush.
//
// Differs from Reader.Open + DecodeBlock in that it does not require
// the fixed header to be finalized; it walks the 8-byte length
// prefixes directly from offset ReservedHeaderBytes forward via the
// existing walkActiveFrames helper.
//
// Halts on the first error returned from fn. Returns os.PathError
// (so os.IsNotExist works) if path does not exist.
func WalkActive(path string, fn func([]Event) error) error {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return err // pass through os.PathError; caller may errors.Is
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("segment: stat %s: %w", path, err)
	}
	walk, err := walkActiveFrames(f, info.Size())
	if err != nil {
		return fmt.Errorf("segment: walk active frames: %w", err)
	}

	for i, b := range walk.infos {
		frame := make([]byte, b.CompressedSize)
		if _, err := f.ReadAt(frame, int64(b.Offset)+8); err != nil {
			return fmt.Errorf("segment: read active block %d: %w", i, err)
		}
		events, _, err := decodeBlockCompressedSized(frame)
		if err != nil {
			return fmt.Errorf("segment: decode active block %d: %w", i, err)
		}
		if len(events) == 0 {
			return nil
		}
		if err := fn(events); err != nil {
			return err
		}
	}
	return nil
}
