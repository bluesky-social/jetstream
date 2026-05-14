package segment

import "errors"

// Sentinel errors. Callers compare with errors.Is.
var (
	// ErrInvalidConfig is returned by New when Config has unusable values.
	ErrInvalidConfig = errors.New("segment: invalid config")

	// ErrCorruptSegment is returned by New when an existing segment
	// file is smaller than the 256-byte reserved header region.
	ErrCorruptSegment = errors.New("segment: file is corrupt")

	// ErrSegmentSealed will be returned by New when the file carries
	// a sealed checksum trailer. The seal/unseal mechanism lands in a
	// later slice; the sentinel is reserved here so callers can already
	// switch on it.
	ErrSegmentSealed = errors.New("segment: file is already sealed")

	// ErrFieldTooLong is returned by Append when a string or Payload
	// field exceeds its on-disk column width.
	ErrFieldTooLong = errors.New("segment: event field exceeds column width")

	// ErrInvalidKind is returned by Append when ev.Kind is outside [1, 6].
	ErrInvalidKind = errors.New("segment: kind out of range")

	// ErrBufferFull is returned by Append when the pending block has
	// already reached MaxEventsPerBlock and the caller did not Flush
	// in response to an earlier "full" signal.
	ErrBufferFull = errors.New("segment: pending block is at capacity; flush required")

	// ErrClosed is returned by Append, Flush, and (re-)Close after Close.
	ErrClosed = errors.New("segment: writer is closed")
)
