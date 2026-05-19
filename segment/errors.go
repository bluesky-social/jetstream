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

	// ErrChecksumMismatch is returned by Reader.Open when the file's
	// xxh3 checksum disagrees with the value in its fixed header. The
	// likely contributing factors are bit rot, a partial CDN download,
	// or replication corruption.
	ErrChecksumMismatch = errors.New("segment: checksum mismatch")

	// ErrInvalidFooter is returned by Reader.Open when the variable-
	// length footer fails structural validation: a section length
	// would overrun the file, internal pointers don't agree, or a
	// length-prefixed sub-region is truncated.
	ErrInvalidFooter = errors.New("segment: invalid footer")

	// ErrInvalidBlockIndex is returned by Reader.Open when a block
	// index entry's offset/size pair doesn't fit within the file.
	ErrInvalidBlockIndex = errors.New("segment: invalid block index")

	// ErrBlockOutOfRange is returned by Reader.DecodeBlock,
	// Reader.BlockBloom, and Reader.BlockCollections when the requested
	// block index is past BlockCount.
	ErrBlockOutOfRange = errors.New("segment: block index out of range")
)
