package catalog

import "time"

// GenerationParts is everything the archive endpoints need to serve one
// sealed segment generation as a virtual file without a local copy
// (design §11.5): the header row, then per block an 8-byte little-endian
// length and the block object, then the footer object.
type GenerationParts struct {
	// Generation is the segment_generations ID the parts belong to.
	Generation uint64
	// Header is the ReservedHeaderBytes sealed header.
	Header    []byte
	CreatedAt time.Time
	// Blocks are the block objects in ordinal order. Each object is the
	// block's zstd frame without the file's length prefix.
	Blocks []ObjectPart
	Footer ObjectPart
}

// ObjectPart is one object of a generation and its byte length.
type ObjectPart struct {
	ID     uint64
	Length int64
}
