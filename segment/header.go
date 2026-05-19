package segment

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/zeebo/xxh3"
)

// Header is the parsed form of the 256-byte fixed header at offset 0
// of every sealed segment file. See DESIGN.md §3.1.2 for the wire
// layout, and the spec §5.1 for byte-offset details.
//
// All offsets are absolute file offsets.
type Header struct {
	Version               uint16
	BlockCount            uint32
	EventCount            uint32
	UniqueDIDCount        uint32
	MinSeq, MaxSeq        uint64
	MinIndexedAt          int64 // unix micros
	MaxIndexedAt          int64 // unix micros
	FooterOffset          uint64
	DIDBloomOffset        uint64
	BlockDIDBloomOffset   uint64
	CollectionIndexOffset uint64
	BlockIndexOffset      uint64
	Checksum              uint64
}

// currentHeaderVersion is the only version produced by this package.
// decodeHeader accepts only this exact version; older or newer files
// land via a future migration path.
const currentHeaderVersion uint16 = 1

// encodeHeader serializes h into a freshly-allocated 256-byte slice.
// Bytes 0..3 are the magic; bytes 4..11 are the checksum; bytes
// 12..97 are the metadata fields; bytes 98..255 are zero-filled
// reserved padding.
func encodeHeader(h Header) []byte {
	buf := make([]byte, ReservedHeaderBytes)
	copy(buf[0:4], segmentMagic)
	le := binary.LittleEndian
	le.PutUint64(buf[4:12], h.Checksum)
	le.PutUint16(buf[12:14], h.Version)
	le.PutUint32(buf[14:18], h.BlockCount)
	le.PutUint32(buf[18:22], h.EventCount)
	le.PutUint32(buf[22:26], h.UniqueDIDCount)
	le.PutUint64(buf[26:34], h.MinSeq)
	le.PutUint64(buf[34:42], h.MaxSeq)
	le.PutUint64(buf[42:50], uint64(h.MinIndexedAt))
	le.PutUint64(buf[50:58], uint64(h.MaxIndexedAt))
	le.PutUint64(buf[58:66], h.FooterOffset)
	le.PutUint64(buf[66:74], h.DIDBloomOffset)
	le.PutUint64(buf[74:82], h.BlockDIDBloomOffset)
	le.PutUint64(buf[82:90], h.CollectionIndexOffset)
	le.PutUint64(buf[90:98], h.BlockIndexOffset)
	// buf[98:256] is left zero — reserved for future expansion.
	return buf
}

// decodeHeader parses a 256-byte slice into Header. It validates:
//   - length == ReservedHeaderBytes
//   - magic == "jss0"
//   - checksum field is non-zero (zero means "active file"; our
//     contract is that decodeHeader is only ever fed sealed-file bytes)
//   - version == currentHeaderVersion
//
// On any failure decodeHeader returns a sentinel-wrapped error.
func decodeHeader(buf []byte) (Header, error) {
	if len(buf) != ReservedHeaderBytes {
		return Header{}, fmt.Errorf("%w: header is %d bytes, want %d",
			ErrInvalidFooter, len(buf), ReservedHeaderBytes)
	}
	if !bytes.Equal(buf[0:4], segmentMagic) {
		return Header{}, fmt.Errorf("%w: bad magic %q",
			ErrCorruptSegment, buf[0:4])
	}
	le := binary.LittleEndian
	checksum := le.Uint64(buf[4:12])
	if checksum == 0 {
		return Header{}, fmt.Errorf("%w: checksum is zero (active file?)",
			ErrCorruptSegment)
	}
	version := le.Uint16(buf[12:14])
	if version != currentHeaderVersion {
		return Header{}, fmt.Errorf("%w: header version %d, want %d",
			ErrInvalidFooter, version, currentHeaderVersion)
	}
	h := Header{
		Version:               version,
		BlockCount:            le.Uint32(buf[14:18]),
		EventCount:            le.Uint32(buf[18:22]),
		UniqueDIDCount:        le.Uint32(buf[22:26]),
		MinSeq:                le.Uint64(buf[26:34]),
		MaxSeq:                le.Uint64(buf[34:42]),
		MinIndexedAt:          int64(le.Uint64(buf[42:50])),
		MaxIndexedAt:          int64(le.Uint64(buf[50:58])),
		FooterOffset:          le.Uint64(buf[58:66]),
		DIDBloomOffset:        le.Uint64(buf[66:74]),
		BlockDIDBloomOffset:   le.Uint64(buf[74:82]),
		CollectionIndexOffset: le.Uint64(buf[82:90]),
		BlockIndexOffset:      le.Uint64(buf[90:98]),
		Checksum:              checksum,
	}
	return h, nil
}

// xxh3HeaderFooter computes xxh3 over headerBytes[12:] followed by
// footerBytes — i.e. version through end-of-footer per DESIGN.md
// §3.1.2. The magic and the checksum field itself are excluded so a
// reader can verify the file's integrity without first knowing the
// checksum value.
//
// Callers pass headerBytes with the checksum field zeroed; the
// computed value is what they store back into bytes 4..11 of the
// finalized header.
func xxh3HeaderFooter(headerBytes, footerBytes []byte) uint64 {
	h := xxh3.New()
	_, _ = h.Write(headerBytes[12:])
	_, _ = h.Write(footerBytes)
	return h.Sum64()
}
