package xrpcapi

import "fmt"

// checksumHex renders a segment's xxh3 checksum as a fixed 16-char hex
// string. This is the single source of truth for the wire representation:
// listSegments emits it as the `checksum` field and getSegment emits it
// (quoted) as the ETag, so a client can revalidate without a second fetch.
func checksumHex(checksum uint64) string {
	return fmt.Sprintf("%016x", checksum)
}
