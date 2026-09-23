package xrpcapi

import "fmt"

// checksumHex renders the 16-character xxh3 value used in archive checksum
// fields and quoted ETags.
func checksumHex(checksum uint64) string {
	return fmt.Sprintf("%016x", checksum)
}
