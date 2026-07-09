// Package zstddict parses the header of a structured zstd dictionary
// (RFC 8878 §5). Shared by the server (which embeds and serves the
// /subscribe-v2 dictionary) and the client (which fetches it and needs
// the ID for the zstdDictionary negotiation param).
package zstddict

import (
	"encoding/binary"
	"fmt"
)

// magic is the structured-dictionary magic number (RFC 8878 §5).
const magic = 0xEC30A437

// ParseID extracts the dictionary ID from a structured zstd dictionary.
// Returns an error for raw/content-only dictionaries (no header), which
// the jetstream wire contract does not use.
func ParseID(dict []byte) (uint32, error) {
	if len(dict) < 8 {
		return 0, fmt.Errorf("zstddict: %d bytes is too short for a structured dictionary header", len(dict))
	}
	if binary.LittleEndian.Uint32(dict[:4]) != magic {
		return 0, fmt.Errorf("zstddict: missing structured-dictionary magic")
	}
	id := binary.LittleEndian.Uint32(dict[4:8])
	if id == 0 {
		return 0, fmt.Errorf("zstddict: dictionary ID 0 is reserved")
	}
	return id, nil
}
