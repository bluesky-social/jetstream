package world

import (
	"crypto/sha256"
	"encoding/base32"
	"strings"
)

// b32 is the lowercase RFC 4648 base32 alphabet matching real
// did:plc identifiers. did:plc identifiers are 24 chars of base32-
// encoded SHA-256 truncated to 15 bytes — close enough that
// atmos.ParseDID accepts them.
var b32 = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

// didFromPubkey derives a "did:plc:<24 base32 chars>" identifier from
// a compressed secp256k1 public key. Real did:plc creation hashes a
// genesis operation; here we hash the pubkey bytes directly. The
// shape is what atproto code paths care about.
func didFromPubkey(pub []byte) string {
	sum := sha256.Sum256(pub)
	return "did:plc:" + strings.ToLower(b32.EncodeToString(sum[:15]))
}
