// Package identity caches atmos identity resolutions in pebble under
// sync/identity/<did>. Each entry stores an 8-byte big-endian Unix-nanosecond
// expiry followed by JSON. Expired or undecodable entries are misses and are
// replaced on resolution.
//
// The persistent cache avoids repeating millions of PLC lookups when the
// in-memory LRU is lost on restart. It has no count-based LRU: maintaining
// one would add read-modify-write operations to the hot path, contrary to the
// identity.Cache contract.
package identity
