// Package identitycache persists atmos identity.Cache resolutions in
// pebble. It owns the key prefix "sync/identity/<did>" and stores
// the JSON-encoded *identity.Identity preceded by an 8-byte big-endian
// unix-nano expiry. Get treats expired or undecodable entries as
// cache misses so the next resolution overwrites the bad row.
//
// The pebble cache backstops atmos's in-memory LRU on the firehose
// hot path: a process restart loses LRU state and would otherwise
// replay millions of plc.directory lookups. Disk-resident cache
// hits stay sub-millisecond and survive restart, so the only cold
// path is "DID never seen before, by anyone, on this jetstream
// instance."
//
// We intentionally do NOT implement an LRU cap. The atproto network
// has tens of millions of DIDs, but the active set on a single
// jetstream instance is bounded by the firehose's per-second event
// rate. Pebble's natural compaction keeps the working set on disk
// modest, and a count-bound LRU would force read-modify-write cycles
// on the hot path that the identity.Cache contract explicitly avoids.
package identitycache
