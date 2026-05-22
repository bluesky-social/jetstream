// Package syncstate persists atmos sync.StateStore in pebble. It owns
// the key prefixes "sync/chain/<did>" (per-DID rev + MST root for the
// last accepted commit) and "sync/host/<did>" (per-DID hosting status
// from the last accepted #account event). State survives restarts so
// the verifier doesn't accept the next event for each DID as ground
// truth after a process restart, the way MemStateStore does.
//
// Encoding is hand-rolled compact binary with a leading version byte:
//
//	chain state v1: [0x01][rev_len uvarint][rev bytes][cid_bytes 36B]
//	host state v1:  [0x01][active u8][status_len uvarint][status][seq u64][time_len uvarint][time]
//
// We don't reuse atmos's CBOR helpers (sync.ChainState / HostingState
// have none anyway) because the records are small and fixed-shape, and
// we want a schema we control. The version byte gives us a forward-
// compat exit hatch if atmos extends the types — readers refuse
// unknown versions rather than silently truncating.
//
// pebble.Sync is used on every Save: per-DID chain state is the
// verifier's source of truth, and a silent revert to a pre-crash value
// would create a chain break the verifier resolves by triggering a
// resync against the account's PDS. Resyncs are rate-limited per DID,
// so a flurry of unnecessary resyncs degrades the live archive.
package syncstate
