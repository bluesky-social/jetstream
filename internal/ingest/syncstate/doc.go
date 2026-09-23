// Package syncstate persists verifier chain and hosting state in pebble under
// sync/chain/<did> and sync/host/<did>. Keeping this state across restart
// avoids treating the next event as an unverified starting point.
//
// Chain encoding v1 is a version byte, uvarint rev length, rev bytes, and a
// 36-byte CID. Hosting encoding v1 is a version byte, active byte,
// length-prefixed status, uint64 seq, and length-prefixed time. Readers
// reject unknown versions.
//
// Pending verifier writes are promoted after their event rows are appended
// and committed with the relay cursor after segment fsync. See
// PebbleStateStore for ordering and replay rules.
package syncstate
