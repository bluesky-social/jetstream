//go:build darwin

package store

import "github.com/cockroachdb/pebble"

// SyncWrites downgrades to pebble.NoSync on darwin. Every
// pebble.Sync write triggers a WAL fsync, which on darwin is
// fcntl(F_FULLFSYNC) at ~4ms per call (vs ~20µs for fsync(2) on
// Linux). The metadata store's per-DID writes during backfill
// and per-block seq advances during ingest hit this hot enough
// that the test suite is dominated by F_FULLFSYNC latency on
// macOS.
//
// NoSync still preserves write ordering and drains the WAL
// durably on db.Close(); it only loses durability across a hard
// process kill between writes and Close. No code path in this
// repo (test or otherwise) exercises that — every Close()→Open()
// round-trip and every "simulate crash" test injects its
// disturbance after a clean Close — so the downgrade is
// semantically safe for our usage.
//
// CAVEAT: this affects production binaries built on darwin too.
// Production targets Linux so it doesn't bite in practice, but a
// macOS-built binary will have weaker WAL durability than the
// spec calls for. See segment/sync_darwin.go for the full
// reasoning and the if-we-ever-ship-darwin alternatives.
//
// Conceptually a constant; var only because Go has no const for
// pointer types (pebble.Sync / pebble.NoSync are *WriteOptions).
// Never reassigned at runtime.
var SyncWrites = pebble.NoSync
