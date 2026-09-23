//go:build darwin

package store

import "github.com/cockroachdb/pebble"

// SyncWrites uses pebble.NoSync on darwin to avoid per-write F_FULLFSYNC
// latency. WAL ordering is preserved and Close flushes it, but writes can be
// lost on a hard kill before Close.
//
// This weakens durability for macOS binaries as well as tests. Production
// targets Linux; see segment/sync_darwin.go before adding macOS production
// support. This variable is never reassigned; Go cannot declare pointer
// constants.
var SyncWrites = pebble.NoSync
