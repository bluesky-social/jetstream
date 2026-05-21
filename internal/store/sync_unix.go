//go:build !darwin

package store

import "github.com/cockroachdb/pebble"

// SyncWrites is the *pebble.WriteOptions every metadata write
// passes when it wants the DESIGN.md §3.1.1 durability anchor.
// On non-darwin platforms pebble.Sync issues a plain fsync(2),
// which is fast enough that no test-mode override is warranted.
//
// See sync_darwin.go for why darwin needs a different value.
//
// Conceptually this is a constant — pebble exports Sync/NoSync
// as package-level *WriteOptions singletons, and Go has no const
// for pointer types, so we use var. It is never reassigned at
// runtime; treat it as immutable.
var SyncWrites = pebble.Sync
