// Package live consumes com.atproto.sync.subscribeRepos into segment files:
// data/backfill/live_segments during bootstrap and data/segments in steady
// state.
//
// Consumer uses ingest.Writer for persistence. events.go converts upstream
// events to segment.Events. The writer's durable batch hook commits the relay
// cursor with seq/next after segment fsync, keeping the cursor at or behind
// durable data.
//
// Config.Verifier must be a non-nil Sync 1.1 verifier or Open returns
// ErrInvalidConfig. Runtime wiring owns the process-wide verifier and its
// resync worker pool, pebble state store, and identity cache.
//
// Sync frames and asynchronous resyncs archive a KindSync tombstone followed
// by ActionResync replacement records as KindCreateResync. V1 hides
// replacement rows; v2 and archive readers expose them. Consumers can
// deduplicate by (DID, Collection, Rkey, Rev).
package live
