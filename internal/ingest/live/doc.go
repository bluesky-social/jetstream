// package live owns the consumer that pumps the upstream
// relay's com.atproto.sync.subscribeRepos firehose into a
// directory of segment files. The package is deliberately generic:
// it is used during the bootstrap phase to populate
// data/backfill/live_segments (docs/README.md §4.1 step 1), and the
// same Consumer type will be reused after the merge step lands
// to populate data/segments in steady state (docs/README.md §4.3).
//
// The Consumer wraps a dedicated *ingest.Writer. The mapping from
// upstream firehose events to segment.Events lives in events.go
// as a pure function so it is straightforward to unit-test
// against arbitrary input. Cursor durability is delegated to the
// writer's durable batch hook so persisted cursor ≤ durable events
// holds in the same synced Pebble commit as seq/next, as docs/README.md §3.1.1
// requires.
//
// Sync 1.1 verification is required: Config.Verifier must be a
// non-nil *sync.Verifier or Open returns ErrInvalidConfig. The
// verifier itself is not owned by this package — its resync worker
// pool is a process-wide resource that cmd/jetstream constructs
// (with a pebble-backed StateStore + identity cache) and shares
// with any future steady-state consumer.
//
// #sync frames and async verifier resync events are archived as a
// segment.KindSync tombstone row first. Any ActionResync ops yielded by
// Event.Operations archive after it as segment.KindCreateResync rows carrying
// the live record bytes (see events.go). These replacement rows are hidden
// from the v1 /subscribe presentation but visible on /subscribe-v2 and to
// archive readers; downstream consumers can dedupe on
// (DID, Collection, Rkey, Rev).
package live
