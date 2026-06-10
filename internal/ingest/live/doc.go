// package live owns the consumer that pumps the upstream
// relay's com.atproto.sync.subscribeRepos firehose into a
// directory of segment files. The package is deliberately generic:
// it is used during the bootstrap phase to populate
// data/backfill/live_segments (DESIGN.md §4.1 step 1), and the
// same Consumer type will be reused after the merge step lands
// to populate data/segments in steady state (DESIGN.md §4.3).
//
// The Consumer wraps a dedicated *ingest.Writer. The mapping from
// upstream firehose events to segment.Events lives in events.go
// as a pure function so it is straightforward to unit-test
// against arbitrary input. Cursor durability is delegated to the
// writer's OnAfterFlush hook so persisted cursor ≤ durable events
// holds for free, as DESIGN.md §3.1.1 requires.
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
// Event.Operations archive after it as segment.KindCreate rows carrying the
// live record bytes (see events.go); downstream consumers can dedupe on
// (DID, Collection, Rkey, Rev).
package live
