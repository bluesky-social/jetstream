// Package livestream owns the consumer that pumps the upstream
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
// This package does not yet act on #sync events (atmos v0.0.16
// does not implement full sync 1.1). #sync frames are archived
// into the segment file as KindSync but no resync is triggered.
// The opt-out is one line and will be removed when the atmos
// dependency is upgraded.
package livestream
