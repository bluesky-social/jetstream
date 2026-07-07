// Package ingest owns the active-segment writer for jetstream. It
// allocates monotonic seq numbers, rotates segment files at a
// configurable byte threshold, and commits the per-block durability
// batch to pebble (docs/README.md §3.1.1, §3.4).
//
// One *ingest.Writer is shared across all goroutines that produce
// events: the bootstrap-phase backfill workers today, the live-tail
// firehose consumer in a future PR, and the replica writer
// eventually. A single sync.Mutex serializes Append, Close, and the
// rotation it triggers; the underlying segment.Writer remains
// caller-serialized as it documents.
//
// The segment package is deliberately unaware of pebble, rotation,
// or seq allocation. All those concerns live here, in the ingestion
// orchestrator that composes Writer with the rest of the system.
package ingest
