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
//
// # Client-visible sequence leases
//
// The steady-state writer publishes an event to the readable log before its
// block is durable. To ensure a SIGKILL can never make that observable seq
// reusable, ReserveClientVisibleSeqs makes Open synchronously reserve one
// MaxEventsPerBlock window in pebble before returning. Append rejects any seq
// at or beyond the exclusive reservation end before readable-log publication.
// Each block's existing synced metadata batch advances both exact seq/next and
// the next one-block lease; terminal Close instead collapses the reservation to
// exact nextSeq.
//
// On unclean startup, seq/max_reserved can lead the durable tip recovered from
// seq/next and segment blocks. Open atomically registers that half-open range
// under seq/gap/, resumes at its end, and reserves the next block. The immutable
// gap set is validated against every non-empty durable block before serving and
// is shared with subscribe cursor resolution and cold replay. Bootstrap writers
// do not lease seqs because lifecycle gating makes their assigned seqs
// unobservable; merge explicitly attests that fact when adopting legacy state.
package ingest
