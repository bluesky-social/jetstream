// Package ingest owns Jetstream's active-segment writer. It assigns seqs,
// rotates files at a configurable size, and commits per-block metadata to
// pebble (docs/README.md §3.1.1, §3.4). A mutex serializes producer access to
// the underlying segment.Writer. The segment package itself does not manage
// pebble, rotation, or seq allocation.
//
// # Client-visible sequence leases
//
// The steady writer publishes events before their blocks are durable.
// ReserveClientVisibleSeqs makes Open durably reserve MaxEventsPerBlock seqs
// before returning. Append rejects seqs at or beyond the exclusive lease end
// before publication. Each synced block batch advances seq/next and renews
// the lease; terminal Close collapses it to exact nextSeq.
//
// After an unclean exit, Open atomically registers the abandoned range
// between the recovered durable tip and seq/max_reserved under seq/gap/,
// resumes at its end, and reserves another block. The gap registry is
// validated against every non-empty durable block and shared with cursor
// resolution and cold replay. This prevents observed seqs from being reused.
//
// Bootstrap writers need no lease because lifecycle gating prevents serving
// their seqs. Merge explicitly attests this when adopting legacy state.
package ingest
