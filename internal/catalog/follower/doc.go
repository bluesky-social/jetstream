// Package follower keeps a disaggregated pod's in-memory mirror of the
// PostgreSQL catalog current and serves reads from it (design §11).
//
// One Follower runs per pod. Each tick reads the rows changed since the
// mirror's catalog_revision in one read transaction, fetches the footers of
// new generations, swaps the mirror in with one atomic store, and feeds new
// Main seals to the manifest. Ticks run on NOTIFY, on a poll timer, on the
// leader's doorbell, and on a reader's synchronous Refresh. After each tick
// a separate feeder fetches and decodes the blocks the readable log still
// lacks, concurrently, and appends them a window at a time, advancing the
// log's durable watermark past each window. A tick's cost therefore does
// not grow with the number of events a revision brings.
//
// The Follower is the pod's catalog.Catalog (Snapshot, Refresh), its
// catalog.Fetcher over the object store, the protocol.RowSource of that
// store's reads, and its lifecycle.Readiness. Corruption it finds in shared
// storage stops Run with a catalog.CorruptionError; transient failures
// keep the previous mirror, or the log's tip; see Ready for when they cost
// readiness.
package follower
