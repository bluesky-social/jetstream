// Package follower keeps a disaggregated pod's in-memory mirror of the
// PostgreSQL catalog current and serves reads from it (design §11).
//
// One Follower runs per pod. Each tick reads the rows changed since the
// mirror's catalog_revision in one read transaction, fetches the footers of
// new generations and the bytes of blocks the readable log still lacks,
// then appends to the readable log, swaps the mirror in with one atomic
// store, advances the log's durable watermark, and feeds new Main seals to
// the manifest. Ticks run on NOTIFY, on a poll timer, on the leader's
// doorbell, and on a reader's synchronous Refresh.
//
// The Follower is the pod's catalog.Catalog (Snapshot, Refresh), its
// catalog.Fetcher over the object store, the protocol.RowSource of that
// store's reads, and its lifecycle.Readiness. Corruption it finds in shared
// storage stops Run with a catalog.CorruptionError; transient failures keep
// the previous mirror, which readiness reports once it outlives MaxViewAge.
package follower
