// Package repair implements the unified per-DID gate, durable inbox, immutable
// staged-replacement store, and off-hot-path ClassBulk publisher for steady-state
// whole-repo replacements (automatic Sync 1.1 resyncs, steady-state failed-repo
// retries, and future operator repairs).
//
// Why this package exists:
//
//  1. Live-ingest isolation (PD #5913): hotWriter's admission control yields
//     ClassBulk whenever ClassLive is waiting (liveWaiting > 0), but only when
//     bulk appends run on a separate goroutine from Consumer.Run. Moving resync
//     writes onto this package's background worker keeps Consumer.Run purely on
//     ClassLive (for unrelated DIDs) and non-blocking durable inbox capture (for
//     gated DIDs).
//  2. Per-DID ordering invariant: hotWriter is DID-unaware. No DID may ever have
//     concurrent ClassLive and ClassBulk writes in flight, or liveWaiting > 0
//     preemption would interleave live commits/deletes between KindSync and
//     KindCreateResync chunks. Both live resyncs and steady-state failed-repo
//     retries publish behind the same per-DID gate.
//  3. Crash-resumable streaming: replacement snapshots are validated once and
//     written to a checksummed staged artifact (local vfs.FS file or S3 blob),
//     then streamed in bounded ClassBulk chunks without materializing a full
//     []segment.Event slice. All repair metadata (gate state, durable inbox,
//     chunk offset NextRow, drain offset DrainedPos, completed cutoff rev) is
//     staged through ingest.Writer's ordered durable-batch hook so relay/cursor
//     can never advance past an unjournaled inbox entry or repair job.
//
// Metadata keyspaces (all under "repair/"):
//
//   - repair/gen/<did>           8-byte big-endian monotonic repair generation
//   - repair/job/<did>           JSON JobRecord while a repair/gate is active
//   - repair/cutoff/<did>        highest completed repair SnapshotRev string
//   - repair/inbox/<did>/<pos20> binary InboxGroup (zero-padded 20-digit pos)
package repair
