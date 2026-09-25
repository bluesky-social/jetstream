# Native S3 block backups and coherent Pebble restore

Date: 2026-09-20. Status: not implemented; design rejected.

## Decision

We decided this was rejected in favor of "disaggregated storage mode" in jetstream.

Build backup and restore into Jetstream. Store existing compressed segment
blocks in S3, deduplicated by content hash, alongside native Pebble checkpoints.
A published snapshot manifest binds an exact set of segment generations and
active-segment prefixes to the checkpoints captured with them. Restore
re-stitches ordinary local `.jss` files and opens the restored Pebble databases.

Backups are asynchronous. Normal event delivery does not wait for a
Santa Clara-to-us-east-2 object-store request. A coordinated local capture
temporarily pauses mutations; hashing and uploading happen after that pause.

Reserve Jetstream sequence ranges durably in S3 and locally before using them.
After machine loss, restore a coherent snapshot, skip the abandoned Jetstream
sequence range, and replay the upstream relay from the snapshot's saved cursor.
The remote reservation ceiling is deliberately not rolled back with a snapshot.

The central rule is:

> Prevent arbitrary metadata/data mismatch. Reuse the existing bounded
> crash-recovery cases. Do not make speculative repair part of normal restore.

No external backup daemon, filesystem-snapshot service, per-event remote commit,
custom recovery WAL, new event encoding, or active-active replication is required.
S3 has a new versioned manifest/recipe format, not a new segment block format.

## Requirements and recovery objectives

- A client-observable Jetstream sequence number must never be reassigned to a
  different event, including after restoring an older backup. Recovery moves
  forward in sequence space; ordinary inclusive replay remains supported.
- Duplicate delivery is acceptable. Replayed events may receive new Jetstream
  sequence numbers. Do not promise preservation of lost local sequence numbers
  or original witness timestamps.
- Recovery must not silently skip upstream relay events. The operator-provided
  retention assumption is three days; verify actual availability during restore.
- Keep local storage, object-store storage, and operational machinery modest.
- Restore one coherent snapshot, not independently selected "latest" files.

| Objective | Definition and target |
| --- | --- |
| S3-only RPO | The recent local work absent from the newest fully published, usable snapshot. Target: a few minutes in healthy steady state. Exact cadence and alert thresholds remain to be measured and selected. |
| Relay-event recovery | No additional permanent loss of in-scope, replayable relay events, conditional on complete replay while the required history remains available. A few minutes of snapshot lag becomes replay work, not intentional event loss. |
| Cursor safety | No reuse of any possibly client-observed Jetstream sequence number. This is a correctness invariant, not a best-effort RPO. |
| RTO | Time from machine loss through fencing, provisioning, downloading, validating, local recovery, and sufficient relay catch-up to reopen serving. Numeric SLO remains benchmark-gated; full-archive restoration is plausibly hours, not an assumed minutes-scale operation. |

Snapshot capture time alone is not a recovery-point measurement. A freshly
captured DB can contain a lagging safe relay cursor. Track both publication age
and the recoverable upstream frontier; do not derive time lag by subtracting
opaque relay cursor numbers.

The conservative retention budget is snapshot replay lag + outage/detection +
restore + catch-up, with ample margin below three days. During catch-up, monitor
the oldest still-needed relay position. An expired cursor or upstream gap must
not silently become "start at the live tip" in recovery mode.

This is not an unconditional zero-loss guarantee for all local state. Operator
imports after the snapshot require their original inputs to be re-submitted.
PDS downloads and synthetic resync replacements may not be reproducible byte for
byte from the relay: a later fetch gets current PDS state. Preserve all captured
state, retain necessary import inputs, and distinguish event-history recovery
from eventual reconstruction of current repo state. Existing intentional
compaction and invalid-input policies remain unchanged.

## Production evidence motivating the design

Earlier read-only `gcx` research of pop1, with the measurement window ending
2026-09-20 01:23 UTC, observed Jetstream v0.2.2 / `aaca9b2`:

| Observation | Approximate result |
| --- | --- |
| Archived event rate | 440 events/s; 38 million/day |
| Fresh compressed block bytes | Estimated 4–5 GiB/day, excluding footers |
| Block flush interval | 9.5 seconds |
| Segment rotations | 17/day |
| Existing archive | 1.82 TiB; about 7,645 sealed files |
| Whole-file compaction writes | 4.34 TB/day in the sampled window; 6.77 TB/day weekly average |
| Compaction pass | About 1.3–1.5 TB rewritten over 71–77 minutes |
| Rewrite locality | About 87% of rewritten files changed at most ten blocks |

These are historical observations and estimates, not configured limits or
capacity commitments. Changed compressed-byte volume was not measured; neither
were Pebble checkpoint size/churn, capture pause, or restore throughput.

The conclusions are still useful: whole-file upload after every compaction
would amplify traffic severely; unchanged compressed frames are worth reusing;
waiting for an entire compaction pass is incompatible with a few-minute backup
objective. New-event bytes alone do not size backup bandwidth or object counts.

## Existing consistency boundaries

The current writer fsyncs a block before committing its synced Pebble durability
batch. In steady state that batch advances `seq/next`, renews the local sequence
lease, and stages the consumer's safe relay cursor and eligible sync state.
Verifier state stays pending in memory until its event group has been appended;
pending verification work must not leak into a backup's durable state.

This deliberately allows files to lead metadata across a crash. It does not
allow metadata to claim work whose required file effects are absent. Jetstream
and relay sequences are different namespaces: consistency is a dependency
relationship, not equality of cursor numbers.

| State | Mismatch risk | Existing recovery or repair limit |
| --- | --- | --- |
| `seq/next` | A stale allocator can reuse numbers; an advanced frontier can mask missing coverage. | Startup floors the allocator using durable segment bounds. This does not reconstruct missing rows or prove all coverage is present. |
| `seq/max_reserved`, `seq/gap/*` | Rolling back a local lease can reuse numbers already observed after the snapshot. | Existing one-block leasing handles local crashes. Disaster restore additionally needs the independent remote ceiling and an explicit restore gap. |
| `relay/cursor` | A cursor ahead of restored data skips missing upstream events. | Ordinary segment rows do not persist `UpstreamRelayCursor`; the safe global cursor cannot be inferred from maximum Jetstream seq or timestamps. |
| `sync/chain/*`, `sync/host/*`, `sync/ident/*`, `sync/acct/*` | Newer verifier state and applied-event ratchets can suppress replay of missing rows. | Restore with the matching archive. Rewinding only `relay/cursor` is unsafe. Some envelope-derived ratchets may be salvageable, but the complete state is not generally reconstructible. |
| `compaction/seq` | Newer watermark with older files declares cleanup complete when superseded rows remain. | Rewrites ahead of an old watermark are retry-safe in the supported crash window. An arbitrary advanced watermark is not automatically repaired. |
| `repo/*`, `pdshost/*`, merge cursor, lifecycle phase | Metadata may skip downloads or merge work whose rows/source files are missing. | Completion follows durability locally. Retrying a current PDS fetch does not guarantee historical recovery. Preserve phase-dependent file dependencies. |
| Import rules and job progress | Done markers may skip unpatched files; lost rules alter future stamping. | Repeat patching from the exact original CSV when appropriate. Existing stamped rows do not encode all future timestamp intent. |

Compaction uses tmp-write, fsync, rename, and directory fsync, then advances its
watermark after all required rewrites. Rewrites preserve historical block and
segment sequence envelopes even when rows disappear. A filename, `MaxSeq`, or
event-count comparison is not a segment-generation identity or completeness
proof. Conversely, missing row sequence values inside a compacted envelope are
not automatically data loss.

Arbitrarily newer files are not universally safe with older metadata either.
A later compaction can remove a row because of a replacement outside the
snapshot's captured tail. The snapshot must include the dependencies that made
that rewrite valid.

These components do not need an independent backup/reconciliation protocol:

- The serving manifest is rebuilt from self-describing segment files, not a
  separate authoritative Pebble index.
- Block indexes, blooms, and collection indexes belong to their sealed-file
  generation. Preserve that generation's footer with its block recipe.
- The readable log and block caches are disposable.
- Identity-resolution cache entries are best-effort and can be refreshed.
- Pebble preserves atomic application batches within its native checkpoint;
  the missing coordination is with external files and the other database.

## Object-store representation

Use a stable archive/instance identity, independent of hostname or pod identity.
The following layout is illustrative; names and the manifest encoding are not
yet an implemented compatibility contract:

```text
<archive-id>/
  objects/sha256/<hash>             immutable content-addressed bytes
  snapshots/<snapshot-id>/manifest immutable, published last
  latest                           optional discovery hint, not the commit record
  control/sequence-reservation      monotonic control state, never snapshot-rolled-back
```

Objects contain existing zstd block frames, exact header/footer bytes, native
Pebble checkpoint files, and required auxiliary files. Object type and length
are recorded in the manifest. Deduplicate identical bytes; do not use mutable
segment filenames or recycled Pebble file numbers as object identities.

A sealed segment recipe contains:

- Local segment name/index and format version.
- Exact header object and footer object.
- Ordered compressed-frame hashes and lengths.
- Expected reconstructed file size and integrity information.

Reconstruction is the original 256-byte header, then for each frame its 8-byte
little-endian length and unchanged compressed bytes, then the original footer.
The length prefix can be regenerated from the manifest's validated frame length.
Do not decode/re-encode blocks or rebuild a sealed footer from surviving rows:
that can discard historical envelopes retained by compaction.

An active recipe contains the captured active header and only the complete,
fsynced block prefix through its recorded end offset. It has no sealed footer.
Do not upload an arbitrary later EOF or pick up a header modified by sealing.

The manifest also contains:

- Version, snapshot identity, archive identity, capture time, compatible build
  and storage-format information, lifecycle phase, and upstream relay identity.
- Captured `seq/next`, safe `relay/cursor`, and compaction watermark for
  validation/diagnostics; the matching DB remains authoritative.
- Exact recipes for every included segment tree, including bootstrap-live
  sources when a supported snapshot phase requires them.
- Full filename-to-content-hash inventories for native `meta.pebble` and
  `import-rules` checkpoints, including required WAL/manifest/options/marker
  files. Use Pebble's checkpoint API, not an ad hoc copy of the live DB directory.
- Auxiliary input/scratch inventories and path mappings required by captured
  import state. Restore paths must remain confined to the target data layout.

Each manifest describes a complete logical snapshot, although its immutable
objects are physically shared with older snapshots. No chain of incremental
manifests is required to reconstruct it. Pebble SSTs can deduplicate across
checkpoints; rewritten SSTs are new objects. Do not initially add custom
sub-file Pebble chunking or incremental-WAL replay machinery.

## Coordinated local capture

Add an application-level capture coordinator. The existing writer lock and
`DrainDurability()` are not a cross-store snapshot API.

1. Establish a safe capture boundary. Stop new archive-affecting mutations and
   let admitted work reach safe points. Cover every event producer, durability
   hook, segment create/seal/publication, rewrite publication and watermark,
   import DB ingest/progress/scratch transition, and lifecycle transition.
   Define lock ordering explicitly; do not acquire a metadata gate and then
   wait for a writer that needs that gate to finish draining.
2. Drain pending rows and their event-backed and metadata-only durability
   hooks. Do not promote still-pending verifier work just to make a checkpoint.
3. Pin the actual file generations in a private capture staging area. Hard
   links preserve immutable sealed inodes across future rename/unlink. A path
   list or delayed reopening by filename does not preserve a generation.
4. For each active file, pin its inode, copy its active header separately, and
   capture its exact complete-block end offset. Subsequent appends and sealing
   may alter the linked file, but not the captured frame bytes within that
   prefix. The uploader must use the saved header and bounded prefix only.
5. Capture required auxiliary files without allowing their bytes to mutate
   underneath the snapshot. A hard link alone does not freeze mutable CSV or
   scratch contents. Immutable input staging and bounded/copy-on-capture
   scratch handling must be designed explicitly; do not copy a huge mutable
   input under the pause and assume the pause is cheap.
6. Create native checkpoints of both Pebble databases while application
   mutations remain excluded. Use `WithFlushedWAL` to cover any NoSync writes.
   Record the exact inventory/frontiers associated with this capture.
7. Release the barrier only after capture has succeeded or been safely
   abandoned. Resume producers; hash and upload the pinned capture outside
   the barrier. Partial captures are not recovery points.

Pinned Pebble v1.1.5 captures its version/manifest under internal locks, releases
them, and copies WAL files later. Thus `Checkpoint()` can include concurrent
application writes. `WithFlushedWAL` guarantees inclusion of prior writes; it
does not supply an application-wide "exactly as of call entry" boundary. Hold
our barrier until checkpoint creation returns. Pebble's own background
compaction is handled by its checkpoint implementation, not by stopping Pebble.

Capture latency includes local drain, file pinning, checkpoint linking, and WAL
copying. Benchmark it; do not promise a millisecond pause. S3 transfer and a
full archive hash pass must not run inside the pause. Hash unchanged generations
once and reuse identities only when the exact generation is known.

### Compaction and imports during capture

Expensive rewrite computation can continue outside the capture boundary.
Coordinate the short publication step (rename/directory durability) and related
metadata writes. The current `rewriteMu` spans whole compaction chunks/import
passes, so simply taking that mutex is not the desired capture mechanism.
Adding a short publication boundary is an implementation change.

A capture may include some completed rewrites with the old compaction
watermark. That is an existing retry-safe crash state; no need to wait for an
entire pass. It must not contain the advanced watermark without every rewrite
that watermark depends on. Pin generations from the actual publication state,
not only delayed serving-manifest callbacks.

Imports require both DBs. Their rule ingestion is independently atomic per
SST chunk, with partial-prefix activation an accepted existing limitation.
Preserve that recovery contract rather than claiming a capture makes the
entire job transactional. A nonterminal job that skips bucketing needs its
matching CSV and offset files; a terminal failed job still requires operator
re-submission. Omitting scratch is only safe after implementing and testing an
explicit restore-time reset/restart procedure. It is not current behavior.

For bootstrap/merge, capture both segment namespaces, both writer frontiers,
merge progress, and source-tree lifetime coherently. It is acceptable to ship
steady-state support first, but unsupported phases must explicitly decline
capture and report the coverage gap. Never label a main-tree-only bootstrap
capture a complete backup. Likewise, long imports/compactions must not silently
turn the healthy few-minute objective into an hours-old snapshot.

## Asynchronous upload and publication

- Upload newly durable blocks eagerly when useful. At seal, add the exact
  header/footer recipe. At compaction/import rewrite, upload changed frames
  and new metadata, reusing unchanged content hashes.
- An eager upload is only an optimization. It becomes recoverable data when a
  coherent manifest references it with the corresponding checkpoints.
- Before publishing, ensure all referenced immutable objects exist with the
  expected integrity metadata. Use explicit checksums, not S3 ETag-as-SHA256.
- Publish one immutable manifest last. An interrupted upload leaves orphaned
  objects, not a half-committed snapshot. A `latest` pointer is only a hint;
  failure to update it must not invalidate an otherwise published manifest.
- Make uploads and retries idempotent. A failed backup retains the previous
  valid snapshot and reports degraded protection; it does not advance recovery
  frontiers or invent successful durability.
- Bound upload concurrency and local pinned generations. Hard links avoid an
  immediate full copy but keep old rewritten data allocated. Account for that
  disk pressure; do not accumulate unlimited captures during an S3 outage.

The required S3-compatible semantics include reliable immutable-object reads
after successful writes and atomic conditional updates for control state.
Validate the actual provider, not just its API resemblance to AWS S3.

## Remote sequence reservations

Use exclusive range ends, matching the existing lease convention. A remote
ceiling `R` means no writer may expose sequence `R` or above without a further
successful reservation. The record belongs to the stable archive identity.

1. Extend the remote ceiling monotonically with a conditional write before
   using the additional range. Prefetch a large range so cross-region requests
   are infrequent and normally off the delivery path.
2. Persist the granted range locally before exposing any sequence it covers.
   Integrate this with the existing local one-block lease: neither local
   renewal nor publication may cross the remote grant.
3. Keep remote-grant bookkeeping distinct from the existing clean-close
   behavior that collapses the local lease. A graceful close must never lower
   the remote ceiling.
4. If S3 is unavailable, continue only within already granted, locally durable
   space. At exhaustion, stop admitting new sequenced events safely; never
   allocate optimistically. An ambiguous reservation response requires
   reconciliation or conservatively abandoning space, not assuming failure.
5. Validate arithmetic against `seqspace` limits, including the `1e15` cursor
   namespace ceiling. Choose range size from measured rate and outage budget,
   not a time-derived cursor scheme.

The control object is excluded from snapshot rollback and object expiration.
Losing trustworthy reservation history means we cannot safely claim continuity
for the same cursor namespace; do not recreate it from an old snapshot.

Reservations are not sufficient split-brain fencing. An old writer can still
hold a previously granted range. Restore requires positive fencing of the old
writer and its serving endpoint, plus a control generation/ownership check so
an old disk cannot later boot and reuse abandoned grants. A conditional S3
update by itself does not stop a partitioned process with an existing grant.
This plan does not introduce automatic HA or distributed consensus.

## Restore protocol

1. Fence the old writer. Select one complete compatible snapshot whose required
   relay history remains recoverable. Do not mutate the original backup.
2. Download into a new staging data directory. Verify object lengths/hashes,
   reconstruct exact segment recipes, and restore complete native DB
   checkpoints and required auxiliary files. Fsync files/directories before
   installing the restored data directory.
3. Validate structure, segment coverage/envelopes, existing registered gaps,
   captured DB frontiers, and phase dependencies before ordinary startup can
   hide damage through tail truncation or counter reconciliation. Compacted
   gaps within historical envelopes are valid; unexplained missing blocks are
   not. No arbitrary old-block substitution is permitted.
4. Read the latest trustworthy remote reservation state, not the value captured
   in the backup. Let `D` be the snapshot's validated exclusive durable coverage
   frontier and `R` the old remote exclusive ceiling. Require `R >= D` and
   abandon `[D, R)`; preserve earlier registered gaps.
5. Obtain a new remote grant beginning at or beyond `R`. Before readiness,
   atomically persist the restore gap, allocator frontier, and usable local
   lease/grant state. Restore retry/crash handling must be idempotent and may
   waste numbers but must never reuse them. The current registry only supports
   crash gaps; add/version explicit restore-gap support as needed.
6. Open normal local recovery with the matching relay cursor and verifier
   state. Rebuild serving metadata, caches, and the live tombstone set through
   their supported startup paths. Resume required import/lifecycle work.
7. Replay upstream from the checkpoint's safe relay cursor, assigning newly
   archived rows fresh Jetstream numbers. Keep public serving recovery-gated
   until validation, cursor setup, and the chosen catch-up criterion succeed.
   Current debug `/readyz` is not this gate and must not be mistaken for it.

Example: the snapshot contains sequences through 10,000, clients may have seen
through 10,500, and the remote exclusive ceiling is 20,001. Restore registers
`[10,001, 20,001)` and starts replayed/new rows at 20,001 or later. A client
resuming at 10,500 crosses the explicit gap to replayed data rather than having
its cursor clamped to an old live tip. Upstream replay still starts at the
snapshot's relay cursor, not at a number derived from 20,001.

Skip abandoned Jetstream numbers, never upstream events. Inclusive duplicate
delivery is unchanged; this is not a promise that an explicitly rewound client
cannot request older retained rows. Verify v1, v2, archive pagination/cutover,
lookback handling, and client-side sequence dedup across the restore gap.

An invalid snapshot is rejected or replaced by an entire earlier valid
snapshot, provided relay retention still permits recovery. Unexpected holes
must not be converted into registered restore gaps. Salvage is separate,
operator-directed work, never a silent normal-restore fallback.

## Integrity, retention, and storage reclamation

Byte integrity and logical consistency are separate checks. SHA-256 and length
verification cover every object; native segment/Pebble validation detects
format-level corruption. The current segment xxh3 covers header/footer
metadata, not block payloads; zstd content checksums cover decoded frames.
None of these hashes proves that a particular relay cursor or compaction
watermark belongs with arbitrary files. Coordinated capture supplies that
relationship, and the immutable manifest preserves it.

Retain multiple complete manifests. Garbage collection marks every object
referenced by retained snapshots and protects in-progress uploads/captures;
only unreachable objects beyond a safety grace are eligible for deletion.
Coordinate GC with publication so an object cannot be deleted between its
existence check and manifest publication. Never apply generic age-based expiry
to shared block objects or the reservation record. Final retention/grace
settings remain an operator decision.

Compaction on the live server does not instantly remove bytes retained in an
older backup. Define backup retention consistently with deletion obligations,
including bucket versions and orphan cleanup. Content addressing saves space
across retained generations; it is not automatic deletion compliance.

## Implementation sequence

1. Define/version manifest and recipe types, strict validation, stable archive
   identity, and the required object-store operations. Keep dependency additions
   subject to the repository whitelist; SDK choice is not decided here.
2. Add remote reservations and restore-aware cursor/gap initialization. Prove
   no reuse across ordinary restart, snapshot rollback, ambiguous S3 responses,
   and interrupted restore before enabling backup-based disaster recovery.
3. Add capture coordination and explicit publication boundaries across the
   writer, metadata stores, compaction, imports, and supported lifecycle phases.
   Add native checkpoint access to both DB wrappers. Benchmark local pauses.
4. Add bounded staging/upload, frame deduplication, manifest-last publication,
   and metrics. Create and verify the initial baseline before claiming backup
   protection or steady-state RPO.
5. Add restore tooling in the Jetstream binary, strict validation, recovery
   readiness gating, relay-retention handling, and an operator fencing runbook.
6. Add retention/GC only with publication-race tests. Run full restore drills
   and choose numeric operational settings from measurements.

Expected code touchpoints:

- `internal/ingest/writer.go`, `seqlease.go`, `internal/seqspace`: durability,
  capture boundary, local allocation, restore vacancies.
- `internal/ingest/live/consumer.go`, `internal/ingest/syncstate`: safe upstream
  watermark and verifier state; preserve existing promotion semantics.
- `segment/writer.go`, `seal.go`, `rewrite.go`, `patch.go`: immutable frame
  identity, active-prefix capture, and short publication boundaries.
- `internal/ingest/orchestrator`, `internal/ingest/backfill`: compaction,
  retries, phase/source-tree coordination, and merge progress.
- `internal/store`, `internal/timestamp/rules.go`, `internal/importer`: native
  checkpoints, second DB, original inputs, scratch and resume dependencies.
- `internal/jetstreamd`, `cmd/jetstream`, `internal/subscribe`, `internal/xrpcapi`:
  wiring, operator commands, readiness, and public replay/cutover validation.

## Verification and observability

Extend the oracle with machine-loss restore scenarios, not just process restart
on the same disk. Use independent simulator relay history and public replay
observers; final repo-state convergence alone cannot prove event completeness.

Required scenarios include:

- Crash after block fsync but before metadata commit; before/after checkpoint
  completion; during upload; before/after manifest publication.
- Capture concurrent with seal, compaction rename/directory fsync/watermark,
  timestamp patch/progress, and delayed serving-manifest refresh.
- Substitute checkpoint/block objects from another snapshot and require
  manifest integrity/identity validation to reject them. Inject capture races
  that pair newer metadata with older blocks, or newer compacted blocks with
  an incomplete older tail, and require the independent oracle to catch them.
  A self-consistent hash inventory of incorrectly paired bytes is not generally
  detectable from scalar restore checks alone; test the capture protocol itself.
- Account/identity replay guards, multi-block sync replacements, concurrent
  retry producers, and verifier work pending at capture.
- Both DBs and import CSV/scratch/progress in every supported phase, including
  partial rule ingestion and terminal-failure behavior.
- Remote reservation timeout, exhaustion, stale ownership, local persistence
  failure, rollback across many grants, and repeated crashes during restore.
- Clients holding cursors above the snapshot tip; registered-gap traversal on
  v1/v2 and archive-to-live cutover without silent future-cursor clamping.
- Missing/corrupt/truncated objects, wrong block order, incompatible versions,
  GC/publication races, and loss of relay retention during recovery.
- Unsupported lifecycle phases fail explicitly rather than emit partial
  backups. Supported bootstrap/merge snapshots preserve both namespaces.

Use the established `just` checks: `just`, relevant package tests, long restart
oracle coverage, `just oracle-sweep`, relevant fuzz targets, and hot-path
benchmarks. Follow `specs/oracle.md` and the mutation discipline when adding
oracle cases; only run `just mutation-gate` on a clean worktree. Prior targeted
durability/replay-guard/compaction/import recovery tests passed during design
research, but they do not validate this unimplemented backup protocol.

Export metrics/traces for capture wait/pause, checkpoint bytes/time, safe
backed-up relay progress, published-snapshot age, upload backlog/errors,
uploaded versus reused bytes/objects, pinned local bytes, reservation headroom
and failures, restore phase/throughput, replay lag, and retention margin.
Alert on loss of backup freshness well before relay retention is threatened.

Before launch, measure actual changed-block bytes/object counts, Pebble
checkpoint churn/WAL-copy latency, capture pauses under imports/compaction,
cross-region restore throughput, full-archive validation/startup time, and relay
catch-up capacity. Select cadence, reservation range/prefetch thresholds,
retention, and a numerical RTO from those results. These are tuning and rollout
decisions, not reasons to weaken the consistency contract.

## Source references and rejected alternatives

Authoritative context: `docs/README.md`, package code/docstrings,
`specs/invariants.md`, `specs/gotchas.md`, and
`specs/notes/2026-08-25-seq-reuse-after-crash.md`. The code audit found stale
prose around steady metadata key names and segment checksum coverage; use the
actual implementation described above when implementing capture, and reconcile
those docs as part of the relevant implementation work.

The design borrows the established pattern of a consistent base plus replay
from PostgreSQL-style recovery, and immutable-file/checkpoint inventories from
LSM backups. Here the retained upstream relay supplies replay; Jetstream does
not add a new application WAL. Relevant prior research:

- https://www.postgresql.org/docs/current/continuous-archiving.html
- https://github.com/facebook/rocksdb/wiki/How-to-backup-RocksDB
- https://apple.github.io/foundationdb/backups.html
- https://litestream.io/how-it-works/
- Pinned Pebble `v1.1.5/checkpoint.go`, especially `Checkpoint` and
  `WithFlushedWAL`.

Rejected for this first design: treating all Pebble state as derived data;
independent periodic copying of live DBs and segment paths; uploading every
whole segment rewrite; synchronous cross-region durability for every delivered
event; a custom per-mutation recovery journal; infrastructure/external-tool
backup dependencies; and automatic mixing/repair of incompatible snapshots.
