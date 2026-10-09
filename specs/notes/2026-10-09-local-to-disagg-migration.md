# Live migration from local to disaggregated storage

2026-10-09. Branch `jc/local-to-disagg-migration`. Status: implemented
(S1–S7). §15 records where the code departs from this plan; where they
disagree, §15 and the code win.

This plan moves a running local-mode archive (segment files and Pebble on one
volume) to disaggregated storage (S3-compatible object storage and
PostgreSQL). Seqs, sealed segment bytes, and ETags stay the same. Ingest
pauses once, for about a minute. Reads never stop.

The disaggregated design (`2026-09-25-disaggregated-storage-v2-design.md`,
"the design" below) lists "migrating an existing local archive" as a non-goal
(§2.2). This plan reverses that. It reuses the design's catalog, upload
protocol, fence, and leader sessions, and adds only what migration needs.
§12 lists every change to the design.

## 1. Answer

Yes, this is possible, with a plain catalog handoff and no in-process mode
switch.

- **The running process replicates its own archive.** A migrator runs inside
  the local-mode server process. It does three things:
  - seeds the object store and PostgreSQL with every sealed segment, byte for
    byte;
  - copies the Pebble metadata;
  - then follows each durable block, each seal, and each metadata change.
- **The migrator writes as a fenced disaggregated leader.**
  - It holds the disaggregated writer lease for the whole migration.
  - Every write it makes is a fenced transaction.
  - The PostgreSQL catalog is therefore always a consistent prefix of the
    local archive, with the same seqs.
- **The copy is asynchronous, not a synchronous double write.**
  - The local committer's path does not change.
  - Object store or PostgreSQL trouble slows the migrator, never local ingest
    or reads.
  - The replica trails local durability by at most one block, plus the
    migrator's own queue. Local mode cuts a block every 4,096 events.
- **The switch is a traffic flip followed by a fenced handoff of the writer
  lease.**
  1. Disaggregated pods have already been running, serving the replica
     read-only on a shadow endpoint. The public routes now move to them.
  2. The local process stops ingest and seals its active segment.
  3. It ships the last blocks, the seal, and the metadata delta.
  4. It commits `migration/state = done` and releases the lease.
  5. A disaggregated pod takes the lease. Its leader session resumes the
     firehose from the copied `relay/cursor`.
  6. The old process closes its remaining websockets, and those clients
     reconnect to the new pods.
- **Clients keep their cursors.**
  - Seqs, segment names, ETags, and byte ranges are identical.
  - No endpoint exposes an archive identity (§9).
  - A websocket client reconnects once (close code 1001) and resumes with its
    own cursor.
  - An in-flight `getSegment` download resumes with `If-Range` against the
    same ETag.

**Why the process does not switch modes in place.** `jetstreamd.Build` wires
the manifest, catalog, cold reader, and subscribe tail once per storage mode.
Swapping them under live subscribers would be a large refactor with new
failure modes, to save one reconnect. Clients of a single-node local instance
already reconnect on every deploy. The same binary does all the work; the
handoff is a lease transfer between processes running the same image.

## 2. What to measure first, and why it matters

Before building, measure these on the source instance. Read-only metrics and
the `/status` page answer most of them. S0 (§13) answers the rest.

| Quantity | Why it matters |
|---|---|
| Sealed segments, total blocks, and compressed bytes | Seed duration, object count, and object-store capacity. Each block becomes one object. Each upload costs one PUT and one read-back GET. |
| Registered seq vacancies (`jetstream_ingest_seq_gap_count`, `/status`) | **Any vacancy is a hard blocker today** (§7). |
| Compaction pass frequency, and segments and bytes rewritten per pass | Whether a copy could keep up with compaction (§6.5). |
| Volume read/write throughput and headroom | How fast the seed may read without hurting cold reads. |
| Pebble key counts and bytes by prefix | Metadata copy time and PostgreSQL size (§6.6). |
| Events per second and blocks per hour | Tail lag, and the tombstone-rebuild backlog after a compaction pause. |
| Subscriber count and cold-read rate | Reconnect and cold-read load at the flip. |
| Round-trip time to the target PostgreSQL | Every hot-batch commit and follower tick pays it, so it sets live latency. |
| Object-store free capacity and physical-to-logical ratio | Replication factor plus unreclaimed garbage. |
| Upstream relay replay window | Bounds the post-handoff rollback (§8). |

Lessons that apply to any large local archive:

- **Local compaction can rewrite most of the archive every pass.** A full
  rewrite replaces a whole segment file if even one row drops. On an archive
  with deletes spread across history, most segments drop a row each pass. A
  copy that chased compaction would never converge, so the migrator pauses
  local compaction while it runs (§6.5).
- **Any registered vacancy blocks the switch until disaggregated mode
  supports it.**
  - Local crash vacancies sit between blocks.
  - Disaggregated mode has no gap registry: `follower.SeqGaps()` returns nil.
  - The catalog invariants call such a hole corruption.
  - Cold replay fails across it with "unregistered sequence hole". A v2
    client whose cursor crosses one gets `InternalError`, reconnects, and
    fails again forever.
  - §7 fixes this.
- **A freshly bootstrapped disaggregated archive of similar size is the best
  sizing reference.** It shows PostgreSQL size, object count, follower
  memory, and compaction read volume at that scale.
- **Leader failovers in disaggregated mode are live-tail pauses.** Each lasts
  up to `JETSTREAM_LEADER_LEASE`. Check failover frequency on an existing
  disaggregated deployment before moving a production one. The migration
  does not cause failovers, but it exposes their clients to them.

## 3. Requirements

Hard requirements:

1. **No seq is reused or skipped.**
   - The disaggregated `seq/next` equals local `seq/next` after a clean close.
   - The catalog holds exactly the local vacancies and no others. That
     includes any vacancy an unclean local restart adds during the migration.
2. **Sealed segments are byte-identical.** The catalog's header, block
   objects, and footer object concatenate to the local file. ETag,
   `Content-Length`, and every byte range therefore match.
3. **No data loss.**
   - Every event at or below the handoff seq is in the catalog.
   - Every event after it is ingested by the disaggregated leader from
     `relay/cursor`.
   - Delivery stays at least once.
4. **One writer.**
   - At no point can local ingest and a disaggregated leader both assign seqs.
   - After handoff, the local process must never resume ingest on its own
     (§6.8).
5. **The source server is never put at risk.**
   - A migrator failure stops the migrator, not the server.
   - It never crashes the server and never writes to local segments.
   - It never blocks local ingest, except in the deliberate handoff stop
     (§6.7).
   - It writes to Pebble only its own `migration/*` keys: the compaction
     pause (§6.5) and the handoff guard (§6.8).
   - Its CPU, memory, disk, and network use are bounded and configurable.
6. **Resumable.** The source restarts on every deploy, and also on OOM or
   node loss. Every migrator phase must resume from durable state without
   starting over.
7. **Reversible until handoff.**
   - Before handoff, aborting leaves the source exactly as it was, apart from
     the compaction pause.
   - After handoff there is an emergency path back (§8), but it is costly.

Non-goals:

- Migrating `bootstrap` or `merging` archives. Only `steady_state` is
  supported, and the migrator refuses any other phase.
- Disaggregated to local, beyond the emergency rollback in §8.
- Migrating timestamp-import state, which was removed in v0.3.0.
- Reusing the local cold-read caches. The disaggregated pods warm their own.

## 4. Options considered

| Option | Verdict |
|---|---|
| **A. Offline copy.** Stop the source, copy segments plus metadata, start disaggregated pods. | Rejected: hours of downtime. |
| **B. Fresh disaggregated bootstrap beside the source, then repoint DNS.** | Rejected as the plan. It creates a new seq namespace: every client cursor and plan becomes invalid, and the archived event history (seq order, deletes, witnessed times) is lost. Kept only as the last-resort fallback. |
| **C. Synchronous double write** in the local block committer: fsync, Pebble, then object store and PostgreSQL before the ack. | Rejected. Production ingest would depend on the new storage. Every block commit would pay a network round trip, and a PostgreSQL outage would stall the source. History would still need a separate backfill. It offers nothing asynchronous tailing doesn't, because the handoff drains the tail anyway. |
| **D. In-process asynchronous replication plus a fenced lease handoff** (this plan). | Chosen. Same binary, nothing added to the commit path, a resumable seed, shadow validation before any client moves, and about a minute of ingest pause at handoff. |
| **E. A sidecar** that reads the data volume read-only. | Rejected. Pebble takes an exclusive lock, and the migrator needs the writer's durability and seal events and the compactor's pause. A sidecar could copy segments but not metadata, and it would race compaction's renames. |

## 5. Overview

```
        local server process (migration mode)                   disaggregated pods (same image)
 ┌────────────────────────────────────────────────┐          ┌──────────────────────────────┐
 │ local leader session (unchanged ingest/serve)  │          │ follower + readers           │
 │   writer ── block durable / seal events ──┐    │          │ election loop: skips while   │
 │   compactor (paused while migrating)      │    │          │   migration/state != done    │
 │   pebble ── commit observer (dirty keys) ─┤    │          └──────────┬───────────────────┘
 │                                           ▼    │                     │ SELECT / GET
 │ migrator (holds disaggregated lease, epoch E) │                     │
 │   seed: sealed segments ─────► PUT blocks+footer ──► object store ◄───┤
 │   tail: blocks, seals,                         │                     │
 │         metadata deltas ─────► fenced txns ───────► PostgreSQL ◄───────┘
 └────────────────────────────────────────────────┘
```

**Migration states.** The current state is the `migration/state` metadata
key in PostgreSQL. The local side mirrors it in Pebble for the guard in §6.8.

| State | Who writes | Meaning |
|---|---|---|
| (catalog absent) | — | Not started. |
| `seeding` | `storage init --migrate-from-local` | The catalog exists, and the migrator is copying sealed segments and bulk metadata. `phase` is not yet written, so pods would answer 503 if they ran. They should not be deployed yet. |
| `tailing` | migrator | The seed is done and `phase = steady_state` is copied. Disaggregated pods may run and serve the replica read-only (shadow mode). The migrator ships each new block, seal, and metadata change. |
| `handing_off` | migrator | Handoff started; local ingest is stopping. |
| `done` | migrator, in the final fenced transaction | The disaggregated lease is released to the pods. The local process must not ingest again. |
| `aborted` | migrator, on operator request | The migration was abandoned before `done`. Local compaction resumes, and the catalog is discarded. |

## 6. Design

### 6.1 Target catalog

After the seed, the catalog looks exactly as if disaggregated mode had built
this archive itself, except for the vacancies.

- **`archive`**: the row from `storage init`, with a fresh `archive_id`.
- **`main`**:
  - Segments `0..N-1` are `sealed`. Each has one generation whose `header` is
    the local file's 256-byte header.
  - `generation_blocks` reference one block object per footer block, in
    order, with `compressed_length` from the footer.
  - `footer_object_id` references the file's bytes `[footer_offset, EOF)`.
  - Segment `N` is `active` and holds the local active segment's durable
    blocks as `active_segment_blocks`.
- **`bootstrap_live`**: no rows. Init creates segment 0 there, and the
  import deletes it in the transaction that writes `phase`, as merge's final
  transaction does.
- **`metadata_kv`**: every Pebble metadata key the disaggregated runtime
  reads, with the same encoding. Local-only keys are dropped or rewritten
  (§6.6).
- **`hot_batches`**: empty until handoff.

Object keys are fresh UUIDs, as in design §7.1. Identical frames, such as
two empty compacted blocks, dedupe to one object with several references.
That is already valid: `generation_blocks.object_id` is not unique, and GC
uses `NOT EXISTS` over every referencing table.

### 6.2 Lease, fence, and the migration guard

**The migrator is a leader.**

- It runs the design's election loop over the PostgreSQL lock, with its own
  `holder_id`.
- Every write goes through a `catalog.Session` with its epoch: imports,
  `BeginUploads`, metadata copies, and state changes.
- When the source restarts, the new process acquires a new epoch. Anything
  the old process still had in flight then fails the fence.

**No disaggregated pod may run a leader session before `done`.** Two layers
enforce this.

1. **Pre-acquire check.**
   - `leader.Run` gets an optional `MayAcquire` callback.
   - In disaggregated mode the callback reads `migration/state` from the
     follower's mirror, so the follower must load that key.
   - It refuses while the state is present and not `done`.
   - So a pod does not even try to take the lease from a restarting
     migrator.
2. **Session check.**
   - The first act of a disaggregated `runSession` is a fenced read of
     `migration/state`.
   - If the state is present and not `done`, the session ends with a new
     error, `leader.ErrStandby`.
   - The loop then releases the lease at once and sleeps for
     `JETSTREAM_MIGRATION_STANDBY_BACKOFF` (default 10s).
   - This covers a pod whose mirror was stale, and a lease that expired while
     the source restarted.

An absent `phase` must never start a bootstrap on a migration catalog. Today
an absent phase means "fresh archive" (`orchestrator.go:58-68`), which would
bootstrap on top of the seed. The session check runs first, so this cannot
happen, and a unit test pins the ordering.

**`storage init --migrate-from-local`**:

1. Creates the schema and the `archive` row, as today.
2. In the same leader session as `InitNamespace`, writes `migration/state =
   seeding`.
3. Releases the lease.

There is no window in which a pod could find a catalog with neither a phase
nor a migration state.

**Reader pods during `tailing`.**

- `phase = steady_state` is present, so followers become ready and serve the
  replica.
- They sit behind their own service and a non-public shadow endpoint (§10),
  not the public route.
- The replica is a consistent prefix of the source archive. Whatever the
  pods serve is correct, just up to one block behind.

### 6.3 Seeding sealed segments

Each local sealed segment is imported as one generation, in three steps.

1. **Read.**
   - Read the file from the manifest's directory.
   - Parse the header and footer, and check the header checksum.
   - Split the file into the header, the block frames (each without its
     8-byte length prefix), and the footer.
   - The footer's block index gives each frame's offset and length. Check
     that the frames tile `[256, footer_offset)` exactly.
2. **Upload** the blocks and the footer through `protocol.Uploader` (design
   §7.3):
   - one `BeginUploads` transaction per batch of objects;
   - then PUT;
   - then a read-back GET with a SHA-256 check.

   Size the batches so every PUT starts within `GC_ORPHAN_AGE/2` of its rows
   committing; otherwise the uploader's `errStale` rule ends the session. A
   256MiB segment is about 825 objects.
3. **Import** with a new fenced script, `ImportSealedSegment` (§6.4), in
   segment index order.

**Pipelining.**

- Up to `JETSTREAM_MIGRATION_SEGMENT_CONCURRENCY` segments (default 4) are
  read and uploaded at once. Imports commit strictly in index order.
- Reads go through a token bucket, `JETSTREAM_MIGRATION_READ_BYTES_PER_SEC`.
  Start conservatively. Raise it while watching volume latency and cold-read
  p99.
- The migrator owns its own object-store client. Its own PUT and GET
  concurrency limits (defaults 32/32) are separate from the process's.
- Seed time is about compressed bytes divided by the read rate. The object
  rate is about block count divided by seed time.

**Resume.**

- The catalog itself records progress: the next segment to import is the
  active `main` segment's index.
- After a restart the migrator re-reads the catalog and continues.
- Uploads that were in flight become orphans. GC collects them once handoff
  completes.

**Reconcile.** Before resuming, and again before handoff, the migrator
compares every imported generation's stored header with the local file's
header. That is one 256-byte read per segment.

- Compaction is paused, so the headers must match.
- A mismatch means the pause failed, or something else rewrote a segment.
  The migrator stops with a loud error and a metric. It does not crash the
  server, and the operator decides whether to abort or re-import.
- A re-import is a `PublishGeneration`-style import. Dedup means only changed
  blocks are uploaded.

### 6.4 New catalog scripts

These are added to `internal/catalog/scripts.go`, with storagefake parity and
`catalogtest` contract coverage. They are built only from existing `Tx`
primitives, so they need no SQL beyond what `Seal` already runs.

**`ImportSealedSegment(ns=main, idx, header, footerObj, blocks[])`**

- Preconditions:
  - `idx` is the active `main` segment, and it has no active blocks.
  - The header parses, and its `BlockCount` equals `len(blocks)`.
  - The header's `MinSeq` is the namespace's `seq/next`, or the end of a
    registered vacancy that starts at `seq/next`.
  - Empty sealed segments (`BlockCount == 0`) skip the seq check, as
    `CheckInvariants` does.
- Then, in order:
  1. Mark the objects available and reference them (design §7.4).
  2. `InsertGeneration` → `InsertGenerationBlocks` → `SealSegment` →
     `InsertSegment(idx+1, active)`.
  3. Set `seq/next = MaxSeq + 1`.
- Unlike `Seal`, it accepts:
  - compacted blocks (`event_count` below the envelope width);
  - empty blocks;
  - vacancies registered in the same transaction or earlier.

**`ImportActiveBlocks(ns=main, blocks[], meta)`**

- This is `CommitBlocks` with one relaxation: a block may start above
  `seq/next` only when the gap is exactly a registered vacancy. The vacancy
  is either already in `metadata_kv` or added by this transaction's meta.
- Local active blocks are never compacted, so they are dense and non-empty,
  as `active_segment_blocks` requires.

**`ImportSeal(ns=main, idx, header, footerObj)`**

- This is `Seal` with the footer and header taken from the local file
  instead of built by the maintainer.
- It keeps `checkSealList`'s exact match between the footer's block list and
  the active rows.
- The one change: the header's `EventCount` check allows registered
  vacancies.

**`SetMigrationState(from, to, meta)`**

- A compare-and-set on `migration/state`, plus a meta batch.
- It is used for every state change. That includes the final `done`
  transaction, which also writes the last metadata delta and releases the
  lease (§6.7).

None of these runs outside migration mode:

- The three import scripts refuse unless `migration/state` is `seeding`,
  `tailing`, or `handing_off`. H3 (§6.7) ships the final blocks and seal
  while the state is `handing_off`.
- `SetMigrationState` allows only these transitions:
  - `seeding → tailing → handing_off → done`;
  - `handing_off → tailing`, the revert in §6.7;
  - any state except `done` → `aborted`.
- Once `done` commits, only the §8 `reverted` path can change the state.

### 6.5 Local compaction pause

**Why pause.**

- Local compaction rewrites a whole segment file when any row in it drops.
  On a large archive with deletes spread across history, that can be most
  segments every pass.
- Following it would mean re-reading most of the archive every pass, and
  uploading every changed block, just when the replica should converge.
- The replica's `compaction/seq` must also describe the generations it
  actually holds.
- A paused compactor solves both:
  - sealed files do not change while the migrator copies them;
  - the replica's watermark is simply the local one at the moment of the
    pause.

**How.** Today the only off switch is `JETSTREAM_COMPACTION_INTERVAL=0`. It
needs a restart, and it also turns off tombstone tracking. The pause still
needs tombstones, because tombstones from the paused period must reach the
first disaggregated pass through `compaction/seq`. So the pause is a new
mechanism:

- `orchestrator.PauseCompaction(ctx)` waits for any running pass to finish.
  - The watermark only advances after a whole chunk publishes, so between
    passes every segment is consistent with it.
  - It then stops new passes and returns the watermark.
- The migrator calls it before the seed starts, and records the pause in
  Pebble as `migration/compaction_paused`.
- Session start honors that key, so the pause survives deploys and restarts.

**What the pause costs.**

- The in-memory tombstone set grows at the delete rate. It must stay under
  `JETSTREAM_COMPACTION_TOMBSTONE_CAP` for the bound below.
- `compaction/deadline` must stay truthful. While paused, the scheduler
  publishes "unknown", so archive responses go out `no-cache`, as on a fresh
  session. Clients lose CDN caching during the migration, but nothing
  becomes stale.
- Deleted and updated rows stay in the archive longer. Clients fold
  tombstones themselves, so this costs size, not correctness.
- After handoff, the first disaggregated leader session rebuilds tombstones
  from `compaction/seq`. It reads every block written during the pause,
  16 in flight, and that time adds to the handoff's ingest pause (§6.7).
  Keep the pause short. An optional improvement (§12) would run live ingest
  alongside the rebuild, which shortens every failover, not just this one.

**Bound.** `JETSTREAM_MIGRATION_MAX_COMPACTION_PAUSE` (default 5 days).

- Past the bound, the migrator refuses to hand off and raises an alert.
- The operator then either hands off or aborts. An abort clears the pause.

### 6.6 Metadata

**Classification.** This table comes from the code at `f42df08`. The S0
inventory (§13) checks it against the source's real Pebble before anything
is copied. Note that README §3.5's `account/<did>` and `sync/<did>` entries
are stale; the real keys are listed here.

| Keys | Writer | Action |
|---|---|---|
| `relay/cursor` | live consumer | Copy byte for byte. The versioned encoding is the same in both modes. |
| `phase`, `phase/entered_at`, `backfill/timing/*` | lifecycle | Copy. Withhold `phase` until the seed completes (§6.2), then copy it as `steady_state`. |
| `compaction/seq` | compactor | Copy. The pause freezes it (§6.5). |
| `repo/<did>`, `handle/<handle>`, `pdshost/<host>`, `host/<host>`, `backfill/counts` | backfill | Copy. |
| `relay/list_repos_cursor`, `bootstrap/last_listrepos_cursor` | retired backfill | Copy, after asserting they are empty or absent. Startup fails loudly when they are non-empty (`specs/gotchas.md`). |
| `sync/chain/*`, `sync/host/*`, `sync/ident/*`, `sync/acct/*` | syncstate | Copy. If these are wrong, the new leader sees chain breaks everywhere and resyncs repos from PDSes at scale. |
| **`sync/identity/<did>`** | identity cache, written with **NoSync** outside `metastore.Store` | **Drop.** Disaggregated mode uses an in-memory LRU instead. This cache is never swept, so it may be a large share of Pebble. Note the prefix: a naive `sync/` copy would include it. |
| `seq/next` | ingest writer | Copy. At handoff, assert it equals the final sealed `MaxSeq + 1`, allowing for vacancies. |
| `seq/gap/*` | seq lease | Copy. §7 makes disaggregated mode read them. |
| `seq/max_reserved` | seq lease | Drop. The disaggregated writer has no lease, and after a clean local close this key equals `seq/next` anyway. |
| `live_segments/seq/next`, `merge/*` | bootstrap and merge | Drop. Local merge leaves `live_segments/seq/next` in Pebble, and copying it fails invariant 2 at the first `Rebuild`. Assert first that the phase is `steady_state`. |
| `compaction/deadline` | — | Never in Pebble locally. The disaggregated scheduler writes its own. |
| `import/current`, `import/job/*` | removed timestamp import | Drop. |
| `migration/*` (local side) | migrator | Not copied. |
| Anything else the inventory finds | — | The migrator refuses to start. A new prefix needs a decision, not a default. |

A local data directory may also hold dead directories from the removed
timestamp-import feature, `import-rules/` and `imports/`. They are ignored.

**Bulk copy.**

1. Install a commit observer on the pebblestore handle (§6.6.1).
2. Take a Pebble snapshot.
3. Iterate the snapshot and stream the rows into PostgreSQL, in fenced
   transactions of about 50k rows each. Each one `COPY`s into a temp table,
   then runs `INSERT ... ON CONFLICT DO UPDATE`.
   - The design's retry-scan benchmark loaded about 100k `repo/` rows a
     second into disk-backed PostgreSQL. Tens of millions of keys therefore
     take minutes to an hour.
   - The copy is throttled.

The observer starts before the snapshot, so every key changed after the
snapshot is already in the dirty set.

**Tailing.**

- Every `JETSTREAM_MIGRATION_META_FLUSH_INTERVAL` (default 2s), the migrator
  swaps out the dirty set and reads each key's current value from Pebble.
  It then upserts the value, or deletes the key if it is absent, in one
  fenced transaction.
- `DeleteRange` ops are recorded as ranges. Flushing one deletes the range in
  PostgreSQL, then re-copies whatever Pebble now holds inside it.
- The dirty set per flush is about the number of distinct keys written in
  one interval.

**Restart.**

- The dirty set lives in memory, so a restart of the source loses it.
- If the bulk copy had not finished, the migrator resumes from the last
  committed key, `migration/meta_cursor`, which is written with each chunk.
- If the bulk copy had finished (state `seeding` or `tailing`), the migrator
  runs a **diff resync**:
  - It merge-joins a new Pebble snapshot against `metadata_kv` in key order.
  - It writes only the differences.
  - It reads both sides in full, but writes little.

**Verification.**

- **Before handoff**, a full diff must find no difference outside the dirty
  set of that moment. It runs as a background job in `tailing` and reports
  as a metric.
- **At handoff** (§6.7), the migrator compares these keys exactly:
  - the critical keys: `relay/cursor`, `seq/next`, `compaction/seq`,
    `phase`, `backfill/counts`;
  - every `seq/gap/*` key.
- The diff check is a handoff precondition because of `sync/chain/*`. Wrong
  chain state would make the new leader treat every commit as a chain break,
  and resync repos from PDSes at scale.

#### 6.6.1 Commit observer

Every metadata write goes through three `pebblestore` methods: `Set`,
`Delete`, and `batch.Commit`. The only bypass is `SetNoSync` and
`DeleteNoSync`. Only the identity cache uses them, and its keys are dropped
anyway.

- A Pebble batch does not expose its staged ops. So the pebblestore batch
  records its keys and delete ranges as it stages them, the same way
  `metastore/fault.go`'s `faultBatch` and `metastore.OpBatch` do.
- After a successful commit, the batch hands them to an optional observer.
- The observer must not block. It appends keys only, not values, to a
  bounded in-memory set.
- If the set grows past `JETSTREAM_MIGRATION_DIRTY_MAX_KEYS` (default 5M),
  the migrator drops it and schedules a diff resync, so it never grows
  without bound.
- The only production `DeleteRange` today is the seq lease's rewrite of
  `seq/gap/`. The observer still handles ranges in general.

`pebblestore` also needs a read-only snapshot iterator for the bulk copy and
the diff resync. Today it keeps `*store.Store` unexported, and no code calls
`NewSnapshot`. The new iterator stays inside `pebblestore`, so
`TestOnlyPebblestoreImportsStore` still holds.

### 6.7 Handoff

The operator starts the handoff with `jetstream migrate handoff`.

- That command writes a request row into a new `migration_control` table.
- The table is operator-only and takes no fence. Only the migrator reads it.
- No HTTP mutation endpoint is added to the source.

**Preconditions.** The migrator checks all of these, and refuses with the
failing reason otherwise:

- the state is `tailing`;
- the replica lags by at most 2 blocks;
- the metadata dirty set was flushed within the last flush interval;
- the last full metadata diff is clean and under 24h old;
- the header reconcile is clean;
- at least one disaggregated pod reports ready and has caught up to the
  replica tip (`jetstream_catalog_revision` matches);
- `migration/compaction_paused` is set, and the pause is within its bound;
- object-store free space is above a configured floor.

**Steps.**

- The ingest pause runs from H2 until the new leader's firehose starts in H8.
- A crash during H1–H5 reverts to local ingest, because the decision point
  is the H6 commit in PostgreSQL.
- This is a two-phase commit. PostgreSQL is the coordinator, and the Pebble
  guard is the local participant's prepared record.

| Step | Action | If the source crashes here |
|---|---|---|
| H1 | `SetMigrationState(tailing → handing_off)`. Mirror it in Pebble. | On restart it sees `handing_off` and no `done`. It returns to `tailing` and keeps ingesting locally. Nothing had stopped yet. |
| H2 | `orchestrator.QuiesceForHandoff`: stop the live consumer, the retry runner, and every other metadata writer. Then `DrainDurability`, then `SealActiveAndClose`. The close collapses the seq lease to the exact frontier. | On restart, Pebble says `handing_off` and PostgreSQL does not say `done`, so it reverts to `tailing` and resumes local ingest. The local close was clean, so no vacancy is added. |
| H3 | Ship the remaining blocks, `ImportSeal` the final segment, and flush the last metadata dirty set. No local metadata writer is running now, so this delta is complete. | Same as H2. |
| H4 | Verify: replica `seq/next` = local `seq/next` = last sealed `MaxSeq + 1`; the critical keys match; the gap keys match; the header reconcile is clean for the final segment. | Same as H2. |
| H5 | **Local guard, part 1.** Write `migration/handoff = pending{archive_id, seq}` to Pebble with `sync=true`. | On restart it sees `pending` and asks PostgreSQL. If `done` is not committed, it clears the guard and reverts. If PostgreSQL is unreachable, it refuses to start ingest and keeps retrying; it may still serve reads. |
| H6 | **The decision.** One fenced transaction writes `migration/state = done` and `migration/handoff_seq`, and releases the lease (`holder_id = NULL`). If the `COMMIT` result is unknown, re-read `migration/state` to learn the outcome. | On restart it sees `pending` in Pebble and `done` in PostgreSQL. It must not ingest (§6.8). |
| H7 | **Local guard, part 2.** Write `migration/handoff = done` to Pebble. | — |
| H8 | A disaggregated pod's `MayAcquire` check passes, and it takes the lease within `ACQUIRE_INTERVAL`. Its session runs the cheap invariants and `Rebuild` (there are no hot batches), rebuilds tombstones from `compaction/seq`, opens the hot writer at `seq/next` on the new active segment, and starts the firehose at `relay/cursor`. | An ordinary failover. |

**Expected ingest pause.** It is the sum of:

- one local drain and seal (seconds);
- shipping the final segment's last blocks and its footer (seconds);
- the PostgreSQL transactions for H4–H7 (under a second);
- lease acquisition (≤ `ACQUIRE_INTERVAL`);
- the new session's cheap invariant check (about 4.5s at a 7,000-segment
  archive, design §22.2);
- the tombstone rebuild over the compaction pause (§6.5).

Expect about a minute. The upstream relay buffers in the meantime, so events
arrive late, not lost.

**Reads never stop.** The local process serves reads through H7, and the
disaggregated pods serve throughout.

### 6.8 After handoff: the local process

**Drained mode.** On H7 the local process enters drained mode:

- It serves archive reads from its frozen local files. These are
  byte-identical to the catalog, so in-flight downloads finish.
- It answers new subscribe requests with 503. The Go client retries a 503
  dial indefinitely, so a client that still reaches the old process through
  a stale route ends up on the disaggregated pods.
- It closes every websocket with 1001, spread over
  `JETSTREAM_MIGRATION_DRAIN_SPREAD` (default 60s) so reconnects don't all
  arrive at once.
- Subscribers reconnect through the routes that were flipped before the
  handoff (§10), land on the disaggregated pods, and resume by cursor.

**Startup guard.**

- A local-mode process whose Pebble has `migration/handoff = done` refuses
  to start ingest, permanently.
- One whose Pebble has `pending` asks PostgreSQL first, as in H5.
- In both cases it may still serve reads, so a stray restart of the old
  workload is harmless.
- Only the rollback command (§8) clears the guard.
- This guard is what keeps requirement 4 true through restarts, crashes, and
  operator mistakes.

### 6.9 Tailing details

**Blocks.**

- Active-block progress is pull-only:
  - `ActiveSource.ActiveSegment()` is sampled under the writer lock;
  - the catalog revision does not move on active flushes.
- The migrator polls it on a 250ms tick, using `ReadableLog.DurableSeq()` as
  the cheap "anything new?" check.
- It ships only blocks at or below the Pebble-durable frontier. A block that
  was fsynced but whose batch never committed is not shipped until a restart
  reconciles `seq/next` over it.
- It reads frames through the local `Fetcher`, which checks the length
  prefix. It never reads a frame past the flushed size.
- The single `OnDurableBatch` slot belongs to the live consumer, so the
  migrator leaves it alone.

**Seals.**

- The migrator registers a `catalog/local.OnSealed` hook. That hook runs
  under the writer mutex, and an error from it fails the rotation. So the
  migrator's hook only enqueues the `SealedView` and the path, and never
  returns an error.
- A worker then ships any blocks not yet shipped, uploads the footer bytes,
  and runs `ImportSeal`.
- The order is strict: segment *k*'s blocks, then the seal of *k*, then
  segment *k+1*'s blocks.
- The footer is copied, never rebuilt. Sealing writes the footer at EOF and
  then the header, and never rewrites block bytes. So `[256, footer_offset)`
  of the sealed file is byte-identical to what was already shipped as active
  blocks.

**Compaction publishes.** These have no catalog hook: `OnSealed` does not
fire on a rewrite. The pause (§6.5) makes that moot, and the header reconcile
catches any violation.

**Vacancies.**

- An unclean restart of the source during migration registers a new
  vacancy.
- The vacancy commit is a Pebble batch, so the observer sees it.
- The migrator ships the `seq/gap/*` key in the same `ImportActiveBlocks`
  transaction as the first block after the gap.

**Backpressure.**

- The migrator's queue is bounded.
- If it falls behind, for example during an object-store outage, it stops
  reading and alerts.
- Local ingest is unaffected. The local files and Pebble act as the queue,
  and they are durable.

**Catch-up must stay small for the readers.**

- A follower's `feed` decodes every seq between its log tip and the new tip
  in one tick, in memory, and treats any unexplained hole as fatal.
- So after an outage, the migrator ships at most
  `JETSTREAM_MIGRATION_TAIL_MAX_BLOCKS_PER_TXN` (default 16) blocks per
  transaction. It spaces the transactions at least one follower poll
  interval apart.
- That bounds what a shadow pod decodes in one tick.
- The same reasoning is why `phase` is withheld until the seed is complete
  (§6.2): every pod's readable log then starts at the post-seed tip.

## 7. Seq vacancies in disaggregated mode

This is needed for any vacancy that exists before the migration, and for any
added during it. The data is static. Disaggregated mode never creates
vacancies, so after handoff the set never changes.

Where a local vacancy can sit (`seqlease.go`, `seqspace.go`):

- **Between two blocks of one segment.** This happens when the active file
  was resumed with blocks, and it is the usual case.
- **Between segments.** This happens after a crash between a rotation and the
  next segment's first durable block, or when the highest file was already
  sealed at restart.
- **At seq 1**, on an empty archive.

A vacancy is never inside a block. S0 reports which case each existing
vacancy is. All cases must be handled anyway, because a crash during the
migration can add one anywhere.

1. **Follower.**
   - Load `seq/gap/*` into the mirror on every tick. The range is tiny, and
     the read can be guarded by revision like the other metadata reads.
   - `Follower.SeqGaps()` returns that set. Today it returns nil.
   - `feed` treats a seq inside a registered vacancy as `FollowerLog.Skip`,
     as it already does for compaction holes, instead of as corruption.
2. **Cold replay** (`internal/subscribe/replay.go`).
   - The floor-mode path passes the follower's gaps instead of nil.
   - The existing local-mode jump logic then applies unchanged.
   - Cursor resolution's `env.Gaps.EndContaining` starts working too.
3. **`catalog.CheckInvariants`.**
   - `InvariantOptions` gets the gap set.
   - Sealed tiling may skip exactly a registered vacancy between segments.
   - Active-block density may skip exactly a registered vacancy between
     blocks.
   - Everything else stays strict.
4. **Normal hot and direct commit paths stay gap-free.** Only the import
   scripts accept a vacancy (§6.4). That keeps the design's rule that
   disaggregated mode never creates vacancies.
5. **Validation at import.** `ImportSealedSegment` and `ImportActiveBlocks`
   check each vacancy against block envelopes for overlap. This is the same
   rule `validateSeqGapsAgainstSegments` applies (`seqlease.go`).

## 8. Rollback

### Before H6: abort

Run `jetstream migrate abort`.

- The migrator sets `aborted`, releases the lease, clears
  `migration/compaction_paused`, and stops.
- The source continues as a normal local instance.
- Then delete the PostgreSQL database and the bucket prefix, or re-init them
  for another attempt.
- A later attempt must re-seed. Compaction has resumed, so the old replica's
  generations are stale, and the header reconcile would re-import most
  segments anyway.

### After H6: emergency only

This path is costly. Use it only if the disaggregated deployment cannot
serve. It is built and tested, but a routine problem should never need it:
fix forward on disaggregated where possible.

1. Scale the disaggregated workload to 0. Set `migration/state = reverted` so
   no pod can take the lease again.
2. Read the catalog's `seq/next`. Call it *D*.
3. Run `jetstream migrate reclaim --data-dir ... --vacancy-end=X` offline on
   the local data volume, with X = *D* + 1e9. It:
   - registers the vacancy `[S, X)`, where *S* is the local `seq/next`, and
     sets `seq/max_reserved = X` to match;
   - clears the local guard.

   The local `relay/cursor` is still the one from handoff time.
4. Start the source in local mode. What happens next:
   - **Re-ingest.** It re-ingests the firehose from its handoff-time cursor,
     and numbers the events from X.
   - **Clients.** Clients holding cursors in `[S, D)` jump to X through the
     vacancy, and get those events again under new seqs. That means
     duplicates, never losses.
   - **The relay must still hold the cursor.** This only works while the
     upstream relay still holds the handoff-time cursor, so measure its
     replay window before handoff. Past it, the events between the cursor and
     the window are lost to the archive. A re-sync from the PDSes is the only
     repair.
   - **Repo state.** Repos the disaggregated leader updated after handoff are
     re-derived from the re-ingest.
   - **Segment names repeat with different content.**
     - The disaggregated leader sealed segments *N+1…* over `[S, D)`. Local
       mode then writes its own segment *N+1* over seqs from X.
     - The ETag differs. An `If-Range` download of one of those segments
       restarts with a full 200, and a client that pinned the old plan sees
       the new content.
     - The oracle must cover this. If it proves messy, `reclaim` can instead
       skip local segment indexes past the catalog's highest.

## 9. Client impact

This section comes from a read-only audit of the client contract in this
repo.

**Identical:**

- seqs and cursor rules;
- segment names (`SegmentFilename(idx)`);
- getSegment and getBlock ETags (the header checksum), `Content-Length`, and
  Range bytes;
- `planSnapshot` and `listSegments` content;
- the zstd dictionary ID, which is embedded in the binary;
- frame formats.

No endpoint exposes an archive identity, so the Go client treats the
hostname as the seq namespace. That is only safe because seqs are preserved.

**Different but harmless:**

- `Last-Modified`: the local file's mtime versus the generation's
  `created_at`, which is the import time.
- Cache-Control is `no-cache` until the new leader writes
  `compaction/deadline`.
- Archive downloads get a new 1h response cutoff. The Go client resumes.
- A 503 with `Retry-After` when the object store is unavailable.
- A cold-read spike right after the flip. Each pod's readable log starts at
  the tip, so each reconnecting subscriber's first event comes from the
  object store.

**Could break, and how this plan avoids it:**

1. **Vacancies.** Without §7, replay across a vacancy loops on
   `InternalError` forever.
2. **A wrong `seq/next`.** H4 prevents it. A lower value would reuse seqs,
   and reconnecting clients would silently drop the new events as
   duplicates.
3. **`planSnapshot` (a POST) during a 503 window.**
   - atmos does not retry a POST on 5xx, so a backfilling Go client would
     get `ErrFatal`.
   - This plan has no 503 window on archive endpoints: the local process
     serves until the flip, and the disaggregated pods are ready before it.
4. **The `/` probe does not reflect readiness.** The flip must check that
   followers are ready, through `/status` or metrics, not just the
   orchestrator's readiness. That check is a §6.7 precondition. A `/readyz`
   that reflects `Follower.Ready` would close the gap for good (§12).

## 10. Deployment shape

**Seeding.** The source deployment gets the migration configuration:

- `JETSTREAM_MIGRATE_TO_DISAGGREGATED=true`;
- the PostgreSQL and object-store settings and credentials, the same ones a
  disaggregated deployment uses;
- the throttles.

Local-mode config validation must accept these variables when migration is
enabled; today it refuses unknown or irrelevant `JETSTREAM_*` variables.

**Tailing.** A separate disaggregated workload with two or more pods, its
own service, and a non-public shadow endpoint for validation.

- Its service selector must not match the local workload.
- Its pods need object-store network access.

**Flip, immediately before `migrate handoff`.**

- Point the public routes, the ingress and any proxy in front of it, at the
  disaggregated service. Confirm that new connections land there.
- From then on, new clients get the replica, which is at most about 2 blocks
  behind.
- Existing websockets stay on the local process until H7 closes them. They
  then reconnect through the already-flipped routes.
- A reconnecting client whose cursor is slightly ahead of the replica's tip
  must wait for the tip, not fail. Verify that the `FutureCursor` path does
  this in S8.
- The handoff then removes the lag.
- Flipping after H8 would be wrong: the 1001 closes at H7 would reconnect to
  the local process.

**Afterwards.**

1. Make the disaggregated workload the primary deployment.
2. Scale the local workload to 0, but keep its volume until the rollback
   window closes, because §8 needs it. Suggested window: 14 days.
3. Then delete the volume.

**Alerts.**

- Data-directory free-space alerts no longer apply after the flip.
- Live-ingest-rate alerts must tolerate the H2–H8 pause.
- Alerts on compaction watermark lag will fire during the compaction pause.
  Silence them.

## 11. Prerequisites and capacity

1. **PostgreSQL.**
   - **Placement.** Measure the round-trip time from the source's network to
     each candidate placement before choosing. Live latency scales with it,
     because every hot-batch commit and follower tick pays at least one
     round trip.
   - **Provisioning.** Use synchronous durability and HA failover (design
     §5.1). Never share the instance with another archive.
   - **Size.** Plan for:
     - the metadata from §6.6 (S0 measures it);
     - one `objects` row and one `generation_blocks` row per block;
     - WAL at the design §22.2 rate of about 510 bytes per live event.
2. **Object-store capacity.**
   - Physical size is compressed segment bytes × the replication factor,
     plus garbage not yet reclaimed by GC and compaction.
   - Keep enough headroom for growth until more capacity can be added.
   - Check per-node object or volume limits, not just bytes.
   - The bucket needs no TTL and no lifecycle rules: Jetstream deletes its
     own unreferenced objects.
3. **The relay.** The disaggregated pods must use exactly the same upstream
   relay URL as the source. `relay/cursor` is relay-specific, and a different
   relay would silently resume at an unrelated seq.
4. **Disaggregated stability at the source's scale.**
   - Check failover frequency and lease duration on an existing
     disaggregated deployment of similar size. Each failover is a live-tail
     pause for every subscriber.
   - Check compaction read volume per pass at that scale. That load arrives
     after the flip, with the first pass.
5. **Memory.**
   - The disaggregated pods need `GOMEMLIMIT` and the design §17 budget
     check.
   - On the source, the migrator stays bounded:
     - segments in flight: `SEGMENT_CONCURRENCY` × 256MiB at worst, though
       in practice it streams per block;
     - the dirty set: `DIRTY_MAX_KEYS` × about 60 bytes.
6. **A deploy freeze on the source** from H1 until the flip completes.
   Deploys during seeding and tailing are fine, because both resume.

## 12. Changes to the disaggregated design

Update the v2 design doc as each change lands.

- §2.2: migration from local is supported through this plan.
- §2.3 and §10.2: seqs are gap-free *for seqs this archive allocates*.
  Imported vacancies are allowed, and they are static.
- §9.3: invariant 1 allows registered vacancies.
- §6.3: the election loop has a `MayAcquire` hook and an `ErrStandby`
  session result.
- §15.1: `storage init --migrate-from-local`.
- Optional, but worth doing for every failover:
  - **Start live ingest while the tombstone rebuild runs.** Rebuild merges
    keep the highest seq per key, so live tombstones can be added
    concurrently. The first compaction pass must still wait for the rebuild.
  - **A `/readyz` on the public port that reflects `Follower.Ready`**, so
    ingress probes see readiness.

## 13. Delivery stages

Each stage ends with `just` green, plus the extra checks it names. S1 and S2
are independent and can run in parallel.

**S0. Dry-run inventory.** Read-only; ships first.

- With `JETSTREAM_MIGRATION_DRY_RUN=true`, the local process computes and
  exports the following, without connecting to PostgreSQL or the object
  store:
  - segment count and index contiguity;
  - each vacancy's position (inside a segment or between segments) and the
    blocks that bound it;
  - Pebble key counts and bytes by prefix, from a low-priority, throttled
    snapshot iteration;
  - confirmation that the identity cache's NoSync writes are the only ones
    that bypass `metastore.Store`;
  - an estimate of the seed's bytes and object count;
  - the size of any leftover dead directories on the data volume.
- It publishes the results on the debug listener as `/debug/migration`
  (read-only JSON), and as metrics.
- Running it on the source settles the §6.6 table and the open items in §14.

**S1. Vacancies in disaggregated mode (§7).**

- Follower, cold reader, cursor resolution, and `CheckInvariants`.
- Tests:
  - unit tests with gaps between blocks, between segments, and at the active
    tail;
  - a disaggregated oracle variant seeded with a catalog that has vacancies;
  - a mutant: the follower ignores imported gaps.
- Checks: `just test ./internal/oracle`, `just oracle-disagg`.

**S2. Migration guard.**

- `migration/state`, the `MayAcquire` hook, `ErrStandby`,
  `storage init --migrate-from-local`, and the follower loading
  `migration/state`.
- Tests: a lockertest-style race between a standby pod and a restarting
  migrator. A pod must never run the orchestrator on a catalog that is not
  `done`, including one with `phase` absent.

**S3. Import scripts (§6.4).**

- storagefake and pgstore implementations, plus `catalogtest` contract
  cases:
  - compacted generations;
  - empty blocks;
  - vacancies;
  - dedup of identical frames;
  - refusal outside migration states.
- A property test: import any local segment built by `segment` test helpers,
  then serve it through `objopener`. The bytes must equal the file, and the
  ETag must match.
- Checks: `just test-storage`.

**S4. Segment migrator (seed and tail).**

- `internal/migrate`: the reader, throttles, uploader pipeline, ordered
  import, reconcile, resume, and the subscriptions to writer and manifest
  progress.
- Crash seams in `internal/crashpoint`:
  - after upload, before import;
  - after import, before its local progress write;
  - mid-seal.

**S5. Metadata migrator (§6.6).**

- The commit observer, bulk copy, dirty flush, and diff resync, with the
  classification table enforced.
- A fuzz or property test: a random metastore op stream against pebblestore
  with the observer attached, then a flush. PostgreSQL (storagefake) must
  equal Pebble.

**S6. Compaction pause and handoff (§6.5, §6.7, §6.8).**

- `PauseCompaction` and `QuiesceForHandoff`.
- The `migration_control` table and the `jetstream migrate
  status|handoff|abort` CLI.
- The local guard, drained mode, and `migrate reclaim`.

**S7. Migration oracle tier.** One run does all of this:

1. Boot a local instance on the simulator.
2. Drive it into steady state, with at least one unclean restart so a
   vacancy exists.
3. Enable migration.
4. Kill the local process at every migrator crash seam and every handoff
   step, H1–H7.
5. Run two disaggregated reader pods during tailing.
6. Hand off.
7. Let a disaggregated leader continue.

Assertions:

- The union of everything delivered matches the model. That is, from the
  local process before the flip and from the disaggregated pods after it.
- No seq is reused, and per-DID order is kept.
- Every pre-handoff sealed segment's bytes equal the local file's.
- There is exactly one writer at all times, through both the fence and the
  guard.
- After a kill at H5 or H6, the local restart never ingests once `done` has
  committed.
- A client that plans on the old process and goes live on the new pods sees
  every event.
- The §8 rollback, run in the oracle, also passes: no seq is reused, and
  duplicates appear only in the reclaimed range.

Mutants:

- the migrator ships a block before its Pebble commit;
- the dirty set misses a `DeleteRange`;
- the handoff skips the `seq/next` check;
- the local guard is not checked at startup;
- the import accepts an unregistered hole.

Checks: `just mutation-campaign`, and record the scorecard.

**S8. Rehearsal.**

1. **At small scale.**
   - Run a local-mode instance for a few days.
   - Migrate it into a scratch database and bucket prefix, following the full
     runbook.
   - Include one deliberate process kill mid-seed, and one abort followed by
     a retry.
2. **At full scale.**
   - Run seeding and tailing for real against the source and its production
     targets.
   - Keep shadow validation running for at least 24h.
   - Then **abort** (§8), clear the pause, and drop the database.
   - This measures the real seed time, the volume and object-store impact,
     and the diff-resync time, with no client exposure.

**S9. Cutover.** Write an operator runbook covering:

1. prerequisites;
2. seed;
3. tailing and shadow validation;
4. flip;
5. handoff;
6. watch;
7. cleanup after the rollback window.

**Shadow validation**, in S8 and S9, before any handoff:

- **Stream equality.** Subscribe to both endpoints with the same cursor.
  Compare `(seq, payload)` frame by frame, for live traffic and for a replay
  across the full cursor lookback.
- **Archive equality.**
  - Compare `planSnapshot` for several filters.
  - Compare ETags for every segment.
  - Compare whole-file SHA-256 for a random 1% sample, and for every segment
    that contains a vacancy.
- **Catalog.** Run the full (not cheap) `CheckInvariants` against the
  replica.

## 14. Open items

- **Pebble inventory.**
  - Sizes per prefix.
  - Confirmation that no prefix outside §6.6's table exists. S0 answers this.
  - The code audit found one bypass, the identity cache's NoSync writes.
    Those keys are dropped.
- **PostgreSQL placement and RTT** (§11.1).
- **The upstream relay's replay window**, for the §8 rollback.
- **Archive endpoints in drained mode.**
  - The local process may still answer an in-flight `planSnapshot` with its
    frozen `sealedTip`.
  - That is correct: the client then cuts over to live, which reaches the
    disaggregated pods because the routes were flipped before the handoff.
  - S7 covers it.
- **A local-ingest stall only for the handoff.**
  - A safer but slower alternative to H2's in-process quiesce: restart the
    source with `JETSTREAM_MIGRATION_HANDOFF=true`, so the handoff runs in a
    fresh session before ingest starts.
  - The in-process path keeps reads serving and avoids a restart, so it is
    preferred.
  - The restart path is the fallback if quiescing every metadata writer
    proves hard to make airtight.

## 15. Implementation notes

What landed, and where it departs from the plan above.

**Where the code is.**

- `internal/catalog`: `migration/state` and its transitions, the import
  scripts (`ImportSealedSegment`, `ImportActiveBlocks`, `ImportSeal`,
  `ImportMeta`), `SetMigrationState`, `InitMigration`, and the vacancy
  registry. `CheckInvariants` accepts registered vacancies only.
- `internal/leader`: `MayAcquire`, `ErrStandby`, and `ErrDone`.
- `internal/migrate`: the migrator (seed, tail, metadata copy, reconcile,
  handoff) and the local guard.
- `internal/jetstreamd`: the wiring in the local process (the ingest gate,
  the compaction gate, drained mode, `/debug/migration`), and the operator
  operations behind the CLI.
- `internal/pgstore`: the operator control tables.
- `cmd/jetstream`: `storage init --migrate-from-local`, `serve
  --migrate-to-disaggregated` (`JETSTREAM_MIGRATE_TO_DISAGGREGATED`) with
  the `JETSTREAM_MIGRATION_*` knobs, and `jetstream migrate
  status|handoff|abort|reclaim`.

**Departures from the plan.**

- **Vacancies are one key.** `seq/vacancies` holds main's whole registry
  (`seqspace.Gaps`), not one key per gap. Only the import scripts write it,
  and only above the imported frontier.
- **No seal at handoff.** H2 stops the local writer session instead of
  sealing: the active segment's durable blocks are shipped as active
  blocks, and the new leader continues that segment. H3 has no
  `ImportSeal`. A seal at handoff would add a seal and an upload to the
  ingest pause, and the catalog already handles an active segment.
- **No `bootstrap_live` segment.** `InitMigration` creates only main's
  segment 0 and `migration/state = seeding`, in one transaction. A migrated
  archive is past merge, so there is nothing for the phase transaction to
  delete.
- **H1 is not mirrored in Pebble.** The local process learns
  `handing_off` from the catalog. The guard (H5, H7) is the only local
  record of a handoff.
- **H6 does not clear the lease holder in the transaction.** The session
  ends with `leader.ErrDone`, so `leader.Run` returns nil and releases the
  lease. A pod's `MayAcquire` passes once the state reads `done`.
- **The session check is `ReadMigrationState`**, a fenced read at the
  start of every disaggregated session, ahead of the orchestrator. The
  follower also mirrors `migration/state` for `MayAcquire`.
- **Operator control is two tables outside the versioned schema**
  (`migration_requests`, `migration_status`), created with `CREATE TABLE IF
  NOT EXISTS`, so `pgstore.SchemaVersion` is unchanged. A request is
  claimed by the migrator before it acts; until then the operator can
  withdraw it (a CLI timeout does). A new migrator session answers every
  unanswered request as abandoned: a handoff is never replayed after a
  restart.
- **Metadata resync is a merge join**, not a cursor walk. A full resync
  iterates the source's snapshot and the catalog together in key order and
  writes only the differences, deletes included. It runs at the start of
  every migrator session (the seed's bulk copy is this first resync), then
  every `VERIFY_INTERVAL`, and whenever the dirty set overflows. Seeding
  becomes tailing only after both the segments and that resync are done.
- **`relay/cursor` is copied only at handoff.** Copied while tailing, it
  could run ahead of the shipped events. H3 copies it after
  ingest stops, and H4 checks it byte for byte with the other critical
  keys.
- **Handoff preconditions.** The migrator checks:
  - the state is `tailing`;
  - the replica trails local ingest by at most `HANDOFF_MAX_LAG_SEQS` seqs
    (default 8,192), not a block count;
  - a full resync finished within `HANDOFF_MAX_RESYNC_AGE` (default 24h);
  - the compaction pause is within `MAX_COMPACTION_PAUSE`;
  - every imported generation still matches the source's
    (`reconcileSealed`, from memory, before H1).

  The rest are the operator's: that a pod has caught up and serves, and
  object-store headroom. `jetstream migrate status` shows what they need.
- **H4 compares seq/next, vacancies, and the critical keys.** An optional
  full metadata comparison (`HANDOFF_FULL_VERIFY`) runs inside the pause and
  refuses on any difference.
- **A vacancy at the source's tip refuses the handoff.** If the source
  registered a vacancy and wrote no event after it, no block carries it to
  the catalog yet. The handoff reverts and asks for a retry once local
  ingest has written an event.
- **Reclaim takes a margin, not a vacancy end.** `jetstream migrate reclaim
  --data-dir ... --margin N` takes the writer lease (so it refuses while a
  pod holds it), sets the catalog to `reverted`, and sets the local
  `seq/max_reserved` to max(catalog next, local next) + N. The local seq
  lease then registers the vacancy at the next start, as after an unclean
  stop. It refuses a resume seq past the cursor ceiling. Segment indexes
  are not skipped, so names from the handoff's active segment on repeat
  with different content (§8).

**Not built.**

- **S0's dry-run inventory.** The migrator itself refuses a metadata key
  with no rule (`errUnclassified`), which stops the migrator and not the
  process, and `/debug/migration` and the status table report seed
  progress. The inventory remains useful for sizing before a migration.
- **S8 and S9** are operational: the rehearsals and the runbook.

**Tests.**

- The oracle's migration tests (`internal/oracle/migration_test.go`) run
  the whole migration against storagefake and the simulator:
  - seed, a mid-migration unclean restart, and tailing;
  - a shadow pod with byte-equal sealed segments;
  - handoff, a drained source, and a pod carrying on;
  - a client that plans on the source and goes live on the pod across the
    handoff;
  - a kill at each of the eight migration crash points;
  - reclaim.

  Every client view is compared with the model, and no commit at or past
  the handoff repeats one archived before it.
- Per-DID rev order is not asserted. A plain local archive under stress can
  archive a commit older than a preceding backfill or resync, without any
  migration code, so the check would test that and not the migration.
- `cmd/jetstream/migrate_test.go` runs the CLI against PostgreSQL and S3
  (`just test-storage`).
- Mutants m079–m084 cover the migration (the `migration` tier); see
  `testing/mutation/RESULTS.md`.
