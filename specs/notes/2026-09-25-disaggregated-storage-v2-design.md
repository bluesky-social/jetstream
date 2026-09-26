# Disaggregated storage v2

Date: 2026-09-25. Status: proposed.

This document replaces `specs/notes/2026-09-20-disaggregated-storage-design.md`.
That file stays for history and is not the design to build. Where they disagree,
this document wins.

Read `docs/README.md` §3 (segment format, metadata keys) and §4 (ingest phases)
first. This document assumes you know them.

## 1. Summary

Jetstream gets a second storage backend. Local mode (segment files and Pebble on
one machine) stays and stays the default. The new mode stores:

- blocks and segment footers as immutable objects in S3-compatible storage;
- everything else in PostgreSQL: catalog, metadata keys, recent events, and the
  leader lease.

No pod keeps anything on local disk. Any number of pods serve reads. One pod at a
time is the leader. The leader runs ingest, bootstrap, merge, fold, seal,
compaction, and GC. Pods never talk to each other. They talk only to PostgreSQL
and S3.

A new event becomes visible when its batch commits to PostgreSQL. Batches are
small (about 15ms). Every pod polls PostgreSQL for new batches, with
`LISTEN/NOTIFY` as a hint to poll sooner. Later, the leader groups batches into
normal segment blocks, uploads them to S3, and removes the batches from
PostgreSQL.

The segment format does not change. A sealed segment served from S3 is
byte-for-byte the file local mode would have written from the same events.

## 2. Scope

### 2.1 Goals

- Zero local disk in disaggregated mode. Caches are memory only.
- All pods serve every read endpoint: `/subscribe`, `subscribeEvents`,
  `planSnapshot`, `getSegment`, `getBlock`, and `/status`.
- Hot-path capacity of 3,000 events/s sustained. Pop1 today: 428 events/s 7-day
  mean, 2,128/s 1-minute peak.
- Live-event latency regression of at most 20–40ms compared to local mode.
- Bulk traffic (failed-repo retries, resyncs, large-PDS recovery) must never
  starve live traffic or overload PostgreSQL.
- If the leader dies, another pod takes over within a few seconds with no data
  loss and no seq reuse.
- The same core code (writer, sealer, compaction, cold reader, manifest,
  planner) runs in both modes, behind storage interfaces.
- Works with AWS RDS PostgreSQL and AWS S3, and with self-hosted
  PostgreSQL and S3-compatible stores such as SeaweedFS and MinIO.

### 2.2 Non-goals

- Migrating an existing local archive. New deployments bootstrap from scratch.
- Point-in-time restore that keeps the same cursor namespace. A restored
  database is a new archive (§15).
- Multi-region or multi-writer ingest.
- Timestamp import. It is removed (§21) and will get its own design later.
- Changing the segment or block format.
- Any local disk cache.

### 2.3 Decisions

| Topic | Decision |
|---|---|
| Visibility point | PostgreSQL commit of a hot batch (steady state) or a block (bootstrap/merge) |
| Wake-up | `NOTIFY` doorbell plus 250ms polling; correctness never depends on `NOTIFY` |
| Leader election | PostgreSQL lease implementing atmos `streaming.DistributedLocker`, driven by a jetstream-owned election loop |
| Fencing | `writer_epoch` checked by the first statement of every leader write transaction |
| Seqs | Gap-free, from `seq/next` in PostgreSQL; no write-ahead seq lease, no vacancies |
| Objects | Immutable, unique keys, never overwritten; SHA-256 recorded and verified |
| S3 features used | PUT, GET (with Range), DELETE. Nothing else |
| Metadata | Existing Pebble key/value encoding in a PostgreSQL table |
| Compaction | Fetches only affected blocks; no format change |
| Recovery | HA failover only; a restore is a new archive |
| New deps | `github.com/jackc/pgx/v5`, `github.com/aws/aws-sdk-go-v2` |

## 3. Terms

- **Leader**: the pod that holds the lease. Exactly one pod writes at a time.
- **Epoch**: `archive.writer_epoch`. Bumped every time a pod acquires the lease.
- **Session**: the time between one pod's lease acquire and its lease loss or
  release. A session has one epoch.
- **Namespace**: `main` (the served archive) or `bootstrap_live` (live events
  captured during bootstrap, merged into `main` later). Each namespace has its own
  segments and its own seq counter, the same as local mode's `segments/` and
  `backfill/live_segments/`.
- **Hot batch**: one or more consecutive events committed as one row in
  `hot_batches`. Either **inline** (the encoded block bytes are in the row) or
  **pointer** (the row names an S3 object holding the encoded bytes).
- **Open block**: the block currently collecting hot batches. Its events are
  visible through hot batches only.
- **Fold**: encode an open block, upload it, attach it to the active segment,
  and delete its hot batches, all in one transaction.
- **Active block**: a folded block attached to the active segment
  (`active_segment_blocks`).
- **Generation**: one immutable sealed version of a segment: header, footer
  object, and an ordered list of block objects. Compaction creates new
  generations.
- **Mirror**: a pod's in-memory copy of the catalog.
- **Follower**: the per-pod loop that keeps the mirror and the readable log in
  sync with PostgreSQL.
- **Revision**: `archive.catalog_revision`. Bumped by every leader write
  transaction. Rows changed by that transaction store the new value.

## 4. Architecture

```
                 relay / PDSes
                      │
          ┌───────────▼────────────┐
          │ leader pod             │        reader pods (N)
          │  ingest writer         │        ┌──────────────────┐
          │  maintainer (fold,seal)│        │ follower         │
          │  compaction, GC        │        │ mirror           │
          │  follower + readers    │        │ readable log     │
          └───┬───────────────┬────┘        │ caches           │
              │ fenced txns   │ PUT/GET     └───┬──────────┬───┘
              ▼               ▼                 │ SELECT   │ GET
        ┌───────────┐   ┌──────────┐            │          │
        │PostgreSQL │◄──┼──────────┼────────────┘          │
        │ archive   │   │ S3 bucket│◄──────────────────────┘
        │ catalog   │   │ objects  │
        │ hot rows  │   └──────────┘
        │ metadata  │
        └───────────┘
```

The leader pod also runs a follower and serves reads, the same as any other
pod. Its follower gets an in-process doorbell after each commit and reads back
from PostgreSQL like everyone else. The leader never feeds its own readable log
directly. That keeps one visibility rule for every pod: **a pod shows an event
only after reading it from a committed PostgreSQL transaction.**

## 5. External requirements

### 5.1 PostgreSQL

- Version 15 or newer.
- One database per archive. Jetstream owns the schema.
- Synchronous durability. On RDS, Multi-AZ is recommended. `synchronous_commit`
  must not be `off` for Jetstream's role.
- Session settings on every connection:
  - `statement_timeout = 10s`
  - `lock_timeout = 5s`
  - `idle_in_transaction_session_timeout = 30s`
- All timestamps come from the database clock (`now()`). Pod clocks are never
  compared with database times.

### 5.2 Object store

Only these operations are used:

- `PutObject` of a whole object. It must be atomic: a GET returns either
  nothing or the complete object.
- `GetObject`, with and without `Range`. A GET after a successful PUT must return
  the new object (read-after-write). AWS S3, SeaweedFS, and MinIO provide this.
- `DeleteObject`.

Not used: conditional writes, versioning, listing, multipart upload, object tags,
lifecycle rules, and S3-computed checksums. Jetstream verifies integrity itself
(§7.3).

Configuration: endpoint URL, region, bucket, key prefix, path-style addressing
on or off, and credentials from the standard AWS SDK chain. Use
`aws-sdk-go-v2/service/s3`. Do not write a custom client.

## 6. Leadership

### 6.1 Why jetstream owns the loop

The atmos client can gate its firehose consumer behind a `DistributedLocker`.
That is not enough here. The leader must also own bootstrap backfill, merge,
fold, seal, compaction, and GC, and the live consumer is stopped during merge.
So Jetstream runs its own election loop (`internal/leader`), with the same
Acquire/Renew/Release semantics and timing as atmos. The PostgreSQL lock type
implements `streaming.DistributedLocker`, so the contract is shared and tested
the same way.

The atmos client inside a leader session is built with `streaming.NoopLock`. It
only ever runs inside a session, so it needs no gating of its own. No atmos
change is needed.

### 6.2 Lease SQL

Each process picks a random `holder_id` (UUID) at startup.

Acquire:

```sql
UPDATE archive
SET writer_epoch = writer_epoch + 1,
    holder_id = $holder,
    lease_expires_at = now() + $lease
WHERE id = 1
  AND (holder_id IS NULL OR lease_expires_at <= now())
RETURNING writer_epoch;
```

Zero rows means `streaming.ErrLockHeld`. On success, store the epoch. The lock
type exposes `Epoch() uint64`, because `Acquire` only returns an error.

Renew:

```sql
UPDATE archive
SET lease_expires_at = now() + $lease
WHERE id = 1 AND writer_epoch = $epoch AND holder_id = $holder
  AND lease_expires_at > now();
```

Zero rows means `streaming.ErrNotHolder`.

Release:

```sql
UPDATE archive SET holder_id = NULL, lease_expires_at = now()
WHERE id = 1 AND writer_epoch = $epoch AND holder_id = $holder;
```

Zero rows means `streaming.ErrNotHolder`.

Defaults match atmos: lease 3s, renew every 1s, acquire attempt every 500ms. All
three are configurable (§18).

### 6.3 Election loop

```
loop until process shutdown:
    err := locker.Acquire(ctx, lease)
    if err == ErrLockHeld or other error: sleep acquireInterval; continue
    epoch := locker.Epoch()
    sessionCtx, cancel := context.WithCancel(ctx)
    start renewer(sessionCtx, cancel):
        every renewInterval: Renew
        on ErrNotHolder: cancel()
        on other error: retry; if no successful renew for `lease`, cancel()
    err = runSession(sessionCtx, epoch)   // blocks until the session ends
    cancel(); wait for every session goroutine to exit
    locker.Release(ctx with 5s timeout)   // best effort
    if err is fatal: exit the process non-zero
    sleep acquireInterval
```

"Fatal" is the default: a session error is restartable only if it wraps
`leader.ErrRestartSession` (lease loss, a fence failure, an unknown commit
result, an S3 failure retries cannot fix). An error nobody classified ends the
process, which keeps the crash-loud rule for corruption. A cancellation error
is benign only when the loop cancelled the session. Each `Renew` call is
bounded by `lastOK + lease`, so a hung call cannot outlive the lease.

Lease timing only affects how fast failover happens. Safety comes only from the
epoch fence (§6.4). A paused or partitioned old leader can keep running for any
length of time. Its writes are rejected by the fence. Its S3 PUTs create
objects that nothing references, and GC removes them (§7.3, §13).

### 6.4 The fence

Every leader write transaction starts with this statement, before it reads or
writes anything else:

```sql
UPDATE archive SET catalog_revision = catalog_revision + 1
WHERE id = 1 AND writer_epoch = $epoch
RETURNING catalog_revision;
```

- Zero rows: the pod has been fenced out. Roll back and end the session.
- Otherwise the returned value is this transaction's `revision`. Every catalog
  row the transaction inserts or updates stores it in its `revision` column.

The row lock taken by this statement serializes all leader transactions. As a
result, revisions increase in commit order and there is no race between leader
transactions. Transactions must stay short (§9.1).

"Leader write transaction" includes every write Jetstream makes during a
session: object rows, hot batches, direct block commits, folds, seals,
compaction publishes, every GC step, and every metadata write (MetaStore writes
from backfill, syncstate, retry runners, the orchestrator, and so on). A reader
pod never writes to PostgreSQL.

### 6.5 Session

`runSession` builds a full ingest runtime (orchestrator, writer, maintainer,
compaction scheduler, GC) from the state in PostgreSQL and runs it until an error
or cancellation. There is no partial restart inside a session. Any of these ends
the session and tears everything down:

- a fence failure;
- a transaction that fails, or whose commit result is unknown (connection lost
  during `COMMIT`);
- an S3 failure that retries cannot fix;
- lease loss.

The next session, on this pod or another, rebuilds from PostgreSQL. Rebuild is
always correct because nothing is visible or durable until it is committed.

These errors are corruption: a referenced object is missing or fails its hash
check, or a catalog invariant is broken (§9.3). Corruption ends the session and
exits the process with a non-zero status, the same as local mode's crash-loud
rule. Another pod will take over. If the corruption is real, every leader will
exit the same way and an operator must step in. The metric
`jetstream_storage_corruption_total` counts these events.

In code (S2.6), `catalog.CorruptionError` carries a `source` label and
implements `SessionFatal()`. `leader.DefaultFatal` treats any error that
reports `SessionFatal()` as fatal, even when it also wraps
`leader.ErrRestartSession`. A failed reference check (§7.4) is corruption: the
leader only references objects it made available. The one exception is §7.3
step 6 finding its `uploading` row gone or already claimed. That is GC
reclaiming an upload that stalled past the orphan age (the accepted leak), so it
ends the session without exiting, and the next session uploads again.

## 7. Objects

### 7.1 Keys

```
<prefix>/<archive_id>/objects/<uuid>
```

`archive_id` is the UUID in `archive.archive_id`. `<uuid>` is a fresh random
UUIDv4 for every upload attempt. A key is never written twice and never reused.
The object store therefore never sees overwrites. That is why no conditional
PUT or versioning is needed.

### 7.2 Object kinds

| Kind | Bytes |
|---|---|
| block | one compressed block frame exactly as in a segment file, without the 8-byte length prefix |
| footer | a sealed segment's footer: bytes `[footer_offset, EOF)` of the segment file |

A pointer hot batch's object is a block object. It may cover fewer than 4,096
events.

### 7.3 Upload protocol

`protocol.Uploader` implements this with the leader's `*catalog.Session`,
because the `uploading` row is a fenced write. `Put(ctx, session, data)` runs
every step and returns the winning `object_id`. The batch form `Upload(ctx,
session, objs)` stops after step 5 and returns pending references: the
referencing transaction runs step 6 (see the end of this section). Steps:

1. `sha := sha256(data)`.
2. If a row exists with `sha256 = sha AND state = 'available' AND
   (unreferenced_at IS NULL OR unreferenced_at > now() - $gc_delay / 2)`, return
   its `object_id` (dedup). No upload. The age condition means GC cannot claim
   the object before the caller's referencing transaction runs (that would need
   more than `gc_delay / 2` between lookup and commit), so a failed reference
   check on a deduped object is a real bug.
3. Insert `objects(key = new uuid, sha256, byte_length, state = 'uploading')`.
   This is a leader write transaction (fenced). One transaction may insert rows
   for several pending uploads at once. Concurrent uploads also share it:
   `Session.BeginUploads` queues a call that arrives while another's step 3
   is in flight, and the next transaction serves the whole queue (S2.23,
   §22.2). A committed `uploading` row makes an
   orphaned upload visible to GC. No PUT happens without one, so a fenced-out
   leader cannot start new uploads.
4. PUT the bytes.
5. GET the object and check its length and SHA-256. On mismatch, retry from
   step 3 with a new key, at most 3 rounds, then end the session. The old row
   is left for GC. A mismatch counts as
   `jetstream_s3_verify_failures_total{path="upload"}`, not as corruption,
   because nothing durable is wrong.
6. `UPDATE objects SET state = 'available' WHERE object_id = $id AND state =
   'uploading'` (fenced). If this hits the partial unique index on `sha256`
   because another upload of the same bytes won, use the winning row's
   `object_id` and leave this row for GC.

Transient S3 errors in steps 4 and 5 are retried with backoff, up to
`JETSTREAM_S3_RETRY_TIMEOUT` (default 30s). After that the upload fails and the
caller decides what happens. Every caller in this document ends the session.

Dedup is an optimization. No correctness property depends on two encodings
matching.

Accepted leak: if a pod pauses for longer than `JETSTREAM_GC_ORPHAN_AGE` between
step 3 and its PUT, GC may delete the row first, and the late PUT then creates an
object no row names. It is never read and never reclaimed. To make this rarer,
skip the PUT if more than `orphan_age / 2` has passed on the pod's monotonic
clock since step 3 committed. A skipped PUT fails the upload, which ends the
session like any other upload failure.

Steps 3 and 6 are extra transactions. The upload pipeline may combine step 6
with the transaction that first references the object (hot batch, fold, direct
commit, seal, compaction publish). In that transaction, set `state =
'available'` and then reference the object. This saves one transaction per
object.

### 7.4 Referencing an object

Any transaction that adds a reference to an object (a `hot_batches.object_id`,
`active_segment_blocks.object_id`, `generation_blocks.object_id`, or
`segment_generations.footer_object_id`) must, in the same transaction, run:

```sql
UPDATE objects SET unreferenced_at = NULL
WHERE object_id = $id AND state = 'available'
RETURNING object_id;
```

Zero rows means the object is being deleted or never became available. Treat
that as an internal error and end the session. This check, together with GC
claiming deletes in fenced transactions (§13), means GC can never delete an
object that is still referenced.

### 7.5 Reading an object

`ObjectStore.Get(ctx, objectID)` looks up key, length, and SHA-256 in the mirror,
GETs the object, and verifies length and SHA-256 before returning bytes. Range
reads (`GetRange`) are used only by `getSegment` for HTTP range requests (§11.8)
and verify only the length. The zstd frame checksum inside each block still
protects the payload.

The object Store (`objstore.Store`, implemented by `protocol.Reader`) is
read-only. It judges "still referenced" as "the object row is still
`available`". If the row is no longer available it returns `objstore.ErrGone`,
and the caller re-resolves its reference and applies step 2. Before declaring
corruption it re-reads the row once more, so a concurrent GC claim is not
misreported. On reads only, S3 404 and 403 `AccessDenied` both mean "maybe
missing" (real AWS returns 403 for a missing key without ListBucket). A 403 on
a write is always an error. A read 403 caused by a broken bucket policy
therefore looks like a missing object. To catch that case before it is
reported as corruption, every pod runs a canary at start (§15.2 step 2):
`objstore.Probe` PUTs, GETs, compares, and DELETEs
`<prefix>/<archive_id>/probe/<uuid>`, and any failure refuses startup.
`storage init` (§15.1) reuses the same probe.

Missing object or hash mismatch:

1. Refresh the mirror (§11.1) and look the reference up again.
2. If the reference is gone (compaction or GC replaced it), retry with the new
   reference.
3. If it is still referenced, this is corruption. Reader pods fail the request
   and increment `jetstream_storage_corruption_total{source="read"}`. Leader
   tasks end the session and exit (§6.5).

## 8. PostgreSQL schema

Migrations live in `internal/pgstore/migrations/NNNN_name.sql`. They are applied
by `jetstream storage init` (§15.1) and checked at startup. A pod refuses to
start if the schema version is not the one it expects. There is no automatic
migration on serve.

```sql
CREATE TABLE archive (
    id               smallint PRIMARY KEY CHECK (id = 1),
    archive_id       uuid NOT NULL,
    format_version   integer NOT NULL,         -- storage layout version, starts at 1
    schema_version   integer NOT NULL,
    writer_epoch     bigint NOT NULL DEFAULT 0,
    holder_id        uuid,
    lease_expires_at timestamptz,
    catalog_revision bigint NOT NULL DEFAULT 0,
    created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE metadata_kv (
    key   bytea PRIMARY KEY,
    value bytea NOT NULL
) WITH (fillfactor = 80);

CREATE TABLE objects (
    object_id       bigserial PRIMARY KEY,
    key             uuid NOT NULL UNIQUE,
    sha256          bytea NOT NULL CHECK (length(sha256) = 32),
    byte_length     bigint NOT NULL CHECK (byte_length > 0),
    state           text NOT NULL CHECK (state IN ('uploading', 'available', 'deleting')),
    created_at      timestamptz NOT NULL DEFAULT now(),
    unreferenced_at timestamptz
);
CREATE UNIQUE INDEX objects_sha256_available ON objects (sha256) WHERE state = 'available';
CREATE INDEX objects_gc ON objects (state, unreferenced_at);

CREATE TABLE segments (
    namespace             text NOT NULL CHECK (namespace IN ('main', 'bootstrap_live')),
    segment_index         bigint NOT NULL,
    state                 text NOT NULL CHECK (state IN ('active', 'sealed')),
    current_generation_id bigint,                 -- NULL while active
    revision              bigint NOT NULL,
    PRIMARY KEY (namespace, segment_index),
    CHECK ((state = 'active') = (current_generation_id IS NULL))
);
CREATE UNIQUE INDEX segments_one_active ON segments (namespace) WHERE state = 'active';
CREATE INDEX segments_revision ON segments (revision);

CREATE TABLE segment_generations (
    generation_id    bigserial PRIMARY KEY,
    namespace        text NOT NULL,
    segment_index    bigint NOT NULL,
    header           bytea NOT NULL CHECK (length(header) = 256),
    footer_object_id bigint NOT NULL REFERENCES objects (object_id),
    created_at       timestamptz NOT NULL DEFAULT now(),
    revision         bigint NOT NULL,
    FOREIGN KEY (namespace, segment_index) REFERENCES segments (namespace, segment_index)
);
CREATE INDEX segment_generations_footer ON segment_generations (footer_object_id);

CREATE TABLE generation_blocks (
    generation_id     bigint NOT NULL REFERENCES segment_generations (generation_id) ON DELETE CASCADE,
    ordinal           integer NOT NULL,
    object_id         bigint NOT NULL REFERENCES objects (object_id),
    compressed_length bigint NOT NULL,
    PRIMARY KEY (generation_id, ordinal)
);
CREATE INDEX generation_blocks_object ON generation_blocks (object_id);

CREATE TABLE active_segment_blocks (
    namespace           text NOT NULL,
    segment_index       bigint NOT NULL,
    ordinal             integer NOT NULL,
    object_id           bigint NOT NULL REFERENCES objects (object_id),
    event_count         integer NOT NULL CHECK (event_count > 0),
    min_seq             bigint NOT NULL,
    max_seq             bigint NOT NULL,
    min_witnessed_us    bigint NOT NULL,
    max_witnessed_us    bigint NOT NULL,
    compressed_length   bigint NOT NULL,
    uncompressed_length bigint NOT NULL,
    revision            bigint NOT NULL,
    PRIMARY KEY (namespace, segment_index, ordinal),
    FOREIGN KEY (namespace, segment_index) REFERENCES segments (namespace, segment_index)
);
CREATE INDEX active_segment_blocks_object ON active_segment_blocks (object_id);
CREATE INDEX active_segment_blocks_revision ON active_segment_blocks (revision);

CREATE TABLE hot_batches (
    first_seq        bigint PRIMARY KEY,
    last_seq         bigint NOT NULL,
    event_count      integer NOT NULL CHECK (event_count > 0),
    min_witnessed_us bigint NOT NULL,
    max_witnessed_us bigint NOT NULL,
    epoch            bigint NOT NULL,
    revision         bigint NOT NULL,
    committed_at     timestamptz NOT NULL DEFAULT now(),
    frame            bytea,
    object_id        bigint REFERENCES objects (object_id),
    CHECK (last_seq - first_seq + 1 = event_count),
    CHECK ((frame IS NULL) <> (object_id IS NULL))
) WITH (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 1000,
        autovacuum_vacuum_cost_delay = 0);
CREATE INDEX hot_batches_object ON hot_batches (object_id) WHERE object_id IS NOT NULL;
```

Notes:

- `hot_batches` exists only for the `main` namespace. Bootstrap and merge never
  use it (§10.6).
- `hot_batches.frame` is a compressed block frame (§7.2), the same as a block
  object. One decoder serves both.
- `hot_batches` is small (seconds to tens of seconds of events) and churns fast.
  The aggressive autovacuum settings stop dead rows from building up.
- `metadata_kv` uses fillfactor 80 so repo-row updates can be HOT updates.
- `generation_blocks.compressed_length` is stored so a pod can compute segment
  offsets without fetching footers.
- `archive.format_version` must equal the value compiled into the binary.
  Otherwise refuse to start.

## 9. Transaction rules

### 9.1 General

- Leader write transactions use `READ COMMITTED` isolation. Correctness comes
  from the fence row lock, not from isolation level.
- The fence is always the first statement (§6.4).
- No S3 calls or other network I/O happen inside a transaction. Upload first,
  then commit.
- A transaction that returns an error, or whose `COMMIT` result is unknown, ends
  the session. Never retry a leader write transaction inside the same session.
  The next session rebuilds from what actually committed.
- Reader transactions (the follower and on-demand lookups) use `REPEATABLE READ
  READ ONLY`, so one tick sees one consistent snapshot.
- A leader session's metadata reads are autocommit queries outside any
  transaction. A failed read (other than not-found, or the session's own
  cancellation) also ends the session, so a dropped connection restarts the
  session instead of reaching the election loop as a fatal error.

### 9.2 Visibility rule

An event, block, segment, or metadata value is visible and durable if and only
if the transaction that wrote it has committed. There is no other visibility
path.

### 9.3 Catalog invariants

These must hold after every commit. The fakes test layer (§20) asserts them after
every transaction, and the leader checks the ones that are cheap to check at
session start.

1. In `main`, the committed seqs `[1, seq/next)` are covered exactly once, in
   order, with no gaps, by:
   sealed generations, then active blocks, then hot batches.
   In `bootstrap_live` the same holds against `live_segments/seq/next`, with no
   hot batches.
2. Segment indexes in a namespace are contiguous from 0. Exactly one segment is
   `active` per namespace once the namespace exists, and it has the highest
   index.
3. Active block ordinals in a segment are contiguous from 0.
4. Every referenced object has `state = 'available'`.
5. Hot batches cover a contiguous seq range. Its start equals
   `(max seq in active blocks and sealed segments of main) + 1`, and its end
   equals `seq/next - 1`.
6. A hot batch never crosses an open-block boundary (§10.3).
7. `metadata_kv['relay/cursor']` never names an upstream seq whose events are not
   all committed (§10.4).

`catalog.CheckInvariants` (S2.6) checks invariant 6 in a weaker form: no hot
batch holds more events than a block. The open block's boundary is not stored
in the catalog, so a snapshot cannot show whether a batch crosses it. Fold's
exact-coverage check enforces the boundary itself: a fold whose deleted batches
do not tile the block exactly is rejected as corruption. An absent seq key
reads as 1. A present seq key that is not 8 bytes is corruption. In `-short`
mode or at session start on a large archive, the cheap subset skips decoding
generation headers.

Invariant 7 is not checkable from the catalog alone: the segment format does
not keep an event's upstream seq, and the live cursor encoding belongs to the
live package. `CheckInvariants` calls a caller-supplied
`InvariantOptions.RelayCursor(snapshot, value)`. Production passes nil (at
session start a malformed cursor still fails loudly when the consumer loads it
at `Run`). Tests install `storagefake.RelayWatch` (S2.12): the test's model says
which rows each upstream seq produces, and the watch finds them by content in
the committed hot batches, checking after every commit. It covers main's hot
batches only, which is where a hot-mode live consumer writes.

## 10. Write path

### 10.1 Writer modes

The existing `internal/ingest` Writer keeps its API (Append, AppendBatch, Flush,
ForceRotate, SealActiveAndClose, DrainDurability). What changes is what happens
behind a block flush. In disaggregated mode it has two modes:

| Mode | Used for | Visibility unit |
|---|---|---|
| hot | `main` namespace in steady state | hot batch |
| direct | `main` during bootstrap backfill, `bootstrap_live` during bootstrap, `main` during merge | block |

The orchestrator chooses the mode when it opens a writer: `ingest.Config.Hot`
selects hot mode and `ingest.Config.Direct` selects direct mode. Every Writer
method dispatches to the mode, so producers keep their `*Writer`. An append's class (§10.5) travels in its context
(`ingest.WithClass`); untagged appends are live. The seq write-ahead
lease (`ReserveClientVisibleSeqs`, `seq/max_reserved`, `seq/gap/*`) is disabled
in disaggregated mode, because a seq is never visible before it commits.

### 10.2 Seq allocation

- On session start, the writer reads its seq key (`seq/next` or
  `live_segments/seq/next`) from `metadata_kv`. That is the next seq to assign.
- Seqs are assigned in memory at append time, as today.
- A committing transaction checks that the stored seq key equals the first seq
  it is committing, then sets the key to one past the last seq. On mismatch,
  end the session with a corruption error.
- Events that were assigned seqs but never committed are dropped when a session
  ends. The next session starts at the committed `seq/next` and assigns those
  seqs to whatever events arrive next. That is safe because no client could have
  seen the dropped events. Relay replay and retries re-deliver the underlying
  data.

As a result, seqs are gap-free in disaggregated mode. The cold reader's
registered-vacancy support stays for local mode and is simply never used here.

### 10.3 Hot mode: batching

The writer keeps one open block and cuts it into batches.

- A **block** holds up to `MaxEventsPerBlock` events (default 4096). It closes
  when it reaches 4096 events, or when its first event is 30s old
  (`JETSTREAM_BLOCK_MAX_AGE`).
- A **batch** is a run of consecutive seqs inside one block. A batch is cut when
  any of these happens first:
  - it has `min(256, remaining capacity in the block)` events;
  - its raw event bytes reach 256KiB;
  - its first event is 15ms old (`JETSTREAM_HOT_BATCH_MAX_AGE`);
  - the block closes;
  - the append class changes (§10.5).

  Bulk batches and live overflow batches replace the first two limits with
  their own (§10.5 rules 4 and 5); live overflow also replaces the age.

A batch never crosses a block boundary. Block boundaries therefore fall exactly
on batch boundaries, and a fold consumes whole batches.

When a batch is cut, it is **frozen**:

1. Encode its events with the existing block encoder into one frame.
2. Sample `DurableBatchPrepareValue` (for example the live relay cursor
   watermark). The sample is tied to this batch.
3. Queue it for commit.

Steps 2 and 3 run under the append lock. Step 1, and a pointer batch's upload,
run off the lock in a goroutine per batch. The committer waits for each one in
turn.

`Append` and `AppendBatch` return once seqs are assigned, as local sync mode's
do. They do not wait for the commit; admission control (§10.5) supplies the
backpressure. `Flush` and `DrainDurability` wait for every batch frozen before
them. `Close` commits everything appended but leaves the open block for the
next session to rebuild (§10.9). `ForceRotate` closes the open block and waits
for its commit, then asks the maintainer to seal.

Frozen batches commit strictly in seq order, one transaction at a time, from one
committer goroutine. Inline batches need no upload, so they commit as soon as
the previous batch has committed. Pointer batches upload concurrently and commit
in order when their turn comes.

### 10.4 Hot mode: batch transaction

```
BEGIN
  fence                                   -- §6.4, returns rev
  SELECT value FROM metadata_kv WHERE key = 'seq/next' FOR UPDATE
      -- must equal batch.first_seq
  [pointer only] reference check on batch.object_id   -- §7.4
  INSERT INTO hot_batches (first_seq, last_seq, event_count, min_witnessed_us,
      max_witnessed_us, epoch, revision, frame | object_id)
  apply metadata batch:
      seq/next = last_seq + 1
      whatever DurableBatchHook staged (relay/cursor, repo/<did>, sync/<did>, ...)
  SELECT pg_notify('jetstream_catalog', rev::text)
COMMIT
```

After the commit succeeds:

- run the hook's `afterCommit` and `afterDone`;
- release producer acks for events in the batch (whatever `AppendBatch` or
  `Flush` waits on today);
- ring the local follower's in-process doorbell.

**Group commit.** One transaction may commit several consecutive batches:
the committer takes every batch at the head of the queue whose frame (and
upload) is ready, up to `MaxCommitBatches` (default 32). The transaction runs
the fence and the `seq/next` check once, inserts each row with the shared
revision, applies each hook's ops in batch order, and moves `seq/next` once, past
the last batch. Every leader transaction serializes on the fence row, so on a
PostgreSQL whose commits wait for a WAL flush, one transaction per batch caps
the leader near one batch per flush (§22.2). A group only takes batches that
are already ready. It never waits to fill, so an idle writer still commits each
batch as soon as it is ready. After the commit, each batch's `afterCommit` runs
in order, then each `afterDone`.

On failure: run `afterDone(err)` for every batch of the group and end the
session. A hook that returns an
error also ends the session, because it runs in the committer with no caller to
return the error to. `DrainDurability` and `Close` commit hook output with no
events as a metadata transaction, and skip it when the hook stages nothing.

**DurableBatchHook.** It keeps its current two-phase contract. The only change
is the batch type, which becomes `metastore.Batch` instead of `*pebble.Batch`
(§12). The hook runs once per committed hot batch, in the committer goroutine,
even when a group commit puts several batches in one transaction.
`nextSeq` is `last_seq + 1` of that batch. `prepareValue` is the value sampled at
freeze time. As today, the hook must not call Writer methods or do unbounded
I/O.

**Relay cursor.** One upstream firehose commit can produce several rows that end
up in different batches. `relay/cursor` may only advance to an upstream seq whose
rows all have seqs `< nextSeq`. The live consumer's existing safe-cursor logic
(the prepare-time watermark carried in `prepareValue`) already enforces this per
block. It must be driven per batch now. A test must cover a single upstream
commit split across two batches, with the leader killed between the two
commits.

The hot writer samples `DurableBatchPrepareValue` when each batch freezes,
under the writer lock (S2.8), and atmos advances its cursor only after the
consumer's `yield` for an event returns. So a batch frozen partway through an
upstream commit carries a cursor from before that commit, however late the
batch commits, and the consumer only has to pass `live.Config.Hot` through
(S2.12). Delivery stays at least once, as in local mode: when the leader dies
between two batches of one upstream commit, the rows already committed are
archived again when the next session re-requests that commit. Nothing is lost,
no seq is reused, and the duplicates are exactly that committed prefix.
`live/hot_cursor_test.go` pins this for both a failed and an unknown-result
commit.

The verifier state that drops replays must follow the same rule (S2.18). Its
chain and hosting state, and the applied `#identity`/`#account` seqs, must
become durable in exactly the batch that holds the event's last row. Earlier
loses the row, because its replay is dropped. Later archives the whole event
twice, because its replay is accepted. So `prepareValue` also carries a
snapshot of the promoted sync state, taken when the batch freezes, and the
hook stages that snapshot rather than whatever has been promoted by the time
the batch commits. The consumer promotes an event's state in the writer's
`OnAppend` hook for its last row, under the writer lock. The layer 3 oracle
found both halves. With them fixed, the committed prefix is the only
duplicate it sees (`specs/oracle/2026-09-25-disagg-syncstate-batch-boundary.md`).

**Metadata batch size.** At 3,000 events/s, a batch carries up to about 256
`repo/<did>` upserts. They are applied as one multi-row upsert (§12.3), not one
statement per key.

### 10.5 Admission control

There are two classes of producers:

- **live**: the firehose consumer;
- **bulk**: failed-repo retries, sync 1.1 resync replacements, large-PDS
  recovery, and any other producer that appends more than one repo's worth of
  events.

Rules:

1. **Append lock priority.** The Writer's append lock prefers live. A bulk
   appender holds the lock for at most one chunk: `min(4096 minus the open bulk
   batch's events, remaining block capacity)` events. So a chunk never spans a
   batch or a block, and a live append lands only on a bulk batch boundary.
   The bulk appender checks for waiting live appenders between chunks.
   Implement this as a mutex plus a "live waiting" counter. Bulk code yields when
   the counter is non-zero. A live append's admission wait
   (`jetstream_admission_wait_seconds`) counts only rule 8 and 9 waits, not the
   wait for the mutex.
2. **Class boundary.** Changing class cuts the current batch. Every batch holds
   events of one class only.
3. **Live batches are inline**, paid for from a token bucket over encoded frame
   bytes. The rate is `JETSTREAM_HOT_INLINE_BYTES_PER_SEC` (default 4MiB/s) and
   the burst is 1s worth. At freeze, if the bucket has at least the frame's size
   in tokens, take them and commit inline. The frame is encoded off the lock,
   after the freeze, so the bucket takes the batch's raw bytes at freeze and
   settles the difference once the frame exists. A frame larger than its raw
   bytes can leave the bucket below zero; the rate still holds over frame bytes.
   A negative rate disables the bucket (every live batch is inline). The bucket
   only applies when the writer has an uploader.
4. **Live overflow.** If the bucket is short at an ordinary cut (events,
   bytes, age), the batch is not frozen. It becomes an overflow batch and keeps
   taking live events until 1024 events, 1MiB raw, or 1s from its first event.
   Then it freezes as a pointer batch. Any other cut of an overflow batch also
   freezes it as a pointer batch. A cut that cannot wait (class change, block
   close, Flush, DrainDurability, Close) of a normal live batch the bucket cannot
   pay for also makes a pointer batch, even a small one. So does an ordinary
   cut of a batch already at the overflow limits, which a configuration may
   set below the ordinary ones. Overflow ends with the
   overflow batch: the next live batch tries the bucket afresh. The overflow
   limits are code constants (§18).
5. **Bulk batches are always pointer batches.** A bulk batch holds at most 4096
   events and never passes the block boundary. It stays open across
   `AppendBatch` calls, so small bulk appends coalesce, and is cut at 4096
   events, block close, a class change, 15ms age, or a Flush/drain/Close. It is
   not cut at 256 events or 256KiB.
6. **Bulk permit.** Before appending a chunk, a bulk appender takes permits for
   its raw bytes from a bulk pending-bytes pool
   (`JETSTREAM_HOT_BULK_PENDING_BYTES`, default 64MiB). The batch holding the
   chunk collects them, and they are released when it commits. A chunk larger
   than the whole pool is admitted when no permits are held. A chunk that fails
   before any of its events lands returns its permits at once.
7. **Upload concurrency.** At most `JETSTREAM_S3_UPLOAD_CONCURRENCY` (default 8)
   uploads run at once, across all writer and maintainer work. The process-wide
   bound is the blob store's PUT limit. The writer separately bounds its
   in-flight uploads, at the same default. A bulk chunk also waits while as
   many bulk batches are frozen and uncommitted as the writer may upload at
   once. Uploads beyond that only queue, and every live batch frozen behind
   them waits for them to commit (§22.2).
8. **Total cap.** If frozen-but-uncommitted raw bytes exceed
   `JETSTREAM_HOT_PENDING_BYTES` (default 256MiB), every append blocks, live
   included. This is the last-resort backstop. Live appenders blocking means the
   firehose consumer stops reading and the relay buffers.
9. **Unfolded cap.** If the events in `hot_batches` (committed but not folded)
   exceed `JETSTREAM_HOT_MAX_UNFOLDED_EVENTS` (default 65,536, which is 16
   blocks), every append blocks until folds catch up. This bounds PostgreSQL
   growth when S3 is slow or down. The writer counts committed-but-unfolded
   events as its commit watermark minus its fold watermark. At open, the fold
   watermark is the first `hot_batches` row. The maintainer moves it forward by
   calling the closed block's `Folded` after the fold commits (§10.7). The cap
   must be at least one block, or appends would wait for every block's age cut.

Every admission wait also ends with the append's context, a writer failure, or
Close. Gauges `jetstream_hot_pending_bytes{class}`,
`jetstream_hot_unfolded_events`, and `jetstream_hot_inline_tokens` show the
caps' state.

Producers tag their context with the class (`ingest.WithClass`). Untagged
appends are live. The live firehose consumer is live. The failed-repo retry
loop is bulk; it also carries sync 1.1 resync replacements. Backfill runs in
direct mode, so it has no class.

Commits stay strictly in seq order, so a live inline batch that follows a pointer
batch waits for the pointer's upload and read-back. It also waits for every
bulk batch frozen ahead of it to commit. On a PostgreSQL whose commits cost a
real WAL flush, that wait was about a second while the 64MiB bulk permit pool
alone set queue depth (§22.2). S2.23 therefore added three things. Rule 7
bounds the bulk batches frozen but not yet committed. Group commit (§10.4)
commits consecutive ready batches in one transaction. Concurrent object
registrations share a transaction too (§7.3 step 3). With them, live p99 stays
under 36ms beside 30k bulk events/s on the disk server (§22.2).

### 10.6 Direct mode

Direct mode is local mode's block flush, pointed at S3 and PostgreSQL:

1. When a block is full (4096 events), or on Flush/ForceRotate/Seal, freeze it:
   encode it and sample `DurableBatchPrepareValue`.
2. Upload it (§7.3). Uploads run concurrently (`JETSTREAM_S3_UPLOAD_CONCURRENCY`),
   so bootstrap can sustain about 100–200 blocks/s. Pop2 recovery measured 173
   blocks/s at about 740 events per block.
3. Commit blocks strictly in order, one transaction each:

```
BEGIN
  fence
  check seq key == block.min_seq (FOR UPDATE)
  reference check on block.object_id
  INSERT INTO active_segment_blocks (..., ordinal = next ordinal, revision = rev)
  apply metadata batch: seq key = block.max_seq + 1, plus DurableBatchHook output
COMMIT
```

4. After commit: `afterCommit`, `afterDone`, and producer acks, as today.
5. Seal when the rotation rule fires (§10.8).

Pods return 503 on every archive and subscribe endpoint until `phase =
steady_state`, the same as local mode's readiness gate. So direct-mode data is
never visible to clients until merge finishes.

The existing async-flush pipeline (`AsyncFlushWorkers`) maps onto step 2:
compress and upload off the writer mutex, commit in order.

As built (S3.1, `internal/ingest/direct.go`):

- `ingest.Config.Direct` selects the mode for `Config.Namespace`. The seq key
  is always the namespace's (`catalog.SeqKey`), and the local-only fields are
  refused.
- The writer does its own encode and upload, as the hot writer does, instead of
  reusing `AsyncFlushWorkers`. Frozen blocks encode and upload concurrently, up
  to `UploadConcurrency`. One committer goroutine commits them in seq order.
- Admission: an append waits while `MaxPendingBlocks` blocks (default twice the
  upload concurrency) are frozen but not committed. It waits before it appends
  anything, so an `AppendBatch` still gets contiguous seqs.
- The active segment is a `maintainer.Segment`, the same type the maintainer
  seals with in hot mode. The committer is its only caller, so seals serialize
  with block commits. The rotation rule runs after each block commit, and also
  before it. The earlier run covers an earlier session that committed the
  threshold-crossing block and ended before its seal.
- Flush and DrainDurability commit a short block and do not rotate unless the
  rule fires, as in local mode. ForceRotate and SealActiveAndClose seal the
  active segment. Close leaves it active: the next direct writer, or the hot
  writer after merge, continues it.
- Session start refuses to open `main` over hot batches (a corruption error):
  direct mode only precedes hot mode.
- Crash seams: `AfterDirectBlockCutBeforeUpload`,
  `AfterDirectBlockUploadBeforeCommit`, and `AfterDirectBlockCommitBeforeAck`.

### 10.7 Fold

The maintainer is one goroutine in the leader session. It runs fold and seal in
order, never concurrently.

The writer hands each closed open block to the maintainer, together with the
in-memory events of all its batches. The maintainer waits until the last batch
of the block has committed, then:

1. Encode the block from the in-memory events with the existing encoder.
2. Upload it (§7.3). If the block's bytes equal an existing object (for example
   a single bulk pointer batch that covered the whole block), dedup returns that
   object and nothing is uploaded.
3. Commit:

```
BEGIN
  fence
  reference check on object_id
  DELETE FROM hot_batches
    WHERE first_seq BETWEEN $min_seq AND $max_seq
    RETURNING first_seq, last_seq, event_count
      -- must cover exactly [min_seq, max_seq], contiguous; else corruption
  INSERT INTO active_segment_blocks (ordinal = next, revision = rev, ...)
COMMIT
```

4. Drop the block's events from leader memory, and call the block's `Folded`
   so the writer releases its events from the unfolded cap (§10.5 rule 9).
5. If the rotation rule now fires, seal (§10.8) before the next fold.

Deleting a pointer batch removes that object's hot reference. If the fold reused
it through dedup, it is still referenced by `active_segment_blocks`. Otherwise GC
eventually removes it.

A reader that was about to read deleted hot batches finds the active block in
the next mirror refresh. See §11.4 for how readers switch.

The maintainer (`internal/ingest/maintainer`) is the writer's `BlockSink`.
`BlockClosed` only queues the block, and the maintainer goroutine takes the
queue in order. The writer delivers a block only after its last batch
commits, so no separate wait is needed. The fold encodes with the same
encoder and block size as the writer's pointer batches, which is what makes
step 2's dedup hit. The maintainer keeps the active segment's index, block
list, and framed size in memory, loaded at session start. If a fold commits
at a segment or ordinal other than the one memory predicts, that is
corruption: the seal's block list is built from memory. Each folded frame goes
into the object cache, so a seal usually reads nothing from S3. Any failure
ends the session and stops the maintainer. The blocks still queued, and the
ones queued during `Close`, stay hot batches for the next session's rebuild
(§10.9). A `Sync` request waits until everything queued before it has folded.
The rebuild uses it.

### 10.8 Seal

The rotation rule is the existing one: after a fold, the active segment's
framed bytes (`Σ(8 + compressed_length)`, the virtual file size minus the
256-byte header, which is what local mode compares) reach `MaxSegmentBytes`
(256MiB). Seal also runs on `ForceRotate` and `SealActiveAndClose`, after
every block closed before them has folded. It does nothing when the active
segment has no blocks, as in local mode. With the same event stream and the
same block boundaries, both modes therefore seal byte-identical files.

1. Build the footer and header with the existing sealer code (`segment/seal.go`:
   block walk plus `buildFooter`), driven by a block source instead of a file.
   The source yields each active block's frame from the object cache or S3.
   Offsets are computed as if the blocks were laid out in a file: block `i`
   starts at `256 + Σ_{j<i}(8 + len_j)`.
2. Upload the footer object.
3. Commit:

```
BEGIN
  fence
  SELECT ordinal, object_id FROM active_segment_blocks
    WHERE namespace = $ns AND segment_index = $idx ORDER BY ordinal FOR UPDATE
    -- must equal the list the footer was built from
  reference check on footer_object_id
  INSERT INTO segment_generations (header, footer_object_id, revision = rev) RETURNING generation_id
  INSERT INTO generation_blocks SELECT (gen, ordinal, object_id, compressed_length) ...
  DELETE FROM active_segment_blocks WHERE namespace = $ns AND segment_index = $idx
  UPDATE segments SET state = 'sealed', current_generation_id = gen, revision = rev
    WHERE namespace = $ns AND segment_index = $idx AND state = 'active'
  INSERT INTO segments (namespace, segment_index = idx + 1, state = 'active', revision = rev)
COMMIT
```

The header's checksum covers header bytes `[12:256)` plus the footer
(`segment/header.go` `xxh3HeaderFooter`). It does not cover block bytes. Blocks
carry their own zstd content checksums. `docs/README.md` §3.1.2 says the checksum
covers the blocks. The code is authoritative, and the README should be fixed.

Seal fetches up to 256MiB of blocks, most of them usually still in the object
cache. A cache miss goes through the §7.5 read protocol, with at most
`ReadConcurrency` (8) block reads in flight ahead of the builder. A block
whose length disagrees with its catalog row is corruption. After the seal
commits, the footer goes into the object cache for readers. A seal that fails
between the footer upload and the commit leaves the catalog unchanged, apart
from the footer's uploading row, which GC reclaims. Hot batches keep committing during a seal. Folds wait. The unfolded cap
(§10.5) bounds how far behind they get.

### 10.9 Session start in hot mode

1. Acquire the lease and read the metadata the orchestrator needs.
2. Load the catalog snapshot and run the cheap `CheckInvariants` subset (§9.3)
   against it, with `relay/cursor` validation (invariant 7). A violation is
   corruption: the session ends and nothing is folded.
3. Load all `hot_batches` in seq order. Decode inline frames. Fetch and verify
   pointer objects. Every batch must decode to exactly its recorded seq range.
4. Group the batches greedily, in order, into groups of at most 4096 events,
   never splitting a batch. The rows came from earlier sessions whose block
   boundaries may differ, so do not assume old boundaries. A batch larger than
   a block is corruption.
5. Fold every group except the last: none of them can grow. Fold the last group
   too if it is full, or if its first batch's `committed_at` (the database
   clock, clamped to now) is at least the block max age old. Apply the rotation
   rule before and after every fold, so a segment an earlier session left at the
   threshold (its fold committed, its seal lost) is sealed before anything is
   folded onto it. Sealed output stays byte-identical to local mode.
6. The remaining group, if any, becomes the new open block
   (`ingest.HotConfig.Resume`). Its committed events are kept in memory and its
   age cut runs from the first batch's `committed_at`. New appends continue at
   `seq/next`, and the batch cap uses the block's remaining capacity. The writer
   refuses to open over hot batches that were not rebuilt, and checks that the
   resumed block matches the catalog's hot batches exactly. Resumed events are
   not in the in-memory read log, which starts at `seq/next`; readers get them
   from the catalog (§11). Rebuilt batches report class `live`, because the
   catalog does not record a batch's class.
7. Rebuild the tombstone set (§12.5), through the existing orchestrator path.
8. Start the live consumer at `relay/cursor`, through the existing orchestrator
   path.

`maintainer.Rebuild` runs steps 2 to 6 and returns the open block. Steps 7 and
8 are unchanged orchestrator code, pointed at the disaggregated catalog and the
fenced metastore by the runtime wiring (S2.16). The rebuild's cost includes the
invariant check, which is O(archive); S2.22 measures it.

### 10.10 Lifecycle phases

The orchestrator's phases (`bootstrap`, `merging`, `steady_state`) do not change.
Each maps onto catalog operations like this:

- **bootstrap**: two direct-mode writers. The backfill writer writes to `main`
  (`seq/next`). The bootstrap-live writer writes to `bootstrap_live`
  (`live_segments/seq/next`). Backfill checkpoints (repo completions, host
  cursors) go through `DurableBatchHook` in block commits, as today.
- **merging**: the live consumer is stopped. Merge reads `bootstrap_live`
  blocks from S3 in seq order, applies the existing rev filter, and appends
  survivors to `main` in direct mode. Then it runs the pending retry pass and
  merge-tail compaction (§12), as today. One final transaction deletes every
  `bootstrap_live` catalog row and `live_segments/*` metadata key, and writes
  `phase = steady_state` and `phase/entered_at`. The objects become unreferenced
  and GC removes them. A crash at any point before that transaction leaves merge
  resumable with the existing merge-cursor logic (`merge_cursor.go`), because
  every step before it commits on its own.
- **steady_state**: the `main` writer runs in hot mode. The active `main`
  segment that merge left behind simply continues. Hot batches start at
  `seq/next`.

As built (S3.2, S3.3; `internal/ingest/orchestrator/disagg.go`):

- On a new catalog (`phase` absent), the orchestrator creates segment 0 in any
  namespace that lacks it and writes `phase = bootstrap` in the last of those
  transactions (§15.1).
- Phase writes go through the leader session's fenced metadata store.
- `main` in direct mode seals at the steady-state segment size, and
  `bootstrap_live` at the bootstrap-live size.
- The runtime opens the maintainer only when steady state starts. Until then a
  direct writer owns `main`'s active segment.
- Merge reads `bootstrap_live` from the catalog rows and the object store. It
  loads no footers, because it needs only the blocks in order. A seal inserts
  the next active segment, so the drain skips a trailing empty active
  segment.
- Merge closes its `main` writer rather than sealing it, so the hot writer
  continues that segment.
- Merge-tail compaction is skipped until stage 4 (D5).
- The final transaction is `DeleteNamespace(bootstrap_live)`. It carries the
  deletes of `live_segments/seq/next` and `merge/next_source_idx` and the phase
  write.
- A `merging` phase with no `bootstrap_live` segments is a corruption error,
  because only that final transaction removes them.

## 11. Read path

### 11.1 Follower

Every pod runs one follower. It wakes on:

- a `NOTIFY jetstream_catalog` (a dedicated `LISTEN` connection; reconnect with
  backoff);
- a 250ms timer (`JETSTREAM_CATALOG_POLL_INTERVAL`);
- an in-process doorbell (leader pod only);
- a synchronous refresh request from a reader (§11.6).

Each tick runs one `REPEATABLE READ READ ONLY` transaction:

1. `SELECT catalog_revision FROM archive`. If it equals the mirror's revision,
   stop.
2. Load changes with `revision > $mirror_revision`:
   - `segments` rows. For a newly sealed or compacted segment, load its current
     generation and its `generation_blocks`.
   - `active_segment_blocks` rows.
   - For every namespace, the set of active block keys, to detect deletes. This
     is small: at most one segment's blocks.
3. Load `hot_batches WHERE first_seq >= $follower_next_seq ORDER BY first_seq`,
   including `frame`. Also load the descriptors of all hot batches (without
   frames) so the mirror knows every hot batch that still exists.
4. Load any `objects` rows referenced by new catalog rows (key, sha256, length).
5. Load `metadata_kv` keys that readers need (`phase`, the compaction deadline
   key, §12.7).
6. Commit.

Then, outside the transaction:

- Fetch and verify pointer-batch objects for new hot batches.
- Append new hot-batch events to the readable log in seq order, then advance its
  durable watermark to the last appended seq. Everything in PostgreSQL is
  durable, so the log has no pending tail.
- Swap in the new mirror atomically (one `atomic.Pointer` store).
- Publish newly sealed and compacted segments to the manifest. Fetch their
  footers first (§11.3).

Deletes are implied. A segment whose generation changed drops its old generation.
Active blocks missing from the active set are gone (sealed). Hot batches below
the lowest remaining hot batch are gone (folded).

If the lowest loaded hot batch starts above `$follower_next_seq`, or there are
no hot batches and the tip is above it, the missing seqs were folded (and maybe
sealed) between two ticks. That is normal. The follower reads them through
`RefsFrom($follower_next_seq)` (active blocks or sealed blocks, decoded through
the block cache) and appends them to the readable log before any later hot
batch. The readable log receives every seq exactly once, in order.

`$follower_next_seq` always sits on a batch boundary, because blocks start on
batch boundaries. At pod start it is set to the `main` tip plus one, so the
readable log starts empty. Older seqs are served by the cold reader.

### 11.2 Mirror contents

- Archive: `archive_id`, `catalog_revision`, and the time of the last successful
  refresh.
- Per namespace, per segment: state, current generation (header, footer object
  ID, block object IDs, compressed lengths), and active block descriptors.
- Hot batch descriptors: `first_seq`, `last_seq`, min/max witnessed, and inline
  frame or object ID. Inline frames of hot batches still in the mirror stay in
  memory. They are bounded by the unfolded cap.
- The object table entries for everything referenced.

**Block ref.** Cold reads, cursor resolution, and repo export all address data
through one type:

```go
type BlockRef struct {
    MinSeq, MaxSeq                 uint64
    MinWitnessedUS, MaxWitnessedUS int64
    Namespace  Namespace // position; stable across compaction
    Segment    uint64
    Block      int
    Generation uint64  // the segment generation the ref was built from
    Loc        Locator // closed sum type, see below
}

// Exactly one of:
type FileBlock struct{ Path string; Offset uint64; Length uint32 } // local mode
type ObjectBlock struct{ ObjectID uint64 } // sealed block, active block, or pointer hot batch
type InlineBlock struct{ Frame []byte }    // inline hot batch
```

A `Fetcher` turns a ref into its zstd frame. It returns `ErrStaleRef` when the
ref's generation is no longer current (a compaction published a new one), so
the caller takes a fresh snapshot and retries. Local mode uses the header
checksum as the generation. Decoded-block caches key on (namespace, segment,
block, generation).

The mirror exposes `RefsFrom(ns, seq) iter.Seq[BlockRef]`: every ref from the
one containing `seq` up to the tip, in order. It is a lazy iterator, so a
cold reader that stops early does not materialize the rest of the archive's
refs.

### 11.3 Manifest and footers

The manifest keeps every sealed segment's DID bloom, per-block DID blooms, and
collection index in memory, as today. In disaggregated mode:

- At pod start, fetch every current generation's footer from S3 (bounded
  concurrency, `JETSTREAM_S3_READ_CONCURRENCY`, default 32) before becoming
  ready. Pop1 has about 7,000 segments. Measure start time (§22).
- The manifest takes seals and compaction rewrites through one call,
  `ApplySegment(idx, gen, header, footer, createdAt, size)`, which parses and
  checksum-verifies the metadata from bytes. The follower calls it, not the
  writer. Local mode reads the bytes from the file (`ApplySegmentFile`).
- Footers stay in memory, as the manifest already requires. There is no separate
  footer cache.

### 11.4 Cold reads and the readable log

The subscribe `Tail.ReadFrom` logic does not change. The readable log serves
seqs it still holds. Below its floor, the cold reader serves. What changes is the
cold reader's source:

- sealed blocks: `ObjectStore.Get`, through the decoded block cache;
- active blocks: the same;
- hot batches: decode the inline frame or fetch the pointer object.

The decoded block cache (`internal/subscribe/blockcache.go`) takes an opaque
comparable key: `(namespace, segIdx, blockIdx, generation)` in local mode, and
object SHA-256 in disaggregated mode. Only sealed blocks are cached; active
blocks are decoded per read. Hits
therefore survive compaction for unchanged blocks and survive folds that dedup.
Inline hot frames are keyed by `first_seq` plus the frame's SHA-256.

When a cold read reaches the tip of its refs, it hands off to the readable log.
The readable log holds everything above its floor, so there is no gap. If the
refs the reader held are gone (fold, compaction, or the seal of the active
segment), the fetch fails with `ErrStaleRef` and the reader asks for a fresh
view with `RefsFrom(nextSeq)`. Eight consecutive stale views without progress
fail the read loudly.

The reader samples the floor before it takes a view, so the view covers every
seq below the floor. A cold read bounded by the floor therefore fails loudly
on any hole a durable gap record does not explain: a hole before the next ref,
or a view that ends below the floor. (This replaced the local reader's
rotation-seam retry, which the coherent snapshot made unnecessary.)

The follower owns the readable log (`ingest.NewFollowerLog`). It starts at the
mirror's tip + 1 when the pod first reaches `steady_state`, and the follower
appends every newly committed seq exactly once, in order: hot batches as they
appear, and seqs folded or sealed between two ticks through `RefsFrom` first.
Before `steady_state` there is no readable log, and cold reads answer
unavailable. The cold reader takes its floor from the follower (`LogFloor`)
instead of from a writer. Its block keys are the object SHA-256 for sealed
blocks and `first_seq` plus SHA-256 for inline frames; active blocks and
pointer hot batches are not cached. The log requires strictly contiguous
seqs, which S4 revisits when compaction can remove seqs.

### 11.5 Cursor resolution

Cursor rules do not change (`docs/README.md` §2, §5). v1 time cursors resolve
through witnessed ranges: sealed segments via the manifest, then active blocks
and hot batches via the mirror. v2 seq cursors use `RefsFrom`. The lookback
floor is computed as today. Inside a manifest-chosen segment, the block index
and bytes come from the catalog view. If the view does not hold that segment
yet, resolution falls back to the segment's first seq. That start is coarser
but loses nothing, because the subscriber drops rows witnessed before the
requested time.

### 11.6 Freshness

- If the mirror's last successful refresh is older than
  `JETSTREAM_MAX_VIEW_AGE` (default 30s), the pod reports not ready and returns
  503 on archive and subscribe endpoints. Existing websocket streams stay open
  and wait.
- A request that names a seq, segment name, block, or `beforeSeq` above what
  the mirror knows triggers one synchronous follower tick before answering. If
  the value is still unknown after the tick, answer as today (for example,
  cursor in the future).

### 11.7 Response lifetime

`getSegment` and `getBlock` responses are cut off after
`JETSTREAM_MAX_ARCHIVE_RESPONSE_DURATION` (default 1h). GC must not delete an
object that a response could still be reading. Startup therefore checks:

```
JETSTREAM_GC_DELAY > JETSTREAM_MAX_VIEW_AGE + JETSTREAM_MAX_ARCHIVE_RESPONSE_DURATION + 10m
```

and refuses to start if it does not hold (`xrpcapi.CheckGCDelay`). Default
`JETSTREAM_GC_DELAY` is 6h. The cutoff is both a context timeout and the
connection's write deadline, so a stalled client cannot hold a response open
past it.

### 11.8 Archive endpoints

- **planSnapshot**: unchanged. It runs over the manifest. The follower feeds
  the manifest every sealed `main` segment, and the request runs a
  synchronous tick first when needed (§11.6), so `sealedTipSeq` matches the
  mirror.
- **getSegment**: serves the virtual file: the 256-byte header from
  `segment_generations.header`, then for each block an 8-byte little-endian
  length followed by the block object, then the footer object.
  `Content-Length = footer_offset + footer length`. Range requests map byte
  ranges onto these parts and use `GetRange`. The ETag is the header checksum,
  as today. `Last-Modified` is the generation's `created_at`. HEAD returns the
  same headers without fetching objects. The whole response comes from one
  generation, pinned from the mirror at request start.
- **getBlock**: serves one block object from the pinned generation. The ETag is
  `checksum:blockIndex`, as today.
- **Cache-Control**: uses the compaction deadline from `metadata_kv` (§12.7).

### 11.9 Repo export and status

`repoexport` reads a DID's events through `BlockRef`s, including hot batches. So
the "pending events" hook (`internal/jetstreamd/pending.go`) returns nothing in
disaggregated mode: everything committed is already readable.

The status page must not scan all repo rows. It takes the existing manifest
fast path, which reads the maintained `backfill/counts` aggregate. The
per-prefix keyspace counts are left out in disaggregated mode:
`pg_class.reltuples` estimates a whole table, not a key prefix. The active
segment shown on `/status` includes the hot batches.

## 12. Compaction

Compaction policy, schedule, triggers, tombstone kinds, and the `compaction/seq`
watermark do not change (`docs/README.md` §3.3). Only the mechanics change: a
rewrite fetches only affected blocks.

### 12.1 Pass

1. Force-rotate the `main` writer, as today, so every tombstone below the pass
   watermark is in a sealed segment or a later block.
2. Snapshot the tombstone set and the watermark `W` (the highest tombstone seq in
   the pass).
3. For each sealed segment with `min_seq < W` whose segment DID bloom hits a
   tombstone DID, run the segment rewrite (§12.2). Run up to
   `JETSTREAM_COMPACTION_REWRITE_WORKERS` segments at once.
4. After every segment in the chunk has published, advance `compaction/seq` in a
   fenced transaction that checks the prior value:
   `UPDATE metadata_kv SET value = $W WHERE key = 'compaction/seq' AND value = $prior`.
   Zero rows means corruption.

### 12.2 Segment rewrite

Input: one segment's current generation (header, footer from the manifest, block
object IDs).

1. **Candidate blocks.** A block is a candidate if its per-block DID bloom hits a
   tombstoned DID, the block's `min_seq` is below that tombstone's seq, and, for
   a record tombstone, the block's collection bitmask contains the tombstone's
   collection.
2. **Decode and drop.** Fetch and decode each candidate. Apply the existing drop
   rule (drop `KindCreate` and `KindUpdate` rows superseded by a newer tombstone).
   Blocks that lose no rows are not changed. If no block changed, stop. The
   segment is not rewritten.
3. **Vanished DIDs.** For every DID that lost at least one row, decide whether it
   still has any row in the segment:
   - check the remaining rows of all decoded blocks;
   - for each block not yet decoded whose per-block DID bloom hits the DID,
     fetch it, decode it, and check.

   A DID vanishes only if no row remains. This is exact. Blooms only decide which
   blocks to look at.
4. **Re-encode changed blocks** with the existing encoder. A block with all rows
   dropped becomes an `event_count = 0` block, as today. Upload each (§7.3).
5. **New footer.**
   - Block index: for changed blocks, new `compressed_size`,
     `uncompressed_size`, and `event_count`. Keep `min_seq`, `max_seq`,
     `min_witnessed_at`, and `max_witnessed_at` (the envelope). Recompute every
     block's `offset` from the new sizes.
   - Segment DID bloom and per-block DID blooms: unchanged. They are now
     supersets, which is allowed: `segment/verify.go` only rejects false
     negatives.
   - Collection index: new count = old count minus rows dropped for that
     collection. Remove a real collection whose count reaches 0. Sentinel
     collections (`$account`, `$identity`, `$sync`) are never removed, because
     their rows are never dropped. Rebuild changed blocks' bitmasks exactly from
     their remaining rows. Remap unchanged blocks' bitmasks to the new collection
     IDs.
6. **New header.** `event_count` = old minus dropped rows. `unique_did_count` =
   old minus vanished DIDs. Keep `block_count`, the seq bounds, and the
   witnessed bounds. Recompute the offsets and the checksum.
7. Upload the footer object.
8. **Publish:**

```
BEGIN
  fence
  SELECT current_generation_id FROM segments WHERE namespace = 'main' AND segment_index = $idx FOR UPDATE
    -- must equal the source generation; else corruption (only the leader compacts)
  reference checks on every new object and every reused object
  INSERT INTO segment_generations (..., revision = rev) RETURNING generation_id
  INSERT INTO generation_blocks (new objects for changed blocks, old object IDs for unchanged)
  UPDATE segments SET current_generation_id = new, revision = rev
  DELETE FROM segment_generations WHERE generation_id = source   -- cascades to generation_blocks
COMMIT
```

Old objects that are no longer referenced become GC candidates. Readers holding
the old generation keep working until GC deletes them, which is at least
`JETSTREAM_GC_DELAY` later.

Every rewrite drops at least one row, so `event_count` strictly decreases. The
header bytes and checksum therefore always differ from the source generation's,
and so does the ETag. Equal-length rewrites with identical headers cannot
happen. That was the only reason for the old checksum content-root extension,
which is not part of this design.

### 12.3 Correctness check

The sparse rewrite must match the existing full rewrite. Required test: for
generated segments and tombstone sets, the sparse rewrite's output

- passes `segment.VerifySealedMetadata`, and
- decodes to exactly the same rows as `segment.Rewrite` on the same input.

Blooms may differ (supersets). Everything else in the footer and header must
match what `VerifySealedMetadata` checks exactly: `unique_did_count`, collection
table membership and counts, and per-block collection sets.

### 12.4 Merge-tail compaction

The same code runs during merge, before `phase = steady_state`.

### 12.5 Tombstone set on session start

The tombstone set is memory only. On session start, rebuild it by reading every
`main` event above `compaction/seq` (sealed blocks, active blocks, hot batches)
through `BlockRef`s. Measure the time this takes (§22).

### 12.6 Compaction working memory

A segment rewrite holds its decoded candidate blocks. With 8 workers and
256MiB-segment worst cases, budget `JETSTREAM_COMPACTION_MEMORY_BYTES` (default
2GiB). A worker waits for budget before decoding. Blocks are decoded one at a
time into a per-worker buffer when possible.

### 12.7 Cache-Control deadline

The compaction scheduler writes its published deadline, and the "pass running
since" value, to `metadata_kv` under `compaction/deadline`, in a fenced
transaction. Every pod reads that key through the follower and computes
Cache-Control exactly as today.

## 13. Garbage collection

A leader task runs every `JETSTREAM_GC_INTERVAL` (default 10m).

1. **Mark.** In a fenced transaction, set `unreferenced_at = now()` on every
   `available` object with `unreferenced_at IS NULL` that no row references:

```sql
UPDATE objects o SET unreferenced_at = now()
WHERE o.state = 'available' AND o.unreferenced_at IS NULL
  AND NOT EXISTS (SELECT 1 FROM hot_batches h WHERE h.object_id = o.object_id)
  AND NOT EXISTS (SELECT 1 FROM active_segment_blocks a WHERE a.object_id = o.object_id)
  AND NOT EXISTS (SELECT 1 FROM generation_blocks g WHERE g.object_id = o.object_id)
  AND NOT EXISTS (SELECT 1 FROM segment_generations s WHERE s.footer_object_id = o.object_id);
```

   Run it in pages of 10,000 rows by `object_id` so each transaction stays
   short.
2. **Claim.** In a fenced transaction, claim up to 1,000 objects:

```sql
UPDATE objects SET state = 'deleting'
WHERE object_id IN (
  SELECT object_id FROM objects
  WHERE (state = 'available' AND unreferenced_at < now() - $gc_delay)
     OR (state = 'uploading' AND created_at < now() - $orphan_age)
  LIMIT 1000)
RETURNING object_id, key;
```

   Then re-run the four `NOT EXISTS` checks on the claimed `available` rows in the
   same transaction. Any row that is referenced again goes back to `available`
   with `unreferenced_at = NULL`. The reference check in §7.4 already stops this
   from happening, so treat a hit as corruption.
3. **Delete.** Outside the transaction, `DeleteObject` each claimed key.
   "Not found" counts as success.
4. **Forget.** In a fenced transaction, `DELETE FROM objects WHERE object_id =
   ANY($ids) AND state = 'deleting'`.

A session that dies between steps 2 and 4 leaves `deleting` rows. The next GC
run also picks up `state = 'deleting'` rows and repeats steps 3 and 4 for them.

`$orphan_age` is `JETSTREAM_GC_ORPHAN_AGE`, default 1h. It must be larger than
`JETSTREAM_S3_RETRY_TIMEOUT` plus the longest upload. `uploading` rows are never
referenced, so no mark step is needed for them.

## 14. Metadata store

### 14.1 Interface

`internal/metastore` defines what the ingest code uses from Pebble today. Pebble
snapshots and indexed batches are not used, so they are not in the interface.

```go
type Store interface {
    Get(ctx context.Context, key []byte) ([]byte, error) // ErrNotFound when absent
    NewBatch() Batch
    NewIter(ctx context.Context, lower, upper []byte) (Iterator, error)
    // Set and Delete are single-op batches.
    Set(ctx context.Context, key, value []byte) error
    Delete(ctx context.Context, key []byte) error
}

type Batch interface {
    Set(key, value []byte)
    Delete(key []byte)
    DeleteRange(start, end []byte) // [start, end)
    Commit(ctx context.Context) error
    Len() int
}

type Iterator interface {
    Next() bool // ordered by key bytes ascending
    Key() []byte
    Value() []byte
    Err() error
    Close() error
}
```

Implementations:

- `metastore/pebble`: wraps the existing `internal/store`. Commit uses
  `store.SyncWrites`, as today. The identity cache's `NoSync` writes stay local
  to local mode.
- `metastore/pg`: backed by `metadata_kv`. Reads are autocommit; commits go
  through the leader session's fenced `CommitMeta`, and a nil commit path is a
  reader pod's read-only view. The SQL lives in `pgstore` (`MetaGet`,
  `MetaScan`, `MetaBatch`), so this package imports no driver.

All 56 `NewBatch`, 12 `NewIter`, and 5 `DeleteRange` call sites move to this
interface. `DurableBatchHook` takes `metastore.Batch`.

### 14.2 PostgreSQL implementation

- **Get**: `SELECT value FROM metadata_kv WHERE key = $1`.
- **Commit**: one fenced transaction on the leader. Ops apply in their original
  order. Consecutive `Set`s are coalesced into one statement (last write per key
  wins):
  `INSERT INTO metadata_kv (key, value) SELECT * FROM unnest($1::bytea[], $2::bytea[]) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`.
  Consecutive `Delete`s become `DELETE FROM metadata_kv WHERE key = ANY($1)`. A
  `DeleteRange` is `DELETE ... WHERE key >= $1 AND key < $2`, and it ends the
  current run. This keeps Pebble's ordered-batch semantics.
- When the batch is part of a hot batch, direct block, or other catalog
  transaction, it is applied inside that transaction instead of its own.
- **Iterator**: keyset paging, `WHERE key >= $lower AND key < $upper AND key >
  $last ORDER BY key LIMIT 10000`. Each page is its own read. An iterator is not
  a snapshot. The `bytea` comparison is bytewise, which matches Pebble's
  default comparer. The Stage 1 audit (S1.6) found six metadata `NewIter`
  call sites, and all tolerate a non-snapshot scan. The rest of the
  original 12 were tests or the simulator's own Pebble, which is not
  metadata.

  | Site | Why a non-snapshot scan is safe |
  |---|---|
  | `backfill/counts.go` `CountStatuses` | Display only. The counts seed that relied on it runs once per session before any `repo/` writer starts (S1.4). A missing counts row after that is an internal error, not a re-tally. |
  | `backfill/retry.go` `scanDue` | A row that changes mid-scan is seen in either state, or is picked up next pass. The failure or completion write re-reads the row under `countsMu` before applying. |
  | `backfill/status.go` `ListPDSHosts` | Display only. |
  | `backfill/diagnostics.go` `ListHostStatuses` | Display only. |
  | `ingest/seqlease.go` `loadSeqGaps` | Runs at writer open, before the single writer produces gaps. Local mode only (§10.1). |
  | `status/collect.go` `countKeysWithPrefix` | Display only. Costly on PostgreSQL; S2.14 replaces it. |
- **Reader pods** get a read-only Store. Writes return an error.

### 14.3 Load

In steady state at 3,000 events/s, the main write load on PostgreSQL is repo row
upserts, not event bytes: up to one `repo/<did>` upsert per event, coalesced per
batch. At about 67 batches/s (3,000/s at 15ms batches, about 45 events each),
that is about 67 multi-row upserts/s touching about 3,000 rows/s, plus the inline
frames (about 0.5MiB/s). Measure p99 commit latency under this load (§22).

The failed-repo retry scan reads all `repo/` rows every 4h
(`DefaultFailedRepoRetryInterval`). That is about 9GB per pass on pop1-sized
data. This is accepted for now and must be measured.

## 15. Startup, failover, restore

### 15.1 Initialize

`jetstream storage init` (new subcommand):

1. Connect to PostgreSQL and apply migrations to an empty database. Refuse if
   the `archive` table already exists.
2. Insert the `archive` row with a new random `archive_id`. Create segment 0,
   `state = 'active'`, in both `main` and `bootstrap_live`. Leave `metadata_kv`
   empty; the orchestrator starts in `bootstrap` when `phase` is absent, as
   today.
3. PUT, GET, and DELETE a probe object under
   `<prefix>/<archive_id>/probe/<uuid>` to check credentials and read-after-write.

If init ends after inserting the `archive` row, running it again refuses. So
the first leader session on a catalog with no `phase` creates any missing
segment 0 itself (§10.10).

### 15.2 Pod start

1. Load config. Check the configurable memory budgets (§17) and the GC-delay
   inequality (§11.7).
2. Connect to PostgreSQL. Check `schema_version` and `format_version`. Run
   the object-store canary (§7.5).
3. Start the follower and do the first full mirror load. Load footers. Check the
   memory budgets again, this time including the measured manifest size. If
   the catalog is not yet in `steady_state` there are no footers to measure;
   the pod logs a warning and skips the recheck.
4. Start the HTTP servers. The pod becomes ready once the mirror is fresh,
   `phase = steady_state`, and all footers are loaded.
5. Start the election loop.

The leader session runs whichever phase it finds, as local mode does
(§10.10). Pods stay unready until the phase is `steady_state`.

### 15.3 Failover

Nothing special happens. The old leader's lease expires, another pod acquires it
with a higher epoch, and the old leader's later writes fail the fence. The new
session rebuilds from PostgreSQL (§10.9). Uncommitted events are re-delivered by
the relay from `relay/cursor`, and by retry state from `repo/<did>`.

### 15.4 Restore

Only HA failover of PostgreSQL (for example RDS Multi-AZ) is supported without
operator action. Restoring PostgreSQL to an earlier point in time can re-issue
seqs that clients have already seen. So a restore always creates a new archive:

1. Stop every pod of the old deployment.
2. Restore PostgreSQL into a new database.
3. Run `jetstream storage new-identity`:
   - assign a new `archive_id`;
   - bump `writer_epoch`;
   - clear `holder_id`;
   - delete `hot_batches` rows whose objects are missing, and check that every
     other referenced object exists (GET plus hash check). Refuse on any missing
     sealed or active block;
   - copying objects to the new `archive_id` prefix is not needed: keys are
     stored per row, so the rows keep pointing at the old keys.
4. Deploy at a new public endpoint. Clients must treat it as a new instance,
   because the cursor namespace is different.

Objects under the old prefix stay until the operator removes the old bucket
prefix. The old deployment must never run against the restored database.

## 16. Failure handling

| Failure | Behavior |
|---|---|
| Leader killed | Lease expires in ≤3s; new leader rebuilds; uncommitted events re-delivered |
| Leader paused or partitioned, later wakes | First write fails the fence; session ends; S3 PUTs become GC garbage |
| Commit result unknown | Session ends; next session reads what actually committed |
| PostgreSQL unreachable | Leader: renew fails, session ends within one lease. Readers: mirror ages; not ready after 30s; open streams wait |
| PostgreSQL HA failover | As unreachable, then recovery |
| S3 unreachable | Uploads retry up to 30s, then the session ends. Live inline batches keep committing until the unfolded cap, then appends block. Readers: cold reads fail with 503; hot reads keep working |
| S3 returns wrong bytes | Read-back check fails; retry with a new key |
| Referenced object missing or corrupt | Reader: request fails, `jetstream_storage_corruption_total` increments. Leader: session ends, process exits |
| `NOTIFY` lost | 250ms polling covers it |
| Follower falls behind | Visible via `jetstream_catalog_lag_seconds`; after 30s the pod is not ready |
| Bulk flood | Bulk permits and live priority hold; live latency rises by at most about one PUT plus GET |
| Invalid upstream data | Unchanged: drop, count, continue (`docs/README.md` §4.4) |
| Catalog invariant broken | Corruption: leader exits; readers fail affected requests |
| Pod paused between object-row insert and PUT for longer than the orphan age | One object leaks in S3 (§7.3). Accepted |

## 17. Memory

Pop1 today: RSS 36.6GB, 7-day max 67GB, Go heap 21GB, container limit 128GiB,
`GOMEMLIMIT` unset. In disaggregated mode, memory is the only cache, so every
large consumer gets an explicit budget.

| Budget | Env var | Default |
|---|---|---|
| Readable log | `JETSTREAM_SUBSCRIBE_READ_LOG_RETENTION_BYTES` (existing) | 256MiB |
| Decoded block cache | `JETSTREAM_SUBSCRIBE_BLOCK_CACHE_BYTES` (existing) | 64MiB (raise in production) |
| Compressed object cache | `JETSTREAM_OBJECT_CACHE_BYTES` | 2GiB |
| Writer pending bytes | `JETSTREAM_HOT_PENDING_BYTES` | 256MiB |
| Compaction working set | `JETSTREAM_COMPACTION_MEMORY_BYTES` | 2GiB |
| Manifest and footers | not configurable; measured at start | pop1: a few GiB |

Rules:

- `GOMEMLIMIT` must be set in disaggregated mode. Refuse to start without it.
- At start, add up the configurable budgets plus the measured manifest size. If
  the total exceeds 75% of `GOMEMLIMIT`, refuse to start with a message listing
  each budget. The check runs twice: before connecting, without the manifest,
  and after the first load, with `Manifest.ResidentBytes()`. That value is an
  estimate from entry counts and slice lengths, not a heap measurement.
- The compressed object cache is an LRU keyed by object SHA-256. It holds raw
  object bytes: block frames, pointer-batch frames, and footers. The manifest
  keeps the decoded footer, but the raw footer object also enters the cache,
  because followers load it through the cache and `SealedMetadata` rereads it
  there. The LRU bounds both. At pop1 start footers fill most of the default
  budget until block reads evict them (§22.2).
- `Manifest.ResidentBytes()` misses the follower's object index and block lists.
  For backfill-shaped footers that is about 2.8GiB at pop1 (§22.2). S5.2 closes
  the gap.
- Export each budget's current use as a gauge (§19).

## 18. Configuration

Disaggregated mode is on when `JETSTREAM_STORAGE=disaggregated`. The default is
`local`. In disaggregated mode `JETSTREAM_DATA_DIR` must be unset. Startup
refuses if it is set at all (flag or env, whatever the value), so no code path
writes to local disk by accident. It also refuses
`JETSTREAM_COMPACTION_INTERVAL > 0` until S4.

S3 credentials are not Jetstream settings. They come from the AWS SDK default
chain (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, web
identity, instance role). There is no `JETSTREAM_S3_*` credential variable.

| Env var | Default | Meaning |
|---|---|---|
| `JETSTREAM_STORAGE` | `local` | `local` or `disaggregated` |
| `JETSTREAM_PG_URL` | — | PostgreSQL connection string (secret; never in `.env`) |
| `JETSTREAM_PG_MAX_CONNS` | 16 | pgx pool size |
| `JETSTREAM_S3_ENDPOINT` | — | endpoint URL; empty means AWS default |
| `JETSTREAM_S3_REGION` | — | region |
| `JETSTREAM_S3_BUCKET` | — | bucket |
| `JETSTREAM_S3_PREFIX` | `""` | key prefix |
| `JETSTREAM_S3_PATH_STYLE` | `false` | path-style addressing (SeaweedFS, MinIO) |
| `JETSTREAM_S3_UPLOAD_CONCURRENCY` | 8 | concurrent uploads |
| `JETSTREAM_S3_READ_CONCURRENCY` | 32 | concurrent GETs for footer load and prefetch |
| `JETSTREAM_S3_RETRY_TIMEOUT` | 30s | give up on one S3 operation after this |
| `JETSTREAM_LEADER_LEASE` | 3s | lease duration |
| `JETSTREAM_LEADER_RENEW_INTERVAL` | 1s | renew period |
| `JETSTREAM_LEADER_ACQUIRE_INTERVAL` | 500ms | acquire attempt period |
| `JETSTREAM_HOT_BATCH_MAX_AGE` | 15ms | batch age cut |
| `JETSTREAM_BLOCK_MAX_AGE` | 30s | open block age cut (hot mode) |
| `JETSTREAM_HOT_INLINE_BYTES_PER_SEC` | 4MiB | live inline token bucket rate |
| `JETSTREAM_HOT_BULK_PENDING_BYTES` | 64MiB | bulk permits |
| `JETSTREAM_HOT_PENDING_BYTES` | 256MiB | total frozen-uncommitted cap |
| `JETSTREAM_HOT_MAX_UNFOLDED_EVENTS` | 65536 | committed-but-unfolded cap |
| `JETSTREAM_CATALOG_POLL_INTERVAL` | 250ms | follower poll |
| `JETSTREAM_MAX_VIEW_AGE` | 30s | not ready when the mirror is older |
| `JETSTREAM_MAX_ARCHIVE_RESPONSE_DURATION` | 1h | archive response cutoff |
| `JETSTREAM_GC_INTERVAL` | 10m | GC period |
| `JETSTREAM_GC_DELAY` | 6h | unreferenced age before delete |
| `JETSTREAM_GC_ORPHAN_AGE` | 1h | `uploading` age before delete |
| `JETSTREAM_OBJECT_CACHE_BYTES` | 2GiB | compressed object cache |
| `JETSTREAM_COMPACTION_MEMORY_BYTES` | 2GiB | compaction working set |

The hot-mode constants of 256 events and 256KiB per batch, and 1024 events,
1MiB, and 1s for overflow pointer batches, are code constants, not config.

Existing variables keep their meaning. `JETSTREAM_TIMESTAMP_IMPORT_*` are
removed (§21).

## 19. Code organization

### 19.1 Interfaces

Every storage seam is an interface with a local implementation and a
disaggregated implementation. Core code (writer, sealer, compaction, cold
reader, manifest, planner, orchestrator) depends only on these interfaces.

| Interface | Package | Local impl | Disaggregated impl |
|---|---|---|---|
| `ObjectStore` | `internal/objstore` | not used | S3 (`aws-sdk-go-v2`) |
| `metastore.Store` | `internal/metastore` | Pebble | `metadata_kv` |
| `Catalog` | `internal/catalog` | segment files plus directory scan | PostgreSQL tables and mirror |
| `HotLog` | `internal/catalog` | readable log fed by the writer | readable log fed by the follower |
| `Locker` | `internal/leader` | `streaming.NoopLock` | PostgreSQL lease |

```go
// ObjectStore stores immutable byte objects addressed by catalog object_id.
type ObjectStore interface {
    Put(ctx context.Context, data []byte) (objectID uint64, err error) // §7.3
    Get(ctx context.Context, objectID uint64) ([]byte, error)          // verified
    GetRange(ctx context.Context, objectID uint64, off, n int64) ([]byte, error)
}

// Catalog is the writer- and reader-facing view of segments and blocks.
type Catalog interface {
    // Reader side.
    Snapshot() CatalogView // immutable; the mirror in disaggregated mode
    Refresh(ctx context.Context) error
    // Writer side (leader only). Each call is one fenced transaction.
    CommitHotBatch(ctx context.Context, b HotBatch, meta metastore.Batch) error
    CommitBlock(ctx context.Context, ns Namespace, blk BlockCommit, meta metastore.Batch) error
    Fold(ctx context.Context, blk BlockCommit) error
    Seal(ctx context.Context, s SealCommit) error
    PublishGeneration(ctx context.Context, g GenerationCommit) error
    DeleteNamespace(ctx context.Context, ns Namespace, meta metastore.Batch) error
}

type CatalogView interface {
    Revision() uint64
    Segments(ns Namespace) []SegmentView
    RefsFrom(ns Namespace, seq uint64) iter.Seq[BlockRef]
    TipSeq(ns Namespace) uint64
}
```

These signatures are a starting point. Adjust names to fit the existing code,
but keep the split: core logic must not import `pgx` or the AWS SDK.

In local mode, `Catalog` wraps today's file-based behavior, so the oracle keeps
exercising the same writer, sealer, compaction, and reader code.

### 19.2 New packages

```
internal/leader/      election loop, Locker interface
internal/pgstore/     pgx pool, migrations, schema checks, catalog.DB in SQL,
                      PG lease (leader.Locker; here, not in leader, because
                      pgstore -> catalog -> leader would otherwise cycle)
internal/objstore/    ObjectStore interface, S3 impl, in-memory fake, fault injection
internal/metastore/   Store interface, pebble and pg impls, in-memory fake
internal/catalog/     Catalog interface, local impl, pg impl, follower, mirror
internal/storagefake/ deterministic fakes of PG catalog semantics for tests
```

`cmd/jetstream` gains `storage init` and `storage new-identity`.

## 20. Testing

Testing is the most important part of this project. The work is not done
until every layer below exists and passes.

### Layer 1: interfaces

Every seam in §19.1 has an interface and an in-memory fake. Core code is tested
against the fakes, so tests stay fast (under 1s per package).

### Layer 2: local oracle

The existing oracle runs against local mode through the new interfaces. The
mutation campaign must not regress. This proves the refactor preserved local
behavior.

### Layer 3: deterministic disaggregated oracle

A second oracle configuration runs the full lifecycle against deterministic
fakes of PostgreSQL catalog semantics (`internal/storagefake`) and the in-memory
object store. The fakes:

- implement the fence, revisions, `seq/next` checks, reference checks, and
  object states exactly;
- run in one process with a seeded scheduler. Only the fault schedule
  replays exactly (the D4 fallback, S2.18). Pipe I/O and pod goroutines run
  outside the scheduler, so where a batch cut lands relative to a crash can
  differ between runs of one seed, and so can which commit prefixes are
  re-archived. The fake's meta reads take no scheduler turn: the atmos
  verifier holds a per-DID mutex across them, and synctest cannot see past a
  mutex wait;
- inject: leader kill at every crashpoint seam (before upload, after upload
  before commit, commit applied but reported failed, after commit before acks),
  lease loss, a stale leader writing after its successor, S3 PUT failure, S3
  returning wrong bytes, a lost `NOTIFY`, and a slow follower;
- check the catalog invariants (§9.3) after every transaction;
- run two or more reader pods whose delivered streams the oracle compares to the
  model: no missing event, no seq reuse, per-DID order kept. Readers must
  agree exactly. The only duplicate allowed is §10.4's re-archived commit
  prefix, at most one per leader change.

The oracle's crash seams are five crashpoints in `internal/crashpoint`: after
a hot batch is cut, after its upload, after its commit, after a fold's
upload, and after a seal's footer upload. A pod's `CrashInjector` kills its
`storagefake.Client` and blob handle at the seam, like SIGKILL, and a fresh
pod replaces it. `storagefake.DB.ExpireLease` models lease loss without
touching the holder's process. `Options.SteadyMaxSegmentBytes` shrinks
segments so seals happen within a short run. What the tier does not prove,
and why, is in `specs/oracle.md` ("Disaggregated Storage Tier").

`internal/storagefake` (S2.7) differs from PostgreSQL in these deliberate ways.
None of them weakens a check:

- Every write statement takes the archive row lock, not just the fence. Leader
  writes run the fence first, so this matches PostgreSQL for them. It also
  serializes lease statements against in-flight leader transactions, as the
  real `UPDATE archive` does.
- The invariant check runs after every commit, including lease statements. A
  violation is recorded (`DB.Violation`, `Config.OnViolation`) and the commit
  still stands, because PostgreSQL would have committed it. The oracle fails
  on the record.
- A listener that falls 64 notifications behind loses the excess. PostgreSQL
  queues them, but the follower's 250ms poll must cover a lost `NOTIFY`
  regardless, so dropping is the harsher and more useful behavior.
- Generation ID 0 stands for SQL `NULL`, and sequences are never reused, even
  after a rollback.

New mutants go into `testing/mutation/mutants/` for disaggregated-specific bugs.
At minimum:

- skip the fence;
- fold deletes one batch too few;
- relay cursor advances per block instead of per batch;
- GC skips the re-check;
- the follower drops a hot batch;
- seal reorders active blocks;
- sparse compaction miscounts `unique_did_count`.

Each must be killed.

### Layer 4: contract suites

One test suite per interface, run against every implementation:

- fakes and local (always, in `just test`);
- real PostgreSQL plus SeaweedFS, and real PostgreSQL plus MinIO, through a new
  `just test-storage` recipe. It requires a running `just up` and does not
  manage the environment's lifecycle, so a failed run can be inspected. It
  runs every package whose tests import `pgtest` or `s3test` with
  `JETSTREAM_TEST_STORAGE_REQUIRED=1`, once against SeaweedFS, and runs the
  object-store packages again against MinIO.

Each suite includes fault injection (connection kill mid-transaction,
`COMMIT`-result loss, S3 5xx and timeouts) and concurrency tests (two lockers
racing; a stale holder after expiry).

Also required:

- the sparse-vs-full compaction equivalence test (§12.3), as a property test;
- a fuzz target for decoding hot-batch rows and footer objects fetched from
  storage;
- the split-upstream-commit relay cursor test (§10.4).

### Layer 5: soak

A long-running deployment (real PostgreSQL, real SeaweedFS, three pods, the
simulator or a real relay) runs for at least 24h with random leader kills,
PostgreSQL failovers, and S3 outages. At the end:

- run an end-state oracle check: every event the source emitted is in the
  archive exactly once, and `getSegment` output verifies with
  `segment.VerifySealedMetadata`;
- compare what websocket clients connected to different pods received;
- report the measurements from §22.

## 21. Timestamp import removed

Timestamp import (`docs/README.md` §8, `internal/timestamp`, `internal/importer`,
the orchestrator import pass, `JETSTREAM_TIMESTAMP_IMPORT_*`, and
`TimestampStamper`) is deleted in both modes. That happens in a separate change,
before this work starts. The `indexed_at` block column stays, and it is always
`0` (meaning "use `witnessed_at`") until a new import design exists.
Re-adding import is tracked in
[#354](https://github.com/bluesky-social/jetstream/issues/354).

## 22. Measurements

Measure these before finishing the stage they belong to. Record the results in
this document.

| What | Why | Stage |
|---|---|---|
| PostgreSQL p50/p99 commit latency at 3,000 events/s with repo upserts, on RDS and self-hosted | latency budget; fenced-transaction throughput | 2 |
| Live event latency, end to end, idle and during bulk recovery | 20–40ms target | 2 |
| Fenced transactions/s during bootstrap (block commits plus metadata writes) | fence serializes all leader writes | 3 |
| Seal duration | fold backlog during seal | 2 |
| Fraction of blocks fetched per compaction pass | selective compaction benefit | 4 |
| Pod start: footer load time and manifest memory at pop1 size | readiness time; memory budget | 2 |
| Tombstone rebuild time on session start | failover time | 4 |
| Retry-scan cost against `metadata_kv` | 9GB-per-pass estimate | 3 |
| PostgreSQL WAL volume per day | sizing | 2 |
| `next_seq - readable_log_durable_seq` in local mode (pop1 shows 7) | looks wrong; explain before relying on the readable log | 1 (done, below) |

### 22.1 Stage 1 result: `next_seq - readable_log_durable_seq`

The gauge is correct. The 7 was one instant sample taken just after a block
commit.

Both series come from the steady-state live writer. It is the only writer that
gets the canonical `ingest.Metrics` in steady state. `next_seq` is set on every
append. `readable_log_durable_seq` is republished on every append too, but it
only changes when `commitDurableBatchLocked` finishes, after the block fsync
and the Pebble commit. So the difference is the number of events in the
unflushed partial block. It climbs by one per append, reads
`MaxEventsPerBlock` (4096) while a full block is being fsynced and committed,
and drops to 0 when the commit lands. The scrape is not atomic across the two
series, so a sample can be off by one.

There is no age-based cut. The steady-state writer cuts a block at 4096 events.
It also drains on three rare, event-driven paths: after each repo that the
failed-repo retry pass resyncs (`DrainDurability`), at the start of each
compaction pass (`ForceRotate`), and on `Close`. None of these is a timer. The
plan's finding 3 is right about the timer and slightly too strong about "count
only".

pop1 over 6h at 15s resolution (2026-09-25, build `3de2d5d`): min 5, p10 460,
p50 2,141, mean 2,132, p90 3,739, max 4,096. That is the uniform sawtooth on
`[0, 4096]` you would expect (mean 2,048). Over the same window, 9,892,604
events went into 2,417 blocks, which is 4,093 events per block. Nearly every
block is a full count cut.

`TestPendingGaugeIsPartialBlockSawtooth` (`internal/ingest/metrics_test.go`)
pins this. It checks the value after each append, checks that the value is a
full block inside the durable-batch hook, and checks that the gauges agree with
`ReadLog().DurableSeq()`.

What this means for the design:

- At pop1's rate (about 330 events/s), an event waits up to about 12.5s to
  become durable in local mode, and the readable log serves it to subscribers
  before then. Hot mode makes the hot batch the visibility point instead
  (§10.3–10.4), so this wait no longer delays or precedes visibility.
- At low rates nothing bounds the local wait. Hot mode bounds it with the 30s
  block age cut (§10.3). Stage 1 leaves local mode as it is.
- A single instant sample of this gauge means nothing. Read it with
  `max_over_time` or `avg_over_time`.

### 22.2 Stage 2 results

Measured 2026-09-25 with `cmd/storagebench` (`just storagebench write`,
`footers`, `calibrate`) on one workstation (32 threads, NVMe under LUKS)
against the `just up` SeaweedFS and MinIO. Two PostgreSQL 18.6 servers:

- **tmpfs**: the `just up` server. Its data directory is tmpfs, so a commit
  never waits for a disk flush (about 0.05ms).
- **disk**: a throwaway container with its data on the NVMe and default
  durability settings. A commit is about 4ms, almost all of it the WAL flush.
  This is the self-hosted estimate.

RDS was not measured: no instance was available. Expect Multi-AZ RDS commits to
cost as much as the disk server's or more, so read the disk rows as the
production case.

Method. `write` drives the hot writer with production defaults. Live events
arrive one at a time at a fixed rate. Each one stages the live consumer's
bookkeeping: a chain-state upsert and the relay cursor. Bulk workers append
whole repos beside them. Two followers read the result: the leader pod's
(doorbell and NOTIFY) and another pod's (its own pool, NOTIFY only). End-to-end
latency runs from an event's witness time to its delivery from a follower's
readable log. Both pods measured within 0.1ms of each other in every phase, so
the tables give one figure. Synthetic events match production's size
(`calibrate`: 82.5 raw bytes per live event against about 94 in production).
WAL is the server's LSN delta over the phase.

**Bug found and fixed first (`425fac0`).** Every durable batch's syncstate
snapshot cloned all verifier state promoted but not yet committed. Hot batches
are pipelined, so each batch restaged every entry of the batches still in
flight. A commit backlog made the next batch bigger, which made commits slower.
With 1M bulk events beside 3,000 live events/s, commits fell from 215k to about
200 events/s, live latency passed 3s, and a batch carried 743 metadata ops on
average. A snapshot now takes only what was promoted since the previous one. A
batch that fails to commit hands its still-current entries to the next batch.
Afterwards the same run staged 3.8 ops per batch. It made 1M bulk events
durable in 2.65s on tmpfs.

Steady state (live only):

| Rate | PG | Hot batch txn p50/p99 | Commit p50/p99 | End-to-end p50/p99 | WAL per day |
|---|---|---|---|---|---|
| 10/s | tmpfs | 0.7/1.6ms | <0.1ms | 17/23ms | — |
| 10/s | disk | 5.0/6.6ms | 4.4/5.8ms | 21.1/24.5ms | 1.0GiB |
| 330/s (pop1) | disk | 4.4/6.1ms | 4.0/5.5ms | 14.3/21.7ms | 15.3GiB |
| 3,000/s | tmpfs | 1.0–1.3/1.6–2.3ms | about 0.05ms | 9.9/17.8ms | 107GiB |
| 3,000/s | disk | 4.8/8.2ms | 4.0/6.6ms | 13.8/21.9ms | 123GiB |

- Steady state meets the 20–40ms target on both servers. The live batch age
  (15ms) dominates latency.
- At 3,000/s the writer commits about 64 hot batches/s (age cuts) and stages
  1.02 metadata ops per live event. Every live batch was inline: the 4MiB/s
  token bucket never ran short, so no forced cut produced a pointer batch.
- WAL is about 510 bytes per live event: the inline frame, the hot batch
  row, the chain-state upsert, and their index entries. Budget about 125GiB
  per day at 3,000 events/s and 15GiB per day at pop1's rate. Bulk batches are
  pointers, so bulk recovery adds little WAL: 117–148GiB per day at 3,000 live
  plus 10k–100k bulk events/s.

Bulk recovery, with 3,000 live events/s:

| Bulk | PG | Bulk achieved | End-to-end p50/p99 | Notes |
|---|---|---|---|---|
| 10k/s paced | tmpfs | 10k/s | 9.2/17.8ms | |
| 30k/s paced | tmpfs | 30k/s | 8.4/17.8ms | |
| 100k/s paced | tmpfs | 100k/s | 6.2/358ms | a seal hit the unfolded cap |
| 1M, unpaced | tmpfs | 377k/s | 415/609ms | |
| 30k/s paced | disk | 29k/s | 958/1,909ms | |
| 4M, unpaced | disk | 36k/s | 6,742/7,102ms | |
| 30k/s paced, 256KiB bulk permits | disk | 30k/s | 26.5/66.7ms | experiment |

The rows above are from before S2.23. The bullets below explain them.

- **Live latency misses the target during bulk recovery on the disk server.**
  §10.5 assumed a live batch waits behind about one S3 PUT. It actually waits
  behind every bulk batch frozen ahead of it, and the 64MiB bulk permit pool
  admits hundreds of them (bulk batches averaged about 520 events, 36KiB raw).
  Each bulk batch costs two transactions: its object's registration (§7.3 step
  3) and its hot batch commit. The fence row lock serializes both kinds with
  every other leader transaction, and on the disk server each holds that lock
  for about 4ms of WAL flush. The leader therefore commits at most about 200
  transactions per second. At 30k bulk events/s, 13,500 transactions in 61s
  spent about 54s in commit alone. tmpfs hid this because its commits are
  almost free.
- Shrinking the bulk pool to 256KiB, about 7 batches in flight, kept 30k bulk
  events/s and cut live latency to 26.5/66.7ms. So queue depth is most of the
  problem. The remaining p99 is the serialized commit path, still about 85%
  busy. S2.23 bounds the bulk batches in flight and commits consecutive ready
  hot batches in one transaction.
- Unpaced bulk on tmpfs misses too (415/609ms), for the same queue-depth
  reason.

After S2.23, with 3,000 live events/s:

| Bulk | PG | Bulk achieved | End-to-end p50/p99 | Hot batches per txn |
|---|---|---|---|---|
| none | disk | — | 13.8/22.0ms | 1.0 |
| 10k/s paced | disk | 10k/s | 14.1/25.8ms | 1.1 |
| 30k/s paced (3 runs) | disk | 30k/s | 16.6–16.9/33.5–35.4ms | 1.4 |
| 4M, unpaced | disk | 140k/s | 41.0/89.6ms | 4.3 |
| 30k/s paced | tmpfs | 30k/s | 8.2/17.7ms | 1.1 |
| 1M, unpaced | tmpfs | 386k/s | 19.8/71.8ms | 3.3 |

- **The target holds on the disk server.** Live p99 stays within 40ms beside
  30k bulk events/s, down from 1.9s. Unpaced bulk runs 3.9 times faster
  (140k against 36k events/s), and its live p99 falls from 7.1s to 90ms.
- Each part moved a different number. Measured one after another on the
  disk server:
  - Rule 7's frozen-bulk bound plus hot batch group commit gave 39.4–41.6ms
    at 30k/s paced, and 105k/s at 72/122ms unpaced.
  - Sharing object registrations then gave 33.5–35.4ms paced, and 140k/s at
    41/90ms unpaced.
- Paced bulk groups little: 1.4 hot batches per transaction and nearly one
  object transaction per pointer batch. The bound on queued batches does most
  of the work there. Unpaced bulk groups 4.3 hot batches per transaction, which
  is where its throughput comes from.
- On the disk server every transaction still costs a 4ms WAL flush on the
  fence row. At 30k/s paced the leader runs about 170 transactions a second:
  about 105 hot batch, 58 object, and 8 fold. The remaining p99 is queueing
  behind them. Bulk beyond about 30k/s paced therefore needs fewer
  transactions per event, not a faster path.
- `cmd/storagebench write` reports hot batches per transaction, and
  `jetstream_hot_commit_batches` shows it in production.

Seal. A 256MiB segment seals in 1.40s (tmpfs) to 1.61s (disk). The seal
transaction itself is 18–28ms. The rest is reading back the active blocks,
building the footer, and uploading it. Folds wait while the maintainer seals,
so unfolded events grow at the append rate for that long. At 100k bulk events/s
that is about 140k events, past the 65,536 unfolded cap (rule 9), so live
appends blocked. That seal caused the 358ms p99 above. At 30k events/s a seal
adds about 45k events, under the cap. The cap and the seal duration together
bound the bulk rate that keeps live latency flat.

Pod start at pop1 size (`footers`). Synthetic sealed segments of 680 blocks,
which is what a 256MiB segment of production-sized blocks holds. Only the
footers are uploaded. Two footer profiles: backfill blocks, with 64 distinct
DIDs per block (repos arrive whole), and live blocks, with 3,500 distinct DIDs
per 4,096 events.

| Profile | Segments | Footers | Load to ready | `ResidentBytes()` | Heap growth, excluding cache | Object cache | Leader invariant check |
|---|---|---|---|---|---|---|---|
| backfill | 7,000 | 1,528MiB (0.22MiB each) | 8.1s | 1,649MiB | 4,475MiB | 1,528MiB | 4.5s |
| live | 50 | 458MiB (9.2MiB each) | 0.42s | 460MiB | 488MiB | 458MiB | 31ms |

- Load time is fine: 188MiB/s for backfill footers and 1.1GiB/s for live
  footers, which have fewer and bigger objects. The backfill catalog has 4.76M
  block object rows and makes a 2.4GiB database.
- The leader's session-start `CheckInvariants` (the cheap subset, §10.9) takes
  4.5s on the pop1-sized catalog. That adds to failover time.
- Where the 7,000-segment heap goes (heap profile):
  - manifest: 2.75GiB, of which blooms are 2.2GiB;
  - the follower's object index: 1.08GiB, about 227 bytes per referenced
    object, and every sealed block is an object;
  - a second copy of each segment's block list, held by the follower
    (0.24GiB);
  - the object cache: 1.5GiB.
- `Manifest.ResidentBytes()` is accurate for live footers (1.01 of footer
  bytes, against 1.07 measured). For backfill footers it undercounts by about
  2.7×, because it counts neither per-bloom overhead nor the follower's
  object index and block lists. The §17 startup check uses it. At pop1 the
  check under-reserves by about 2.8GiB. S5.2 must count the follower's
  structures, or use heap growth across the first load.
- Footers do enter the object cache: `SealedMetadata` rereads them through it
  (§17 corrected). The LRU still bounds them. At pop1 start they fill 1.5GiB
  of the 2GiB default until block reads evict them.
- pop1's real mix of the two profiles is not known here. 7,000 backfill-shaped
  segments are the floor. Each live-shaped segment adds about 9.75MiB of heap
  plus 9.2MiB of cache churn.

## 23. Metrics

All metrics use the existing `obs` package. Names:

- `jetstream_leader_is_leader` (gauge), `jetstream_leader_epoch` (gauge),
  `jetstream_leader_sessions_total{result}` (ended sessions; result is fatal,
  lease_lost, shutdown, or restart), `jetstream_leader_session_starts_total`,
  `jetstream_leader_fence_failures_total`
- `jetstream_pg_txn_duration_seconds{kind}` (hot_batch, block, fold, seal,
  compaction, gc, metadata, plus read for catalog snapshots and meta_read for
  metastore reads), `jetstream_pg_txn_errors_total{kind}`
- `jetstream_hot_batches_total{class, storage=inline|pointer}`,
  `jetstream_hot_batch_events` (histogram),
  `jetstream_hot_commit_batches` (histogram, batches per group commit),
  `jetstream_hot_unfolded_events` (gauge), `jetstream_hot_pending_bytes{class}`
- `jetstream_admission_wait_seconds{class}`,
  `jetstream_hot_inline_tokens` (gauge)
- `jetstream_maintainer_folds_total{result=uploaded|dedup}`,
  `jetstream_maintainer_fold_duration_seconds`,
  `jetstream_maintainer_seals_total`,
  `jetstream_maintainer_seal_duration_seconds`,
  `jetstream_maintainer_queued_blocks` (gauge),
  `jetstream_maintainer_active_segment_bytes` (gauge: the rotation rule's input),
  `jetstream_maintainer_rebuild_duration_seconds`
- `jetstream_s3_requests_total{op, result}`,
  `jetstream_s3_request_duration_seconds{op}`,
  `jetstream_s3_bytes_total{op}`, `jetstream_s3_verify_failures_total{path=upload|read}`
  (request metrics count every attempt, retries included)
- `jetstream_objects{state}` (gauge, refreshed by GC),
  `jetstream_gc_deleted_total`, `jetstream_gc_run_duration_seconds`
- `jetstream_catalog_revision` (gauge), `jetstream_catalog_lag_seconds`
  (gauge: now minus the last successful refresh),
  `jetstream_catalog_refresh_duration_seconds`,
  `jetstream_catalog_refresh_errors_total`,
  `jetstream_catalog_notify_received_total`,
  `jetstream_catalog_listen_errors_total`
- `jetstream_event_visibility_latency_seconds`: follower append time minus
  `witnessed_at`, per pod
- `jetstream_storage_corruption_total{source}`
- `jetstream_memory_budget_bytes{budget}` and
  `jetstream_memory_used_bytes{budget}` (the object cache reports
  `budget="object_cache"`)

Add OTEL spans around every PostgreSQL transaction and S3 call, with the
revision and object ID as attributes: `objstore.Upload`, `objstore.Get`, and
`objstore.GetRange` carry object IDs, and `s3.<op>` carries the key.

## 24. Security

- PostgreSQL and S3 credentials come from the environment or the AWS SDK chain.
  They are never logged, never put in `.env`, and never shown on `/status`.
  Connection strings are logged only through `pgstore.RedactURL`. It masks the
  userinfo password and every parameter value outside a fixed allowlist (host,
  port, dbname, user, ssl* paths and modes, application_name, connect_timeout,
  target_session_attrs, pool_*), and replaces any string it cannot fully parse
  with `<unparsable connection string>`.
- Use TLS for PostgreSQL (`sslmode=verify-full` recommended) and HTTPS for S3
  unless the operator sets a plain `http://` endpoint for an on-prem store.
- Object keys are random UUIDs and never contain user data.
- Reader pods need only `SELECT` on the schema, plus `LISTEN`. Document an
  optional read-only role. The leader needs full DML on the schema.
- Bytes read from S3 are treated as untrusted input until their hash verifies.

## 25. Dependencies

Add to the AGENTS.md whitelist:

- `github.com/jackc/pgx/v5`
- `github.com/aws/aws-sdk-go-v2` (core, `config`, `credentials`,
  `service/s3`)

No other new dependencies. Container test tooling for `just test-storage` uses
the host's container runtime from the justfile, not a Go library.

## 26. Delivery stages

Each stage ends with `just` green, the listed extra checks, and the listed
measurements recorded.

1. **Interfaces.** Add `metastore.Store`, `Catalog`, `HotLog`, and `ObjectStore`.
   Move all local-mode code onto them with no behavior change. Remove the
   write-ahead seq lease dependency from the interfaces (local mode keeps it
   internally). Exit: local oracle and mutation campaign unchanged;
   `just test-long ./internal/oracle` and `just oracle-sweep` pass.
2. **Steady state on fakes and real storage.** PG schema, lease, fence, hot
   mode, admission, fold, seal, follower, mirror, all read endpoints. Bootstrap
   is skipped in tests by starting from a seeded catalog. Exit: layer 3 oracle
   for steady state with failover; layer 4 suites pass on SeaweedFS and MinIO;
   stage-2 measurements recorded.
3. **Bootstrap and merge.** Direct mode, both namespaces, merge, pending retry
   pass, `storage init`. Exit: the layer 3 oracle covers the full lifecycle with
   kills in every phase.
4. **Compaction and GC.** Sparse rewrite, generation publish, GC. Exit:
   equivalence property test, compaction mutants killed, stage-4 measurements
   recorded.
5. **Soak and operations.** `storage new-identity`, memory budget checks,
   dashboards, soak run. Exit: 24h soak passes the end-state oracle check.

Only after stage 5: deploy the new pop instance in disaggregated mode.
