# Disaggregated primary storage: S3 blocks and a PostgreSQL catalog

Date: 2026-09-20. Status: accepted design direction; not implemented.

## Decision

Add a second, operator-selected storage backend for production deployments:

- S3 holds immutable, SHA-256-addressed compressed segment block frames and
  the immutable header/footer objects needed to reproduce a `.jss` file.
- A highly available PostgreSQL service is the authoritative metadata store,
  segment catalog, and transaction/commit point.
- Local disk is a disposable bounded cache and staging area. It is not needed
  to recover authoritative state.
- One fenced writer owns an archive at a time. Readers may scale out against
  PostgreSQL and S3.

Keep the existing `.jss` + Pebble implementation as the default `local`
backend. It remains the simplest deployment, the fastest development loop,
and the reference implementation for the segment format. The new backend is
`disaggregated`; it does not emulate S3 as a filesystem and it does not make
Pebble snapshots part of normal production durability.

The disaggregated commit rule is the analogue of today's “fsync segment,
then commit Pebble” rule:

> Successfully create and verify every required immutable S3 object first;
> then commit its references and all event-dependent metadata in one fenced
> PostgreSQL transaction. PostgreSQL commit is the point at which the block is
> part of the durable archive.

An S3 object without a PostgreSQL reference is garbage. A PostgreSQL reference
without its S3 object is corruption. S3 listing is never authoritative.

This supersedes the primary direction of the earlier
[native S3 backup/restore exploration](https://github.com/bluesky-social/jetstream/blob/jc/s3-backups/specs/notes/2026-09-20-native-s3-backup-restore-plan.md).
That note remains useful for its format analysis, restore hazards, and
sequence-reuse analysis, but this design makes remote storage primary instead
of periodically coordinating local segment and Pebble snapshots.

## Why this design

Jetstream's large state is already mostly immutable compressed blocks. The
expensive operation is repeatedly moving whole segment files even when a
rewrite changes only a few blocks. Object storage is a natural home for those
blocks, but not for an appendable file or a mutable database. PostgreSQL gives
us the missing transactional authority: it can atomically publish a block,
advance the safe relay cursor, update verifier/repo state, renew the local
sequence lease, and expose a new segment generation.

This division also makes the failure boundary legible:

```text
                          authoritative commit
                                  |
relay/PDS -> encode -> S3 immutable objects -> PostgreSQL catalog + metadata
                         |                   |
                         |                   +-> plans, generations, cursors
                         +-> local cache          and writer fencing

Readers -> PostgreSQL recipe/snapshot -> cache or S3 -> unchanged segment decoder
```

The segment encoding remains the unit of interoperability. A generation
recipe can recreate the exact bytes of a sealed `.jss` file, so existing
decoders and archive APIs do not need a second event format.

## Goals

1. Preserve Jetstream's existing correctness guarantees: client-observable
   sequence numbers are never reused, event-dependent metadata never leads
   durable event data, per-DID order is stable, and unexplained holes fail
   loud.
2. Remove local disk and a single process's Pebble database from the recovery
   path in production-grade deployments.
3. Keep ingest and archive serving fast at current production scale, including
   during compaction and timestamp import.
4. Let stateless or near-stateless readers scale independently and let a new
   writer take over without copying a multi-terabyte archive.
5. Reuse unchanged compressed frames across compaction/import generations.
6. Preserve the current local backend with no S3 or PostgreSQL requirement.
7. Make every partial failure resolve to either unpublished garbage or a
   complete database transaction, with deterministic recovery.
8. Retain the current public protocol and exact `.jss` representation unless
   a separately versioned protocol change is justified.

## Non-goals

- Active-active ingest. The initial design has one writer and active-passive
  failover. Running two sequence allocators or lifecycle orchestrators for one
  archive is explicitly unsupported.
- Treating S3 as POSIX, implementing append with multipart uploads, or putting
  Pebble on an object-store `vfs.FS`.
- Eliminating all local storage. Bounded cache, temporary rewrite space, and
  multipart staging may use ephemeral SSD.
- Making S3 the transaction coordinator. It stores bytes and an independent
  sequence ceiling; PostgreSQL publishes archive state.
- Exactly-once delivery. Existing at-least-once behavior and inclusive cursors
  remain the contract.
- Serving correctness-sensitive state from an asynchronously lagging database
  replica without an explicit freshness bound.
- Transparent multi-region active-active service. Regional disaster recovery
  needs a reachable monotonic sequence authority or a new archive identity.
- Replacing PostgreSQL backups. Database PITR and object retention are both
  required; neither substitutes for the other.
- A general storage abstraction usable by arbitrary applications. Interfaces
  should express Jetstream operations and invariants.

## Production workload and design implications

Read-only Grafana measurements for pop1 over
2026-09-20 03:42:37–07:48:49 UTC, plus the public status endpoint, showed:

| Observation | Approximate result | Design implication |
| --- | ---: | --- |
| Archived events | 564/s | Per-event S3 writes are unacceptable; blocks remain the upload unit. |
| Upstream relay events | 236/s | Backfill, repair, and synthetic work are a meaningful part of ingest. |
| Block flushes | 2,035, one every 7.3s | Upload-before-commit adds one S3 PUT per new block, not one per event. |
| Segment rotations | 3, about 17.5/day | Seal transactions and footer objects are infrequent. |
| Synced metadata batches | Exactly 2,035 | The existing block boundary already defines a useful remote transaction cadence. |
| Metadata point reads | 6.12 million, about 415/s | Remote SQL is feasible, but multi-get, prepared statements, and safe caching matter. |
| Mean Pebble batch commit | 1.44ms | PostgreSQL commit latency is a benchmark gate, not assumed free. |
| Seven-day whole-file compaction writes | 6.57TB/day | Uploading rewritten whole files would be gross write amplification. |
| Mean compaction pass | 77.4 minutes | Publication cannot require pausing or serializing the full pass. |
| `getBlock` requests | 88,649; 1.21MB/s | A block cache and bounded concurrent S3 reads should handle the observed load. |
| Current archive | 1.82TiB, 7,651 sealed segments, 6.40M blocks, 25.7B events | Initial migration and catalog size are first-class work. |

The earlier backup investigation found that about 87% of rewritten files
touched at most ten blocks. Content addressing therefore attacks the dominant
compaction amplification directly: unchanged frames retain the same identity
and need neither upload nor another physical copy.

These numbers are observations, not capacity limits. Before production
cutover, benchmark peak backfill, import, compaction, cache-cold replay, S3
throttling, PostgreSQL failover, and archives larger than pop1.

## Preserved and new invariants

All invariants in `specs/invariants.md` continue to apply. Their storage-specific
forms are:

1. **Published generations are immutable.** A generation's header, ordered
   block references, and footer never change. Rewrites publish another
   generation and atomically change a pointer.
2. **Objects lead metadata.** Every S3 object needed by a database transaction
   is durably readable and verified before the transaction may reference it.
   Metadata must never lead object data.
3. **PostgreSQL is authoritative.** Readers discover segments, blocks,
   generations, vacancies, and current pointers through the catalog. Bucket
   listing can audit or find garbage but cannot repair catalog state.
4. **Hashes name exact bytes.** The SHA-256 is over the stored bytes, not
   decoded events, an ETag, or a logical block. Reads verify length and hash
   before decoded data becomes observable.
5. **One fenced writer.** Every writer transaction proves the current fencing
   epoch. Losing leadership or the database session immediately makes the old
   process unable to publish.
6. **Event and metadata effects are atomic.** The block reference, exact
   `seq/next`, sequence lease, safe `relay/cursor`, and all eligible repo,
   verifier, sync, and lifecycle mutations commit together.
7. **Client-observable seqs are never reused.** The existing one-block local
   lease protects process crashes. An independent S3 ceiling protects the same
   cursor namespace across database rollback or loss.
8. **Unregistered coverage holes fail loud.** Recipes and explicit
   `seq/gap/*` vacancies explain all durable coverage. Missing objects, missing
   ordinals, or unexplained sequence holes are corruption, not an invitation
   to skip forward.
9. **Published segment order is stable.** Logical segment indices retain the
   same creation/time order as lexically sorted local filenames. Generation
   changes never reorder logical segments.
10. **Bad upstream input is dropped; bad owned state stops the process.** An
    invalid relay record remains an input error. A hash mismatch, impossible
    recipe, lost fence, or referenced missing object is internal corruption.

The operator configuration is part of the durability contract. PostgreSQL
must use synchronous commit, durable storage, and a failover configuration
that will not promote a replica missing acknowledged transactions. S3 must
provide atomic whole-object writes, strong reads after successful writes, and
working conditional requests. Startup refuses a deployment that explicitly
configures weaker database commit behavior.

## Object representation

### Keys and identities

An archive has a stable random `archive_id` that does not change with pod,
hostname, database instance, or region. It prevents two Jetstream archives
from accidentally sharing sequence control state. An illustrative layout is:

```text
<prefix>/<archive-id>/
  objects/sha256/ab/cd/<64-lowercase-hex-digits>
  control/sequence-ceiling
  probes/<deployment-id>/...
```

Objects under `objects/` are immutable and content addressed. Use
`If-None-Match: *` when the provider supports it. If an object already exists,
verify its recorded size and checksum rather than overwriting it. A conflicting
object under the same SHA-256 key is corruption.

The initial implementation stores three object kinds:

| Kind | Stored bytes | Notes |
| --- | --- | --- |
| `block-frame` | Existing zstd frame, without its 8-byte local length prefix | The catalog records compressed length; the prefix is deterministic when materializing `.jss`. |
| `segment-header` | Exact reserved header bytes | Small but content addressed for one uniform integrity model. |
| `segment-footer` | Exact sealed footer bytes | Includes the indexes and historical envelopes belonging to that generation. |

The hash is over raw stored bytes. The object catalog records length, SHA-256,
creation time, and optional provider checksum/version; each recipe reference
declares its expected kind. Kind is not folded into the key, so identical raw
bytes may safely satisfy references in more than one role.

Do not use S3 ETags as content hashes. Multipart upload, encryption, and
provider differences make that invalid. Send or validate the provider's
SHA-256 checksum when available and always retain Jetstream's own digest.

### Exact `.jss` reconstruction

A sealed generation recipe contains:

- logical segment identity, stable segment index/name, and format version;
- exact header object reference;
- ordered block-frame references with compressed length;
- the immutable per-block descriptors needed for planning (event/sequence and
  witnessed-time envelopes, count, and relevant blooms/index summaries);
- exact footer object reference;
- total materialized file length and an optional hash of the complete `.jss`.

Reconstruction writes the header, then for each block an 8-byte little-endian
frame length and the unchanged compressed frame, then the footer. It does not
decode/re-encode events or rebuild a footer from surviving rows. Rebuilding a
footer would risk losing historical envelopes intentionally preserved by
compaction.

An active segment has a header plus an ordered list in
`active_segment_blocks`; it has no footer and is never advertised as a sealed
generation. The database list, rather than an appendable S3 object, is the
active segment.

### S3 semantics relied upon

AWS S3 currently provides strong read-after-write and list consistency, but
this design needs only strong reads of known keys. It assumes:

- a successful PUT or completed multipart upload exposes either the complete
  object or no object, never a prefix;
- GET/HEAD of its known key can confirm the newly written object;
- conditional create and conditional replacement work for immutable objects
  and the sequence control record;
- an aborted multipart upload is not a visible object.

“S3 compatible” is not sufficient evidence. A provider must pass integration
tests for these behaviors, ambiguous timeouts, checksum headers, and concurrent
conditional requests. The service never depends on rename, append, directory
fsync, or LIST for correctness.

Enable bucket versioning. Object Lock/governance retention is recommended for
the control object and as a safety net for archive objects, but it is not a
substitute for application-level GC. Cross-region replication is asynchronous
and does not make two regions one linearizable sequence authority.

## PostgreSQL data model

The schema below is logical. Names and column widths may change after query
benchmarks, but the transaction boundaries and identities are part of the
design.

### Archive and writer control

```text
archives(
  archive_id uuid primary key,
  format_version integer,
  created_at timestamptz,
  catalog_commit bigint,
  writer_epoch bigint,
  writer_identity text,
  remote_sequence_ceiling bigint,
  remote_ceiling_etag text,
  active_namespace text,
  configuration_fingerprint bytea
)
```

`catalog_commit` is a monotonically increasing logical revision allocated
inside publishing transactions. `writer_epoch` is the fencing token. Remote
ceiling fields record the S3 control state observed by the current writer;
they cannot lower the S3 value.

Leadership uses a session-level PostgreSQL advisory lock derived from
`archive_id`. After taking it, a candidate locks the archive row, reconciles
the remote sequence ceiling, increments `writer_epoch`, and records its unique
process identity. Every mutating transaction locks or conditionally updates
the row with that epoch. A database reconnect is a new leadership attempt, not
permission to keep using an old token.

The advisory lock gives prompt mutual exclusion; the durable epoch gives
fencing and auditability. Wall-clock lease expiry is not the correctness
mechanism and clock skew cannot create two valid writers.

This fencing domain is one logical PostgreSQL cluster. A restored/forked second
cluster has an independent advisory-lock namespace, so an operator must revoke
or otherwise fence the old cluster's writer before promoting the fork. The S3
ceiling prevents sequence reuse between such sessions; it does not reconcile
two divergent catalogs. Automatic cross-cluster writer election would require
a separate consensus control plane and is outside this design.

### Compatibility metadata KV

```text
metadata_kv(
  archive_id uuid,
  namespace text,
  key bytea,
  value bytea,
  updated_commit bigint,
  primary key (archive_id, namespace, key)
)
```

The first implementation preserves current Pebble key/value encodings. This
reduces semantic migration risk for `relay/cursor`, `seq/*`, `repo/*`,
`pdshost/*`, `sync/*`, lifecycle, retry, import, and compaction state. The SQL
store supports atomic put/delete, ordered prefix/range scans, compare-and-set,
and multi-get. PostgreSQL `bytea` ordering must be covered by compatibility
tests against Pebble iteration.

Typed tables should replace opaque keys where they materially improve queries
or constraints, but not as part of the minimum storage cutover. Segment
catalog rows are typed from the beginning because recipes, snapshot visibility,
and referential integrity are new semantics.

### Objects and upload state

```text
objects(
  archive_id uuid,
  sha256 bytea,
  byte_length bigint,
  state smallint,              -- uploading, available, gc_claimed, deleted
  provider_version text,
  created_at timestamptz,
  unreferenced_at timestamptz,
  delete_after timestamptz,
  primary key (archive_id, sha256)
)
```

Register an upload attempt before PUT and mark it `available` only after
verification. A stale `uploading` row is recoverable staging, not a reference.
All recipe/reference insertions require `available`. This registration avoids
making a full bucket listing the normal way to discover orphan uploads; S3
Inventory remains useful for audit.

Object kind belongs to the reference, not this row. The object table describes
opaque bytes; in the fantastically unlikely but valid case that two roles have
identical bytes, one hash can satisfy both references without a contradictory
single-kind column.

### Segments, generations, and blocks

```text
segments(
  archive_id uuid,
  namespace text,
  segment_index bigint,
  stable_name text,
  state smallint,              -- active or sealed
  active_header_sha256 bytea null,
  current_generation_id uuid,
  created_commit bigint,
  primary key (archive_id, namespace, segment_index),
  unique (archive_id, namespace, stable_name)
)

segment_generations(
  generation_id uuid primary key,
  archive_id uuid,
  namespace text,
  segment_index bigint,
  generation_number bigint,
  header_sha256 bytea,
  footer_sha256 bytea,
  materialized_length bigint,
  materialized_sha256 bytea null,
  min_seq bigint,
  max_seq bigint,
  min_witnessed_us bigint,
  max_witnessed_us bigint,
  visible_from_commit bigint,
  visible_until_commit bigint null,
  creation_reason smallint,    -- seal, compaction, import, migration
  unique (archive_id, namespace, segment_index, generation_number)
)

generation_blocks(
  generation_id uuid,
  ordinal integer,
  block_sha256 bytea,
  compressed_length bigint,
  event_count integer,
  min_seq bigint,
  max_seq bigint,
  min_witnessed_us bigint,
  max_witnessed_us bigint,
  descriptor bytea,
  primary key (generation_id, ordinal)
)

active_segment_blocks(
  archive_id uuid,
  namespace text,
  segment_index bigint,
  ordinal integer,
  block_sha256 bytea,
  compressed_length bigint,
  event_count integer,
  min_seq bigint,
  max_seq bigint,
  min_witnessed_us bigint,
  max_witnessed_us bigint,
  descriptor bytea,
  committed_commit bigint,
  primary key (archive_id, namespace, segment_index, ordinal)
)
```

Foreign keys bind object references to `available` object identities through
transactional checks/triggers where a plain foreign key cannot express state.
Generation block ordinals are dense from zero. Constraints reject inverted
envelopes, overlapping active ordinals, invalid sequence bounds, and a sealed
segment without a current generation.

`visible_from_commit`/`visible_until_commit` provide catalog history for
auditing, database recovery, and a possible future generation-bound API. Old
generation rows and objects remain available for the existing HTTP cache grace
and recovery window after `segments.current_generation_id` changes.

Additional typed tables will likely cover import jobs, catalog snapshot pins,
and GC claims. Avoid denormalizing a second mutable manifest that can disagree
with these rows.

## Core protocols

### New block commit

The block commit path is deliberately small and ordered:

1. Under the ingest writer's existing ordering rules, prepare and compress a
   complete block. Compute its descriptors and SHA-256 over the compressed
   frame.
2. Ensure the current process has enough local and remote sequence lease for
   every seq already made client-observable. This happens before readable-log
   publication, as today.
3. Register/claim the object row, PUT the frame by hash if necessary, and
   verify known-key HEAD/GET metadata. Deduplication may turn this into no
   network write.
4. Begin a PostgreSQL transaction and lock/assert the current writer epoch,
   archive row, and active segment tail.
5. Insert exactly the next active block ordinal and object reference. Apply in
   the same transaction all metadata mutations eligible through this block:
   exact `seq/next`, `seq/max_reserved`, relay cursor, repo ratchets, verifier
   state, sync/account state, and any lifecycle progress.
6. Increment `catalog_commit` and commit with synchronous commit enabled.
7. Only after success, acknowledge remote durability to the ingest pipeline
   and advance the durable readable-log boundary. Cache population and
   notifications happen after commit and are disposable.

If steps 1–3 fail, there is no catalog change. If the process dies after the
PUT but before step 6, the object is unreferenced garbage. If the transaction
outcome is ambiguous, reconnect and query the unique active ordinal plus
commit identity; never append another ordinal based on an assumption.

The unique segment/ordinal key, expected prior tail, block hash, and fencing
epoch make retries idempotent. Repeating a successful transaction must either
observe the identical result or fail loud on different bytes/state.

The current live readable log may still expose a prepared event before this
remote commit. The existing durable sequence lease bounds that window. An
unclean failover registers the abandoned lease tail as a vacancy, so those seqs
are not reassigned.

### Metadata-only transactions

Not every current Pebble batch accompanies a block. Metadata-only progress may
commit directly to PostgreSQL when it has no dependency on uncommitted event
bytes. Code must declare the dependency class; it may not infer safety from an
empty block. Pending verifier, cursor, merge, compaction, and import state keep
their current event-dependency rules.

Batch low-value point mutations and use multi-row SQL rather than translating
one Pebble batch into hundreds of round trips. Import-sized changes should use
COPY into a temporary/staging table followed by one constrained merge
transaction.

### Sealing and rotation

Sealing converts the active database list into an immutable generation:

1. Pin the active segment's committed block list at a catalog revision.
2. Generate the exact final header/footer and recipe using the existing
   segment format code. Upload and verify the header and footer objects.
3. In one fenced transaction, assert that the active tail has not changed;
   insert a generation and its ordered block rows; mark it visible; point the
   logical segment at it; mark the segment sealed; and create the next active
   logical segment with the next stable index.
4. Commit, then publish cache/reader notifications.

No S3 rename or copy is involved. A crash before the transaction leaves
unreferenced header/footer objects and the old active list. A crash after it
leaves one complete sealed generation and one new active segment.

The transaction may copy hundreds or a few thousand active block rows, which
is infrequent at the measured rotation rate. If that is too costly, an
immutable list object can optimize representation later, but it must not give
up SQL-level validation or snapshot semantics without measurement.

### Compaction

Compaction is copy-on-write at generation granularity and content-addressed at
block granularity:

1. Capture the source generation IDs and compaction input watermark in a
   short database snapshot. They remain immutable.
2. Outside a database transaction, fetch/decode only needed blocks, apply
   tombstones, and build replacement frames and the exact new footer. Reuse
   old hashes for byte-identical unchanged frames; do not recompress them.
3. Upload and verify every new block/header/footer object.
4. In one fenced publication transaction, compare each segment's current
   generation with the captured source. Insert complete new generations,
   close old visibility intervals, CAS current pointers, and advance the
   compaction watermark only through work represented by those generations.
5. On a CAS conflict, discard or rebase the candidate. Never publish a recipe
   computed from one generation over another generation's pointer.

A chunk that updates several segments and one watermark publishes them in one
transaction. Keep chunks small enough for bounded lock and WAL pressure. Heavy
decode/rewrite/upload work never holds writer or catalog locks.

An in-flight response that pinned the older recipe completes from that
immutable generation; HTTP caches may retain it through the configured grace
period. A later name lookup resolves the new generation and exposes its new
ETag. Compaction no longer needs filesystem rename or a manifest rescan.

### Timestamp import and other rewrites

Timestamp import uses the same generation CAS protocol as compaction. Its job
progress, rules state, and generation pointer changes commit together at each
declared job boundary. A rule database must not claim a segment was patched
unless the corresponding generation is visible.

Long-running jobs pin source generation IDs, not filenames. Conflicts retry
from the new current generation. Original import inputs remain operator-owned
recovery inputs until the job reaches its terminal durable state; storing
segment blocks remotely does not make a partially consumed CSV reproducible.

Bootstrap live trees and merge namespaces are represented by the same
`namespace` dimension. Lifecycle transitions atomically change typed/control
metadata and retain all source generations required to resume. A first release
may support steady-state migration only, but it must explicitly refuse an
unsupported bootstrap/merge state rather than publish an incomplete archive.

### Archive planning and reads

The initial implementation preserves the current public contract. Within one
`planSnapshot` request, a database transaction selects one `catalog_commit`
and resolves a self-consistent set of current generations. Each returned
segment already includes its segment-format checksum. Existing pagination
continues to hold `sealedTipSeq` fixed with `beforeSeq`; it does not introduce
a new token.

Today `getSegment` and `getBlock` accept only the stable segment name (and a
block ordinal), not a generation ID. They therefore resolve the current
generation at the start of the request and pin that immutable recipe for the
life of the response. Their ETags retain current semantics: the exact segment
format checksum for `getSegment`, and that checksum plus ordinal for
`getBlock`. A client whose planned checksum no longer matches the response
has raced compaction/import and re-plans, just as it does when a local file was
atomically replaced. Never splice references from two generations in one
response.

After resolving that pinned recipe, `getBlock`:

1. checks the local content-addressed cache;
2. otherwise GETs the known S3 key with bounded retries;
3. verifies length and SHA-256 before decoding or returning it;
4. populates the cache atomically.

`getSegment` initially streams a virtual `.jss`: header, synthesized length
prefixes, frames fetched with bounded parallel prefetch but emitted in order,
and footer. Backpressure bounds memory. The response has the same bytes,
content length, ETag semantics, and corruption checks as a materialized file.
Range and conditional requests must operate on this virtual byte address space;
a range may fetch only the intersecting immutable objects plus the necessary
length prefixes. Materializing the generation in cache is an optimization, not
a requirement for `http.ServeContent` compatibility.

Do not issue one serial cross-region GET per block for dense downloads. The
cache, prefetch window, and S3 connection pool are required parts of the first
implementation. An optional derived representation can later improve dense
reads:

- a fully materialized sealed `.jss` object per hot generation; or
- immutable pack/extents containing several adjacent frames plus an index.

These are caches. The canonical recipe remains per-block and every packed
entry maps back to its expected block hash. A missing or stale pack falls back
to canonical objects. Measure request cost and latency before adding this
complexity.

Cold subscribe replay reads the same immutable generation recipes. The hot
tail should be routed to the writer in the initial HA topology so it retains
today's sub-block latency. Read replicas can serve sealed history and may
follow committed active blocks with block-scale lag, but must not pretend they
have the writer's in-memory readable log.

### Catalog snapshots and database replicas

PostgreSQL `REPEATABLE READ` (or an equivalent explicitly tested query shape)
makes each plan call internally consistent. The existing checksum/ETag retry
contract handles generation changes between plan pages or later downloads;
`sealedTipSeq` prevents newly sealed tail data from moving the requested
sequence snapshot. A future additive API may carry an authenticated archive
ID + catalog commit/generation token to bind downloads directly to retained
generations, but this design does not require a wire change.

An asynchronous read replica may serve archive plans only with a measured lag
bound no larger than the accepted compaction/cache grace, and only while all
generations it can expose remain physically retained. Otherwise use the
primary. Writer leadership, cursor resolution, sequence allocation, lifecycle
changes, and compaction publication always use the authoritative writer
endpoint. Never combine a recipe from one database snapshot with block
descriptors from another.

### Failover and fencing

A candidate writer performs this startup sequence:

1. Open PostgreSQL and validate archive identity/schema/configuration.
2. Acquire the archive advisory lock on one dedicated session.
3. Reconcile and advance the S3 sequence ceiling as described below.
4. In a transaction, register any abandoned local/remote sequence space,
   increment `writer_epoch`, and record the new identity and ceiling token.
5. Rebuild active writer state, block descriptors, gaps, and the readable-log
   start from PostgreSQL. Verify the active tail's referenced objects before
   accepting input.
6. Start producers and only then advertise write/live readiness.

Loss of the lock session, failure to assert the epoch, or inability to commit
causes immediate write-readiness loss and producer shutdown. The process must
also stop publishing new in-memory live events; continuing to serve cold
immutable history is a separate readiness decision.

An old writer can have uploaded objects and can have exposed events within its
reserved seq lease, but cannot publish database references after fencing. Its
uploaded objects become garbage and its possibly observed seqs become an
explicit vacancy. New leadership does not scan S3 to infer what happened.

PostgreSQL HA is configured outside Jetstream, but Jetstream must test its
promises. An infrastructure failover that loses an acknowledged synchronous
transaction is database corruption from Jetstream's point of view and invokes
the remote sequence rollback protocol; it is not normal at-least-once replay.

## Sequence safety across PostgreSQL rollback

The existing `seq/max_reserved` and `seq/gap/*` protocol prevents reuse after
a process crash against intact local state. It cannot by itself survive
restoring PostgreSQL to an earlier point, because both the used frontier and
lease would rewind.

Maintain a small, independently conditional S3 object:

```json
{
  "version": 1,
  "archive_id": "...",
  "exclusive_ceiling": 123456789,
  "reservation_id": "...",
  "updated_at": "..."
}
```

The exclusive ceiling is monotonically increased with `If-Match` against the
last ETag. It is never part of PostgreSQL backup restoration and never expires.
The safe protocol is based on writer sessions, not on detecting rollback:

1. Before a new writer session exposes any event, it reads the remote ceiling
   `R`, conditionally replaces it with `R + grant`, and verifies the outcome.
2. The session starts allocation no lower than old `R`. If PostgreSQL's exact
   coverage frontier is below `R`, it atomically registers the entire interval
   `[frontier, R)` as a vacancy with reason `writer_failover` or
   `database_restore`.
3. It durably records the new exclusive ceiling and local one-block lease in
   PostgreSQL before publication. It may allocate within `[R, R+grant)`.
4. Before exhausting that grant, the same fenced session conditionally extends
   the remote ceiling and records the new grant locally. Failure stops intake
   before the boundary.
5. Every leadership acquisition or database reconnect burns the unused tail
   up to the previously published remote ceiling. It never resumes halfway
   through an old remote grant, even if PostgreSQL appears current.

Burning on every writer session is essential. A PITR snapshot can contain the
same remote grant end but an earlier `seq/next`; trying to infer “no rollback”
from equality would reuse seqs consumed later in that grant.

Choose grant size from peak event rate, acceptable S3-control outage, and
acceptable gap consumption. It should make control PUTs rare without burning
an alarming fraction of the `1e15` cursor namespace during failover tests.
Prefetch the next grant before it is needed. The steady block upload path does
not update the control object.

An ambiguous conditional PUT is reconciled by GET and matching
`reservation_id`. If another value won, abandon the attempted range and retry
from the observed ceiling. Never decrement or overwrite unconditionally.

The control object must live in a bucket/region that remains reachable during
writer promotion. If it is irretrievably lost, Jetstream cannot prove that the
old cursor namespace is safe. Recovery then requires restoring that authority
or deliberately creating a new archive identity with an explicit client-visible
discontinuity; guessing a high number is not a protocol.

## Garbage collection and retention

Content addressing makes leaks safe and premature deletion dangerous. GC is
therefore conservative, catalog-driven, and delayed.

An object is live if referenced by any of:

- an active segment block/header;
- a generation whose visibility interval is current or within plan/cache grace;
- an in-flight response, rewrite, migration, export, or repair pin;
- a database recovery point within the supported PITR window;
- an explicit legal/operational hold.

When the last ordinary reference disappears, set `unreferenced_at` and
`delete_after`; do not delete immediately. The minimum delay is:

```text
max PostgreSQL PITR window
+ maximum HTTP cache/client retry grace
+ cross-region replication or backup lag
+ operational/clock safety margin
```

This coupling is mandatory. Restoring PostgreSQL to yesterday while S3 has
already deleted objects referenced yesterday produces an internally consistent
database pointing at missing bytes. Jetstream startup should report the
configured PITR and object-retention assumptions and alert when they diverge.

Deletion protocol:

1. Select expired candidates in small batches.
2. In a transaction, lock each object row, recompute that no retained
   reference exists, and move it to `gc_claimed` with a unique claim.
3. Publishers encountering `gc_claimed` must not create a reference; they
   either wait or re-upload/verify and transactionally return it to
   `available` after the claim resolves.
4. Delete the known S3 key/version. With versioning, retain noncurrent versions
   according to the disaster-recovery policy.
5. Mark the row deleted. Retries reconcile HEAD/version state idempotently.

Never attach a blanket S3 lifecycle expiration rule to `objects/`. Lifecycle
rules may clean incomplete multipart uploads and, after a deliberately longer
period, old versions/delete markers. S3 Inventory compares physical objects
with catalog rows and finds unexpected orphans; discrepancies produce reports,
not automatic catalog reconstruction.

Old catalog generation rows can be pruned only after their object/PITR window
ends. Retaining their small recipes longer than their objects is useful for
auditing but must clearly mark the physical availability interval.

## Caching and local resource behavior

The disaggregated backend may use local SSD for:

- a size-bounded content-addressed block/header/footer cache;
- bounded temporary rewrite/import files;
- optional materialized `.jss` generation cache;
- multipart upload buffers when streaming is unavailable.

The cache is never authoritative. Entries are named by SHA-256, written to a
temporary file, verified, fsynced when reuse across process crash matters, and
atomically renamed. Eviction cannot affect correctness. Cache corruption is a
miss followed by re-fetch; repeated hash disagreement is an internal/provider
error and removes readiness.

Use request coalescing so concurrent misses for one hash cause one S3 GET.
Separate concurrency and bandwidth limits for foreground block reads,
background prefetch/materialization, compaction reads, and uploads prevent a
large rewrite from starving subscribe or XRPC. Expose each queue and throttle.

Disk-full behavior should shed cache/staging work and remain able to serve via
streaming where possible. It must not be confused with authoritative archive
loss. Local mode retains its existing crash-loud disk semantics.

## Database choice

### PostgreSQL (selected)

PostgreSQL matches the required shape:

- atomic transactions across opaque KV mutations, catalog rows, writer state,
  and generation publication;
- conditional updates, constraints, advisory locks, and fencing rows;
- ordered `bytea` keys and efficient point/range access;
- COPY/bulk operations for migration/import;
- mature managed Multi-AZ offerings, PITR, metrics, and operational knowledge;
- enough throughput for the measured hundreds of metadata reads per second
  and block-scale write transactions, subject to benchmark.

Use a direct PostgreSQL-compatible service with well-defined synchronous
durability and failover. RDS PostgreSQL is the conservative baseline. Aurora
PostgreSQL is plausible, but its failover, replica visibility, storage
semantics, and latency must pass the same chaos suite; “PostgreSQL compatible”
does not waive validation.

The implementation likely needs a dependency-whitelist exception for
`github.com/jackc/pgx/v5` and its narrow transitive set. The AWS SDK v2 S3 and
credential/config modules likewise need explicit approval. Avoid an ORM.

### Alternatives considered

| Database | Strength | Why it is not the initial choice |
| --- | --- | --- |
| DynamoDB | Managed, highly available, natural KV | Cross-item transactions have size/count limits; ordered scans, generation recipes, import publication, and local testing become more specialized. It also couples the design more tightly to AWS. |
| CockroachDB | Serializable distributed SQL and multi-region options | Higher operational/query complexity and a less conservative dependency/behavior surface than needed for one writer. Worth revisiting for multi-region requirements. |
| Google Cloud Spanner | Strong transactions and excellent HA | Vendor-specific, expensive at this scale, and a poor fit for AWS/S3-first operators. |
| FoundationDB | Excellent ordered transactional KV primitive | Operational specialization, C client/runtime dependency, and no benefit sufficient to outweigh PostgreSQL familiarity. |
| etcd/Consul | Strong small control-state stores | The archive catalog and migration/import workload are well beyond their intended database shape. |
| Redis-compatible stores | Low latency | Persistence/failover and multi-row archival correctness are not a comfortable authority for this data. |
| Cassandra/ScyllaDB | High write availability | Lacks the straightforward cross-entity transaction boundary this design relies on. |
| Object-store manifests only | Simple physical architecture | Requires conditional replacement of large mutable manifests, makes metadata queries/scans awkward, and cannot atomically bind cursor/verifier/lifecycle state to blocks. |
| Pebble on network/block storage | Minimal code change | Retains single-node filesystem semantics and slow/fragile failover; mounting S3 as a filesystem is especially unsafe. |

If PostgreSQL benchmarks fail, first reduce chatty reads with multi-get and
safe caches. Do not weaken the block/metadata transaction boundary to chase
latency. A different authoritative database must demonstrate the same atomic
and ordered semantics through the oracle and fault suite.

## Backend boundaries and code organization

Do not hide the difference behind `vfs.FS`. Local append/fsync/rename and
remote upload/transaction/generation-CAS are different protocols. Model the
operations Jetstream actually needs.

A likely internal shape is:

```go
type DurableWriter interface {
    CommitBlock(ctx context.Context, block PreparedBlock, meta MetadataBatch) (Commit, error)
    CommitMetadata(ctx context.Context, meta MetadataBatch) (Commit, error)
    Seal(ctx context.Context, candidate SealCandidate) (Generation, error)
}

type Catalog interface {
    Plan(ctx context.Context, req PlanRequest) (PlanSnapshot, error)
    Generation(ctx context.Context, id GenerationID) (Recipe, error)
    ActiveCoverage(ctx context.Context, namespace string) (Coverage, error)
}

type ObjectStore interface {
    Ensure(ctx context.Context, object Object) (ObjectInfo, error)
    OpenVerified(ctx context.Context, ref ObjectRef) (io.ReadCloser, error)
}
```

The real API should make it impossible to commit an event-dependent metadata
batch separately after a remote block. `ObjectStore` is lower-level and cannot
publish catalog references. Generation publication and rewrite CAS belong to
the database-backed catalog, not to general object methods.

Suggested packages:

```text
internal/storage/                 invariant-level backend contracts
internal/storage/local/           adapters around segment + Pebble + manifest
internal/storage/disaggregated/   PostgreSQL protocols and recipes
internal/objectstore/s3/          narrow S3 implementation, checksums, retries
```

The public `segment` package remains the one encoder/decoder. Refactor it to
operate on exact frames/headers/footers and recipes without teaching it about
S3 or SQL.

Expected touchpoints:

- `internal/ingest/writer.go` and `seqlease.go`: commit abstraction, remote
  grant guard, failover recovery, readable-log durability boundary.
- `internal/store`: a storage-neutral metadata batch contract plus local
  Pebble and PostgreSQL implementations; keep existing key encodings first.
- `internal/manifest`: local directory manifest remains; disaggregated catalog
  implements planning from immutable generations.
- `segment/writer.go`, `reader.go`, `seal.go`, `rewrite.go`, `patch.go`: expose
  exact prepared frames and deterministic header/footer assembly without
  requiring an appendable file.
- `internal/subscribe/replay.go`: recipe-backed cold reader and cache.
- `internal/xrpcapi/getsegment.go`, `getblock.go`, `plansnapshot.go`: generation
  tokens and virtual materialization.
- compaction/timestamp/orchestrator: source-generation pins and transactional
  CAS publication.
- `internal/jetstreamd`: backend selection, role/readiness, connection
  lifecycle, graceful fencing, and startup validation.
- status/diskspace/metrics: remote protection and cache capacity replace local
  authoritative-disk assumptions in disaggregated mode.

Backend selection is static for a process, for example
`JETSTREAM_STORAGE_BACKEND=local|disaggregated`. Disaggregated configuration
includes stable archive ID, PostgreSQL DSN/secret reference, S3 bucket/prefix,
region/endpoint, cache directory/size, and writer versus reader role. Validate
the entire set at startup. Never place credentials in the committed `.env`.

Schema migrations are explicit, versioned, forward-compatible, and normally
run by one operator job or the fenced writer before readiness. Reader binaries
must reject a schema newer than they understand.

## Availability and disaster recovery

### Normal component failures

| Failure | Behavior |
| --- | --- |
| S3 PUT unavailable | Retry within a bounded policy; block cannot commit. Intake applies backpressure and eventually becomes unready before sequence grants are violated. |
| PostgreSQL unavailable before transaction | Uploaded object may be orphaned; no durable block is claimed. Stop metadata-dependent intake. |
| Ambiguous PostgreSQL commit | Query by fence/segment/ordinal/idempotency identity before retrying. |
| S3 GET unavailable | Cached reads continue; misses return retryable errors/unready rather than fabricated gaps. Writes also stop when objects cannot be verified. |
| Object hash mismatch/missing referenced key | Internal corruption: quarantine cache entry, retry a bounded independent read, alert and remove affected readiness. Never skip the block. |
| Writer process/pod loss | New process fences, advances remote ceiling, registers abandoned seq space, verifies active tail, and resumes without archive download. |
| Cache disk loss | Performance degradation only. Recreate from catalog + S3. |
| PostgreSQL primary failover | Reconnect through new leadership protocol; never continue an old writer epoch. |

Backpressure must be bounded. Do not retain unbounded encoded blocks or events
in memory through a long S3/database outage. Stop upstream consumers at a safe
point and rely on relay replay within its retention window, while exposing the
safe cursor age as an urgent metric.

### Backup and regional DR

PostgreSQL PITR captures catalog and metadata. S3 versioning/replication or an
independent object backup protects objects. Their retention windows are
coordinated as described under GC; a database backup alone is not a complete
archive backup.

Regularly create a recovery attestation containing archive ID, PostgreSQL
backup/LSN range, catalog commit, object-retention lower bound, remote sequence
ceiling, and sampled/full recipe verification. This is an audit record, not a
second catalog.

Recovery procedure:

1. Fence or prove the old writer is gone.
2. Restore PostgreSQL to the chosen supported point.
3. Point at the retained/replicated immutable object set and audit all active
   plus sampled sealed recipes (full audit may run in background only if the
   operator explicitly accepts that availability tradeoff).
4. Contact the original monotonic sequence-control authority, advance it, and
   register the restoration vacancy.
5. Reconcile the saved safe relay cursor with actual upstream retention.
6. Catch up, verify invariants, then open write and live readiness.

If S3 replication lags the restored database, choose an older database point
whose objects are known present; never advance a relay cursor past missing
objects. If the original control authority is unreachable after a regional
loss, stay read-only or start a deliberately new archive/cursor namespace.

Run restoration drills. RPO/RTO remain benchmark- and infrastructure-gated;
the architecture removes the multi-terabyte local restore from ordinary
writer failover but not from every regional catastrophe.

## Migration and rollout

### Existing archive migration

Migration must preserve exact bytes and never create two authorities. A safe
steady-state path is:

1. Provision the PostgreSQL schema, bucket, archive identity, retention, cache,
   credentials, dashboards, and sequence control object.
2. While local Jetstream remains authoritative, scan immutable sealed `.jss`
   generations. Parse exact headers/frames/footers, hash and upload them, and
   stage catalog rows as not-yet-visible migration data. Verify reconstruction
   hashes. This bulk phase may be restarted and deduplicates naturally.
3. Continuously pre-stage newly sealed local segments. Do not call this a
   mirrored commit or use it for serving.
4. Enter a planned maintenance barrier: stop producers, drain the writer,
   finish active segment sealing or capture it through a defined conversion,
   stop compaction/import publication, and take an exact logical export of all
   Pebble metadata plus current segment generations.
5. Upload the remaining objects. In one migration publication transaction,
   import compatible KV state, validate sequence/relay/lifecycle/compaction
   frontiers against the recipes, publish all current generations, and create
   writer control state.
6. Initialize/advance the independent remote sequence ceiling beyond every
   possibly client-observed local seq and record the required vacancy/grant.
7. Start a disaggregated reader, run full coverage and sampled byte/event
   comparisons, then start the fenced writer and reopen traffic.
8. Retain the stopped local data directory read-only through a rollback safety
   period. It is evidence, not a concurrently writable replica.

Migrating during bootstrap, merge, or an active timestamp import requires a
phase-specific proof. The first migration tool should refuse those states and
require completion/quiescence.

Returning to local mode after disaggregated writes is an export operation with
an outage: materialize every current generation and export compatible metadata
at one catalog commit. Changing the environment variable alone is not rollback.

### Delivery stages

1. **Format seams:** exact frame/header/footer extraction and reconstruction,
   generation recipe validation, no behavioral change.
2. **Storage contracts:** adapt local mode through invariant-level interfaces;
   oracle output must remain identical.
3. **PostgreSQL metadata compatibility:** KV encoding/iteration suite, schema,
   migrations, fencing, and transaction fault tests.
4. **S3 object layer:** conditional creates, checksum verification, retries,
   cache, inventory audit, and provider conformance tests.
5. **Disaggregated ingest:** block commit, seal/rotation, seq ceiling, failover,
   and active cold replay.
6. **Read APIs:** checksum-consistent plans, pinned virtual segment streaming,
   reader replicas, and load tests.
7. **Rewriters:** compaction, timestamp import, bootstrap/merge, and GC.
8. **Shadow production:** local remains authoritative; asynchronously upload
   and compare recipes/reads without serving or publishing remote commits.
9. **Canary archive:** disaggregated authority on a disposable/rebuildable
   deployment, followed by failover and restore drills.
10. **Existing production migration:** only after correctness, performance,
    cost, and operational gates pass.

Feature flags must not permit local and disaggregated writers for the same
archive. Prefer one explicit backend choice over a long-lived dual-write mode;
cross-system dual writes cannot be made atomic and create an ambiguous owner.

## Observability and operations

Every metric is labeled by backend and archive identity where cardinality is
bounded. At minimum expose:

- block prepare/upload/verify/database-commit latency histograms and bytes;
- object dedup hit, upload retry, ambiguous result, checksum failure, and
  orphan/staging counts;
- PostgreSQL pool saturation, query/transaction latency, rollback/deadlock,
  primary/replica replay position, catalog commit, and schema version;
- writer leadership state, epoch, advisory-lock loss, fence rejection, and
  time since last successful durable block;
- exact seq frontier, local lease end/headroom, remote ceiling/headroom,
  reservation attempts, burned vacancies, and namespace remaining;
- safe relay cursor age/lag using a meaningful upstream timestamp, not numeric
  subtraction of opaque relay cursors;
- active segment block count/age, seal latency, generation publish conflicts,
  old generation/pin counts;
- cache hit bytes/requests, miss coalescing, eviction, corruption, disk usage,
  foreground/background queue depth, and S3 GET latency;
- virtual segment time-to-first-byte, throughput, per-response object count,
  and client cancellation;
- compaction/import blocks reused, changed, uploaded, bytes avoided, source
  conflicts, and publish transaction time;
- GC candidates by state/age, protected bytes by reason, deletes, inventory
  discrepancies, and minimum retained recovery timestamp;
- database PITR horizon versus object delete horizon as one explicit safety
  gauge;
- recovery audit age, last verified catalog commit, missing object count, and
  restore/failover drill results.

Trace one block from preparation through object ensure and SQL commit, but do
not create one span per archived event. Logs use bounded archive, generation,
segment, ordinal, hash prefix, epoch, and commit identifiers; never log DSNs,
credentials, event bodies, or unbounded provider responses.

Readiness is component-specific:

- writer readiness requires current fence, usable PostgreSQL primary, remote
  sequence headroom, and successful object writes/verifications;
- archive-read readiness requires catalog access plus S3 or sufficient cache;
- live readiness requires the writer's readable log and lifecycle cutover;
- a replica beyond the configured safe-lag/cache-grace bound is not ready for
  archive plans.

The status endpoint should make the backend and degraded modes obvious without
exposing secrets.

## Verification strategy

### Deterministic protocol tests

Build model/fake object and transaction layers with crashpoints before and
after every externally visible phase:

- object registration, PUT, timeout, verification, and availability mark;
- SQL begin, fence assertion, block row, each metadata class, commit send,
  ambiguous response, and commit acknowledgement;
- active-tail assertion, header/footer upload, generation insert, pointer CAS,
  old visibility close, watermark/job progress, and notification;
- advisory lock loss, PostgreSQL disconnect, S3 ceiling conditional update,
  ambiguous reservation, vacancy registration, and new epoch;
- GC mark, claim, last-reference recheck, physical delete, and row completion.

After every injected crash, reopening must produce exactly one of: old state
plus garbage, or fully committed new state. It must never produce a referenced
missing object, metadata ahead of data, duplicate ordinal, reused seq, advanced
watermark without generations, or two accepted writer epochs.

### Format/property/fuzz tests

- For arbitrary valid local segments, parse into a recipe and reconstruct
  byte-for-byte identical `.jss` files.
- Decode local and virtual readers and compare events, block descriptors,
  footer indexes, selection plans, and corruption errors.
- Fuzz malformed recipes, lengths, ordinals, hashes, headers, footers, plan
  tokens, cache files, and provider responses with strict allocation bounds.
- Property-test local Pebble and PostgreSQL KV ordering, prefix bounds, deletes,
  batches, and integer encodings over randomized byte keys.
- Prove generation visibility at catalog-commit boundaries under randomized
  compaction/import publication.

### Oracle and mutation campaign

Run the full oracle against both backends. Add seeded scenarios for:

- crash in every block/seal/publication stage;
- database failover and stale replica reads;
- S3 delay, throttling, lost responses, corrupt cache/provider bytes, and
  missing referenced objects;
- writer split-brain attempts and fencing;
- database PITR behind client-observed seqs;
- compaction/import racing plans and long-lived cold replay;
- bootstrap, merge, repair, and relay replay across failover;
- GC racing dedup/republication and PITR-window retention.

Curated mutants should remove/reorder upload verification, fence checks,
block+cursor atomicity, remote ceiling advance, vacancy registration,
generation CAS, visibility pinning, and GC last-reference checks. The campaign
must demonstrate that the relevant oracle tier kills each one.

### Real-service conformance and chaos

Fakes cannot establish cloud semantics. Test supported S3 providers and the
chosen managed PostgreSQL topology for:

- concurrent `If-None-Match`/`If-Match`, multipart completion, timeout
  ambiguity, checksum headers, versioning, and immediate known-key reads;
- primary crash during commit, synchronous replica promotion, DNS/connection
  pool behavior, advisory-lock release, and replay position;
- IAM/KMS denial and rotation, bucket throttling, network partitions, and
  availability-zone loss;
- PITR plus object-retention restoration and sequence-ceiling reconciliation.

Run periodic production restore drills into an isolated archive identity until
the audit completes; never let a drill contend for the real writer fence.

### Performance and cost gates

Measure, rather than assume:

- sustained and burst ingest with PUT + synchronous SQL commit;
- p50/p95/p99 block commit and live-delivery lag;
- PostgreSQL QPS/CPU/WAL/storage under backfill, steady ingest, compaction,
  import, status collection, and many readers;
- cold/warm `getBlock`, virtual `getSegment`, cold subscribe, and plan
  throughput with realistic concurrency and client cancellation;
- cache hit rate and size curve; S3 request count and egress;
- compaction changed-block bytes versus whole-file baseline and generation
  publication conflict rate;
- initial 6.4M-block migration time, SQL index size, upload/list/inventory cost,
  and verification throughput;
- writer and database failover time, burned seq range, relay catch-up, and
  connection storm behavior;
- GC scan/delete rate and database/object retention cost.

Baseline against the current segment benchmark (encode about 1GB/s, decode
about 4GB/s on the development host), SHA-256 throughput (about 2.2GB/s/core),
and local-mode oracle/performance results. Hashing should not be the bottleneck,
but virtual read fan-out or database round trips could be.

## Security

- Use workload identity/short-lived credentials, TLS, least-privilege IAM, and
  separate writer, reader, migration, and GC permissions.
- Readers need known-key GET, not LIST or DELETE. Only the sequence controller
  can conditionally replace the control object. Only GC can delete archive
  objects, and it does not get database writer credentials.
- Use S3 server-side encryption (KMS where required), PostgreSQL encryption at
  rest, encrypted backups, and credential rotation. KMS availability becomes
  part of readiness and must be tested.
- Bucket policy should deny public access, unencrypted transport, and
  unconditional overwrite of protected control data where expressible.
- Authenticate any future generation-bound download token and bind it to the
  archive/configuration so it cannot select another tenant's generation.
- Treat object bytes and database values as untrusted on read despite access
  controls: enforce size limits before allocation and verify hashes/formats.
- Record administrative generation deletion, migration, schema change,
  sequence reservation, and recovery actions in an audit trail.

## Rejected shortcuts

- **Periodic Pebble snapshots as primary durability:** leaves an RPO window,
  makes failover a restore workflow, and requires coordinating arbitrary
  segment/database points. Useful as backup, not the production commit path.
- **One S3 object per complete segment:** simple reads, but compaction reuploads
  terabytes per day and active append remains awkward.
- **Mutable object per active segment:** S3 has no atomic append. Repeated copy
  or multipart tricks create visibility and recovery protocols inferior to a
  database block list.
- **S3 LIST as manifest:** listings contain physical garbage and cannot
  atomically bind relay cursor, verifier state, or generations.
- **Store blocks directly in PostgreSQL:** transactions become simple but
  pushes multi-terabyte blob capacity, I/O, backup, and egress through the
  expensive database tier.
- **Database first, upload eventually:** creates acknowledged catalog state
  whose bytes can be permanently absent after a crash.
- **Dual-write local and remote as co-authorities:** no atomic commit spans
  Pebble/filesystem/PostgreSQL/S3, so a failure cannot determine which archive
  owns the relay cursor.
- **Resume inside a remote sequence grant after restart:** unsafe after PITR;
  the restored database can look consistent while forgetting later observed
  seqs in that same grant.
- **Delete objects as soon as the current generation changes:** breaks
  in-flight responses, caches, retries, readers, and PostgreSQL PITR.
- **Active-active writers with database sequences:** sequence uniqueness alone
  does not serialize relay cursor, per-DID order, lifecycle, verifier state, or
  active segment topology.

## Open, benchmark-gated decisions

The architecture is decided; these parameters and optimizations are not:

1. PostgreSQL service/topology (RDS PostgreSQL versus Aurora PostgreSQL), pool
   sizes, transaction isolation, and exact schema indexes/partitioning.
2. Remote sequence grant size and renewal threshold.
3. S3 multipart threshold, upload concurrency, retry budget, and supported
   compatible providers.
4. Cache size policy and whether active block frames should be synchronously
   retained locally after remote commit.
5. When dense-read request economics justify full-generation or extent packs.
6. Plan-token lifetime, old-generation grace, and the resulting minimum object
   retention beyond database PITR.
7. Whether footer/header bytes should also be mirrored inline in PostgreSQL
   for small-read latency; S3 objects remain canonical if so.
8. Catalog partitioning for tens or hundreds of millions of generation-block
   rows and how much descriptor data remains normalized versus encoded.
9. Migration maintenance-window target and whether a local change journal is
   worth building to shorten it.
10. Numerical RPO/RTO/SLOs for managed database failover, S3 outage headroom,
    relay retention, regional recovery, and full integrity audit.

None of these may weaken upload-before-reference, atomic block+metadata commit,
writer fencing, immutable generations, remote rollback protection, or
PITR-aware garbage collection.

## Implementation exit criteria

The backend is production-ready only when:

- local mode remains green and within its performance baseline;
- the same oracle expectations pass on disaggregated mode, including restart,
  failover, compaction, import, bootstrap/merge, and archive-to-live cutover;
- mutation tests prove the new ordering/fencing/GC invariants are observed;
- a supported real S3 service and PostgreSQL HA topology pass conformance and
  destructive failover tests;
- an existing production-sized archive migrates and reconstructs with exact
  recipe/file/event comparisons;
- database PITR is restored against retained objects and no seq is reused;
- S3, database, KMS, and cache outage behavior is bounded and observable;
- GC completes a full retention cycle without deleting anything reachable by
  the oldest supported database recovery point, HTTP cache, or active pin;
- dashboards, alerts, runbooks, capacity model, restore drill, credential
  rotation, and explicit operator configuration validation are complete;
- measured throughput, tail latency, request cost, and recovery objectives are
  accepted for the target deployment.

Until then, `local` remains the production reference and disaggregated mode is
experimental.

## References

- `docs/README.md` — authoritative Jetstream architecture and format contract.
- `specs/invariants.md` — correctness invariants this backend must preserve.
- `segment/doc.go` — `.jss` block/header/footer format behavior.
- `specs/notes/2026-08-25-seq-reuse-after-crash.md` — current local sequence
  lease and registered-vacancy protocol.
- [Amazon S3 data consistency model](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel)
- [Amazon S3 conditional requests](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html)
- [Amazon S3 checking object integrity](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html)
- [PostgreSQL synchronous commit](https://www.postgresql.org/docs/current/runtime-config-wal.html#GUC-SYNCHRONOUS-COMMIT)
- [PostgreSQL advisory locks](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS)
- [PostgreSQL continuous archiving and PITR](https://www.postgresql.org/docs/current/continuous-archiving.html)
