# Disaggregated primary storage: S3 blocks and a PostgreSQL catalog

Date: 2026-09-20. Status: accepted design direction; not implemented.

## Decision

Add an optional `disaggregated` storage mode for production deployments:

- S3 stores the large data: compressed segment blocks, headers, and footers.
  Each object is immutable and named by its SHA-256 hash.
- A highly available PostgreSQL service stores the small, mutable data: relay
  cursors, sequence state, repo state, and the catalog that says which blocks
  make up each segment.
- Local disk is only a bounded cache and temporary workspace. Losing it does
  not lose archive data.
- One writer is active at a time. Any number of readers can use PostgreSQL and
  S3.

Keep the existing `.jss` + Pebble implementation as the default `local`
mode. It remains the simplest way to deploy and test Jetstream. The new mode
does not mount S3 as a filesystem and does not use Pebble snapshots for normal
durability.

The central write rule mirrors today's “fsync the segment, then commit
Pebble” rule:

> First upload and verify the S3 object. Then, in one PostgreSQL transaction,
> add the block to the segment and advance all metadata that depends on that
> block. The block is durable only after the database transaction commits.

This ordering makes failures simple:

- An uploaded object that is not referenced by PostgreSQL is harmless garbage
  and can be deleted later.
- A PostgreSQL reference to a missing object is corruption and stops serving
  that data.
- Bucket listing is never used to decide what belongs in the archive.

This supersedes the primary direction of the earlier
[native S3 backup/restore exploration](https://github.com/bluesky-social/jetstream/blob/jc/s3-backups/specs/notes/2026-09-20-native-s3-backup-restore-plan.md).
That note is still useful for its format, restore, and sequence-reuse analysis.
The important change here is that S3 and PostgreSQL are the primary storage,
not a backup of local files and Pebble.

## The design in one minute

Jetstream's large state is already mostly immutable compressed blocks. The
expensive part of compaction is rewriting whole files when only a few blocks
changed. Storing each compressed block by hash means unchanged blocks are
shared by old and new versions of a segment.

S3 is good at storing immutable bytes, but it cannot append to a file or update
several pieces of metadata in one transaction. PostgreSQL fills that role. It
publishes a new block and advances the relay cursor, sequence state, and repo
state together.

The write path is:

```text
relay or PDS
    |
    v
encode one block -> upload block to S3 -> commit its reference in PostgreSQL
                           |                         |
                           v                         v
                    disposable cache       cursors, sequence state,
                                           repo state, segment catalog
```

The read path is:

```text
read segment recipe from PostgreSQL -> fetch blocks from cache or S3
                                    -> use the existing segment decoder
```

An active segment is an ordered list of block hashes in PostgreSQL. When it is
sealed, Jetstream publishes an immutable **generation**: the exact header,
ordered block list, and footer for that version of the segment. Compaction and
timestamp import create a new generation and reuse every unchanged block.

A **recipe** is the information needed to reconstruct one generation. It can
recreate the exact bytes of a sealed `.jss` file, so the segment format and
public archive APIs do not need a second event encoding.

A **fence** is a database-issued writer number. Every write must present the
current number. When a new writer takes over, it gets a higher number, so an
old writer can no longer commit even if it is still running.

## Goals

1. Keep all current correctness guarantees. In particular, never reuse a
   sequence number seen by a client, never advance metadata ahead of its data,
   keep per-DID ordering, and stop on unexplained archive holes.
2. Allow a production writer to recover on another machine without copying a
   multi-terabyte local archive or restoring Pebble.
3. Keep ingest, replay, compaction, and timestamp import fast at current scale.
4. Let readers scale independently from the single writer.
5. Reuse unchanged compressed blocks across segment rewrites.
6. Keep local mode simple and free of S3 and PostgreSQL dependencies.
7. Make partial failures leave either a complete database commit or harmless
   unreferenced objects.
8. Keep the current public protocol and exact `.jss` representation unless a
   later, versioned design changes them.

## Non-goals

- Active-active ingest. One writer is active; another may take over after it
  fences the old writer.
- Treating S3 like a local filesystem, implementing append with multipart
  uploads, or putting Pebble on an S3-backed `vfs.FS`.
- Eliminating all local storage. Bounded cache, temporary rewrite space, and
  multipart staging may use ephemeral SSD.
- Making S3 the transaction coordinator. It stores bytes and an independent
  sequence ceiling; PostgreSQL publishes archive state.
- Exactly-once delivery. Existing at-least-once behavior and inclusive cursors
  remain the contract.
- Serving mutable state from a lagging database replica without a clear lag
  limit.
- Transparent multi-region writes. Regional recovery needs access to the
  original sequence-control object or a new archive identity.
- Replacing PostgreSQL backups. Database point-in-time recovery (PITR) and S3
  object retention are both required; neither replaces the other.
- A general storage library. The interfaces should describe Jetstream's own
  operations and safety rules.

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
| Mean Pebble batch commit | 1.44ms | PostgreSQL commit latency must be measured against this baseline. |
| Seven-day whole-file compaction writes | 6.57TB/day | Uploading whole rewritten files would waste large amounts of bandwidth. |
| Mean compaction pass | 77.4 minutes | Publication cannot require pausing or serializing the full pass. |
| `getBlock` requests | 88,649; 1.21MB/s | A block cache and bounded concurrent S3 reads should handle the observed load. |
| Current archive | 1.82TiB, 7,651 sealed segments, 6.40M blocks, 25.7B events | Migration time and catalog size must be tested before rollout. |

The earlier backup investigation found that about 87% of rewritten files
changed at most ten blocks. With content-addressed storage, every unchanged
block keeps the same hash. It does not need to be uploaded or stored again.

These numbers are observations, not capacity limits. Before production
cutover, benchmark peak backfill, import, compaction, cache-cold replay, S3
throttling, PostgreSQL failover, and archives larger than pop1.

## Preserved and new invariants

All invariants in `specs/invariants.md` continue to apply. Their storage-specific
forms are:

1. **Published generations are immutable.** A generation's header, ordered
   block references, and footer never change. Rewrites publish another
   generation and atomically change a pointer.
2. **Upload before commit.** Every S3 object must be readable and verified
   before PostgreSQL may reference it.
3. **PostgreSQL is authoritative.** Readers discover segments, blocks,
   generations, vacancies, and current pointers through the catalog. Bucket
   listing can audit or find garbage but cannot repair catalog state.
4. **Hashes name exact bytes.** The SHA-256 is over the stored bytes, not
   decoded events, an ETag, or a logical block. Reads verify length and hash
   before decoded data becomes observable.
5. **Only the current writer can commit.** Every write includes the current
   fence number. Losing the leadership database connection invalidates that
   number.
6. **A block and its metadata commit together.** The block reference, exact
   `seq/next`, sequence lease, safe `relay/cursor`, and all eligible repo,
   verifier, sync, and lifecycle mutations commit together.
7. **Client-observable seqs are never reused.** The existing one-block local
   lease protects process crashes. An independent S3 ceiling protects the same
   cursor namespace across database rollback or loss.
8. **Unexplained gaps stop the process.** Recipes and explicit
   `seq/gap/*` vacancies explain all durable coverage. Missing objects, missing
   block numbers, or unexplained sequence holes are corruption, not an invitation
   to skip forward.
9. **Published segment order is stable.** Logical segment indices retain the
   same creation/time order as lexically sorted local filenames. Generation
   changes never reorder logical segments.
10. **Bad upstream input is dropped; bad owned state stops the process.** An
    invalid relay record remains an input error. A hash mismatch, impossible
    recipe, lost fence, or referenced missing object is internal corruption.

These guarantees also depend on the managed services. PostgreSQL must use
synchronous commit and must not promote a replica that is missing acknowledged
transactions. S3 must provide atomic whole-object writes, immediate reads of
successful writes, and working conditional updates. Jetstream refuses to
start when it can detect weaker settings.

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

The hash covers the bytes exactly as stored. The object table records the
length, hash, creation time, and optional S3 checksum/version. Each recipe says
whether it expects a block, header, or footer. Object type is not part of the
key, so identical bytes can safely be reused in more than one role.

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

### Required S3 behavior

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

Enable bucket versioning. Object Lock or governance retention is recommended
for the sequence-control object and as protection against accidental deletion.
Jetstream must still manage garbage collection itself. Cross-region S3
replication is asynchronous; it cannot coordinate writers in two regions.

## PostgreSQL data model

The database has four main groups of tables:

1. One archive row records writer ownership and global control state.
2. A key/value table initially preserves the existing Pebble metadata format.
3. An object table tracks uploads and garbage collection.
4. Segment tables describe active segments and immutable generations.

The proposed columns follow. Names, widths, and indexes may change after
benchmarks. The identities and transaction boundaries should not.

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

`catalog_commit` is a revision number increased by every catalog publication.
`writer_epoch` is the writer's fence number. The remote ceiling columns record
the last S3 sequence-control value seen by this writer; they may never reduce
the value in S3.

To become writer, a process takes a PostgreSQL advisory lock for `archive_id`.
It then checks the S3 sequence ceiling, increases `writer_epoch`, and records
its process identity. Every later write checks that epoch. If the database
connection drops, the advisory lock is released. Reconnecting requires taking
leadership again and getting a new epoch.

The advisory lock prevents two healthy writers. The epoch prevents an old
writer from committing after failover. This does not use a clock or timeout,
so clock skew cannot create two valid writers.

The lock protects one PostgreSQL cluster. A restored copy of that cluster has
its own locks. Before promoting a restored copy, the operator must revoke the
old writer's access or otherwise prove it has stopped. The S3 sequence ceiling
prevents sequence reuse, but it cannot merge two databases that were allowed
to diverge. Automatic election across separate database clusters is outside
this design.

### Existing metadata keys and values

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

The first implementation stores current Pebble keys and values unchanged. This
reduces migration risk for `relay/cursor`, `seq/*`, `repo/*`,
`pdshost/*`, `sync/*`, lifecycle, retry, import, and compaction state. The SQL
store supports atomic put/delete, ordered prefix/range scans, compare-and-set,
and multi-get. PostgreSQL `bytea` ordering must be covered by compatibility
tests against Pebble iteration.

Individual key families can move to normal SQL columns later when that makes
queries or validation better. That conversion is not required for the first
release. Segment catalog rows use normal columns from the start because they
are new and benefit from database constraints.

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

Before uploading, create an `uploading` row. After the PUT has been verified,
change it to `available`. Recipes may reference only available objects. If a
process dies during upload, another process can inspect or clean up the stale
row without listing the whole bucket. S3 Inventory is still useful for audits.

The object row describes bytes, not their use. If two roles happen to have the
same bytes, the same hash can satisfy both references.

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
  ordinal integer,              -- zero-based block number
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
  ordinal integer,              -- zero-based block number
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

References must point to available object rows. A foreign key plus a
transactional check or trigger enforces that rule. Block numbers start at zero
and have no gaps. Constraints reject invalid time/sequence ranges, duplicate
active block numbers, and sealed segments without a current generation.

`visible_from_commit`/`visible_until_commit` preserve catalog history for
auditing and database recovery. Old generation rows and objects remain
available for the existing HTTP cache grace and recovery window after
`segments.current_generation_id` changes.

Import jobs, temporary references held by reads or rewrites, and
garbage-collection claims will probably need their own tables. Do not build a
second mutable manifest that can disagree with this catalog.

## Core protocols

### New block commit

One block is committed as follows:

1. Prepare and compress a complete block in ingest order. Calculate its
   SHA-256 and its planning metadata, such as event count and sequence/time
   ranges.
2. Before any event is visible in the live readable log, confirm that its
   sequence number is inside both the local and remote reserved ranges.
3. Create or claim the object row. If S3 does not already have this hash,
   upload it. Verify the known key with HEAD or GET.
4. Start a PostgreSQL transaction. Check the writer fence and lock the active
   segment tail.
5. Insert the next block number. In the same transaction, update `seq/next`,
   `seq/max_reserved`, the safe relay cursor, durable repo/version checks,
   verifier and sync/account state, and related lifecycle progress.
6. Increase `catalog_commit` and commit with synchronous commit enabled.
7. Report the block as durable. Cache writes and reader notifications happen
   afterward because they can safely be lost.

If upload fails, PostgreSQL does not change. If the process dies after upload
but before database commit, it leaves an unreferenced object. If the process
does not know whether the database commit succeeded, it reconnects and checks
the expected segment/block row before doing anything else.

The unique segment and block number, previous tail, hash, and writer fence make
retries safe. Repeating a successful commit must find the same result. Finding
different bytes or state is corruption.

The current live readable log may still expose a prepared event before this
remote commit. The existing durable sequence lease bounds that window. An
unclean failover registers the abandoned lease tail as a vacancy, so those seqs
are not reassigned.

### Metadata-only transactions

Some current Pebble batches do not accompany a block. They may commit directly
to PostgreSQL only when they do not depend on uncommitted events. Callers must
say which class a change belongs to; an empty block is not proof that a change
is independent. Existing dependency rules for verifier, cursor, merge,
compaction, and import state still apply.

Combine small updates into multi-row SQL instead of making hundreds of network
round trips. Large imports should COPY into a staging table and then merge the
staged rows in one checked transaction.

### Sealing and rotation

Sealing converts the active database list into an immutable generation:

1. Read the active segment's committed block list at one catalog revision and
   keep that exact list for the rest of the operation.
2. Generate the exact final header/footer and recipe using the existing
   segment format code. Upload and verify the header and footer objects.
3. In one transaction, check the writer fence and confirm the active tail did
   not change. Insert the generation and ordered block rows, make it current,
   mark the segment sealed, and create the next active segment.
4. Commit, then publish cache/reader notifications.

No S3 rename or copy is involved. A crash before the transaction leaves
unreferenced header/footer objects and the old active list. A crash after it
leaves one complete sealed generation and one new active segment.

This transaction may copy hundreds or a few thousand block rows, but segment
rotation happens only about 17 times per day in the measured deployment. If
the transaction is too expensive, an immutable block-list object can be tested
later. Do not trade away validation merely to predict an optimization.

### Compaction

Compaction never edits a published generation:

1. Capture the source generation IDs and compaction input watermark in a
   short database snapshot. They remain immutable.
2. Outside a database transaction, fetch/decode only needed blocks, apply
   tombstones, and build replacement frames and the exact new footer. Reuse
   old hashes for byte-identical unchanged frames; do not recompress them.
3. Upload and verify every new block/header/footer object.
4. In one transaction, check the writer fence and verify that each current
   generation is still the source read in step 1. Insert the new generations,
   close the old generations' visibility ranges, change the current pointers,
   and advance the compaction watermark.
5. If a source pointer changed, discard the candidate or rebuild it from the
   new source. Never publish a recipe built from an outdated generation.

A chunk that updates several segments and one watermark publishes all of them
in one transaction. Keep chunks small enough to limit lock time and PostgreSQL
write-ahead log (WAL) growth. Decoding, rewriting, and uploading happen outside
database locks.

An in-flight response keeps using the exact older recipe it started with; HTTP
caches may retain it through the configured grace period. A later name lookup
finds the new generation and its new ETag. Compaction no longer needs a
filesystem rename or manifest rescan.

### Timestamp import and other rewrites

Timestamp import uses the same “verify source, then replace pointer” protocol
as compaction. Job progress, rule state, and generation changes commit together
at each job boundary. Import state cannot say a segment was patched unless the
new generation is visible.

Long jobs remember exact generation IDs, not filenames. If one is replaced,
the job retries from the new generation. Operators must retain the original
import input until the job finishes; remote segment storage cannot recreate a
partially consumed CSV.

The `namespace` column distinguishes the main archive from bootstrap live data
and merge data. A lifecycle transition updates its control metadata in the
same transaction and retains every source generation needed after restart. A
first release may migrate only steady-state archives, but it must reject other
states instead of silently omitting their data.

### Archive planning and reads

The initial implementation preserves the current public contract. Within one
`planSnapshot` request, a database transaction selects one `catalog_commit`
and resolves a self-consistent set of current generations. Each returned
segment already includes its segment-format checksum. Existing pagination
continues to hold `sealedTipSeq` fixed with `beforeSeq`; it does not introduce
a new token.

Today `getSegment` and `getBlock` accept only the stable segment name (and a
block number), not a generation ID. They therefore resolve the current
generation at the start of the request and keep that exact recipe for the
life of the response. Their ETags retain current behavior: the exact segment
format checksum for `getSegment`, and that checksum plus block number for
`getBlock`. A client whose planned checksum no longer matches the response
has raced compaction/import and re-plans, just as it does when a local file was
atomically replaced. Never splice references from two generations in one
response.

After choosing that recipe, `getBlock`:

1. checks the local content-addressed cache;
2. otherwise GETs the known S3 key with bounded retries;
3. verifies length and SHA-256 before decoding or returning it;
4. populates the cache atomically.

`getSegment` initially streams a `.jss` without first writing the whole file to
disk. It sends the header, generated length prefixes, blocks, and footer in
order. Blocks can be fetched in parallel, but memory stays bounded and output
remains ordered. Content length, ETag, corruption checks, Range, and conditional
requests keep their current behavior. A Range request fetches only the objects
that overlap the requested bytes. Caching a complete `.jss` is optional.

Do not issue one S3 GET at a time for a full-segment download. The
cache, prefetch window, and S3 connection pool are required parts of the first
implementation. An optional derived representation can later improve dense
reads:

- a fully materialized sealed `.jss` object per hot generation; or
- immutable packs containing several adjacent frames plus an index.

These are only caches. The per-block recipe remains the source of truth, and
every packed entry keeps its expected block hash. If a pack is missing or
stale, read the individual objects. Add packing only if measurements justify
the extra code.

Cold subscribe replay uses the same generation recipes. Initially, route the
hot tail to the writer so clients keep today's sub-block latency. Other readers
can serve sealed history and committed active blocks, but they do not have the
writer's in-memory readable log.

### Consistent plans and database replicas

PostgreSQL `REPEATABLE READ`, or an equivalent tested query, makes each plan
call internally consistent. The existing checksum/ETag retry
rule handles generation changes between plan pages or later downloads;
`sealedTipSeq` prevents newly sealed tail data from moving the requested
sequence snapshot. No wire change is required.

A read replica may serve archive plans only when its measured lag is within
the accepted cache grace and all generations it can return are still stored.
Otherwise, use the primary. Leadership, cursor resolution, sequence
allocation, lifecycle changes, and compaction publication always use the
primary. A response must get its recipe and block descriptions from the same
database snapshot.

### Failover and fencing

A candidate writer performs this startup sequence:

1. Open PostgreSQL and validate archive identity/schema/configuration.
2. Acquire the archive advisory lock on one dedicated session.
3. Read and advance the S3 sequence ceiling as described below.
4. In a transaction, register any abandoned local/remote sequence space,
   increment the `writer_epoch` fence, and record the new identity, ceiling
   ETag, and reservation ID.
5. Rebuild active writer state, block descriptors, gaps, and the readable-log
   start from PostgreSQL. Verify the active tail's referenced objects before
   accepting input.
6. Start producers and only then advertise write/live readiness.

If the lock connection is lost, a fence check fails, or commits stop working,
the process becomes write-unready and stops its producers. It must also stop
publishing new in-memory live events. It may continue serving immutable
history if that separate read path is healthy.

An old writer can have uploaded objects and can have exposed events within its
reserved sequence lease, but cannot publish database references after a new
writer takes over. Its uploaded objects become garbage and its possibly
observed sequence numbers become an
explicit vacancy. New leadership does not scan S3 to infer what happened.

The operator configures PostgreSQL high availability, but Jetstream must test
the result. If
database failover loses an acknowledged synchronous transaction, Jetstream
treats it as rollback and uses the remote sequence recovery protocol below.

## Sequence safety across PostgreSQL rollback

The existing `seq/max_reserved` and `seq/gap/*` records prevent reuse after a
normal process crash. They are stored in the database, so restoring an older
database would also restore an older sequence reservation. A separate value in
S3 prevents reuse in that case.

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

The ceiling is the first sequence number not reserved for a writer. It
only increases. Jetstream updates it with `If-Match` against the last ETag. It
is not stored in PostgreSQL backups and never expires.

Let `R` be the ceiling read from S3 and `G` be the configured range size:

1. Before a new writer session exposes any event, it reads the remote ceiling
   `R`, conditionally replaces it with `R + G`, and verifies the outcome.
2. The session starts allocation no lower than old `R`. If PostgreSQL's next
   durable sequence is below `R`, it atomically registers the entire interval
   `[next_seq, R)` as a vacancy with reason `writer_failover` or
   `database_restore`.
3. It records the new ceiling and local one-block lease in PostgreSQL before
   publishing events. It may allocate within `[R, R+G)`.
4. Before exhausting that range, the same writer conditionally extends the
   remote ceiling and records the new range locally. Failure stops intake
   before the boundary.
5. Every leadership acquisition or database reconnect burns the unused tail
   up to the previously published remote ceiling. It never resumes halfway
   through an old remote grant, even if PostgreSQL appears current.

Every new writer must discard the unused part of the previous writer's range.
An old database backup may remember the correct range end but forget some
sequence numbers already used inside the range. Reusing that range would give
those sequence numbers to different events.

For example, suppose S3 says all numbers below 1,000,000 were reserved, while
the restored database says the next number is 990,000. The new writer records
`[990000, 1000000)` as an intentional gap. It then raises the S3 ceiling and
starts at 1,000,000. This can create duplicates after relay replay, which the
protocol already allows, but it cannot reuse an observed sequence number.

Choose the range size from peak event rate, desired operation during an S3
control outage, and the acceptable gap after failover. It should avoid frequent
control writes without wasting a large part of the `1e15` sequence space.
Reserve the next range before it is needed. The steady block upload path does
not update the control object.

If a PUT times out and its result is unknown, GET the control object and compare
`reservation_id`. If another writer's value won, abandon the attempted range
and retry from the new ceiling. Never decrease the ceiling or overwrite it
without a condition.

The control object must remain reachable during writer promotion. If it is
lost, Jetstream cannot prove that old sequence numbers are safe. Recovery must
restore the object or create a new archive identity and explicitly tell clients
that the cursor namespace changed. Choosing an arbitrary high number is unsafe.

## Garbage collection and retention

Leaking an object wastes money but loses no data. Deleting one too early can
make the archive unreadable. Garbage collection (GC) is therefore conservative
and delayed.

An object is live if referenced by any of:

- an active segment block/header;
- a generation that is current or still within the HTTP cache grace period;
- an in-flight response or active rewrite, migration, export, or repair job;
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
5. Mark the row deleted. A retry checks HEAD/version state and safely continues
   from the last completed step.

Never attach a blanket S3 lifecycle expiration rule to `objects/`. Lifecycle
rules may clean incomplete multipart uploads and, after a deliberately longer
period, old versions/delete markers. S3 Inventory compares physical objects
with catalog rows and finds unexpected orphans; discrepancies produce reports,
not automatic catalog reconstruction.

Old generation rows can be removed only after the object-retention and PITR
window ends. Recipes may be kept longer for audit, but they must clearly show
when their objects are no longer available.

## Caching and local resource behavior

The disaggregated backend may use local SSD for:

- a size-bounded content-addressed block/header/footer cache;
- bounded temporary rewrite/import files;
- optional materialized `.jss` generation cache;
- multipart upload buffers when streaming is unavailable.

The cache is never a source of truth. Cache files are named by SHA-256. Write
them to temporary files, verify them, fsync them when they should survive a
process crash, and then rename them atomically. Eviction cannot lose archive
data. A corrupt cache entry is deleted and fetched again. Repeated hash
mismatches make the service unready and trigger an alert.

Use request coalescing so concurrent misses for one hash cause one S3 GET.
Separate concurrency and bandwidth limits for foreground block reads,
background prefetch/materialization, compaction reads, and uploads prevent a
large rewrite from starving subscribe or XRPC. Expose each queue and throttle.

Disk-full behavior should shed cache/staging work and remain able to serve via
streaming where possible. It must not be confused with authoritative archive
loss. Local mode retains its existing crash-loud disk semantics.

## Database choice

### PostgreSQL (selected)

PostgreSQL provides the features this design needs:

- atomic transactions across existing key/value metadata, catalog rows,
  writer state, and generation publication;
- conditional updates, constraints, advisory locks, and fencing rows;
- ordered `bytea` keys and efficient point/range access;
- COPY/bulk operations for migration/import;
- mature managed Multi-AZ offerings, PITR, metrics, and operational knowledge;
- enough throughput for the measured hundreds of metadata reads per second
  and block-scale write transactions, subject to benchmark.

Use a service with clearly documented synchronous durability and failover. RDS
PostgreSQL is the conservative baseline. Aurora PostgreSQL may also work, but
its failover, replica lag, storage behavior, and latency must pass the same
failure tests. API compatibility alone is not enough.

The implementation likely needs a dependency-whitelist exception for
`github.com/jackc/pgx/v5` and its narrow transitive set. The AWS SDK v2 S3 and
credential/config modules likewise need explicit approval. Avoid an ORM.

### Alternatives considered

| Database | Strength | Why it is not the initial choice |
| --- | --- | --- |
| DynamoDB | Managed, highly available, natural key/value model | Cross-item transactions have size/count limits; ordered scans, generation recipes, import publication, and local testing become more specialized. It also couples the design more tightly to AWS. |
| CockroachDB | Serializable distributed SQL and multi-region options | More operational and query complexity than one writer needs. Revisit it if multi-region writes become a requirement. |
| Google Cloud Spanner | Strong transactions and excellent HA | Vendor-specific, expensive at this scale, and a poor fit for AWS/S3-first operators. |
| FoundationDB | Excellent ordered transactional key/value store | Operational specialization, a C client/runtime dependency, and no benefit sufficient to outweigh PostgreSQL familiarity. |
| etcd/Consul | Strong small control-state stores | The archive catalog and migration/import workload are much larger and more complex than their intended use. |
| Redis-compatible stores | Low latency | Their persistence, failover, and multi-row transaction model is a poor fit for archive authority. |
| Cassandra/ScyllaDB | High write availability | Lacks the straightforward cross-entity transaction boundary this design relies on. |
| Object-store manifests only | Simple physical architecture | Requires conditional replacement of large mutable manifests, makes metadata queries/scans awkward, and cannot atomically bind cursor/verifier/lifecycle state to blocks. |
| Pebble on network/block storage | Minimal code change | Retains single-node filesystem semantics and slow/fragile failover; mounting S3 as a filesystem is especially unsafe. |

If PostgreSQL is too slow, first combine reads and add safe caches. Do not split
the block and metadata transaction to save latency. Any replacement database
must pass the same ordering, oracle, and failure tests.

## Backend boundaries and code organization

Do not hide the difference behind `vfs.FS`. Local append/fsync/rename and
remote upload/database/generation replacement are different protocols. Model
the operations Jetstream actually needs.

The internal interfaces may look like this:

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

The real API must prevent event-dependent metadata from being committed
separately from its block. `ObjectStore` can upload and read bytes, but it
cannot publish them in the catalog. The database-backed catalog owns generation
publication and checked pointer replacement.

Suggested packages:

```text
internal/storage/                 backend contracts that enforce storage rules
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
- `internal/xrpcapi/getsegment.go`, `getblock.go`, `plansnapshot.go`: checksums,
  generation-consistent reads, and streaming `.jss` reconstruction.
- compaction/timestamp/orchestrator: exact source-generation references and
  transactional pointer replacement.
- `internal/jetstreamd`: backend selection, role/readiness, connection
  lifecycle, graceful fencing, and startup validation.
- status/diskspace/metrics: remote protection and cache capacity replace local
  authoritative-disk assumptions in disaggregated mode.

Backend selection is static for a process, for example
`JETSTREAM_STORAGE_BACKEND=local|disaggregated`. Disaggregated configuration
includes stable archive ID, PostgreSQL DSN/secret reference, S3 bucket/prefix,
region/endpoint, cache directory/size, and writer versus reader role. Validate
the entire set at startup. Never place credentials in the committed `.env`.

Database schema migrations are explicit and versioned. One operator job or the
current writer runs them before the service becomes ready. A reader must reject
a schema version newer than it understands.

## Availability and disaster recovery

### Normal component failures

| Failure | Behavior |
| --- | --- |
| S3 PUT unavailable | Retry for a limited time. The block cannot commit. Slow and then stop intake before reserved sequence space runs out. |
| PostgreSQL unavailable before transaction | Uploaded object may be orphaned; no durable block is claimed. Stop metadata-dependent intake. |
| PostgreSQL commit result is unknown | Look up the writer fence, segment, and block number before retrying. |
| S3 GET unavailable | Cached reads continue. Cache misses return a retryable error instead of skipping data. Writes stop when newly uploaded objects cannot be verified. |
| Object hash mismatch or referenced object missing | Delete any cache copy and retry the known S3 key a limited number of times. Alert and become unready. Never skip the block. |
| Writer process/pod loss | A new process takes leadership, advances the remote ceiling, records abandoned sequence space, verifies the active tail, and resumes without downloading the archive. |
| Cache disk loss | Performance degradation only. Recreate from catalog + S3. |
| PostgreSQL primary failover | Reconnect through the leadership protocol; never continue using the old fence number. |

Memory use must stay bounded during an outage. Do not keep accumulating encoded
blocks or events while S3 or PostgreSQL is unavailable. Stop upstream consumers
at a safe point and rely on relay replay, while prominently reporting the age
of the saved relay cursor.

### Backup and regional DR

PostgreSQL PITR captures the catalog and metadata. S3 versioning, replication,
or an independent object backup protects objects. Their retention windows are
coordinated as described under garbage collection. A database backup alone is
not a complete archive backup.

Regularly create a recovery report containing the archive ID, PostgreSQL backup
and log position, catalog revision, oldest retained object, remote sequence
ceiling, and recipe verification results. This is an audit record, not another
catalog.

Recovery procedure:

1. Fence or prove the old writer is gone.
2. Restore PostgreSQL to the chosen supported point.
3. Use the retained or replicated S3 objects. Verify every active recipe and a
   sample of sealed recipes. A full check may run in the background only if the
   operator accepts the risk of opening sooner.
4. Read and advance the original sequence-control object, then record the gap
   created by the restore.
5. Confirm that upstream still retains the saved relay cursor needed for
   catch-up.
6. Catch up, verify invariants, then open write and live readiness.

If S3 replication lags the restored database, choose an older database point
whose objects are known present; never advance a relay cursor past missing
objects. If the original control authority is unreachable after a regional
loss, stay read-only or start a deliberately new archive/cursor namespace.

Run restoration drills. Recovery point and recovery time goals depend on the
chosen database and S3 setup. Ordinary writer failover no longer requires a
multi-terabyte download, but regional recovery may still take substantial
time.

## Migration and rollout

### Existing archive migration

Migration must preserve exact bytes and must never run two active writers. A
safe path for an archive already in steady state is:

1. Provision the PostgreSQL schema, bucket, archive identity, retention, cache,
   credentials, dashboards, and sequence control object.
2. While local Jetstream remains authoritative, scan immutable sealed `.jss`
   generations. Parse exact headers/frames/footers, hash and upload them, and
   stage catalog rows without making them visible. Reconstruct files and verify
   their hashes. This bulk phase is restartable and automatically reuses
   objects already uploaded.
3. Continue staging newly sealed local segments. They are not committed remote
   archive data and must not be served yet.
4. Start a planned maintenance window: stop producers, drain the writer,
   finish active segment sealing or capture it through a defined conversion,
   stop compaction/import publication, and take an exact logical export of all
   Pebble metadata plus current segment generations.
5. Upload the remaining objects. In one migration publication transaction,
   import compatible key/value state, compare sequence, relay, lifecycle, and
   compaction positions with the recipes, publish all current generations,
   and create writer control state.
6. Move the remote sequence ceiling past every sequence number that a client
   might have seen. Record the resulting gap and the new reserved range.
7. Start a disaggregated reader, run full coverage and sampled byte/event
   comparisons, then start the fenced writer and reopen traffic.
8. Retain the stopped local data directory read-only through a rollback safety
   period. It is evidence, not a concurrently writable replica.

The first migration tool should refuse to run during bootstrap, merge, or an
active timestamp import. Supporting those states later requires a separate
correctness design.

Returning to local mode after disaggregated writes is an export operation with
an outage: materialize every current generation and export compatible metadata
at one catalog commit. Changing the environment variable alone is not rollback.

### Delivery stages

1. **Format support:** extract and reconstruct exact frames, headers, and
   footers; validate recipes; make no behavior change.
2. **Storage contracts:** adapt local mode through invariant-level interfaces;
   oracle output must remain identical.
3. **PostgreSQL metadata compatibility:** key/value encoding and iteration
   tests, schema, migrations, fencing, and transaction failure tests.
4. **S3 object layer:** conditional creates, checksum verification, retries,
   cache, inventory audit, and real-provider behavior tests.
5. **Disaggregated ingest:** block commit, seal/rotation, seq ceiling, failover,
   and active cold replay.
6. **Read APIs:** checksum-consistent plans, generation-consistent segment
   streaming, reader replicas, and load tests.
7. **Rewriters:** compaction, timestamp import, bootstrap/merge, and GC.
8. **Shadow production:** local remains authoritative; asynchronously upload
   and compare recipes/reads without serving or publishing remote commits.
9. **Canary archive:** disaggregated authority on a disposable/rebuildable
   deployment, followed by failover and restore drills.
10. **Existing production migration:** only after correctness, performance,
    cost, and operational requirements pass.

Feature flags must not permit local and disaggregated writers for the same
archive. Prefer one explicit backend choice over a long-lived dual-write mode;
cross-system dual writes cannot be made atomic and create an ambiguous owner.

## Observability and operations

Keep metric labels bounded. At minimum, dashboards should answer these
questions:

- **Are writes healthy?** Show block preparation, upload, verification, and
  database commit latency and bytes. Also show upload retries, unknown results,
  checksum failures, deduplication hits, and unreferenced uploads.
- **Does the current writer still own the archive?** Show writer identity,
  fence number, lock loss, rejected stale writes, and time since the last
  durable block.
- **Can sequence allocation continue safely?** Show `seq/next`, the local lease
  end, remaining remote range, reservation attempts, discarded ranges, and
  remaining sequence namespace.
- **Can upstream replay recover an outage?** Show the age of the safe relay
  cursor using an upstream timestamp. Relay cursor numbers are opaque and must
  not be subtracted to estimate lag.
- **Is PostgreSQL healthy?** Show pool use, query and transaction latency,
  rollbacks, deadlocks, primary/replica lag, catalog revision, and schema
  version.
- **Are reads healthy?** Show cache hit rate and bytes, coalesced misses,
  eviction, corruption, disk use, S3 GET latency, queue depths, segment
  time-to-first-byte, throughput, objects per response, and cancellations.
- **Are rewrites efficient?** Show reused, changed, and uploaded blocks; bytes
  avoided; source-generation conflicts; sealing time; and publication time.
- **Is deletion safe?** Show GC objects by state and age, protected bytes by
  reason, inventory differences, the oldest recoverable timestamp, and one
  alertable comparison between the database PITR window and S3 deletion time.
- **Can we recover?** Show the age and catalog revision of the last recovery
  audit, missing objects, and results from failover and restore drills.

Trace one block from preparation through S3 verification and database commit.
Do not create one trace span per event. Logs may include bounded archive,
generation, segment, block number, hash prefix, fence, and commit identifiers.
Never log database connection strings, credentials, event bodies, or large S3
responses.

Readiness is component-specific:

- writer readiness requires the current fence, a usable PostgreSQL primary,
  remaining remote sequence range, and successful S3 writes and checks;
- archive-read readiness requires catalog access plus S3 or sufficient cache;
- live readiness requires the writer's readable log and lifecycle cutover;
- a replica beyond the configured safe-lag/cache-grace bound is not ready for
  archive plans.

The status endpoint should make the backend and degraded modes obvious without
exposing secrets.

## Verification strategy

### Crash tests for each protocol step

Build fake S3 and database implementations. Add crashpoints before and after
every step that changes persistent state:

- object registration, PUT, timeout, verification, and availability mark;
- SQL begin, fence assertion, block row, each metadata class, commit send,
  ambiguous response, and commit acknowledgement;
- active-tail check, header/footer upload, generation insert, pointer change,
  old visibility close, watermark/job progress, and notification;
- advisory lock loss, PostgreSQL disconnect, S3 ceiling conditional update,
  ambiguous reservation, vacancy registration, and new epoch;
- GC mark, claim, final reference check, S3 delete, and row completion.

After every injected crash, restart must find either the old state plus harmless
objects or the complete new state. It must never find a missing referenced
object, metadata ahead of data, a duplicate block number, a reused sequence,
an advanced watermark without its generations, or two valid writers.

### Format/property/fuzz tests

- For arbitrary valid local segments, parse into a recipe and reconstruct
  byte-for-byte identical `.jss` files.
- Decode local and virtual readers and compare events, block descriptors,
  footer indexes, selection plans, and corruption errors.
- Fuzz malformed recipes, lengths, block numbers, hashes, headers, footers,
  cache files, and S3 responses. Keep strict memory allocation limits.
- Compare local Pebble and PostgreSQL key ordering, prefix bounds, deletes,
  batches, and integer encodings over randomly generated byte keys.
- Prove generation visibility at catalog-commit boundaries under randomized
  compaction/import publication.

### Oracle and mutation campaign

Run the full oracle against both backends. Add seeded scenarios for:

- crash in every block/seal/publication stage;
- database failover and stale replica reads;
- S3 delay, throttling, lost responses, corrupt cache/provider bytes, and
  missing referenced objects;
- attempts to run two writers and the resulting fencing;
- database PITR behind client-observed seqs;
- compaction/import racing plans and long-lived cold replay;
- bootstrap, merge, repair, and relay replay across failover;
- GC racing dedup/republication and PITR-window retention.

Add deliberate test mutations that remove or reorder upload checks, writer
fences, atomic block-and-cursor commit, remote ceiling updates, gap
registration, source-generation checks, response pinning, and GC's final
reference check. The mutation campaign must catch every one.

### Real service and failure tests

Fakes cannot prove how a managed service behaves. Test every supported S3
provider and PostgreSQL setup for:

- concurrent `If-None-Match`/`If-Match`, multipart completion, timeout
  ambiguity, checksum headers, versioning, and immediate known-key reads;
- primary crash during commit, synchronous replica promotion, DNS/connection
  pool behavior, advisory-lock release, and replay position;
- IAM/KMS denial and rotation, bucket throttling, network partitions, and
  availability-zone loss;
- PITR plus object-retention restoration and sequence-ceiling reconciliation.

Run periodic production restore drills into an isolated archive identity until
the audit completes; never let a drill contend for the real writer fence.

### Performance and cost requirements

Measure all of the following before production use:

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
- Treat object bytes and database values as untrusted on read despite access
  controls: enforce size limits before allocation and verify hashes/formats.
- Record administrative generation deletion, migration, schema change,
  sequence reservation, and recovery actions in an audit trail.

## Rejected shortcuts

- **Periodic Pebble snapshots as primary durability:** leaves a data-loss
  window, turns failover into a restore, and requires matching a database
  snapshot to the correct segment files. It is useful for backup, not as the
  production commit path.
- **One S3 object per complete segment:** simple reads, but compaction reuploads
  terabytes per day and active append remains awkward.
- **Mutable object per active segment:** S3 has no atomic append. Repeated copy
  or multipart tricks create visibility and recovery protocols inferior to a
  database block list.
- **S3 LIST as manifest:** listings contain physical garbage and cannot
  atomically bind relay cursor, verifier state, or generations.
- **Store blocks directly in PostgreSQL:** transactions become simple, but this
  pushes multi-terabyte storage, I/O, backup, and egress through the expensive
  database tier.
- **Database first, upload eventually:** creates acknowledged catalog state
  whose bytes can be permanently absent after a crash.
- **Dual-write local and remote as co-authorities:** no atomic commit spans
  Pebble/filesystem/PostgreSQL/S3, so a failure cannot determine which archive
  owns the relay cursor.
- **Resume inside an old reserved sequence range after restart:** unsafe after PITR;
  the restored database can look consistent while forgetting later observed
  seqs in that same grant.
- **Delete objects as soon as the current generation changes:** breaks
  in-flight responses, caches, retries, readers, and PostgreSQL PITR.
- **Active-active writers with database sequences:** sequence uniqueness alone
  does not serialize relay cursor, per-DID order, lifecycle, verifier state, or
  active segment topology.

## Decisions that still require measurement

The architecture is decided; these parameters and optimizations are not:

1. PostgreSQL service/topology (RDS PostgreSQL versus Aurora PostgreSQL), pool
   sizes, transaction isolation, and exact schema indexes/partitioning.
2. Remote sequence grant size and renewal threshold.
3. S3 multipart threshold, upload concurrency, retry budget, and supported
   compatible providers.
4. Cache size policy and whether active block frames should be synchronously
   retained locally after remote commit.
5. Whether S3 request count and download latency justify whole-generation or
   multi-block cache objects.
6. HTTP cache and old-generation grace periods, and the resulting
   object-retention time beyond database PITR.
7. Whether footer/header bytes should also be mirrored inline in PostgreSQL
   for small-read latency; S3 objects remain canonical if so.
8. Catalog partitioning for tens or hundreds of millions of generation-block
   rows and how much descriptor data remains normalized versus encoded.
9. Migration maintenance-window target and whether a local change journal is
   worth building to shorten it.
10. Recovery point, recovery time, and availability targets for database
    failover, S3 outages, relay retention, regional recovery, and full archive
    verification.

No measurement or optimization may weaken upload-before-reference, atomic
block-and-metadata commit, writer fencing, immutable generations, rollback
protection, or PITR-aware garbage collection.

## Implementation exit criteria

The backend is production-ready only when:

- local mode remains green and within its performance baseline;
- the same oracle expectations pass on disaggregated mode, including restart,
  failover, compaction, import, bootstrap/merge, and archive-to-live cutover;
- mutation tests catch violations of the new write-order, fencing, and GC
  rules;
- a supported S3 service and PostgreSQL HA setup pass behavior and forced
  failover tests;
- an existing production-sized archive migrates and reconstructs with exact
  recipe/file/event comparisons;
- database PITR is restored against retained objects and no seq is reused;
- S3, database, KMS, and cache outage behavior is bounded and observable;
- GC completes a full retention cycle without deleting anything reachable by
  the oldest supported database recovery point, HTTP cache, active read, or
  active job;
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
