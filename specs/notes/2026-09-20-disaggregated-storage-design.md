# Disaggregated primary storage: S3 blocks and a PostgreSQL catalog

Date: 2026-09-20. Status: accepted design direction; not implemented.

Updated 2026-09-22 after code review, a checksum compatibility experiment, and
read-only production measurements from both pop1 and pop2. This revision keeps
the durability protocols and narrows the first implementation: one fenced
writer owns maintenance, readers use the primary, timestamp rules use the
existing local Pebble index as a rebuildable view, and remote sequence grants
replace (rather than supplement) the local one-block lease. No backend code is
implemented by this document.

## Decision

Add an optional `disaggregated` storage mode for production deployments:

- S3 stores the large data: compressed segment blocks and footers.
  These objects are immutable and named by their SHA-256 hashes. A separate
  keyspace permanently retains timestamp import inputs.
- Highly available PostgreSQL stores the exact 256-byte segment headers,
  relay cursors, sequence state, repo state, and the catalog that says which
  blocks make up each segment.
- Local disk holds bounded caches, temporary workspace, and the derived rule
  index. Losing it does not lose durable data, but rebuilding required rule
  state can delay writer readiness.
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

- An unreferenced archive object under `objects/` is harmless garbage and can
  be deleted after the retention checks below. Import inputs and the sequence
  control object are not garbage-collected.
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

A **fence** is a database-issued writer number. Every authoritative writer
transaction must present the current number. When a new writer takes over, it
gets a higher number, so an old writer can no longer commit even if it is still
running. Maintenance runs under that same writer epoch; GC additionally uses
version-specific deletion claims so a delayed S3 DELETE cannot destroy a later
upload.

The first implementation deliberately has no distributed rewrite scheduler,
read-replica routing, durable per-HTTP-request pins, pack files, or general
filesystem emulation. Short database transactions publish state; bounded
workers do encoding and network I/O outside those transactions. Extra
mechanisms need a measured problem, not just a possible future use.

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
8. Preserve the public protocol and reconstruct each supported `.jss`
   generation byte-for-byte. The checksum extension below preserves version-1
   reader compatibility and makes new rewrites distinguishable even when their
   compressed lengths do not change.

## Non-goals

- Active-active ingest. One writer is active; another may take over after it
  fences the old writer.
- Treating S3 like a local filesystem, implementing append with multipart
  uploads, or putting Pebble on an S3-backed `vfs.FS`.
- Eliminating all local storage. Bounded cache, temporary rewrite space,
  multipart staging, and derived timestamp rule state may use ephemeral SSD.
- Making S3 the transaction coordinator. It stores bytes and an independent
  sequence ceiling; PostgreSQL publishes archive state.
- Exactly-once delivery. Existing at-least-once behavior and inclusive cursors
  remain the contract.
- SQL read-replica routing in the first release; all catalog reads use the
  primary.
- Transparent multi-region writes. Regional recovery needs access to the
  original sequence-control object or a new archive identity.
- Replacing PostgreSQL backups. Database point-in-time recovery (PITR) and S3
  object retention are both required; neither replaces the other.
- A general storage library. The interfaces should describe Jetstream's own
  operations and safety rules.

## Production workload and design implications

The original pop1 observation (2026-09-20 03:42:37–07:48:49 UTC) measured
564 archived events/s, one block every 7.3s, and about 17.5 rotations/day.
Its status snapshot reported 1.82TiB, 7,651 sealed segments, 6.40M blocks, and
25.7B events. Keep that as an archive-size baseline, not a fleet capacity
estimate. The earlier backup investigation's finding that about 87% of
rewritten files changed at most ten blocks supports retaining block objects.
It is not a measurement of changed-block bytes or S3 savings.

A repeat review queried Grafana's `all-pops` datasource with `gcx`, UID
`8bbffde2-f4c0-46f9-9f30-acf2d37c7625`, at **2026-09-22 12:00:00 UTC**.
The six-hour window below is 06:00–12:00 UTC; the seven-day window ends at the
same time. Selectors are `job="jetstream",instance=~"pop[12]"`, excluding
`localhost:6009`. Both instances reported `v0.2.2`, commit `aaca9b2`; code
review used `4accd6a`. Counter increases account for resets, and series are
aggregated by instance after applying rate/increase.

| Observation | pop1 (typical steady state) | pop2 (large-PDS recovery) |
| --- | ---: | ---: |
| Archived events/s, six-hour mean | 305 | 127,616 |
| Block flushes/s, six-hour mean | 0.0745 | 173.0 |
| Block flushes/s, largest sampled five-minute rate | 0.173 | 245.6 |
| Segment rotations in six hours | 3 | 777 |
| Metadata batches/s, six-hour mean | 0.0772 | 641.8 |
| Metadata batches/s, largest sampled five-minute rate | 0.24 | 1,201 |
| Metadata point reads/s, six-hour mean | 419 | 1,191 |
| Metadata point sets/s, six-hour mean | 9.72 | 15.86 |
| Mean successful metadata batch latency | 1.95ms | 0.248ms |
| Successful failed-repo retries in six hours | 0 | 5.06M |
| Whole-file compaction bytes/day, seven-day mean (decimal TB) | 6.80TB | 2.75TB |
| Rewritten segments/day, seven-day mean | 26,188 | 10,506 |
| Mean compaction pass, seven-day window | 76.8 minutes | 66.5 minutes |
| Successful `getBlock` requests/s, six-hour mean | 9.49 | No request series |
| `getBlock` response bytes/s, six-hour mean | 1.95MB | 0 |

Both instances reported the steady-state lifecycle phase throughout the
sampled six-hour window. Operator context supplied after this review explains
the difference: **pop2 was recovering one very large PDS; pop1 is representative
of typical steady-state operations.** The lifecycle gauge does not distinguish
that exceptional recovery from ordinary live ingest. Pop2's last 30 minutes
still averaged 104 blocks/s and 364 metadata batches/s, but those numbers are
an exceptional-workload benchmark, not the baseline or a minimum normal-service
throughput requirement.

Here, repo repair/recovery means retrying previously failed repository fetches
from a PDS and ingesting the recovered repo state through the normal sync-marker
and record write path. Recovering a large PDS can produce far more archive
writes than its ordinary live traffic.

The current batch metric does not distinguish synchronous from non-synchronous
commits or identify all callers; do not describe every batch as a block
durability transaction. Likewise, `getBlock` traffic is not all read traffic:
pop1 also reported about 1,877 cold subscribe reads/s, which may share cached
decoded blocks and are **not** an S3 GET-rate estimate.

Representative reproducible queries (run with the datasource above and
`--time 2026-09-22T12:00:00Z -o json`):

```promql
sum by(instance) (rate(jetstream_ingest_blocks_flushed_total{job="jetstream",instance=~"pop[12]"}[6h]))
sum by(instance,op) (rate(jetstream_store_op_duration_seconds_count{job="jetstream",instance=~"pop[12]"}[6h]))
max_over_time(rate(jetstream_ingest_blocks_flushed_total{job="jetstream",instance=~"pop[12]"}[5m])[6h:5m])
sum by(instance) (increase(jetstream_compaction_bytes_rewritten_total{job="jetstream",instance=~"pop[12]"}[7d])) / 7
sum by(instance) (increase(jetstream_backfill_failed_repo_retry_succeeded_total{job="jetstream",instance=~"pop[12]"}[6h]))
```

Use pop1 to size the initial steady-state path and pop2 to test bounded
behavior and recovery time under exceptional load:

- **Start with one block upload and commit at a time per writer namespace.**
  Pop1 averaged one flush every 13.4s, with a sampled five-minute peak of
  0.173 blocks/s. These measurements do not justify a concurrent upload
  pipeline or multiple client-visible pending blocks as first-release
  requirements. Benchmark PUT/verification/SQL latency and its effect on live
  delivery before adding those mechanisms.
- **Recovery may take longer, but must stay correct and bounded.** Pop2's
  sampled peak would require less than 4.1ms per block on a serial path to
  match its existing local throughput. Matching that exceptional rate is not
  a correctness requirement. Limit repair/backfill producer pressure, preserve
  live ingest and safe-cursor progress, and measure recovery completion time
  against an explicit operational target and upstream replay retention. Add
  bounded concurrent uploads/ordered publication only if those targets cannot
  be met by the simpler path. Never accept unbounded backlog or silent loss.
- **Preserve existing metadata batching first.** Keep block-dependent mutations
  in their existing durability batch and use prepared SQL. Measure point-read
  latency and producer throughput; add multi-get or coalescing of independent
  metadata updates where measurements justify it. Do not split a batch's
  durability boundary to reduce latency.
- **Keep unchanged block bytes.** Whole-segment uploads would inherit the
  measured multi-terabyte rewrite traffic. A changed block still needs an
  upload; deduplication does not eliminate compaction reads or footer work.
- **Account for catalog amplification.** Using the earlier mean of about
  837 blocks/segment with pop1's current rewrite rate gives a rough
  22 million generation-block rows/day if each rewrite copies all references.
  This is a sizing estimate, not a measured SQL workload. Keep those rows
  narrow and test a complete retention window before choosing database size.
- **Measure caches before adding representations.** Pop1's mean successful
  block response was about 206KB. Bounded prefetch, request coalescing, and the
  existing decoded-block cache come first; pack files and full-segment cache
  objects remain deferred.

These are observed workloads, not capacity limits or remote-backend benchmarks.
Test typical steady state and exceptional backfill/PDS recovery separately,
along with import, cache-cold replay, throttling, failover, and larger archives.
Accept bounded throttling during exceptional work only when live-service and
recovery-time targets still pass.

## Preserved and new invariants

All invariants in `specs/invariants.md` continue to apply. Their storage-specific
forms are:

1. **Published generations are immutable.** A generation's header, ordered
   block references, and footer never change. Rewrites publish another
   generation and atomically change a pointer.
2. **Upload before commit.** Every S3 object must be readable and verified
   before PostgreSQL may publish a reference treating it as durable. Upload
   registration is bookkeeping, not archive publication.
3. **PostgreSQL is authoritative.** Readers discover segments, blocks,
   generations, vacancies, and current pointers through the catalog. Bucket
   listing can audit or find garbage but cannot repair catalog state.
4. **Hashes name exact bytes.** The SHA-256 is over the stored bytes, not
   decoded events, an ETag, or a logical block. Reads verify length and hash
   before decoded data becomes observable.
5. **Only the current writer can publish archive state.** Every authoritative
   writer transaction includes the current fence number and validates it while
   holding the archive control row lock through commit. Leadership acquisition
   takes the same row lock. Losing the leadership connection stops new work and
   requires a new epoch before reuse; an already-running transaction must
   finish before takeover or be rejected.
6. **A block and its metadata commit together.** The prepared block's reference,
   exact `seq/next`, safe `relay/cursor`, and all eligible
   repo, verifier, sync, and lifecycle mutations commit together. A later
   prepared block cannot make its metadata eligible early.
7. **Client-observable seqs are never reused.** A monotonically increasing S3
   ceiling reserves remote writer sessions' ranges across both process crashes
   and database rollback. PostgreSQL records the current grant and exact
   durable frontier; it does not maintain a second, one-block reservation.
8. **Unexplained gaps stop the process.** Recipes and explicit
   `seq/gap/*` vacancies explain all durable coverage. Missing objects, missing
   block numbers, or unexplained sequence holes are corruption, not an invitation
   to skip forward.
9. **Published segment order is stable.** Logical segment indices retain the
   same creation/time order as lexically sorted local filenames. Generation
   changes never reorder logical segments.
10. **Bad upstream input is dropped; bad owned state stops the process.** An
    invalid relay record remains an input error. A confirmed authoritative
    hash mismatch, impossible recipe, or referenced missing object is internal
    corruption. Losing writer authority instead stops writes and live
    publication; it need not stop a healthy immutable-history read path.

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
  imports/<import-id>/input.csv
  control/sequence-ceiling
  probes/<deployment-id>/...
```

Objects under `objects/` are immutable and content addressed. Use
`If-None-Match: *`, as required of supported providers. If an object already
exists, verify its recorded size and checksum rather than overwriting it. A
conflicting object under the same SHA-256 key is corruption.

The initial implementation stores two content-addressed object kinds:

| Kind | Stored bytes | Notes |
| --- | --- | --- |
| `block-frame` | Existing zstd frame, without its 8-byte local length prefix | The catalog records compressed length; the prefix is deterministic when materializing `.jss`. |
| `segment-footer` | Exact sealed footer bytes | Includes the indexes and historical envelopes belonging to that generation. |

The hash covers the bytes exactly as stored. The object table records the
length, hash, creation time, mandatory S3 version ID once available, and
optional provider checksum. Each object reference says whether it expects a
block or footer. Object type is not part of the key, so identical bytes can
safely be reused in more than one role. Import inputs have their own job
references and permanent-retention rules; the mutable sequence-control object
has its own conditional-update protocol.

Do not use S3 ETags as content hashes. Multipart upload, encryption, and
provider differences make that invalid. Send or validate the provider's
SHA-256 checksum when available and always retain Jetstream's own digest.
Verification means a service-validated full-object checksum and length for the
exact version, or a GET whose bytes Jetstream hashes itself. A HEAD returning
the digest we supplied as arbitrary user metadata is not independent
verification; multipart composite checksums are not whole-object SHA-256.
Use ordinary single PUTs for bounded block/footer objects. Multipart handling
belongs to large import inputs; do not put it on the small-block path.

### Exact `.jss` reconstruction

A sealed generation recipe contains:

- logical segment identity, stable segment index/name, and format version;
- exact 256-byte header, stored inline in the generation row;
- ordered block-frame references with compressed length;
- the immutable per-block descriptors needed for planning (event/sequence and
  witnessed-time envelopes, count, and relevant blooms/index summaries);
- exact footer object reference;
- total materialized file length; a full-file SHA-256 is optional for migration
  audits, not a prerequisite for a sparse rewrite.

The fixed header is tiny, already checksummed, and published in the same SQL
transaction as its recipe. Keeping it inline removes tiny S3 requests and one
class of object references. Footers remain in S3 because their bloom/index data
can be large. Footer bytes own the sealed planning indexes; catalog summaries
are checked against them rather than independently rebuilt from surviving rows.

Reconstruction writes the header, then for each block an 8-byte little-endian
frame length and the unchanged compressed frame, then the footer. It does not
decode/re-encode events or rebuild a footer from surviving rows. Rebuilding a
footer would risk losing historical envelopes intentionally preserved by
compaction.

An active segment has its exact header inline plus an ordered list in
`active_segment_blocks`; it has no footer and is never advertised as a sealed
generation. The database list, rather than an appendable S3 object, is the
active segment.

### Segment identity without a reader-breaking checksum change

The current `xxh3HeaderFooter` hashes header bytes `[12:256]` and the footer,
not block frames. `segment.Patch` recomputes it, but two different imported
timestamps can produce equal-length frames and identical header/footer bytes.
They then share a checksum and HTTP ETag. Repeating that hash calculation does
not fix the bug. Changing the hash algorithm to scan the whole file in
`Reader.Open` would break existing readers and sparse-read performance.

Use the reserved header space for a **block-content root**, shared by both
backends. For newly sealed or rewritten files, retain header version 1 and the
existing xxh3 algorithm. Reserve bytes `[98:102]` for an extension tag (`bsh1`)
and `[102:134]` for this SHA-256 value:

```text
SHA256("jetstream-blocks-v1\0"
       || uint64_le(block_count)
       || for each block in order:
            uint64_le(compressed_length) || SHA256(exact_compressed_frame))
```

The tag and digest participate in the existing header/footer xxh3. A timestamp
rewrite therefore changes the checksum even if every frame length stays the
same. The ordered lengths and hashes bind all framed-block bytes; the existing
metadata checksum covers header/footer layout. Keep the distinction between
this 64-bit format checksum and S3's authoritative SHA-256 object identity.
Neither S3 ETags nor the format checksum replace object-hash verification.

This path was tested during this review using the unmodified public segment
writer, patcher, and reader: timestamps `1600000000000000` and
`1600000000000002` produced different 1,184-byte files with the same metadata
checksum `2a96bad02e19177c`. Adding the root and recomputing the existing xxh3
produced `05fe6d8c7217231e` and `0d63c85f8cd402d8`; the unmodified
`segment.Open` and `DecodeBlock` accepted both extended version-1 files.
This demonstrates compatibility with this implementation, not every external
reader. Cross-version fixtures and supported-client checks remain a delivery
gate, and the shared format documentation must specify the extension when it
is implemented.

Compatibility and verification rules:

- Existing files with zero reserved bytes remain readable and reconstruct
  byte-for-byte. Migration does not rewrite every old file just to add a root.
  Every subsequent non-no-op rewrite emits the extension. Legacy xxh3 values
  remain metadata checksums; do not claim they already cover frame identity.
- A new local writer computes frame hashes during its existing seal/rewrite
  walk. A remote rewriter uses verified catalog hashes for unchanged frames,
  so refreshing the root does not download unchanged data. The no-op rewrite
  path leaves the source untouched, including legacy headers.
- New remote readers check the ordered recipe against the root and verify
  each fetched object's exact length/hash. Local sparse opens keep the current
  metadata validation and zstd frame checks; full verification can recompute
  the root while scanning all blocks. An old reader accepts the extension but
  does not verify the root against the body: do not overstate that guarantee.
- New format code explicitly recognizes legacy zero padding and `bsh1`,
  preserves the reserved bytes in its parsed/serialized header, and rejects
  unsupported nonzero extension tags. All shared seal/patch/rewrite paths must
  adopt this together; an old `Header` struct round trip drops the extension.
- Any writer that rewrites a file must recompute the extension; it must not
  copy a stale digest or silently zero it. Disable downgrade to an old writer
  after the feature is enabled. Read-only old clients remain compatible.
- Cover equal-length timestamp changes, empty compacted blocks, legacy files,
  unknown extension tags, Range resumes, and zero/no-op rewrites. Keep legacy
  golden fixtures and add extended fixtures rather than replacing the evidence
  that old files still decode.

The public checksum/ETag shape stays unchanged. This resolves the original
format-compatibility prerequisite without a second event encoding, a new
planning token, or an archive-wide conversion.

### Required S3 behavior

AWS S3 currently provides strong read-after-write and list consistency, but
this design needs only strong reads of known keys. It assumes:

- a successful PUT or completed multipart upload exposes either the complete
  object or no object, never a prefix;
- GET/HEAD of its known key can confirm the newly written object;
- conditional create and conditional replacement work for immutable objects
  and the sequence control record;
- version IDs identify exact stored versions, and repeated deletion of one
  version cannot affect a later upload under the same key;
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
  remote_ceiling_etag text,
  remote_reservation_id uuid,
  configuration_fingerprint bytea
)
```

`catalog_commit` is a revision number increased by every catalog publication.
`writer_epoch` is the writer's fence number. The remote reservation fields
identify the current S3 grant, whose end is recorded as `seq/max_reserved` in
metadata. This is a cached grant, not an independently allocated lease; neither
SQL state nor a restored backup may reduce S3's ceiling.

To become writer, a process takes a PostgreSQL advisory lock for `archive_id`.
It then checks the S3 sequence ceiling and, in a transaction, locks the archive
control row with `SELECT ... FOR UPDATE`, increases `writer_epoch`, and records
its process identity. Every later authoritative writer transaction takes that
same row lock, checks the epoch after obtaining it, and holds it until commit
or rollback. This includes metadata-only writes and sequence reservations as
well as block and generation publication. Take the archive lock before segment
or object locks in these transactions to keep lock ordering consistent.

An ordinary epoch read followed by later writes is insufficient: a transaction
on a pooled connection can outlive the dedicated advisory-lock connection.
The row lock ensures it either commits before the new epoch takes effect or
observes the new epoch and fails. If the leadership connection drops,
reconnecting requires taking leadership again and getting a new epoch.

The advisory lock prevents two healthy writers. The epoch prevents an old
writer from committing after failover. Election does not depend on clocks.
Use a direct/session-pooled connection for the advisory lock, never a
transaction-pooling proxy; set bounded transaction, idle-transaction, statement,
and lock timeouts so a lost session or stalled transaction cannot block
promotion indefinitely. Takeover never bypasses the locks on a timeout.

All maintenance publication and GC claim transactions use the same epoch and
lock order. GC may use a restricted connection/credential, but it is scheduled
by the current writer, not an independently elected daemon. No S3 network call
runs while holding the archive control row.

The lock protects one PostgreSQL cluster. A restored copy of that cluster has
its own locks. Before promoting a restored copy, the operator must revoke the
old writer's and old garbage collector's access or otherwise prove they have
stopped, including outstanding deletion work. The S3 sequence ceiling
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

The separate `import-rules` Pebble database is not part of `meta.pebble` and
must not disappear during migration. Its per-record lookup representation is
discussed under timestamp import; retaining the existing metadata encodings
does not imply moving all timestamp rules into PostgreSQL.

### Objects and upload state

```text
objects(
  archive_id uuid,
  sha256 bytea,
  byte_length bigint,
  state smallint,              -- uploading, available, gc_claimed, deleted
  provider_version text,       -- required in available/gc_claimed states
  created_at timestamptz,
  unreferenced_at timestamptz,
  delete_after timestamptz,
  primary key (archive_id, sha256)
)
```

Before uploading, register or reuse the object row under the publication/GC
locking rules below. A new hash starts in `uploading`; after the PUT has been
verified, change it to `available`. Recipes may reference only available
objects. If a process dies during upload, another process can inspect or clean
up the stale row without listing the whole bucket. S3 Inventory is still useful
for audits.

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
  active_header bytea null,    -- exact 256 bytes
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
  header bytea,               -- exact 256 bytes
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
Keep composite foreign keys scoped to the archive/namespace/segment, enforce
32-byte hashes and 256-byte headers, and allow the historical envelopes of
empty compacted blocks. A recipe's offsets, lengths, counts, and summaries must
match its verified footer; an inconsistency is corruption, not grounds to
silently regenerate metadata.

`visible_from_commit`/`visible_until_commit` identify publication/retirement.
They are not a second MVCC system or a public historical-query API; PostgreSQL
snapshots and PITR already provide those storage guarantees. Old generation
rows and objects remain available for the HTTP cache grace and recovery window
after `segments.current_generation_id` changes.

Add a retirement timestamp for retention, and narrow indexes for object
reference checks and segment sequence lookup. Keep immutable bloom/collection
indexes in the footer cache, not duplicated in every `generation_blocks` row.
Active descriptors retain enough validated information to seal after disk loss;
if a summary cannot reproduce the footer correctly, sealing fetches and decodes
those blocks. Do not claim the few sequence/time columns alone can recreate
DID blooms and collection counts.

Import jobs, exceptional long-lived generation holds, and deletion claims need
catalog records. Ordinary bounded HTTP reads do not write pin rows (see
retention below). Do not add a mutable reference count or another manifest;
foreign keys and reference queries remain the source of truth. Normalize
references first and measure the projected retention-window size before
considering persistent block-list trees or recipe packs.

## Core protocols

### New block commit

Before assigning a seq or publishing to the readable log, confirm it is inside
the current session's S3 grant, recorded in a fenced PostgreSQL transaction.
The grant protects pending events without a second PostgreSQL lease. Start
with the current synchronous flush shape: one pending/prepared block per writer
namespace, upload/verify it, then publish it and its dependent metadata before
accepting the next block. Local behavior is unchanged. Bound encoded bytes and
flush age, including readable-log entries pinned until durability. The much
larger S3 grant is never a memory or pending-event budget.

A bounded concurrent pipeline is a possible later optimization if measured
backfill/recovery or live-latency targets require it. It would prepare in ingest
order, upload concurrently, and publish only contiguous ready blocks in order.
That requires an explicit audit of callback ownership and metadata eligibility,
plus tests for multiple client-visible pending blocks; removing the small SQL
lease alone does not make today's callbacks safe for pipelining.

For each block:

1. Freeze the block, namespace, ordinal, exact sequence boundary, and its
   dependent metadata eligibility boundary. Calculate SHA-256 and the validated
   descriptors needed for recovery/sealing. Preserve per-DID ingest order.
2. Register or reuse the object row, upload if needed, and verify length/hash
   and the exact provider version. Do this outside catalog publication locks.
3. Once the block is verified, start a short SQL transaction. Lock the archive
   control row, check the writer epoch, then check the expected active tail
   and lock referenced object rows in a stable order. Attach only available
   objects; publication never jumps over an unfinished block.
4. Insert the block reference and update the exact `seq/next`, safe relay
   cursor, eligible repo/host completion, verifier/sync/account state, and
   lifecycle progress in the same transaction. Capture eligibility at prepare
   time; sampling the producer's latest mutable state at commit time can put
   metadata ahead of a later, still-uncommitted block. Preserve existing
   safe-cursor and whole-repo completion rules.
5. Commit with synchronous durability. Only then advance the readable log's
   durable floor and release the corresponding completion acknowledgements.
   Cache population and notifications follow; they are expendable.

Unreferenced uploads are harmless. If publication fails or its result is
unknown, poison this writer session, stop intake and live publication, and
recover through the single startup/leadership path below. Do not retry a
prepared operation under a new epoch. Acquiring the archive row lock during
recovery resolves the ordering with old pooled transactions before reading the
frontier: a plain read that finds no block is not sufficient proof of rollback.
Abandoning all pending work and replaying upstream is simpler than a separate
in-place commit-reconciliation state machine.

Bound every remote operation with a timeout, including commit work detached
from caller cancellation. The local use of `context.WithoutCancel` must not
turn into an unbounded network call. A dependency-compatible metadata-only
batch may be grouped with a block transaction, but neither a timer nor an
empty block overrides its dependency requirements.

### Metadata-only transactions

Some current Pebble batches do not accompany a block. They may commit directly
to PostgreSQL only when they do not depend on uncommitted events. Callers must
say which class a change belongs to; an empty block is not proof that a change
is independent. Existing dependency rules for verifier, cursor, merge,
compaction, and import state still apply.

These transactions use the same archive-row lock and fence check as block
commits. Combine small updates into multi-row SQL instead of making hundreds
of network round trips. Large metadata transfers may COPY into staging tables
and publish in checked transactions. This is not a requirement to put the
per-record timestamp rule map in PostgreSQL.

### Sealing and rotation

Sealing converts the active database list into an immutable generation:

1. Read the active segment's committed block list at one catalog revision and
   keep that exact list for the rest of the operation.
2. Generate the exact final header/footer and recipe using shared format code.
   Compute the content root from ordered frame hashes; upload and verify the
   footer. Prepare the next active segment's inline header.
3. In one transaction, check the writer fence and confirm the active tail did
   not change. Insert the generation and ordered block rows, make it current,
   mark the segment sealed, retire its active block rows/header, and
   create the next active segment. The new generation now owns the sealed
   segment's references; retiring active references does not delete objects.
4. Commit, then publish cache/reader notifications.

No S3 rename or copy is involved. A crash before the transaction leaves
an unreferenced footer object and the old active list. A crash after it
leaves one complete sealed generation and one new active segment.

Drain pending blocks before sealing; no append can race the frozen tail.
The transaction may copy hundreds or a few thousand reference rows. Measure
its lock duration under typical live traffic and rate-limited PDS recovery. If
copying rows is expensive, measure staging the immutable generation before the small
pointer-swap transaction; staged rows must be unservable and protected from GC.
Do not introduce that extra state until the straightforward transaction fails
the measured lock-duration budget.

### Compaction

Keep the current single writer's rewrite mutex: compaction and timestamp
patching do not compete to rewrite the same segment. Parallel workers within
one pass may process different segments. Ingest remains independent. This is
an in-process maintenance lock, never a database transaction held across the
pass. Epoch checks still reject an abandoned writer's prepared replacements.

Compaction never edits a published generation:

1. Force-rotate/drain the active segment as today, choose the sealed target
   watermark, and fold tombstones from the durable `(previous, target]` event
   window. Do not use the live in-memory tombstone set as the source: it can
   contain a later update above the target and lose the window's predecessor.
   Bound the fold in tombstone chunks as today. Capture the source generation
   IDs and prior watermark in a short snapshot under the rewrite mutex, before
   selecting inputs for that chunk. Retain its inputs
   and identify every segment that must be checked for those tombstones.
2. Outside a database transaction, use the existing rewrite/index-building
   algorithm over the source generation, apply tombstones, and build replacement
   frames/footer. Initially this may read/decode every block in an affected
   segment: today's footer builder needs distinct DIDs and collection counts,
   not just the block envelopes in SQL. Reuse old hashes for byte-identical
   unchanged frames; do not recompress or upload them. More selective reads
   require sufficient exact summaries and their own tests, not an assumption
   that unchanged output means an unread input.
3. Upload and verify new block/footer objects and finalize the inline header.
4. Publish one segment replacement per transaction initially. Each transaction
   takes the archive-row lock, checks the writer fence, and verifies that each
   current generation is still its expected source. Insert the replacements,
   close the old visibility ranges, and change the current pointers. Do not
   advance the global compaction watermark for a partial batch of segments.
5. Check the expected source pointer even with the rewrite mutex. An epoch
   change discards the old session's work; an unexpected same-epoch source
   change is an invariant error. Do not build a normal conflict/retry scheduler
   for writers that the ownership model already excludes.
6. Only after every segment affected by the fixed tombstone chunk has been
   processed, advance the global watermark in a fenced transaction that checks
   the expected prior watermark. A new writer reconstructs or repeats
   unfinished work from the unchanged watermark.

A tombstone window and a SQL publication batch are different units. One
account deletion may affect thousands of segments; completing the first batch
does not complete that tombstone. Replacements may safely become visible ahead
of the watermark because applying the same tombstones again is idempotent.
Keep publication batches small enough to limit lock time and PostgreSQL
write-ahead log (WAL) growth. Decoding, rewriting, and uploading happen outside
database locks. The watermark must never claim unfinished work.

An in-flight response keeps using the exact older recipe it started with; HTTP
caches may retain it through the configured grace period. A later name lookup
finds the new generation and its new ETag. Compaction no longer needs a
filesystem rename or manifest rescan.

### Timestamp import and other rewrites

Timestamp import uses the same “verify source, then replace pointer” protocol
as compaction. Per-segment job progress and generation changes commit together.
Import state cannot say a segment was patched unless the new generation is
visible. Finalize the corrected segment checksum before publication, as
described under the content-root extension.

A prepared replacement names its exact source generation. On restart, repeat
unfinished logical segments against their current generations. Completed
import checkpoints remain valid through compaction because compaction preserves
`IndexedAt`; record completion with publication and never let a stale candidate
overwrite newer work.

Timestamp CSV inputs are immutable S3 objects in the separate `imports/`
keyspace. Upload and verify the exact input before accepting the durable job
or activating any rules. The job records its key, version, length, and digest.
Inputs are retained permanently, including after job completion or failure;
neither Jetstream GC nor bucket lifecycle rules may delete them. Local copies
are disposable staging files. This also keeps inputs available when PITR
restores a job to an earlier phase.

PostgreSQL records the small control state: import order, input references,
rule activation/progress, and patch completion. Bucket listing cannot decide
which inputs are active: after PITR, S3 may contain inputs newer than the
restored catalog. Rule reconstruction must preserve import order,
last-write-wins behavior, and the committed progress of interrupted imports.

Use the existing **local Pebble rule index** for per-record lookups. It is a
rebuildable view, not another durable authority. Keep its collection fast-path
and existing key/value encodings; do not add per-event SQL queries or assume
that a full-network rule map fits in RAM. Provision a separate disk budget and
measure cold rebuild time before setting a failover SLO.

The catalog must describe the exact rule state used by appends, including a
terminally failed import's committed prefix. Merely retaining the CSV and a
job status is insufficient: today's `ruleSSTBuilder.Ingest` installs one chunk
at a time, and a completed job's rules continue to stamp future events. Keep
that incremental behavior and the existing crash-resume/operator-resubmit
contract; do not require an all-or-nothing full-network rule-map transaction.

For remote mode, record an ordered activation journal with input identity,
parser/rule-encoding version, and the cumulative valid-row boundary of each
activated chunk. Chunk boundaries must be reproducible, including duplicate
paths and specific-version rules; they are not arbitrary byte offsets into a
quoted CSV. The current chunk builder counts generated entries, not CSV rows,
so persisting only its configured chunk size is insufficient. Prepare/validate
input as today, then for each chunk:

1. Under a short append/stamping barrier, commit that prefix's activation in
   a fenced SQL transaction. Its input is already durable in S3.
2. Install that exact chunk into the local index and update its local applied
   revision/collection filter before releasing the barrier. No event may use
   uncommitted rules or silently bypass a committed activation.
3. On an unknown commit result or failure to catch the local index up, stop
   this writer session. Startup rebuilds/replays the catalog's activations
   before any stamping or live readiness. A disk cache ahead of a restored
   catalog must be discarded, not accepted just because it opens successfully.

Reconstruct imports in activation order with the existing last-write-wins and
specific-version precedence. Nonterminal jobs then resume; terminal failures
retain their committed prefixes until an operator re-submits. Retained CSVs
are the authority, so an optional verified local checkpoint only speeds this
rebuild. A future S3 index checkpoint is an optimization requiring the same
revision checks, not a prerequisite for the first implementation. Preserve
local mode's accepted behavior; the extra journal is needed because its rule
database ceases to be durable in remote mode.

After successful rule activation, drain/force-rotate the writer before taking
the bucket/patch target inventory, as the current import preamble does. This
must include all pending/prepared work from before activation. Those
rows may have entered the readable log before activation and still need the
historical patch; later appends are stamped under the active rule revision.

Import bucket/offset files are also derived workspace. A persisted `Bucketed`
flag alone is not proof that they exist on a new machine. Missing, incomplete,
or mismatched workspace must be rebuilt from the exact retained input before
resuming patch work; an empty directory must not turn into successful job
completion. Test recovery after deleting all local import files.

The `namespace` column distinguishes the main archive from temporary bootstrap
live data. Preserve the existing lifecycle and per-namespace sequence/cursor
keys; a global `archives.active_namespace` must not imply only one producer
namespace exists during bootstrap. Merge still replays/filter-assigns rows
into the main archive, rather than renaming source recipes into place. Commit
its source progress only with/after the covering destination data, and retire
sources only after the durable phase transition makes recovery independent of
them. Preserve retry/sync marker ordering and the pre-serve compaction gate.

The first migration tool supports only a quiescent steady-state archive and
rejects other states. That restriction is about migration, not normal runtime
support: fresh disaggregated bootstrap, merge, steady repair, and restart must
work before the backend is production-ready.

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
life of the response. The intended ETag shape remains the segment-format
checksum for `getSegment`, and that checksum plus block number for `getBlock`,
with new generations carrying the content-root extension above. Never splice
references from two generations in one response.

There is no new client replanning requirement. Compaction preserves block
numbers and historical envelopes, removes superseded rows, and retains
markers; timestamp import changes display timestamps without moving rows.
An earlier plan therefore remains usable against the current generation for
retrievable matching data. The current client can restart a whole-segment
download when a Range validator changes; the backend must not assume it checks
every response against a planned checksum or replans the archive.

The client's existing bounded retries and surfaced download errors remain its
contract. Test those behaviors against remote outages, but do not describe
exhausted retries as automatic recovery of a failed page. Any change to client
error continuation is separate from this storage design.

HEAD and GET share the same recipe resolution, validation, validators, and
content length. HEAD must not fetch every frame or allocate a segment-sized
buffer. Persist a generation modification time for consistent Last-Modified
behavior across processes, but never rely on second-resolution dates to
identify two rewrites within one second; strong ETags govern Range splicing.

After choosing that recipe, `getBlock`:

1. checks the local content-addressed cache;
2. otherwise GETs the known S3 key with bounded retries;
3. verifies length and SHA-256 over the complete compressed object before
   decoding or returning any of its bytes;
4. populates the cache atomically.

`getSegment` initially streams a `.jss` without first writing the whole file to
disk. It sends the header, generated length prefixes, blocks, and footer in
order. Blocks can be fetched in parallel, but memory stays bounded and output
remains ordered. Content length, ETag, corruption checks, Range, and conditional
requests keep their current behavior. A Range request fetches only the objects
that overlap the requested bytes, but fetches/verifies each overlapping object
in full unless it is already verified in cache; an arbitrary S3 byte range
cannot be checked against a whole-object digest. Caching a complete `.jss` is
optional. Bound multi-range requests, prefetch bytes, and outstanding fetches;
cancel work promptly when a client disconnects. If a later fetch fails after
HTTP headers were sent, terminate the incomplete response, never append an
XRPC JSON error to segment bytes or report a successful truncated body.

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

Cold subscribe replay on the writer uses the same recipes and retains the
existing shared decoded-block cache and shared `Entry` encodings. Key remote
immutable blocks by content hash, with generation-specific envelopes held
separately. A compressed-object cache alone would still decode every cold
subscriber's traffic repeatedly.

Route both websocket endpoints, cursor resolution, and import-control APIs to
the current writer; route archive planning/downloads to any healthy reader.
Keep this routing internal to the deployment so clients still use one host.
Reader-only processes do not pretend to offer a hot tail from polled committed
blocks. They return the existing readiness error if a writer-only endpoint
reaches them. This avoids a new inter-process live fanout or delayed cutover
protocol while preserving today's sub-block delivery latency.

### Consistent plans without replica routing

Use the PostgreSQL primary for all catalog/control reads in the first release.
Reader processes still scale HTTP decoding, caches, and S3 egress independently
of the writer. A lagging SQL replica adds no correctness benefit and introduces
coverage and deletion races; defer it until primary query load requires it.

Use a short `REPEATABLE READ` transaction (or one equivalent tested query) to
capture a plan or bounded cold-replay view: current generations, committed
active blocks, coverage frontier, and registered vacancies together. Copy the
needed immutable references, then close the transaction before S3 I/O. Keeping
an SQL snapshot open across a slow download harms vacuum and takeover.

Stable block topology and historical envelopes keep plans usable across
rewrites; fixed `sealedTipSeq` prevents new seals from moving the requested
window. Notifications may prompt cache refresh, but a missed notification
cannot authorize skipping coverage or keep a mutable current pointer stale
indefinitely. Resolve current pointers on the primary for each request; cache
immutable metadata by generation identity. A cold replay batch must prove
coverage to its captured durable stop, not to a later hot-log floor sampled
from another instant. Preserve gap validation and fail on unexplained holes.

There is no new client planning token, automatic replan assumption, replication
watermark, or reader election. Any future SQL replica support needs its own
proof for active/sealed coverage, stale views, retention, and cutover.

### Failover and fencing

A candidate writer performs this startup sequence:

1. Open PostgreSQL and validate archive identity/schema/configuration.
2. Acquire the archive advisory lock on one dedicated session.
3. Read and advance the S3 sequence ceiling as described below.
4. In a transaction, acquire the archive control row lock before reading the
   durable sequence frontier. Validate coverage, register abandoned remote
   sequence space, increment `writer_epoch`, and record the new identity,
   grant, ceiling ETag, and reservation ID. Hold the row lock through commit.
5. Rebuild active writer state, block descriptors, gaps, and the readable-log
   start from PostgreSQL. Verify the active tail's referenced objects before
   accepting input. Restore the timestamp rule lookup state required by the
   catalog before accepting events that need stamping.
6. Start producers and only then advertise write/live readiness.

If the lock connection is lost, a fence check fails, or commits stop working,
the process becomes write-unready and stops its producers. It must also stop
publishing new in-memory live events. It may continue serving immutable
history if that separate read path is healthy.

An old writer can have uploaded objects and can have exposed events within its
reserved remote grant, but cannot publish database references after a new
writer takes over. Any objects it uploaded but did not publish become garbage,
and its possibly observed but uncommitted sequence numbers become an explicit
vacancy. New leadership does not scan S3 to infer what happened.

The operator configures PostgreSQL high availability, but Jetstream must test
the result. Losing an acknowledged synchronous transaction violates the
supported HA contract; it is not an ordinary failover. If detected, stop
automatic write recovery and use the database-rollback recovery procedure,
including GC suspension and remote sequence reconciliation. The S3 ceiling
prevents sequence reuse; it cannot recover lost catalog transactions.

## One remote sequence grant for crashes and database rollback

Local mode keeps `seq/max_reserved` as a one-block lease. Remote mode instead
uses one S3 grant to cover **both** pending client-visible events and rollback
of PostgreSQL. Maintaining both independent allocators would add states without
preventing an additional reuse case. The durable coverage frontier `seq/next`
and the shared `seq/gap/*` validation remain necessary in both modes.

Maintain a small, independently conditional S3 object:

```json
{
  "version": 1,
  "archive_id": "...",
  "exclusive_ceiling": 123456789,
  "reservation_id": "..."
}
```

The exclusive ceiling only increases. Update with `If-Match` against the last
ETag; a fresh archive is initialized explicitly with conditional create.
Missing control state on an existing archive is corruption, not permission to
initialize from a database backup. Never use a timestamp to decide ownership.
The authoritative latest object never expires or rolls back. PostgreSQL stores
only the last verified grant and its reservation ID/ETag.

Let `R` be the ceiling read from S3 and `G` the configured grant size:

1. A new writer session acquires the database leadership lock, then reads `R`
   and conditionally replaces it with `R + G` using a fresh reservation ID.
   Verify the successful reservation before use.
2. In the fenced startup transaction, acquire the archive row lock **before**
   reading durable coverage. All old publication transactions have now either
   committed before this lock or will fail their epoch check afterward.
   Validate `next_seq <= R` and every persisted grant/sequence bound against
   the remote ceiling. If `next_seq < R`, register `[next_seq, R)` as a vacancy
   and advance the exact coverage frontier to `R`. Never hide a hole below the
   preexisting durable frontier inside this new gap.
3. Record the new grant end as `seq/max_reserved`, reservation ID, ETag, and
   new epoch atomically. Allocate only in `[R, R+G)` after this commits. All
   non-empty durable block envelopes must remain disjoint from vacancies.
4. Before exhausting the range, the same session conditionally extends its
   **own last verified** ceiling and records the extension in a fenced SQL
   transaction before using it. Serialize renewals. An unexpected control
   value/failed compare ends the writer session; never adopt an intervening
   writer's range or silently skip it inside a pending block.
5. Every new leadership acquisition burns the unused tail of the previous
   remote grant, even after graceful shutdown. Never lower S3's ceiling or
   resume an old grant. Empty restarts and failed startup attempts may burn
   ranges too; this is bounded waste of seq values, not data loss.

The main archive allocator uses this rule during bootstrap as well as steady
state, so its eventual public seqs are covered. Temporary bootstrap-live seqs
remain private to their namespace and are reassigned during merge; they must
never be served as main-archive cursors. Test both allocators and lifecycle
transitions explicitly.

A successful S3 reservation followed by a failed SQL transaction just burns
space. A timed-out conditional PUT can still finish later: do not infer failure
from one GET that sees the old value. Retry/reconcile using the same expected
ETag and reservation ID, or abandon the session and conditionally acquire a
fresh grant through startup. A successor's successful CAS invalidates the old
operation's precondition. Only a positively established, fenced grant may be
used; otherwise remain write-unready.

A stale writer might reserve more space in S3, but it cannot record that
extension under a revoked database epoch. Its previously observed events lie
below the ceiling the new writer burns. An old session must not publish new
in-memory events after detecting authority loss; fencing cannot retroactively
recall events already delivered. No per-event database authority check is
required for sequence uniqueness.

After PITR, for example, S3 might say `R = 1000000` while PostgreSQL says
`next_seq = 990000`. Startup reserves above `R`, records `[990000,1000000)`,
and starts at 1000000. Upstream replay may duplicate events with new seqs,
which the existing contract allows; it cannot give an observed seq to a
different event. S3's ceiling does not reconstruct lost catalog transactions
or prove the upstream still retains the lost interval: PITR is explicit
recovery, not a zero-data-loss failover guarantee.

Choose `G` and its renewal threshold for typical live traffic and the supported,
possibly rate-limited backfill/PDS-recovery rate. Check arithmetic and stop before
the shared `1e15` cursor namespace limit. Alert on burned ranges and remaining space.
A large grant reduces control PUTs but increases restart gaps; it does not
permit unbounded buffering or continued ingest when block uploads fail.

If the latest control authority is unavailable, remain write-unready. If it
is lost, recover the latest ceiling without rollback or deliberately establish
a new archive/cursor namespace and force consumers to reset/re-backfill. The
current public API has no negotiated archive UUID, so a new internal UUID at
the same endpoint alone does **not** invalidate clients' saved cursors. Use an
explicit operator/client cutover (for example a new endpoint), never an
invisible namespace reset. An older object version or an arbitrary high number
is not evidence that reuse is safe.

## Garbage collection and retention

Leaking an object wastes money but loses no data. Deleting one too early can
make the archive unreadable. Garbage collection (GC) is therefore conservative
and delayed.

An object is live if referenced by any of:

- an active segment block;
- a generation that is current or still within the HTTP cache grace period;
- an unexpired bounded read view or an explicit long-job hold;
- a database recovery point within the supported PITR window;
- an explicit legal/operational hold.

Use delayed retirement for ordinary readers rather than a durable pin per
request. Every HTTP response, plan snapshot, and cold-replay batch has a hard
maximum view age `D`, starting when it selects current generations on the
primary (including time waiting on S3/queues). Before the bound expires, finish
or cancel; a slow websocket takes a new bounded batch view. SQL selection has
its own short deadline. A process suspended past the deadline must revalidate
before fetching or exposing more data. Immutable cached bytes may survive
longer, but a cached old current-pointer is not a fresh view.

Retired generations remain reachable for at least `D` plus a conservative
clock/operational margin and the PITR protection below. Retirement cannot
precede a current-view selection that still names that generation; bound the
selection transaction too. That gives a response the remote equivalent of
holding today's open file descriptor without a SQL write per request. HTTP
Range retries select a new current generation and use validators as today.
An export/migration/repair that must retain an exact view longer than `D`
creates an explicit durable hold before releasing its selection transaction.
Unreleased holds leak storage safely and are visible to operators. Ordinary
rewrite work stays with the single fenced writer; no job needs to hold a
transaction open while reading S3.

When the last ordinary reference disappears, set `unreferenced_at` and
`delete_after`; do not delete immediately. The minimum delay is:

```text
max PostgreSQL PITR window
+ max(maximum read-view age D, HTTP cache/client retry grace)
+ cross-region replication or backup lag
+ operational/clock safety margin
```

This coupling is mandatory. Restoring PostgreSQL to yesterday while S3 has
already deleted objects referenced yesterday produces an internally consistent
database pointing at missing bytes. Jetstream startup should report the
configured PITR and object-retention assumptions and alert when they diverge.
If retention or time-safety assumptions cannot be established, suspend deletion
and alert; leaking objects is the safe degradation. Extending the PITR window
requires retaining enough older objects first, not merely changing a setting.

Deletion protocol:

1. The current writer's GC task selects expired candidates in small batches.
2. In a short fenced transaction, take the archive row and then object locks
   in the same order as publication, recompute that no retained
   reference exists, and move it to `gc_claimed` with a unique claim recording
   the exact provider version ID. Reference publication must lock that same
   object row while checking availability and attaching a reference, so the
   availability check cannot race a deletion claim.
3. Publishers encountering `gc_claimed` must not create a reference; they
   either wait or re-upload/verify and transactionally return it to
   `available` after the claim resolves.
4. Delete only the claimed version ID. Key-only DELETE is forbidden: a request
   that times out may finish after a retry and a later reupload of the same
   hash. Every retry must still target the original version. Other retained
   versions follow the disaster-recovery policy.
5. In a fenced transaction, mark the row deleted only if its claim ID and
   provider version still match.
   A retry checks the exact version's state and continues from the last
   completed step; it cannot mark a later upload deleted.

GET/HEAD use the recorded version ID as well as the hash key; otherwise a
current delete marker or a later upload could change the object observed by a
recipe. A stale upload that completes after its row was abandoned cannot be
published without a fresh availability check and current epoch. Orphan cleanup
uses the same delayed/version-specific rules, including objects discovered
only by inventory after a database restore.

Never attach blanket expiration to current or noncurrent versions under
`objects/`. An old version can still be referenced by the catalog or a supported
PITR point; age alone does not prove it is dead. The first release uses explicit
version-specific GC for all such versions. A lifecycle rule may abort incomplete
multipart uploads, whose bytes are not visible objects. S3 Inventory compares
physical objects with catalog rows and finds unexpected orphans; discrepancies
produce reports, not automatic catalog reconstruction.

The separate `imports/` keyspace is permanently retained. The GC credential has
no delete permission there, and no lifecycle expiration applies to those
inputs. Restoring an older PostgreSQL catalog also requires suspending GC and
retiring old workers, as described under disaster recovery.

Old generation rows can be removed only after the object-retention and PITR
window ends. Recipes may be kept longer for audit, but they must clearly show
when their objects are no longer available.

## Caching and local resource behavior

The disaggregated backend may use local SSD for:

- a size-bounded content-addressed block/footer cache;
- bounded temporary rewrite/import files;
- the derived timestamp rule index, with its own provisioned size budget and
  rebuild requirement;
- optional materialized `.jss` generation cache;
- multipart upload buffers when streaming is unavailable.

The cache is never a source of truth. Cache files are named by SHA-256. Write
them to temporary files, verify them, and rename them atomically. No cache
fsync is required for correctness: revalidate entries after restart and refetch
truncated/corrupt ones. Do not add an authoritative local durability path for
cache survival. Eviction cannot lose archive
data. A corrupt cache entry is deleted and fetched again. Repeated cache
corruption makes the service unready and triggers an alert; confirmed
corruption in authoritative S3 data follows the fail-loud policy above.

Use request coalescing so concurrent misses for one hash cause one S3 GET.
Start with explicit byte/concurrency budgets for ingest uploads, foreground
reads, and maintenance I/O. A shared global cap must prevent multiplying
memory use across worker pools. Prefetch consumes the relevant budget; it is
not another unbounded queue. Expose wait time and throttling before adding a
more elaborate priority scheduler.

Disk-full behavior should shed cache/staging work and remain able to serve via
streaming where possible. It must not be confused with authoritative archive
loss. Losing a required timestamp rule index delays writer readiness until it
can be rebuilt; do not ingest unstamped events as a fallback. Local mode
retains its existing crash-loud disk semantics.

## Database choice

### PostgreSQL (selected)

PostgreSQL provides the features this design needs:

- atomic transactions across existing key/value metadata, catalog rows,
  writer state, and generation publication;
- conditional updates, constraints, advisory locks, and fencing rows;
- ordered `bytea` keys and efficient point/range access;
- COPY/bulk operations for migration/import;
- mature managed Multi-AZ offerings, PITR, metrics, and operational knowledge;
- a plausible fit for the measured workload if round trips are batched;
  throughput under repair and retention-window catalog growth remains a gate,
  not an established property of the selected service.

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

Extract narrow seams at the actual callers, not a replacement for all Pebble
or POSIX APIs. `internal/store.Store` currently embeds `*pebble.DB`, and callers
use `*pebble.Batch`, iterators, snapshots, and package-local read/modify/write
locks. Retaining key encodings is useful but is not a drop-in SQL adapter.
Audit each caller's ordering, snapshot lifetime, ownership of returned bytes,
and dependent-state eligibility. Local mode keeps its direct implementation
behind the necessary seams and its existing fault hooks.

The required contracts are:

| Operation | Responsibility |
| --- | --- |
| Publish a prepared block | Its reference and exactly eligible metadata in one durability boundary; epoch/grant guards belong here. |
| Commit independent metadata | Reject dependent mutations; preserve ordered range/prefix scans and atomic batches. |
| Seal or replace a generation | Expected source/tail check, immutable recipe, reference retention, and progress in one transaction. |
| Capture a read view | Consistent sealed/active coverage and vacancies; release SQL before remote I/O. |
| Ensure/read an object | Immutable versioned bytes with size/hash checks; no authority to publish archive metadata. |

Use one backend implementation per mode and a narrow S3 adapter. Reuse the
public `segment` encoder, decoder, footer builders, and filters. Teach shared
format code to accept exact frames/metadata without teaching it SQL or S3.
A verified-read API must verify a bounded object **before returning bytes**;
a stream that reports a hash mismatch only at EOF is unsafe for HTTP egress.
Large import input staging may stream to disk, but jobs cannot activate it
until verification completes.

A virtual read-only `.jss` byte view can reuse `http.ServeContent` semantics
without pretending to support filesystem writes. Keep local append/fsync and
remote object/publication protocols explicit. Share invariant logic and
transforms where that reduces duplication; do not force both backends through
a large generic storage framework merely to make their types look alike.

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
| S3 PUT unavailable | Retry within a bounded budget; stop intake when pending bytes/blocks reach their cap. No block or dependent metadata commits without verified objects. |
| PostgreSQL unavailable before transaction | Uploaded object may be orphaned; no durable block is claimed. Stop metadata-dependent intake. |
| PostgreSQL commit result is unknown | End the writer session and recover through leadership startup. The archive-row lock orders recovery after old transactions; do not replay prepared work under a new epoch. |
| S3 GET unavailable | Cached reads continue. Cache misses return a retryable error instead of skipping data. Writes stop when newly uploaded objects cannot be verified. |
| Object hash mismatch or referenced object missing | Discard a bad cache copy and retry the exact S3 version a bounded number of times. Confirmed owned-data corruption fails loud; distinguish it from transport/permission failures. Never skip the block. |
| Writer process/pod loss | A new process takes leadership, advances the remote ceiling, records abandoned sequence space, verifies the active tail, and resumes without downloading the archive. |
| Archive cache loss | Cache misses fall back to S3; performance may degrade. No archive data is lost. |
| All local workspace lost, including required rule state | Rebuild from the catalog and retained S3 inputs before writer readiness. Reads may continue without the rule index. |
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

This procedure is for restoring an older database backup or recovery point,
not an ordinary writer restart against the same current catalog. The restored
catalog can make an old generation current again. A GC worker using the
abandoned catalog must not remain able to delete that generation later, even
if the retention window protects it at the moment restoration begins.

Recovery procedure:

1. Suspend object deletion and protect the selected recovery point for the
   entire recovery operation. Fence or prove the old writer and all old GC
   workers are gone. Account for outstanding deletion requests; changing
   PostgreSQL ownership does not cancel an S3 request already in flight.
2. Restore PostgreSQL to the chosen supported point.
3. Use the retained or replicated S3 objects. Verify every active recipe and a
   sample of sealed recipes. A full check may run in the background only if the
   operator accepts the risk of opening sooner.
4. Read and advance the original sequence-control object, then record the gap
   created by the restore.
5. Confirm that upstream still retains the saved relay cursor needed for
   catch-up. If it does not, stop recovery for an explicit operator decision;
   jumping to the relay tip or fetching current repos cannot recreate lost
   intermediate events and is not lossless recovery.
6. Reconstruct timestamp rule state and any needed import workspace from the
   retained inputs and restored catalog. Catch up, verify invariants, then
   open write and live readiness.
7. Resume GC only against the newly authoritative catalog after recovery holds
   and outstanding deletion work have been reconciled. Old-cluster workers
   must remain unable to delete objects in the shared bucket.

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

Support quiescent steady-state migration first; reject bootstrap, merge, and
active imports. Migration must preserve exact bytes and have one authority.

1. Provision schema, bucket, archive ID, credentials, and retention. Keep the
   target archive explicitly unready with no remote writer or GC enabled.
2. While local mode is authoritative, open sealed file descriptors, parse/hash
   their exact headers/frames/footers, upload objects, and bulk-stage catalog
   rows. Identify bytes by hashes, not a mutable filename or legacy xxh3 alone.
   A concurrent rename leaves the descriptor stable, but the final inventory
   must still discover the replacement. Verify reconstruction. Stage metadata
   in bounded transactions/COPY batches behind the unready archive gate.
3. Enter the maintenance window. Stop every local producer and maintenance
   task, drain and seal the active writer, and quiesce the store. Export the
   exact current segment inventory and a consistent metadata snapshot. Record
   the maximum possibly observed seq from the frontier and reservation state;
   a failed drain must not erase the old lease. Reconcile staged generations
   against this **final** inventory, including compactions/imports during the
   bulk-copy phase, not just newly named segments.
4. Export the separate `import-rules` database as an exact, verified, immutable
   baseline in the permanently retained import keyspace. Always take this
   baseline: existing failed jobs can have partially installed rules with no
   recoverable activation journal. Retain the original CSVs that still exist,
   but do not infer effective rules from job status or archived timestamps.
   New remote activations replay after the baseline; old inputs must not be
   replayed over it and change the captured state.
5. Upload/stage the remaining data and metadata in bounded transactions. Check
   completeness, coverage, object references, rules, and final inventory against
   the frozen source. No enormous transaction needs to COPY tens of millions
   of metadata rows while holding the publication lock: only a final checked
   transaction marks the already-loaded archive ready for promotion. A crash
   before that marker leaves an unservable, resumable migration.
6. Initialize/advance the remote sequence ceiling beyond every seq the local
   writer may have exposed. Start the remote writer through normal fenced
   startup, which records abandoned space before accepting input. Validate a
   read-only serving view and rule reconstruction before reopening public
   traffic and producers. Never run both writers for comparison.
7. Retain the stopped local directory read-only through the rollback safety
   period. It is evidence, not a writable replica.

Once the remote writer has exposed new seqs, returning to local mode requires
an offline export at one quiescent catalog state, including the rule baseline
plus later activations and all possibly observed remote reservations. Materialize
current `.jss` generations, export compatible metadata, register abandoned
remote space, and then resume the local one-block lease protocol. Changing an
environment variable or restarting the old local directory is not rollback.

### Delivery stages

1. **Format support:** implement and compatibility-test the reserved-header
   content root in shared format code. Extract and reconstruct exact frames,
   headers, and footers for each supported version; validate recipes.
2. **Storage contracts:** adapt local mode through invariant-level interfaces;
   preserve its behavioral expectations, oracle assertion strength, and
   applicable coverage.
3. **PostgreSQL metadata compatibility:** key/value encoding and iteration
   tests, schema, migrations, fencing, and transaction failure tests.
4. **S3 object layer:** conditional creates, checksum verification, retries,
   cache, inventory audit, and real-provider behavior tests.
5. **Disaggregated ingest:** serial block upload/commit, existing metadata
   batching, rotation, remote grants, failover, and active cold replay. Add a
   concurrent pipeline only if measured service/recovery targets require it.
6. **Read APIs:** checksum-consistent plans, generation-consistent segment
   streaming, primary-backed reader processes, and load tests.
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
- **Can sequence allocation continue safely?** Show `seq/next`, remote grant
  end/headroom, reservation attempts, burned ranges, and remaining namespace.
  Local mode continues to report its one-block lease.
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
  remaining remote sequence range, successful S3 writes and checks, a verified
  active tail, and reconstructed timestamp rule state required by the catalog;
- archive-read readiness requires catalog access plus S3 or sufficient cache;
- live readiness requires writer readiness, its readable log, and lifecycle
  cutover;
- reader-only processes never advertise writer/live readiness; the deployment
  routes those endpoints to the current writer.

The status endpoint should make the backend and degraded modes obvious without
exposing secrets.

## Verification strategy

### Crash and delayed-operation tests for each protocol step

Build fake S3 and database implementations. Add crashpoints before and after
every step that changes persistent state:

- object registration, PUT, timeout, verification, and availability mark;
- SQL begin, fence assertion, block row, each metadata class, commit send,
  ambiguous response, and commit acknowledgement;
- active-tail check, footer upload, inline header/generation insert, pointer
  change, old visibility close, watermark/job progress, and notification;
- advisory lock loss, PostgreSQL disconnect, S3 ceiling conditional update,
  ambiguous reservation, vacancy registration, and new epoch;
- GC mark, claim, final reference check, S3 delete, and row completion.

After every injected crash, each atomic catalog transaction must be either
absent or complete; verified but unpublished objects may remain. Multi-step
jobs may retain completed batches, and sequence reservations may be burned.
Restart must never find a missing referenced object, metadata ahead of data,
a duplicate block number, a reused sequence, an advanced watermark without
its generations, or two valid writers.

Timeouts also need tests in which the original operation remains alive and
finishes later. Use barriers to exercise at least:

- a pooled transaction paused around its fence check while the dedicated
  leadership session drops and another writer attempts promotion;
- a timed-out DELETE completing after a successful retry, reupload of the
  same hash, and publication of a new reference;
- one account tombstone affecting more segments than a publication batch,
  with crashes between batches and before the global watermark advances;
- a replacement writer with all local rule and bucket files removed, including
  a persisted `Bucketed` job and a completed import that must stamp new events;
- a stalled block upload with bounded producer backpressure and safe metadata;
  if a concurrent pipeline is added, also test out-of-order uploads, multiple
  pending client-visible blocks, and rejection of later-block metadata committed
  early; crash/restore afterward and prove every observed seq remains unique;
- a timed-out ceiling CAS finishing after a later GET, renewal racing takeover,
  and repeated empty/clean restarts; no path may resume an old grant;
- rule activation committed in SQL before local install, a failed import's
  partial prefix, and a cached rule index ahead of a PITR-restored catalog;
- a response or cold-replay batch suspended past its maximum view age while
  retirement/GC runs, plus a long export protected by an explicit hold;
- an older database restore while an old GC worker still tries to delete
  generations that are current in the restored catalog.

For compaction, a valid intermediate crash state includes some published
replacements and the old watermark. The next pass must finish the window.
Every configured fault or pause must be observed, so an unexercised fault path
cannot make a scenario pass.

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
- Change an imported timestamp without changing the compressed frame length;
  prove the corrected checksum and HTTP validators change, and Range requests
  cannot combine generations unnoticed. Cover legacy-format compatibility.
- Verify partial HTTP ranges only from fully verified objects; cover HEAD,
  multi-range requests, cancellation, corrupt cache contents after restart,
  and an S3 failure after the HTTP response has already started.
- Compare reconstructed rule lookup results with the source rule database,
  including import order, duplicate paths, version-specific rules, and
  interrupted imports restored to an earlier catalog point.

### Oracle and mutation campaign

The existing local-storage oracle remains fully supported with its current
correctness properties and coverage. Adding disaggregated mode must not weaken
local assertions, skip local fault/restart/power-loss tiers, reduce detection
of applicable mutants, or introduce cloud-service requirements into local
tests. This applies especially to refactors of shared ingest, segment, and
storage interfaces. See `specs/oracle.md` for the preservation requirement.

Reuse the simulator's independent world model, expected event history,
fold-convergence checks, and public client observers for both backends. The
existing filesystem observer, strict-memory-filesystem power-loss tests, and
Pebble fault hooks cannot simply be run against PostgreSQL/S3. Keep those local
tiers and add remote equivalents for their applicable guarantees. Remote
coverage should follow the local oracle's independent-model, fault-accounting,
and mutation-testing principles even where the harness differs. Share code
where useful; separate backend-specific harnesses are acceptable and must not
force either backend down to a weaker common set of assertions.

Remote durability tests should run the real runtime and SQL/object adapters
against disposable services, kill and replace writer processes, and inspect
catalog plus object bytes independently of the production read path. Use
controllable proxies or service fault seams for delayed responses and
partitions. Fakes exercise protocol state transitions cheaply; real-service
tests establish transaction, session-lock, and object-version behavior. Heavy
failover, retention, and restore drills belong in explicit test recipes rather
than making every short test depend on cloud services.

Add seeded scenarios for:

- crash in every block/seal/publication stage;
- database failover and inconsistent/stale cached catalog views;
- S3 delay, throttling, lost responses, corrupt cache/provider bytes, and
  missing referenced objects;
- attempts to run two writers and the resulting fencing;
- database PITR behind client-observed seqs;
- compaction/import racing plans and long-lived cold replay;
- bootstrap, merge, repair, and relay replay across failover;
- GC racing dedup/republication and PITR-window retention.

Exercise the current client under transient and exhausted download retries,
generation changes, and archive-to-live cutover. Assert its documented error
behavior as well as successful delivery; do not invent automatic replanning
or silently weaken the independent server data-preservation checks.

Add deliberate test mutations that remove or reorder upload checks, writer
fences, atomic block-and-cursor commit, remote ceiling updates, gap
registration, source-generation checks, bounded-view retention, and GC's final
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

Run restore drills against isolated database/object/control copies, with no
write/delete access to production. Merely changing the archive UUID or restoring
the database does not isolate S3 control/GC effects. A read-only audit may use
production object versions with read-only credentials. Never let a drill
advance the production sequence ceiling, delete production objects, or claim
the real writer's ownership.

### Performance and cost requirements

Measure all of the following before production use:

- typical steady-state ingest using pop1 as the baseline, with headroom and
  serial PUT/verification/synchronous SQL publication; report pending-byte caps,
  oldest-pending-block age, and live-delivery latency at flush boundaries;
- exceptional large-PDS recovery using pop2 as the stress input: report producer
  throttling, live-ingest fairness, saved-cursor age, and completion time. Matching
  pop2's local peak is not required unless the agreed recovery target demands it;
  benchmark a bounded pipeline only if the serial path misses those targets;
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
- retained generation-block rows, footer/active-descriptor bytes, indexes, WAL,
  and cleanup throughput over a full retention window of rewrites. A generation with
  1,000 blocks currently adds 1,000 new reference rows even if just one block
  changes in S3; object reuse does not imply catalog-row reuse;
- retained timestamp rule count/key bytes, local-index disk and scratch needs,
  per-chunk activation pause, and cold reconstruction from baseline/CSV inputs;
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

## Remaining decisions and measurements

The first implementation's mechanisms are selected above. Remaining work is
to validate compatibility and choose deployment budgets:

1. RDS PostgreSQL baseline sizing, pool sizes, publication batch limits,
   transaction timeouts, and catalog indexes/cleanup over a full retention
   window. Aurora or other S3-compatible providers are later validation work,
   not a requirement to build a portable provider framework up front.
2. Remote grant size/renewal threshold, byte caps, read concurrency, retry
   deadlines, and typical steady-state latency targets. Separately set an
   acceptable completion time and producer rate limit for exceptional PDS
   recovery/backfill. Add upload concurrency only if the serial path fails those
   targets; never hide insufficient throughput behind an unbounded queue.
3. Cache and derived rule-index disk budgets, rule activation pause/rebuild
   time, and maximum read-view age `D`. Whole-file/pack caches, SQL replicas,
   and remote rule-index checkpoints require measurements before being added.
4. PITR/object-retention windows, maximum HTTP grace, safety margins, and
   generation/job-hold cleanup. Validate the whole policy with delayed deletes
   and an actual restore, including the time spent recovering.
5. Supported-client compatibility for the reserved-header content root,
   legacy/extended fixtures, and exact reconstruction. Sparse opening must
   remain cheap and legacy files must remain readable without conversion.
6. Migration outage/RTO/RPO targets, relay replay retention, and database HA
   settings. Full verification and baseline rule export must fit the operational
   budget; a source change journal is deferred until measured outage requires it.

No optimization may weaken upload-before-reference, atomic block-and-metadata
commit, writer fencing, immutable generations, rollback protection, or
PITR-aware garbage collection. Failures of these gates keep the backend
experimental; they are not reasons to lower the assertions.

## Implementation exit criteria

The backend is production-ready only when:

- local mode retains the existing oracle's assertion strength, applicable
  tiers, and mutation-backed detection, remains green, and stays within its
  performance baseline; passing after weakening or skipping checks does not
  satisfy this criterion;
- the applicable independent oracle expectations pass through remote storage
  and real-service tests, including restart, failover, compaction, import,
  bootstrap/merge, and archive-to-live cutover; local-only test mechanisms have
  explicit remote coverage rather than being silently dropped;
- mutation tests catch violations of the new write-order, fencing, and GC
  rules;
- a supported S3 service and PostgreSQL HA setup pass behavior and forced
  failover tests;
- an existing production-sized archive migrates and reconstructs with exact
  recipe/file/event comparisons;
- database PITR is restored against retained objects and no seq is reused;
- timestamp inputs remain available permanently, rule state survives migration
  and total local-disk loss, and missing import workspace cannot report false
  completion;
- corrected segment checksums cover timestamp changes and pass existing-format
  and client compatibility checks;
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
- `specs/client.md` and `segmentfetch.go` — pagination, bounded retries, and
  Range/ETag download behavior.
- `specs/gotchas.md` and `internal/timestamp/rules.go` — incremental rule ingest
  and terminal-failure remediation that remote reconstruction must retain.
- `specs/oracle.md` — independent correctness checks and preservation of local
  fault, restart, power-loss, and mutation coverage.
- `segment/doc.go` — `.jss` block/header/footer format behavior.
- `segment/header.go`, `reader.go`, `seal.go`, `rewrite.go`, and `patch.go` —
  checksum experiment and existing full-segment index-building behavior.
- `internal/ingest/async_flush.go`, `writer.go`, and
  `internal/ingest/orchestrator/compact_deletes.go` — ordered flush publication,
  dependency eligibility, force rotation, and tombstone chunk watermarks.
- `specs/notes/2026-08-25-seq-reuse-after-crash.md` — current local sequence
  lease and registered-vacancy protocol.
- [Amazon S3 data consistency model](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel)
- [Amazon S3 conditional requests](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html)
- [Amazon S3 checking object integrity](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html)
- [PostgreSQL synchronous commit](https://www.postgresql.org/docs/current/runtime-config-wal.html#GUC-SYNCHRONOUS-COMMIT)
- [PostgreSQL advisory locks](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS)
- [PostgreSQL continuous archiving and PITR](https://www.postgresql.org/docs/current/continuous-archiving.html)
