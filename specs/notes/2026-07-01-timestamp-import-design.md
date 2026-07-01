# Timestamp import + witnessed/indexed timestamp rename

Date: 2026-07-01
Branch: `timestamp-import-design` (design); impl branches per milestone (§9)
Tracking issue: [#193](https://github.com/bluesky-social/jetstream/issues/193)
Status: design DONE + §8 rewritten; impl M0–M6 DONE (rename + display resolver + segment.Patch + Phase A/B parse+bucket + Phase C apply wired through the compactor + bearer-gated XRPC job model with durable resume, status panel, and metrics), M7 (docs close-out) in progress

> **M4 revised two decisions during implementation** (recorded in place below):
> **Q-FORMAT** dropped zstd — the import CSV is now **plain (uncompressed)** so
> it is randomly seekable, which lets Phase B store per-row byte offsets and
> Phase C `Seek`+decode a single row with no decompression subsystem and no
> scratch copy. **Phase B grouping** uses a bounded generation-gated
> DID→candidate-segments LRU absorber (mirroring the #188 LiveEnqueuer) instead
> of an external merge sort, and the per-segment intermediate is an **offset
> file** (packed `uint64` offsets into the plain CSV), not a re-emitted
> patch-tuple file. See §3.2 / §4a / Q-FORMAT / Q-SORT / Q-RESUME.
Author: jcalabro (with Claude)

> This is the **living document** for **§8 Timestamp Import** of `docs/README.md`.
> §0–§8 capture the decisions and their rationale (all settled). **§9 is the
> implementation plan / progress tracker — update it as we go.** The `docs/README.md`
> §8 prose has already been rewritten to match; remaining doc touches (§3.2, §3.1.2,
> §5.1 terminology) land with the rename milestone (§9, M1).

---

## 0. Problem statement

Not every atproto lexicon has a `createdAt` field, and where it exists it is
client-supplied and therefore spoofable. Production AppViews (Bluesky's
"Atlantis" dataplane, on Scylla) work around this by computing a `sorted_at`:

```go
func GetSortedAt(createdAt time.Time, indexedAt time.Time) time.Time {
    if createdAt.IsZero() {
        return indexedAt
    }
    if createdAt.Before(indexedAt) {
        return createdAt
    }
    return indexedAt
}
```

The load-bearing input is `indexed_at`: the wall-clock time the *indexer* first
witnessed the record. Atlantis has run since late 2022, so it holds a faithful
`indexed_at` for the whole history of the network. Jetstream is new and has not
run in production. If a freshly stood-up jetstream trusts its own ingestion
wall-clock as the displayed timestamp, every historical record looks like it was
created *today* (the day of backfill), which is unacceptable for building a
real AppView.

Atlantis only stores some collections (all `app.bsky.*`, plus a few like
`site.standard.*`); it does **not** have historical timestamps for every lexicon
jetstream ingests. So imported coverage is inherently partial, and records with
no imported value must fall back to jetstream's own witnessed time.

**Scope of this doc:** (1) a one-time / occasional bulk **timestamp import** so a
jetstream operator can carry over an authoritative historical display timestamp;
(2) the **timestamp-column rename** that makes the two timestamps' meanings
unambiguous and keeps the import from corrupting anything ordering depends on.

### 0.1 The creative-alternatives detour (why we still import)

We explored whether jetstream could avoid a load-bearing witness timestamp
entirely — some *verifiable* notion of "when was this record first seen" that
doesn't require trusting an operator's import. Conclusion: **no trustless
absolute witnessed time is possible without a new trust anchor.** The only
time-bearing, signature-covered on-protocol data is `createdAt` (spoofable) and
`rev` (a TID — monotonic within a repo and covered by the commit signature, so
good for *ordering* and a decent creation-time fallback, but the wall-clock it
encodes is still whatever the PDS chose, so not a trustworthy clock).

Two sane trust anchors exist, both **out of scope** here:

1. **Signed witness log (CT-style), forward-only.** From launch, jetstream could
   batch `(uri, cid, seq, witnessed_at)` into an append-only log with periodic
   signed tree heads, making *future* witnessed times independently auditable and
   letting independent jetstreams cross-witness. Cannot retroactively cover
   2022–2025. Candidate future epic; noted so we're on record that
   `indexed_at`-as-load-bearing is a known protocol wart with a planned direction.
2. **Signed historical export.** Ship Atlantis's export under Atlantis's key /
   a content-hashed signed manifest, upgrading provenance from "operator asserts"
   to "Atlantis attests." Composes with import; not required for v1.

So the past can only be attested by a party that was present (Atlantis) → we
import. The rest of this doc is the import.

---

## 1. DECIDED: two timestamp columns, renamed

We keep **two** per-row timestamp columns (same column count as the current
on-disk format — this is a rename + semantic reassignment, **not** an add/remove),
and rename them so their meanings are unambiguous:

| New name | Was | Meaning | Mutable? | Monotonic w/ seq? |
|---|---|---|---|---|
| `witnessed_at` | `indexed_at` | Wall-clock time **this** jetstream first saw the event. Firsthand observation. | **No** — immutable | **Yes** |
| `indexed_at` | `rendered_at` (dead) | Operator-overwritable **display** value: "the time callers should show." Defaults to `witnessed_at`. | Yes — via import | No |

Rationale for the names: "witnessed at" is unambiguous ("when I saw it").
"indexed at" is an established industry term recognized as "the value you display
to the user," so reassigning it to the display value matches existing mental
models in other codebases.

### 1.1 Why two columns and not one (the analysis we did)

We seriously considered collapsing to a single overwritable timestamp. Key
findings from auditing every consumer of the timestamps in the tree:

- **`rendered_at` is currently dead.** It exists only in `segment/{event,block,
  writer}.go` as encode↔decode plumbing; **zero** consumers (not on the wire, not
  in cursor logic, not in status). So "keep two" is really "don't delete a column
  you already backfilled," and it costs ≈0 bytes because an un-imported display
  column is a run of sentinel-`0`s that compresses to nearly nothing.
- **`indexed_at` (old) is doing two incompatible jobs today:** the wire display
  value (`encoder.go` `time_us`) *and* the seq-monotonic key that the v1
  timestamp-cursor binary-searches (`cursor.go translateTimeUSToSeq`,
  `manifest.SegmentForTimeUS`), *and* the per-segment/per-block envelope
  (`Header.Min/MaxIndexedAt`, `BlockInfo.Min/MaxIndexedAt`) used by cursor search
  and operator reporting. Importing historical values into that one column would
  shatter the seq-monotonic assumption those searches rely on.
- **Single-column would *mostly* not corrupt the cursor** — because imports only
  ever *lower* a value (Atlantis saw everything no later than jetstream), and the
  segment/block search keys on `Max…At`, which a lowering can't move (the max row
  is the newest-witnessed ≈ now, which Atlantis also saw ≈ now). The intra-block
  scan can only start *too early* (extra old rows — fine under at-least-once +
  fold), never too late. So the cursor is *not* the reason to keep two.
- **The real reasons to keep two are provenance + reversibility + invariant
  hygiene:** jetstream's differentiator (§1.1 goal #3) is being an interrogatable
  witness. Overwriting jetstream's firsthand observation with an operator's
  imported claim, with no way to tell them apart, destroys exactly the data that
  makes jetstream auditable. Two columns keeps "what I saw" (immutable) distinct
  from "what I was told to display" (imported), makes a bad import reversible /
  re-runnable instead of a corruption, and keeps the seq-monotonic invariant true
  *by construction* (import never touches the column any search/envelope uses)
  rather than by a subtle "imports only lower + we only search max" argument that
  a future code change could silently break.

### 1.2 Sentinel-0 fallback (DECIDED)

The display `indexed_at` column stores **`0`** for any row that has not been
imported. Readers/wire resolve:

```
display = (indexed_at_col == 0) ? witnessed_at : indexed_at_col
```

Chosen over eagerly materializing `indexed_at = witnessed_at` at seal because
sentinel-0:

- gives a free "**was this imported?**" provenance bit (status metric, audit,
  re-import idempotency);
- compresses to almost nothing for un-imported segments (run of zeros) vs. two
  near-identical monotonic columns;
- matches the column's already-documented `== 0` convention (`event.go:59`);
- costs only one integer compare per row on read. `0` is a safe sentinel: real
  timestamps are always positive.

The imported/not bit stays **fully internal** — not exposed on the wire or the
status page (§5 Q-EXPOSE).

---

## 2. DECIDED: wire semantics (`time_us`)

Replication is explicitly **out of scope** (the `docs/README.md` §6 replication
section is being rewritten; the extended wire may even be removed in a future
change). That removes the only *hard* requirement for putting `witnessed_at` on
the wire. Resulting model:

| Surface | Value | Status |
|---|---|---|
| simple wire `time_us` | resolved display `indexed_at` = `imported ?: witnessed` | **required** |
| extended wire `witnessed_at` (new field) | raw firsthand witnessed | **optional audit nicety; safe to drop** |
| v1 timestamp-cursor translation | searches `witnessed_at` (internal) | unchanged |
| status / header envelope | `min/max_witnessed_at` (internal) | rename only |

- `time_us = display` is **forced by the feature's purpose**: simple-wire clients
  are AppViews that render the timestamp; if `time_us` stayed on witnessed, an
  imported 2022 post would still show as 2026 and the whole feature would be
  pointless.
- **Pre-import, `display == witnessed` for every row**, so `time_us` is
  byte-identical to today. It only diverges for imported (historical) rows, which
  are all far below the 36h lookback floor.
- **The 36h lookback is unaffected** because the cursor-search column
  (`witnessed_at`) and the wire value (`display`) are *decoupled*. The v1
  `?cursor=<unix-micros>` translation still searches the seq-monotonic
  `witnessed_at`; imported divergence lives only below the floor, where the path
  clamps anyway. Wire value ≠ cursor-search column, and they coincide exactly in
  the live window where the lookback operates.
- `#identity` / `#account` / `#sync` rows have no `(collection, rkey)` → import
  (keyed by AT URI) can never touch them → their display column stays 0 → their
  `time_us` stays witnessed. No special-casing needed.

The `encoder.go` change: the four `TimeUS: evt.IndexedAt` sites (lines ~80, 125,
217, 232) become `TimeUS: displayOf(evt)` applying the sentinel-0 fallback. The Go
struct field `evt.IndexedAt` → `evt.WitnessedAt`; the new display column becomes
`evt.IndexedAt`, keeping code and on-disk names in lockstep.

---

## 3. DECIDED: import mechanics (the parts we're confident about)

### 3.1 Reuse the rewrite machinery, add a *mutate* mode

`segment.Rewrite` is **drop-only** today (`decide(*Event) RowDecision` →
`Keep|Drop`; callers in `internal/ingest/orchestrator/compact_deletes.go`).
Timestamp import is *topology-preserving*: patching the display column changes
**no** block boundaries, seq envelopes, DID membership, per-block blooms, or the
collection index — only block *bodies* and the file checksum. So:

- Add a mutate mode (either `decide` returns `{Keep, Drop, Mutated}` or a sibling
  `segment.Patch(path, mutate func(*Event) bool)`).
- It **must NOT** rebuild blooms / collection index (delete-compaction does; a
  patch pass must not — this is the easy bug to introduce). The witnessed
  envelope (`Min/MaxWitnessedAt`) is **preserved** unchanged, exactly as
  `rewrite.go` already preserves `Min/MaxIndexedAt` across rewrites.
- Early-return when `mutatedCount == 0` (mirrors the existing `rowsDropped == 0`
  skip): makes re-running an already-applied file cheap and the whole op
  idempotent.

### 3.2 Three-phase, disk-backed pipeline (one rewrite per touched segment)

"Bucket by DID" cannot be an in-RAM map — full-network history is billions of
rows. Disk-backed, produces exactly one rewrite per touched segment:

- **Phase A — parse & validate.** Stream the **plain (uncompressed) CSV**;
  parse+validate each row (`uri`, `timestamp`, `scope`, optional `cid`); extract
  the DID. Reject malformed rows at this durable boundary (the #188 lesson:
  reject bad data at the edge so it can't wedge a later pass). Capture each valid
  row's **byte offset** into the CSV (`csv.Reader.InputOffset`) — the plain
  (not zstd) file is randomly seekable, so the offset is all Phase C needs to
  re-read a row.
- **Phase B — assign to segments via blooms.** For each row's DID, resolve its
  candidate sealed segments from the manifest's in-RAM DID blooms
  (`SelectBlocksForDID`; one-sided, no false negatives so no candidate missed)
  and append the row's byte offset to each candidate segment's **offset file**
  (packed `uint64` offsets into the CSV). A bounded, **manifest-generation-gated**
  DID→candidate-segments LRU absorbs the recommended DID-grouped input to ~one
  bloom selection per distinct DID (Q-SORT); a bounded FD pool caps open offset
  files. **Phases A and B are fused into one streaming pass** (Phase B is the
  `OnRow` sink of the Phase A parser). Generation-gating is the correctness core:
  a segment sealed/compacted mid-import invalidates any stale cached selection
  rather than silently misrouting a row.
- **Phase C — patch.** For each segment with an offset file, `Seek` each offset
  in the plain CSV, decode+revalidate that single row into a
  `(did, collection, rkey[, cid])→ts` lookup map, and run **one** mutate-mode
  `segment.Patch`. On success emit `segment_compacted` (reusing the §6
  notification path). *(M5.)*

### 3.3 Concurrency (correctness-critical)

Import rewrites and delete-compaction rewrites both do tmp+fsync+rename on the
same segment files. They **must be serialized per segment through the same
rewrite owner** (the compactor's rewrite lock) — two concurrent renames would
lose one set of writes. Decision: **run import as a compaction-pass variant** so
it shares the lock and the `segment_compacted` notification path for free.

### 3.4 Crash safety / idempotency

Each segment rewrite is atomic (existing tmp+fsync+rename+dir-sync). The overall
multi-segment import is **not** atomic, but every scope is idempotent (re-running
produces zero mutations on already-applied segments → skips rename), so a crash
resumes by re-running rather than restarting. See Q-JOB (§5) for whether we add
explicit segment-granularity checkpointing.

### 3.5 DECIDED: keyed by AT URI, not CID

Uploads are keyed by AT URI (`at://did/collection/rkey`), not CID. URIs embed the
DID, which lets the segment-level DID bloom do almost all the filtering.
CID-keyed imports would force a full scan or a second per-segment CID bloom — not
worth it. Operators with only CIDs resolve them upstream first. (A per-row
`scope=specific_version` may *additionally* supply a CID to disambiguate a
version — see §4 D and Q-SCOPE — but the primary key is always the URI.)

### 3.6 DECIDED: 2-way per-row scope selector

Each row chooses which record version(s) the timestamp applies to, so jetstream
isn't tied to Atlantis's storage semantics (Atlantis keeps only latest for posts,
but full history for e.g. `site.standard.document`; other operators differ):

1. `all_versions` (**default**) — patch every create/update/resync row sharing
   `(did, collection, rkey)`. Matches "sorted_at is stable across edits."
2. `specific_version` — requires `cid`; recompute the CID from the stored raw
   DAG-CBOR payload on disk and patch only the matching row.

**Dropped: `latest` scope.** An earlier draft had a third `latest` scope ("patch
the row that survives compaction"). We are **not** implementing it. Its semantics
are ambiguous under our three-phase pipeline: import runs mutually-exclusive with
(not after) delete-compaction (§3.3, §6 H), so the same `(did, collection, rkey)`
can still have multiple un-compacted materializations spread across sealed
segments at import time. A single global "surviving version" would require a
cross-segment resolution pass the pipeline doesn't have; resolving "highest rev
within each segment" independently would instead patch *one row per candidate
segment* — multiple rows for one path — which is not "latest" in any useful sense.
Rather than ship an ambiguous scope nobody needs yet, we omit it. `all_versions`
covers the common case; `specific_version` covers per-version targeting. If a real
`latest` use case appears, add it then with an explicit cross-segment resolution
design.

### 3.7 DECIDED: there is NO on-disk format change and NO migration

The rename is a **pure code + documentation change**. The on-disk segment format
is byte-for-byte unchanged; **the `version` field is NOT bumped.** All existing
segment files on the workstation and test server remain valid as-is. Verified in
code:

- **Ingest never wrote `rendered_at`.** There is no non-test assignment to
  `RenderedAt` anywhere in `internal/ingest`; the live and backfill paths set only
  `IndexedAt`. So in every existing block, the `rendered_at[]` column is **all
  zeros**.
- **The block body is byte-identical under the rename.** `block.go` writes the
  fixed columns in fixed positions/widths: `seq[]`, then `indexed_at[]` (int64),
  then `rendered_at[]` (int64). The rename moves/resizes/adds/removes nothing.

Old bytes map onto the new meaning with zero rewriting:

| On-disk column (fixed position) | Old name | New name | Value in existing archive |
|---|---|---|---|
| col 1 (int64) | `indexed_at` | **`witnessed_at`** | jetstream ingestion time ✓ |
| col 2 (int64) | `rendered_at` | **`indexed_at`** (display) | `0` = "not imported" sentinel ✓ |

Old col-1 bytes are exactly what `witnessed_at` should hold; old col-2 (all
zeros) is exactly the sentinel-0 that resolves display → witnessed for
un-imported rows. The header `min/max_indexed_at` envelope was computed from
old-`indexed_at` = new-`witnessed_at`, so it is already the correct
`min/max_witnessed_at` envelope. **The existing archive is correct under the new
scheme with no byte changes.**

This is *why* the two-column decision (§1) pays off: keeping both columns makes
the rename free. Collapsing to one column would have forced a real migration.

### 3.8 DECIDED: no format-version bump, no legacy decoder, no active-segment hazard

Because the bytes and their meanings are unchanged, we deliberately **do not** bump
the header `version`:

- No legacy vs. new decoder split — there is only one format, read the same way,
  just with renamed fields in code.
- **No active-segment upgrade hazard.** The mixed-format active-segment problem
  (an old binary's in-flight headerless active segment read by a new binary) only
  exists if the block layout differs across the upgrade. It does not. The new
  binary reads the old binary's in-flight active segment identically. This is the
  entire reason we are NOT bumping `version` — a defensive marker bump would buy
  nothing (bytes/meaning unchanged) while *reintroducing* this hazard.
- No forced re-backfill (the 4.5-day archive stays valid) and nothing to migrate.

---

## 4. DECIDED-ish: import file schema & validation (confirm the leans)

Leaning these; call out any you want changed.

- **D. Row schema:** `uri, timestamp, scope, cid`.
  - `uri` — `at://did/collection/rkey`.
  - `timestamp` — **RFC3339** (legible, atproto-conventional), parsed to unix
    micros internally.
  - `scope` — `all_versions` (default when empty) | `specific_version`.
  - `cid` — required iff `scope=specific_version`; ignored otherwise.
- **Validation (durable boundary):** reject malformed URIs, non-positive
  timestamps, `specific_version` without a parseable CID. **Skip-and-report**
  bad rows with counts-by-reason + a bounded sample + a durable rejects artifact
  (§5 Q-REJECT), never whole-file abort.
- **E. Conflict semantics:** **last-write-wins** within a file and across
  re-runs; re-importing the same value → zero mutations → skip. Import
  **overwrites** an already-imported display value (§5 Q-OVERWRITE — operator is
  authoritative, can fix a bad prior import).
- **F. `specific_version` CID mechanics — DECIDED, see §4a.**

---

## 4a. DECIDED: `specific_version` mechanics

Content-addressing makes this clean and cheap. The segment stores the
**byte-exact DAG-CBOR record payload** (`Event.Payload`): backfill fetches it via
`Store.GetBlock(cid)`, live uses `op.BlockData()` — in both cases the exact bytes
the record's CID addresses. So recomputing the CID reproduces it exactly.
`github.com/jcalabro/atmos/cbor` (already a dep) provides everything:
`ComputeCID(codec, data) CID`, `ParseCIDString(s) (CID, error)`, `CID.Equal`.

**Phase A (parse):** for a `specific_version` row, parse the operator CID **once**
with `ParseCIDString` to validate it at the durable boundary (unparseable CID →
reject the row, per Q-REJECT). *(Implementation note, revised in M4: because the
per-segment intermediate is a byte-offset into the plain CSV — not a re-emitted
patch record — the parsed CID is not separately persisted; Phase C re-reads and
re-parses the one row it seeks to. The parsed `cbor.CID` is 36 bytes via
`CID.Bytes()`, not 33 as an earlier draft stated; irrelevant now that it isn't
stored, but corrected for the record. Re-parsing one already-validated CID per
matched `specific_version` row in Phase C is negligible against that row's block
decompress+recompress.)*

**Phase C (patch), per candidate row in a decompressed block:**

```
// key match (did, collection, rkey) already selected this row, as for all_versions
if patch.scope == specific_version && ev.Kind.IsMaterialization() {
    got := cbor.ComputeCID(cbor.CodecDagCBOR, ev.Payload)  // one SHA-256 over stored bytes
    if patch.cid.Equal(got) {
        ev.IndexedAt = patch.ts                            // patch display column
    }
}
```

Rules and rationale:

- **Match = `(did, collection, rkey)` AND CID.** The path key selects the rows (as
  in `all_versions`); the CID disambiguates *which version* among them.
- **Codec is hard-coded DAG-CBOR (`0x71`).** atproto records are always dag-cbor;
  a raw-codec mismatch simply fails `Equal` → safe no-op.
- **Only materialization rows are considered** (`Kind.IsMaterialization()` =
  Create/Update/CreateResync). Delete rows have no payload; Identity/Account/Sync
  have no `(collection,rkey)`. All correctly skipped.
- **Duplicate-CID rows: patch ALL of them (DECIDED).** A content-identical record
  re-created after deletion has the *same* CID (content-addressed) but a different
  rev/seq. The operator keyed by content, and such rows are indistinguishable by
  that key, so we update the display timestamp on **every** row whose CID matches.
- **Unmatched CID → reported no-op** (Q-REJECT-style report line, not an error):
  the `(uri)` path exists but no row carries that exact CID (version compacted
  away, or never witnessed). Patch nothing; count as "unmatched."
- **Cost is bounded:** one `ComputeCID` (SHA-256 over a few-hundred-byte to
  few-KB payload) only for rows already decompressed for a patch that also match a
  `specific_version` key — negligible against the block decompress+recompress the
  rewrite already pays.

**Operational limitation (document in §8):** `specific_version` requires the
operator to *have* a per-version CID for each row. Sources that keep only the
latest version (e.g. Atlantis's `posts` table stores one `cid`) can't supply it
for historical versions — those collections use `all_versions` (default).
`specific_version` is the opt-in path for sources with full per-version
history + CIDs (e.g. `site.standard.document`), letting each historical edit carry
its own original witnessed time instead of one timestamp smeared across versions.

---

## 5. Resolved forks (decision log — all settled; kept for rationale)

### Q-JOB — job & transport model — **DECIDED: online, in-server, bearer-gated**
**Downtime is not acceptable** (imports must run against a live serving instance),
so the offline-CLI option is rejected. Import runs as an **in-server job**:

- Executes inside the running server so it shares the **compactor rewrite lock**
  (§3.3 — serialize segment rewrites; no race with steady-state compaction) and
  the manifest/serving-metadata refresh + `segment_compacted` notification path.
- **Auth (DECIDED): bearer token.** The import endpoint is gated by a bearer
  token supplied at startup via CLI flag / env var (e.g.
  `--timestamp-import-token` / `JETSTREAM_TIMESTAMP_IMPORT_TOKEN`).
  **Secure by default: if no token is configured, the endpoint is disabled and
  always returns HTTP 401.** Only server admins holding the token may upload or
  modify timestamps. This is jetstream's **first** authenticated surface — no
  admin-auth infra exists today, so we build a minimal bearer-check middleware.
- Compare tokens in **constant time** (`crypto/subtle.ConstantTimeCompare`).
  Wrong/missing token → 401; disabled (no token configured) → 401 (do not
  distinguish, to avoid leaking whether import is enabled).
- **REVISED in M6 (drop the in-code TLS requirement).** The original design said
  "require the connection be TLS." Jetstream serves plain HTTP on a bare TCP
  listener with TLS terminated at an upstream proxy, so an in-process `r.TLS`
  check would inspect a connection that is plaintext by construction — it would
  be theater, not enforcement (Jim's call: "that is a stupid requirement, we
  should drop it"). The token is still a bearer secret; the **operator is
  responsible for fronting the endpoint with TLS at the proxy**, documented in
  the operator notes (§8). No in-code TLS check ships.

**Transport (DECIDED — Q-TRANSPORT): server-local path reference.** The admin
stages the (tens-of-GB) file onto the box out-of-band (scp/rsync/object-store
mount) and the bearer-gated endpoint imports a **server-local path** (e.g.
`{ "path": "/data/imports/atlantis.csv.zst", "dryRun": false }`). This sidesteps
the fragile multi-hour single-HTTP-connection upload entirely; the admin holding
the bearer token has box access in practice. (Guard rail: validate/normalize the
path and confine it to a configured import directory — reject `..`/symlink
escapes — so the endpoint can't be used to read arbitrary files off the host.)

**Job model (DECIDED — Q-JOBMODEL): async job + status endpoint.** The POST
validates auth + input, enqueues the job, and returns a **job ID**. *(M6 impl
note: the atmos `xrpcserver` framework serializes a successful procedure as
HTTP 200, not 202 — it does not expose a per-handler success status — so the
endpoint returns 200 + `{ "job": "<id>" }`. The async semantics are unchanged;
only the success code differs from the original "202" phrasing.)*
A bearer-gated `getImportStatus?job=<id>` reports phase (A/B/C), rows
parsed/rejected/mutated, segments touched/total, and terminal state. **Only one
import at a time** (it holds the rewrite lock); a concurrent submit → **409
Conflict**. Progress is **also surfaced on the operator status page** (rare,
watch-it-happen op).

**Resumability (DECIDED — Q-RESUME): persist checkpoint, auto-resume.** Per-segment
progress is checkpointed in pebble (`import/<job>/progress`) so a process restart
**auto-resumes** the same job without re-submission and skips already-patched
segments. Backstop remains full idempotency (re-applying → zero mutations → skip
rename), so even a lost checkpoint degrades to a cheap re-scan rather than
corruption.

### Q-FORMAT — upload file format — **DECIDED (revised M4): plain CSV + header, ONLY**
Canonical and **sole** accepted format: **RFC4180 CSV with a header row,
uncompressed** (`.csv`). No NDJSON, no Parquet, no zstd, no other formats — one
parser surface, kept minimal.

- **Revised from `.csv.zst` to plain `.csv` during M4** for one mechanical
  reason: a zstd stream is **forward-only**, not randomly seekable, so "record a
  byte offset in Phase B and `ReadAt` it in Phase C" is impossible against a
  `.zst` without either a full re-decompress-and-scan per segment or a plaintext
  scratch copy. A plain CSV **is** seekable, so Phase B stores a `uint64` offset
  per row and Phase C seeks straight to it — no decompression subsystem, no
  scratch copy, minimal machinery (the operator's staged file *is* the seekable
  working store). **Tradeoff:** the on-box file is larger uncompressed (a
  full-network export is hundreds of GB vs. tens compressed). Accepted: the file
  is staged server-local out-of-band (Q-TRANSPORT) onto a box that already holds
  the multi-TB segment archive, and the operator can `zstd -d` during staging.
- Legible; streamable **row-at-a-time** (Phase A cannot load tens of GB into
  RAM); zero new dependency (stdlib `encoding/csv`).
- Header row names the columns so optional `cid`/`scope` are positional-agnostic;
  an unrecognized/duplicate/missing-required column fails the whole file loudly
  (a bad header makes every row ambiguous), distinct from per-row skip-and-report.
- **DID-sort is recommended, not required (DECIDED — Q-SORT).** DID-grouped input
  lets Phase-B bucketing stream with a warm DID→segments cache (~one bloom
  selection per distinct DID). But sortedness is an **optimization, not a
  correctness requirement**. *(Revised M4: the tolerance mechanism is a bounded
  generation-gated DID→candidate-segments LRU absorber, not an external merge
  sort. Unsorted input stays fully correct — a cache miss just recomputes
  `SelectBlocksForDID`, a cheap resident-bloom test with no disk I/O.
  Pathologically-shuffled huge input pays extra bloom selections but never blows
  memory and never misroutes.)* We do **not** hard-fail on unsorted input (don't
  push work onto operators or abort late on huge files).

### Q-MIGRATE — **DISSOLVED: no format change, so no migration hazard**
This fork existed only under the false assumption of a byte-level format change.
There is none (§3.7/§3.8): ingest never wrote `rendered_at` (always 0), the block
layout is byte-identical under the rename, and we do **not** bump `version`.
Therefore there is no legacy/new decoder split and no mixed-format active-segment
hazard across a binary upgrade. Nothing to decide. Existing archives stay valid.

### Q-OVERWRITE — re-import over an already-imported value — **DECIDED: overwrite**
A new import **clobbers** a prior imported (non-zero) display value. The operator
is authoritative; this is what lets a bad earlier import be corrected by
re-running. Idempotent when the value is unchanged (zero mutations → skip rename).

### Q-REJECT — bad-row handling — **DECIDED: skip-and-report, bounded status**
Skip malformed rows (bad URI, non-positive timestamp, `specific_version`
missing/unparseable CID), continue, and **report counts-by-reason** in job status.
A billion-row file must not abort on one typo — but it also must not force an
unbounded status payload: a mostly-malformed (or adversarial) input has as many
rejects as rows, so the surfaced list must be capped. Job status therefore returns
**counts by reason + the first `N` reject samples** (default `N=100`: row number,
reason, offending field), never the full list. The complete rejected-row set is
persisted durably to `data/imports/<job>/rejects.csv.zst` (row number, reason,
bounded diagnostic fields) for offline inspection. Non-silent (counts + sample +
durable artifact are all surfaced), satisfying the "no silent fallback" directive
without letting status memory/response grow with the reject count.

### Q-EXPOSE — surface the "was imported?" bit — **DECIDED: do NOT expose**
The sentinel-0 imported/not distinction stays **purely internal**. No per-event
wire field and **no status-page metric**. (It's still available internally for
re-import idempotency and any future need, but nothing external depends on it.)

---

## 6. Implementation decisions (settled)

### I. Patch-mode rewrite — **DECIDED: new sibling `segment.Patch`, not overloaded `Rewrite`**
Add a distinct entry point rather than extending `Rewrite`'s `RowDecision`:

```go
// Patch rewrites a sealed segment in place, mutating the display
// (indexed_at) column of matching rows. It preserves ALL rows and the
// entire block topology; it never drops rows. mutate returns true iff it
// changed the event.
func Patch(path string, mutate func(*Event) bool, opts PatchOptions) (PatchResult, error)
```

Rationale for a sibling over reusing `Rewrite(decide RowDecision)`:

- **Drop and mutate have opposite invariants.** `Rewrite` (drop) changes row
  membership → it *rebuilds* per-block/segment DID blooms, the collection index,
  and event counts. `Patch` (mutate display column only) changes **none** of
  those; it must **copy them verbatim**. Sharing one function invites a future
  edit to rebuild-on-patch (a correctness+perf bug). Separate functions make the
  invariant structural.
- **Hard invariants for `Patch` (assert in code + test):**
  - Row count per block unchanged; block offsets recomputed only because
    compressed size changes, but **event_count, seq envelope, witnessed envelope
    (`Min/MaxWitnessedAt`), DID blooms, per-block DID blooms, and the collection
    index are byte-preserved** (copied from source footer, not recomputed).
  - Only `ev.IndexedAt` (display column) may change; assert `mutate` touched no
    other field (debug/test build).
  - **Skip-rename when `mutatedCount == 0`** (mirrors `Rewrite`'s `rowsDropped==0`
    early return) → idempotent, cheap re-runs, cheap resume.
  - Same atomic tmp+fsync+rename+dir-sync + `CrashInjector` hooks as `Rewrite`.
  - Reuse the segment-level DID bloom pre-filter (`CandidateDIDs`) so segments
    with no matching DID are skipped without decompressing.

### H. Integration — **DECIDED: import runs through the compactor as a pass variant**
Import executes as a **variant of the existing compaction pass** in
`orchestrator/compact_deletes.go` (worker pool over sealed segments), so it:

- **shares the compactor's single rewrite ownership / lock** — no tmp+rename race
  with steady-state delete-compaction (§3.3);
- reuses the manifest/serving-metadata refresh + **`segment_compacted`**
  notification the compactor already emits after a rewrite;
- swaps the per-segment op from `segment.Rewrite(decide)` to
  `segment.Patch(mutate)` fed by that segment's loaded patch map (§3.2 Phase C).

An in-flight import and a scheduled delete-compaction are **mutually exclusive**
(both take the rewrite lock); the loser waits. Only one import at a time (§
Q-JOBMODEL 409).

*(M5 impl note: the "rewrite lock" was implicit before import existed — only the
single `runSteadyCompactor` goroutine ran rewrites. Import is dispatched from a
separate request-handler goroutine, so M5 added an **explicit**
`Orchestrator.rewriteMu`/`withRewriteLock` both passes acquire. Same guarantee,
now enforced by a lock rather than goroutine topology.)*

### J. Observability — **DECIDED: mirror compaction metrics + job status**
Counters/gauges in the existing compaction-metrics style: rows parsed, rows
rejected (by reason), DIDs matched, segments examined, segments skipped-by-bloom,
segments patched, rows mutated, bytes rewritten, phase (A/B/C) progress, job
duration. Surfaced via `getImportStatus` and the operator status page
(Q-JOBMODEL). Per Q-EXPOSE, none of this includes a per-event imported/not bit.

### K. Dry-run mode — **DECIDED: NO dry-run. Keep it simple.**
Explicitly not building a parse/validate/estimate-only mode. The job already
skip-and-reports bad rows (Q-REJECT) and is idempotent + resumable
(Q-RESUME/Q-OVERWRITE), so a real run is safe to start and observe via
`getImportStatus`.

---

## 6a. Rename inventory — the rename is TOTAL (code + comments + docs + wire?)

The rename is not just the per-row column; **every** derived symbol, comment,
JSON/CBOR tag, log field, metric, and doc reference that carries the old meaning
must change. Two mappings, applied **atomically in one pass** (they collide —
old `IndexedAt` → `WitnessedAt` frees the name that `RenderedAt` → `IndexedAt`
then takes; a sequential rename would alias them):

- `RenderedAt` / `renderedAt` / `rendered_at`  → `IndexedAt` / `indexedAt` /
  `indexed_at`   (the display value)
- `IndexedAt`  / `indexedAt`  / `indexed_at`   → `WitnessedAt` / `witnessedAt` /
  `witnessed_at` (the firsthand witnessed clock)

Scale (non-test grep, approximate): ~640 references. Derived symbols that MUST
move with the base rename (all → `…WitnessedAt`):

- **Struct fields / envelopes:** `Event.IndexedAt`, `Header.{Min,Max}IndexedAt`,
  `BlockInfo.{Min,Max}IndexedAt`, `SealResult.{Min,Max}IndexedAt`,
  `manifest.*.{Min,Max}IndexedAt`, `status/inspect.*.{Min,Max}IndexedAt`.
- **Locals / walk state:** `minIndexedAt`, `maxIndexedAt`, `watermarkIndexedAt`,
  `tipIndexedAt`, `oldestIndexedAt`, `maxSrcIndexedAt`, `indexedAtMicros`, etc.
- **Cursor path:** the `translateTimeUSToSeq` comments and the `ev.IndexedAt >=
  timeUS` scan (`cursor.go`) — this searches the **witnessed** clock; rename +
  update the doc comments to say so explicitly (it's the subtlety that keeps the
  36h lookback correct — see §2).
- **Column accessors / encode-decode:** `eventColumns.IndexedAt/RenderedAt`,
  `pendingBlock.IndexedAt/RenderedAt`, `columns` interface methods, the
  `block.go` encode/decode comments ("rendered_at[]" → "indexed_at[]",
  "indexed_at[]" → "witnessed_at[]").
- **Tests / goldens / fuzz / bench:** `testIndexedAt`, the golden block bytes
  comments, swarm/fuzz field setters. Golden *bytes* do not change (format
  unchanged); only the field *names* and comments in the test source do.
- **Metrics / logs / status page:** any metric label or log key using
  `indexed_at` → `witnessed_at`; status-page column headers.

### Q-WIRE-NAMES — external contract field names — **DECIDED: rename wire too**
Rename the external wire fields as well: `minIndexedAt`/`maxIndexedAt` →
`minWitnessedAt`/`maxWitnessedAt` in `listSegments.json`, then **regenerate**
`api/jetstream` via lexgen (do not hand-edit). Pre-production, so the contract
break is cheap now and expensive later; leaving a wire that says "indexed" but
means "witnessed" would defeat the rename. (`time_us` is unaffected — §2: it stays
`time_us` and now carries the display value for v1 parity.)

Original framing / options for the record:

Some `indexed_at` names are **external wire contracts**, not internal symbols:

- **Lexicon** `lexicons/network/bsky/jetstream/listSegments.json` — the `segment`
  object has **required** `minIndexedAt` / `maxIndexedAt` (+ descriptions), from
  which `api/jetstream/jetstreamlistsegments.go` is **code-generated by lexgen**
  ("DO NOT EDIT"): Go fields `Min/MaxIndexedAt`, `json:"minIndexedAt"`, and
  `cborKey_/jsonKey_…minIndexedAt`.
- Wire `time_us` (already handled in §2 — stays `time_us`, now carries display).

These carry the **witnessed** clock (segment envelope), so semantically they
should become `minWitnessedAt` / `maxWitnessedAt`. But they are a published
contract. Fork:

- **(a) Rename the wire fields too** (`minWitnessedAt`/`maxWitnessedAt`): update
  the lexicon, regen `api/jetstream`, bump the lexicon consumers. Cleanest /
  most honest; jetstream is pre-production so breaking the contract is cheap
  **now** and expensive later. **[Claude's lean, given pre-prod + zero-tech-debt.]**
- **(b) Keep wire names `minIndexedAt`/`maxIndexedAt`, rename only internal Go
  symbols.** Preserves the published lexicon, but leaves a permanent
  name/meaning mismatch on the wire (wire says "indexed", means "witnessed").
  Contradicts the whole point of the rename (unambiguous names).

Regardless of (a)/(b): regenerate, don't hand-edit, the lexgen output.

## 7. Downstream doc edits once decisions land

- `docs/README.md` §8 — rewrite around this model.
- §3.2 (block format) — rename columns; note sentinel-0 display semantics, and
  explicitly state that the block layout and segment `version` do **not** change
  (§3.7/§3.8).
- §3.1.2 (header) — `min/max_indexed_at` → `min/max_witnessed_at`.
- §5.1 — clarify `time_us` = resolved display; retire the line-718 "rendered vs
  indexed" TODO.
- §5.2 — optional `witnessed_at` extended field (contingent on extended surviving).
- Remove the "rendered at" term everywhere.

---

## 8. Open-question status tracker

| ID | Question | Lean | Status |
|---|---|---|---|
| Q-JOB | execution / auth model | online in-server, bearer-gated, 401-by-default | **DECIDED** |
| Q-TRANSPORT | how the GB-scale file reaches the server | server-local path reference | **DECIDED** |
| Q-JOBMODEL | sync request vs. async job + status | async job id + status endpoint + status page | **DECIDED** |
| Q-RESUME | crash resumability | persist checkpoint, auto-resume | **DECIDED** |
| Q-FORMAT | CSV+zstd / NDJSON / Parquet | plain CSV+header only (zstd dropped M4 for seekability) | **DECIDED** |
| Q-SORT | require+verify DID-sortedness vs. recommend | recommend + tolerate (gen-gated LRU absorber, M4) | **DECIDED** |
| Q-OVERWRITE | re-import clobber vs. fill-only | overwrite | **DECIDED** |
| Q-REJECT | skip-and-report vs. whole-file reject | skip-and-report + bounded status (counts + sample + durable artifact) | **DECIDED** |
| Q-EXPOSE | surface "was imported?" bit | do not expose (fully internal) | **DECIDED** |
| Q-MIGRATE | active-segment format on upgrade | dissolved — no format change | **DISSOLVED** |
| Q-WIRE-NAMES | rename external wire fields (minIndexedAt→minWitnessedAt) too? | rename wire too, regen lexgen | **DECIDED** |
| Q-SCOPE-CID | specific_version match / duplicate-CID rows | recompute CID, patch all matches (§4a) | **DECIDED** |
| I. Patch rewrite | overload Rewrite vs. new entry point | new `segment.Patch` (no bloom/index rebuild) | **DECIDED** |
| H. Integration | standalone vs. compactor pass | compactor pass variant (shares lock + notify) | **DECIDED** |
| J. Observability | metrics surface | compaction-style counters + job status | **DECIDED** |
| K. Dry-run | build vs. skip | **no dry-run** (keep simple) | **DECIDED** |

---

## 9. Implementation plan / progress tracker (LIVING — update as we go)

Sequenced into milestones. Each milestone is one branch → one PR. Commits within
a milestone are small and independently green (`go test ./... -race` + lint).
Check boxes off as landed; add a one-line note + PR/commit ref per box.

Legend: `[ ]` todo · `[~]` in progress · `[x]` done.

### M0 — Design + docs  ✅ DONE
- [x] Design doc + all decisions (this file). — commit `1b76825`
- [x] `docs/README.md` §8 rewrite + drop resolved TODO. — commit `e6c926d`
- [x] Tracking issue #193.

### M1 — The rename (no behavior change, no format change)  ✅ DONE
> Pure code/comment/wire rename. Lands first and alone so the diff is reviewable
> as "nothing changed but names." Format bytes untouched; goldens keep their bytes.
> Did the two mappings via an **ordered** textual sweep (not per-symbol gopls,
> since every token is globally unique + must rename): `IndexedAt → WitnessedAt`
> fully first, then `RenderedAt → IndexedAt` (once step 1 leaves zero `IndexedAt`
> tokens, step 2 can't collide). Compiler + full `-race` suite + byte-golden are
> the safety net.

- [x] **`segment/`**: `Event.IndexedAt → WitnessedAt`, `Event.RenderedAt →
  IndexedAt`; block encode/decode column comments (`block.go` "indexed_at[]" →
  "witnessed_at[]", "rendered_at[]" → "indexed_at[]"); `eventColumns` +
  `pendingBlock` accessors; `event.go` doc. **Physical column order preserved**
  (`seq`, `witnessed_at`, `indexed_at`) — verified byte-identical.
- [x] **Header/footer/seal**: `Header.{Min,Max}IndexedAt → {Min,Max}WitnessedAt`;
  `BlockInfo.{Min,Max}IndexedAt`; `SealResult.*`; `seal.go` envelope fill reads
  `ev.WitnessedAt`.
- [x] **`internal/manifest`**: all `{Min,Max}IndexedAt` → `WitnessedAt`;
  `SegmentForTimeUS` doc/comments say "witnessed".
- [x] **`internal/subscribe`**: `cursor.go translateTimeUSToSeq` scan + comments
  (searches `WitnessedAt`). NOTE: `encoder.go` `TimeUS` still reads the renamed
  raw field (now `evt.WitnessedAt`) — the **display resolver is M2**, deferred so
  M1 stays behavior-preserving. (Pre-import display==witnessed, so no behavior
  change either way, but keeping it in M2 keeps M1 a pure rename.)
- [x] **`internal/status`, `cmd/jetstream/inspect_*`**: envelope fields + inspect
  printf labels (`witnessed_at range`); regenerated `inspect_all_basic.golden`.
- [x] **`internal/web`**: `status.html` template fields `.MinIndexedAt →
  .MinWitnessedAt` + user label "Indexed range" → "Witnessed range"; test
  assertion updated. (`.html` was missed in the first `.go`-only sweep — caught by
  the web tests.)
- [x] **Lexicon + regen (Q-WIRE-NAMES)**: `listSegments.json`
  `minIndexedAt/maxIndexedAt → minWitnessedAt/maxWitnessedAt` (+ descriptions);
  regenerated `api/jetstream` via `just lexgen`.
- [x] **Tests/goldens/fuzz/bench**: renamed field setters + comments; **golden
  `testdata/golden_block.bin` bytes unchanged** (NOT regenerated — the pre-rename
  fixture still matches post-rename encode output, proving the 4.5-day archive
  reads correctly). `inspect_all_basic.golden` regenerated for the label change.
- [x] **Docs**: `docs/README.md` §3.2 column list + new two-timestamp paragraph
  (states layout/version unchanged), §3.1.2 header (`min/max_witnessed_at`), §5.1
  `time_us` clarification, prose (segment ordering, FAQ), root `event.go` client
  `TimeUS` comment.
- [x] Full suite (`go test ./...`) + lint (`golangci-lint`, gofmt) green; byte
  goldens (`TestGolden`, `TestSealGolden`) pass; grep proves zero stray
  `RenderedAt` and zero old-meaning envelope tokens. (`-race`: pre-existing
  atmos-streaming iterator flake under parallel load in `internal/ingest/live`,
  unrelated to rename — passes in isolation.)

### M2 — Display resolver on the wire (`time_us = imported ?: witnessed`)
> Tiny, but it's the first behavioral change. Safe: identical to today until an
> import runs (all display columns are 0 → resolves to witnessed).

- [x] Resolver implemented as `(*segment.Event).DisplayTimeUS()` (method, not a
  free `displayOf` — it's a property of the event, self-documenting, and lives
  next to the columns M3's `Patch` mutate func will touch). Pure sentinel check:
  `IndexedAt != 0 ? IndexedAt : WitnessedAt`. Docstring cites §3.2 and states the
  pre-import invariant (all IndexedAt==0 → display==witnessed).
- [x] `encoder.go`: all four `TimeUS:` sites (v1 commit/identity/account +
  extended envelope) now call `evt.DisplayTimeUS()`.
- [x] Tests: `segment/event_test.go` `TestEvent_DisplayTimeUS` (table:
  unimported→witnessed, imported wins, future-import wins, both-zero→0).
  `encoder_test.go` `TestEncode_TimeUSResolvesDisplayValue` proves the resolver
  reaches the wire across every encoder entry point + kind (v1 commit/identity/
  account; extended adds #sync). Full suite + lint green; v1 golden round-trips
  unchanged (goldens carry IndexedAt==0, so display==witnessed as before).

### M3 — `segment.Patch` (mutate-mode rewrite)  [§6 I]
> New sibling to `Rewrite`. Mutates the display column only; preserves topology,
> blooms, collection index, and the witnessed envelope verbatim.

- [x] `segment/patch.go`: `Patch(path, mutate func(*Event) bool, opts
  PatchOptions) (PatchResult, error)`. Decompress each candidate block, run
  `mutate`, re-encode only dirty blocks; **copy the footer tail (segment DID
  bloom + per-block DID blooms + collection index) verbatim** in one pread —
  those regions are keyed only on DID/collection/counts and embed no absolute
  offsets, so a display-only patch leaves them byte-identical. Only the block
  index (Offset/CompressedSize shift after recompression), header section
  offsets, and checksum are rebuilt. Atomic tmp+fsync+rename+dir-sync; reuses
  `CandidateDIDs` bloom pre-filter + new `CrashInjector` seams.
- [x] Early-return when `rowsMutated == 0` (skip rename) → byte-identical file,
  inode + mtime preserved. Idempotent for a fixed `mutate` (re-run finds every
  target already at value → no-op).
- [x] **Correctness enforced at the durable boundary, not just asserted**:
  `eventGuard` snapshots every non-display field before each `mutate` call and
  refuses to persist (returns `ErrInvalidConfig`, source untouched) if `mutate`
  changed Seq/WitnessedAt/Kind/DID/Collection/Rkey/Rev/Payload-len — a changed
  DID would silently invalidate the verbatim-copied blooms. Also asserts each
  dirty block's uncompressed size is invariant (IndexedAt is fixed-width) and
  the rebuilt block index length matches source (block count invariant). Crash
  beats corruption.
- [x] Crash seams: 4 new `CrashPointPatch*` constants (segment/crash.go),
  registered in `internal/crashpoint` (AllPoints 12→16, count-pin test bumped).
  Mirror the rewrite tmp→fsync→rename→dir-sync recovery contract.
- [x] Tests (`segment/patch_test.go`): patch subset → only IndexedAt moves, all
  other columns + per-block blooms (byte-compared) + collection table/counts/
  bitmasks + witnessed/seq envelope preserved, `DisplayTimeUS()` reflects import;
  reopen with checksum verification; zero-mutation → no rename + inode/mtime
  intact; idempotent re-run; immutable-field-mutation table (8 fields) each
  rejected + source untouched; CandidateDIDs skip; 4 crash seams (reopens
  pristine-or-fully-patched, never torn); `FuzzPatch` (20s clean). Full suite +
  `-race` + lint green.

### M4 — CSV parse + validate + DID bucketing (Phases A/B)  ✅ DONE
> Streaming, disk-backed. No segment writes yet — output is per-segment offset
> files. Independently testable end-to-end. Branch `timestamp-import-m4` on M3.
> **Note the two in-flight decision revisions (Q-FORMAT plain CSV, Q-SORT LRU
> absorber) recorded above.**

- [x] **`manifest.Generation()`** (commit `b47b0dd`): monotonic counter bumped
  under `mu` on load/seal/compact (never on read-only queries; refresh bump only
  after the seq-monotonicity gate passes). The cache-coherence primitive Phase B
  gates on. Red-first test pins seal & compact each advance it and reads do not.
- [x] **Phase A — `internal/timestamp` parse+validate** (commit `b4bed29`):
  `Parse(io.Reader, Options)` streams a **plain** CSV (header `uri,timestamp,
  scope,cid`), validates each row (`atmos.ParseATURI` → DID/collection/rkey with
  handle-authority and incomplete-URI rejects; RFC3339 → micros with a
  non-positive reject so a stored value can't collide with the sentinel-0;
  scope enum default `all_versions`; `cbor.ParseCIDString` required iff
  `specific_version`, ignored for `all_versions` per §4 D), and invokes an
  `OnRow` hook per valid row carrying the row's **byte offset**
  (`csv.InputOffset`). **Skip-and-report** bad rows: complete counts-by-reason
  (10 stable reasons) + a **bounded** in-memory sample (Q-REJECT) + an `OnReject`
  hook (M6's durable-artifact seam). Structural header errors fail the whole file
  (`ErrHeader`). One bad row never aborts the file.
- [x] **Phase B — `internal/timestamp` bucketer** (commit `35c8069`): `Bucketer`
  is the `OnRow` sink. Resolves each row's DID to candidate segments via
  `manifest.SelectBlocksForDID` (resident blooms, no disk I/O) and appends the
  row's offset to per-segment **offset files** (`offsets_<idx>.bin`, packed
  `uint64` LE) under the job dir. Bounded **generation-gated** DID→segments LRU
  (Q-SORT tolerate; the gen is sampled *before* the select so a raced seal is
  never falsely-fresh) + bounded FD pool (offset files reopened `O_APPEND` after
  eviction). Phases A+B are one fused streaming pass. `var _ Selector =
  (*manifest.Manifest)(nil)` pins the M5 wiring.
- [x] Tests: malformed rows skipped+counted (per-reason); scope defaulting incl.
  absent column; `specific_version` requires parseable cid; offset seeks back to
  the exact row bytes; bounded reject sample + `OnReject` fires for all;
  bucketing routes to correct candidate segments + fan-out counts; **stale cache
  recomputes on generation bump**; **seal-during-selection is not falsely
  fresh**; DID-cache & FD-pool eviction are correctness-transparent; end-to-end
  `Parse`→`Route`→offset-seek-back. Full suite + `-race` + lint green.
- Follow-up for M5 (Phase C): `Seek` each offset in the plain CSV, decode+
  revalidate the one row, build the per-segment `(did,collection,rkey[,cid])→ts`
  map, run one `segment.Patch`, emit `segment_compacted`. (No orchestrator wiring
  or durable job checkpoint yet — those are M5/M6.)

### M5 — Phase C: apply via `segment.Patch`, wired through the compactor  [§6 H]  ✅ DONE
- [x] **Phase C apply core** (`internal/timestamp/apply.go`, commit `4d62d93`):
  `RowReader` seeks+revalidates a single row from the plain CSV by byte offset
  (header column mapping parsed once at open, so a non-canonical column order
  still decodes a header-less mid-file row); re-validation is deliberate defense
  in depth against a resumed job reading offsets a prior process wrote.
  `BuildPatchPlan` folds a segment's offset file into a per-path
  `(did,collection,rkey)→{allVersionsTS, cid→ts}` lookup (last-write-wins;
  corrupt offset counted+skipped, torn offset file rejected). `BuildMutate`'s
  closure applies scope rules (materialization-only; `specific_version` via
  `ComputeCID(CodecDagCBOR,payload).Equal` **wins over** `all_versions` and
  patches **all** CID matches, §4a; sets only `IndexedAt`, returns true iff
  changed → preserves `segment.Patch` guard + zero-mutation idempotency);
  unmatched specific CIDs counted.
- [x] **Explicit rewrite lock** (`orchestrator.go`, commit `010f73e`): **design
  deviation, recorded.** §3.3/§6 H assumed import and delete-compaction are
  mutually exclusive "through the same rewrite owner." In the code that owner
  was *implicit* — only the single `runSteadyCompactor` goroutine ever called a
  rewrite pass. M6 dispatches import from a **separate request-handler
  goroutine**, so the implicit guarantee no longer holds. Added an explicit
  `Orchestrator.rewriteMu` + `withRewriteLock`; delete-compaction wraps its
  per-chunk `applyCompactionChunk`, import wraps its whole Phase C worker pool.
  The two are now mutually exclusive with the loser waiting, exactly as §6 H
  specifies — just enforced by a real lock instead of goroutine topology.
- [x] **Import pass wired** (`orchestrator/import_pass.go`, commit `a66b172`):
  `RunImport(ctx, ImportJob{CSVPath, JobDir}) (ImportResult, error)` runs A+B
  unlocked (parse+bucket → offset files) then C under the rewrite lock (worker
  pool → `segment.Patch(mutate)` per touched segment, `CandidateDIDs` bloom
  prefilter, `OnSegmentCompacted` manifest refresh, `crashpoint.ForSegment`
  seams). New `Config.ImportSelector` (manifest; nil → `ErrImportUnavailable`).
  A segment that vanished between B and C is skipped+warned; a re-run
  re-buckets it (§3.4).
- [x] Tests: e2e on a real sealed segment + real manifest — display column
  patched, witnessed/seq/other columns + envelope byte-intact, `DisplayTimeUS`
  reflects import, idempotent re-run = no-op; `specific_version` CID-only match;
  no-candidate row = no-op; disabled-without-selector errors; in-flight import
  holds the rewrite lock against a competing acquirer. Full suite + `-race` +
  lint green; whole tree builds.
- Not yet: no XRPC endpoint / bearer auth / job model / durable resume
  checkpoint / status surface / metrics — all M6. `RunImport` is the callable
  core M6 will expose.

### M6 — Job model, auth, resumability, status  [Q-JOB / Q-TRANSPORT / Q-JOBMODEL / Q-RESUME]  ✅ DONE
- [x] **Resume + progress hooks in `RunImport`** (`orchestrator/import_pass.go`):
  optional `SkipBucket` / `SkipSegment` / `OnSegmentApplied` / `OnPhase` on
  `ImportJob`. All optional; a zero-value job runs the pre-M6 behavior. The
  checkpoint fires from the worker right after a segment's atomic rewrite is
  durable, and a failed checkpoint aborts the job (resume safety lost → stop
  loud). Red-first tests: resume-skip, SkipBucket-no-reparse, checkpoint-error.
- [x] **`internal/importer` store-backed single-job manager**: Submit confines
  the CSV path to the import dir (lexical `..` check *before* touching the fs,
  then a post-`EvalSymlinks` check for symlink escape), refuses a second
  concurrent job (`ErrJobInProgress` → 409), persists an initial record, runs
  async. Durable pebble checkpoint under `import/{current,job/<id>/meta,
  job/<id>/seg/<idx>}`; `ResumeIncomplete` auto-resumes a non-terminal job at
  boot; a ctx-cancelled run **pauses** (stays resumable) rather than failing;
  `Wait` drains in-flight runs before the store closes. TDD caught 3 real bugs
  (mutex re-entrancy deadlock, store-close-vs-goroutine race, path-order).
- [x] **Bearer middleware** (`xrpcapi/auth.go`): 401-by-default when no token,
  constant-time compare (`crypto/subtle`), disabled ≠ wrong-token
  indistinguishable. **In-code TLS check dropped** — see revised Q-JOB above;
  the operator fronts the endpoint with TLS at the proxy.
- [x] **XRPC** (`xrpcapi/importts.go` + lexicons + regen): `importTimestamps`
  (procedure; server-local path confined to the import dir) → job id;
  `getImportStatus` (query). Manager sentinels → lexicon error names
  (`ImportInProgress`→409, `InvalidPath`→400, `JobNotFound`→404). Registered
  only when a manager is wired; both bearer-gated.
- [x] **Serve flags**: `--timestamp-import-token` /
  `JETSTREAM_TIMESTAMP_IMPORT_TOKEN` and `--timestamp-import-dir` /
  `JETSTREAM_TIMESTAMP_IMPORT_DIR` (default `<data-dir>/imports`). Runtime
  builds the manager (Runner = orchestrator, `ImportSelector` = manifest,
  sharing the rewrite lock), auto-resumes at boot, and on shutdown cancels +
  drains import goroutines **before** closing the store.
- [x] **Status page**: `status.ImportInfo` + `ImportReporter` seam (status stays
  decoupled from the importer's types) → "Timestamp import" summary panel
  (state/phase, apply progress, row totals, error). No per-event imported bit
  (Q-EXPOSE).
- [x] **Metrics** (§6 J): `jetstream_import_*` — jobs_total by result,
  job_duration, phase gauge (0=idle/1=parse_bucket/2=apply), rows
  parsed/rejected-by-reason/mutated/matched-by-scope/corrupt, DIDs matched,
  segments examined/patched, bytes_rewritten. (Note: "skipped-by-bloom" is not
  separately surfaced — the segment-level bloom prefilter happens inside
  `segment.Patch` after the DID already routed the segment into Phase C, so a
  bloom-skip here would double-count against `segments_examined`; the metric we
  ship, patched-vs-examined, is the operationally meaningful one.)
- [x] Tests: 401 default + wrong token + missing header; path-escape 400; 409
  concurrent; crash(ctx-cancel) mid-import → resume skips checkpointed segments;
  status panel render; metrics folded + phase reset. Commits `e8f0ef0`,
  `3d94104`, `3c64325`, `338f382`, `a300382`, `832b535`.

### M7 — Docs + close-out
- [ ] Reconcile `docs/README.md` §8 with the final wire/flag names if they drifted.
- [ ] Operator note: CSV schema, DID-sort recommendation, `specific_version`
  needs per-version CIDs (only some collections), token setup.
- [ ] Update this tracker to DONE; close #193.

### Cross-cutting notes for the implementer
- **Format is NOT changing** (§3.7/§3.8). Any milestone that alters segment bytes
  beyond the display column, or bumps `version`, is a bug — stop.
- **Witnessed is load-bearing** for range scans + `?cursor=<timestamp>` lookback;
  never import into it, keep it seq-monotonic.
- **Idempotency everywhere**: re-running any phase must converge (zero-mutation
  skip). This is what makes crash-resume + operator re-run safe.
- Keep the giant-file bytes off the HTTP path (server-local path only).
