# Imported indexed_at durability: rule map stamped at ingest

**Status: DECIDED (design), not yet implemented.** Tracking issue: #269.

## 1. Problem

The timestamp import (docs/README.md §8, specs/notes/2026-07-01-timestamp-import-design.md)
is a **one-shot patch of rows that exist in sealed segments at import time**:
Phase C sets `ev.IndexedAt` on matching rows via `segment.Patch` and nothing
else. The import's intent has no durable home outside the patched rows
themselves — the offset files are scratch, and the CSV is not retained.

Meanwhile every ingest path — live (`internal/ingest/live/events.go`), backfill
(`internal/ingest/backfill/handler.go`), resync, and the merge runner
(`merge_runner.go`, which re-stamps only `WitnessedAt`) — writes new rows with
`IndexedAt = 0`.

Consequence: **any operation that supersedes an imported row erases the
imported timestamp permanently.** Two scenarios, both confirmed with executable
tests against the real `segment.Patch` → `tombstone.Fold`/`ShouldDrop` →
`segment.Rewrite` pipeline (2026-07-07, scratch tests since removed):

1. **Record update → compaction.** The `#update` tombstones the original
   create (`tombstone.ShouldDrop`, record key, `seq >` superseding); compaction
   physically drops the create — and the imported `indexed_at` riding on it.
   The surviving update row has `IndexedAt = 0`, so `DisplayTimeUS()` falls
   back to the *new* `witnessed_at`. Note the loss is two-stage: the moment the
   update arrives, the current version already displays `witnessed_at` (the
   update row was never patched); compaction then makes the imported value
   physically unrecoverable from the archive.
2. **`#sync` resync → compaction.** The DID tombstone drops *every*
   materialization row for the DID below the superseding seq; the re-downloaded
   `CreateResync` rows carry `IndexedAt = 0`. Every imported timestamp for the
   DID is erased even though the record content (and CID) is byte-identical.

A control test confirmed the loss vector is exclusively **row supersession +
physical drop**: rows *kept* through a rewrite preserve `indexed_at` faithfully
(`Rewrite` re-encodes decoded events verbatim; `Patch` guards every other
column).

Adjacent gap found during the same investigation: the bucketer only routes to
segments **sealed at import time** (`internal/timestamp/bucket.go`), so a row
sitting in the active segment during an import is never patched.

Requiring operators to re-run imports after compaction/resyncs is **not
acceptable** (Jim, 2026-07-07): once imported, `indexed_at` must permanently
stick to the record no matter how history is rewritten. (`witnessed_at` may
change on supersession — a new event is genuinely a new witnessing.)

## 2. Decision

The import stops being only a *patch of rows* and becomes a **durable rule**:
`(did, collection, rkey) → indexed_at` (plus per-CID entries for
`specific_version` rows), stored in pebble, consulted whenever the system
materializes a row for that path.

Key constraint shaping everything: **backfill is served as raw segment-file
downloads**, so the correct value must end up in the segment bytes themselves.
Any overlay that only fixes the websocket path fails for archive downloaders.

### 2.1 Import time (new flow)

Timestamp import is **steady-state only**. Before steady state there is no
stable public archive contract to repair, and supporting imports across the
bootstrap backfill writer, bootstrap-live `live_segments` writer, and merge
destination writer would add correctness surface with no operational need. The
XRPC submit path should reject non-steady-state imports with a clear operator
error.

Phases A/B/C mostly run as today (parse+bucket, then patch sealed segments under
the rewrite lock), but the ordering changes to make the rule map load-bearing:
build and durably ingest the rule map first, refresh/activate the in-process
stamper, force-rotate the steady writer so every pre-activation active row is
sealed, then run the sealed-segment patch pass against the refreshed manifest.
Rows appended after activation are stamped at birth by `Writer.Append`; rows
that were active before activation are now sealed and patchable by Phase C. The
map — not the CSV — becomes the permanent memory of the operator's intent; the
CSV is deletable after the job.

- Built offline as **sorted SST files and bulk-ingested**
  (`DB.Ingest` / `DB.IngestWithStats` for local files in the pinned Pebble
  version), not point-Sets through the memtable/WAL — 5B writes through the
  memtable would be a write-amp storm against a live store. Pre-sorted SSTs land
  in the bottom level with essentially zero compaction debt. Do **not** use
  Pebble `IngestExternalFiles` for this local import path unless the design is
  deliberately changed to configure `Experimental.RemoteStorage`: in Pebble
  v1.1.5 that API is for remote/external objects and errors without shared
  storage configured.
- **Separate pebble instance** at `<data-dir>/import-rules`, NOT the existing
  metadata store. Today's store is a small, hot db with cursor commits on the
  ingest path and tight fsync latency expectations; dropping a 100+ GB cold
  keyspace into it changes its character (shared memtable/WAL/compaction
  scheduling, cache pollution, and it turns "back up the tiny metadata db"
  into "back up 100 GB"). The rule map is rebuildable from a re-import in
  disaster scenarios.

### 2.2 Ingest time (the fix itself)

Every event that materializes a record (Create/Update/CreateResync) flows
through one choke point: `ingest.Writer.Append`. The check lives there, as a
two-stage funnel:

1. **Collection filter (free, resident):** is this collection in the set of
   collections that appear in imported rules? The set is derived from the map
   itself (maintained resident, extended by later imports — nothing hardcoded
   to posts/reposts/etc., so future collections work with no code change).
   Likes/follows/blocks — the overwhelming bulk of the firehose — fail here.
2. **Pebble `Get`:** if a rule exists, stamp `ev.IndexedAt` *before* the event
   is buffered — so it lands in the segment bytes AND on the live wire the
   moment the event is witnessed.

`specific_version` rules: the map holds `{cid: ts}` entries; stamping
recomputes the payload CID (only for rows whose path hits a specific rule) and
applies on match. A resync re-download of identical content recomputes the
same CID → re-stamped; a genuinely new version doesn't match → correctly
unstamped.

### 2.3 Compaction and resync (unchanged)

**Compaction never checks pebble. It stays a dumb drop-only rewrite.** By the
time it drops a superseded row, the superseding row was already stamped at
append time. No new coupling, no read amplification in the rewrite loop.

`#sync` resync and merge/re-backfill (the #262 shape): re-downloaded /
re-promoted rows all flow through the same `Append` seam and get stamped at
birth. One seam covers every way a row can be reborn.

### 2.4 Semantics pinned

Under `all_versions`, a *future genuine edit* of an imported record keeps
displaying the imported timestamp forever ("sorted_at is stable across
edits" — now load-bearing). `specific_version` rules also live in the map
forever (cheap; resyncs need them).

## 3. Rejected alternatives

- **Re-run imports operationally after rewrites.** Rejected outright by Jim:
  unacceptable operational burden; leaves display wrong between rewrite and
  re-run; requires retaining the CSV forever.
- **Apply rules at compaction-rewrite time** (keep hot path untouched).
  Rejected: display is wrong for up to a compaction interval after every
  edit/resync; live-tail subscribers see the wrong `time_us` permanently
  (their events are never restamped); converges on disk but never on the wire.
- **Carry-forward at drop time** (when `ShouldDrop` kills a row with
  `IndexedAt != 0`, propagate to the survivor; no rule map). Rejected: the
  survivor may be in another segment, the active file, or not yet flushed —
  cross-segment coordination inside the rewrite loop is racy and complex; the
  pre-compaction window is still wrong; and it cannot express "future versions
  inherit" (a new update after compaction has nothing to inherit from).
- **Read-time overlay** (resolve `time_us` at serve time). Rejected: violates
  the documented no-read-time-overlay design; puts a lookup on the fan-out
  path (per subscriber, much hotter than ingest); archive segment downloads
  bypass it entirely.
- **TID-rkey cutoff shortcut** (skip pebble when the rkey's embedded TID
  postdates the import). Considered in two forms and **dropped**:
  - *Cutoff = import wall-clock time*: a pre-import record with a
    future-dated TID rkey (rkeys are network-user-supplied, adversarial)
    would be in the map but skipped → silent missed stamp. Fixing it needs a
    resident exception set — complexity judged not worth it (Jim).
  - *Cutoff = max rkey-TID present in the map per collection*: correct by
    construction, but Jim confirmed the Atlantis import is **guaranteed** to
    contain far-future TID rkeys, so the cutoff is poisoned by construction
    (filters nothing for exactly the collections that matter).
  - The shortcut was never load-bearing anyway: a negative pebble lookup with
    resident filter blocks **is** a bloom check (hash-and-probe against
    in-memory filters, sub-µs, zero IO). The TID check was optimizing a bloom
    check with another bloom check. Dropping it also removes all TID decoding
    and adversarial-rkey reasoning from the hot path — the rkey is an opaque
    key component, the most honest treatment of untrusted input.

## 4. Napkin math (cpu2-pop3, 2026-07-07 status page)

Collections of interest today (event counts; upper bounds on record counts
since they include markers and deleted records won't be exported):

| Collection | Events |
|---|---|
| `app.bsky.feed.post` | 2,532,895,752 |
| `app.bsky.feed.repost` | 2,187,330,990 |
| `app.bsky.actor.profile` | 18,419,391 |
| `site.standard.document` | 442,941 |
| **Total** | **~4.74B** (call it 4–5B rows) |

- **CSV**: ~70 B URI + ~26 B RFC3339 + newline ≈ ~98 B/row → **~430–470 GB**
  for ~4.7B rows. (The original import design assumed "tens of GB" staging;
  this revises that by ~10×. The box holds a multi-TB archive, so acceptable,
  but the staging budget is no longer a rounding error — and it strengthens
  the rule map being the durable artifact rather than "keep the CSV".)
- **Rule map**: compact key `prefix(1) + did-sans-plc-prefix(24) +
  collection-id(1, interned) + rkey(~13)` ≈ 39 B + 8 B value ≈ 47 B/entry
  logical → ~220 GB logical; after pebble restart-point prefix compression
  (DID-sorted keys share 25-byte prefixes) + block compression roughly 2–3× →
  **~80–120 GB on disk**. Optional: delta-encode the value against the rkey's
  own TID timestamp where the rkey is a TID (zigzag varint, 1–3 B typical) →
  **~40–60 GB**. Either is a few percent of the archive. Non-TID rkeys (e.g.
  `self`) store the full 8 B value — 18M profiles is noise.
- **Filter blocks**: ~5B keys × 10 bits/key ≈ **~6 GB** of blooms. **Pinning
  filter + index blocks resident in the table cache is the load-bearing
  performance property** (graduates from footnote to acceptance criterion):
  resident → negative lookups (every genuinely new post) never touch disk;
  evicted → occasional disk read for a filter block — degraded, not wrong,
  visible on a cache-hit-rate metric.
- **Hot-path cost**: stage 1 kills the lookup for the bulk of the firehose;
  posts+reposts run on the order of hundreds of creates/sec, each paying a
  sub-µs resident bloom probe. Resync bursts have DID locality against the
  DID-sorted map (adjacent SST blocks; single-digit ms per repo). Noise.
- **Pebble at this scale**: comfortably fine — CockroachDB production nodes
  run pebble stores in the hundreds-of-GB-to-TB range, and this workload is
  LSM-friendly: write-once (bulk-ingested, rewritten only by later imports),
  read-mostly, point reads with high locality. No ongoing write amp, no
  tombstone accumulation.

All estimates are inspection-bounded hypotheses, not measurements — see §6.

## 5. Trust model

- **Imported timestamps: trusted.** They come from the operator (Atlantis
  stamps `time.Now()` server-side since inception; other self-hosting
  operators supply their own). The operator can already import arbitrary
  timestamps honestly — this is a general product feature, hence generic CSV
  rather than querying Atlantis directly.
- **Rkeys: untrusted** (network-user-supplied, including far-future TIDs —
  guaranteed present in the Atlantis import). With the TID shortcut dropped,
  the hot path does no rkey interpretation at all.

## 6. Definition of done / validation

- Import writes rules durably (separate pebble instance, SST bulk ingestion)
  in addition to today's Phase C segment patching.
- Import is steady-state-only; non-steady submissions fail explicitly.
- `Writer.Append` stamps `IndexedAt` on materialization rows via the
  two-stage funnel, before buffering.
- Compaction/resync code untouched; the two §1 scenarios pass as oracle
  coverage: **the oracle currently never exercises import-then-rewrite at
  all** (why this went unnoticed) — add import → update/resync → compaction →
  restart tiers asserting the imported display value survives in both the
  archive bytes and the live wire.
- Active-segment gap (§1) closed: rules are activated before a forced steady
  writer rotation, then the sealed patch pass covers the pre-activation active
  rows while append-time stamping covers every later row.
- Observability: per-collection counters for funnel stage hit rates and stamp
  counts; rule-map size and filter-cache hit-rate gauges.
- **Synthetic 100M-row load test** validates the two numbers the design leans
  on before full implementation is committed: real on-disk bytes/entry after
  compression, and SST build+ingest wall time; plus filter-cache residency
  under load.

## 7. Accepted limitations (post-implementation roast, 2026-07-08)

Two findings from the branch roast were accepted rather than fixed; the
operator re-submission contract is the remedy for both. Authoritative
write-ups live in `specs/gotchas.md` ("A failed timestamp import can leave a
partial rule set active" and "A shutdown racing the import preamble can
terminally fail the job"):

- Chunked rule-SST ingestion is not atomic across chunks: a crash mid-ingest
  self-heals via auto-resume, but a terminal failure leaves a partial rule
  set active until the operator re-submits the same CSV (last-write-wins
  re-ingest heals it, including the stale cross-chunk duplicate-path edge).
- A graceful shutdown that closes the steady writer before the import ctx
  cancellation is observed makes the preamble's ForceRotate return
  ingest.ErrClosed, terminally failing the job instead of pausing it; same
  remedy.
