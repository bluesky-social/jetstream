# Backfill bandwidth over-send — analysis & task tracker

Status: **CLOSED / analysis only — no code changes.** GitHub marker issue:
[#192](https://github.com/bluesky-social/jetstream/issues/192).

> ⚠ UPDATE 2026-06-30 (supersedes task 1 below). We started task 1 (the
> whole-segment cost-model fix) and, while confirming it on live data, found the
> premise was wrong. See "Correction" immediately below. **Decision (Jim): make
> no changes.** The remaining real lever is DID-marker sentinel over-selection,
> which is load-bearing for deletion-compliance and not something we want to
> work around. Tasks 2–4 are not pursued at this time. This note is kept as the
> reasoning trail.

## Correction (2026-06-30): why task 1 is rejected

Task 1 assumed the `WholeSegmentThreshold` (0.75 block-*coverage*) was upgrading
scattered-collection segments to whole-segment downloads for no benefit, and
that forcing them back to block-mode would cut tangled backfill from ~30 GB
toward ~12 GB. Confirming on the live archive showed two errors:

1. **Block-count coverage ≈ byte coverage.** Block-mode also fetches *whole*
   blocks. When ~90% of a segment's blocks are selected, block-mode fetches
   ~94% of the segment's bytes. Converting whole-segment → block-mode therefore
   saves only **1–6%** of bytes while adding **~500 extra HTTP requests** per
   segment. Measured on 4 of the 69 whole-segment picks:

   | segment | block-coverage | blocks w/ REAL tangled | block-mode byte saving | extra requests |
   |---|---|---|---|---|
   | `seg_...4qv` | 90.1% | 3.3% | 5.9% | +566 |
   | `seg_...4r1` | 79.8% | 1.7% | 2.5% | +529 |
   | `seg_...4rt` | 83.0% | 1.4% | 1.0% | +526 |
   | `seg_...4so` | 84.6% | 2.2% | 4.6% | +542 |

2. **The 80–90% coverage is DID markers, not tangled.** Only 1.4–3.3% of a
   whole-segment pick's blocks contain a real `sh.tangled.*` record; tangled is
   **0.001%** of events. The blocks are selected because `#account` /
   `#identity` / `#sync` markers carry an empty collection and are
   *unconditionally admitted under any collection filter* via the sentinel
   mechanism (`internal/manifest/plan.go:339`, docs §"collection-filtered
   backfill"). Marker density is **~0.64% of events (~26/block), present in ~91%
   of blocks** in recent (live-tail) segments, and **0% in early backfill-era
   segments** (which had no live DID-marker traffic). So marker-bearing blocks
   dominate selection in exactly the recent segments the threshold upgrades.

### Consequence

- The whole-segment threshold is **not** the cost driver; changing it saves
  single-digit percents and *increases* request count. Rejected.
- The real driver for rare-collection backfill is **DID-marker sentinel
  over-selection**: markers are ubiquitous in recent segments and always
  admitted, so a collection filter drags in ~90% of every recent segment's
  blocks regardless of how rare the collection is.
- Those markers are load-bearing: a collection-filtered consumer MUST see
  account deletes to purge data (the whole reason the sentinel exists). A
  thinner delivery path (e.g. a compact seq-keyed marker side-channel so markers
  don't force block selection) is conceivable but delicate, and **Jim's call is
  not to work around this**. No change.

### Salvageable, independent of the above

The **caching** finding (task 2) stands on its own and is unaffected by this
correction — sealed frames are immutable and ETag'd but served `no-cache`.
Recorded here for the future; not being pursued now per the same decision.

---

_Original analysis and task list below is preserved as written; task 1's
premise is superseded by the Correction above._

This note is the working tracker for reducing the volume of block data we send
to clients during historical backfill, especially for transverse queries (a
single DID, or a rare long-tail collection). It records the measured problem,
why the obvious re-layout fixes are illegal under our ordering constraint, and a
short prioritized task list. Check items off here as we land them.

## TL;DR

- Backfill for a transverse filter over-sends **~100x (single DID)** to
  **~270–580x (rare collection)** at the *event* level, and **~40–265x** at the
  *byte* level, measured against a live 1.73 TB / 22.3 B-event archive.
- This is intrinsic: the archive is laid out by `indexed_at` (ingestion time),
  which is the correct global layout given our hard ordering constraint (see
  "Constraint" below). We **cannot** re-lay-out to fix transverse queries
  without breaking the streaming/global-order case or duplicating the archive.
- Therefore only *order-preserving* levers are legal. Three, in priority order:
  1. **Fix the whole-segment planner cost model** (cheap, a real bug).
  2. **Make sealed frames CDN-cacheable** (cheap, currently `no-cache`).
  3. **Server-side filtered transcode** (heavy; defer until traffic justifies).
- Plus: **add an amplification metric** so real traffic tells us which shape
  actually hurts before we build the heavy lever.

## The constraint that shapes everything

Within a single DID, events MUST be delivered in exactly their original
ingestion (seq) order. Across DIDs there is no ordering requirement. See
`docs/README.md` §"ordering" (lines ~67, 134, 148, 383) and the v0 retro quote
at line ~676:

> I did a v0 of this system storing data by collection... when creating
> real-world AppViews, the requirement to replay events in order for a single
> DID would require a k-way sort if we ordered by collection. That's a deal
> breaker. Ordering by indexed_at timestamp has the property that events within
> a single DID are ordered in the order in which they were created.

So per-DID order is an *emergent property* of the time-order sort, not physical
DID-grouping. A single DID is therefore physically **smeared across the entire
archive timeline** — it is not clustered on disk. This is why a single-DID
backfill scatters across dozens of segments with ~1 match per block.

### Why re-layout is off the table (both variants)

- **Collection-major layout**: dead. This is exactly the v0 experiment above;
  reconstructing per-DID order needs a k-way merge across collection segments.
  Also breaks DID-level markers (`#account`/`#identity`/`#sync` carry no
  collection and must interleave at their seq position within a DID).
- **DID-major derived layout**: preserves per-DID order, but breaks the
  *global/time-ordered* backfill and live-tail path (any "everything since
  cursor X" or multi-DID request would need a k-way merge across DID clusters to
  restore seq order), and duplicates the whole archive. Trades the common case
  to optimize a rare one. Rejected.

Conclusion: the time-ordered layout is a genuine global optimum under the
ordering constraint. The over-send is a property of transverse queries against
it, and can only be attacked with levers that ship the *same rows in the same
seq order* using fewer bytes.

## Architecture recap (for future readers)

- On-disk unit served to clients is a **block**: one independent zstd frame,
  default 4096 events, columnar layout. `getBlock`
  (`internal/xrpcapi/getblock.go:110`) serves the *whole raw frame*; there is no
  sub-block serving. The client decompresses and filters *after* download
  (`internal/client/downloader.go:490`), so any block with ≥1 matching event is
  fetched and decompressed in full.
- The planner (`internal/manifest/plan.go`, `PlanBackfill`) prunes at block
  granularity using per-block DID blooms + a per-block collection index. It has
  a one-sided contract: no false negatives, possible false positives. It is
  already effective at dropping *zero-match* blocks (only ~1% of fetched blocks
  had zero matches in the DID case). It cannot help when nearly every block has
  exactly one match — that is the intrinsic scatter.
- `PlanBackfill` upgrades a segment to **whole-segment mode** when selected
  blocks / total blocks ≥ `WholeSegmentThreshold` (default **0.75**,
  `internal/xrpcapi/planbackfill.go:21`). See finding #1 — this metric is wrong
  for scattered collections.

## Measurements (live archive `http://cpu2-pop3:8080`, 2026-06-30)

Archive totals: **6,355 segments, 22,285,032,455 events, 1,727,752,463,791 bytes
(1.73 TB)**, ~3.5M events / ~272 MB per segment, ~4096 events/block.

Method: throwaway tool (`cmd/bwanalysis`, since removed) ran the real
`planBackfill` over HTTP, fetched planned block frames via `getBlock`, decoded
them with the real `segment.DecodeBlockFrame`, and counted matching-vs-total
events. "Repack floor" = matched events re-encoded into their own block(s) and
compressed with the real codec, i.e. the achievable transport size if the data
were served dense.

### Scenario A — single DID (`did:plc:4uz2445cjiw7w4nobfgnu35f`)

Plan: 176 blocks, all block-mode, across **58 segments**.

| metric | value |
|---|---|
| matching events | 6,995 |
| events actually downloaded | 701,054 |
| **event over-send** | **100.2x** (1 match per 100 events) |
| compressed bytes downloaded | 73.1 MB |
| useful payload (matched CBOR) | 1.7 MB |
| **byte amplification** | **42.8x** |
| repack floor | 0.5 MB → **137.5x** achievable |
| matches/block | min 0, **median 1**, p90 2, max 2595 |
| zero-match blocks | 2 / 176 (1.1%) |

The median of 1 match/block is the signature of intrinsic time-order scatter.

### Scenario B — rare collection `sh.tangled.*`

Full plan: **32,909 block-mode blocks + 69 whole-segment entries** across 3,116
segments. Estimated total download: **~30 GB** (~11.7 GB block-mode + ~19 GB
whole-segment) to deliver ~464K matching events.

| metric | value (300-block sample) | value (800-block sample) |
|---|---|---|
| match rate | 0.368% (1 in 272) | 0.173% (1 in 578) |
| **event over-send** | **271.7x** | **578x** |
| **byte amplification** | **131.4x** | **264.8x** |
| repack floor | **404x** smaller | **593x** smaller |
| matches/block | med 5, p90 14, max 2048 | med 5, p90 13, max 99 |
| zero-match blocks | 0% | 0% |

### Scenario B, whole-segment picks (drives finding #1)

Probed 2 of the 69 whole-segment picks at the *event* level:

| segment | block-coverage (≥1 match) | event-density | over-send |
|---|---|---|---|
| `seg_00000004qv.jss` | 186/200 = **93.0%** | 8362/779088 = **1.07%** | **93x** |
| `seg_00000004r1.jss` | 168/200 = **84.0%** | 1683/676636 = **0.25%** | **402x** |

Both trip the 0.75 block-coverage threshold → upgraded to whole-segment (272 MB)
downloads, despite <1.1% of their *events* matching. Block-coverage is the wrong
proxy. (n=2; re-run across all 69 before sizing the fix — see task 1.)

## CDN / caching findings

- `getBlock` and `getSegment` currently serve **`Cache-Control: public,
  no-cache`** (default `segment-cache-max-age=0`,
  `internal/xrpcapi/getsegment.go:93`). Sealed frames are immutable and ETag'd by
  content checksum, so `no-cache` forces revalidation (RTT + origin egress) on
  every fetch even though the bytes never change. This is the biggest lever for
  the "many clients backfill the same popular filter" shape.
- The `segment-cache-max-age` flag doc notes the real constraint: end-to-end
  deletion-compliance latency = compaction watermark lag + this value, so it
  must stay well under `--compaction-interval`, OR we wire a CDN purge into the
  post-rewrite hook.
- `getBlock` uses `http.ServeContent` (supports conditional/Range). Confirm
  `getSegment` Range requests survive to the CDN (whole-segment mode can't be
  partially cached otherwise) — TODO, not yet verified end-to-end.

## Task list (this note is the tracker)

### 1. Fix the whole-segment planner cost model  — priority: HIGH, size: S
- [ ] Re-run the event-density probe across **all 69** tangled whole-segment
      picks (and ideally another rare collection) to confirm the mechanism and
      size the win beyond n=2.
- [ ] Change `WholeSegmentThreshold` from a block-*count* ratio to an
      **estimated-bytes** comparison: `sum(selected block CompressedSize)` vs
      segment file size. `CompressedSize` is already in `BlockInfo` (footer
      block index), so this needs no new on-disk data. Interim cheaper option:
      gate on *event* density via summed `EventCount`.
- [ ] Keep the one-sided planner contract (no false negatives). This only
      changes transport precision, never which rows are eligible.
- [ ] TDD: a segment that is block-dense but event-sparse must plan as
      `blocks`, not `segment`. Add a planner test with a synthetic segment whose
      blocks each hold exactly one matching event.
- Expected: cuts tangled backfill from ~30 GB toward ~12 GB, all block-mode
  (better cacheability), zero correctness change.

### 2. Make sealed frames CDN-cacheable  — priority: HIGH, size: S
- [ ] Set a non-zero default (or explicit prod value) for
      `segment-cache-max-age` so `getBlock`/`getSegment` emit
      `public, max-age=...` instead of `no-cache`.
- [ ] Wire a CDN purge into the post-compaction/rewrite hook so we can use a
      long max-age without violating deletion-compliance latency (the flag doc
      already anticipates this).
- [ ] Verify `getSegment` Range requests survive to the CDN edge.
- Expected: repeated/overlapping backfills of the same filter become edge hits
  instead of origin egress. Biggest lever for popular-repeated-filter traffic.

### 3. Amplification observability  — priority: MEDIUM, size: S
- [ ] Emit a server-side metric per backfill/plan: `planned_events /
      matched_events` (or the byte-level ratio), so real traffic reveals which
      shape (single-DID vs rare-collection vs popular-repeated) actually drives
      egress once there are consumers.
- Rationale: we have no prod traffic yet. This is the instrument that tells us
  whether task 4 is ever worth building, and where.

### 4. Server-side filtered transcode  — priority: LOW / DEFERRED, size: L
- [ ] **Do not build speculatively.** Revisit only if task-3 metrics show a
      sustained hot filter whose egress hurts after tasks 1–2.
- Sketch: `getBlockFiltered(segment, block, filterHash)` decompresses a block,
  drops non-matching rows, recompresses. Order-preserving (rows still ship in
  seq order). Turns 40–265x byte amplification toward ~1x.
- Costs: response becomes filter-specific → cache key grows to
  `(block-etag, filter-hash)` (shares well for a popular collection like
  tangled, poorly for a unique DID); origin CPU per miss. Best behind an
  origin-shield cache. This is the only lever that dents *intra-block*
  over-send, so it's the eventual answer for a genuinely hot rare filter — but
  only that.

## Rejected / non-starters (record so we don't relitigate)

- Collection-major on-disk layout — v0, killed by per-DID k-way sort.
- DID-major derived segments — breaks global/time-ordered streaming + duplicates
  archive.
- Smaller blocks (halve `MaxEventsPerBlock`) — roughly halves scatter over-send
  but hurts compression ratio, doubles index/bloom overhead and request count.
  Trades CDN request volume for marginal per-request waste; not the lever.
