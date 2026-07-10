# Bloom-filter memory exploration — right-sizing vs. non-residency (2026-07-10)

Status: **measured; decision made (right-sizing only). Filed as #302 (seal-time) and #303 (compaction rewrite).**

Follow-up to §7.2 of `2026-07-08-performance-exploration-cpu2-pop3.md`: per-block
DID blooms are 90%+ of server heap (49 GiB on js1, 88 GiB on js2). This session
measures, with the real archives on cpu2-pop3, exactly how much memory each of
the two candidate strategies saves and what each costs in DID-query latency:

1. **Right-size** per-block blooms to actual block DID cardinality at seal time.
2. **Demote** per-block blooms to non-resident (pread the footer region on demand).

Re-blocking sparse live segments (§7.2 lever 3) is explicitly out of scope.

Method: a throwaway measurement binary (`/data/jcalabro/perf/bloomexplore` on
cpu2-pop3; source in `/tmp/bloomexplore` on the workstation) with three modes:
`stats` (parallel footer scan of every sealed segment; reads each bloom's
persisted `Count` field — gloom serializes the exact number of items added — and
recomputes `gloom.OptimalParams` sizing per block), `harvest` (samples real DIDs
from mid-file blocks scattered across the archive), and `query` (full-archive
DID block selection under three residency models, timed per DID, warm and cold
page cache). Both jetstream servers were **stopped** during this session (they
were running at the end of the 07-09 session; something since stopped them —
worth a mention to Jim), so the box was otherwise idle. All numbers below are
measured on cpu2-pop3 (AMD EPYC 9745, md raid0 NVMe, 1.5 TiB RAM), single-run.

## 1. The core measurement: blooms are sized for events, filled with DIDs

Every per-block bloom is currently built with capacity `DefaultMaxEventsPerBlock`
= 4096 at FP 0.001 (`segment/bloom.go:44`, `seal.go:445`), which marshals to a
fixed 8,409 bytes. But the filter only ever receives the block's **unique DIDs**
(`seal.go:447-449`), and real blocks hold *runs of a few repos*, not 4096
distinct accounts:

| per-block unique-DID count | p1 | p10 | p25 | p50 | p75 | p90 | p99 | max |
|---|---|---|---|---|---|---|---|---|
| js1 (full net, 6,340 segs, 5.54M blocks, 22.5B events, 4,067 ev/block) | 1 | 1 | 2 | **3** | 5 | 9 | 171 | 3,322 |
| js2 (100k repos, 3,280 segs, 11.0M blocks, 11.5B events, 1,045 ev/block) | 1 | 1 | 1 | **1** | 15 | 29 | 65 | 3,175 |

The median block on the full-network archive contains **3 unique DIDs** in a
bloom sized for 4,096. Backfill writes one repo's events contiguously (getRepo),
so dense 4,091-event blocks are nearly single-DID; js2's live-written blocks are
DID-diverse per event but hold ~1k events. The 8.4 KB/block cost is ~1000×
oversized at the median. This is the whole ballgame.

## 2. Strategy 1 — right-sizing (measured exactly, from persisted counts)

`gloom` filters serialize their item count, so the scan recomputes what
`OptimalParams(actual_count, fp)` would have allocated for every one of the
16.6M blocks. Two variants:

- **Uniform per segment** (capacity = max block DID count in that segment):
  **requires no format change** — the on-disk region already stores
  `bloom_size_bytes` per segment and only requires all blooms *within one
  segment* to be equal (`segment/bloom.go:52-58`); the reader derives
  everything from the region header.
- **Per-block variable** (each bloom exactly sized): requires a format change
  (per-bloom offsets instead of indexing by multiplication).

At the current FP target (0.001):

| resident GiB | current | uniform/segment | per-block | segment-level blooms |
|---|---|---|---|---|
| js1 | 44.3 | **2.2** (−95%) | 1.8 | 0.08 |
| js2 | 88.1 | **12.9** (−85%) | 3.1 | 0.10 |

At a 10× stricter FP target (0.0001), in case we ever want fewer false-positive
block decodes: js1 2.6 GiB, js2 18.0 GiB uniform — still a ~7–17× reduction.
Worst realized per-block FP stays exactly at target in all variants (the tool
verifies via gloom's Poisson estimator against each block's real count).

Notes:

- The uniform-per-segment variant captures almost all of the win on js1
  (backfill segments are homogeneous). On js2 the gap to per-block (12.9 vs
  3.1 GiB) exists because live segments occasionally contain one DID-dense
  block (max 3,175) that inflates the whole segment's uniform size. Still, 88
  → 13 GiB without touching the format.
- **Query latency cost: none.** Same one-sided contract, same FP target, less
  memory traffic per test. The change surface is seal-time only
  (`buildFooter`: capacity = max unique DIDs across the walked blocks — the
  walk already collects per-block DID sets, so the count is free).
- **Old archives don't shrink by themselves.** The format is self-describing,
  so right-sized and legacy segments coexist freely. The compaction rewrite
  path currently *pins* the source segment's bloom params
  (`rewrite.go:58-66`, via `bloomParamsFromFilter(BlockBloom(0))`) — flipping
  that to recompute right-sized params during rewrites would shrink existing
  archives incrementally as tombstone compaction touches them, no migration
  step needed. A one-shot rewrite pass is also possible if we want the RAM
  back immediately.
- On-disk savings ride along (43.4 → 1.2 GiB on js1, 86.3 → 11.0 GiB on js2),
  ~2.5% of archive bytes — nice but not the point.

## 3. Strategy 2 — non-resident blooms (pread on demand)

The query tool compares three residency models over the *whole archive per
DID*, mirroring `manifest.SelectBlocksForDID` (the planBackfill/repoexport
path — serve/replay never touches per-block blooms):

- `resident`: today's model — everything in heap.
- `pread`: segment-level blooms stay resident (they're only ~0.1 GiB and
  provide the candidate-pruning short-circuit); per-block bloom regions are
  pread + unmarshaled per query via `Reader.BlocksContainingDID`, fanned out
  over candidate segments with bounded parallelism. Readers stay open (one fd
  per sealed segment; 6,340 fds — production would do the same via manifest).
- Cold variant: `fadvise(DONTNEED)` on every segment before each query.

Heap and startup:

| | resident heap | pread heap | startup (resident) | startup (pread) |
|---|---|---|---|---|
| js1 | 50.5 GiB, 6.6 s | **0.8 GiB, 0.3 s** | | |
| js2 | 100.2 GiB, 12.0 s | **1.4 GiB, 0.4 s** | | |

(The pread heap is block index + collection index + segment blooms — i.e. the
non-negotiable manifest metadata. Startup also stops paying the cumulative
292 s (js1) / 394 s (js2) of LoadAllBlockBlooms CPU currently burned across
manifest-open workers.)

Full-archive DID selection latency (50 real DIDs js1 / 39 js2, 20 nonexistent
DIDs, warm page cache):

| | existing p50 | existing p90 | existing max | missing p50 |
|---|---|---|---|---|
| js1 resident | 1.4 ms | 4.4 ms | 6.6 ms | ~0 µs |
| js1 pread par=16 | 8.2 ms | 44.5 ms | 60.4 ms | 5.0 ms |
| js1 pread par=64 | 7.1 ms | 45.7 ms | 75.5 ms | 5.1 ms |
| js1 pread par=64 **cold cache** | 41.4 ms | 63.2 ms | 70.4 ms | 7.0 ms |
| js2 resident | 5.2 ms | 62.6 ms | 365.6 ms | 1.1 ms |
| js2 pread par=16 | 49.2 ms | 423.9 ms | 2.73 s | 15.7 ms |
| js2 pread par=64 | 35.0 ms | 410.3 ms | 2.28 s | 14.9 ms |

Reading the tail: pread cost scales with the number of *candidate segments*
(segment-bloom hits), because each candidate costs one region pread (~7 MB on
js2's 4k-block segments) + unmarshal of every block bloom in it. The 2.3 s
worst case is a bot-like DID appearing in 1,364 of 3,280 segments (10,787
blocks); heavy multi-segment DIDs (100+ segments) sit in the 150–450 ms band.
Missing DIDs cost ~5 ms (js1) / ~15 ms (js2): segment blooms at FP 0.001 over
6,340 segments produce a handful of false-positive candidates whose regions
must be pread to clear.

For context, gloom unmarshal micro-costs (workstation, single-thread): 8,409 B
bloom ≈ 1.7 µs, 153 B ≈ 0.12 µs — the pread path is bandwidth/unmarshal-bound
on big regions, not syscall-bound.

Is 35 ms p50 / 2.3 s max acceptable? The consumers are planBackfill (one call
per client backfill negotiation) and repoexport — both already dominated by
block download/decode downstream. §5 of the 07-08 note measured the *whole*
3-DID filtered backfill at 0.59 s; adding tens of ms of selection is invisible.
It is NOT a serve/subscribe-path regression — that path never reads these.

## 4. The interaction the strategies have (this is the interesting bit)

The two strategies compose multiplicatively for the pread path: right-sizing
shrinks the region a pread must fetch by the same ~10–40× it shrinks heap
(js2's ~7 MB regions become ~200 KB), so **right-sizing first makes
non-residency nearly free** — the pread tail shrinks proportionally.

But right-sizing alone also makes non-residency *unnecessary* at current
scale: 2.2 GiB (js1) / 12.9 GiB (js2) resident is a rounding error on these
boxes, and AGENTS.md's "RAM is cheap" stance was only wrong because of the
1000× oversizing, not the architecture.

Recommended sequencing if we act on this:

1. **Right-size at seal, uniform per segment** — no format change, seal-time
   only, zero latency cost, −95%/−85% heap. Do this first; it's a pure win.
2. **Recompute (don't pin) bloom params in compaction rewrites** — existing
   archives shrink as compaction touches them. One-line-ish change in
   `rewrite.go`; also fixes the current behavior where a rewrite of a legacy
   segment re-inherits oversized blooms forever.
3. **Only then decide on residency.** If heap still matters (e.g. much bigger
   archives, cheaper boxes), the pread model measured here is a
   straightforward manifest change (drop `BlockBlooms` from
   `SegmentMetadata`, route `SelectBlocksForDID` through per-segment
   `BlocksContainingDID` with bounded fan-out) with the latency profile in §3
   — improved further by right-sized regions. An LRU adds complexity for a
   working set (\~2–13 GiB post-right-sizing) that plainly fits; skip it
   unless measurements after (1)+(2) say otherwise.

Per-block variable sizing (format change) buys js2 another 12.9 → 3.1 GiB but
zero on js1's already-uniform segments. Not worth the format churn now; if
sparse live segments ever get re-blocked at compaction (§7.2 lever 3), their
blocks densify and the uniform variant converges toward per-block anyway.

## 5. Loose ends / observations recorded while measuring

- `manifest.Open`'s per-segment `LoadAllBlockBlooms` is also the dominant
  *startup CPU* (292–394 s cumulative across workers); right-sizing shrinks it
  ~20×, non-residency eliminates it.
- The segment-level blooms are cheap (0.08–0.10 GiB resident across the whole
  archive) and load in one pread each; they should stay resident under any
  strategy — they're what makes both the resident scan and the pread
  candidate-pruning fast.
- gloom's serialized `Count` field is what made the exact retrospective sizing
  possible — worth remembering it's there.
- Both jetstream servers on cpu2-pop3 were stopped at session start — Jim
  confirmed he stopped them intentionally.
- js2's 11.0M blocks vs js1's 5.5M (half the events, 2× the blocks) restates
  the sparse-live-segment problem — deferred, lever 3.

## 6. State left behind

- Servers on cpu2-pop3: still stopped (as found). Archives untouched — all
  scans opened segments read-only with checksum verification skipped.
- `/data/jcalabro/perf/bloomexplore` (measurement binary),
  `dids-js1.txt` / `dids-js1-small.txt` / `dids-js2.txt` (harvested DID
  samples) on cpu2-pop3.
- Workstation: tool source at `/tmp/bloomexplore/` (module with `replace` to
  the repo checkout; `main.go` has stats/harvest/query modes, `microbench/`
  has the gloom unmarshal micro-benchmark). Throwaway — rebuild from this
  note if needed.
