# Oracle Mutation Campaign Results

Each campaign appends a dated section; history is never overwritten so the
oracle's detection power is visible over time. See
`docs/superpowers/specs/2026-06-12-oracle-mutation-campaign-design.md` for the
method and `testing/mutation/run.sh` for the driver.

**Current catalog (keep this line current): 25 active mutants on disk
(m001–m027; m007 and m010 retired). Latest full campaign: 2026-06-21 at
`df3fc4b` — 18 killed, 7 survived over m001–m027 (the authoritative current
scorecard; see the dated section at the end of this file). This is the run that
seeded `testing/mutation/baseline.json` and is now the enforced gate baseline
(#108). Counts inside older dated sections describe the catalog *as of that
date* and are intentionally not back-edited.**

## The baseline gate (#108)

This prose scorecard is the human record; the **enforced** scorecard is
`testing/mutation/baseline.json` — a machine-readable `{commit, mutants:[{id,
disposition, ...}]}` document. The scheduled `mutation-campaign` workflow
(`.github/workflows/mutation-campaign.yml`) runs the full campaign at HEAD,
emits a result with `run.sh --json`, and diffs it against the baseline via
`testing/mutation/gate`. The job **fails** on:

- a **KILLED→SURVIVED regression** (the oracle lost detection power),
- a **STALE** or **BUILD-BROKEN** patch (a mutant that no longer applies or
  compiles), and
- **catalog drift** — a baseline mutant missing from the run, or a new mutant
  the baseline does not record (so a mutant can't be added or dropped without
  recording its disposition), or an unrecognised disposition.

A **SURVIVED→KILLED improvement** is reported but does **not** fail the job;
bank it by refreshing the baseline. This is what converts the prose above from
something that silently drifted (the bug #108 was filed for) into an enforced
contract. It is also the anti-vacuity guard the #110 m005 re-home relies on: if
the merge rev-filter branch ever goes dead again, m005 flips KILLED→SURVIVED and
this gate catches it.

**Refresh the baseline** (and review the diff before committing) whenever you
intentionally add/retire a mutant or bank an improvement:

```bash
just mutation-baseline   # full campaign at HEAD -> testing/mutation/baseline.json
# or run the gate locally without rewriting the baseline:
just mutation-gate
```

The baseline's `disposition` field is the coarse verdict
(`KILLED|SURVIVED|STALE|BUILD-BROKEN`); the per-mutant `result`/`note` carry the
tier/seed detail the gate ignores. A seed-sensitive mutant (e.g. m002) is
recorded by its full-campaign fixed-seed disposition; the gate does not re-run
seed sweeps.

## Retired mutants

The active catalog no longer carries mutants that have been reclassified as
stale/dead under the current implementation. Their historical scorecard rows
remain below so the reasoning is not lost.

| mutant | retired | reason |
|---|---|---|
| m007_compaction_chunk_boundary | 2026-06-15 | Invalid/dead under current compaction chunk construction; the modeled corrupt shape cannot be produced by current chunk snapshots. |
| m010_nextblockoffset_reset | 2026-06-15 | Stale/dead for sealed oracle observations; `Writer.Seal` rebuilds footer block metadata from physical frames. |

## Active catalog check 2026-06-15 — retired mutants removed

- commit under test: `767792e`
- driver: `just mutation-campaign`
- catalog: 17 active mutants; retired mutants `m007` and `m010` are absent
- purpose: verify the active catalog still runs after removing stale/dead
  legacy patches

| mutant | result | note |
|---|---|---|
| m001_delete_mapped_to_update | KILLED@default | default oracle killed the mutant |
| m002_watermark_floor_off_by_one | SURVIVED | known seed-dependent/future-roadmap gap |
| m003_merge_cursor_no_advance | SURVIVED | known restart-depth gap |
| m004_rev_filter_inverted | KILLED@default | missing expected event diagnostic |
| m005_backfill_status_check_inverted | KILLED@stress | rev-regression diagnostic |
| m006_merge_commit_error_swallowed | SURVIVED | known store-fault gap |
| m008_header_offset_byteslice | KILLED@default | default oracle killed the mutant |
| m009_checksum_range_off_by_one | SURVIVED | known closed-loop checksum blind spot |
| m011_wire_frame_length | KILLED@default | active segment walk failed |
| m012_block_event_count_off_by_one | KILLED@default | default oracle killed the mutant |
| m013_collection_rkey_swap | SURVIVED | known dead path in this simulator config; companion `m017` covers hot path |
| m014_rev_dropped | SURVIVED | known dead path in this simulator config; companion `m018` covers hot path |
| m015_collection_count_double | SURVIVED | known read-path index blind spot |
| m016_bloom_size_off_by_one | KILLED@default | default oracle killed the mutant |
| m017_commit_collection_rkey_swap | KILLED@default | event-log mismatch diagnostic |
| m018_commit_rev_dropped | KILLED@default | event-log mismatch diagnostic |
| m019_sync_tombstone_dropped | KILLED@default | event-log equivalence caught missing sync row |

## Targeted follow-up 2026-06-15 — event-log equivalence

- commit under test: branch `testing-revamp` after
  `oracle: assert lifecycle event-log equivalence`
- driver: targeted manual equivalent of `just mutation-campaign m019`
  (`git apply --check`, apply, `go build ./...`, default oracle tier, reverse)
- scope: Workstream 3 event-log equivalence

| mutant | result | disposition |
|---|---|---|
| m019_sync_tombstone_dropped | KILLED@default | New event-log equivalence assertion caught a missing `KindSync` row even though replacement rows can allow final state to converge. Diagnostic: `oracle: missing expected event ... kind=sync`. |

## Targeted follow-up 2026-06-15

- commit under test: branch `testing-revamp` after
  `oracle: reclassify stale mutation expectations`
- driver: targeted `just mutation-campaign m018`, `m010`, and `m007`
  before retiring stale/dead mutants `m010` and `m007` from the active catalog
- scope: Phase 1 of the oracle robustness roadmap

| mutant | result | disposition |
|---|---|---|
| m018_commit_rev_dropped | KILLED@default | Fixed by rejecting empty `Rev` on commit-kind observed events. |
| m010_nextblockoffset_reset | SURVIVED | Reclassified stale/dead for sealed oracle observations. `Writer.Seal` rebuilds footer block metadata by walking physical frames, so the mutated `Writer.nextBlockOffset` only corrupts active `Writer.Blocks()` metadata; existing segment tests already cover that active API. |
| m007_compaction_chunk_boundary | SURVIVED | Reclassified invalid/dead under current compaction chunk construction. A row at `seq == chunkEnd` can only be dropped if the same chunk snapshot contains a tombstone with `seq > chunkEnd`; merge and steady chunk snapshots are both bounded to `<= chunkEnd`, and later chunks still rewrite older rows. |

The original campaign remains below as historical data. These follow-up
results correct two stale mutant interpretations rather than counting them as
oracle assertion work.

## Campaign 2026-06-15

- commit: `75d9251` (branch `mutation-campaign`)
- default seed: 42; survivors swept with 5 random stress seeds where the
  hypothesis was seed-dependent (see "Seed sweeps" below)
- catalog: 18 mutants — orchestrator 6, segment 7, live 5
- driver: `just mutation-campaign`; tiers escalate default → stress → restart
- runtime: full campaign ≈ 14 min (several kills land via the harness
  5-minute after-bootstrap barrier timeout, not a fast assertion)

### Scorecard

| mutant | subsystem | expected | actual | note |
|---|---|---|---|---|
| m001_delete_mapped_to_update | live | default | KILLED@default | delete archived as update → extra record (via bootstrap barrier timeout) |
| m002_watermark_floor_off_by_one | orchestrator | stress | KILLED@stress (4/5 seeds) | flaky detection — assertCompacted, seed-dependent |
| m003_merge_cursor_no_advance | orchestrator | stress | SURVIVED | ESCAPE — restart tier does not exercise this crash seam |
| m004_rev_filter_inverted | orchestrator | default | KILLED@default | `oracle: missing … app.bsky.feed.post/…` |
| m005_backfill_status_check_inverted | orchestrator | default | KILLED@stress | `oracle: rev regression for DID …` (needed scale) |
| m006_merge_commit_error_swallowed | orchestrator | stress | SURVIVED | ESCAPE (predicted) — needs store-fault injection |
| m007_compaction_chunk_boundary | orchestrator | stress | SURVIVED (5 seeds) | ESCAPE — boundary row never re-evaluated |
| m008_header_offset_byteslice | segment | default | KILLED@default | corrupt header offset → segment open fails |
| m009_checksum_range_off_by_one | segment | default | SURVIVED | ESCAPE — symmetric checksum (write==read), closed loop |
| m010_nextblockoffset_reset | segment | default | SURVIVED (5 seeds) | ESCAPE — block read by index, not recorded offset |
| m011_wire_frame_length | segment | stress | KILLED@default | `walk active frames: …` torn-tail on reopen |
| m012_block_event_count_off_by_one | segment | default | KILLED@default | block decode truncated/trailing bytes |
| m013_collection_rkey_swap | live | default | SURVIVED | dead path in this config — see m017 (companion on hot path) |
| m014_rev_dropped | live | default | SURVIVED | dead path in this config — see m018 (companion on hot path) |
| m015_collection_count_double | segment | none (survival) | SURVIVED | confirmed blind spot — footer collection index unread by oracle |
| m016_bloom_size_off_by_one | segment | none (survival) | SURVIVED | confirmed blind spot — bloom filters unread by oracle |
| m017_commit_collection_rkey_swap | live | default | KILLED@default | `oracle: missing … app.bsky.feed.post/…` — hot path IS exercised |
| m018_commit_rev_dropped | live | default | SURVIVED | ESCAPE — rev never compared (see Escapes) |

Summary: **8 killed, 10 survived.** Of the 10 survivors, 2 were predicted
(m015/m016 blind-spot demonstrators), 2 are dead-code-path artifacts that
their companions m017/m018 corrected, and 6 are genuine escapes or
escape-class findings.

Note on catalog growth: m013/m014 originally targeted `convertVerifiedOps`,
the async-resync fallback branch. The simulator emits real `#commit`
envelopes, which route through `convertCommit`, so those two mutated a path
this config never executes. m017/m018 were added mid-campaign to mutate the
same two fields on the hot path. This is itself a finding: the resync/
unknown-envelope conversion path has zero oracle coverage.

### Seed sweeps

Only seed-dependent hypotheses were swept; structural escapes (m009, m013,
m014, m015, m016, m018) survive by construction and gain nothing from more
seeds, and m003/m006 are restart-timing-dependent rather than seed-dependent,
so they were not swept. This is a deliberate scope limit, not full coverage.

- **m002**: KILLED on 4 of 5 random stress seeds → *flaky detection*. The
  oracle CAN see this watermark off-by-one, but only when the boundary seq
  happens to carry a superseded create/update. This is the single most
  actionable operational result: it argues the nightly `oracle-sweep` seed
  count is load-bearing, not decorative — a single-seed CI run would have a
  ~20% chance of missing this real bug.
- **m007**: SURVIVED all 5 seeds → originally treated as a true escape; later
  reclassified as invalid/dead under current compaction chunk construction and
  retired from the active catalog.
- **m010**: SURVIVED all 5 seeds → originally treated as a true escape; later
  reclassified as stale/dead for sealed oracle observations and retired from
  the active catalog.

### Escapes — analysis and disposition

**m018 / m014 — dropped `rev` is invisible (oracle gap; fix recommended).**
`invariants.go:21` skips events with empty rev, and `compare.go:35` compares
rev only when *both* sides populate it — but ground truth never populates rev
(`model.go` RecordValue.Rev doc). So dropping rev entirely blinds both
checks. m017 proves the hot path is otherwise exercised (the collection/rkey
swap on the same struct was caught instantly), so this is a true gap, not a
dead path. Disposition: **fix the oracle** — have CheckInvariants reject an
empty rev on a commit-kind event (a create/update/delete must carry a rev),
which costs nothing and closes the hole. This gap was closed in Milestone A.

**m009 — symmetric checksum (oracle structurally cannot catch; accepted).**
`xxh3HeaderFooter` is used both to write the seal checksum (seal.go:123) and
to verify it on read (reader.go:193). A mutation to its byte range changes
both sides identically, so they always agree. This is a miniature of the
"atmos closed loop" blind spot described in `docs/oracle/DESIGN.md`: the oracle cannot
detect a bug that lives in a function shared by the writer and reader.
Disposition: **accepted blind spot** — only an independent checksum oracle
(or a committed golden segment with a known-good checksum) would catch it.
Cross-referenced in the oracle design document.

**m010 — block read by index, not by recorded offset (historical, retired).**
`DecodeBlock` (reader.go:301) seeks via the block-index entry's offset, and
the oracle decodes blocks 0..N by index. The `nextBlockOffset` bookkeeping the
mutant corrupts feeds a path the oracle's read does not depend on in this
config. A real consumer using offset-based seeks could diverge. Disposition:
**fix the oracle** candidate — add a read mode that follows recorded offsets,
or assert offset monotonicity in ObserveSegments. Later Milestone A review
found this mutant stale/dead for sealed oracle observations because
`Writer.Seal` rebuilds footer metadata by walking physical frames; the active
patch was retired on 2026-06-15.

**m007 — compaction boundary row never re-evaluated (historical, retired).**
The `>` → `>=` weakening keeps the row at exactly chunkEnd, and because the
watermark advances to chunkEnd it is never revisited. assertCompacted did not
catch it across 6 seeds. Disposition: **fix the oracle** — CheckCompacted
should assert that the boundary seq itself is evaluated, not just rows
strictly below it. Later Milestone A review found this mutant invalid/dead
under current compaction chunk construction because the modeled corrupt shape
cannot be produced by current chunk snapshots; the active patch was retired on
2026-06-15.

**m003 — merge-cursor off-by-one not exercised by restart tier (real gap).**
The restart oracle only covers 4 enumerated crashpoints; a merge-cursor
double-process needs a crash precisely between source completion and the next
run, which the current harness does not stage for this seam. Disposition:
**fix the oracle** — extend the restart harness to crash at the
source-complete seam, OR (better) the random-time kill loop from
the crash/restart tier in `docs/oracle/DESIGN.md` would cover this class
without enumeration.

**m006 — swallowed commit error needs store-fault injection (predicted).**
Predicted to survive: under normal runs `commitSourceComplete` never fails,
so the inverted check is dormant. Confirms the store-fault tier requirement in
`docs/oracle/DESIGN.md`: the oracle has no way to make a store write fail.
Disposition: **accepted, pending** the store-fault oracle tier.

**m015 / m016 — footer/bloom read-path indexes (confirmed blind spots).**
Predicted survival, confirmed. The oracle decodes every block sequentially
and never consults the footer collection-count index or the per-block bloom
filters, so corruption there is invisible. These mutants exist to *document*
the gap with evidence. Disposition: **accepted blind spots** — would be
closed by the product replay and XRPC egress tiers in
`docs/oracle/DESIGN.md`, which exercise the read indexes a client uses.

### Prediction misses (corrections to our model of the oracle)

- **m005** predicted default, killed at **stress**: the inverted backfill
  status check only produces a detectable rev regression at scale; default
  mode's smaller overlap window didn't surface it. The oracle's default tier
  is weaker on merge-dedup bugs than assumed.
- **m011** predicted stress, killed at **default**: torn-tail truncation on
  reopen is caught immediately; no scale needed.
- **m002** predicted a clean stress kill; actual is **probabilistic** (4/5).
  Detection of boundary bugs is seed-sensitive — the most important
  correction, see Seed sweeps.
- **m013/m014** predicted default kills; actually mutated a **dead path** in
  this simulator config. Correction: not every plausible-looking production
  line is on the path simulator traffic takes; companions m017/m018 were
  required to test the hot path.

### Bottom line

The campaign measured the oracle rather than re-confirming it. It caught
every hot-path data-shape bug we threw at it (m001, m004, m008, m011, m012,
m017) and several at scale (m005), but it surfaced **two active oracle gaps
worth fixing** (m018 rev-blindness and m003 merge-cursor restart seam), **three structural blind spots**
now named in `docs/oracle/DESIGN.md` (m006 store-fault, m009 closed-loop
checksum, m015/m016 read-path indexes), and one operational signal — **m002's flaky
detection justifies the multi-seed nightly sweep**. The over-fitting worry was
warranted in specific, now-documented places; it was not warranted as a
blanket claim about the oracle.

## Campaign 2026-06-15 (getTombstones overlay)

- commit: `bb135af` (branch `feat/gettombstones-overlay`)
- default seed: the driver's default tier seed
- catalog: 4 new mutants — overlay encoder 3 (m020, m021, m023), tombstone 1 (m022)
- target test: `TestOracle_DefaultLifecycle`, which now runs
  `assertOverlayReconstruction` (segments(<=W) + overlay((W,M]) +
  live((M,inf)) reconstruction must equal ground truth) alongside the
  existing compacted/oracle checks
- driver: `just mutation-campaign mNNN`; all four are `tiers: default`
- review refresh: renamed `m019_overlay_drop_record_tombstones` to
  `m023_overlay_drop_record_tombstones` to avoid the main-branch `m019`
  duplicate, and refreshed `m020_overlay_drop_did_tombstones` after the
  DID-tombstone delta-order hardening so the patch applies again. The oracle
  lifecycle now stages a late account-delete tombstone inside `(W, M]`, closing
  the prior m020 dead path. Targeted manual driver equivalent confirmed both
  `m020` and `m023` are `KILLED@default`.

### Scorecard

| mutant | subsystem | expected | actual | note |
|---|---|---|---|---|
| m020_overlay_drop_did_tombstones | overlay | default | KILLED@default | late account-delete tombstone forced into `(W,M]`; dropping DID tombstones makes the overlay poll fail with `did_tombstones=0` while `M` covers the tombstone seq |
| m021_overlay_record_seq_base_zero | overlay | default | KILLED@default | record seq delta encoded against base 0 not W; decoder re-adds W, inflating tombstone seqs above live records → `failed to emit a live record` |
| m022_shoulddrop_did_seq_inverted | tombstone | default | KILLED@default | `>`→`<` in ShouldDrop DID branch; caught by the compacted oracle and/or reconstruction |
| m023_overlay_drop_record_tombstones | overlay | default | KILLED@default | record-tombstone group count forced to 0 → deleted record in (W,M] emitted; `emitted a record that ground truth deleted` |

Summary: **4 killed, 0 survived.** The kills confirm the reconstruction
assertion has detection power on both overlay sections (m020 DID tombstones,
m023 record tombstones), on seq-delta base correctness (m021), and on the
shared ShouldDrop suppression logic (m022).

## Targeted follow-up 2026-06-20 — compaction over-drop check (#100)

- commit: `fed7c1b` (branch `oracle-improvements`)
- new mutant: `m024_compaction_over_drop_survivors` (1 new; catalog now 22
  active, m001–m024 with m007/m010 retired)
- target test: `TestOracle_DefaultLifecycle`, which now runs a metamorphic
  compaction over-drop check (`compactionOverDropRecorder.Assert`): a
  pre-rewrite sealed-segment snapshot (via the new `OnBeforeCompactionPass`
  hook) is compared against the post-rewrite snapshot at the same watermark,
  asserting every row the documented compaction filter says survives is still
  present. Closes the §4.2 over-drop / data-loss blind spot.
- driver: `just mutation-campaign m024`

### Scorecard

| mutant | subsystem | expected | actual | note |
|---|---|---|---|---|
| m024_compaction_over_drop_survivors | compaction | default | KILLED@default | rewrite keep-guard `RowKeep`→`RowDrop`; blanket over-drop of survivors, caught at after-merge final-state `Compare` |

Summary: **1 killed, 0 survived.**

Honest scope note (verified empirically + structurally, not assumed): m024 is a
*blanket* over-drop, so it also deletes permanently-live rows and is killed by
final-state `Compare` at the after-merge barrier — **before** the new
metamorphic over-drop check runs. m024 therefore proves the over-drop class is
caught, but does **not** demonstrate the new check's UNIQUE power: catching an
over-drop that final-state convergence hides (a survivor dropped at/below W but
independently superseded above W).

Five candidate single-edit over-drop mutations (widening the steady tombstone
`SnapshotRange` low/high bounds, the merge-path `FoldRange` bound, and the
`ev.Seq > chunkEnd` keep-guard) were tested and **all survived the default
scenario** — they are equivalent/dead mutants in steady mode: each compaction
pass force-rotates the active segment first, so the target watermark W covers
every event in existence and the live tombstone set never holds a tombstone
above W. A single production bug thus cannot produce a convergence-hiding
over-drop in steady mode; it is structurally unreachable.

The new check's unique power is proven by the unit test
`TestCompareEventLogsCompactedMultisetToleratesReorderingButCatchesOverDrop`
(a survivor dropped but deleted above W: final-state converges, the check
fails). An end-to-end convergence-hiding mutant is deferred to the crash-mid-
pass restart tier (#103), where a pass can be interrupted with W not covering
every event — the regime in which the unique case becomes reachable.

## Campaign 2026-06-20 — full catalog at HEAD (#101)

- commit under test: `b937b6e` (branch `oracle-improvements`)
- driver: `testing/mutation/run.sh` (full catalog), plus `--seeds 5` sweeps for
  the two seed-sensitive movers (m002, m005)
- catalog: 22 active mutants (m001–m024; m007/m010 retired)
- context: first full campaign since 2026-06-15 (baseline `bb135af`); 139
  commits intervened, including the client tier (#77), the bisect work, the
  event-log rework, and this branch's #99/#100 changes. Run to re-establish a
  trustworthy scorecard and to validate the refreshed m022 patch and the new
  m024 mutant.
- harness fix landed alongside: the default tier now passes `-timeout 5m`, so a
  mutant that breaks liveness (m001) is killed fast instead of hanging on Go's
  silent 10m default.

### Scorecard

| mutant | result | note |
|---|---|---|
| m001_delete_mapped_to_update | KILLED@default | kills via test-timeout hang (delete->update stalls the bootstrap seq-ack contiguity wait → after-bootstrap barrier never releases). Baseline also recorded this as a barrier-timeout kill (not a fast assertion); this campaign only makes the bound explicit at 5m instead of relying on Go's silent 10m default — the failure mode is unchanged. |
| m002_watermark_floor_off_by_one | SURVIVED@seed42 / KILLED@stress(4/5 seeds) | seed-dependent, unchanged from baseline. The fixed campaign seed (42) is one of the ~1/5 that survives; a 5-seed sweep reproduced 4/5 kills. NOT a regression. |
| m003_merge_cursor_no_advance | SURVIVED | unchanged — restart tier does not stage the merge-cursor crash seam (real gap, tracked). |
| m004_rev_filter_inverted | KILLED@default | `oracle: missing … app.bsky.actor.profile/…` |
| m005_backfill_status_check_inverted | SURVIVED (6 seeds: 42 + 5 random) | **REGRESSION from baseline KILLED@stress.** Root-caused (see below): the merge rev-filter runs over 7129 completed-repo events but every live event carries rev > backfillRev, so the inverted guard never changes the output — equivalent in this scenario. Filed as a coverage gap. |
| m006_merge_commit_error_swallowed | SURVIVED | unchanged — predicted, needs store-fault injection (#30). |
| m008_header_offset_byteslice | KILLED@default | corrupt header offset → segment open fails. |
| m009_checksum_range_off_by_one | SURVIVED | unchanged — symmetric checksum closed loop (#32 corpus/golden-segment gap). |
| m011_wire_frame_length | KILLED@default | `walk active segment …: segment: walk active frames` torn-tail on reopen. |
| m012_block_event_count_off_by_one | KILLED@default | block decode truncated/trailing bytes. |
| m013_collection_rkey_swap | SURVIVED | unchanged — dead path in this config; companion m017 covers the hot path. |
| m014_rev_dropped | SURVIVED | unchanged — dead path in this config; companion m018 covers the hot path. |
| m015_collection_count_double | SURVIVED | unchanged — footer collection index unread by oracle (known blind spot). |
| m016_bloom_size_off_by_one | KILLED@default | (was SURVIVED at baseline) bloom-size corruption now caught at default. Improvement. |
| m017_commit_collection_rkey_swap | KILLED@default | `oracle: event mismatch … key=app.bsky.feed.like/…` hot path exercised. |
| m018_commit_rev_dropped | KILLED@default | **IMPROVEMENT from baseline SURVIVED** — the event-log tier now compares rev (`oracle: event mismatch … rev=`), closing the documented rev-never-compared escape. |
| m019_sync_tombstone_dropped | KILLED@default | event-log equivalence catches the missing `kind=sync` row. |
| m020_overlay_drop_did_tombstones | KILLED@default | overlay reconstruction. |
| m021_overlay_record_seq_base_zero | KILLED@default | overlay reconstruction. |
| m022_shoulddrop_did_seq_inverted | KILLED@default | **patch refreshed this campaign** (context → `IsMaterialization()`); now applies and kills again. |
| m023_overlay_drop_record_tombstones | KILLED@default | overlay reconstruction. |
| m024_compaction_over_drop_survivors | KILLED@default | **new** (#100); blanket compaction over-drop caught by final-state `Compare`. |

Summary: **14 killed, 8 survived.** Movement vs. the 2026-06-15 baseline:
- **m018 SURVIVED → KILLED** (improvement: rev now compared by the event-log tier).
- **m016 SURVIVED → KILLED** (improvement at default).
- **m005 KILLED → SURVIVED** (regression; root-caused below, coverage gap filed).
- **m002** apparent change is fixed-seed variance, not a regression (4/5 sweep confirms).
- m022 refreshed; m024 added (both KILLED).
- All other mutants unchanged from baseline disposition.

### m005 regression — root cause (diagnosed, verified)

The mutated guard inverts "skip rev-filtering unless backfill is complete"
(`merge_filter.go` `shouldKeep`, `!= StatusComplete` → `== StatusComplete`), so
completed-backfill repos bypass rev filtering. The kill required a
completed-repo live event with `rev <= backfillRev` to survive into the merged
tree and trip the `rev regression` invariant (`invariants.go:32`).

File-based instrumentation of `shouldKeep` on a stress run (seed 42) showed the
merge processes **7129** completed-repo rev-filterable events, but **every one**
has `ev.Rev > st.Backfill.Rev` (live events arrive strictly newer than the
backfill head). **Zero** events hit the `rev <= backfillRev` case, so the
inverted guard and the correct guard produce identical output — m005 is an
equivalent mutant in the current scenario.

(Methodology note: an initial `fmt.Fprintf(os.Stderr, …)` probe reported "0
calls" and was wrong — the oracle harness's `go test` stderr buffering swallows
sub-goroutine writes. A `panic()` probe proved `shouldKeep` *is* called; the
file-based probe then gave the real distribution. Recorded so the next
investigator does not repeat the stderr mistake.)

This is a real loss of detection power caused by simulator/scenario drift (the
merge live-overlap traffic no longer generates an in-flight event below the
backfill rev for an already-completed repo), not a deleted assertion. Tracked
as a follow-up coverage gap: the merge rev-filter tier needs traffic that
stages a completed-repo event at/below the backfill snapshot rev.

### m007 retirement re-confirmed (#101)

The report's §5 flagged m007's retirement rationale as possibly fragile under
the merge-mode tombstone cap. Reconstructed the historical m007 patch
(`ev.Seq > chunkEnd` → `>=` in `applyCompactionChunk`) and ran it at HEAD: it
**SURVIVED** both default(-short) and stress(seed 42), consistent with the
retirement. Reasoning confirmed: with `CompactionTombstoneCap:1` the chunk
snapshot is bounded so the boundary row at `seq == chunkEnd` is never
superseded by a tombstone with `seq > chunkEnd`, making `>` vs `>=`
behaviorally equivalent. m007 stays retired (dead/equivalent), not a true
escape.

## Campaign 2026-06-20 (m003 re-disposition — benign, not a gap)

- scope: re-characterize **m003** only, via the restart tier, after #103
  investigation; no full re-run.
- context: prior sections (2026-06-12, 2026-06-15, 2026-06-20 b937b6e) all
  recorded m003 as a SURVIVED **real gap** — "restart tier does not stage the
  merge-cursor crash seam." That disposition is **wrong on both counts** and is
  corrected here (history above is left intact per the no-back-edit convention).

### m003 is benign / equivalent in the current scenario — NOT a real gap

Two empirical corrections, both verified by direct on-disk observation:

1. **The seam IS staged.** The restart tier's
   `after-merge-dst-flush-before-source-commit` case already crashes at
   `crashpoint.AfterMergeDstFlushBeforeSourceCommit` — exactly the point m003's
   patch (`commitSourceComplete(..., sf.Idx, ...)` instead of `sf.Idx+1`)
   double-processes. The earlier "seam not exercised" claim was incorrect.

2. **m003 produces no observable effect.** Applied the patch and dumped the
   on-disk event multiset for the merge crashpoint case vs. baseline: **identical**
   — 11 events, zero duplicates, zero losses. The two runs differ only in
   seq-assignment *order*, and two clean baseline runs differ in that same order
   too (per-DID backfill concurrency nondeterminism), so the ordering is not a
   mutation effect.

Mechanism (file-probed `shouldKeep`, not inferred): in this scenario the merge
keeps **zero** `live_segments` rows — the 16 rev-filterable events evaluated in
the merge case are **all DROP, zero KEEP** (the pre-backfill `preLiveEvents` are
rev-subsumed by the backfill snapshot, same family as the m005 finding above).
With zero rows kept, m003 re-processing the source segment on restart also keeps
zero rows, so nothing is double-appended. In a scenario where the merge *does*
keep rows, m003 would re-append them with **fresh seqs** (the dst `ingest.Writer`
re-stamps `ev.Seq = nextSeq`, `writer.go`), yielding a benign at-least-once
**duplicate** — which is explicitly contract-permitted (`docs/README.md`:
sequence numbers are never duplicated, and all subscribers must be idempotent to
duplicate event delivery). It is not a data-loss bug either way.

Disposition: **benign / equivalent in the current scenario — accepted, not a
gap.** The mutant author's prediction (restart should trip `CheckInvariants`
duplicate-seq or `Compare` extra-record) was based on a seq-preserving re-append;
the dst re-stamps, so neither fires, and an at-least-once duplicate is correct
behavior. The earlier "fix the oracle / extend the restart harness" disposition
is withdrawn.

Note: the restart tier's broader weakness — it lands **only surviving creates**
on disk (`disk_total == final_records`, no durable tombstones/updates), so it
cannot exercise the §180-182 lost-intermediate class at all — is real but
orthogonal to m003. It is tracked as a scenario expansion in #113 (which closed
the original #103). m003 is not the mutant that demonstrates that gap.

## Campaign 2026-06-20 (m025 — convergence-hiding compaction over-drop, KILLED@stress)

Added `m025_compaction_overdrop_above_watermark` to give the #100 metamorphic
`compactionOverDropRecorder` an end-to-end mutant that exercises its **unique**
power: catching an over-drop that final-state convergence hides. The blanket
m024 mutant does not do this — it drops rows that are still live, so final-state
`Compare` kills it first.

**Mutant:** the steady compaction pass takes its tombstone snapshot with an
unbounded high seq (`SnapshotRange(current, ^uint64(0))`) instead of bounding at
`targetWatermark`. A delete that arrived in the new active segment after the
pass's force-rotate (seq > W) leaks into the snapshot and suppresses its own
create — a create ≤ W whose only superseding tombstone is above W, i.e. a
legitimate survivor of this pass. One or more such survivors can be dropped per
pass (the observed failing pass reported `dropped=1`).

**Result:** KILLED@stress via `compactionOverDropRecorder.Assert` at
steady-state-shutdown-flush: `compaction over-drop at watermark=W (... dropped=1)`,
with the dropped row a `create` whose record is deleted above W. **Final-state
`Compare` stays green** (the record is absent in the end either way), so this is
caught *only* by the pre/post survivor check — the property #100 exists for.
Verified killed at full default (~1s) and stress (~20s, the cataloged tier);
**survives `-short`** (too little steady traffic to materialize a straddle +
pass), so its home is the stress tier.

**Reachability correction (supersedes the earlier m024 note):** the
convergence-hiding case is **reachable in steady mode**, not (as previously
claimed) structurally impossible there. Force-rotating the active segment before
a pass does NOT make W cover every event: a delete arriving in the new active
segment after the rotate has seq > W and stays in the live tombstone set above W,
so the `(create ≤ W, delete > W)` straddle is the *normal* steady-state shape.
It is the restart/merge-tail path that cannot host it — there the compaction
tombstone snapshot always spans the whole sealed stream, so every drop decision
is complete and no above-scope straddle exists. This was confirmed by direct
on-disk probing of the crashed restart child (single merged segment, snapshot
folds the full `(0, targetWatermark]` range). Consequently the #100 end-to-end
over-drop proof lives in the **steady-state lifecycle tier** (m025), and the
restart-tier B-crash variant contemplated by #113's plan was withdrawn as
structurally infeasible. The exploratory restart-tier merge-segment plumbing
added during that investigation was reverted to keep the harness lean.

## Campaign 2026-06-20 (m005 re-homed to the restart tier — #110)

The 2026-06-20 full campaign found m005_backfill_status_check_inverted had
gone from `KILLED@stress` (2026-06-15) to `SURVIVED` across 6 seeds — a real
loss of detection power, diagnosed as an **equivalent mutant in the lifecycle
scenario**: every completed-repo live event there arrives strictly newer than
the backfill head (`ev.Rev > BackfillRev`), so the `rev <= BackfillRev` branch
of `shouldKeep` is never reached and the inverted vs. correct guard produce
identical output (file-probed: 7129 rev-filterable events, zero in the
`<=` branch). The production `merge_filter.go` logic was unchanged; only the
lifecycle simulator traffic had drifted away from the branch.

**Resolution (#110): re-home m005 to the restart tier.** The crash tier
(`TestOracle_RestartCrashPointsDoNotLoseRecords`) generates its `preLiveEvents`
on the parent BEFORE the child backfills, so they are rev-subsumed by the
getRepo snapshot (`ev.Rev <= BackfillRev`) — exactly the branch the lifecycle
no longer reaches. Verified: with the inverted guard, those rev-subsumed events
bypass rev-filtering, survive into the merged tree, and trip the per-DID
rev-regression invariant (`CheckInvariants`): `rev regression for DID ... rev=
"3ke6kg3wk2722" after ... rev="3ke6kg3wk2c22"`. m005 is **KILLED@restart**;
the catalog `tiers:` is changed from `default,stress` to `restart`.

**Anti-vacuity is the mutation campaign itself.** Rather than a bespoke
in-run counter (the merge drop metric lives inside the restart child
subprocess, disk presence can't distinguish a rev-filtered row from its
backfill-reintroduced create, and reading the child's `BackfillRev` would
couple the oracle to the system under test), the guard against the branch
silently going dead again is the m005 catalog entry: if the rev-filter branch
ever stops being exercised, m005 flips `KILLED -> SURVIVED`, which the
scheduled campaign gating (#108) is designed to catch. This is recorded here
and in the m005 patch `expected-detection` note so the contract is explicit.

## Uncovered-code mutants 2026-06-21 (#105)

#105 asked for mutants in four previously-mutant-free areas: fault-injection,
compaction over-drop, XRPC egress, and the client decode/plan/cutover path.
Disposition after building + empirically probing each:

- **Fault-injection — DONE (m027, KILLED@default).** `m027_getrepo_http_fault_disabled`
  flips the simulator getRepo fault-budget guard so injection silently never
  fires; `assertFaultPlanFired` kills it crisply and fast. This is the proof
  the anti-vacuity machinery's own kill power is asserted by a mutant, not
  just by code reading.

- **Compaction over-drop — already DONE (m024/m025/m026).** The blanket
  over-drop (m024), the convergence-hiding over-drop (m025), and the
  wrong-rev event-log mutant (m026) cover this area; no new mutant needed.

- **XRPC getsegment egress — verified killable, NOT committed (deliberate).**
  Two egress mutants were tried by hand: serving the wrong segment
  (`SegmentByIdx(idx+1)`) and inverting the found-guard so a valid segment
  404s. Both ARE caught — crisply, by the client tier's final-state `Compare`
  ("client stream final state does not match ground truth"), with the segment
  served as valid-but-wrong content the client accepts. But the kill only
  lands at the client tier's full convergence deadline (~300s), because the
  client climbs to the target seq with wrong content and only the end-state
  Compare notices. A 300s mutant is NOT added to the catalog: it would have to
  run `stress`-tier (the 5m `default` tier would race it and read as a flaky
  timeout) and would add ~5 minutes to every stress campaign run for one
  mutant. The area is verified reachable/killable; the cost/benefit of a
  permanent catalog entry is negative, so it is documented here instead.

  A no-progress watchdog on the client tier was prototyped to make this fast,
  but it cannot catch this mutant class: the broken segment serves
  wrong-but-PRESENT content, so the client's max-seq reaches target and the
  watchdog (which must disarm at target to avoid false-tripping legitimate
  convergence-settling) never fires. Reverted.

- **Client decode/plan/cutover — coverage gap found, deferred.** A mutant
  making the client's tombstone suppressor never drop (`Suppressor.ShouldDrop`
  -> always false) SURVIVED: a leaked superseded row was not caught. This is a
  real client-tier coverage gap (the overlay-suppression window in the default
  run has too few tombstones for the comparator to bite), overlapping #102's
  note that the client tier was "near-vacuous". Closing it is scenario/
  assertion work (force a tombstone into the client's overlay window and assert
  the leak), not a one-line mutant; deferred as a follow-up, cross-linked #102.

Net: #105's fault-injection and over-drop goals are met with committed crisp
mutants; XRPC egress is verified-killable but intentionally uncommitted to
keep the campaign fast; client-decode is a deferred coverage-gap follow-up.

Side effect (product feature): the investigation surfaced that the public
jetstream client could override transport (`WithHTTPClient`) but not retry, so
a test/tool could not make a broken backend fail fast instead of riding a long
retry/backoff. Added `WithMaxDownloadAttempts(n)` (bounds total attempts on
both XRPC clients); unit-tested in client_test.go.

## Campaign 2026-06-21 — full catalog at HEAD, baseline for the #108 gate

- commit under test: `df3fc4b` (branch `oracle-improvements`)
- driver: `testing/mutation/run.sh --json` (full catalog, fixed campaign seed)
- catalog: 25 active mutants (m001–m027; m007/m010 retired)
- purpose: establish the authoritative current scorecard and seed
  `testing/mutation/baseline.json`, the machine-readable source of truth the
  scheduled `mutation-campaign` gate (#108) now enforces.

### Scorecard

| mutant | result | note |
|---|---|---|
| m001_delete_mapped_to_update | KILLED@default | liveness break — kills via the default-tier 5m timeout (bootstrap barrier never releases). |
| m002_watermark_floor_off_by_one | SURVIVED | unchanged — fixed-seed variance; a 5-seed stress sweep kills ~4/5 (not a regression). |
| m003_merge_cursor_no_advance | SURVIVED | unchanged — benign/equivalent in this scenario (see 2026-06-20 m003 re-disposition). |
| m004_rev_filter_inverted | KILLED@default | `oracle: missing … app.bsky.actor.profile/…`. |
| m005_backfill_status_check_inverted | KILLED@restart | **confirms the #110 re-home** — `oracle: rev regression for DID …`. The merge rev-filter branch is exercised by the restart tier's rev-subsumed preLiveEvents; the gate now enforces this so a future re-regression flips KILLED→SURVIVED and fails CI. |
| m006_merge_commit_error_swallowed | SURVIVED | unchanged — needs store-fault injection (#30). |
| m008_header_offset_byteslice | KILLED@default | corrupt header offset → segment open fails. |
| m009_checksum_range_off_by_one | SURVIVED | unchanged — symmetric checksum closed loop (#32). |
| m011_wire_frame_length | KILLED@default | torn-tail active-segment walk fails on reopen. |
| m012_block_event_count_off_by_one | KILLED@default | block decode truncated/trailing bytes. |
| m013_collection_rkey_swap | SURVIVED | unchanged — dead path in this config; companion m017 covers the hot path. |
| m014_rev_dropped | SURVIVED | unchanged — dead path in this config; companion m018 covers the hot path. |
| m015_collection_count_double | SURVIVED | unchanged — footer collection index unread by oracle. |
| m016_bloom_size_off_by_one | KILLED@default | bloom-size corruption caught at default. |
| m017_commit_collection_rkey_swap | KILLED@default | `oracle: event mismatch … key=app.bsky.feed.like/…`. |
| m018_commit_rev_dropped | KILLED@default | `oracle: event mismatch … rev=` (event-log tier compares rev). |
| m019_sync_tombstone_dropped | KILLED@default | event-log equivalence catches the missing `kind=sync` row. |
| m020_overlay_drop_did_tombstones | KILLED@default | overlay reconstruction. |
| m021_overlay_record_seq_base_zero | KILLED@default | overlay reconstruction. |
| m022_shoulddrop_did_seq_inverted | KILLED@default | overlay reconstruction. |
| m023_overlay_drop_record_tombstones | KILLED@default | overlay reconstruction. |
| m024_compaction_over_drop_survivors | KILLED@default | blanket compaction over-drop caught by final-state `Compare` (#100). |
| m025_compaction_overdrop_above_watermark | KILLED@stress | convergence-hiding over-drop caught by `compactionOverDropRecorder` (#100). |
| m026_commit_rev_altered | KILLED@default | wrong non-empty commit rev caught by the event-log tier (#104). |
| m027_getrepo_http_fault_disabled | KILLED@default | getRepo fault injection silently disabled; `assertFaultPlanFired` kills it. |

Summary: **18 killed, 7 survived.** This supersedes the 2026-06-20 `b937b6e`
count (14 killed / 8 survived over m001–m024): m005 is now KILLED@restart (the
#110 re-home), and m025/m026/m027 were added after that run (all KILLED). The 7
survivors are all documented known gaps (m002 seed-variance; m003
benign/equivalent; m006 #30; m009 #32; m013/m014 dead-path covered by
m017/m018; m015 footer-index blind spot) — no true escapes. This run seeds
`baseline.json`; subsequent scheduled runs are diffed against it and a
KILLED→SURVIVED flip fails the job.
