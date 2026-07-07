# Oracle Mutation Campaign Results

Each campaign appends a dated section; history is never overwritten so the
oracle's detection power is visible over time. See
`specs/mutation.md` for the method and `testing/mutation/run.sh` for the
driver.

**Current catalog (keep this line current): 43 active mutants on disk
(m001–m051; m007, m010, m013, m014, m020, m021, m023, m025 retired). Current
union baseline after #206 frame-tier coverage, #208 footer-index/bloom
verification, #203 account-status exactness, and #264 power-loss durability
coverage: **43 killed, 0 survived,
zero STALE/BUILD-BROKEN** in
`testing/mutation/baseline.json` (the commit field is provenance-only). #208 banked the old m015 footer-index survivor as
KILLED@default; #203 added m043 and banks it as KILLED@default.
m046-m050 cover fsync omission/reordering and Linux SyncWrites downgrades.
m051 covers the merge-cleanup data-dir fsync ordering (the powerloss tier now
also runs `./internal/ingest/orchestrator`'s `TestRunMerge_StrictMemPowerLoss*`
to catch it and the restart-after-cleanup guard sibling).
m042 (the #206 frames-tier mutant) was renumbered from its original m036 id
at this merge — the #204 branch minted m036–m040 concurrently; same
precedent as m041's renumber in 82b2dd9.
m015 is now KILLED@default by the #208 sealed metadata verifier.
m013/m014 retired 2026-07-04: dead path under atmos v0.2.10, not
dormant-under-polite-traffic — see the #204 campaign section; their bug
class is covered by m017/m018 (convertCommit) and m036/m037 (convertSync).
(#183 closed 2026-07-04: no recorder-unique replacement for the retired
m025 exists post-#178 — see the dated analysis section; the #100 over-drop
recorder's gated mutant m034 guards its hook integrity, not its drop
logic.) Counts inside older dated sections describe the catalog *as of that
date* and are intentionally not back-edited.**

## The baseline gate (#108)

This prose scorecard is the human record; the **enforced** scorecard is
`testing/mutation/baseline.json` — a machine-readable `{commit, mutants:[{id,
disposition, ...}]}` document. The scheduled `mutation-campaign-scheduled` workflow
(`.github/workflows/mutation-campaign-scheduled.yml`) runs the full campaign at HEAD,
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
| m020_overlay_drop_did_tombstones | 2026-06-29 | Targets `internal/overlay/format.go`, the read-path overlay deleted in #177 (drop-client-tombstones). No served overlay remains; DID-marker coverage is now the in-archive sentinel index (#175). |
| m021_overlay_record_seq_base_zero | 2026-06-29 | Same — `internal/overlay` deleted in #177. |
| m023_overlay_drop_record_tombstones | 2026-06-29 | Same — `internal/overlay` deleted in #177. |
| m025_compaction_overdrop_above_watermark | 2026-06-29 | Mutated `Set.SnapshotRange` (unbounded in-memory snapshot), deleted in #178. The on-disk windowed fold cannot reproduce it: `targetWatermark` is the last sealed segment's MaxSeq, so no decoded event exceeds the fold window. The above-watermark over-drop is unreachable post-#178. #183's re-derivation analysis (2026-07-04 section below) concluded no single-edit replacement exists: the recorder is a regression assertion without a gated mutant. |

## Campaign 2026-07-07 (#264 — power-loss durability boundary)

Targeted clean-copy mutation-driver runs after adding the power-loss tier and
the durable-operation recorder. The shared working tree was dirty by design
while this change was under construction, so the five new mutants were verified
in a temporary clean git repo copied from this tree; each was applied and
reverted by `testing/mutation/run.sh`.

| mutant | result | note |
|---|---|---|
| m046_segment_block_fsync_deleted | KILLED@powerloss | strict-mem segment/ingest tests and durable-order checks catch a block write with no covering segment fsync |
| m047_rewrite_parent_dir_fsync_deleted | KILLED@segmentfault | Rewrite seam-count sweep catches the missing parent-dir fsync |
| m048_patch_parent_dir_fsync_deleted | KILLED@segmentfault | Patch seam-count sweep catches the missing parent-dir fsync |
| m049_ingest_flush_order_inverted | KILLED@powerloss | ingest operation-order test catches seq/next commit before segment data fsync |
| m050_store_syncwrites_linux_nosync | KILLED@powerloss | strict-mem store test catches `store.SyncWrites` losing durability |
| m051_merge_cleanup_dir_fsync_deleted | KILLED@powerloss | strict-mem merge test (`TestRunMerge_StrictMemPowerLossCleanupComplete`) catches a re-drain that duplicates survivors when the data-dir fsync before the SyncWrites cursor deletes is dropped |

## Campaign 2026-07-05 (#203 — account lifecycle statuses and getRepo unavailable)

Full campaign at `2450fec` after adding deterministic simulator/oracle coverage
for non-deleted account lifecycle statuses (`takendown`, `suspended`,
`deactivated`, `unknown`), reactivation, and terminal `getRepo`
`RepoTakendown`/`RepoSuspended`/`RepoDeactivated` classification. **35 active
mutants: 34 KILLED, 1 SURVIVED, zero STALE/BUILD-BROKEN.** At that point the
only survivor was the documented m015 footer-count escape; #208 later banked it
as KILLED@default.

**Re-run at `4f2c153` (same day) after an adversarial-review finding proved the
`2450fec` result vacuous at the end-to-end tier.** m043 originally declared
`tiers: tombstone,default`; the driver stops at the first killing tier, so the
pre-existing tombstone unit matrix killed the mutant and
`TestOracle_DefaultLifecycle` never gated the new #203 lifecycle coverage —
confirmed by hand-applying the mutant: the default tier PASSED, because the
lifecycle was injected at the end of the steady window, above every compaction
watermark, where the tombstone-exactness fold never touches rows. Fixes: the
lifecycle injection moved to the start of the steady window (below the
tombstone-triggered pass watermark), the harness now asserts the final
watermark covers the lifecycle rows (anti-vacuity), and m043's tiers reordered
to `default,tombstone` so the end-to-end tier is the recorded killer. The
mutant's expected-detection block also cited a nonexistent test
(`TestTombstoneSet_AccountStatusExactness`); corrected to the real unit
backstops (`TestObserveAccountDeletedOnlyPurgesLiteralDeleted`,
`TestObserveAccountStatusMatrixRetains`). m001's recorded tier also moved
stress→default in this regen (tier-order note only; disposition unchanged).

Drivers:

```bash
testing/mutation/run.sh m043 --json /tmp/m043.json
testing/mutation/run.sh --json testing/mutation/baseline.json
```

### Scorecard

| mutant | result | note |
|---|---|---|
| m043_account_status_exactness | KILLED@default | `oracle: missing did:plc:jqwkem7rbggmxanbfb7e6gbl app.bsky.feed.like/...` — under the mutant the fixture account's inactive statuses fold as DID tombstones and compaction over-drops its records; the tombstone unit matrix remains the fast backstop tier. |

## Campaign 2026-07-05 (#208 — footer-index/bloom verification)

Full campaign at temporary-worktree commit `0a89f40` carrying the #208 changes.
**33 active mutants: 33 KILLED, 0 SURVIVED, zero STALE/BUILD-BROKEN.**
`testing/mutation/baseline.json` was regenerated from this run.

The surviving m015 footer-index blind spot is now banked as a kill. The new
segment verifier re-derives footer collection counts, per-block collection
sets, segment DID bloom membership, and per-block DID bloom membership from
decoded rows, then the oracle calls it from sealed segment observation. The
mutant's doubled collection count now fails in the default oracle tier before
row-level reconstruction can mask it:

| mutant | result | note |
|---|---|---|
| m015_collection_count_double | KILLED@default | `oracle: verify sealed segment ... footer metadata: segment: verify ... collection "app.bsky.actor.profile" count mismatch: footer=2 rows=1` |

## Campaign 2026-06-29 (step 11 #182 — partb tier; catalog refresh; baseline regen)

Full campaign at `b9543d9` after step 11 of the drop-client-tombstones +
paginated-bufferless-cutover refactor. **27 mutants: 20 KILLED, 7 SURVIVED, zero
STALE/BUILD-BROKEN.** `testing/mutation/baseline.json` was regenerated from this
run and gate-verified self-consistent (`gate: PASS — 27 mutants match
baseline`).

**New `partb` tier.** `TestOracle_DefaultLifecycle` never truncates a plan
(default `MaxEntries`) nor ages a cursor below the lookback floor, so the
paginated-cutover paths had no mutation coverage. The new tier runs the §16
hermetic end-to-end scenarios (`TestPartB*`, `internal/oracle`) plus the manifest
planner's per-page truncation unit tests (`TestPlanBackfill*`, so the planner
mutants kill fast rather than via a client-loop livelock timeout). All five new
mutants kill there.

**Catalog churn.** Retired the overlay-format mutants m020/m021/m023
(`internal/overlay` deleted in #177) and m025 (its `Set.SnapshotRange` mechanism
deleted in #178 — the above-watermark over-drop is unreachable on the on-disk
windowed fold since `targetWatermark` is the last sealed segment's MaxSeq; #183
tracks re-deriving a dedicated #100-over-drop-recorder mutant). Refreshed m015 to
its post-#175 location (the per-collection count increment moved into the shared
`internCollection` helper). Both refreshes surfaced only because step 11's full
campaign re-ran every mutant against current code — the prior baseline had
hidden m015 (STALE) and m025 (BUILD-BROKEN) since #175/#178 landed.

### Scorecard (new and refreshed rows)

| mutant | result | note |
|---|---|---|
| m015_collection_count_double | SURVIVED | refreshed to internCollection (seal.go:244); still a documented footer-index blind spot the row-by-row oracle cannot see (predicted survival). |
| m029_plan_continuation_cursor_off_by_one | KILLED@partb | `PlannedThroughSeq = lastUnitMaxSeq + 1` skips a page boundary; union of pages no longer folds to ground truth (TestPartB_MultiPageBackfillCutover / _MidSegmentTruncation). |
| m030_plan_midsegment_cut_reports_segment_maxseq | KILLED@partb | mid-segment cut reports the enclosing segment MaxSeq; the un-included tail blocks are skipped forever (TestPartB_MidSegmentTruncation). |
| m031_plan_truncation_zero_units_unadvanced | KILLED@partb | `Entries+1 >= MaxEntries` trips the cap before the first unit is admitted → zero units, cursor unadvanced (TestPlanBackfill_OneUnitOverCapStillAdvances). |
| m032_v2_below_floor_silent_clamp | KILLED@partb | `if false` on the RejectBelowFloor branch re-introduces the v1 silent clamp; a below-floor v2 cursor no longer 400s (TestPartB_StaleCursorSignal / _CaughtUpHandoffBelowFloorReBackfills). |
| m033_client_too_old_400_not_rebackfilled | KILLED@partb | cutover never reports the §14 too-old 400, so the client stops at the seam instead of re-backfilling (TestPartB_CaughtUpHandoffBelowFloorReBackfills). |

The other 22 rows are unchanged from the 2026-06-25 gate pass (m020/m021/m023/m025
removed from the catalog). Six of the 7 survivors — m002, m003, m009, m013, m014,
m015 — are pre-existing documented escapes (watermark-floor off-by-one,
merge-cursor no-advance, checksum-range off-by-one, collection/rkey swap on a
path the oracle folds through, rev-dropped on a non-asserted field, footer count
blind spot). The seventh, **m022 (DID-seq ShouldDrop inversion), is a regression
this branch introduced, not a pre-existing escape**: it was `KILLED@default` on
`main` by the overlay-reconstruction oracle, which this branch deleted with
`internal/overlay` (#177). The mutation drops live records on the delete-compaction
path (`compact_deletes.go:357`), so its escape is a genuine coverage loss, not an
inherently-undetectable mutant — #184 tracks re-deriving a steady-state oracle to
return it to KILLED. The baseline records SURVIVED because the *current* oracle
genuinely cannot kill it; the gate would otherwise be perpetually red.

## Campaign 2026-06-25 (m025 stale refresh; gate pass)

- commit under test: `b6d3f09`
- issue: #159
- driver:
  - targeted reproduction before refresh: `just mutation-campaign m025`
  - targeted verification after refresh: `just mutation-campaign m025`
  - full gate verification: `just mutation-gate`
- catalog: 25 active mutants; baseline unchanged

The scheduled mutation gate failed on 2026-06-26 UTC because
`m025_compaction_overdrop_above_watermark` was `STALE`: its patch still targeted
the old bounded `SnapshotRange(current, targetWatermark)` compaction snapshot
line. Current compaction no longer uses that in-memory snapshot path; it folds
sealed segment rows via `collectCompactionTombstones`, so the mutant had drifted
after the oracle/compaction fixes.

Refreshed m025 to reintroduce the same failure mode at the current mutation
point: steady compaction still starts with the real sealed-row fold, then the
mutant replaces the steady-mode snapshot with
`Tombstones.SnapshotRange(current, ^uint64(0))`. This keeps merge-tail compaction
on the real path, preserving m025's intended target: a steady-state
convergence-hiding over-drop that only the pre/post compaction over-drop
recorder should catch.

Targeted verification after refresh:

| mutant | result | note |
|---|---|---|
| m025_compaction_overdrop_above_watermark | KILLED@stress | `steady-state-shutdown-flush`: `compaction over-drop at watermark=30558 (pre=28782 post=28780 survivors=28781 dropped=1)`; missing expected `app.bsky.feed.repost/22bab3wsv...` create at seq 29284 |

Full gate verification passed:

```text
gate: PASS — 25 mutants match baseline (commit b6d3f09000c578400264db8b3c0b56b553427c7e)
```

## m003 re-disposition 2026-06-26 (#29 — accepted-benign, mechanism nailed down)

Re-examined m003 while building the #29 predicate-driven kill tier (the issue
named this tier the home for killing m003). m003 remains **SURVIVED** on current
`main` after #113/#114, and a direct spike confirms it is **architecturally
benign in every scenario the restart tier can construct** — not a coverage gap.
This reaffirms the 2026-06-20 "benign, not a gap" disposition below, now with the
full mechanism nailed down, and three independent guards each of which alone
neutralizes the mutant:

1. **Source segments are deleted on clean completion, and the cursor is then
   ignored.** After a full merge, `merge.go` `os.RemoveAll(.../backfill)` removes
   the source tree; on the next start the restart-after-cleanup guard sees
   `live_segments` gone, deletes the cursor keys, and returns. So m003's
   `commitSourceComplete(sf.Idx)` vs `sf.Idx+1` off-by-one is **never read** on a
   clean restart — there is no source tree left to re-process. Idempotency holds
   independently of the cursor value.

2. **The restart-chain scenario produces a single source segment** (verified:
   `live_segments` empties to one merged `segments/seg_0000000000.jss`). With one
   source, the only mid-merge crashpoint (`AfterMergeDstFlushBeforeSourceCommit`)
   fires *before* `commitSourceComplete`, so the cursor is uncommitted under both
   correct and mutant code and the re-run reprocesses identically. There is no
   "earlier-committed source + later-pending source" gap where `Idx` vs `Idx+1`
   diverges.

3. **The destination re-stamps seqs** (`ingest.Writer` sets `ev.Seq = nextSeq`),
   so even a hypothetical re-append is a contract-permitted at-least-once
   duplicate, which #113's replay-aware `CheckStructuralInvariants` + the `≥`
   coverage comparator now *correctly* tolerate. The mutant author's predicted
   kill signal (CheckInvariants duplicate-seq / Compare extra-record) assumed a
   seq-preserving re-append; the re-stamp defeats it.

Empirical confirmation (#29 work): applying m003 and running the durable-chain
crash tier at `AfterMergeDstFlushBeforeSourceCommit` (rows kept, unlike the old
nil-coordinator tier) still **passes** — no duplicate, no loss, final-state
`Compare` converges. Killing m003 would require a multi-retained-source-segment
scenario with a crash between per-source commits, which the architecture
deliberately does not produce and whose cursor mechanics are already covered by
orchestrator unit tests.

**Disposition: accepted-benign, owned by #29.** Kept in the catalog as a SURVIVED
documented blind spot (its mutation point is real; it simply has no observable
effect here), consistent with the enforced baseline. The #29 DoD clause "this
tier kills m003" is withdrawn; #29 delivers the seeded/predicate-driven
kill-point selection instead. Note: this is the same class of finding as #114's
withdrawn convergence-hiding over-drop — a "kill the mutant" goal written before
the idempotency guarantees were fully traced.

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
"atmos closed loop" blind spot described in `specs/oracle.md`: the oracle cannot
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
the crash/restart tier in `specs/oracle.md` would cover this class
without enumeration.

**m006 — swallowed commit error needs store-fault injection (predicted).**
Predicted to survive: under normal runs `commitSourceComplete` never fails,
so the inverted check is dormant. Confirms the store-fault tier requirement in
`specs/oracle.md`: the oracle has no way to make a store write fail.
Disposition: **accepted, pending** the store-fault oracle tier.

**m015 / m016 — footer/bloom read-path indexes (confirmed blind spots).**
Predicted survival, confirmed. The oracle decodes every block sequentially
and never consults the footer collection-count index or the per-block bloom
filters, so corruption there is invisible. These mutants exist to *document*
the gap with evidence. Disposition: **accepted blind spots** — would be
closed by the product replay and XRPC egress tiers in
`specs/oracle.md`, which exercise the read indexes a client uses.

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
now named in `specs/oracle.md` (m006 store-fault, m009 closed-loop
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

## Update 2026-06-26 (#114 — infeasibility now enforced; crash tier exercises durable intermediates)

The 2026-06-20 "reachability correction" above was prose: it asserted the
restart/merge-tail path cannot host the `(create ≤ W, delete > W)` straddle, but
nothing enforced it. #114 (filed after that finding, re-challenging it) is
resolved by turning the claim into a test and closing the genuine adjacent gap.

- **No-straddle invariant, now enforced**
  (`internal/oracle/restart_crash_chain_test.go`,
  `TestOracle_RestartChainShapeB_NoStraddleAfterMergeTailCrash`): crashes shape B
  at `AfterCompactionRewriteBeforeWatermark` — the exact crashpoint #114 named —
  recovers, and asserts `maxDurableSeq(on-disk) ≤ W` with anti-vacuity (a real
  merge-tail watermark committed, the shape-B delete tombstone durable ≤ W, the
  backfilled create compacted away). Observed `W=26, maxDurableSeq=26`: every
  durable row sits at or below the watermark, so no convergence-hiding straddle
  exists on disk. If a future merge-tail refactor ever lets a durable row outrun
  W, this goes red and the B-crash infeasibility finding must be revisited.
  Verified red-first: tightening the bound to `W-1` fails with the real seqs.
- **Crash tier now exercises durable intermediates**
  (`TestOracle_RestartChainCrashConsistency`): the pre-existing crash tier
  (`TestOracle_RestartCrashPointsDoNotLoseRecords`) wires a `nil` coordinator, so
  it only ever landed surviving creates — no update/delete/tombstone was exposed
  to a crash. The new test runs the full seed-derived chain through SIGKILL +
  re-merge and asserts `assertChainDurable` (at-least-once coverage `≥`,
  `CheckCompacted`, no-permanent-tombstone) over the recovered segments, with a
  red-first power check (dropping the recovered shape-B delete tombstone breaks
  coverage). This is the crash-consistency coverage #114 is really about, distinct
  from the (infeasible-here) convergence-hiding over-drop.

**Net for the mutation catalog:** unchanged. The #100 convergence-hiding
over-drop proof remains m025 in the steady tier (KILLED@stress); no new
restart-tier mutant is added, because the over-drop it would target cannot form
in merge-tail (now enforced by the no-straddle test rather than asserted in
prose). m024 (blanket over-drop, KILLED@default via final-state `Compare`) is
also unaffected.

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

- commit under test: `df3fc4b` (branch `oracle-improvements`) — NOTE: a later
  rebase (inserting the simulator race fix 523d4e1) orphaned this hash; the
  identical logical run is recorded under `testing/mutation/baseline.json`'s
  commit field (`007abab`), which is the machine source of truth.
- driver: `testing/mutation/run.sh --json` (full catalog, fixed campaign seed)
- catalog: 25 active mutants (m001–m027; m007/m010 retired)
- purpose: establish the authoritative current scorecard and seed
  `testing/mutation/baseline.json`, the machine-readable source of truth the
  scheduled `mutation-campaign-scheduled` gate (#108) now enforces.

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

## 2026-06-26 — store-fault tier; m006 killed, m028 added (#30)

Closes the long-standing m006 gap by adding the store-fault injection tier
the issue called for. Net scorecard change: **m006 SURVIVED→KILLED@storefault**
and a new **m028 KILLED@storefault**; every other mutant keeps its prior
disposition (gate: PASS, 26 mutants). `baseline.json` refreshed to bank both.

**The seam.** `store.FaultInjector` is a nil-gated hook on
`store.Store.Set/Delete/Commit`, installed via the new
`store.Open(dir, metrics, opts...)` variadic option and threaded through
`jetstreamd.Options.StoreFaultInjector`. Production never installs one (the
Options field is nil, mirroring the nil-in-prod `CrashInjector`), so the fault
path is unreachable off the test harness. `store.KeyPrefixFault` is the
canonical injector: fail the Ordinal-th write op touching a key prefix,
optionally scoped to one `WriteOp`; batch commits match against their staged
keys via the public `BatchReader`.

**The `storefault` campaign tier** runs the kill at two layers in one
`go test`, so a regression in either fails CI:

- *oracle level* — `TestOracle_RestartStoreFaultOnMergeCursor_FailsLoudThenRecovers`
  drives a real `jetstreamd` through the merge with a fault on the
  `merge/next_source_idx` batch commit, asserts the runtime fails LOUD (the
  injected sentinel surfaces up through `Orchestrator.Run` — never swallowed),
  then re-runs the merge fault-free and asserts convergence (the data the
  faulted commit could not persist is not lost). Anti-vacuity: the first
  child's after-merge barrier must NOT have fired (the fault really aborted the
  merge).
- *orchestrator unit level* — `TestMerge_StoreFaultOnCursorCommit_FailsLoudNoSilentAdvance`
  pins the same contract directly on `runMerge` (fail loud, cursor not
  advanced, backfill tree preserved); `TestMerge_MultiSourceDrainsAllSources`
  catches m006's *other* inverted branch (the `err==nil` early-return drops
  later sources — the pre-existing `TestMerge_MultiSourceContiguousCommit` only
  checked post-cleanup state and missed this).

**m028 (new, DoD "new swallowed-persistence-error mutants").** Inverts the
error check on `saveCompactionWatermark` in `runDeleteCompaction` — a distinct
high-risk fault point named in #30 and a different store op from m006 (a plain
`Set` on `compaction/seq`, not a batch commit, so it exercises the seam's `Set`
path). `TestCompaction_StoreFaultOnWatermarkSave_FailsLoudNoAdvance` forces the
watermark `Set` to fail and asserts the pass fails loud and the durable
watermark does not advance.

Both kills were verified red-under-mutant / green-on-correct by applying and
reverting each patch before wiring the gate.

| mutant | result | note |
|---|---|---|
| m006_merge_commit_error_swallowed | KILLED@storefault | store-fault tier: forced `merge/next_source_idx` commit failure must fail loud; multi-source test also catches the clean-commit early-return. Was SURVIVED (no store-fault tier). |
| m028_compaction_watermark_save_error_swallowed | KILLED@storefault | store-fault tier: forced `compaction/seq` Set failure must fail loud; durable watermark must not advance. |

**Remaining #30 fault points — fail-loud contract tests (no mutants).** The
issue lists more high-risk write boundaries than the two with mutants. The
`KeyPrefixFault` seam covers each; rather than mint a swallow-mutant per site,
the fail-loud / no-silent-advance contract is pinned directly at each boundary
(these are regression tests, not gated kills — a single store-fault seam bug is
already caught by m006/m028):

- **seq/next** — `TestWriter_DurableBatchFailsLoudOnStoreFault`
  (internal/ingest): a forced `seq/next` durable-batch commit failure must
  surface out of `Append`/flush; `seq/next` must not become durable.
- **relay cursor** — `TestConsumer_SaveCursorFailsLoudOnStoreFault`
  (internal/ingest/live): a forced `relay/cursor` commit failure must surface
  out of `saveCursorAndSyncState`; the cursor must not advance.
- **syncstate commits** — `TestStateStore_FlushFailsLoudOnStoreFault`
  (internal/ingest/syncstate): a forced `sync/` commit failure must surface out
  of `Flush`; promoted verifier state must not be durable.

**manifest refresh after compaction — N/A (no pebble write).** The DoD names
"manifest refresh/update after compaction" as a fault point, but the manifest
reconcile (`manifest.OnSegmentCompacted` → `refreshSegment`) updates only the
in-memory manifest and the on-disk segment headers; it performs NO
`store.Store` (pebble) write. The store-fault seam therefore cannot target it,
and faulting it would require a separate manifest/segment IO-fault seam — out
of scope for this metadata-store tier. Recorded here so the gap is explicit
rather than silently unaddressed. *(Update 2026-07-06: that seam now exists —
`segment.IOFaultInjector` covers every segment-file write/fsync/rename
including the header rewrites the reconcile reads back, and the `segmentfault`
tier (#200, m044/m045) gates it. The pebble-write N/A stands.)*

## Campaign 2026-06-30 — full catalog at HEAD (review remediation, `tombstone` tier + m022 re-bank)

- commit under test: `dba121e` (branch `tombstone-query-plan-refactor`, the
  pre-ship review remediation that added F1–F10 fixes).
- driver: `testing/mutation/run.sh --json testing/mutation/baseline.json`
  (full catalog, fixed campaign seed).
- catalog: 27 active mutants (m001–m033; m007, m010, m020, m021, m023, m025
  retired) — unchanged from the `b9543d9` campaign.
- result: **21 KILLED, 6 SURVIVED, zero STALE/BUILD-BROKEN.** `baseline.json`
  regenerated and gate-verified self-consistent (`gate: PASS — 27 mutants match
  baseline`).
- purpose: re-bank **m022** as KILLED after the new `tombstone` tier restored
  its detection, and confirm the F1–F10 code changes did not weaken detection of
  any other mutant.

### What changed vs the 2026-06-29 (`b9543d9`) campaign

- **m022_shoulddrop_did_seq_inverted: SURVIVED → KILLED@tombstone.** The only
  disposition change. A new `tombstone` campaign tier runs `./internal/tombstone`,
  whose `TestSnapshotShouldDropDIDChainsWithSpecificReason` now asserts
  `Snapshot.ShouldDrop` in BOTH seq directions — a materialization below the DID
  tombstone seq is dropped, AND a reactivation row above it survives. The m022
  inversion (`ts.Seq > ev.Seq` → `<`) fails that assertion, so the tier kills it.
  This closes the regression #182 introduced when `internal/overlay` (m022's old
  overlay-reconstruction oracle) was deleted in #177.
- Root cause was harness wiring, not a missing assertion: the killing test
  already existed, but no campaign tier ran `./internal/tombstone` (every tier
  ran only the oracle + a couple of packages). m022's patch header now declares
  `tiers: tombstone`.
- **No other disposition changed and there was no catalog drift** — verified by
  diffing the fresh result against the prior baseline. The F1–F10 fixes touched
  the planner/cursor/client/segment paths several mutants target (notably
  m029–m033 in the `partb` tier and m032/m033 specifically), and all stayed
  KILLED, confirming the remediation did not regress detection.

### Survivors (6) — all pre-existing documented escapes

m002 (fixed-seed variance; stress sweep kills ~4/5), m003 (benign/equivalent in
this scenario), m009 (symmetric checksum closed loop, #32), m013 (dead path in
this config; m017 covers the hot path), m014 (dead path; m018 covers the hot
path), m015 (footer collection index unread by the oracle — a documented
footer-index blind spot). See the earlier dated sections for the per-mutant
analysis; none is a new escape. (#183 still tracks re-deriving a #100-recorder
mutant to replace the retired m025.)

## Campaign 2026-07-03 — `compaction` tier; m002 SURVIVED→KILLED (#199)

Full campaign at `075fafd`. **27 mutants: 22 KILLED, 5 SURVIVED, zero
STALE/BUILD-BROKEN.** `testing/mutation/baseline.json` regenerated from this run
and gate-verified self-consistent (`gate: PASS — 27 mutants match baseline`).

**New `compaction` tier; m002 banked KILLED@compaction.** m002 (first-init
compaction watermark floor off-by-one, `initCompactionWatermarkFloor` returning
`nextSeq` instead of `nextSeq-1`) had been a documented seed-dependent escape:
the fixed-seed campaign recorded SURVIVED and only ~4/5 stress seeds killed it,
because detection required a seed to place a superseding event exactly at the
watermark boundary. #199's position: a boundary invariant should be checked
boundary-exactly, not probabilistically.

The new tier runs two deterministic orchestrator tests
(`internal/ingest/orchestrator`, <1s, seed-independent):

- `TestMerge_FirstInitWatermarkFloor_BoundarySeqCompacts` constructs the
  boundary by hand: a sealed bootstrap create at seq 1 leaves `seq/next = 2`;
  a live-source delete of the same record survives the merge rev-filter and
  merges at seq 2 — exactly where the first-init floor lands. Correct floor
  (`nextSeq-1 = 1`): the merge-tail pass folds window (1,2] and physically
  drops the superseded create. Mutated floor (`nextSeq = 2`): the pass no-ops
  (`targetWatermark <= watermark`) and the miss is **permanent** — every later
  pass folds `(W, target]`, exclusive below, so the boundary delete is never
  folded again and the superseded row survives forever. The test asserts the
  committed watermark, the survivor contract, and anti-vacuity (the boundary
  delete itself must survive the merge filter).
- `TestInitCompactionWatermarkFloor_*` pins the floor contract directly
  (nextSeq-1; zero-seq floors at 0; an existing watermark is never re-floored).

Red-first verified: both fail under the m002 patch with the modeled failure
(`oracle: superseded record row survived ... seq=1 watermark=2`) and pass
clean. m002's header now declares `expected-tier: compaction` with
`tiers: compaction,default,stress` — the stress tier stays as the end-to-end
backstop for the same class.

### Survivors (5) — all pre-existing documented escapes with owning issues

m003 (multi-source-segment scenario ceiling, #209), m009 (symmetric checksum
closed loop; drift pin tracked in #208), m013/m014 (dead paths in this
simulator config; adversarial-traffic modes in #204 make them live), m015
(footer-index blind spot, #208). No disposition regressed vs the `dba121e`
baseline; the only change is m002 SURVIVED→KILLED — a bankable improvement.

## Analysis 2026-07-04 — #183: no recorder-unique over-drop mutant exists post-#178

Closes the question #183 left open when m025 was retired: can a single-edit
mutant be re-derived that the #100 compaction over-drop recorder
(`compactionOverDropRecorder.Assert`) catches UNIQUELY — an at/below-watermark
survivor wrongly dropped while final-state Compare stays green because the
record is independently superseded above the watermark? **Conclusion: no.**
The recorder is downgraded to a pure regression assertion (it still runs on
every oracle lifecycle run; it has no gated mutant). The issue's
definition-of-done explicitly permitted this exit when analysis shows the
recorder can no longer be uniquely tripped under the current architecture.

The argument has two load-bearing halves, each verified by independent
adversarial review (two reviewers, boundary-arithmetic and lifecycle lenses,
each instructed to refute; both upheld after tracing ~10 candidate single
edits each):

1. **Filter-legality: every genuinely-folded drop is recorder-invisible by
   construction.** Post-#178 the pass's tombstone snapshot is folded from the
   exact on-disk sealed window `(current, targetWatermark]` it is about to
   compact, and `targetWatermark` is *defined* as the max `MaxSeq` over those
   same sealed segments (`listSealedCompactionSegments`) — no fold input
   exceeds the window, so the m025 mechanism (an above-watermark tombstone
   leaking into the snapshot) has no source to leak from; re-widening
   `FoldRange`'s upper bound to `^uint64(0)` is a literal no-op. The
   recorder's expected side (`filterCompactedExpectedRows`) recomputes
   tombstones from the identical pre-pass bytes at the identical
   targetWatermark with the same strict `tombstoneSeq > rowSeq`, per-key max.
   Any drop justified by a real folded tombstone is therefore also
   filter-approved — the recorder cannot see it. Edits that *shrink* or
   mis-bound the snapshot (fold-bound flips, `Snapshot.Merge` min-for-max,
   segment/block window-skip inversions, cap `chunkEnd` corruption) produce
   **under-drops** against the committed watermark — `CheckCompacted`'s
   exclusive domain, asserted earlier in the harness. The cap path was probed
   specifically: snapshot tombstones can never exceed `chunkEnd` (segments
   fold whole, ascending, and the cap break sets `chunkEnd` to the tripping
   segment's own MaxSeq), so the `ev.Seq > chunkEnd` keep-guard edits are
   no-ops, and cap bugs corrupt the *watermark contract* (CheckCompacted),
   never the *drop justification* (the recorder).

2. **Maximality: the only edits that manufacture a filter-illegal drop are
   caught by Compare first, on every seed.** Producing a drop the filter
   rejects requires corrupting a seq comparison or seq value
   (`ShouldDrop`'s strict `>` → `>=`, `observeLocked`'s `ev.Seq` → `ev.Seq+1`).
   Because segment seqs are unique, the only new victim such an edit creates
   is the tombstone-row-itself — the self-superseding update — so the edit is
   maximal: it kills *every* update row at/below the watermark, including
   records whose final state is that update. The merge-tail pass compacts the
   entire bootstrap stream, and `assertOracleMatches` (final-state Compare)
   runs at after-merge, long before `overDrop.Assert` at shutdown — Compare
   goes red on every realistic seed. There is no single edit that isolates
   the convergence-hiding shape (dropped survivor re-superseded above W)
   without also dropping final-state updates; that selectivity was exactly
   what the deleted `Set.SnapshotRange(current, ^uint64(0))` readout provided
   and nothing in the current architecture reintroduces.

Also verified in passing: the in-memory `tombstone.Set` feeds no drop decision
(production consumers are `Observe`/`Evict`/`Replace`/`Len`/`ApproxBytes` —
trigger accounting and gauges only), and `segment.Rewrite`'s candidate-DID
bloom prefilter can only skip-vs-scan (under-drop or no-op, never over-drop).

**What the recorder is now for.** It remains live insurance: it is the only
checker whose contract is "the pass dropped exactly what the documented filter
says at W," so any future re-architecture that reintroduces an out-of-window
tombstone source (an in-memory readout, a cross-window cache, a
manifest-derived snapshot) reactivates its unique power. Anyone making such a
change must re-derive a mutant for it at that point — this analysis is
architecture-specific, not a permanent pardon.

**Residual gaps found by the adversarial review, filed as follow-ups rather
than scope-crept here:**

- A single edit can vacuously *disarm* the recorder without failing anything:
  `runDeleteCompaction` passing the stale `watermark` instead of
  `targetWatermark` to `OnBeforeCompactionPass` bounds both recorder scans
  below every drop the pass makes, so pre == post trivially. The recorder
  should cross-check its observed watermark against the pass result's
  committed watermark (tracked in a follow-up issue).
- `ShouldDrop`'s DID branch `ts.Seq > ev.Seq` → `>=` survives every tier
  entirely (a same-seq DID-tombstone/materialization pair cannot exist, so
  the edit is a no-op — an equivalent mutant, not an escape).

## Campaign 2026-07-04 — m034 recorder hook-integrity mutant (#226)

Full campaign at `40e79cc`. **28 mutants: 23 KILLED, 5 SURVIVED, zero
STALE/BUILD-BROKEN.** `testing/mutation/baseline.json` regenerated from this
run and gate-verified self-consistent.

**New m034_overdrop_hook_stale_watermark, KILLED@default.** The #183
adversarial re-derivation analysis (previous section) surfaced this as
candidate C9: a single edit passing the stale committed `watermark` instead of
`targetWatermark` to `OnBeforeCompactionPass` (`compact_deletes.go:136`) bounds
both of the #100 over-drop recorder's scans BELOW every drop the pass makes,
so pre == post trivially and the recorder passes vacuously — anti-vacuity
guards included (older survivors keep `survivorsChecked` positive), with every
other tier legitimately green (the pass still compacts correctly). A watchdog
that can be blinded by a one-token production edit with zero red tests is not
a trustworthy watchdog.

The detecting check: `compactionOverDropRecorder.ObserveAfter` now cross-checks
its pending scan bound against the watermark the pass actually committed
(`result.Watermark != pendingW` → scan error surfaced by `Assert`). A
successful watermark-advancing pass commits exactly the targetWatermark
`ObserveBefore` was handed, so any divergence means the hook fed the recorder a
wrong bound. Red-first verified: with the m034 edit applied, -short
`TestOracle_DefaultLifecycle` fails with `oracle: over-drop recorder watermark
mismatch: pre-pass hook saw 20 but the pass committed 33`; the clean tree is
green. Kills in the default tier (the merge-tail pass already exposes the
mismatch), no stress needed.

No other disposition changed vs the `075fafd` baseline; the 5 survivors remain
the documented escapes with owning issues (m003→#209, m009/m015→#208,
m013/m014→#204).

## Campaign 2026-07-04 — full catalog after the #197 ingest validation gate

Full campaign at `d08ed8b` (ingest: validate rev and repo path on upstream
events, closes #197). **28 mutants: 23 KILLED, 5 SURVIVED, zero
STALE/BUILD-BROKEN.** `testing/mutation/baseline.json` regenerated from this
run and gate-verified self-consistent (`gate: PASS — 28 mutants match
baseline`).

**Why this run mattered:** #197 rewrites both ingest conversion paths —
`live/events.go` gains rev/op-path validation and generalizes
`DroppedMissingBlocksError` to `DroppedOpsError`; `backfill/handler.go` gains
the rev gate and replaces `splitMSTKey` (fail-whole-repo) with
`splitRecordPath` (drop-record-keep-siblings). Seven mutants patch those
exact files (m001, m013, m014, m017, m018, m019, m026), so this was the
STALE-risk re-run the catalog discipline requires after major ingest changes.
Result: every hunk still applies under `--unidiff-zero` (the mutated struct
literals and switch arms were relocated, not rewritten) and every prior kill
reproduces at the same tier with the same shape of note.

No disposition changed. The 5 survivors remain the documented escapes with
owning issues: m003 → #209 (merge cursor no-advance), m009/m015 → #208
(footer/checksum blind spots), m013/m014 → #204 (verified-ops path is dead
under polite simulator traffic — exactly the gap the #204 adversarial modes
will close, now unblocked by this gate).
## Campaign 2026-07-04 — `replay` tier; m035 banked (#205, #231)

Original campaign at `c1dbc39` (branch `relay-seq-replay-205`, pre-rebase):
**28 mutants: 23 KILLED, 5 SURVIVED, zero STALE/BUILD-BROKEN.** The branch was
subsequently rebased onto main at `3921e5f` (post-#228/#229, which added m034
and the #197 gate) and the full campaign re-run at the rebased head `5d6fc9e`:
**29 mutants: 24 KILLED, 5 SURVIVED, zero STALE/BUILD-BROKEN** — m034 and m035
both KILLED side by side, every prior disposition reproduced, gate PASS.
`baseline.json` re-banked from that run.

**New `replay` tier; m035 banked KILLED@replay.** #205 made relay seq
duplicates and regressions live: atmos's gap check is forward-only
(`seq > last+1`), so a relay that re-delivers frames — a duplicate burst or a
whole window replayed by a relay restored from backup — passes silently. The
simulator gained `SubscribeReposReplayFault` schedules (duplicate-last-N,
regress-to-K) that re-send previously delivered frame bytes verbatim, with
fired/replayed-frame counters for anti-vacuity.

Building the oracle scenario found a production bug before the assertions were
even written (#231): the verifier rev-replay-drops duplicate #commit/#sync, but
replayed **#account** events flowed through and re-archived at fresh jetstream
seqs. A replayed account-delete landing above a later reactivate + recreate
made every fold — oracle reconstruct, the tombstone set, compaction — treat
the account as deleted after the recreate: permanent erasure of live records
from folded state and, post-compaction, from the archive. Fixed with a consumer
guard dropping #account events at/below the DID's APPLIED hosting-state seq
(promoted/pebble only, never pending — pending state can run ahead under
pipelined verification and would drop legitimate intermediates), metric
`replayed_account_events_dropped_total`.

The replay contract is therefore exact, not merely bounded: durable rows must
equal the once-per-frame expansion of the world's firehose
(`CompareEventLogMultiset` — zero bloat, zero loss), final state must converge,
and structural invariants must hold. `TestOracle_RelaySeqDuplicates` /
`TestOracle_RelaySeqRegression` (internal/oracle/replay_fault_test.go, <1s)
drive the REAL live consumer — real websocket, real atmos pipeline,
pebble-backed verifier state — against the simulator relay over a
delete→reactivate→recreate window, with anti-vacuity on the fault firing, the
exact replayed-frame count, and the guard's drop counter.

m035 (`m035_account_replay_guard_inverted`) inverts the guard's kind check so
it never examines an #account event — the exact pre-#231 production state.
Red-first verified: with the guard disabled both scenarios fail on final-state
divergence (`oracle: missing ... recreated1`); clean tree green. KILLED@replay.

Also in this change, riding along per the issue body: relay sequence gaps split
out of `decode_errors_total` into `sequence_gaps_total` +
`sequence_gap_missed_seqs_total`, so relay data loss is
operator-distinguishable from garbage frames.

### Survivors (5) — unchanged, all documented escapes with owning issues

m003 (#209), m009 (#208), m013/m014 (#204), m015 (#208). No disposition
regressed vs the `075fafd` baseline; the only change is m035 added KILLED.


## Campaign 2026-07-04 — `corpus` tier; m009 SURVIVED→KILLED (#32)

Targeted run at `af7c00a` (`testing/mutation/run.sh m009`):
**m009 KILLED@corpus** (default and stress tiers still pass it, as the
structural analysis predicts — the kill comes from the new tier).
Banked into the `d08ed8b` baseline for m009 only; no other disposition
touched, leaving 24 KILLED / 4 SURVIVED.

**New `corpus` tier; the symmetric-checksum blind spot is closed.** m009
(`xxh3HeaderFooter` computing over `headerBytes[13:]` instead of `[12:]`) was
the catalog's canonical structural escape: seal and `Reader.Open` share the
function, so the mutated writer and mutated reader always agree, and every
write-then-read-back check in the tree — the whole oracle included — passes.
The 2026-06-16 analysis dispositioned it "accepted blind spot — only an
independent checksum oracle (or a committed golden segment with a known-good
checksum) would catch it." #32 built exactly that.

The tier runs `internal/corpus` (<150ms, offline): real network bytes with
expected outputs pinned by foreign implementations at capture time
(production Jetstream v1 JSON for a contiguous relay firehose window,
indigo/goat for a production getRepo CAR), plus
`testdata/golden_corpus_segment.jss` — a sealed segment produced from the
real CAR's records by a known-good build. `TestCorpusSegmentGolden` kills
m009 twice over:

- write side: the mutated seal produces a different checksum in the fixed
  header, so the byte-exact compare against the committed golden fails;
- read side: `segment.Open` on the COMMITTED file recomputes the digest with
  the mutated range and rejects the stored (correct) checksum.

Either alone suffices; together they also pin the failure a symmetric shift
actually causes in production — every segment the mutated build writes is
checksum-corrupt for any correct build that later opens it (rolling upgrade,
downgrade, `inspect-segment`, external tooling).

Red-first verified during development: the golden test failed under the m009
patch (header bytes `41 b6 8f 2c...` vs golden `dc ca c2 5d...`) before the
tier was wired. m009's header now declares `expected-tier: corpus` with
`tiers: default,stress,corpus` — default/stress stay as documentation that
the closed loop still cannot see it.

Beyond m009, the tier's firehose replay pins the full live path (raw relay
CBOR frames → atmos decode → offline Sync 1.1 signature verification →
ConvertEvent → ingest → v1 JSON vs production Jetstream's own output,
field-for-field), and the CAR test pins atmos's MST walk + CID derivation
against indigo's on the same real repo — coverage for the broader
atmos-closed-loop class (`specs/oracle.md` "Real-Data Corpus Tier"), not just
the checksum instance. Fixture provenance and the re-capture procedure:
`internal/corpus/testdata/README.md`; capture-tool source preserved on #32.

Remaining survivors: m003 (#209), m013/m014 (#204), m015 (#208).

## Campaign 2026-07-04 — m041 identity-swallowed mutant (#202)

Full 30-mutant campaign at `d77fa93` (the m041-mutant commit, on the #202 branch
with the identity traffic + harness asserts): **26 KILLED / 4 SURVIVED,
zero STALE / BUILD-BROKEN**. Survivors unchanged and all owned:
m003 (#209), m013/m014 (#204, dead-path retirement planned), m015 (#208).
Baseline re-banked.

**m041_identity_swallowed — KILLED@default, banked on arrival.**
`convertIdentity` returns `nil, nil` (the legitimate #info no-op shape),
modeling a ConvertEvent dispatch refactor that folds #identity into the
"informational, nothing to archive" branch. Before #202 the simulator
never emitted #identity, so this exact edit was invisible to every
oracle tier — the m013/m014 green-by-vacancy class, now with a live
path. Kill mechanism: the bootstrap traffic closure injects a
deterministic #identity frame whose archive ack never fires under the
mutant, so the after-bootstrap barrier times out; if pacing ever let a
run proceed, `assertIdentityArchived` (injection-keyed anti-vacuity)
and the exact-multiset event-log compares (the expected side decodes
#identity from firehose history) catch the missing rows, and 11
unit/corpus tests fail besides. Red-first verified before the harness
asserts landed.

Context: #202 also made the default `TrafficMix` emit #identity (~3%,
vs 0.061% measured on production 2026-07-04 — deliberately above rate
for statistical reach at oracle scale), and its first non-short run
found production bug #234 (#identity relay-replay re-archival; fixed
same branch — durable applied-seq ratchet + consumer guard, the #231
sibling). The enqueuer's malformed-DID gate is now oracle-asserted via
the harness's first debug-/metrics scrape (two-sided: invalid_did ≥ 1
AND already_known ≥ 1).

## Campaign 2026-07-04 — #204 adversarial ingest traffic; m013/m014 retired, five gate mutants added

Full campaign at `a4e96c5` (branch adversarial-ingest-traffic-204, forked
from main before the `replay`/`corpus` sections above landed). **31 mutants:
28 KILLED, 3 SURVIVED, zero STALE/BUILD-BROKEN.** Baseline regenerated and
banked.

**Catalog changes:**

- **m013/m014 RETIRED (dead path, not sleeping).** Both mutated
  `convertVerifiedOps`, the ConvertEvent default arm. Under atmos v0.2.10
  that arm is unreachable: `verify_worker.go` mutates the original event in
  place (the Commit envelope stays set) and async resyncs are wrapped in a
  synthetic Sync envelope by `eventFromAsyncResync`, so every verified-ops
  event routes through `convertCommit` or `convertSync`. No traffic mode can
  ever execute the mutated lines — they model bugs in dead code, and the
  catalog convention retires dead mutants. The tracking doc's original
  prediction ("#204 kills m013/m014 by making the path live") was WRONG in an
  instructive way: the path is not dormant-under-polite-traffic, it is
  structurally dead. The bug class stays covered by m017/m018 (the
  convertCommit copy of the field mapping) and the new m036/m037 (the
  convertSync copy).
- **m036/m037 ADDED** — collection/rkey swap and rev-drop in `convertSync`'s
  resync-op loop, the third copy of the field mapping, live under every
  sync-divergence resync. Both KILLED@default via the steady-state event-log
  compare (expected replacement rows carry correct coordinates/rev).
- **m038 ADDED** — live per-op #197 gate skipped (`validateOpPath` result
  ignored in convertCommit). KILLED@default: the #204 adversarial phase's
  ledger-filtered expected log flags the archived lie as an extra row.
- **m039 ADDED** — backfill rkey gate declassified in `splitRecordPath`.
  KILLED@default: ledger-filtered ground truth flags the archived backfill
  lie as an extra record at the after-bootstrap Compare.
- **m040 ADDED** — per-op drop escalated to whole-event drop in convertCommit
  (survivors-contract break). KILLED@default: the benign sibling in every
  adversarial commit becomes a missing expected row.

**Why this run mattered:** it banks the #204 adversarial coverage as
gate-enforced. The five new kills all depend on machinery this branch adds
(world adversarial ledger, expected-side filtering, drop-counter floors), so
a regression in any of it now fails the scheduled gate, not just the
lifecycle test.

**Merge resolution 2026-07-05.** Merging origin/main (post-#230 corpus +
#232 replay) into this branch produced the predicted baseline conflict;
resolved as the union: m009 stays KILLED@corpus and m035 KILLED@replay from
main's bank, m013/m014 removed and m036–m040 KILLED@default from this
branch's bank. The merged baseline is **32 mutants, 30 KILLED / 2
SURVIVED** (m003 → #209, m015 → #208 — m009 no longer survives). The
`commit` field records `a4e96c5` (this branch's campaign head); per the
#228 precedent that field is provenance-only — the gate diffs dispositions.
The full campaign was re-run at the merge head to verify the union bank
(see the 2026-07-05 section below).

Survivors at this branch's own run: m003 → #209 (merge cursor no-advance),
m009/m015 → #208 (footer/checksum blind spots; m009's corpus kill lives on
main and joins at the merge).

## Campaign 2026-07-05 — merge-head verification of the #204 union bank

Full campaign + gate at the merge commit `389b29c` (adversarial-ingest-
traffic-204 ∪ main's #230 corpus + #232 replay): **gate PASS — 32 mutants
match baseline, 30 KILLED / 2 SURVIVED, zero STALE/BUILD-BROKEN.** Every
disposition from both parents reproduced side by side at one head: m009
KILLED@corpus and m035 KILLED@replay (main's banks) alongside m036–m040
KILLED@default (this branch's #204 gate mutants), with m013/m014 absent
(retired). Survivors: m003 → #209, m015 → #208. The union-merge resolution
described in the section above is verified, not just asserted.

## Campaign 2026-07-05 — second union merge: #204 ∪ #202 (m041)

Merging origin/main (post-#236, the #202 identity work with m041 and its
30-mutant bank at `d77fa93`) into the #204 branch produced the same
baseline-conflict shape as the #230/#232 merges; resolved as the union:
m041 KILLED@default from main's bank (already renumbered from its original
m036 id in 82b2dd9 to avoid colliding with this branch's m036), m013/m014
absent and m036–m040 KILLED@default from this branch's bank. The merged
baseline is **33 mutants, 31 KILLED / 2 SURVIVED** (m003 → #209,
m015 → #208). The `commit` field stays `a4e96c5` (provenance-only). The
full campaign was re-run at this second merge head to verify the union —
results in the section below.

## Campaign 2026-07-05 — verification at the second merge head

Full campaign + gate at the merge commit `09f44f6` (#204 ∪ #202/m041 ∪
#230 corpus ∪ #232 replay): **gate PASS — 33 mutants match baseline,
31 KILLED / 2 SURVIVED, zero STALE/BUILD-BROKEN.** All four in-flight
banks now reproduce side by side at one head: m036–m040 KILLED@default
(#204 gate mutants), m041 KILLED@default (#202 identity), m035
KILLED@replay (#232), m009 KILLED@corpus (#230), with m013/m014 absent
(retired). Survivors: m003 → #209, m015 → #208. Also green at this head:
short suites, default + stress lifecycle, -race on oracle/simulator,
lint. This is the PR-merge state for #235.

## Campaign 2026-07-05 — `frames` tier; m042 banked (#206)

Original campaign at `248b9c5` (branch `frame-adversity-206`, pre-merge,
where this mutant was numbered m036): **30 mutants: 26 KILLED, 4
SURVIVED, zero STALE/BUILD-BROKEN**; no disposition changed vs the
`5d6fc9e` baseline. Merging origin/main first pulled in the #204 ∪ #202 ∪
#230 ∪ #232 union above; that collided with the #204 branch's freshly-minted
m036–m040, so the frames mutant was renumbered to **m042** (same precedent as
m041's renumber in 82b2dd9). A later merge pulled in #209's
`restart-multisource` tier, changing m003 from SURVIVED to KILLED. The current
baseline.json resolves as the full union: main's 33 dispositions with m003
KILLED@restart-multisource plus m042 KILLED@frames — **34 mutants, 33 KILLED /
1 SURVIVED** (m015 #208).

**New `frames` tier; m042 banked KILLED@frames.** #206 made frame-level
wire adversity live: the simulator relay can inject arbitrary bytes on
the subscribeRepos socket (`SubscribeReposInjectFault` — inject-only,
inject+swallow positional replace, swallow-only pure gap) and emit
partial-CAR commits whose ops reference record leaf blocks absent from
the CAR (`GenerateMultiOpCommitForTest` with per-op `StripBlock`; the
commit block and MST nodes always survive, so the fault is precisely
"op without its block", not a malformed CAR). Six oracle scenarios
(internal/oracle/frame_fault_test.go, real consumer end-to-end) pin the
poison-frame contract: garbage CBOR (decode counter, same-conn
continue), unknown frame type (unknown counter, body seq suppresses the
spurious gap), op=-1 error frame (`stream_error_frames_total{code}`),
oversized frame (read-limit reconnect, #205 guards keep redelivery at
zero loss and zero bloat), swallowed frame (a REAL gap, loss bounded to
exactly the swallowed window, chain-break resync self-heal), and
stripped leaf block (`dropped_events_total{source=live,
reason=missing_block}` per op, siblings archive, NO chain break).

m042 (`m042_missing_block_arm_discards_siblings`) makes the consumer's
per-op-drop arm advance the cursor and `continue` instead of falling
through to the append — the well-formed siblings of a partial-CAR
commit are silently discarded while the drop counters report exactly
the drops the operator expects to see. The tier kills it at two layers:
the oracle scenario's surviving-rows barrier times out, and the
live-package unit test fails its new `WriterMetrics.EventsAppended==1`
assertion. That assertion was added FOR this mutant: pre-strengthening,
`TestProcessBatch_MissingBlockOpDoesNotShutDownConsumer` passed under
m042 (cursor advanced, counter bumped, no error propagated) while the
survivor was discarded — a vacuity worth recording. Red-first verified
in both directions before the patch was banked.

## Campaign 2026-07-05 — verification at the #206 merge head

Full campaign + gate at `1fa6640` (frame-adversity-206 ∪ main's #204 ∪
#202 ∪ #201): **gate PASS — 34 mutants match baseline, 32 KILLED / 2
SURVIVED, zero STALE/BUILD-BROKEN.** All in-flight banks reproduce side
by side at one head: m042 KILLED@frames (#206), m036–m040 KILLED@default
(#204 gate mutants), m041 KILLED@default (#202 identity), m035
KILLED@replay (#232), m009 KILLED@corpus (#230). Survivors: m003 → #209,
m015 → #208.

One STALE fixed en route: the first merge-head run flagged m011 (its
flushLocked hunk moved when #201 inserted the beforeIO fault hook above
the file write). Hunk regenerated against the current writer.go — the
mutation itself is unchanged — and the re-run banked it KILLED@default.
Also green at this head: short suites (2017 tests), lint, -race over
oracle/live/simulator, and 3× repeats of the frame + replay oracle
tiers under atmos v0.2.13.

## Campaign 2026-07-05 — #209 deterministic multi-source restart tier

Full campaign at `fdbfaa7` after adding
`TestOracle_RestartMultiSourceMergeCursorNoReprocess` and the
`restart-multisource` mutation tier: **33 active mutants, 32 KILLED / 1
SURVIVED, zero STALE/BUILD-BROKEN.** The baseline was refreshed with m003
banked as **KILLED@restart-multisource**; m015 remains the sole survivor and
continues to belong to #208.

The new tier forces bootstrap-live to rotate after every accepted event
(`MaxEventsPerBlock=1`, `MaxSegmentBytes=1`), kills the restart child at the
9th `AfterMergeDstFlushBeforeSourceCommit` occurrence, captures rows from the
already-committed source immediately before the crash boundary, then proves
surviving captured rows appear exactly once after recovery. Under m003's
`commitSourceComplete(sf.Idx)` off-by-one, restart reprocesses that committed
source and the precise no-reprocess assertion fails.

Incidental maintenance: m011's patch context was refreshed for the existing
`beforeIO(IOOpWrite)` hook in `segment/writer.go`; the modeled bug and kill
tier are unchanged, and the full campaign re-confirmed **KILLED@default**.

## Merge resolution 2026-07-05 — #206 frame adversity ∪ #209 restart-multisource

Merging origin/main after #209 into `frame-adversity-206` produced the expected
scorecard conflict: #206 had added m042 KILLED@frames, while #209 had changed
m003 from SURVIVED to KILLED@restart-multisource. The resolved baseline is the
union: **34 active mutants, 33 KILLED / 1 SURVIVED**, with m015 as the only
remaining survivor. The m011 conflict was metadata-only; both sides carried the
same mutation body and current `segment/writer.go` context.

## Campaign 2026-07-06 — `segmentfault` tier; m044/m045 banked, m035 refreshed (#200)

Full campaign at `ad35629` (branch `segment-io-fault-200`) after landing the
segment I/O fault layer: **37 active mutants, 37 KILLED, zero
STALE/BUILD-BROKEN.** Baseline refreshed.

New tier `segmentfault` (#200), the segment-file sibling of storefault: one
`go test` across `./internal/oracle ./internal/ingest/orchestrator ./segment`
covering the oracle restart segment-fault scenarios (fail-loud observed-marker
protocol, ENOSPC operator-message e2e, rename fault deterministically landing
on the merge-tail compaction rewrite), the torn-tail corruption sweep, the
orchestrator-level compaction/import ENOSPC + import fault sweeps, and the
segment package's exhaustive (op, ordinal) Patch/Rewrite sweeps.

| mutant | result | note |
|---|---|---|
| m044_flush_write_fault_check_dropped | KILLED@segmentfault | flushLocked drops the pre-write fault consult's error; killed fast by TestFlushReturnsENOSPCOnBlockWrite. Oracle-layer second kill pending #262 (the flush-reaching case is that bug's skipped repro). |
| m045_compaction_rewrite_error_swallowed | KILLED@segmentfault | compaction-rewrite call-site err check inverted (m006's shape on the segment path); killed at BOTH layers — oracle rename-eio observed-marker absent AND TestRunDeleteCompaction_ENOSPCRewriteReturnsFatalOperatorMessage. |

Both kills verified non-vacuously by hand-applying each patch before banking.

Incidental maintenance: m035's patch context was refreshed for the #255
account-replay-guard rewrite (`a350dd5` moved its context lines); the modeled
bug and kill tier are unchanged, and the scoped re-run plus this full campaign
re-confirmed **KILLED@replay**.

The tier's first end-to-end run also flushed out a real pre-existing data-loss
bug (#262: stale account tombstone erases re-backfilled records after a
mid-backfill crash) — filed with a diary entry
(`specs/oracle/2026-07-06-rebackfill-erased-by-stale-account-tombstone.md`);
its deterministic repro is the `t.Skip`'d `write-shortwrite-first-flush` case.
