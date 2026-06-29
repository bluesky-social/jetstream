# Implementation plan: drop client tombstones + paginated bufferless cutover

Date: 2026-06-28
Branch: `tombstone-query-plan-refactor` (work continues here)
Status: **plan — awaiting review** (not yet implemented)
Design source of truth: `specs/notes/2026-06-28-drop-client-tombstones-design.md`
Authoritative sections of the design: the Revision block **§R1–R8** (everything below
the "READING ORDER" banner, §1–§16, is the reasoning trail only — §R wins on conflict).

> **How to use this document.** This is the *living* implementation plan. Each step below
> maps a design decision to concrete, code-verified file changes, the tests that gate it,
> and a verification command. Update the checkboxes and the "Status / notes" lines as we
> land each step. File one GitHub issue per step before starting it (AGENTS.md), reference
> the issue in the branch/commits, and close it via `Closes #N` in the PR.

---

## 0. Orientation — what we verified before planning

All file:line references below were read this session against the current tree. Key facts
the plan rests on (each is a *load-bearing invariant*; a future refactor that breaks one
re-opens a correctness hole):

- **Compaction ordering is rewrite → save-watermark → evict.** `compact_deletes.go`:
  `applyCompactionChunk` (the segment rewrite that physically drops victim creates) returns
  *before* `saveCompactionWatermark(chunkEnd)` (~`:185`) which is *before*
  `o.cfg.Tombstones.Evict(chunkEnd)` (~`:189`). This is the chain the §R4 race-freedom proof
  depends on. **Do not reorder.** (Verified: lines 155–195.)
- **`getBlock`/`getSegment` read on-disk truth at fetch time** (no planned-checksum trust).
  The §R4 proof relies on this.
- **DID-level tombstones carry an empty collection** and are never indexed into a block's
  collection summary, so a collection-filtered `planBackfill` never downloads them. This is
  the entire reason §R4's start-snapshot exists. (`segment/event.go`, `segment/seal.go`,
  `selectPlanBlocks`/`collectionIDsForSegment` in `internal/manifest/plan.go`.)
- **`tombstone.Set.SnapshotRange(lowExcl, highIncl)`** already exists and already filters to
  `seq ∈ (low, high]`, keyed by DID and RecordKey (`tombstone.go:73-91`). §R4 reuses it; it
  is **kept**, not pruned (overriding the older §4.3 "likely removable" note).
- **`Snapshot.ShouldDrop`** checks BOTH DID-level and record-level entries
  (`tombstone.go:172-184`). §R4 step 3 requires a **DID-only** application; we will add a
  DID-only variant (or pass a snapshot whose `Records` map is empty).
- **Seq counter seed point** is `internal/ingest/writer.go:126-142` (`reconciled := pebbleSeq;
  if foundEvents && maxSeq+1 > reconciled { reconciled = maxSeq+1 }; w.nextSeq = reconciled`).
  `loadNextSeq` (`:700-703`) **discards the present-bit** from `store.GetUint64LE` (which does
  return `(uint64, bool, error)` — `internal/store/encoding.go:16`). Fresh-dir detection needs
  that bit. The running increments — `w.nextSeq++` (`:331`) and `prepared.MaxSeq()+1`
  (`async_flush.go:114`) — are correct and **must not be touched**.
- **Watermark init** (`initCompactionWatermarkFloor`, `compaction_watermark.go:29-40`) needs
  **no change**: with `nextSeq = 1` its `else` branch yields watermark `nextSeq-1 = 0`
  ("nothing compacted"), which is correct. It is a DIFFERENT counter (`compaction/seq`) from
  the event seq (`seq/next`).
- **Cursor resolution happens before the websocket upgrade** specifically so a bad cursor
  can return HTTP 400 (`handler.go:131,177-186`; upgrade at `:223`). §14 routes the too-old
  case into this existing 400 path. `ResolveCursor` has exactly one production caller
  (`handler.go:177`).
- **Per-endpoint v1/v2 policy already varies via Subscription flags**
  (`EmitResyncReplacementRows`, `FilterIdentityByCollection`) set at the v2 route
  (`runtime.go:420-430`). §14's `RejectBelowFloor` and §R3's identity change live in the same
  place.
- **The bundled CLI client (`cmd/client`) and the oracle drive the public `jetstream` engine**,
  so removing `SeedFromOverlay` from the engine fixes them automatically; only their *test*
  mocks of `getTombstones` need deleting.
- **API codegen**: `just lexgen` runs `github.com/jcalabro/atmos/cmd/lexgen -lexdir lexicons
  -config lexgen.json`, regenerating `api/jetstream/*` from `lexicons/network/bsky/jetstream/
  *.json`. Every lexicon edit below is followed by `just lexgen` + a build.

---

## 1. Global rules for this effort

- **TDD, gated.** Where the design says a fix is gated by a test (notably §R7 gating step 3,
  and the §16 mutants), the test is written **first** and must **fail** without the change.
- **One issue per step.** Title `subsystem: …` per AGENTS.md. Comment on start / approach
  change / finding. Close via `Closes #N`.
- **No tech debt, cut deep.** Delete dead code outright (nothing is deployed, no consumers —
  per the design's deployment-context note). No compat shims, no renamed `_unused` vars, no
  "removed" comments.
- **On-disk segment format is frozen.** None of these steps change the segment file format.
  (The wire/lexicon and the in-memory/Pebble `seq/next` *seed* change; segment bytes do not.)
- **Run the right tests after each correctness-touching step** (3, 6, 7, 8, 10, 11):
  `just test ./internal/oracle`, `just test-long ./internal/oracle`, and the
  `just oracle` / `just oracle-sweep` recipes; plus `just lint test` for the whole tree.
- **Crash loud, never corrupt.** Snapshot-fetch failure is fatal (§R6.6). Invalid *external*
  data (relay/firehose/backfill rows) is dropped-with-metric, never fatal.

---

## 2. Dependency graph (from design §8, restated)

```
Part A:  [6 oracle tests] ──gates──> [3 snapshot fix] ──> [4 remove overlay] ──> [5 prune tombstone API]
         [1 deliver acct/id/sync] ──> [3]
Part B:  [7 seqs@1]  (do early; simplifies 8,9,10)
         [8 paginate planBackfill] ─┐
         [9 v2 too-old 400] ────────┼──> [10 client paginate + delete buffer] ──> [11 Part B oracle]
         [2 remove suppression] ────┘
         [12 docs] (last)
```

Concrete ordering we will follow: **1 → 2 → 7 → 6 → 3 → 4 → 5 → 8 → 9 → 10 → 11 → 12.**
(7 is pulled before 6 because 1-based seqs simplify the oracle harness and delete the
`backfillCoveredNothing`/dedup-floor machinery the buffer tests touch; 6 still gates 3.)

---

## 3. Step-by-step plan

### ☐ Step 1 — `subscribe+client: always deliver #account/#identity/#sync on v1 and v2` (§R3, design §3/§5.2)

**Goal.** `#account`, `#identity`, `#sync` are delivered **unconditionally** on both
`/subscribe` (v1) and `/subscribe-v2`, bypassing the collection filter (still subject to the
DID filter). v1 and v2 become identical on this axis. The Go-client `Matcher` stops dropping
account/identity under a collection filter.

**Server changes (`internal/subscribe/`).**
- `filter.go`: delete the `filterIdentityByCollection` field (`:56`), the
  `withIdentityCollectionPolicy` helper (`:71-77`), and the v2 branch in `Wants`
  (`:337-344`). After removal, in `Wants`: a non-commit kind (identity/account/sync) with a
  collection filter set **always returns true** (bypass). Keep the empty-collection commit
  bypass (`:353`). Update the type doc (`:42-57`) and the `Wants` doc (`:302-320`) to state the
  uniform contract.
- `handler.go`: delete the `FilterIdentityByCollection` field from `Subscription` (`:60-69`)
  and the two `withIdentityCollectionPolicy(...)` calls (`:126`, `:328`).
- `runtime.go`: drop `FilterIdentityByCollection: true` from the v2 route (`:429`).

> Verified current state (report #11): `#account`/`#sync` already bypass on the server; only
> `#identity` on v2 actually changes server-side here.

**Client change (`internal/client/filter.go`).**
- In `Matcher.Wants`, delete the `!ev.Kind.IsCommit() → return false` drop under a collection
  filter (`:97-103`). New rule: with a collection filter set, identity/account/sync bypass
  (return true, subject to the DID filter already applied above). Update the type doc
  (`:9-36`) — and **remove the now-false claim** that "tombstone folding happens in the
  Suppressor BEFORE this matcher" (`:29-33`); that coupling is being deleted in steps 2–4.

**Tests.**
- `internal/subscribe/filter_test.go`: `TestWants_V2IdentityFilteredByCollection_AccountStays`
  becomes "v2 identity also delivered under collection filter"; keep
  `TestWants_IdentityBypassesCollectionFilter`. Add an explicit v2-identity-delivered case.
- `internal/client/filter_test.go`: flip
  `TestMatcherCollectionFilterDropsIdentityAccount` → `…DeliversIdentityAccount` (now
  delivered); keep `TestMatcherNoCollectionFilterDeliversIdentityAccount`.

**Verify.** `just test ./internal/subscribe ./internal/client`.
**Status / notes.** ✅ **Done** (issue #171). Server: removed `filterIdentityByCollection`
field + `withIdentityCollectionPolicy` helper + the v2 `Wants` branch (`filter.go`); removed
`Subscription.FilterIdentityByCollection` + its two call sites (`handler.go`); dropped the v2
route flag (`runtime.go`). Client: `Matcher.Wants` now bypasses the collection filter for all
non-commit (DID-level) kinds (`internal/client/filter.go`), type doc rewritten (dropped the
false "Suppressor folds before this matcher" coupling note). Tests flipped: subscribe
`filter_test.go` (`TestWants_DIDLevelEventsBypassCollectionFilter`,
`…DeliveredWithoutCollectionFilter`), `handler_test.go`
(`TestHandler_DIDLevelEventsBypassCollectionFilter`), client `filter_test.go`
(`TestMatcherCollectionFilterDeliversDIDLevelEvents`), `live_test.go` + `engine_test.go`
(account/identity now delivered under a collection filter). Full `just lint test` +
`just test ./internal/oracle` green.
**Carried to step 12 (docs):** `docs/README.md:562-566` (§4.4) still describes the old
"v2 drops #identity under a collection filter / client hides #account" policy. It is now
false, but it is coupled to the suppression narrative that survives until steps 2–4, so the
correction lands with the consolidated doc rewrite in step 12 (which already lists §4.4).

---

### ☐ Step 2 — `client: remove tombstone suppression from backfill` (design §5.1, §4.1)

**Goal.** The backfill download path keeps the `Matcher` and drops the `Suppressor`. (The
cutover buffer's *ordering* role stays for now; it is deleted in step 10. Its *folding* role
goes away here — this is exactly why A precedes B, design §15.)

**Changes (`internal/client/`).**
- **Delete** `suppress.go` and `suppress_test.go` (`Suppressor`, `SeedFromOverlay`, `Merge`,
  `ObserveLive`, `ShouldDrop`). This drops the only client import of `internal/overlay` and of
  `api/jetstream.JetstreamGetTombstones`.
- **Delete** `selector.go` + `selector_test.go`; have the downloader filter via
  `matcher.Wants` directly. `RowSelector` interface in `downloader.go` stays (still used to
  pre-decode-filter); supply it a thin matcher-only adapter, or pass a `func(*segment.Event)`
  — implementer's choice; prefer deleting the `rowSelector` indirection. Update `NewDownloader`
  call sites (`engine.go:388,493`) accordingly.
- `engine.go`: remove the `suppressor` field (`:123,137`), the `NewSuppressor()` call, and the
  two `SeedFromOverlay` steps (`runBackfillOnly` `:371-374`, `runBackfillThenLive` `:419-422`,
  incl. the now-irrelevant `(w,m)` discard). The cutover boundary already keys on
  `plan.PlannedThroughSeq`, independent of the overlay — confirm no other consumer of the
  overlay `M` exists (status page, metrics) before deleting (design §9).
- `livesink.go`: delete `observeTombstone` (`:172-175`) and the `suppressor.ShouldDrop` check
  inside `wantLive` (`:165-167`); `wantLive` keeps only `matcher.Wants`. The buffer/flip/drain
  role stays until step 10. Remove the `suppressor` field from `liveSink` and `newLiveSink`.
- Audit `live_test.go`, `reconstruct_test.go`, `slab_test.go`, `engine_test.go` for
  `NewSuppressor`/`newRowSelector` construction and reduce to matcher-only.

**Tests.** Existing client backfill/live tests must still pass minus the suppression
assertions. The end-to-end correctness regression is covered by the oracle in step 6 (which is
authored before step 3, but step 2 itself is a pure removal — its safety is that the oracle
still passes the *eventually-consistent* contract once step 6 lands).

**Verify.** `just test ./internal/client`. **Status / notes.** ✅ **Done** (issue #172).
Deleted `suppress.go`/`suppress_test.go` + `selector.go`/`selector_test.go`. `*Matcher` now
satisfies `RowSelector` directly via a new `Keep` method (deleted the `rowSelector`
indirection). `engine.go`: removed the `suppressor` field/`NewSuppressor`, both
`SeedFromOverlay` steps, and passes `e.matcher` straight to `NewDownloader`; renumbered the
phase comments. `livesink.go`: removed the `suppressor` field, `observeTombstone`, and the
`ShouldDrop` check — `wantLive` is matcher-only now. `segview.go`: dropped `accountPayload`
(was only needed for tombstone folding; the matcher never reads Payload). Tests: deleted the
client-side suppression property test `reconstruct_test.go` (the fold-convergence property
moves to the oracle in step 6); flipped `live_test.go`, `engine_test.go`
(`TestEngineBackfillCreateThenLiveDeleteConverges` now asserts the create is emitted + the
live delete arrives), `slab_test.go`; stripped the overlay mock from the engine harness and
`cmd/client/subscribe_test.go`. Full `just lint test`, `just test ./internal/client`, and
`just test-long ./internal/oracle` all green (end-to-end backfill still converges: the client
emits dead creates and folds the live-tail deletes).
**Note for step 4:** `internal/overlay`, `internal/obs/overlay.go`, the `getTombstones`
handler/route/lexicon/stub, and the server-side overlay cache/ticker are now ONLY referenced
by server-side wiring + the oracle's `overlay_*` files — no client/cmd consumer remains.

---

### ☐ Step 7 — `seqs: start at 1` (§R8) — pulled early

**Goal.** Event seq counter starts at 1; seq 0 becomes a pure "nothing yet" sentinel. Delete
the presence machinery that existed only to disambiguate "seq 0" from "nothing".

**Core change (`internal/ingest/writer.go`).**
- Change `loadNextSeq` (`:698-703`) to propagate the present-bit:
  `func loadNextSeq(st, key) (val uint64, present bool, err error)` returning
  `st.GetUint64LE(key)` verbatim.
- In `Open` (`:126-142`): on a **fresh dir** (`!present`), floor the seed to 1:
  ```
  pebbleSeq, present, err := loadNextSeq(...)
  reconciled := pebbleSeq
  if foundEvents && maxSeq+1 > reconciled { reconciled = maxSeq + 1 }
  if !present && reconciled < 1 { reconciled = 1 }   // fresh archive: first event is seq 1
  ```
  Leave the crash-recovery reconcile (`maxSeq+1`) untouched. **Do not** touch `w.nextSeq++`
  (`:331`) or `prepared.MaxSeq()+1` (`async_flush.go:114`).
- **Confirm by test**: the `+1` chain yields seq **1** for the first-ever event given the new
  seed (`candidate.Seq = w.nextSeq` at `:324`).

**Watermark.** **No change** to `initCompactionWatermarkFloor` — its `else` branch already
yields `nextSeq-1 = 0` under `nextSeq=1`. Do not edit `compaction_watermark.go`.

**Collapse the presence machinery (mostly deletion).** Audit every `gt.Option[uint64]` seq
site and collapse those that existed only to distinguish "seq 0" from "nothing":
- `internal/client/live.go`: `lastSeq` (`:120`) and `dedupFloor` (`:79`) become plain
  `uint64` (0 = nothing delivered; first real event seq 1 > 0 passes the dedup
  `ev.Seq <= lastSeq` automatically — the **seq-0 swallow becomes structurally impossible**).
  - **Keep one genuine sentinel**: `runLiveOnly`'s `LiveCursor==0 ⇒ "live from tip" (omit the
    wire cursor)` is a *user-API contract* (`WithLiveCursor`), NOT the seq-0/nothing collision.
    Express it with an explicit `fromTip bool` (or "omit cursor when 0 on the live-only path"),
    not a re-introduced Option. Document why this one stays.
- `internal/client/engine.go`: delete `backfillCoveredNothing` (`:448`), the `dedupFloor`
  Some/None split (`:449-452`), and `coveredThrough` Some/None (`:537-540`). With 1-based seqs,
  an empty archive is `plannedThroughSeq == 0` unambiguously; the live tail connects at
  `cursor=0` (replay-all) and the first event seq 1 passes. (Most of this is moot after step 10
  deletes the buffer, but collapsing now keeps the tree compiling and the diff honest.)
- `internal/client/livesink.go`: `flipAndDrain`'s `coveredThrough gt.Option[uint64]` and the
  `lastDelivered`/`lastForwarded` Options become plain `uint64`. (File deleted in step 10; this
  keeps step 7 self-consistent in between.)
- Remove the `#112`/`#111` "don't swallow seq 0" comments throughout (they describe a bug whose
  cause is now gone). Symbol-named anchors only — re-grep `#111`, `#112`, `seq 0`, `0-based`
  and clear each.

**Docs/comment statements that become false.**
- `docs/README.md:58` ("The seq space is 0-based, so `?cursor=0` replays the first-ever
  event") → seqs start at 1; `?cursor=0` replays from before the first event (= everything).
- `internal/client/filter.go:119-127` load-bearing comment ("jetstream's seq space is 0-based
  — the first-ever event is seq 0", ref #111). The `afterSeq>0` guard stays correct (afterSeq
  is exclusive; first real seq is 1). Update the comment text.
- Any parallel server-side comment in `internal/subscribe/cursor.go:24` ("monotonic counter
  starting at 0").

**Semantics that stay correct (state in the issue, verify by inspection).**
- `cursor=0` ⇒ replay from before the first real event = replay everything (same effective
  behavior as today).
- `afterSeq=0` ⇒ exclusive lower bound imposes nothing; first real seq 1 included.
- v1/v2 cursor split `CursorSeqMaxThreshold = 1e15` (`cursor.go:29`) unaffected.
- Segment *index* stays 0-based (`planner.go`) — different counter; do not conflate.

**Tests.**
- Re-point the empty-archive→first-event regression test: start from a genuinely empty
  archive, ingest the first-ever event (now **seq 1**), assert it is delivered exactly once
  across a from-empty backfill→live handoff (the old seq-0-swallow test, re-pointed).
- `internal/client/filter_test.go:TestMatcherAfterSeqZeroIncludesFirstEvent` → assert the
  first event is seq **1** and `WithAfterSeq(0)` still includes it.
- `internal/subscribe/cursor_test.go`: `TestResolveCursor_ZeroSeqClampsToFloor` and any
  "first seq 0" assertions → re-point to seq 1 semantics.
- Oracle invariant checks (`internal/oracle/invariants.go`) — seqs still strictly increasing &
  unique; only the *starting value* changes. Audit any test asserting "min seq == 0".

**No migration** (nothing deployed).
**Verify.** `just test ./internal/ingest ./internal/client ./internal/subscribe`, then
`just test ./internal/oracle` and `just test-long ./internal/oracle`.
**Status / notes.** ✅ **Done** (issue #173). Writer: `loadNextSeq` now returns the
present-bit; `Open` seeds `nextSeq=1` **in memory only** on a fresh dir (no persisted counter
AND no recovered events), preserving the "Open never writes pebble for a fresh dir" invariant.
Crash-recovery reconcile (`maxSeq+1`) and the running increments untouched;
`initCompactionWatermarkFloor` untouched (its `else` already yields watermark 0 under
`nextSeq=1`). Presence machinery collapsed: `live.go` `cursor`/`dedupFloor`/`lastSeq` are now
plain `uint64`; the "live from tip" sentinel is an explicit `fromTip bool` (set by `runLiveOnly`
when `LiveCursor==0`); `engine.go` dropped `backfillCoveredNothing` + the `dedupFloor`/
`coveredThrough` Some/None splits; `livesink.go` `flipAndDrain` takes a plain `uint64` (converts
to the buffer's `Option` at the `Replay` boundary, since 0↔None are now equivalent — no event
is seq 0). Comments/docs updated (`docs/README.md:58`, `cursor.go`, `filter.go`, root buffer
comments; cleared the #111/#112 anchors). Tests: ingest +1 shifts (delegated, verified — no
production bugs, fresh-dir resume/reconcile tests kept their seeded-value semantics); client
`live_test.go`/`engine_test.go`/`filter_test.go` re-pointed to seq 1 (incl. the from-empty
backfill→live regression test `TestEngineEmptyArchiveCutoverDeliversFirstEvent` and the live
analog `TestLiveConsumerDeliversFirstEvent`); root `buffer_test.go` generalized off the seq-0
rationale. Verified: `just lint`, full `just test`, `just test-long ./internal/ingest`,
`just test-long ./internal/oracle`, `just oracle` (20s stress) all green.

---

### ☐ Step 6 — `oracle: fold-convergence + DID-tombstone delivery` (§R7, design §6) — gates step 3

**Goal.** Replace the point-in-time `CheckOverlayReconstruction` with the §R7
eventually-consistent invariants, and **author the eviction-interleaving + reactivation tests
that must FAIL without step 3** (they gate step 3). This step is partly written before step 3
and re-run after it.

**New / rewritten invariants (the heart of the change — get these exactly right).**
1. **Output-restricted fold-convergence** (replaces old invariant 1 & §6.1). Fold the **full**
   received stream (creates/updates apply; deletes/account-deletes/syncs remove) in seq order
   with the same rules as `groundTruthLive` (`overlay.go:105-158`), **then restrict the OUTPUT
   record set by the query's collection filter**. Match a dead record's killer to a DID-level
   tombstone **by DID** (not by collection). Do **NOT** cross-check filtered-vs-filtered on the
   same server (that self-comparison is blind to the §R3 gap).
2. **At-least-once coverage (liveness).** Every record live in ground truth appears **≥ once**
   as a create/update in the emitted stream. Remove the old "emitted ≤ ground truth"
   assertion — transient stale rows are now expected.
3. **Tombstone delivery.** For every record ground truth considers dead, the emitted stream
   contains the delete/update/account-delete/sync row that kills it.

**New tests (§R7) — author NOW, several must fail until step 3:**
- **Eviction-interleaving (the core race).** Drive a collection-filtered backfill; between
  pages advance compaction so it crosses an account-delete `D` whose victim create `C < D` was
  already downloaded. Assert the client does **not** end up holding `C` (the start-snapshot
  suppressed it). A mutant that snapshots at the seam (not at start) must fail this.
- **Reactivation-after-snapshotted-delete.** Account deleted at `D ≤ S`, reactivated +
  re-creates a record at `R > S`. Assert the snapshot suppresses only `seq < D` creates, and
  the post-`S` reactivation `#account` + new record (from the live tail) are retained.
- **`afterSeq < W` boundary.** A backfill whose `afterSeq` is below the watermark; assert no
  create in `(afterSeq, W]` survives un-dropped on disk while lacking a snapshot tombstone
  (cannot happen by rewrite-before-evict, but lock the invariant).
- **Snapshot-before-first-fetch ordering.** A test that fails if the snapshot is read after
  any `getBlock`/`getSegment`. (Hook a counter/assertion into the test harness's transport.)

**File-level work (`internal/oracle/`).**
- `overlay.go`: keep `groundTruthLive`; replace `CheckOverlayReconstruction` with the new
  fold-convergence checker (output-restricted, by-DID killers). Remove imports of
  `internal/overlay` and any Suppressor reference.
- `overlay_test.go`: re-point the 3 reconstruction tests to fold-convergence semantics.
- `overlay_integration_test.go`: delete `fetchOverlay`/`fetchOverlayWithDIDTombstone`
  (the `getTombstones` HTTP calls). Fold its useful coverage into the client-observer path; the
  file likely loses its reason to exist.
- `client_observer_test.go`: update `assertClientBackfillCompacted` expectations from
  point-in-time to eventually-consistent (final-state Compare stays; drop "emitted ≤ ground
  truth"). Keep the zero-recoverable-error budget assertion.
- `main_test.go`: drop `overlay.WarmEncoder()` once `internal/overlay` is deleted (step 4).
- `harness_test.go`: no longer wires `FilterIdentityByCollection` (step 1) — verify.

**Mutation campaign (§6.1).** Retire the overlay-format mutants (`m020`, `m021`, `m023`) and
add a mutant that **reverts the snapshot/suppression path** (snapshot-at-seam instead of
at-start) — killed by eviction-interleaving. Keep `m022`, `m025`, `m027`, etc. Refresh
`testing/mutation/baseline.json`; a STALE scorecard is expected until re-reviewed.

**Verify.** `just test ./internal/oracle` (new fold-convergence passes on the
eventually-consistent path), `just test-long ./internal/oracle`. The eviction-interleaving and
reactivation tests **fail here** (no snapshot yet) — that failure is the gate for step 3.
**Status / notes.** ✅ **Done** (issue #174). Replaced the point-in-time
`CheckOverlayReconstruction` with `CheckFoldConvergence` (`internal/oracle/foldconvergence.go`,
renamed from `overlay.go`): folds the **full emitted** stream, restricts the OUTPUT by collection,
matches killers **by DID**, and compares against an **independent** ground truth
(`groundTruthLive` over the full observed stream — NOT a filtered-vs-filtered self-comparison,
§R7). Switched `groundTruthLive`/the checker to the oracle's own `RecordKey` and **dropped the
`internal/tombstone` import** — the oracle's correctness model is now independent of the
production package it checks. Deleted the dead `toSegmentEvent`/`maxU64`. Unit tests rewritten
(`foldconvergence_test.go`): added the checker's own gate
(`TestFoldConvergence_MissingDIDKillerDiverges` — a filtered stream missing the DID-killer folds
to *present* → divergent; passes now) + stale-version, reactivation, collection/wildcard
restriction cases. Deleted `overlay_integration_test.go` (the `getTombstones` HTTP path). Harness
(`harness_test.go`): removed the late-DID-tombstone overlay-blob capture + the now-dead
`accountTombstoneAck` type/helpers + `assertOverlayReconstruction`; the late account-delete
injection added no fold coverage (it landed after the client drain). `client_observer_test.go`:
re-doc'd the unfiltered Reconstruct→Compare-to-convergence as the no-filter fold-convergence
invariant under §R1/§R7 (it already converges — the gap is collection-filtered only).
`trace_determinism_test.go`: dropped the two `late_overlay_did_tombstone*` allowlist kinds (no
longer emitted). Kept `overlay.WarmEncoder()` in `main_test.go` (the overlay package is still
server-side until step 4). **The step-3 gate** is
`TestFoldConvergence_CollectionFilteredDIDTombstoneGap` (`foldconvergence_gate_test.go`): a
real-socket (httptest, **no synctest bubble** — one bubble per process, owned by the lifecycle
test) collection-filtered backfill-only client over hand-built sealed segments (create C in the
filter; account-delete D with empty collection in its own segment, both below the tip). **Captured
failure**: `client stream folds to a record that ground truth DELETED: {did:plc:victim
app.bsky.feed.post rkey} emitted_seq=1` — C downloaded, the empty-collection D never delivered to a
collection-filtered plan. Per the review decision, the failure was captured once as gate evidence,
then the test is `t.Skip`'d referencing #174/step-3 to keep the tree green; step 3 removes the
skip. **(Step 3 has since landed (#175): the skip is gone and this test now passes.)** Verified:
`just lint` (0 issues), `just test` (1675), `just test-long ./internal/oracle`,
explicit `TestOracle_DefaultLifecycle` (synctest, fast + swarm), `TestOracle_SameSeedTraceDeterminism`
(20 deterministic sections identical), and `just oracle` (20s stress) all green.

**Deferred (recorded, per §R7 / dependency order):**
- *Eviction-interleaving **between pages*** needs the pagination loop (step 10) and
  *snapshot-before-first-fetch ordering* needs the snapshot fetch (step 3); the single-shot
  filtered gate above gates step 3 at the right granularity. The mid-pagination sharpening +
  the ordering test land in steps 11 / 3.
- *Mutation campaign* (retire `m020`/`m021`/`m023`, add a snapshot-at-seam mutant): the
  overlay-format mutants reference `internal/overlay` (deleted in step 4) — refresh **after**
  step 4 so the mutants compile, alongside the step-11 Part-B mutants.
- **Step 12 doc debt found:** `options.go:100-102` (`WithCollections` doc) still claims
  "records for a deleted account are correctly suppressed — you just don't see the Account event
  itself" — false under the dropped suppression; coupled to the §4.4 narrative, fold into step 12.

---

### ☐ Step 3 — `client: backfill DID-tombstone start-snapshot` (§R4, §R4.1, §R6) — finding-#1 fix

**Goal.** Close the collection-filtered DID-tombstone gap with a backfill-start snapshot of the
server's in-memory `tombstone.Set`, piggybacked on `planBackfill` page 1. This is the single
most correctness-sensitive step. **Gated by step 6's eviction-interleaving + reactivation
tests** (they must already be failing).

**Wire surface (§R4.1) — lexicon + codegen.**
- `lexicons/network/bsky/jetstream/planBackfill.json`:
  - **Input**: add optional `wantDidTombstones: boolean` ("set true only on the first page of a
    fresh backfill to request the DID-tombstone snapshot over `(afterSeq, sealedTipSeq]`").
  - **Output**: add `didTombstones: array of #didTombstone` where
    `#didTombstone = { did: string (did), seq: integer }`. Populated **only** when
    `wantDidTombstones` was set. DID-level only; record-level tombstones are never sent (they
    ride inline). Also add `sealedTipSeq` here? — **No**: `sealedTipSeq` is added in **step 8**
    (pagination). Step 3 can land before step 8 by reading the snapshot range as
    `(afterSeq, plannedThroughSeq]` (today `plannedThroughSeq` == the tip for a single plan).
    When step 8 introduces the continuation/goal split, the snapshot range becomes
    `(afterSeq, sealedTipSeq]`. **Note this seam explicitly in both issues** so the range is
    re-pinned to `S = sealedTipSeq` when step 8 lands.
- `just lexgen`; rebuild; the generated `JetstreamPlanBackfill_Input/_Output` gain the fields.

**Server (`internal/xrpcapi/planbackfill.go`, `server.go`, `runtime.go`).**
- Thread the live `*tombstone.Set` into the planBackfill handler: add `Tombstones
  *tombstone.Set` to `xrpcapi.Config`, pass `tombstones` from `runtime.go` (the set
  constructed at `runtime.go:273`), and into `newPlanBackfillHandler`.
- In the handler, when `input.WantDidTombstones` is true, after planning, populate
  `didTombstones` from `Set.SnapshotRange(afterSeq, <upper>).DIDs` where `<upper>` is the
  plan's tip (`plannedThroughSeq` pre-step-8; `sealedTipSeq` post-step-8). For a DID-filtered
  request, **filter the snapshot to the requested DIDs server-side** (bound the payload).
- **Co-atomicity (§R4 step 2 / §R6.2).** Capture the tip and the snapshot in the **same handler
  invocation**. The manifest lock (tip) and tombstone lock (snapshot) are independent; the
  race-freedom proof relies on rewrite-before-evict + on-disk-read-at-fetch, **not** on holding
  both locks. State this in code comments at the capture site, and note that re-ordering the
  two reads is safe *for the server* but the **client-side** ordering (snapshot strictly before
  first `getBlock`) is the hard invariant — guaranteed structurally because the snapshot rides
  on the page-1 response that precedes all downloads.

**Client (`internal/client/planner.go`, `engine.go`).**
- `Planner.Plan` / `PlanRequest`: add a `WantDIDTombstones bool` input flag and surface
  `DIDTombstones []DIDTombstone` (`{DID string; Seq uint64}`) on the `Plan` result. Validate
  the wire ints (non-negative, ≤ MaxInt64) symmetric with existing guards.
- `engine.runBackfillThenLive` / `runBackfillOnly`:
  1. On the **first** `planBackfill` call set `WantDIDTombstones=true`; read `didTombstones`
     into a held snapshot (`tombstone.Snapshot` with only `DIDs` populated, `Records` empty).
  2. **Fail closed (§R6.6).** If the snapshot field is absent/unparseable on a page-1 response
     that requested it, that is **fatal** (`emitErr(fatal(...))`, return). An empty snapshot is
     shape-indistinguishable from a fetch failure only if we don't gate on the
     request/response contract — so gate on "we asked, the server is post-refactor, the field
     must be present (possibly empty array)". A *missing* field from a too-old server is fatal.
  3. Fold the snapshot as **DID-only, seq-scoped suppression** in the download row filter:
     drop an emitted materialization row iff `snap.DIDs[row.DID].Seq > row.Seq`. Use a DID-only
     path (do **not** consult `Records`). Add `tombstone.Snapshot.ShouldDropDID(ev)` or pass a
     `Records`-empty snapshot to `ShouldDrop`. **Never synthesize an `#account`/`#delete`
     event** from a snapshot entry (reactivation note, §R4).
  4. Encode **snapshot-before-first-fetch** as a hard ordering constraint: capture the snapshot
     from the page-1 plan *before* constructing/running the `Downloader`. A step-6 test pins
     this.
- **Pinned upper bound.** Pre-step-8 there is one plan, so the range is naturally fixed at the
  tip. The plan's §R6.1 "pin `beforeSeq = S` for the whole backfill" becomes load-bearing in
  step 10 (pagination); record in the step-10 issue that the snapshot range and the paginated
  download range must both be `(afterSeq, S]` with `S` pinned from page 1.

**Reactivation correctness (§R4 note).** `Set.Observe` records only the max account-delete seq
and does not clear on reactivation. Correct for us: the snapshot suppresses only the client's
own emitted creates with `seq < D` inside `(afterSeq, S]`; a record re-created at `seq > D` is
retained, and the reactivation `#account` arrives on the live tail above `S`. A client must
**never** treat a snapshot DID entry as "this account is dead now, purge it."

**Tests.** Step 6's eviction-interleaving + reactivation + snapshot-ordering tests now **pass**.
Add a client-unit test for the DID-only suppression fold and the fail-closed path.

**Verify.** `just test ./internal/client ./internal/xrpcapi ./internal/oracle`,
`just test-long ./internal/oracle`, `just oracle`. **Status / notes.** ✅ **Done** (issue #175).
Wire (`lexicons/.../planBackfill.json` + `just lexgen`): input `wantDidTombstones: boolean`;
output `didTombstones: [#didTombstone{did,seq}]` + **`didTombstonesIncluded: boolean`** — I added
the explicit presence flag rather than relying on empty-array-vs-absent (JSON can't distinguish
them robustly), so the §R6.6 fail-closed gate is unambiguous. Server (`xrpcapi.Config.Tombstones`,
`newPlanBackfillHandler`, `attachDIDTombstones`): when `wantDidTombstones` is set, snapshots
`Set.SnapshotRange(afterSeq, plannedThroughSeq).DIDs` co-atomically with the plan, filtered to the
requested DIDs server-side, and sets the flag true (even when empty); **fails loud (500)** if the
set is unwired. Wired `Tombstones: tombstones` in `runtime.go`. Client
(`planner.go`/`engine.go`/new `snapshot.go`): `PlanRequest.WantDIDTombstones`,
`Plan.DIDTombstones`/`DIDTombstonesIncluded`; new `planBackfillStart` requests the snapshot on
page 1, **fails closed** (`errSnapshotMissing`, wrapped `fatal`) if `!DIDTombstonesIncluded`, and
captures the snapshot strictly **before** building the Downloader (snapshot-before-first-fetch
ordering). `snapshotSelector` composes the matcher with DID-only seq-scoped suppression (drop a
materialization row iff `snap[DID] > row.Seq`, strictly-greater so reactivation survives); applied
to the backfill downloader ONLY, never the live sink. Tests: removed the `t.Skip` from the step-6
gate (`TestFoldConvergence_CollectionFilteredDIDTombstoneGap` — now **passes**, with its
`serveArchive` helper wiring a populated `tombstone.Set`); added planner wire-parse tests
(request flag, snapshot decode, included-but-empty vs absent, malformed-entry rejection), client
`snapshot_test.go` (suppression/reactivation/compose), an engine fail-closed test (both backfill
paths), and xrpcapi server tests (gated, DID-filtered, seq-window, fail-closed-when-unwired).
Verified: `just lint` (0), `just test` (1698), `just test-long ./internal/oracle`,
`TestOracle_DefaultLifecycle` (default + swarm), `just oracle` (20s stress) all green.
**⚠ §3↔§8 seam (carried to step 8):** the snapshot upper bound is currently
`plannedThroughSeq` (single-plan == tip today). When step 8 splits continuation-cursor vs
`sealedTipSeq`, `attachDIDTombstones` MUST re-pin the upper bound to `sealedTipSeq`, and step 10
must pin `beforeSeq = S` across pages so the snapshot range `(afterSeq, S]` lines up with the
downloaded bytes. Comment in `attachDIDTombstones` flags this.

---

### ☐ Step 4 — `server: remove getTombstones overlay endpoint` (design §4.2) — gated on step 3

**Goal.** Delete the overlay machinery now that §R4's snapshot replaces its DID-tombstone
coverage. **Do not start before step 3 lands.**

**Delete.**
- `internal/overlay/` — entire package (`format.go`, `cache.go`, `doc.go`, `metrics.go`,
  `format_test.go`, `cache_test.go`, `bench_test.go`).
- `internal/xrpcapi/gettombstones.go` + `gettombstones_test.go`; the `OverlaySource` interface;
  unregister the route in `server.go` (`:73-75`) and remove the `Overlay` field from
  `xrpcapi.Config` and `Server`.
- `internal/jetstreamd/overlay_source.go` (the `overlaySource` adapter).
- `internal/jetstreamd/runtime.go`: remove `overlayMetrics`/`overlay.NewCache` construction
  (`:274-276`), the `overlayCache` field (`:60`), the `Rebuild()` call in `onCompactionPass`
  (`:339`), the `Overlay: overlayCache` wiring (`:450`), the ticker goroutine (`:509-515`), the
  `overlayRebuildInterval` const (`:46`) + the validation/option
  (`OverlayRebuildInterval`, options.go:119, runtime.go:77-78, runtime_test.go:49-55), and the
  import (`:23`).
- `internal/obs/overlay.go` (Prometheus metrics) + its registration.
- `lexicons/network/bsky/jetstream/getTombstones.json`.
- `api/jetstream/jetstreamgettombstones.go` (regenerate via `just lexgen` so nothing
  references it).
- Test mocks: `cmd/client/subscribe_test.go` (`emptyOverlayBlob`, the getTombstones handler),
  `internal/client/engine_test.go` getTombstones mock handler, `internal/oracle/main_test.go`
  `overlay.WarmEncoder()`.

**Verify.** `grep -rn "overlay\|getTombstones\|GetTombstones" --include=*.go` returns nothing
outside the design notes; `just lint test`. **Status / notes.** _(unstarted)_

---

### ☐ Step 5 — `tombstone: prune overlay-only API` (design §4.3) — gated on step 4

**Goal.** Remove `tombstone.Set` members only the overlay used; **keep `SnapshotRange`** (step 3
now uses it) and everything compaction needs (`Observe`, `Evict`, `FoldRange`,
`Snapshot.ShouldDrop`, the compaction `decide` path).

**Audit & remove (only if no remaining caller after steps 3–4).**
- `Set.Dirty` / `dirty atomic.Uint64` (`tombstone.go:48-51,65,108,125`) — overlay-cache only.
  Remove if compaction metrics don't need it.
- `Set.ApproxBytes` + `bytes` accounting — overlay cache + metrics. Verify the
  `tombstone_set_bytes` gauge: if it's a *compaction* metric keep the accounting; if
  overlay-only, remove. (Check `orchestrator/metrics.go` `NewMetrics(reg, tombstones)`.)
- single-event `Fold` (vs `FoldRange`) — `Suppressor.ObserveLive` was the only caller and is
  deleted in step 2; remove `Fold` if nothing else calls it.
- `Snapshot.Merge` — was used by the `Suppressor` copy-on-write; remove if unused after step 2.
- The package doc (`tombstone.go:1-6`) mentions "the read-path overlay" — rewrite to "purely a
  compaction-internal structure" (also `compact_deletes.go:152` comment).

Do not over-prune: re-grep each symbol across the tree before deleting.
**Verify.** `just test ./internal/tombstone ./internal/ingest/...`. **Status / notes.** _(unstarted)_

---

### ☐ Step 8 — `manifest: paginate planBackfill` (design §12, §12.1) — before step 10

**Goal.** Replace `ErrPlanTooLarge` (normal path) with truncate-and-continue at a clean **work
unit** boundary, and add `sealedTipSeq` as the pagination goal. (`ErrInvalidPlanRequest` stays
for malformed input.)

**Planner change (`internal/manifest/plan.go`).** This is the trickiest correctness work.
- **Unit of truncation = the included work unit, not the enclosing segment.** A unit is one
  whole-segment entry (segment mode) or one coalesced block range (block mode). Today the loop
  adds *all* of a segment's units then checks the cap (`:151-161`); change it to check after
  **each unit**.
- **Continuation cursor = `MaxSeq` of the LAST included unit.**
  - Block mode: the last included coalesced block range's `MaxSeq`. Note `BlockRange` currently
    carries only `First/Last` block **indices** — we need each range's seq `MaxSeq`. Source it
    from `seg.Blocks[last].MaxSeq` (`segment.BlockInfo.MaxSeq`). Extend the internal accounting
    to track, per coalesced range, the `MaxSeq` of its last block (the block index → seq map is
    already in `seg.Blocks`).
  - Whole-segment mode: that segment's `MaxSeq`.
  - **NOT** the enclosing segment's `MaxSeq` after a mid-segment cut (that would skip the
    segment's un-included tail blocks — `blockOverlapsSeq` drops `MaxSeq <= afterSeq` on the
    next page, `:185`, losing the band forever).
- **Always admit ≥1 unit per page.** If the first matched unit alone exceeds `MaxEntries`,
  include it anyway and set the cursor to its `MaxSeq`. Never return zero units with the cursor
  unadvanced (livelock).
- **Two result fields (§12.2).** `PlanBackfillResult` gains `SealedTipSeq` (the current sealed
  tip, capped by `beforeSeq`) alongside `PlannedThroughSeq` (now the **continuation cursor** =
  truncation boundary when truncated, else the tip). When *not* truncated, the two are equal.
- **Gap-free + progressing proof (state in the issue, §12.1):** blocks within a segment are
  seq-disjoint and monotonic by index (single `nextSeq++` under the writer lock,
  `writer.go:324-331`). So last-included `MaxSeq = X` cleanly separates included from not-yet;
  next page's `afterSeq = X` (exclusive) drops every included block (`MaxSeq <= X`) and the
  next block (`MinSeq > X`) is re-planned first. `X` strictly exceeds the prior cursor whenever
  ≥1 unit was admitted, so the loop advances. Exclusive-`afterSeq` aligns exactly — no
  off-by-one.

**XRPC + lexicon (`internal/xrpcapi/planbackfill.go`, lexicon, codegen).**
- Add required output field `sealedTipSeq` to `planBackfill.json` output; `just lexgen`.
- `planOutput`: populate `sealedTipSeq`; populate `plannedThroughSeq` as the continuation
  cursor.
- `newPlanBackfillHandler`: stop mapping `manifest.ErrPlanTooLarge` to a 400 — it no longer
  arises on the normal path. (Keep the `ErrInvalidPlanRequest → InvalidRequest` mapping.) The
  `PlanTooLarge` lexicon error and `ErrJetstreamPlanBackfill_PlanTooLarge` can be removed
  (nothing returns it); confirm `isPlanTooLarge` in the client planner is deleted in step 10.
- Keep `MaxEntries` config plumbing; it now bounds per-page entry count (operational win) not a
  hard refusal.

**Client planner (`internal/client/planner.go`).** Surface `SealedTipSeq` on the `Plan` (the
loop that consumes it lands in step 10). Remove `ErrPlanTooLarge` + `isPlanTooLarge` (step 10
will rely on pagination, not the sentinel) — coordinate the exact removal with step 10 so the
tree compiles between steps.

**Tests (`internal/manifest/plan_test.go`, `internal/xrpcapi/planbackfill_test.go`).**
- Multi-page truncation: small `MaxEntries`, assert continuation cursor = prior page's last
  unit `MaxSeq`, union of pages folds to ground truth, no skipped block.
- **Mid-segment block-mode cut**: a single block-mode segment whose matched coalesced ranges
  exceed `MaxEntries`; assert the cursor = last included **block range's** `MaxSeq` (strictly
  inside the segment, NOT the segment `MaxSeq`); next page resumes in-segment, no skipped
  block; cursor strictly advances.
- **One-unit-over-cap**: `MaxEntries` below a single block range's entry count → still returns
  that one unit and advances (no zero-progress livelock).
- `sealedTipSeq` correctness under `beforeSeq` cap and under a filter that matches nothing
  (tip still reported).

**Verify.** `just test ./internal/manifest ./internal/xrpcapi`, then
`just test ./internal/oracle`. **Status / notes.** _(unstarted)_

---

### ☐ Step 9 — `subscribe: v2 too-old cursor → HTTP 400` (design §14, D5) — before step 10

**Goal.** `/subscribe-v2` returns a pre-upgrade HTTP 400 with the floor seq when a v2 seq
cursor resolves below the lookback floor. `/subscribe` (v1) keeps silent clamping (legacy
parity). Standalone server change.

**Changes (`internal/subscribe/cursor.go`, `handler.go`, `runtime.go`).**
- `CursorEnv` gains `RejectBelowFloor bool`.
- `ResolveCursor` (`cursor.go:124-130`, seq path): when `RejectBelowFloor` **and**
  `startSeq < floorSeq`, return a typed `ErrCursorTooOld` wrapping requested seq + floor,
  **instead of** clamping. When the flag is unset (v1), keep the clamp verbatim. The timestamp
  path (`:149-155`) keeps clamping under both (v1-style legacy translation); document the
  asymmetry at the clamp site (finding #14) and give v1's clamp a distinct metric label so it
  stays visible.
- `ErrCursorTooOld.Error()` must include the floor seq (machine- + human-readable), e.g.
  `"cursor 1000 below lookback floor 1500; re-backfill from your last seq"` — `handler.go:183-186`
  already maps the error to a 400 with `err.Error()` as the body, so this surfaces the floor.
- `Subscription` gains `RejectCursorBelowFloor bool`; set `true` on the v2 route
  (`runtime.go:420-430`), `false` (default) on v1. Plumb into the `CursorEnv` built at
  `handler.go:177`.

**Tests (`internal/subscribe/cursor_test.go`, `handler_test.go`).**
- v2 below-floor seq → `ErrCursorTooOld` (and the handler returns 400 with the floor in the
  body); v1 below-floor seq → still clamps (no error). The §16 "Stale-cursor signal" oracle
  test (step 11) is the end-to-end version.
**Verify.** `just test ./internal/subscribe`. **Status / notes.** _(unstarted)_

---

### ☐ Step 10 — `client: paginate backfill, delete the cutover buffer` (design §11, §13, §R4, §R8)

**Goal.** Rewrite `runBackfillThenLive` as the pagination loop; delete the cutover buffer
entirely; pin `beforeSeq = S`; carry the §R4 snapshot across pages; handle the §14 400 by
re-entering the loop. Depends on steps 2, 3, 7, 8, 9.

**Delete (`internal/client/` + root).**
- `internal/client/livesink.go` (`liveSink`, `flipAndDrain`, `onLive`), the `Buffer` interface
  + `LiveFrame` type (`engine.go:14-31`), the `liveRewindMargin` const (`:33-37`), the
  concurrent live-tail-during-download goroutine (`:477-480`), and the
  `backfillCoveredNothing`/dedup-floor special-casing (already collapsed in step 7).
- Root package: `buffer.go`, `buffer_mem.go`, `buffer_file.go`, the public `LiveBuffer`
  interface + `LiveFrame`, `NewMemLiveBuffer`/`NewFileLiveBuffer`, `WithLiveBuffer`
  (`options.go:185-192`), and `bufferAdapter` (`engine.go:221-224`). Update `doc.go` examples.

**Rewrite `runBackfillThenLive` (the §11 loop, with §R4 + §14 corrections).**
```
// page 1: learn S and the snapshot, co-atomically
p := plan(afterSeq=cursor, wantDidTombstones=true)        // cursor = request.AfterSeq (0 = full)
S := p.SealedTipSeq                                        // PIN this for the whole backfill
snap := p.DIDTombstones                                   // held for the whole backfill
// fail closed if snap missing (§R6.6)
download+emit p.Segments  (filter = matcher AND DID-only snapshot suppression, range (afterSeq,S])
cursor = p.PlannedThroughSeq
for cursor < S {
    p = plan(afterSeq=cursor, beforeSeq=S)                // wantDidTombstones=false; beforeSeq PINNED to S
    download+emit p.Segments
    cursor = p.PlannedThroughSeq
}
// done: every sealed segment in (request.AfterSeq, S] consumed
subscribe(cursor=S)                                       // connect ONCE; may clamp → §14 400
```
- **Done predicate**: `plannedThroughSeq >= sealedTipSeq` (here pinned `S`). No boolean, no
  empty-segment inference (sparse filters can match zero segments yet have data above).
- **Connect `/subscribe` at `cursor = S`.** Replay is inclusive + the consumer dedups by seq,
  so the seam is at-least-once with **no rewind margin** (delete `liveRewindMargin`). Segments
  sealed *during* backfill are picked up by cold replay (`WalkFromCursor` re-reads the
  manifest at connect).
- **§14 400 handling.** The terminal `/subscribe-v2` connect must treat an HTTP 400 "too old"
  as "re-enter the pagination loop from `Batch.LastCursor()`", NOT a fatal abort and NOT
  generic reconnect churn. A pre-upgrade 400 arrives synchronously at connect, before events
  flow, so it is trivially distinguishable from live-tail disconnects
  (`live.go:154-182`). **Bound the re-backfill cycles** and assert the cursor advances
  monotonically across them (anti-ping-pong). This closes both the fell-off-live and
  terminal-hop cases with one recovery path.
- `runBackfillOnly`: page until `plannedThroughSeq >= sealedTipSeq`, then return (no
  `/subscribe`).
- `Planner.Plan` stays a single-call wrapper; the loop lives in the engine.
- **Resumability bonus**: a crashed backfill resumes from `Batch.LastCursor()` instead of
  restarting (fixes the "backfill not resumable in v1" limitation).

**Live consumer wiring.** The steady-state `liveConsumer` already dedups by seq
(`live.go` `lastSeq`, now plain `uint64` after step 7). The cutover handoff seeds the wire
cursor = `S`; the dedup floor = `S` (drops the at-least-once re-delivery of `S`). With 1-based
seqs and the bufferless model, no `gt.Option` remains on this path except the live-only
from-tip sentinel (step 7).

**Observability (the §8 still-open item, decide here).** Expose `sealedTipSeq -
plannedThroughSeq` (residual gap) and a per-backfill page counter. The client library has no
Prometheus registry; surface via a `Stats` accessor or progress callback (decide in the
issue — lean toward a lightweight `Stats()` accessor on the engine/Batch). The §14 400 already
carries the floor seq.

**Tests.** Covered end-to-end by step 11; add client-unit tests for the loop's done-predicate,
the pinned-`beforeSeq` range, and the 400-driven re-backfill (bounded, monotonic).
**Verify.** `just test ./internal/client`, then the oracle recipes. **Status / notes.** _(unstarted)_

---

### ☐ Step 11 — `oracle: Part B scenarios` (design §16)

**Goal.** Exercise pagination + the bufferless handoff end-to-end. New mutants per §16.

**Tests (`internal/oracle/`).**
- **Multi-page backfill correctness** (small `MaxEntries`): union of pages folds to ground
  truth; continuation cursor = prior page's `plannedThroughSeq`; no row skipped at a page
  boundary.
- **Mid-segment truncation**: continuation cursor = last included block range's `MaxSeq`
  (inside the segment); next page emits the un-included tail blocks; cursor strictly advances;
  one-unit-over-cap case advances.
- **Mid-download seal**: seal segments between pages; assert page k+1 picks them up (no client
  buffer).
- **Caught-up handoff**: client connects `/subscribe` exactly when
  `plannedThroughSeq >= sealedTipSeq`. The connect cursor **MAY** be below the lookback floor
  (slow handoff) → assert §14 400 fires and the client re-enters pagination; when in-window,
  assert a clean connect with no re-backfill. **Do NOT** assert "≥ floor / never clamps" (the
  corrected finding-#2 claim).
- **Stale-cursor signal**: tiny `--cursor-lookback`, connect below the floor → explicit "too
  old" 400, not a silently truncated stream (regression test for the §10.1 bug).
- **Fell-off-live recovery**: force the consumer below the floor mid-stream → re-enters the
  backfill loop and re-converges.
- **Exhaust-sealed termination**: ingest paused, loop pages until
  `plannedThroughSeq == sealedTipSeq` then connects; resume ingest → just-sealed segments
  arrive via cold replay (the §14.1 backstop), losslessly.
- **Sustained-ingest convergence**: moderate continuous ingest (below bulk-download
  throughput) → loop reaches the tip and hands off; residual-gap metric observable.

**Mutants (§16).** off-by-one continuation cursor that skips a page boundary; a below-floor
handoff NOT surfaced as a §14 400 (silent clamp re-introduced); client treating the §14 400 as
a fatal abort; §12.1 mid-segment cut reporting the enclosing segment `MaxSeq`; truncation
returning zero units with the cursor unadvanced. Wire each to the killing test above. Re-run
`just mutation-campaign`; refresh `RESULTS.md` + `baseline.json`.

**Verify.** `just test ./internal/oracle`, `just test-long ./internal/oracle`, `just oracle`,
`just oracle-sweep`, `just mutation-campaign`. **Status / notes.** _(unstarted)_

---

### ☐ Step 12 — `docs: rewrite for the relaxed cooperative contract` (design §7, §R1/§R2)

**Goal.** Document the new contract: at-least-once, no-silent-loss, cooperative completeness,
bounded incompleteness; the paginated loop; 1-based seqs; overlay removed.

**Changes.**
- `docs/README.md`: rewrite §3.3 to drop the `getTombstones` overlay subsection (`:352-394`)
  while keeping the compaction narrative — the in-memory tombstone set is now
  compaction-internal with no read-time exposure. Rewrite the "putting it all together" client
  flow (`:406-417`) to the loop model: `planBackfill (page 1: learn S + DID-tombstone snapshot)
  → page until plannedThroughSeq ≥ sealedTipSeq → connect /subscribe at S`. No overlay download,
  no record suppression; the only client-side suppression is the bounded DID-only start-snapshot
  over `(W, S]` (the §R5 "bounded suppression, not zero suppression" wording). Update §4.4
  (`:558-568`) to "account + identity (+ sync) always delivered on v1 and v2"; delete the
  "tombstone folding happens before the delivery filter" justification. Update `:58`
  (1-based seqs) and `:160` (overlay-at-delivery-time). Add an explicit **eventual
  consistency** statement (§R1/§R2): backfill is at-least-once and converges under fold;
  consumers must apply deletes/updates idempotently; completeness is a joint property of
  server + folding client + the markers the consumer subscribes to.
- `README.md`: scrub any overlay/`getTombstones`/suppression mention.
- `specs/oracle.md`: point-in-time → fold-convergence + at-least-once + the §R7 tests.
- `doc.go` (root): the public-contract obligation (#15) — show the fold pattern explicitly so
  users don't assume point-in-time correctness; document the §14 400 + re-backfill behavior and
  resumability via `Batch.LastCursor()`.
- Correct the stale `specs/notes/2026-05-27-cursor-replay-design.md:94-95` "v2 seq cursors have
  no window cap" claim (v2 now rejects too-old with a 400; v1 still clamps).
- Lexicon docs / generated index: ensure `getTombstones` is gone; `planBackfill` description
  updated (it now paginates and optionally returns `didTombstones` + `sealedTipSeq`).

**Verify.** Manual read-through; `grep` for stale overlay/0-based/point-in-time language.
**Status / notes.** _(unstarted)_

---

## 4. Cross-cutting risks & how the plan retires each

| Risk | Where addressed | Backstop |
|---|---|---|
| Collection-filtered consumer keeps a deleted account's records forever (finding #1) | Step 3 (§R4 snapshot) | Step 6 eviction-interleaving test gates step 3 |
| Snapshot/`S` reordered by a careless refactor → race re-opens (§R6.2) | Step 3 (snapshot-before-first-fetch invariant) | Step 6 snapshot-ordering test |
| Empty snapshot indistinguishable from fetch failure (§R6.6) | Step 3 fail-closed on missing field | Crash over corruption (CLAUDE.md) |
| seq-0 swallow on from-empty handoff (#112) | Step 7 (1-based seqs make it structurally impossible) | Re-pointed regression test |
| Mid-segment truncation skips the segment's tail blocks (§12.1) | Step 8 cursor = last **unit** `MaxSeq` | Step 11 mid-segment-truncation test + mutant |
| Zero-progress livelock at the page cap | Step 8 always-admit-≥1-unit | Step 11 one-unit-over-cap test + mutant |
| Slow handoff: connect cursor ages below floor → silent gap (#2) | Step 9 (§14 400) + step 10 re-backfill | Step 11 caught-up-handoff + stale-cursor tests + mutants |
| Pathological 400 ping-pong | Step 10 bounded cycles + monotonic-cursor assert | Step 11 fell-off-live recovery |
| v1 silent clamp regressions (legacy, intentional) | Step 9 distinct metric label + comment | finding #14 note |
| Replication consumes the overlay | Step 4 audit (design §9) — replica tails extended `/subscribe`, writes raw rows | re-verify before delete |
| Status page surfaces overlay health | Step 4 audit (`internal/status`, design §9) | grep gate |

---

## 5. Open items to resolve **during** implementation (not blockers)

- **Step 3 vs step 8 snapshot-range seam.** Pre-step-8 the snapshot range is
  `(afterSeq, plannedThroughSeq]`; step 8 must re-pin it to `(afterSeq, sealedTipSeq]` and
  step 10 must pin `beforeSeq = S` across pages. Tracked as an explicit cross-reference in the
  step 3, 8, and 10 issues so the range stays correct as the fields split.
- **Step 10 client telemetry mechanism** (Stats accessor vs progress callback) — the §8
  still-open item; decide in the step-10 issue. Lean: lightweight `Stats()` on the engine.
- **`tombstone_set_bytes` gauge ownership** (step 5) — confirm it is a compaction metric before
  keeping the `ApproxBytes`/`bytes` accounting.
- **`PlanTooLarge` lexicon error removal** (step 8) — confirm no consumer expects it before
  deleting the error from the lexicon + generated bindings.

---

## 6. Checklist (update as we land)

- [x] 1. deliver #account/#identity/#sync on v1+v2 (#171)
- [x] 2. remove client tombstone suppression (#172)
- [x] 7. seqs start at 1 (+ collapse presence machinery) (#173)
- [x] 6. oracle fold-convergence + DID-tombstone delivery tests (gates 3) (#174)
- [x] 3. backfill DID-tombstone start-snapshot (fail-closed, ordering invariant) (#175)
- [ ] 4. remove getTombstones overlay endpoint (gated on 3)
- [ ] 5. prune overlay-only tombstone API (keep SnapshotRange)
- [ ] 8. paginate planBackfill (+ sealedTipSeq, per-unit truncation)
- [ ] 9. /subscribe-v2 too-old cursor → HTTP 400 (v1 unchanged)
- [ ] 10. client pagination loop + delete cutover buffer + 400 re-backfill
- [ ] 11. Part B oracle scenarios + mutants
- [ ] 12. docs rewrite (relaxed cooperative contract)

Final gates before calling the effort done: `just lint test`, `just test-long ./internal/oracle`,
`just oracle`, `just oracle-sweep`, `just mutation-campaign` (scorecard re-reviewed, not STALE).
