# Implementation plan: drop client tombstones + paginated bufferless cutover

Date: 2026-06-28
Branch: `tombstone-query-plan-refactor` (work continues here)
Status: **implementation in progress** (steps 1, 2, 7, 6, 3, 4, 5, 8, 9, and 10 landed; steps 11 and 12 remain).
**NOTE (RESOLVED):** step 9 flipped `/subscribe-v2` to reject too-old cursors with a 400, which
made the oracle's `steady-state-client-backfill` lifecycle check known-red until step 10. **Step 10
(#181) landed the bufferless pagination loop + client-side 400 re-backfill + `liveRewindMargin`
removal, and the oracle is green again** (`just test`, `just test-long ./internal/oracle`,
`just oracle` 20s stress all pass).
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
- **DID-level tombstones carry an empty collection** and so are not indexed under a real
  collection. **The §R4-revised fix (step 3, shipped):** the seal/rewrite index tags each
  marker-bearing block with a reserved sentinel collection (`$account`/`$identity`/`$sync`,
  `segment/sentinel.go`), and `collectionIDsForSegment` admits those sentinels under every
  collection filter, so the markers are selected and ride inline through `getBlock` — the same
  path record-level deletes take. (`segment/event.go`, `segment/seal.go`, `segment/rewrite.go`,
  `selectPlanBlocks`/`collectionIDsForSegment` in `internal/manifest/plan.go`.) The original
  §R4 start-snapshot that this bullet used to motivate was reverted.
- **`tombstone.Set.SnapshotRange` / `Snapshot` are NOT used by the read path** anymore (the
  snapshot was reverted; the sentinel index needs no tombstone state on reads). Compaction folds
  on-disk via `FoldRange`, so post-overlay-deletion these become removable (§8 step 5).
- **`Snapshot.ShouldDrop`** is compaction-internal (the `decide` callback in
  `compact_deletes.go`); the read path no longer calls it. No DID-only read-path variant is
  needed — there is no read-path suppression.
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

- **TDD, gated.** Where the design says a fix is gated by a test (notably §R7's
  collection-filtered fold-convergence gate gating step 3, and the §16 mutants), the test is
  written **first** and must **fail** without the change. (Step 3's gate is
  `TestFoldConvergence_CollectionFilteredDIDTombstoneGap`; it failed until the sentinel index
  landed.)
- **One issue per step.** Title `subsystem: …` per AGENTS.md. Comment on start / approach
  change / finding. Close via `Closes #N`.
- **No tech debt, cut deep.** Delete dead code outright (nothing is deployed, no consumers —
  per the design's deployment-context note). No compat shims, no renamed `_unused` vars, no
  "removed" comments.
- **On-disk segment format (schema) is frozen.** None of these steps add a footer section, bump
  a format version, or change framing/layout. (The wire/lexicon and the in-memory/Pebble
  `seq/next` *seed* change.) **Caveat (step 3):** marker-bearing segments now carry reserved
  sentinel collection names (`$account`/`$identity`/`$sync`) in their footer string table and the
  sentinel id in the block's collection set — same schema, but new *content* that is load-bearing
  once sealed (see §R4-revised). Event payload bytes and seq envelopes are unchanged.
- **Run the right tests after each correctness-touching step** (3, 6, 7, 8, 10, 11):
  `just test ./internal/oracle`, `just test-long ./internal/oracle`, and the
  `just oracle` / `just oracle-sweep` recipes; plus `just lint test` for the whole tree.
- **Crash loud, never corrupt.** Missing DID-marker coverage is a correctness bug, not a
  runtime fail-closed path: sentinel indexing (seal/rewrite) and planner sentinel admission are
  locked by tests + a §R7 mutant and must fail loudly during verification (§R4-revised — there is
  no snapshot to fail closed on). Invalid *external* data (relay/firehose/backfill rows) is
  dropped-with-metric, never fatal.

---

## 2. Dependency graph (from design §8, restated)

```
Part A:  [6 oracle tests] ──gates──> [3 sentinel-index fix] ──> [4 remove overlay] ──> [5 prune tombstone API]
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

> ⚠ SUPERSEDED by the §R4-revised mechanism (see this step's ✅ Status/notes below and the
> Deferred section). The snapshot-flavored bullets that follow (start-snapshot suppression,
> snapshot-at-seam mutant, "snapshot suppresses only `seq < D`", snapshot-before-first-fetch
> ordering) describe the **reverted** design and were NOT the tests shipped. What shipped:
> `CheckFoldConvergence` + the collection-filtered gate `TestFoldConvergence_*DIDTombstoneGap`
> and the **sentinel-index-reverting** mutant. The snapshot-before-first-fetch ordering test is
> moot — there is no snapshot. The original bullets are retained below only as the reasoning trail.

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
add a mutant that **reverts the sentinel index** (seal: skip `indexEventCollection`'s sentinel
branch; or planner: drop the `IsDIDMarkerSentinelCollection` admit) — killed by the
collection-filtered fold-convergence gate. Keep `m022`, `m025`, `m027`, etc. Refresh
`testing/mutation/baseline.json`; a STALE scorecard is expected until re-reviewed.

**Verify.** `just test ./internal/oracle` (new fold-convergence passes on the
eventually-consistent path), `just test-long ./internal/oracle`. The collection-filtered gate
test **fails here** (the gap is unclosed until step 3's sentinel index) — that failure is the
gate for step 3.
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
skip. Verified: `just lint` (0 issues), `just test` (1675), `just test-long ./internal/oracle`,
explicit `TestOracle_DefaultLifecycle` (synctest, fast + swarm), `TestOracle_SameSeedTraceDeterminism`
(20 deterministic sections identical), and `just oracle` (20s stress) all green.

**Deferred (recorded, per §R7 / dependency order):**
- *Eviction-interleaving **between pages*** needs the pagination loop (step 10); the single-shot
  filtered gate above gates step 3 at the right granularity. (The snapshot-before-first-fetch
  ordering test is **moot** under the revised step 3 — there is no snapshot fetch.) The
  mid-pagination convergence sharpening lands in step 11.
- *Mutation campaign* (retire `m020`/`m021`/`m023`, add a **sentinel-index-reverting** mutant):
  the overlay-format mutants reference `internal/overlay` (deleted in step 4) — refresh **after**
  step 4 so the mutants compile, alongside the step-11 Part-B mutants.
- **Step 12 doc debt found:** `options.go:100-102` (`WithCollections` doc) still claims
  "records for a deleted account are correctly suppressed — you just don't see the Account event
  itself" — false under the dropped suppression; coupled to the §4.4 narrative, fold into step 12.

---

### ✅ Step 3 — `segment+manifest: DID-marker sentinel collections` (§R4 REVISED, #175) — finding-#1 fix

> **2026-06-29: approach changed.** The original step 3 (a `wantDidTombstones`
> start-snapshot piggybacked on `planBackfill`) was implemented (commit 154eee3) and then
> **reverted** in favor of an in-archive **reserved DID-marker sentinel collection** index.
> The snapshot text that was here is preserved in the design doc's "§R4 (ORIGINAL,
> SUPERSEDED)" as the reasoning trail. This entry records the mechanism that shipped.

**Goal.** Close the collection-filtered DID-tombstone gap **where it originates — the segment
index** — so DID-level markers (#account/#identity/#sync) become selectable by a
collection-filtered plan and ride inline through `getBlock`, the same path record-level deletes
already take. No wire field, no client snapshot, no `tombstone.Set` on the read path, no
fail-closed gate, no snapshot-before-first-fetch ordering invariant, no race proof.

**Mechanism (shipped).**
- **Reserved sentinels (`segment/sentinel.go`).** `SentinelCollectionAccount = "$account"`,
  `…Identity = "$identity"`, `…Sync = "$sync"`. `didMarkerSentinel(Kind)` maps the three
  DID-level marker kinds to their name (others → `""`); `IsDIDMarkerSentinelCollection(name)`
  is the planner's predicate. The `$` prefix makes them invalid NSIDs, and planBackfill only
  admits real NSIDs / NSID-authority wildcard prefixes, so **no client request can name or
  prefix-match a sentinel** — locked by `TestSentinelCollectionsAreInvalidNSIDs`. These strings
  are written into sealed footers and are therefore on-disk format (load-bearing once sealed).
- **Index at seal AND rewrite (one shared helper).** Extracted
  `blockWalkResult.internCollection` + `indexEventCollection` (in `segment/seal.go`), called by
  both `walkActiveFrames` (seal) and `accumulateRewriteBlock` (compaction rewrite) so the two
  index paths cannot drift. For a marker-bearing block, the sentinel id is added to the block's
  collection set; the marker's empty collection is still not interned, and the sentinel does
  **not** increment `collectionEventCounts` (selection hint, not traffic). Rewrite re-derives
  the index, so a marker that survives compaction keeps its sentinel.
- **Admit in the planner (`manifest.collectionIDsForSegment`).** When building the matched
  collection-id set under a collection filter, always include the segment's sentinel ids. This
  only widens the set (one-sided no-false-negatives contract preserved); the per-block DID bloom
  still narrows by DID, so a collection+DID-filtered request pulls only marker blocks that may
  contain the requested DID.
- **Client: nothing new.** The exact `Matcher` already delivers the markers under a collection
  filter (step 1, `!Kind.IsCommit()` bypass). The marker arrives inline in seq order, the
  consumer folds it, and reactivation is handled for free (no synthesized event, no suppression
  to mis-scope).

**Race-freedom.** Trivial: `getBlock` reads on-disk truth at fetch time, and the killer marker
is downloaded by the same plan, in seq order, as its victim. There is no second source of truth
to be stale, so the original §R4 eviction-interleaving race does not arise.

**Tests.** `segment/sentinel_test.go` (NSID rejection; seal indexes per-kind without inflating
counts; coalesced marker+real-collection block; rewrite re-indexes); `internal/manifest/plan_test.go`
(collection-filtered plan selects marker blocks; collection+DID filter narrows by bloom); the
§R7 gate `TestFoldConvergence_CollectionFilteredDIDTombstoneGap` — **skip removed, now PASSES**
via the inline path, with `serveArchive` wiring **no** `tombstone.Set`.

**Verify.** `just lint`, `just test`, `just test-long ./internal/oracle`,
`TestOracle_DefaultLifecycle`, `just oracle`, `just fuzz` on `segment`. **Status / notes.**
✅ **Done** (#175), replacing the reverted snapshot. **§3↔§8 seam is GONE:** the sentinel index
is independent of `plannedThroughSeq`/`sealedTipSeq`, so step 8's pagination needs no re-pinning
of any snapshot range (there is no range). One fewer cross-step coupling.

---

### ☐ Step 4 — `server: remove getTombstones overlay endpoint` (design §4.2) — gated on step 3

**Goal.** Delete the overlay machinery now that the §R4-revised sentinel index replaces its
DID-tombstone coverage. **Do not start before step 3 lands.**

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
outside the design notes; `just lint test`. **Status / notes.** ✅ **Done** (#177). Deleted the
`internal/overlay` package, `xrpcapi/gettombstones.go`+test + the `OverlaySource` interface +
`Config.Overlay` + the route registration, `jetstreamd/overlay_source.go`, `obs/overlay.go`, and
the `getTombstones.json` lexicon + regenerated stub (`just lexgen`). `jetstreamd/runtime.go`:
removed the `overlay` import, `overlayRebuildInterval` const, `overlayCache` field +
construction + `Rebuild()` in `onCompactionPass` + the `Overlay:` wiring + the ticker goroutine +
the interval-resolution block in `Run`; `options.go` dropped `OverlayRebuildInterval` (no CLI
flag existed) and `runtime_test.go` dropped its negative-validation test. Oracle scaffolding:
dropped `overlay.WarmEncoder()`+import (`main_test.go`) and `OverlayRebuildInterval`
(`harness_test.go`). **Audit (verified before deleting):** neither `internal/status` nor any
replication package consumes the overlay — no live consumer outside server wiring + test
scaffolding (design §9 risk cleared). **Doc sweep (folded in):** corrected now-false
`getTombstones` mentions (`client/errors.go`, `client/engine.go`, root `options.go`), stale
reverted-snapshot comments in `foldconvergence_gate_test.go`, and overlay-mechanism comments in
`harness_test.go`/`restart_shape_d_test.go`/`restart_chain_coordinator_test.go`/`synctest_test.go`.
**Deferred to step 5 (correctly):** the "read-path overlay" wording in `tombstone.go:3,49` and
`compact_deletes.go:152`, plus the `tombstone.Set.SnapshotRange`/`Snapshot` methods —
`overlay_source.go` was their last caller, now gone, so step 5 deletes the methods. Net −1407
lines. Verified: `just lint` (0), `just test` (1659), `just test-long ./internal/oracle` (156),
`just oracle` (20s stress) all green. `getTombstones`/`GetTombstones` grep is clean; the
`overlay` substring still matches the known step-5 Go comment/API leftovers listed above
(`tombstone.go:3,49`, `compact_deletes.go:152`).

---

### ✅ Step 5 — `tombstone: prune overlay-only API` (design §4.3) — gated on step 4

**Goal.** Remove `tombstone.Set` members only the overlay used; keep everything compaction needs
(`Observe`, `Evict`, `FoldRange`, `Snapshot.ShouldDrop`, the compaction `decide` path).
**2026-06-29 update:** with the snapshot reverted, the `Set.SnapshotRange` method (and the
`Set.Snapshot` method, which wraps it) is **overlay-only again** — step 3's sentinel index does
not use it, and compaction folds on-disk via `FoldRange` — so once step 4 deletes the overlay,
both `Set` methods become removable. Keep the `Snapshot` **type** and its `ShouldDrop` method
(compaction's `decide` path builds a `Snapshot` via `FoldRange` and calls `ShouldDrop`); only the
two `Set` methods that produced snapshots for the overlay go away. Re-grep before deleting.

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
**Verify.** `just test ./internal/tombstone ./internal/ingest/...`.

**Status / notes.** ✅ **Done** (#178). The pre-deletion re-grep changed the prune set materially
from the speculative bullets above — recorded here as the reasoning trail:

- **Removed (production-dead, overlay-only):** `Set.SnapshotRange` (the overlay's bounded-window
  readout) and `Set.Snapshot(maxSeq)` (its wrapper); `Set.Dirty()` + the `dirty atomic.Uint64`
  field + the three `s.dirty.Add(1)` calls (Observe/Evict/Replace) + the now-unused `sync/atomic`
  import. The overlay cache was the only `Dirty()` reader and the only consumer of bounded
  snapshots; #177 deleted it.
- **KEPT — the audit overturned the "remove if unused" bullets (live compaction callers):**
  - `ApproxBytes` + `bytes` accounting + `entryOverheadBytes`/`recordEntryBytes`/`didEntryBytes`
    → feeds the **compaction** gauge `jetstream_compaction_tombstone_set_bytes`
    (`metrics.go:152,158`), NOT overlay-only. Stays.
  - single-event `Fold` → `compact_deletes.go:538,552`. Stays.
  - `Snapshot.Merge` → `compact_deletes.go:309,543,556`. Stays.
- **Deviation from "delete both `Set` methods":** the external `orchestrator` test
  (`TestRebuildLiveTombstones_BoundedByWatermark`) and the internal tombstone property tests must
  read Set contents to assert rebuild==incremental. Rather than drop a valuable property test or
  reach into unexported state cross-package, the two bounded variants were **collapsed into a
  single no-arg `Set.Snapshot()`** returning the full contents (existing public `Snapshot` type).
  Overlay-coupled windowing gone; one honest inspection accessor remains.
- **Post-overlay invariant confirmed:** production never reads the in-memory `Set`'s *contents* —
  only `Len()` (compaction-cap trigger, `consumer.go:508`) and `ApproxBytes()` (the size gauge).
- **Doc sweep:** package doc (`tombstone.go:1-5`) dropped "(and, later, the read-path overlay)";
  `compact_deletes.go:152-153` comment rewritten ("used only for the compaction-cap trigger
  (Len) and the size gauge (ApproxBytes)"); two stale `SnapshotRange` comment refs reworded.
- **Tests:** deleted `TestSetDirtyChangesOnMutation` and `TestSnapshotRangeFiltersLowAndHighBounds`
  (subjects removed); repointed all `Snapshot(N)`/`SnapshotRange(0,max)` inspection calls.
- **Verify:** `just test ./internal/tombstone ./internal/ingest/...` (436), full `just lint` (0)
  + `just test` (1657), `just test-long ./internal/oracle` (156), `just oracle` (21s) — all green.

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
`just test ./internal/oracle`. **Status / notes.** ✅ **Done** (#179).

- **Planner (`internal/manifest/plan.go`).** Per-unit truncation: the segment loop
  admits one work unit at a time (a whole-segment entry, or — in block mode — one coalesced
  block range) and stops at the cap, so truncation can land mid-segment. `atCap()` checks
  `Entries >= MaxEntries` **before** admitting the next unit, so the first unit of a page is
  always admitted (Entries==0) even at `MaxEntries=1` → no zero-progress livelock. Continuation
  cursor (`PlannedThroughSeq` on truncation) = `seg.Blocks[range.Last].MaxSeq` (block mode) or
  `seg.Header.MaxSeq` (whole-segment mode) of the **last included unit** — never the enclosing
  segment's `MaxSeq` after a mid-segment cut, which would skip the un-included tail blocks
  forever. Added `SealedTipSeq` (= old `PlannedThroughSeq`: sealed tip capped by `beforeSeq`,
  match-stable); `PlannedThroughSeq` now defaults to it and is overwritten only on truncation.
  Per-page `Stats` (`SegmentsMatched`/`BlocksMatched`/`Entries`) count only what's in the page.
- **Gap-free proof (locked by tests).** Blocks within a segment are seq-disjoint and
  index-monotonic (writer assigns seqs under one lock; seal walks frames in ascending offset),
  so a range's `MaxSeq=X` cleanly separates included (`<=X`) from not-yet (`MinSeq>X`); the next
  page's exclusive `afterSeq=X` re-admits exactly the next block, and `X` strictly exceeds the
  prior cursor whenever ≥1 unit was admitted.
- **`ErrPlanTooLarge` removed end-to-end** (zero-tech-debt, nothing returns it post-change): the
  `manifest` error var, the lexicon `PlanTooLarge` error + regenerated
  `ErrJetstreamPlanBackfill_PlanTooLarge` const, the xrpcapi 400 mapping, and the client
  `ErrPlanTooLarge` var + `isPlanTooLarge` helper + the wrapping branch. `ErrInvalidPlanRequest`
  stays for malformed input (negative `MaxEntries`, inverted window, bad threshold). The lexgen
  deletion *forced* the client cleanup to compile — confirming no live consumer remained.
- **Lexicon + codegen.** `planBackfill.json` output gained required `sealedTipSeq`;
  `plannedThroughSeq` re-described as the continuation cursor; `PlanTooLarge` error removed.
  `just lexgen` regenerated `api/jetstream/jetstreamplanbackfill.go`.
- **Wire/client.** `planOutput` populates `sealedTipSeq` (overflow-guarded via `int64FromUint64`);
  client `Plan` surfaces `SealedTipSeq`, and `planFromOutput` rejects an incoherent
  `plannedThroughSeq > sealedTipSeq` (and negative `sealedTipSeq`) so a buggy server can't make
  the step-10 loop livelock or skip the tail. CLI `--plan-max-entries` usage reworded
  (per-page cap → paginate, not refuse).
- **Intermediate state.** The single-shot client path (`runBackfillThenLive`) is untouched and
  keeps using `PlannedThroughSeq`; with the default `MaxEntries=100000` nothing truncates, so
  `PlannedThroughSeq == SealedTipSeq` and the oracle/engine stay correct until step 10 adds the
  pagination loop. Tree compiles and all tests pass throughout.
- **Tests.** manifest: `TestPlanBackfill_TruncatesAtUnitBoundary` (block-range pagination, union
  covers all, no skip/dup), `…_MidSegmentCutCursorIsBlockMaxSeq` (cursor strictly inside the
  segment; full page-walk delivers every matching block once and strictly advances),
  `…_OneUnitOverCapStillAdvances`, `…_TruncatesAtSegmentBoundary`, plus `SealedTipSeq` assertions
  on the `beforeSeq`-cap and match-nothing cases; removed `…_PlanTooLargeDoesNotReturnTruncatedResult`.
  xrpcapi: `…_TruncatesAndPaginatesOverWire` + `…_ZeroMaxEntriesDisablesPagination` (replaced the
  `PlanTooLarge`-400 tests); `sealedTipSeq` on the wire. client: `TestPlanSurfacesContinuationCursorAndTip`,
  `TestPlanRejectsCursorAboveTip`, `TestPlanXRPCErrorIsWrapped` (replaced the `ErrPlanTooLarge`
  tests); all fixtures carry `sealedTipSeq`.
- **Verify.** `just test ./internal/manifest ./internal/xrpcapi ./internal/client` (289), full
  `just lint` (0) + `just test` (1661), `just test-long ./internal/oracle` (156), `just oracle`
  (21s stress) — all green.

**Carried to step 10:** the §12.2 open-item "PlanTooLarge lexicon error removal — confirm no
consumer" is **resolved** (removed; lexgen-forced client cleanup proved no consumer). Step 10's
`runBackfillThenLive` rewrite now consumes `Plan.SealedTipSeq` + the `PlannedThroughSeq`
continuation cursor to drive the pagination loop.

---

### ☐ Step 9 — `subscribe: v2 too-old cursor → HTTP 400` (design §14, D5) — before step 10

**Goal.** `/subscribe-v2` returns a pre-upgrade HTTP 400 with the floor seq when a v2 seq
cursor resolves below the lookback floor. `/subscribe` (v1) keeps silent clamping — **wire
parity with jetstream-legacy** (`bluesky-social/jetstream-legacy`), whose `/subscribe` never
rejects a too-old cursor (a future cursor live-tails, an old cursor replays what's available);
real legacy consumers depend on that contract, so v1 must not start returning a 400. This is
**not** a violation of the no-silent-loss goal: the loss is made operator-visible via a distinct
metric label (finding #14), and the at-least-once/no-silent-loss contract is delivered on the v2
path. Standalone server change.

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
**Verify.** `just test ./internal/subscribe`. **Status / notes.** ✅ **Done** (#180).

- **Resolver (`cursor.go`).** Added `CursorEnv.RejectBelowFloor bool` and the exported
  `ErrCursorTooOld`. In the `ModeReplaySeq` path, when `startSeq < floorSeq` **and**
  `RejectBelowFloor`, return `fmt.Errorf("%w: cursor %d below lookback floor %d; re-backfill
  from your last seq", ErrCursorTooOld, n, floorSeq)` **instead of** clamping. With the flag
  unset (v1) the clamp is byte-for-byte unchanged. The **timestamp path always clamps** under
  both endpoints (legacy v1 timestamp translation contract); the asymmetry is commented at the
  clamp site (finding #14).
- **Handler (`handler.go`).** Added `Subscription.RejectCursorBelowFloor bool`, plumbed into the
  `CursorEnv`. The existing pre-upgrade `if err != nil { http.Error(w, err.Error(), 400) }`
  surfaces the floor seq in the body verbatim — no new mapping. A too-old reject is counted under
  a distinct `too_old` metric label (v1 clamps stay under `clamped`, finding #14).
- **Route (`runtime.go`).** `RejectCursorBelowFloor: true` on `/subscribe-v2` only; v1 default
  false. Metric Help updated to list `too_old`.
- **Tests.** `cursor_test.go`: `…SeqBelowFloorRejectedWhenRejectBelowFloor` (typed error carries
  both seqs), `…SeqBelowFloorClampsWhenV1` (parity), `…TimeUSBelowFloorClampsEvenWhenRejectBelowFloor`
  (asymmetry). `handler_integration_test.go`: `TestHandler_V2TooOldCursorReturns400` (400 + floor
  in body), `TestHandler_V1TooOldCursorClampsAndUpgrades` (v1 clamps + upgrades), via a
  `newCursorReplaySubscription` helper over a recent single-segment archive.
- **Verify.** `just test ./internal/subscribe` (232) + `just lint` (0) green.

> ✅ **RESOLVED by step 10 (#181).** The oracle is green again — step 10 deleted `liveRewindMargin`
> (connect exactly at the sealed tip) and handles the §14 400 by re-entering the pagination loop.
> The historical root-cause analysis below is retained as the reasoning trail.
>
> ⚠ **KNOWN-RED, INTENTIONAL until step 10** (user-approved 2026-06-29). Flipping the v2 route to
> `RejectCursorBelowFloor: true` makes `just test`'s `TestOracle_DefaultLifecycle /
> steady-state-client-backfill` fail. **Root cause** (diagnosed, not a bug in this step): the
> oracle drives the *real* pre-step-10 client, which connects its live tail at
> `plannedThroughSeq − liveRewindMargin` and **relies on the old silent clamp** when that dips
> below the floor. In the oracle the floor is artificially high — all simulator events are
> 2023-dated while `LookbackFloor` compares real wall-clock `now()`, so every sealed segment is
> "older than the floor" and the floor collapses to the *last* segment's MinSeq — so the rewind
> margin readily dips below it. The new v2 400 turns that silent clamp into fatal reconnect-churn.
> **Step 10 fixes it** (its job per design §14 client-side): it (a) deletes `liveRewindMargin` and
> connects exactly at `S = sealedTip` (≥ floor, no dip in the common case), and (b) handles a
> genuine below-floor 400 by re-entering the pagination loop from `Batch.LastCursor()`. The
> server-side oracle checks (`after-bootstrap`, `after-merge`) still PASS; only the real-client
> backfill→live handoff churns. Step 9's own verify scope (`./internal/subscribe`) is green; the
> plan's oracle-gating list (global rule) is steps 3/6/7/8/10/11 — step 9 is intentionally not
> oracle-gated. **Do not "fix" this in production to satisfy the oracle — it is the step-10 cutover
> rewrite's responsibility.**

---

### ☐ Step 10 — `client: paginate backfill, delete the cutover buffer` (design §11, §13, §R4, §R8)

**Goal.** Rewrite `runBackfillThenLive` as the pagination loop; delete the cutover buffer
entirely; pin `beforeSeq = S`; rely on the §R4-revised sentinel index for DID-level marker
coverage (no snapshot, no client suppression, no `wantDidTombstones`); handle the §14 400 by
re-entering the loop. Depends on steps 2, 3, 7, 8, 9.

**Delete (`internal/client/` + root).**
- `internal/client/livesink.go` (`liveSink`, `flipAndDrain`, `onLive`), the `Buffer` interface
  + `LiveFrame` type (`engine.go:14-31`), the `liveRewindMargin` const (`:33-37`), the
  concurrent live-tail-during-download goroutine (`:477-480`), and the
  `backfillCoveredNothing`/dedup-floor special-casing (already collapsed in step 7).
- Root package: `buffer.go`, `buffer_mem.go`, `buffer_file.go`, the public `LiveBuffer`
  interface + `LiveFrame`, `NewMemLiveBuffer`/`NewFileLiveBuffer`, `WithLiveBuffer`
  (`options.go:185-192`), and `bufferAdapter` (`engine.go:221-224`). Update `doc.go` examples.

**Rewrite `runBackfillThenLive` (the §11 loop, with §R4-revised + §14 corrections).**
```
// page 1: learn S, the sealed upper bound, and PIN it for the whole backfill
p := plan(afterSeq=cursor)                                // cursor = request.AfterSeq (0 = full)
S := p.SealedTipSeq                                       // PIN this for the whole backfill
download+emit p.Segments  (filter = matcher only, range (afterSeq,S])  // DID markers ride inline via sentinels
cursor = p.PlannedThroughSeq
for cursor < S {
    p = plan(afterSeq=cursor, beforeSeq=S)                // beforeSeq PINNED to S
    download+emit p.Segments
    cursor = p.PlannedThroughSeq
}
// done: every sealed segment in (request.AfterSeq, S] consumed
subscribe(cursor=S)                                       // connect ONCE; may clamp → §14 400
```
There is no DID-tombstone snapshot, no `wantDidTombstones` plan flag, no fail-closed gate, and
no client-side suppression: DID-level markers (`#account`/`#identity`/`#sync`) are selected
inline by every page whose plan touches their blocks (via the §R4-revised sentinel index) and
folded by the consumer with no special handling — the same download path a record-level delete
takes.
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
**Verify.** `just test ./internal/client`, then the oracle recipes. **Status / notes.**
✅ **Done** (#181).

- **Engine (`internal/client/engine.go`).** Both archive paths now share
  `sweepSealedArchive(ctx, dl, emit, backfillStopped, startCursor)`: it pages `planBackfill` from
  `startCursor`, **pins `beforeSeq = S`** (the page-1 `sealedTipSeq`) for every subsequent page,
  downloads + emits each page in seq order, advances `cursor = plannedThroughSeq`, and returns when
  `cursor >= S` (the unambiguous done predicate — works for a sparse filter that matched zero
  segments in a sub-range, and an empty archive terminates on page 1 at `0 >= 0`). `runBackfillOnly`
  is just the sweep + flush, no websocket. `runBackfillThenLive` is the sweep then a single
  `tailLiveFromCutover` connect at `cursor = S` (dedup floor `S`, **no rewind margin**); the
  consumer's seq dedup makes the seam at-least-once with no gap. Mid-download seals (seqs `> S`) are
  left to `/subscribe`'s cold replay (§14.1), not chased by a moving tip.
- **§14 too-old 400 handling.** `internal/client/live.go`: the dialer maps a pre-upgrade HTTP 400
  whose body contains "cursor too old" to the typed `errLiveCursorTooOld`; `liveConsumer.Run`
  returns it **terminally** (not a reconnect-loop — the floor only advances), and exposes
  `LastSeq()`. The engine catches it in the loop and re-enters pagination from the consumer's last
  durably-processed seq (or `S` if it delivered nothing). Re-backfill cycles are **bounded**
  (`maxRebackfillStalls = 5`) and the resume cursor must **strictly advance** past the cursor the
  prior sweep started from; a non-advancing ping-pong is surfaced as **fatal** (crash-loud, never a
  silent infinite loop). A fresh sweep re-learns the *current* sealed tip (≥ floor), so the
  realistic slow-handoff case converges in one extra cycle.
- **Deleted (bufferless cutover).** `internal/client/livesink.go` (whole file); the engine `Buffer`
  interface + `LiveFrame` type; `liveRewindMargin`; the concurrent live-tail-during-download
  goroutine + `liveWG`/`flipAndDrain`/`sink` machinery. Root package: `buffer.go`, `buffer_mem.go`,
  `buffer_file.go`, `buffer_test.go`, the public `LiveBuffer`/`LiveFrame`, `NewMemLiveBuffer`,
  `NewFileLiveBuffer`, `WithLiveBuffer`, `bufferAdapter`, the `cfg.liveBuffer`/`buf`/`ownBuf` wiring
  (`engine.go`), and the `cmd/client --live-buffer-file` flag. `doc.go` + `options.go` + `client.go`
  docs rewritten to the loop model (no buffer, no suppression, fold-to-converge, transparent
  re-backfill on a too-old cursor).
- **Telemetry (the §8/§10 open item).** Deferred a dedicated `Stats()` accessor: the client library
  has no Prometheus registry and the §14 400 already carries the floor seq for the only
  operationally-interesting event (a fall-behind). Re-evaluate alongside the step-11 residual-gap
  oracle assertion rather than add an unexercised accessor now (kaizen — recorded, not built).
- **Tests.** `internal/client/engine_test.go`: harness reworked to serve **paginated** plan
  responses keyed by `afterSeq` (`planResponder`/`planPageJSON`); removed the buffer-era
  `memBuffer`/`failOnNthAppendBuffer`/`replayErrBuffer` + the two `TestEngineCutover*` sink tests.
  Added `TestEngineMultiPageBackfillCutover` (3-page union folds to ground truth, exactly-once
  across page + cutover seams), `TestEnginePinnedBeforeSeqAcrossPages` (page 2 carries
  `afterSeq=cont, beforeSeq=page-1 tip`), `TestEngineTooOldHandoffReBackfills` (a handoff-sealed
  segment is downloaded by the re-backfill, converges, no skip/dup), `TestEngineTooOldPingPongIsFatal`
  (non-advancing re-backfill is fatal + bounded). `internal/client/live_test.go`:
  `TestLiveConsumerCursorTooOldIsTerminal` (terminal, no reconnect-loop) and the matcher-level
  `TestLiveAccountDeleteDeliveredDespiteCollectionFilter` (replacing the deleted `newLiveSink` test).
- **Verify.** `just test ./internal/client . ./cmd/client` (green); `go test -race` on the client +
  root (clean); full `just lint` (0) + `just test` (1649); `just test-long ./internal/oracle` (156);
  `just oracle` (21s stress). The previously known-red `TestOracle_DefaultLifecycle /
  steady-state-client-backfill` is **green**.

**Carried to step 11:** the multi-page / mid-segment-truncation / mid-download-seal / caught-up-handoff
/ stale-cursor / fell-off-live / exhaust-sealed / sustained-ingest **oracle** scenarios + their
mutants (§16) — the client-unit tests above gate the loop mechanics at the right granularity; the
end-to-end + residual-gap-metric coverage is step 11's job. The §8 client-telemetry decision
(Stats accessor) is folded into that residual-gap work.

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
- **Mid-download seal**: seal segments between pages; their seqs are `> S` (the page-1 sealed
  tip, pinned as `beforeSeq` for the whole loop), so they are **outside** every page's
  `(afterSeq, S]` range and are **not** picked up by page k+1. Assert instead that they are
  delivered by the terminal `/subscribe` cold replay at cutover (`WalkFromCursor` re-reads the
  manifest at connect, §14.1) — losslessly and with no client buffer. (Note: pinning vs floating
  `beforeSeq` is a simplicity/efficiency choice, not a correctness one — a floating upper bound
  would still deliver these seals losslessly, just via page k+1 instead of cold replay — so this
  test asserts *which channel* delivers them under the pinned model, not loss-vs-no-loss.)
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
  flow (`:406-417`) to the loop model: `planBackfill (page 1: learn S)
  → page until plannedThroughSeq ≥ sealedTipSeq → connect /subscribe at S`. No overlay download,
  no record suppression, no DID-tombstone snapshot; DID-level markers are delivered inline
  because the segment index tags marker blocks with reserved sentinel collections that the
  planner always admits under a collection filter (the §R4-revised mechanism). Update §4.4
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
  updated (it now paginates and returns `sealedTipSeq`; there is no `didTombstones` field —
  DID-level markers are covered by the §R4-revised sentinel index, not a plan response field).

**Verify.** Manual read-through; `grep` for stale overlay/0-based/point-in-time language.
**Status / notes.** _(unstarted)_

---

## 4. Cross-cutting risks & how the plan retires each

| Risk | Where addressed | Backstop |
|---|---|---|
| Collection-filtered consumer keeps a deleted account's records forever (finding #1) | Step 3 (§R4-revised sentinel index: seal/rewrite tags marker blocks, planner admits sentinels) | Step 6 collection-filtered fold-convergence gate + sentinel-index unit tests |
| Sentinel not indexed at seal/rewrite, or not admitted by the planner → DID markers omitted from a collection-filtered backfill | Step 3 shared `indexEventCollection` helper + planner sentinel admit | Mutant removing the sentinel branch or planner admit must fail the §R7 fold-convergence gate |
| Reverted snapshot wire/client suppression accidentally reintroduced | Step 3 deletes the snapshot surface; steps 10/12 describe matcher-only inline delivery | Grep for `wantDidTombstones`/`DIDTombstones`/snapshot suppression before completing those steps |
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

- **Step 3 vs step 8 snapshot-range seam — RESOLVED (2026-06-29).** The sentinel-index
  replacement removed the snapshot, so there is no snapshot range to re-pin when `sealedTipSeq`
  lands. Step 8 still adds `sealedTipSeq` for pagination and step 10 must pin `beforeSeq = S`
  across pages, but DID-level marker coverage now comes from the segment/planner sentinel path
  and is independent of `plannedThroughSeq`/`sealedTipSeq` — one fewer cross-step coupling.
- **Step 10 client telemetry mechanism** (Stats accessor vs progress callback) — the §8
  still-open item; decide in the step-10 issue. Lean: lightweight `Stats()` on the engine.
- **`tombstone_set_bytes` gauge ownership** (step 5) — confirm it is a compaction metric before
  keeping the `ApproxBytes`/`bytes` accounting.
- **`PlanTooLarge` lexicon error removal** (step 8) — **RESOLVED (#179):** removed from the
  lexicon + generated bindings + the manifest/xrpcapi/client surface; the lexgen deletion forced
  the client cleanup to compile, proving no remaining consumer.

---

## 6. Checklist (update as we land)

- [x] 1. deliver #account/#identity/#sync on v1+v2 (#171)
- [x] 2. remove client tombstone suppression (#172)
- [x] 7. seqs start at 1 (+ collapse presence machinery) (#173)
- [x] 6. oracle fold-convergence + DID-tombstone delivery tests (gates 3) (#174)
- [x] 3. DID-marker sentinel collections close the §R3 gap inline (#175; replaced the reverted start-snapshot)
- [x] 4. remove getTombstones overlay endpoint (#177)
- [x] 5. prune overlay-only tombstone API (#178)
- [x] 8. paginate planBackfill (+ sealedTipSeq, per-unit truncation) (#179)
- [x] 9. /subscribe-v2 too-old cursor → HTTP 400 (v1 unchanged) (#180; oracle known-red until step 10)
- [x] 10. client pagination loop + delete cutover buffer + 400 re-backfill (#181; oracle green again)
- [ ] 11. Part B oracle scenarios + mutants
- [ ] 12. docs rewrite (relaxed cooperative contract)

Final gates before calling the effort done: `just lint test`, `just test-long ./internal/oracle`,
`just oracle`, `just oracle-sweep`, `just mutation-campaign` (scorecard re-reviewed, not STALE).
