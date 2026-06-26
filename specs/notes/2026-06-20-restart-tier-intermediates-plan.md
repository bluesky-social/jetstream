# #113 — Restart tier: durable create/update/delete + on-disk & client-visibility checks

**Status:** PLAN, awaiting review. No code written yet.
**Issue:** #113 (supersedes #103). Epic #35.
**Driver:** the #103 investigation (see `testing/mutation/RESULTS.md` 2026-06-20 m003 section, and #103 close comment).
**Author:** session of 2026-06-20.

---

## 0. TL;DR

The crash/restart tier (`internal/oracle/restart_harness_test.go`) currently lands
**only surviving `create` rows** on disk (`disk_total == final_records`; verified
empirically in #103). It therefore cannot exercise the one thing the spec says
event-log equivalence exists for: **a durable intermediate event lost while final
state still converges** (`specs/oracle.md`:180-182). Nor can it exercise the
**no-permanent-tombstone** client-visibility contract (`docs/README.md` §3.3),
because no durable tombstone ever lands.

This plan expands the restart scenario so a **seed-derived create→update→delete
(and create→update, and delete-then-recreate) series survives the merge into
`data/segments`**, then adds three independent post-restart assertions. The chain
is *reproducible from a seed* but *varies across seeds* — pinned shapes,
seed-varied specifics; see §2.1 (this is NOT a hardcoded fixed chain).

1. **On-disk final state** — `Compare` (exists today; retained).
2. **At-least-once event-log coverage** — seq-agnostic multiset `≥`: every
   world-implied durable event appears on disk at least once. Tolerant of
   duplicates (at-least-once), sensitive to losses.
3. **Per-event logical client visibility** — segments(≤W) + overlay((W,M]) +
   live((M,∞)) reconstruction equals ground truth, AND the compaction contract
   holds, AND a deleted-then-recreated record is correctly **visible again**
   (no permanent tombstone).

It is also the designated home for the **deferred #100 convergence-hiding
over-drop mutant** (see `testing/mutation/mutants/m024…patch` note).

The central difficulty is **architectural**, not assertional: the simulator
world runs in the **parent** test process, while jetstream runs as a **child
subprocess** that is SIGKILL'd and restarted. Landing a durable intermediate
requires generating traffic on a DID *after* the child's backfill captured that
DID's head — across the process boundary. §3 is the design of that mechanism and
is the part most needing review.

---

## 0.1 Production fidelity: `getRepo` serves head only (load-bearing fact)

Production PDSes do **not** retain repository history. `com.atproto.sync.getRepo`
serves only the **latest state**: a snapshot of the records currently live in the
repo. It never replays updates or deletes. The simulator models this faithfully:
backfill walks the MST (current state) and emits **one `KindCreate` per live
record, every one stamped with the repo's head rev** (`backfill/handler.go:111-118`);
`getRepo` exports the current CAR (`ExportRepoCAR`).

Two consequences that shape this entire design:

1. **A durable update or delete tombstone can ONLY reach disk via the live
   firehose** — an event whose rev exceeds the backfill head rev. There is no
   other path, in the simulator or in production. So generating the chain in the
   post-backfill live window (§3) is not a test contrivance to force tombstones
   onto disk; it is **the actual production path by which every durable tombstone
   is born.** This is the strongest argument for Option A and against Option C.

2. **The durable disk stream has a clean closed form:**
   `disk(pre-compaction) = {backfill creates, each at the DID's head rev} ∪ {live
   events with rev > that DID's head rev}`. The backfill set is each DID's
   snapshot *as of the instant its `getRepo` was served*. This is exactly what the
   expected-durable-log helper (§5) must produce, and it is why the
   `OnGetRepoServed` hook does double duty: coordination signal AND the snapshot
   instant.

(Backfill creates for one DID all share the head rev — equal revs, which the
per-DID non-regression invariant permits since it only rejects strictly
decreasing revs, `invariants.go:31`.)

## 1. Why today's restart tier is blind here (verified facts)

All confirmed by reading code + empirical probing in #103:

- **`getRepo` always serves current head** (`internal/simulator/http/pds.go:11-14`,
  ignores `since`). So any event generated *before* the child backfills a DID is
  captured by the backfill snapshot at `rev ≥ event`.
- **The merge rev-filter drops rev-subsumed rows.** `shouldKeep`
  (`internal/ingest/orchestrator/merge_filter.go:40-55`): for a completed repo,
  keep a commit-shaped row only if `ev.Rev > st.Backfill.Rev`. The `preLiveEvents`
  are generated in the parent *before* the child starts → all rev-subsumed → all
  dropped. Probed: 16 evaluations in the merge case, **all DROP, zero KEEP**.
- **Result:** disk = backfill creates only. Dumped across all 4 cases:
  `disk_total == final_records`, `kinds = create:N`. No tombstones, no updates.
- **`preLiveEvents` is generated on the PARENT before the child spawns**
  (`restart_harness_test.go:100-101`), and the child **exits at the after-merge
  barrier** (`TestOracleRestartChild` cancels ctx in the `BarrierAfterMerge`
  callback, `restart_harness_test.go:169-178`) — so **steady state never runs**
  in the child, and there is currently **no hook to inject live-window traffic.**

Conclusion: the assertion gap is downstream of a **traffic gap**. We must fix the
traffic first or any new assertion is vacuous (this is exactly why #103 was
closed rather than implemented).

---

## 2. What "a durable intermediate" requires (the survival rule)

For a commit-shaped row to survive the merge into `data/segments`, its rev must
exceed the per-DID `BackfillRev` the child recorded when it backfilled that DID.
Because `getRepo` serves head, that means:

> **The event must be generated on the firehose AFTER the child has performed its
> `getRepo` for that DID (i.e. after that DID's backfill captured head).**

Such an event arrives at the child's **bootstrap-live consumer** (which is
subscribed to the fanout, `relay_subscribe.go:76-77` subscribes *before* history
replay, so no gap), lands in `backfill/live_segments`, and at merge time
`shouldKeep` returns true (`ev.Rev > BackfillRev`) → it is promoted to
`data/segments`. That is a **durable intermediate**.

Because `getRepo` serves head only (§0.1), there are two distinct durable-
intermediate shapes, and we should test both — they exercise different seams:

- **`R_live` — born entirely in the live window** (no backfill create exists for
  it). Its whole lifecycle is live-sourced, so its expected-log is exactly the
  chain rows. Cleanest fixture for lost-intermediate and no-permanent-tombstone,
  and independent of the backfill snapshot.
- **`R_bf` — existed at backfill** (one `KindCreate` at the DID head rev), then
  mutated live. This is the one that directly exercises the **rev-filter survival
  boundary**: the live update/delete carries `rev > head rev`, so the merge keeps
  it stacked on top of the backfilled create. `R_bf`'s backfill create is the
  pre-chain payload; its live mutations are the durable intermediates.

Chains we want on disk (rkeys tracked so the assertions have exact expectations):

- **create→update** (both shapes): durable create + durable update; final =
  updated record present. Compaction at a watermark covering both must drop the
  stale create (superseded by the update) but keep the update.
- **create→delete** (both shapes): durable create + durable delete tombstone;
  final = record absent. The lost-intermediate case: final state (absent) is
  identical whether or not the create landed — only the event log distinguishes.
- **delete→recreate** (the no-permanent-tombstone case): a delete tombstone on
  `(did,collection,rkey)` followed by a fresh create reusing the SAME rkey at a
  higher seq. Final = record present; client visibility must show it present (the
  overlay mask is half-open `seq < tombstone.seq`, so a recreate above the
  tombstone is NOT masked — `docs/README.md`:358). NOTE: this requires a
  key-reusing generator; ordinary traffic picks fresh TID rkeys, so a same-rkey
  recreate is a deliberate fixture (§7 step 2).

## 2.1 Determinism vs. variance (seeded, not hardcoded) — DECIDED

The chain must be **reproducible from a seed** (so a CI failure replays exactly)
AND **vary across seeds** (so successive runs explore the state space rather than
re-running identical logic). The decided model is **pinned shapes, seed-varied
specifics**:

- **Pinned (always present, every seed):** the load-bearing shapes — `R_live`
  create→update→delete, `R_bf` create(@backfill)→update→delete, delete→recreate
  (same rkey), and the **straddles-watermark** create≤W / delete>W case (§4.4).
  This guarantees every assertion always has its fixture; no run is vacuous.
- **Seed-varied (derived from the run seed):** which account hosts the chain,
  collection(s), rkey values, op interleaving and count between the pinned
  anchors, payload contents, and which of the (≥2) eligible DIDs is the chain DID
  vs. a pure-backfill regression DID.

**Current gaps this fixes (verified):**
- The restart tier today **hardcodes `Seed: uint64(101 + i)`**
  (`restart_harness_test.go:74`) and does NOT read `JETSTREAM_ORACLE_SEED` (only
  the lifecycle `defaultConfig()` does). → We make the restart tier honor
  `JETSTREAM_ORACLE_SEED`, defaulting to `101+i` for push CI (so push CI stays
  fixed/fast/reproducible). The chain shape-specifics derive from a chain-local
  RNG seeded off `cfg.Seed` (e.g. `rand.NewPCG(seed^k1, seed^k2)`), mirroring the
  existing runtime-RNG seeding at `restart_harness_test.go:283`.
- The scheduled sweep (`.github/workflows/oracle-scheduled.yml` → `just
  oracle-sweep`) runs **only `-run TestOracle_DefaultLifecycle`** with fresh
  random uint64 seeds; it does **not** run the restart tier at all. → We add
  `TestOracle_RestartCrashPointsDoNotLoseRecords` to the sweep so nightly random
  seeds actually exercise the new intermediates. (Recipe edit; see §7 step 6 and
  Q7.)

Caveat already true of this tier (document, don't fight): crash *timing* is real
wall-clock/scheduling nondeterminism (SIGKILL races), not seeded — so even a
fixed seed is not bit-reproducible. The seed fixes INPUTS (world, chain shape,
fault schedule); interleaving is explored by repetition. This matches the
existing sweep's documented repro caveat.

## 2.2 Lazy/out-of-band compaction — two on-disk shapes the assertions MUST handle

Compaction is **out-of-band and lazy**, NOT synchronous with the firehose. An
update/delete arriving upstream is appended as a normal tombstone row to the
active segment; the **physical rewrite** of older sealed segments happens later,
and only for superseded create/update rows **at or below the watermark W**
(`docs/README.md`:342-348). Two standing facts:

- **Tombstones are retained forever** as event rows (delete/update/sync/account-
  delete; `docs/README.md`:348). They are never physically removed.
- **A superseded create/update is removed only once it is sealed AND ≤ W AND a
  compaction pass has run over it.** Until then it is still physically on disk —
  and that is *correct*, not a bug.

In the restart child specifically (verified): the ONLY compaction that runs is
the single **merge-tail pass** (`runDeleteCompaction(compactionMergeTail)`,
`merge.go:103`), whose watermark `W = targetWatermark = the max MaxSeq over
sealed segments` (`listSealedCompactionSegments`). The child then exits at the
after-merge barrier — **no steady-state compaction interval ever fires.** So
whether a chain row is physically compacted depends entirely on whether it is
sealed and ≤ W when that one pass runs. This yields two on-disk shapes, BOTH
valid, and the assertions must distinguish them:

1. **create ≤ W and superseded ≤ W** (both sealed before the pass): the stale
   create is **physically GONE**; the tombstone remains. `CheckCompacted` REQUIRES
   the create absent here.
2. **create or its tombstone > W, or still in the active (unsealed) segment**:
   the create is **still physically PRESENT**. `CheckCompacted` must NOT flag it
   (supersession only applies ≤ W). This is the `straddles-watermark` shape and
   the basis of the #100 over-drop mutant (§4.4): a survivor dropped ≤W but
   superseded >W is an over-drop that final-state convergence hides.

Implications threaded through the rest of the doc:
- **Coverage (§4.2)** is about *presence*, and tombstones are never removed, so a
  lost delete/update tombstone is always caught. But coverage's expected set MUST
  subtract creates that compaction *legitimately* removed (shape 1) — otherwise it
  would demand a row the contract says is gone. See §5 (the closed form is
  pre-compaction; we apply the compaction filter at W).
- **Compaction contract (§4.3)** is the check that is *sensitive* to compaction
  having run; it is asserted at the child's actual merge-tail `W`.
- This does NOT change the §3 getRepo mechanism at all: that mechanism governs
  getting the tombstone *onto disk* (surviving the merge rev-filter); lazy
  compaction is a later, separate stage that *rewrites* what is already on disk.

---

## 3. The mechanism: landing a post-backfill mutation across the process boundary

This is the design decision needing review. The parent must (a) learn that the
child has backfilled the target DID(s), then (b) generate the chain, then (c)
ensure the chain is durably merged before it SIGKILLs / before the after-merge
barrier completes. Three candidate designs:

### Option A — getRepo-served TIMING SIGNAL + live-firehose chain injection (RECOMMENDED)

**Critical clarification (the name "getRepo hook" is misleading on its own):**
getRepo is used ONLY as a *timing signal*, never as a data channel. getRepo
serves the repo's current-head snapshot — creates only, never updates/deletes
(§0.1) — so the chain CANNOT and DOES NOT travel through getRepo. The chain
(create/update/delete) travels over the **live firehose**: the parent generates
real `#commit` frames → simulator fanout → the child's bootstrap-live consumer
(`subscribeRepos`, subscribed before history replay, `relay_subscribe.go:76-77`)
→ `live_segments` → merge. This is the SAME path that carries every durable
update/delete in production; the mechanism is production-faithful, not a hack.

There are two independent parent→child channels, and Option A coordinates them:

| channel | carries | role here |
|---|---|---|
| `getRepo` (path 1) | current-head snapshot (creates only) | **starting gun** — fires the hook so we know `BackfillRev` is pinned |
| `subscribeRepos`/fanout (path 2) | live `#commit`/`#sync`/etc. (incl. updates/deletes) | **data path** — the chain rides this |

Add an optional callback to the simulator HTTP handler that fires **after**
`getRepo` serves a DID's CAR:

```go
// internal/simulator/http/handler.go
type HandlerOptions struct {
    Faults          *FaultPlan
    OnGetRepoServed func(did string) // NEW; nil = no-op
}
```

Flow: on the FIRST getRepo for the chain DID, the child has snapshotted that
DID's head and will record `BackfillRev = commit.Rev` on download completion
(`backfill/store.go:308-309`). The parent's hook then generates the chain on that
DID via targeted helpers. Each commit gets a **fresh rev**, which is therefore
`> BackfillRev` (head cannot advance between snapshot and chain-gen because only
the parent advances it, and it waits for the hook). Those frames ride path 2 and,
at merge, `shouldKeep` keeps them (`ev.Rev > BackfillRev`) → durable intermediates.

**Why the hook is required (the ordering hazard it removes):** if the parent
emitted the update/delete *before* the child backfilled the DID, the getRepo
snapshot would capture head at-or-above those revs and the merge rev-filter would
drop them (`rev ≤ BackfillRev`) — the exact vanishing observed in #103. The hook
guarantees `chain rev > BackfillRev` by construction (generate only after snapshot).

**Sub-variant A′ — key off `backfill_complete.log` instead of getRepo-served.**
The durable per-DID completion record (README §3.5/§6) is written strictly AFTER
`BackfillRev` is committed, so it's a marginally tighter "rev is pinned"
guarantee. Trade-off: it fires LATER (closer to cutover), shrinking the runway to
get the chain durable in `live_segments` before the bootstrap-live consumer
closes. getRepo-served fires earlier → more timing slack → preferred; A′ is the
fallback if the slack proves insufficient. (Folds into Q2.)

- **Pros:** Minimal, uses the existing process model unchanged (parent = upstream
  relay, child = jetstream). The hook is a natural extension of the existing
  `Faults` hook (same struct, same call site). No new crashpoints, no steady-state
  in the child. Production-faithful (chain born on the live firehose, §0.1).
- **Cons:** Adds a hook to production-adjacent simulator code (test-only, but in a
  non-`_test.go` file). Timing: must ensure the chain is consumed by bootstrap-
  live AND durable in `live_segments` *before* backfill completion triggers
  cutover. Mitigation: target a DID backfilled *early* and keep the chain short;
  escalate to a cutover gate or A′ only if flaky — see §3.1, Q2. The no-crash
  baseline (§7 step 4) proves durability before any crash case relies on it.

### Option B — generate during a backfill barrier (BarrierAfterBootstrap-style)

Reuse/extend a phase barrier: pause the child after backfill captures heads but
before cutover, and have the parent inject the chain during the pause.

- **Pros:** No getRepo hook; uses the existing barrier vocabulary.
- **Cons:** There is no existing "after all repos backfilled, before bootstrap-
  live close" barrier exposed to the child harness; `BarrierAfterBootstrap`
  exists in the main harness (`harness_test.go`) but the **restart child does not
  wire it** today. We'd add a barrier the child blocks on, then signal the parent
  across the process boundary (marker file) to generate, then release. More
  moving parts (two-way cross-process handshake) and more new surface than A.

### Option C — enable steady state in the child + generate post-merge

Let the restart child run steady state (don't cancel at after-merge), generate
the chain as steady-state traffic, then stop.

- **Pros:** Exercises steady-state durability too; closest to production.
- **Cons:** Largest behavioral change to the restart harness; steady-state rows
  land via a *different* path (not the merge), so they don't test the
  merge-survival rev-filter or the merge-tail compaction interaction — which is
  precisely the restart tier's differentiator. Also reintroduces the
  force-rotate-before-pass property that makes over-drops unreachable (the m024
  note: steady mode can't host the convergence-hiding mutant). **This would
  defeat the #100 over-drop goal.** Rejected for the primary scenario.

**Recommendation: Option A.** It is the smallest change that produces durable
intermediates *through the merge path*, keeps the crash-mid-pass property needed
for #100, and reuses the existing hook idiom. (Open question Q1 below.)

### 3.1 Timing / ordering guarantees (Option A)

The chain must be durably merged before the run's terminal assertion. Ordering:

1. Child starts; backfill begins; bootstrap-live subscribes (fanout, no gap).
2. Parent's `OnGetRepoServed(targetDID)` fires → parent generates chain on
   targetDID. Each commit is published to the fanout → bootstrap-live writes it
   to `live_segments`.
3. Backfill completes (all 4 small repos) → cutover → merge drains
   `live_segments`, `shouldKeep` keeps the chain (rev > BackfillRev) → durable in
   `data/segments` → merge-tail compaction runs over it.
4. After-merge barrier → child exits cleanly (no crash case) OR the crash case
   SIGKILLs at an enumerated crashpoint and a second child re-runs merge
   idempotently.

**Risk:** backfill of the *other* repos could finish before the chain is fully
consumed by bootstrap-live, racing cutover. **Q2 RESOLVED → (a), baseline-gated:**
- (a) **[chosen]** Target a DID that sorts/backfills early AND keep the chain
  short (3-4 commits); bootstrap-live is fast (in-memory fanout, localhost).
- (b) Stronger fallback: a one-shot gate so the parent's chain generation
  completes before backfill cutover — e.g. the parent holds a
  `BarrierAfterBootstrap`-style release until it has generated + observed the
  chain echoed on a second subscribeRepos tap. **Escalate to (b) ONLY if the
  baseline proves flaky** (record the flake evidence in this doc if so).
- **TDD gate (mandatory):** verify the chain is durable by asserting its rows are
  present on a no-crash baseline run (§7 step 0e) before any crash case relies on
  it.

### 3.2 Determinism

Seq-assignment order across DIDs is nondeterministic (backfill concurrency,
verified in #103). The chain therefore must be asserted **seq-agnostically**
(multiset / key-based), never positionally by seq. The chain's *intra-DID* order
(create before update before delete) is guaranteed by rev monotonicity and
single-DID generation. The harness **tracks** the chain's `(did, collection,
rkey)`s — which are themselves seed-derived (§2.1), not hardcoded — so the
visibility assertion has an exact expectation regardless of the seed.

---

## 4. The three assertions (what we add post-restart)

All run in `TestOracle_RestartCrashPointsDoNotLoseRecords` after the existing
`assertOracleMatches`, reusing existing checkers (mapped below).

### 4.1 On-disk final state (EXISTS — retain)
`assertOracleMatches` (`harness_test.go:580`): `GroundTruthFromWorld` +
`ObserveSegments` + `CheckInvariants` + `Reconstruct` + `Compare`. Already
catches a lost create that changes final state, and the blanket m024 over-drop.

### 4.2 At-least-once event-log coverage (NEW)
- Build expected durable rows: the world-implied set after the merge rev-filter.
  This is NOT `ExpectedEventLogFromFirehose` directly (that's upstream-cursor
  seq-space and includes pre-backfill rows the merge drops). It is: the backfill
  snapshot creates (final repo state at backfill time) **plus** the durable
  intermediates (the chain rows with rev > BackfillRev). See §5 for the
  derivation — this is the main new helper.
- Normalize both sides with `NormalizeEventLog`, compare **seq-agnostically as a
  multiset with `≥` semantics** (every expected row appears ≥1×; extras/dupes
  tolerated). NOTE: existing `CompareEventLogMultiset` is `==` (exact). We need a
  new **at-least-once** variant (`CompareEventLogCoverage`?) — or reuse `==` IF
  we can prove the merge path never duplicates in the no-crash case and bound
  crash-case dupes. **Decision Q3:** add a `≥` coverage comparator (safer, honors
  the at-least-once contract) vs. assert `==` (stronger but may flake on a
  legitimate at-least-once duplicate across the crash boundary). Plan assumes the
  `≥` comparator.
- Seq-space: on-disk rows carry jetstream-seq; `UpstreamRelayCursor` is NOT
  persisted (`segment/event.go:72`). The coverage check is **key-based**
  (kind+did+collection+rkey+rev+payload-hash), so it is seq-space-agnostic by
  construction — this sidesteps the two-seq-space trap entirely.

### 4.3 Per-event client visibility (NEW)
Two sub-checks, both over the post-restart durable segments:
- **Compaction contract:** `CheckCompacted(events, W)` at the child's merge-tail
  compaction watermark. The watermark IS non-zero in the restart child
  (`initCompactionWatermarkFloor` sets `nextSeq-1`, merge-tail compaction runs —
  `merge.go:83,103`). With a durable create→delete below W, this now has real
  rows to check (today it's vacuous).
- **Overlay reconstruction + no permanent tombstone (Q4 RESOLVED → model-derived,
  on-disk):** segments(≤W) + overlay((W,M]) + live((M,∞)) == ground truth, with
  the overlay derived from the durable tombstone rows on disk rather than a served
  `getTombstones` blob (the restart child exits at after-merge; no server is up,
  and keeping one up would entangle the exit-at-barrier lifecycle the #100 design
  needs). The delete→recreate chain is the key fixture: the recreate above the
  tombstone seq must reconstruct as **present**, proving the half-open mask
  (`docs/README.md`:358). `CheckOverlayReconstruction` (`overlay.go:52`) takes a
  `tombstone.Snapshot` for `(W, M]`; we build that snapshot from the on-disk
  tombstones in the `(W, M]` window instead of decoding a fetched blob. (Confirm
  during implementation that constructing a `tombstone.Snapshot` directly from
  observed rows is ergonomic; if not, fall back to asserting the compaction
  contract on-disk + a model-level visibility check over the recovered segments —
  same coverage, simpler plumbing.) The served wire path stays covered by the
  steady-state + client-driven tiers.

### 4.4 #100 convergence-hiding over-drop mutant — SUPERSEDED 2026-06-26
**This subsection's premise was refuted during implementation.** The hypothesis
below — that crash-mid-compaction-pass on the restart path yields a `create ≤W /
delete >W` straddle — does not hold: merge-tail compaction runs at a quiescent
point after the merge has sealed every segment, so `targetWatermark` spans the
whole durable stream and every durable row is ≤W. Crashing at
`AfterCompactionRewriteBeforeWatermark` leaves W un-advanced for that *chunk*, but
the recovered re-merge recomputes a complete snapshot — there is no above-scope
row to over-drop invisibly. This is now **enforced** by
`TestOracle_RestartChainShapeB_NoStraddleAfterMergeTailCrash` (`maxDurableSeq ≤
W`). The convergence-hiding over-drop proof lives in the **steady tier** (m025,
KILLED@stress), where a delete *can* land in the fresh active segment above the
force-rotate watermark. No restart-tier over-drop mutant is added. See
`testing/mutation/RESULTS.md` 2026-06-20 reachability correction + 2026-06-26
update.

Original (refuted) hypothesis, kept for the audit trail:

> Per the m024 patch note: the unique power (an over-drop hidden by final-state
> convergence) needs "a survivor dropped at/below W but independently superseded
> ABOVE W … where W need not cover everything." The crash-mid-compaction-pass
> restart provides exactly this: crash at `AfterCompactionRewriteBeforeWatermark`
> leaves a rewritten segment with the watermark NOT advanced, so on restart W
> does not cover the just-rewritten rows. With a durable record created ≤W but
> deleted >W, an over-drop of that record is invisible to final-state `Compare`
> but caught by event-log coverage. We add a crash case at that crashpoint and a
> mutation entry — verify it KILLS.

---

## 5. The new expected-durable-log helper (the core new logic)

The crux of 4.2. We need `want` = the multiset of event-log rows that MUST be on
disk, in jetstream-seq-agnostic key form. Per §0.1 the **pre-compaction** durable
stream has a clean closed form:

> `want_pre = {backfill creates, each at the DID head rev as of its getRepo} ∪
> {live events on any DID with rev > that DID's head rev}`

But the post-restart disk has been through the merge-tail compaction pass (§2.2),
which physically removes superseded creates ≤ W. So the coverage expectation is:

> `want = filterCompactedExpectedRows(want_pre, W)`

— i.e. drop from `want_pre` the create/update rows that a tombstone ≤ W
supersedes ≤ W (shape 1 of §2.2). This is the SAME filter the existing
event-log-compacted comparators apply (`eventlog.go:151
filterCompactedExpectedRows`), so we reuse it rather than reimplement. W is the
child's merge-tail watermark, read from disk/pebble post-restart. Then assert
coverage `≥`: every row of `want` appears on disk at least once (tombstones and
surviving creates present; compacted-away creates correctly NOT demanded).

Composition of `want_pre`:
1. **Backfill creates:** for each non-deleted account, exactly the records live in
   its repo *at the instant its getRepo was served* (the `OnGetRepoServed` hook
   instant), one `KindCreate` per record at the head rev. For DIDs we never
   mutate, that's the final record set. For the chain DID, it's the pre-chain
   snapshot (so `R_bf`'s create reflects its pre-mutation payload; `R_live` has no
   backfill create at all).
2. **Durable intermediates (the chain):** the exact create/update/delete rows the
   parent generated after getRepo, each as a key-form row (kind, did, coll, rkey,
   rev, payload-hash). These are the only updates/deletes that can be on disk.
   Tombstone rows survive compaction (retained forever); chain creates/updates
   superseded ≤ W are subtracted by the filter above.

Two derivations possible:
- **(5a) Model-derived:** reconstruct `want` from what we generated. We KNOW the
  chain (we issued it) and we know the backfill snapshot (the pre-chain world
  state for the target DID, plus the full state for others). Build `want`
  directly. Most precise; least coupling to firehose seq-space.
- **(5b) Firehose-derived + rev-filter:** take `ExpectedEventLogFromFirehose`
  over the full range and apply the same `shouldKeep` rev-filter logic the merge
  uses, against the per-DID backfill revs. Reuses existing machinery but must
  replicate the rev-filter and know each DID's BackfillRev — which lives in the
  child's pebble (`repo/<did>`), readable post-restart.

**Recommendation: 5a (model-derived)** for the chain rows (we authored them, so
the expectation is exact and independent of the firehose seq-space), combined
with the existing `GroundTruthFromWorld` for the steady-state record set. This
keeps `want` independent of the implementation-under-test. Validate against 5b as
a cross-check in a unit test if cheap. (Q5.)

---

## 6. Shape catalog — full scope, one issue + one commit per shape (Q6 RESOLVED)

Decision: implement the **full scope**, decomposed into **one GitHub issue and
one commit per shape**, each carrying its own red-first power test (a shape's
commit cannot merge until its assertion is shown to FAIL on the matching injected
fault). **Representative origin per shape** (NOT the full R_live×R_bf matrix):
pick the origin that targets each shape's distinct seam. Shapes share one
generator family (record ops on a caller-specified key), so the per-shape
increment after the infra lands is small.

**Dependency:** all shapes depend on the **infra issue (§7 step group 0)** — the
getRepo hook, targeted generators, seed-spec, coverage comparator (`≥`),
model-derived expected-log (5a), and the no-crash baseline. No shape can be tested
before that lands.

Core record-level shapes (share the generator family):

| ID | shape | origin | distinct seam it proves | red-first fault |
|----|-------|--------|--------------------------|------------------|
| A | create→update | **R_bf** | backfilled create (head rev) superseded by a live update (higher rev); compaction drops the backfilled create, keeps the update | drop the update row → coverage fails |
| B | create→delete | **R_bf** | §180-182 lost-intermediate: final state identical with/without the create; only the log distinguishes. Rev-filter boundary. | drop the create row → coverage fails while final-state `Compare` passes |
| C | create→update→delete | **R_live** | full lifecycle with NO backfill create (live-only path) | drop any middle row → coverage fails |
| D | delete→recreate (same rkey) | R_live | record-level **no-permanent-tombstone**: recreate above the tombstone must be visible (`docs/README.md`:358) | mask the recreate → overlay reconstruction fails |

Layered crash application (NOT a separate fixture — it's shape B at a specific
crashpoint):

| ID | what | crashpoint | distinct seam |
|----|------|-----------|----------------|
| B-crash (#114) — RESOLVED 2026-06-26 | shape B crashed mid-compaction-rewrite | `AfterCompactionRewriteBeforeWatermark` (`compact_deletes.go:178`) | The convergence-hiding over-drop framing was **withdrawn as infeasible in merge-tail** (the `create ≤W / delete >W` straddle cannot form — the pass spans the whole sealed stream; see `testing/mutation/RESULTS.md` 2026-06-20 reachability correction). That infeasibility is now an **enforced test** (`TestOracle_RestartChainShapeB_NoStraddleAfterMergeTailCrash`: `maxDurableSeq ≤ W`). The convergence-hiding over-drop proof stays in the **steady tier** (m025, KILLED@stress). The real restart-tier value — durable intermediates surviving a crash — is delivered by `TestOracle_RestartChainCrashConsistency` (full `assertChainDurable` bundle over the recovered segments, red-first power check). No new restart-tier mutant. |

DID-level shapes:

| ID | shape | cost | distinct seam |
|----|-------|------|----------------|
| F | account-delete → reactivate → post | low–moderate | **DID-level no-permanent-tombstone**: a DID tombstone removes all records ≤ tombstone; a record created after reactivation (seq above) is visible. Distinct tombstone class (DID-keyed). Clean to drive (account frame, no verifier re-fetch). In full scope. |
| G | #sync divergence | **HIGH — flagged, lands LAST as its own issue** | `KindSync` DID tombstone + `KindCreateResync` replacement rows (`docs/README.md`:567). On recovered state this entangles the getRepo path (the verifier re-fetches to build resync rows), unlike every other shape. Own issue, sequenced last so the simpler shapes de-risk the harness first; spike the sync-repair-across-crash interaction as part of it. |

Control / regression guards (cheap, high value — catch a fixture that tests
nothing):

| ID | guard | proves |
|----|-------|--------|
| H1 | R_live create-only record (no tombstone) | live-window survival works independent of any tombstone |
| H2 | a pure-backfill untouched DID | the existing all-creates path still works (no regression) |

**Explicitly NOT in scope (proliferation, diminishing returns):**
- Depth variants (update→update→update, create→delete→recreate→delete): negligible
  new coverage over A–D.
- Full R_live×R_bf cross product: doubles issue count for marginal seam coverage.
- Identity-only / non-delete account (takedown) events: they supersede nothing, so
  they are irrelevant to the compaction/visibility goal; covered incidentally in
  other tiers.

Run-level edge cases every shape must tolerate (assert, don't fight):
- **Crash between dst flush and source-commit** (existing case): chain rows may be
  re-merged with fresh seqs → benign at-least-once duplicate. Coverage `≥`
  tolerates (Q3); `==` would flake.
- **Empty overlay** (chain fully ≤ W after compaction): overlay (W,M] may be
  empty; reconstruction must still pass (empty overlay is valid,
  `docs/README.md`:392).
- **Lazy/watermark-bounded compaction (§2.2):** a superseded create is gone only
  if sealed AND ≤W; above W it is correctly still present. `CheckCompacted` and
  the coverage `want` (filtered at W) must both respect this.

Test-power discipline: every shape's commit includes the red-first verification
above — inject the fault, watch THAT shape's specific assertion fail, then remove.
The four canonical failure modes: lost intermediate → coverage fails; over-dropped
survivor hidden by convergence (B-crash) → coverage fails, final-state passes;
permanent tombstone → overlay reconstruction fails; compaction under-drop →
`CheckCompacted` fails.

---

## 7. Implementation plan — issue/commit decomposition (Q6 RESOLVED)

Full scope, **one GitHub issue + one commit per unit**, all under #113 (or as
#113 child issues). Infra lands first; shapes layer on top; each shape commit
carries its own red-first power test (§6). Sub-issues of #113 — file them as the
infra issue merges so the shape issues reference concrete helper names.

**Group 0 — infra (single issue/commit unless it grows; the only hard
prerequisite for every shape):**
0a. **Simulator hook (Option A):** add `OnGetRepoServed` to `HandlerOptions`, fire
    post-serve in `pds.go`. Unit-test it fires once per DID getRepo.
0b. **Targeted, key-reusing generators:** `GenerateRecordOpForTest(idx, action,
    coll, rkey)` so a delete→recreate reuses the same rkey. Unit-test the op
    shapes + same-rkey recreate (the §2 probe already proved CAR/MST mechanics).
0c. **Seed-derived chain spec (§2.1):** pure `seed → chainSpec` (pinned shapes,
    varied account/coll/rkey/interleave/payload). Restart tier honors
    `JETSTREAM_ORACLE_SEED` (default `101+i`); chain RNG off `cfg.Seed`. Unit-test
    determinism + variance + all-pinned-shapes-present.
0d. **Model-derived expected-log (§5a) + `≥` coverage comparator (§4.2/Q3):** new
    `CompareEventLogCoverage`; build `want` from the chain spec + backfill snapshot
    + compaction filter at W. Unit-test incl. red-first (drop a row → fails).
0e. **Harness wiring + NO-CRASH BASELINE (Q2 gate):** coordinator generates the
    chain on the first getRepo for the chain DID; land a no-crash case FIRST and
    assert the chain rows are durably present + coverage/visibility pass. This
    proves the mechanism (and the Q2 passive-timing assumption) before any crash
    case depends on it. If flaky → escalate to the cutover gate (Q2) and record it.

**Per-shape issues/commits (each: add the shape to the seed-spec's pinned set +
its assertion + its red-first power test):**
- Issue **A** — create→update (R_bf).
- Issue **B** — create→delete (R_bf); the §180-182 lost-intermediate.
- Issue **C** — create→update→delete (R_live).
- Issue **D** — delete→recreate same-rkey (R_live); record-level no-perm-tombstone.
- Issue **F** — account-delete → reactivate → post; DID-level no-perm-tombstone.
- Issue **H** — control guards (H1 live create-only, H2 pure-backfill untouched DID).
- Issue **B-crash (#100)** — shape B at `AfterCompactionRewriteBeforeWatermark`;
  add the crash case + mutation entry (or re-point m024); verify it KILLS.
- Issue **G (flagged, LAST)** — #sync divergence DID-tombstone; spike the
  sync-repair-across-crash / verifier-getRepo interaction first, then implement.

**Final — sweep inclusion + docs (own commit):** add
`TestOracle_RestartCrashPointsDoNotLoseRecords` to the `oracle-sweep` recipe
(`justfile`, alongside `TestOracle_DefaultLifecycle`) so nightly random seeds
exercise the intermediates (Q7); update `specs/oracle.md` crash/restart tier
description and `testing/mutation/RESULTS.md` for the m024/over-drop entry.

Run matrix per commit: `-run TestOracle_RestartCrashPointsDoNotLoseRecords` (all
cases), then a couple of explicit `JETSTREAM_ORACLE_SEED` values to confirm
variance, then the full oracle suite (fast/default/stress/race), then the relevant
mutation case. Restart tier is skipped under `-short`.

---

## 8. Open questions for review (Q1–Q7)

- **RESOLVED (seed variance):** pinned shapes + seed-varied specifics; restart
  tier honors `JETSTREAM_ORACLE_SEED` (default `101+i`); add the restart tier to
  the nightly sweep. See §2.1. (Was the unnumbered determinism gap Jim raised.)
- **Q1 (mechanism): RESOLVED → Option A** (getRepo-served *timing signal* + chain
  injected over the live firehose — NOT through getRepo, which carries creates
  only). B (barrier) and C (steady state) rejected; C specifically defeats the
  #100 crash-mid-pass property. The getRepo-served-vs-A′ trigger sub-choice rolls
  into Q2 (it's a timing/flake-runway tradeoff).
- **Q2 (timing/flake guard): RESOLVED → getRepo-served trigger + PASSIVE
  (early-backfilled DID + short chain, 3.1a), baseline-gated.** A mandatory
  no-crash baseline test (§7 step 4) must prove the chain is durably on disk
  before any crash case relies on it. Escalate to the active cutover gate (3.1b)
  ONLY if that baseline proves flaky. Do NOT build the gate or use the A′ trigger
  up front. If escalation happens, record it as a doc update with the observed
  flake evidence (don't silently add machinery).
- **Q3 (coverage comparator): RESOLVED → at-least-once `≥`.** Every expected
  durable row must appear ≥1× on disk; duplicates tolerated (honors the
  `docs/README.md`:156 at-least-once contract, and avoids the known benign-dup
  flake from the `after-merge-dst-flush-before-source-commit` re-merge, #103).
  Sensitive to LOSS (the goal), blind to dupes. Spurious-duplication classes stay
  covered elsewhere (final-state `Compare`, `CheckInvariants` unique-seq guard).
  Add a new `≥` comparator (e.g. `CompareEventLogCoverage`); do NOT reuse the
  exact-multiset `CompareEventLogMultiset`.
- **Q4 (overlay fetch): RESOLVED → model-derived overlay, on-disk (b).** Derive
  the expected `(W, M]` overlay from the durable tombstone rows on disk and assert
  reconstruction + no-permanent-tombstone against the recovered data; do NOT keep
  a server up. Rationale: the restart tier's UNIQUE value is "crash recovery
  preserved correct visibility semantics on durable data," and keeping a server up
  would entangle the clean exit-at-barrier lifecycle that the #100 crash-mid-pass
  design depends on. The served `getTombstones` wire path is already covered by
  the steady-state + client-driven tiers, so it is not left uncovered.
- **Q5 (expected-log derivation): RESOLVED → 5a, model-derived.** Build `want`
  from the test's own authoritative record — the chain spec it injected (§2.1)
  plus each DID's backfill snapshot (final state from `GroundTruthFromWorld` for
  unmutated DIDs; the pre-chain state for the chain DID, which the harness knows
  because it controls when the chain starts) — then apply the compaction filter at
  W. This keeps `want` INDEPENDENT of the system under test (the oracle-
  independence principle): no reading `BackfillRev` from the child's pebble, no
  re-running jetstream's `shouldKeep` in the test, so a rev-filter bug can't
  corrupt both sides into agreement. 5b (firehose + re-applied rev-filter) is NOT
  the primary path; keep it only as an optional cheap unit-test cross-check if
  convenient.
- **Q6 (scope): RESOLVED → full scope, one issue + one commit per shape,
  representative origin per shape.** Shape catalog A,B,C,D,F + B-crash(#100) + H
  guards, each its own issue/commit with a red-first power test; **G (#sync)
  flagged highest-complexity, its own issue, landed LAST** (spike the
  verifier-getRepo-across-crash interaction first). Infra (§7 group 0) is the hard
  prerequisite. NOT in scope: depth variants, full R_live×R_bf matrix, identity/
  takedown events. See §6 for the catalog and §7 for the issue decomposition.
- **Q7 (sweep cost): RESOLVED → add all cases now, revisit if wall-clock bites.**
  The restart tier SIGKILLs real subprocesses (≈0.13s/case at current scale).
  Add `TestOracle_RestartCrashPointsDoNotLoseRecords` to the sweep at full
  per-seed frequency; restart cases are cheap vs. DefaultLifecycle stress and the
  job budget is 360min. Revisit (reduced cadence / crash-case subset) only if
  sweep wall-clock becomes a problem.

---

## 9. Key references (for the implementer)

- `internal/oracle/restart_harness_test.go` — the tier (parent/child model,
  crashpoints, BarrierAfterMerge cancel-on-merge).
- `internal/simulator/http/{handler.go,pds.go,relay_subscribe.go}` — getRepo
  serves head; subscribeRepos subscribes-before-replay (no gap); HandlerOptions
  hook idiom.
- `internal/ingest/orchestrator/{merge.go,merge_runner.go,merge_filter.go,
  compact_deletes.go,compaction_watermark.go,orchestrator.go}` — merge,
  rev-filter survival rule, merge-tail compaction, watermark floor, crashpoints.
- `internal/oracle/{compare.go,reconstruct.go,compacted.go,overlay.go,
  eventlog.go,segments.go,groundtruth.go,expected_eventlog.go,model.go}` — the
  checker library (see #113 research map).
- `internal/oracle/{harness_test.go,client_observer_test.go,
  overlay_integration_test.go}` — assert helpers, ack types, overlay fetch.
- `docs/README.md` §3.3 (compaction + overlay + no-permanent-tombstone),
  §4.1-4.4 (bootstrap/merge/steady/identity-account-sync).
- `specs/oracle.md` §180-182 (final-state vs event-log), Crash/Restart Tier.
- `testing/mutation/mutants/m024…patch` (the #100 over-drop mutant note),
  `testing/mutation/RESULTS.md` 2026-06-20 (m003 re-disposition).

### GitHub issues

- **#113** — this work. https://github.com/bluesky-social/jetstream-v2/issues/113
- **#103** — superseded by #113 (closed); investigation comment explains why an
  event-log check was vacuous against the old all-creates restart traffic.
  https://github.com/bluesky-social/jetstream-v2/issues/103
- **#100** — compaction over-drop / data-loss check; the m024 convergence-hiding
  over-drop mutant deferred to this tier (shape B-crash, §6).
  https://github.com/bluesky-social/jetstream-v2/issues/100
- **#35** — oracle testing revamp epic (parent).
  https://github.com/bluesky-social/jetstream-v2/issues/35
