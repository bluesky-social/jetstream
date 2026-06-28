# Oracle failure diary — restart-chain cutover delivery race

- **Date:** 2026-06-27
- **Commit (failure observed on):** `cd4207b9d3e903e0e73e32e85367b4387d624137`
- **Test:** `TestOracle_RestartChainCrashConsistency/after-bootstrap-live-close-before-seal` (and other subtests; see below)
- **Symptom:** `oracle: missing did:plc:… app.bsky.feed.post/… rev=` — a chain record present in ground truth (world repo state) is absent from the recovered on-disk segments. Sometimes surfaces instead as `oracle: payload mismatch …` on a different DID/subtest.
- **Classification:** flaky, wall-clock/scheduling dependent (not seed-deterministic).
- **Status:** FIXED
- **Tracking issue:** [#166](https://github.com/bluesky-social/jetstream/issues/166) (related: #114, epic #35).
- **Original CI run:** https://github.com/bluesky-social/jetstream/actions/runs/28277269841/job/83786320138

## Repro

```
JETSTREAM_ORACLE_SEED=11212589348287832646 \
  GOMAXPROCS=2 go test ./internal/oracle -run 'TestOracle_Restart' \
    -count=50 -failfast -timeout 60m -v
```

The failure is timing-dependent: which subtest trips, and whether it trips as
`missing` vs `payload mismatch`, varies run to run. It does **not** require the
full suite. The cleanest minimal repro is the **no-crash baseline** (see
analysis) — it removes the crash variable entirely and still fails:

```
JETSTREAM_ORACLE_SEED=11212589348287832646 \
  GOMAXPROCS=2 go test ./internal/oracle \
    -run 'TestOracle_RestartChainDurableIntermediates_Baseline$' \
    -count=60 -failfast -timeout 30m
```

Measured pre-fix flake rate: ~1 failure per 40 single runs of the baseline at
this seed.

## Analysis

The decisive observation: the **no-crash baseline**
`TestOracle_RestartChainDurableIntermediates_Baseline` reproduces the identical
`oracle: missing … rev=` error at the same seed. Since that test never crashes,
the bug is **not** in crash recovery — the crash tier merely inherits it.

Data-flow that must hold for a durable intermediate to land:

1. The parent's `chainCoordinator` (installed as the simulator's
   `OnGetRepoServed` hook) injects the seed-derived chain
   (create/update/delete/recreate, plus shapes F/G) onto the relay's live
   firehose **during the child's backfill**, at a rev above the now-pinned
   backfill head (`restart_chain_coordinator_test.go`,
   `internal/simulator/http/pds.go:79` — generation fires synchronously before
   the getRepo body's EOF).
2. The child's bootstrap-live consumer must read those frames off the fanout and
   durably append them to `backfill/live_segments/` **before** cutover cancels
   the consumer.
3. At cutover, `runBootstrap` writes `phase=merging`, then calls `cancelLive()`
   to tear down the bootstrap-live consumer
   (`internal/ingest/orchestrator/bootstrap.go:128-156`). The merge then drains
   `live_segments`, the rev-filter keeps the chain (`rev > BackfillRev`), and
   compaction runs.

The race: backfill of the *other* repos can finish — triggering cutover — before
the bootstrap-live consumer has drained the chain's tail frames. Any undrained
frame at `cancelLive()` is lost. In production this is benign: steady-state
re-subscribes from the persisted cursor and re-fetches the in-flight events. But
the restart child **exits at the after-merge barrier and never runs
steady-state**, so the lost tail is never recovered → the record is missing on
disk while still present in ground truth → `Compare` fails.

Why it looked seed-stable but wasn't: the seed fixes the *inputs* (world, chain
shape, which DID hosts the chain), but backfill concurrency + bootstrap-live
delivery timing are real wall-clock nondeterminism. At this seed
`deriveChainSpec` picked `chainAccountIdx=3` — the **last** of 4 accounts — so
the plan's mitigation of "target an early-sorting DID" never actually held (the
host is chosen by RNG, not by sort order).

Contributing factors:

- No cross-process cutover gate in the restart child (the main harness has an
  in-process one — `bootstrapTraffic.WaitDelivered` wired to
  `BarrierBeforeCutover` in `harness_test.go`).
- Chain host DID chosen by RNG, defeating the "early DID" passive-timing
  assumption documented in the plan (§3.1, Q2 option a).
- The restart child's exit-at-after-merge lifecycle removes the steady-state
  re-fetch that masks this race in production.

This was anticipated: the plan
`specs/notes/2026-06-20-restart-tier-intermediates-plan.md` §3.1 Q2 says to
escalate to the active cutover gate (option b) "**ONLY if the baseline proves
flaky** (record the flake evidence in this doc if so)."

## Root cause

A generation-vs-cutover **delivery race**: chain frames injected on the live
firehose during backfill are not guaranteed to be durably archived into
`live_segments` before `cancelLive()` tears down the bootstrap-live consumer at
cutover. The restart child, lacking both the early-DID timing slack and the
steady-state re-fetch that protect the no-gate path elsewhere, can lose the
undrained tail.

## Fix (test-only; no production behavior change)

Implemented the plan's Q2 option (b) — a cross-process cutover gate, the
test-process analogue of the main harness's in-process
`bootstrapTraffic.WaitDelivered`:

- **New read-only sim endpoint** `GET /_oracle/firehose-tip`
  (`internal/simulator/http/firehose_tip.go`), mounted only under
  `HandlerOptions.EnableFirehoseTip` (off by default; production `NewHandler`
  never sets it). Reports `world.CurrentSeq()`.
- **`cutoverDeliveryGate`** in `internal/oracle/restart_harness_test.go`, wired
  in `TestOracleRestartChild` to `BarrierBeforeCutover` (the wait) and
  `OnBootstrapLiveEvent` (the observations). At cutover it samples the relay
  firehose tip once and blocks until the bootstrap-live consumer has
  *contiguously* archived every frame up to it.
  - Soundness: every `world.seq.Add` stages exactly one firehose frame (shape
    G's silent mutation bumps no seq → no gaps), and every generated frame
    yields ≥1 archived event with `UpstreamRelayCursor == seq`. bootstrap-live
    runs `BatchSize=1` and fires `OnEvent` after each durable Append.
  - Floors contiguity at the **lowest observed seq**, so a child resuming
    bootstrap at a persisted cursor `C` (e.g. an `AfterRepoComplete` crash before
    the merging-phase write) correctly waits on `C+1..tip` rather than hanging on
    `1..C` (already durable from the prior child).
  - Fails **loud** (timeout error surfaced by the child) if the tip is never
    delivered — never silently proceeds on an undrained tail
    (AGENTS.md: crashing > silent data loss).
- **Doc update:** recorded the escalation + flake evidence in the plan's §3.1
  Q2.

### Regression tests

`internal/oracle/cutover_gate_test.go` (fast, deterministic, can't flake):

- `TestCutoverGateBlocksUntilTipDelivered` — the core guard: gate stays blocked
  while a tip frame is withheld, releases once delivered.
- `TestCutoverGateFloorsAtLowestObserved` — recovering-child case (resume at
  cursor C, observe only C+1..tip).
- `TestCutoverGateNoLiveOps` — nil-coordinator / tip=0 is a no-op.
- `TestCutoverGateTimesOutLoud` — fails loud on a never-delivered (gappy) tail.
- `TestCutoverGateRespectsContextCancel` — clean unblock on ctx cancel.

**Red-first verified:** stubbing `contiguousToTip` to always return `true` (the
"no gate" behavior) makes `TestCutoverGateBlocksUntilTipDelivered` and
`TestCutoverGateTimesOutLoud` fail; restoring the gate makes them pass.

## Verification

- Original repro (`TestOracle_Restart`, count=50, failfast) at the flaky seed:
  PASS.
- No-crash baseline: 0/80 failures (was ~1/40).
- `TestOracle_RestartChainCrashConsistency`: 0/60 failures.
- 5-seed × full-restart-tier sweep (count=2 each) at the flaky seed: 0 failures.
- Full `internal/oracle`, `internal/simulator/...`, `internal/ingest/...`: green.
- `go build ./...`, `go vet`, `gofmt`: clean.

## Files touched

- `internal/simulator/http/firehose_tip.go` (new)
- `internal/simulator/http/handler.go` (`EnableFirehoseTip` option + mount)
- `internal/oracle/restart_harness_test.go` (`cutoverDeliveryGate` + wiring)
- `internal/oracle/cutover_gate_test.go` (new; regression guards)
- `specs/notes/2026-06-20-restart-tier-intermediates-plan.md` (Q2 escalation note)
