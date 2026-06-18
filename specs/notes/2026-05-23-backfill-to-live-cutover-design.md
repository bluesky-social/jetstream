# Backfill-to-Live Cutover Orchestration

**Date:** 2026-05-23
**Scope:** Implement the runtime transition from the bootstrap phase (backfill engine + bootstrap-time live tail writing to `data/backfill/live_segments/`) to the steady-state phase (a single live consumer writing to `data/segments/`). Introduce a new `internal/ingest/orchestrator` package that owns the entire ingestion-lifecycle state machine. Add a `merging` lifecycle phase as a real durable state, with the actual merge body left as a stubbed no-op for a follow-up PR. Make the cutover crash-safe via durable phase markers at each commit point.

This implements DESIGN.md §4.2 ("Merge Phase") and §4.3 ("Steady State Phase") at the orchestration level. The substantive compaction work — reading `backfill/live_segments/*.jss` and rewriting events into `segments/` with new seq numbers, deduplicating by `(did, rev)` — is explicitly deferred and will be a single-function change inside this package's `merge.go`.

## 1. Goals

1. When the backfill engine drains, transparently switch the upstream firehose consumer from writing to `backfill/live_segments/` to writing to `segments/`, without losing or duplicating events at the at-least-once tier.
2. Make the bootstrap → merging → steady-state transition crash-safe: a process killed at any point during cutover restarts to a well-defined recovery path.
3. Allow `cmd/jetstream` to be started against any of the three persisted phases without manual intervention.
4. Keep the `internal/ingest/live` package untouched. Its existing `Config.SegmentsDir`/`SeqKey`/`CursorKey` knobs are sufficient; the cutover is achieved by constructing a second `live.Consumer` instance, not by mutating an existing one.
5. Leave the substantive merge/compaction logic as a clearly-marked stub so the future PR is isolated to one file.
6. `cmd/jetstream` exposes one method on the orchestrator (`Run(ctx)`) and does not branch on phase. Phase dispatch is internal to the orchestrator.

## 2. Non-Goals

- **Compaction** of `backfill/live_segments/*.jss` into `segments/`. State 5 of the state machine is a no-op stub. Future PR territory.
- **Failed-DID retry in steady state** (DESIGN.md §4.3, paragraph on `repo/<did>.Status == StatusFailed` retry). Not part of this PR.
- **Cleanup of `backfill/`**, including the orphan `live_segments/seq/next` pebble key. The whole `backfill/` directory's lifecycle is being decided separately.
- **Replication-related signals** during cutover (DESIGN.md §6). No `segment_compacted` events are emitted because no compaction happens.
- **Multi-leader / promotion logic** on cutover. The orchestrator runs on a single leader and does not coordinate with replicas.
- **Lookaside compaction** (DESIGN.md §3.3.1). Unrelated to this PR.

## 3. Lifecycle Phases

Three durable phase values in `internal/lifecycle`:

```go
const (
    PhaseBootstrap   Phase = "bootstrap"
    PhaseMerging     Phase = "merging"      // NEW
    PhaseSteadyState Phase = "steady_state"
)
```

`merging` is the durable marker for "backfill drained, but the merge / cutover sequence has not yet completed." A process that observes `phase=merging` on startup re-enters the cutover state machine at the merge step.

## 4. State Machine

The orchestrator's `Run(ctx)` walks through these states. The two **commit points** are durable `WritePhase` calls; everything else is in-memory or filesystem state that's recoverable from the previous commit point.

```
                ┌─────────────────────────────────┐
                │ State 0: Bootstrap                │
                │  phase=bootstrap                  │
                │  backfill + bootstrap-live both   │
                │  running                          │
                └─────────────┬───────────────────┘
                              │ backfill drains (nil error)
                              ▼
                ┌─────────────────────────────────┐
                │ State 1: WritePhase(merging)     │ ← commit point #1
                │  pebble.Sync                      │
                └─────────────┬───────────────────┘
                              ▼
                ┌─────────────────────────────────┐
                │ State 2: Drain bootstrap-live    │
                │  cancel ctx, wait Run() return    │
                └─────────────┬───────────────────┘
                              ▼
                ┌─────────────────────────────────┐
                │ State 3: SealAndClose bootstrap  │
                │  bootstrap-live's writer seals    │
                │  its active live_segments file    │
                └─────────────┬───────────────────┘
                              ▼
                ┌─────────────────────────────────┐
                │ State 4: Close backfill writer   │
                │  flush, no seal                   │
                └─────────────┬───────────────────┘
                              ▼
                ┌─────────────────────────────────┐
                │ State 5: Merge (compact)         │
                │  TODO: stub no-op for this PR     │
                │  Future: read live_segments/*,    │
                │   drop events whose rev <=        │
                │   repo/<did>.BackfillRev,         │
                │   re-write survivors to segments/ │
                │   with new real seq numbers,      │
                │   delete live_segments/           │
                └─────────────┬───────────────────┘
                              ▼
                ┌─────────────────────────────────┐
                │ State 6: WritePhase(steady_state)│ ← commit point #2
                │  pebble.Sync                      │
                └─────────────┬───────────────────┘
                              ▼
                ┌─────────────────────────────────┐
                │ State 7: Steady-state run        │
                │  open new live.Consumer against   │
                │  data/segments/, seq/next,        │
                │  relay/cursor. Run until ctx done │
                └─────────────────────────────────┘
```

## 5. Sequence Numbers and Cursors

Three pebble keys are involved. The semantics differ across phases.

| Key | Role | Lifetime |
|---|---|---|
| `seq/next` | Permanent, source-of-truth seq counter for events in `data/segments/`. | Written by backfill writer in State 0; written by steady-state live consumer in State 7. Continues monotonically across cutover. |
| `live_segments/seq/next` | Throwaway seq counter used by the bootstrap-time live consumer for `backfill/live_segments/*.jss`. | Written only in State 0. Becomes orphan-but-harmless after State 4. The future compaction PR is responsible for cleanup. |
| `relay/cursor` | Upstream firehose cursor. | Written by the bootstrap-live consumer in State 0 (last write happens during State 2's drain). Read by the steady-state consumer at State 7 startup. Continues seamlessly across cutover by virtue of being the same key. |

**Critical invariant:** the throwaway seq numbers in `backfill/live_segments/*.jss` have no relationship to the real `seq/next` space and must never leak out. They exist only so the bootstrap-time live consumer can produce well-formed segment files. When future compaction reads those files, it discards the seq column and assigns fresh values from `seq/next`.

**At-least-once duplication:** because `relay/cursor` is shared between the two consumers, the steady-state consumer resumes from exactly where bootstrap left off. A small overlap (one block at most) of upstream events may have been written into both `backfill/live_segments/` (with throwaway seqs) and `data/segments/` (with real seqs) due to at-least-once redelivery during reconnect. This is by design — DESIGN.md §3.1.1's at-least-once invariant — and is correctly handled by future compaction's per-DID rev comparison against `repo/<did>.BackfillRev`.

## 6. Crash Recovery

| Persisted phase on startup | Orchestrator entry path | Behavior |
|---|---|---|
| `bootstrap` (or unset, treated as bootstrap) | Run states 0 → 7 | Normal flow. If backfill is already complete (all DIDs at StatusComplete), the engine drains immediately and the cutover sequence runs. The bootstrap-live consumer reattaches to `relay/cursor` and resumes normally. |
| `merging` | Run states 5 → 7 | Skip backfill and bootstrap-live entirely. Re-run merge stub (no-op today; idempotent in the future). WritePhase(steady_state). Run steady-state consumer. |
| `steady_state` | Run state 7 only | Open the steady-state live consumer against `data/segments/`. Run until ctx done. |

**Idempotency requirements:**

- States 2–4 are not re-run on `merging` recovery. Their effects are durable on disk before commit point #1: the bootstrap-live writer's segment is sealed (or sealable on next access), the backfill writer's data is flushed. If a crash lands between commit point #1 and any of these states, the on-disk artifacts are not strictly in the post-state-4 shape, but: the bootstrap-live consumer is not re-started in `merging` recovery, so its writer is never re-opened; the future compaction code in State 5 handles the existing `backfill/live_segments/` tree as-is regardless of whether its trailing file is sealed.
- State 5 (the merge stub) must be idempotent. Today it is trivially so — it does nothing. The follow-up compaction PR is required to make its real implementation idempotent against partial completion.
- State 6's WritePhase is itself a single pebble.Sync; either it lands or it doesn't.

## 7. Architecture

### 7.1 Package Layout

```
internal/lifecycle/
  phase.go                        MODIFIED: add PhaseMerging
  phase_test.go                   MODIFIED: include merging in round-trips

internal/ingest/
  writer.go                       MODIFIED: add SealActiveAndClose method

internal/ingest/orchestrator/     NEW
  doc.go                          package overview tying back to DESIGN.md §4.2/§4.3
  config.go                       Config + validate
  errors.go                       sentinel errors
  metrics.go                      phase gauge, transition counter, state durations
  orchestrator.go                 Orchestrator struct, New, Run
  states.go                       per-state private methods
  bootstrap.go                    runBootstrap: builds backfill engine + bootstrap-live consumer, runs them, awaits drain
  merge.go                        runMerge: stub no-op for this PR; the future compaction PR edits this file
  steady.go                       runSteadyState: builds the steady-state consumer and runs until ctx done

  orchestrator_test.go            end-to-end happy path: bootstrap → merging → steady_state
  bootstrap_test.go               unit tests for the bootstrap-phase orchestration
  states_test.go                  per-state unit tests with injected dependencies
  recovery_test.go                resume-from-merging, resume-from-steady-state
  config_test.go
  metrics_test.go

cmd/jetstream/
  main.go                         MODIFIED: drop the inline backfill+live wiring
                                  in runServe; construct only the orchestrator
                                  and shared primitives (verifier, directory,
                                  store, metrics). One errgroup goroutine for
                                  orchestrator.Run.
```

### 7.2 Boundary Discipline

- `internal/ingest/orchestrator` imports `internal/ingest`, `internal/ingest/backfill`, `internal/ingest/live`, `internal/lifecycle`, `internal/store`, `internal/obs`, plus the atmos primitives the contained subsystems need (xrpc, identity, sync). It is the only place in the codebase that imports both `backfill` and `live` together.
- `internal/lifecycle` gains one constant. No interface change.
- `internal/ingest` gains one method on `Writer`. No new fields or config knobs.
- `cmd/jetstream` no longer imports `internal/ingest/backfill` or `internal/ingest/live` directly. Both are now orchestrator-internal concerns.

### 7.3 Public API of the Orchestrator Package

```go
type Config struct {
    // Filesystem and store
    DataDir   string
    Store     *store.Store

    // Upstream
    RelayURL   string
    HTTPClient *http.Client
    Directory  *identity.Directory
    Verifier   *atmossync.Verifier

    // Observability
    Logger          *slog.Logger
    IngestMetrics   *ingest.Metrics
    LiveMetrics     *live.Metrics
    BackfillMetrics *backfill.Metrics
    Metrics         *Metrics    // orchestrator-level metrics
}

type Orchestrator struct { /* unexported */ }

func New(cfg Config) (*Orchestrator, error)

// Run reads the persisted phase from cfg.Store and dispatches to the
// appropriate entry path. Phase transitions during a single Run are
// handled internally — callers see one Run that returns when ctx is
// cancelled or the steady-state consumer exits.
func (o *Orchestrator) Run(ctx context.Context) error
```

That is the entire public surface.

### 7.4 Internal Construction Per Phase

`runBootstrap(ctx)` constructs:

- `ingest.Writer` for `data/segments/` (the shared backfill writer; passed to backfill.Run as `Writer`, and re-used in nothing else after State 4 closes it).
- `live.Consumer` pointed at `data/backfill/live_segments/` with `SeqKey="live_segments/seq/next"`, `CursorKey="relay/cursor"`.
- An internal errgroup that runs both. When backfill returns nil, it cancels the bootstrap-live consumer's derived context, awaits its return, then walks States 1–6.

`runMerge(ctx)` is a no-op for this PR. It returns nil. A `// TODO(merge): see DESIGN.md §4.2` comment with a list of invariants the implementation must satisfy lives at the top of `merge.go`.

`runSteadyState(ctx)` constructs:

- A fresh `ingest.Writer` for `data/segments/` (this is the *second* time the backfill writer's directory has been opened — `ingest.Open` runs `ScanMaxSeq` and reconciles `seq/next` against the last block in the latest active segment, so the steady-state writer continues exactly where backfill left off).
- A fresh `live.Consumer` with `SegmentsDir=data/segments/`, `SeqKey="seq/next"`, `CursorKey="relay/cursor"`. This consumer's underlying `ingest.Writer` is the one constructed above.
- Runs the consumer's `Run(ctx)` until ctx is cancelled.

Both `runBootstrap` and `runSteadyState` are responsible for closing the resources they constructed (via `defer`).

## 8. Modifications Outside the New Package

### 8.1 `internal/lifecycle`

```go
const PhaseMerging Phase = "merging"
```

Add to the `valid()` switch and to `phase_test.go`'s round-trip cases.

### 8.2 `internal/ingest.Writer`

```go
// SealActiveAndClose flushes any pending block, seals the active segment
// (writes the variable-length footer and finalizes the fixed header),
// persists nextSeq, and closes the writer. Idempotent.
//
// Used by the orchestrator at cutover time to finalize the bootstrap-phase
// live_segments writer's trailing active file. Steady-state callers should
// continue to use Close() instead — sealing is otherwise a rotation-time
// concern, not a Close-time one.
func (w *Writer) SealActiveAndClose() error
```

Implementation: under `w.mu`, if not closed and `w.active != nil`, call `w.active.Flush()` (if needed), then `w.active.Seal()`, then mark closed and persist `seq/next`. The existing `flushAndRotateLocked` already exercises Flush+Seal+pebble.Sync at rotation; we factor those operations so `SealActiveAndClose` reuses them.

### 8.3 `cmd/jetstream/main.go`

`runServe` shrinks substantially. The new shape:

1. Logger, tracing, metrics setup (unchanged).
2. Open the metadata store (unchanged).
3. Construct the verifier, identity directory, HTTP client (unchanged — these are shared across phases, and the verifier's async-error drain goroutine is sibling to the orchestrator).
4. Construct the orchestrator with the above primitives.
5. Run the server, the orchestrator, and the verifier-drain under one errgroup. No phase switch here.

The current "phase=steady_state → fail" early return is removed; the orchestrator handles all three phases.

## 9. Error Handling

| Phase | Error source | Handling |
|---|---|---|
| Bootstrap | Backfill engine returns non-nil | Internal errgroup cancels; orchestrator returns the wrapped error. No phase change. Process exits. |
| Bootstrap | Bootstrap-live consumer returns non-nil | Internal errgroup cancels; orchestrator returns the wrapped error. No phase change. |
| Bootstrap | Both return nil but ctx is done | Orchestrator returns ctx.Err(). No phase change. Restart resumes bootstrap normally. |
| State 1 | WritePhase(merging) fails | Return error wrapped. Phase remains `bootstrap`. Restart re-runs bootstrap; if backfill was already complete, the engine drains immediately and cutover retries. |
| State 2 | bootstrap-live's `Close()` returns error after drain | Return error wrapped. Phase is `merging`. Restart enters `runMerge`. |
| State 3 | `SealActiveAndClose` fails | Return error wrapped. Phase is `merging`. Restart enters `runMerge`; the segment is left in whatever state — `runMerge` does not depend on it being sealed. |
| State 4 | backfill writer Close fails | Return error wrapped. Same recovery as State 3. |
| State 5 | Merge stub returns nil unconditionally | n/a today. Future compaction implementation must make its error path idempotent on retry. |
| State 6 | WritePhase(steady_state) fails | Return error wrapped. Phase remains `merging`. Restart re-runs merge stub (no-op) and retries. |
| State 7 | Steady-state consumer returns | Propagate. ctx.Canceled means clean shutdown. |

**Verifier behavior is unchanged:** verifier async-errors are diagnostic, not fatal. They are warning-logged with metrics incremented and the verifier's existing `OnVerificationFailure` hook continues to fire. Verification failures typically reflect adversarial or malformed PDS input, not jetstream bugs, and must never crash the process.

## 10. Observability

### 10.1 Metrics (orchestrator package)

- `jetstream_orchestrator_phase` (gauge): 0=bootstrap, 1=merging, 2=steady_state. Sampled.
- `jetstream_orchestrator_phase_transitions_total` (counter, labels `from_phase`, `to_phase`).
- `jetstream_orchestrator_state_duration_seconds` (histogram, label `state`): how long each state took. State labels are `drain_bootstrap`, `seal_bootstrap`, `close_backfill`, `merge`, `write_phase_steady`.

### 10.2 Logs

INFO at every phase transition with explicit boundary markers:

- `"orchestrator: starting"` with the read phase
- `"orchestrator: cutover begin"` (entering State 1)
- `"orchestrator: phase=merging"` (after commit point #1)
- `"orchestrator: bootstrap consumer drained"`
- `"orchestrator: bootstrap segment sealed"`
- `"orchestrator: backfill writer closed"`
- `"orchestrator: merge complete"`
- `"orchestrator: phase=steady_state"` (after commit point #2)
- `"orchestrator: steady-state consumer running"`

Per-state durations are logged at the state's terminal log line.

### 10.3 Tracing

A single span `orchestrator.cutover` covering States 1–6, with child spans per state. The bootstrap and steady-state phases produce their own per-event spans inside the wrapped `live.Consumer`'s tracing path (unchanged).

## 11. Testing

### 11.1 Per-State Unit Tests (`states_test.go`)

Each state's private function is structured to take its dependencies as arguments. Tests inject fake collaborators (e.g. a fake live.Consumer that records cancel calls; a fake ingest.Writer that records SealActiveAndClose calls) and verify the state's behavior in isolation.

### 11.2 End-to-End Happy Path (`orchestrator_test.go`)

A test that:

1. Constructs a real pebble store, a fake atmos streaming source (in-process WS server, same pattern as the existing live consumer test), a fake backfill engine that drains after producing some events.
2. Calls `Orchestrator.Run(ctx)`.
3. Asserts:
   - Phase progresses bootstrap → merging → steady_state.
   - The trailing `backfill/live_segments/seg_*.jss` is sealed.
   - The steady-state writer's `seq/next` continues from the backfill writer's last allocation.
   - Steady-state events land in `data/segments/` with monotonically-increasing seq.
   - Cancelling ctx during steady-state returns ctx.Err() and shuts down cleanly.

### 11.3 Recovery Tests (`recovery_test.go`)

For each of the three persistent phase values, a test that:

1. Pre-populates the store with that phase plus on-disk segment files representative of that recovery point.
2. Calls `Orchestrator.Run(ctx)`.
3. Asserts the correct entry path is taken, no bootstrap or merge work re-runs spuriously, and the orchestrator settles into the steady-state run loop.

### 11.4 Bootstrap-Phase Tests (`bootstrap_test.go`)

Tests of `runBootstrap` covering:
- Backfill engine error propagation
- Bootstrap-live consumer error propagation
- Clean drain when ctx is cancelled before backfill completes
- Drain ordering (bootstrap-live cancellation does not happen before backfill returns)

### 11.5 No New `cmd/jetstream` Integration Test

The existing `cmd/jetstream/serve_test.go` covers process startup. It will be extended to assert that all three phase values lead to a successful start (the current "fail on steady_state" assertion is replaced).

## 12. Open TODOs (deferred to follow-up PRs)

1. **Compaction implementation** in `internal/ingest/orchestrator/merge.go`. Must read `backfill/live_segments/*.jss`, drop events whose rev is `<= repo/<did>.BackfillRev`, write surviving events to `data/segments/` with new seq numbers from `seq/next`, and be idempotent under partial completion. Code comment in `merge.go` enumerates these requirements.
2. **Cleanup of `backfill/`** including the orphan `live_segments/seq/next` pebble key. The fate of the entire `backfill/` directory will be decided alongside compaction.
3. **Failed-DID retry in steady state** (DESIGN.md §4.3). To be wired into `runSteadyState` once that PR lands.
4. **Replication signals during cutover** (DESIGN.md §6). No `segment_compacted` or analogous events fire during the cutover described here, since no compaction is performed.

## 13. Migration Notes

- A data directory currently in `phase=bootstrap` will, on first run of the new code, walk the cutover state machine the moment backfill drains. No operator action is required.
- A data directory that's already had `phase=steady_state` written by the old "we don't support this" gate (only possible if an operator hand-edited pebble) will start running the steady-state consumer immediately. There's no version-ratcheting for this — it's purely a code-path that was previously fatal becoming functional.
- The cutover does not depend on any specific atmos version and adds no new dependencies.
