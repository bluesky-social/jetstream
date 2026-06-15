# Oracle Robustness Roadmap

Date: 2026-06-15
Status: draft for review

## Purpose

Make the oracle simulator a much stronger bug-finding tool for Jetstream. The
goal is not perfect deterministic simulation testing. Go's runtime, real
networking, Pebble, filesystem behavior, and third-party libraries make true
bit-identical execution expensive enough that it should not be the next
milestone.

The near-term goal is higher confidence:

- stronger assertions over the storage and replay contracts,
- better fault and restart coverage,
- deterministic-enough inputs,
- failure artifacts that explain what happened when a run fails,
- empirical measurement through the mutation campaign.

This document folds together the current oracle code review,
`internal/oracle/DETERMINISM_DESIGN.md`, `ORACLE_TODO.md`, and
`testing/mutation/RESULTS.md`.

## Design Principles

1. **Assertion power before schedule control.**
   A perfectly reproducible weak oracle is still weak. Close known detection
   gaps before spending large effort on fake transports or scheduler control.

2. **Product contract before storage internals.**
   Segment files are an implementation detail. Clients consume replay through
   `/subscribe` and XRPC segment downloads. The oracle must validate those
   paths.

3. **Traceability is the practical substitute for perfect DST.**
   When a failure is interleaving-dependent, a seed alone is not enough. Every
   oracle run should emit a compact structured trace of the decisions and
   durable transitions that matter.

4. **Keep tiers separate.**
   Logical correctness, replay correctness, crash durability, adversarial
   failure handling, and long-horizon soak behavior need different test modes.
   Mixing them into one monolithic test makes failures harder to interpret.

5. **Do not weaken production semantics for test determinism.**
   Restart/crash tests need real process and persistence behavior. In-memory
   or fake I/O is useful for logical and replay tiers, but must not replace the
   durability tier.

6. **Every major improvement should kill or explain a mutant.**
   The mutation campaign is the scorecard. New oracle work should map to known
   escapes or add new mutants that document the bug class being targeted.

## Current Strengths

The existing oracle is already valuable:

- `GroundTruthFromWorld` walks the simulator's repo/MST state independently of
  Jetstream's emitted segment files.
- The simulator emits real atproto wire frames and CAR/MST payloads through the
  live ingestion stack.
- The harness has phase gates, durable append acknowledgements, and
  anti-vacuity checks for fault injection.
- Restart coverage uses a real subprocess and `SIGKILL`, not an in-process
  mock crash.
- `CheckCompacted` re-derives compaction correctness rather than trusting the
  compactor.
- The curated mutation campaign has already measured several real oracle gaps.

This roadmap preserves those strengths and builds around them.

## Current Weaknesses

The current oracle is weaker than it should be in these areas:

- It mostly checks final materialized repo state. Intermediate event loss can
  be hidden when a later event converges the final state.
- The product replay path is not a primary oracle observation.
- The mutation campaign found concrete escapes:
  empty commit revs, block offset bookkeeping, compaction boundary handling,
  merge-cursor crash seams, store-fault paths, and footer/bloom read indexes.
- The harness has useful hooks, but no single typed trace tying simulator
  inputs, faults, runtime transitions, append/flush/seal/cursor events,
  compaction, replay, shutdown, and restart together.
- The simulator is intentionally polite: bounded-success faults, limited data
  diversity, little malformed input, no identity path coverage, no hard
  failure-contract modes.
- `DETERMINISM_DESIGN.md` correctly identifies runtime nondeterminism, but
  transport/synctest work should follow stronger assertions and traceability,
  not precede them.

## Target Architecture

The oracle should become a set of related tiers sharing common building blocks:

- **Scenario config:** seed, scale, fault mode, restart mode, replay mode.
- **Simulator world:** deterministic world generation plus richer event/fault
  generation.
- **Runtime driver:** starts Jetstream, controls barriers, starts and stops
  child processes for restart tests.
- **Trace recorder:** captures canonical events from the simulator, harness,
  Jetstream hooks, and observers.
- **Observers:** filesystem segment observer, `/subscribe` observer, XRPC
  segment-download observer, metadata observer, metrics observer.
- **Checkers:** final-state comparison, event-log equivalence, physical
  invariants, replay invariants, compaction invariants, failure-contract
  assertions.
- **Artifacts:** trace JSONL, summarized failure report, seed/config, observed
  event digests, segment manifest digest, optional failing replay frames.

The existing package can evolve toward this without a large rewrite. Add small
components beside the current harness, then migrate `TestOracle_DefaultLifecycle`
to use them.

## Workstream 1: Close Known Assertion Gaps

### Rationale

The mutation campaign found near-free failures the oracle should catch today.
These are not determinism problems. They are missing assertions.

### What It Buys

- Immediate improvement in bug-detection power.
- A cleaner baseline before larger refactors.
- Fast feedback because these checks live in the existing default/stress tier.

### Scope

Fix the known mutation escapes that require no new runtime infrastructure:

- Reject empty `Rev` on create/update/delete events.
- Check recorded segment block offsets, not only block decode-by-index.
- Strengthen compaction boundary assertions so rows at exactly the boundary are
  evaluated.
- Add tests proving each checker rejects the corresponding corrupt shape.

### Lightweight Implementation Guidance

- Extend `CheckInvariants` rather than placing these checks in the harness.
  The invariant checker is already the canonical physical-stream gate.
- Add a small sealed-segment structural checker used by `ObserveSegments`.
  It should verify block offsets are monotonic, in range, and consistent with
  block sizes where available.
- Add a focused `CheckCompacted` test case where the superseding event lands at
  exactly the committed watermark/chunk boundary.
- Keep diagnostics specific: include seq, file, block index, DID, collection,
  rkey, and phase where possible.

### Verification

- Unit tests under `internal/oracle`.
- Re-run the relevant mutation mutants:
  `m018`, `m010`, and `m007`.
- Run `just test ./internal/oracle`.

## Workstream 2: Add Canonical Oracle Traces

### Rationale

Seeds currently fix inputs, not interleavings. When a failure depends on timing,
the failure artifact must explain the relevant ordering. Logs are not enough:
they are unstructured, incomplete, and not designed for comparison.

### What It Buys

- Failed CI runs become diagnosable without reproducing the exact schedule.
- Trace digests allow "same logical run or not?" comparisons.
- Future random-kill and adversarial-fault modes can assert over trace facts.
- The trace becomes an executable checklist of what the oracle believes it
  exercised.

### Scope

Introduce an `oracle.Trace` recorder with canonical event records:

- run metadata: seed, mode, Go version, GOMAXPROCS, config, fault mode;
- simulator events: generated seq, event kind, DID, rev, op count, payload
  digest;
- scheduled faults and fired faults;
- phase transitions and phase barriers;
- backfill repo discovered/complete/fail where hooks exist;
- append events after durable append;
- writer flush/seal/rotate where hooks exist or can be added;
- cursor saves;
- compaction pass start/result/watermark/rewrites;
- replay observations;
- shutdown/restart/crash markers.

### Lightweight Implementation Guidance

- Use JSONL so large stress traces can stream without holding everything in
  memory.
- Use bounded fields and hashes for payloads. Do not dump full CARs or full
  records by default.
- Make `Trace.Record` goroutine-safe. Assign a local monotonic trace index so
  the trace preserves observation order even when event timestamps are equal.
- Keep trace emission nil-safe and low overhead. Production should not depend
  on it.
- Prefer existing hooks first: `OnBootstrapLiveEvent`, `OnSteadyStateEvent`,
  `OnCompactionPass`, phase barriers, crash injector, fault plan counters.
- Add new hooks only where a trace fact cannot be inferred reliably.

### Verification

- Unit test canonical JSON output and payload digest stability.
- Assert that a default oracle run records required event categories.
- On failure, print trace path and a short digest in the test error.
- Add a mutation or test that disables a fault and confirm trace/anti-vacuity
  catches it.

## Workstream 3: Event-Log Equivalence

### Rationale

Final-state comparison can hide stream infidelity. A dropped intermediate
update may not change the final repo state if a later update supersedes it,
but clients replaying the stream would still observe a hole.

The simulator already persists firehose frames by seq. That gives us an
independent expected log for live-generated events.

### What It Buys

- Detects missing intermediate events.
- Separates "final state correct" from "stream contract correct."
- Improves detection of merge, dedupe, cursor, shutdown, reconnect, and replay
  bugs.

### Scope

Build an expected event log from simulator firehose history plus bootstrap repo
state, then compare observed Jetstream events after normalization.

The first version should cover:

- steady-state live events,
- bootstrap-live overlap events,
- account delete and sync events generated by the oracle scenario,
- expected compaction removal rules at or below watermarks.

### Lightweight Implementation Guidance

- Start with live/steady events because simulator firehose seqs and Jetstream
  `UpstreamRelayCursor` already line up.
- Normalize events to a small comparable shape:
  upstream seq, kind, DID, collection, rkey, rev, payload hash.
- For compaction-aware comparison, allow superseded rows to be absent only when
  the committed compaction watermark and tombstone rules justify absence.
- Keep final-state comparison as a separate checker. Event-log equivalence
  should not replace it.

### Verification

- Unit tests where final state converges but event log is missing an
  intermediate update.
- Stress oracle should fail with a clear "missing expected event" diagnostic.
- Add or refresh mutants for intermediate event loss.

## Workstream 4: Promote Replay Path To Primary Oracle Surface

### Rationale

Jetstream's product contract is replay. Reading segment files directly is
necessary for storage validation, but insufficient for client correctness.
The current oracle only samples `/subscribe` replay around compaction.

### What It Buys

- Validates hot tail / cold reader handoff.
- Exercises cursor handling, JSON encoding, extended event shapes, pending
  writer snapshots, manifest refresh, and compaction cache invalidation.
- Catches footer/bloom/read-index blind spots documented by `m015` and `m016`.

### Scope

Add replay observers that collect events through public surfaces:

- `/subscribe?extended=true&cursor=0`;
- `/subscribe` from mid-stream cursors around block, segment, and compaction
  boundaries;
- selected filtered subscriptions;
- XRPC `listSegments` plus `getSegment` download and independent decode.

### Lightweight Implementation Guidance

- Reuse `collectSubscribeReplay`, but turn it into a reusable observer with
  explicit target criteria and better failure reports.
- Compare replay observations to the same normalized event stream used by
  event-log equivalence.
- Exercise cursors just before, at, and after known boundary seqs: block
  starts, block ends, segment starts, segment ends, compaction watermark.
- Do not hide `/subscribe` failures by falling back to filesystem observation.
  A replay-path failure is a product failure.
- Keep direct filesystem observation in the storage tier so storage and replay
  failures can be distinguished.

### Verification

- Unit/integration tests for observer normalization.
- Oracle run must compare filesystem observation and replay observation.
- Mutants `m015` and `m016` should either be killed or explicitly reclassified
  with evidence if the public path still cannot observe them.

## Workstream 5: XRPC Segment Egress Oracle

### Rationale

Backfill clients download segment files through XRPC and CDN-like paths. A
file can be correct on disk and still be served incorrectly through headers,
caching, manifest state, or handler logic.

### What It Buys

- Validates the archive download contract.
- Exercises manifest readiness and cache invalidation after compaction.
- Adds independent pressure on segment headers, footers, block indexes, and
  checksums.

### Scope

After each major phase, fetch the server's segment list and download every
eligible segment through XRPC. Decode those bytes as a client would and compare
to filesystem observation and expected event log.

### Lightweight Implementation Guidance

- Use the public HTTP listener, not internal manifest pointers.
- Include segment metadata in trace: index, byte length, header checksum, max
  seq, ETag/checksum if exposed.
- Validate that compacted segments served after refresh match on-disk headers.
- Keep the first version simple: full download, no range requests unless the
  API already exposes them as a first-class contract.

### Verification

- Add an oracle assertion that XRPC-observed events match filesystem-observed
  events for all sealed segments.
- Add a targeted test that compaction changes a segment and the XRPC observer
  sees the rewritten version.

## Workstream 6: Deterministic-Enough Inputs

### Rationale

Full scheduler control is not realistic in the near term, but several current
inputs can be made more stable cheaply. Removing avoidable nondeterminism makes
traces easier to compare and failures easier to interpret.

### What It Buys

- More stable traces across machines.
- Less noisy failure diagnosis.
- Better foundation for future `synctest` or in-process transport work.

### Scope

Fix controllable nondeterminism:

- simulator account events should use the logical clock instead of
  `time.Now`;
- oracle-configured retry paths should disable jitter or use seeded jitter;
- map-derived trace output should be sorted;
- timeouts should be event-driven where practical;
- all simulator RNG use should remain single-owner or explicitly locked.

### Lightweight Implementation Guidance

- Do not introduce a broad production `Clock` interface yet. Start with oracle
  seams that already exist (`live.Config.now`, simulator logical clock).
- Add `now` plumbing only where it directly affects oracle-observed bytes or
  trace stability.
- Prefer event acks and barriers over polling loops when a runtime hook exists.
- Keep real wall-clock timeouts as deadlock guards, not as success criteria.

### Verification

- Unit test simulator account event times for seed stability.
- Run the same small oracle seed multiple times and compare trace digests for
  deterministic input sections.

## Workstream 7: In-Process Simulator Transport

### Rationale

Real sockets introduce kernel scheduling and EOF/flush timing. This is a known
source of nondeterministic behavior for injected network faults, especially
truncated CAR bodies and websocket disconnects.

### What It Buys

- More reproducible fault behavior.
- Faster tests by removing loopback networking overhead.
- Better control over failure shapes: clean short body, abrupt read error,
  status response, malformed frame, delayed frame.

### Scope

Build an oracle-only transport option for simulator endpoints:

- listRepos;
- getRepo;
- PLC/DID document resolution;
- subscribeRepos.

### Lightweight Implementation Guidance

- Start with HTTP `RoundTripper` for listRepos/getRepo/PLC. This is lower-risk
  than replacing websocket behavior.
- Model fault types explicitly. Do not collapse all truncations into "half
  bytes and EOF"; clean EOF and abrupt read error are different client-visible
  failures.
- Keep real socket mode available. It remains useful as an end-to-end surface
  check.
- Do not route public `/subscribe` replay through fake transport; that tier is
  specifically about public serving behavior.

### Verification

- Existing oracle passes in socket mode and in-process mode.
- Fault trace shows the same scheduled fault decisions in both modes.
- Truncated-CAR tests assert exact error/fault shape.

## Workstream 8: Random-Time Restart Kill Loop

### Rationale

Enumerated crashpoints cover seams we already anticipated. The hard bugs often
live between named checkpoints. The existing restart harness already has most
of the subprocess and kill machinery needed for random-time kills.

### What It Buys

- Finds crash/restart bugs without needing to name every crashpoint.
- Covers merge-cursor and mid-operation timing classes like mutation `m003`.
- Exercises real OS process death and persistence recovery.

### Scope

Add a restart tier that:

- starts a child with a seeded scenario;
- waits for a seeded duration, trace event count, or phase/event predicate;
- sends `SIGKILL`;
- restarts;
- asserts storage, event-log, replay, and compaction invariants.

### Lightweight Implementation Guidance

- Prefer trace-event-count kill points over wall-clock-only kill points when
  possible. Wall-clock should remain as an exploration mode.
- Persist the kill decision in the parent trace: seed, child PID, phase, trace
  count, elapsed time.
- Keep the number of iterations configurable. CI can run a small count; nightly
  can run more.
- Ensure the child writes enough trace data before kill by flushing trace
  records or using line-buffered JSONL.

### Verification

- Add a deterministic test mode with one fixed kill point.
- Add a nightly recipe for multiple random kills.
- Add or refresh merge-cursor crash mutants and require this tier to kill them.

## Workstream 9: Store-Fault Oracle Tier

### Rationale

Some dangerous bugs only occur when local persistence calls fail. The current
oracle has no way to make store writes fail, so mutation `m006` survived by
construction.

### What It Buys

- Tests the "crash loud, do not corrupt" contract for local persistence
  failures.
- Forces error propagation paths that are currently dormant under normal runs.
- Improves confidence in rollback/retry/restart behavior around Pebble,
  cursor saves, manifest refresh, compaction watermark commits, and repo status
  commits.

### Scope

Introduce a test-only faulting store wrapper or fault hooks at the store
boundary. Fault points should cover:

- repo status writes;
- listRepos cursor writes;
- seq/next writes;
- relay cursor writes;
- syncstate flush/commit;
- compaction watermark writes;
- manifest refresh/update after compaction.

### Lightweight Implementation Guidance

- Keep production store APIs clean. Prefer an interface/wrapper only where the
  code already accepts a store dependency.
- Faults should be deterministic by operation name and ordinal count.
- The assertion should be phase-specific:
  some faults should abort startup/run;
  some should leave the previous durable state intact;
  none should allow silent cursor advancement past unarchived data.
- Record every injected and consumed fault in the oracle trace.

### Verification

- Unit test the fault injector.
- Add one oracle scenario per high-risk persistence boundary before expanding.
- Re-run mutation `m006` and add new mutants for swallowed persistence errors.

## Workstream 10: Simulator Fidelity Expansion

### Rationale

The simulator currently exercises a valuable but narrow slice of upstream
behavior. Production relays and PDSes produce identity changes, account status
changes, malformed or partial data, unknown lexicons, unicode, oversized
fields, sequence anomalies, and failure modes beyond bounded transient errors.

### What It Buys

- Covers dark paths in live ingest and subscribe encoding.
- Tests documented "invalid external input must not crash the daemon" policy.
- Improves confidence that the oracle catches real network diversity, not only
  the polite happy path.

### Scope

Add scenario-controlled generation for:

- `#identity` events;
- account statuses: active, deactivated, takedown, deleted;
- unicode and near-limit records;
- unknown lexicons;
- over-width fields that must be dropped and counted;
- commits with missing CAR blocks;
- malformed but bounded frames;
- sequence gaps and duplicate frames in adversarial modes.

### Lightweight Implementation Guidance

- Keep default mode conservative. Add fidelity knobs so new adversarial cases
  can be enabled in stress/nightly modes first.
- For each invalid-input case, define the expected contract:
  archive surviving good events, drop unarchivable event, advance cursor or not,
  increment metric, log bounded fields, no process crash.
- Do not let invalid upstream data silently coerce into valid archive rows.
- Add trace counters for dropped/ignored cases so coverage is visible.

### Verification

- Unit tests in simulator world/http packages for generated frame shapes.
- Live ingest tests for each invalid-input contract.
- Oracle stress mode should assert expected drop counters or trace events.

## Workstream 11: Real-Data Corpus

### Rationale

The simulator and Jetstream share `atmos` for protocol encoding/decoding. A
symmetric bug in that shared library can pass the oracle. Real network bytes
from diverse implementations are the practical way to break that closed loop.

### What It Buys

- Detects disagreement with production atproto implementations.
- Covers protocol edge cases the simulator will not invent.
- Provides stable regression fixtures for historically observed bad inputs.

### Scope

Build a small committed or externally fetched corpus:

- firehose frames from production;
- representative getRepo CARs from diverse PDS implementations;
- known malformed or partial cases with expected handling;
- golden normalized outputs.

### Lightweight Implementation Guidance

- Keep privacy and size constraints explicit. Store only what is acceptable for
  the public repo, or provide a documented fetch step for non-committed corpus
  data.
- Normalize outputs to hashes and event metadata where possible.
- Run corpus tests outside the main oracle lifecycle; they are a different
  independence check.

### Verification

- CI runs a small corpus.
- A larger optional corpus can run nightly or manually.
- Add corpus cases when production incidents occur.

## Workstream 12: Long-Horizon Soak Oracle

### Rationale

Single lifecycle tests miss accumulation bugs: watermark drift, manifest/cache
growth, goroutine leaks, compaction lag, tombstone memory growth, repeated
restart effects, and slow cursor divergence.

### What It Buys

- Finds bugs that require multiple bootstrap/steady/compact/restart cycles.
- Gives operational confidence for a long-lived daemon.
- Exercises cleanup and idempotency repeatedly.

### Scope

Add a weekly or manual soak mode:

- multiple steady-state traffic epochs;
- repeated compaction passes;
- repeated restart or random-kill cycles;
- replay checks after each epoch;
- final full storage/replay/event-log comparison.

### Lightweight Implementation Guidance

- Keep it out of normal CI.
- Emit periodic trace checkpoints and metrics snapshots.
- Bound wall-clock runtime and data size.
- Fail on leaks or monotonic growth where a bounded invariant exists.

### Verification

- Manual `just oracle-soak` recipe.
- Document expected runtime and machine requirements.
- Treat every failure as a real investigation, not a flaky test to ignore.

## Workstream 13: Determinism Experiments

### Rationale

`testing/synctest` and deeper fake-clock/fake-transport work may be useful, but
only after the oracle has stronger assertions and clearer artifacts. Otherwise
we risk building a reproducible harness that still misses important bugs.

### What It Buys

- Potentially faster and more reproducible logical tests.
- Better isolation for retry/timer-heavy code.
- A path toward deterministic subsets without committing to a full executor
  rewrite.

### Scope

Prototype narrow uses of:

- `testing/synctest` around isolated concurrent units with no real I/O;
- fake transport plus fake time around bootstrap/listRepos/getRepo;
- in-memory Pebble/VFS for non-crash logical tests only.

### Lightweight Implementation Guidance

- Do not put restart durability tests inside `synctest`.
- Do not replace public serving tests with fake I/O.
- Start with one small bootstrap slice and measure reliability, complexity,
  and runtime.
- Keep the single-goroutine deterministic executor as an explicit long-term
  architectural decision, not an accidental refactor.

### Verification

- Prototype result document: what became deterministic, what remained
  nondeterministic, runtime impact, code churn.
- Adopt only if it reduces flakes or makes a class of failures materially more
  diagnosable.

## Suggested Delivery Order

### Milestone A: Stronger Existing Oracle

1. Close mutation assertion gaps.
2. Add canonical trace recorder.
3. Make simulator account event times deterministic.
4. Add event-log equivalence for live/steady events.

This is the first milestone because it improves detection and diagnostics
without changing the transport or runtime architecture.

### Milestone B: Product-Path Oracle

1. Promote `/subscribe` replay comparison.
2. Add XRPC segment egress comparison.
3. Add cursor-boundary replay scenarios.
4. Re-run footer and bloom mutants, then either kill them or reclassify them
   with specific evidence.

This validates what clients actually consume.

### Milestone C: Crash And Fault Depth

1. Add random-time restart kill loop.
2. Add store-fault injection.
3. Add merge-cursor and persistence-failure mutants.

This targets durability and partial-failure bugs.

### Milestone D: Fidelity And Independence

1. Add identity/account-status/adversarial simulator cases.
2. Add real-data corpus.
3. Add invalid-input contract assertions.

This breaks polite-simulator assumptions and the atmos closed loop.

### Milestone E: Determinism And Longevity

1. Add in-process simulator transport.
2. Prototype `synctest` where it fits.
3. Add long-horizon soak mode.

This improves reproducibility and operational confidence once the oracle's eye
is strong enough to justify the investment. These modes supplement the real
socket/filesystem durability tiers; they do not replace them.

## Success Criteria

The roadmap is successful when:

- every known "fix the oracle" mutation escape has either been killed or
  deliberately reclassified with evidence;
- a failed oracle run produces a trace artifact sufficient to identify the
  phase, fault, cursor/watermark, and first divergent event;
- storage observation, `/subscribe` replay, and XRPC segment egress agree for
  the same scenario;
- event-log equivalence catches intermediate event loss that final-state
  comparison misses;
- random restart and store-fault tiers run independently of the normal
  success-path lifecycle;
- simulator fidelity modes cover identity, account status, invalid input, and
  oversized/drop behavior;
- the mutation campaign scorecard improves and remains part of the release
  discipline.

## Non-Goals

- Full FDB/TigerBeetle-style deterministic scheduling in the next iteration.
- Replacing crash/restart tests with in-memory storage.
- Hiding real production failure contracts behind fake transports.
- Making one giant oracle test that covers everything at once.
- Treating a green oracle as proof of correctness. It is a high-value bug
  detector, not a proof system.

## Open Questions

1. Should oracle traces live only under `t.TempDir`, or should failed CI runs
   copy them into a predictable artifact path?
2. Which public XRPC segment APIs are stable enough to treat as part of the
   oracle product contract today?
3. How much real production corpus data can be committed safely, and what must
   be fetched out-of-band?
4. Should store-fault injection wrap `internal/store.Store`, Pebble directly,
   or specific high-risk call sites first?
5. What minimum mutation kill-rate or escape disposition standard do we want
   before treating the upgraded oracle as release-blocking?
