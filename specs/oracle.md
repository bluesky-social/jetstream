# Oracle And Simulator Design

## Purpose

Jetstream v2 is a durable archive and replay service. The oracle simulator
exists to catch bugs that ordinary unit tests and happy-path integration tests
will miss: data loss, replay holes, compaction mistakes, restart corruption,
cursor drift, and failure handling that silently advances past unarchived data.

The oracle is not a proof of correctness. It is a high-value bug detector. Its
job is to create realistic enough atproto traffic, drive Jetstream through the
same public and persistence paths clients rely on, and compare the result
against an independently derived model. A green oracle means one set of strong
contracts held for one scenario. It does not mean every interleaving,
transport failure, protocol edge case, or production data shape has been
covered.

This document is the durable design guide for that testing system. It explains
why the oracle and simulator are structured the way they are, what requirements
future changes must preserve, and how to extend the system without weakening
its bug-finding value. Active work is tracked in GitHub issues under the
testing epic, not in this document.

## Design Goals

### Test The Product Contract

Jetstream's product is replay. Segment files are the storage format, but
clients consume data through `/subscribe` and XRPC segment download paths. The
oracle must therefore validate both:

- storage correctness: what Jetstream wrote to disk is physically and
  logically valid;
- product-path correctness: what clients can replay through public APIs is the
  same stream after applying the documented compaction and tombstone rules.

Direct segment observation remains valuable because it distinguishes storage
bugs from serving bugs. It must not be the only observation surface.

### Prefer Independent Checks

The strongest oracle checks do not ask Jetstream what it thinks happened. They
derive expectations from separate state:

- simulator world state and MST data for final materialized repo state;
- simulator firehose history for expected event-log rows;
- public replay and XRPC observations for client-visible behavior;
- physical segment scans for storage invariants;
- mutation campaigns for empirical bug-detection power.

Shared code is a known blind spot. Both the simulator and Jetstream use
`github.com/jcalabro/atmos` for atproto protocol handling, so symmetric bugs in
that library can pass the oracle. Real-data corpus tests exist to reduce that
closed-loop risk.

### Assertion Power Before Determinism

Perfect deterministic simulation testing is not a near-term requirement. Go's
runtime scheduler, real network I/O, wall-clock timers, Pebble, and filesystem
behavior make bit-identical execution expensive enough that it should not come
before stronger assertions.

The practical target is deterministic enough:

- seed simulator world generation and traffic decisions;
- keep avoidable nondeterminism out of oracle-observed input bytes;
- use barriers and durable append acknowledgements instead of timing-based
  success conditions;
- emit structured traces so an interleaving-dependent failure is diagnosable
  even when it is not exactly replayable.

Real crash/restart and public-serving tests must keep real process, socket,
filesystem, and persistence behavior. Fake transports, fake time, in-memory
stores, and `testing/synctest` experiments are supplemental logical tiers, not
replacements for durability and product-path tiers.

### Fail Loud Over Corrupt

The production daemon must not crash on invalid upstream input, but the oracle
should crash loud on invalid internal state, persistence corruption, fsync
failures, impossible segment structure, and test harness anti-vacuity failures.
Silent fallbacks create false confidence and are worse than a noisy test.

Every injected fault must be accounted for. If a fault plan is configured, the
oracle should prove the expected faults fired; otherwise a disabled fault path
can make a test pass vacuously.

### Keep Tiers Separate

One giant oracle test would be hard to understand and hard to debug. The
testing system should keep related tiers that share helpers but fail with
different explanations:

- storage and final-state correctness;
- event-log equivalence;
- `/subscribe` replay correctness;
- XRPC segment egress correctness;
- compaction and tombstone invariants;
- crash/restart durability;
- local store-fault behavior;
- adversarial simulator fidelity;
- real-data corpus independence;
- long-horizon soak behavior;
- deterministic-enough experiments.

The default lifecycle should stay fast enough to run locally. Heavier stress,
restart, mutation, and soak modes should be explicit recipes.

## Current Architecture

### Simulator World

`internal/simulator/world` owns deterministic account generation, repo state,
traffic generation, firehose history, and simulator-side persistence. It
creates real atproto-shaped bytes: CBOR frames, signed commits, CAR blocks,
MST data, account events, and sync events. It is not a set of mocked
`segment.Event` structs.

The world has two important roles:

- provide upstream-like inputs to Jetstream through simulator HTTP and
  websocket handlers;
- provide independent expected state and event history for oracle checkers.

Simulator data that appears in oracle-observed bytes should be stable under a
fixed seed where practical. For example, account event timestamps use the
simulator logical clock rather than wall-clock time.

### Runtime Driver

`internal/oracle` starts Jetstream with simulator-backed relay, PDS, and PLC
endpoints, then drives lifecycle phases:

- bootstrap and bootstrap-live overlap;
- merge into the steady archive;
- steady-state live traffic;
- compaction;
- shutdown and restart scenarios.

The driver uses phase gates, durable append callbacks, sequence acknowledgers,
and subprocess crash harnesses. These are not decorations: they are how the
oracle avoids relying on sleeps as success criteria and how it proves that
data reached durable surfaces before assertions run.

### Observers

Observers collect what Jetstream produced through different surfaces:

- filesystem segment observer: reads active and sealed segment files directly;
- event-log recorder: captures lifecycle hook events keyed by upstream relay
  cursor;
- `/subscribe` replay observer: reads public websocket replay and live-tail
  behavior;
- XRPC segment observer: downloads public archive segments and decodes bytes as
  a client would;
- metadata and metrics observers: inspect durable cursor/status/watermark
  state and operational signals where needed.

Observers must not silently substitute for one another. If `/subscribe` replay
is wrong while storage is correct, that is a product-path failure, not a reason
to pass by falling back to filesystem reads.

### Checkers

Checkers turn observations into contracts:

- physical invariants: seq ordering, valid event shape, non-empty commit revs,
  valid segment block metadata;
- final-state comparison: reconstruct observed rows and compare to simulator
  world state;
- event-log equivalence: compare normalized observed rows to simulator
  firehose-derived expected rows;
- compaction invariants: prove rows that must be removed at or below a
  watermark are absent, while rows above that watermark are not over-dropped;
- replay invariants: prove public replay surfaces match archive observations
  and normalized event expectations;
- failure-contract assertions: prove faults fail loud, preserve prior durable
  state, or drop invalid upstream data with bounded diagnostics as specified.

Final-state comparison and event-log comparison are complementary. Final state
can converge after an intermediate event is lost; event-log equivalence catches
that stream hole.

### Trace Artifacts

Oracle runs emit JSONL traces with monotonic trace indices and bounded payload
digests. The trace is the practical substitute for perfect deterministic
scheduling. It should identify:

- scenario config, seed, mode, Go version, and fault mode;
- generated events and compact event identities;
- phase transitions and barriers;
- scheduled and fired faults;
- durable append observations;
- compaction passes and watermarks;
- replay observations;
- shutdown, restart, and crash markers;
- first divergence details when a checker fails.

Traces should stay compact. Hash large payloads; do not dump full CARs or full
records by default.

### Mutation Campaign

`testing/mutation` measures oracle detection power with curated, realistic
single-edit bugs. A mutant is not a production patch; it is a bug model with a
documented failure mode and expected detection tier.

The mutation campaign is the scorecard for major oracle improvements:

- new oracle capabilities should kill a mutant or explain why they cannot;
- stale or dead mutants should be retired from the active catalog and kept in
  `testing/mutation/RESULTS.md` with rationale;
- every full or targeted campaign result should be appended to
  `testing/mutation/RESULTS.md`;
- never "fix" production code to match a mutant.

The scorecard is historical evidence, not task tracking. Active follow-up work
belongs in GitHub issues.

## Oracle Tiers

### Storage And Final-State Tier

This tier observes segment files directly, checks physical invariants, rebuilds
materialized repo state, and compares it to simulator world state. It is the
foundation for detecting archive data loss and corruption.

It cannot prove public replay correctness by itself.

### Event-Log Tier

This tier derives expected rows from simulator firehose history and compares
them to normalized Jetstream observations. It is designed to detect missing
intermediate events that final-state comparison can hide.

Compaction-aware comparison may allow a missing row only when a committed
watermark and later tombstone/update make that absence legal.

### Client-Driven Historical Tier

This tier drives the real Go client (`github.com/bluesky-social/jetstream`)
through the full archive-negotiation path — paginated `planBackfill` →
`getSegment`/`getBlock` → cutover to `/subscribe-v2` — and asserts the
documented **fold-convergence** contract on what the client replayed. This is
the historical product-path surface: it validates what real clients replay
through the public APIs, exercising the paginated bufferless cutover (pin
`sealedTipSeq`, page until `plannedThroughSeq` reaches it, connect once) that a
bespoke whole-archive `/subscribe?cursor=0` replay lacks.

The client is an **observation surface only**, and the check is
eventually-consistent, not point-in-time: the oracle folds the full emitted
stream (creates/updates apply; deletes/account-deletes/syncs remove) and
compares the converged result against ground truth derived independently from
simulator world state and the firehose history — matching a dead record's
killer to a DID-level marker by DID, never comparing the client against itself.
The contract is at-least-once with no silent loss of in-scope retrievable data;
transient stale rows that a later marker kills are expected, not a violation.
Because the client and Jetstream share `atmos` (and the client shares the
segment decoders with the server), the direct segment and event-log tiers remain
the independent storage check that distinguishes a server bug from a client bug
— the client tier runs alongside them, not instead.

The client emits jetstream's own seq, so the drain stops at a jetstream-seq
watermark (e.g. the steady compaction watermark), not the simulator's upstream
relay cursor — the two spaces do not map.

### Live-Tail Replay Tier

This tier treats `/subscribe` replay as a first-class observation surface for
its real role — the recent live tail. It covers mid-stream cursors, boundary
cursors around blocks and segments, compaction watermarks, and selected
filters. It does **not** replay the whole archive from cursor zero; historical
reads go through the client-driven tier above (the archive transport real
clients use), per issue #77.

The replay tier validates hot tail / cold reader handoff, cursor semantics,
JSON encoding (v1 and v2 wire shapes), pending writer snapshots, manifest
refresh, and compaction cache invalidation.

### XRPC Segment Egress Tier

This tier downloads archive segments through public XRPC/HTTP paths, decodes
the bytes independently, and compares them to filesystem and expected event
observations. It validates manifest readiness, cache behavior, headers,
checksums, block indexes, and post-compaction refresh.

### Crash And Restart Tier

This tier kills real child processes and reopens real persistent state. It
must not be replaced with in-memory storage. Enumerated crashpoints and seeded
crashpoint ordinals are the preferred mutation-gated form because failures
replay exactly; random wall-clock kills are not part of the mutation campaign
contract.

Beyond "no records lost across a crash," this tier lands **durable
intermediate events** through the merge so it can exercise the lost-intermediate
and no-permanent-tombstone contracts the storage tier's final-state check is
blind to (per §180-182). Because a production PDS's `getRepo` serves only the
current head (creates, never updates/deletes), the only way a durable
update/delete/tombstone reaches disk is via the live firehose at a rev above the
backfill head. The harness reproduces that exactly: a `OnGetRepoServed` timing
signal tells the simulator (running in the parent process) that a DID's backfill
head is pinned, after which it generates a seed-derived chain on the live
firehose that survives the merge rev-filter. The chains are pinned shapes with
seed-varied specifics (account/collection/rkey/payload), so every run exercises
the same seams while the nightly sweep explores the state space:

- R_bf create→update and create→delete (a backfilled create superseded by a
  live mutation — the rev-filter survival boundary, and the §180-182 lost-create
  shape);
- R_live create→update→delete (a record born entirely in the live window);
- R_live delete→recreate on a reused rkey (record-level no-permanent-tombstone:
  the recreate above the tombstone must reconstruct visible);
- account-delete→reactivate→post (DID-level no-permanent-tombstone);
- #sync divergence (a silent mutation + #sync forces a verifier getRepo resync
  whose KindSync tombstone + KindCreateResync rows must survive the merge and
  rebuild the full repo — landed on an early-served DID so the async resync
  completes before cutover);
- control guards (a live create-only record and a pure-backfill untouched DID)
  so a fixture that lands nothing fails loud rather than passing vacuously.

Three post-restart checks run over the recovered segments: final-state
`Compare` (existing); at-least-once event-log **coverage** (every model-derived
durable row is present at least once, tolerant of the contract-permitted
re-merge duplicate, sensitive to loss); and the compaction contract via
fold-convergence (fold the recovered stream and compare the converged result to
ground truth). The expected side is model-derived from the chain the test issued
(oracle independence), using on-disk seqs only to position the
watermark-compaction filter.

The convergence-hiding compaction over-drop (#100) is NOT reachable here: the
merge-tail compaction snapshot always spans the whole sealed stream, so every
drop decision is complete. That check's end-to-end proof lived in the
steady-state tier (mutation `m025`), where a delete arriving after the pass's
force-rotate sits above the watermark and a survivor can be wrongly dropped
while final state still converges. `m025` was retired when its
`Set.SnapshotRange` mechanism was deleted in #178 (the on-disk windowed fold can
no longer reach the above-watermark over-drop it modelled). #183's re-derivation
analysis concluded that NO single-edit mutant can uniquely trip the recorder
under the windowed-fold architecture: the pass folds its tombstones from the
exact on-disk window it compacts (so every genuinely-folded drop is also
approved by the recorder's identically-bounded filter — invisible to it), and
the only edits that manufacture a filter-illegal drop (seq-comparison or
seq-value corruption, whose sole new victim is the self-superseding update row)
are maximal and die at after-merge final-state Compare on every seed. The
recorder is therefore a **regression assertion without a gated mutant**: it
still runs on every lifecycle run, and its unique power reactivates only if a
future change reintroduces an out-of-window tombstone source (an in-memory
readout, a cross-window cache) — whoever makes such a change must re-derive a
mutant for it then. Full argument: the 2026-07-04 section of
`testing/mutation/RESULTS.md`.

### Store-Fault Tier

This tier injects deterministic local persistence failures at high-risk store
boundaries. It tests fail-loud and no-corruption contracts for cursor saves,
seq/next writes, repo status writes, syncstate commits, compaction watermark
updates, and manifest refreshes.

### Simulator Fidelity Tier

This tier expands upstream behavior beyond polite happy paths: identity
events, account statuses, unknown lexicons, Unicode, near-limit records,
oversized fields, malformed bounded frames, missing CAR blocks, sequence gaps,
duplicates, and reconnect/resume edge cases.

Each invalid-input case must define whether Jetstream should archive surviving
good events, drop an unarchivable event, advance a cursor, increment a metric,
log bounded diagnostics, and continue running.

The adversarial ingest-gate modes (#204) set the tier's conventions:

- **The world lies through the honest pipeline, not around it.** Op-path lies
  enter via raw `mst.Tree.Insert` (bypassing `repo.Create` validation) inside
  otherwise-real signed commits, so atmos's verifier — which checks MST
  consistency, not spec validity — passes them through to the ingest gate.
  Rev lies are signed into the inner commit. A lie the verifier would reject
  structurally proves nothing about the gate.
- **Layer ownership is explicit.** Each case is asserted at the layer that
  owns it: gate-owned cases assert the labeled reason on
  `jetstream_ingest_dropped_events_total`; verifier-owned cases (non-TID /
  future / regressing #commit revs) assert rejection or resync repair with no
  bad archive. `internal/simulator/world/adversarial.go` documents which lies
  land where; the wire itself blocks one class (invalid UTF-8 cannot ride a
  live op.Path — CBOR text strings reject it — so it is backfill-only).
- **Every lie is ledgered.** `world.AdversarialLedger` records each lie at
  generation time; the oracle filters expected event logs, final-state ground
  truth, and cursor-gap accounting through it (one-directional-safe: a
  wrongly-archived lie still fails compares as an extra). Anti-vacuity is
  per-layer: gate-owned lies assert per-(source, reason) drop-counter floors
  (`ExpectedDropFloors` skips verifier-layer entries — they never reach the
  gate counter); verifier-owned lies prove they fired through their own
  observable (the awaited resync-repair tombstone).
- **Honest traffic must not touch lie records** (`pickUntouchedRecord` skips
  ledgered keys): an honest mutation of an unrepresentable-but-spec-valid
  record would be gate-dropped as an unledgered event and corrupt the
  cursor accounting.

### Real-Data Corpus Tier

This tier breaks the `atmos` closed loop by running real network bytes with
expectations pinned by foreign implementations through stable offline
regression tests. It lives in `internal/corpus` (separate from the lifecycle
oracle) and runs in normal CI; the `corpus` mutation tier gates it.

Four fixture families, all under `internal/corpus/testdata/` with provenance
and the re-capture procedure in its README:

- a contiguous raw relay firehose window (byte-for-byte websocket frames)
  replayed through the complete production live path — atmos frame decode,
  offline Sync 1.1 signature verification against captured DID documents,
  ConvertEvent, ingest, segment seal — with the v1 JSON output required to
  match what production Jetstream v1 (an independent implementation) served
  concurrently for the same events;
- a production getRepo CAR (maintainer-owned repo) through the backfill
  handler, with rows and recomputed record CIDs compared to a listing pinned
  by indigo's `goat` at capture time;
- a byte-pinned golden sealed segment built from the real CAR's records,
  compared byte-exactly on the write side and opened through the full reader
  path on the read side — the committed bytes are facts from a known-good
  build, so a symmetric writer+reader bug (the m009 class the closed loop
  structurally cannot see) fails against them from both directions;
- malformed variants derived mechanically from the real bytes (truncated
  frames, bit flips, cut CARs) asserting the drop-and-continue and fail-loud
  contracts on production-shaped garbage.

Independence discipline: fixture expectations must never be derived with
atmos. The capture tool is built on `bluesky-social/indigo` and is
deliberately NOT committed (indigo must not enter this module's dependency
graph); its source is preserved on issue #32. Manifest event counts serve as
anti-vacuity assertions, and the committed corpus stays small (~640 KiB) —
production incident fixtures can be added as new windows via the documented
re-capture procedure.

### Soak Tier

This tier runs repeated traffic, compaction, restart, replay, and final
comparison cycles to catch accumulation bugs: watermark drift, manifest/cache
growth, goroutine leaks, tombstone memory growth, and slow cursor divergence.

Soak belongs in manual or scheduled runs, not the default developer loop.

### Determinism Experiments Tier

This tier explores `testing/synctest`, in-process transport, fake time, and
in-memory storage for narrow logical slices. Adoption should depend on measured
value: fewer flakes, clearer failures, or materially faster tests.

Do not move public serving or crash durability checks into fake I/O modes.

## Requirements For Future Changes

### Adding Oracle Coverage

When adding a new oracle capability:

1. define the contract in terms of product behavior or durable state;
2. choose the observation surface that can actually see violations;
3. make anti-vacuity explicit so the scenario proves it exercised the path;
4. record bounded trace facts that explain failures;
5. add focused unit tests for new checkers or decoders;
6. run the relevant oracle tier and mutation cases;
7. update `testing/mutation/RESULTS.md` when mutation evidence changes.

### Adding Simulator Behavior

Simulator additions should preserve deterministic-enough input generation for
seeded runs. Use logical clocks and seeded RNGs where output bytes or trace
facts depend on time or randomness. If a behavior is intentionally adversarial
or nondeterministic, put it behind an explicit scenario mode and trace what it
did.

### Adding Mutants

Only add mutants that model a realistic single-edit bug. Every mutant should:

- compile;
- avoid trivial panics;
- include metadata describing the production failure mode;
- state the expected oracle tier before the first run;
- be retired when code movement makes it stale or dead.

### Updating Documentation

Keep this document focused on durable design and contracts. Do not add task
lists or implementation progress here. Use GitHub issues for active work; the
current testing revamp epic is #35.

Keep `testing/mutation/RESULTS.md` as the mutation scorecard. It is historical
evidence and should not be folded into this design document.

## Current Work Tracking

Active oracle testing work is tracked in GitHub issues:

- #35: overall testing revamp epic;
- child issues for product replay, XRPC egress, crash/fault depth, simulator
  fidelity, corpus, soak, and determinism experiments;
- targeted issues for specific mutation escapes or coverage gaps.

When an issue reveals smaller independently shippable work, split it into new
issues rather than expanding scope in place. Close issues through commits or
PRs with `Closes #N`.
