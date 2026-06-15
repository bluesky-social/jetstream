# Oracle Simulator Testing: Analysis & Roadmap

An assessment of whether the oracle simulator tests (`internal/oracle` +
`internal/simulator`) are rigorous enough to surface real production bugs, or
whether they are over-fit and merely confirm the code they were written
alongside. Includes a ranked roadmap of improvements.

## Verdict

The oracle is **not** "testing itself" in the naive sense — it has real
structural independence in several places, and the engineering quality of the
harness itself is unusually high. However, there are **four structural blind
spots** where the test and the system share assumptions, and one of them is
significant: the oracle validates **storage**, but the product is **replay**.
The read path — the entire reason this system exists — is never oracle-checked.

## What's genuinely rigorous (evidence it's not just self-confirmation)

These properties are what separate a real oracle from a tautology, and we have
them:

1. **Independent ground truth.** `GroundTruthFromWorld`
   (`internal/oracle/groundtruth.go:11`) walks the simulator's *own* MST repo
   state in pebble — it is not a recording of what jetstream emitted. The
   payload bytes that get compared traverse two fully disjoint pipelines:
   sim-pebble → MST walk on one side; firehose CBOR → ingest → segment file →
   segment reader on the other. A corruption anywhere in jetstream's write
   path *will* diverge.

2. **Real wire format.** The simulator emits genuine CBOR `#commit` frames
   carrying real CAR blocks with real MST nodes and signed commits over real
   HTTP/websocket — not mocked Go structs. Jetstream's actual frame parser,
   CAR decoder, and MST verifier are exercised.

3. **Anti-vacuity meta-assertions.** `assertFaultPlanFired`
   (`internal/oracle/harness_test.go:202`) explicitly requires the fault plan
   to be non-empty and every scheduled fault to have fired. A config
   regression that silently disabled injection fails loudly instead of
   passing vacuously.

4. **Invariants checked on physical order before sorting**
   (`internal/oracle/segments.go:72-74`) — sorting first would hide
   source-order regressions; the code knows it and documents it.

5. **The checker is itself tested.** `compare_test.go`, `reconstruct_test.go`,
   `compacted_test.go` each verify the oracle *rejects* specific corruptions
   (missing record, payload mismatch, surviving superseded row, etc.).

6. **Real crash semantics.** The restart harness SIGKILLs an actual
   subprocess at crashpoints and re-runs the oracle on the survivor's data
   dir (`internal/oracle/restart_harness_test.go:94-113`). Not an in-process
   simulation.

7. **`CheckCompacted` is an independent re-derivation** of the compaction
   guarantee (`internal/oracle/compacted.go:18`) — it doesn't ask the
   compactor what it did; it recomputes which rows must be gone.

8. **Nightly random-seed sweep** with printed repro instructions, swarm fault
   mode on by default (`justfile` `oracle-sweep`, scheduled CI every 6h).

This is well above the bar for "we wrote tests that confirm our own code."

## Where the over-fitting concern is real

Ranked by how likely each is to let a production bug through.

### 1. The read path is not oracle-checked — and it's the product

`ObserveSegments` reads segment files directly off disk. The oracle never:

- connects a websocket to `/subscribe` and replays from cursor 0,
- exercises cursor semantics, filters, hot-ring/cold-reader handoff under
  oracle comparison,
- downloads segments through the XRPC `getSegment`/`listSegments` surface and
  decodes them as a *client* would.

`internal/subscribe` has solid unit/fuzz/golden tests, and the E2E test reads
exactly **one** event and checks it's JSON with a DID in it
(`cmd/simulator/e2e_test.go:132-140`). But "a consumer who replays the whole
network through the public API gets exactly the ground-truth event stream" —
the core product contract — is asserted nowhere. This is the largest gap, and
closing it would also de-risk the subtlest component (hot tail / cold segment
handoff under concurrent ingest).

### 2. Final-state comparison hides event-stream infidelity

`Reconstruct` + `Compare` validates the *final* materialized state. If
jetstream drops an update event that's later superseded by another update to
the same record, the final state converges and the oracle passes — but a
replaying consumer has a hole in their stream. `CheckInvariants` catches seq
duplication/regression, and `CheckCompacted` catches over-retention, but
**nothing catches under-retention of non-superseded intermediate events**
except when they happen to be the final touch on a record. The simulator
already persists the full firehose log in pebble — everything needed exists
to assert *event-log equivalence modulo compaction rules*, a much stronger
statement than final-state equivalence.

### 3. The atmos closed loop

Both the simulator (encode) and jetstream (decode) use `jcalabro/atmos` for
CBOR/CAR/MST/TID/signing. A bug in atmos that's symmetric across
encode/decode — a CBOR edge case, an MST key-split quirk, a sign/verify pair
that agrees with itself but not with the spec — is **structurally invisible**
to the oracle. This is the purest form of the over-fitting problem in this
setup: the test rig and the system under test share their understanding of
the protocol via a common library, with one author. The real network is
implemented by indigo and the TypeScript stack; the oracle never confronts
their bytes.

### 4. Fidelity gaps: the simulator is too polite

The fault plan is deliberately bounded *inside* atmos's retry budget so every
run converges (`internal/oracle/faults.go:50-55`) — reasonable for a
deterministic oracle, but it means the simulator never produces conditions
where the correct behavior is to surface an error, resync, or drop-and-count.
Concretely missing, ordered by risk:

- **`#identity` events are never generated**
  (`internal/simulator/world/firehose.go:195` — dead scaffolding) — yet
  `KindIdentity` is a live production code path
  (`internal/ingest/live/events.go:223`, `internal/subscribe/encoder.go`).
  That entire ingest→store→replay path for identity events has zero oracle
  coverage. Same for account statuses other than `deleted`
  (deactivated/takendown).
- **No seq gaps, no `#info`/OutdatedCursor exercise during oracle runs, no
  tooBig**, no out-of-order delivery, no duplicate frames. Real relays
  produce all of these. The reconnect-after-disconnect faults always resume
  into a perfectly contiguous history.
- **`getRepo` always returns the full repo**; the `since` param is ignored.
  Truncation faults cut at exactly `len/2`
  (`internal/simulator/http/pds.go:50`) — no corrupt CBOR, no missing MST
  blocks, no bad signatures reaching jetstream.
- **Data shape**: ASCII-only `[a-z ]` text, 5 hardcoded bsky collections, max
  ~3KB records (`internal/simulator/world/records.go`). No unicode, no
  unknown lexicons, no near-column-width-limit fields — despite AGENTS.md
  having an explicit policy ("drop the record, increment a metric") whose
  enforcement is never oracle-tested.

### 5. Enumerated crashpoints are a form of over-fitting

The restart oracle covers 4 of 12 crashpoints (the rest have unit-level
coverage), but all crash sites are places **someone thought to name**. The
bugs that hurt are at seams nobody anticipated. A complementary mode —
kill -9 at a *random* time during a seeded run, restart, oracle-check, loop —
explores crash timing without enumeration bias. Nearly free to build: the
restart harness already has the subprocess + SIGKILL machinery.

### 6. The oracle's bug-detection power is unmeasured

The question "would this oracle catch a real bug?" is *measurable*. There are
meta-tests for the comparator, but no evidence about the **end-to-end**
harness — would a one-line bug in merge cursor advancement, or a dropped
flush on shutdown, actually propagate to a `Compare` failure under default
mode, or only under stress with the right seed?

## Roadmap, ranked for 10-year confidence

### Tier 1 — changes the confidence level

- [x] **Mutation campaign against the oracle.** DONE 2026-06-15 — see
      `testing/mutation/` (driver, 18-mutant catalog, `RESULTS.md`); run with
      `just mutation-campaign`. First campaign: 8 killed, 10 survived. It
      confirmed the oracle catches every hot-path data-shape bug (and several
      only at stress scale — `default` mode is genuinely weaker on merge-dedup
      bugs), and surfaced concrete gaps now tracked below. Notably m002's
      *flaky* detection (4/5 seeds) is hard evidence that the multi-seed
      nightly sweep is load-bearing. Repeat after major ingest/segment/
      orchestrator changes; a STALE result means a mutant needs refresh.
- [ ] **Oracle gaps surfaced by the first mutation campaign (fix these).**
      - *Rev-blindness (m018/m014):* `CheckInvariants` skips empty revs and
        `Compare` only checks rev when both sides have it, so dropping `rev`
        on a commit event is invisible. Fix: reject an empty rev on a
        create/update/delete event in CheckInvariants (near-free).
      - *Offset-vs-index (m010):* the oracle reads blocks by index, so a
        corrupt recorded block offset escapes. Fix: an offset-following read
        mode, or assert block-offset monotonicity in ObserveSegments.
      - *Compaction boundary (m007):* CheckCompacted does not evaluate the row
        at exactly the chunk boundary seq. Fix: assert the boundary seq itself
        is compaction-evaluated.
      - *Merge-cursor restart seam (m003):* not covered by the 4 enumerated
        crashpoints; subsumed by the random-time kill loop in Tier 2 below.
      Accepted (not fixable without other roadmap items): m006 (needs
      store-fault injection, Tier 3), m009 (symmetric write/read checksum — the
      closed-loop blind spot in §3), m015/m016 (footer/bloom read-path indexes
      — closed by the replay-path oracle below).
- [ ] **Replay-path oracle.** A websocket client in the harness that replays
      from cursor 0 (and from mid-stream cursors, with and without filters)
      and feeds the result into the same `Reconstruct`/`Compare`. Likewise
      pull segments through the XRPC API rather than the filesystem. Makes
      the oracle test the product contract, not the storage implementation.
- [ ] **Event-log equivalence checking.** Compare the observed event stream
      against the simulator's persisted firehose log modulo compaction rules,
      not just final state.

### Tier 2 — fidelity and independence

- [ ] **Light up the dark paths**: generate `#identity` events, account
      status transitions (deactivated/takendown), and post-fault seq gaps in
      the simulator; add unicode/oversized/unknown-lexicon records and assert
      the documented drop-and-count policy.
- [ ] **Break the atmos loop with real data.** Capture a few minutes of real
      production firehose (plus a handful of real `getRepo` CARs from diverse
      PDS implementations) as a committed corpus; replay it through ingest in
      CI and assert invariants + golden output. Also schedule a periodic
      repo-verification smoke job against production by polling the
      `/status` repo-verification endpoint (the standalone `verify-repo`
      CLI was removed in favor of the HTTP path, which reuses the live
      manifest cache). This is the only defense against "atmos agrees with
      itself but not the network."
- [ ] **Random-time kill loop** as a nightly companion to the enumerated
      crashpoint tests: SIGKILL at a random moment in a seeded run, restart,
      oracle-check, repeat.

### Tier 3 — longevity

- [ ] **Long-horizon soak oracle**: multiple
      bootstrap→steady→compact→restart cycles in one run, weekly schedule;
      catches accumulation bugs (manifest growth, watermark drift across
      cycles) that single-lifecycle runs can't.
- [ ] **Adversarial-fault oracle mode** where injected faults exceed the
      retry budget and the assertion becomes "jetstream surfaced the failure
      correctly and didn't corrupt what it had" — testing the failure
      contract, not just the success path.
- [ ] **A short ORACLE.md** documenting the oracle's threat model: what it
      can catch, what it structurally cannot (the four blind spots above),
      and the mutation-campaign results. Ten years from now, that document is
      what stops a new engineer from assuming green oracle = correct system.

## Bottom line

The harness is legit — independent ground truth, real wire bytes,
anti-vacuity checks, tested checkers, real SIGKILL crash recovery. It will
catch storage-side data-loss and corruption bugs. What it will *not* catch
today: replay-path bugs (untested end-to-end), intermediate-event loss masked
by final-state convergence, protocol disagreements hidden inside atmos, and
behavior under network conditions the polite simulator never produces —
including the `#identity` path that exists in production code but has never
seen a single simulated event.

If only one thing gets done: run the mutation campaign. It converts "this
might be over-fit" into a measured kill-rate, and identifies exactly which of
the gaps above to close first.
