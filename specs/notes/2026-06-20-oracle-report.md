# Oracle Simulator — Status Review

**Date:** 2026-06-20
**Reviewer:** Claude (Opus 4.8), driven by Jim Calabro
**Scope:** `internal/oracle/`, `internal/simulator/`, `testing/mutation/`, the design spec `specs/oracle.md`, and open GitHub issues #25–#35, #77.
**Method:** Direct code reading by the author, plus an adversarially-verified multi-agent review (8 tier-mapping readers, 6 over-fit/blind-spot hunters whose 40 findings were each re-checked by an independent skeptic — 33 confirmed/partially-confirmed — 3 external-practice research streams, and a mechanical mutation-catalog apply-check). Every load-bearing claim below was re-verified against the source by the author; a few were reproduced empirically by the agents (noted inline).

> **Note on confidence.** Where I write "verified" I read the code or ran the command. Where I write "inferred" I am reasoning from code I read but did not exhaustively trace. One inter-agent contradiction surfaced and was resolved by hand (the "random-time kill loop" claim — see §6, Crash/Restart); I flag it so you don't trust the raw agent transcripts uncritically.

---

## 1. TL;DR

The oracle is a **genuinely strong, unusually disciplined bug detector** for the *storage* path, and the over-fitting worry is **not** warranted as a blanket claim — most checkers run real code and assert independently-derived contracts that can and do fail (the mutation campaign proves it). The determinism substrate (seeded RNG, logical clock, barriers-not-sleeps, structured traces) and the fault anti-vacuity discipline (`assertFaultPlanFired`) are textbook-grade.

But there are **three classes of real problem**, and the most important one is recent:

1. **The flagship product-path tier is weak.** The new client-driven historical tier (#77, `client_observer_test.go`, added 2026-06-20) — the one that drives the *real Go client* through `getTombstones → planBackfill → getSegment/getBlock → cutover`, i.e. the surface that exists specifically to validate "what clients can replay" — asserts **only** `CheckCompacted` on the client's own output. That is a one-directional, self-referential invariant that passes vacuously on an empty/sparse stream and never compares the client against independent ground truth. A client that drops records, skips DIDs, serves stale payloads, or loses rows across cutover **passes**. This is the single most over-fit spot in the system, and it is brand new. (§4.1)

2. **Compaction over-drop / data-loss is invisible to every wired tier.** `CheckCompacted` only catches *over-retention* (a superseded row that wrongly survived); it never asserts the latest survivor is *present*. `CheckOverlayReconstruction` derives its ground truth from the same post-compaction stream it checks, so an over-dropped row vanishes from both sides. The event-log tier — the spec's designated stream-hole catcher — observes the *pre-compaction* append stream. The one checker that *would* catch this (`CompareEventLogsCompacted`) exists and is unit-tested but **has zero non-test call sites.** A row over-dropped at/below the watermark that is later superseded above it (final state converges) escapes everything. (§4.2)

3. **Whole tiers from the spec are unimplemented, and the mutation scorecard is stale.** Store-fault (#30), simulator-fidelity/adversarial-input (#31), real-data corpus (#32), soak (#33), live-tail `/subscribe` (#25), and XRPC-egress HTTP-semantics (#26) tiers are absent or only partially present. The mutation campaign (`RESULTS.md`, last full run 2026-06-15) predates the entire client tier and the 06-18..20 overlay/eventlog churn; exactly one mutant (m022) is now stale, and **no mutant targets any of the new/churned code.** (§5, §6)

None of these mean the oracle is bad. They mean its strongest assertions are concentrated on the *storage* surface, while the *product/serving/client* surface — the newest code and the actual product contract — is the least-tested. That is exactly backwards from where confidence should be highest, and it is fixable with targeted, mostly-cheap work.

---

## 2. Where to start reading (a human reviewer's path)

The system is ~10.6k LOC across three packages. Don't read it file-alphabetically. Read it in **dataflow order**, following one event from generation to assertion. Suggested path:

**Step 0 — the contract.** `specs/oracle.md` (you wrote it; re-read §"Design Goals" and the 11 tiers). This is the yardstick everything else is measured against.

**Step 1 — the spine.** `internal/oracle/harness_test.go:28` `TestOracle_DefaultLifecycle`. This ~39k-line file *is* the oracle. Read the top-level body first (lines ~28–340): it walks bootstrap → bootstrap-live overlap → merge → steady-state → compaction → shutdown, calling an `assert*` helper at each phase boundary. Make a one-line note of each `assert*` call and the phase it guards (lines 228, 237, 239, 256, 280, 330, 331, 336). Those eight calls are the entire assertion surface of the default run.

**Step 2 — the independent ground truth.** This is the heart of the design. Read in this order:
- `model.go` (45 lines) — the `Model`/`ObservedEvent`/`RecordKey` types everything compares.
- `groundtruth.go` (54 lines) — `GroundTruthFromWorld` builds expected final state *directly from the simulator world's MST*, independent of Jetstream. **Note line 49–50: it never sets `Rev`** (this matters — see §4.3).
- `expected_eventlog.go` (260 lines) — builds the expected *event log* from simulator firehose history, decoding frames with an independent CBOR parser (not the segment decoder). This is the most independent checker in the system.

**Step 3 — the observers (what Jetstream actually produced).**
- `segments.go` (191 lines) — `ObserveSegments` reads sealed + active segment files. `checkSegmentStructure` (123–164) independently validates block-index structure. **Note: it decodes with the server's own `segment` package** (see §4.5 independence caveat).
- `eventlog.go` (296 lines) — `eventLogRecorder` captures the live hook stream; `RowsByUpstreamCursor` normalizes by upstream relay cursor.
- `client_observer_test.go` (205 lines) — the new client tier. **Read this carefully and skeptically; it is the weakest link (§4.1).**

**Step 4 — the checkers (observations → verdicts).**
- `compare.go` (137 lines) — final-state diff. The rev branch at line 35 is dead in real runs (§4.3).
- `invariants.go` (40 lines) — seq uniqueness/monotonicity, rev presence on commits, per-DID rev non-regression.
- `compacted.go` (71 lines) — `CheckCompacted`. **Read this twice.** Internalize that it only flags *survivors that should have been dropped*, never *survivors that should be present but aren't* (§4.2).
- `overlay.go` (165 lines) — `CheckOverlayReconstruction`. Note `ground := groundTruthLive(events)` at line 53 is folded from the *same* `events` it then checks — self-consistency, not independence (§4.2).
- `bisect.go` (135 lines) — `ClassifyCompactedFailure`: when a compacted check fails, re-runs it on disk to classify DURABLE vs SERVING vs INCONCLUSIVE. Elegant; read the doc comment (lines 60–78) on why the watermark-capture ordering makes the DURABLE verdict sound.

**Step 5 — faults & determinism.**
- `faults.go` (253 lines) + `harness_test.go:382-407` (`assertFaultPlanFired`) — the swarm fault plan and its anti-vacuity proof. This is the part to *emulate* elsewhere.
- `trace.go` (81 lines) — the JSONL trace; note `TraceRecord.At` is declared but never set, deliberately keeping wall-clock out of traces.

**Step 6 — the simulator.** `internal/simulator/world/{world,traffic,records,accounts,firehose}.go` — how realistic atproto bytes are generated. Read `traffic.go:RunTraffic` to see the (deliberately polite) steady-state generation — this is where adversarial inputs are *missing* (§6, #31).

**Step 7 — the scorecard.** `testing/mutation/RESULTS.md` + `run.sh` + skim a few `mutants/*.patch`. This is the empirical measure of everything above. Read the "Escapes — analysis and disposition" section of the 2026-06-15 campaign; it is an honest, high-quality self-assessment — but stale (§5).

**How to gain understanding of a live run.** A run is not bit-reproducible (real sockets, real Pebble, Go scheduler). Your reproducibility substitute is the **JSONL trace** — but be aware (§4.6, and issue #35) that the scheduled CI sweep currently writes the trace to a temp dir and destroys it on exit, with no upload step, so production failures are diagnostically blind. To watch a run locally: `just oracle` (stress mode, one seed) and tail the trace file; the `recordTraceOrError` calls in `harness_test.go` are your timeline. The trace records phase transitions, compaction watermarks, fired faults, and first-divergence details.

---

## 3. Strengths (what is genuinely excellent)

These were verified by reading the code and corroborated by all three external-practice research streams (which independently rated each against FoundationDB/TigerBeetle/Jepsen/Antithesis practice).

1. **Independent model oracle (the core design principle, well executed).** Ground truth for final state comes from `GroundTruthFromWorld` (simulator MST), and for the event log from `ExpectedEventLogFromFirehose` (simulator firehose history) — never from Jetstream. The SUT is an observation surface. This is textbook Jepsen-style independence. (`groundtruth.go`, `expected_eventlog.go`)

2. **Fault anti-vacuity is done right.** `assertFaultPlanFired` (`harness_test.go:382-393`) requires the swarm plan be **non-empty** AND that **every** scheduled getRepo 503 and CAR truncation **fired** (`UnfiredGetRepoHTTPFailures()`/`...CARTruncations()` empty). `assertSubscribeReposFaultPlanFired` (395-407) requires ≥1 disconnect AND ≥2 connections (a reconnect followed). This is exactly FoundationDB's "prove the fault fired" discipline. The fault swarm is also on **by default** (`config.go:161`), so a regression that silently disabled injection would fail the plan-non-empty guard.

3. **Determinism substrate is mature.** Seeded PCG RNG with salt-decoupled streams for world/traffic/faults (`faults.go:83,131`; `world` bootstrap). Logical clock for observed-byte timestamps instead of wall-clock (`world/logical_clock.go`). Barriers and durable-append acks instead of sleeps — `phaseGate`, `seqAck` (gap-free contiguous watermark), `accountTombstoneAck`, `syncTombstoneAck`, `compactionPassRecorder` — all `select` on `<-run.exited` and a timeout. Only **one** `time.Sleep` remains in the assertion path (the 5ms poll at `harness_test.go:481`, flagged in §4.6). JSONL trace with monotonic index and sha256 payload digests; `TraceRecord.At` deliberately never populated to keep wall-clock out (`trace.go`).

4. **Layered, complementary checks at different surfaces.** Final-state `Compare` + event-log equivalence + overlay reconstruction + physical structure + compaction contract. The complementarity is real and proven: m019 (dropped sync tombstone) survived final-state convergence but was killed by event-log equivalence (`RESULTS.md:45-57`).

5. **A genuinely independent overlay-suppression check.** `CheckOverlayReconstruction` computes the *emitted* side via the SUT's shared `tombstone.ShouldDrop`, but compares it against `groundTruthLive` (`overlay.go:108-158`), a **hand-rolled fold that does not call `ShouldDrop`**. This asymmetry is why m020–m023 were killed. This is the right pattern — and the one the client tier should copy (§4.1). (Minor caveat: the account-delete sub-dimension uses `oracleAccountDeleted`, a byte-identical copy of `tombstone.accountDeleted` — parallel reimplementation, not fully independent.)

6. **The compaction-failure bisection (`bisect.go`) is well-engineered triage.** It soundly classifies DURABLE vs SERVING vs INCONCLUSIVE, with a correct argument (lines 60–78) for why a disk violation is real even under a racing compaction pass (rewrites are strictly subtractive; watermark captured before scan). All four verdicts + the nil-guard are unit-tested. (It is a *classifier*, not a *detector* — see §4.1, finding on bisection.)

7. **The mutation campaign is an unusually disciplined program** *as a methodology*: hand-curated realistic mutants with documented failure modes and pre-declared expected tiers; append-only history; explicit retirement of dead/equivalent mutants (m007/m010) with rationale; dead-path corrections (m013/m014 → m017/m018); seed-sweep handling of probabilistic kills (m002 killed 4/5). The over-fit worry "is handled where it has been looked at." Its problems are freshness, automation, and breadth — not rigor (§5).

---

## 4. Blind spots and weaknesses

Severity reflects bug-class importance for a durable archive/replay product. Every item cites file:line and was adversarially verified.

### 4.1 CRITICAL — The client-driven historical tier (#77) is near-vacuous

**Location:** `client_observer_test.go:131-147` (`assertClientBackfillCompacted`), `:57-121` (`collectClientBackfill`), wired at `harness_test.go:280`.

The tier drives the **real Go client** through the full archive negotiation — exactly what the spec's #1 goal ("Test The Product Contract") demands, and the reason #77 was a substantial rewrite. But its **only assertion on the client's replayed stream is `CheckCompacted(events, watermark)`** (`client_observer_test.go:135`). Problems, all verified (two reproduced empirically by the agents):

- **`CheckCompacted` is one-directional and self-referential.** It builds tombstone sets from the same `events` it then checks (`compacted.go:22-60`) and only fails when a *surviving* materialization row is superseded by a higher-seq tombstone. **It returns `nil` on an empty or sparse slice.** A client that drops half the archive's records, skips entire DIDs/collections, or serves stale `RecordCBOR` payloads **passes**. (Empirically reproduced: empty, dropped-DID, and stale-payload streams all returned `nil`.)
- **No independent comparison.** The client output is never fed to `Compare`/`Reconstruct` (final state), `CompareEventLogMultiset` (event log), or `CheckOverlayReconstruction`. Those all run on `ObserveSegments(dataDir)` (the direct segment scan) or the hook-fed recorder — **never on the client stream.** So nothing ties the client's output back to simulator world/firehose ground truth.
- **The only completeness guard is a coarse high-water mark:** `require.GreaterOrEqualf(maxSeq, targetSeq)` (`:117`). It proves the client *reached* a seq, not that the `(-inf, targetSeq]` window is *complete or contiguous*. `event_count` is traced (`:113`) but never asserted. A backfill delivering `1..5, 9..12` (dropping 6,7,8) passes.
- **Errors are swallowed.** `for batch, err := range client.Events(ctx) { if err != nil { continue } }` (`:87-90`). Per-segment `getSegment`/`getBlock` download errors surface as recoverable `EntryResult.Err`, are forwarded by the engine, and the test silently continues. A run where *every* historical segment download errors, but live frames arrive, satisfies the high-water guard with an empty/incomplete window — undetected.

> One correction to a tempting-but-wrong story: the vacuity is **not** caused by the live tail racing ahead of archive rows in normal operation. The real client drains the entire backfill (all seq ≤ `plannedThroughSeq`) before emitting any live frame, and `targetSeq` (compaction watermark) ≤ `plannedThroughSeq`, so on a healthy run the window is complete before any live event arrives. The vacuity is reachable specifically through the **swallowed-download-error path** and the **structural weakness of `CheckCompacted`**.

**Why it matters:** This is the product contract. Replay holes, data loss, stale payloads, and cutover bugs on the client path are precisely what the oracle exists to catch (`oracle.md:7`), and the new flagship tier is blind to all of them. The storage tiers cover the *storage* side, but the spec is explicit (oracle.md:240-256) that direct segment observation "must not be the only observation surface."

**Fix (high value, moderate cost):** Feed the client-observed stream to the *same* independent comparators the segment scan uses. Concretely: run `Reconstruct(clientEvents)` → `Compare(GroundTruthFromWorld(w), …)` and `CompareEventLogMultiset(ExpectedEventLogFromFirehose(...), NormalizeEventLog(clientEvents))` and/or `CheckOverlayReconstruction` over the client stream. Add a contiguity/expected-count assertion on the `(-inf, targetSeq]` window. Stop swallowing download errors silently — at minimum count them and assert the count is within the intended fault budget. This converts the tier from "the client didn't obviously self-contradict" to "the client replayed exactly what the firehose said, completely."

### 4.2 HIGH — Compaction over-drop / data-loss escapes every wired tier

This is a structural gap, confirmed across three independent hunters and reproduced empirically.

- **`CheckCompacted` is one-sided** (`compacted.go:18-63`): it proves no *superseded* row survived, never that the *latest survivor is present*. An over-dropped survivor — or a survivor lost because a tombstone was mis-keyed to the wrong DID — simply isn't in the iteration, so nothing fires. (Reproduced: both scenarios returned `nil`.)
- **`CheckOverlayReconstruction` is self-referential for this case** (`overlay.go:52-103`): `ground := groundTruthLive(events)` is folded from the *same* post-compaction `events` as `emitted`. An over-dropped row is absent from **both**, so the "failed to emit a live record" branch (lines 97-101) is dead against physical over-drop. (Reproduced: PASS on an over-dropped stream.)
- **The event-log tier observes the wrong surface for this:** `assertFirehoseEventLogMatches` reads the `eventLogRecorder`, fed by the live `OnEvent` hook at *append time, before compaction runs* (`consumer.go:475-484`). It cannot see what compaction later removes from disk.
- **Final-state `Compare`** converges (the over-dropped row was later superseded, so it's not in the live set either).

**The detector exists but is unwired.** `CompareEventLogsCompacted` + `filterCompactedExpectedRows` (`eventlog.go:138-181`) is precisely a watermark-aware, firehose-independent presence check. It is unit-tested (`eventlog_test.go:209` `RejectsMissingRowAboveWatermark`, `:255` `RejectsUnjustifiedMissingCreate`) and **has zero non-test call sites** (verified by grep).

**Why it matters:** The spec's compaction invariant explicitly requires proving "rows above that watermark are not over-dropped" (oracle.md:172-173), and names event-log equivalence as the tier that catches stream holes final-state convergence hides (oracle.md:180-183). The canonical replay-hole bug — a client misses an intermediate event though final state is correct — is exactly this, and no wired tier catches it for compaction defects.

**Fix (cheap, high value):** Wire `CompareEventLogsCompacted` with `want` = the recorder rows (pre-compaction, firehose-cross-checked) and `got` = `NormalizeEventLog(ObserveSegments(dataDir))` at the compaction watermark, after each compaction phase. The function already works; it just needs a caller. Add an over-drop mutant (e.g. corrupt the `ev.Seq > chunkEnd` keep-guard or the `RowDrop` callback in `compact_deletes.go`) to the campaign to prove the new wiring kills it.

### 4.3 MEDIUM — Final-state `Compare` never validates record `rev`

**Location:** `compare.go:35` + `groundtruth.go:49` + `model.go:14-21`.

`Compare` guards the rev check with `wantVal.Rev != "" && gotVal.Rev != "" && …`. Ground truth (`snapshotRepo`) **never populates `Rev`** (documented at `model.go:18-20`: the final MST exposes record bytes, not the commit rev). So in *every* real lifecycle/bootstrap/restart run the rev branch is **dead code**; it fires only in hand-built unit tests (`compare_test.go`).

**Why it matters:** A bug that materializes correct bytes under the wrong rev is invisible to the final-state tier. It *is* covered by the event-log tier (`expected_eventlog.go` sets rev; `EventLogRow` equality includes rev) — but only for phases the event-log tier covers. After restart, the event-log tier doesn't run (§4.4), so materialized-state rev correctness is unchecked there. This is a documented tradeoff, not a total blind spot, but the prominent-looking rev branch in `Compare` gives false confidence.

### 4.4 HIGH — Crash/restart tier checks only final-state convergence

**Location:** `restart_harness_test.go:146-148`. The sole post-restart assertion is `assertOracleMatches` → `CheckInvariants` + `Reconstruct` + `Compare` (final state only). No event-log equivalence, no compaction check.

**Why it matters:** This is exactly backwards from the spec's own reasoning (oracle.md:180-182): "Final state can converge after an intermediate event is lost; event-log equivalence catches that stream hole." The restart tier is the tier **most likely** to lose an intermediate event (it crashes mid-merge / mid-seal / mid-flush at four crashpoints), yet it validates only the surface that provably cannot see a lost-but-reconverged intermediate. The mutation campaign already flags m003 (merge-cursor double-process) as a SURVIVED "restart-depth gap," and this missing assertion is a contributing factor.

**Fix:** The in-process `eventLogRecorder` can't survive the child-process restart, but an on-disk event log is derivable: `ObserveSegments(dataDir)` → `NormalizeEventLog` → `CompareEventLogsCompacted`. `assertOracleMatches` already calls `ObserveSegments`, so the check is buildable from data already on hand.

### 4.5 MEDIUM — The "independent" storage observer shares the server's codec

**Location:** `segments.go:98,112,168` uses `segment.Open`/`DecodeBlock`/`WalkActive` — the same `github.com/bluesky-social/jetstream/segment` reader the compactor (`compact_deletes.go:199,278`) and merge runner use, over bytes the same package's writer produced.

A **symmetric** encode/decode payload-codec bug (truncate-on-write compensated-on-read) decodes "correctly" on both surfaces, so `Reconstruct`/`CheckCompacted`/`Compare` all pass. The spec acknowledges this exact blind spot (oracle.md:253-256). With the new client tier, it is **wider, not narrower**: the real Go client's `internal/client/{decode,downloader,suppress,segview}.go` also import `segment`, `internal/overlay`, and `internal/tombstone`, so the client surface is *also* not codec-independent.

**Important nuance (don't overstate it):** The *suppression/compaction logic* is independently re-derived — `CheckCompacted` reimplements the tombstone contract locally (not via `tombstone.ShouldDrop`), and `expected_eventlog.go` decodes firehose frames with an independent CBOR parser. So a *logic* bug in suppression is caught; what's shared is the *byte-decoder*. `checkSegmentStructure` (`segments.go:123-164`) also independently re-derives header/block-index structure from raw bytes, so structural corruption is caught. The residual gap is the per-event payload codec.

**Fix (the spec's own answer):** the real-data corpus tier (#32) — run real firehose frames / real getRepo CARs from diverse implementations, plus a committed golden sealed segment with an independently-computed checksum (which would also kill the accepted m009 closed-loop checksum blind spot).

### 4.6 MEDIUM — Determinism/flakiness residue (the #35 scheduled-sweep symptom)

Three verified sub-issues, all matching open issue #27 / #35:

- **The scheduled stress/swarm sweep never runs under `-race`.** `oracle-scheduled.yml:75` → `just oracle-sweep` runs `JETSTREAM_ORACLE_MODE=stress … -timeout 360m` with **no** `-race`. The only race coverage is `ci.yml`'s `test (race)` job (10-min cap), which runs default mode (25 accounts/1000 records), never stress/swarm. The mutation campaign also runs stress without `-race`. *(Caveat: the concurrency topology — `fanout.New(4096)`, the parallel steady consumer — is identical in default and stress mode, so `-race` does exercise the machinery, just at ~4–25× lower volume. The gap is interleaving/volume coverage, not categorical blindness.)*
- **`assertFirehoseEventLogMatches` still uses a 30s wall-clock poll** with `time.Sleep(5ms)` (`harness_test.go:474-482`) rather than a deadlock-guard barrier. On a slow-but-alive runner the deadline can fire with `got < want`, surfacing a *timing miss* as a confusing multiset-mismatch instead of a clear timeout. (It does *not* cause a false pass — the preceding `steadyAck.Wait` at `:255` catches a dead runner, and a genuinely dropped row never reaches the count.) This is the #27 conversion still outstanding.
- **The compaction-race bracket in the bisect counts only *completed* passes** (`harness_test.go:544-548`; `OnCompactionPass` fires in a `defer` at pass end). A single steady-compactor pass that *brackets* the on-disk scan (starts before, ends after) yields `passesDuringScan == 0`, so a clean-but-cross-file-torn read is mislabeled `SERVING_DEFECT` instead of `INCONCLUSIVE`. One-directional: a disk *violation* is still correctly DURABLE (rewrites are subtractive). The fix is a pass-*start* (in-flight) signal, or quiescing the compactor around the scan.

**Diagnostic gap (from issue #35, verified):** `oracle-scheduled.yml` has **no trace-artifact upload on failure** — the JSONL trace (the design's stated determinism substitute) is written to a temp dir and destroyed on exit. Every scheduled failure is currently diagnostically blind. This is the cheapest high-leverage fix in the whole report.

> **Has #77 actually fixed the original flakiness?** Partially. The leading suspect — replaying the *whole archive* over `/subscribe?cursor=0` while compaction rewrites underneath — was removed (the bespoke replay is gone; verified `subscribe_replay_test.go` no longer exists). The client tier carries the `(W,M]` snapshot envelope that the live-socket replay lacked. So the structural cause is addressed. But the bisect torn-read gap above and the wall-clock poll mean a residual class of confusing failures can persist; and the new tier's vacuity (§4.1) means it might now fail *silently* (green) where it used to fail noisily.

---

## 5. Mutation campaign status (the scorecard is stale)

**Verified mechanically** (`git apply --check` and the driver's `--unidiff-zero` on all 21 patches; `RESULTS.md` + `run.sh` read in full; cross-checked by hand for m022):

- **21 mutant patches on disk** (m001–m023, with m007/m010 retired → a numbering gap). `RESULTS.md` headers still say "17 active"/"18 mutants" + "4 new" — itself a freshness mismatch with the on-disk count.
- **Apply-check: 20 clean, 1 stale.** Only **m022** fails to apply. Its patch context `if ev.Kind != segment.KindCreate && ev.Kind != segment.KindUpdate` was refactored on 2026-06-18 into `if !ev.Kind.IsMaterialization()` (`tombstone.go:173`). **I verified this by hand:** the patch fails under both plain and `--unidiff-zero` apply, but the *mutated line* (`ts.Seq > ev.Seq`, `tombstone.go:176`) still exists, so the hypothesis is valid — only the context needs refreshing. The `RESULTS.md` "KILLED@default" row for m022 is therefore currently untrustworthy.
- **The scorecard predates the code it measures.** Last full campaign: 2026-06-15 (commit bb135af). The client tier (`client_observer_test.go`, entire `internal/client/` package) landed 2026-06-18; 29 oracle/simulator commits landed since the campaign baseline (client tier, bisect, `/subscribe`-replay deletion, eventlog rework). **No recorded campaign has ever scored the client tier.** The 20 clean mutants were last validated against code that has since moved and have not been re-run — treat them as last-known-good, not current.
- **Catalog coverage gaps (verified by mapping every patch's `+++` target):** zero mutants target `internal/client/*` (the client decode/plan/cutover path), `internal/xrpcapi/{getsegment,getblock,gettombstones,planbackfill}.go` (the served-read egress), or `internal/subscribe/*` / `internal/simulator/http/relay_subscribe.go` (live-tail). **Zero mutants target the fault-injection subsystem itself** (`internal/oracle/faults.go`, `internal/simulator/http/faults.go`, or the `assert*FaultPlanFired` assertions) — so the anti-vacuity machinery's *own* kill power is asserted only by code reading, never by a mutant. And **zero mutants model compaction over-drop / data-loss** (the §4.2 gap) — the overlay mutants m020–m023 model over-*emission*, which the checkers catch; physical over-*drop* is unmodeled.
- **m007 was retired, not fixed.** Its own recommended assertion ("CheckCompacted should assert the boundary seq is evaluated") was never added; I confirmed the mutant *still survives* `TestOracle_DefaultLifecycle` on seeds 42 and 7. Worse, the retirement rationale (snapshots bounded ≤ chunkEnd) holds only in steady mode; in **merge mode with the tombstone cap** (which the oracle harness sets, `CompactionTombstoneCap:1`), `collectCompactionTombstones` can hold tombstones with seq > chunkEnd, and the mutant is equivalent only because the *final* chunk's end equals the max sealed seq — a fragile, unguarded invariant.

**Bottom line:** the campaign's methodology is excellent; its *currency* is not. A green 2026-06-15 scorecard is being read as a statement about today's oracle, and it isn't one.

---

## 6. Tier-by-tier implementation status vs. `specs/oracle.md`

| Spec tier | Status | Evidence / gap |
|---|---|---|
| Storage & final-state | **Implemented, default** | `assertOracleMatches`, `Compare`, `Reconstruct`, `CheckInvariants`, `checkSegmentStructure`. Strong. Rev value unchecked (§4.3). |
| Event-log equivalence | **Implemented, default** | `assertFirehoseEventLogMatches` + `CompareEventLogMultiset`. Observes pre-compaction stream only; compaction-aware variant unwired (§4.2). Wall-clock poll (§4.6). |
| Client-driven historical (#77) | **Implemented, default — but near-vacuous** | `client_observer_test.go`. Drives the real client correctly; asserts almost nothing (§4.1). The single most important fix. |
| Live-tail `/subscribe` replay (#25) | **Absent as a distinct surface** | No oracle test issues mid-stream/boundary cursors, filters (`WithCollections`/`WithDIDs`), or JSON-vs-CBOR encoding checks. The only `Subscribe` call is the archive-path client. Issue #25 open. |
| XRPC segment egress (#26) | **Partial / absent at oracle level** | Only 2 `getTombstones` headers asserted (`overlay_integration_test.go:51-54`). No oracle test asserts ETag/checksum-matches-bytes/Range→206/If-None-Match→304/manifest-readiness. *These HTTP semantics are well-tested in `internal/xrpcapi/*_test.go` with fixtures* — but not as a simulator-driven oracle tier. |
| Crash & restart | **Implemented, gated** | Real subprocess SIGKILL at **4 enumerated crashpoints** (`restart_harness_test.go:49-67`). Skips under `-short`, so the default dev loop runs **zero** crash coverage. Post-restart asserts final-state only (§4.4). |
| Random-time kill loop (#29) | **Absent** | No random/predicate-driven kill point. *(An agent misread `restart_harness_test.go:283` as a kill-time RNG; I verified it is the seeded world-traffic RNG passed to `AttachRuntime` — the kill is deterministic/marker-triggered. #29 is genuinely open.)* m003 escape persists. |
| Store-fault (#30) | **Absent entirely** | No mechanism makes a store `Set`/`Commit` return an error mid-run. `crashpoint.Injector` only simulates process crash at boundaries — a different capability. m006 escape persists; `RESULTS.md` documents "the oracle has no way to make a store write fail." |
| Simulator fidelity / adversarial (#31) | **Absent** | Steady traffic is happy-path only: ASCII text, clamped sizes, 5 known lexicons, monotonic seqs, always-valid CBOR. No Unicode/oversized/unknown-lexicon/seq-gap/duplicate/malformed-frame/identity-event generation. Only CAR truncation + getRepo 503 + disconnect exist. (A test-only account-delete hook emits one real `#account` frame.) |
| Real-data corpus (#32) | **Absent** | No corpus fixtures or loader in `internal/oracle/`. The atmos closed loop (now widened by the shared client decoders) is unbroken. Issue #32 open. |
| Soak (#33) | **Absent** | No soak loop, no goroutine-leak detection (`goleak` is a transitive dep, imported nowhere). The 6h sweep is a per-seed *sweep*, not a long-running accumulation soak. |
| Determinism experiments (#34) | **Absent — spec-compliant** | No synctest/fake-time/in-process transport. Spec explicitly makes these adoption-on-measured-value and forbids fake I/O for durability/serving tiers. Absence is by-design, not a defect. |

---

## 7. The over-fit question, answered directly

You asked specifically whether the tests are "over-fit" — running lots of code under test without meaningfully asserting behavior. The honest, evidence-based answer:

**For the storage path: no.** `Compare`, `CheckInvariants`, `CheckCompacted` (in its over-retention direction), event-log equivalence, and overlay reconstruction all assert independently-derived contracts that can and do fail — proven by the mutation campaign killing hot-path data-shape bugs (m001, m004, m008, m011, m012, m017, m019, m020–m023). The fault tier proves its faults fired. This is not over-fit; it is rigorous.

**For the product/serving/client path: yes, in specific, now-identified places.**
- The client tier (§4.1) runs an enormous amount of real code (the entire archive negotiation) and asserts essentially nothing that ties it to ground truth. This is the textbook over-fit smell: maximum code-under-test, minimum assertion power.
- Compaction over-drop (§4.2) runs through every tier and is asserted by none.
- The rev branch in `Compare` (§4.3) is dead code that *looks* like an assertion.
- The restart tier (§4.4) runs real crashes but asserts only the surface that can't see the bug class crashes cause.

**Meta-point:** the system's bug-finding power is **inversely correlated with code novelty.** The oldest, most-reviewed storage code has the strongest assertions; the newest product-path code (client tier, added five days ago) has the weakest. The mutation scorecard — the tool designed to catch exactly this — is stale and has no mutants on the new code, so the over-fit went unmeasured. Re-running and extending the campaign is the mechanism that would have flagged §4.1 automatically.

---

## 8. External practice benchmarking (FoundationDB, TigerBeetle, Antithesis, Jepsen)

The oracle is correctly positioned as a "deterministic-enough" detector, not a single-threaded DST engine (the spec defers that, rightly, for real-socket/Pebble durability tiers). Against that stance, the highest-leverage imports:

1. **BUGGIFY-style in-code fault seams (FoundationDB's most-credited technique).** Today's faults hit only *external* surfaces (getRepo 503, CAR truncation, disconnect). The dangerous states (partial fsync, torn rename, watermark-vs-rewrite ordering, manifest races) live *inside* the server and are reachable only via the 4 SIGKILL crashpoints. Generalize `crashpoint.Injector` into a seeded, simulation-only hook that can inject a spurious *error* or *delay* (not just SIGKILL) at registered seams, each armed-once-per-run, fired probabilistically, with per-seam fired-counts surfaced like `UnfiredGetRepoHTTPFailures` so coverage is provable. This directly feeds the store-fault tier (#30) and attacks the m003/m006 escapes.

2. **A storage fault model (TigerBeetle's highest-value class) — the store-fault tier (#30).** fsync failure, short/torn write, rename-not-durable, read-back corruption at the Pebble/segment boundary, with fail-loud + no-corruption + no-silent-cursor-advance assertions, each fired-proven.

3. **"Sometimes" / reachability assertions (Antithesis).** Anti-vacuity is currently proven only for faults. Nothing proves the *interesting states* were reached: a cold-batch read happened, a replay spanned a compaction pass, a cutover occurred, a segment sealed mid-replay. Add cheap reachability counters to the trace and `require ≥1` at end-of-run — config drift could silently stop exercising these and every test still passes.

4. **A fault/seam coverage ledger (FoundationDB's `TEST()` macro hunt).** A single registry of all fault/crash seams with per-run fired counts emitted to the trace; the campaign flags seams never hit across a seed sweep. Will Wilson's own warning — "terrifyingly easy to build a DST system that appears to test a lot but explores little" — is exactly the §4.1/§7 risk, and this ledger is how you detect it.

5. **Metamorphic relations (cheap, no second implementation needed) to dent the atmos closed loop:** `replay(cursor=0)` prefix == `replay(cursor=k)` over the overlap; compaction is idempotent (second pass at same watermark is a no-op); seal is byte-stable; XRPC-decoded bytes == `/subscribe` bytes after normalization (partially present in `bisect`'s two-surface compare — extend it). These catch symmetric encode/decode bugs a single atmos-based expectation cannot.

6. **Liveness/progress assertion (TigerBeetle).** Generalize the existing "watermark advanced" check (`harness_test.go:266`) into an explicit contract: under a bounded recoverable fault set, the archive *must* advance its watermark within a deadline — so a stuck-but-not-corrupt regression fails loud instead of hanging to a test timeout.

7. **Multi-seed/coverage-guided search + shrinking.** Default is a single seed (42). The nightly sweep helps; pairing it with Go-native fuzzing or a coverage-recording loop that surfaces a *minimal* failing seed (shrink traffic/account/fault set) would make counterexamples actionable.

8. **Schedule the mutation campaign in CI (currently manual-only).** The scorecard drifted precisely because nothing runs `run.sh` automatically. A weekly job that fails on any KILLED→SURVIVED flip or STALE patch converts `RESULTS.md` from human-maintained prose into an enforced gate — and would have caught the current m022 staleness and the unscored client tier.

---

## 9. Prioritized recommendations

Ordered by (bug-class severity × inverse cost). I have **not** filed a GitHub issue per your instruction; this is the raw material for when you do.

**Do first (cheap, high leverage):**
1. **Upload the JSONL trace + goroutine dump on scheduled-sweep failure** (`oracle-scheduled.yml`). One CI step. Ends the current diagnostic blindness. (Maps to #35.)
2. **Wire `CompareEventLogsCompacted`** against the post-compaction on-disk stream with firehose-derived `want` (§4.2). The detector already exists and is unit-tested; it needs a caller. Closes the over-drop blind spot.
3. **Refresh m022's patch context** to `IsMaterialization()` and **re-run the full 21-mutant campaign at HEAD**; append a dated `RESULTS.md` section (§5). This is the spec's own contract (oracle.md:211,214).
4. **Add real comparators to the client tier** (§4.1): `Compare`/`CompareEventLogMultiset`/`CheckOverlayReconstruction` over the client stream, plus a contiguity/expected-count guard, plus stop swallowing download errors silently. This is the single most important *correctness* fix.

**Do next (moderate cost, high value):**
5. **Add event-log equivalence (+ compaction check) to the restart tier** from on-disk segments (§4.4). Attacks m003.
6. **Add mutants for the new/uncovered code**: client decode/plan/cutover (`internal/client/`), XRPC egress (`getsegment.go`), the fault-injection subsystem itself, and a compaction over-drop mutant. Without these, the newest and highest-churn product code has zero mutation evidence (§5).
7. **Convert the remaining wall-clock waits to deadlock-guards** (§4.6, #27), and add a pass-in-flight signal to the bisect race bracket.
8. **Run the scheduled stress/swarm sweep (and the mutation campaign) under `-race`** at low seed count (§4.6, #35).

**Build out (larger, spec-tracked):**
9. **Store-fault tier (#30)** — the FDB/TB highest-value fault class; retires m006.
10. **BUGGIFY-style internal fault seams + a fault-coverage ledger + "sometimes" assertions** (§8.1, §8.3, §8.4) — the framework that makes 5/9/11 measurable and prevents future §4.1-style silent vacuity.
11. **Random-time kill loop (#29)**, **simulator fidelity/adversarial inputs (#31)**, **real-data corpus (#32)** (+ golden segment to kill m009), **live-tail `/subscribe` tier (#25)**, and **soak (#33)**.

**Schedule the mutation campaign in CI** (§8.8) — sequence wherever convenient, but it is what keeps all of the above honest over time.

---

## 10. Other questions worth asking (asked & answered)

- **"Is a green default run a meaningful signal today?"** For storage correctness, yes. For product-path correctness, only weakly — §4.1 means the client tier can be green while the client drops data. Until rec. #4 lands, don't read a green client tier as "clients replay correctly."

- **"Does the fault swarm being on-by-default protect us?"** Yes for *injection being enabled* (`assertFaultPlanFired` would fail on an empty plan), but the swarm is deliberately bounded *inside* atmos's retry budget (so the durable model is unchanged). That means faults never push the system into a degraded/lossy regime — the fail-loud/bounded-drop contract under *budget-exceeding* faults is untested. The hot-DID schedule sits at exactly the retry ceiling (2×503 + 1 truncation + 1 clean = 4 attempts vs. 4 available) with **zero margin** and **no assertion** pinning `per-DID faults < maxRetries+1`; an atmos retry-count bump would silently turn this into a confusing backfill timeout. Cheap to guard with a compile-time/unit assertion.

- **"Is the design itself sound?"** Yes. The 11-tier separation, independent-ground-truth principle, fail-loud stance, and assertion-power-before-determinism ordering are all correct and well-argued. The gaps are *implementation completeness* and *currency*, not design. The one design-doc imprecision: `oracle.md:253-256` calls the direct-segment tier "the independent storage check," but that tier shares the `segment` byte-decoder with the server (§4.5); the phrasing should distinguish "independent of the compactor/overlay/client *logic*" (true) from "independent of the `segment` *codec*" (false — only the corpus tier delivers that).

- **"What would a single new bug most likely slip through today?"** A serving/client-path data-loss bug (client drops or staleness on `getSegment`/`getBlock`/overlay/cutover) — invisible per §4.1 — or a compaction over-drop of a later-superseded intermediate — invisible per §4.2. Both are squarely in the "replay holes / data loss" class the oracle exists to catch. Recs #2 and #4 close them.

---

## Appendix: artifacts

- Full adversarial-review output (40 findings with per-finding verifier reasoning, 8 tier maps, mutation apply-check, research): workflow run `wf_6832d588-73f`. Confirmed/partial findings: 33/40.
- Key files re-read by hand for this report: `specs/oracle.md`; `internal/oracle/{compare,invariants,compacted,overlay,eventlog,groundtruth,model,bisect,config,client_observer_test,harness_test,restart_harness_test}.go`; `testing/mutation/RESULTS.md` + `mutants/m022…patch`; `.github/workflows/oracle-scheduled.yml`; `internal/tombstone/tombstone.go`.
- One inter-agent discrepancy resolved by hand: the "random-time kill loop at restart_harness_test.go:283" claim is **wrong** — it is the seeded world-traffic RNG; #29 remains open.
