# Oracle Test Reproducibility — Living Brainstorm

Status: **ACTIVE.** Working document we edit as we go. Foundations (atmos
Conn/Dial seam, jetstream LiveDial + HTTPTransport + Headless, seeded jitter, and
a lightweight ingest/compaction synctest tier) are implemented and committed.
Now executing the FULL-BUBBLE EXECUTION PLAN below.

Owner: Jim (jcalabro). Started 2026-06-24.

## Problem statement

The `oracle-scheduled` CI sweep fails with some regularity, and **we have
repeatedly (≈8 times) been unable to reproduce those failures locally.** A test
that fails in CI but passes everywhere we can observe it is not a useful test —
it is noise. Noise erodes trust: once we learn to shrug at a red oracle run, the
suite stops protecting us, and a *real* regression hiding in the noise ships.

The goal of this document is the **meta-problem**, not any specific application
bug:

> Make oracle failures **reproducible and actionable**. When CI goes red, we want
> to be able to (1) reproduce it locally as a red test, (2) attach a debugger,
> add logging/tracing/metrics, and gain real visibility, (3) implement a
> candidate fix, and (4) re-run to watch it go green — and trust that green.
>
> It should be **extremely rare** that an oracle run fails in CI but passes
> locally.

Fixing the actual application bugs the oracle finds is explicitly **out of scope
here** and comes *after* the suite is trustworthy.

Non-goal: bit-for-bit determinism. This is Go — the goroutine scheduler, GC, and
real I/O make perfect reproducibility impractical, and we are not chasing it. We
want *substantially more* reliability and reproducibility, not a proof.

---

# EXECUTION PLAN (active, 2026-06-25)

## Sequencing decision (revised 2026-06-25 after independent review)

An independent review caught a real contradiction and a sequencing error in the
first draft of this plan, both confirmed against the code:

- **WI-9 (forced-interleaving reproducer) is INDEPENDENT of WI-1–8.** The shipped
  lightweight tier (`TestOracle_Synctest`) ALREADY drives the resync seam
  (`GenerateSilentMutationThenSyncForTest`, synctest_test.go:210) and ALREADY runs
  `CheckCompacted` (line 231). The triaged CI bug (#100/#106 superseded-row /
  over-drop) lives exactly there. So the on-demand reproducer needs nothing from
  the full-bubble work.
- **The full bubble does NOT serve the reproduction goal.** By its own honest
  limitation, synctest fakes TIME, not goroutine SCHEDULING — a seed still does
  not pin an interleaving. What reproduces the correctness bug is forced
  interleaving (`synctest.Wait()` between the resync and the compaction pass),
  i.e. WI-9 — which sits on code that already ships.
- **The earlier doc decision (§"do NOT full-bubble now") was reversed without
  reconciliation.** Resolving that now: the full bubble remains DEFERRED as a
  separate *noise-elimination + speed* play (it kills the `-race`-lane liveness
  false-failures and makes the serving tiers gateable — real, but orthogonal to
  reproducing the correctness bug). It is NOT the path to the stated goal.

REVISED ORDER:
1. **WI-9 FIRST** — forced-interleaving reproducer on the existing lightweight
   tier. Small, serves the actual goal, validates the reproduction premise before
   any expensive refactor. ← BUILDING NOW.
2. **Reassess the full bubble** as a separate decision once WI-9 proves the
   premise. If pursued, fold in the four gaps the review found (see "Full-bubble
   gaps" below).

## Full bubble (DEFERRED — value, precise)

What it WOULD buy: (1) whole oracle in ms → gateable on every push; (2) the
`-race`-lane "barrier not reached / runtime did not exit" timeout noise becomes
IMPOSSIBLE under a fake clock; (3) the near-vacuous serving/client tiers become
cheap to exercise hard. What it does NOT buy: reproducing the
interleaving-dependent correctness bug (that's WI-9). Deferred until after WI-9.

### Full-bubble gaps the review surfaced (fold in IF we build it)
1. **Fanout is drop-on-full** (`simulator/fanout` Publish: `select{ case ch<-:
   default: drops++ }`) — a dropped frame is avoidable input nondeterminism
   (oracle.md:63) and a silent fallback. The full tier must assert `Drops()==0`
   or prove reconnect-recovery. Not an issue for the lightweight tier (firehoseConn
   replays from `?cursor=` on reconnect) but the competing-goroutine quiescence
   window differs under the full bubble.
2. **`t.Cleanup(client.Close)` + background-ctx watcher goroutines are
   bubble-illegal** (client_observer_test.go:86,92). `t.Cleanup` runs AFTER the
   bubble fn returns, but all bubble goroutines must exit BEFORE it returns →
   "blocked goroutines remain" panic masks the real failure. Every observer's
   Close + watcher must be drained INSIDE the bubble.
3. **`-race` happens-before**: reading shared observer state after `synctest.Wait()`
   needs Wait() to establish the edge or `-race` flags it. WI-8 wants `-race`
   clean — coupled, must be explicit.
4. **Ticker poll loops** beyond `require.Eventually`: `WaitAfter`
   (harness_test.go:776) and `waitForRuntimePublicURL` (client_observer_test.go:25)
   are `time.NewTicker(10ms)` loops. NOTE: they block in a `select` with no
   `default`, so under synctest's faked ticker they DO advance the clock (not a
   hang) — but `waitForRuntimePublicURL` needs rework anyway (no public addr
   in-bubble), and all such loops should be audited.
5. **WI-3 justification is FAULT FIDELITY, not "CAR streaming."** getRepo
   CAR-truncation faults model a mid-stream connection drop; a ResponseRecorder
   yields a complete-but-short body (a different failure mode than a reset). The
   PipeListener-for-simulator is justified specifically by reproducing the reset.

Foundations already shipped this session (do not redo): atmos `Conn`/`Dial` seam
(committed `15ff15c`); `live.Config.Dial` ← `orchestrator.Config.LiveDial` ←
`jetstreamd.Options.LiveDial` (`7da62b0`); `jetstreamd.Options.HTTPTransport`
(`233a433`); `jetstreamd.Options.Headless` + the lightweight ingest tier
(`d062e52`); seeded backoff jitter (`94c2ff8`); `oracle/inprocess.go`
(`firehoseConn`, `handlerTransport`, `inProcessDial`). The lightweight tier
(`TestOracle_Synctest`) is the proven base to grow from.

## Ground rules (synctest invariants — verified against Go 1.26 docs + spikes)

- Fake clock advances ONLY when every bubble goroutine is durably blocked
  (channel/cond/WaitGroup/`time.Sleep`). Network/file I/O and runnable goroutines
  are NOT durably blocked → no real sockets, no spin loops.
- ALL bubble goroutines must exit before the bubble fn returns, else
  "blocked goroutines remain" panic MASKS the real failure. Always `defer` a
  drain that cancels, waits `rt.Run`, AND calls `rt.Close()` (verifier pool).
- Advance the bubble clock past the simulator's logical-clock epoch (~2023)
  before starting the runtime (verifier rejects >5m-future revs). Already solved
  by `advanceClockToSimulatorEpoch`.
- One bubble per process (global zstd encoders bind goroutines to the first
  bubble). Re-runs = separate `go test` invocations.
- `websocket.Accept` needs `http.Hijacker`; a real `http.Server.Serve` over a
  `net.Pipe`-backed listener supplies it (spike-confirmed). `websocket.Dial`
  accepts an `HTTPClient`, so a PipeListener-backed `http.Client` dials the
  runtime's `/subscribe` in-memory.

## Work items (check off as completed; keep notes inline)

### WI-1 — PipeListener primitive  [ ]
A `net.Listener` whose `Accept()` blocks on a channel (durably blockable) and
returns `net.Pipe()`-backed conns, plus a paired `DialContext`/`http.Client`
that connects to it in-process. Lives in `internal/oracle` (test-support, non-
`_test.go` so it's importable). One implementation reused for: runtime public
server, and the client-observer's HTTP client.
- [ ] Implement `pipeListener` (Accept/Close/Addr) + `pipeDialer` (DialContext).
- [ ] Build an `*http.Client` whose `Transport.DialContext` → the listener.
- [ ] Unit test: `http.Server.Serve(pipeListener)` + a websocket Accept/echo +
      graceful `Shutdown`, all inside a `synctest.Test` bubble, fake clock
      advances. (Re-confirm the verifier's spike at home in this repo.)
- Risk: close/EOF + concurrent Accept correctness. Mitigate with the unit test.

### WI-2 — Server listener injection  [ ]
Let `internal/server` serve on an injected listener instead of binding TCP.
- [ ] Add `Config.PublicListener` / `Config.DebugListener` (`net.Listener`, nil
      = bind TCP as today). Branch `Run` at server.go:166/171 to use them.
- [ ] Preserve `PublicAddr()`/`DebugAddr()` (read `ln.Addr()` either way).
- [ ] Thread `jetstreamd.Options.PublicListener`/`DebugListener` →
      `server.Config` in runtime.go:391. (Keep `Headless` for the ingest-only
      tier; the full tier passes listeners instead.)
- [ ] Existing server unit tests still green.
- Risk: low (mechanical). The handlers are unchanged.

### WI-3 — Simulator over the in-process transport  [ ]
Serve the simulator mux without `httptest.NewServer`.
- [ ] In the full-bubble harness, run the simulator handler on a real
      `http.Server` bound to a PipeListener (preserves CAR streaming + getRepo
      CAR-truncation fault fidelity that a ResponseRecorder would blur).
- [ ] Runtime's outbound `HTTPTransport` + `LiveDial` target the simulator
      (HTTPTransport already proven; LiveDial firehoseConn already proven).
- [ ] Decide: keep `handlerTransport` (ResponseRecorder) for the ingest-only
      tier, use the PipeListener server for the full tier. Document why.

### WI-4 — Public client live-dial seam  [ ]
The client-observer's live `/subscribe-v2` cutover must dial in-process.
- [ ] DECISION (verified options): `internal/client.Config.Dial` ALREADY exists
      (engine.go:66) but the root `jetstream` engine never sets it, and
      `websocket.Dial` accepts an `HTTPClient`. PREFER reusing the existing
      `WithHTTPClient` (options.go:204) by threading `cfg.httpClient` into the
      live dial's `websocket.DialOptions.HTTPClient` — avoids a NEW public
      `WithDial` API. Fallback: add `WithDial` if the HTTPClient path can't carry
      the in-memory transport through the WS upgrade.
- [ ] Implement the chosen path in root `engine.go` + `internal/client/live.go`
      (`liveDialOptions` takes the client's HTTPClient).
- [ ] Confirm `assertTypedLikeBackfill` (uses `WithBackfillOnly`, no live tail)
      still works; only `collectClientBackfill` needs the live seam.
- Risk: this is the one PUBLIC API-surface area. Keep it minimal; if `WithDial`
  is needed, design the type with only stdlib/atmos types.

### WI-5 — Client-observer transport wiring  [ ]
Point the observer tier at the runtime in-process.
- [ ] One shared in-process `*http.Client` (PipeListener-backed) passed to the 3
      `jetstream.Subscribe` sites (client_observer_test.go:81/245/289) via
      `WithHTTPClient`.
- [ ] Replace `http.DefaultClient` at overlay_integration_test.go:29 with it.
- [ ] `waitForRuntimePublicURL` returns the in-process base URL (no real addr).

### WI-6 — OTEL/metrics guard  [ ]
- [ ] Confirm the oracle sets no `OTEL_EXPORTER_OTLP_*` (it doesn't) → tracing is
      noop, no exporter goroutine. Add a defensive assertion/comment. (Likely a
      no-op; verified `otlpConfigured()` gates the only network exporter.)

### WI-7 — Full-bubble harness entrypoint  [ ]
The big one. A new `TestOracle_DefaultLifecycle_Synctest` (or refactor the
existing lifecycle body to run in both modes) that runs the WHOLE lifecycle in a
bubble.
- [ ] Extract the lifecycle body so it can run with either real sockets (today)
      or in-bubble in-process transports, to avoid duplicating ~380 lines.
- [ ] Move world setup + bootstrap inside the bubble (heavy synchronous work; OK,
      it doesn't block the bubble).
- [ ] Move the client-observer assertions into the bubble; replace
      `require.Eventually`/wall-clock polls with channel blocks or
      `synctest.Wait()` (Eventually's real-time tick loop won't progress under a
      fake clock).
- [ ] Drain: `defer` cancel + `rt.Run` wait + `rt.Close` + simulator server
      Shutdown + world Close — ALL bubble goroutines must exit.
- [ ] Verify barriers (`phaseGate`/`seqAck`) work under the fake clock (they use
      channels + `time.NewTimer`, both faked — should be fine; confirm).
- Risk: HIGHEST. Whole-graph quiescence. Expect iterative hang/panic debugging;
  use goroutine dumps to localize the non-durably-blocked goroutine each time.

### WI-8 — Validation & stabilization  [ ]
- [ ] Passes `-count=1` reliably; `-race` clean.
- [ ] 20+ separate-process runs green (stability; not `-count` due to one-bubble
      rule).
- [ ] Wall-clock runtime recorded (target: seconds, not minutes).
- [ ] Existing real-socket `TestOracle_DefaultLifecycle` still passes unchanged.
- [ ] `golangci-lint` clean; `just` recipe added (`just oracle-bubble`).
- [ ] Doc + decision log updated; atmos `replace` resolution noted.

### WI-9 — Forced-interleaving reproducer  [~] DONE (staging) — see outcome below

OUTCOME (2026-06-25): `TestOracle_Synctest` now deterministically STAGES the full
seam — steady traffic → silent-mutation+sync → silent-mutation+commit (async
resync) → late account-delete tombstone → a compaction pass crossing the
tombstone seq — and asserts `CheckCompacted` + `CheckInvariants` + `Compare` on
the durable segments, with an anti-vacuity guard (tombstone actually observed).
Committed `a819d69`. GREEN across 12 seeds incl. the 3 CI-failing ones; `-race`
clean; ~0.2s.

HONEST RESULT: it does NOT spontaneously reproduce the CI failure. Confirmed root
reason (matches the doc's standing limitation): synctest fakes TIME, not
goroutine SCHEDULING — replaying a CI seed does not replay its interleaving. The
test pins the ORDER of staged operations (via acks + the compaction-crossing
wait) but not the fine-grained goroutine interleaving inside the runtime. So:
- It is a strong REGRESSION GUARD on the seam, fast and socket-free.
- It is the SUBSTRATE for true on-demand reproduction, which needs a code seam to
  force the bad micro-interleaving — specifically a yield/hook at
  `dropStaleOrderedAsyncResync` (consumer.go) so the test can interpose the
  stale-order drop decision relative to the compaction snapshot. That hook does
  NOT exist yet → tracked as WI-10 below.

### WI-10 — Yield seam: INVESTIGATED, seam NOT added (2026-06-25)

A multi-agent investigation + adversarial verification (workflow
`wi10-interleaving-map`) mapped the survivor mechanism against the real code. The
adversarial pass REFUTED the proposed H1 consumer-side seam, and I independently
confirmed the two load-bearing facts by reading the code directly:

1. **CheckCompacted keys on the MAX DID tombstone** (compacted.go:33,41,51 — it
   keeps `ev.Seq > didTombstones[did].seq`). The trace failure is "superseded
   ACCOUNT row survived ... tombstone_seq=30235" — keyed off the account-delete at
   30235, NOT the dropped async KindSync at 28989. So forcing the H1 stale-resync
   drop (which only removes the 28989 sync tombstone) cannot produce THIS failure:
   the 30235 account tombstone already dominates (30235>421 drops seq=421 via
   ShouldDrop regardless of the sync).
2. **A steady pass re-examines EVERY sealed segment against a fresh snapshot**
   (listSealedCompactionSegments, compact_deletes.go:200-222 returns all sealed
   files; applyCompactionChunk offers all of them, compact_deletes.go:360). Steady
   mode is SINGLE-CHUNK (`chunkEnd=targetWatermark`, compact_deletes.go:138; the
   `for current<targetWatermark` loop runs once), so the H2 "Evict-by-chunkEnd
   then a later pass skips the old segment" window is STRUCTURALLY IMPOSSIBLE in
   steady mode (it only collects/chunks in merge-tail mode). The account tombstone
   is observed under the writer mutex via OnAppend BEFORE its Append returns, and
   the cap-trigger fires AFTER Append, so the tombstone is provably in the crossing
   pass's snapshot → seq=421 is dropped → the forced H1 test goes GREEN.

CONCLUSION: the proposed consumer-side prod seam gates the WRONG invariant and a
forced H1 (and H1+H2-in-steady) test would be GREEN on current code. **Do NOT add
the production seam.** Decision: NOT adding `onBeforeStaleResyncDrop` (or any new
prod hook) — it would be dead code justified by a refuted hypothesis.

WHERE THE REAL BUG LIKELY LIVES (redirected triage, for the eventual bug-fix
work, not this reproducibility track):
- An **atmos-internal resync DELIVERY ordering** issue: the async KindSync/account
  events arriving with a seq ABOVE a newer commit's rows, or the account-delete
  itself being reordered/dropped — only reachable with the atmos Conn/Dial
  frame-injection seam (Option α), not a jetstream compaction seam.
- OR a **merge-tail-mode** pass (NOT steady) where `collectCompactionTombstones`
  + `CompactionTombstoneCap` chunking + the `f.header.MaxSeq<=watermark` segment
  filter (compact_deletes.go:269) genuinely CAN skip a segment. A merge-tail
  forced-interleaving test is the more promising reproduction target than the
  steady consumer seam.
- OR a crash/rebuild between Observe and the pass (rebuildLiveTombstones).

WHAT WE KEEP: the WI-9 staged tier remains a strong deterministic regression guard
for the steady resync+compaction seam (proven correct under these orderings). The
investigation's real value: it RULED OUT the steady-mode H1/H2 hypotheses with
code evidence, which is genuine progress on the standing triage.

### WI-10 (original, NOT pursued) — Yield seam for true forced micro-interleaving  [x] superseded by the finding above
- [ ] Add a test-only hook (build-tagged or an injected no-op func) at the
      `dropStaleOrderedAsyncResync` decision point and/or the compaction snapshot,
      so WI-9's staging can pin the EXACT order that produces superseded-row.
- [ ] Drive it from the synctest tier with `synctest.Wait()` between the resync
      drop and the crossing pass; assert `CheckCompacted` goes RED, then GREEN
      after the production fix. THIS is the on-demand bug reproducer.
- Decision to raise with Jim: is the seam worth adding to production code (it is
  test-only and inert in prod), or do we accept the regression-guard + the
  artifact-driven diagnosis we already proved works on the triaged bug?

### WI-9 (original full-bubble follow-on text retained for reference)  [ ] ← superseded by the OUTCOME above
On the existing lightweight `TestOracle_Synctest` (NO full-bubble work needed),
drive the resync-vs-compaction seam deterministically and assert the compaction
contract, so a real storage-path failure reproduces on demand (red → green on a
fix). This is the item that serves the stated reproduction goal.
- [ ] Extend the lightweight tier (or a sibling test) to stage the bad ordering:
      generate steady traffic, a silent-mutation+sync (resync rows), and a late
      account-delete tombstone for the same DID, using `synctest.Wait()` between
      steps to pin the state the runtime has reached at each point.
- [ ] Force a compaction pass that crosses the tombstone at a controlled
      watermark (the runtime exposes a compaction trigger via TombstoneCap / the
      compaction interval; drive it deterministically rather than by wall clock).
- [ ] Assert `CheckCompacted` / `CheckInvariants` on the resulting on-disk
      segments — this is where superseded-row-survived shows up.
- [ ] Anti-vacuity: assert the seam actually fired (resync rows present, tombstone
      observed, a pass crossed it) so a green result can't be vacuous.
- [ ] If it reproduces a real defect: capture it, hand off to the (separate) bug
      fix. If it does NOT on current code: document that the seam is currently
      correct here and the harness is ready to catch a regression.

## Sequencing & checkpoints (revised)
WI-9 FIRST (now). Then reassess the full bubble (WI-1 → WI-2 → (WI-3 ‖ WI-4) →
WI-5 → WI-6 → WI-7 → WI-8) as a SEPARATE noise/speed decision.
Hard checkpoints to raise with Jim: (a) after WI-9, review what it reproduces and
DECIDE whether the full bubble is worth the XL cost; (b) if the full bubble is
pursued, at WI-1 if the PipeListener+websocket spike misbehaves; (c) at WI-4 if
reusing `WithHTTPClient` fails and a new public `WithDial` is required; (d) at
WI-7 if whole-graph quiescence proves intractable after a timeboxed effort.

---

## Why this is happening (contributing factors)

We do not believe in a single root cause; these are the contributing factors,
grounded in code we read on 2026-06-24.

1. **The seed fixes inputs, not interleavings.** `JETSTREAM_ORACLE_SEED` seeds
   the simulator world, traffic, and fault schedule (via salt-decoupled PCG
   streams) — i.e. *what* happens. It does **not** seed goroutine scheduling,
   fault-vs-retry timing, or socket frame ordering — i.e. *when/what order*. The
   `justfile` repro hint says this out loud. The failures we see are
   interleaving-dependent, and the seed does not pin the interleaving.

2. **Environment mismatch (CI vs local).** The most recent failing race-lane run
   recorded `gomaxprocs=4` (GitHub's shared runner); a dev box is often 8–32
   cores and idle. The Go scheduler behaves completely differently at
   GOMAXPROCS=4-under-contention vs 32-idle, so the race windows that open in CI
   rarely open locally. We confirmed this empirically: a 58-iteration local loop
   of a known-failing seed at `GOMAXPROCS=2` on a 32-core idle box reproduced
   nothing.

3. **Real wall-clock dependence.** The runtime under test uses real time in many
   places. The compaction cadence (`time.NewTimer(CompactionInterval)` +
   `time.Since(lastPass)` + a hard-coded `minCompactionTriggerSpacing = 30s`),
   reconnect backoff, ticker-driven overlay/ping loops, and several harness
   deadlines (30s / 5min / 10s) all key off the clock. "Fast machine green, slow
   runner red" is the classic signature of wall-clock dependence, and we see it.

4. **`-race` slowdown changes outcomes.** The race detector is 5–15× slower. Some
   CI failures appear **only** in the race lane (`after-bootstrap barrier not
   reached` / `runtime did not exit after cancellation`) — these are very likely
   the wall-clock deadlines being blown by the slowdown, i.e. **not real bugs**,
   just timeouts. They are diluting the signal from the genuine correctness
   failures.

5. **Two failure classes wear one costume.** We are conflating:
   - **Correctness** failures (e.g. `superseded row survived`) — appear in *both*
     race and non-race lanes; these are genuine, interleaving-triggered defects.
   - **Liveness/timeout** failures (`barrier not reached`, `did not exit`) —
     race-lane only; almost certainly deadline noise.
   Treating them as one bucket makes the whole suite read as "flaky."

6. **A documented async-resync ordering race exists in the runtime.**
   `internal/ingest/live/consumer.go:dropStaleOrderedAsyncResync` exists *because*
   atmos delivers async-resync events "on a separate channel from the ordered
   result stream." Its comment says the root fix is "ordered delivery in atmos
   (compaction spec §12)." This is a real, non-deterministic seam — and it's at
   the heart of the `superseded row survived` failures we triaged.

7. **The artifact captures observations, not decisions.** The JSONL trace records
   what the oracle *saw* (events, compaction watermarks) but not the daemon's
   *internal causal decisions* (which tombstones a pass's snapshot held, what
   `Evict` removed, when a resync was dropped). So an artifact tells us *that*
   something diverged, not *why* — forcing source-reading and theorizing instead
   of reading the answer.

## Existing stance (what the spec already says)

This is not greenfield. `specs/oracle.md` already stakes out a deliberate
position we are now revisiting *because it is producing noise*:

- §"deterministic enough" (oracle.md:56–76): perfect DST is explicitly **not** a
  near-term requirement. Target is: seed world/traffic/faults; keep avoidable
  nondeterminism out of observed input bytes; use barriers + durable-append acks
  instead of timing; emit traces so interleaving-dependent failures are
  *diagnosable even when not replayable*.
- §traces (oracle.md:184–201): the JSONL trace is "the practical substitute for
  perfect deterministic scheduling."
- §determinism experiments tier (oracle.md:363–369): `testing/synctest`,
  in-process transport, fake time, in-memory stores are sanctioned as
  **supplemental logical tiers**, adoption gated on *measured value* — and
  explicitly **forbidden** as replacements for the real-socket/real-time crash,
  durability, and public-serving tiers.

The tension we must resolve: the spec bet on "traces make failures diagnosable
without reproduction." For the bug *I* triaged this session, the trace was good
enough to root-cause without reproduction — but the lived experience (8 failed
repros) says that bet is not paying off broadly. Either the traces aren't rich
enough yet (factor 7), or we genuinely need reproducibility (Jim's position), or
both. This doc treats **both** levers as in-play and not mutually exclusive.

## Current state of the code (determinism readiness, 2026-06-24)

Clock injection — *partially* there already:

- **Injectable today:** live consumer (`cfg.now`), backfill handler (`h.now`),
  backfill retry runner (`cfg.now`), merge runner, slow detector, identity cache,
  status collector, backfill store (`var timeNow`, package-level). Live reconnect
  backoff is injectable via `streaming.BackoffPolicy` and the oracle already sets
  it to ~1ms no-jitter.
- **NOT injectable today (blockers):** steady compactor timer +
  `time.Since(lastPass)` + hard-coded `minCompactionTriggerSpacing`; backfill
  retry/selected `time.NewTimer`; subscribe ping ticker; overlay cache rebuild
  ticker; manifest `ForwardCursor` lookback (`time.Now().UnixMicro()` in serving
  path); orchestrator phase-write timestamps; **unseeded `rand/v2` jitter** in
  `backfill/selected.go:selectedBackoffDelay`.

Transport — seams are better than expected:

- `ConvertEvent(streaming.Event, …)` and `Consumer.processBatch(ctx,
  []streaming.Event)` are **transport-agnostic** — they consume plain structs,
  not a socket. Good seam for in-process feeding.
- **But** `streaming.Client` (atmos) is a **concrete type, not an interface**, and
  is constructed internally by `streaming.NewClient` inside `Consumer.Run`. To
  feed events in-process we'd need an interface seam (wrapper in jetstream, or an
  exported interface in atmos). atmos's internal backoff/retry/verifier-resync
  goroutines also use real time we don't control.

CI capture today (`.github/workflows/oracle-scheduled.yml` + `justfile`):

- Uploads on failure: the **JSONL trace**, `gotestsum.jsonl`, and
  `test-output.log` (carries the `GOTRACEBACK=all` goroutine dump on timeout).
- Does **NOT** upload: the segment data dir, pebble state, or the runtime
  datadir. (See Strategy 2 — this is a cheap, high-value gap.)
- Race lane: `-race`, 3 seeds, 90m/seed. Non-race lane: 10 seeds, 30m/seed. No
  explicit `GOMAXPROCS` pin in CI (uses runner default, observed 4).

## The four starting strategies

These came out of the initial brainstorm. They are **complementary**, ordered
roughly by cost/leverage. Detail and status will evolve.

### Strategy 1 — Flight-recorder: make failures carry their own root cause

Record the daemon's *causal decisions*, not just the oracle's observations, into
the existing JSONL trace (gated behind an oracle/debug build tag so production
is untouched). Candidate decision points:

- every tombstone `Observe` (did, seq, kind, reason);
- each compaction pass's snapshot **digest** (DIDs/records + seqs it held),
  `chunkEnd`, and what `Evict` removed at the chosen watermark;
- `dropStaleOrderedAsyncResync` firings (did, resync_rev, chain_rev) — the
  suspected seam;
- segment/bloom prefilter skip decisions.

Payoff: turns "seq=421 survived a tombstone at 30235" from a mystery into a
trace line. **This is the highest-leverage move for the "I can't act on the
failure" pain**, and it complements (does not replace) reproducibility.

Cost: low–medium. Risk: trace volume; mitigate with digests + a verbosity gate.

### Strategy 2 — Separate deterministic *detection* from non-deterministic *generation*

The run that *produces* bad on-disk state is non-deterministic, but the checks
that *detect* it (`CheckCompacted`, `Reconstruct`, `CheckInvariants`,
`Compare`) are **pure functions over the on-disk bytes** — 100% deterministic.

So: **on failure, tar + upload the actual `segments/` dir + pebble state (+ the
in-memory tombstone-set snapshot).** Locally, replay the deterministic checker
half against the exact failing bytes — reproduces every time. We don't reproduce
the *race*; we reproduce the *check*, which is enough to (a) confirm the bytes
are genuinely wrong vs a serving/transport artifact, and (b) iterate a fix
against a frozen red fixture. The bisect machinery (`bisect.go`) already wants
exactly this disk image.

Payoff: high; converts "irreproducible" → "trivially reproducible offline" for
the entire *durable-defect* class. Cost: low (mostly CI capture + a load path).
Caveat: only covers defects that are durable on disk; a pure
serving/timing-only defect won't be caught by replaying static bytes.

### Strategy 3 — When we *do* hammer, hammer like CI

Raise the local hit-rate by matching the conditions that open the windows:

- **Pin `GOMAXPROCS` to the CI value by default** in the oracle test (not just in
  a repro hint) so local == CI without anyone remembering a flag.
- Run under **real CPU contention** (cgroup cpu quota / `taskset` to 4 cores +
  background load) to mimic a noisy shared runner.
- Hammer **random seeds**, not just the failing one (the seed doesn't fix the
  interleaving, so breadth × contention beats replaying one seed).
- Package as a one-command `just oracle-hammer` target.

Payoff: moves local repro from ~0% to nonzero; cheap. Limitation: still
probabilistic — does not by itself give a reliable red test.

### Strategy 4 — Cut the false-positive noise at the source

Stop liveness flakes from masquerading as correctness alarms:

- **Classify + tag** failures in the CI summary (correctness vs liveness/timeout)
  so a timeout never reads as data corruption.
- Make `-race`-lane deadlines `-race`-aware (scale them) or event-driven, killing
  a chunk of the race-lane-only noise.
- **Surgically auto-retry only the liveness class once** before failing. **Never**
  retry a correctness failure — those are the gold we're trying to keep.

Payoff: immediate noise reduction; cheap. Limitation: cosmetic/triage — does not
improve reproducibility of the real defects (that's 1/2/3 and the determinism
work below).

## The bigger question: a "relatively deterministic" oracle

Jim's framing: can we make event emission order, check cadence, and the
runtime-under-test behave like a **seeded state machine** — relatively
deterministic, not perfectly so — so the *same seed reliably reproduces the same
failure*? This is the part that would give a true red→green debugging loop. Open
research; options below, smallest-blast-radius first.

### Option A — Finish clock injection + drive a fake clock (synctest)

Route *every* `time.Now/After/Timer/Ticker/Sleep` in the runtime through one
injected clock interface, then in the oracle drive a **fake clock**. Go 1.25+
`testing/synctest` (we're on 1.26) gives a fake clock *and* a "all goroutines
durably blocked" signal that replaces wall-clock polls/deadlines with
event-driven waits.

- Removes failure factors 3 (wall-clock) and 4 (`-race` deadline noise) wholesale
  — an entire class of CI-only failures.
- The clock surface is already ~50% injected (see readiness above); the blockers
  are enumerable (compactor timer/spacing, backfill timers, tickers, the unseeded
  jitter, manifest lookback).
- **Hard constraints / unknowns:**
  - The spec forbids fake-time for the real-socket durability/serving tiers.
    synctest also does not play well with real network goroutines (the websocket
    + httptest server are real OS threads/sockets outside synctest's bubble). So
    this likely applies to a **logical, in-process tier**, not the full
    real-socket harness — which intersects Option B.
  - atmos's internal time (backoff/retry/resync worker) is outside our process
    boundary unless atmos exposes a clock. Needs investigation.
- Verdict: **high leverage, medium cost, but probably requires Option B's
  in-process transport to be fully effective** because real sockets won't live in
  a synctest bubble.

### Option B — In-process deterministic transport (logical tier)

Replace the real websocket/HTTP between simulator and runtime with a direct
in-memory hand-off, so event delivery order is controlled, not socket-timed.

- Seam quality is good on the jetstream side (`processBatch`/`ConvertEvent` take
  plain `[]streaming.Event`). The obstacle is `streaming.Client` being a concrete
  atmos type built inside `Consumer.Run`. Needs an interface seam (jetstream
  wrapper or atmos change) so the oracle can inject a channel-backed client that
  the simulator fanout feeds directly.
- Combined with Option A's fake clock, this is the realistic path to "seed
  reliably reproduces the failure" for the *logical* correctness tiers
  (compaction, tombstone, resync ordering).
- **Constraint:** by spec, this is a *supplemental* tier. The real-socket,
  real-time, real-pebble crash/durability/serving tiers **stay** as-is — those
  catch a different bug class and must not be moved into fake I/O. So this is
  additive coverage that's reproducible, not a replacement.
- The async-resync ordering race (factor 6) is partly *inside atmos*. An
  in-process tier could deterministically inject the bad ordering (resync vs
  later commit) and turn that whole seam into a reproducible unit-style test —
  arguably the single most valuable target, since it's where the real bugs are.
- Verdict: **highest reproducibility payoff for correctness defects; highest
  cost**; needs an atmos seam.

### Option C — Deterministic check cadence

Independent of transport/clock: make compaction passes fire at
**deterministic points relative to ingestion** (e.g. a test-only hook that
triggers a pass after N events / at a barrier) instead of timer + 30s-spacing +
coalesced trigger. Removes a major source of "which pass straddled which event"
nondeterminism that sits underneath the `superseded survived` failures.

- Lower cost than A/B; can land independently and *also* helps A/B.
- Risk: a test-only cadence diverges from production cadence, so we'd keep a
  production-cadence tier too. Metamorphic: same inputs, different cadences,
  same final state.

### Option D (rejected) — `rr` record/replay

Rejected by Jim: flaky and poor Go support (Go's scheduler/GC and many threads
fight `rr`; it serializes onto one core, changing the very interleaving we want
to study). Not pursuing.

### Option E (parked) — Antithesis / full DST engine

Commercial deterministic hypervisor (Antithesis) or a FoundationDB/TigerBeetle
single-threaded DST engine. Highest reproducibility, highest cost / biggest
rearchitecture; the spec explicitly defers this. Parked, not rejected — revisit
if A/B/C prove insufficient.

## Working hypothesis on sequencing (to debate, not decided)

1. **Strategy 4 + Strategy 2 first** — cheapest, immediate: stop the liveness
   noise, and start capturing the disk image so *durable* failures become
   replayable offline today.
2. **Strategy 1** — enrich the trace with causal decisions; pairs with 2 to make
   real failures self-diagnosing.
3. **Strategy 3** — `just oracle-hammer` with GOMAXPROCS-pinned + contention, for
   the cases we still want to chase live.
4. **Option C then A then B** — the determinism ladder, smallest blast radius
   first, each gated on whether the prior rung closed the gap. B is the endgame
   for a reliable seed→failure red test on the logical tiers, and depends on an
   atmos interface seam.

## Open questions / research TODO

- [ ] Confirm exactly which harness deadlines are wall-clock vs event-driven, and
      which would move into a synctest bubble cleanly vs depend on real sockets.
- [ ] Does atmos expose (or can it expose) a clock injection point and an
      ordered-delivery mode for resync? (Factor 6 / compaction spec §12.) This
      gates Option B's completeness.
- [ ] Is `streaming.Client` wrappable behind a jetstream-side interface without an
      atmos change? Sketch the seam.
- [ ] Quantify: of the last N scheduled failures, how many are correctness vs
      liveness? (Validates Strategy 4's premise that liveness is much of the
      noise.) Need the failure history, not just the 4 runs triaged so far.
- [ ] Seed the `rand/v2` jitter in `backfill/selected.go` (or make it injectable)
      — small, removes one concrete nondeterminism source regardless of path.
- [ ] Measure trace size impact of Strategy 1 before committing to always-on.
- [ ] Decide: do we relax the spec's "traces, not reproduction" stance, or double
      down on traces (Strategy 1) — or formally adopt "both"?

## Blast-radius analysis: streaming.Client wrapper + synctest (2026-06-24)

Jim asked specifically: what is the blast radius of (1) a jetstream-side wrapper
interface for atmos `streaming.Client`, and (2) injectable clocks + `testing/synctest`?
Researched the synctest docs + atmos v0.2.6 source + every jetstream call site.

### Finding 1 — the two changes are COUPLED, not independent

`testing/synctest` (stable in Go 1.25; we're on 1.26) advances its fake clock only
when **every goroutine in the bubble is "durably blocked"** — blocked such that
*only another bubble goroutine* can unblock it (channel ops, `sync.Cond/WaitGroup`,
`time.Sleep`). **Blocking on network I/O is explicitly NOT durably blocking** (an
external OS event can unblock it), so a goroutine sitting in a real websocket read
*prevents the fake clock from advancing* and breaks the bubble (deadlock-detection
panic / hang). The official docs' own example replaces real sockets with
`net.Pipe()`.

Consequence: **synctest is not usable while the live consumer reads a real
websocket.** So the clock work (Option A) is gated on the transport work (Option
B). You cannot get the synctest payoff without first removing the real socket.
This is the single most important finding — they must be scoped as one project,
not two.

### Finding 2 — jetstream-side seam is SMALL; atmos-side is the blocker

jetstream's use of the concrete `*streaming.Client` is tiny and localized to
`internal/ingest/live/consumer.go`:
- `client atomic.Pointer[streaming.Client]` (field, consumer.go:62)
- `streaming.NewClient(opts)` (consumer.go:342)
- `client.Events(ctx)` (consumer.go:364)
- `cl.Cursor()` (consumer.go:229)
- `client.Close()` (consumer.go:358)

So the minimal interface jetstream needs is just:
```go
type EventStream interface {
    Events(ctx context.Context) iter.Seq2[[]streaming.Event, error]
    Cursor() int64
    Close() error
}
```
Extracting that interface and injecting it via `live.Config` is a **small, clean,
low-risk change** (one field, one constructor seam, a handful of call sites). A
production factory returns a real `streaming.NewClient`; the oracle injects a
fake. `streaming.Event/Action/ResyncKind/BackoffPolicy/Jetstream*` are plain data
types used across the codebase and **stay as-is** — the wrapper yields the same
`streaming.Event` values, so `ConvertEvent`/`processBatch` (already
transport-agnostic, taking `[]streaming.Event`) don't change.

**BUT** the hard part is what the fake must *produce*, and that's blocked by atmos:

1. **`streaming.Event` has unexported fields** the resync path depends on:
   `verifiedOps`, `verifierRan`, `syncClient`, `ctx`, `strictValidation`
   (event.go:120-148). `Event.Operations()` (operation.go:86) branches on
   `verifierRan`/`verifiedOps` first, then `syncClient`. An out-of-package
   producer **cannot set these**, so a hand-built `Event` literal can carry a
   `Commit` (CAR-decoded ops work) but **cannot reproduce the verifier-driven
   async-resync** — which is *exactly* the code path in the `superseded row
   survived` failures (`eventFromAsyncResync` at client.go:1192 builds an Event
   with `verifiedOps`+`verifierRan` set, internally). Feeding events at the
   decoded-`Event` level would silently lose the most valuable behavior.

2. **atmos exposes no transport/dialer/conn injection.** `dial()` is an
   unexported free function (`dial_other.go`) that hard-calls
   `websocket.Dial`; `NewClient` has no `Dialer`/`Conn`/`Transport`/`HTTPClient`
   option. The verifier + per-DID parallel worker pool + async-resync delivery
   all run *inside* `Client.readLoop`/`consumeLoop`. So we cannot feed atmos a
   fake connection either.

### Finding 3 — the realistic options, re-scoped

Given Findings 1-2, "wrapper + synctest" lands in one of three shapes:

- **B1 (small, but lower fidelity): wrapper at the `EventStream` interface, fake
  produces decoded `streaming.Event`s.** Blast radius on jetstream: ~1 interface +
  inject through `live.Config` (and `orchestrator`/`jetstreamd` config plumbing) +
  ~5 call sites in consumer.go. The fake reads from the simulator fanout in-process
  (no socket) → synctest becomes viable. **Limitation:** can't reproduce the
  verifier/async-resync path (unexported fields), so it would NOT cover the actual
  failures we're chasing without an atmos change. Good for the deterministic
  *logical* tier of commit/identity/account flows; blind to the resync seam.

- **B2 (full fidelity): atmos change required.** atmos must either (a) export a
  way to construct resync/verified Events (a public constructor or exported
  fields), or (b) expose a dialer/conn injection seam so we feed it a fake
  in-memory transport carrying real wire frames and let atmos's *real* verifier
  run. (b) is the higher-fidelity option (keeps atmos's verifier/resync/parallel
  logic exactly as in prod) and is the only path that makes the *real failures*
  reproducible under synctest. Blast radius now spans **two repos** (atmos +
  jetstream). atmos is also jcalabro's, so feasible — but it's a real API addition
  + the in-memory transport carrying frame bytes (the simulator already produces
  wire frames via `internal/simulator/world/firehose.go` and serves them over the
  fanout, so the bytes exist).

- **B3 (jetstream-only, sidestep atmos): move the seam ABOVE atmos.** Instead of
  wrapping `streaming.Client`, introduce the `EventStream` interface and have the
  *real* impl wrap atmos exactly as today, while the *fake* impl is fed
  pre-decoded `[]streaming.Event` **including synthetic resync events that
  jetstream itself constructs**. Problem: jetstream can't construct the
  resync-bearing Event (unexported fields) — same wall as B1. So B3 collapses into
  B1 unless atmos exposes the constructor. Conclusion: **some atmos surface is
  required for full fidelity; there is no jetstream-only path to reproducing the
  resync failures under synctest.**

### Finding 4 — clock blast radius (the Option A half)

Even setting transport aside, synctest needs ALL runtime time inside the bubble to
be fake. Inventory (from earlier research, this session):
- Already injectable (~9 sites): live consumer `now`, backfill handler/retry `now`,
  merge runner, slow detector, identity cache, status collector, backfill store
  (package var). Live reconnect backoff already injected to ~1ms no-jitter.
- NOT yet injectable (blockers, must be routed through one clock): steady compactor
  `time.NewTimer`/`time.Since(lastPass)` + hard-coded `minCompactionTriggerSpacing
  = 30s`; backfill retry/selected `time.NewTimer`; subscribe ping ticker; overlay
  cache rebuild ticker; manifest `ForwardCursor` lookback; orchestrator phase-write
  timestamps; **unseeded `rand/v2` jitter** in `backfill/selected.go`.
- **atmos internal time** (its backoff/retry/verifier-worker goroutines) is ALSO
  outside our control and outside the bubble's fake clock — another reason B2's
  in-memory-transport-into-real-atmos still has a synctest hole unless atmos's time
  is bubble-compatible (atmos uses stdlib time/timers, which synctest *does* fake
  IF those goroutines run inside the bubble — they would, if atmos is called from
  within the bubble and uses no real I/O).

This means the clock change is a **moderate, mechanical, repo-wide sweep**: define
one `Clock` interface, thread it through orchestrator/live/backfill/compactor/
overlay/subscribe configs, replace ~10 hard `time.*` sites. Each is individually
trivial; the cost is breadth and the discipline to not miss one (a single real
`time.Sleep` left in the bubble silently defeats the fake clock).

### Bottom line on blast radius

- **jetstream `EventStream` wrapper interface: SMALL** (1 interface, config
  plumbing through 3 layers, ~5 consumer.go call sites). Low risk.
- **Clock injection for synctest: MODERATE** (repo-wide mechanical sweep, ~10
  not-yet-injectable `time.*` sites + one Clock interface + config plumbing).
- **The catch: full fidelity (reproducing the actual resync failures) needs an
  atmos API addition** (exported resync-Event constructor OR a dialer/conn
  injection seam). Without it, wrapper+synctest gives a deterministic logical tier
  for the commit/account/identity flows but is **blind to the resync seam where
  the real bugs live.** atmos being jcalabro's own repo makes B2 viable, but it is
  explicitly a two-repo change.
- **Coupling:** synctest is unusable over a real socket, so the clock payoff is
  gated on the transport change. Scope them as one project.

### Suggested decomposition (smallest shippable increments)

1. Seed/inject the `rand/v2` jitter in `backfill/selected.go` — tiny, standalone,
   removes one nondeterminism source regardless of the rest.
2. Extract `EventStream` interface + inject via config (real impl wraps atmos
   unchanged). Ships value immediately: a test seam for the consumer, no behavior
   change. Prerequisite for everything else.
3. Decide atmos surface for B2 (resync-Event constructor and/or conn seam). This is
   the gating decision for whether we can reproduce the *real* failures. Needs an
   atmos design note.
4. Clock interface sweep (Option A), landed incrementally subsystem-by-subsystem.
5. In-process transport fake + synctest harness tier, once 2-4 exist.

### Implementation progress (2026-06-24)

- **Step 3 DONE (atmos side, Option α):** On local atmos checkout (branch
  `client-interface`), added exported `Conn` interface (`Read`/`Close`/`CloseNow`/
  `SetReadLimit`) + `DialFunc` type + optional `Options.Dial gt.Option[DialFunc]`.
  `Client.conn` is now `atomic.Pointer[Conn]`; `dial()` uses the injected dialer
  when set, else the real `websocket.Dial`. Both `dial_other.go`/`dial_js.go`
  return `Conn`. Default path byte-identical when `Dial` unset; `*websocket.Conn`
  satisfies `Conn` so existing callers compile unchanged. New tests
  (`dial_inject_test.go`): drive the client over an in-memory `memConn` with no
  socket and assert events decode through the real pipeline + cursor-in-URL. Full
  atmos streaming suite passes under `-race`.
- **jetstream wiring DONE:** `replace github.com/jcalabro/atmos => ../../jcalabro/atmos`
  in go.mod (temporary, while iterating; drop on tagged atmos release). jetstream
  builds; `internal/oracle` + `internal/ingest/live` compile and live unit tests
  pass against local atmos.
- **Step 2 DONE (revised approach):** Dropped the planned jetstream-side
  `EventStream` interface — redundant now that the atmos `Dial` seam exists, and
  lower fidelity (a jetstream fake would bypass atmos's verifier/resync/decode, the
  code path under test). Instead threaded the atmos `Dial` hook through the
  existing config layers, mirroring `LiveReconnectBackoff` exactly:
  `jetstreamd.Options.LiveDial` → `orchestrator.Config.LiveDial` (set on
  `live.Config.Dial` in steady.go + bootstrap.go) → `streaming.Options.Dial` in
  `consumer.Run`. One field per layer, nil = real socket, no behavior change. The
  real `streaming.Client` (and its verifier/resync) keeps running; only the
  transport is swappable. New test `internal/ingest/live/dial_inject_test.go`
  drives the consumer over an in-memory `memConn` (no socket) and asserts events
  archive through the real pipeline + the derived subscribeRepos URL reaches the
  dialer. Live suite passes under `-race`; orchestrator + jetstreamd suites green.
- **Step 1 DONE:** `backfill/selected.go` `selectedBackoffDelay` jitter is now an
  injectable `jitterFunc` threaded through both retry runners (default
  `rand.Int64N`), with unit tests for seeded determinism + bounds. Committed.
- **Step 4 RE-SCOPED (2026-06-24):** Two findings collapsed the planned manual
  clock sweep into something much smaller:
  1. **synctest auto-fakes stdlib time.** Per the Go docs, every goroutine inside
     a `synctest.Test` bubble gets a fake clock for `time.Now/Sleep/Timer/Ticker/
     After` and `context` deadlines — NO code change needed, transitively for
     atmos/pebble goroutines too. The existing `now func()` fields default to
     `time.Now`, so they return fake time inside the bubble for free. A manual
     `Clock` interface sweep is therefore REDUNDANT inside the bubble. Decided
     (with Jim) to SKIP it. (A manual sweep would still help the *legacy
     real-socket* harness's slow-runner flakes, but that's not where we're headed.)
  2. **SPIKE: pebble works inside a synctest bubble.** Opened a real pebble DB in a
     bubble, did Set/Get, and a `time.Sleep(1s)` completed instantly under the fake
     clock — confirming pebble's background goroutines don't prevent the bubble
     from reaching "all durably blocked." This de-risks running the real runtime
     (which is pebble-backed) under synctest.
  So step 4 becomes: **the real prerequisite for step 5 is removing the remaining
  real sockets, not faking time.** The live firehose socket is already handled by
  `LiveDial` (step 2). Backfill `getRepo`/`listRepos` and the verifier's
  `getRepo`/PLC calls still hit the simulator's `httptest` server over a real
  socket. Inject an in-process `http.RoundTripper` that serves the simulator
  handler directly (via the existing `jttp.WithTransport` escape hatch + a new
  optional `jetstreamd.Options` transport field) — no atmos change, no jttp change.
- **Step 4 DONE:** `jetstreamd.Options.HTTPTransport` (an `http.RoundTripper`)
  routes every outbound jttp client (backfill getRepo/listRepos, identity/PLC)
  through an injected transport via `jttp.WithTransport`. Nil = real sockets. No
  atmos/jttp change. Committed.
- **Step 5 SCOPING (2026-06-24): the "full bubble" cost center is the public
  serving path, not ingest.** Two real-socket dependencies block running the FULL
  `TestOracle_DefaultLifecycle` in a bubble:
  1. The runtime binds REAL public+debug TCP listeners (`internal/server`
     `Run` → `net.ListenConfig.Listen` + `http.Server.Serve`); `Accept` goroutines
     are not durably blocked.
  2. The oracle client-observer tier drives the runtime's public API over real
     HTTP/websocket (`waitForRuntimePublicURL` → `/subscribe`, `/xrpc`).
  KEY FINDING: `websocket.Accept` requires `http.Hijacker` (coder/websocket
  accept.go:130,159). `httptest.ResponseRecorder` does NOT implement Hijacker, so
  the client-observer `/subscribe` path cannot be served by a simple
  handler-RoundTripper — it needs a real `http.Server` over an in-memory
  `net.Pipe`-backed listener (whose conns support hijack). The unary `/xrpc` +
  simulator getRepo/listRepos/PLC paths CAN use a `ResponseRecorder`
  handler-RoundTripper. The firehose path needs neither (handled by `LiveDial`
  feeding the fanout directly, bypassing the simulator's `websocket.Accept`).
  So "full bubble" = build an in-memory-listener seam for the runtime's public
  server + route the client-observer through it. A focused ingest/compaction tier
  (no public server) needs none of that and still reproduces the
  superseded-row-survived / resync class. Effort estimate for "full bubble" being
  produced by a parallel investigation workflow (see decision log).

## Concrete atmos change options (2026-06-24)

Jim: "What are the specific atmos changes? If small in scope and don't make the
library harder to use, I'll consider them." Read atmos v0.2.6 `NewClient`, the
`Client` struct, `dial`, and the full connection surface to answer precisely.

Encouraging precedent: **atmos already injects time for testing.** `Client` has
`lockSleep func(ctx, d) error` with the comment "overridable for testing"
(client.go:175), defaulted to the package `sleep` (backoff.go:43) and overridden
in `leader_test.go`. So adding a narrow injectable seam is idiomatic to this
library, not a foreign concept.

The entire `*websocket.Conn` surface atmos touches is **4 methods** (verified by
grepping every `conn.` use in client.go):
- `conn.Read(ctx) (msgType, []byte, error)` (client.go:784, the reader goroutine)
- `conn.Close(status, reason) error` (client.go:379)
- `conn.CloseNow() error` (client.go:482)
- `conn.SetReadLimit(n)` (client.go:603, in `dial`)

That's a tiny interface. Three candidate atmos changes, smallest/cleanest first:

### Option α (RECOMMENDED): inject the dialer — a `Conn` interface + `Dial` option

Add to atmos `streaming`:
```go
// Conn is the minimal websocket surface the client consumes. *websocket.Conn
// from coder/websocket already satisfies it.
type Conn interface {
    Read(ctx context.Context) (websocket.MessageType, []byte, error)
    Close(code websocket.StatusCode, reason string) error
    CloseNow() error
    SetReadLimit(n int64)
}

// Options.Dial, when set, replaces the default websocket.Dial. Receives the
// fully-resolved URL (cursor/query already appended). Default is the real
// websocket dial.
Dial gt.Option[func(ctx context.Context, url string) (Conn, *http.Response, error)]
```
Changes inside atmos: `Client.conn` becomes `atomic.Pointer[Conn]` (or stores the
interface); `dial()` calls `c.opts.Dial.Val()` when set else the real
`websocket.Dial`; `readLoop`/`consumeLoop`/`Close` already only use the 4 methods
above, so they're untouched beyond the type. **Scope: ~30-50 lines in atmos, one
new exported type + one new Option field. Zero behavior change when unset.**

Why this is the best option:
- **Highest fidelity:** atmos's REAL verifier, async-resync worker, per-DID
  parallel scheduler, cursor watermarking, gap detection, and reconnect logic all
  run unchanged. The fake only supplies *wire frame bytes*. The
  `superseded-row-survived` resync path is exercised exactly as in production —
  this is the only option that reproduces the actual bugs.
- **Doesn't touch the Event type** — no unexported-field problem, because we feed
  bytes and atmos decodes/verifies them itself.
- **jetstream already produces the bytes:** `internal/simulator/world/firehose.go`
  encodes real subscribeRepos wire frames; the simulator fanout already serves
  them. The fake `Conn.Read` pulls from an in-process channel fed by the fanout —
  no socket, so synctest's bubble holds.
- **Doesn't make the library harder to use:** new field is optional
  (`gt.Option`), default path identical. The `Conn` interface is satisfied by
  `*websocket.Conn` out of the box, so existing callers compile unchanged.
- **Mirrors the existing `lockSleep` testing-seam philosophy.**

Caveat to verify: atmos's verifier itself makes PLC/CAR **HTTP getRepo** calls
(`sync.NewClient`/`xrpc.Client`) for resync. Those are separate from the
websocket and already injectable via `SyncClient`/`Verifier` options + the
`HTTPClient` on the xrpc client (the oracle already points these at the
simulator's httptest server). For a fully-in-bubble synctest run those HTTP calls
would also need to be in-process (httptest is a real socket) — but that is a
jetstream/oracle wiring choice using atmos's EXISTING injection points, not a new
atmos change. Flag: confirm the verifier path can be fed in-process via existing
`SyncClient` injection.

### Option β: export a resync-Event constructor

Add `func NewResyncEvent(did, rev string, ops []Operation) Event` (and maybe
`NewCommitEvent`) plus an exported `Operation` constructor, so jetstream can build
verified/resync Events directly and feed them to an `EventStream` fake at the
decoded level.
- Scope: small (a constructor + making `Operation` buildable).
- **Lower fidelity:** bypasses atmos's verifier/scheduler/gap/cursor logic
  entirely — the fake would re-implement ordering/verification, which is where the
  bugs are. Risks the fake and prod diverging exactly at the seam under test.
- Mild API-surface growth, but arguably a reasonable public API regardless.
- Verdict: useful for *unit-style* tests of jetstream's `convertSync`/compaction
  in isolation, but NOT a faithful end-to-end reproducer. Weaker than α.

### Option γ: in-memory `DistributedLocker`/transport via existing seams only

Investigated whether existing options suffice with zero atmos change. They don't:
`SyncClient`/`Verifier`/`Backoff`/`Locker` are injectable, but the **websocket
transport is not** — `dial()` is an unexported free function with no Option hook.
So some atmos change is unavoidable for in-process delivery. (Confirmed: no
`Dialer`/`Conn`/`Transport`/`HTTPClient` field on `Options` today.)

### Recommendation

**Option α (inject the dialer behind a tiny `Conn` interface).** It is small
(~30-50 lines, one exported interface + one optional Option), idiomatic to atmos
(mirrors `lockSleep`), invisible to existing callers (optional field, default
unchanged, `*websocket.Conn` already satisfies `Conn`), and the *only* option that
reproduces the real resync failures because atmos's own verifier/scheduler runs
over the injected transport. β is a fine complementary addition for unit tests but
not a substitute.

## Decision log

- 2026-06-24: Document started. `rr` (Option D) rejected. Bit-exact determinism
  declared a non-goal. Reproducibility *and* richer artifacts both in-scope.
  No build work started yet — researching/brainstorming.
- 2026-06-24: Blast-radius analysis added. KEY FINDING: synctest is unusable over a
  real websocket (network I/O is not "durably blocked"), so the clock change and
  the transport change are COUPLED. The jetstream `EventStream` wrapper is small,
  but FULL fidelity (reproducing the verifier/async-resync failures we actually
  see) requires an atmos API addition because `streaming.Event`'s resync path uses
  unexported fields and atmos exposes no transport injection seam. Wrapper+synctest
  alone (jetstream-only) yields a deterministic logical tier blind to the resync
  seam. atmos is jcalabro's repo, so a coordinated two-repo change is feasible —
  flagged as the gating decision.
- 2026-06-24: Concrete atmos options drafted. RECOMMEND Option α: a tiny `Conn`
  interface (4 methods, satisfied by `*websocket.Conn` already) + an optional
  `Options.Dial` hook (~30-50 lines in atmos, zero behavior change when unset,
  mirrors atmos's existing `lockSleep` test seam). It's the only option that runs
  atmos's real verifier/scheduler over an injected in-process transport, so it
  reproduces the resync failures faithfully and avoids the unexported-Event-field
  problem (we feed wire-frame bytes, atmos decodes them). Option β (exported
  resync-Event constructor) is a useful unit-test helper but lower fidelity.
  Open item: confirm atmos's verifier getRepo/PLC HTTP calls can be driven
  in-process via the EXISTING SyncClient/Verifier injection (no new atmos change)
  for a fully-in-bubble synctest run.
- 2026-06-24: Option α implemented in atmos (committed) and the `Dial` hook
  threaded through jetstream config. DECIDED to ABANDON the jetstream-side
  `EventStream` interface (original plan step 2). Rationale: with the atmos `Dial`
  seam in place, a jetstream `EventStream` wrapper is redundant churn AND lower
  fidelity — a jetstream-level fake would bypass atmos's verifier/resync/decode
  pipeline, which is exactly the code path the failures exercise. Threading `Dial`
  (one config field per layer, mirroring `LiveReconnectBackoff`) keeps the real
  atmos client running and swaps only the transport. Less code, higher fidelity.

## Appendix: evidence from the 2026-06-24 triage

The session that prompted this doc triaged the 4 most recent failing scheduled
runs. Two failure classes:

- **Correctness (both lanes):** `superseded {account,record} row survived` at
  `harness_test.go:620` (`assertCompacted`, phase `steady-state-shutdown-flush`,
  post-shutdown quiescent disk scan). Examples: seq=421/tombstone=30235 (race
  lane), seq=0/tombstone=23119 and seq=785/tombstone=41773 (non-race lane). Each
  was preceded by a `verification failure: chain break` WARN and involved the
  test's deliberate async-resync + late-DID-tombstone of the same DID. Root cause
  localized (not yet proven) to the resync-vs-compaction-eviction seam
  (`dropStaleOrderedAsyncResync` + `Evict`-by-`chunkEnd`).
- **Liveness (race lane only):** `after-bootstrap barrier not reached` +
  `runtime did not exit after cancellation`. Suspected `-race` deadline noise,
  not a real defect.

Local repro attempt: 58 iterations of the cheapest non-race failing seed
(`14025298673910416591`) at `GOMAXPROCS=2` on a 32-core idle box — **0
reproductions** (the deterministic chain-break WARN fired every run, but the
correctness assertion never tripped). This is the concrete evidence for failure
factors 1 and 2.

## Full-bubble effort estimate (2026-06-24, workflow + adversarial verify)

Question: how much work to run the FULL `TestOracle_DefaultLifecycle` inside a
synctest bubble ("the right way")? Five parallel investigators mapped each
real-I/O subsystem; an Opus synthesis produced a plan; an adversarial verifier
ran its own synctest spikes under `-race` (Go 1.26) to refute it.

**Verdict: feasible, NO showstoppers, but XL (~9-15 engineer-days).** Verifier
confirmed by spike (not just code reading): a full websocket handshake + frames +
close + graceful `http.Server.Shutdown` over a `net.Pipe`-backed listener runs in
a bubble; the fake clock advanced 3m30s in 0.00s with an idle keep-alive conn
open. The load-bearing primitive works.

Work items:
1. (S) Inject `net.Listener` into `server.Config` (public+debug); branch
   `server.Run` (internal/server/server.go:166/171); thread through
   `jetstreamd.Options` + runtime.go:391.
2. (M) `PipeListener` — `net.Pipe`-backed listener; `Accept` = channel receive
   (durably blockable); conns are hijackable so `websocket.Accept` works for both
   the simulator's `/subscribeRepos` and the runtime's `/subscribe`. The keystone.
3. (M) Serve the simulator over the PipeListener instead of `httptest.NewServer`
   (preserves CAR streaming + getRepo-truncation fault fidelity).
4. (M) PUBLIC API change to the jetstream client module: add `WithDial` so the
   client's live `/subscribe-v2` cutover (`internal/client/live.go:288`) can dial
   in-memory. `WithHTTPClient` only reaches unary XRPC today. (Verifier's key
   catch; `assertTypedLikeBackfill` uses `WithBackfillOnly` so it's exempt, but
   `collectClientBackfill` needs it.)
5. (M) Thread one in-process client through the 3 observer Subscribe sites +
   `fetchOverlay` (`overlay_integration_test.go:29`).
6. (S) OTEL guard — effectively free; already noop without `OTEL_EXPORTER_OTLP_*`.
7. (L) Wrap lifecycle in `synctest.Test`; move observer assertions into bubble
   goroutines; re-derive barriers under the fake clock. DOMINANT RISK: whole-graph
   quiescence — runtime + simulator + atmos + pebble + observer parallel-decode
   workers must ALL be durably blocked simultaneously; one real-I/O or spinning
   goroutine = hard-to-localize hang. Also heavy synchronous bootstrap + 2 pebble
   DBs move inside the bubble; `t.Cleanup(client.Close)` ordering needs care.
8. (M) Verify pebble + atmos workers never touch real I/O across the FULL
   lifecycle under `-race`.

**DECISION (recommended by both synthesis and verifier): do NOT full-bubble now.**
The triaged CI failures (#100/#106 superseded-row / over-drop) live in the STORAGE
tier (firehose -> live consumer -> pebble -> compaction), observed by the direct
segment observers (`ObserveSegments`, `compactionOverDropRecorder`,
`assertCompacted`). That graph is bubble-ready TODAY via `LiveDial` +
`HTTPTransport` + the proven `memConn` pattern — NO public server, NO
PipeListener, NO public-API change. ~M / 2-3 days, reproduces the exact defects.
The full bubble buys SERVING/CLIENT-tier coverage that is real but NOT where the
observed bugs are (`client_observer_test.go:286-297` documents that served-replay
path as the historical leading cause of CheckCompacted flakiness #94 triages).
Defer the full bubble (items 1,2,4,5,7) as a separate effort.

## Step 5 DONE: lightweight synctest tier shipped (2026-06-24)

`internal/oracle/synctest_test.go` (`TestOracle_Synctest`) + `inprocess.go`. Runs
the real jetstreamd ingest path (bootstrap → merge → steady → compaction) inside
a `synctest.Test` bubble with NO sockets and the fake clock. Firehose via
`LiveDial`→`firehoseConn` (reads the simulator fanout in-memory, mirroring
relay_subscribe.go's subscribe-before-replay); unary HTTP via
`handlerTransport` (an `http.Handler`-backed `RoundTripper` over
`httptest.NewRecorder`); runtime headless (`Options.Headless` skips the public
server). Asserts `CheckInvariants` + `CheckCompacted` + `Compare` on the durable
on-disk segments. **~20ms per run; passes under `-race`; 10/10 stable across
separate process invocations.** Existing real-socket `TestOracle_DefaultLifecycle`
untouched and still passes.

Synctest gotchas discovered (institutional knowledge for the full-bubble effort):
1. **Clock epoch mismatch.** synctest's fake clock starts 2000-01-01; the
   simulator stamps commit revs at its logical-clock epoch (~2023, see
   `logical_clock.go` `logicalClockBaseMicros`). atmos's verifier rejects revs
   >5m in the future → every event fails verification (`seen=0`). Fix: sleep the
   bubble clock forward to just past the simulator epoch before starting the
   runtime (`advanceClockToSimulatorEpoch`).
2. **No spin loops.** A `for { synctest.Wait(); select{...; default:} }` barrier
   keeps a goroutine runnable, so the bubble never reaches "all durably blocked"
   and the fake clock never advances → hang. Just block on the channel
   (`<-gate.entered`); that's durably-blocking and lets the clock advance.
3. **All bubble goroutines must exit before the bubble fn returns.** A failed
   `require` calls `runtime.Goexit`, leaving the runtime goroutine alive →
   synctest panics "blocked goroutines remain", MASKING the real assertion error.
   Fix: `defer` a shutdown that `cancel()`s, drains `rt.Run`, AND calls
   `rt.Close()` (the verifier worker pool exits only on Close, not on Run return).
4. **One bubble per process.** The production zstd encoders (overlay/segment/
   subscribe) are package globals whose worker goroutines+channels bind to the
   first bubble that uses them; a second same-process bubble (`-count>1`) hits
   "receive on synctest channel from outside bubble" (fatal). `WithEncoderConcurrency(1)`
   does NOT fix it (the pool still initializes lazily in-bubble). Guard: skip a
   second same-process bubble with a clear message; re-runs are separate `go test`
   invocations. The full-bubble effort will need to grapple with this if it wants
   `-count` soak in one process (e.g. reset/inject the encoders, or accept
   process-per-run).

HONEST LIMITATION (what this tier does and does NOT yet buy): synctest removes
wall-clock skew and gives a fast, socket-free, fake-clock harness — but it does
NOT serialize goroutine scheduling, so the interleaving is still nondeterministic.
The triaged failures (superseded-row-survived) are interleaving-dependent
(resync-vs-compaction ordering), so this tier does not yet SPONTANEOUSLY reproduce
them on a fixed seed. Its value is: (a) a reliable, fast, no-socket reproduction
harness with the seams to (b) FORCE the bad interleaving deterministically — a
small follow-on that drives the resync + compaction pass in a controlled order
via `synctest.Wait()` between steps. That forced-interleaving test is the actual
red→green reproducer; this tier is its foundation.

## Decision log (continued)
- 2026-06-24: Steps 1 (jitter), 2 (LiveDial threading), 4 (HTTPTransport)
  implemented, tested, committed. Manual clock sweep SKIPPED (synctest auto-fakes
  time; pebble-in-bubble spike passed).
- 2026-06-24: Full-bubble estimated XL / 9-15 days via workflow + adversarial
  verify (spikes confirm primitives, no showstopper). Recommend the focused
  ingest/compaction synctest tier (~M / 2-3 days) for step 5 instead. Full bubble
  deferred. Awaiting Jim's call on which to build.
