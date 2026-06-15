# Design: Real Determinism for the Oracle Harness (DST)

Status: draft for discussion. Author: investigation 2026-06-15.

## Problem

`TestOracle_DefaultLifecycle` is seeded but not deterministic. The seed fixes
inputs (simulator world, runtime RNG, fault schedule) but the harness runs the
real `jetstreamd` runtime concurrently against real time and real sockets
(`harness_test.go:96-139`). Goroutine scheduling, fault-vs-retry timing, and
socket I/O ordering are not controlled, so the same seed produces different
interleavings per run and per machine. Failures reproduce in CI (16 cores,
contended) but not on a 32-core dev box. The printed `Repro:` command overstates
reproducibility (fixed in A).

This is randomized concurrent integration testing, not Deterministic Simulation
Testing (DST). We want DST: same seed -> bit-identical execution, every time,
everywhere. The model to emulate is FoundationDB / TigerBeetle / `madsim`.

## Why this is hard in Go specifically

DST requires controlling every source of nondeterminism. The Go runtime hides
three of them behind APIs we cannot intercept without help:

1. **The goroutine scheduler is not controllable or observable.** `GOMAXPROCS=1`
   makes execution single-threaded but NOT deterministic: the runtime still
   preempts at function-call/loop backedges and async-preemption points, and the
   order in which blocked goroutines become runnable (channel ops, mutex
   handoff, netpoll readiness) is not seeded. There is no public hook to
   serialize "which runnable goroutine runs next" against a PRNG. Rust's
   `madsim`/`tokio` can do this because the async runtime IS the scheduler and is
   user-space; Go's is in the runtime.

2. **Time is read directly.** 59 `time.Now`/timer call sites in prod code, ~17
   files. Real timers fire on wall-clock via `runtime.timeSleepUntil`. A virtual
   clock requires every one to route through an injected clock, and timers must
   be driven by the simulation, not the OS.

3. **Network and disk are real.** 28 files touch `net/http`/`websocket`/`httptest`;
   the segment store hits a real filesystem and pebble. Real sockets introduce
   kernel scheduling and real EOF/flush timing — this is the exact source of the
   truncated-CAR nondeterminism in finding B (clean-short-body vs severed-stream
   depends on flush race).

Go gives us no equivalent of `madsim`'s "replace the whole runtime" seam. So we
cannot get *runtime-level* determinism without either changing Go or accepting a
boundary below which we trust real primitives.

## Option matrix

### Option 0 — Stay as-is, lean on `-count` + `GOMAXPROCS` (done in A)
- Cost: ~0. Surfaces interleaving bugs probabilistically.
- Limit: never bit-reproducible; a failing seed may take thousands of runs to
  recur; can't bisect a specific interleaving.
- Verdict: necessary floor, insufficient alone.

### Option 1 — Inject clock + RNG + transport seams (no scheduler control)
Make every nondeterministic *input* injectable, but keep the real Go scheduler.
- **Clock:** one `Clock` interface threaded everywhere (extend the existing
  `c.now func() time.Time` pattern already in 8 files to all 17). Virtual time
  advanced by the sim. Timers become sim-driven (a `Clock.NewTimer` that the sim
  fires). This is mechanical but invasive: ~59 call sites.
- **RNG:** already mostly seeded; audit the 8 `math/rand` sites + map-iteration
  order (Go randomizes map ranging — must sort or use ordered structures on any
  path that affects output).
- **Transport:** replace `httptest` + real sockets with an in-memory transport
  (an `http.RoundTripper` / a `net.Conn` pair backed by buffers) so getRepo /
  subscribeRepos / firehose run without kernel sockets. This alone kills the
  finding-B flake class (truncation becomes a deterministic byte operation, not a
  flush race).
- **What it buys:** removes time, RNG, socket, and map-order nondeterminism.
- **What it does NOT buy:** goroutine scheduling order is still the real runtime.
  Two goroutines both runnable after a channel send still race. So: *more*
  reproducible, not *fully* reproducible.
- Cost: large (weeks), high churn across ingest/subscribe/orchestrator.
- Verdict: high value, achievable, but stops short of true DST.

### Option 2 — Single-goroutine deterministic executor (true DST)
Rearchitect the runtime under test so all concurrency runs on a cooperative,
single-threaded, seeded scheduler the test owns. The sim picks which task
advances next via the PRNG. This is how FDB/TigerBeetle get bit-reproducibility.
- Requires: no raw `go` statements on the SUT's hot path; everything is a task on
  our executor. Channels/selects become executor primitives. Blocking I/O
  becomes cooperative yields. Effectively an actor/state-machine rewrite of the
  ingest+serving core (the 9 `go func` sites + 34 channel/select sites are the
  surface).
- **Buys:** full determinism, replay, interleaving bisection, "shrink a failing
  schedule" — the real DST payoff.
- **Cost:** very large; a structural commitment. The SUT must be written in a
  DST-friendly style from the core out. Retrofitting is a multi-quarter effort
  and constrains all future code.
- Verdict: the gold standard, but only justified if DST becomes a first-class
  project value, not a test nicety.

### Option 3 — `testing/synctest` (Go 1.25+, stable in 1.26)
Go now ships `testing/synctest`: a bubble where time is fake and the bubble's
goroutines are scheduled deterministically *relative to fake time* — the runtime
advances fake time only when all bubbled goroutines are durably blocked.
- **Buys:** real, in-runtime fake clock + quiescence detection, no manual clock
  injection. Designed exactly for concurrent-code tests. We're on go 1.26 already.
- **Limits:** the bubble must contain all the goroutines; real network/disk I/O
  is NOT bubbled (a goroutine blocked on a real socket is not "durably blocked"
  in the synctest sense, and breaks the model). So synctest requires Option 1's
  in-memory transport + an in-memory/deterministic store to be effective. It also
  doesn't seed the *order* among simultaneously-runnable goroutines beyond what
  the runtime does — it controls time-advance, not full interleaving.
- Verdict: this is the highest-leverage Go-native lever and it did not exist when
  this harness was written. It changes the cost calculus of Option 1
  substantially: clock injection becomes mostly free inside the bubble.

## Recommendation (for discussion)

Phased, each independently valuable:

1. **Now (A, done):** honest repro message.
2. **Near term:** Option 1's **in-memory transport** first — it's the highest
   value-per-effort piece and directly kills the finding-B flake class. Replacing
   `httptest` + real sockets with an in-process transport removes the flush/EOF
   race and the kernel from the loop. Scope it to the getRepo / subscribeRepos /
   firehose seams.
3. **Medium term:** evaluate **`testing/synctest` (Option 3)** for the harness
   now that we're on go 1.26. Combined with the in-memory transport + a
   deterministic store, this gets us most of the way to DST without the Option 2
   rewrite. Prototype on the bootstrap phase first and measure.
4. **Long term / explicit decision:** Option 2 (single-goroutine executor) only
   if we decide DST is a core architectural value worth constraining all future
   ingest/serving code. Do not drift into it; choose it deliberately.

## Open questions

- Does the segment store / pebble admit a deterministic in-memory mode, or do we
  need a memory-backed `vfs`? (pebble has an in-mem FS — investigate.)
- Map-iteration order: audit every path from ingest -> served output for
  range-over-map that affects byte output. Any such site is a latent
  nondeterminism even after clock/transport/RNG are fixed.
- synctest + cgo/netpoll interactions if any real I/O sneaks into the bubble.
- atmos is a separate module: a DST-friendly SUT needs atmos's sync/backfill
  engine to also route time and transport through injected seams. Coordinate the
  contract (this also bears on finding B's fix location).
