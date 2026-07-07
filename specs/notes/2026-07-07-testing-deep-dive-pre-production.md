# Testing deep dive — pre-production opportunities (2026-07-07)

Status: **exploration for Jim's review. No issues filed yet** (per request).

Purpose: before first production deployment, find the highest-value improvements
to the testing methodology across three questions: (1) methodologies we haven't
used, (2) tests worth adding to the current system, (3) infrastructure changes
(CI, justfile, dev loop) that make bug-finding more reliable.

Method: nine parallel audits — six over the repo (test inventory, oracle
internals, mutation scorecard, CI/dev-loop, product-contract surfaces, decision
history) and three researching the 2024–2026 external state of the art
(crash-consistency tooling, Go ecosystem, distributed-systems/database
methodologies). Findings were cross-checked against the decision record in
`specs/notes/` and the key claims verified directly in source. Everything below
either conflicts with no settled decision or explicitly cites the decision it
touches.

Ranked: P0 is "do before production," descending from there. Each item notes
cost, evidence, and — following the house rule — how to prove the new coverage
has teeth (a mutant or red-first check).

---

## P0.1 — Close the durability blind spot: un-fsynced data survives every crash test

**The single most important structural finding of this audit.** The crash tier
kills children with SIGKILL (`restart_harness_test.go:978`), which terminates
the process but leaves the OS page cache intact — so every written-but-un-fsynced
byte, in both segment files and pebble's WAL, *survives* to the restarted child.
The tier therefore always tests recovery under the most benign durability
outcome. Consequences, all verified:

- **No test or mutant can detect a deleted fsync.** No mutant touches
  `syncFile` or `store.SyncWrites` (grep over `testing/mutation/mutants/` is
  empty), and none could be killed if it existed.
- **No test can detect an inversion of the core invariant** — "fsync the
  segment before you commit to pebble" (`specs/invariants.md` rule 2,
  `internal/ingest/writer.go:599` `flushBlockLocked`). It is enforced by
  construction and code review only.
- The torn-tail sweep (`restart_segmentfault_test.go:228`) mutates the active
  segment's tail *bytes* but never models **loss** of un-fsynced data, never
  touches pebble's WAL tail, and never models cross-file ordering (pebble
  cursor durable while the covering segment block is lost — exactly the state
  an inverted ordering produces on power loss).
- A regression flipping `store.SyncWrites` to `pebble.NoSync` on Linux would
  pass every tier. It already *is* NoSync on darwin, and `segment/sync_darwin.go`
  no-ops fsync entirely — documented, accepted for tests, but proof that the
  suite genuinely cannot see fsync behavior.
- The rename-then-parent-dir-fsync discipline in `segment/rewrite.go` /
  `segment/patch.go` has enumerated crashpoints, but SIGKILL at them proves
  nothing (the dirent survives the page cache). A mutant deleting a dir-fsync
  would survive today.

The decision record was checked: nothing settles or declines this territory —
it is open, not a trade-off someone made (the segment I/O fault layer #200
injects fsync *errors*, a different class from fsync *omission/reordering*).

What the flagship systems do: none of SQLite, RocksDB, Pebble, FoundationDB, or
TigerBeetle rely on kernel tooling as the primary defense. All run a **simulated
storage layer under the real code** that tracks written-vs-synced state and
discards un-synced state on simulated power cut (SQLite TH3 snapshot VFS,
RocksDB db_stress "lost buffered writes" mode, Pebble `vfs.NewStrictMem`, FDB
`AsyncFileNonDurable`, TigerBeetle's VOPR simulated disk).

**Recommended shape (three pieces, one direction):**

1. **In-process crash-simulating FS seam** (~1–2 weeks, the big piece).
   Abstract segment's file ops (`os.OpenFile`/`Rename`/`Sync` in `writer.go`,
   `seal.go`, `rewrite.go`, `patch.go`) behind a small FS interface — prod =
   `os`, test = a strict-mem FS that models file bytes *and directory entries*
   as separately-synced state. The pebble side is nearly free: our pebble
   v1.1.5 already ships `vfs.NewStrictMem()` (verified), and
   `internal/store/store.go` currently builds `pebble.Options{}` with the
   default FS — one field. New oracle tier: run the runtime in-process against
   the fake FS, trigger simulated power-cuts at existing crashpoints (and
   random points), discard all un-synced state, then reuse the existing
   recovery/final-state/event-log checkers unchanged. Deterministic,
   CI-native, works on darwin.
2. **LazyFS nightly lane** (~3–7 days, independent, zero production-code
   changes). LazyFS (INESC TEC, VLDB'24; v0.3.1 May 2026; it's the filesystem
   Jepsen commissioned — found 8 new durability bugs in PostgreSQL/etcd/Redis/
   LevelDB) is a FUSE filesystem with a private page cache that persists only
   on fsync and drops un-synced data on command. Mount the restart child's
   data dir on it, SIGKILL at existing crashpoints, issue `clear-cache`, run
   the existing convergence checks. This validates the *real* syscall layer
   (wrong fd synced, pebble config regressions) that a Go-level fake can't.
   FUSE works on ubuntu-24.04 GitHub runners; budget for flake management
   (etcd abandoned their LazyFS integration — treat as scheduled-lane only,
   never per-push).
3. **Always-on ordering assertion** (~1–2 days once the seam exists; SQLite
   journal-test-VFS pattern). Record the durable-path op stream during every
   oracle run and assert "segment fsync happens-before the covering pebble
   cursor commit" and "rename is followed by parent-dir fsync before the next
   durable claim" as properties — catches ordering inversions deterministically
   with **no crash injection at all**.

**Prove it has teeth:** add mutants in the same PR — delete the block-flush
fsync; delete the dir-fsync in `rewrite.go` and `patch.go`; invert
`flushBlockLocked`'s flush/commit order; flip `store.SyncWrites` to NoSync on
Linux. All four SURVIVE today (unkillable); all must be KILLED by the new tier
before banking. This is the m006-storefault precedent applied to the invariant
the whole design leans on.

## P0.2 — Fix #262 before production (already known; listed for completeness)

Open, root-caused data-loss bug: a crash during an account delete→reactivate
window lets merge-tail compaction fold a stale account tombstone that erases
re-backfilled records. Its detecting oracle case is `t.Skip`'d
(`restart_segmentfault_test.go`), which also leaves m044's oracle-layer kill
pending. Un-skipping it is the designated red-first proof. Nothing new here —
just: this is a production-blocking archive-erasure path and should gate deploy.

---

## P1.1 — Close the CI feedback loop: scheduled failures are currently silent

The entire correctness strategy lives in scheduled lanes (oracle sweeps 4×/day,
mutation gate daily, fuzz 4×/day), and their only failure signal is the Actions
tab plus opt-in email (`oracle-scheduled.yml:4-6` says so verbatim). This is
not hypothetical: on 2026-07-06 alone, the fuzz lane failed twice (runner
shutdown) and oracle-scheduled failed twice — all discovered manually.

**Fix (~half a day):** an `if: failure()` step in each scheduled workflow that
files or updates a pinned GitHub issue (fits the issues-as-worklog discipline;
needs only `issues: write` on that job). Optionally a companion "scheduled-lane
health" badge/summary. For a repo whose bug-detection power is measured by
whether these lanes go red, the red signal must reach a human reliably.

**Adjacent, same lane:** the fuzz job is arithmetically over budget — 22 fuzz
targets × 300s = 110 min of pure fuzz floor against a 120-min job timeout (the
last green run used 116m43s). Any new target makes it permanently red. Shard
per-package or drop per-target time; and since CI caching is (deliberately)
banned, decide on corpus persistence — either commit grown corpus periodically
or accept cold restarts, but record the decision. Currently every 6-hour
session restarts from 9 committed seed files, so fuzzing depth is bounded
forever.

## P1.2 — On-disk format compat + upgrade lane, while it's still cheap (pre-1.0)

Every test in the rig runs **one binary version against data it just wrote**.
Nothing tests "new binary opens yesterday's data dir." For an archive service
whose entire value proposition is old bytes staying readable, this is
core-contract coverage, and the only cheap moment to adopt it is now — before
heterogeneous production archives exist. This is standard practice for storage
engines (RocksDB continuous format-compat tests, SQLite's compat guarantees,
Pebble's cross-version metamorphic "tree of histories").

**Shape (~3–5 days initial, near-zero marginal per revision):**

1. Grow `segment/testdata` into a **per-format-revision golden corpus** (a
   sealed segment + a mid-write tail + a pebble dir, generated from a pinned
   seed), frozen forever once a revision ships anywhere. CI asserts HEAD reads
   every corpus entry correctly or rejects it cleanly.
2. An **upgrade-lane oracle variant**: populate a data dir using the binary
   built from the last tagged release, SIGKILL, restart with HEAD, run the
   existing recovery checkers. Reuses the entire restart-tier machinery; it is
   the one scenario no tier performs.
3. **Pin pebble's format-major-version explicitly** and test it — a dependency
   bump can silently ratchet pebble's own on-disk format.
4. A small pairwise sweep over the `JETSTREAM_*` env vars that affect
   persistence, so a config value can't produce files a default-config reader
   mishandles.

Complements (does not duplicate) the planned migration-policy doc #217.

## P1.3 — Serve-path depth: the read side is the least-measured surface, and it's where the real bug was

Evidence stack, from three independent audits:

- The one **real production serving bug** found so far (2026-07-05 diary:
  retry appends punched a seq hole in the /subscribe hot ring, serving *wrong
  events* to replaying clients) was on this surface, and it was masked for
  weeks by a vacuous scenario.
- Mutant distribution: **~31 of 37 mutants sit on the ingest/storage write
  path; the serve/egress side has ~3.** `internal/xrpcapi` has zero committed
  mutants (two were verified killable in #105 and rejected purely on runtime
  cost); `internal/subscribe` has one mutant across 14 files (filter.go,
  encoder.go, replay.go, tail.go, compress.go all bare); `internal/client` has
  one plus a *known surviving* Suppressor.ShouldDrop mutant deferred since #105.
- No test runs **more than one real websocket client at a time** at the
  handler layer (the strong concurrency tests — `burst_test.go`,
  `replay_floor_test.go`'s 16-walker rotation race — drive Tail/WalkFromCursor
  directly, bypassing handler.go's filter-swap atomics, per-conn goroutine
  pair, write timeout, slow-detector wiring).
- No test exercises a subscriber **racing a compaction segment swap**
  end-to-end (compaction → `OnSegmentCompacted` → `ColdReader.InvalidateSegment`,
  `internal/jetstreamd/runtime.go:309-315` — covered only by a blockcache unit
  test).
- **Stalled-reader backpressure is never exercised for real**: the 5s
  frameWriteTimeout (`handler.go:31`) and slow-detector drop are unit-tested
  with synthetic observations only; no test opens a real conn, stops reading
  until kernel buffers fill, and asserts the disconnect + metrics — the exact
  scenario the pull-based design exists to survive.
- Two documented v1 quirks are pinned by zero tests: the **empty-collection
  commit bypass** (`filter.go:317-323` — verified untested; a refactor
  inverting that branch silently drops events for every filtered subscriber)
  and the 100x pre-dedupe abuse guards (`filter.go:97,156`). Also untested:
  zstd × maxMessageSizeBytes interaction, and the zstd wire has no
  independent consumer test (the bundled client only speaks deflate).

**Shape:**

1. **Subscriber-swarm oracle scenario** (~1 week): N real websocket clients
   with seeded random connect/disconnect/cursor/filter/options-update churn
   during live ingest + rotation + compaction, asserting per-conn contiguity,
   per-DID order, and at-least-once against the event-log model. Extends the
   planned #25 boundary-cursor observer rather than replacing it.
2. **One real stalled-reader test** (~1 day): stop reading, fill TCP buffers,
   assert the write-timeout disconnect fires and a stalled-but-caught-up
   client survives.
3. **Serve-side mutant expansion** (~2–3 days once the campaign-runtime item
   below lands): subscribe encoder/filter/replay + xrpcapi getSegment/checksum
   + client suppressor. The #105 rejection was on runtime grounds, not value
   grounds — unblock it, don't relitigate it.
4. The two one-test quirk fixes (empty-collection bypass, 100x guards) — do
   immediately, they're hours.

## P1.4 — Shadow verifier (online oracle) + rollout gate, before launch

The only methodology in this entire review that catches **environment bugs no
simulator can model**: real relay quirks, real CAR/MST shapes beyond the
corpus, kernel/filesystem behavior, clock skew, config drift, slow corruption.
The pattern (Netflix-school continuous verification) is: a standing verifier
binary that folds Jetstream's production `/subscribe` stream from a checkpoint
while independently consuming the upstream relay firehose, continuously
cross-checking per-DID convergence, at-least-once coverage, and per-DID order
— alerting on divergence with bounded diagnostics. It is the oracle's fold/
event-log checkers promoted to production.

We already own almost all the parts: `internal/client` *is* the negotiation/
fold/cutover engine, and the oracle checkers define the comparisons. The work
(~1–2 weeks) is checkpointing, memory-bounding per-DID state (sample a
deterministic rotating DID slice if full-network state is too large), and
alerting. It doubles as the **blue/green cutover gate** for a stateful
single-node service: green node comes up (alternate rebuild-from-upstream with
snapshot-restore, so *both* recovery stories stay certified), verifier
certifies green against blue before cutover.

For a service that all Bluesky production infrastructure will consume, this is
the difference between detecting corruption in minutes and detecting it after
downstream AppView state is poisoned. Run it against staging permanently the
moment staging consumes the real firehose. (Complements #215's freshness SLI —
that's health; this is correctness.)

---

## P2.1 — Coverage-of-the-oracle: the blind-spot map (GOCOVERDIR)

The mutation campaign measures whether the oracle *verifies* code it reaches;
nothing measures **what the oracle never executes at all**. Since Go 1.20,
`go build -cover -coverpkg=./...` produces an instrumented binary writing
counters to `$GOCOVERDIR`; the oracle already boots the real server as a child
process, so this is one build-flag change plus env plumbing, then
`go tool covdata` merges across tiers and seeds into a "never-executed
production code" report. Every red region is dead code (delete), an
untestable-by-oracle path (targeted test), or a missing simulator behavior
(extend the world).

Sharp edge (verified in the design): SIGKILL'd crash-tier children flush no
counters — call `runtime/coverage.WriteCountersDir` at the crashpoint seam
(already test-gated) just before the block-and-die.

~2–3 days; no new dependencies; extends the planned-but-unissued `just cover`
item and directly feeds the planned #218 coverage-guided seed retention.

## P2.2 — Metamorphic relations: the only cheap defense against model-shared bugs

The oracle's acknowledged structural blind spot: the simulator model and the
product can agree on the same wrong answer (the atmos closed loop is the
accepted instance; the 2026-06-28 diary proved the risk is real). Metamorphic
relations compare **the system to itself**, so they hold even where the model
shares the bug — and the expensive part (a seeded world driving the real
server) already exists. This is also sanctioned territory: broader metamorphic
relations sit in the 2026-06-20 oracle report §8's unclaimed idea pool.

Concrete relations, each a new checker over existing observer streams
(~5–10 days for the first four; hours each after):

1. **Cursor-suffix**: replay(cursor=N) == the suffix of replay(cursor=0) from
   the first seq ≥ N, per-DID.
2. **Filter commutation**: filtered /subscribe == unfiltered stream filtered
   client-side.
3. **Compaction idempotence**: compact(compact(S)) == compact(S), and
   fold(S) == fold(compact(S)).
4. **Restart no-op**: restarting a converged idle server changes no durable
   output (event log, XRPC plan, segment bytes modulo active tail).
5. **Plan stability**: planBackfill twice with no ingest in between returns
   equal plans.

CockroachDB runs exactly this shape as a first-class suite for Pebble;
SQLancer's variants found hundreds of DBMS logic bugs.

## P2.3 — Fuzz-surface completion

The 22 targets are well-aimed, but four untrusted-input parse paths have none
(verified):

1. **`api/jetstream` generated decoders — the highest-risk zero-coverage spot
   in the repo.** ~130KB of hand-rolled `UnmarshalJSONAt`/`UnmarshalCBORAt`
   parser code with zero tests of any kind, parsing untrusted input on both
   sides: planBackfill POST bodies server-side (`internal/xrpcapi/
   planbackfill.go:67`) and archive-server responses client-side
   (`internal/client/planner.go:163`). A table-driven fuzz over each
   Unmarshal entry point covers regenerated code automatically. (Worth asking
   whether lexgen should also emit fuzz targets — fix upstream in atmos's
   generator and every consumer benefits.)
2. **Segment bloom-region decode** (`segment/bloom.go:111,127` + the
   `Reader.BlockBloom` pread-offset math at `segment/reader.go:351`) — the
   only sealed-footer component without a fuzz target.
3. **Whole-file `segment.Open` fuzz** — component fuzzers exist but nothing
   fuzzes an arbitrary byte blob as a sealed segment, so cross-field
   validation interplay (`validateHeaderOffsets` × `validateBlockOffsets`) is
   only unit-tested against hand-corrupted files.
4. **`segment.ReadBlockFrame`** (`blockframe.go:20`) — the exact decode path
   remote clients run on getBlock bytes; adversarial unit tests but no fuzz
   over hostile header × index-entry space.

Plus: **apply to OSS-Fuzz** (native Go targets via `compile_native_go_fuzzer`;
a bluesky-social full-network archive plausibly qualifies as critical OSS
infra; ~2–3 days integration, runs on Google compute so it doesn't touch our
hardened CI; already named "longer-term" in the maintainability doc — the
application itself is an afternoon). Skip ClusterFuzzLite (fights the
egress-blocked, no-cache CI posture).

## P2.4 — Two-fault (pairwise) scheduling over the existing seams

Everything injects **one fault at a time** today. Recovery code is the
least-executed code in the system, and nothing tests "fault during recovery
from a fault": SIGKILL at crashpoint C, restart, then a pebble commit error
during replay; a torn tail plus a rename fault during the same restart's
merge. The empirical basis (NIST interaction studies, LDFI/Molly's results) is
that most fault-tolerance bugs need ≤2 interacting faults — and the expensive
machinery (enumerated crashpoints, store-fault seam, segment I/O seam, the
checkers) is all built. The work (~1–2 weeks) is a driver mode that samples a
bounded pairwise product {crashpoints} × {store faults, segment faults, second
crash during recovery}, seeded, trace-annotated, nightly. Skip full
LDFI/Molly (needs dataflow lineage; poor fit for one process).

## P2.5 — Hygiene batch (near-free, do as one PR)

All verified directly:

1. **`gocritic` config is dead**: `.golangci.yaml` sets
   `settings.gocritic.enable-all: true` but gocritic is not in the `enable`
   list, so it never runs. Someone intended it at maximum strength; today it's
   silent. Enable it (or delete the setting deliberately).
2. **Dependency whitelist is doc-only and already stale**: no
   depguard/gomodguard rule enforces the AGENTS.md list, and go.mod's direct
   requires already include four non-whitelisted modules (pebble,
   prometheus/client_model, prometheus/common, secp256k1-voi). Nothing
   mechanical stops indigo — deliberately kept out of the corpus tier — from
   entering via a future PR or a Dependabot bump. Add gomodguard + fix the
   AGENTS.md list to match reality. Also: no `go mod tidy` drift gate (lexgen
   drift IS gated; tidy is not).
3. **goleak** (`uber-go/goleak`, test-only dep): `VerifyTestMain` across
   goroutine-spawning packages. Today the only leak checks are two bespoke
   `NumGoroutine` polls in `internal/client`, and
   `internal/subscribe/handler_test.go:1718` explicitly punts on leak
   detection — so the websocket handler, the component most exposed to
   abandoned clients, has no leak assertion at all.
4. **`-shuffle=on`** in the justfile gotestsum invocations — zero occurrences
   today; 210 files use `t.Parallel`; shuffle prints its seed for repro.
5. **`exhaustive` linter** — the codebase is full of enum-shaped switches
   (event kinds, lifecycle phases, block types); a missed case on a new event
   kind is precisely the silent-drop class the invariants forbid, and the
   oracle only catches it if the simulator generates that kind. **`deadcode`**
   as an advisory justfile recipe pairs with the P2.1 coverage map.
6. **Package test budget**: `internal/repoexport` (1.6s — one test burning
   ~1.7s in atmos retry/backoff against a 500 relay; injectable retry policy
   fixes it) and `internal/client` (1.2s) exceed the team's own 1s rule.
7. **`just clean` footgun**: `rm -rf data*` silently deletes `./data-prod`
   (a real-network backfill). Exclude it or prompt.
8. **`just preflight`**: encode the AGENTS.md change-class table
   (oracle-long/sweep/fuzz/mutation-gate/bench/lexgen) as a parameterized
   recipe so the matrix isn't executed from memory.

---

## P3 — Worth doing, after the above

- **rapid property suite** (`pgregory.net/rapid`, test-only whitelist add,
  ~3–6 days): roundtrip/idempotence properties for the segment format algebra,
  fold commutativity over duplicate/overlapping seqs, compaction refinement vs
  tombstones. Thousands of adversarial small worlds per second with automatic
  shrinking to a minimal repro — which the oracle structurally cannot produce.
  Complements `internal/tombstone`'s existing hand-rolled property test.
- **Bounded automated-mutation survey** (avito go-mutesting fork, offline,
  quarterly at most, never a CI gate): mechanically probe operators the
  37 curated mutants never model (off-by-one in offset math, inverted
  comparisons in clamps); human-triage survivors into new tests or new curated
  mutants. It is the only way to measure the curated catalog's own
  completeness. Expect 10–30% equivalent-mutant noise; ~2–4 days first pass.
- **Mutation campaign runtime work**: the driver is serial (full
  `go build ./...` per mutant), m001 burns its full 5-minute timeout every
  run, per-mutant wall time isn't recorded, and the #105 egress mutants were
  rejected purely on runtime cost. Record per-mutant timing in the JSON,
  parallelize via worktrees, and the P1.3 serve-side mutant expansion becomes
  affordable.
- **Oracle observation adds** (each cheap): a counting slog handler failing
  the lifecycle run on unexpected ERROR-level records (today a data-correct
  run that screams errors passes silently); one `/status` scrape asserting
  phase/cursor coherence with the store (never asserted anywhere today); a
  goroutine-count bracket on the real-process tiers (the synctest bubble's
  implicit leak check covers one tier only).
- **Anti-vacuity rot fixes** (from the oracle-internals audit): (a)
  `chainCoverage`'s seq-0 join can silently excuse a genuinely-lost create
  whose key has an on-disk tombstone below the watermark
  (`restart_chain_assert_test.go:44-60`) — distinguish "absent from disk"
  from seq 0; (b) `adversarialGateReasonMatrix` rots by omission when
  production adds a new drop reason — assert the matrix enumerates the
  production reason constants; (c) the three coverage-shaping `time.Sleep`s
  in `partb_scenarios_test.go` (212/454/497) silently select *which seam*
  gets exercised — replace with explicit client-state barriers.
- **Small Quint/TLA+ spec of the orchestrator lifecycle** (~1–2 weeks
  including learning curve, moderate priority): model
  bootstrap→merge→steady with crash re-entry and the two durable stores as
  separate variables with a lossy crash action. Exhaustively enumerates all
  small-scope crash × phase × durable-state interleavings — design insurance
  the sampled-execution rig can't give. The mutation-campaign-style sanity
  check: deleting the fsync-ordering from the *model* must produce a
  counterexample. MongoDB-style trace conformance (the oracle already emits
  JSONL) is a natural follow-on. Do after P0.1, which addresses the same
  invariant empirically and more urgently.
- **-race soak mode**: make `-race` a mode of the planned soak tier (#33)
  rather than new staging infra; Go 1.26's experimental
  `goroutineleakprofile` pprof endpoint is a free addition to the debug
  listener for soak runs.
- **Client crash-resume contract**: docs claim "a crashed backfill resumes
  from its last continuation cursor" (`docs/README.md` §2.1) but the engine
  keeps the cursor only in memory — either add the persistence hook + test,
  or soften the doc.

## Explicitly skipped, with reasons (so future agents don't re-propose)

- **Porcupine / linearizability checking** — abstraction mismatch: the
  contract is at-least-once + per-DID order, not linearizability; forcing a
  register model would produce false positives or degenerate into checks the
  event-log tier + metamorphic suffix relation already do. Record as settled.
- **NilAway** — still false-positive-prone, custom-plugin build complicates
  the pinned golangci-lint v2 setup, duplicates the crash-loud posture.
  Revisit only if merged into golangci-lint core.
- **dm-log-writes / dm-flakey / CrashMonkey** — kernel tooling whose unique
  value (sub-fsync-boundary block reordering) is below the invariant we
  actually rely on (the torn-tail walk already assumes arbitrary garbage past
  the last fsynced offset). P0.1's two layers cover the classes that matter
  at far lower ops cost. Revisit only if a production incident implicates
  block-level reordering.
- **ClusterFuzzLite / CIFuzz** — needs cache/corpus storage and egress that
  fight the hardened CI posture; OSS-Fuzz proper runs on Google infra instead.
- **Flaky-test quarantine tooling / gotestsum --rerun-fails** — would
  actively undermine the specs/oracle root-cause diary discipline, which is
  strictly better.
- **Per-PR benchmark gating** — #220 already declined bench CI on runner-
  noise grounds; respected. One new fact worth a future note: `allocs/op` is
  deterministic on any runner (unlike ns/op), so a tiny allocs-only ratchet
  on the segment hot path would be noise-free if we ever want a partial
  reopen. Not proposed now.
- **synctest as an oracle substrate** — already the primary lifecycle tier
  where it fits; real-process tiers stay real by decision (#28 withdrawn).

## Corrections found during the audit (fix regardless)

- `testing/mutation/RESULTS.md:8` — the "keep this line current" header says
  35 active mutants (m001–m043); disk and baseline.json carry 37 (m044/m045
  banked 2026-07-06).
- `internal/subscribe/doc.go:69` cites `TestWants_IdentityBypassesCollectionFilter`,
  which doesn't exist under that name (actual tests differ) — stale reference.
- AGENTS.md dependency whitelist vs go.mod drift (see P2.5.2).
- `flake.nix` lacks `gh` (the issue-tracking workflow depends on it).

## What this audit deliberately did not relitigate

atmos as an accepted black box; v1 differential testing (closed); bit-exact
determinism (non-goal); CI caching ban; the adversarial-input contracts
(#204); the ENOSPC crash-loud posture; test-suite consolidation (declined);
restart tier staying real-process; the gotchas.md accepted limitations. The
already-planned-and-open items (#25, #26, #31, #33, #218, #233, #215, #217)
are extended by the recommendations above where noted, not duplicated.
