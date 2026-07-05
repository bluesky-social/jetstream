# Tech-debt paydown — exploration & ideation (2026-07-02)

Status: **evidence archive — superseded for planning purposes (2026-07-03).**
Jim reviewed all sections; decisions are recorded inline as `DECISION (jrc)`
markers. The forward-looking plans were split into two focused docs:

- `2026-07-03-oracle-testing-improvements.md` — oracle/simulator work,
  including the full adversarial-traffic catalog (was §2/§2b here).
- `2026-07-03-maintainability-improvements.md` — fix-now bugs, CI, DX,
  operational readiness, format longevity, agent-docs bundle, deferred
  release engineering (was §0/§3/§4 here).

This doc remains the record of the exploration evidence, the decision trail,
and the dropped/closed items (test-deletion analysis §1, atmos
independent-decoder rationale, v1 differential closure). SECURITY.md landed
2026-07-03. No issues filed yet from either child doc.

Four parallel deep-exploration passes were run over the repo, one per question Jim
raised: (1) test-suite value audit, (2) oracle rigor assessment, (3) long-term
maintainability survey, (4) agent-docs gap analysis. This doc synthesizes the
evidence and proposes candidate work items for discussion. Nothing here is
committed-to; sections end with open questions.

The one-paragraph verdict up front: **the codebase is not "vibes" — it is in
unusually rigorous shape, and in two places (mutation gate, anti-vacuity
discipline) it is well ahead of typical infrastructure projects.** The debt that
exists is real but specific: ~25–35% of tests are deletable/collapsible (not
50%), the oracle's blind spots are precisely itemized by its own mutation
survivors and just haven't been paid down, release engineering is entirely
absent, and two actual bugs were found during the survey (below).

---

## 0. Bugs found during the survey (fix-now candidates, independent of any debate)

These fell out of the maintainability pass. Both are small and unambiguous:

1. **Dockerfile version stamp is silently broken.** `Dockerfile:46-48` injects
   ldflags into `github.com/bluesky-social/jetstream-v2/internal/version.*`, but
   `go.mod` declares module `github.com/bluesky-social/jetstream`. `-X` against a
   nonexistent symbol path is silently ignored, so every Docker build ships
   `Version=dev Commit=unknown` in `build_info`, `/status`, and the `version`
   command. The `justfile` `build` recipe uses the correct path, which is why
   local builds look fine. Same stale `jetstream-v2` string in the ldflags
   example comment in `internal/version/version.go` and the README CI badge URL.
   Root cause of survival: nothing in CI ever builds the Docker image.

2. **`.env` leaks dev-only flags into `just run-prod`.** `set dotenv-load` loads
   the committed `.env` for every recipe; `run-prod` overrides only
   `JETSTREAM_RELAY_URL`, `JETSTREAM_PLC_URL`, `JETSTREAM_DATA_DIR`. So
   `JETSTREAM_SKIP_MERGE_DISCOVERY=true`,
   `JETSTREAM_DISABLE_REPO_ACTION_RATE_LIMITS=true`, and
   `JETSTREAM_STATUS_CACHE_TTL=1s` silently apply when running against real
   production services. Fix options: explicit resets in `run-prod`, or split
   `.env` → `.env.sim` loaded only by sim-facing recipes.

---

## 1. Test suite: can we delete half?

### Measured reality

- **1,385 top-level `func Test`** — not ~1,816. The `rg 'func Test'` count picks
  up helpers and the tokei Go-in-Markdown figure inflates the impression;
  actual test entities: 1,385 Test + 127 subtests + 22 Fuzz + 18 Benchmark.
- Test LOC 57,731 vs prod LOC 47,821 → **1.21 : 1 overall.** High-normal for
  infrastructure, not pathological.
- The AGENTS.md philosophy is already mostly implemented: real oracle, 22 fuzz
  targets, 11 swarm tests, enforced mutation baseline gate.

Densest packages by test:prod ratio (excluding `internal/oracle`, whose 3.57
ratio is the harness itself plus mandated anti-vacuity tests):

| package | Test funcs | test:prod LOC |
|---|---|---|
| internal/ingest/live | 44 | 2.09 |
| internal/subscribe | 162 | 1.93 |
| cmd/jetstream | 28 | 1.87 |
| internal/ingest | 74 | 1.86 |
| internal/xrpcapi | 54 | 1.76 |
| internal/client | 98 | 1.60 |
| segment | 164 | 1.36 |
| internal/ingest/backfill | 130 | 1.14 |

### Verdict: delete-half is not realistic; ~25–35% is, in three risk tiers

**Tier 1 — ~zero risk (~180–210 tests, ~7k LOC).** No mutant, oracle assertion,
or production invariant depends on these:

- **7 copy-pasted `metrics_test.go` files** (~15 tests, ~700 LOC): e.g.
  `internal/ingest/metrics_test.go:15-52` calls each inc helper and asserts the
  counter is 1; plus nil-receiver-doesn't-panic tests. Same pattern in
  `ingest/live`, `ingest/backfill`, `ingest/orchestrator`, `store`, `obs`.
- `internal/obs/verifier_test.go:13-67` — 8 one-assert tests restating a
  string-contains switch (worst case: a mislabeled Prometheus label).
- `internal/web/handler_test.go` — 20 of 24 tests assert HTML substrings of the
  debug UI against fake snapshots. Keep `TestHandler_EscapesXSS:356` and the
  rate-limit pair (`:542,:586`).
- `internal/store/encoding_test.go:19-49,74-117` — 7 LE-integer round-trips
  (keep `:51 TestPrefixUpperBound` — real carry/0xFF logic).
- Root `client_test.go:60-131` — options getter/clamp tests.
- `internal/status/collect_test.go:317-503` — 5 mock-mirror account-lookup
  plumbing tests.
- `internal/web/format_test.go` — duration/relative-time format helpers.

**Tier 1.5 — consolidation, zero semantic change (~200–300 tests → ~60–80).**
Merging over-granular Test funcs into tables removes ~200 from the count with
literally no coverage change:

- `internal/subscribe/cursor_test.go:53-421` — 20 Test funcs for one
  `ResolveCursor` function; ~8 boundary cases carry all the signal (and
  `cursor_fuzz_test.go` + the partb tier / m032 guard the floor semantics).
- `internal/xrpcapi/planbackfill_test.go:298-401` — 6 wildcard permutations → 2-3.
- `internal/timestamp/parse_test.go:33-323` — 17 malformed-CSV variants → table
  (FuzzParseRoundTrip + the hostile round-trip test carry the load).
- `segment/writer_test.go:39-111` — 5 rejects-invalid-X tests → 1 table.

**Tier 2 — low risk, requires mutation verification (~150–250 tests, ~8–10k
LOC).** Oracle-shadowed happy paths:

- `internal/ingest/writer_test.go` lifecycle happy paths (`TestOpen_FreshDir:72`,
  `TestAppend_AllocatesMonotonicSeq:369`, …) — folded through every oracle run.
  **Keep** the async-flush/rotation/torn-tail cluster
  (`TestOpen_RecoversFromTornTail:661`,
  `TestAsyncFlush_SeqIntegrityAcrossRotationAndReopen:2349`, the concurrent
  producer tests) — those reach interleavings the oracle can't schedule.
- `internal/ingest/orchestrator/merge_test.go` happy paths — **keep** the
  crash-idempotency trio (`:244,:293,:333`) and all `*_storefault_test.go`
  (named in `testing/mutation/run.sh:246-255`).
- `internal/subscribe/filter_test.go` unit tier (33 tests) vs the same semantics
  at websocket level in `handler_test.go:875-1185` — **except** the 5 tests
  marked `V1 PARITY` (e.g. `filter_test.go:92-103`), which encode knowledge
  that exists nowhere else (v1's code contradicts v1's docs).
- ~Half of `internal/ingest/backfill/store_test.go` status/aggregate bookkeeping.

**Do not touch (~700+ tests):** all fuzz/swarm/golden/corruption/crash/restart
tests, everything named in `testing/mutation/run.sh`, anything with a `#N`
issue reference (~90 references across 33 files) or `V1 PARITY` comment, all of
`segment/` and `internal/tombstone` (whose
`TestSnapshotShouldDropDIDChainsWithSpecificReason` is the *sole* kill for m022).

### The empirical guardrail (this is the key insight)

Two facts from `testing/mutation/RESULTS.md` turn this from taste into
measurement:

1. **16 of 21 mutant kills come from oracle tiers, not unit tests** — so mass
   unit deletion mostly doesn't move the scorecard. BUT the **6 survivors map
   exactly onto oracle blind spots where unit tests are the only nearby
   coverage** (m009 checksum range, m015 footer index, m002 watermark boundary).
2. **We have already been burned once:** deleting `internal/overlay` in #177
   silently flipped m022 KILLED→SURVIVED (regression #182), caught only because
   the gate exists.

### Proposed process (if we proceed)

1. **Before each deletion batch, add 2–4 new mutants** targeting the code that
   batch covers (the catalog is thin outside ingest/segment — nothing targets
   `internal/subscribe/filter.go` or `internal/client/engine.go`), bank them
   KILLED via `just mutation-baseline`.
2. One cohort per PR: delete → `just mutation-gate` + full `-race` +
   `just oracle-sweep`. Any KILLED→SURVIVED flip identifies the specific test
   that was load-bearing; revert just that one.
3. Order: Tier 1.5 consolidations first (zero risk, biggest count reduction),
   then Tier 1, then Tier 2 area-by-area.
4. Skip `segment/` and `internal/tombstone` entirely.

### DECISION (jrc, 2026-07-03): leave the tests alone

The goal was maintenance burden, and the analysis shows the codebase is already
low-churn — so the expected maintenance cost of the extra tests is small, and
the deletion protocol (mutant-first, batch-verified) costs more than it saves.
**No test deletion or consolidation work will be undertaken.** Standing policy
instead: when a future major refactor lands, delete tests that are no longer
relevant at that time rather than preserving them for their own sake. The
cohort analysis above stays as the map for that future moment (and the
"do not touch" list remains binding whenever tests are removed — the m022/#182
precedent stands).

---

## 2. Oracle rigor: how much confidence, and how to get more

### What confidence is currently warranted

**Strong** (independent model + mutation kills back each): archive final-state
integrity (`GroundTruthFromWorld` walks the simulator's own MST — genuinely
independent), event-log multiset completeness, compaction drop-rule correctness
(re-implemented independently of `internal/tombstone`,
`foldconvergence.go:123-131`), paginated cutover (partb kills m029–m033),
enumerated crash seams. Anti-vacuity discipline is pervasive and unusual:
fault-must-fire guards, red-first coverage power tests, trace-kind presence
checks.

**Weak**: anything both sides delegate to atmos; segment read-path indexes a
real reader depends on; behavior under hostile/diverse upstream input;
unenumerated crash timings; long-horizon behavior. Both failure-diary entries
share a pattern: bugs surfaced where fault injection or timing *luck* happened
to reach, not where a systematic sweep was pointed.

### The identity-function audit (the "testing ourselves" question)

Seam-by-seam ratings:

- **Green (independent):** final-state ground truth; compaction checks;
  the hand-rolled frame-header CBOR walk in
  `expected_eventlog.go:179-260` (kill of m019 proves teeth).
- **Yellow (partial):** the whole loop rides `jcalabro/atmos` on *both* sides
  for CBOR/MST/commit-signing/CAR — a symmetric atmos bug is invisible. Not
  hypothetical: the 2026-06-28 diary entry (LoadFromCAR accepting
  boundary-truncated CARs) is exactly this shape, found only because a
  truncation fault landed on a block boundary by seed luck. Also: expected
  event-log *bodies* still decode via atmos even inside the "independent"
  decoder; the client tier shares `segment.Event` decoders with the server
  (acknowledged in `specs/oracle.md:257-262`).
- **Red (circular, each documented but open):**
  - m009: `xxh3HeaderFooter` writes (seal.go:123) and verifies (reader.go:193)
    with the same function; golden fixtures pin format bytes but don't
    independently recompute the checksum — they'd catch a format *change*, not
    a symmetric range bug present since before the fixture existed.
  - m015: oracle decodes blocks sequentially and never consults the footer
    collection-count index / per-block blooms the production planner and cursor
    reader actually use.
  - m013/m014: survive because simulator traffic never takes the mutated path —
    a *fidelity* gap masquerading as coverage. Corollary: no `#identity` frames
    exist in `world/firehose.go`, so `segment.KindIdentity` ingest is dead path
    under the oracle.
  - m002: killed only 4/5 stress seeds — boundary bug detected probabilistically
    instead of by a boundary-exact scenario.
  - m003: unkillable because the restart harness can only construct
    single-source-segment worlds — a scenario-space ceiling, not a proof.

### Determinism status

The synctest-bubble main tier is *more* deterministic than `specs/oracle.md`'s
stated aspiration (fake clock, pipe listeners, no sockets, salted PCGs
decoupling fault RNG from world RNG). Remaining gaps:

- The trace-determinism allowlist (`trace_determinism_test.go:126-142`) excludes
  worker-completion-order kinds — i.e. **the entire data plane's interleaving is
  scheduler-decided, not seed-decided.** Same seed ⇒ same inputs and same final
  assertions, not same execution. Diary entry 2026-06-27 (seed flaking ~1/40) is
  the canonical instance.
- Restart tier is least deterministic: real subprocesses/sockets/disk, 5ms
  polls, 30s timeouts. Both diary flakes came from restart/stress.
- One-bubble-per-process (zstd package-global goroutines) forces
  process-per-seed for any swarm exploration.

### Ranked improvements (leverage ÷ effort)

- **P1 — Real-data corpus + independent decoder cross-check.** Capture real
  firehose frames + getRepo CARs from diverse PDS implementations, commit as
  testdata; cross-check record extraction via a second CBOR/CAR decoder. The
  only proposal that attacks the atmos closed loop head-on. Include an
  independently-computed (or pinned known-good) checksum for a committed
  segment fixture → kills the m009 class. Corpus tier is *promised* in
  `specs/oracle.md:357-363` and currently vaporware.
- **P2 — Simulator adversarial-traffic fidelity.** Seeded modes for `#identity`
  frames, non-deleted account statuses, unknown lexicons, near-limit + Unicode
  records, relay seq gaps/duplicates, malformed-but-bounded frames — each with a
  defined contract per `oracle.md:349-355`, plus an archived-≥1-of-each-kind
  anti-vacuity assert. Revives the dead paths behind m013/m014.
- **P3 — Random-time kill loop + multi-source-segment worlds.** Nightly seeded
  SIGKILL at random offsets/ordinals over a world with multiple merge source
  segments (the missing scenario behind m003). Assertion bundle already exists;
  new machinery is just the kill scheduler. Closest cheap analogue to VOPR's
  "explore timings nobody named."
- **P4 — Segment-file I/O fault layer.** The fault seam stops at pebble
  (`store/fault.go`). Wrap segment write/fsync/rename: short-write, fsync-error,
  ENOSPC, truncate-at-offset sweeps, asserting fail-loud via `ObserveSegments`.
  (Dovetails with the ENOSPC operational gap in §3.)
- **P5 — Pay two named debts.** Re-derive the #100 over-drop mutant (#183 —
  right now nothing proves that recorder can go red), and make m002's watermark
  boundary check exact (pin a tombstone/survivor pair *at* the boundary, partb
  style, instead of hoping a seed lands there).
- **P6 — In-bubble seed swarm.** Bubble lifecycle runs in <1s; a
  process-per-seed driver gets hundreds of deterministic-input lifecycles per CI
  hour vs the current 10–20 per 6h sweep. Coverage-guided seed retention (keep
  seeds whose traces hit rare kinds) is a cheap multiplier — the trace JSONL
  already has the features.
- **P7 — Differential vs jetstream v1** (worth it iff v1 wire parity is a
  product promise — currently leaning on 10 golden events).
- **P8 — Soak tier** (hours-long steady→compact→restart loop watching goroutine
  counts, tombstone-set size, manifest growth, watermark monotonicity). Do
  after P1–P4; it finds slower bugs. Relates to open #33.

### DECISIONS (jrc, 2026-07-03)

- **No second CBOR decoder / atmos alternative — accepted.** Atmos is treated
  as an owned, controlled black box: it has completed full-network backfills
  repeatedly, is rigorously tested itself, and Jim owns it (jcalabro/atmos) so
  fixes and test improvements land there directly. A parallel implementation
  would generate more bugs and false positives than it catches. The P1
  cross-check idea is dead; the symmetric-atmos-bug risk is accepted and
  mitigated by improving atmos's own test suite when warranted.
- **Real-data corpus: yes, but low expectations.** Cheap to capture/store/run;
  only exercises happy paths, so it's a small confidence gain, not a structural
  fix. Privacy is a non-concern — atproto data is public by ethos and the
  sample is small. No hashing/normalization gymnastics needed; store real
  frames/CARs as-is.
- **Simulator adversarial-traffic fidelity (P2) is THE priority.** Jim wants
  the simulator to "throw all sorts of crazy shit at jetstream" and verify
  correct handling. Plan: enumerate all reasonably-high-value adversarial input
  classes, priority-order them, attack each. Catalog in §2b below (grounded in
  a code survey of the actual ingest input surface).
- **P7 (v1 differential) is already done and closed.** `cmd/compare` was built
  for exactly this; a 5-minute production run detected no material differences
  vs jetstream v1. v1 is buggy with poor testing/operational practices — we are
  already more rigorous than parity requires. No further investment.
  (Note for §3: this also answers the `cmd/compare` ship-or-delete question —
  it has already paid for itself; deletion before 1.0 remains an option once
  it's no longer useful.)
- **P3/P4/P5/P6/P8 all approved in spirit.** The oracle should be
  "exceptionally well done — no stone unturned (within reason)."

### §2b. Adversarial-traffic catalog (P2 detail)

Grounded in a full code survey of the upstream-input surface (2026-07-03).
Architecture note: frame decode, seq-gap detection, verification, and reconnect
live in atmos; jetstream's own surface starts at `streaming.Event` in
`internal/ingest/live/events.go:33` and `*repo.Repo` in
`internal/ingest/backfill/handler.go:72`. Segment column limits
(`segment/block.go:18-38`): DID ≤65535, Collection/Rkey/Rev ≤255, Payload
≤4 GiB; over-limit → `ErrFieldTooLong` → drop+metric per AGENTS.md.

Live-consumer error taxonomy (`live/consumer.go:394-498`): decode/gap →
`incDecodeErrors`+continue; `ErrUnknownEventKind` → cursor held;
`DroppedMissingBlocksError` → per-op drop, survivors archived;
`ErrFieldTooLong` → drop+metric; any other validation/append failure → fatal.

Today's fault surface is exactly three knobs (`oracle/faults.go`): per-DID
getRepo HTTP status, per-DID getRepo CAR truncation, per-connection disconnect
thresholds. **All firehose-frame-level adversity is absent**, and the world
generates only polite traffic (5 bsky collections, lowercase ASCII, TID rkeys,
complete CARs, monotonic seqs, honest TID revs, only status="deleted").

Every case below must land with a defined contract per `specs/oracle.md:355`:
archive survivors? drop? advance cursor? which metric? bounded log? keep
running? — plus an archived-≥1-of-each-kind anti-vacuity assert per mode.

#### P0 — silently-wrong and untested at ANY level (attack first)

1. **Non-TID / regressing revs.** No monotonicity or format check at ingest;
   rev ordering is consumed by merge filter (`orchestrator/merge_filter.go:40-55`,
   lexicographic TID compare), stale-resync guard (`consumer.go:199-220`), and
   syncstate `PromoteChain`. A malformed rev compares lexicographically-wrong
   **silently** → wrong compaction/merge decisions. Simulator: emit non-TID,
   empty, regressing, and future revs. Contract decision needed: validate rev
   shape at ingest (drop+metric) vs document garbage-in-garbage-out.
2. **Arbitrary-byte / invalid-UTF-8 rkeys.** MST enforces ASCII on *insert*,
   but wire commits aren't re-validated (`StrictValidation` never set — zero
   uses). Bytes archive faithfully, then `json.Marshal` at delivery replaces
   invalid UTF-8 with U+FFFD → **a client's delete-by-rkey never matches:
   silently wrong delivery**, byte-faithful archive. Simulator: rkeys with
   null bytes, invalid UTF-8, emoji, RTL. Contract decision: reject
   non-conforming rkeys at ingest (drop+metric) vs escape-safe delivery.
3. **`$`-prefixed wire collections.** Sentinels `$account/$identity/$sync` are
   collision-safe only because `$` is invalid in an NSID — but jetstream never
   rejects a wire collection starting with `$` (`events.go:89,155` cast as-is)
   → potential sentinel shadowing in block indexing/planning. Contract: almost
   certainly drop+metric at ingest. Simulator: hostile collection names ($
   prefix, 255/256-byte, empty, Unicode).
4. **Seq duplicates / regressions from relay.** atmos gap check fires only for
   `seq > last+1`; duplicates and regressions pass through and re-archive under
   new jetstream seqs. Correctness holds by idempotent folding — **by accident,
   untested**. A relay restored from backup replays a whole window. Simulator:
   duplicate-N, regress-to-K replay modes. Assert: no invariant breaks, storage
   bloat bounded, (probably) a new metric distinguishing this from gaps —
   today gap and decode errors share one metric (`consumer.go:367-376`).

#### P1 — dead paths with tombstone/erasure blast radius (the m013/m014 lesson)

5. **#identity frames.** `world/firehose.go:16-21` has no identity header —
   `KindIdentity` ingest is dead path under the oracle; the oracle's expected
   eventlog can already decode them (`expected_eventlog.go:93-98`), so this is
   purely a generator gap. Also feeds net-new-DID enqueue (malformed-DID
   handling already exists + metric'd). Cheapest P1, unlocks an entire kind.
6. **Non-deleted #account statuses** (takendown/suspended/deactivated/unknown +
   reactivation transitions). Tombstone set treats exactly `"deleted"` as
   tombstone (`tombstone.go:238-244`) — if that exactness ever broke,
   **compaction would permanently erase live records**. Currently correct,
   verified only by unit matrices. Simulator: full status lifecycle generation;
   oracle asserts non-deleted statuses never fold as tombstones. Also getRepo
   `RepoTakendown/Suspended/Deactivated` → terminal StatusUnavailable
   classification (`diagnostics.go:291-302`) has no simulator path.
7. **Near-limit / over-limit fields through the oracle.** Drop+metric contract
   is unit-tested (`consumer_test.go:314`) but no oracle run has ever exercised
   it; world text caps (≤3000) sit far from the real limits. Simulator: rkeys
   at 254/255/256, collections at limit, huge payloads. Assert: over-limit
   dropped with metric AND all surrounding events archived (the survivors
   contract). Note a spec-valid >255-byte rkey (MST allows ~1023) is dropped
   *by design* — document as accepted limitation in gotchas.md.

#### P2 — frame-level adversity (relay sends garbage)

8. **Unknown frame types** (`#future`): atmos → batch error → decode-error
   metric + continue. Unit-tested; the documented data-loss window (unknown
   events behind the watermark at restart are unreachable,
   `consumer.go:397-410`) is unobserved by any oracle scenario.
9. **Malformed CBOR frames (bounded).** atmos decode is fuzzed and never
   panics, but no test runs garbage frames *through the live consumer* — the
   log+metric+continue path and cursor behavior under interleaved garbage are
   asserted nowhere end-to-end.
10. **Oversized frames.** atmos `SetReadLimit` (2 MiB) → websocket close →
    reconnect. A relay persistently sending >2 MiB = reconnect loop. Untested.
    Simulator: one oversized frame (assert clean reconnect + no loss around
    it), persistent-oversize mode (assert bounded behavior + visibility).
11. **Partial firehose-#commit CARs** (missing record blocks on the *live*
    path). Per-op drop with survivors is well unit/swarm-tested, but the
    simulator can only truncate getRepo CARs — firehose CARs are always
    complete. Simulator: strip N blocks from a commit's CAR. Assert survivors
    archived + `dropped_ops_missing_block_total`.

#### P3 — backfill-side adversity

12. **getRepo terminal/exotic responses**: RepoNotFound (incl. the
    non-canonical message variant), 429 + RetryAfter (incl. hostile far-future
    reset — clamp exists, `retry.go:377-394`), redirects. Only 503s and
    truncation exist today.
13. **listRepos pagination faults**: duplicate pages, cursor loops, shrinking
    pages, empty-final-cursor variants. (Dup/missed DIDs are recoverable —
    idempotent downloads + net-new enqueue — so this asserts the recovery.)
14. **DID-doc weirdness**: missing PDS endpoint, malformed handle, resolution
    failures under load. PLC handler today always returns a well-formed doc
    (`simulator/http/plc.go:38-72`). Asserts the `unresolved.did`/`invalid-pds`
    host-bucket classification end-to-end.
15. **Net-new DID mid-run** (event for a DID listRepos never served). Unit
    coverage is strong (16 tests); no oracle scenario. Requires world support
    for adding an account after bootstrap that listRepos omits.

#### P4 — decide-and-document (not simulator work)

16. **tooBig commits**: `TooBig` field exists in the atmos generated type,
    nothing reads it anywhere. If a v1-era relay sent one with empty blocks it
    would surface as missing-block drops. Decide: is tooBig extinct upstream?
    If yes → gotchas.md entry; if no → explicit handling + simulator mode.
17. **Gap-vs-decode metric conflation** (`incDecodeErrors` for both): split so
    operators can distinguish relay data loss from garbage frames. Trivial;
    fold into whichever P0/P2 item touches that code first.
18. **Huge single-commit repos** (batched 1024 appends, no ceiling): soak-tier
    concern (P8), not adversarial-traffic.

#### Suggested execution shape

Each P0/P1 item is one issue: (simulator generator + contract decision +
oracle assertion + anti-vacuity check) land together. P0 items likely uncover
production fixes (rev validation, rkey validation, `$`-collection rejection) —
those are the point. After P0+P1, re-run the mutation campaign and add mutants
against the newly-live paths (e.g. a mutant that breaks tombstone status
exactness, one that breaks the survivors contract) so the new coverage is
gate-enforced, not just present.

---

## 3. Long-term maintainability

Baseline is strong (verified, not vibes): full non-short `-race` suite per push,
6-hourly oracle sweeps + race lane, daily mutation gate with committed baseline,
golden files pinning disk and wire formats, 13 fuzz targets, lexgen drift gate,
genuinely hardened Actions (SHA-pinned, `permissions: {}`, egress allowlists,
`GOTOOLCHAIN=local`), zero TODO/FIXME in non-test code, `/healthz` + `/readyz`,
pprof on debug listener, consistent metric naming, `segment/` has zero internal
imports.

### Gaps, ranked by leverage

**Release engineering — the biggest absolute gap.** Zero git tags, no release
workflow, no changelog, no SBOM, no image publishing, and nothing exercises the
Dockerfile (which is how bug #0.1 survived). Minimal setup, ~a day:
`release.yml` on `v*` tags (linux/{amd64,arm64} + buildx multi-arch → GHCR,
same hardening posture, separate minimal-permission workflow);
`--sbom=true --provenance=true` nearly free; CHANGELOG.md discipline (the
issue-per-unit convention already generates the raw material); push-CI builds
the Docker image (no push) so it can't drift.

**A now-or-never API decision:** the module root is a public Go client library,
and `segment/` is exported beside it. Tagging 1.0 freezes both. Either sealed-
file parsing is deliberately public API (plausible for an archive format), or
`segment/` moves under `internal/` **before** the first tag. Also: one module ⇒
server and client version in lockstep — document that.

**CI additions (keep push CI fast; use the scheduled lanes):**
- `govulncheck` — nothing scans deps today (`just vuln` + scheduled step;
  egress allowlist needs `vuln.go.dev:443`).
- Fuzz lane: 13 targets, zero committed `testdata/fuzz/` corpus dirs, no CI
  invocation. (a) commit regression corpora, (b) `just fuzz 60s` in the
  6-hourly job, (c) longer-term ClusterFuzzLite/OSS-Fuzz — this parses hostile
  network input.
- Benchmark regression tracking: weekly `-count=10` + benchstat vs committed
  baseline — segment writer/sealer is the hot path of a full-network archive.
- Compile-only `GOOS=darwin GOARCH=arm64 go build ./...` matrix step —
  `segment/sync_darwin.go` has real behavioral divergence and never compiles in
  CI. (Also: consider a runtime warning log on darwin, since the fsync no-op is
  a footgun against real data.)
- Pin the `just` version in `extractions/setup-just` (currently latest-per-run —
  nondeterministic tool download).
- Verify once that dependabot gomod PR branches actually trigger the push-only
  workflow.
- `timeout-minutes: 10` on the race job will page spuriously before it pages
  truthfully as the suite grows; consider 20.

**Operational readiness (pre-production):**
- **Disk-free / ENOSPC posture — the most predictable incident for an
  append-forever archive.** No Statfs anywhere; `/status` reports bytes used,
  never bytes free. Minimum: `jetstream_data_dir_free_bytes` gauge + a *decided*
  writer behavior on ENOSPC mid-block + an oracle/crashpoint case (ties to §2
  P4).
- Startup warning on unrecognized `JETSTREAM_*` env vars (~30 lines; kills the
  classic typo'd-var silent-ignore hour).
- Panic posture for long-lived goroutines: one `recover()` in the codebase;
  an ingest/orchestrator panic dies as a bare stderr traceback that log
  shippers mangle. Top-of-goroutine recover-log-rethrow in
  `internal/jetstreamd/runtime.go` so panic value + build info land in
  structured logs before death, + `GOTRACEBACK` operator guidance.
- Named end-to-end freshness SLI: upstream cursor minus last durable seq, as a
  duration — the one number a "how far behind the network" SLO hangs off.
  Define it now so dashboards don't derive it from two gauges.
- Document that `/readyz` means "listeners bound," not "ingest healthy"
  (probably correct for a single-process service — but say so in the handler).

**Format longevity:**
- The migration story is a comment ("older or newer files land via a future
  migration path", `decodeHeader`) — before 1.0, write the actual policy in
  docs §3.1: does a v2 reader read v1 files in place (preferred at multi-TB
  scale)? Is `segment/rewrite.go` the migration vehicle? Decision, not code —
  but it constrains what the 158 reserved header bytes can be used for.
- Dropped 2026-07-05 (jrc): do not add an `inspect-segment --verify`
  deep-check mode. Footer/index/bloom verification belongs in reusable segment
  code and oracle coverage (#208), not a new operator CLI surface.

**Local DX (small, cheap):** `just cover` (coverage of `segment/` +
`internal/ingest/` is the number worth watching), `just profile-cpu`/`heap`
wrapping the pprof endpoints, `just docker-build` (would have caught bug #0.1),
optional pre-commit hook installer via `install-tools` (gofmt + lexgen drift —
the classic fails-in-CI-10-minutes-later pair).

**OSS hygiene before 1.0:** SECURITY.md (vuln-report channel), CONTRIBUTING.md
(must explain the push-only-CI "maintainer pushes your branch" flow or external
contributors will assume CI is broken), CODEOWNERS, issue templates. Decide
whether `cmd/compare` ("temporary black-box diagnostic tool") ships or dies
before a tag makes it permanent. Resolve docs §10 inline `NOTE (jrc):` comments.

### DECISIONS (jrc, 2026-07-03)

- **All §3 suggestions approved in general.**
- **`segment/` is intentionally public.** It's a well-defined file format that
  will be exposed to the world with documentation (in the forthcoming
  user-facing docs). No move to `internal/`.
- **Release engineering: deferred, deliberately.** Product is brand new and not
  ready for public release; Jim doesn't want people running it until it's
  vetted further. Shortlist item 5 moves to a "later, before 1.0" bucket. The
  two §0 bugs (Dockerfile ldflags, run-prod env leak) are still fix-now — they
  are correctness bugs, not release engineering.
- **New item: nix flake for local dev.** A small flake so new developers get a
  stable, easy build environment; CI should use it too so CI and local envs
  match. Open design question: whether the Dockerfile should also build via nix
  (nix-in-docker is a real pattern — e.g. flake-built image layers — but adds
  complexity; probably keep the distroless Dockerfile as-is and let the flake
  own the dev/CI toolchain).
- **SECURITY.md: done (2026-07-03)** — copied from bluesky-social/atproto
  (reporting to security@bsky.app, no public issues, 3-business-day ack;
  CONTRIBUTORS clause dropped as this repo has no CONTRIBUTORS.md).
- Still open: ENOSPC posture (crash-loud vs read-only degrade — recommendation
  remains crash-loud, same class as fsync failure).

---

## 4. Agent-facing documentation

### State

The durable set — AGENTS.md + docs/README.md + specs/oracle.md + the failure
diary — is ~100 KB and healthy. Package doc coverage is the repo's strongest
layer: 19/22 internal packages have real doc.go contracts (role, concurrency,
error handling). The problem is concentrated:

- **`specs/notes/` is 56 files / 2.3 MB, write-once, unindexed.** Git shows 1–2
  commits per file. No status signal — an agent cannot tell that
  `2026-05-14-segment-file-format.md` describes a pre-sealing embryo while
  `2026-07-01-timestamp-import-design.md` is near-current. 23× the size of the
  durable set, undifferentiated.
- **The highest-value knowledge category exists nowhere shared:**
  accepted-limitation decisions and hard-won lessons live only in one agent's
  private per-machine memory — e.g. the single-event net-new-DID loss window
  Jim explicitly declined to harden, the ReadRow quoted-newline hole, "grep all
  cancellation-classifier copies," pebble Get-after-Close panics,
  mutation-campaign-vs-dirty-tree. A future agent *will* "fix" an accepted
  limitation. Proof of rot: the old `-jetstream-v2` memory directory is already
  orphaned by a path rename.
- AGENTS.md's repo-layout tree is missing 10 of 22 internal packages
  (`manifest`, `crashpoint`, `tombstone`, `timestamp`, `importer`, `xrpcapi`,
  `repoexport`, `jetstreamd`, `client`, `format`).
- `internal/oracle` — the most convention-laden package — has **no package
  doc**; "bubble" is defined nowhere, not even specs/oracle.md. Also missing:
  `internal/ingest/live`, `internal/simulator` top-level; `backfill` and
  `orchestrator` use per-file `// Package X: foo.go...` comments (godoc picks
  one arbitrarily).
- Invariants are good but scattered: docs §2 list, seal-immutability only in
  `segment/doc.go`, block-topology-across-generations at docs line ~357, the
  crash-loud/never-crash split in AGENTS.md.

### Recommended minimal set (value ÷ maintenance-cost order)

1. **`specs/notes/README.md` index + status convention** (~1 hr, ~zero
   maintenance). One line per note: date, title, status
   (`active` / `landed — superseded by docs §N` / `abandoned`). Rule in
   AGENTS.md: mark a note `landed` in the PR that merges its work; durable facts
   move to docs/README.md or a doc.go. Don't delete notes — make staleness
   machine-readable.
2. **`specs/gotchas.md`** — accepted-limitations + lessons file, the
   generalization of the `specs/oracle/` diary pattern (~2 hrs to seed from
   memory). One bullet per entry with a code anchor. Entries are decisions
   ("considered and declined"), which don't go stale the way descriptions do.
   **Highest-value new doc** — it's the only knowledge category that currently
   has no shared home.
3. **Invariants hoist**: a "§0 Invariants" summary at the top of docs/README.md
   (seal immutability, fsync-then-pebble ordering, cursor inclusivity/seq-0,
   block topology across generations, crash-loud boundary), linked from
   AGENTS.md. Prefer hoisting over a separate file — no second copy to rot.
4. **Three missing doc.go files + style fix** (~1 hr, self-maintaining):
   `internal/oracle` (define *bubble*, observer/checker/driver roles),
   `internal/ingest/live`, `internal/simulator`; canonical doc.go for
   `backfill` and `orchestrator`.
5. **Glossary routing table in AGENTS.md** (~30 min): term → one sentence →
   authoritative source. segment, block, seal, generation, watermark, tombstone,
   merge phase, cutover, hot ring, cold reader, bubble, tier, mutant, bucket(er),
   manifest.
6. **Refresh AGENTS.md layout tree** (15 min) + testing decision table
   (~15 lines: change class → required just recipe, including the
   mutation-campaign-needs-clean-tree gotcha).
7. **Memory-promotion rule in AGENTS.md**: auto-memory is a scratchpad; any
   fact worth a second session goes to gotchas/diary/docs in the same PR.

**Explicitly not recommended:** a formal ADR directory (three working decision
sinks already exist: docs rationale prose, doc.go decision notes, design
notes — a fourth format adds filing overhead, and gotchas.md covers the one
uncovered class); a cross-package "flows" doc (highest-churn category — the
subscribe fan-out was rewritten wholesale; rely on doc.go seam descriptions).

---

## Synthesis: work-item shortlist (v2 — reordered per Jim's 2026-07-03 review)

Now-track, in rough priority order:

| # | Item | Section | Effort | Status/notes |
|---|---|---|---|---|
| 1 | Fix Dockerfile ldflags path + CI docker-build check | §0 | hours | fix-now (correctness, not release eng) |
| 2 | Close `.env` → `run-prod` flag leak | §0 | hours | fix-now |
| 3 | SECURITY.md copied from bluesky-social/atproto | §3 | — | **done 2026-07-03** |
| 4 | Docs bundle: notes index, gotchas.md, invariants hoist, 3 doc.go, glossary, tree refresh | §4 | ~1 day | approved |
| 5 | **Oracle P2: simulator adversarial-traffic modes** | §2b | days–weeks | **top oracle priority per Jim**; catalog in progress, then priority-ordered attack |
| 6 | Oracle P5: #183 over-drop mutant + m002 boundary-exact | §2 | ~1 day | approved |
| 7 | ENOSPC posture + disk-free gauge + oracle P4 segment I/O faults | §2/§3 | days | approved; crash-loud vs degrade still open |
| 8 | govulncheck + scheduled fuzz lane + committed corpora | §3 | ~1 day | approved |
| 9 | Oracle P3: random-kill loop + multi-source worlds | §2 | days | approved |
| 10 | Nix flake for local dev + CI | §3 | ~1 day | new item from Jim; Dockerfile stays non-nix (tentative) |
| 11 | Real-data corpus (as-is frames/CARs, no privacy gymnastics, no independent decoder) | §2 | ~1 day | approved with low expectations |
| 12 | Oracle P6 seed swarm; P8 soak (#33); benchstat lane; darwin compile check | §2/§3 | days | approved, after the above |

Dropped / deferred / closed:

- **Test deletion/consolidation (was items 4, 12): dropped** — leave tests
  alone; delete opportunistically during future refactors (§1 decision).
- **Release engineering (was item 5): deferred until pre-1.0 vetting** — the
  §3 writeup stays as the plan for when it's time.
- **Oracle P1 independent decoder: dropped** — atmos accepted as owned black
  box (§2 decision). Corpus survives as item 11 without the cross-check.
- **Oracle P7 v1 differential: closed** — already done via `cmd/compare`
  (5-minute prod run, no material differences).
- Per Jim from the start: user-facing docs, HA mode.
