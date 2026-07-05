# Oracle testing improvements — plan of record (2026-07-03)

Status: **active planning doc.** Split out of
`2026-07-02-tech-debt-paydown-ideation.md` (§2/§2b) after Jim's review; the
parent doc holds the full exploration evidence and the decision trail. This doc
is the forward-looking plan only. No issues filed yet.

Goal (Jim): the oracle tests should be "exceptionally well done — no stone
unturned (within reason)." Aspiration is TigerBeetle/FDB-style rigor, adapted
to what Go allows.

## Settled decisions (do not relitigate)

- **atmos is an accepted black box.** No second CBOR decoder, no independent
  reimplementation of MST/CAR/commit verification. Atmos is owned
  (jcalabro/atmos), has done repeated full-network backfills, and is itself
  rigorously tested; improvements to its testing land in atmos, not here.
  The symmetric-atmos-bug risk is accepted.
- **v1 differential testing is closed.** `cmd/compare` already did this
  (5-minute production run, no material differences). No further investment.
- **Real-data corpus: yes, cheap, low expectations.** Store real frames/CARs
  as-is (atproto data is public; no hashing/normalization). Happy-path
  regression value only.
- **Simulator adversarial-traffic fidelity is the top priority** — the
  catalog below is the centerpiece of this plan.

## Current-state summary (evidence in parent doc §2)

Confidence is **strong** where an independent model + mutation kills back the
assertion: final-state integrity (`GroundTruthFromWorld` walks the simulator's
own MST), event-log multiset completeness, independent compaction drop-rule
reimplementation, partb cutover, enumerated crash seams. Anti-vacuity
discipline is pervasive and good.

Confidence is **weak** exactly where the mutation survivors point:

| Survivor | Blind spot | Addressed by |
|---|---|---|
| m002 | watermark boundary tested probabilistically (4/5 seeds) | Work item 2 |
| m003 | restart harness can only build single-source-segment worlds | Work item 5 |
| m009 | segment checksum write/verify share one function; goldens pin bytes, not independently-computed checksums | Work item 6 (partial; independent-decoder mitigation declined) |
| m013/m014 | simulator traffic never takes the mutated path | Work item 1 |
| m015 | oracle reads blocks sequentially, never the footer index/blooms the production planner uses | Work item 6 |

Determinism status: the synctest-bubble tier is highly deterministic (fake
clock, pipe listeners, salted PCGs); the data plane's *interleaving* remains
scheduler-decided (trace allowlist excludes worker-completion-order kinds).
Restart tier is least deterministic (real subprocesses/sockets/disk) — both
diary flakes came from there.

## Work items, priority order

### 1. Simulator adversarial-traffic modes (THE priority)

Full catalog below in §Catalog. Execution shape: **each P0/P1 item is one
issue** bundling (simulator generator + contract decision + oracle assertion +
anti-vacuity check). Every case defines, per `specs/oracle.md:355`: archive
survivors? drop? advance cursor? which metric? bounded log? keep running?
Every mode gets an archived-≥1-of-each-kind anti-vacuity assert.

After P0+P1 land: re-run the mutation campaign and **add mutants against the
newly-live paths** (e.g. break tombstone status exactness; break the
survivors-archived contract) so new coverage is gate-enforced.

### 2. Pay two named check debts (small, do early)

- Re-derive the #100 over-drop mutant (#183) — nothing currently proves the
  steady-state over-drop recorder can go red.
- Make m002's watermark boundary check exact: pin a tombstone/survivor pair
  *at* the boundary (partb-style controlled scenario) instead of relying on
  seed luck.

### 3. Segment-file I/O fault layer

The fault seam stops at pebble (`store/fault.go`). Wrap segment
write/fsync/rename behind a crash-injector-style seam: short-write,
fsync-error, ENOSPC, truncate-at-offset sweeps; assert fail-loud + no silent
corruption via `ObserveSegments`.

ENOSPC posture is DECIDED (jrc 2026-07-03, detail in the maintainability
doc): **crash-loud** with a clear, prominent error message — same class as
fsync failure; no read-only degraded mode for now. Jim wants this tested
thoroughly at every level: unit/integration on the writer error path, oracle
fault injection here proving injected ENOSPC → clean crash → clean recovery
via the torn-tail walk (no corrupt tail), and coverage in the advanced tiers
(crashpoint enumeration, a mutant that swallows the write error — the m006
storefault precedent shows this class pays for itself).

### 4. Real-data corpus tier

Capture a few hundred real firehose frames + a handful of real getRepo CARs
(diverse PDS implementations), commit as testdata, run through decode→archive
regression tests. As-is storage, no privacy gymnastics. Low expected yield;
cheap insurance against decode regressions.

### 5. Deterministic multi-source restart kills

Nightly/mutation-gated seeded SIGKILL at deterministic crashpoint ordinals,
over a world configured to produce **multiple merge source segments** (the
missing scenario that makes m003 unkillable). Random wall-clock offsets are
deliberately out of scope for this mutation-campaign item: they are harder to
reproduce and not needed to expose the merge cursor off-by-one. The assertion
bundle already exists; new machinery is the multi-source restart fixture and
the precise duplicate/no-reprocess assertion.

### 6. Footer-index/bloom read-path oracle coverage (m015 class)

Make the oracle consult the same footer collection-count index and per-block
blooms the production planner and cursor reader use — either by routing an
oracle observer through the production read path AND the sequential path and
diffing, or by adding an independent index walker. Also: pin an
independently-computed (or known-good captured) checksum for a committed
segment fixture so a symmetric range bug in `xxh3HeaderFooter` is at least
pinned against drift.

### 7. In-bubble seed swarm

Process-per-seed driver (one-bubble-per-process constraint) — bubble lifecycle
runs <1s, so hundreds of deterministic-input lifecycles per CI hour vs today's
10–20 per 6h sweep. Then coverage-guided seed retention: keep seeds whose
traces hit rare kinds (CAR truncation on hot DID, sync divergence early,
disconnect near block boundary). The trace JSONL already contains the features
to select on.

### 8. Soak tier (#33)

Hours-long steady→compact→restart→assert loop watching goroutine counts,
tombstone-set size, manifest/cache growth, watermark monotonicity. Scheduled,
not developer-loop. Do after items 1–5; it finds slower bugs.

---

## Catalog: adversarial-traffic modes (work item 1 detail)

Grounded in the 2026-07-03 code survey of the full upstream-input surface
(30 input classes; full table with file:line in parent doc §2b — key
anchors repeated here).

Context: jetstream's own surface starts at `streaming.Event`
(`internal/ingest/live/events.go:33`) and `*repo.Repo`
(`internal/ingest/backfill/handler.go:72`); atmos owns frame decode, gap
detection, verification, reconnect. Segment column limits
(`segment/block.go:18-38`): DID ≤65535, Collection/Rkey/Rev ≤255, Payload
≤4 GiB. Today's fault surface is exactly three knobs (getRepo HTTP status,
getRepo CAR truncation, disconnect thresholds) — all firehose-frame-level
adversity is absent, and the world generates only polite traffic.

### P0 — silently-wrong and untested at ANY level

1. **Non-TID / regressing revs.** No shape/monotonicity check at ingest; rev
   ordering drives merge filter (`orchestrator/merge_filter.go:40-55`),
   stale-resync guard (`live/consumer.go:199-220`), syncstate `PromoteChain`.
   Malformed rev → silently wrong compaction/merge decisions.
   Simulator: non-TID, empty, regressing, future revs.
   DECIDED (jrc 2026-07-03): **validate and drop.** Revs must be valid TID
   strings per the atproto repository spec
   (https://atproto.com/specs/repository#commit-objects). On failure: drop the
   record, increment a **shared "invalid commit"-style metric** (a labeled
   reason on one counter, not a new isolated metric per error class), no
   stdout/stderr logging, set trace status if a trace is active (fine to skip
   if not). This defensive posture applies to all invalid/adversarial upstream
   input.
2. **Arbitrary-byte / invalid-UTF-8 rkeys.** Wire commits never re-validated
   (`StrictValidation` never set). Archive is byte-faithful; `json.Marshal`
   at delivery substitutes U+FFFD → client delete-by-rkey silently never
   matches. Simulator: null bytes, invalid UTF-8, emoji, RTL rkeys.
   DECIDED (jrc 2026-07-03): **validate per-op via
   `atmos.ParseRepoPath`** (splits `collection/rkey`, validates NSID +
   RecordKey against the spec: empty/512-byte/`.`,`..`/charset
   `[A-Za-z0-9._:~-]`). On failure: drop the op (keep well-formed siblings,
   matching the missing-block per-op precedent), shared invalid-commit metric
   with a distinct reason label, no logging, trace status if active. Two gates
   stay metric-distinct: spec-invalid (this) vs spec-valid-but-unrepresentable
   (rkey 256–512 > our 255 column → existing `ErrFieldTooLong` reason;
   accepted limitation → gotchas.md). Delivery-side U+FFFD problem disappears:
   validated rkeys are pure ASCII.
3. **`$`-prefixed wire collections.** Sentinel safety relies on `$` being
   invalid in an NSID, but nothing rejects it on the wire
   (`events.go:89,155`) → potential sentinel shadowing in indexing/planning.
   Simulator: `$`-prefix, at-limit/over-limit, empty, Unicode collection names.
   DECIDED (jrc 2026-07-03): **subsumed by decision 2** — `ParseRepoPath`'s
   NSID validation rejects `$`-prefixed/malformed collections. Same drop-op +
   shared-metric contract (reason distinguishes invalid collection vs invalid
   rkey per `ParseRepoPath`'s error). Simulator cases above still land to
   prove the gate end-to-end.
4. **Seq duplicates / regressions from relay.** atmos gap check is
   forward-only; dups/regressions re-archive under new seqs. Correct today by
   idempotent folding — by accident, untested. Simulator: duplicate-N,
   regress-to-K replay modes. Assert invariants hold + storage bloat bounded.
   Fold in: split the shared gap/decode-error metric
   (`consumer.go:367-376`) so relay data loss is distinguishable.

### P1 — dead paths with tombstone/erasure blast radius

5. **#identity frames.** No generator in `world/firehose.go:16-21`;
   `KindIdentity` ingest is dead under the oracle; oracle expected-eventlog
   already decodes them (`expected_eventlog.go:93-98`). Cheapest P1.
6. **Non-deleted #account statuses** (takendown/suspended/deactivated/unknown
   + reactivation). Tombstoning depends on exact-`"deleted"` matching
   (`tombstone.go:238-244`) — breakage = compaction erases live records.
   Simulator: full status lifecycle; oracle asserts non-deleted never folds as
   tombstone. Also cover getRepo `RepoTakendown/Suspended/Deactivated` →
   terminal StatusUnavailable (`diagnostics.go:291-302`).
7. **Near-limit / over-limit fields through the oracle.** Drop+metric contract
   unit-tested only; world text sits far below limits. Simulator: rkeys at
   254/255/256, collections at limit, huge payloads. Assert drop+metric AND
   surrounding survivors archived. Document in gotchas.md: spec-valid rkeys
   >255 bytes (MST allows ~1023) are dropped by design.

### P2 — frame-level adversity

8. **Unknown frame types** — decode-error metric + continue is unit-tested;
   the documented unknown-behind-watermark loss window
   (`consumer.go:397-410`) is unobserved by any oracle scenario.
9. **Malformed CBOR frames** — atmos decode is fuzzed/panic-free, but garbage
   frames have never run *through the live consumer* end-to-end.
10. **Oversized frames** — 2 MiB read limit → close → reconnect; persistent
    oversize = reconnect loop. Simulator: one-shot (assert clean reconnect,
    no loss around it) + persistent mode (assert bounded + visible).
11. **Partial firehose-#commit CARs** — per-op drop with survivors is well
    unit-tested; simulator can only truncate getRepo CARs today. Strip N
    blocks from a live commit's CAR; assert survivors +
    `dropped_ops_missing_block_total`.

### P3 — backfill-side adversity

12. **getRepo exotic responses**: RepoNotFound (incl. non-canonical message
    variant), 429 + RetryAfter (incl. hostile far-future reset — clamp at
    `retry.go:377-394`), redirects.
13. **listRepos pagination faults**: duplicate pages, cursor loops, shrinking
    pages (asserts the idempotent-download + net-new-enqueue recovery).
14. **DID-doc weirdness**: missing PDS endpoint, malformed handle, resolution
    failure under load (PLC handler is always-well-formed today,
    `simulator/http/plc.go:38-72`).
15. **Net-new DID mid-run**: world support for an account added after
    bootstrap that listRepos omits; asserts the LiveEnqueuer → pending →
    backfill path end-to-end (unit coverage is strong, oracle absent).

### P4 — decide-and-document (not simulator work)

16. **tooBig commits**: `TooBig` exists in the atmos generated type; nothing
    reads it anywhere. Decide extinct-upstream (→ gotchas.md) vs handle
    explicitly (→ simulator mode).
17. **Huge single-commit repos**: soak-tier concern (work item 8), not
    adversarial traffic.

## Contract decisions — all resolved (jrc 2026-07-03)

1. **Revs: validate and drop.** Must be valid TIDs per the atproto repository
   spec. Drop record + shared invalid-commit metric (reason label), no
   stdout/stderr logging, trace status if active.
2. **Rkeys + collections: validate per-op via `atmos.ParseRepoPath`** (NSID +
   RecordKey spec validation in one call). Drop op, keep siblings, same shared
   metric with distinct reasons.
3. **`$`-prefixed collections: subsumed by #2** (NSID validation rejects them).

Shared observability pattern for all invalid/adversarial upstream input: ONE
labeled invalid-commit-style counter with per-reason labels — not isolated
per-error metrics; metrics over logs; trace status when a trace is active.
