# Oracle Mutation Campaign — Design

Date: 2026-06-12
Status: approved

## Purpose

Measure the bug-detection power of the oracle simulator tests
(`internal/oracle`) empirically. The oracle compares jetstream's durable output
against a seeded simulator world, but we wrote both the tests and the code
under test — so a green oracle might reflect shared assumptions rather than
real detection power. This campaign answers: **which realistic bugs does the
oracle catch, at which tier, and which escape?**

Origin: ORACLE_TODO.md, Tier 1, item 1.

## What this is (and is not)

- **Is:** a repeatable fixture — a committed catalog of hand-curated mutants
  (small synthetic bugs as patch files) plus a driver script that applies each
  one, runs the oracle tiers, and records kill/survive results.
- **Is not:** automated operator-level mutation testing (gremlins,
  go-mutesting). Those generate noisy, often-unrealistic mutants with weak
  mapping to production failure modes. Rejected for v1; possible v2 if the
  curated catalog proves too easy to kill.
- **Is not:** mutant composition (applying multiple mutants at once,
  higher-order mutants). Explicitly rejected: composing mutants destroys
  attribution (which bug did the oracle see?) and mutants can mask each other
  (compensating off-by-ones producing correct final state). The campaign
  measures the oracle's eye, one known bug at a time. Environmental
  composition (scale, fault injection, crash timing, seeds) is instead
  achieved by the kill-tier escalation and the optional seed sweep.

## Repo layout

```
testing/mutation/
  mutants/
    m001_<short_name>.patch     # self-contained: metadata header + git diff
    m002_...
  run.sh                        # driver (~80 lines of bash)
  RESULTS.md                    # scorecard, appended per campaign
```

Plus:

- a `mutation-campaign` recipe in the justfile (`just mutation-campaign` for
  all mutants, `just mutation-campaign m007` for one),
- one paragraph in AGENTS.md (Testing section) describing the campaign so
  future sessions/staff know it exists, when to run it, and that
  `testing/mutation/mutants/*.patch` are deliberate bugs — never to be
  applied outside the driver.

**Zero changes to production code.** No build tags, no injection hooks, no
mutation scaffolding in `internal/` or `segment/`. A reader of production
code never encounters this machinery.

## Mutant file format

One self-contained `.patch` file per mutant. `git apply` ignores text before
the `diff --git` header, so metadata lives in the same file:

```
mutant: m007_watermark_floor_off_by_one
target: internal/ingest/orchestrator/compaction_watermark.go
failure-mode: |
  On first startup, the compaction watermark floor is initialized to
  nextSeq instead of nextSeq-1. Models a classic inclusive/exclusive
  boundary mistake when initializing a cursor from a "next" value.
expected-detection: |
  assertCompacted should fail: CheckCompacted re-derives which rows must
  be gone at-or-below the watermark, and a row at seq=nextSeq survives
  uncompacted. May only fire on seeds where the boundary seq has a
  superseding event.
expected-tier: stress
tiers: default,stress          # optional; append ",restart" for crash-seam mutants

diff --git a/internal/ingest/orchestrator/compaction_watermark.go b/...
--- a/internal/ingest/orchestrator/compaction_watermark.go
+++ b/internal/ingest/orchestrator/compaction_watermark.go
@@ -37,5 +37,5 @@ func initCompactionWatermarkFloor(...)
 	if nextSeq == 0 {
 		return saveCompactionWatermark(s, 0)
 	}
-	return saveCompactionWatermark(s, nextSeq-1)
+	return saveCompactionWatermark(s, nextSeq)
 }
```

Rules for every mutant:

- **Exactly one logical edit.** Multi-edit mutants destroy attribution.
- **Must compile.** A build break is a catalog bug (`BUILD-BROKEN`), not data.
- **Must not crash.** The oracle trivially catches panics; we are testing
  detection of silent corruption and silent loss.
- **`failure-mode` is the realism justification.** Every mutant must map to a
  plausible production mistake a competent engineer could ship past code
  review, or it is rejected during catalog review.
- **`expected-tier` / `expected-detection` are predictions written before any
  campaign run.** The gap between prediction and result is a primary finding:
  a wrong prediction means our mental model of the oracle is off.

## Catalog composition (~18 mutants, v1)

| Subsystem | Count | Example mutation territory |
|---|---|---|
| `internal/ingest/orchestrator` | ~7 | merge cursor math, watermark boundaries, discovery/cleanup predicates, phase transitions |
| `segment` (write/seal/rewrite) | ~6 | flush ordering, seal boundary conditions, rewrite filtering, torn-tail handling |
| `internal/ingest/live` + `backfill` | ~5 | cursor resume math, reconnect state, event→segment.Event field mapping, drop paths |

The subscribe/replay path is deliberately out of scope for v1: the oracle
reads segment files off disk, so replay-path mutants are expected to survive
~100% (a known blind spot already documented in ORACLE_TODO.md). Adding 2-3
replay mutants to *document* that gap with evidence is a candidate for v2.

### Population process (the over-fitting guard)

1. **Cold-read proposals.** Three independent subagents each review one
   target package with the prompt: "propose 6-8 realistic single-edit bugs a
   competent engineer could ship past code review." They see production code
   only — **not** the oracle internals — so proposals are not biased toward
   what the oracle is known to check.
2. **Adversarial filter.** Jim + assistant review proposals for realism,
   de-duplicate, and select the catalog. Borderline rejections get a recorded
   reason.
3. **Predictions.** Each accepted mutant gets its `failure-mode`,
   `expected-tier`, and `expected-detection` written before the first run.

## Driver (`run.sh`)

Per-mutant flow:

1. Refuse to start unless `git status` is clean.
2. `git apply mutants/mNNN_*.patch` (failure → record `STALE`, continue).
3. `go build ./...` (failure → record `BUILD-BROKEN`, revert, continue).
4. Escalate through kill tiers until one fails:
   - **Tier 1 (default):**
     `go test ./internal/oracle -run TestOracle_DefaultLifecycle -count=1 -short` (~1.5s)
   - **Tier 2 (stress):**
     `JETSTREAM_ORACLE_MODE=stress go test ./internal/oracle -run TestOracle_DefaultLifecycle -count=1` (~17s)
   - **Tier 3 (restart):**
     `go test ./internal/oracle -run TestOracle_Restart -count=1` — only for
     mutants whose `tiers:` metadata opts in (crash-seam mutants).
5. `git apply -R` to revert — also wired into a shell `trap` so an
   interrupted run (Ctrl-C, timeout) still leaves the tree clean.
6. Record result: `KILLED@tier1|2|3`, `SURVIVED`, `STALE`, or `BUILD-BROKEN`,
   plus the failing assertion's message when killed.

Full-campaign worst case: 18 × ~20s ≈ 6-7 minutes on a dev machine.

**Seed sweep (manual, survivors only).** A mutant that survives all its tiers
on the default seed can be re-run with `run.sh mNNN --seeds N` (stress mode,
N additional random seeds, each printed for repro) before being recorded as a
true escape. This catches probabilistic detection — bugs the oracle can see
only on seeds where the right event pattern occurs — without complicating the
default campaign. Result class: `KILLED@stress(k/N seeds)`.

`STALE` handling: a patch that no longer applies is a loud per-mutant failure
in the report, not a campaign abort. Staleness is signal — the mutated logic
changed, so the mutant needs re-review and refresh.

## RESULTS.md scorecard

Appended per campaign (never overwritten), so detection-power history is
visible across refactors. Each campaign section contains:

1. **Metadata:** date, commit SHA, seed(s), total runtime.
2. **Result table:** one row per mutant — id, subsystem, expected tier,
   actual result, seed, one-line note on how it was detected (which assertion
   fired).
3. **Escapes:** every `SURVIVED` mutant analyzed — what the oracle would need
   in order to catch it, with a written disposition: *fix the oracle* (file
   it) or *accepted blind spot* (document in ORACLE_TODO.md).
4. **Prediction misses:** mutants killed at a different tier or via a
   different assertion than predicted — what that corrects in our model of
   the oracle.

## Success criteria

- Every cataloged mutant has a definitive result (no `STALE`/`BUILD-BROKEN`
  left unresolved at campaign end).
- Every escape has a written disposition.
- The campaign is re-runnable by a future engineer from the justfile recipe
  and the AGENTS.md paragraph alone.

## Deliverables checklist

- [ ] `testing/mutation/run.sh` driver
- [ ] `testing/mutation/mutants/` catalog (~18 mutants, populated via the
      cold-read + adversarial-filter process)
- [ ] justfile `mutation-campaign` recipe
- [ ] AGENTS.md paragraph (Testing section)
- [ ] First campaign executed; `testing/mutation/RESULTS.md` committed
- [ ] Escapes dispositioned (oracle fixes filed or blind spots documented in
      ORACLE_TODO.md)

## Rejected alternatives

- **In-tree mutation hooks (build tags / injection points):** permanent
  mutation scaffolding interleaved with production logic — exactly the
  confusing-to-read-later outcome this design avoids; injection seams can
  also perturb inlining/ordering.
- **Fully ephemeral campaigns (no committed catalog):** zero footprint but
  not reproducible; cannot re-run "the same campaign" after a refactor to
  detect regressions in oracle power; nothing for future staff to extend.
- **Automated mutation tooling and mutant composition:** see "What this is
  (and is not)" above.
