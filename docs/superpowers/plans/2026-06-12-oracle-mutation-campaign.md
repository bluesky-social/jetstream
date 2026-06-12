# Oracle Mutation Campaign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a repeatable mutation campaign that measures the oracle tests' bug-detection power: a catalog of curated single-edit bug patches, a driver that applies each one and checks whether the oracle kills it, and a committed scorecard.

**Architecture:** Mutants are plain `git diff` patch files with a metadata header, stored in `testing/mutation/mutants/`. A bash driver (`run.sh`) applies one at a time to the working tree, escalates through oracle kill tiers (default → stress → restart), reverts, and prints a markdown scorecard. Zero changes to production code. Spec: `docs/superpowers/specs/2026-06-12-oracle-mutation-campaign-design.md`.

**Tech Stack:** bash, git apply, the existing `internal/oracle` test suite, justfile.

**Timing context (measured on dev machine):** tier 1 (default oracle) ~1.5s; tier 2 (stress oracle) ~17s. Full 18-mutant campaign ≈ 6-7 min.

---

### Task 1: Scaffold + canary mutant m001

The canary is a mutant we are highly confident the oracle kills at tier 1. It exists to validate the driver's kill path before the real catalog lands.

**Files:**
- Create: `testing/mutation/mutants/m001_delete_mapped_to_update.patch`

Patches are authored by editing the production file, capturing `git diff`, reverting, then prepending the metadata header. This guarantees hunk offsets are always correct.

- [ ] **Step 1: Make the mutant edit in production code (temporarily)**

In `internal/ingest/live/events.go`, function `actionKind` (around line 193), change the `ActionDelete` arm:

```go
	case streaming.ActionDelete:
		return segment.KindDelete, nil
```

to:

```go
	case streaming.ActionDelete:
		return segment.KindUpdate, nil
```

- [ ] **Step 2: Capture the diff and revert**

```bash
mkdir -p testing/mutation/mutants
git diff > testing/mutation/mutants/m001_delete_mapped_to_update.patch
git checkout -- internal/ingest/live/events.go
git diff --quiet   # expect: exits 0 (tree clean again)
```

- [ ] **Step 3: Prepend the metadata header**

Edit `testing/mutation/mutants/m001_delete_mapped_to_update.patch` so the file begins with this header, followed by a blank line, followed by the captured `diff --git ...` content (`git apply` ignores everything before the diff header):

```
mutant: m001_delete_mapped_to_update
target: internal/ingest/live/events.go
failure-mode: |
  The live-ingest action->kind switch maps ActionDelete to KindUpdate.
  Models a copy-paste error in a switch mapping — the adjacent arms all
  return similar-looking constants and review eyes glaze over. Deletes are
  archived as updates, so a replaying consumer never removes the record.
expected-detection: |
  Compare should fail with "oracle: extra <did> <collection>/<rkey>":
  ground truth (simulator MST) no longer contains the deleted record, but
  the reconstructed model retains it as an update. Default mode generates
  ~10% deletes across 400 live events, so tier 1 should see many.
expected-tier: default
tiers: default,stress
```

- [ ] **Step 4: Verify the patch round-trips**

```bash
git apply --check testing/mutation/mutants/m001_delete_mapped_to_update.patch   # exits 0
git apply testing/mutation/mutants/m001_delete_mapped_to_update.patch
git apply -R testing/mutation/mutants/m001_delete_mapped_to_update.patch
git diff --quiet   # exits 0
```

- [ ] **Step 5: Verify the canary is killed by hand (driver doesn't exist yet)**

```bash
git apply testing/mutation/mutants/m001_delete_mapped_to_update.patch
go test ./internal/oracle -run 'TestOracle_DefaultLifecycle$' -count=1 -short
git apply -R testing/mutation/mutants/m001_delete_mapped_to_update.patch
```

Expected: test FAILS with an `oracle: extra ...` (or payload mismatch) message. If it passes, stop — the canary premise is wrong and the mutant or our oracle understanding needs investigation before building anything else.

- [ ] **Step 6: Commit**

```bash
git add testing/mutation/mutants/m001_delete_mapped_to_update.patch
git commit -m "test(mutation): scaffold mutant catalog with canary m001

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: Seed mutant m002 (watermark off-by-one)

A second seed mutant with a *stress*-tier prediction, taken from the spec's worked example. It may legitimately survive — that's data, and it exercises the survivor/seed-sweep path later.

**Files:**
- Create: `testing/mutation/mutants/m002_watermark_floor_off_by_one.patch`

- [ ] **Step 1: Make the edit temporarily**

In `internal/ingest/orchestrator/compaction_watermark.go`, function `initCompactionWatermarkFloor` (around line 37), change:

```go
	return saveCompactionWatermark(s, nextSeq-1)
```

to:

```go
	return saveCompactionWatermark(s, nextSeq)
```

- [ ] **Step 2: Capture, revert, prepend header**

```bash
git diff > testing/mutation/mutants/m002_watermark_floor_off_by_one.patch
git checkout -- internal/ingest/orchestrator/compaction_watermark.go
```

Header to prepend:

```
mutant: m002_watermark_floor_off_by_one
target: internal/ingest/orchestrator/compaction_watermark.go
failure-mode: |
  On first startup, the compaction watermark floor is initialized to
  nextSeq instead of nextSeq-1. The watermark claims a seq that was never
  compacted is below the compaction line. Classic inclusive/exclusive
  boundary mistake when initializing a cursor from a "next" value.
expected-detection: |
  assertCompacted should fail: CheckCompacted re-derives which rows must
  be gone at-or-below the watermark, and a row at the boundary seq can
  survive uncompacted. Likely seed-dependent — only fires when the
  boundary seq carries a superseding event. If it survives the default
  seed, run the seed sweep before recording it as an escape.
expected-tier: stress
tiers: default,stress
```

- [ ] **Step 3: Verify round-trip and commit**

```bash
git apply --check testing/mutation/mutants/m002_watermark_floor_off_by_one.patch
git add testing/mutation/mutants/m002_watermark_floor_off_by_one.patch
git commit -m "test(mutation): add seed mutant m002 (watermark floor off-by-one)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: The driver (`run.sh`)

**Files:**
- Create: `testing/mutation/run.sh`

- [ ] **Step 1: Write the driver**

```bash
#!/usr/bin/env bash
# Oracle mutation campaign driver.
# See docs/superpowers/specs/2026-06-12-oracle-mutation-campaign-design.md
#
# Usage:
#   testing/mutation/run.sh                 # run every mutant
#   testing/mutation/run.sh m007            # run one mutant (filename prefix match)
#   testing/mutation/run.sh m007 --seeds 5  # stress sweep over 5 random seeds
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
MUTANTS_DIR="testing/mutation/mutants"

ONLY=""
SEEDS=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --seeds) SEEDS="$2"; shift 2 ;;
        *) ONLY="$1"; shift ;;
    esac
done

if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "error: uncommitted changes to tracked files; commit or stash first" >&2
    exit 1
fi
if [[ "$SEEDS" -gt 0 && -z "$ONLY" ]]; then
    echo "error: --seeds requires a single mutant id" >&2
    exit 1
fi

LOG_ROOT="$(mktemp -d -t mutation-campaign-XXXXXX)"

# Safety net: any exit path (including Ctrl-C) reverts an applied mutant.
CURRENT_PATCH=""
cleanup() {
    if [[ -n "$CURRENT_PATCH" ]]; then
        git apply -R "$CURRENT_PATCH" 2>/dev/null || true
        CURRENT_PATCH=""
    fi
}
trap cleanup EXIT INT TERM

declare -a ROWS=()

for patch in "$MUTANTS_DIR"/*.patch; do
    id="$(basename "$patch" .patch)"
    if [[ -n "$ONLY" && "$id" != "$ONLY"* ]]; then
        continue
    fi

    tiers="$(sed -n 's/^tiers: *//p' "$patch" | head -n1)"
    tiers="${tiers:-default,stress}"
    echo "=== $id (tiers: $tiers) ==="

    if ! git apply --check "$patch" 2>"$LOG_ROOT/$id.apply.log"; then
        echo "    STALE (patch no longer applies — refresh needed)"
        ROWS+=("| $id | STALE | patch no longer applies — refresh needed |")
        continue
    fi
    git apply "$patch"
    CURRENT_PATCH="$patch"

    if ! go build ./... >"$LOG_ROOT/$id.build.log" 2>&1; then
        echo "    BUILD-BROKEN (mutant does not compile — refresh needed)"
        ROWS+=("| $id | BUILD-BROKEN | mutant does not compile — refresh needed |")
        git apply -R "$patch"
        CURRENT_PATCH=""
        continue
    fi

    result=""
    if [[ "$SEEDS" -gt 0 ]]; then
        kills=0
        for i in $(seq 1 "$SEEDS"); do
            seed="$(od -An -N8 -tu8 /dev/urandom | tr -d ' ')"
            echo "    seed sweep $i/$SEEDS seed=$seed"
            if ! env JETSTREAM_ORACLE_MODE=stress JETSTREAM_ORACLE_SEED="$seed" \
                go test ./internal/oracle -run 'TestOracle_DefaultLifecycle$' \
                -count=1 -timeout 30m >"$LOG_ROOT/$id.seed$i.log" 2>&1; then
                kills=$((kills + 1))
                echo "        KILLED seed=$seed"
            fi
        done
        if [[ "$kills" -gt 0 ]]; then
            result="| $id | KILLED@stress($kills/$SEEDS seeds) | flaky detection — see logs |"
        else
            result="| $id | SURVIVED($SEEDS seeds) | true escape candidate |"
        fi
    else
        for tier in ${tiers//,/ }; do
            case "$tier" in
                default)
                    cmd=(go test ./internal/oracle -run 'TestOracle_DefaultLifecycle$' -count=1 -short) ;;
                stress)
                    cmd=(env JETSTREAM_ORACLE_MODE=stress go test ./internal/oracle
                         -run 'TestOracle_DefaultLifecycle$' -count=1 -timeout 30m) ;;
                restart)
                    cmd=(go test ./internal/oracle -run 'TestOracle_RestartCrashPointsDoNotLoseRecords$'
                         -count=1 -timeout 10m) ;;
                *)
                    echo "error: unknown tier '$tier' in $id" >&2
                    exit 1 ;;
            esac
            echo "    tier=$tier ..."
            if ! "${cmd[@]}" >"$LOG_ROOT/$id.$tier.log" 2>&1; then
                note="$(grep -m1 -o 'oracle: [^"]*' "$LOG_ROOT/$id.$tier.log" | head -c 140 || true)"
                result="| $id | KILLED@$tier | ${note:-see log} |"
                echo "    KILLED@$tier"
                break
            fi
        done
        if [[ -z "$result" ]]; then
            result="| $id | SURVIVED | escape — analyze and disposition |"
            echo "    SURVIVED"
        fi
    fi

    git apply -R "$patch"
    CURRENT_PATCH=""
    ROWS+=("$result")
done

if [[ ${#ROWS[@]} -eq 0 ]]; then
    echo "error: no mutants matched '$ONLY'" >&2
    exit 1
fi

echo
echo "| mutant | result | note |"
echo "|---|---|---|"
printf '%s\n' "${ROWS[@]}"
echo
echo "logs: $LOG_ROOT"
```

```bash
chmod +x testing/mutation/run.sh
```

- [ ] **Step 2: Verify the clean-tree guard**

```bash
echo "// scratch" >> internal/oracle/model.go
testing/mutation/run.sh m001 ; echo "exit=$?"
git checkout -- internal/oracle/model.go
```

Expected: `error: uncommitted changes to tracked files...`, `exit=1`.

- [ ] **Step 3: Verify the kill path with the canary**

```bash
testing/mutation/run.sh m001
git status --porcelain   # expect: only untracked files (no modified tracked files)
```

Expected output includes `KILLED@default` for m001 and a final markdown table. Confirm a real `oracle: ...` note appears in the table row (not just "see log").

- [ ] **Step 4: Run m002 (either result is fine; record what happens)**

```bash
testing/mutation/run.sh m002
```

Expected: `KILLED@default`, `KILLED@stress`, or `SURVIVED` — note the result; if SURVIVED, also exercise the sweep: `testing/mutation/run.sh m002 --seeds 3`.

- [ ] **Step 5: Verify STALE and BUILD-BROKEN handling with throwaway patches**

```bash
# STALE: a patch whose context doesn't exist
cat > testing/mutation/mutants/m999_stale_test.patch <<'EOF'
mutant: m999_stale_test
tiers: default

diff --git a/internal/oracle/model.go b/internal/oracle/model.go
--- a/internal/oracle/model.go
+++ b/internal/oracle/model.go
@@ -1,3 +1,3 @@
 package oracle
-// this line does not exist in the real file
+// mutated
EOF
testing/mutation/run.sh m999
rm testing/mutation/mutants/m999_stale_test.patch
```

Expected: row shows `STALE`, exit code 0, tree clean. Then the same for BUILD-BROKEN: author a temporary patch that renames a used identifier (capture via the edit/diff/revert workflow against any production file, e.g. rename `Reconstruct` to `Reconstructx` in `internal/oracle/reconstruct.go` — callers won't compile), run it, confirm `BUILD-BROKEN`, delete the patch, confirm `git status` clean.

- [ ] **Step 6: Commit**

```bash
git add testing/mutation/run.sh
git commit -m "test(mutation): add mutation campaign driver

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: justfile recipe + AGENTS.md paragraph

**Files:**
- Modify: `justfile` (append recipe near the other test recipes, after `oracle-sweep`)
- Modify: `AGENTS.md` (after the oracle paragraph that ends at line 55)

- [ ] **Step 1: Add the justfile recipe**

```make
# Runs the oracle mutation campaign: applies each curated mutant patch in
# testing/mutation/mutants one at a time and verifies the oracle kills it.
# Pass a mutant id to run one (e.g. `just mutation-campaign m007`), or
# `m007 --seeds 5` for a stress-mode seed sweep of a survivor. Scorecard
# lives in testing/mutation/RESULTS.md.
mutation-campaign *ARGS="":
    testing/mutation/run.sh {{ARGS}}
```

- [ ] **Step 2: Add the AGENTS.md paragraph**

Insert after the oracle test paragraph (the one describing `just oracle` / `just oracle-sweep`, ending around line 55):

```markdown
The oracle's bug-detection power is measured by a mutation campaign.
`testing/mutation/mutants/*.patch` are curated single-edit bugs ("mutants"),
each documented with the production failure mode it models and a prediction
of which oracle tier should catch it. `just mutation-campaign` applies them
one at a time and verifies the oracle kills them; the scorecard lives in
`testing/mutation/RESULTS.md`. Never apply these patches outside the driver,
and never "fix" production code to match a mutant — they are deliberate bugs.
Re-run the campaign after major changes to ingest, segment, or orchestrator
logic; a STALE result means the underlying code moved and the mutant needs
re-review and refresh.
```

- [ ] **Step 3: Verify and commit**

```bash
just mutation-campaign m001   # expect: same KILLED@default result as Task 3
git add justfile AGENTS.md
git commit -m "test(mutation): wire campaign into justfile and AGENTS.md

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: Populate the catalog (cold-read proposals + adversarial filter)

This task is interactive — it requires Jim's judgment at the filter step.

**Files:**
- Create: `testing/mutation/mutants/m003_*.patch` … `m0NN_*.patch` (~16 more mutants)

- [ ] **Step 1: Dispatch three cold-read subagents in parallel**

One per subsystem, with this prompt shape (substitute the package list):

> Read the production code in `<package(s)>` of jetstream-v2 (an atproto
> network cache). Propose 6-8 realistic SINGLE-EDIT bugs a competent
> engineer could ship past code review: off-by-ones in cursor/boundary
> math, inverted or weakened predicates, swapped constants in mappings,
> dropped error-path side effects, wrong field copied in a conversion,
> skipped flush/ordering steps. For each: the exact file/function/line,
> the exact edit (before/after code), and why it's a plausible real-world
> mistake. Rules: must compile; must NOT panic or crash (we want silent
> corruption/loss); one logical edit only. Do NOT look at internal/oracle
> or any *_test.go — propose bugs from the production code alone.

Subagent A: `internal/ingest/orchestrator`. Subagent B: `segment` (writer/seal/rewrite paths). Subagent C: `internal/ingest/live` + `internal/ingest/backfill`.

- [ ] **Step 2: Adversarial filter with Jim**

Present the combined proposals (expect ~20). For each: keep/reject on realism, de-duplicate overlapping proposals, record one-line rejection reasons for borderline cases (these go into RESULTS.md campaign metadata). Target ~16 accepted (≈7 orchestrator / ≈6 segment / ≈5 live+backfill, per spec).

- [ ] **Step 3: Author each accepted mutant**

For each accepted proposal, follow the Task 1 workflow exactly: edit production file → `git diff > testing/mutation/mutants/mNNN_<short_name>.patch` → `git checkout -- <file>` → prepend metadata header. **Write `expected-tier` and `expected-detection` before running anything.** Crash-seam mutants (those interacting with recovery, e.g. merge/seal ordering) get `tiers: default,stress,restart`. Verify each with `git apply --check`.

- [ ] **Step 4: Commit the catalog**

```bash
git add testing/mutation/mutants/
git commit -m "test(mutation): populate mutant catalog (~18 mutants, 3 subsystems)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: First campaign + RESULTS.md

**Files:**
- Create: `testing/mutation/RESULTS.md`
- Modify: `ORACLE_TODO.md` (check off the mutation-campaign item; add any newly discovered blind spots)

- [ ] **Step 1: Run the full campaign**

```bash
just mutation-campaign 2>&1 | tee /tmp/campaign-run.log
git status --porcelain   # confirm no modified tracked files afterward
```

Expected: a result row for every mutant; ~6-8 minutes. If any row is STALE or BUILD-BROKEN, fix that mutant (Task 1 workflow) and re-run just that mutant before proceeding — the spec's success criteria require zero unresolved STALE/BUILD-BROKEN.

- [ ] **Step 2: Seed-sweep every survivor**

```bash
testing/mutation/run.sh <survivor-id> --seeds 5
```

Record per-survivor: `KILLED@stress(k/5 seeds)` (flaky detection) or `SURVIVED(5 seeds)` (true escape).

- [ ] **Step 3: Write RESULTS.md**

Structure (append a new section like this per campaign, never overwrite):

```markdown
# Oracle Mutation Campaign Results

## Campaign 2026-06-DD

- commit: <sha>
- default seed: 42; survivors swept with 5 random stress seeds (listed per row)
- total runtime: <minutes>
- catalog: NN mutants (orchestrator N, segment N, live/backfill N)
- filter notes: <one line per borderline rejection from Task 5 step 2>

| mutant | subsystem | expected | actual | note (assertion that fired) |
|---|---|---|---|---|
| m001_delete_mapped_to_update | live | default | KILLED@default | oracle: extra did:plc:... |
| ... | | | | |

### Escapes

For each SURVIVED mutant: what the oracle would need to catch it, and the
disposition — "fix the oracle: <issue/plan>" or "accepted blind spot:
documented in ORACLE_TODO.md".

### Prediction misses

For each mutant killed at a different tier or via a different assertion
than predicted: what that corrects in our model of the oracle.
```

- [ ] **Step 4: Disposition escapes and update ORACLE_TODO.md**

Every escape gets a written disposition in RESULTS.md. Check off the Tier 1 mutation-campaign item in ORACLE_TODO.md and add any newly discovered blind spots to its gap list.

- [ ] **Step 5: Commit**

```bash
git add testing/mutation/RESULTS.md ORACLE_TODO.md
git commit -m "test(mutation): first campaign results and escape dispositions

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Self-review notes

- **Spec coverage:** layout (T1/T3), mutant format + authoring workflow (T1/T2), driver with tiers/STALE/BUILD-BROKEN/trap/seed-sweep (T3), justfile + AGENTS.md (T4), population process with cold-read prompt + filter (T5), RESULTS.md format + escape dispositions + success criteria (T6). Rejected alternatives need no tasks.
- **Note on TDD:** this plan builds test infrastructure in bash, not production Go code, so "failing test first" maps to functional verification steps (T3 steps 2-5 verify guard, kill, STALE, BUILD-BROKEN paths against known inputs).
- **Canary risk:** if T1 step 5 shows m001 surviving, stop and investigate before continuing — the whole driver validation strategy depends on it.
- **docs/ is gitignored:** spec/plan commits under `docs/` require `git add -f` (existing repo precedent).
