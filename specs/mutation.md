# Mutation Campaign Design

## Purpose

The mutation campaign measures whether the oracle detects realistic bugs. It applies small production-code defects and runs the oracle against each one. A surviving mutant identifies missing coverage. Read `specs/oracle.md` for the oracle’s design.

This document defines the campaign’s rules. Active work lives in GitHub issues; results live in `testing/mutation/RESULTS.md` and the enforced baseline in `testing/mutation/baseline.json`.

## What a mutant is

A **mutant** is a single-edit patch to production code that models a realistic bug. Each one lives as a `.patch` file in `testing/mutation/mutants/` (e.g. `m019_sync_tombstone_dropped.patch`) and carries a small metadata header before the diff:

- `mutant` — its id.
- `target` — the file it breaks.
- `failure-mode` — the production bug it models, in prose.
- `expected-detection` — which oracle check should catch it and why.
- `expected-tier` — the tier we predict kills it, stated *before* the first run so we can't rationalize after the fact.
- `tiers` — which tiers to actually run for this mutant.

A mutant is a bug *model*, not a production patch. Two hard rules follow from that, and both are also in `AGENTS.md`:

- **Never apply these patches outside the driver.** They are deliberate bugs; applying one by hand and forgetting leaves the tree broken.
- **Never "fix" production code to make a mutant behave.** If a mutant won't die, the finding is a missing oracle check, not a wrong mutant. Fix the oracle (or retire the mutant with a reason), never the code-under-test.

## How the campaign runs

`testing/mutation/run.sh` is the driver; `just mutation-campaign` is the wrapper. For each mutant it:

1. applies the patch to a clean tree,
2. runs the tiers named in the mutant's `tiers` field,
3. records whether the oracle went red (KILLED) or stayed green (SURVIVED),
4. reverts the patch.

The disposition of a run is one of:

- **KILLED** — the oracle caught it. The result note records which tier killed it (e.g. `KILLED@default`, `KILLED@partb`, `KILLED@corpus`).
- **SURVIVED** — the oracle stayed green with the bug present. A blind spot; analyze and disposition it.
- **STALE** — the patch no longer applies. The production code moved out from under the mutant; the mutant needs re-review and a refresh.
- **BUILD-BROKEN** — the patch applies but no longer compiles. Same cause, same fix: refresh the mutant.

Run requirements and options:

- **The working tree must be clean.** The driver applies and reverts patches with `git`, and refuses to start on a dirty tree. The driver aborts if a revert fails. Commit or stash first. (This one is also in `specs/gotchas.md`.)
- **`--seeds N`** stress-sweeps a single mutant across N random seeds. Some bugs are boundary-dependent and only show up on some seeds; a mutant killed 4/5 seeds is a probabilistic detection, not a boundary-exact one, and that distinction matters when judging oracle strength.
- **`--race`** runs every tier under the data-race detector so a race-only regression in the stress/restart interleavings becomes a kill. It's much slower, so the driver widens the per-tier timeouts.

## The baseline gate

`just mutation-gate` enforces the committed baseline in CI.

`testing/mutation/baseline.json` is the committed source of truth: `{commit, mutants: [{id, disposition, ...}]}`. The `testing/mutation/gate` command (via `just mutation-gate`, run on a schedule in CI) runs a fresh campaign and diffs it against the baseline. It fails on:

- **REGRESSION** — a mutant the baseline records as KILLED is now SURVIVED. The oracle lost detection power.
- **STALE / BUILD-BROKEN** — a patch that no longer applies or compiles.
- **MISSING / NEW** — a baseline mutant absent from the run, or a run mutant absent from the baseline (the catalog and baseline drifted apart).

A **SURVIVED→KILLED** flip is an *improvement*, not a failure: the gate surfaces it so you can bank it, but it never fails the build. To bank an improvement (or to add/retire a mutant), regenerate the baseline with `just mutation-baseline` and commit the reviewed diff.

`RESULTS.md` is the human-readable history — each campaign appends a dated section and old sections are never back-edited, so the oracle's detection power over time stays visible. When the two disagree, `baseline.json` is the enforced truth; `RESULTS.md` is the narrative.

## Evidence from past campaigns

Past campaigns found:

- Most mutant kills come from oracle tiers, not unit tests — so the survivors map precisely onto oracle blind spots, which is exactly the signal we want.
- We've been burned once already: deleting a package silently flipped a mutant from KILLED to SURVIVED, and the gate is what caught it. Without the gate, that lost coverage would have been invisible.

## Requirements for future changes

### Adding a mutant

Only add mutants that model a realistic single-edit bug. Every mutant should compile, avoid trivial panics, carry the metadata header above (including `expected-tier` stated before the first run), and be retired when code movement makes it stale or dead. After adding one, bank it KILLED via `just mutation-baseline` so the gate enforces the new coverage.

### After changing ingest, segment, or orchestrator logic

Re-run the campaign. A STALE result means the code moved and the mutant needs re-review, rather than an automatic refresh. Measure new oracle checks with a previously surviving or new mutant.

### Retiring a mutant

A mutant that models a now-impossible bug (a code path that no longer exists, a dependency that changed behavior) should be retired from the active catalog with a recorded reason in `RESULTS.md`, not silently deleted. The reason is what stops a future agent from "restoring" it.

## See also

- `specs/oracle.md` — the oracle and simulator this campaign measures. Read first.
- `testing/mutation/RESULTS.md` — the dated human scorecard and full mutant history.
- `testing/mutation/baseline.json` — the machine-enforced baseline.
- `testing/mutation/run.sh` — the driver.
- `AGENTS.md` — the "never apply patches by hand, never fix code to match a mutant" rules and the change-class → recipe table.
