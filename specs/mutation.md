# Mutation Campaign Design

## Purpose

The oracle (`specs/oracle.md`) is our main bug detector, but a passing oracle only tells you the oracle didn't complain — it doesn't tell you the oracle *would* complain if a real bug were present. The mutation campaign is how we measure that. It deliberately breaks the production code in small, realistic ways and checks that the oracle catches each break. If a mutant survives, the oracle has a blind spot exactly there.

So the campaign is the scorecard for the oracle's detection power. It turns "the tests are green" into "the tests can go red for the bugs we care about," which is the property that actually matters. Read `specs/oracle.md` first — the campaign only makes sense as a measurement of the thing that doc describes.

This document explains what the system is and the rules that keep it honest. It is not a task list; active work lives in GitHub issues. The live scorecard lives in `testing/mutation/RESULTS.md` (human-facing) and `testing/mutation/baseline.json` (machine-enforced).

## What a mutant is

A **mutant** is a single-edit patch to production code that models a realistic bug. Each one lives as a `.patch` file in `testing/mutation/mutants/` (e.g. `m019_sync_tombstone_dropped.patch`) and carries a small metadata header before the diff:

- `mutant` — its id.
- `target` — the file it breaks.
- `failure-mode` — the production bug it models, in prose. This is the point of the mutant: it's a bug someone could plausibly write.
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

- **KILLED** — the oracle caught it. This is the outcome we want. The result note records which tier killed it (e.g. `KILLED@default`, `KILLED@partb`, `KILLED@corpus`).
- **SURVIVED** — the oracle stayed green with the bug present. A blind spot; analyze and disposition it.
- **STALE** — the patch no longer applies. The production code moved out from under the mutant; the mutant needs re-review and a refresh.
- **BUILD-BROKEN** — the patch applies but no longer compiles. Same cause, same fix: refresh the mutant.

A few things about running it that bite people:

- **The working tree must be clean.** The driver applies and reverts patches with `git`, and refuses to start on a dirty tree. If a revert ever fails it aborts loud rather than trust a corrupted tree — crash-loud over corrupt, same as the rest of the project. Commit or stash first. (This one is also in `specs/gotchas.md`.)
- **`--seeds N`** stress-sweeps a single mutant across N random seeds. Some bugs are boundary-dependent and only show up on some seeds; a mutant killed 4/5 seeds is a probabilistic detection, not a boundary-exact one, and that distinction matters when judging oracle strength.
- **`--race`** runs every tier under the data-race detector so a race-only regression in the stress/restart interleavings becomes a kill. It's much slower, so the driver widens the per-tier timeouts.

## The baseline gate

The campaign is only useful if a regression fails CI, so the scorecard is enforced, not just written down.

`testing/mutation/baseline.json` is the committed source of truth: `{commit, mutants: [{id, disposition, ...}]}`. The `testing/mutation/gate` command (via `just mutation-gate`, run on a schedule in CI) runs a fresh campaign and diffs it against the baseline. It fails on:

- **REGRESSION** — a mutant the baseline records as KILLED is now SURVIVED. The oracle lost detection power. This is the important one.
- **STALE / BUILD-BROKEN** — a patch that no longer applies or compiles.
- **MISSING / NEW** — a baseline mutant absent from the run, or a run mutant absent from the baseline (the catalog and baseline drifted apart).

A **SURVIVED→KILLED** flip is an *improvement*, not a failure: the gate surfaces it so you can bank it, but it never fails the build. To bank an improvement (or to add/retire a mutant), regenerate the baseline with `just mutation-baseline` and commit the reviewed diff.

`RESULTS.md` is the human-readable history — each campaign appends a dated section and old sections are never back-edited, so the oracle's detection power over time stays visible. When the two disagree, `baseline.json` is the enforced truth; `RESULTS.md` is the narrative.

## Why this matters more than usual here

Two facts from past campaigns show why the gate earns its keep:

- Most mutant kills come from oracle tiers, not unit tests — so the survivors map precisely onto oracle blind spots, which is exactly the signal we want.
- We've been burned once already: deleting a package silently flipped a mutant from KILLED to SURVIVED, and the gate is what caught it. Without the gate, that lost coverage would have been invisible.

That's the whole argument for the campaign: it's the difference between believing the tests are strong and knowing which bugs they'll actually catch.

## Requirements for future changes

### Adding a mutant

Only add mutants that model a realistic single-edit bug. Every mutant should compile, avoid trivial panics, carry the metadata header above (including `expected-tier` stated before the first run), and be retired when code movement makes it stale or dead. After adding one, bank it KILLED via `just mutation-baseline` so the gate enforces the new coverage.

### After changing ingest, segment, or orchestrator logic

Re-run the campaign. A STALE result means the code moved and the mutant needs re-review, not a rubber-stamp refresh. New oracle capabilities should either kill a previously-surviving mutant or come with a new mutant that proves the new check has teeth — otherwise the capability is present but not measured.

### Retiring a mutant

A mutant that models a now-impossible bug (a code path that no longer exists, a dependency that changed behavior) should be retired from the active catalog with a recorded reason in `RESULTS.md`, not silently deleted. The reason is what stops a future agent from "restoring" it.

## See also

- `specs/oracle.md` — the oracle and simulator this campaign measures. Read first.
- `testing/mutation/RESULTS.md` — the dated human scorecard and full mutant history.
- `testing/mutation/baseline.json` — the machine-enforced baseline.
- `testing/mutation/run.sh` — the driver.
- `AGENTS.md` — the "never apply patches by hand, never fix code to match a mutant" rules and the change-class → recipe table.
