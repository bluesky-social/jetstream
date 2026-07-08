# Gotchas: accepted limitations and hard-won lessons

This file is the shared home for two kinds of knowledge that otherwise live only in one person's head:

- **Accepted limitations** — things that look like bugs but are deliberate. We considered them and decided to live with them. Don't "fix" one without checking here first and reopening the decision on purpose.
- **Lessons** — mistakes that were expensive to learn, and traps that are easy to fall into twice.

Each entry says what the thing is, why it's the way it is, and roughly where in the code it lives (by area, not line number, so it doesn't rot). If you hit something surprising and figure out why, add an entry. If you find yourself about to change something an entry describes, that's your cue to talk to Jim first.

---

## Accepted limitations

### Live first sighting is not a getRepo trigger

This one repeatedly tempts agents into "repairing" a perceived missing-history gap. Do not reintroduce first-sighting backfill.

A repo can appear live that we never backfilled — say its PDS was firewalled during the bootstrap `listRepos` sweep, so the first event we ever see for it is well past its start. Jetstream archives the live event it actually received and does **not** create a `repo/<did>` row, mark it pending, or call `getRepo` merely because this is the first time we saw the DID.

Why: a first live event is not an authoritative repair signal. If the repo was hidden behind a firewall and later comes online, that is a PDS/operator condition; the operator should emit a fresh `#sync` event to trigger a full re-download through the sync verifier. A speculative `getRepo` from Jetstream captures current state, not event history, and conflates relay discovery with repo repair.

The retained background download path is only for repos discovered by authoritative `listRepos` bookkeeping that failed their original download (`StatusFailed`), including the post-merge discovery pass. `StatusPending` is also used for bootstrap crash recovery of a pre-existing `not_started` row (#262), but that producer is separate: merge runs an explicit one-shot pending pass after the captured live tail has landed. New live first sightings must not create pending rows, and the steady-state failed-repo retry loop must not treat pending as eligible.

Area: `internal/ingest/backfill/retry.go`, `internal/ingest/orchestrator/steady.go`, `docs/README.md` §4.3, issue #247.

### Timestamp-import ReadRow can accept a suffix behind a quoted newline

(Sourced from the in-code comment, which marks this a known, accepted limitation.) Phase C of the timestamp import re-reads one CSV row by byte offset and re-validates it, checking that the byte before the offset is a newline so a stale offset can't land mid-record. A newline embedded inside a quoted CSV field also satisfies that check, so an offset into such a multi-line record could parse as a suffix row. Closing it would need global quote-parity tracking (a full re-scan, or binding the CSV to the job by size+hash). The code comment records the decision not to: the only actor who can swap the CSV under a resumed job is the operator, who can already import arbitrary timestamps honestly, and an accidental desync that happens to produce a valid-parsing suffix behind a quoted newline is vanishingly unlikely. Area: `internal/timestamp/apply.go` (`ReadRow`) — read the comment there before touching it.

### A spec-valid rkey longer than 255 bytes is dropped by design

atproto record keys can be up to ~1023 bytes, but our segment format caps the rkey column at 255 bytes. A record with a legal-but-longer rkey is dropped at the ingest gate under `ErrFieldTooLong` with its own metric reason — distinct from "the network sent garbage" — so operators can tell a representation limit from actual bad input. This is a deliberate format trade-off, not a validation bug. Area: `internal/ingest` validation gate, `segment/block.go` column limits, `docs/README.md` §4.4.

### A failed timestamp import can leave a partial rule set active — operator re-submits

Rule-map ingestion (`ruleSSTBuilder.Ingest` in `internal/timestamp/rules.go`) installs the sorted chunk SSTs one `pebble.Ingest` at a time; each call is individually atomic and immediately durable. A crash or error partway through the loop therefore leaves a committed *prefix* of the CSV resident, with no marker distinguishing it from a complete import — and since every chunk carries its collections' activation markers, `Stamp` runs against that partial keyspace after the next boot. Consequences in the window: events whose rules landed are stamped, later ones are not, and a path whose CSV last-write-wins winner lives in a not-yet-ingested chunk can carry a *stale* stamp into segment bytes and the live wire.

This is deliberate (Jim, 2026-07-08). The remediation contract, not a marker/atomic-ingest scheme, closes the gap:

- A **crash** mid-ingest leaves the job non-terminal; the next boot auto-resumes (`ResumeIncomplete`) and re-runs rule ingestion from the CSV. Self-healing.
- A **terminal failure** (e.g. ENOSPC) does not auto-resume — by design, since re-running a deterministically-failing job would loop. The operator re-submits the same CSV via the import XRPC once the cause is fixed. The importer never modifies or deletes the staged CSV (it opens it read-only; terminal cleanup removes the *scratch* dir under `import-scratch/<job>`, not the import dir), so the exact same file is re-submittable. Re-ingest is last-write-wins over the full CSV, which heals both missing entries and the stale cross-chunk duplicate edge; the bucket+patch phases were already idempotent.

Alternatives considered and rejected as not worth the cost against this remediation story: k-way-merging chunks into one atomic multi-file `db.Ingest` (~2x scratch write amp on a ~200GB entry stream), and deferring collection markers to a post-ingest commit batch (still leaves the window for re-imports into an already-active collection). Area: `internal/timestamp/rules.go` (`Ingest` — comment there), `internal/importer/importer.go`, `docs/README.md` §8.

### A shutdown racing the import preamble can terminally fail the job instead of pausing it

`RunImport`'s steady-state preamble calls `Writer.ForceRotate` after rule ingestion (`internal/ingest/orchestrator/import_pass.go`). On graceful shutdown the orchestrator closes the steady writer concurrently with cancelling the import context; if the close wins the race, `ForceRotate` returns `ingest.ErrClosed`, which `IsCancellationOnly` correctly refuses to classify as a pause — so the importer marks the job terminally **failed** rather than leaving it resumable. The window is narrow (cancellation usually surfaces first), and the failure is loud: the job lands in `failed` state on `/status` with the rotate error recorded.

Accepted (Jim, 2026-07-08): the operator re-submits the same CSV, exactly as for any other terminal failure — see the previous entry for why re-submission is safe and complete. Do not teach `IsCancellationOnly` about `ingest.ErrClosed` (it would couple the shared classifier, also used by import metrics, to an ingest sentinel) and do not translate the error at the call site without revisiting this decision. Area: `internal/ingest/orchestrator/import_pass.go` (comment at the `ForceRotate` call), `internal/ingest/orchestrator/import_metrics.go` (`IsCancellationOnly`), `internal/importer/importer.go` (`run`).

### `just run-prod` inherits the dev-speed flags from `.env`

`set dotenv-load` in the justfile loads the committed `.env` for every recipe, and `run-prod` only overrides the relay/PLC/data-dir vars. So dev-speed flags like `JETSTREAM_SKIP_MERGE_DISCOVERY`, `JETSTREAM_DISABLE_REPO_ACTION_RATE_LIMITS`, and the 1s status-cache TTL still apply when `run-prod` points at real upstream services. This is intentional: `run-prod` is a local dev loop aimed at real upstream for fast iteration, not a production-config rehearsal. A faithful production-config recipe is deferred to pre-1.0 vetting (nothing runs the exact config production will use yet). If you're doing a maintainability/config audit, this is expected — don't file it as a bug. Area: `justfile` (`run-prod`).

---

## Lessons

### There are several copies of the "is this just cancellation?" classifier — grep them all

Deciding whether an error is a clean shutdown (context cancellation) versus a real failure shows up in more than one place, and they are NOT all the same predicate. The subtle one is `IsCancellationOnly` in the import path: a plain `errors.Is(err, context.Canceled)` is wrong there because `RunImport` can return `errors.Join(context.Canceled, realFailure)`, and `errors.Is` matches *any* leaf — which would launder a real failure into a resumable pause. It reports cancellation only when *every* leaf is cancellation. Other spots (`orchestrator/steady.go`, `backfill/retry.go`, `jetstreamd/runtime.go`, the simulator) do a plain `errors.Is` because they only care whether the top-level context was cancelled. Lesson: if you touch cancellation-vs-failure logic, grep for every copy and understand which semantics each one needs — they are not interchangeable. Area: `internal/ingest/orchestrator/import_metrics.go` (`IsCancellationOnly`) and the call sites above.

### A restart-tier recovery child hangs if the relay is quiet — generate traffic between children

The oracle restart child's cutover delivery gate (`cutoverDeliveryGate` in the restart harness) deliberately treats zero observations as "the bootstrap-live consumer hasn't delivered yet — keep waiting," because a fresh child always replays from seq 1. But a *recovery* child whose predecessor already archived every firehose frame and persisted cursor == relay tip resumes at the tip, observes nothing, and the gate waits forever — the test fails as an opaque 30s child timeout. The fix is not to weaken the gate (the zero-observations rule is what catches real delivery loss): generate a couple of fresh live events between the first child's exit and the recovery child's start (`liveEventsBetweenChildren` in the segment-fault scenarios), which mirrors reality — the relay doesn't stop when jetstream restarts. If you write a new fault/crash scenario whose first child runs long enough to fully drain the firehose, you need this too. Area: `internal/oracle/restart_harness_test.go` (gate), `restart_segmentfault_test.go` (the pattern).

### The mutation campaign needs a clean git working tree

`just mutation-campaign` / `mutation-gate` apply and revert mutant patches with `git apply`. If the working tree is dirty, a revert can fail, and the driver crashes loud rather than trust a corrupted tree (`FATAL: ... working tree is DIRTY — aborting`). Commit or stash your work before running the campaign. Also: never apply the mutant patches by hand outside the driver, and never "fix" production code to match a mutant — they are deliberate bugs. Area: `testing/mutation/run.sh` (`revert_current`), `AGENTS.md`.
