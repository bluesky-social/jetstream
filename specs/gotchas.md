# Gotchas: accepted limitations and hard-won lessons

This file is the shared home for two kinds of knowledge that otherwise live only in one person's head:

- **Accepted limitations** — things that look like bugs but are deliberate. We considered them and decided to live with them. Don't "fix" one without checking here first and reopening the decision on purpose.
- **Lessons** — mistakes that were expensive to learn, and traps that are easy to fall into twice.

Each entry says what the thing is, why it's the way it is, and roughly where in the code it lives (by area, not line number, so it doesn't rot). If you hit something surprising and figure out why, add an entry. If you find yourself about to change something an entry describes, that's your cue to talk to Jim first.

---

## Accepted limitations

### Net-new DID backfill is correct by design — don't "fix" it

This one repeatedly confuses agents into thinking there's a data-loss bug. There isn't; the behavior is intentional. Here's what actually happens so you don't try to harden it.

A repo can appear live that we never backfilled — say its PDS was firewalled during the bootstrap listRepos sweep, so the first event we ever see for it is well past its start. When the steady-state live consumer archives an event for a DID with no `repo/<did>` row, it writes one at `StatusPending` (`EnqueueNetNewRepo`). The retry loop treats `pending` exactly like a failed repo, so its next pass does a full `getRepo` and captures the repo's current state — the same model we use for a `#sync` resync.

Two things that look like problems but aren't:

- The event that triggered the discovery **is** archived normally. It isn't dropped while we go fetch the repo.
- Events that predate our first sighting were never on our wire and can't be recovered event-by-event — `getRepo` serves the current head, not history. That's inherent to atproto, not a jetstream defect. The current repo state is captured; the archive stays correct going forward.

This only matters in steady state. During bootstrap, a DID that first appears mid-sweep is still enumerated later in the same listRepos pagination, so there's no gap to close there. Area: `internal/ingest/backfill/enqueue.go` and `store.go` (`EnqueueNetNewRepo`), `docs/README.md` §4.3, issue #188.

### Timestamp-import ReadRow can accept a suffix behind a quoted newline

(Sourced from the in-code comment, which marks this a known, accepted limitation.) Phase C of the timestamp import re-reads one CSV row by byte offset and re-validates it, checking that the byte before the offset is a newline so a stale offset can't land mid-record. A newline embedded inside a quoted CSV field also satisfies that check, so an offset into such a multi-line record could parse as a suffix row. Closing it would need global quote-parity tracking (a full re-scan, or binding the CSV to the job by size+hash). The code comment records the decision not to: the only actor who can swap the CSV under a resumed job is the operator, who can already import arbitrary timestamps honestly, and an accidental desync that happens to produce a valid-parsing suffix behind a quoted newline is vanishingly unlikely. Area: `internal/timestamp/apply.go` (`ReadRow`) — read the comment there before touching it.

### A spec-valid rkey longer than 255 bytes is dropped by design

atproto record keys can be up to ~1023 bytes, but our segment format caps the rkey column at 255 bytes. A record with a legal-but-longer rkey is dropped at the ingest gate under `ErrFieldTooLong` with its own metric reason — distinct from "the network sent garbage" — so operators can tell a representation limit from actual bad input. This is a deliberate format trade-off, not a validation bug. Area: `internal/ingest` validation gate, `segment/block.go` column limits, `docs/README.md` §4.4.

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
