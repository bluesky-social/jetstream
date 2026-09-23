# Gotchas: accepted limitations and hard-won lessons

Known limitations and lessons from previous failures. Check these before changing the behavior they describe, and discuss changes to accepted tradeoffs with Jim. Add an entry when a surprising behavior needs a lasting explanation.

## Accepted limitations

### Relay listHosts accountCount is a floor, not PDS truth

The relay roster's `accountCount` is useful for starting the largest PDSes
first and sizing worker pools, but it is not an authoritative count. A direct
PDS `listRepos` crawl can return substantially more accounts because the relay
was rebuilt, or fewer after migrations. Do not use the relay count as a drain
condition, completeness proof, or exact status denominator. A host is complete
only when its direct pagination drains; status presents relay count as a floor
and direct enumeration separately. Area: Atmos `backfill`,
`internal/ingest/backfill/status.go`, `internal/status/collect.go`.

### Relay-global bootstrap cursors cannot migrate to per-PDS cursors

`relay/list_repos_cursor` and `bootstrap/last_listrepos_cursor` belong to the
retired relay-global enumeration scheme. Their opaque values have no safe
translation into host-local PDS cursor spaces. Startup therefore fails loudly
when either key is non-empty: finish that bootstrap with the old binary or use
a fresh data directory. Do not silently ignore, clear, or reinterpret these
keys; doing so would turn an operator-visible incompatibility into possible
data loss. Area: `internal/ingest/backfill/cursor.go`.

### Live first sighting is not a getRepo trigger

Do not add backfill on a DID’s first live event.

A repo can appear live that we never backfilled — say its PDS was firewalled during the bootstrap `listRepos` sweep, so the first event we ever see for it is well past its start. Jetstream archives the live event it actually received and does **not** create a `repo/<did>` row, mark it pending, or call `getRepo` merely because this is the first time we saw the DID.

Why: a first live event is not an authoritative repair signal. If the repo was hidden behind a firewall and later comes online, that is a PDS/operator condition; the operator should emit a fresh `#sync` event to trigger a full re-download through the sync verifier. A speculative `getRepo` from Jetstream captures current state, not event history, and conflates relay discovery with repo repair.

The retained background download path is only for repos discovered by authoritative `listRepos` bookkeeping that failed their original download (`StatusFailed`), including the post-merge discovery pass. `StatusPending` is also used for bootstrap crash recovery of a pre-existing `not_started` row (#262), but that producer is separate: merge runs an explicit one-shot pending pass after the captured live tail has landed. New live first sightings must not create pending rows, and the steady-state failed-repo retry loop must not treat pending as eligible.

Area: `internal/ingest/backfill/retry.go`, `internal/ingest/orchestrator/steady.go`, `docs/README.md` §4.3, issue #247.

### Timestamp-import ReadRow can accept a suffix behind a quoted newline

Phase C of timestamp import re-reads and validates CSV rows by byte offset. `ReadRow` checks that the preceding byte is a newline, but cannot distinguish a record boundary from a newline inside a quoted field. A stale offset could therefore parse a valid suffix row. Detecting this would require a full quote-aware scan or binding the CSV to the job by size and hash. This limitation is accepted: only the operator can replace the CSV, and the operator already controls imported timestamps. See `internal/timestamp/apply.go` (`ReadRow`).

### A spec-valid rkey longer than 255 bytes is dropped by design

atproto record keys can be up to ~1023 bytes, but our segment format caps the rkey column at 255 bytes. A record with a legal-but-longer rkey is dropped at the ingest gate under `ErrFieldTooLong` with its own metric reason — distinct from "the network sent garbage" — so operators can tell a representation limit from actual bad input. This is a deliberate format trade-off, not a validation bug. Area: `internal/ingest` validation gate, `segment/block.go` column limits, `docs/README.md` §4.4.

### A failed timestamp import can leave a partial rule set active — operator re-submits

Rule-map ingestion (`ruleSSTBuilder.Ingest` in `internal/timestamp/rules.go`) installs the sorted chunk SSTs one `pebble.Ingest` at a time; each call is individually atomic and immediately durable. A crash or error partway through the loop therefore leaves a committed *prefix* of the CSV resident, with no marker distinguishing it from a complete import — and since every chunk carries its collections' activation markers, `Stamp` runs against that partial keyspace after the next boot. Consequences in the window: events whose rules landed are stamped, later ones are not, and a path whose CSV last-write-wins winner lives in a not-yet-ingested chunk can carry a *stale* stamp into segment bytes and the live wire.

Accepted by Jim on 2026-07-08. Recovery depends on completing the import:

- A **crash** mid-ingest leaves the job non-terminal; the next boot auto-resumes (`ResumeIncomplete`) and re-runs rule ingestion from the CSV.
- A **terminal failure** (e.g. ENOSPC) does not auto-resume — by design, since re-running a deterministically-failing job would loop. The operator re-submits the same CSV via the import XRPC once the cause is fixed. The importer never modifies or deletes the staged CSV (it opens it read-only; terminal cleanup removes the *scratch* dir under `import-scratch/<job>`, not the import dir), so the exact same file is re-submittable. Re-ingest is last-write-wins over the full CSV, which heals both missing entries and the stale cross-chunk duplicate edge; the bucket+patch phases were already idempotent.

Alternatives considered and rejected as not worth the cost against this remediation story: k-way-merging chunks into one atomic multi-file `db.Ingest` (~2x scratch write amp on a ~200GB entry stream), and deferring collection markers to a post-ingest commit batch (still leaves the window for re-imports into an already-active collection). Area: `internal/timestamp/rules.go` (`Ingest` — comment there), `internal/importer/importer.go`, `docs/README.md` §8.

### A shutdown racing the import preamble can terminally fail the job instead of pausing it

`RunImport`'s steady-state preamble calls `Writer.ForceRotate` after rule ingestion (`internal/ingest/orchestrator/import_pass.go`). On graceful shutdown the orchestrator closes the steady writer concurrently with cancelling the import context; if the close wins the race, `ForceRotate` returns `ingest.ErrClosed`, which `IsCancellationOnly` correctly refuses to classify as a pause — so the importer marks the job terminally **failed** rather than leaving it resumable. The window is narrow (cancellation usually surfaces first), and the failure is loud: the job lands in `failed` state on `/status` with the rotate error recorded.

Accepted (Jim, 2026-07-08): the operator re-submits the same CSV, exactly as for any other terminal failure — see the previous entry for why re-submission is safe and complete. Do not teach `IsCancellationOnly` about `ingest.ErrClosed` (it would couple the shared classifier, also used by import metrics, to an ingest sentinel) and do not translate the error at the call site without revisiting this decision. Area: `internal/ingest/orchestrator/import_pass.go` (comment at the `ForceRotate` call), `internal/ingest/orchestrator/import_metrics.go` (`IsCancellationOnly`), `internal/importer/importer.go` (`run`).

### `just run-prod` inherits the dev-speed flags from `.env`

`set dotenv-load` loads `.env` for every recipe. `run-prod` overrides only relay, PLC, and data-directory settings, so `JETSTREAM_SKIP_MERGE_DISCOVERY`, `JETSTREAM_DISABLE_REPO_ACTION_RATE_LIMITS`, and the 1s status-cache TTL still apply. It is a local development recipe against real services; a recipe matching production configuration is deferred to pre-1.0 review. Area: `justfile` (`run-prod`).

---

## Lessons

### There are several copies of the "is this just cancellation?" classifier — grep them all

`IsCancellationOnly` in the import path requires every error leaf to be cancellation. `errors.Is(err, context.Canceled)` would also match `errors.Join(context.Canceled, realFailure)` and incorrectly make a failed import resumable. Other callers in `orchestrator/steady.go`, `backfill/retry.go`, `jetstreamd/runtime.go`, and the simulator only need to know whether cancellation occurred. Search all callers before changing this logic; their predicates serve different purposes. Area: `internal/ingest/orchestrator/import_metrics.go`.

### A restart-tier recovery child hangs if the relay is quiet — generate traffic between children

The oracle restart child's cutover delivery gate (`cutoverDeliveryGate` in the restart harness) deliberately treats zero observations as "the bootstrap-live consumer hasn't delivered yet — keep waiting," because a fresh child always replays from seq 1. But a *recovery* child whose predecessor already archived every firehose frame and persisted cursor == relay tip resumes at the tip, observes nothing, and the gate waits forever — the test fails as an opaque 30s child timeout. The fix is not to weaken the gate (the zero-observations rule is what catches real delivery loss): generate a couple of fresh live events between the first child's exit and the recovery child's start (`liveEventsBetweenChildren` in the segment-fault scenarios), which mirrors reality — the relay doesn't stop when jetstream restarts. If you write a new fault/crash scenario whose first child runs long enough to fully drain the firehose, you need this too. Area: `internal/oracle/restart_harness_test.go` (gate), `restart_segmentfault_test.go` (the pattern).

### Package-global zstd encoders and testing/synctest bubbles don't mix — warm them first

klauspost/compress encoders create an internal worker channel lazily on the first `EncodeAll`. A channel created inside a `testing/synctest` bubble fatals the whole test binary ("receive on synctest channel from outside bubble") when the encoder is later used outside it — and vice versa. `segment.WarmEncoder` and `subscribe.WarmEncoder` exist to force that creation at `TestMain`, outside any bubble; the oracle calls both. This is also why `subscribe`'s encoder pools (`encoderpool.go`, #295) are channel free lists and NOT `sync.Pool`: `WarmEncoder` pre-creates every pooled encoder to the cap so in-bubble compressions only ever draw bubble-safe encoders, whereas `sync.Pool`'s GC eviction would drop warmed encoders and lazily rebuild them in-bubble. Residual edge: more than pool-cap concurrent in-bubble compressions would build a fresh in-bubble encoder; no current test approaches that. If you add a new package-global encoder (or another lazily-channel-creating global), give it a warm hook and call it from the oracle's `TestMain`. Area: `internal/subscribe/encoderpool.go`, `internal/subscribe/compress.go` (`WarmEncoder`), `segment/zstd.go`, `internal/oracle/main_test.go`.

### The mutation campaign needs a clean git working tree

`just mutation-campaign` / `mutation-gate` apply and revert mutant patches with `git apply`. If the working tree is dirty, a revert can fail, and the driver crashes loud rather than trust a corrupted tree (`FATAL: ... working tree is DIRTY — aborting`). Commit or stash your work before running the campaign. Also: never apply the mutant patches by hand outside the driver, and never "fix" production code to match a mutant — they are deliberate bugs. Area: `testing/mutation/run.sh` (`revert_current`), `AGENTS.md`.

### Mutant patches must carry context lines — zero-context hunks corrupt the tree on revert

A mutant patch whose hunk has no context lines is anchored only by its line number. When the target file later drifts (code added above the hunk), the *forward* `git apply --unidiff-zero` still lands correctly via git's offset search — the campaign runs, the mutant is KILLED, the gate reports PASS — but the *reverse* apply of a pure deletion is a pure insertion with no content anchor, so git re-inserts the lines at the stale line number, silently corrupting an unrelated spot in the file **after** the PASS (found twice as `merge.go` residue after gate runs; #305). The driver cannot catch this: the revert "succeeds," so `revert_current`'s failure path never fires. Lessons: (1) generate mutant patches as standard context diffs (`git diff`, 3 context lines) — content anchoring makes both directions drift-safe; (2) check `git status` after any campaign/gate run before building on the tree; (3) when refreshing a mutant, verify the round-trip leaves `git status` clean: `git apply --unidiff-zero <p> && git apply --unidiff-zero -R <p>`. Area: `testing/mutation/mutants/*.patch`, `testing/mutation/run.sh`.
