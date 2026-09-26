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

### A spec-valid rkey longer than 255 bytes is dropped by design

atproto record keys can be up to ~1023 bytes, but our segment format caps the rkey column at 255 bytes. A record with a legal-but-longer rkey is dropped at the ingest gate under `ErrFieldTooLong` with its own metric reason — distinct from "the network sent garbage" — so operators can tell a representation limit from actual bad input. This is a deliberate format trade-off, not a validation bug. Area: `internal/ingest` validation gate, `segment/block.go` column limits, `docs/README.md` §4.4.

### `just run-prod` inherits the dev-speed flags from `.env`

`set dotenv-load` loads `.env` for every recipe. `run-prod` overrides only relay, PLC, and data-directory settings, so `JETSTREAM_SKIP_MERGE_DISCOVERY`, `JETSTREAM_DISABLE_REPO_ACTION_RATE_LIMITS`, and the 1s status-cache TTL still apply. It is a local development recipe against real services; a recipe matching production configuration is deferred to pre-1.0 review. Area: `justfile` (`run-prod`).

---

### Timestamp-cursor failover is approximate and at-least-once

Under the client's `CursorTime` mode, a resume on a different host uses `witnessedAt - rewind`. Each instance stamps `witnessed_at` with its own clock when it sees an event, so the same event has slightly different times on different hosts. The rewind (default 5s) is a skew allowance, not a guarantee: skew larger than the rewind can skip events, and everything inside the rewind is re-delivered with no way to dedup across seq namespaces. Callers in this mode must be idempotent. The client identifies a seq namespace by the configured hostname alone: a reconnect to the same name always resumes by seq. So each hostname given to `WithHost`/`WithFailoverHosts` must address exactly one instance; a name that load-balances across instances (or is repointed at a different instance) gets a foreign seq and can skip or replay events. Area: `live.go` (`planSession`, `adoptNamespace`).

---

### The local catalog sees only the newest unsealed file in a namespace

`catalog/local` treats the highest-index unsealed file as the namespace's active segment and skips any unsealed file below it. The writer cannot produce such a file: `rotateLocked` finishes the seal before it creates the next file, and `ingest.Open` resumes the highest index. A stranded older unsealed file therefore means external damage. Cold replay then fails loud on the unregistered hole rather than skipping it. The oracle's catalog observer cross-checks against the path walk, so a quiescent directory with such a file fails the observation (`TestObserveSegments_CrossCheckCatchesStrandedActive`). Area: `internal/catalog/local`, `internal/oracle/catalog_observer.go`.

## Lessons

### A restart-tier recovery child hangs if the relay is quiet — generate traffic between children

The oracle restart child's cutover delivery gate (`cutoverDeliveryGate` in the restart harness) deliberately treats zero observations as "the bootstrap-live consumer hasn't delivered yet — keep waiting," because a fresh child always replays from seq 1. But a *recovery* child whose predecessor already archived every firehose frame and persisted cursor == relay tip resumes at the tip, observes nothing, and the gate waits forever — the test fails as an opaque 30s child timeout. The fix is not to weaken the gate (the zero-observations rule is what catches real delivery loss): generate a couple of fresh live events between the first child's exit and the recovery child's start (`liveEventsBetweenChildren` in the segment-fault scenarios), which mirrors reality — the relay doesn't stop when jetstream restarts. If you write a new fault/crash scenario whose first child runs long enough to fully drain the firehose, you need this too. Area: `internal/oracle/restart_harness_test.go` (gate), `restart_segmentfault_test.go` (the pattern).

### `drain()` is not "the consumer processed frame X"

`drain()` is `synctest.Wait`, which returns when every bubble goroutine is durably blocked, and a goroutine asleep on a timer counts. A live consumer in reconnect backoff after a scheduled subscribeRepos disconnect looks quiescent with frames still undelivered. For anything with no ack-visible row, such as a whole-event drop, wait on a positive signal or poll the observable under fake time up to a deadline. Area: `internal/oracle/adversarial_harness_test.go`, `specs/oracle/2026-09-25-adversarial-drop-scrape-races-reconnect.md`.

### Replay-guard state must enter the durable batch with its rows

The live consumer's replay guards (verifier chain and hosting state, the applied `#identity`/`#account` seqs) drop a redelivered event whose state is already durable. That state has to become durable in exactly the batch that holds the event's last row. If it becomes durable earlier, a crash loses the row: its replay is dropped. If it becomes durable later, a crash archives the whole event twice. So promotion and seq ratchets run in the writer's `OnAppend` hook, under the writer mutex, and each durable batch stages the snapshot taken when it was cut (`durablePrepare`), not whatever was promoted by the time it committed. Code that promotes after `Append` returns, or stages at commit time, reintroduces both bugs. Only the layer 3 oracle checks duplicates exactly, so do not count on the single-node crash oracle to catch them.

The atmos verifier runs ahead of the appends: it can save a DID's state for a later event before the earlier event's last row lands. So `syncstate.StateStore` keeps an ordered queue of pending entries per DID, and promotion takes the newest entry at or below the appended event's rev or seq. A single pending slot per DID lets the later save hide the earlier event's state, and after a crash the successor either resyncs needlessly or archives the event twice. Area: `internal/ingest/live/consumer.go` (`promoteSyncState`, `prepareDurable`), `internal/ingest/syncstate`, `specs/oracle/2026-09-25-disagg-syncstate-batch-boundary.md`, `specs/oracle/2026-09-26-disagg-pipelined-chain-state-hidden.md`.

### A storage call under a third-party mutex wedges the layer 3 scheduler

`storagefake.Seeded` admits a parked call only after `synctest.Wait`, and synctest does not count a goroutine waiting on a `sync.Mutex` as durably blocked. If one goroutine parks at a yield point while holding a mutex that another goroutine wants, the bubble hangs until the child times out. It looks like a hang, not a failure. Jetstream code must not hold a mutex across a storage or blob call. The atmos verifier holds a per-DID lock across syncstate loads, which is why the fake's meta reads take no scheduler turn. If you add a yield point, check who can call it while holding a lock. Area: `internal/storagefake/sched.go`, `meta.go`, `specs/oracle/2026-09-25-disagg-scheduler-wedged-by-verifier-mutex.md`.

The same rule applies to locks held across a write. The backfill `Store`'s `countsMu` and `rosterMu` span catalog commits, so they are channel-based (`chanMutex`), which synctest counts as durably blocked. atmos backfill holds a per-DID-shard `sync.Mutex` across the Store's discovery write, which Jetstream cannot change, so the layer 3 oracle runs with `BackfillMaxActiveHosts = 1`. Only one reconcile runs at a time, so none waits on that shard. With one active host, atmos lists a host fully before dispatching its downloads. A fault that must fire mid-bootstrap needs the listing held elsewhere, which is what the lifecycle harness's host gate (`disaggListGate`) does. Area: `internal/ingest/backfill/store.go`, `internal/oracle/disagg_oracle_test.go`, `internal/oracle/disagg_lifecycle_test.go`.

### A deposed leader still reaches crash seams

A killed or deposed leader keeps running until its context and client die, and it can reach an armed crashpoint after its successor has started. If the old session takes the seam, the kill lands on a process that is already dead, the fault never exercises the new leader, and the harness waits for a crash that doesn't come. The layer 3 injector only fires for the pod that started the newest session (`newestSession`). Any new harness that arms seams across leader changes needs the same check. Area: `internal/oracle/disagg_oracle_test.go` (`disaggCrashInjector`).

### A v1 subscriber sees a projection of the stream, not the stream

A v1 subscriber gets no sync rows, receives resync replacements as ordinary creates, and sees a rev only on commits. Comparing a v1 observer to the model row for row fails as soon as a resync happens. The layer 3 oracle compares v1 observers against `disaggV1Project`, not against the raw model. Area: `internal/oracle/disagg_oracle_test.go`.

### Package-global zstd encoders and testing/synctest bubbles don't mix — warm them first

klauspost/compress encoders create an internal worker channel lazily on the first `EncodeAll`. A channel created inside a `testing/synctest` bubble fatals the whole test binary ("receive on synctest channel from outside bubble") when the encoder is later used outside it — and vice versa. `segment.WarmEncoder` and `subscribe.WarmEncoder` exist to force that creation at `TestMain`, outside any bubble; the oracle calls both. This is also why `subscribe`'s encoder pools (`encoderpool.go`, #295) are channel free lists and NOT `sync.Pool`: `WarmEncoder` pre-creates every pooled encoder to the cap so in-bubble compressions only ever draw bubble-safe encoders, whereas `sync.Pool`'s GC eviction would drop warmed encoders and lazily rebuild them in-bubble. Residual edge: more than pool-cap concurrent in-bubble compressions would build a fresh in-bubble encoder; no current test approaches that. If you add a new package-global encoder (or another lazily-channel-creating global), give it a warm hook and call it from the oracle's `TestMain`. Area: `internal/subscribe/encoderpool.go`, `internal/subscribe/compress.go` (`WarmEncoder`), `segment/zstd.go`, `internal/oracle/main_test.go`.

### Per-process readers must survive a writer-session change

The subscribe tail, cold reader, status, and cursor resolution outlive writer sessions (`internal/jetstreamd/session.go`). Two mistakes here are easy to make and hard to spot:

- Do not clear the published writer when a session ends. A subscriber that already passed the handler's writer check would anchor at `Tip()` 0 and replay the whole archive. Keep the closed writer until the next session publishes; the seq lease keeps the new writer's seqs above the old tip.
- A reader parked at the tip waits on the old log's notify channel, and that log never grows again. Replacing the source with `Tail.SetReadLogSource` must wake it, which is why the park also selects on the tail's own notify channel (`TestTail_SetReadLogSourceWakesReaderParkedOnOldLog`).

Anything per-session that a per-process component reads (tombstone gauges, the compaction deadline) goes through an indirection that the session swaps in and out. Area: `internal/jetstreamd`, `internal/subscribe/tail.go`.

### The mutation campaign needs a clean git working tree

`just mutation-campaign` / `mutation-gate` apply and revert mutant patches with `git apply`. If the working tree is dirty, a revert can fail, and the driver crashes loud rather than trust a corrupted tree (`FATAL: ... working tree is DIRTY — aborting`). Commit or stash your work before running the campaign. Also: never apply the mutant patches by hand outside the driver, and never "fix" production code to match a mutant — they are deliberate bugs. Area: `testing/mutation/run.sh` (`revert_current`), `AGENTS.md`.

### Mutant patches must carry context lines — zero-context hunks corrupt the tree on revert

A mutant patch whose hunk has no context lines is anchored only by its line number. When the target file later drifts (code added above the hunk), the *forward* `git apply --unidiff-zero` still lands correctly via git's offset search — the campaign runs, the mutant is KILLED, the gate reports PASS — but the *reverse* apply of a pure deletion is a pure insertion with no content anchor, so git re-inserts the lines at the stale line number, silently corrupting an unrelated spot in the file **after** the PASS (found twice as `merge.go` residue after gate runs; #305). The driver cannot catch this: the revert "succeeds," so `revert_current`'s failure path never fires. The forward apply can go wrong too. If the file has an identical line near the stale line number, the zero-context hunk lands there instead of on its target. The mutant then SURVIVES, which looks like an oracle gap, not a patch problem: m044 did this after S1.7 moved `flushLocked`, landing on the identical fault check in `commitPreparedFlushLocked`. Lessons: (1) generate mutant patches as standard context diffs (`git diff`, 3 context lines) — content anchoring makes both directions drift-safe; (2) check `git status` after any campaign/gate run before building on the tree; (3) when refreshing a mutant, verify the round-trip leaves `git status` clean: `git apply --unidiff-zero <p> && git apply --unidiff-zero -R <p>`. Area: `testing/mutation/mutants/*.patch`, `testing/mutation/run.sh`.

### The mutation driver reports BUILD-BROKEN from a git worktree

Inside a `git worktree`, under some sandboxes, Go's VCS stamping fails with "error obtaining VCS status: exit status 128" even though git itself works. When that happens, every mutant reports BUILD-BROKEN, which says nothing about the mutants. Run the driver with `GOFLAGS=-buildvcs=false` in that case. Area: `testing/mutation/run.sh`.
