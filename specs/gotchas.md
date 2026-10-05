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

Because the steady loop never selects pending rows, the pending pass must not leave any behind. It ignores a pending row's `NextAttemptAt`, and it waits out a rate-limited host in memory instead of deferring that host's rows. A repo still rate limited after eight attempts, or on a host parked sixteen times in a row with no success, is recorded `failed`, so the steady loop retries it. The pass also records each completion in the block commit that makes the repo's rows durable, as bootstrap does. Before this, the pass deferred a 429'd host's rows hours ahead and left them pending forever, and its per-repo durability barrier capped it at a few dozen repos a second (`specs/notes/2026-10-05-disagg-merge-batching.md`).

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

### Compaction reads every live-history block, every pass

Compaction selects blocks by DID (segment and per-block DID blooms). A backfill block holds a few repos, so it narrows well. A live block holds about 3,700 distinct DIDs, so at any pass's tombstone volume nearly every live block has a tombstoned DID and a matching collection, and the rewrite reads it even when it drops nothing. A pass therefore reads the whole live-shaped archive, and that cost grows with the archive's age. Local mode does the same from disk. In disaggregated mode it is object GETs: about 650GiB per pass after 30 days of 3,000 events/s. Measured in S4.6 (design §22.4). A record-level filter or a tiered schedule is open as plan S5.7. Don't read a high `jetstream_compaction_blocks_fetched_total` ratio as a bug. Area: `segment/sparse.go` (`candidates`), `internal/ingest/orchestrator/compact_disagg.go`.

## Lessons

### A restart-tier recovery child hangs if the relay is quiet — generate traffic between children

The oracle restart child's cutover delivery gate (`cutoverDeliveryGate` in the restart harness) deliberately treats zero observations as "the bootstrap-live consumer hasn't delivered yet — keep waiting," because a fresh child always replays from seq 1. But a *recovery* child whose predecessor already archived every firehose frame and persisted cursor == relay tip resumes at the tip, observes nothing, and the gate waits forever — the test fails as an opaque 30s child timeout. The fix is not to weaken the gate (the zero-observations rule is what catches real delivery loss): generate a couple of fresh live events between the first child's exit and the recovery child's start (`liveEventsBetweenChildren` in the segment-fault scenarios), which mirrors reality — the relay doesn't stop when jetstream restarts. If you write a new fault/crash scenario whose first child runs long enough to fully drain the firehose, you need this too. Area: `internal/oracle/restart_harness_test.go` (gate), `restart_segmentfault_test.go` (the pattern).

### `drain()` is not "the consumer processed frame X"

`drain()` is `synctest.Wait`, which returns when every bubble goroutine is durably blocked, and a goroutine asleep on a timer counts. A live consumer in reconnect backoff after a scheduled subscribeRepos disconnect looks quiescent with frames still undelivered. For anything with no ack-visible row, such as a whole-event drop, wait on a positive signal or poll the observable under fake time up to a deadline. Area: `internal/oracle/adversarial_harness_test.go`, `specs/oracle/2026-09-25-adversarial-drop-scrape-races-reconnect.md`.

### Replay-guard state must enter the durable batch with its rows

The live consumer's replay guards (verifier chain and hosting state, the applied `#identity`/`#account` seqs) drop a redelivered event whose state is already durable. That state has to become durable in exactly the batch that holds the event's last row. If it becomes durable earlier, a crash loses the row: its replay is dropped. If it becomes durable later, a crash archives the whole event twice. So promotion and seq ratchets run in the writer's `OnAppend` hook, under the writer mutex, and each durable batch stages the snapshot taken when it was cut (`durablePrepare`), not whatever was promoted by the time it committed. Code that promotes after `Append` returns, or stages at commit time, reintroduces both bugs. Only the layer 3 oracle checks duplicates exactly, so do not count on the single-node crash oracle to catch them.

The atmos verifier runs ahead of the appends: it can save a DID's state for a later event before the earlier event's last row lands. So `syncstate.StateStore` keeps an ordered queue of pending entries per DID, and promotion takes the newest entry at or below the appended event's rev or seq. A single pending slot per DID lets the later save hide the earlier event's state, and after a crash the successor either resyncs needlessly or archives the event twice. Area: `internal/ingest/live/consumer.go` (`promoteSyncState`, `prepareDurable`), `internal/ingest/syncstate`, `specs/oracle/2026-09-25-disagg-syncstate-batch-boundary.md`, `specs/oracle/2026-09-26-disagg-pipelined-chain-state-hidden.md`.

### A forced durable batch must cover every appended event

`DrainDurability` ends in a forced durable batch, and the backfill completion batcher stages every queued completion into it. The batcher fails the writer if a completion was appended below the batch's nextSeq but left out ("forced durable batch … excludes appended completion"). So a writer's drain must hold appends from the moment it fixes nextSeq until the forced batch commits: local mode does it with `drainMu`, and direct mode queues the checkpoint behind earlier blocks and holds appends until it runs. Direct mode's first version let appends continue, and only a 60s benchmark caught it, because backfill's periodic drain fires every 30s and no oracle run lasts that long. A new writer mode needs the same hold, and a test that drains beside concurrent appends. Area: `internal/ingest/direct.go`, `internal/ingest/backfill/completion_batcher.go`, `internal/ingest/maintainer/direct_test.go` (`TestDirect_DrainHoldsAppends`).

### A storage call under a third-party mutex wedges the layer 3 scheduler

`storagefake.Seeded` admits a parked call only after `synctest.Wait`, and synctest does not count a goroutine waiting on a `sync.Mutex` as durably blocked. If one goroutine parks at a yield point while holding a mutex that another goroutine wants, the bubble hangs until the child times out. It looks like a hang, not a failure. Jetstream code must not hold a mutex across a storage or blob call. The atmos verifier holds a per-DID lock across syncstate loads, which is why the fake's meta reads take no scheduler turn. If you add a yield point, check who can call it while holding a lock. Area: `internal/storagefake/sched.go`, `meta.go`, `specs/oracle/2026-09-25-disagg-scheduler-wedged-by-verifier-mutex.md`.

The same rule applies to locks held across a storage call. The backfill `Store`'s `countsMu` and `rosterMu` span catalog reads, so they are channel-based (`chanMutex`), which synctest counts as durably blocked; so do its `groupCommitter`, whose followers wait on channels, and its `commitPipe`, whose writes wait their commit turn on channels. atmos backfill used to hold a per-DID-shard `sync.Mutex` across the Store's discovery write. Its per-DID locks are channel-based since the Store callbacks became batched (atmos `TestLockDIDs_WaiterIsDurablyBlocked` pins this), but the layer 3 oracle still runs with `BackfillMaxActiveHosts = 1`. Only one reconcile runs at a time, so none waits on that shard. With one active host, atmos lists a host fully before dispatching its downloads. A fault that must fire mid-bootstrap needs the listing held elsewhere, which is what the lifecycle harness's host gate (`disaggListGate`) does. Area: `internal/ingest/backfill/store.go`, `internal/oracle/disagg_oracle_test.go`, `internal/oracle/disagg_lifecycle_test.go`.

### In disaggregated mode a per-key metadata call is a WAN round trip

Code written against local Pebble treats a metadata `Get` or a one-row commit as free. Against PostgreSQL in another region a `Get` costs 10–25 ms, and a commit is a fenced catalog transaction (BEGIN, fence bump, apply, NOTIFY, COMMIT) of about 80 ms. The fence bump row-locks `archive`, so a leader's write transactions serialize. The first pop2 disaggregated bootstrap (2026-10-02) spent 12 minutes enumerating 6,240 relay hosts at three serial round trips each before downloading anything. Then it discovered about 8 repos per second, because every discovery held `countsMu` across four reads and a commit, and the completion staging behind the same lock stalled the writer. Rules: read through `metastore.GetMany` or a prefetched `metaView` instead of `Get` in a loop. Fold a page's aggregate updates into one write. Let concurrent writers share a transaction (`groupCommitter`). Never hold a lock across per-key round trips, or across a commit: stage under the lock and commit in order through `commitPipe` (next entry). `TestStore_BatchRoundTrips` pins the budgets. atmos's Store takes its enumeration callbacks a page at a time for this reason. Area: `internal/ingest/backfill/store.go`, `metaview.go`, `internal/metastore`, `internal/pgstore/meta.go`.

### Releasing a lock before its commit needs ordered commits and taint tracking

The backfill `Store` stages read-modify-writes of the shared aggregate rows (counts, host aggregates, roster) under `countsMu` but commits them after releasing it, so the segment writer's completion staging no longer waits out a discovery's catalog transaction (`commitPipe`). That is only safe with three rules, each pinned by a test. A write staged after another reads the other's uncommitted rows through the pipe, or it loses that update. It commits only after the write ahead of it committed, or a later commit can overwrite an earlier one's aggregates with stale values. A write never commits if any write it read failed. The third rule is the subtle one: writes finish outside the lock, so a staging can read a write's rows, then see that write fail and leave the pipe before taking its own ticket, and its ticket then waits behind nothing. The first version missed this; only the seeded swarm with injected commit failures (`TestStore_CommitPipeSwarm`, which recounts every aggregate from the rows) caught it, as counts running ahead of rows. Each staging now records the tickets it read from, and `stage` refuses it if one failed. Readers outside the lock (Lookup, host cursors, status) still read committed rows: they act on what they read with no commit to chain it to, so a dirty read could skip data. Area: `internal/ingest/backfill/commitpipe.go`, `store.go` (`writeLocked`, `stageDurableBatch`).

### A durable batch staged behind an open one must not carry what the open one carries

Direct mode stages each block group's durable batch while the group before it commits (`Writer.PipelinesDurableBatches`). The backfill completion batcher removes a completion or host cursor from its queue only in the batch's afterCommit, so an open batch's entries are still queued while the next batch stages. Staged again, a completion would complete twice, and a host cursor would write its stale state over any write to the host row that landed between the two batches. The batcher tracks open batches (`inflight`) and skips what they carry. A cursor whose covered completion is in the open batch may ride in the next one: that batch commits first, or the writer fails and neither commits. The store's hook guard admits one batch staged ahead, and only for a pipelined writer. Hot mode's group commit calls the hook twice before committing either, so the second call would wait on the first forever. Area: `internal/ingest/backfill/completion_batcher.go`, `store.go` (`stageDurableBatch`), `internal/ingest/direct.go`.

### Don't wake every waiter when only one can proceed

The direct writer used to wake waiting appends by closing one shared channel. In pop2 about 500 backfill appends waited on it. Every committed block woke all of them, one could proceed, and the rest took the writer's mutex in turn only to wait again. The committer and stager need that mutex for every item, so they queued behind the crowd. They spent about 45% of their time waiting for the mutex or for a CPU, and the leader ran with GOMAXPROCS 256. Appends now wait in a FIFO `waitQueue`: a commit wakes the oldest, and each append wakes the next if it left room. Two details are easy to get wrong. A waiter whose context ends just after a signal chose it must pass the turn on, or the room sits unused while the next waiter waits. A woken waiter that finds the room gone queues again at the front. Failure and Close still wake everyone. Hot mode admission still broadcasts, because its conditions differ by class. Area: `internal/ingest/waitqueue.go`, `internal/ingest/direct.go` (`awaitRoomLocked`).

### A group commit stops where the rotation rule fires

The direct writer commits a group of ready blocks in one transaction, but the rotation rule belongs to each block: the segment seals right after the block that takes its framed bytes to the threshold. A group that ran past that block would put the rest in the full segment, and a seal would build its footer over more than the threshold, so segments would no longer end on the same blocks as local mode's. The committer asks the sealer how many blocks fit (`SegmentSealer.BlocksBeforeRotation`), commits those, seals, and commits the rest in a second transaction. The group's hook output belongs to its last block, so it rides in the last transaction: committed with the first, it would mark completions durable before their blocks were. `TestDirect_GroupCommitSplitsAtRotation` compares the segments against one commit per block. Area: `internal/ingest/direct.go` (`commitBlocks`), `internal/ingest/maintainer/segment.go`.

### Backfill workers per host are not a rate limit

A Bluesky PDS allows 6000 `getRepo` calls per IP per five-minute fixed window, separately on each hostname; other PDSes set their own limits, which atmos reads from each response's `RateLimit-*` headers. Workers per host only set the rate through download time. When v0.3.5's faster writer let pop2 pull about 620 repos/s at 32 workers per host, the big bsky.network hosts spent their windows in about two minutes. Each repo after that took a 429, waited at most 30 seconds, took another, and failed, at about 1,000 repos a minute. atmos v0.7.0 parks a host whose quota is spent until its reset (up to `RateLimitMaxWait`) without holding a download slot, keeps 5% of each quota for other clients from the same address (the live consumer's resyncs), and gives a repo three rate-limit retries. A retry pass skips a candidate whose PDS client is parked instead of waiting inside the download: the steady loop defers it, and merge's pending pass requeues it. So once a window is spent, extra workers only spend the next one sooner. Area: `internal/ingest/backfill/run.go`, `retry.go` (`clientParkedUntil`), atmos `backfill` and `xrpc`.

### A deposed leader still reaches crash seams

A killed or deposed leader keeps running until its context and client die, and it can reach an armed crashpoint after its successor has started. If the old session takes the seam, the kill lands on a process that is already dead, the fault never exercises the new leader, and the harness waits for a crash that doesn't come. The layer 3 injector only fires for the pod that started the newest session (`newestSession`). Any new harness that arms seams across leader changes needs the same check. Area: `internal/oracle/disagg_oracle_test.go` (`disaggCrashInjector`).

### Sealed blocks have holes; hot batches and active blocks don't

Compaction keeps each block's [MinSeq, MaxSeq] envelope and drops rows inside it, so a sealed block can skip seqs. A follower whose last tick predates a seal and a compaction pass reads the missing seqs from the compacted block, not from hot batches. Anything that walks refs seq by seq must allow holes inside a sealed ref's envelope (`BlockRef.Generation != 0`) and still demand density from hot batches and active blocks, where a hole really is corruption. The follower's readable log leaves those seqs vacant (nil entries) and every reader skips them. The first version treated the hole as corruption and stopped the follower. Only the layer 3 oracle's held reader, which refreshes rarely, caught it. Area: `internal/catalog/follower/tick.go` (`feed`), `internal/ingest/readlog.go`, `specs/oracle/2026-09-26-disagg-follower-compaction-hole.md`.

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

A mutant patch whose hunk has no context lines is anchored only by its line number. When the target file later drifts (code added above the hunk), the *forward* `git apply --unidiff-zero` still lands correctly via git's offset search — the campaign runs, the mutant is KILLED, the gate reports PASS — but the *reverse* apply of a pure deletion is a pure insertion with no content anchor, so git re-inserts the lines at the stale line number, silently corrupting an unrelated spot in the file **after** the PASS (found twice as `merge.go` residue after gate runs; #305). The driver cannot catch this: the revert "succeeds," so `revert_current`'s failure path never fires. The forward apply can go wrong too. If the file has an identical line near the stale line number, the zero-context hunk lands there instead of on its target. The mutant then SURVIVES, which looks like an oracle gap, not a patch problem: m044 did this after S1.7 moved `flushLocked`, landing on the identical fault check in `commitPreparedFlushLocked`. Lessons: (1) generate mutant patches as standard context diffs (`git diff`, 3 context lines) — content anchoring makes both directions drift-safe; (2) check `git status` after any campaign/gate run before building on the tree; (3) when refreshing a mutant, verify the round-trip leaves `git status` clean: `git apply <p> && git apply -R <p>`. On 2026-09-29 the last 13 zero-context patches were refreshed. Two of them, m027 and m060, had drifted onto identical lines in sibling functions and kept reporting KILLED while mutating the wrong code. The driver now applies patches without `--unidiff-zero`, so a zero-context patch reports STALE, and `TestCatalogPatchesCarryContext` rejects one in the default `just` run. A consequence: plain `git apply --check` is now the right staleness check. Before the refresh it flagged every zero-context patch, applicable or not. Area: `testing/mutation/mutants/*.patch`, `testing/mutation/run.sh`, `testing/mutation/gate/catalog_test.go`.

### The mutation driver reports BUILD-BROKEN from a git worktree

Inside a `git worktree`, under some sandboxes, Go's VCS stamping fails with "error obtaining VCS status: exit status 128" even though git itself works. When that happens, every mutant reports BUILD-BROKEN, which says nothing about the mutants. Run the driver with `GOFLAGS=-buildvcs=false` in that case. Area: `testing/mutation/run.sh`.
