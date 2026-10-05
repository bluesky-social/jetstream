# Disaggregated merge: batch round trips, and fix the pending pass

2026-10-05. Branch `jc/disagg-merge-batching`.

## What pop2 showed

pop2 was near the end of its first disaggregated bootstrap: 95.6M events in 23,340 bootstrap_live blocks, 1.94M rows left `pending` by bootstrap restarts (#262), and 507k `failed`. The bootstrap tail was fine, bounded by PDS 429s. Read-only audits of what runs after it found that merge was slow only because of our code. Every serial PostgreSQL round trip costs about 14–19 ms (`pg_txn{kind="meta_read"}` p10 14 ms).

- **The merge drain read one repo row per distinct DID, serially** (`repoStatusLookup.get`). At 2–3M DIDs that is 6–16 hours.
- **`commitSourceComplete` upserted a segment's revs in one statement**, about 200k rows against a 10 s `statement_timeout`. A timeout would fail the same segment on every retry and wedge merge.
- **Every leader object read cost three round trips** (BEGIN, SELECT, ROLLBACK through `protocol.DBRows`) before the S3 GET, even on a cache hit. This hit the merge drain, compaction, the sealer, and the maintainer.
- **The pending pass ran at about 25–35 repos/s and stranded rows.**
  - Each repo paid a `DrainDurability` barrier and its own `OnComplete` transaction.
  - A 429 parked the whole host for the retry interval (4–6 h). The host's other rows got `DeferRetryAttempt` and stayed `pending`, which the steady loop never selects.
- **Merge-tail compaction**:
  - collected tombstones serially over 23k blocks;
  - ran `min(NumCPU, 8)` workers;
  - went dense (read the whole 1.6 TiB archive) past 4.19M probes.
- **Merge discovery** made about 2 serialized commits per host through the backfill store's pipe.

## Change

One commit per item.

- **Leader object reads go through the follower mirror** (`jetstreamd/disagg.go`). `Follower.Object` falls back to the database on a miss and on a verification-failure refresh, and the mirror lagged about 0.2 s with no refresh errors.
- **`pgstore.MetaBatch` splits runs of Sets or Deletes into 10k-row statements** inside the same batch and transaction, so `commitSourceComplete` stays atomic.
- **Merge drain** (`orchestrator/merge_runner.go`, `merge_filter.go`):
  - Before each block, it reads the uncached DIDs of the block's rev-filtered events in one `GetMany` (chunks of 10k). Only those kinds consult the row; identity and account events never do.
  - In disaggregated mode it fetches and decodes up to 8 blocks ahead, delivered in order. Local mode stays serial.
- **The PostgreSQL iterator reads one page ahead** (`metastore/pg`), which halves the `repo/` scans.
- **Merge-tail compaction** (`compact_deletes.go`, `compact_disagg.go`):
  - Tombstone collection folds a segment's blocks 16-way.
  - Disaggregated mode defaults to 32 rewrite workers.
  - The sparse probe limit becomes 1<<26, because there a skipped block saves an S3 GET, not a decode.
  - Dense fallbacks are counted (`jetstream_compaction_dense_rewrites_total`).
- **Discovery's per-host writes group-commit** (`updateHostStateDirect` through `commitGrouped`).
- **The pending pass** (`backfill/retry.go`, `retry_sched.go`):
  - It collects every candidate, then works through per-host queues (`retryScheduler`). The per-host limit is unchanged (`FailedRepoRetryHostWorkers`). Workers default to 256 (`JETSTREAM_PENDING_REPO_PASS_WORKERS`).
  - It ignores a pending row's `NextAttemptAt`, so rows stranded by earlier code are picked up.
  - **Rate limits.**
    - A 429 parks the host in memory until the server's reset, clamped to 10 minutes.
    - With no future reset, the park is 1 s, doubling per consecutive park up to 60 s, with jitter.
    - The candidate goes back on its host's queue.
    - After 8 rate-limited attempts, a repo is recorded `failed` with its normal backoff.
    - After 16 consecutive parks with no success, the host's remaining repos are recorded `failed` (`failed_repo_retry_hosts_abandoned_total`).
    - No row is left `pending`.
  - **Completions ride the writer's block commits**, as in bootstrap (completion batcher plus durable batch hook). A periodic drain cuts a block when the pass goes quiet, and one final drain runs at the end. A relay-fallback PDS re-stamp travels with the queued completion.
  - **Failures and deferrals commit 256 at a time** (`recordRetryFailures`, `deferRetryAttempts`): one read and one transaction per batch. Terminal answers keep `OnFail`'s semantics. A row still unwritten at a crash keeps its old status, and the next pass selects it again.
  - Progress shows in `failed_repo_retry_queue_remaining` and `failed_repo_retry_requeued_total`.
- **The steady loop shares** the scheduler, the batched writes, and the short host parks (a row's own backoff is unchanged). Its first pass now runs `min(Interval, 5m)` after start rather than a full interval.

## Correctness evidence

- `TestMergeRunner_ReadAheadMatchesSerial` checks 8 seeds: identical output events and repo rows with read-ahead and prefill as without.
- `TestMergeRunner_BatchesRepoLookups` pins zero point reads, at most one `GetMany` per block, one read per DID, and no reads for identity- or account-only DIDs. `TestMergeRunner_ReadErrorStopsDrain` checks that a failed block read leaves the cursor where it was.
- `TestMetaBatchSplitsLongRuns` covers 9,999 / 10,000 / 10,001 / 20,001 rows, and mixed runs.
- `TestStore_DirectHostStateGroupCommit` requires the sequential keyspace with fewer commits.
- `TestStore_RecordRetryFailuresMatchesSequential` requires a batched keyspace identical to one-at-a-time writes, covering terminal answers and raced completions. A missing row fails the whole batch and writes nothing.
- `TestRunPendingRepoRetryPass_WaitsOutRateLimits` uses 429s with no reset and with a reset in the past.
  - It checks: no row left pending, a host that never clears abandoned and its repos failed, a deferred pending row still attempted, and fewer commits than completed repos.
  - With the completion batcher unhooked, the commit count goes from under 16 to 34, and the test catches it.
- `TestRunPendingRepoRetryPass_BatchesFailures` gives 600 failures at most 5 commits.
- `TestRetryRunner_RateLimitParkBackoff` pins the park schedule, the reset after a success, and both modes' reset clamps.

## Follow-ups

- **Steady-state completion batching on the hot writer.** The steady loop still drains durability and commits once per repo. That is fine at its volume, but it could stage completions whose last seq is at or below `ReadLog().DurableSeq()`.
- **Steady-state catch-up after merge.** Once `promotedChain` is flushed, the live verifier's `LoadChain` makes a PostgreSQL `Get` per commit, through atmos's 32 parallel workers (about 1.9k commits/s). Measure the catch-up rate after cutover before building a chain cache.
- **Bootstrap `OnFail`** is still a serialized write per repo. It runs at about 3/s, which is fine.
- **`MetaScan`'s generic plan.** Run `EXPLAIN (GENERIC_PLAN)` to confirm that `($2 IS NULL OR key < $2)` stays an index bound and does not become a filter.
- **Relay retention.** Confirm that relay1's replay window is longer than the expected merge duration.
- **In-segment prefetch for the sparse rewrite.** This was planned and dropped: 32 workers already overlap S3 reads across segments. Add it if one huge segment dominates a merge-tail pass.
