# Live tail latency: fewer catalog round trips

2026-10-08. Branch `jc/live-tail-latency`.

## What pop2 showed

This was the first load test of the steady-state disaggregated deployment (v0.3.8, 3 pods, PostgreSQL on RDS about 15–18 ms away). A harness on jimvm subscribed to both the relay and jetstream and matched commits by `(did, rev)`. Relay to subscriber took p10 183 ms, p50 241 ms, p99 330 ms. Nearly all of it was serial round trips to the catalog:

- **The follower tick made 8–9 serial round trips**: BEGIN, Archive, MetaGet, SegmentsSince, ActiveBlocksSince, ActiveBlockKeys, HotBatches, ROLLBACK, and Objects when needed.
  - That is about 150 ms, back to back, about 8 ticks a second.
  - An event waited out the tick in progress and then a whole one of its own.
- **The leader's `hot_batch` transaction made 2 round trips** (38 ms p50).
  - The first exists only to bring the seq row back for `checkSeq`.
  - The `archive` row lock was held across the second. That lock serializes every leader transaction, and GC transactions holding it caused the 10-07 `hot_batch/fence` lock timeouts.
- **`syncstate.LoadChain` read the catalog for most commits, about 414 a second.**
  - `CommitStaged` dropped each DID's state once its batch committed, so the next commit from that DID missed.
  - The read happens before the event is witnessed. 32 verifier workers each waiting about 17 ms per read cap ingest near 1.9k commits a second; the 7-day max `meta_read` rate was 1,971/s.
- **The Go client flushed partial live batches on a free-running 20 ms ticker**, with no option to change it.

## Change

- **The follower's incremental tick is one round trip** (`catalog.DB.ReadChanges`, `pgstore/changes.go`).
  - BEGIN REPEATABLE READ, every read, and COMMIT go in one pipeline, so they share BEGIN's snapshot.
  - Reads that depended on an earlier result compute it as a subquery: generations of the changed sealed segments, and objects referenced by those, by new active blocks, and by pointer hot batches.
  - Statements without a revision filter of their own are guarded by `catalog_revision > $since`, so an unchanged tick reads nothing.
  - More than `MaxChangedGenerations` (256) changed sealed segments reports `Overflow`. The follower then uses the old chunked `readTx` path, as it does for the first load and for a phase change.
  - `catalog.ReadChangesTx` is the reference built from `ReadTx` statements. storagefake uses it, and the catalogtest contract compares pgstore against it.
- **An inline hot batch commit is one round trip** (`Tx.FenceBumpAt`, `Session.runAt`).
  - The session tracks the highest revision it committed. The fence must take the next one (`catalog_revision = $rev - 1` in the UPDATE's WHERE).
  - The seq check moves into the database as a `1 / count(*)` statement, the fence's own trick.
  - With nothing for the script to read, BEGIN, the fence, the check, the inserts, the meta ops, NOTIFY, and COMMIT go in one pipeline. The lock is now held only while PostgreSQL executes them.
  - **On any precondition failure** (another transaction took the revision, a stale epoch, or a seq mismatch):
    - PostgreSQL skips the rest of the pipeline, so nothing applied and nothing stayed locked.
    - Commit returns `catalog.ErrPrecondition`. It does not end the session, because the result is known.
    - The script runs again the ordinary way, and that run tells the cases apart: `ErrFenced`, `SourceSeq` corruption, or success.
    - This is the only retry a session makes, and only of a transaction known not to have committed. `jetstream_catalog_commit_fallbacks_total{kind}` counts them.
  - Pointer batches still take two round trips: their object rows must be resolved first.
  - A new session does not know its revision, so its first commit takes the ordinary path.
- **Committed chain states are cached** (`syncstate.StateStore.committedChain`).
  - `CommitStaged` keeps what it committed in a bounded recent-use cache (about 262k entries, around 50 MB). LoadChain checks pending, then promoted, then the cache, then the store.
  - Nothing else writes `sync/chain/`. A StateStore lives for one writer session, and a failed commit ends that session, so the cache always equals the store for its DIDs.
  - `Delete` drops entries.
  - A superseded capture is still cached, because it is what the store holds; the newer promotion shadows it.
- **The Go client flushes when it drains a burst** (`live.go`).
  - A reader goroutine feeds frames to the decode loop through a queue of 8. When the queue is empty after an emit, the batcher flushes.
  - The ticker stays as the upper bound. `WithMaxBatchDelay` exposes it.

## Validation

- `TestPipelinedRoundTrips` pins ReadChanges and inline `CommitHotBatches` at one round trip each against PostgreSQL. `TestPreconditionKeepsConnection` checks that a failed precondition returns its connection to the pool instead of replacing it.
- catalogtest `ReadChanges` and `FenceBumpAt` run on both backends. `TestScripts_CommitHotBatchesFenceAt` covers the fast path, the fallback, and pointer batches.
- Mutation catalog: m067 was refreshed for the restructured script and m078 added for the in-database check. Both are unit-only.
- `just`, `just test-storage`, `just oracle-disagg`, `just test-long ./internal/oracle`, and `just oracle-sweep`.

## Result on pop2

Measured 2026-10-09 against v0.3.9 with the same harness. The run lasted 5 minutes and matched about 123k commits at about 430 events a second. The arithmetic predicted a p50 of 70–80 ms.

| Relay to subscriber, ms | p10 | p50 | p90 | p99 |
|---|---|---|---|---|
| v0.3.8, raw websocket | 183 | 241 | — | 330 |
| v0.3.9, raw websocket | 44 | 49 | 73 | 117 |
| v0.3.9, Go client | 49 | 52 | 76 | 121 |
| v0.3.9 server, pre-change Go client | 57 | 58 | 97 | 136 |

Server metrics, per minute across the rollout:

- **Follower visibility latency:**
  - p50 went from 200 to 49 ms.
  - p99 went from 491 to 127 ms.
- **Follower ticks:**
  - All of them now take `read_changes`, with a p50 of 19.5 ms.
  - About 25 a second per pod, up from about 8, because each tick is shorter.
  - `readTx` dropped to zero.
- **`hot_batch`:** p50 went from 38 to 19 ms.
- **Leader `meta_read`:** went from about 420 to about 96 a second, still falling as the chain cache warmed.
- **Leader CPU:** went from 0.27 to 0.17 cores.

The remaining ~49 ms is made of:

- about 2 ms of ingest before witnessing (it was ~36 ms),
- one round trip for the commit,
- one round trip for the tick,
- the wait for a tick already in progress.

That is near the floor for two serial round trips to a catalog 17 ms away.

**Fallbacks:** `jetstream_catalog_commit_fallbacks_total{kind="hot_batch"}` stays steady at 6–10 a minute on the leader, under 1% of hot batches.

- The leader's other transactions (fold, objects, metadata, seal) still take the ordinary path at about 0.6 a second. When one commits while a hot batch is in flight, it takes the revision `runAt` predicted.
- A hot batch is in flight about 40% of the time, which accounts for the rate.
- Serializing those transactions inside the session would remove the fallbacks. At this rate it is not worth doing.
