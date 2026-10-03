# Disaggregated backfill: pipelined catalog transactions and a commit pipe

2026-10-02. Branch `jc/commit-pipeline`. This follows `2026-10-02-disagg-backfill-batching.md`.

## What pop2 showed on v0.3.1

Batching discovery fixed enumeration: discovery ran at thousands of repos a second. Downloads were about 47 repos a second, about 12,600 events a second. On the leader:

- About 1,900 `HandleRepo` goroutines were parked in `directWriter.waitLocked`.
- The backfill writer's commit loop committed about 3 blocks a second, at about 4,000 events each.
- The leader used 0.8 of its 8 cores.
- The pool was idle: 2 of 32 connections in use.

Sampling the commit loop 62 times gave this breakdown:

| Share | Where |
|---|---|
| 52% | the `CommitBlock` catalog transaction |
| 27% | waiting for `countsMu`, held by discovery group commits across their own transaction |
| 11% | sealing a full segment inside the commit loop |
| 8% | the hook's prefetch reads |

`CommitBlock` was 12 round trips at about 16 ms each:

- BEGIN and the fence;
- the seq lock, the active segment, and the last block;
- the dedup lookup, marking the object available, and the ref check;
- the insert, the metadata apply, NOTIFY, and COMMIT.

## Change 1: pipelined leader transactions (pgstore)

`pgstore.tx` no longer sends each statement alone.

- BEGIN is queued, and goes in the same round trip as the first statement, which is always the fence.
- Some primitives return only an error and write: `ApplyMeta`, `InsertHotBatch`, `InsertActiveBlock`, `InsertGenerationBlocks`, `InsertSegment`, `DeleteNamespace`, and `Notify`. These are queued too. They go out with the next statement that returns a result, or with COMMIT.

PostgreSQL runs a pipeline in order and aborts the transaction at the first failure, skipping the rest, so what commits is unchanged. What changes is which call reports a queued write's failure. The `catalog.Tx` contract now says that call is the next one, at the latest `Commit`. The error is named for the statement that failed, for example `insert_hot_batch (sent with meta_get_for_update)`. Scripts treat every error alike.

storagefake reports queued-write failures the same way, so the layer 3 oracle runs with the semantics PostgreSQL has.

| | before | after |
|---|---|---|
| `CommitMeta` round trips | 5 | 2 |
| `CommitBlock` round trips | 12 | 8 |

The `archive` row lock is held from the fence to COMMIT, so a metadata commit now holds it for one round trip instead of four. That shortens how long every other leader write waits for it.

Tests:

- `TestPipelinedRoundTrips` counts round trips through the pgtest proxy.
- The proxy now recognizes a COMMIT sent through the extended protocol, so `TestCommitResultUnknown` still applies.
- The contract suite gained `QueuedWrites`. It covers reads that see queued writes, a failure reported by each kind of later call, and nothing queued around a failure committing.
- `failing` now also requires the failed transaction not to commit.

That stricter `failing` exposed a contract case that had never tested what it claimed. The GC "forget a referenced object" case failed on its own precondition: the mark count was 3, not 1, because two earlier rows were also unreferenced. So the forget never ran. It does now.

## Change 2: the backfill commit pipe

`countsMu` used to be held from a read-modify-write's first read to the end of its commit. That covered discovery group commits, the durable-batch hook (`stageDurableBatch`, across the whole `CommitBlock`), OnFail, and the rest. Now:

- Every such write stages under the lock, reading through a `commitPipe` that overlays the uncommitted rows of writes staged ahead of it.
- The write takes a ticket, releases the lock, waits for the ticket ahead of it, and commits. Commits stay strictly in staging order.
- A write whose predecessor failed does not commit. A write that read the rows of a failed write does not commit either. That case is a race: the failed write can leave the pipe before the reader takes its ticket. See `specs/gotchas.md`.
- Readers outside the lock still see only committed rows.

The hook stages again when a write it built on fails, rather than fail the segment writer over another write's commit. It refuses re-entry: hot mode's multi-batch group commit would make it wait on its own uncommitted write.

Tests:

- **Unit:** ordering, failure isolation, refusing a tainted read, range deletes, and synctest-durable waiting.
- **Gated-commit scenarios:**
  - staging proceeds during a held commit, for OnFail and for the hook;
  - a failed commit fails only the writes staged on it;
  - the hook stages again after a failed predecessor;
  - the hook refuses re-entry.
- **`TestStore_CommitPipeSwarm`:** 12 seeds, tried once at 300. It runs concurrent discovery pages, Active flips, failures, host-state writes, and a segment-writer loop, with seeded commit delays and 10% commit failures. Every aggregate must match a recount of the rows.
- **Self-mutation:** each of these mutations is caught:
  - ignoring a failed predecessor;
  - skipping the taint check;
  - reads ignoring staged rows;
  - dropping commit ordering;
  - the hook not restaging.

## Expected effect

Per block, the commit loop no longer waits out discovery's prefetch and transaction under the lock. It waits only for a discovery commit already in flight, now about 2 round trips. The block transaction itself is 8 round trips instead of 12. Measure on pop2:

- `jetstream_ingest_blocks_flushed_total`;
- `jetstream_backfill_completed_total`;
- the commit-loop goroutine samples.

## Follow-ups

- **Bigger blocks during backfill.** Throughput is commits a second times events per block. Block size is part of the segment format and sets per-block client download size, so check it against `docs/README.md`.
- **Seal off the commit path.** Sealing was 11% of the commit loop. `RotateIfFull` seals inside the commit loop, so block commits wait for the seal to finish.
- **Overlap consecutive block commits.** The hook for block n+1 could stage while block n commits. The pipe already orders the commits.
- **Fewer reads in `CommitBlock`.** The seq lock and the active segment are independent reads. A primitive that returns both, or that returns the segment with its last block, would save round trips without moving a check out of the scripts.
