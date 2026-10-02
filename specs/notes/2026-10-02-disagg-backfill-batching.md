# Disaggregated backfill: batch metadata round trips

2026-10-02. Branch `jc/pg-batch-backfill`, with atmos v0.6.0 (jcalabro/atmos#12).

## What pop2 showed

The first disaggregated full-network bootstrap in pop2 (v0.3.0, c5c5b38) had two problems.

- **No downloads for 12 minutes.** atmos enumerated the relay's 6,240 hosts one at a time, with `OnHost` (a read and a fenced commit) and then `HostCursor` (a read) for each. That is about 120 ms per host against RDS in us-east-2, or about 9 hosts per second.
- **About 8 repos per second after that.** 176 host producers were queued on `countsMu` in `putRepoStatusAndCounts`. Each discovery held the lock across 4 serial PostgreSQL round trips: the counts, host-status, and roster reads, then a `CommitMeta` transaction. `stageDurableBatch` takes the same lock and read one row per completion, so the writer's commit loop stalled and 98 `HandleRepo` goroutines backed up in `directWriter.waitLocked`. CPU sat at 0.1–0.3 cores. The process was waiting on latency the whole time.

A metadata commit is BEGIN, a fence bump that row-locks `archive`, the apply, NOTIFY, and COMMIT. That is about 80 ms, and the fence lock serializes every leader write transaction. Commit count, not row count, bounds throughput.

## Change

- **atmos `backfill.Store` takes batches.** This is a breaking change (atmos is pre-1.0). `Lookup`, `OnDiscover`, `OnUpdate`, `OnHost`, and `HostCursor` now take a slice, and a batch of one behaves like the old per-entry call. An earlier draft added an optional `BatchStore` beside the per-entry `Store`; it was replaced so there is one interface and no second code path. The engine reconciles each run of distinct DIDs in a listRepos page with one `Lookup`, then at most one `OnDiscover` and one `OnUpdate`. It records each listHosts page with one `OnHost` and one `HostCursor`. A page that repeats a DID splits at the repeat, which keeps per-entry semantics. The per-DID shard `sync.Mutex`es became exact per-DID channel locks, taken in sorted order. Only identical DIDs contend, and waiters count as durably blocked under synctest.
- **`metastore.Store.GetMany`** reads many keys in one round trip. The PostgreSQL backend uses a single `key = ANY($1)` query. It is part of the storetest contract.
- **The backfill `Store`** implements the batched callbacks.
  - `metaView` is a read-through view that serves prefetched keys and the writes staged earlier in the same batch.
  - `groupCommitter` folds concurrent hosts' page writes into one transaction. Its followers wait on channels.
  - Every locked read-modify-write now prefetches its keys: `stageDurableBatch`, `updateRepoStatusesAndCounts`, and `updateRepoHostActive`.
  - Merge discovery's `discoveryStore` gets the same batch path.
- **Pool:** `JETSTREAM_PG_MAX_CONNS` now defaults to 32 (was 16). The pool keeps a floor of MaxConns/8 warm connections, recycles connections after an hour with 10 minutes of jitter, and exports `jetstream_pg_pool_*` metrics.

Round trips per listRepos page of N entries:

| | before | after |
|---|---|---|
| reads | about 4N (lookup, counts, host status, roster) | 2 (`Lookup`, one prefetch) |
| commits | N | at most 2 (discoveries, then Active flips), often shared with other hosts |

Round trips per listHosts page: 3 per host before. After, `OnHost` makes 1 read and at most 1 shared commit, and `HostCursor` makes 1 read.

## Correctness evidence

- `TestStore_BatchMatchesPerEntry` checks 40 seeds. It requires identical lookups and an identical keyspace from batches of one and whole-page batches, across:
  - prior rows in every status;
  - interrupted `not_started` rows;
  - host moves and Active flips;
  - repeated DIDs and pre-existing roster rows.
- `TestStore_ConcurrentHostsGroupCommit` runs 16 hosts concurrently with group commits and requires the same keyspace as a sequential run in batches of one.
- `TestStore_BatchRoundTrips` pins the read and commit budgets.
- atmos checks its batch contract (non-empty, no duplicate DIDs or hostnames, discover before update) on every call in its test store. It runs `TestStore_ReconcilesWholePages`, `TestStore_HostRosterBatches`, and the seeded `TestStore_FleetSwarm` (cross-host duplicates: each DID discovered, completed, and downloaded once). It also has lock tests for exclusion, deadlock freedom, and the synctest durable-block property.
- The mutants that targeted the moved code are refreshed: m053 now targets `bootstrapHostCursor` and m055 targets `discoverMode.newRow`.

One fixture lesson: aggregate decrements clamp at zero. A test that seeds repo rows without matching host aggregates makes legitimately reordered writes compare unequal. Within a chunk, the batch path defers interrupted rows before it stages discoveries.

## Follow-ups

- **Pipeline the catalog transaction.** Each commit makes 5 round trips (BEGIN, fence, apply, NOTIFY, COMMIT), and the `archive` row lock is held for 4 of them. Sending BEGIN with the fence, and the apply with NOTIFY and COMMIT, would cut both commit latency and lock hold time for every leader write, block commits included.
- **Cache the verifier's chain reads.** The live verifier does one `syncstate` `MetaGet` per commit, about 300 per second. A cache in front of `LoadChain` would cut that load.
- **Revisit the layer 3 single-host setting.** `BackfillMaxActiveHosts = 1` in layer 3 no longer has to protect against atmos's mutex. An 8-host experiment passed 20 seeds, but so did unmodified main, because the simulated world is too small to collide. Raising the setting needs a mutation re-run.
