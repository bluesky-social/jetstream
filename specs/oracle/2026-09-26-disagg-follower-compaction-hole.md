# Oracle failure diary — a follower behind a compaction pass reports corruption

- **Date:** 2026-09-26
- **Commit (failure observed on):** `ab02b22` plus the uncommitted S4.4 compaction harness (branch `jc/new-storage`)
- **Test:** `TestDisagg_Oracle` (layer 3 disaggregated oracle), `-short` seed 1, in the held-reader check
- **Symptom:** the held reader's follower failed every refresh with
  `refresh: catalog: storage corruption (hot_batch): main block [108,115] holds seq 111 where 110 was due`
  and never advanced again. Every other check (streams, archives,
  `CheckCompacted`) had passed.
- **Classification:** production bug in the disaggregated catalog follower
  (`internal/catalog/follower`). Disaggregated mode only. It needs a follower
  whose previous tick is older than a seal plus a compaction pass over the
  sealed segment.
- **Status:** FIXED

## Repro

```
go test ./internal/oracle -short -run 'TestDisagg_Oracle$' -count=1
```

on the pre-fix tree. The deterministic unit repro is
`TestFollower_CompactedBetweenTicks` (`internal/catalog/follower`), which
fails on the old `feed` with `main block [1,6] holds seq 3 where 2 was due`.

## Analysis

- The held reader runs its own follower, which refreshes only when the
  harness polls it. Pods tick every 10ms, so they had not hit this.
- Between two of its ticks, the leader sealed the segment holding [108,115]
  and a compaction pass dropped seq 110 from that block. The next tick's
  `feed` read the missing seqs through `RefsFrom(log tip)`, which now yielded
  the compacted block.
- `feed` required every seq from the log's tip to the mirror's tip exactly
  once, because `FollowerLog` required strictly contiguous seqs. Design
  §11.4 had flagged this ("S4 revisits when compaction can remove seqs"), but
  S4.2 did not revisit it.
- The error is classified as corruption, so the follower stopped: a pod
  whose follower stalls once across a seal and a pass (a slow read, a
  PostgreSQL hiccup, a long GC pause) would stop following for good.

## Root cause

The follower treated a hole inside a sealed block's envelope as corruption.
Compaction keeps each block's [MinSeq, MaxSeq] envelope but drops rows inside
it, and the cold reader already allowed that; the follower and its readable
log did not.

## Fix

- `feed` allows missing seqs inside a sealed ref's envelope
  (`BlockRef.Generation != 0`) and still requires hot batches and active
  blocks to be dense, and every ref to end at its envelope's `MaxSeq`. It
  returns the tip the log moves to.
- `FollowerLog.Skip` advances the log's tip past vacant seqs (nil entries).
  `ReadableLog.ReadFrom` skips vacancies; a cursor with only vacancies above
  it waits at the tip. A leading vacancy run below durable is evicted
  regardless of the byte budget.
- Tests: `TestFollower_CompactedBetweenTicks`, `TestFollowerLog_Vacancies`,
  `TestFollowerLog_VacantFloorEvicts`.

## Verification

The short oracle passes on seeds 1-12, `just oracle-disagg` passes, 16 full
seeds pass, and an 8-seed race sweep passes.
