# Compaction publish: hold the archive lock for one round trip

2026-10-13. Branch `jc/compaction-publish-one-round-trip`.

## What pop2 showed

During a steady-state delete compaction pass, the pop2 live tail went from smooth to choppy. The pass runs every 4 h (`JETSTREAM_COMPACTION_INTERVAL`), about 25 minutes per pass, and this one published 6,855 generations with `JETSTREAM_COMPACTION_REWRITE_WORKERS=8`. Subscribers on the public and internal endpoints, with both websocat and a Go client, saw the same thing:

| | latency from `witnessedAt`, p50 / p90 | empty 100 ms windows | shape |
|---|---|---|---|
| during the pass (18:46–18:52 UTC) | 0.44–2.3 s / 1.2–4.3 s | 55–82 % | 300–1,000 events in a pulse about once a second |
| after it (18:54–19:05 UTC) | 65 ms / 101 ms | 1 % | about 24 small bursts a second |

- **The pass and the stalls line up.** `jetstream_catalog_commit_fallbacks_total{kind="hot_batch"}` ran at 100–126/min during the pass, against a baseline of 6–10/min. The leader used 10–12 cores, against a baseline of about 4. Both fell back the moment the leader logged `delete compaction pass complete`.
- **The followers were not behind.** `jetstream_catalog_lag_seconds` stayed at 30–150 ms throughout.

Every leader transaction holds the `archive` row lock from the fence to COMMIT, and the inline hot batch commit that gates every live event is one of them. `Session.PublishGeneration` made 8 round trips, 7 of them after the fence:

1. BEGIN, the fence, and the object reads.
2. `SegmentForUpdate`.
3. `Generation`.
4. `BlocksOfGeneration`.
5. `InsertGeneration ... RETURNING`.
6. `SetSegmentGeneration`, which needs its command tag.
7. `DeleteGeneration`, which also needs its command tag.
8. COMMIT.

At about 17 ms each to RDS, each publish held the lock for about 120 ms. With 8 rewrite workers, a hot batch could queue behind several publishes at once, then release a second's worth of events together.

## Change

`PublishGeneration` is now 2 round trips, 1 of them after the fence. Each publish holds the lock for COMMIT's round trip alone, about 17 ms on pop2.

- **Every read rides with the fence** as a `catalog.Read`:
  - `SegmentRead`: the segment row, row-locked.
  - `GenerationRead` and `GenerationBlocksRead`: the source generation and its blocks. Their ID is in the `Publish`.
  - `NewGenerationIDRead`: reserves the new generation's ID with `nextval`, so nothing waits for `RETURNING`.
  - These replace the `Tx` methods `SegmentForUpdate`, `Generation`, and `BlocksOfGeneration`.
- **Every write is queued.** `InsertGenerationAt` inserts the generation at the reserved ID.
- **`SetSegmentGeneration` and `DeleteGeneration` are now guarded writes, so the corruption checks survive:**
  - Each is queued, and wrapped in the fence's `1 / count(*)` trick, so changing no row aborts the pipeline before COMMIT.
  - pgstore and storagefake report the failure as `ErrPrecondition`.
  - The script has already checked those rows under the segment's row lock. So in `Session.run`, a failed guard is `SourceInvariant` corruption, and nothing commits.
- **A reserved ID is not transactional.** One a rolled-back publish took is never used, which is harmless, and `InsertGeneration` draws from the same sequence.

Seal is unchanged. It runs once per segment rather than thousands of times per pass, and it still takes 6 round trips. `TestScripts_PublishGenerationHoldsTheLockForCommitOnly` pins both shapes.

## Tests

- **`TestScripts_PublishGenerationHoldsTheLockForCommitOnly`** runs on storagefake in every `just`, and on PostgreSQL under `just test-storage`.
  - It wraps `Tx` method by method and records each call that waits for a round trip. A publish must be exactly `FenceBump`, `Commit`, with or without reused blocks.
  - A new `Tx` method does not compile until the wrapper classifies it.
- **`TestScripts_PublishGenerationRoundTripsPG`** counts round trips through `pgtest.Proxy`. It is 2 on this branch; the same test on `main` counted 8.
- **`TestScripts_PublishGenerationGuardIsCorruption`** sends each guarded write to a missing row. The publish must be corruption, commit nothing, and leave the next publish working.
- **`catalogtest` (both backends):**
  - `ReplaceGeneration` covers the new reads, the reservation (unique, never reused after a rollback, shared with `InsertGeneration`), and the `InsertGenerationAt` constraints.
  - `GuardedWrites` covers every way a guard fails, whether Commit or a later Read reports it, and checks that nothing queued around it commits and that the lock is released.
  - `FenceReads` now also checks that a stale fence's `SegmentRead` locks nothing.

Mutations checked by hand:
- A guard that silently succeeds fails both the contract and the script test.
- One extra `Read` in the publish fails the call-shape test, and fails the proxy count as 3 round trips instead of 2.

## Not covered

- **Fewer rewrite workers would still shorten the queue** a hot batch can wait behind.
- **A leader-side priority gate** that lets hot batch commits go first remains an option if the pass still shows up in live latency.
- **A separate 1–5 s witness stall**, about twice per 10 minutes, was not root-caused. Some of these stalls end exactly when the consumer logs `dropped stale-ordered async resync`. That suggests head-of-line blocking behind a slow resync fetch in the atmos verifier.
