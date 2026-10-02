# Oracle failure diary — verifier sync state crosses a durable batch boundary

- **Date:** 2026-09-25
- **Commit (failure observed on):** `92ed03d` plus the uncommitted S2.18 harness (branch `jc/new-storage`)
- **Test:** `TestDisagg_OracleChild` (layer 3 disaggregated oracle, full mode, seeds 1, 2, 3, 5)
- **Symptom:** first, `not converged: seq/next 1xx, want 1xx`, with the catalog one or two rows short; seed 3 was missing upstream frame 54, a follow create. After the first fix: `catalog seq/next 106 past the 103 expected rows`, and the extra rows were whole events archived twice.
- **Classification:** production bug in live ingest (`internal/ingest/live`, `internal/ingest/syncstate`). It affects local mode too. It shows up only when a process dies between two durable batches.
- **Status:** FIXED

## Repro

```
JETSTREAM_ORACLE_DISAGG_CHILD=1 JETSTREAM_ORACLE_DISAGG_SEED=3 \
JETSTREAM_ORACLE_DISAGG_MODE=full go test ./internal/oracle \
  -run '^TestDisagg_OracleChild$' -count=1 -v
```

The deterministic unit repro is
`TestConsumer_Hot_ChainStateCommitsWithLastRow` (`internal/ingest/live`) for
the second half, and `TestStateStore_StageSnapshotExcludesLaterPromotions`
(`internal/ingest/syncstate`) for the first.

## Analysis

- Every failing seed had a leader crash or a lost commit in the failing wave.
  The `not delivered to pod` diagnostic named the missing model rows. The
  successor had received those frames again: its cursor was older. But the
  atmos verifier dropped them as rev replays, because the stored chain state
  was already at their rev.
- A durable batch holds a relay cursor, sampled when the batch is cut
  (`DurableBatchPrepareValue`). But `onDurableBatch` called
  `StateStore.StageFlush` when the batch *committed*. That flushed every
  promotion made up to commit time, including those for events in later
  batches that had not committed. After a crash, the chain state of an
  unarchived event was durable, and its replay was dropped. The row was lost.
- Fix 1 took a snapshot of the promoted state at cut time, next to the
  cursor (`durablePrepare`, `StateStore.Snapshot`/`StageSnapshot`). The oracle
  then found duplicates instead. `promoteSyncState` ran after `Append`
  returned, outside the writer mutex. A batch cut in that gap (a full block,
  the batch size, or the ager) held the event's rows but not its promotion.
  If the leader died before a later batch committed, the rows were durable
  under the old chain state. The event's seq can sit above a cursor held back
  by a slower event on another DID, so the event was redelivered and archived
  again in full.

## Root cause

Verifier state and the rows it describes must become durable in the same
batch. Two things broke that. The state was staged at commit time instead of
cut time, and promotion happened outside the writer mutex, after the append.

## Fix

- Each durable batch stages the promoted-state snapshot taken when it was cut,
  together with the cursor.
- Promotion runs in the writer's `OnAppend` hook for the event's last kept
  row, under the writer mutex (`Consumer.promoteAt`). The batch holding that
  row is the batch that makes the promotion durable.

What remains is design §10.4's at-least-once cursor: a batch cut inside one
upstream commit can leave that commit's prefix durable, and the successor
archives the whole commit again. The oracle allows exactly that, at most once
per leader change (`disaggCover`).

## Verification

- Without the promotion change, full-mode seeds 1, 2, 3, 5 and 6 fail with a
  whole event archived twice, and `TestConsumer_Hot_ChainStateCommitsWithLastRow`
  fails. With it, seeds 1-20 and 40 fresh random seeds pass, as do both unit
  tests.
- The fix then broke `TestOracle_SessionRestartInProcess/steady-segment-sync-fault`
  (local mode) about 1 run in 4: the second session's delivery gate waited
  for a frame that never came. Under parallel verification the first session
  archived frame 8 before frame 5 (its fsync then failed), with its chain
  state, above the durable cursor 4. The second session resumed at 4 and
  correctly dropped the redelivered frame 8 as a replay. Before the fix it
  archived it a second time, which is how the gate had always been satisfied.
  The session's gates now inherit the previous session's observed frames
  (`cutoverDeliveryGate.inherit`); the final model check still catches a lost
  frame. After that, 40 of 40 runs passed, as did `just test-long
  ./internal/oracle` and `just oracle-sweep`.

## Lesson

Anything a replay guard reads (verifier chain state, applied seqs) must enter
the durable batch at the same point as the rows it guards. That point is
under the writer mutex, at append time. Before this fix, the single-node
crash oracle checked at-least-once coverage (`CompareEventLogCoverage`), so
it tolerated duplicates. The layer 3 oracle checks the exact set of
duplicates the design allows, and it caught both halves.
