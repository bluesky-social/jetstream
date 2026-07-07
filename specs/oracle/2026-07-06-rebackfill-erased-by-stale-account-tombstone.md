# Oracle failure diary — re-backfilled records erased by stale account tombstone

- **Date:** 2026-07-06
- **Commit (failure observed on):** branch `segment-io-fault-200`, on top of `a350dd5`
- **Test:** `TestOracle_RestartSegmentFault_FailsLoudThenRecovers/write-shortwrite-first-flush` (new in #200)
- **Symptom:** `oracle: missing did:plc:… app.bsky.feed.repost/… rev=` — chain shape F's post-reactivation record exists in ground truth but is absent from the recovered archive.
- **Classification:** seed-deterministic (seed 113); NOT a flake and NOT a bug in the new tier — the injected fault is only the crash trigger for a pre-existing recovery-ordering bug.
- **Status:** FIXED — the repro is unskipped and green.
- **Tracking issue:** [#262](https://github.com/bluesky-social/jetstream/issues/262) (found by #200, epic #35).

## Repro

```
go test ./internal/oracle -count=1 \
  -run 'TestOracle_RestartSegmentFault_FailsLoudThenRecovers/write-shortwrite-first-flush' -v
```

(remove the `t.Skip` first). Deterministic at the pinned seed (restartSeed(12) = 113).

## Analysis

The first child crashes loud mid-backfill on the injected short-write (that part
is the #200 contract working as designed). The data loss happens in the SECOND,
fault-free child:

1. First child: chain coordinator sees the shape-F DID's getRepo and generates
   account-delete → reactivate → create on the live firehose. The child dies
   (backfill flush error) before the bootstrap-live consumer archives those
   frames and before this repo's completion row is durable.
2. Second child re-runs bootstrap. Backfill re-downloads the repo; the snapshot
   now already contains the post-reactivation record, so its create lands as a
   backfill row at a LOW seq (11 in the repro) and `repo/<did>.Backfill.Rev` is
   the post-reactivation head rev.
3. The bootstrap-live consumer replays the relay history from cursor 0: the
   stale account-delete and reactivate frames archive at HIGH seqs (14, 15).
   Account frames are not rev-filtered by the merge (`shouldKeep`,
   internal/ingest/orchestrator/merge_filter.go:40 — only commit kinds + sync).
   The live create commit IS filtered (rev == Backfill.Rev fails the strict
   `>`), which is correct in isolation — backfill covers it.
4. Merge-tail compaction folds the account-delete payload (`active=false,
   status=deleted`) into a DID tombstone at seq 14
   (internal/tombstone/tombstone.go:217) and `ShouldDrop` erases every
   materialization row for the DID below seq 14 — including the seq-11
   re-backfilled create. The reactivation frame at seq 15 recreates nothing.

Contributing factors (no single root cause):
- Bootstrap re-backfill of a previously-incomplete repo lands at low seqs,
  unlike the failed-repo retry path which recreates at HIGH seqs with
  #sync-like tombstone+recreate semantics — the retry path is immune.
- Account frames carry no rev, so the merge rev-filter cannot know the
  delete is superseded by the backfill snapshot.
- The #255 applied-account-seq ratchet only dedupes REPLAYS of already-appended
  account rows; here the rows were never appended pre-crash.

## Fix

Fixed by deferring inherited `not_started` rows instead of re-downloading them
through the bootstrap writer:

1. `backfill.Store.Lookup` treats a pre-existing `StatusNotStarted` row as an
   interrupted prior bootstrap attempt, promotes it to `StatusPending`, and
   returns `StateComplete` to the atmos bootstrap engine so it does not dispatch
   a low-seq getRepo.
2. The merge phase runs one immediate pending-repo retry pass after draining
   `backfill/live_segments` into the permanent writer and before sealing the
   destination segment. The retry path emits the existing whole-repo replacement
   shape: `KindSync` tombstone followed by `KindCreateResync` rows.
3. Those replacement rows now land above the stale account-delete/reactivate
   frames replayed from the captured live tail, so merge-tail compaction cannot
   permanently erase the current repo snapshot.

The chain-coverage assertion now also accounts for observed repair tombstones
when deciding which expected materialization rows compaction legitimately
removed; otherwise the new synthetic sync could make a correct repair look like
a lost intermediate.

## Follow-up: false failure from an over-strict visibility assertion (2026-07-07)

After un-skipping this case, `oracle-sweep` surfaced a NON-deterministic (real
wall-clock crash-timing dependent, ~1-in-6) failure at a swept base seed:

```
segmentfault-write-shortwrite-first-flush: recreated record
  app.bsky.feed.post/… must be visible (no permanent tombstone)
```

This was NOT data loss and NOT a re-manifestation of #262. Evidence: the full
final-state oracle `Compare(want, got)` PASSED in the same run — jetstream's
archive exactly equalled the world ground truth — while only the per-shape
`assertRecreatedRecordsVisible` check failed. Dumping ground truth showed the
world itself no longer had the record at head: this case carries
`liveEventsBetweenChildren: 2`, and `generateN` picks a random active author +
random action. When it draws the chain-host account with `action=delete`,
`pickUntouchedRecord` (world/traffic.go) can select the just-recreated record
and legitimately delete it — a valid world mutation the archive faithfully
reproduced.

The bug was in the assertion: it assumed a `shapeLiveDeleteRecreate` record is
ALWAYS present at head, which is only true absent later traffic on that key.
Fixed by gating the check on ground truth (restart_chain_assert_test.go): the
record must be present on disk iff ground truth has it. This still catches a
masked recreate (truth has it → disk must too) AND a failure to honor a later
delete (truth dropped it → disk must too), without flagging a correctly-absent
record as a permanent tombstone. Lesson: a per-shape invariant that hardcodes an
expected final state is fragile when random background traffic can mutate the
same key — assert against ground truth, not a static assumption. When only a
narrow per-shape check fails but the full `Compare` passes, suspect the check,
not the system under test.
