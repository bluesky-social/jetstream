# Oracle failure diary — re-backfilled records erased by stale account tombstone

- **Date:** 2026-07-06
- **Commit (failure observed on):** branch `segment-io-fault-200`, on top of `a350dd5`
- **Test:** `TestOracle_RestartSegmentFault_FailsLoudThenRecovers/write-shortwrite-first-flush` (new in #200)
- **Symptom:** `oracle: missing did:plc:… app.bsky.feed.repost/… rev=` — chain shape F's post-reactivation record exists in ground truth but is absent from the recovered archive.
- **Classification:** seed-deterministic (seed 113); NOT a flake and NOT a bug in the new tier — the injected fault is only the crash trigger for a pre-existing recovery-ordering bug.
- **Status:** OPEN — case `t.Skip`ped pending fix.
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

Open — see #262 for candidate directions. Un-skip the oracle case as the
red-first proof when fixing.
