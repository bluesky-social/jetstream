# Oracle failure diary — Darwin metadata writes are not durable across a kill

- **Date:** 2026-10-27
- **Commit:** `a0d12128757549c41af0af4f72b248705c2a74a6`; reproduced before and after the working-tree dependency upgrade to Go 1.27.3 and atmos v0.8.0.
- **Tests:** eight restart-chain leaf assertions listed below; the related strict-memory steady-phase case hangs on this host.
- **Host:** Darwin arm64. The baseline assertion also reproduced with Go 1.26.10.
- **Classification:** actual archive mismatches under the deliberately weaker Darwin metadata persistence policy. These results do not establish a Linux production recovery bug or an oracle comparison bug.
- **Status:** root cause verified in an isolated copy; no runtime or test-policy fix applied.

## Reproduction and scope

On Darwin, this focused existing test reproduces the missing-record assertion:

```sh
just test-long ./internal/oracle -p=4 -timeout=60s \
  -run '^TestOracle_RestartChainCrashConsistency$/^after-repo-complete$'
```

The failing leaf tests in both baseline and upgraded runs were:

- `TestOracle_RestartChainShapeB_NoStraddleAfterMergeTailCrash`
- `TestOracle_RestartChainCrashConsistency/after-repo-complete`
- `TestOracle_RestartChainCrashConsistency/after-merge-dst-flush-before-source-commit`
- `TestOracle_RestartChainCrashConsistency/after-merge-dst-seal-before-discovery`
- `TestOracle_RestartChainCrashConsistency/after-bootstrap-live-close-before-seal`
- `TestOracle_RestartChainCrashConsistency/after-compaction-rewrite-before-watermark`
- `TestOracle_RestartMultiSourceMergeCursorNoReprocess`
- `TestOracle_RestartPredicateKill/after-merge-dst-seal-before-discovery#1`

The two failing parent aggregations are not additional defects. The upgraded
full race run also observed an intermittent `TestMigration_Reclaim` payload
mismatch. That is a separate incident: it passed three isolated race retries
and is not explained by the experiment below.

## Evidence

The production platform distinction is explicit in the source:

- `internal/store/sync_darwin.go` uses `pebble.NoSync` to avoid per-write
  `F_FULLFSYNC` latency. Its comment warns that writes can be lost on a hard
  kill before `Close`, and that production targets Linux.
- `internal/store/sync_unix.go` uses `pebble.Sync` on non-Darwin platforms.
- `internal/metastore/pebblestore/pebblestore.go` passes `store.SyncWrites` to
  metadata sets, deletes, and batch commits.
- `runRestartChild` in `internal/oracle/restart_harness_test.go` sends
  `SIGKILL` at the marker. The first child therefore does not close Pebble.

Preserving the OS page cache across `SIGKILL` does not make an unsynced Pebble
commit durable: WAL data can still be buffered in the killed process. This is
why the earlier distinction between process kill and strict-memory power loss
did not rule out Darwin's `NoSync` policy.

A temporary diagnostic test in `/tmp/jetstream-restart-diagnostic` ran the
actual seed-101 `AfterRepoComplete` recovery and printed the recovered events
and simulator ground truth. The missing record
`did:plc:azigtzsojyppqzxwbzm7rsl2 app.bsky.feed.like/22apghbvmpqmf` belongs to
the account-delete/reactivation fixture (shape F). The recovered archive
contained only its stale account-delete and reactivation markers at seqs 27
and 28, with no surviving replacement record. Other DIDs contained duplicate
backfill snapshots, consistent with metadata loss causing fresh enumeration
and download after the kill.

This reaches the stale-account/re-backfill interaction described in
[the July 6 incident](2026-07-06-rebackfill-erased-by-stale-account-tombstone.md).
The recovery guard in `backfill.Store.lookupEntry` relies on the earlier
`not_started` metadata row surviving: it defers an inherited row to the
post-merge pending-repo replacement pass. Losing that row removes the evidence
that the download belongs to an interrupted bootstrap. Changing the oracle
model to ignore the missing record would hide the actual Darwin mismatch.

## Controlled verification

The isolated source copy came from `git archive HEAD`, with the working-tree
`go.mod` and `go.sum` copied into it. A temporary wrapper invoked precisely
the eight existing failing leaf tests through their four parent test
functions. Both runs used native Go 1.27.3 and the same upgraded dependencies:

```sh
PATH=/tmp/jetstream-go127/bin:/Users/sarah.murphy/go/bin:$PATH \
GOTOOLCHAIN=local just test-long ./internal/oracle -p=4 -timeout=90s \
  -run '^TestAuditAllEightRestartFailures$' -v -artifacts \
  -outputdir=/tmp/jetstream-restart-sync-metadata-output
```

Only `internal/store/sync_darwin.go` in that temporary copy changed:

| Metadata write option | Eight existing leaves | JSON outcomes, including wrapper parents | Elapsed |
|---|---|---|---|
| `pebble.Sync` | All pass | 11 pass | 4.434s |
| Original `pebble.NoSync`, restored as a control | All fail | 11 fail | 1.918s |

The second run used a 60s timeout and
`/tmp/jetstream-restart-nosync-control-output` as its artifact directory.
Segment syncing, runtime logic, fixtures, and oracle comparisons were
unchanged. The back-to-back control isolates metadata syncing as the cause
of these eight failures; it does not require a rearchitecture or a relaxed
correctness model.

Related strict-memory evidence came from a separate small probe using the
actual Pebble metastore and `lifecycle.WritePhase`: `steady_state` was present
before `ResetToSyncedState`, absent afterward with `NoSync`, and survived with
`pebble.Sync`. A custom segment filesystem does call `f.Sync` through
`segment/syncSegmentFile`; the Darwin segment no-op does not explain this
strict-memory metadata loss. A full-runtime forced-sync fix for the hanging
case was not tested as part of this experiment.

Local artifacts from this investigation:

- `/tmp/jetstream-restart-diagnostic-output/`: fixture identities and recovered event dump.
- `/tmp/jetstream-restart-sync-metadata-output/`: all eight leaves green with synced metadata.
- `/tmp/jetstream-restart-nosync-control-output/`: all eight leaves red with original metadata policy.
- `/tmp/jetstream-finding-probe/output.log`: strict-memory phase-reset probe.
- `/tmp/jetstream-upgrade-race/TRIAGE.md`: the preserved full-suite failures and counts.

## Follow-up boundary

No repository persistence behavior changed during diagnosis. The smallest
future choices are to run durability tests with synced metadata in their
isolated processes, or explicitly skip tests requiring that guarantee on
Darwin, as `store.TestOpen_StrictMemDropsUnsyncedWrites` already does. Restoring
durable syncing for macOS binaries is a separate product decision with a
documented latency cost. Linux already selects `pebble.Sync`; a Linux run is
still needed to verify that platform, but these Darwin failures are not
evidence that its existing durability policy is broken.
