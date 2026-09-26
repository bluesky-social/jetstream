# Oracle failure diary — the startup catalog load races a writer creating its directory

- **Date:** 2026-09-26
- **Commit (failure observed on):** `4effd3f` (branch `jc/new-storage`)
- **Test:** `TestOracle_RestartCrashPointsDoNotLoseRecords/after-repo-complete`, in `just oracle-sweep` run 3/10, seed 11701313285367832394
- **Symptom:** the restarted child exited at once with
  `runtime error: catalog load: ingest: readdir …/backfill/live_segments: open …/backfill/live_segments: no such file or directory`.
- **Classification:** production bug in local mode's segment catalog
  (`internal/catalog/local`), present since S1.9/S1.10. It fails process
  startup and loses no data: the process exits, and the next start usually
  gets past it.
- **Status:** FIXED

## Repro

```
JETSTREAM_ORACLE_SEED=11701313285367832394 GOMAXPROCS=2 \
  go test ./internal/oracle -run 'TestOracle_Restart' -count=50 -failfast -timeout 60m -v
```

The timing is wall-clock scheduling, not seeded, so the oracle rarely hits
it. The deterministic unit repro is
`TestLocal_RefreshDirectoryCreatedDuringScan` (`internal/catalog/local`),
which fails on the old code with `ingest: readdir /data/segments: … file
does not exist`.

## Analysis

- The runtime loads the local catalog in the background while the first
  writer session starts (`startBackgroundLoad(…, segCatalog.Refresh)` in
  `internal/jetstreamd/runtime.go`). A failed load ends the process
  (`catalog_wait` in `runLocal`).
- `refreshNamespace` listed the namespace directory, and on an error it
  statted the directory. It treated the error as "empty" only if the stat
  also said the directory was missing.
- The crash most likely landed before the first process's bootstrap-live
  writer created `backfill/live_segments`. In the restarted child, the
  listing found no directory. The session's writer then created it before
  the stat, so the refresh returned the listing error.

## Root cause

The missing-directory check asked the filesystem a second time instead of
reading the listing's own error. A directory missing when listed held no
segments at the scan's sample. A writer that creates it later publishes its
segments to the catalog itself, which is the same rule
`TestLocal_RefreshKeepsSegmentsSealedDuringScan` relies on.

## Fix

`refreshNamespace` treats a not-exist listing error as an empty directory
and returns any other error. The unit test above fails before the fix and
passes after. The seed's repro command passes 30 runs.
