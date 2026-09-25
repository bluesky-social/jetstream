# Disaggregated storage v2: implementation plan

**Status: Stage 0 done (2026-09-25); Stage 1 done (2026-09-25); Stage 2 in progress.** This is the work tracker for
`specs/notes/2026-09-25-disaggregated-storage-v2-design.md` (the "design"). It
breaks the design's delivery stages (§26) into PR-sized tasks with file
references, dependencies, checks, mutants, and exit criteria. The design says
*what* to build. This plan says *in what order*, *where in the code*, and *how
we know each piece is done*.

Prerequisite already landed: the ephemeral dev environment (`just up` /
`just down`, `compose.yaml`, `testing/devenv/`, commit `ba6c301`).

## How to use this document

- Each `- [ ]` task is intended to be one PR. Sub-bullets are the acceptance
  list for that PR. Tick the box in the same PR that finishes the task.
- Task IDs (`S1.4`) are stable. Reference them in commit messages and PR titles
  (`S1.4: move backfill store onto metastore`).
- Size tags are rough: **S** is under a day, **M** is a few days, **L** is a
  week or more and should probably be split when it is picked up.
- `Deps:` lists tasks that must merge first. Anything without deps within a
  stage can run in parallel.
- When implementation forces a deviation from the design, update the design in
  the same PR and note it under "Decisions log" at the bottom of this file.
- Measurements (design §22) are recorded in the design doc, not here. This plan
  only tracks that they were done.
- Every PR runs `just`. The per-stage "Checks" list says what else to run. The
  AGENTS.md "Which checks to run" table always applies on top.

## Stage overview

| Stage | Goal | Status |
|---|---|---|
| 0 | Remove timestamp import (design §21) | done |
| 1 | Storage interfaces; local mode moved onto them with no behavior change | done |
| 2 | Steady state in disaggregated mode on fakes and real storage | in progress |
| 3 | Bootstrap, merge, `storage init` | not started |
| 4 | Sparse compaction and GC | not started |
| 5 | `storage new-identity`, memory budgets, dashboards, 24h soak | not started |

```
S0 ──► S1.1 metastore ──► S1.2–S1.6 call sites ──┐
       S1.7 segment refactors ──────────────────┤
       S1.8 ObjectStore iface ──────────────────┤
       S1.9 Catalog/HotLog iface + local impl ◄─┤──► S1.10 read seams ──► S1.12 session split
                                                 └──► S1.11 orchestrator seams ─┘
S1 done ──► S2.1 deps ──► S2.2 pgstore ──► S2.3 leader, S2.4 metastore/pg, S2.5 objstore
            S2.6 catalog core (Tx primitives) + S2.7 storagefake ──► S2.8–S2.12 write path
                                                                 ──► S2.13–S2.14 read path
            ──► S2.15–S2.16 config + runtime ──► S2.17–S2.19 layer 3 + mutants
            S2.20 layer 4 + test-storage (can start right after S2.2/S2.5)
S2 done ──► S3 (bootstrap/merge) ──► S4 (compaction/GC) ──► S5 (ops/soak)
```

## Ground rules for every stage

1. **Local mode is the default and stays first-class.** `just` must never need
   PostgreSQL, S3, containers, or credentials (`specs/oracle.md` "Preserve
   Local Coverage When Adding Storage Backends"). Container-backed tests are
   behind `just test-storage` only.
2. **Core code never imports `pgx` or the AWS SDK.** Only `internal/pgstore`,
   `internal/metastore/pg`, `internal/catalog/pg`, `internal/objstore/s3`, and
   `internal/leader` (PG lock) may. Add a lint or `go list` test that enforces
   this (S2.1).
3. **Crash-loud rules carry over.** Upstream data errors: drop, count, continue.
   Storage corruption or broken invariants: end the session and exit non-zero
   (design §6.5). A failed or unknown-result transaction ends the session and is
   never retried inside it (design §9.1).
4. **Tests stay under 1s per package** for everything in `just`. Layer 3 oracle
   runs follow the existing oracle tier rules (fast in `-short`, heavier modes
   behind recipes).
5. **Mutation discipline.** Refactors will move mutant target code. A STALE
   mutant gets refreshed in the same PR that moved its target, with the same
   semantic bug, then `just mutation-gate` on a clean tree. Never weaken a
   mutant to make it apply.
6. **No new dependencies** beyond `pgx/v5` and `aws-sdk-go-v2` (design §25).
   Test tooling (TCP fault proxy, S3 fault transport) is written in-repo.
7. **Secrets.** `JETSTREAM_PG_URL` and S3 credentials are never logged, never in
   `.env`, never on `/status`. Add a test that the status snapshot and startup
   logs do not contain the configured URL's password (S2.15).

## Survey findings that change or sharpen the design

These came from the code surveys done while writing this plan. The relevant
tasks below already account for them. Fold them into the design doc in the
first PR that touches each area.

1. **Call-site counts.** Design §14.1 says 56 `NewBatch`, 12 `NewIter`, and 5
   `DeleteRange` sites. Those counts include tests, the simulator, and the
   importer. Non-test `meta.pebble` sites are 15 `NewBatch`, 8 `NewIter`, and
   2 `DeleteRange`. After S0 that drops to 6 `NewIter` and 1 `DeleteRange`
   (`internal/ingest/seqlease.go:135`). About 70 test files and 230 references
   use `*store.Store` directly. The simulator (`internal/simulator/world`) has
   its own Pebble DB and is out of scope.
2. **`CountStatuses` as a counts seed is not safe on a non-snapshot scan.**
   `backfill/counts.go:91` tallies all `repo/` rows. It serves display, and it
   also seeds `backfill/counts` when that key is missing
   (`backfill/store.go:217,305,399,823,1101`). A paged, torn scan would be
   saved permanently. Fix in S1.4: seed once at writer or session start, before
   any `repo/` writer runs, and treat "counts missing mid-run" as an internal
   error.
3. **Only count-based block cuts exist.** Blocks close at 4096 events or on
   explicit Flush, Drain, ForceRotate, or Seal. Nothing cuts by age. The 30s
   block age and 15ms batch age (design §10.3) are new code that needs an
   injectable clock.
4. **Seal is mostly pure already.** `buildFooter`, `encodeHeader`, and
   `xxh3HeaderFooter` are pure. Only the frame walk
   (`segment/seal.go` `walkActiveFrames`, which reads through `io.ReaderAt`) and
   the file writes need refactoring.
5. **Async flush bypasses the injected FS.** `internal/ingest/async_flush.go:168`
   calls `os.Stat` instead of `statFS(w.cfg.FS, …)`. Fix in S1.7.
6. **atmos has no locker contract suite.** atmos v0.4.0 `streaming/lock.go`
   defines `DistributedLocker`, `NoopLock`, `ErrLockHeld`, and `ErrNotHolder`.
   It only has a private `mockLock` in `leader_test.go`. S2.3 writes the
   contract suite.
7. **The store fault injector reads Pebble batches directly.**
   `internal/store/fault.go:69` walks `b.Reader()`. The metastore fault seam
   becomes a wrapping `Store` whose `Batch` records keys, so `Batch` does not
   need an introspection method.
8. **Identity cache writes without sync.** `internal/identity/cache.go:114,122`
   use `pebble.NoSync`, and they are the only non-synced writes. Reader pods
   cannot write PostgreSQL. Decision proposed in S1.6: the identity cache takes
   its own small KV interface. Local mode wires it to Pebble with NoSync.
   Disaggregated mode wires it to a bounded pod-local memory cache.
9. **`data-dir` has a default value.** "`JETSTREAM_DATA_DIR` must be unset in
   disaggregated mode" (design §18) must check `cmd.IsSet`/env presence, not
   the value (`cmd/jetstream/main.go:220-225`).
10. **Unknown `JETSTREAM_*` env vars are rejected** by walking every declared
    flag (`cmd/jetstream/main.go:527-589`). Every new variable must be a
    declared flag on `serve` or `storage`. Test-only variables use a prefix
    added to `knownForeignJetstreamEnvPrefixes` (`JETSTREAM_TEST_`).
11. **One synctest bubble per process** (`internal/oracle/doc.go`). The layer 3
    oracle must run every pod of a scenario in the same bubble, or re-exec per
    run.
12. **CI race runs the oracle without `-short`.** `just test-race-ci` runs the
    default-mode lifecycle under race in a 20m job. New oracle tiers must fit
    that budget or be excluded from the race job.
13. **The checksum statement in the README is wrong.** `docs/README.md` §3.1.2
    says the header checksum covers the blocks. It covers header `[12:256)`
    plus the footer (design §10.8). Fix in S1.7.
14. **403 vs 404.** SeaweedFS and MinIO return 404 for a missing key without
    ListBucket. AWS returns 403. `objstore/s3` must handle both (AGENTS.md
    already notes this). See S2.5.
15. **Status reads `meta.pebble` from disk.** `internal/status/collect.go:618`
    walks the directory for byte counts. That becomes a local-only optional
    stats interface (S1.10).
16. **`rewriteMu` and `steadyWriter` in the orchestrator exist for import.**
    After S0 they can be removed (`orchestrator.go:90,95`), since compaction is
    the only rewriter.

## Key implementation decisions

These are the plan's answers to questions the design leaves open. They are
proposed defaults, not approved decisions. Challenge them in the PR that first
implements each one.

### D1. Catalog logic runs on a small transaction interface

The design wants layer 3 (fakes) to kill mutants such as "skip the fence", "fold
deletes one batch too few", and "GC skips the re-check". A mutant can only be
killed by the fakes if the mutated code runs against the fakes. So the
transaction *scripts* (fence first, seq-key check, reference checks, fold
coverage check, seal block-list check, GC re-check) live in backend-neutral code
in `internal/catalog`. They are written against a narrow `catalog.Tx` interface
of primitives:

```go
// Tx is one leader write transaction. Implementations: pgstore (SQL) and
// storagefake (in-memory tables with PostgreSQL's locking and visibility).
type Tx interface {
    FenceBump(ctx context.Context, epoch uint64) (revision uint64, ok bool, err error)
    MetaGetForUpdate(ctx context.Context, key []byte) ([]byte, error)
    ApplyMeta(ctx context.Context, ops []metastore.Op) error
    RefCheck(ctx context.Context, objectID uint64) (ok bool, err error)
    InsertHotBatch(ctx context.Context, row HotBatchRow) error
    DeleteHotBatches(ctx context.Context, lo, hi uint64) ([]HotBatchSpan, error)
    InsertActiveBlock(ctx context.Context, row ActiveBlockRow) error
    ActiveBlocksForUpdate(ctx context.Context, ns Namespace, idx uint64) ([]ActiveBlockKey, error)
    // ... seal, generation publish, object rows, GC mark/claim/forget, notify
    Commit(ctx context.Context) error
    Rollback(ctx context.Context) error
}
```

`pgstore` implements each primitive as one SQL statement. `storagefake`
implements them over in-memory tables. It does **not** re-implement the checks,
so it cannot hide a bug in them. The fake does emulate PostgreSQL semantics
exactly, though: the row lock on `archive`, commit-order revisions, and
constraint violations (unique, check, FK). As a result, an unfenced write by a
stale leader really succeeds in the fake, and the oracle observes the
corruption. It is not rejected by some fake-only assertion.

The layer 4 contract suite runs the same primitive-level tests against pgstore
and storagefake. That proves they agree.

### D2. ObjectStore is split: blob transport vs. object protocol

- `objstore.Blob`: raw `PutKey`, `GetKey`, `GetKeyRange`, `DeleteKey` on string
  keys. Implementations: S3 and in-memory. Fault injection lives here.
- `objstore.Store`: the design's `ObjectStore`, with `Put`, `Get`, `GetRange`
  by `object_id`. Backend-neutral. Implements §7.3 (dedup, `uploading` row,
  PUT, read-back verify, mark `available`) over `Blob` plus `catalog.Tx`. Also
  implements §7.5 (verify, refresh-and-retry, corruption).

So the upload protocol, including the "skip PUT after `orphan_age/2`" rule,
is shared code tested by layer 3.

### D3. One fake, not two

The design lists "in-memory fakes" for layer 1 and `internal/storagefake` for
layer 3. Plan: `storagefake` is the only PostgreSQL fake. It provides
`catalog.Tx`, a read-only snapshot reader for the follower, `metastore.Store`
over its `metadata_kv` table, and the lease for `leader`. The in-memory `Blob`
lives in `objstore/memblob`. Unit tests of core packages use these same fakes.

### D4. Deterministic scheduling for layer 3

Every call into `storagefake` and `memblob` is a yield point. A seeded
scheduler admits one parked call at a time. It uses `synctest.Wait` to know
that every goroutine in the bubble is durably blocked, then picks the next call
from a PCG stream. That makes interleavings at storage boundaries replayable.
Network I/O stays on in-process pipes (`internal/oracle/pipelistener.go`).

A determinism test (same seed produces the same trace), modeled on
`TestOracle_SameSeedTraceDeterminism`, guards the scheduler. If full
determinism proves impractical, fall back to "deterministic fault schedule,
nondeterministic interleaving" and say so in `specs/oracle.md`. That is a
weaker claim, and the design must be updated.

### D5. Compaction is disabled in disaggregated mode until stage 4

Stages 2 and 3 run with compaction off in disaggregated mode. Startup refuses
`JETSTREAM_COMPACTION_INTERVAL > 0` together with `JETSTREAM_STORAGE=disaggregated`
until S4 lands. Merge skips merge-tail compaction in that mode. The layer 3
oracle compares against the uncompacted model until S4.

### D6. The writer is split into block building and block committing

`segment.Writer` currently both accumulates the pending block and writes the
file. Stage 1 separates:

- a pure **block builder**, the existing `pendingBlock` and column encoder that
  produce a `PreparedBlock` (`segment/writer.go:345-424, 564-625`);
- a **block committer** in ingest, which persists a prepared block together
  with a metastore batch. Local mode: write, fsync, then Pebble commit (today's
  order). Disaggregated direct mode: upload, then `CommitBlock` in one fenced
  transaction. Hot mode adds batches and the maintainer on top.

The public Writer API (`Append`, `AppendBatch`, `Flush`, `ForceRotate`,
`SealActiveAndClose`, `DrainDurability`) stays as it is (design §10.1).

### D7. The session split happens in stage 1, under local mode

`internal/jetstreamd/runtime.go` currently builds everything once. Stage 1
splits construction into per-process and per-leader-session parts, and runs
local mode through `internal/leader` with `streaming.NoopLock`. Local mode has
exactly one session that lasts the whole process. This moves the riskiest
wiring change into a stage where the local oracle can check it, before any
PostgreSQL code exists.

---

## Stage 0: Remove timestamp import

Design §21. This is a pure deletion. The `indexed_at` column stays, always
`0`. About 12-13k lines go, roughly 60% of them tests and generated code.

- [x] **S0.1 Delete the import pipeline** (M)
  - Delete `internal/timestamp/` (including `testdata/fuzz`) and
    `internal/importer/`.
  - Orchestrator: delete `import_pass.go`, `import_pass_test.go`,
    `import_metrics.go`, and `import_metrics_test.go`. Remove the import fields
    from `config.go` (`:16`, `:224-240`: `ImportSelector`, `ImportMetrics`,
    `ImportRules`, `TimestampStamper`) and from `steady.go:80`.
  - Remove `steadyWriter` and `rewriteMu`/`withRewriteLock`
    (`orchestrator.go:78-95,208`). Delete `rewrite_lock_test.go` if nothing
    else remains in it. Update the comments at `compact_deletes.go:83-87,165-170`.
  - Delete the import rigs from `powerloss_strictfs_test.go` (`:110-166`,
    `:394-460`) and `segment_iofault_test.go` (`:81-160`).
  - Ingest: delete the `TimestampStamper` interface, field, and call
    (`config.go:20-26,74-77`; `writer.go:444-449`). Keep
    `ev.IndexedAt = candidate.IndexedAt` or simplify it. Delete
    `writer_test.go` `fakeTimestampStamper` and its tests (`:124-133`, `:790`,
    `:819`). Remove `live/config.go:137-139` and `live/consumer.go:145`.
  - Segment: delete `patch.go` and `patch_test.go` (which includes `FuzzPatch`),
    the `CrashPointPatch*` constants (`crash.go:32-53`), and the patch half of
    `iofault_test.go` (`:15`, `:46-53`, `:166`).
  - Crashpoint: delete the four `AfterSegmentPatch*` points and their
    `AllPoints` entries (`crashpoint.go:95-114,135-138`), and update
    `crashpoint_test`.
  - jetstreamd: remove `TimestampImportToken`/`TimestampImportDir`
    (`options.go:137-142`) and all import wiring in `runtime.go` (the rule
    store `:208-215`, orchestrator fields `:396-399`, importer `:426-458`,
    status `:468`, xrpcapi `:552-556`, `import_resume` `:668-687`, Close
    `:771,785-805,814-820`, and `importReporter` `:890-920`). Delete
    `TestBuild_CreatesImportDir` and `TestClose_FailedImportDrainLeavesStoreOpen`.
  - xrpcapi: delete `importts.go`, `importts_test.go`, and `auth.go`. Remove
    `ImportConfig` and the route from `server.go`.
  - status and web: remove `ImportInfo`/`ImportReporter` (`snapshot.go:25-57`,
    `collector.go:38-40`, `collect.go:754-756`) and the template section
    (`status.html` from `:325`, plus `handler_test.go`).
  - API: delete the lexicons `getImportStatus.json` and `importTimestamps.json`.
    Run `just lexgen` and confirm the generated files are gone with no other
    drift.
  - cmd: remove the flags and mapping (`main.go:398-409,472-473`) and the
    `serve_test.go` references. Delete `import_e2e_test.go`.
- [x] **S0.2 CI, mutation, dashboards** (S). Deps: S0.1.
  - `.github/workflows/ci-scheduled.yml`: drop the `./internal/timestamp
    FuzzParseRoundTrip` and `./segment FuzzPatch` matrix entries. Update
    `testing/ci/workflows_test.go`, which pins the matrix.
  - Retire mutant `m048_patch_parent_dir_fsync_deleted` with a reason in
    `testing/mutation/RESULTS.md`. Remove it from `baseline.json` via
    `just mutation-baseline`.
  - `testing/mutation/run.sh:261-269`: drop `TestRunImport_ENOSPC`,
    `TestRunImport_SegmentIOFaultSweep`, and `TestPatchIOFaultSweep` from the
    segmentfault tier regex. Confirm m044 and m045 are still killed.
  - `contrib/grafana/jetstream.json`: remove the "Timestamp import" row and the
    `jetstream_import_*` expressions.
- [x] **S0.3 Docs** (S). Deps: S0.1.
  - `docs/README.md`: remove §8, and update the mentions at `:337,494,635,683,735`.
    State that `indexed_at` is always 0 until a new import design exists.
  - AGENTS.md repo layout (`timestamp/`, `importer/`, the `cmd/` line).
  - `specs/architecture.md:75`, `specs/glossary.md:49`, `specs/gotchas.md`
    (import entries), and `specs/oracle.md` (`:78,206,212,232,234`).
  - Mark `specs/notes/2026-07-01-timestamp-import-design.md` and
    `2026-07-07-imported-indexed-at-durability.md` as superseded (a status
    line, not deletion).

**Checks:** `just`, `just lexgen` (no drift), `just test-long ./internal/oracle`,
`just mutation-gate` on a clean tree (only m048 retired, the rest unchanged),
`just fuzz 30s ./segment`.

**Exit:** no references to `timestamp`, `importer`, `TimestampStamper`,
`segment.Patch`, or `jetstream_import_` remain (`git grep`). The mutation
baseline matches, minus m048.

---

## Stage 1: Interfaces, with no behavior change

Design §26 stage 1, plus §14.1, §19.1, and the §22 stage 1 measurement. Local
mode keeps its exact behavior and on-disk format. Every PR in this stage must
leave the local oracle and the mutation baseline unchanged, apart from
refreshed STALE mutants.

### Metadata store

- [x] **S1.1 `internal/metastore` interface, Pebble impl, fault wrapper** (M).
  Deps: S0.
  - Add `Store`, `Batch`, `Iterator`, `ErrNotFound`, and `Op` (the value type
    that `ApplyMeta` and the PG impl consume), per design §14.1.
    - `Get` returns a copied `[]byte` with no `io.Closer`.
    - `Iterator` uses `Next() bool` / `Key` / `Value` / `Err` / `Close`.
    - `Batch.DeleteRange` is included.
  - Add `metastore/pebble` wrapping `internal/store`. Commit uses
    `store.SyncWrites`. Keep the existing `jetstream_store_op_duration_seconds`
    metrics.
  - `metastore/memstore`: an in-memory sorted map for unit tests. It becomes
    `storagefake`'s `metadata_kv` in S2.7.
  - Fault seam: a `metastore.WithFaults(Store, FaultInjector)` wrapper that
    records batch keys itself, replacing `store/fault.go:69`'s `b.Reader()`
    walk. Keep the `KeyPrefixFault{Prefix, Op, Ordinal, Err}` semantics and
    port `store/fault_test.go`.
  - `metastore/storetest`: a contract suite run against every impl.
    - Get/Set/Delete and missing keys.
    - Ordered batch semantics: Set then Delete then Set on the same key;
      DeleteRange ending a run.
    - Iterator bounds are `[lower, upper)` with bytewise order, including keys
      containing `0x00` and `0xff`.
    - An empty batch commit.
    - Commit error surfacing.
  - Tests: the contract suite against pebble and memstore. Fuzz the ordered
    batch semantics: pebble and memstore must agree on random op sequences.
- [x] **S1.2 Move `internal/ingest` and `live` onto metastore** (M). Deps: S1.1.
  - `config.go`: `Store metastore.Store`. `DurableBatchHook` takes
    `metastore.Batch` (`config.go:18`).
  - `writer.go`: `loadNextSeq`/`saveNextSeq`/`stageNextSeq` and
    `commitDurableBatchLocked` (`:944-1054`). Replace
    `pebble.ErrNotFound` at `:951`.
  - `seqlease.go`: batch, iterator, and `DeleteRange` (`:82-145`). The lease
    stays local-mode-only internally (§10.1). Document that on the config
    field.
  - `live/consumer.go` hook (`:342-380`), and `live/cursor.go` versioned
    reads.
  - Port `storefault_test.go` and `writer_test.go` store usage. Keep
    `TestWriterFlushOrdersSegmentSyncBeforeStoreCommit` green; it pins the
    fsync-before-commit invariant.
- [x] **S1.3 Move `syncstate`, `lifecycle`, and `identity` onto metastore**
  (S). Deps: S1.1.
  - `syncstate/store.go`: `StageFlush(metastore.Batch)` and the Gets.
    Remove `Flush`/`Delete` if they still have no production callers (survey:
    none).
  - `lifecycle/phase.go`: retype every function to `metastore.Store`.
    `IsSteadyState` is replaced by a `Readiness` interface in S1.10. Keep a
    local adapter until then.
  - `identity/cache.go`: introduce `identity.KV` (Get/Set/Delete, best
    effort). Local impl: Pebble with NoSync. Disaggregated impl: a bounded LRU
    (S2.16). See finding 8.
- [x] **S1.4 Move `internal/ingest/backfill` onto metastore** (L). Deps: S1.2.
  - `store.go`: all Gets, Sets, and batches (`:78-1387`), and
    `stageDurableBatch(metastore.Batch)`. Also `completion_batcher.go:158`,
    `counts.go`, `status.go`, `diagnostics.go`, `cursor.go`, and `retry.go`
    (`RetryConfig.Store`).
  - Counts seed (finding 2): compute the seed once at `Run`/`RunPendingRepoRetryPass`
    start, before any `repo/` writer starts. Make the five in-flight fallbacks
    an internal error. Add a test that starts with the counts key absent and
    rows present, and checks the seeded counts equal a full tally.
  - Iterator callers switch to `Next() bool`. Callers that treated `Error()`
    returning `ErrNotFound` as benign (`status.go:87`) get explicit handling.
  - Port the heavy test files: `completion_batcher_test.go` (36 direct batch
    uses), `store_test.go`, `diagnostics_test.go`, `retry_test.go`, and
    `run_test.go`.
- [x] **S1.5 Move the orchestrator and status onto metastore** (M). Deps: S1.2,
  S1.3.
  - `merge_cursor.go`, `merge_filter.go`, `compaction_watermark.go`, and
    `states.go`.
  - `status/collect.go`: `GetUint64LE` and `countKeysWithPrefix`. Put the
    on-disk `meta.pebble` walk (`:618`) behind an optional
    `metastore.DiskStats` interface that only the Pebble impl implements.
  - `store/encoding.go` helpers (`GetUint64LE`, `Get/SetVersionedUint64LE`,
    `PrefixUpperBound`) move to `metastore` as backend-neutral functions.
- [x] **S1.6 NewIter audit and `internal/store` shrink** (S). Deps: S1.4, S1.5.
  - Record the audit in the design (§14.2 asks for it). Survey verdicts:

    | Site | Verdict |
    |---|---|
    | `backfill/counts.go` `CountStatuses` | safe for display only; seed moved (S1.4) |
    | `backfill/retry.go` `scanDue` | safe: a changed row is seen in its new state or next pass; failure recording re-checks under `countsMu` |
    | `backfill/status.go` `ListPDSHosts` | safe (display) |
    | `backfill/diagnostics.go` `ListHostStatuses` | safe (display) |
    | `ingest/seqlease.go` `loadSeqGaps` | safe: single writer at open; local-mode only |
    | `status/collect.go` `countKeysWithPrefix` | safe (display); expensive on PG, replaced in S2.14 |

  - `internal/store` keeps only `Open`/`Close`, metrics, and the Pebble
    handle used by `metastore/pebble`. No package outside `metastore/pebble`
    and `jetstreamd` imports it (enforce with a test).
  - Port the remaining tests that open `store.Open` directly, such as
    `internal/oracle/account_status_harness_test.go:141-190`, which aliases it
    as `metastore`.

### Segment and writer

- [x] **S1.7 Segment refactors for block sources** (L). Deps: S0.
  - **Seal from a frame source.** Extract a pure
    `segment.BuildSealed(src FrameSource) (header, footer []byte, Header, error)`
    from `seal.go` `sealAfterFlush`/`walkActiveFrames` (`:93-397`). It computes
    virtual offsets `256 + Σ(8+len)`. Keep the empty-block break (`:307`)
    semantics explicitly.
    - The file path becomes a thin sink that writes the footer, fsyncs, writes
      the header, and fsyncs. Keep `truncateFooterTail`.
  - **Reader over bytes.** `segment.OpenReaderAt(io.ReaderAt, size int64, …)`,
    plus a constructor from header+footer bytes plus a block fetcher
    `func(i int) ([]byte, error)`. `validateHeaderOffsets` takes the virtual
    size. `Open(ReaderConfig)` keeps working on files.
  - **Rewrite split.** A pure `computeRewrite(frames, decide) → (outFrames,
    header, footer)`, and a sink. The file sink keeps tmp+fsync+rename, the
    crash seams, and the IO seams (`rewrite.go:155-242`).
  - **Block builder.** Expose the pending-block encoder (`pendingBlock`,
    `PrepareFlush`, `CompressPreparedBlock`) as a type usable without a file
    (D6).
  - Fix `ingest/async_flush.go:168` (`os.Stat` → `statFS`).
  - Fix `docs/README.md` §3.1.2 (finding 13).
  - Tests:
    - Property test: `BuildSealed` over the frames of a file equals the
      file-sealed header and footer byte for byte (reuse the
      `seal_swarm_test.go` generators).
    - The ReaderAt reader and the file reader agree on every
      `DecodeBlock`/`BlockBloom`/`BlocksContainingDID` over random segments.
    - `VerifySealedMetadata` passes over a bytes-backed reader.
    - Fuzz target: `BuildSealed` and the reader over arbitrary frame bytes.
      Neither may panic; errors are fine.
    - `just bench ./segment` against baseline: no regression on the
      seal/flush hot paths.
- [x] **S1.8 `ObjectStore` interfaces and in-memory blob** (S). Deps: S0.
  - `internal/objstore`: `Blob` and `Store` interfaces (D2), `memblob` with a
    fault injector (ops `put`/`get`/`get_range`/`delete`, key-prefix plus
    ordinal, modeled on `segment/iofault.go` and `store/fault.go`). The faults
    include "return wrong bytes" and "PUT acknowledged but not stored".
  - `objstore/blobtest` contract suite: round trip, range reads, delete of a
    missing key succeeds, get of a missing key returns `objstore.ErrNotFound`,
    and wrong-bytes detection happens at the `Store` layer, not the `Blob`
    layer.
  - No production users yet. `Store`'s implementation lands in S2.5, because
    it needs `catalog.Tx`.

### Catalog and read seams

- [x] **S1.9 `Catalog`, `CatalogView`, `HotLog`, and `BlockRef`; local impl**
  (L). Deps: S1.2, S1.7.
  - `internal/catalog`: types from design §11.2 and §19.1. Adjust the names to
    fit the code:
    - `Namespace` (`main`, `bootstrap_live`)
    - `SegmentView`
    - `BlockRef{MinSeq, MaxSeq, MinWitnessedUS, MaxWitnessedUS, ObjectID|Frame}`
    - `CatalogView.RefsFrom`/`TipSeq`/`Segments`/`Revision`
  - **Local impl** (`catalog/local`):
    - wraps the segments directory scan (`ingest.SegmentFilesFS`,
      `scanSegmentsDir`) and `segment.Open`;
    - wraps the active file's flushed block index;
    - `RefsFrom` over sealed blocks, then active flushed blocks. For local
      mode, a ref is a (path, block index) handle rather than an object ID.
      Model this as a small ref source interface, so the cold reader does not
      care which one it has.
  - **Block committer** (D6): the ingest Writer commits through a
    committer. The local committer does write+fsync+Pebble commit exactly as
    `flushBlockLocked`/`commitDurableBatchLocked` do today. Seal goes through
    `BuildSealed` plus the file sink.
  - `OnAfterSeal(idx, path)` and `SegmentsDir` on `ingest.Config` become a
    catalog notification (sealed segment view) instead of a path.
  - `HotLog`: interface over `ReadableLog` (`ReadFrom`, `FloorSeq`, `TipSeq`,
    `DurableSeq`, notify). The local impl is the writer's log. Record
    `PendingForDID` as local-only.
  - Tests:
    - `catalog/local` against real segment dirs built by the writer (swarm:
      random block sizes, rotation, reopen).
    - `RefsFrom(seq)` covers `[seq, tip]` exactly once in order (property).
- [x] **S1.10 Read-path seams** (L). Deps: S1.9.
  - Cold reader (`subscribe/replay.go` `walkSealedSegment` `:352-425`,
    `walkActiveRegion` `:242-317`, `decodeSealedBlock` `:327-343`, and
    `ColdReader.Read` `:484-530`): read through `CatalogView.RefsFrom` and a
    block fetcher instead of paths plus `WalkActiveRangeFS`.
  - Cursor resolution (`subscribe/cursor.go` `translateTimeUSToSeq`
    `:288-368`): witnessed ranges from the manifest plus `CatalogView`.
  - Block cache (`subscribe/blockcache.go`): make the key an opaque
    comparable type. Local mode keeps `{segIdx, checksum, blockIdx, generation}`;
    disaggregated mode will use the object SHA-256 (S2.14).
    `InvalidateSegment` stays local-only.
  - xrpcapi: `getsegment.go:49` and `getblock.go:91` stop calling `os.Open`.
    They go through a `SegmentOpener` returning an `io.ReaderAt`-plus-size
    virtual file pinned to one generation. Keep the ETag and Content-Range
    rules (`specs/client.md`): a strong ETag equal to the header checksum hex,
    52-byte index entries, and each frame at offset+8.
  - Readiness: replace `lifecycle.IsSteadyState(*store.Store)` in
    `subscribe/handler.go:145` and xrpcapi `withReady` (`runtime.go:530-561`)
    with a `Readiness` interface. Local impl: phase plus manifest loaded, as
    today.
  - Compaction deadline: `xrpcapi` reads a `CompactionDeadlineSource`
    interface. Local impl: the in-memory `CompactionScheduleState`.
  - Manifest feed: replace `OnSegmentSealed(idx, path)` and
    `OnSegmentCompacted` with
    `ApplySegment(idx, gen, header, footer, createdAt, size)`. `readSealedMetadata`
    becomes a footer parser over bytes.
  - repoexport (`reconstruct.go:94`): read through a selector over
    `BlockRef`s. Drop the direct `backfill/live_segments` scan in favor of a
    namespace argument.
  - status: `scanActiveTail`/`InspectAll` through `CatalogView`. diskspace
    becomes optional (nil in disaggregated mode).
  - Tests: the existing subscribe, xrpcapi, and repoexport suites pass
    unchanged. Add a byte-identity test: the `getSegment` virtual file equals
    the on-disk sealed file for random segments.
- [x] **S1.11 Orchestrator seams** (L). Deps: S1.9.
  - `merge.go`/`merge_runner.go`: list and read merge sources through the
    catalog (`namespace = bootstrap_live`) and a block fetcher, instead of
    `SegmentFilesFS` + `segment.Open` (`merge_runner.go:63,117`,
    `merge.go:202,211`). The `live_segments` "not exists" guard (`:51-69`)
    becomes "namespace has no segments".
  - `compact_deletes.go`: list sealed segments via the catalog (`:222`). The
    rewrite goes through the S1.7 rewrite split with a local sink (`:362`).
    `rebuildLiveTombstones` (`:541-616`) reads through `RefsFrom` above the
    watermark (design §12.5).
  - Local-only filesystem work (stale tmp cleanup `:656`,
    `removeAllStorageFS(backfill)`, directory fsyncs, `fs.go`) moves behind
    the local catalog's namespace-delete and startup-cleanup methods.
  - Crashpoints stay in the same logical places. `specs/oracle.md` crash tier
    coverage is unchanged.
- [x] **S1.12 Leader skeleton and per-session runtime split (local mode)** (L).
  Deps: S1.10, S1.11.
  - `internal/leader`: `Locker` (the atmos `streaming.DistributedLocker` plus
    `Epoch() uint64`) and the election loop from design §6.3, with an
    injectable clock. Local mode uses `streaming.NoopLock` with epoch 1.
  - `internal/jetstreamd/runtime.go`: split `Build` into:
    - **per-process:** logger, registries, manifest, cold reader, subscribe
      tail, identity, status, web, server, xrpcapi, readiness, follower (a
      no-op in local mode);
    - **per-session:** orchestrator, writer, tombstone set, compactor, retry
      runners, syncstate, the metastore write handle.
    - The `writerPtr` coupling (`:235`), `OnSteadyStateWriter`/
      `tail.SetReadLogSource` (`:412-419`), and `steadyReady` become
      `HotLog`/`Readiness` plumbing that survives a session change.
  - Close ordering: per-session state tears down before the metastore closes
    (today `:828-833` keys this off orchestrator drain).
  - Tests:
    - election loop unit tests with a fake locker: renew failure cancels the
      session; ErrNotHolder cancels immediately; a corruption error exits;
      release is best effort;
    - a local-mode test that a session ending on a non-corruption error starts
      a new one in-process and the oracle still matches.

### Oracle and measurement

- [x] **S1.13 Storage-neutral oracle observers** (M). Deps: S1.9.
  - `internal/oracle/segments.go` `ObserveSegments`/`ObserveSealedSegments`/
    `ObserveBootstrapSegments` read through `CatalogView` + fetcher instead of
    directories. Local mode keeps a path-based observer as a cross-check. The
    oracle rule "observers must not silently substitute" applies: in local
    mode both must agree.
  - `compactionOverDropRecorder` and `bisectServedCompactedFailure` go through
    the same observer.
  - `durable_order_test.go` stays local-only by design (fsync ordering). Note
    that in `specs/oracle.md`.
- [x] **S1.14 Measurement: `next_seq - readable_log_durable_seq`** (S).
  - Explain why pop1 shows 7 (design §22 stage 1). Likely the unflushed
    partial block, since there are no age cuts (finding 3), but confirm with a
    test or trace. Record the explanation in the design.
- [x] **S1.15 Docs** (S). Deps: S1.12.
  - `specs/architecture.md`: storage interfaces, local impl, and the session
    split.
  - `specs/invariants.md`: phrase "fsync segment before Pebble commit" as a
    local committer rule, and add the backend-neutral form: "block durable
    before its metadata batch is visible".
  - `docs/README.md`: no behavior change. Update the internal architecture
    notes only.

**Checks (every S1 PR):** `just`, `just test-long ./internal/oracle`,
`just oracle-sweep`, `just fuzz 30s ./segment`, `just bench ./segment` for
S1.7/S1.9, and `just mutation-gate` on a clean tree.

**Exit:** local oracle and mutation campaign unchanged, apart from refreshed
STALE mutants with the same bug; `just test-long ./internal/oracle` and
`just oracle-sweep` pass; no core package imports `internal/store` or Pebble
for metadata; the S1.14 measurement is recorded.

---

## Stage 2: Steady state on fakes and real storage

Design §26 stage 2. Bootstrap is skipped in disaggregated tests by starting from
a seeded catalog (S2.17). Compaction is off in disaggregated mode (D5).

### Foundations

- [x] **S2.1 Dependencies and import boundary** (S).
  - Add `github.com/jackc/pgx/v5` and `github.com/aws/aws-sdk-go-v2` (core,
    `config`, `credentials`, `service/s3`). Update the AGENTS.md whitelist.
  - Add an import-boundary test: only the allowed packages import `pgx`/`aws`
    (ground rule 2).
  - Add `JETSTREAM_TEST_` to `knownForeignJetstreamEnvPrefixes`
    (`cmd/jetstream/main.go:74-83`) and update its tests.
  - `just vuln` is clean.
- [x] **S2.2 `internal/pgstore`: pool, migrations, schema checks, tx helpers**
  (M). Deps: S2.1.
  - `migrations/0001_init.sql`: the schema from design §8, exactly. An embedded
    migration runner: apply to an empty DB only, record `schema_version`.
  - Startup check: `schema_version` and `format_version` equal the compiled
    constants, otherwise refuse (design §8, §15.2).
  - Leader tx helper: `READ COMMITTED`. Any error or unknown `COMMIT` returns a
    session-ending error type (`pgstore.ErrSessionEnded`, wrapping the cause).
    It never retries. Metrics are `jetstream_pg_txn_duration_seconds{kind}`
    and `jetstream_pg_txn_errors_total{kind}`, with OTEL spans carrying the
    revision.
  - Reader tx helper: `REPEATABLE READ READ ONLY`.
  - `catalog.Tx` primitives as single SQL statements (D1). The multi-row
    metadata upsert uses `unnest` (design §14.2).
  - `LISTEN jetstream_catalog` connection helper with reconnect and backoff.
  - `pgstore/pgtest` test harness:
    - reads `JETSTREAM_TEST_PG_URL`; the test is skipped when unset, and fails
      when unset if `JETSTREAM_TEST_STORAGE_REQUIRED=1`, which `just test-storage`
      sets;
    - each test gets a scratch database `jst_<random>` created with the
      `jetstream` role's CREATEDB and dropped in cleanup, so worktrees sharing
      one `just up` do not collide;
    - an in-process TCP fault proxy (no deps) that can kill a connection
      mid-transaction, and drop the response to `COMMIT` after forwarding it
      ("commit applied, result unknown").
- [x] **S2.3 `internal/leader`: PG lease and contract suite** (M). Deps: S2.2,
  S1.12.
  - A PG lock implementing `DistributedLocker` + `Epoch()` with the §6.2 SQL.
    `holder_id` is a random UUID per process.
  - `leader/lockertest` contract suite (finding 6), run against `NoopLock`
    (the subset that applies), the storagefake lease (S2.7), and PG:
    - acquire while held by a live lease → `ErrLockHeld`;
    - renew after expiry or takeover → `ErrNotHolder`;
    - release when not holder → `ErrNotHolder`;
    - epoch strictly increases across holders;
    - two lockers racing to acquire → exactly one wins;
    - a stale holder after expiry cannot renew;
    - the old epoch's fence fails once a new holder acquires.
  - Metrics: `jetstream_leader_is_leader`, `_epoch`,
    `_sessions_total{result}`, and `_fence_failures_total`.
- [x] **S2.4 `metastore/pg`** (M). Deps: S2.2.
  - Get, and ordered batch apply with coalescing (consecutive Sets become one
    upsert, last write wins; consecutive Deletes become `= ANY`; `DeleteRange`
    ends a run). Keyset-paged iterator with 10,000-row pages. Read-only mode
    for reader pods: writes return an error.
  - A standalone `Commit` runs inside a fenced transaction. When a batch rides
    in a catalog transaction, `catalog` applies its ops through
    `Tx.ApplyMeta`.
  - The `metastore/storetest` contract suite runs against PG under
    `just test-storage`.
- [x] **S2.5 `objstore`: S3 blob and object protocol** (L). Deps: S1.8, S2.2,
  S2.6.
  - `objstore/s3`: `Blob` on aws-sdk-go-v2 with endpoint, region, bucket,
    prefix, path-style, and concurrency. Missing object: treat both 404
    `NoSuchKey` and 403 as "maybe missing", and have the protocol layer decide
    from catalog state (finding 14). Never treat 403 alone as proof of
    absence.
  - Retry with backoff up to `JETSTREAM_S3_RETRY_TIMEOUT`, then fail. Use an
    injectable `http.RoundTripper` so tests can inject 5xx, timeouts,
    truncated bodies, and wrong bytes (no deps).
  - `objstore.Store` protocol (D2):
    - §7.3 with dedup, the `gc_delay/2` age condition, batched `uploading`
      rows, read-back verify, the unique-index race, and the
      skip-PUT-after-`orphan_age/2` rule;
    - an option to combine step 6 with the referencing transaction;
    - §7.5 read verify and refresh-and-retry, with corruption counted as
      `jetstream_storage_corruption_total{source}`.
  - Compressed object cache: an LRU by SHA-256 with a byte budget
    (`JETSTREAM_OBJECT_CACHE_BYTES`) and a usage gauge.
  - Metrics: `jetstream_s3_requests_total{op,result}`, `_request_duration_seconds`,
    `_bytes_total`, `_verify_failures_total`, plus spans with the object ID.
  - Contract suite (`objstore/blobtest`) against memblob always, and against
    SeaweedFS and MinIO under `just test-storage`, using per-test key
    prefixes.

### Catalog core

- [x] **S2.6 `internal/catalog` transaction scripts** (L). Deps: S1.9, S2.2.
  - Backend-neutral scripts over `catalog.Tx` (D1):
    - `CommitHotBatch` (§10.4)
    - `CommitBlock` (§10.6)
    - `Fold` (§10.7), with the exact-coverage check
    - `Seal` (§10.8), with the block-list-equals-footer check
    - `PublishGeneration` (stub until S4)
    - `DeleteNamespace` (S3)
  - Each script runs the fence first, checks the seq key with FOR UPDATE, and
    runs reference checks for every object it references (§7.4).
  - `catalog.CheckInvariants(snapshot)`: all seven invariants from §9.3,
    written once. It runs after every transaction in storagefake, at leader
    session start (cheap subset), and in `just test-storage` against PG.
  - A corruption error type, distinct from session-ending errors, that the
    election loop turns into a process exit (§6.5).
  - Tests: script-level tests on storagefake covering each rejection path
    (fence lost, seq mismatch, missing reference, fold gap, seal list
    mismatch).
- [x] **S2.7 `internal/storagefake`** (L). Deps: S2.6.
  - In-memory tables for every §8 table, with PostgreSQL semantics:
    - the `archive` row lock that serializes fenced transactions;
    - revisions in commit order;
    - unique, check, and FK constraints, including the partial unique index
      `objects_sha256_available` and `segments_one_active`;
    - `REPEATABLE READ` snapshots for readers;
    - NOTIFY delivered on commit.
  - The lease implementation, `metastore.Store` over `metadata_kv`, and the
    reader snapshot API the follower uses.
  - Fault injection, each with a fired counter, following the "schedule, fired
    counter, Unfired check" pattern from `internal/oracle/faults.go`:
    - commit fails;
    - commit applied but reported failed;
    - connection lost mid-transaction;
    - NOTIFY lost;
    - slow reader snapshot.
  - The deterministic scheduler hook (D4).
  - The layer 4 primitive contract suite runs against storagefake here, and
    against PG in S2.20.

### Write path

- [x] **S2.8 Writer hot mode: batching and committer** (L). Deps: S1.9, S2.6.
  - Batch cuts (§10.3): `min(256, block remaining)` events, 256KiB raw, 15ms
    age, block close, or class change. Block cuts: 4096 events or 30s age
    (new, finding 3), using an injectable clock.
  - Freeze: encode the frame and sample `DurableBatchPrepareValue`.
  - One committer goroutine commits in strict seq order. Pointer batches
    upload concurrently and commit in turn. `afterCommit`/`afterDone`, producer
    acks, and the follower doorbell run after commit. On failure the session
    ends.
  - Seq allocation (§10.2): read the seq key at session start. The commit
    checks the stored value equals `first_seq`. Uncommitted seqs are dropped
    at session end. The local seq lease stays off.
  - Closed blocks go to the maintainer (S2.10) with their in-memory events.
  - Tests:
    - swarm over random batch/block/age parameters on storagefake: committed
      batches tile `[1, seq/next)` with no gaps (§9.3 inv. 1, 5, 6);
    - producer acks never precede commit;
    - a batch never crosses a block boundary.
- [x] **S2.9 Admission control** (M). Deps: S2.8.
  - Live and bulk classes. Append-lock live priority, with bulk yielding
    between chunks when the "live waiting" counter is non-zero. Class change
    cuts the batch. Live batches are inline, paid from a token bucket over
    frame bytes. Overflow mode produces pointer batches (1024 events / 1MiB /
    1s). Bulk batches are always pointer batches, gated by bulk permits.
  - Caps: upload concurrency, total frozen-uncommitted bytes, and the unfolded
    events cap (§10.5 rules 1-9).
  - Tag the producers: live consumer = live; failed-repo retry, sync 1.1
    resync, and PDS recovery = bulk. Survey the call sites in
    `backfill/retry.go`, `live/consumer.go`, and the orchestrator steady
    errgroup (`steady.go:106-139`).
  - Metrics: `jetstream_hot_batches_total{class,storage}`, `_batch_events`,
    `_unfolded_events`, `_pending_bytes{class}`,
    `jetstream_admission_wait_seconds{class}`, and `jetstream_hot_inline_tokens`.
  - Tests:
    - deterministic (synctest) tests showing live latency stays bounded while
      a bulk flood runs;
    - each cap blocks and unblocks;
    - a class change always cuts the batch.
- [x] **S2.10 Maintainer: fold and seal** (L). Deps: S2.8, S1.7.
  - One goroutine that runs fold and seal in order, never concurrently.
  - Fold (§10.7): encode the block from memory, upload (dedup may hit), then
    commit with the exact-coverage delete.
  - Seal (§10.8): `BuildSealed` over a frame source backed by the object cache
    or S3, upload the footer, then commit with the block-list check.
    Rotation uses virtual size ≥ `MaxSegmentBytes`, plus `ForceRotate` and
    `SealActiveAndClose`.
  - Hot batches keep committing during a seal. Folds wait. The unfolded cap
    bounds the lag.
  - Tests:
    - a sealed generation served as a virtual file passes
      `segment.VerifySealedMetadata`, and is byte-identical to the local-mode
      seal of the same events;
    - fold dedup reuses a whole-block pointer batch;
    - a seal crash between upload and commit leaves the catalog unchanged.
- [x] **S2.11 Session start rebuild** (M). Deps: S2.10.
  - The §10.9 steps: load the active segment and its blocks, load all hot
    batches, group greedily into ≤4096 without splitting a batch, fold full or
    aged groups, seal on rotation, and keep the remainder as the open block.
  - Rebuild the tombstone set via `RefsFrom` above `compaction/seq` (§12.5).
  - Start the live consumer at `relay/cursor`.
  - Tests: hot batch rows left by sessions with different block boundaries
    rebuild correctly (property test over random prior-session shapes).
- [ ] **S2.12 Live consumer: per-batch relay cursor** (M). Deps: S2.8.
  - Drive the safe-cursor watermark (`prepareValue`) per batch instead of per
    block (`live/consumer.go:322,342-380`).
  - The required test (§10.4, §20): a single upstream commit split across two
    batches, with the leader killed between the two commits. The next session
    re-requests from a cursor at or before that commit, and no row is lost or
    duplicated.
  - §9.3 invariant 7 is added to `CheckInvariants`, fed by storagefake's view
    of which upstream seqs are fully committed.

### Read path

- [ ] **S2.13 Follower and mirror** (L). Deps: S2.6, S2.7.
  - One follower per pod. It wakes on NOTIFY, a 250ms timer, the in-process
    doorbell, or a synchronous refresh request.
  - Each tick is one `REPEATABLE READ READ ONLY` transaction running the §11.1
    steps. After it: verify pointer objects, append new hot events to the
    readable log in seq order and advance its durable watermark, swap the
    mirror (`atomic.Pointer`), and publish sealed segments to the manifest
    after fetching their footers.
  - Folded-between-ticks handling: read the missing seqs via `RefsFrom` before
    any later hot batch. The readable log receives every seq exactly once.
  - Pod start: a full mirror load and footer load with bounded concurrency
    (`JETSTREAM_S3_READ_CONCURRENCY`). The readable log starts at tip+1.
  - Metrics: `jetstream_catalog_revision`, `_lag_seconds`,
    `_refresh_duration_seconds`, `_notify_received_total`, and
    `jetstream_event_visibility_latency_seconds`.
  - Tests on storagefake:
    - lost NOTIFY is covered by polling;
    - a fold between ticks does not skip or duplicate seqs;
    - a slow follower drops the pod out of readiness after `MAX_VIEW_AGE`;
    - property: the readable log equals the committed stream.
- [ ] **S2.14 Read endpoints in disaggregated mode** (L). Deps: S2.13, S1.10,
  S2.5.
  - Cold reader: a disaggregated `BlockRef` fetcher (object cache, then
    `objstore.Store`; inline frames from the mirror). The block cache is keyed
    by object SHA-256, and by `first_seq` plus SHA-256 for inline frames.
  - Cursor resolution over manifest plus mirror. A synchronous tick runs
    before answering for a seq, segment, block, or `beforeSeq` above what the
    mirror knows (§11.6).
  - `getSegment` as a virtual file over one pinned generation: header row,
    then length-prefixed block objects, then the footer object. Range requests
    map onto parts via `GetRange`. HEAD fetches no objects. `Last-Modified` is
    the generation's `created_at`.
  - `getBlock`, and `planSnapshot` with `sealedTipSeq` from the mirror.
  - Response cutoff after `JETSTREAM_MAX_ARCHIVE_RESPONSE_DURATION`. The
    startup GC-delay inequality check (§11.7) lands here even though GC is S4.
  - Readiness: mirror fresh, `phase = steady_state`, and footers loaded.
  - The Cache-Control deadline is read from `metadata_kv`
    `compaction/deadline` via the follower (the writer side lands in S4).
  - status: switch the no-manifest scans to `backfill/counts` or
    `pg_class.reltuples` (§11.9). `pending.go` returns nothing.
  - Tests:
    - the real Go client (`client_observer_test.go` path) downloads and
      verifies segments from a disaggregated pod on storagefake + memblob;
    - range requests across part boundaries;
    - ETag stability;
    - an object replaced mid-request by a generation change triggers the
      client's generation retry.

### Wiring

- [x] **S2.15 Configuration and CLI** (M). Deps: S2.2, S2.5.
  - Flags with env sources for every design §18 variable, on `serve` (and on
    `storage` where relevant): `JETSTREAM_STORAGE`, `JETSTREAM_PG_*`,
    `JETSTREAM_S3_*`, `JETSTREAM_LEADER_*`, `JETSTREAM_HOT_*`,
    `JETSTREAM_BLOCK_MAX_AGE`, `JETSTREAM_CATALOG_POLL_INTERVAL`,
    `JETSTREAM_MAX_VIEW_AGE`, `JETSTREAM_MAX_ARCHIVE_RESPONSE_DURATION`,
    `JETSTREAM_OBJECT_CACHE_BYTES`, and the GC and compaction memory knobs
    (which must be declared now so they are not rejected).
  - Validation: `JETSTREAM_DATA_DIR` explicitly set together with
    disaggregated mode is refused (finding 9). Compaction plus disaggregated
    mode is refused until S4 (D5).
  - Secrets: the PG URL is redacted in every log and error path, and absent
    from `/status`. Add a test that the password does not appear in captured
    logs, the status snapshot, or `serve --help`.
  - `.env` gets no new secret values. README documents how to point `serve`
    at `just up`.
- [ ] **S2.16 Runtime wiring for disaggregated mode** (L). Deps: S1.12,
  S2.3-S2.5, S2.8-S2.14.
  - Per process: pgx pool, read-only metastore, follower, mirror, manifest fed
    by the follower, object cache, and the identity cache as a pod-local LRU
    (finding 8).
  - Election loop with the PG lease. Per session: fenced metastore, hot-mode
    writer, maintainer, live consumer, and retry runners.
  - Startup order (§15.2): config and budget checks, PG schema check, follower
    plus first full load plus footers, HTTP, then the election loop.
  - `jetstreamd.Options` gains an injectable storage bundle (catalog Tx
    factory, blob, lease, clock), so tests can pass storagefake + memblob
    without env vars.
- [ ] **S2.17 Seeded-catalog test fixture** (M). Deps: S2.10.
  - Build a catalog that is already in `steady_state` (sealed segments, an
    active segment with blocks, a `relay/cursor`, and `repo/` rows) from a
    simulator world. The easiest correct route is running local-mode bootstrap
    in memory and importing the result through the catalog scripts. Use it for
    the layer 3 steady-state oracle and the stage 2 measurements.

### Testing

- [ ] **S2.18 Layer 3 oracle: steady state with failover** (L). Deps: S2.16,
  S2.17.
  - A new oracle configuration (`internal/oracle`, file prefix `disagg_`):
    storagefake + memblob, a seeded scheduler (D4), and a single synctest
    bubble (finding 11).
    - Two or more reader pods plus leader-capable pods, all real
      `jetstreamd` runtimes on pipe listeners.
    - The simulator, model, event-log, and checkers are reused unchanged
      (`specs/oracle.md`, "Preserve Local Coverage").
  - Fault kinds, each scheduled from the seed and proven fired:
    - leader kill at each seam: before upload, after upload before commit,
      commit applied but reported failed, after commit before acks;
    - lease loss;
    - a stale leader writing after its successor (the old session handle kept
      alive);
    - S3 PUT failure and S3 wrong bytes;
    - lost NOTIFY;
    - slow follower.
    - New crashpoints go into `internal/crashpoint` `AllPoints` (e.g.
      `after-hot-batch-upload-before-commit`, `after-fold-upload-before-commit`,
      `after-seal-footer-upload-before-commit`).
  - Checks:
    - `catalog.CheckInvariants` after every transaction;
    - each reader pod's delivered stream (v1 and v2 websocket) vs the model:
      no missing event, no seq reuse, per-DID order kept;
    - cross-pod equality of delivered streams over the common seq range;
    - archive download by the real client from each pod.
  - Tiers: `-short` runs one small seed in under about 1s wall time. The
    default mode runs the full fault mix. A `just oracle-disagg` recipe and an
    `oracle-disagg-sweep` for multi-seed runs. Check the CI race budget
    (finding 12).
  - Determinism test (D4): the same seed produces the same trace.
  - `specs/oracle.md`: add the tier, what it proves and does not prove (it
    cannot prove real PG/S3 semantics; layer 4 does).
- [ ] **S2.19 Stage 2 mutants** (M). Deps: S2.18.
  - Add, each with header fields (`expected-tier: disagg`) written before the
    first run:
    - skip the fence (in the `catalog` script, D1);
    - fold deletes one batch too few;
    - relay cursor advances per block instead of per batch;
    - the follower drops a hot batch;
    - seal reorders active blocks.
    - Plus two more, recommended: the hot-batch commit skips the seq-key
      check, and the reference check is skipped in `CommitHotBatch`.
  - `testing/mutation/run.sh`: add a `disagg` tier case (layer 3, no
    containers). Bank the results with `just mutation-baseline`, and add a
    dated `RESULTS.md` section plus the catalog line.
- [x] **S2.20 Layer 4 contract suites and `just test-storage`** (L). Deps:
  S2.2-S2.5, S2.7.
  - justfile `test-storage`: requires a running `just up` (or starts it and
    tears it down: decide in the PR, and document). It sets
    `JETSTREAM_TEST_PG_URL`, the S3 endpoints for SeaweedFS and MinIO, and
    `JETSTREAM_TEST_STORAGE_REQUIRED=1`, then runs the storage-tagged
    packages. Each suite runs once per object store.
  - Suites:
    - `catalog.Tx` primitives: pg vs storagefake, the same tests;
    - `catalog` scripts on PG, with `CheckInvariants` after each;
    - `metastore/storetest` on PG;
    - `objstore/blobtest` plus the protocol on SeaweedFS and MinIO;
    - `leader/lockertest` on PG.
  - Fault injection on real storage:
    - connection kill mid-transaction and COMMIT-result loss (TCP proxy,
      S2.2);
    - S3 5xx, timeouts, and wrong bytes (RoundTripper, S2.5);
    - two lockers racing and a stale holder after expiry;
    - a follower on the reader role (`jetstream_reader`) proves reader pods
      need no write privilege.
  - CI: a new `test-storage` job in `.github/workflows/ci.yml`:
    - harden-runner with the standard allowlist plus the Docker Hub hosts
      from the `release.yml` precedent (`registry-1.docker.io`,
      `auth.docker.io`, `production.cloudflare.docker.com`,
      `production.cloudfront.docker.com`);
    - `just up`, `just test-storage`, and `just down` under `if: always()`;
    - confirm the `docker compose` plugin resolves inside `nix develop` on
      ubuntu-24.04.
- [ ] **S2.21 Fuzz targets** (S). Deps: S2.6, S2.13.
  - A hot-batch row decoder (descriptor plus frame) and a footer object parser
    fed with arbitrary bytes as they come from storage (design §20). Add both
    to the `ci-scheduled.yml` fuzz matrix and to `testing/ci/workflows_test.go`.
- [ ] **S2.22 Stage 2 measurements** (M). Deps: S2.16, S2.20.
  - Record in the design (§22):
    - PG p50/p99 commit latency at 3,000 events/s with repo upserts (RDS and
      self-hosted);
    - end-to-end live latency, idle and during bulk recovery (20-40ms target);
    - seal duration;
    - pod start footer load time and manifest memory at pop1 size (synthetic
      7,000 segments);
    - PG WAL volume per day.
  - Add a load driver (a simulator traffic mode or a `cmd/` tool) that runs
    against `just up`, so these can be repeated.

**Checks:** `just`, `just test-storage`, `just oracle-disagg` (plus a sweep),
`just test-long ./internal/oracle` (local still green),
`just mutation-gate` (clean tree), `just fuzz` for the new targets, and
`just bench ./segment`.

**Exit:** the layer 3 oracle passes steady state with failover and all stage 2
faults, with every scheduled fault proven fired; layer 4 suites pass on
PG+SeaweedFS and PG+MinIO in CI; every stage 2 mutant is KILLED; stage 2
measurements are recorded.

---

## Stage 3: Bootstrap and merge

Design §10.6, §10.10, §15.1, and §26 stage 3.

- [ ] **S3.1 Direct mode** (L). Deps: S2 complete.
  - The block committer for direct mode: freeze the block, upload it
    concurrently (the `async_flush.go` compress workers extended to upload),
    then commit in order via `CommitBlock` with the seq-key check and
    reference check, carrying the metadata batch (`DurableBatchHook` output).
  - Namespaces: `main` with `seq/next`, and `bootstrap_live` with
    `live_segments/seq/next`.
  - Seal in direct mode uses the same maintainer seal path over committed
    blocks.
  - Tests: swarm (the `writer_swarm_test.go` pattern) on storagefake;
    invariants hold per namespace.
- [ ] **S3.2 Bootstrap on the catalog** (M). Deps: S3.1.
  - `orchestrator/bootstrap.go`: both writers in direct mode. Backfill
    checkpoints go through the completion batcher hook inside block commits
    (`backfill/run.go:117`, `completion_batcher.go:158`). `finishBootstrap`
    seal-reopen (`:238-263`) goes through the catalog.
  - The phase write (`states.go` `writeMergingPhase`) becomes a fenced
    metastore commit.
- [ ] **S3.3 Merge on the catalog** (L). Deps: S3.1, S1.11.
  - The merge runner reads `bootstrap_live` blocks via `RefsFrom` + objstore in
    seq order, applies the rev filter unchanged (`merge_filter.go`), and
    appends survivors to `main` in direct mode.
    `commitSourceComplete` becomes a fenced metastore batch.
  - Pending retry pass (`merge.go:116-134`) as today, on the direct writer.
  - Merge-tail compaction is skipped in disaggregated mode until S4 (D5).
  - Discovery (`merge_discovery.go`) writes through metastore.
  - One final fenced transaction (`catalog.DeleteNamespace` plus the phase
    write): delete all `bootstrap_live` rows and `live_segments/*` keys, and
    write `phase = steady_state` and `phase/entered_at`. The crashpoints
    `AfterMergeDiscoveryBeforeCleanup` and `AfterMergeCleanupComplete` bracket
    it.
  - Resumability: a kill at any point before the final transaction resumes
    through `merge/next_source_idx`.
- [ ] **S3.4 `jetstream storage init`** (M). Deps: S2.2, S2.5.
  - The §15.1 steps: refuse if `archive` exists; apply migrations; insert
    `archive` with a random `archive_id`; create segment 0 active in both
    namespaces; PUT/GET/DELETE a probe under
    `<prefix>/<archive_id>/probe/<uuid>`.
  - Flags share the `serve` storage flags. Test against `just up`, plus a
    storagefake unit test.
- [ ] **S3.5 Layer 3: full lifecycle with kills in every phase** (L). Deps:
  S3.2, S3.3.
  - Extend the S2.18 harness to start from an empty initialized catalog. Kill
    the leader in bootstrap, merging (at every merge crashpoint), and steady
    state. Add the leader-kill seams for direct mode: before upload, after
    upload before commit, commit applied but reported failed.
  - The oracle checks the bootstrap snapshot, the post-merge state, and steady
    streams, as the local lifecycle harness does
    (`harness_test.go:370-392`), against the uncompacted model (D5).
  - Mutants (recommended): the direct commit skips the seq-key check, and the
    final merge transaction leaves `live_segments/seq/next` behind.
- [ ] **S3.6 Stage 3 measurements** (M). Deps: S3.3.
  - Fenced transactions per second during bootstrap (block commits plus
    metadata writes).
  - Retry-scan cost against `metadata_kv` (the 9GB-per-pass estimate).
  - Record both in the design.

**Checks:** `just`, `just test-storage`, `just oracle-disagg` (plus a sweep),
`just test-long ./internal/oracle`, and `just mutation-gate`.

**Exit:** the layer 3 oracle covers the full lifecycle with kills in every
phase; `storage init` works against `just up`; stage 3 measurements are
recorded.

---

## Stage 4: Compaction and GC

Design §12, §13, and §26 stage 4.

- [ ] **S4.1 Sparse rewrite (pure)** (L). Deps: S1.7.
  - In `segment`: given a generation (header, footer, block fetcher) and a
    tombstone snapshot, run the §12.2 steps:
    - candidate blocks by per-block bloom plus seq plus collection bitmask;
    - decode and drop;
    - the exact vanished-DID check, fetching bloom-hit blocks;
    - re-encode changed blocks;
    - a new footer (sizes, recomputed offsets, blooms unchanged, collection
      counts, remapped bitmasks);
    - a new header (`event_count`, `unique_did_count`, checksum).
  - The output is a list of new frames by ordinal, reused ordinals, the new
    header, and the new footer. No I/O.
  - **Equivalence property test** (§12.3): for generated segments and
    tombstone sets, the sparse output passes `VerifySealedMetadata` and
    decodes to exactly the rows of `segment.Rewrite`. Every field that
    `VerifySealedMetadata` checks exactly must match.
  - A fuzz target over random tombstone sets and segments.
- [ ] **S4.2 Compaction on the catalog** (L). Deps: S4.1, S2.10.
  - The pass (§12.1): force-rotate, snapshot tombstones and W, pick segments by
    segment DID bloom with `min_seq < W`, rewrite up to
    `JETSTREAM_COMPACTION_REWRITE_WORKERS` at once, upload the new blocks and
    footer, then `PublishGeneration` (source generation check plus reference
    checks). Then a fenced CAS on `compaction/seq`.
  - Memory budget: workers wait for `JETSTREAM_COMPACTION_MEMORY_BYTES`
    before decoding (§12.6).
  - Merge-tail compaction enabled in disaggregated mode (§12.4).
  - `compaction/deadline` plus "pass running since" written in fenced
    transactions (§12.7). Followers compute Cache-Control from it.
  - Remove the D5 startup refusal.
- [ ] **S4.3 GC** (M). Deps: S2.5, S2.6.
  - Leader task every `JETSTREAM_GC_INTERVAL`:
    - mark, in pages of 10,000;
    - claim up to 1,000 plus the in-transaction re-check, where a hit is
      corruption;
    - delete outside the transaction, where "not found" (404 or 403) is
      success;
    - forget;
    - a `deleting` rows resume path.
  - This is backend-neutral code over `catalog.Tx` (D1).
  - Metrics: `jetstream_objects{state}`, `jetstream_gc_deleted_total`, and
    `jetstream_gc_run_duration_seconds`.
- [ ] **S4.4 Layer 3 with compaction and GC** (M). Deps: S4.2, S4.3.
  - Turn on compaction and GC in the disaggregated oracle, and switch it to the
    compacted-model checks (`assertCompacted`, fold convergence via the
    client).
  - GC runs with short fake-clock delays. Readers holding old generations
    across a compaction keep working until `GC_DELAY`.
  - Leader kills during rewrite upload, before publish, between chunks, and
    during each GC step.
- [ ] **S4.5 Stage 4 mutants** (S). Deps: S4.4.
  - GC skips the re-check.
  - Sparse compaction miscounts `unique_did_count`.
  - Recommended extras: publish skips the source-generation check, and the
    sparse rewrite keeps a stale collection count.
- [ ] **S4.6 Stage 4 measurements** (S). Deps: S4.2.
  - Fraction of blocks fetched per compaction pass, and tombstone rebuild time
    on session start. Record both in the design.

**Checks:** `just`, `just test-storage`, `just oracle-disagg` (plus a sweep),
`just fuzz 30s ./segment`, `just test-long ./internal/oracle`, and
`just mutation-gate`.

**Exit:** the equivalence property test passes; compaction and GC mutants are
KILLED; stage 4 measurements are recorded.

---

## Stage 5: Soak and operations

Design §15.4, §17, §20 layer 5, and §26 stage 5.

- [ ] **S5.1 `jetstream storage new-identity`** (M).
  - The §15.4 steps: new `archive_id`, bump `writer_epoch`, clear
    `holder_id`, delete hot batches whose objects are missing, and verify every
    other referenced object (GET plus hash). Refuse on any missing sealed or
    active block.
  - Test: restore-like fixtures on storagefake plus `just test-storage`.
  - Operator doc in `docs/`.
- [ ] **S5.2 Memory budgets** (M).
  - `GOMEMLIMIT` required in disaggregated mode. Sum the configurable budgets
    plus the measured manifest size, and refuse above 75% with a per-budget
    message (§17). Check before and after the footer load.
  - Gauges: `jetstream_memory_budget_bytes{budget}` and
    `jetstream_memory_used_bytes{budget}`.
- [ ] **S5.3 Dashboards and alerts** (S).
  - `contrib/grafana/jetstream.json`: rows for leader, PG transactions, hot
    path and admission, S3, catalog follower, GC, and memory budgets
    (design §23).
  - Alerts: corruption > 0, catalog lag > 10s, no leader for > 10s, unfolded
    events near the cap.
- [ ] **S5.4 Soak harness and 24h run** (L).
  - Three pods against real PostgreSQL and SeaweedFS, fed by the simulator (or
    a real relay).
  - A chaos driver for random leader kills, PG failovers (restart or
    `pg_terminate_backend`), and S3 outages (a proxy or a stopped container).
  - End-state check (§20 layer 5): every source event is archived exactly once;
    every `getSegment` output passes `VerifySealedMetadata`; websocket clients
    on different pods received equal streams.
  - Report the §22 measurements.
- [ ] **S5.5 Docs and living specs** (M).
  - `docs/README.md`: disaggregated mode, config, operations, restore, and the
    fixed checksum statement.
  - `specs/architecture.md`, `specs/invariants.md` (catalog invariants,
    fence), `specs/glossary.md` (epoch, fence, hot batch, fold, mirror,
    generation), and `specs/gotchas.md` (403 vs 404, accepted S3 orphan leak,
    compaction was disabled before S4).
  - `specs/client.md`: confirm no wire changes; `Last-Modified` semantics.

**Exit:** a 24h soak passes the end-state oracle check, and the measurements
are reported. Only after this: deploy the new pop instance in disaggregated
mode.

---

## Risks and mitigations

| Risk | Where | Mitigation |
|---|---|---|
| The session split (S1.12) destabilizes local mode | runtime wiring | Done under local mode first with the full local oracle and restart tiers; keep the PR narrowly about construction order |
| The layer 3 scheduler is not actually deterministic | D4 | Determinism test from day one; fall back and document a weaker claim rather than paper over it |
| Mutant refresh churn in stage 1 hides regressions | S1.x | Refresh STALE mutants in the same PR that moved their target; the reviewer checks the refreshed diff is the same bug |
| The storagefake drifts from PostgreSQL | D1, D3 | One primitive contract suite runs against both in CI (`test-storage`) |
| CI Docker egress breaks `test-storage` | S2.20 | Pin images by digest (already done); allowlist from the `release.yml` precedent; retry workflow if runner-loss becomes common |
| Stage 2 performance misses the 20-40ms live latency target | S2.8, S2.9 | S2.22 measures early with the load driver; batch age and token bucket are config |
| The metadata write path is slower on PG than Pebble at bootstrap rates | S3 | S3.6 measures fenced transactions per second before stage 3 exits |

## Decisions log

Record deviations from the design and answers to D1-D7 here, newest first, with
the PR that made them.

- **S2.11 (2026-09-25): session start rebuild.**
  - `Maintainer.Rebuild(ctx, RebuildConfig)` runs §10.9 steps 2 to 6 and
    returns an `*ingest.OpenBlock`, which the hot writer takes as
    `HotConfig.Resume`. The session order is `maintainer.Open`, `Rebuild`,
    `ingest.Open` with the resumed block, then the live consumer (S2.16 wires
    it).
  - The rebuild starts with the cheap `CheckInvariants` subset on one read
    snapshot, with `RebuildConfig.RelayCursor` for invariant 7 (S2.12 supplies
    it). A violation ends the session before anything is folded. The check is
    O(archive); S2.22 measures it.
  - Every group but the last folds, since none can grow. The last folds if it
    is full, or if its first batch's `committed_at` (database clock, clamped to
    now) is at least `BlockMaxAge` old. Otherwise it is the open block, and its
    age cut runs from that same `committed_at`.
  - The rotation rule now also runs before each fold and on `Sync`/`Rotate`
    requests, not only after a fold. An earlier session whose fold committed
    but whose seal was lost leaves a segment at the threshold; this seals it
    before folding onto it, so output stays byte-identical to local mode.
  - The writer refuses to open over hot batches it was not given, and checks
    `Resume` against the catalog exactly (batch count, ranges, object ids,
    event seqs, and the block being non-empty and not full). A mismatch is
    `ErrInvalidConfig`; a gap in the catalog rows is corruption.
  - Resumed events are not added to the in-memory read log, which starts at
    `seq/next`; readers get them from the catalog. Rebuilt batches report class
    `live`, because the catalog does not record class.
  - Steps 7 and 8 (tombstone rebuild, live consumer at `relay/cursor`) are the
    existing orchestrator paths (`rebuildLiveTombstones`, `live.Open`). S2.16
    points them at the disaggregated catalog and fenced metastore.
  - New metric `jetstream_maintainer_rebuild_duration_seconds`.
  - The property test runs 40 seeds of 3 to 7 prior sessions each, with random
    batch sizes, bulk pointer batches, block ages, segment thresholds, sinks
    that drop some or all closed blocks, and one-shot commit failures and lost
    commits on hot batch, fold, and seal transactions. After a final clean
    session, the sealed and active archive must equal the model exactly.
    Six hand mutants of the grouping, age, full, rotation, and invariant logic
    were all killed.

- **S2.10 (2026-09-25): maintainer fold and seal.**
  - New package `internal/ingest/maintainer`. `Maintainer` implements
    `ingest.BlockSink`, and one goroutine per session runs its queue in order.
    `Open(ctx, cfg)` loads main's active segment and blocks from one read
    snapshot. ctx bounds every fold and seal, so pass the session's context.
  - The rotation rule compares `Σ(8 + compressed_length)` without the
    256-byte header. That is local mode's `activeBytes`, and design §10.8 is
    corrected to say so. It means both modes seal byte-identical files from
    the same events and block boundaries, which the tests check.
  - `Rotate` seals only when the active segment has blocks, the same no-op as
    local `ForceRotate`. `Sync` waits for everything queued before it to fold,
    for S2.11's rebuild.
  - A fold that commits at a segment or ordinal other than the one memory
    predicts is corruption, because the seal's block list comes from memory.
    Each fold adds its frame to the object cache, and each seal adds its
    footer. A seal reads blocks through `objstore.Store`, with
    `ReadConcurrency` (default 8) reads in flight.
  - Any failure ends the session, and `OnFailure` runs once. The owner then
    closes the writer, which releases appends blocked on the unfolded cap,
    and then the maintainer. `Close` finishes the fold or seal in progress
    and leaves queued blocks as hot batches for the rebuild. Requests still
    queued return `maintainer.ErrClosed`.
  - The seal-crash test uses storagefake's `FaultCommitFails` and
    `FaultCommitLost` on `TxSeal`, not a new crashpoint. After the fault, the
    next session seals the right file either way.
  - Metrics: `jetstream_maintainer_{folds_total{result},
    fold_duration_seconds, seals_total, seal_duration_seconds, queued_blocks,
    active_segment_bytes}`. S2.16 registers them.
- **S2.9 (2026-09-25): hot mode admission control.**
  - The token bucket charges a live batch its raw bytes at freeze and settles
    to the frame's size after `prepare` encodes it off the lock. The rate
    holds over frame bytes, and the bucket can dip below zero. It applies only
    with an uploader. A negative `InlineBytesPerSec` disables it.
  - Live cuts: an ordinary cut (events, bytes, age) that the bucket cannot pay
    for turns the batch into an overflow batch. Overflow batches are always
    pointers. A forced cut (class change, block close, Flush, drain, Close) of
    an unpaid live batch makes a pointer batch at once, even a tiny one. S2.22
    should measure how often that happens.
  - A bulk batch stays open across `AppendBatch` calls up to
    `BulkChunkMaxEvents`. The chunk size is capped by the open bulk batch's
    room and the block's room, so a chunk never spans batches and live appends
    land only on bulk batch boundaries. The batch holding a chunk collects its
    permits and releases them at commit. A chunk that fails before landing any
    event returns them at once.
  - Rule 7: the process-wide PUT bound is `s3.Blob`'s upload concurrency. The
    writer's `UploadConcurrency` bounds its own in-flight `Upload` calls. S2.16
    sets both.
  - Unfolded events are counted as the commit watermark minus the fold
    watermark. At open, the fold watermark comes from the first `hot_batches`
    row. `ClosedBlock.Folded`, which is idempotent and callable from any
    goroutine, moves it forward. S2.10's maintainer calls it after each fold
    commits. With a nil Sink, a block folds as soon as it commits.
    `MaxUnfoldedEvents` must be at least one block (the effective block size
    when `MaxEventsPerBlock` is 0).
  - The live admission-wait histogram leaves out the mutex wait: it measures
    cap waits only.
  - Overflow sizes and the bulk chunk size are `HotConfig` fields for tests.
    Their defaults are code constants, with no env vars (design §18).
  - Producer survey: the live consumer is tagged live. `RunFailedRepoRetry`
    is tagged bulk; it also carries sync 1.1 resync replacements. There is no
    separate PDS-recovery producer yet. Bootstrap backfill and merge run in
    direct mode, and the steady compactor does not append.
  - Tests: `admission_test.go` holds synctest tests for:
    - live priority on batch and block boundaries;
    - bucket overflow by size and by age, forced-cut pointers, and bucket
      settlement;
    - each cap blocking and releasing at once;
    - permit return on a failed chunk;
    - the unfolded cap with and without a sink;
    - live latency under a 100-chunk bulk flood. The bound is batch age plus
      one upload; without permits the worst latency is 645ms against a 70ms
      bound.
    The swarm also randomizes every new limit. 23 hand-made mutants against
    `admission.go` and the cut logic are all killed.
  - Fixed along the way: `MaxUnfoldedEvents` validation now uses the default
    block size when `MaxEventsPerBlock` is 0.

- **S2.20 (2026-09-25): layer 4 storage suites and `just test-storage`.**
  - `test-storage` requires a running `just up`. It exits early with a clear
    message if postgres, seaweedfs, or minio is not running, and it never
    starts or stops the environment, so a failed run can be inspected.
  - The package list comes from `go list`: every package whose tests import
    `pgtest` or `s3test`, so a new suite cannot be left out. Pass 1 runs all
    of them on PostgreSQL plus SeaweedFS. Pass 2 runs the `s3test` importers
    on MinIO. Arguments pass through to go test.
  - The catalog script tests run on both storagefake and PostgreSQL. A
    `catalog.DB` wrapper runs `LoadSnapshot` plus `CheckInvariants` after
    every commit on both. The two tests that expect a violation pass on PG,
    so the check does fire there. Each PG script test creates a scratch
    database (about 0.18s); plain `just` skips them.
  - New `TestReaderRole`: a follower on `jetstream_reader` can LISTEN, run
    CheckVersions, load a snapshot, and read hot-batch frames. It fails on
    the fence, lease acquire, and raw UPDATE/INSERT/DELETE/TRUNCATE (SQLSTATE
    42501).
  - The other required suites and faults already existed and now run under
    the recipe: catalogtest, the locker contract, proxy kill and
    COMMIT-result loss (pgstore), metastore/pg with a reader-role test, S3
    wrong bytes plus 5xx/timeout/truncation on both stores, and racing
    lockers and a stale holder in lockertest on PG.
  - CI job `test-storage`: harden-runner (standard allowlist plus the 4
    Docker Hub hosts), `docker compose version`, `just up`, `just
    test-storage`, compose logs on failure, and `just down` under `if:
    always()`. The flake ships only the Docker CLI; compose resolves from
    `/usr/libexec/docker/cli-plugins`, as the `docker-build` job's buildx
    does. `testing/ci` pins the order, the hosts, the recipe's env vars, and
    that plain `just test` sets no `JETSTREAM_TEST_*`.
  - Open: the CI job has not run yet. If Docker Hub serves layers from
    another CDN host, `just up` will be blocked and the host needs adding.
    The recipe's not-running check was not exercised, because `just up` is
    shared across worktrees.
  - `just test-storage` (and with `-race`) passes locally: 300 tests on
    PG + SeaweedFS, 99 on PG + MinIO, none skipped. Design §20 layer 4
    records the recipe's contract.
- **S2.15 (2026-09-25): storage configuration, CLI flags, PG URL redaction.**
  - `jetstreamd.Options.Storage` (`StorageConfig`, `internal/jetstreamd/
    storage.go`) holds every §18 variable; the zero value is local mode.
    `DefaultStorageConfig` gathers the defaults from the packages' `Default*`
    constants. `Validate` names variables, never values, and checks: known
    mode, `DataDir` empty and `CompactionInterval` 0 in disaggregated mode,
    PG URL and S3 region/bucket present, every number and duration > 0,
    `RenewInterval < Lease`, and `CatalogPollInterval < MaxViewAge`.
  - `Build` validates and logs `storage_mode`. In disaggregated mode it logs
    the redacted config and returns `errDisaggregatedUnavailable` until S2.16
    wires the runtime. Local mode with storage settings present logs a
    redacted warning.
  - No `JETSTREAM_S3_*` credential flags: S3 keys come only from the AWS SDK
    chain (design §18, §24).
  - The data-dir refusal checks whether `--data-dir`/`JETSTREAM_DATA_DIR` is
    set, not its value. `.env` sets it, so the README runs the binary with
    `env -u JETSTREAM_DATA_DIR` rather than `just run`.
  - Redaction: `pgstore.RedactURL` handles the URL and libpq keyword forms,
    keeps an allowlist of parameters, and replaces anything it cannot parse
    whole. `String`, `GoString`, and `LogValue` redact on `StorageConfig`,
    `PGConfig`, and `pgstore.Config`. `--pg-url` hides its default in help.
    `FuzzRedactURL` found four leaks (fragments, `?` in the host,
    `sslpassword`, `://` inside keyword values), all fixed and kept as
    regression inputs; it is in the scheduled fuzz matrix.
  - Leak tests (ground rule 7): the password never appears in JSON or text
    startup logs in disaggregated mode, in `/status`, `/metrics`, or logs in
    local mode, or in `serve --help`. Pointing `PGConfig.LogValue` at the raw
    URL fails them. CLI tests that read env clear `JETSTREAM_*` first,
    because `just` loads `.env`.
  - Byte-size flags use `IntFlag`, converted to int64.
  - Handed to S2.16: the GOMEMLIMIT and budget-sum check (§17); setting both
    the Uploader's and the Blob's upload concurrency from
    `JETSTREAM_S3_UPLOAD_CONCURRENCY`; the startup canary PUT/GET/DELETE;
    passing `GC.Delay` and `GC.OrphanAge` into `protocol.UploaderConfig`.
  - S2.15 and S2.20 ran in a forked worktree off S2.5 and were
    cherry-picked after S2.8 without conflicts. Design §18 and §24 record
    the data-dir, credential, and redaction rules.
- **S2.8 (2026-09-25): writer hot mode.**
  - Hot mode lives inside `ingest.Writer` behind `Config.Hot` (`HotConfig`):
    every public method dispatches to `hotWriter` (`internal/ingest/hot.go`)
    the way the async pipeline already does, so the live consumer, backfill,
    and orchestrator keep their `*Writer`. Hot mode refuses the seq lease,
    `AsyncFlushWorkers`, `Catalog`, and any namespace or seq key but `main`.
    It does not use `Store` or `SegmentsDir`: seq/next is read through a
    catalog read transaction at open (§10.2).
  - The admission class travels in the context (`ingest.WithClass`,
    untagged = live), so producers sharing one Writer need no new Append
    variants. S2.9 tags the producers.
  - "Injectable clock" is the synctest bubble: the writer uses `time.Now` and
    timers, and the tests run inside `synctest.Test`, like the oracle. The
    package gained a `TestMain` that calls `segment.WarmEncoder` so the shared
    zstd encoder is not bound to the first bubble.
  - Freeze (under the lock) detaches the batch's events and samples
    `DurableBatchPrepareValue`. Encoding, and for a pointer batch the upload,
    run in a per-batch goroutine. The committer waits for each batch in seq
    order. Pointer batches use `Uploader.Upload`'s pending refs, so the hot
    batch transaction also makes the object available (§7.3 step 6).
  - As in local sync mode, `Append`/`AppendBatch` return once seqs are
    assigned, without waiting for commit. The acks are `Flush` and
    `DrainDurability` (barriers behind every earlier freeze), the read log's
    durable watermark, and the hook's `afterCommit`. Backpressure comes from
    S2.9's caps; until then the queue of frozen batches is unbounded.
  - S2.8 makes bulk batches pointers whenever an uploader is set, and live
    batches always inline. S2.9 adds the token bucket, overflow, permits, and
    caps.
  - A hook error ends the writer and the session. In local mode the error
    only returns to the caller, but in hot mode the hook runs in the
    committer and there is no caller to return it to (§9.1). `OnFailure` is
    called once; it must not call back into the Writer.
  - `DrainDurability` and `Close` commit the hook's output with
    `Session.CommitMeta`, and skip the transaction when the hook stages
    nothing.
  - `Close` commits every appended event but does not close the open block:
    the next session rebuilds it (§10.9, S2.11). `ForceRotate` closes the
    open block, waits for its commit, then calls `BlockSink.Rotate`, which
    seals once the maintainer has folded (S2.10). `SealActiveAndClose` is
    `ForceRotate` plus `Close`.
  - Closed blocks go to `BlockSink.BlockClosed` from the committer, in order,
    after their last batch commits. Each carries its events and a
    `HotBatchInfo` per batch (class, resolved object ID). After a failure,
    blocks are dropped: the next session rebuilds from what committed.
  - The open block and the read log share one copy of each event
    (`ReadableLog.appendEntry`).
  - Hot mode's `ActiveTimeFloorSeq` returns `NextSeq`: time lookups move to
    the catalog (S2.13). `ActiveSegment` reports false and `ActiveIndex` 0.
  - `jetstream_hot_batches_total{class,storage}` and
    `jetstream_hot_batch_events` landed here, not in S2.9, since the
    committer is where they are counted.
  - Tests (`hot_test.go`, storagefake + memblob + the real `protocol`
    uploader and reader):
    - a swarm over random block, batch, byte, and age limits with 1–3
      concurrent producers and random class changes, checking:
      - committed rows tile `[1, seq/next)`;
      - frames decode to the appended events;
      - one class per batch, pointer iff bulk;
      - the hook runs once per batch with the freeze-time prepare value;
      - neither the read log nor `afterCommit` runs ahead of the commit;
      - `Flush` acks cover the producer's appends;
      - closed blocks tile whole batches;
      - no batch crosses a block;
    - batch and block age cuts;
    - class change and `ForceRotate`;
    - `FaultCommitFails`/`FaultCommitLost` end the writer and session, and
      the next session resumes at the committed seq/next and reassigns the
      dropped seqs;
    - a hook failure;
    - config validation.
  - Hand-made mutants (7 of them: block close without a freeze, no durable
    advance, no class cut, dropped barrier, wrong prepare value, no pointers,
    `Close` dropping the tail) were all caught.

- **S2.5 (2026-09-25): objstore S3 blob, upload/read protocol, object cache.**
  - `objstore.Store` is read-only (`Get`, `GetRange`). Uploads go through
    `objstore/protocol.Uploader` with a `*catalog.Session`, because the
    `uploading` row is a fenced write. `Upload(ctx, s, objs)` returns one ref
    per input (identical inputs share one object): a dedup hit comes back
    non-pending, a fresh upload comes back `Pending` for the referencing
    transaction to make available (§7.3 step 6 folded in). `Put` is `Upload`
    plus `MarkAvailable` and returns the winner's ID.
  - S3 maps 404 and 403 `AccessDenied` to `ErrNotFound` on GET only. Write
    403s, `NoSuchBucket`, and credential 403s stay plain errors. An early
    draft applied the mapping to every op, and because `DeleteKey` treats
    not-found as success, a denied DELETE looked successful and GC would have
    leaked the object; `TestWriteDenialIsNotMissing` covers it.
  - The Reader decides corruption per object row: available after refresh plus
    missing or bad bytes is corruption (source `read`); a row that is no
    longer available returns `objstore.ErrGone` and the caller re-resolves. It
    re-checks the row once more before declaring corruption, so a concurrent
    GC claim is not misreported. Source `object` means the row's key, SHA, or
    length changed between lookups.
  - Read-back mismatch retries with fresh keys for up to 3 rounds, then ends
    the session. It counts as `jetstream_s3_verify_failures_total{path}`, not
    as corruption, since nothing durable is wrong.
  - Any upload failure ends the session through the new `catalog.Session.End`
    (session-ending failures outside a transaction), including the skip-PUT
    rule. Skip-PUT is checked per object just before its PUT, on the monotonic
    clock taken after `BeginUploads` commits.
  - `objstore/objcache`: an LRU keyed by SHA-256 that caches whole objects only
    after a verified `Get`, serves range reads from cached whole objects, and
    skips objects larger than the budget (default 2GiB). Gauges:
    `jetstream_memory_{budget,used}_bytes{budget="object_cache"}`
    (`obs.NewMemoryMetrics`).
  - S3 request metrics count every attempt. Retries use jittered backoff until
    `RetryTimeout` and never retry cancellation, `ErrNotFound`, or
    `ErrInvalidRange`. `s3test` provides the env-driven real-store config
    (`s3test.Env`, for S2.20), an in-memory S3 RoundTripper, and a fault
    RoundTripper (5xx, timeouts, lost responses, truncated bodies, wrong
    bytes).
  - Open items handed forward:
    - A read 403 from a broken bucket policy looks like a missing object and
      so like corruption on an available row. S2.16/S3.4 add a startup canary
      PUT/GET/DELETE.
    - Non-read 403s are retried until `RetryTimeout` in case credentials are
      refreshing, so a plainly wrong key fails only after up to 30s.
    - The Uploader's `Concurrency` and the Blob's `UploadConcurrency` overlap:
      S2.16 sets both from `JETSTREAM_S3_UPLOAD_CONCURRENCY`.
    - The SDK's default logger stays on (only the checksum-skip warning is
      suppressed), because smithy-go's logging package is not on the
      dependency whitelist.
  - The work ran in a forked worktree off S2.2 and was cherry-picked after
    S2.4. `go mod tidy` made no further changes. Design §7.3, §7.5, and §23
    were updated.

- **S2.4 (2026-09-25): metastore/pg.**
  - `internal/metastore/pg` (package `pg`; import it as `metapg`) mirrors
    storagefake's `MetaStore`: `Config{DB, Commit, PageSize}`. Commits go
    through `Commit`, the leader session's `CommitMeta`, so every metadata
    write is fenced and coalesced by `pgstore.MetaBatch` (S2.2) inside
    `Tx.ApplyMeta`. A nil `Commit` is the reader pod's store: every commit,
    even an empty one, returns `metastore.ErrReadOnly`, as storagefake does.
  - The SQL stays in pgstore: `Store.MetaGet` and `Store.MetaScan(lower,
    upper, limit)` are autocommit reads with spans `pg.meta_get` and
    `pg.meta_scan`, and metric kind `meta_read`, separate from catalog
    snapshots' `read`. So metastore/pg imports no driver.
  - The iterator pages by key: 10,000 rows (`DefaultPageSize`), with the
    next lower bound set to the last key plus a zero byte, which is the
    smallest key after it under bytea's memcmp order. It holds no
    transaction between pages, which is all the interface promises. The
    first page is fetched in `NewIter`, so an error surfaces there.
  - Tests under `just test-storage`: storetest with a page size of 2, so
    every multi-key scan crosses pages; page boundaries on zero- and
    0xff-suffixed keys; a 25,000-op mixed batch scanned with the default
    page; the read-only store over a real `jetstream_reader` connection
    (`pgtest.ReaderURL`); and a fenced-out pod's writes failing with
    `ErrFenced` and then `ErrSessionEnded`.

- **S2.3 (2026-09-25): locker contract suite and leader metrics.**
  - The PG lease shipped in S2.2 as `pgstore.Lease` (import cycle, see the
    S2.2 entry). S2.3 adds `internal/leader/lockertest`: a `Backend` gives a
    `NewLocker` factory (one holder ID per call, as separate processes),
    an `Advance` clock hook, a lease duration, and an optional `Fence`
    (`lockertest.CatalogFence(db)` runs `Begin` + `FenceBump` + commit).
  - Nine tests: lifecycle; acquire while held (and not reentrant); renew
    extends; renew after expiry; renew after takeover; release when not
    holder (never acquired, another's lease, stale holder after takeover,
    not idempotent, frees without waiting for expiry); epoch strictly
    increases across holders, by release and by expiry; 8 lockers racing →
    exactly one wins; and the old epoch's fence fails once a new holder
    acquires, but not on expiry alone (§6.3: the fence, not lease timing,
    keeps a paused leader out).
  - `leader.Local` runs the lifecycle subset (`Exclusive: false` skips the
    rest). storagefake runs everything on a fake clock in `just`. PG runs
    everything with a 500ms lease and real sleeps (about 1.3s, parallel);
    margins are a fifth of the lease.
  - Metrics: `jetstream_leader_sessions_total{result}` now counts *ended*
    sessions by result (fatal, lease_lost, shutdown, restart), replacing
    `session_ends_total{reason}`, so the name matches §23. Starts moved to
    `jetstream_leader_session_starts_total`. `is_leader`, `epoch`, and
    `fence_failures_total` were already there.

- **S2.2 (2026-09-25): pgstore.**
  - `migrations/0001_init.sql` is §8 byte for byte, behind a two-line comment
    header. `TestMigrationMatchesDesign` extracts the design's `sql` block and
    compares, so the schema cannot drift from the doc.
  - `Store.Initialize` runs the migrations and inserts the archive row in one
    transaction under an advisory lock, and refuses a database that already has
    `archive` (`ErrInitialized`). It creates no segment rows: `storage init`
    (S2.15) creates segment 0 through a catalog script fenced at epoch 0, which
    works before any lease exists. This matches `storagefake.New`.
  - `catalog.FormatVersion` (new) and `pgstore.SchemaVersion` are the compiled
    constants. `SchemaVersion` must equal the embedded migration count;
    `storagefake` reuses `catalog.FormatVersion`, and a pgstore test pins its
    `SchemaVersion` to pgstore's. `CheckVersions` returns
    `ErrNotInitialized`/`ErrVersionMismatch`.
  - The plan's `pgstore.ErrSessionEnded` is `catalog.ErrSessionEnded`: every
    error from a leader `Tx` method, `Begin`, or `Commit` wraps it (and so
    `leader.ErrRestartSession`), labelled `pgstore <kind>/<statement>`. No
    retries anywhere. `Rollback` returns nil when the connection is already
    dead, since the server has rolled back.
  - Session settings (`statement_timeout` 10s, `lock_timeout` 5s,
    `idle_in_transaction_session_timeout` 30s, `application_name`) ride in the
    startup packet via `RuntimeParams`, so they cost no round trip.
  - `ApplyMeta` sends the §14.2 statements as one `pgx.Batch` (one round
    trip). A Set run dedupes keys, last write wins, because one `INSERT ... ON
    CONFLICT` cannot touch a row twice. Nil keys and values become empty
    `bytea`, never NULL. A `DeleteRange` with a nil end is unbounded.
  - Other primitives: `InsertObjects` is one unnest insert that maps
    `RETURNING` rows back by key, since RETURNING order is not guaranteed.
    `RefCheck` is the §7.4 statement batched with `= ANY`. `DeleteNamespace` is
    one statement with data-modifying CTEs, and `generation_blocks` go by
    cascade. Reads order namespaces `COLLATE "C"` so the order is bytewise,
    matching storagefake, whatever the database collation.
  - The writer lease (§6.2 SQL, `holder_id` random per process) lives in
    `pgstore` as `Store.NewLease`, not in `internal/leader`: pgstore imports
    catalog, which imports leader, so a PG lock in leader would cycle. Design
    §19.2 was updated. S2.3 adds its contract suite and metrics.
  - `Listen` uses a dedicated connection outside the pool. The first LISTEN
    must succeed. After that it reconnects with 100ms→5s backoff, counts
    `jetstream_pg_listen_reconnects_total`, and drops notifications a slow
    reader has not taken, as storagefake does.
  - Metrics: `jetstream_pg_txn_duration_seconds{kind}` (reader transactions
    use `kind="read"`) and `jetstream_pg_txn_errors_total{kind}`. The spans
    are `pg.txn`, carrying kind, epoch, and revision (or `fenced`), and
    `pg.read`, carrying revision.
  - `pgtest`:
    - `URL` skips, or fails under `JETSTREAM_TEST_STORAGE_REQUIRED=1`.
    - `NewDatabase` creates `jst_<random>`, grants `jetstream_reader` CONNECT,
      USAGE, and default SELECT when that role exists, and drops the database
      `WITH (FORCE)` in cleanup.
    - `Open` returns an initialized, version-checked store.
    - `Proxy` parses just enough of the wire protocol to find message
      boundaries. `KillAll` kills connections mid-transaction.
      `DropCommitResponse` forwards the next simple-protocol `COMMIT` (pgx's
      `Tx.Commit`), swallows the reply through ReadyForQuery, and closes the
      connection.
  - Results against `just up` (PostgreSQL 18):
    - `catalogtest.Run` passes all 19 contract tests on PostgreSQL unchanged,
      the first real evidence that storagefake's semantics match.
    - Also covered: result-unknown commits (the data is durable and the error
      ends the session), a killed transaction releasing the row lock, listener
      reconnect, session settings, version checks, atomic init, and URL
      redaction in parse errors.
    - The package runs in about 1.3s under race. Without the env var it skips
      and runs only the pure tests.
  - `github.com/jackc/puddle/v2` is new in go.mod as pgxpool's own
    dependency, not a new direct dependency.

- **S2.7 (2026-09-25): storagefake.**
  - Each committed state is a set of copy-on-write tables (`layer`), frozen
    on commit. A reader snapshot is one pointer. A write transaction builds a
    child of the latest state once it takes the archive row lock, which is a
    ctx-aware channel. Layers flatten past depth 16, so lookups stay cheap
    over long runs.
  - Deviations, recorded in design §20 layer 3: every write statement takes
    the row lock, invariants run after every commit (lease statements too),
    and violations are recorded without failing the commit. A listener more
    than 64 notifications behind drops the excess. Generation ID 0 stands for
    `NULL`.
  - Faults (`InjectFaults`, `Fired`, `Unfired`): `commit_fails`,
    `commit_lost` (applied, then error), `conn_lost` at statement N or at
    `COMMIT` (releases locks at once), `notify_lost`, and `slow_read`. Every
    fault counts every matching event, so ordinals are independent.
  - `Lease` implements `leader.Locker` with the §6.2 semantics.
    `MetaStore(commit)` is a `metastore.Store` over `metadata_kv`, and
    passes `storetest` with commits routed through `Session.CommitMeta`. A
    nil commit makes a read-only store that returns the new
    `metastore.ErrReadOnly`. `catalog.Listener` is the NOTIFY interface.
  - `Seeded` is the D4 scheduler. Storage calls yield at named points and are
    labelled with an actor (`WithActor`). `Run` owns the bubble's
    `synctest.Wait`. Once every goroutine is durably blocked, it admits one
    parked call, chosen by the seed from the list sorted by (actor+point,
    arrival). `TestSeededDeterminism` checks that one seed gives the same
    trace and the same final state, and that different seeds differ. S2.18
    must not call `synctest.Wait` elsewhere in a scheduled bubble.
  - `internal/catalog/catalogtest` is the layer 4 primitive contract suite:
    fence, row lock serialization, acquire fencing the old epoch, ordered
    `ApplyMeta`, object states and both unique indexes, sequences that burn
    on rollback, every CHECK and FK, `one_active`, cascade on
    `DeleteNamespace`, reader snapshots, aborted transactions, and NOTIFY on
    commit only. storagefake runs it now; pgstore runs it in S2.20.

- **S2.6 (2026-09-25): catalog transaction scripts.**
  - `catalog.Session` holds the scripts (`CommitHotBatch`, `CommitBlock`,
    `Fold`, `Seal`, `InitNamespace`, `DeleteNamespace`, `CommitMeta`,
    `BeginUploads`, `MarkAvailable`; `PublishGeneration` returns
    `ErrNotImplemented`). Each is one `run`: fence, body, NOTIFY, commit. Any
    error rolls back and ends the Session for good. Every later call
    returns `ErrSessionEnded`, which wraps `leader.ErrRestartSession`.
    `ErrFenced` wraps `ErrSessionEnded`, and fence failures increment
    `leader.Metrics.FenceFailures`.
  - `CorruptionError{source}` implements `SessionFatal()`, and
    `leader.DefaultFatal` honors it. That is how the election loop exits
    the process on corruption. It is counted by
    `jetstream_storage_corruption_total{source}`, with sources `seq`, `ref`,
    `fold`, `seal`, `invariant`, `object`, `read`, `meta`, `hot_batch`, and
    `generation`.
  - Ref checks cover every referenced object, including the seal's block
    list. A pending object is made available inside the referencing
    transaction (the §7.3 step 6 fold-in option). If another upload of the
    same bytes won first, the script references the winner. `insertActiveBlock`
    also checks continuity with the segment's last block, which coverage
    alone cannot see.
  - Added `InitNamespace`, since a namespace's first active segment needs a
    transaction too.
  - `CheckInvariants` implements all seven §9.3 invariants: 7 via a
    caller-supplied `RelayCursor` function, filled in by S2.12, and 6 in the
    weaker form recorded in design §9.3. storagefake runs it after every
    commit. The leader session-start call (cheap subset) lands with the
    rebuild in S2.11, and the PG run lands in S2.20.
  - Tests: `scripts_test.go` covers each rejection path on storagefake: fence
    lost, seq gap and overlap, garbage seq key, missing and uploading
    references (hot batch, fold, seal footer), fold coverage gaps, block
    discontinuity, the seal list (missing, reordered, wrong object, header
    mismatch, wrong segment), and the upload race referencing the winner.
    `invariants_test.go` breaks each invariant in a valid snapshot.

- **S2.1 (2026-09-25): dependencies and import boundary.**
  - pgx v5.11.0 and aws-sdk-go-v2 (`config` v1.33.6, `credentials` v1.20.6,
    `service/s3` v1.113.4) are required, and AGENTS.md whitelists both.
    `TestStorageDriverImportBoundary` (in `internal/pgstore`) walks every
    package with `go/parser` and fails on a pgx or aws import outside the
    ground rule 2 list.
  - `JETSTREAM_TEST_` is a known foreign prefix, so the test harness
    variables never trip the unknown-key check.
  - `just vuln` exits clean: no reachable vulnerability. It reports
    pre-existing unreachable advisories in `golang.org/x/crypto` v0.55.0
    and `google.golang.org/grpc` v1.83.1, which the new modules did not
    bring in.

- **Stage 1 exit (2026-09-25).**
  - Checks at `537ab1c` plus the S1.15 docs: `just`, `just test-long
    ./internal/oracle`, `just oracle-sweep`, and `just fuzz 30s ./segment`
    (7 targets) all pass. `just mutation-gate` on a clean tree: 52 mutants
    match the baseline.
  - `just bench ./segment` against `main`, 3 runs each: Append, SteadyFlush,
    FlushToTmpfs, Seal, ReaderOpen, DecodeBlockSealed, and EncodeBlock are
    all within run-to-run noise. Allocations are unchanged.
  - No core package imports `internal/store` or Pebble for metadata. Only
    `metastore/pebblestore` imports `internal/store`, and
    `TestOnlyPebblestoreImportsStore` enforces it. Core packages import only
    `pebble/vfs`, the filesystem abstraction. `simulator/world` uses Pebble
    for its own model state, not Jetstream metadata.
  - The S1.14 measurement is recorded in design §22.1.
  - S1.15: `specs/architecture.md` gains storage-seam and writer-session
    sections. `specs/invariants.md` states the backend-neutral durability
    rule, with the Pebble ordering as the local committer rule, and adds the
    session invariant. `specs/gotchas.md` records the reader/session lessons.
    `docs/README.md` gets only internal notes.

- **S1.12 (2026-09-25): leader loop and per-session runtime split.**
  - `internal/leader.Run(ctx, Config, SessionFunc)`. `leader.Local` embeds
    `streaming.NoopLock`, has epoch 1, and skips the renew ticker. The loop
    sleeps `AcquireInterval` between sessions.
  - Error classification: a session error is fatal unless it wraps
    `leader.ErrRestartSession` (`Config.Fatal` overrides). An unclassified
    error keeps the crash-loud rule, so local mode behaves as before. A
    cancellation error is benign only when the loop cancelled the session.
    A fatal error wins over lease loss and shutdown.
  - No injectable clock: the loop tests run under `testing/synctest`. Each
    `Renew` call is bounded by the deadline `lastOK + Lease`, where `lastOK`
    is the start of the last successful call, so a hung renew cannot keep a
    session alive past its lease.
  - Per-process: logger, registries, metastore handle, manifest, catalog,
    cold reader, tail, identity, status, web, server, xrpcapi, and the
    orchestrator and leader metrics. Per-session: orchestrator (writer,
    compactor, retry runners), syncstate store, tombstone set, verifier and
    its async-error drain, and the compaction schedule. The local metastore
    write handle is the process handle: a local data directory has one
    writer.
  - `HotLog`/`Readiness` plumbing: a `writerSlot` holds the current steady
    writer. It feeds the tail, cold reader, status, cursor resolution, and
    repo actions. The slot keeps the last writer after its session ends
    instead of clearing it. Clearing would let a subscriber that passed the
    handler's writer check anchor at `Tip()` 0 and replay the archive. The
    seq lease keeps the next writer's seqs above the old tip. Readiness
    needed no change: it reads the persisted phase.
  - Tail fix: a reader parked at the tip now also wakes on the tail's own
    notify channel, so `SetReadLogSource` with a new log wakes readers
    parked on the old one. `TestTail_SetReadLogSourceWakesReaderParkedOnOldLog`
    pins it.
  - The tombstone gauges read through `Metrics.SetTombstones`. xrpcapi reads
    the compaction deadline through an indirection that is unknown (no
    caching) between sessions. The catalog does not detach a closed writer;
    the next writer's attach replaces it.
  - `Build` builds the first session up front, so invalid orchestrator or
    verifier config still fails in `Build`. `Close` closes it if `Run` never
    took it.
  - "Follower" is implicit in local mode: the per-process readers follow the
    slot.
  - New test-only `Options`: `OnSessionStart`, `SteadyMaxEventsPerBlock` (the
    steady writer only fsyncs on a full block), and `SessionRestartDelay`.
  - `TestOracle_SessionRestartInProcess` ends session 1 with a restartable
    error in five places: four crashpoints across bootstrap-live seal, merge,
    and steady entry, plus a steady segment fsync fault after the steady
    writer has published. It asserts exactly two sessions and that the
    oracle matches.

- **S1.11 (2026-09-25): orchestrator seams.**
  - The orchestrator reads segments through a `SegmentCatalog`. When
    `Config.Catalog` is nil, it builds a private lazy `catalog/local`
    catalog. The catalog is refreshed at four points: merge start, each
    compaction pass (after the forced rotate), the merge-to-steady manifest
    reconcile, and the live-tombstone rebuild.
  - The merge restart guard is now "`bootstrap_live` has no segments". The
    merge runner checks that sources are contiguous and merges only sealed
    segments.
  - `RewriteSegment` reloads the new generation. `DeleteNamespace` fsyncs the
    parent directory even when the tree is already gone. Stale-tmp cleanup
    errors are prefixed `catalog/local:`.
  - Refresh race: the merge kept HEAD's sample-then-merge `Refresh` (S1.10)
    plus the attached-writer skip and a stale-tail guard. It did not take a
    rev-retry loop.
  - Mutants: m051 is retargeted to `DeleteNamespace` in `catalog/local`.
    m003, m006, m024, m034, m045, and m051 are refreshed.

- **S1.13 (2026-09-25): catalog-based oracle observers.**
  - `ObserveSegments`, `ObserveSealedSegments`, and `ObserveBootstrapSegments`
    read each namespace through a fresh `catalog/local` catalog (`Refresh`,
    `RefsFrom`, `Fetcher`). They cross-check against the path walk, which
    keeps the sealed-structure and footer checks and reports its errors
    first, so existing messages are unchanged. With the directory quiescent,
    the two must match segment by segment.
  - Observations taken while the server runs (the over-drop recorder, the
    compaction bisection) use a live cross-check. It allows only newer
    segments, a grown or newly sealed tail, and rows missing where a
    compaction may race the scan (a subsequence, never new rows).
  - The observer turns `local.Catalog.Snapshot`'s overlap panic into an
    error.
  - The cross-check shows that the local catalog skips an unsealed file
    below the newest one. The writer cannot produce one; `specs/gotchas.md`
    records it and a test pins the observer failing on it.
  - `durable_order_test.go` stays local-only (fsync ordering), and
    `specs/oracle.md` says why. The single-file probe in
    `restart_crash_chain_test.go` also stays path-based.
  - `just mutation-gate`: 52 mutants match the baseline; the new cross-check
    was never the killing check.

- **S1.10 (2026-09-25): read-path seams.**
  - Cold replay walks `CatalogView.RefsFrom(Main, seq)` and decodes through
    the catalog's `Fetcher`.
    - The old rotation-seam retry is gone. The writer publishes each seal
      before advancing `activeIdx`, so a view taken after the floor is read
      covers every seq below it.
    - The only retry is `ErrStaleRef`: an active segment sealed mid-walk, or
      a sealed one rewritten. Eight consecutive stale views without progress
      fail the read loudly (`maxStaleRetries`).
    - A bounded walk now fails loud with "unregistered sequence hole [a,b)
      before segment S block B" on any hole before a ref, whether in one
      segment or across segments. A view that ends below the floor fails
      with "cold replay reached the end of the catalog ... before
      readable-log floor". Registered gaps are still jumped.
    - m059 and m061 are refreshed as context diffs against these two sites.
      Both are KILLED at the seqlease tier with the driver.
  - Block cache: the key is `blockKey{id any, seg, epoch}`. Local mode uses
    `localBlockID{ns, seg, block, generation}`, and the generation is the
    header checksum. Only sealed refs (generation != 0) are cached; active
    blocks are decoded per walk, since a seal changes their generation.
    `InvalidateSegment` stays local-only.
  - `catalog/local` `Refresh` fix: it samples the known segments before
    listing the directory, then merges seals and reloads that landed during
    the scan. Before this, a segment sealed mid-scan was dropped from the
    catalog (`TestLocal_RefreshKeepsSegmentsSealedDuringScan`).
  - The runtime loads the catalog in the background, like the manifest. The
    cold reader, status, and repo verification all wait on it
    (`backgroundLoad.Wait`). Refresh is now called at startup, replacing the
    S1.9 note.
  - Timestamp cursors: the manifest picks the segment by witnessed range,
    and the catalog view supplies its block index and block bytes. If the
    view does not hold the segment (the catalog is still loading), or no
    catalog is configured, resolution falls back to the segment's MinSeq.
    That is lossless, because the subscriber loop drops rows witnessed
    before the cursor. `CursorEnv.FS` and `Subscription.FS` are replaced by
    `Catalog` and `Fetcher`. The resolve-failure test now corrupts a block
    frame: a removed file is a legitimate "segment gone" catalog state that
    falls back, not a read fault.
  - Manifest feed: `ApplySegment(idx, gen, header, footer, createdAt, size)`
    replaces `OnSegmentSealed` and `OnSegmentCompacted`. The metadata parser
    runs `segment.OpenReaderParts` over the bytes, so it never touches the
    block region. Behavior change: seals are now checksum-verified too (only
    rewrites were before). The startup scan still skips verification.
    `ApplySegmentFile` is the local-mode adapter. `Manifest.BlockIndex`
    stays, for the manifest's own readers.
  - `Writer.ActiveFlushedRange` and `Writer.SegmentsDir` are removed; the
    catalog's `ActiveSegment` view replaced them. Its test is now
    `TestActiveSegmentExcludesPendingAndAdvancesByBlock`.
  - xrpcapi (`SegmentOpener`):
    - `FileOpener` reads the manifest, so it agrees with
      `listSegments`/`planSnapshot`.
    - The ETag and the bytes come from one opened `SegmentFile`.
    - The three 500 messages are unified into "failed to open segment".
    - `TestFileOpener_ByteIdentity` checks the virtual file against the
      on-disk file for random segments.
    - `CompactionDeadlineSource` is the existing
      `xrpcapi.CompactionDeadline`; it needed no code.
  - Readiness: `lifecycle.Readiness`, `ReadinessFunc`, `SteadyState`,
    `AllReady`, and `ErrBootstrapInProgress`. These replace
    `IsSteadyState` in `Subscription.Ready` and xrpcapi `Config.Ready`.
    Response text is unchanged.
  - repoexport:
    - Reconstruction replays each namespace (Main, then BootstrapLive)
      through `RefsFrom` and `DecodeRef`, with no directory scans.
    - Pruning is a per-namespace `Selection`: segment index to candidate
      blocks, where an absent segment is decoded in full. The manifest
      adapter takes `SegmentChecksums()` before `SelectBlocksForDID`, so a
      segment absorbed between the two calls is decoded in full.
    - `FooterSelector` reads footer blooms through
      `local.Catalog.SealedMetadata` for segments the manifest does not hold.
      That makes bootstrap_live pruned for the first time.
    - On `ErrStaleRef`, a pass resumes from the first seq it had not
      replayed, so no block is replayed twice.
    - Known and unchanged: a merge's `DropNamespace(BootstrapLive)` can land
      between the two namespace passes.
  - status:
    - Segment trees come from one catalog view: main's sealed stats from the
      manifest, main's active tail and all of bootstrap_live from the view.
    - File mtimes are not in a view, so they are left zero.
    - Error handling: the newest segment in a namespace fails silently,
      older failures become warnings, and a stale segment is skipped.
    - `InspectAll` stays only for the offline CLI. A seeded parity test pins
      the view-based aggregate equal to `InspectAll`, apart from mtimes.
    - status has no diskspace dependency, and the data-dir free-bytes gauge
      is skipped when there is no data dir.
  - Tests: repoexport runs in about 1.7s, over the 1s target. The unchanged
    baseline is about 1.6s, because of the existing
    `TestVerify_HTTPFailureReturnsError`.

- **S1.9 (2026-09-25): catalog, HotLog, local catalog, block committer.**
  - `internal/catalog` imports only `segment`. `RefsFrom` returns
    `iter.Seq[BlockRef]`, not a slice, so a reader that stops early does not
    build every ref. `BlockRef` carries `Segment`/`Block` (stable across
    compaction) and a closed `Locator` sum type (`FileBlock`, `ObjectBlock`,
    `InlineBlock`) in place of the `ObjectID|Frame` pair. `FileBlock` is the
    plan's "(path, block index) handle", pinned to the header checksum as
    its generation. A `Fetcher` returns `ErrStaleRef` on a generation
    mismatch or a missing file. The design (§11.2, §19.1) is updated.
  - The writer-side `Catalog` methods (`CommitHotBatch`, `CommitBlock`,
    `Seal`, ...) are not declared yet. They land with their first
    implementation in S2.6. `LogEntry` moved from `ingest` to `catalog`,
    since `HotLog` hands entries out. `PendingForDID` is documented as
    local-only.
  - The block committer is unexported (`blockCommitter`, with
    `localCommitter` its only implementation). S2.8 exports it when the
    object-store committer arrives. The m049/m057/m060 target text in
    `writer.go` is unchanged, and every mutant still applies.
  - `ingest.Config.OnAfterSeal(idx, path)` is replaced by
    `Config.Catalog` (a `SegmentCatalog`: `AttachActive` plus
    `Sealed(SegmentView)`) and `Config.Namespace`. `SegmentsDir` stays,
    because it is the local committer's directory. `SealResult` now returns
    the header and footer bytes Seal wrote, so the sealed view is parsed
    with `OpenReaderParts` and not re-read from disk. `ingest.SealedPathFunc`
    adapts the old `(idx, path)` callbacks.
  - Behavior change: bootstrap_live seals now reach the catalog too. The
    manifest hook is still registered only for `catalog.Main`, so the
    manifest sees exactly the seals it saw before.
  - `segment.ActiveBlocksFS` is new. It lists an unsealed file's flushed
    blocks, ignoring a torn tail as `segment.New` does, so a catalog with no
    attached writer can still serve the active tail.
    `Writer.ActiveSegment` keeps reporting the durable blocks after
    `Close`.
  - Snapshot coherence during rotation: the writer publishes the seal
    before advancing `activeIdx`. `Snapshot` samples active sources before
    the sealed list, and drops an active view whose index is not above the
    last sealed one. An overlapping view panics, because it can only mean
    internal corruption.
  - The local fetcher opens the file on every fetch. S1.10's block cache
    sits in front of it.
  - The runtime builds the local catalog and hands it to the orchestrator,
    and compaction calls `Reload`. `Refresh` is not called at startup yet,
    because nothing reads through the catalog until S1.10.
  - Bench: segment and ingest writer benchmarks are at parity with 80f1d00.
  - Follow-up (60555ed): each namespace's sealed segments live in an
    immutable, pre-validated `SegmentList`, updated copy-on-write, so
    `Snapshot` validates only the active tail rather than every block in the
    archive. `BlockRef` gained `Namespace` and `Generation`. A stale sealed
    fetch calls `Reload` on the segment, so a reader's retry converges
    without waiting for the compactor to report the rewrite.

- **S1.7 (2026-09-25): segment refactors.** New `segment` API:
  `BlockBuilder` (`NewBlockBuilder`, `Append`, `Len`, `Cap`, `Snapshot`,
  `PendingBounds`, `Encode`), `FrameSource`, `SliceFrameSource`,
  `BuildSealed`, `ReaderOptions`, `BlockFetcher`, `OpenReaderAt`, and
  `OpenReaderParts`. `Open` now uses the same parser as the byte readers.
  - Empty-block break: in `BuildSealed`, the first zero-event frame stops
    indexing, but later frames are still read so the footer offset counts
    every frame. That matches the file seal, whose footer sits at end of
    file. `TestBuildSealedEmptyBlockEndsIndex` pins it. The file seal now
    also fails with `ErrCorruptSegment` if `BuildSealed`'s footer offset is
    not the active file size.
  - `OpenReaderParts` takes all metadata, blooms included, from the footer.
    `fetch` is only called by `DecodeBlock`, and a fetched frame whose
    length disagrees with the index is `ErrCorruptSegment`. Byte readers do
    not own their source, so `Close` leaves `src` open.
  - `computeRewrite` takes a `*Reader` (from any of the three constructors),
    not raw frames, and does not use `BuildSealed`. A rewrite keeps emptied
    blocks with their original seq and witnessed bounds, but `BuildSealed`
    would stop indexing at the first empty block.
  - Bench: at parity with f463e6f on every segment benchmark. Seal makes one
    more allocation per call (116→117), because the file frame source
    escapes through the interface.
  - Mutants: m011, m047, and m044 are refreshed; all nine touched mutants
    are KILLED at their baseline tiers. The old zero-context m044 still
    passed `git apply --check`, but it landed on an identical line in
    `commitPreparedFlushLocked` and survived. It is now a context diff, and
    `specs/gotchas.md` has the lesson. `FuzzBuildSealed` is in the
    `ci-scheduled.yml` fuzz matrix.
- **S1.6 (2026-09-25): `internal/store` shrink.**
  - The audit is recorded in design §14.2. Six metadata call sites all
    tolerate a non-snapshot scan. The rest of the "12" were tests or the
    simulator's own Pebble.
  - `internal/store` keeps `Open`/`Close`, `WithFS`, the instrumented
    `Get`/`Set`/`Delete`/`Commit`, metrics, and `SyncWrites`. The fault
    seam, encoding helpers, and `ErrNotFound` alias are gone, and so is
    `pebblestore.New`.
  - `TestOnlyPebblestoreImportsStore` enforces that only `pebblestore`
    imports the package. jetstreamd no longer imports it either: it opens
    `pebblestore` directly, so it is not on the allowlist the plan
    suggested.
  - jetstreamd wraps with `metastore.WithFaults` only when
    `Options.StoreFaultInjector` is set.
  - The identity cache stays on the unwrapped store. Its writes are best
    effort and their errors are ignored, so a fault injected there could
    never be observed. That leaves them outside the store-fault tier, and
    the durable-order recorder no longer sees them.
- **S1.2, S1.4, S1.5 (2026-09-25): one commit, not three.** The
  `DurableBatchHook` signature and the `ingest.Config.Store` type reach backfill,
  the orchestrator, status, and jetstreamd, so porting ingest alone would
  have needed throwaway shims in each of them. The three tasks landed together
  instead. `subscribe.Subscription.Store` also moved to `metastore.Store`.
  After this commit only `jetstreamd` and `pebblestore` import
  `internal/store`, so S1.6 is left with the shrink, the enforcing test, and
  the fault bridge.
  - Counts seed (finding 2): `backfill.(*Store).SeedCounts(ctx)` tallies once
    under `countsMu` when `backfill/counts` is missing, and is idempotent.
    Three entry points call it before any `repo/` write: `Run`,
    `RunFailedRepoRetry`/`RunPendingRepoRetryPass`, and merge discovery
    (`merge_discovery.go`), since each builds its own `Store` with its own
    `countsMu`. A missing counts row on a later write returns
    `errCountsNotSeeded`. It used to trigger a fresh full tally, which is
    racy now that the metastore iterator is not a snapshot.
  - Backfill helpers that have no ctx pass `context.Background()` to the
    store. Only the retry scan (`scanDue`) threads its caller's ctx. Adding
    ctx to the rest is left for the S2 call sites that actually cancel.
  - `ListPDSHosts` no longer treats `ErrNotFound` from the iterator as
    benign. The metastore iterator never returns it, so it is just an error.
  - `status` reads the `meta.pebble` size through `metastore.DiskStatsOf`,
    and stores without a local footprint report 0. `Options.DataDir` is
    kept for the other panels.
  - Tests that wrote through `backfill.NewStore` directly
    (`cmd/jetstream/serve_test.go`, `status/collect_test.go`) now call
    `SeedCounts` first, the same as production.
  - `newMergeFixture` takes `...metastore.FaultInjector` instead of
    `...store.Option`, and wraps the store with `metastore.WithFaults`.
- **S1.3 (2026-09-25): syncstate, lifecycle, identity.**
  - `syncstate.Delete` stays. atmos's `sync.StateStore` interface requires
    it, and jetstreamd hands the store to the verifier as that interface.
    `Flush` is removed.
  - `StageFlush(metastore.Batch)` returns nothing, because
    `metastore.Batch.Set` cannot fail.
  - Renames: `PebbleStateStore` → `StateStore` and `PebbleCache` →
    `identity.Cache`, since neither is Pebble-specific now.
    `identity.NewPebbleKV(*pebblestore.Store)` writes with
    `SetNoSync`/`DeleteNoSync`.
  - Every lifecycle function takes `ctx` first. `IsSteadyState(ctx, s)` is
    the local adapter until S1.10's `Readiness`.
  - The mutation driver reports BUILD-BROKEN when run from a git worktree,
    because VCS stamping fails there. Run it with `GOFLAGS=-buildvcs=false`
    in that case.
- **S1.14 (2026-09-25): measurement recorded in design §22.1.** The 7 was
  one instantaneous sample of a 0–4096 sawtooth: the events in the unflushed
  partial block. A 6h pop1 range query gave a mean of 2,132, a minimum of 5,
  and a maximum of 4,096. `TestPendingGaugeIsPartialBlockSawtooth` pins the
  relationship.
  - Finding 3 is slightly too strong. There is no age cut, but three rare
    paths flush a partial block besides the count cut: the retry pass's
    `DrainDurability`, compaction's `ForceRotate`, and `Close`.
  - No bug. Dashboards should read this gauge with `avg_over_time` or
    `max_over_time`.
- **S1.8 (2026-09-25): objstore interfaces.**
  - Range reads follow S3. A read past the end is truncated. A read that
    starts at or after the end, a negative offset, or `n <= 0` returns
    `ErrInvalidRange`. `objstore.RangeLen` holds the rule so every backend
    agrees.
  - `objstore.Verify(data, wantLen, wantSHA256)` checks length before hash
    and returns `ErrCorrupt`. The S2.5 `Store` will use it.
  - memblob fault kinds:
    - `error`;
    - `error_after`, which applies the put or delete and then fails, like
      a lost response;
    - `wrong_bytes`, which corrupts the stored object on put and only the
      returned copy on get;
    - `drop_put`, which acknowledges the PUT but does not store it.

    A kind that makes no sense for an op makes the op fail rather than
    being ignored.
  - `ErrNotFound` is documented as "maybe missing": an S3 blob may map
    both 404 and 403 to it, and the protocol layer decides from catalog
    state (finding 14).
  - The wrong-bytes contract case is in `blobtest`, gated on
    `Config.NewWrongBytes`, so the S3 blob can run it through its
    RoundTripper in S2.5.

- **S1.1 (2026-09-25): metastore interface.**
  - The Pebble impl lives at `internal/metastore/pebblestore` (package
    `pebblestore`), not `metastore/pebble`. That avoids shadowing the
    cockroachdb `pebble` import in every file that uses both. Plan and
    design references to `metastore/pebble` mean this package.
  - `Batch` has no `Close`. Committing twice returns
    `ErrBatchCommitted`. An abandoned uncommitted Pebble batch is just
    garbage. Pebble's staging methods only fail on a closed batch, so the
    first such error is latched and returned by `Commit`.
  - `NewIter` treats a nil bound as unbounded (`PrefixUpperBound` returns
    nil for an all-0xff prefix). The Pebble impl ignores `ctx`, so a
    cancelled context never turns a formerly successful shutdown commit
    into a failure. That keeps S1 free of behavior changes.
  - `metastore.OpBatch` is the recording `Batch` that memstore uses, and
    that the PG impl and `Tx.ApplyMeta` will consume through `Ops()`.
  - `WithFaults` exposes `Unwrap()`, so optional capabilities such as
    `DiskStats` (used in S1.5) and the identity cache's
    `SetNoSync`/`DeleteNoSync` (used in S1.3) stay reachable through the
    wrapper.
  - Fault matching is unchanged: a `DeleteRange` contributes its start
    key, as the old `batchKeys` walk did.
  - The encoding helpers (`GetUint64LE`, `Get/SetVersionedUint64LE`,
    `EncodeVersionedUint64LE`, `PrefixUpperBound`) landed in `metastore`
    here, ahead of S1.5, so the ports in S1.2 onward use them directly.
  - The fuzz target `FuzzBatchSemanticsAgree` is in the scheduled CI fuzz
    matrix.

- **S0 (2026-09-25): timestamp import removed.** Re-adding it under a design
  that fits disaggregated storage is tracked in
  [#354](https://github.com/bluesky-social/jetstream/issues/354). Notes on
  what S0 did beyond, or differently from, the task list:
  - The rewrite lock (`rewriteMu`/`withRewriteLock`) is gone rather than kept
    as a no-op: compaction is now the only segment rewriter and its passes
    never overlap. `segment.Rewrite` and `createSegmentFileExclusive` document
    that single-rewriter assumption. Any second rewriter must bring back
    mutual exclusion.
  - `ev.IndexedAt = candidate.IndexedAt` was dropped from the writer. Nothing
    sets `IndexedAt`, so events carry 0. `segment.Event.DisplayTimeUS`
    and the wire `time`/`time_us` resolution are kept, so a future import
    needs no wire or format change.
  - The `subscribeEvents.json` lexicon descriptions and the public client
    docs (`event.go`, `options.go`) still say `time` may be an
    operator-imported timestamp. That is the stable wire contract, and
    editing it would cause lexgen drift for a feature that is coming back.
    So the exit `git grep` is read as "no import *implementation*
    references", not "no mention of timestamps".
  - `docs/README.md` §8 is a short "removed, to be redesigned" stub rather
    than being deleted, the same way §6 handles replication. This keeps
    the section numbers stable.
  - Removed more dead code the import had left behind:
    `Runtime.WaitSteadyState` and its `steadyReady` plumbing,
    `waitRuntimeSteadyState` in `cmd/jetstream`, and the
    `IsCancellationOnly` lesson in `specs/gotchas.md`.
    `manifest.Generation()` is kept (its comments are updated) even though
    its only production consumer was the import bucketer. It is a
    candidate for deletion if Stage 1 does not reuse it.
  - The `baseline.json` m048 row was removed by hand, not regenerated with
    `just mutation-baseline`. The rest of the baseline is unchanged:
    `just mutation-gate` on a clean snapshot of the S0 tree passed, with
    52/52 mutants matching the baseline and m044, m045, and m047 still
    KILLED@segmentfault.
  - The oracle `TestOracle_DefaultLifecycle` anti-vacuity assertion
    ("final compaction watermark did not cover the #203 account-status
    lifecycle rows") flakes under heavy parallel load. It flakes at the
    same rate on the pre-S0 HEAD (6/60 vs 4/60 runs at 24-way
    concurrency), so it predates S0. It is not root-caused yet.
