# Issue #200 — segment-file I/O fault injection layer — implementation handoff

Plan of record for GitHub issue #200 (`oracle: segment-file I/O fault
injection layer`, label `testing`). Epic #35, oracle campaign Phase 3. This
doc is a post-compaction handoff: it carries the full analysis, all verified
code anchors, the approved plan, and the locked decisions so the build can
proceed without re-deriving context.

Branch: not yet created. One branch closes #200; sub-work filed as granular
issues that reference #200 (see "Tracking" below).

Repo: `~/go/src/github.com/bluesky-social/jetstream` (main branch clean at
handoff; recent HEAD `3d45ab7`).

---

## 1. What the issue asks for (DoD, verbatim intent)

- Crash-injector-style seam around segment file operations.
- Fault sweeps: short-write, fsync-error, ENOSPC, post-crash
  truncate/corrupt-at-offset. Assertions: fail-loud, clean restart recovery
  via the torn-tail walk, no silent corruption (`ObserveSegments` +
  final-state `Compare`).
- Injected ENOSPC → clean crash → clean recovery proven end-to-end.
- At least one mutant that swallows a segment write error, killed by this
  tier (m006 storefault precedent).

ENOSPC posture is DECIDED (specs/notes/2026-07-03-maintainability-improvements.md
and #201): crash-loud with a clear, prominent operator message; same class as
fsync failure; no read-only degraded mode.

---

## 2. Groundwork already landed by #201 (PR #239, merged 2026-07-05)

- `segment.IOFaultInjector` seam in `segment/iofault.go`:
  ```go
  type IOOp string
  const ( IOOpWrite IOOp = "write"; IOOpSync IOOp = "sync" )
  type IOFaultInjector interface { BeforeSegmentIO(path string, op IOOp) error }
  ```
- Consulted via `cfg.beforeIO(op)` helper (`segment/writer.go:254-259`) in the
  ACTIVE-WRITER paths only: `initializeNewSegment` (writer.go:237,244),
  `syncParentDir` (writer.go:210-225, takes a `faults IOFaultInjector` arg),
  `flushLocked` (writer.go:553,581), `commitPreparedFlushLocked`
  (writer.go:671, and a second write ~:? plus its fsync), `sealAfterFlush`
  (seal.go:133,141,151,173 — footer write, footer fsync, header write, header
  fsync).
- ENOSPC → operator message: `internal/ingest/enospc.go`:
  ```go
  func (c Config) wrapSegmentPersistenceError(op string, err error) error // returns err unless errors.Is(err, syscall.ENOSPC)
  func (w *Writer) wrapSegmentPersistenceError(op string, err error) error // delegates to w.cfg
  ```
  Message text (the fail-loud contract to assert against):
  `"fatal persistence error: disk full while %s (data_dir=%q): free space or move the data directory, then restart jetstream: %w"`.
  Called at every active-writer persistence site (writer.go:62,146,151,162,
  202,263,310,602,650,668,883 and async_flush.go:159,286,316). writer.go:883
  is the pebble durable-batch commit (`commitDurableBatchLocked`).
- Fields `SegmentIOFaultInjector segment.IOFaultInjector` ALREADY EXIST on:
  - `ingest.Config` (internal/ingest/config.go:142), forwarded to segment.New
    in ingest/writer.go:81,143,159,665.
  - `live.Config` (internal/ingest/live/config.go:130), forwarded in
    live/consumer.go:139.
- Prometheus-only disk-free gauge `jetstream_data_dir_free_bytes` in
  internal/obs/diskspace.go (NOT on /status by Jim's call). Not touched by #200.
- Reference test impl of the seam: `opOrdinalIOFault` in segment/writer_test.go:16-31
  (op + 1-based ordinal, atomic counter). Ingest-level analogue `segmentIOFault`
  in internal/ingest/writer_test.go:75-90, used at :107 (IOOpWrite ord 2) and
  :142 (IOOpSync ord 3), asserting the disk-full operator message.

---

## 3. The three gaps #200 must close (all VERIFIED by code inspection)

### Gap 1 — `segment.Patch` and `segment.Rewrite` have NO I/O seam.
- Both call `initializeNewSegment(f, Config{Path: tmp})` with NO injector:
  patch.go:290, rewrite.go:148.
- Their frame/footer/header writes and tmp fsyncs are direct `os.File` calls
  with no seam:
  - patch.go: frame-len write :296, frame write :299, footer WriteAt :304,
    header WriteAt :307, tmp `syncFile(f)` :313, `os.Rename` :322,
    `syncParentDir(path, nil)` :328.
  - rewrite.go: frame-len write :154, frame write :157, footer WriteAt :177,
    header WriteAt :180, tmp `syncFile(f)` :186, `os.Rename` :195,
    `syncParentDir(path, nil)` :201.
- They DO already have `CrashInjector` (crash-BEFORE-durable seams), threaded
  from orchestrator via `crashpoint.ForSegment(o.cfg.CrashInjector)`. The
  I/O-ERROR mode (short write / fsync err / ENOSPC / rename err) is the missing
  half. `PatchOptions`/`RewriteOptions` are in patch.go:16-19 / rewrite.go:18-21.
- NOTE: there is NO `os.Rename` in the active-writer path, so `IOOpRename` is a
  NEW op used only by Patch/Rewrite.

### Gap 2 — the injector never reaches a running server.
- `SegmentIOFaultInjector` is set ONLY by unit tests directly on ingest.Config.
- The orchestrator's `live.Open` calls (steady.go:94, bootstrap.go:67) and its
  `ingest.Open`/`ingest.Config{...}` literals (bootstrap.go ~:52/:246,
  merge.go ~:71/:167) NEVER set the field.
- There is NO `orchestrator.Config.SegmentIOFaultInjector` and NO
  `jetstreamd.Options.SegmentIOFaultInjector`. So no oracle/e2e path can inject
  a segment I/O fault today. This is the core plumbing work.
- orchestrator.Config is built in internal/jetstreamd/runtime.go:321-379;
  it forwards `CrashInjector: opts.CrashInjector` at :372. Store fault is baked
  into the store at runtime.go:180
  (`store.Open(opts.DataDir, storeMetrics, store.WithFaultInjector(opts.StoreFaultInjector))`).
- orchestrator.Config.CrashInjector is at internal/ingest/orchestrator/config.go:288.

### Gap 3 — ENOSPC NOT wrapped on the Patch/Rewrite orchestrator call sites.
- Compaction rewrite: compact_deletes.go:368 calls `segment.Rewrite(...)`, and
  on error returns raw `fmt.Errorf("orchestrator: compaction: rewrite %s: %w", ...)`
  at :379 — no ENOSPC operator message.
- Import patch: import_pass.go:419-424 calls `segment.Patch(...)`, returns raw
  wrapped error — no ENOSPC operator message.
- Both propagate FAIL-LOUD (verified): steady compactor errors flow up through
  the errgroup (`runSteadyCompactor` -> steady.go:139-143 returns non-cancel
  errors) to `Orchestrator.Run` -> `rt.Run`. Import errors likewise abort the
  job / RunImport. So the crash-loud posture already holds; only the actionable
  "disk full / free space / restart jetstream" MESSAGE is missing on these two
  paths. Decision: ADD IT (see decisions below).

---

## 4. Verified assertion & harness infrastructure to MIRROR

Store-fault tier is the end-to-end precedent for everything in Part D.

- Env-var plumbing: internal/oracle/restart_harness_test.go
  - const block :34-60 declares `envRestartStoreFault{Prefix,Ordinal,Observed}`
    (:57-59).
  - sentinel `errStoreFaultInjected` :67.
  - `newOracleStoreFaultFromEnv(t)` :431-449 builds `&store.KeyPrefixFault{
    Prefix, Op: store.WriteOpBatchCommit, Ordinal, Err: errStoreFaultInjected}`,
    nil when prefix unset.
  - Child `TestOracleRestartChild` :297 reads env, builds injector :312, passes
    `StoreFaultInjector: storeFault` to `jetstreamd.Build(jetstreamd.Options{...})`
    :343-375.
  - Child records outcome :393-401: writes observed-marker IFF
    `errors.Is(runErr, errStoreFaultInjected)`; marker absence == kill signal.
  - Parent injects env in `runRestartChild` :751, storefault vars appended
    :790-798.
  - `restartChildArgs` struct :723-743 has `storeFault{Prefix,Ordinal,ObservedPath}`.
- Scenario: internal/oracle/restart_storefault_test.go
  - `TestOracle_RestartStoreFaultOnMergeCursor_FailsLoudThenRecovers` :49.
  - First child arms fault on `merge/next_source_idx` ord 1; parent asserts
    `require.FileExistsf(observedPath, "runtime must FAIL LOUD ...")` :112 and
    anti-vacuity `require.NoFileExistsf(firstMergeDonePath, ...)` :119.
  - Second fault-free child recovers idempotently :126-136.
  - Convergence: `assertOracleMatchesAfterReplay(dataDir, w, cfg, ...)` :141 +
    `assertChainDurable(dataDir, coord, ...)` :142.
  - `mergeNextSourceIdxStoreKey` const :20 duplicates the orchestrator's
    private key; guard test `TestMergeNextSourceIdxKeyMatchesOracleStoreFault`
    (in orchestrator package) pins them together. MIRROR this guard for the
    segment fault op constants.
- Recovery reader (torn-tail): `oracle.ObserveSegments(dataDir)`
  (internal/oracle/segments.go:16) walks the active segment via
  `segment.WalkActive(path, fn)` (segment/scan.go:105) at segments.go:184.
  Siblings: `ObserveSealedSegments` :26, `ObserveBootstrapSegments` :33.
- Final-state assertions:
  - `Compare(want, got *Model) error` internal/oracle/compare.go:13 (rev NOT
    compared, see :35-50).
  - `CheckCompacted(events, watermark) error` internal/oracle/compacted.go:20.
  - `assertOracleMatches` harness_test.go:1028 (strict).
  - `assertOracleMatchesAfterReplay` harness_test.go:1042 (crash-recovery
    variant: at-least-once replay tolerant, then Compare convergence).
  - `assertChainDurable` restart_chain_assert_test.go:68 (coverage +
    CheckCompacted + recreated-records-visible bundle).
- Fail-loud message template test: internal/ingest/writer_test.go:92
  (`TestWriter_ENOSPCSyncFlushReturnsFatalOperatorMessage`) asserts
  "fatal persistence error", "disk full", data-dir path, "restart jetstream".
- Compaction is driven by `CompactionInterval` / the compaction trigger;
  harness hooks `OnBeforeCompactionPass` (harness_test.go:288) and
  `OnCompactionPass`. Restart child drives backfill->merge and exits at the
  after-merge barrier (BarrierAfterMerge). The chain coordinator
  (`newChainCoordinator`, `chainSpec` via `deriveChainSpec`) injects a
  create/update/delete chain on the live firehose during backfill.

Mutation campaign mechanism:
- Each mutant patch has a `tiers:` header (default `default,stress`), read in
  testing/mutation/run.sh:167-168; tier loop switch starts :221.
- storefault tier `case` :248-264 runs:
  `go test ./internal/oracle ./internal/ingest/orchestrator -run
  'TestOracle_RestartStoreFault|TestMerge_StoreFault|TestMerge_MultiSourceDrainsAllSources|TestCompaction_StoreFault'`
  with `storefault_timeout` (:60 = 10m, :66 = 30m under --race).
- m006 `testing/mutation/mutants/m006_merge_commit_error_swallowed.patch`:
  inverts `commitSourceComplete(...) err != nil` -> `err == nil` in
  merge_runner.go; `tiers: storefault`; failure-mode = swallowed persistence
  error. Sibling m028 (compaction watermark save error swallowed),
  `tiers: storefault`.
- RESULTS.md records storefault tier at :974-975; the EXPLICIT gap note at
  :994-1001 says manifest/segment IO faults "would require a separate
  manifest/segment IO-fault seam — out of scope for this metadata-store tier."
  #200 introduces exactly that seam; UPDATE this note when done.

---

## 5. Approved plan (BUILD THIS)

### Part A — segment package: seam Patch & Rewrite (`segment/`)
1. Add `IOOpRename IOOp = "rename"` to iofault.go (only Patch/Rewrite rename).
2. Add `IOFaultInjector` field to `PatchOptions` and `RewriteOptions`.
3. Thread into `initializeNewSegment` (pass
   `Config{Path: tmp, IOFaultInjector: opts.IOFaultInjector}`). Add a
   consult before each direct write/WriteAt (frame-len, frame, footer,
   header), the tmp `syncFile`, and the `os.Rename`. Replace
   `syncParentDir(path, nil)` with `syncParentDir(path, opts.IOFaultInjector)`.
4. Add a small free helper `beforeSegmentIO(faults IOFaultInjector, path string, op IOOp) error`
   in segment (Patch/Rewrite hold `opts.IOFaultInjector`, not a Config). Keep
   `Config.beforeIO` delegating to it to avoid two code paths.
5. Tests RED-FIRST (segment/patch_test.go, segment/rewrite_test.go or extend
   existing): short-write, fsync-error, ENOSPC, rename-error at each seam
   ordinal; assert error propagates AND the ORIGINAL file is untouched and the
   tmp is cleaned up (Patch/Rewrite `success=false` defer removes tmp). Mirror
   opOrdinalIOFault. Extend it to recognize IOOpRename.

### Part B — plumb injector to a running server
6. orchestrator/config.go: add `SegmentIOFaultInjector segment.IOFaultInjector`
   (doc: nil in prod, mirrors CrashInjector).
7. Forward into every live.Open/ingest.Config literal: steady.go:94,
   bootstrap.go:67 (live), bootstrap.go/merge.go ingest writers. AND into the
   `segment.RewriteOptions` at compact_deletes.go:377 and `segment.PatchOptions`
   at import_pass.go:420.
8. jetstreamd/options.go: add `SegmentIOFaultInjector segment.IOFaultInjector`
   to Options. runtime.go:321: forward into orchestrator.Config. Nil-in-prod.

### Part C — ENOSPC fail-loud parity on compaction/import (DECISION: INCLUDE)
9. Lift the ENOSPC operator-message wrapper so the orchestrator can call it.
   Currently `wrapSegmentPersistenceError` is a method on ingest.Config /
   ingest.Writer (internal/ingest/enospc.go). Extract the core into a shared
   helper the orchestrator package can reach WITHOUT an import cycle — options:
   (a) a small exported func in a low-level package (e.g. segment or a new
   internal/diskfull), or (b) an exported ingest func
   `ingest.WrapDiskFull(dataDir, op, err)`. Prefer the option with no new
   dependency edge; verify import graph before choosing. Then wrap:
   - compact_deletes.go rewrite error (~:379) with op like
     "rewriting segment during compaction".
   - import_pass.go patch error (~:424) with op like
     "patching segment during timestamp import".
   Contributing-factor rationale: without this, disk-full during compaction /
   import crashes with a raw syscall.ENOSPC, inconsistent with the writer path
   and #201's decided posture.

### Part D — oracle segment-fault tier (`internal/oracle/`) — FULL DoD SWEEP
10. Env vars `JETSTREAM_ORACLE_RESTART_SEGMENT_FAULT_{OP,ORDINAL,OBSERVED}` +
    `segmentFault{Op,Ordinal,ObservedPath}` on restartChildArgs + env plumbing
    in runRestartChild + `newOracleSegmentIOFaultFromEnv(t)` builder returning a
    `segment.IOFaultInjector` (op+ordinal, sentinel error
    `errSegmentFaultInjected`). Wire `SegmentIOFaultInjector:` into
    jetstreamd.Options in TestOracleRestartChild.
11. New restart_segmentfault_test.go scenarios (full sweep across
    writer-flush, seal, compaction-rewrite, import-patch):
    - short-write / fsync-error / ENOSPC on steady flush + seal → fail-loud
      (sentinel or disk-full msg observed via rt.Run) → clean recovery child
      converges (assertOracleMatchesAfterReplay + assertChainDurable). Proves
      torn-tail walk recovers, no silent corruption.
    - ENOSPC e2e asserting the #201 operator message specifically.
    - post-crash truncate/corrupt-at-offset: harness mutates active segment
      bytes between children, asserts WalkActive/ObserveSegments recovers to
      last good frame + Compare holds.
    - compaction-rewrite + import-patch fault variants (needs the child to
      reach a compaction pass and/or an import; may require a small barrier or
      hook — the child currently exits at after-merge and may not naturally
      reach steady-state compaction. Investigate: either lengthen the child to
      steady-state with CompactionInterval small + a compaction-done barrier,
      or add a targeted after-compaction barrier. Call it out if it grows.)
12. Guard test pinning oracle segment-fault op constants to the segment
    package (mirror TestMergeNextSourceIdxKeyMatchesOracleStoreFault).

### Part E — mutation campaign (DECISION: TWO mutants)
13. New tier `segmentfault` in run.sh: new `case` in the :221 switch + a
    `segmentfault_timeout` (mirror storefault; shares restart subprocess
    budget). Command runs the new oracle scenario (+ any orchestrator unit
    tests that pin the compaction/import fail-loud contract directly).
14. Two mutant patches, `tiers: segmentfault`:
    - m044 (or next free id — CHECK `ls testing/mutation/mutants/`; current max
      is m043): swallow an ACTIVE-WRITER/seal segment write error (invert an
      `err != nil` on flushLocked/sealAfterFlush return, or drop stickyErr).
    - m045: swallow the COMPACTION-REWRITE call-site error
      (compact_deletes.go:379 `err != nil` -> `err == nil`) — mirrors m006's
      shape on the segment path. (Import-patch is an alternative for the second
      mutant if compaction proves awkward to reach in the tier.)
    Both must be KILLED@segmentfault. Re-run campaign, refresh baseline +
    RESULTS.md, and UPDATE the :994-1001 gap note (seam now exists).

### Part F — docs & tracking
15. docs/README.md: segment I/O fault posture + compaction/import ENOSPC
    parity. specs/oracle.md: add the segmentfault tier to the tier list.
    ~/oracle.md scoreboard: flip #200 to DONE with PR ref; note "+2 mutants".

---

## 6. Locked decisions (from Jim, 2026-07-06)

1. ENOSPC parity (Part C): INCLUDE in #200 (not split). Uniform disk-full
   posture across all segment persistence paths.
2. Tier scope (Part D): FULL DoD sweep — writer-flush + seal + compaction-
   rewrite + import-patch, across short-write/fsync/ENOSPC/truncate-at-offset.
3. Mutants (Part E): TWO — one active-writer/seal, one compaction-rewrite
   (m006+m028 pattern).

---

## 7. Tracking (AGENTS.md granular-issue discipline)

File BEFORE starting code (so numbers are available for branch/commits):
- Issue: "segment: fault-inject Patch/Rewrite I/O (write/fsync/rename)" — Part A.
- Issue: "ingest,orchestrator: thread SegmentIOFaultInjector + ENOSPC parity on
  compaction/import" — Parts B+C.
- Issue: "oracle: segment-fault tier + segmentfault mutation tier" — Parts D+E.
All reference #200; the final commit/PR closes #200 via `Closes #200`. Link to
epic #35. Post a starting comment on #200.

## 8. Verification gates (before calling DONE)

- `just lint`
- `just test ./segment ./internal/ingest ./internal/ingest/orchestrator ./internal/oracle`
- `just test-long ./internal/oracle -run TestOracle_Restart`
- `just mutation-campaign` (or scoped to segmentfault tier): both new mutants
  KILLED@segmentfault, no existing mutant regressed, zero STALE/BUILD-BROKEN.
- Red-first discipline throughout: every new assertion must be shown to FAIL
  before the production change makes it pass (neuter the seam / apply the
  mutant by hand to confirm the kill is non-vacuous).

## 9. Known risk / open sub-question to resolve during build

The restart child exits at the after-merge barrier and may NOT naturally reach
steady-state compaction or an import. To exercise the compaction-rewrite and
import-patch fault paths e2e, the segment-fault scenario likely needs either a
longer-running child (steady-state with small CompactionInterval + a
compaction-completed barrier) or a new targeted barrier/hook. Resolve within
Part D; if it requires a production-observable hook addition, keep it test-only
(nil in prod), mirroring the existing PhaseBarrier / OnCompactionPass hooks.
Segment package tests (Part A) cover Patch/Rewrite faults directly and are NOT
subject to this risk.
