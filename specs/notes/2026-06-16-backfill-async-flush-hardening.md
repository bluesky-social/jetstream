# Backfill Async Flush Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Promote the bootstrap backfill async flush path to a production default, remove temporary high-cardinality/debug observability, and verify concurrency/durability behavior.

**Architecture:** Keep the segment `PreparedBlock` API and ingest async flush pipeline, but remove temporary per-host/getRepo and AppendBatch timing metrics used only for measurement. Add focused tests for async commit ordering, close/seal behavior, default configuration, and absence of temporary metric names.

**Tech Stack:** Go, Pebble metadata store, Prometheus metrics, Jetstream segment writer, `just` test/oracle recipes.

---

### Task 1: Remove Temporary Metrics Coverage

**Files:**
- Modify: `internal/ingest/metrics_test.go`
- Modify: `internal/ingest/backfill/metrics_test.go`

- [ ] **Step 1: Write failing tests for no debug metrics**

Replace temporary-metric assertions with tests that stable metrics register and gathered metric names do not contain `_debug_`:

```go
func requireNoDebugMetrics(t *testing.T, reg *prometheus.Registry) {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		require.NotContains(t, mf.GetName(), "_debug_")
	}
}
```

- [ ] **Step 2: Verify red**

Run:

```sh
just test ./internal/ingest ./internal/ingest/backfill -run 'TestNewMetrics'
```

Expected: fails while current `debug_*` metrics are still registered.

- [ ] **Step 3: Remove temporary metric fields and helpers**

Delete debug metric fields/helpers from `internal/ingest/metrics.go` and `internal/ingest/backfill/metrics.go`.

- [ ] **Step 4: Remove temporary hook wiring**

Remove `jttp.WithBodyObserver(backfillMetrics.ObserveDebugGetRepoJttpBody)` from `internal/jetstreamd/runtime.go` and remove `OnWorkerStageChange` wiring from `internal/ingest/backfill/run.go`.

- [ ] **Step 5: Verify green**

Run:

```sh
just test ./internal/ingest ./internal/ingest/backfill ./internal/jetstreamd
```

Expected: pass.

### Task 2: Harden Async Flush Tests

**Files:**
- Modify: `internal/ingest/writer_test.go`
- Modify: `segment/writer_test.go`

- [ ] **Step 1: Write tests for async parity**

Add tests covering:

```go
func TestAppendBatch_AsyncFlushConcurrentBatchesRemainContiguous(t *testing.T)
func TestWriter_AsyncCloseFlushesPendingBlockAndPersistsNextSeq(t *testing.T)
func TestWriter_AsyncSealActiveAndCloseSealsPendingBlock(t *testing.T)
```

- [ ] **Step 2: Write segment prepared-block tests**

Add tests covering:

```go
func TestPreparedFlushRequiresOriginalCommitOrder(t *testing.T)
func TestCommitPreparedFlushRejectsMismatchedPreparedBlock(t *testing.T)
```

- [ ] **Step 3: Verify red**

Run:

```sh
just test ./segment ./internal/ingest -run 'Prepared|Async'
```

Expected: any newly exposed missing guard fails before implementation.

- [ ] **Step 4: Implement minimal hardening**

If tests reveal missing validation, update `segment.Writer.CommitPreparedFlush` to reject commits not matching the writer's current pending offset/block order. If close/seal tests reveal async lifecycle gaps, fix `internal/ingest/async_flush.go`.

- [ ] **Step 5: Verify green**

Run:

```sh
just test ./segment ./internal/ingest
```

Expected: pass.

### Task 3: Productionize Defaults and Documentation Comments

**Files:**
- Modify: `cmd/jetstream/main.go`
- Modify: `cmd/jetstream/serve_test.go`
- Modify: `internal/jetstreamd/options.go`
- Modify: `internal/ingest/config.go`
- Modify: `internal/ingest/orchestrator/config.go`

- [ ] **Step 1: Keep CLI default test at 4**

Ensure `TestServeOptionsFromCLI_Defaults` expects `BackfillAsyncFlushWorkers == 4`.

- [ ] **Step 2: Remove experimental/debug wording**

Update comments/help text so async flush is described as a bootstrap backfill option, with `0` as the explicit synchronous fallback.

- [ ] **Step 3: Verify focused CLI/runtime tests**

Run:

```sh
just test ./cmd/jetstream ./internal/jetstreamd ./internal/ingest/orchestrator
```

Expected: pass.

### Task 4: Full Verification

**Files:**
- No code changes.

- [ ] **Step 1: Run full short suite**

Run:

```sh
just
```

Expected: lint and short tests pass.

- [ ] **Step 2: Run oracle coverage**

Run:

```sh
just test ./internal/oracle
just test-long ./internal/oracle -run TestOracle_Restart -v
```

Expected: oracle short and restart/recovery checks pass.

- [ ] **Step 3: Report residual risk**

Summarize any verification skipped or any remaining non-production temporary surface.
