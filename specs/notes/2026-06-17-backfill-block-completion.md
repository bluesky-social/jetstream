# Backfill Block Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove per-repo backfill fsyncs by queueing repo completion metadata and committing it at segment block durability barriers.

**Architecture:** Add a generic ingest-writer durable batch hook that stages metadata into the same synced Pebble batch as `seq/next` after a segment block fsync. Add a backfill completion batcher that records each repo's final appended seq, lets `Store.OnComplete` queue completion in memory, and stages durable `repo/<did>` completion rows only when the writer reports a durable seq high enough to cover that repo. Move `listRepos` cursor checkpointing to batch/final durability barriers so cursor persistence never outruns queued completions.

**Tech Stack:** Go, Pebble batches, existing `internal/ingest`, `internal/ingest/backfill`, `segment.Writer`, `just test`.

---

## File Structure

- Modify `internal/ingest/config.go`: add a durable batch hook type and config field that is valid with async flush.
- Modify `internal/ingest/writer.go`: stage `seq/next` in a Pebble batch, call the durable hook, commit once, and expose a drain method for metadata-only barriers.
- Modify `internal/ingest/async_flush.go`: use the same durable batch commit helper after async block fsync.
- Modify `internal/ingest/writer_test.go`: cover durable hook ordering, async compatibility, drain behavior, and error propagation.
- Create `internal/ingest/backfill/completion_batcher.go`: queue in-process repo completions and stage them at durable seq barriers.
- Create `internal/ingest/backfill/completion_batcher_test.go`: cover batching, visibility after flush, metadata-only completion, and hook failure.
- Modify `internal/ingest/backfill/store.go`: split completion staging from immediate `OnComplete`, and delegate success completion to the batcher when configured.
- Modify `internal/ingest/backfill/handler.go`: remove per-repo `Flush` and record the last appended seq for the batcher.
- Modify `internal/ingest/backfill/run.go`: wire the batcher, move cursor persistence from `OnPageComplete` to `OnBatchComplete`, and drain before cursor writes and run return.
- Modify `internal/ingest/backfill/selected.go`: drain the batcher after selected repos finish.
- Modify `internal/ingest/backfill/metrics.go`: add queue/commit/backlog metrics.
- Modify targeted tests in `internal/ingest/backfill/*_test.go` and restart/oracle tests called out in Tasks 5, 6, and 8.

---

### Task 1: Add Ingest Durable Batch Hook

**Files:**
- Modify: `internal/ingest/config.go`
- Modify: `internal/ingest/writer.go`
- Modify: `internal/ingest/async_flush.go`
- Test: `internal/ingest/writer_test.go`

- [ ] **Step 1: Write failing sync-hook test**

Add this test near the existing flush hook tests in `internal/ingest/writer_test.go`:

```go
func TestFlush_StagesDurableBatchHookWithSeq(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	var hookSeq uint64
	w, err := Open(Config{
		SegmentsDir:       filepath.Join(dir, "segments"),
		Store:             st,
		MaxEventsPerBlock: 2,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnDurableBatch: func(ctx context.Context, b *pebble.Batch, nextSeq uint64, force bool) (func(), error) {
			require.False(t, force)
			hookSeq = nextSeq
			return nil, b.Set([]byte("hook/ran"), []byte("yes"), nil)
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	require.NoError(t, w.AppendBatch(t.Context(), []segment.Event{
		{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r1", Rev: "1"},
		{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r2", Rev: "1"},
	}))

	require.Equal(t, uint64(2), hookSeq)
	got, closer, err := st.Get([]byte("hook/ran"))
	require.NoError(t, err)
	require.Equal(t, "yes", string(got))
	require.NoError(t, closer.Close())
	persisted, err := loadNextSeq(st, w.cfg.SeqKey)
	require.NoError(t, err)
	require.Equal(t, uint64(2), persisted)
}
```

- [ ] **Step 2: Run test and verify it fails**

Run: `just test ./internal/ingest -run TestFlush_StagesDurableBatchHookWithSeq -v`

Expected: FAIL because `Config.OnDurableBatch` is undefined.

- [ ] **Step 3: Add the hook type and config field**

In `internal/ingest/config.go`, add the Pebble import and type:

```go
import (
	"context"
	"fmt"
	"log/slog"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble"
)

type DurableBatchHook func(ctx context.Context, b *pebble.Batch, nextSeq uint64, force bool) (afterCommit func(), err error)
```

Add this field after `OnAfterFlush`:

```go
	// OnDurableBatch, if non-nil, stages extra metadata into the same synced
	// Pebble batch that persists SeqKey after a segment block has been fsynced.
	// The hook may return an afterCommit callback, which runs only after the
	// batch commit succeeds. Unlike OnAfterFlush, this hook is supported with
	// AsyncFlushWorkers because it is tied to a specific durable block.
	OnDurableBatch DurableBatchHook
```

- [ ] **Step 4: Add seq staging and durable commit helper**

In `internal/ingest/writer.go`, add this helper near `saveNextSeq`:

```go
func stageNextSeq(b *pebble.Batch, key string, v uint64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	if err := b.Set([]byte(key), buf[:], nil); err != nil {
		return fmt.Errorf("ingest: stage %s: %w", key, err)
	}
	return nil
}

func (w *Writer) commitDurableBatchLocked(ctx context.Context, nextSeq uint64, force bool) error {
	b := w.cfg.Store.NewBatch()
	defer func() { _ = b.Close() }()

	if err := stageNextSeq(b, w.cfg.SeqKey, nextSeq); err != nil {
		return err
	}
	var afterCommit func()
	if w.cfg.OnDurableBatch != nil {
		cb, err := w.cfg.OnDurableBatch(ctx, b, nextSeq, force)
		if err != nil {
			return fmt.Errorf("ingest: on_durable_batch: %w", err)
		}
		afterCommit = cb
	}
	if err := w.cfg.Store.Commit(b, store.SyncWrites); err != nil {
		return fmt.Errorf("ingest: commit durable batch: %w", err)
	}
	if afterCommit != nil {
		afterCommit()
	}
	return nil
}
```

Add `github.com/cockroachdb/pebble` to `writer.go` imports.

- [ ] **Step 5: Use the helper in sync flush**

Replace this block in `flushBlockLocked`:

```go
	if err := saveNextSeq(w.cfg.Store, w.cfg.SeqKey, w.nextSeq); err != nil {
		return err
	}
```

with:

```go
	if err := w.commitDurableBatchLocked(ctx, w.nextSeq, false); err != nil {
		return err
	}
```

Keep `OnAfterFlush` after the durable batch commit.

- [ ] **Step 6: Use the helper in async flush**

In `internal/ingest/async_flush.go`, replace:

```go
	if err := saveNextSeq(w.cfg.Store, w.cfg.SeqKey, job.nextSeq); err != nil {
		return err
	}
```

with:

```go
	if err := w.commitDurableBatchLocked(ctx, job.nextSeq, false); err != nil {
		return err
	}
```

- [ ] **Step 7: Run sync-hook test**

Run: `just test ./internal/ingest -run TestFlush_StagesDurableBatchHookWithSeq -v`

Expected: PASS.

- [ ] **Step 8: Add async compatibility test**

Add this test to `internal/ingest/writer_test.go`:

```go
func TestAppendBatch_AsyncFlushRunsDurableBatchHook(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	var committed atomic.Bool
	w, err := Open(Config{
		SegmentsDir:       filepath.Join(dir, "segments"),
		Store:             st,
		MaxEventsPerBlock: 2,
		AsyncFlushWorkers: 2,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnDurableBatch: func(ctx context.Context, b *pebble.Batch, nextSeq uint64, force bool) (func(), error) {
			require.False(t, force)
			require.Equal(t, uint64(2), nextSeq)
			return func() { committed.Store(true) }, b.Set([]byte("async/hook"), []byte("ok"), nil)
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	require.NoError(t, w.AppendBatch(t.Context(), []segment.Event{
		{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r1", Rev: "1"},
		{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r2", Rev: "1"},
	}))
	require.True(t, committed.Load())
}
```

- [ ] **Step 9: Run async compatibility test**

Run: `just test ./internal/ingest -run TestAppendBatch_AsyncFlushRunsDurableBatchHook -v`

Expected: PASS.

- [ ] **Step 10: Run ingest package tests**

Run: `just test ./internal/ingest`

Expected: PASS.

---

### Task 2: Add Writer Durability Drain Barrier

**Files:**
- Modify: `internal/ingest/writer.go`
- Modify: `internal/ingest/async_flush.go`
- Test: `internal/ingest/writer_test.go`

- [ ] **Step 1: Write failing metadata-only drain test**

Add this test to `internal/ingest/writer_test.go`:

```go
func TestDrainDurability_CommitsHookWithoutPendingEvents(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	w, err := Open(Config{
		SegmentsDir: filepath.Join(dir, "segments"),
		Store:       st,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnDurableBatch: func(ctx context.Context, b *pebble.Batch, nextSeq uint64, force bool) (func(), error) {
			require.True(t, force)
			require.Equal(t, uint64(0), nextSeq)
			return nil, b.Set([]byte("metadata/only"), []byte("ok"), nil)
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	require.NoError(t, w.DrainDurability(t.Context()))

	got, closer, err := st.Get([]byte("metadata/only"))
	require.NoError(t, err)
	require.Equal(t, "ok", string(got))
	require.NoError(t, closer.Close())
}
```

- [ ] **Step 2: Run test and verify it fails**

Run: `just test ./internal/ingest -run TestDrainDurability_CommitsHookWithoutPendingEvents -v`

Expected: FAIL because `Writer.DrainDurability` is undefined.

- [ ] **Step 3: Add `durableNextSeq` to writer**

Add this field to `Writer` in `internal/ingest/writer.go`:

```go
	durableNextSeq uint64
```

After `w.nextSeq = reconciled` in `Open`, add:

```go
	w.durableNextSeq = reconciled
```

After successful `commitDurableBatchLocked`, add:

```go
	w.durableNextSeq = nextSeq
```

- [ ] **Step 4: Implement `DrainDurability`**

Add this method near `Flush`:

```go
// DrainDurability forces pending event-backed metadata to its block durability
// point and commits metadata-only durable hooks even when no events are pending.
func (w *Writer) DrainDurability(ctx context.Context) error {
	if w.async != nil {
		if err := w.Flush(ctx); err != nil {
			return err
		}
		w.asyncJobs.Wait()
		w.mu.Lock()
		defer w.mu.Unlock()
		if w.closed {
			return ErrClosed
		}
		return w.commitDurableBatchLocked(ctx, w.durableNextSeq, true)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if w.active == nil {
		return nil
	}
	if w.active.Pending() > 0 {
		if err := w.flushAndRotateLocked(ctx); err != nil {
			return err
		}
	}
	return w.commitDurableBatchLocked(ctx, w.durableNextSeq, true)
}
```

- [ ] **Step 5: Run drain test**

Run: `just test ./internal/ingest -run TestDrainDurability_CommitsHookWithoutPendingEvents -v`

Expected: PASS.

- [ ] **Step 6: Run ingest package tests**

Run: `just test ./internal/ingest`

Expected: PASS.

---

### Task 3: Add Backfill Completion Batcher

**Files:**
- Create: `internal/ingest/backfill/completion_batcher.go`
- Test: `internal/ingest/backfill/completion_batcher_test.go`

- [ ] **Step 1: Write failing completion visibility test**

Create `internal/ingest/backfill/completion_batcher_test.go`:

```go
package backfill

import (
	"testing"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	"github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/stretchr/testify/require"
)

func TestCompletionBatcherStagesCompletionAtDurableSeq(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	bs := NewStore(st, nil)
	did := atmos.DID("did:plc:completebatch")
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(did)))

	cb := NewCompletionBatcher(bs, nil)
	cb.RecordWatermark(did, 41, true)
	require.NoError(t, cb.QueueComplete(t.Context(), did, &repo.Commit{DID: string(did), Rev: "rev1"}))

	b := st.NewBatch()
	after, err := cb.StageDurable(t.Context(), b, 42, false)
	require.NoError(t, err)
	require.NoError(t, st.Commit(b, store.SyncWrites))
	require.NotNil(t, after)
	after()
	require.NoError(t, b.Close())

	got, err := bs.Lookup(t.Context(), did)
	require.NoError(t, err)
	require.Equal(t, atmosbackfill.StateComplete, got.State)
}
```

- [ ] **Step 2: Run test and verify it fails**

Run: `just test ./internal/ingest/backfill -run TestCompletionBatcherStagesCompletionAtDurableSeq -v`

Expected: FAIL because `NewCompletionBatcher` is undefined.

- [ ] **Step 3: Implement completion batcher skeleton**

Create `internal/ingest/backfill/completion_batcher.go`:

```go
package backfill

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/repo"
)

type completionBatcher struct {
	mu         sync.Mutex
	store      *Store
	metrics    *Metrics
	watermarks map[atmos.DID]completionWatermark
	queued     []queuedCompletion
}

type completionWatermark struct {
	lastSeq  uint64
	appended bool
}

type queuedCompletion struct {
	did       atmos.DID
	commit    *repo.Commit
	completed time.Time
	watermark completionWatermark
}

func NewCompletionBatcher(st *Store, m *Metrics) *completionBatcher {
	return &completionBatcher{
		store:      st,
		metrics:    m,
		watermarks: make(map[atmos.DID]completionWatermark),
	}
}

func (b *completionBatcher) RecordWatermark(did atmos.DID, lastSeq uint64, appended bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.watermarks[did] = completionWatermark{lastSeq: lastSeq, appended: appended}
}

func (b *completionBatcher) QueueComplete(ctx context.Context, did atmos.DID, commit *repo.Commit) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	wm := b.watermarks[did]
	delete(b.watermarks, did)
	b.queued = append(b.queued, queuedCompletion{
		did: did, commit: commit, completed: timeNow(), watermark: wm,
	})
	b.metrics.incCompletionQueued()
	return nil
}

func (b *completionBatcher) StageDurable(ctx context.Context, batch *pebble.Batch, nextSeq uint64, force bool) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	ready, remaining := splitReadyCompletions(b.queued, nextSeq, force)
	if len(ready) == 0 {
		b.mu.Unlock()
		return nil, nil
	}
	b.mu.Unlock()

	if err := b.store.stageCompleteBatch(ctx, batch, ready); err != nil {
		return nil, err
	}
	return func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.queued = removeCommittedCompletions(b.queued, ready)
		b.metrics.incCompletionCommitted(len(ready))
		_ = remaining
	}, nil
}

func splitReadyCompletions(in []queuedCompletion, nextSeq uint64, force bool) ([]queuedCompletion, []queuedCompletion) {
	ready := make([]queuedCompletion, 0, len(in))
	remaining := make([]queuedCompletion, 0, len(in))
	for _, c := range in {
		if !c.watermark.appended || c.watermark.lastSeq < nextSeq {
			ready = append(ready, c)
			continue
		}
		if force {
			remaining = append(remaining, c)
			continue
		}
		remaining = append(remaining, c)
	}
	return ready, remaining
}

func removeCommittedCompletions(in, committed []queuedCompletion) []queuedCompletion {
	done := make(map[atmos.DID]struct{}, len(committed))
	for _, c := range committed {
		done[c.did] = struct{}{}
	}
	out := in[:0]
	for _, c := range in {
		if _, ok := done[c.did]; ok {
			continue
		}
		out = append(out, c)
	}
	return out
}
```

- [ ] **Step 4: Add test helper**

Add this helper in `completion_batcher_test.go`:

```go
func testListReposEntry(did atmos.DID) atmossync.ListReposEntry {
	return atmossync.ListReposEntry{DID: did, Active: true}
}
```

- [ ] **Step 5: Add store staging method**

In `internal/ingest/backfill/store.go`, add:

```go
func (s *Store) stageCompleteBatch(ctx context.Context, batch *pebble.Batch, completions []queuedCompletion) error {
	s.countsMu.Lock()
	defer s.countsMu.Unlock()

	counts, ok, err := LoadCounts(s.db)
	if err != nil {
		return err
	}
	if !ok {
		counts, err = CountStatuses(s.db)
		if err != nil {
			return err
		}
	}

	hostCache := make(map[string]*HostStatus)
	for _, c := range completions {
		rs, err := s.readRepoStatus(c.did)
		if err != nil {
			return err
		}
		hadRow := rs != nil
		old := Status("")
		if rs == nil {
			rs = &RepoStatus{}
		} else {
			old = rs.Backfill.Status
		}
		rs.Backfill.Status = StatusComplete
		rs.Backfill.Rev = c.commit.Rev
		rs.Backfill.CompletedAt = c.completed
		rs.Backfill.LastError = ""
		rs.Rev = c.commit.Rev
		rs.UpdatedAt = c.completed
		rs.LastAttemptedAt = c.completed
		applyCountTransition(&counts, hadRow, old, StatusComplete)

		enc, err := encodeRepoStatus(rs)
		if err != nil {
			return err
		}
		if err := batch.Set(repoKey(c.did), enc, nil); err != nil {
			return fmt.Errorf("backfill: stage repo/%s: %w", c.did, err)
		}
		if rs.Host != "" {
			hs := hostCache[rs.Host]
			if hs == nil {
				var err error
				hs, _, err = loadHostStatus(s.db, rs.Host)
				if err != nil {
					return err
				}
				hostCache[rs.Host] = hs
			}
			applyHostStatusTransition(hs, hadRow, rs.Active, old, StatusComplete)
			hs.LastAttemptedAt = c.completed
		}
	}
	countsEnc, err := encodeCounts(counts)
	if err != nil {
		return err
	}
	if err := batch.Set([]byte(countsKey), countsEnc, nil); err != nil {
		return fmt.Errorf("backfill: stage counts: %w", err)
	}
	for _, hs := range hostCache {
		if err := stageHostStatus(batch, hs); err != nil {
			return err
		}
	}
	return nil
}
```

Add `github.com/cockroachdb/pebble` to `store.go` imports.

- [ ] **Step 6: Run completion batcher test**

Run: `just test ./internal/ingest/backfill -run TestCompletionBatcherStagesCompletionAtDurableSeq -v`

Expected: PASS.

---

### Task 4: Wire Handler and Store to Queue Completion

**Files:**
- Modify: `internal/ingest/backfill/handler.go`
- Modify: `internal/ingest/backfill/store.go`
- Modify: `internal/ingest/backfill/run.go`
- Test: `internal/ingest/backfill/handler_test.go`
- Test: `internal/ingest/backfill/store_test.go`

- [ ] **Step 1: Write failing handler test for no per-repo flush**

Replace `TestSegmentHandler_HandleRepoFlushesBeforeReturning` with:

```go
func TestSegmentHandler_HandleRepoDoesNotFlushBeforeReturning(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	segmentsDir := filepath.Join(dir, "segments")
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       segmentsDir,
		Store:             st,
		MaxEventsPerBlock: 4096,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	did := atmos.DID("did:plc:no-flush-before-complete")
	r, commit := buildSingleRecordRepo(t, did, "app.bsky.feed.post", "rkey1", map[string]any{"text": "queued"})
	h := NewSegmentHandler(w, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)

	require.NoError(t, h.HandleRepo(t.Context(), did, r, commit))
	events := collectActiveEvents(t, filepath.Join(segmentsDir, ingest.SegmentFilename(0)))
	require.Empty(t, events, "HandleRepo should leave the partial block buffered")
	require.NoError(t, w.Flush(t.Context()))
	events = collectActiveEvents(t, filepath.Join(segmentsDir, ingest.SegmentFilename(0)))
	require.Len(t, events, 1)
}
```

- [ ] **Step 2: Run test and verify it fails**

Run: `just test ./internal/ingest/backfill -run TestSegmentHandler_HandleRepoDoesNotFlushBeforeReturning -v`

Expected: FAIL because `HandleRepo` still flushes.

- [ ] **Step 3: Add completion batcher to handler**

Add a field to `SegmentHandler`:

```go
	completions *completionBatcher
```

Add a setter:

```go
func (h *SegmentHandler) SetCompletionBatcher(c *completionBatcher) {
	h.completions = c
}
```

In `HandleRepo`, track `lastSeq` after each successful `AppendBatch`:

```go
	var lastSeq uint64
	var sawSeq bool
```

After `h.writer.AppendBatch(ctx, batch)` succeeds:

```go
	for i := range batch {
		lastSeq = batch[i].Seq
		sawSeq = true
	}
```

Remove:

```go
	if appended {
		if err := h.writer.Flush(ctx); err != nil {
			err = fmt.Errorf("backfill: did=%s flush before complete: %w", did, err)
			h.abortOnWriterError(err)
			return err
		}
	}
```

Replace it with:

```go
	if h.completions != nil {
		h.completions.RecordWatermark(did, lastSeq, appended && sawSeq)
	}
```

- [ ] **Step 4: Make Store.OnComplete queue when batcher exists**

Add a field to `Store`:

```go
	completions *completionBatcher
```

Add a setter:

```go
func (s *Store) SetCompletionBatcher(c *completionBatcher) {
	s.completions = c
}
```

At the top of `OnComplete`, after `ctx.Err()` check:

```go
	if s.completions != nil {
		if err := s.completions.QueueComplete(ctx, did, commit); err != nil {
			return err
		}
		return nil
	}
```

Keep the existing immediate path for tests and callers that do not configure a batcher.

- [ ] **Step 5: Wire the batcher in Run**

In `backfill.Run`, after store and handler construction:

```go
	completions := NewCompletionBatcher(st, cfg.Metrics)
	st.SetCompletionBatcher(completions)
	handler.SetCompletionBatcher(completions)
	cfg.Writer.SetDurableBatchHook(completions.StageDurable)
```

If `Writer` has no setter, add:

```go
func (w *Writer) SetDurableBatchHook(h DurableBatchHook) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.cfg.OnDurableBatch = h
}
```

- [ ] **Step 6: Run focused handler and store tests**

Run: `just test ./internal/ingest/backfill -run 'TestSegmentHandler_HandleRepoDoesNotFlushBeforeReturning|TestStore_OnComplete_WritesComplete' -v`

Expected: PASS. The store test should still use the immediate path because it does not set a batcher.

---

### Task 5: Move Cursor Persistence to Batch Durability Barrier

**Files:**
- Modify: `internal/ingest/backfill/run.go`
- Modify: `internal/ingest/backfill/cursor.go`
- Test: `internal/ingest/backfill/run_test.go`

- [ ] **Step 1: Write failing cursor safety test**

Add this test to `internal/ingest/backfill/run_test.go` near cursor tests:

```go
func TestRun_CursorWaitsForQueuedCompletions(t *testing.T) {
	t.Parallel()

	dids := []atmos.DID{"did:plc:cursor-a", "did:plc:cursor-b", "did:plc:cursor-c"}
	fixtures := make(map[atmos.DID]repoFixture, len(dids))
	for _, d := range dids {
		fixtures[d] = buildRepoFixture(t, d)
	}
	srv := newStubServer(t, fixtures)

	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.NoError(t, runWithStub(t, t.Context(), srv, db))

	cursor, err := LoadListReposCursor(db)
	require.NoError(t, err)
	require.Empty(t, cursor, "final batch checkpoint persists the relay's terminal empty cursor")

	bf := NewStore(db, nil)
	for _, did := range dids {
		got, err := bf.Lookup(t.Context(), did)
		require.NoError(t, err)
		require.Equal(t, atmosbackfill.StateComplete, got.State)
	}
}
```

- [ ] **Step 2: Run test and verify current cursor path fails or is not sufficient**

Run: `just test ./internal/ingest/backfill -run TestRun_CursorWaitsForQueuedCompletions -v`

Expected before implementation: FAIL because completions are queued but cursor checkpointing is still page-based.

- [ ] **Step 3: Stop saving cursor from OnPageComplete**

In `backfill.Run`, change `OnPageComplete` to only update in-memory observability if needed, or omit it:

```go
OnPageComplete: gt.None[func(string) error](),
```

If `gt.None` is not ergonomic for struct literal omission, remove the `OnPageComplete` field entirely.

- [ ] **Step 4: Save cursor from OnBatchComplete after drain**

Add this to `engineOpts`:

```go
OnBatchComplete: gt.Some(func(cursor string) error {
	if err := cfg.Writer.DrainDurability(ctx); err != nil {
		return err
	}
	if err := SaveListReposCursor(cfg.Store, cursor); err != nil {
		return err
	}
	return MaybeSaveBootstrapLastListReposCursor(cfg.Store, cursor)
}),
```

- [ ] **Step 5: Update cursor.go comments**

Replace the known durability hole comment in `internal/ingest/backfill/cursor.go` with:

```go
// # Persistence semantics
//
// SaveListReposCursor is called from atmos OnBatchComplete after all jobs in
// the batch have returned and the backfill runner has drained queued completion
// metadata through the ingest writer durability barrier. A persisted cursor
// therefore never covers a repo whose queued completion is still only in memory.
```

- [ ] **Step 6: Run cursor safety test**

Run: `just test ./internal/ingest/backfill -run TestRun_CursorWaitsForQueuedCompletions -v`

Expected: PASS.

---

### Task 6: Drain Completion Queue on Selected Runs and Bootstrap Cutover

**Files:**
- Modify: `internal/ingest/backfill/run.go`
- Modify: `internal/ingest/backfill/selected.go`
- Test: `internal/ingest/backfill/run_test.go`

- [ ] **Step 1: Add selected-repo drain test**

Add this test to `internal/ingest/backfill/run_test.go`:

```go
func TestRun_SelectedReposDrainQueuedCompletions(t *testing.T) {
	t.Parallel()

	dids := []atmos.DID{"did:plc:selected-a", "did:plc:selected-b"}
	fixtures := make(map[atmos.DID]repoFixture, len(dids))
	for _, d := range dids {
		fixtures[d] = buildRepoFixture(t, d)
	}
	srv := newStubServer(t, fixtures)

	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.NoError(t, runWithStubRepos(t, t.Context(), srv, db, dids))

	bf := NewStore(db, nil)
	for _, did := range dids {
		got, err := bf.Lookup(t.Context(), did)
		require.NoError(t, err)
		require.Equal(t, atmosbackfill.StateComplete, got.State)
	}
}
```

- [ ] **Step 2: Run test and verify it fails**

Run: `just test ./internal/ingest/backfill -run TestRun_SelectedReposDrainQueuedCompletions -v`

Expected: FAIL until selected path drains.

- [ ] **Step 3: Pass batcher into selected runner**

Add to `selectedReposConfig`:

```go
	Completions *completionBatcher
	Writer      *ingest.Writer
```

Pass these from `Run`.

- [ ] **Step 4: Drain after selected repos**

At the end of `runSelectedRepos`, before returning nil:

```go
	if cfg.Writer != nil {
		if err := cfg.Writer.DrainDurability(ctx); err != nil {
			return err
		}
	}
	return nil
```

- [ ] **Step 5: Drain after atmos Run returns**

In the normal backfill path, after `engine.Run` returns nil and before `backfill.Run` returns nil:

```go
if err := cfg.Writer.DrainDurability(ctx); err != nil {
	return fmt.Errorf("backfill: drain completion durability: %w", err)
}
```

- [ ] **Step 6: Run selected drain test**

Run: `just test ./internal/ingest/backfill -run TestRun_SelectedReposDrainQueuedCompletions -v`

Expected: PASS.

---

### Task 7: Add Completion Metrics

**Files:**
- Modify: `internal/ingest/backfill/metrics.go`
- Test: `internal/ingest/backfill/metrics_test.go`

- [ ] **Step 1: Write metrics test**

Add to `internal/ingest/backfill/metrics_test.go`:

```go
func TestMetricsCompletionBatching(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.incCompletionQueued()
	m.incCompletionCommitted(3)
	m.setCompletionQueueDepth(7)

	require.InDelta(t, 1.0, testutil.ToFloat64(m.CompletionQueued), 0)
	require.InDelta(t, 3.0, testutil.ToFloat64(m.CompletionCommitted), 0)
	require.InDelta(t, 7.0, testutil.ToFloat64(m.CompletionQueueDepth), 0)
}
```

- [ ] **Step 2: Run test and verify it fails**

Run: `just test ./internal/ingest/backfill -run TestMetricsCompletionBatching -v`

Expected: FAIL because metrics fields are undefined.

- [ ] **Step 3: Add metric fields and registration**

Add fields:

```go
	CompletionQueued    prometheus.Counter
	CompletionCommitted prometheus.Counter
	CompletionQueueDepth prometheus.Gauge
```

Initialize them:

```go
CompletionQueued: prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: metricsNamespace, Subsystem: metricsSubsystem,
	Name: "completion_queued_total",
	Help: "Number of repo completions queued in memory before block durability.",
}),
CompletionCommitted: prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: metricsNamespace, Subsystem: metricsSubsystem,
	Name: "completion_committed_total",
	Help: "Number of queued repo completions committed durably.",
}),
CompletionQueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: metricsNamespace, Subsystem: metricsSubsystem,
	Name: "completion_queue_depth",
	Help: "Number of queued repo completions not yet durably committed.",
}),
```

Register them in `reg.MustRegister`.

- [ ] **Step 4: Add nil-safe helpers**

```go
func (m *Metrics) incCompletionQueued() {
	if m != nil {
		m.CompletionQueued.Inc()
	}
}

func (m *Metrics) incCompletionCommitted(n int) {
	if m != nil {
		m.CompletionCommitted.Add(float64(n))
	}
}

func (m *Metrics) setCompletionQueueDepth(n int) {
	if m != nil {
		m.CompletionQueueDepth.Set(float64(n))
	}
}
```

- [ ] **Step 5: Update batcher to maintain depth**

In `QueueComplete`, after append:

```go
b.metrics.setCompletionQueueDepth(len(b.queued))
```

In `afterCommit`, after removing committed completions:

```go
b.metrics.setCompletionQueueDepth(len(b.queued))
```

- [ ] **Step 6: Run metrics test**

Run: `just test ./internal/ingest/backfill -run TestMetricsCompletionBatching -v`

Expected: PASS.

---

### Task 8: Backfill Integration and Restart Tests

**Files:**
- Modify: `internal/ingest/backfill/run_test.go`
- Modify: `internal/oracle/restart_harness_test.go` only if crashpoint expectations need adjustment.

- [ ] **Step 1: Run current focused backfill suite**

Run: `just test ./internal/ingest/backfill`

Expected: FAIL only in tests whose assertions still assume immediate `OnComplete` durability or per-page cursor writes.

- [ ] **Step 2: Update immediate-completion assertions to durable barriers**

For tests that call `Run` and then inspect complete rows, ensure the path has drained before assertion. Production `Run` should already drain; direct unit tests of `Store.OnComplete` without a batcher should keep existing immediate assertions.

Use this assertion shape for queued-completion tests:

```go
require.NoError(t, writer.DrainDurability(t.Context()))
got, err := backfillStore.Lookup(t.Context(), did)
require.NoError(t, err)
require.Equal(t, atmosbackfill.StateComplete, got.State)
```

- [ ] **Step 3: Add crash-before-drain test**

Add a test proving queued completion is not persisted before drain:

```go
func TestCompletionBatcherKeepsCompletionInvisibleBeforeDurability(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	bs := NewStore(st, nil)
	did := atmos.DID("did:plc:not-yet-durable")
	require.NoError(t, bs.OnDiscover(t.Context(), atmossync.ListReposEntry{DID: did, Active: true}))

	cb := NewCompletionBatcher(bs, nil)
	cb.RecordWatermark(did, 10, true)
	require.NoError(t, cb.QueueComplete(t.Context(), did, &repo.Commit{DID: string(did), Rev: "rev1"}))

	got, err := bs.Lookup(t.Context(), did)
	require.NoError(t, err)
	require.NotEqual(t, atmosbackfill.StateComplete, got.State)
}
```

- [ ] **Step 4: Run backfill package tests**

Run: `just test ./internal/ingest/backfill`

Expected: PASS.

- [ ] **Step 5: Run ingest package tests**

Run: `just test ./internal/ingest`

Expected: PASS.

---

### Task 9: Oracle and Full Verification

**Files:**
- No planned code edits.

- [ ] **Step 1: Run short oracle tests**

Run: `just test ./internal/oracle`

Expected: PASS.

- [ ] **Step 2: Run restart oracle**

Run: `just test-long ./internal/oracle -run TestOracle_Restart -v`

Expected: PASS.

- [ ] **Step 3: Run default test target**

Run: `just`

Expected: PASS.

- [ ] **Step 4: Inspect git diff**

Run: `git diff --stat && git diff --check`

Expected: stat lists only source/test changes for #62; `git diff --check` prints no whitespace errors.

- [ ] **Step 5: Post issue update**

Run:

```bash
gh issue comment 62 -b "Implemented batched backfill completion: repo completion metadata now queues in memory, commits at writer durability barriers, and listRepos cursor checkpoints wait for queued completions to drain. Verification: just test ./internal/ingest/backfill; just test ./internal/ingest; just test ./internal/oracle; just test-long ./internal/oracle -run TestOracle_Restart -v; just."
```

Expected: GitHub issue comment URL is printed.

---

## Self-Review Notes

- Spec coverage: tasks cover writer hook, async flush support, completion queueing, durable staging, cursor checkpoint movement, selected repos, metrics, crash/restart testing, and oracle verification.
- Cursor safety is explicit: cursor writes move from `OnPageComplete` to `OnBatchComplete` after `Writer.DrainDurability`.
- Completion visibility is explicit: queued completions can drive atmos progress before Pebble visibility, but `StatusComplete` is written only in a post-fsync synced metadata batch.
- No code under `docs/superpowers` should be added to git for this work; this plan is local and ignored by `/docs`.
