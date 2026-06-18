# Public Status Page Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a public `/status` HTML page on the `:8080` listener that shows aggregate process state (phase, backfill progress, segment counts and sizes, cursors, pebble keyspace counts), driven by a TTL-cached + singleflight-collapsed snapshot collector.

**Architecture:** Two new packages. `internal/status` gathers data from pebble + the segments directory tree into a typed `Snapshot`, with a 30s TTL cache and `singleflight` collapsing concurrent cold-cache requests. `internal/web` renders the snapshot as HTML via `html/template`. `internal/server` mounts the web handler at `GET /status` on the public mux. A small `internal/lifecycle` change persists a `phase/entered_at` timestamp atomically with each phase write so the status page can show "in this phase for X."

**Tech Stack:** Go (stdlib `html/template`, `embed`), `golang.org/x/sync/singleflight`, `cockroachdb/pebble` (already wired through `internal/store`), `prometheus/client_golang`, `stretchr/testify` for tests, `gotestsum` for the runner.

**Spec:** `docs/superpowers/specs/2026-05-25-public-status-page-design.md`

---

## Task overview

1. `lifecycle` — add `phase/entered_at`, change `WritePhase` to take a timestamp, add `ReadPhaseEnteredAt`.
2. Orchestrator — pass `time.Now()` into the three `WritePhase` call sites.
3. Shared helpers — export `ingest.ParseSegmentIndex` / `ingest.SegmentFilename`; add `store.GetUint64LE`; add `backfill.CountStatuses`; add `segment.QuickStats`.
4. `internal/status` package — types, collector, gather, cache + singleflight.
5. `internal/web` package — handler, template, formatting helpers.
6. `internal/server` — wire `StatusHandler` into the public mux.
7. `cmd/jetstream` — construct collector and handler in `runServe`.
8. End-to-end smoke test.

Run `just lint test` after each task. Commit at the end of each task.

---

## Task 1: Lifecycle — `phase/entered_at`

**Files:**
- Modify: `internal/lifecycle/phase.go`
- Modify: `internal/lifecycle/phase_test.go`

- [ ] **Step 1.1: Write failing tests for the new behavior**

Append to `internal/lifecycle/phase_test.go`:

```go
func TestPhaseEnteredAt_RoundTrip(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)

	want := time.Date(2026, 5, 25, 12, 0, 0, 123456000, time.UTC)
	require.NoError(t, WritePhase(st, PhaseBootstrap, want))

	got, err := ReadPhaseEnteredAt(st)
	require.NoError(t, err)
	require.True(t, got.Equal(want), "got %s, want %s", got, want)
}

func TestReadPhaseEnteredAt_Empty(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	got, err := ReadPhaseEnteredAt(st)
	require.NoError(t, err)
	require.True(t, got.IsZero())
}

func TestWritePhase_AtomicWithEnteredAt(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)

	want := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	require.NoError(t, WritePhase(st, PhaseMerging, want))

	gotPhase, err := ReadPhase(st)
	require.NoError(t, err)
	require.Equal(t, PhaseMerging, gotPhase)

	gotAt, err := ReadPhaseEnteredAt(st)
	require.NoError(t, err)
	require.True(t, gotAt.Equal(want))
}
```

Add the import for `time` to that test file if it's not already there.

Update existing test `TestPhase_RoundTrip` to pass a timestamp:

```go
func TestPhase_RoundTrip(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)

	now := time.Date(2026, 5, 25, 0, 0, 0, 0, time.UTC)
	for _, p := range []Phase{PhaseBootstrap, PhaseMerging, PhaseSteadyState} {
		require.NoError(t, WritePhase(st, p, now))
		got, err := ReadPhase(st)
		require.NoError(t, err)
		require.Equal(t, p, got)
	}
}
```

Update `TestWritePhase_RejectsUnknown` to pass a timestamp:

```go
func TestWritePhase_RejectsUnknown(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)

	err := WritePhase(st, Phase("banana"), time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "banana")
}
```

- [ ] **Step 1.2: Run tests to confirm they fail to compile**

```
just test ./internal/lifecycle
```

Expected: compile error — `WritePhase` takes 2 args, called with 3; `ReadPhaseEnteredAt` undefined.

- [ ] **Step 1.3: Implement the new lifecycle API**

Replace the contents of `internal/lifecycle/phase.go` with:

```go
package lifecycle

import (
	"errors"
	"fmt"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/cockroachdb/pebble"
)

// Phase names a single jetstream-process lifecycle state.
type Phase string

const (
	PhaseBootstrap   Phase = "bootstrap"
	PhaseMerging     Phase = "merging"
	PhaseSteadyState Phase = "steady_state"
)

const (
	phaseKey          = "phase"
	phaseEnteredAtKey = "phase/entered_at"
)

// ReadPhase returns the persisted phase. Empty on a fresh data dir.
// An unknown value crashes the read rather than silently mapping to a
// default.
func ReadPhase(s *store.Store) (Phase, error) {
	val, closer, err := s.Get([]byte(phaseKey))
	if errors.Is(err, store.ErrNotFound) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("lifecycle: read phase: %w", err)
	}
	defer func() { _ = closer.Close() }()

	p := Phase(string(val))
	if !p.valid() {
		return "", fmt.Errorf("lifecycle: unrecognized phase value %q in pebble", string(val))
	}
	return p, nil
}

// ReadPhaseEnteredAt returns the timestamp at which the current phase
// was entered. Zero time + nil error means the key isn't present (fresh
// data dir, or a process that pre-dates this field).
func ReadPhaseEnteredAt(s *store.Store) (time.Time, error) {
	val, closer, err := s.Get([]byte(phaseEnteredAtKey))
	if errors.Is(err, store.ErrNotFound) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("lifecycle: read phase/entered_at: %w", err)
	}
	defer func() { _ = closer.Close() }()

	t, err := time.Parse(time.RFC3339Nano, string(val))
	if err != nil {
		return time.Time{}, fmt.Errorf("lifecycle: decode phase/entered_at %q: %w", string(val), err)
	}
	return t.UTC(), nil
}

// WritePhase atomically persists p and enteredAt with pebble.Sync.
// Both keys land together via a single batch commit so a crash cannot
// leave a phase value paired with the wrong timestamp.
func WritePhase(s *store.Store, p Phase, enteredAt time.Time) error {
	if !p.valid() {
		return fmt.Errorf("lifecycle: refuse to write unrecognized phase %q", string(p))
	}
	b := s.NewBatch()
	defer func() { _ = b.Close() }()

	if err := b.Set([]byte(phaseKey), []byte(p), nil); err != nil {
		return fmt.Errorf("lifecycle: stage phase: %w", err)
	}
	tsBytes := []byte(enteredAt.UTC().Format(time.RFC3339Nano))
	if err := b.Set([]byte(phaseEnteredAtKey), tsBytes, nil); err != nil {
		return fmt.Errorf("lifecycle: stage phase/entered_at: %w", err)
	}
	if err := s.Commit(b, &pebble.WriteOptions{Sync: true}); err != nil {
		return fmt.Errorf("lifecycle: commit phase write: %w", err)
	}
	return nil
}

func (p Phase) valid() bool {
	switch p {
	case PhaseBootstrap, PhaseMerging, PhaseSteadyState:
		return true
	default:
		return false
	}
}
```

Note: `internal/store` exports `SyncWrites` as the canonical sync option; if it does, prefer `s.Commit(b, store.SyncWrites)`. Verify with `grep -n SyncWrites internal/store/`.

- [ ] **Step 1.4: Run tests to confirm they pass**

```
just test ./internal/lifecycle
```

Expected: PASS.

- [ ] **Step 1.5: Commit**

```
git add internal/lifecycle/
git commit -m "lifecycle: persist phase/entered_at atomically with phase"
```

---

## Task 2: Orchestrator — pass timestamps to WritePhase

**Files:**
- Modify: `internal/ingest/orchestrator/orchestrator.go`
- Modify: `internal/ingest/orchestrator/states.go`
- Modify any orchestrator tests that fake `WritePhase` (none expected; verify with grep).

- [ ] **Step 2.1: Run tests first to capture the compile failures from Task 1's signature change**

```
just test ./internal/ingest/orchestrator
```

Expected: compile errors in `orchestrator.go` and `states.go` — `WritePhase` takes 3 args, called with 2.

- [ ] **Step 2.2: Update `orchestrator.go` initial-write site**

In `internal/ingest/orchestrator/orchestrator.go`, the `Run` method has:

```go
if phase == "" {
    phase = lifecycle.PhaseBootstrap
    if err := lifecycle.WritePhase(o.cfg.Store, phase); err != nil {
        return fmt.Errorf("orchestrator: write initial phase: %w", err)
    }
}
```

Change to:

```go
if phase == "" {
    phase = lifecycle.PhaseBootstrap
    if err := lifecycle.WritePhase(o.cfg.Store, phase, time.Now().UTC()); err != nil {
        return fmt.Errorf("orchestrator: write initial phase: %w", err)
    }
}
```

Add `"time"` to the imports if it's not already there.

- [ ] **Step 2.3: Update `states.go` transition sites**

Replace the body of `writeMergingPhase`:

```go
func (o *Orchestrator) writeMergingPhase() error {
    start := time.Now()
    if err := lifecycle.WritePhase(o.cfg.Store, lifecycle.PhaseMerging, start.UTC()); err != nil {
        return fmt.Errorf("orchestrator: write phase=merging: %w", err)
    }
    o.cfg.Metrics.observeState("write_phase_merging", time.Since(start).Seconds())
    o.cfg.Metrics.incTransition(lifecycle.PhaseBootstrap, lifecycle.PhaseMerging)
    o.cfg.Metrics.setPhase(PhaseGaugeMerging)
    return nil
}
```

Replace the body of `writeSteadyStatePhase`:

```go
func (o *Orchestrator) writeSteadyStatePhase() error {
    start := time.Now()
    if err := lifecycle.WritePhase(o.cfg.Store, lifecycle.PhaseSteadyState, start.UTC()); err != nil {
        return fmt.Errorf("orchestrator: write phase=steady_state: %w", err)
    }
    o.cfg.Metrics.observeState("write_phase_steady", time.Since(start).Seconds())
    o.cfg.Metrics.incTransition(lifecycle.PhaseMerging, lifecycle.PhaseSteadyState)
    o.cfg.Metrics.setPhase(PhaseGaugeSteadyState)
    return nil
}
```

- [ ] **Step 2.4: Run orchestrator tests**

```
just test ./internal/ingest/orchestrator
```

Expected: PASS.

- [ ] **Step 2.5: Run the whole test suite to catch any other callers**

```
just test
```

Expected: PASS. If a test fails, it's a caller of `WritePhase` we missed; update it to pass `time.Now()` (or a fixed time for tests).

- [ ] **Step 2.6: Commit**

```
git add internal/
git commit -m "orchestrator: timestamp phase transitions"
```

---

## Task 3a: Export `ingest.ParseSegmentIndex` / `ingest.SegmentFilename`

**Files:**
- Modify: `internal/ingest/filename.go`
- Modify: `internal/ingest/filename_test.go`
- Modify: `internal/ingest/writer.go` (callers of the renamed helpers)

- [ ] **Step 3a.1: Rename and re-export the helpers**

In `internal/ingest/filename.go`, replace the file contents with:

```go
package ingest

import (
	"strconv"
	"strings"
)

const (
	segmentFilenamePrefix = "seg_"
	segmentFilenameSuffix = ".jss"
	segmentIndexDigits    = 10
)

// SegmentFilename formats idx as the on-disk segment filename
// "seg_<10-digit base36>.jss". Names sort lexicographically in
// creation order so a directory scan reproduces the segment manifest.
func SegmentFilename(idx uint64) string {
	enc := strconv.FormatUint(idx, 36)
	if len(enc) < segmentIndexDigits {
		enc = strings.Repeat("0", segmentIndexDigits-len(enc)) + enc
	}
	return segmentFilenamePrefix + enc + segmentFilenameSuffix
}

// ParseSegmentIndex returns the index encoded in name and whether the
// name has the expected segment shape.
func ParseSegmentIndex(name string) (uint64, bool) {
	if !strings.HasPrefix(name, segmentFilenamePrefix) {
		return 0, false
	}
	if !strings.HasSuffix(name, segmentFilenameSuffix) {
		return 0, false
	}
	mid := name[len(segmentFilenamePrefix) : len(name)-len(segmentFilenameSuffix)]
	if len(mid) != segmentIndexDigits {
		return 0, false
	}
	idx, err := strconv.ParseUint(mid, 36, 64)
	if err != nil {
		return 0, false
	}
	return idx, true
}
```

- [ ] **Step 3a.2: Update internal callers in `writer.go`**

In `internal/ingest/writer.go` replace every occurrence of `parseSegmentIndex(` with `ParseSegmentIndex(`, and every occurrence of `segmentFilename(` with `SegmentFilename(`. Verify with:

```
grep -n "parseSegmentIndex\|segmentFilename(" internal/ingest/
```

Expected: no matches in non-test code.

- [ ] **Step 3a.3: Update `filename_test.go` to use the exported names**

Replace any `parseSegmentIndex` / `segmentFilename` references in `internal/ingest/filename_test.go` with `ParseSegmentIndex` / `SegmentFilename`.

- [ ] **Step 3a.4: Run tests**

```
just test ./internal/ingest/...
```

Expected: PASS.

- [ ] **Step 3a.5: Commit**

```
git add internal/ingest/
git commit -m "ingest: export ParseSegmentIndex and SegmentFilename"
```

---

## Task 3b: Add `store.GetUint64LE`

**Files:**
- Create: `internal/store/encoding.go`
- Create: `internal/store/encoding_test.go`
- Modify: `internal/ingest/writer.go` (use the new helper)

- [ ] **Step 3b.1: Write failing test**

Create `internal/store/encoding_test.go`:

```go
package store_test

import (
	"encoding/binary"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestGetUint64LE_Missing(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	got, ok, err := store.GetUint64LE(st, "k/missing")
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, uint64(0), got)
}

func TestGetUint64LE_RoundTrip(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)

	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], 0xDEADBEEFCAFEBABE)
	require.NoError(t, st.Set([]byte("k/v"), buf[:], store.SyncWrites))

	got, ok, err := store.GetUint64LE(st, "k/v")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(0xDEADBEEFCAFEBABE), got)
}

func TestGetUint64LE_WrongLength(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	require.NoError(t, st.Set([]byte("k/v"), []byte{0x01, 0x02}, store.SyncWrites))

	_, _, err := store.GetUint64LE(st, "k/v")
	require.Error(t, err)
}
```

- [ ] **Step 3b.2: Run test, expect failure**

```
just test ./internal/store
```

Expected: compile error — `store.GetUint64LE` undefined.

- [ ] **Step 3b.3: Implement**

Create `internal/store/encoding.go`:

```go
package store

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// GetUint64LE reads key as an 8-byte little-endian uint64. Returns
// (0, false, nil) when the key is absent. Returns an error if the
// stored value is not exactly 8 bytes.
//
// Centralized so every consumer that stores a uint64 counter under a
// pebble key — seq/next, live_segments/seq/next, future maintained
// counters — uses the same encoding.
func GetUint64LE(s *Store, key string) (uint64, bool, error) {
	val, closer, err := s.Get([]byte(key))
	if errors.Is(err, ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("store: get %s: %w", key, err)
	}
	defer func() { _ = closer.Close() }()

	if len(val) != 8 {
		return 0, false, fmt.Errorf("store: %s has wrong length %d (want 8)", key, len(val))
	}
	return binary.LittleEndian.Uint64(val), true, nil
}
```

- [ ] **Step 3b.4: Update ingest/writer.go to use it**

In `internal/ingest/writer.go`, replace the body of `loadNextSeq`:

```go
func loadNextSeq(st *store.Store, key string) (uint64, error) {
	v, _, err := store.GetUint64LE(st, key)
	return v, err
}
```

- [ ] **Step 3b.5: Run tests**

```
just test ./internal/store ./internal/ingest
```

Expected: PASS.

- [ ] **Step 3b.6: Commit**

```
git add internal/store/ internal/ingest/writer.go
git commit -m "store: add GetUint64LE helper"
```

---

## Task 3c: Add `backfill.CountStatuses`

**Files:**
- Create: `internal/ingest/backfill/counts.go`
- Create: `internal/ingest/backfill/counts_test.go`

- [ ] **Step 3c.1: Write failing test**

Create `internal/ingest/backfill/counts_test.go`:

```go
package backfill_test

import (
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/sync"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestCountStatuses_Empty(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	got, err := backfill.CountStatuses(st)
	require.NoError(t, err)
	require.Equal(t, backfill.Counts{}, got)
}

func TestCountStatuses_MixedStates(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	bs := backfill.NewStore(st, nil)

	// Three discovered.
	for i := range 3 {
		did := atmos.DID("did:plc:disc" + string(rune('a'+i)))
		require.NoError(t, bs.OnDiscover(t.Context(), sync.ListReposEntry{DID: did, Active: true}))
	}
	// Two completed.
	for i := range 2 {
		did := atmos.DID("did:plc:done" + string(rune('a'+i)))
		require.NoError(t, bs.OnDiscover(t.Context(), sync.ListReposEntry{DID: did, Active: true}))
		require.NoError(t, bs.OnComplete(t.Context(), did, &fakeCommit{rev: "1"}))
		_ = time.Now() // silence unused if go gets pedantic about empty body
	}
	// One failed.
	did := atmos.DID("did:plc:fail")
	require.NoError(t, bs.OnDiscover(t.Context(), sync.ListReposEntry{DID: did, Active: true}))
	require.NoError(t, bs.OnFail(t.Context(), did, errFake, 1))

	got, err := backfill.CountStatuses(st)
	require.NoError(t, err)
	require.Equal(t, backfill.Counts{
		Total:      6,
		Discovered: 3,
		Complete:   2,
		Failed:     1,
	}, got)
}
```

Add a helper file `internal/ingest/backfill/test_helpers_test.go` with:

```go
package backfill_test

import (
	"errors"

	"github.com/jcalabro/atmos/repo"
)

var errFake = errors.New("fake fail")

type fakeCommit struct{ rev string }

// We only need .Rev for OnComplete; cast through repo.Commit.
var _ = (*repo.Commit)(nil)
```

If `repo.Commit` is a struct (which it is in atmos), instead use `&repo.Commit{Rev: "1"}` directly inline and drop the helper file. Verify shape with `grep -n "type Commit" $(go env GOPATH)/pkg/mod/github.com/jcalabro/atmos*/repo/`. Replace the test's `&fakeCommit{rev: "1"}` with `&repo.Commit{Rev: "1"}` in that case.

- [ ] **Step 3c.2: Run test, expect failure**

```
just test ./internal/ingest/backfill
```

Expected: compile error — `backfill.CountStatuses` and `backfill.Counts` undefined.

- [ ] **Step 3c.3: Implement**

Create `internal/ingest/backfill/counts.go`:

```go
package backfill

import (
	"fmt"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/cockroachdb/pebble"
)

// Counts is the per-status row count produced by CountStatuses.
type Counts struct {
	Total      uint64
	Discovered uint64
	Complete   uint64
	Failed     uint64
}

// CountStatuses range-scans the repo/ keyspace and tallies rows by
// Backfill.Status. Total is the sum of the three buckets plus any
// rows whose status doesn't decode to a recognized value (those are
// counted under Total but not under any bucket; surfacing the
// mismatch via Total != sum is intentional).
//
// At full network scale this scans tens of millions of keys; cost
// scales linearly with row count. Use behind a TTL cache.
func CountStatuses(s *store.Store) (Counts, error) {
	var c Counts

	prefix := []byte(repoKeyPrefix)
	upper := upperBound(prefix)

	it, err := s.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return Counts{}, fmt.Errorf("backfill: open iter: %w", err)
	}
	defer func() { _ = it.Close() }()

	for it.First(); it.Valid(); it.Next() {
		c.Total++
		val, err := it.ValueAndErr()
		if err != nil {
			return Counts{}, fmt.Errorf("backfill: read value: %w", err)
		}
		rs, err := decodeRepoStatus(val)
		if err != nil {
			// Don't fail the whole count for one bad row — the row is
			// counted in Total. Log via the metric? For now, skip.
			continue
		}
		switch rs.Backfill.Status {
		case StatusNotStarted:
			c.Discovered++
		case StatusComplete:
			c.Complete++
		case StatusFailed:
			c.Failed++
		}
	}
	if err := it.Error(); err != nil {
		return Counts{}, fmt.Errorf("backfill: iter error: %w", err)
	}
	return c, nil
}

// upperBound returns the lexicographically-next byte slice after
// prefix, suitable for use as IterOptions.UpperBound. For "repo/"
// that's "repo0" (the byte after '/' is '0'). Returns nil for an
// all-0xFF prefix (caller should fall back to no UpperBound, but
// this is a theoretical case for jetstream's prefixes).
func upperBound(prefix []byte) []byte {
	out := make([]byte, len(prefix))
	copy(out, prefix)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] < 0xFF {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}
```

- [ ] **Step 3c.4: Run test**

```
just test ./internal/ingest/backfill
```

Expected: PASS.

- [ ] **Step 3c.5: Commit**

```
git add internal/ingest/backfill/
git commit -m "backfill: add CountStatuses range-scan helper"
```

---

## Task 3d: Add `segment.QuickStats`

**Files:**
- Create: `segment/quickstats.go`
- Create: `segment/quickstats_test.go`

QuickStats reads only the 256-byte fixed header to decide sealed/active and (for sealed) to find the block index, then reads the block index to sum compressed and uncompressed sizes. No decompression. For active files we fall back to walking framed blocks via `walkActiveFrames` (already implemented for `Inspect`); for QuickStats we accept the higher cost on the active file because there's at most one of those per tree.

- [ ] **Step 3d.1: Write failing test**

Create `segment/quickstats_test.go`:

```go
package segment_test

import (
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/stretchr/testify/require"
)

func TestQuickStats_SealedAndActive(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sealedPath := filepath.Join(dir, "sealed.jss")
	activePath := filepath.Join(dir, "active.jss")

	// Build a sealed file with a couple of events.
	w, err := segment.New(segment.Config{
		Path:              sealedPath,
		MaxEventsPerBlock: 2,
	})
	require.NoError(t, err)
	for i := range 4 {
		_, err := w.Append(segment.Event{
			Seq:        uint64(i + 1),
			DID:        "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
			IndexedAt:  1700000000_000_000,
			Collection: "app.bsky.feed.post",
			Payload:    []byte("hello"),
		})
		require.NoError(t, err)
	}
	_, err = w.Seal()
	require.NoError(t, err)

	// And an active (unsealed) file.
	w2, err := segment.New(segment.Config{
		Path:              activePath,
		MaxEventsPerBlock: 2,
	})
	require.NoError(t, err)
	for i := range 2 {
		_, err := w2.Append(segment.Event{
			Seq:        uint64(i + 1),
			DID:        "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
			IndexedAt:  1700000000_000_000,
			Collection: "app.bsky.feed.post",
			Payload:    []byte("hi"),
		})
		require.NoError(t, err)
	}
	require.NoError(t, w2.Flush())
	require.NoError(t, w2.Close())

	sealed, err := segment.QuickStats(sealedPath)
	require.NoError(t, err)
	require.True(t, sealed.Sealed)
	require.Greater(t, sealed.FileSize, int64(0))
	require.Greater(t, sealed.CompressedBytes, int64(0))
	require.GreaterOrEqual(t, sealed.UncompressedBytes, sealed.CompressedBytes)

	active, err := segment.QuickStats(activePath)
	require.NoError(t, err)
	require.False(t, active.Sealed)
	require.Greater(t, active.FileSize, int64(0))
	require.Greater(t, active.CompressedBytes, int64(0))
	require.GreaterOrEqual(t, active.UncompressedBytes, active.CompressedBytes)
}
```

If `segment.Event` / `segment.New` / `segment.Config` field names differ from this draft, mirror what `segment/writer_test.go` does. The test is structural; adapt names as needed without changing behavior.

- [ ] **Step 3d.2: Run, expect failure**

```
just test ./segment -run TestQuickStats
```

Expected: compile error — `segment.QuickStats` undefined.

- [ ] **Step 3d.3: Implement**

Create `segment/quickstats.go`:

```go
package segment

// QuickStats is the cheap aggregate-size view of a segment file.
// Used by the status page to sum compressed and uncompressed bytes
// across an entire segment tree without decompressing blocks.
type QuickStats struct {
	Path              string
	FileSize          int64
	Sealed            bool
	CompressedBytes   int64
	UncompressedBytes int64
}

// QuickStats reads enough of the file at path to populate a
// QuickStats: the 256-byte header (to decide sealed/active and find
// the block index), and either the block index (sealed) or the
// framed-block walk (active).
//
// Errors only on I/O failure or magic mismatch. Active files with
// torn tails return whatever was readable plus a nil error — partial
// active-file content is the normal case during ingest. Sealed files
// with corrupt block indexes do return an error, since a sealed-file
// invariant violation indicates a bug.
func QuickStats(path string) (QuickStats, error) {
	ins, err := Inspect(path)
	if err != nil {
		return QuickStats{}, err
	}
	var comp, uncomp int64
	for _, b := range ins.Blocks {
		comp += int64(b.CompressedSize)
		uncomp += int64(b.UncompressedSize)
	}
	return QuickStats{
		Path:              path,
		FileSize:          ins.FileSize,
		Sealed:            ins.Sealed,
		CompressedBytes:   comp,
		UncompressedBytes: uncomp,
	}, nil
}
```

Note: this is a thin wrapper over `Inspect`. We chose this implementation
over a hand-rolled minimal reader because:
- `Inspect` already exists, is tested, and handles both sealed and
  active files correctly.
- The cost is dominated by the block-index read (sealed) or frame walk
  (active). The status collector only opens the latest segment per
  tree for `LatestSegment`, and uses `QuickStats` for size aggregation
  across the rest. For sealed files (the overwhelming majority of any
  tree) `Inspect` does only a header parse + block-index decode — no
  decompression.

If profiling later shows this is hot, replace with a minimal direct
reader that skips the per-block-collections decoding.

- [ ] **Step 3d.4: Run test**

```
just test ./segment -run TestQuickStats
```

Expected: PASS.

- [ ] **Step 3d.5: Commit**

```
git add segment/quickstats.go segment/quickstats_test.go
git commit -m "segment: add QuickStats for cheap aggregate size reads"
```

---

## Task 4: `internal/status` package — types

**Files:**
- Create: `internal/status/doc.go`
- Create: `internal/status/snapshot.go`

- [ ] **Step 4.1: Create the package doc**

`internal/status/doc.go`:

```go
// Package status gathers the data shown by the public /status page.
//
// The package exposes one type per logical group of stats (see
// snapshot.go) and a Collector that builds a Snapshot on demand,
// caches it for a configurable TTL, and uses singleflight to collapse
// concurrent cold-cache requests into a single backend call.
//
// status is rendering-agnostic: the internal/web package consumes
// Snapshot via a Snapshotter interface. A future JSON or Prometheus
// surface would consume the same Snapshot.
//
// All cost-bearing reads (pebble range scans, segment file walks)
// happen in build paths, never in the cache-hit path. A warm cache
// answers in microseconds.
package status
```

- [ ] **Step 4.2: Create the snapshot types**

`internal/status/snapshot.go`:

```go
package status

import (
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
)

// Snapshot is the rendering-agnostic view of jetstream's state at a
// single moment. Treated as immutable after construction; consumers
// share a *Snapshot pointer without locks.
type Snapshot struct {
	GeneratedAt time.Time
	Process     ProcessInfo
	Phase       PhaseInfo
	Backfill    BackfillStats
	Live        LiveStats
	Segments    SegmentTreeStats
	LiveSegs    SegmentTreeStats
	Pebble      PebbleStats
}

// ProcessInfo carries the per-process build + uptime context.
type ProcessInfo struct {
	Version   string
	Commit    string
	BuiltAt   string
	StartedAt time.Time
	Uptime    time.Duration
	GoVersion string
}

// PhaseInfo holds the current persisted phase plus its transition
// timestamp. PhaseEnteredAt is zero if no phase/entered_at key exists
// (a process that pre-dates the field, or a fresh data dir).
type PhaseInfo struct {
	Phase          lifecycle.Phase
	PhaseEnteredAt time.Time
}

// BackfillStats summarizes the repo/ keyspace.
type BackfillStats struct {
	TotalDIDs       uint64
	Discovered      uint64
	Complete        uint64
	Failed          uint64
	PercentComplete float64
	ListReposCursor string
}

// LiveStats summarizes the upstream cursor and seq counters.
type LiveStats struct {
	UpstreamCursor int64
	NextSeq        uint64
	BootstrapSeq   uint64
	EventsAppended uint64
}

// SegmentTreeStats summarizes a single segment tree (segments/ or
// backfill/live_segments/). LatestSegment is nil if the directory is
// empty or the latest file couldn't be inspected.
type SegmentTreeStats struct {
	Dir               string
	SealedCount       int
	ActiveCount       int
	CompressedBytes   int64
	UncompressedBytes int64
	OldestMTime       time.Time
	NewestMTime       time.Time
	LatestSegment     *SegmentSummary
}

// SegmentSummary mirrors segment.Inspection's user-facing fields,
// converted to time.Time so renderers don't have to know about
// unix-micros.
type SegmentSummary struct {
	Index           uint64
	Sealed          bool
	EventCount      uint64
	UniqueDIDCount  uint32
	BlockCount      uint32
	CollectionCount int
	MinSeq          uint64
	MaxSeq          uint64
	MinIndexedAt    time.Time
	MaxIndexedAt    time.Time
	SizeBytes       int64
}

// PebbleStats summarizes meta.pebble/ on disk plus per-prefix key
// counts.
type PebbleStats struct {
	DiskBytes      int64
	KeyspaceCounts map[string]uint64
}
```

- [ ] **Step 4.3: Verify it compiles**

```
go build ./internal/status
```

Expected: success (no symbols used yet, but the file is well-formed).

- [ ] **Step 4.4: Commit**

```
git add internal/status/
git commit -m "status: snapshot types"
```

---

## Task 5: `internal/status` — gather functions

**Files:**
- Create: `internal/status/collect.go`
- Create: `internal/status/collect_test.go`

We start with a single test that hammers the empty-store path through every gather function, then add fixture-driven tests as we implement each gather.

- [ ] **Step 5.1: Test — empty store, empty data dir**

`internal/status/collect_test.go`:

```go
package status_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/status"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestCollect_FreshDataDir(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	c, err := status.New(status.Options{
		Store:   st,
		DataDir: dataDir,
		TTL:     30 * time.Second,
		Now:     func() time.Time { return time.Date(2026, 5, 25, 0, 0, 0, 0, time.UTC) },
	})
	require.NoError(t, err)

	snap, err := c.Snapshot(context.Background())
	require.NoError(t, err)
	require.NotNil(t, snap)

	// Empty data dir: no segments, no phase, no cursors.
	require.Equal(t, "", string(snap.Phase.Phase))
	require.True(t, snap.Phase.PhaseEnteredAt.IsZero())
	require.Equal(t, status.BackfillStats{}, snap.Backfill)
	require.Equal(t, status.LiveStats{}, snap.Live)
	require.Equal(t, 0, snap.Segments.SealedCount+snap.Segments.ActiveCount)
	require.Equal(t, 0, snap.LiveSegs.SealedCount+snap.LiveSegs.ActiveCount)
	require.Equal(t, filepath.Join(dataDir, "segments"), snap.Segments.Dir)
	require.Equal(t, filepath.Join(dataDir, "backfill", "live_segments"), snap.LiveSegs.Dir)
}
```

- [ ] **Step 5.2: Run, expect failure**

```
just test ./internal/status
```

Expected: compile error — `status.New`, `status.Options`, `Collector.Snapshot` undefined.

- [ ] **Step 5.3: Implement gather functions**

Create `internal/status/collect.go`:

```go
package status

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream-v2/internal/ingest/live"
	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/internal/version"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/cockroachdb/pebble"
)

// keyspacePrefixes lists the pebble prefixes the status page exposes
// in PebbleStats.KeyspaceCounts. sync/identity/ is intentionally
// excluded from the public surface.
var keyspacePrefixes = []string{
	"repo/",
	"sync/chain/",
	"sync/host/",
	"relay/",
}

func collectProcess(now time.Time, startedAt time.Time) ProcessInfo {
	info := version.Get()
	return ProcessInfo{
		Version:   info.Version,
		Commit:    info.Commit,
		BuiltAt:   info.Date,
		StartedAt: startedAt,
		Uptime:    now.Sub(startedAt),
		GoVersion: runtime.Version(),
	}
}

func collectPhase(s *store.Store) (PhaseInfo, error) {
	p, err := lifecycle.ReadPhase(s)
	if err != nil {
		return PhaseInfo{}, err
	}
	at, err := lifecycle.ReadPhaseEnteredAt(s)
	if err != nil {
		return PhaseInfo{}, err
	}
	return PhaseInfo{Phase: p, PhaseEnteredAt: at}, nil
}

func collectLive(s *store.Store) (LiveStats, error) {
	cur, err := live.LoadUpstreamCursor(s, live.CursorKey)
	if err != nil {
		return LiveStats{}, err
	}
	nextSeq, _, err := store.GetUint64LE(s, live.SteadySeqKey)
	if err != nil {
		return LiveStats{}, err
	}
	bootSeq, _, err := store.GetUint64LE(s, live.BootstrapSeqKey)
	if err != nil {
		return LiveStats{}, err
	}
	return LiveStats{
		UpstreamCursor: cur,
		NextSeq:        nextSeq,
		BootstrapSeq:   bootSeq,
		EventsAppended: nextSeq,
	}, nil
}

func collectBackfill(s *store.Store) (BackfillStats, error) {
	counts, err := backfill.CountStatuses(s)
	if err != nil {
		return BackfillStats{}, err
	}
	cursor, err := backfill.LoadListReposCursor(s)
	if err != nil {
		return BackfillStats{}, err
	}
	pct := 0.0
	if counts.Total > 0 {
		pct = float64(counts.Complete) / float64(counts.Total) * 100
	}
	return BackfillStats{
		TotalDIDs:       counts.Total,
		Discovered:      counts.Discovered,
		Complete:        counts.Complete,
		Failed:          counts.Failed,
		PercentComplete: pct,
		ListReposCursor: cursor,
	}, nil
}

func collectSegmentTree(dir string) (SegmentTreeStats, error) {
	stats := SegmentTreeStats{Dir: dir}

	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return stats, nil
		}
		return stats, fmt.Errorf("status: readdir %s: %w", dir, err)
	}

	type segFile struct {
		idx   uint64
		path  string
		info  os.FileInfo
	}
	var files []segFile
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		idx, ok := ingest.ParseSegmentIndex(e.Name())
		if !ok {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			return stats, fmt.Errorf("status: stat %s: %w", e.Name(), err)
		}
		files = append(files, segFile{idx: idx, path: filepath.Join(dir, e.Name()), info: fi})
	}
	if len(files) == 0 {
		return stats, nil
	}
	sort.Slice(files, func(i, j int) bool { return files[i].idx < files[j].idx })

	stats.OldestMTime = files[0].info.ModTime()
	stats.NewestMTime = files[0].info.ModTime()

	for i, f := range files {
		stats.CompressedBytes += f.info.Size()
		mt := f.info.ModTime()
		if mt.Before(stats.OldestMTime) {
			stats.OldestMTime = mt
		}
		if mt.After(stats.NewestMTime) {
			stats.NewestMTime = mt
		}

		qs, err := segment.QuickStats(f.path)
		if err != nil {
			// Latest file may be torn during rotation; tolerate it.
			if i == len(files)-1 {
				continue
			}
			return stats, fmt.Errorf("status: quickstats %s: %w", f.path, err)
		}
		stats.UncompressedBytes += qs.UncompressedBytes

		if qs.Sealed {
			stats.SealedCount++
		} else {
			stats.ActiveCount++
		}
	}

	// Latest-segment summary (cheap full Inspect on one file).
	latest := files[len(files)-1]
	if summary, err := buildSegmentSummary(latest.path, latest.idx, latest.info.Size()); err == nil {
		stats.LatestSegment = summary
	}

	return stats, nil
}

func buildSegmentSummary(path string, idx uint64, size int64) (*SegmentSummary, error) {
	ins, err := segment.Inspect(path)
	if err != nil {
		return nil, err
	}
	return &SegmentSummary{
		Index:           idx,
		Sealed:          ins.Sealed,
		EventCount:      ins.TotalEvents,
		UniqueDIDCount:  ins.UniqueDIDCount,
		BlockCount:      uint32(len(ins.Blocks)),
		CollectionCount: len(ins.Collections),
		MinSeq:          ins.MinSeq,
		MaxSeq:          ins.MaxSeq,
		MinIndexedAt:    microsToTime(ins.MinIndexedAt),
		MaxIndexedAt:    microsToTime(ins.MaxIndexedAt),
		SizeBytes:       size,
	}, nil
}

func microsToTime(us int64) time.Time {
	if us == 0 {
		return time.Time{}
	}
	return time.UnixMicro(us).UTC()
}

func collectPebble(s *store.Store, dataDir string) (PebbleStats, error) {
	stats := PebbleStats{KeyspaceCounts: make(map[string]uint64, len(keyspacePrefixes))}

	// On-disk size of meta.pebble/.
	pebbleDir := filepath.Join(dataDir, store.PebbleSubdir)
	if err := filepath.WalkDir(pebbleDir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return fs.SkipAll
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		stats.DiskBytes += fi.Size()
		return nil
	}); err != nil {
		return PebbleStats{}, fmt.Errorf("status: walk %s: %w", pebbleDir, err)
	}

	// Per-prefix key counts.
	for _, prefix := range keyspacePrefixes {
		c, err := countKeysWithPrefix(s, prefix)
		if err != nil {
			return PebbleStats{}, err
		}
		stats.KeyspaceCounts[prefix] = c
	}
	return stats, nil
}

func countKeysWithPrefix(s *store.Store, prefix string) (uint64, error) {
	lower := []byte(prefix)
	upper := nextLexBound(lower)

	it, err := s.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
		KeyTypes:   pebble.IterKeyTypePointsOnly,
	})
	if err != nil {
		return 0, fmt.Errorf("status: open iter %q: %w", prefix, err)
	}
	defer func() { _ = it.Close() }()

	var n uint64
	for it.First(); it.Valid(); it.Next() {
		n++
	}
	if err := it.Error(); err != nil {
		return 0, fmt.Errorf("status: iter %q: %w", prefix, err)
	}
	return n, nil
}

func nextLexBound(prefix []byte) []byte {
	out := make([]byte, len(prefix))
	copy(out, prefix)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] < 0xFF {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}

// build composes the gather functions into a Snapshot. ctx is checked
// between sections so a client-disconnect during a cold-cache miss
// doesn't waste the rest of the gather.
func build(ctx context.Context, opts Options, startedAt time.Time) (*Snapshot, error) {
	now := opts.Now()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	phase, err := collectPhase(opts.Store)
	if err != nil {
		return nil, err
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	live, err := collectLive(opts.Store)
	if err != nil {
		return nil, err
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	bf, err := collectBackfill(opts.Store)
	if err != nil {
		return nil, err
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	segs, err := collectSegmentTree(filepath.Join(opts.DataDir, "segments"))
	if err != nil {
		return nil, err
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	livesegs, err := collectSegmentTree(filepath.Join(opts.DataDir, "backfill", "live_segments"))
	if err != nil {
		return nil, err
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pdb, err := collectPebble(opts.Store, opts.DataDir)
	if err != nil {
		return nil, err
	}

	return &Snapshot{
		GeneratedAt: now,
		Process:     collectProcess(now, startedAt),
		Phase:       phase,
		Backfill:    bf,
		Live:        live,
		Segments:    segs,
		LiveSegs:    livesegs,
		Pebble:      pdb,
	}, nil
}
```

- [ ] **Step 5.4: Run — still failing because `status.New` not yet defined**

```
just test ./internal/status
```

Expected: compile error — `status.New` undefined. The next task adds it.

---

## Task 6: `internal/status` — Collector with cache + singleflight

**Files:**
- Create: `internal/status/collector.go`
- Modify: `internal/status/collect_test.go` (add cache + concurrency tests)

- [ ] **Step 6.1: Implement Collector**

`internal/status/collector.go`:

```go
package status

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"golang.org/x/sync/singleflight"
)

// Options configures a Collector. All fields except Now are required;
// Now defaults to time.Now.
type Options struct {
	Store   *store.Store
	DataDir string

	// TTL is the cache lifetime for successful snapshots. Default 30s.
	TTL time.Duration
	// NegTTL is the cache lifetime for errored snapshots. Default 1s.
	NegTTL time.Duration

	// Now overrides the wall clock; tests pin it for determinism.
	Now func() time.Time
}

const (
	defaultTTL    = 30 * time.Second
	defaultNegTTL = 1 * time.Second
)

// Collector builds Snapshots on demand and caches them.
type Collector struct {
	opts      Options
	startedAt time.Time

	mu     sync.Mutex
	cached *cacheEntry

	sf singleflight.Group
}

type cacheEntry struct {
	snap      *Snapshot
	err       error
	expiresAt time.Time
}

// New validates opts and returns a Collector ready for use.
func New(opts Options) (*Collector, error) {
	if opts.Store == nil {
		return nil, fmt.Errorf("status: Options.Store is required")
	}
	if opts.DataDir == "" {
		return nil, fmt.Errorf("status: Options.DataDir is required")
	}
	if opts.TTL <= 0 {
		opts.TTL = defaultTTL
	}
	if opts.NegTTL <= 0 {
		opts.NegTTL = defaultNegTTL
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Collector{
		opts:      opts,
		startedAt: opts.Now(),
	}, nil
}

// TTL returns the configured success-cache TTL. Used by the renderer
// to set Cache-Control: max-age.
func (c *Collector) TTL() time.Duration { return c.opts.TTL }

// Snapshot returns the latest cached snapshot, building a new one if
// the cache is cold or expired. Concurrent callers on a cold cache
// share a single in-flight build via singleflight.
func (c *Collector) Snapshot(ctx context.Context) (*Snapshot, error) {
	now := c.opts.Now()

	c.mu.Lock()
	cached := c.cached
	c.mu.Unlock()

	if cached != nil && now.Before(cached.expiresAt) {
		return cached.snap, cached.err
	}

	v, err, _ := c.sf.Do("status", func() (interface{}, error) {
		// Re-check inside singleflight; another goroutine may have
		// populated the cache while we were queued.
		c.mu.Lock()
		if c.cached != nil && c.opts.Now().Before(c.cached.expiresAt) {
			cached := c.cached
			c.mu.Unlock()
			return cached, nil
		}
		c.mu.Unlock()

		snap, buildErr := build(ctx, c.opts, c.startedAt)

		entry := &cacheEntry{snap: snap, err: buildErr}
		if buildErr == nil {
			entry.expiresAt = c.opts.Now().Add(c.opts.TTL)
		} else {
			entry.expiresAt = c.opts.Now().Add(c.opts.NegTTL)
		}

		c.mu.Lock()
		c.cached = entry
		c.mu.Unlock()

		return entry, nil
	})
	if err != nil {
		return nil, err
	}
	entry := v.(*cacheEntry)
	return entry.snap, entry.err
}
```

- [ ] **Step 6.2: Run the existing test — should pass now**

```
just test ./internal/status -run TestCollect_FreshDataDir
```

Expected: PASS.

- [ ] **Step 6.3: Add cache-behavior tests**

Append to `internal/status/collect_test.go`:

```go
func TestCollect_CacheReusesPointer(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	now := time.Date(2026, 5, 25, 0, 0, 0, 0, time.UTC)
	c, err := status.New(status.Options{
		Store:   st,
		DataDir: dataDir,
		TTL:     30 * time.Second,
		Now:     func() time.Time { return now },
	})
	require.NoError(t, err)

	a, err := c.Snapshot(context.Background())
	require.NoError(t, err)
	b, err := c.Snapshot(context.Background())
	require.NoError(t, err)
	require.Same(t, a, b, "cached snapshot pointer should be reused")
}

func TestCollect_CacheExpiresAfterTTL(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	now := time.Date(2026, 5, 25, 0, 0, 0, 0, time.UTC)
	c, err := status.New(status.Options{
		Store:   st,
		DataDir: dataDir,
		TTL:     30 * time.Second,
		Now:     func() time.Time { return now },
	})
	require.NoError(t, err)

	a, err := c.Snapshot(context.Background())
	require.NoError(t, err)

	// Advance the clock past TTL; the next call should build fresh.
	now = now.Add(31 * time.Second)
	b, err := c.Snapshot(context.Background())
	require.NoError(t, err)
	require.NotSame(t, a, b, "snapshot pointer should change after TTL")
}

func TestCollect_ConcurrentCallsCollapse(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	c, err := status.New(status.Options{
		Store:   st,
		DataDir: dataDir,
		TTL:     30 * time.Second,
	})
	require.NoError(t, err)

	const N = 64
	var wg sync.WaitGroup
	results := make([]*status.Snapshot, N)
	wg.Add(N)
	for i := range N {
		go func() {
			defer wg.Done()
			snap, err := c.Snapshot(context.Background())
			require.NoError(t, err)
			results[i] = snap
		}()
	}
	wg.Wait()

	for i := 1; i < N; i++ {
		require.Same(t, results[0], results[i])
	}
}
```

Add `import "sync"` to the test file imports if not present.

- [ ] **Step 6.4: Run new tests**

```
just test ./internal/status
```

Expected: PASS.

- [ ] **Step 6.5: Run with race**

```
just test-race ./internal/status
```

Expected: PASS, no races.

- [ ] **Step 6.6: Commit**

```
git add internal/status/
git commit -m "status: cached snapshot collector with singleflight"
```

---

## Task 7: Fixture-driven gather tests

This task validates that the gather functions read the right pebble keys, segment files, and pebble counts. Each step adds one test.

**Files:**
- Modify: `internal/status/collect_test.go`

- [ ] **Step 7.1: Add test for phase + entered_at**

Append:

```go
func TestCollect_PhaseAndEnteredAt(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	enteredAt := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, lifecycle.WritePhase(st, lifecycle.PhaseSteadyState, enteredAt))

	c, err := status.New(status.Options{
		Store:   st,
		DataDir: dataDir,
		Now:     func() time.Time { return enteredAt.Add(24 * time.Hour) },
	})
	require.NoError(t, err)
	snap, err := c.Snapshot(context.Background())
	require.NoError(t, err)

	require.Equal(t, lifecycle.PhaseSteadyState, snap.Phase.Phase)
	require.True(t, snap.Phase.PhaseEnteredAt.Equal(enteredAt))
}
```

Add `"github.com/bluesky-social/jetstream-v2/internal/lifecycle"` to test imports.

- [ ] **Step 7.2: Add test for backfill counts**

Append:

```go
func TestCollect_BackfillCounts(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	bs := backfill.NewStore(st, nil)
	ctx := context.Background()

	for i := range 5 {
		did := atmos.DID("did:plc:disc" + string(rune('a'+i)))
		require.NoError(t, bs.OnDiscover(ctx, sync.ListReposEntry{DID: did, Active: true}))
	}
	for i := range 3 {
		did := atmos.DID("did:plc:done" + string(rune('a'+i)))
		require.NoError(t, bs.OnDiscover(ctx, sync.ListReposEntry{DID: did, Active: true}))
		require.NoError(t, bs.OnComplete(ctx, did, &repo.Commit{Rev: "1"}))
	}

	c, err := status.New(status.Options{Store: st, DataDir: dataDir})
	require.NoError(t, err)
	snap, err := c.Snapshot(ctx)
	require.NoError(t, err)

	require.Equal(t, uint64(8), snap.Backfill.TotalDIDs)
	require.Equal(t, uint64(5), snap.Backfill.Discovered)
	require.Equal(t, uint64(3), snap.Backfill.Complete)
	require.Equal(t, uint64(0), snap.Backfill.Failed)
	require.InDelta(t, 37.5, snap.Backfill.PercentComplete, 0.001)
}
```

Add to test imports:
- `"github.com/bluesky-social/jetstream-v2/internal/ingest/backfill"`
- `"github.com/jcalabro/atmos"`
- `"github.com/jcalabro/atmos/repo"`
- `sync "github.com/jcalabro/atmos/sync"` (aliased to avoid collision with stdlib `sync`)

If your existing test file already imports stdlib `sync` (it should, from Task 6's `TestCollect_ConcurrentCallsCollapse`), import the atmos package as `atmossync "github.com/jcalabro/atmos/sync"` and use `atmossync.ListReposEntry` everywhere.

- [ ] **Step 7.3: Add test for live cursor + seq counters**

Append:

```go
func TestCollect_LiveCursors(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	require.NoError(t, live.SaveUpstreamCursor(st, live.CursorKey, 1234567))

	var seqBuf [8]byte
	binary.LittleEndian.PutUint64(seqBuf[:], 4242)
	require.NoError(t, st.Set([]byte(live.SteadySeqKey), seqBuf[:], store.SyncWrites))
	binary.LittleEndian.PutUint64(seqBuf[:], 1111)
	require.NoError(t, st.Set([]byte(live.BootstrapSeqKey), seqBuf[:], store.SyncWrites))

	c, err := status.New(status.Options{Store: st, DataDir: dataDir})
	require.NoError(t, err)
	snap, err := c.Snapshot(context.Background())
	require.NoError(t, err)

	require.Equal(t, int64(1234567), snap.Live.UpstreamCursor)
	require.Equal(t, uint64(4242), snap.Live.NextSeq)
	require.Equal(t, uint64(1111), snap.Live.BootstrapSeq)
}
```

Add `"encoding/binary"` and `"github.com/bluesky-social/jetstream-v2/internal/ingest/live"` to test imports.

- [ ] **Step 7.4: Add test for pebble keyspace counts**

Append:

```go
func TestCollect_PebbleKeyspaces(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	require.NoError(t, st.Set([]byte("repo/a"), []byte("x"), store.SyncWrites))
	require.NoError(t, st.Set([]byte("repo/b"), []byte("x"), store.SyncWrites))
	require.NoError(t, st.Set([]byte("sync/chain/a"), []byte("x"), store.SyncWrites))
	require.NoError(t, st.Set([]byte("sync/host/a"), []byte("x"), store.SyncWrites))
	require.NoError(t, st.Set([]byte("relay/cursor"), []byte("x"), store.SyncWrites))
	require.NoError(t, st.Set([]byte("sync/identity/a"), []byte("x"), store.SyncWrites))

	c, err := status.New(status.Options{Store: st, DataDir: dataDir})
	require.NoError(t, err)
	snap, err := c.Snapshot(context.Background())
	require.NoError(t, err)

	require.Equal(t, uint64(2), snap.Pebble.KeyspaceCounts["repo/"])
	require.Equal(t, uint64(1), snap.Pebble.KeyspaceCounts["sync/chain/"])
	require.Equal(t, uint64(1), snap.Pebble.KeyspaceCounts["sync/host/"])
	require.Equal(t, uint64(1), snap.Pebble.KeyspaceCounts["relay/"])
	_, hasIdentity := snap.Pebble.KeyspaceCounts["sync/identity/"]
	require.False(t, hasIdentity, "sync/identity/ must not be exposed")
}
```

- [ ] **Step 7.5: Run all status tests**

```
just test ./internal/status
```

Expected: PASS.

- [ ] **Step 7.6: Commit**

```
git add internal/status/
git commit -m "status: fixture tests for phase, backfill, live, pebble"
```

---

## Task 8: `internal/web` package — formatting helpers

**Files:**
- Create: `internal/web/doc.go`
- Create: `internal/web/format.go`
- Create: `internal/web/format_test.go`

- [ ] **Step 8.1: Package doc**

`internal/web/doc.go`:

```go
// Package web renders the public /status page from a status.Snapshot.
//
// The handler exposes a single route. Snapshots are produced by an
// injected Snapshotter (typically *status.Collector) so the rendering
// path stays decoupled from data gathering.
//
// Templates are embedded at build time; CSS lives inside the template
// in a <style> block. No JavaScript, no external assets.
package web
```

- [ ] **Step 8.2: Failing tests for formatting**

`internal/web/format_test.go`:

```go
package web

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHumanBytes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   int64
		want string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.00 KiB"},
		{1536, "1.50 KiB"},
		{1024 * 1024, "1.00 MiB"},
		{int64(1.5 * 1024 * 1024 * 1024), "1.50 GiB"},
		{int64(2 * 1024 * 1024 * 1024 * 1024), "2.00 TiB"},
	}
	for _, c := range cases {
		require.Equal(t, c.want, humanBytes(c.in), "humanBytes(%d)", c.in)
	}
}

func TestHumanDuration(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   time.Duration
		want string
	}{
		{0, "0s"},
		{500 * time.Millisecond, "0s"},
		{45 * time.Second, "45s"},
		{2*time.Minute + 5*time.Second, "2m 5s"},
		{3 * time.Hour, "3h 0m"},
		{27*time.Hour + 30*time.Minute, "1d 3h"},
	}
	for _, c := range cases {
		require.Equal(t, c.want, humanDuration(c.in), "humanDuration(%v)", c.in)
	}
}

func TestRelativeTime(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	require.Equal(t, "never", relativeTime(time.Time{}, now))
	require.Equal(t, "5s ago", relativeTime(now.Add(-5*time.Second), now))
	require.Equal(t, "in 5s", relativeTime(now.Add(5*time.Second), now))
}

func TestHumanInt(t *testing.T) {
	t.Parallel()
	require.Equal(t, "0", humanInt(0))
	require.Equal(t, "999", humanInt(999))
	require.Equal(t, "1,000", humanInt(1000))
	require.Equal(t, "1,234,567", humanInt(1234567))
}
```

- [ ] **Step 8.3: Run, expect failure**

```
just test ./internal/web
```

Expected: compile error — symbols undefined.

- [ ] **Step 8.4: Implement**

`internal/web/format.go`:

```go
package web

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// humanBytes formats n as a base-1024 human-readable size.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	suffixes := []string{"KiB", "MiB", "GiB", "TiB", "PiB"}
	if exp >= len(suffixes) {
		exp = len(suffixes) - 1
	}
	return fmt.Sprintf("%.2f %s", float64(n)/float64(div), suffixes[exp])
}

// humanDuration renders d as the most-significant two units. Sub-
// second values render as "0s" — this is a status page, not a profiler.
func humanDuration(d time.Duration) string {
	if d < time.Second {
		return "0s"
	}
	d = d.Round(time.Second)

	days := int(d / (24 * time.Hour))
	d -= time.Duration(days) * 24 * time.Hour
	hours := int(d / time.Hour)
	d -= time.Duration(hours) * time.Hour
	minutes := int(d / time.Minute)
	d -= time.Duration(minutes) * time.Minute
	seconds := int(d / time.Second)

	switch {
	case days > 0:
		return fmt.Sprintf("%dd %dh", days, hours)
	case hours > 0:
		return fmt.Sprintf("%dh %dm", hours, minutes)
	case minutes > 0:
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	default:
		return fmt.Sprintf("%ds", seconds)
	}
}

// relativeTime renders t relative to now ("5s ago", "in 3m"). Zero
// time renders as "never".
func relativeTime(t, now time.Time) string {
	if t.IsZero() {
		return "never"
	}
	diff := now.Sub(t)
	if diff < 0 {
		return "in " + humanDuration(-diff)
	}
	return humanDuration(diff) + " ago"
}

// humanInt renders n with comma separators ("1,234,567").
func humanInt(n uint64) string {
	s := strconv.FormatUint(n, 10)
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	pre := len(s) % 3
	if pre > 0 {
		b.WriteString(s[:pre])
		if len(s) > pre {
			b.WriteByte(',')
		}
	}
	for i := pre; i < len(s); i += 3 {
		b.WriteString(s[i : i+3])
		if i+3 < len(s) {
			b.WriteByte(',')
		}
	}
	return b.String()
}
```

- [ ] **Step 8.5: Run tests**

```
just test ./internal/web
```

Expected: PASS.

- [ ] **Step 8.6: Commit**

```
git add internal/web/
git commit -m "web: human-readable formatting helpers"
```

---

## Task 9: `internal/web` — handler and template

**Files:**
- Create: `internal/web/handler.go`
- Create: `internal/web/templates/status.html`
- Create: `internal/web/handler_test.go`

- [ ] **Step 9.1: Create the template**

`internal/web/templates/status.html`:

```html
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>jetstream status</title>
<style>
  :root {
    --fg: #1a1a1a;
    --muted: #666;
    --bg: #fafafa;
    --card: #fff;
    --border: #e0e0e0;
    --accent: #2860c8;
    --bar-bg: #eee;
    --bar-fill: #2860c8;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --fg: #eee;
      --muted: #999;
      --bg: #181818;
      --card: #222;
      --border: #333;
      --accent: #6aa3ff;
      --bar-bg: #333;
      --bar-fill: #6aa3ff;
    }
  }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
    margin: 0; padding: 2rem 1rem;
    color: var(--fg); background: var(--bg);
    line-height: 1.5;
  }
  main { max-width: 1000px; margin: 0 auto; }
  h1 { margin: 0 0 0.25rem 0; font-size: 1.75rem; }
  h2 { margin: 0 0 0.75rem 0; font-size: 1.1rem; color: var(--accent); }
  .sub { color: var(--muted); font-size: 0.9rem; margin-bottom: 1.5rem; }
  section {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 1rem 1.25rem;
    margin-bottom: 1rem;
  }
  dl { display: grid; grid-template-columns: max-content 1fr; gap: 0.25rem 1rem; margin: 0; }
  dt { color: var(--muted); }
  dd { margin: 0; font-variant-numeric: tabular-nums; }
  .grid2 {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 1rem;
  }
  @media (max-width: 700px) {
    .grid2 { grid-template-columns: 1fr; }
  }
  .bar {
    background: var(--bar-bg);
    border-radius: 4px;
    height: 12px;
    overflow: hidden;
    margin: 0.25rem 0 0.5rem 0;
  }
  .bar > div { background: var(--bar-fill); height: 100%; }
  code { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.9em; }
  .cursor {
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 0.85em;
    color: var(--muted);
    word-break: break-all;
  }
  .latest {
    margin-top: 0.75rem; padding-top: 0.75rem;
    border-top: 1px solid var(--border);
  }
  .latest h3 { margin: 0 0 0.5rem 0; font-size: 0.95rem; color: var(--muted); font-weight: 500; }
</style>
</head>
<body>
<main>
  <h1>jetstream</h1>
  <div class="sub">
    {{.Process.Version}} (commit {{.Process.Commit}})
    &middot; built {{.Process.BuiltAt}}
    &middot; uptime {{humanDuration .Process.Uptime}}
    &middot; generated {{relativeTime .GeneratedAt .Now}}
  </div>

  <section>
    <h2>Phase</h2>
    <dl>
      <dt>State</dt>
      <dd>{{if .Phase.Phase}}<code>{{.Phase.Phase}}</code>{{else}}starting{{end}}</dd>
      <dt>Entered</dt>
      <dd>
        {{if .Phase.PhaseEnteredAt.IsZero}}
          since process start
        {{else}}
          {{relativeTime .Phase.PhaseEnteredAt .Now}}
        {{end}}
      </dd>
    </dl>
  </section>

  <section>
    <h2>Backfill</h2>
    {{if gt .Backfill.TotalDIDs 0}}
      <div class="bar"><div style="width: {{percentString .Backfill.PercentComplete}}"></div></div>
      <dl>
        <dt>Progress</dt>
        <dd>{{printf "%.2f%%" .Backfill.PercentComplete}}
            ({{humanInt .Backfill.Complete}} / {{humanInt .Backfill.TotalDIDs}} DIDs)</dd>
        <dt>Discovered</dt>
        <dd>{{humanInt .Backfill.Discovered}}</dd>
        <dt>Failed</dt>
        <dd>{{humanInt .Backfill.Failed}}</dd>
        {{if .Backfill.ListReposCursor}}
        <dt>listRepos cursor</dt>
        <dd class="cursor">{{.Backfill.ListReposCursor}}</dd>
        {{end}}
      </dl>
    {{else}}
      <p class="sub">Backfill has not started.</p>
    {{end}}
  </section>

  <section>
    <h2>Live ingest</h2>
    <dl>
      <dt>Upstream cursor</dt>
      <dd>{{if eq .Live.UpstreamCursor 0}}not yet started{{else}}{{humanInt64 .Live.UpstreamCursor}}{{end}}</dd>
      <dt>Sequence allocated</dt>
      <dd>{{humanInt .Live.NextSeq}} events</dd>
      {{if gt .Live.BootstrapSeq 0}}
      <dt>Bootstrap-time live seq</dt>
      <dd>{{humanInt .Live.BootstrapSeq}}</dd>
      {{end}}
    </dl>
  </section>

  <section>
    <h2>Segments</h2>
    <div class="grid2">
      {{template "tree" dict "Stats" .Segments "Now" .Now "Title" "segments/"}}
      {{template "tree" dict "Stats" .LiveSegs "Now" .Now "Title" "backfill/live_segments/"}}
    </div>
  </section>

  <section>
    <h2>Metadata store</h2>
    <dl>
      <dt>On disk</dt>
      <dd>{{humanBytes .Pebble.DiskBytes}}</dd>
      {{range $prefix, $count := .Pebble.KeyspaceCounts}}
      <dt><code>{{$prefix}}</code></dt>
      <dd>{{humanInt $count}}</dd>
      {{end}}
    </dl>
  </section>
</main>
</body>
</html>

{{define "tree"}}
<div>
  <h3 style="margin-top: 0; font-size: 0.95rem; color: var(--muted); font-weight: 500;">{{.Title}}</h3>
  <dl>
    <dt>Files</dt>
    <dd>{{.Stats.SealedCount}} sealed{{if gt .Stats.ActiveCount 0}} + {{.Stats.ActiveCount}} active{{end}}</dd>
    <dt>Compressed</dt>
    <dd>{{humanBytes .Stats.CompressedBytes}}</dd>
    <dt>Uncompressed</dt>
    <dd>{{humanBytes .Stats.UncompressedBytes}}</dd>
    {{if not .Stats.OldestMTime.IsZero}}
    <dt>Oldest</dt>
    <dd>{{relativeTime .Stats.OldestMTime .Now}}</dd>
    <dt>Newest</dt>
    <dd>{{relativeTime .Stats.NewestMTime .Now}}</dd>
    {{end}}
  </dl>
  {{with .Stats.LatestSegment}}
  <div class="latest">
    <h3>Latest segment ({{if .Sealed}}sealed{{else}}active{{end}})</h3>
    <dl>
      <dt>Index</dt>
      <dd>{{.Index}}</dd>
      <dt>Events</dt>
      <dd>{{humanInt .EventCount}}</dd>
      <dt>Unique DIDs</dt>
      <dd>{{humanInt64Cast .UniqueDIDCount}}</dd>
      <dt>Blocks</dt>
      <dd>{{humanInt64Cast .BlockCount}}</dd>
      <dt>Collections</dt>
      <dd>{{.CollectionCount}}</dd>
      <dt>Seq range</dt>
      <dd>[{{humanInt .MinSeq}}, {{humanInt .MaxSeq}}]</dd>
      <dt>Size</dt>
      <dd>{{humanBytes .SizeBytes}}</dd>
    </dl>
  </div>
  {{end}}
</div>
{{end}}
```

- [ ] **Step 9.2: Implement the handler**

`internal/web/handler.go`:

```go
package web

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/status"
)

//go:embed templates/status.html
var templateFS embed.FS

// Snapshotter is what the handler needs from a status collector. The
// concrete *status.Collector satisfies it; tests pass a fake.
type Snapshotter interface {
	Snapshot(ctx context.Context) (*status.Snapshot, error)
	TTL() time.Duration
}

// Handler renders the public /status page. Construct via New.
type Handler struct {
	tpl *template.Template
	src Snapshotter
	now func() time.Time
}

// Options configures Handler. Now is overridable for tests.
type Options struct {
	Snapshotter Snapshotter
	Now         func() time.Time
}

// New parses templates at construction time so a malformed template
// surfaces at startup, not on first request.
func New(opts Options) (*Handler, error) {
	if opts.Snapshotter == nil {
		return nil, errors.New("web: Options.Snapshotter is required")
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}

	funcs := template.FuncMap{
		"humanBytes":     humanBytes,
		"humanDuration":  humanDuration,
		"humanInt":       humanInt,
		"humanInt64":     func(n int64) string { return humanInt(uint64(n)) },
		"humanInt64Cast": func(n any) string {
			switch v := n.(type) {
			case uint32:
				return humanInt(uint64(v))
			case uint64:
				return humanInt(v)
			case int:
				return humanInt(uint64(v))
			default:
				return fmt.Sprint(n)
			}
		},
		"relativeTime":   relativeTime,
		"percentString":  func(p float64) string { return strconv.FormatFloat(p, 'f', 2, 64) + "%" },
		"dict":           dictFunc,
	}

	tpl, err := template.New("status.html").Funcs(funcs).ParseFS(templateFS, "templates/status.html")
	if err != nil {
		return nil, fmt.Errorf("web: parse template: %w", err)
	}

	return &Handler{tpl: tpl, src: opts.Snapshotter, now: opts.Now}, nil
}

// dictFunc lets the template build map[string]any inline so we can
// pass multiple values to a sub-template.
func dictFunc(kv ...any) (map[string]any, error) {
	if len(kv)%2 != 0 {
		return nil, errors.New("dict: odd number of args")
	}
	m := make(map[string]any, len(kv)/2)
	for i := 0; i < len(kv); i += 2 {
		k, ok := kv[i].(string)
		if !ok {
			return nil, errors.New("dict: keys must be strings")
		}
		m[k] = kv[i+1]
	}
	return m, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	snap, err := h.src.Snapshot(r.Context())
	if err != nil {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`<!doctype html><meta charset="utf-8"><title>jetstream</title><p>Status temporarily unavailable.</p>`))
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d", int(h.src.TTL().Seconds())))
	w.Header().Set("X-Status-Generated-At", snap.GeneratedAt.UTC().Format(time.RFC3339Nano))

	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}

	data := struct {
		*status.Snapshot
		Now time.Time
	}{
		Snapshot: snap,
		Now:      h.now(),
	}
	if err := h.tpl.Execute(w, data); err != nil {
		// Header already written; log via the http.Server's ErrorLog
		// by surfacing through default logger.
		_ = err
	}
}
```

- [ ] **Step 9.3: Handler tests**

`internal/web/handler_test.go`:

```go
package web_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
	"github.com/bluesky-social/jetstream-v2/internal/status"
	"github.com/bluesky-social/jetstream-v2/internal/web"
	"github.com/stretchr/testify/require"
)

type fakeSnapshotter struct {
	snap *status.Snapshot
	err  error
	ttl  time.Duration
}

func (f *fakeSnapshotter) Snapshot(_ context.Context) (*status.Snapshot, error) {
	return f.snap, f.err
}
func (f *fakeSnapshotter) TTL() time.Duration { return f.ttl }

func newFixtureSnap() *status.Snapshot {
	return &status.Snapshot{
		GeneratedAt: time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC),
		Process: status.ProcessInfo{
			Version: "v1.2.3", Commit: "abcdef0", BuiltAt: "2026-05-20",
			StartedAt: time.Date(2026, 5, 25, 11, 0, 0, 0, time.UTC),
			Uptime:    time.Hour, GoVersion: "go1.24",
		},
		Phase: status.PhaseInfo{
			Phase:          lifecycle.PhaseSteadyState,
			PhaseEnteredAt: time.Date(2026, 5, 24, 0, 0, 0, 0, time.UTC),
		},
		Backfill: status.BackfillStats{
			TotalDIDs: 100, Discovered: 10, Complete: 80, Failed: 10,
			PercentComplete: 80.0,
			ListReposCursor: "<script>alert('xss')</script>",
		},
		Live:    status.LiveStats{UpstreamCursor: 1234567, NextSeq: 999, BootstrapSeq: 0},
		Segments: status.SegmentTreeStats{
			Dir: "/tmp/segments", SealedCount: 5, ActiveCount: 1,
			CompressedBytes: 1024 * 1024, UncompressedBytes: 4 * 1024 * 1024,
		},
		LiveSegs: status.SegmentTreeStats{Dir: "/tmp/backfill/live_segments"},
		Pebble: status.PebbleStats{
			DiskBytes: 5 * 1024 * 1024,
			KeyspaceCounts: map[string]uint64{
				"repo/": 100, "sync/chain/": 50, "sync/host/": 50, "relay/": 1,
			},
		},
	}
}

func TestHandler_RendersOK(t *testing.T) {
	t.Parallel()
	h, err := web.New(web.Options{
		Snapshotter: &fakeSnapshotter{snap: newFixtureSnap(), ttl: 30 * time.Second},
		Now:         func() time.Time { return time.Date(2026, 5, 25, 12, 0, 5, 0, time.UTC) },
	})
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, "text/html; charset=utf-8", rr.Header().Get("Content-Type"))
	require.Equal(t, "public, max-age=30", rr.Header().Get("Cache-Control"))
	require.NotEmpty(t, rr.Header().Get("X-Status-Generated-At"))

	body := rr.Body.String()
	require.Contains(t, body, "jetstream")
	require.Contains(t, body, "v1.2.3")
	require.Contains(t, body, "steady_state")
	require.Contains(t, body, "Backfill")
	require.Contains(t, body, "80.00%")
}

func TestHandler_EscapesXSS(t *testing.T) {
	t.Parallel()
	h, err := web.New(web.Options{
		Snapshotter: &fakeSnapshotter{snap: newFixtureSnap(), ttl: 30 * time.Second},
	})
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	h.ServeHTTP(rr, req)

	body := rr.Body.String()
	require.NotContains(t, body, "<script>alert('xss')</script>")
	require.True(t,
		strings.Contains(body, "&lt;script&gt;") || strings.Contains(body, "&#x3C;script&#x3E;") || strings.Contains(body, "&#34;"),
		"expected the cursor's HTML to be escaped, body=%s", body)
}

func TestHandler_MethodNotAllowed(t *testing.T) {
	t.Parallel()
	h, err := web.New(web.Options{
		Snapshotter: &fakeSnapshotter{snap: newFixtureSnap(), ttl: 30 * time.Second},
	})
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/status", nil)
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusMethodNotAllowed, rr.Code)
	require.Equal(t, "GET, HEAD", rr.Header().Get("Allow"))
}

func TestHandler_503OnError(t *testing.T) {
	t.Parallel()
	h, err := web.New(web.Options{
		Snapshotter: &fakeSnapshotter{err: errors.New("boom"), ttl: 30 * time.Second},
	})
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusServiceUnavailable, rr.Code)
	require.Contains(t, rr.Body.String(), "temporarily unavailable")
}

func TestHandler_HEAD(t *testing.T) {
	t.Parallel()
	h, err := web.New(web.Options{
		Snapshotter: &fakeSnapshotter{snap: newFixtureSnap(), ttl: 30 * time.Second},
	})
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodHead, "/status", nil)
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.Empty(t, rr.Body.Bytes())
	require.NotEmpty(t, rr.Header().Get("Cache-Control"))
}
```

- [ ] **Step 9.4: Run web tests**

```
just test ./internal/web
```

Expected: PASS.

- [ ] **Step 9.5: Commit**

```
git add internal/web/
git commit -m "web: status page handler and template"
```

---

## Task 10: Wire `/status` into `internal/server`

**Files:**
- Modify: `internal/server/server.go`
- Modify: `internal/server/server_test.go`

- [ ] **Step 10.1: Failing test — /status returns 200 when wired**

Append to `internal/server/server_test.go`:

```go
func TestPublicHandler_StatusUnwired(t *testing.T) {
	t.Parallel()
	base := mountPublic(t, newServer(t))

	resp, err := http.Get(base + "/status")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestPublicHandler_StatusWired(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := obs.NewMetrics()
	srv := server.New(server.Config{
		PublicAddr:      "127.0.0.1:0",
		DebugAddr:       "127.0.0.1:0",
		ShutdownTimeout: 5 * time.Second,
		StatusHandler:   http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte("ok"))
		}),
	}, logger, metrics)

	ts := httptest.NewServer(srv.PublicHandler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/status")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}
```

The test uses an unqualified `server.New` because the existing `newServer` helper is in the same `package server` test file. The new test must use `server.New` from `package server_test` — actually, `server_test.go` uses `package server`, so adapt:

If `server_test.go` is `package server`, write the test using the unqualified `New`. The existing test file's `package server` is determined by `cat -n internal/server/server_test.go | head -3` from the existing helpers — they reference `s.srv` directly, which is a private field, so they're `package server`. Use unqualified `New(Config{...})` accordingly. Replace `server.New` and `server.Config` with `New` and `Config`. Also drop the `server.` qualifier on `StatusHandler`.

- [ ] **Step 10.2: Run, expect failure**

```
just test ./internal/server
```

Expected: compile error — `Config.StatusHandler` undefined.

- [ ] **Step 10.3: Wire StatusHandler**

In `internal/server/server.go`, add to `Config`:

```go
type Config struct {
    PublicAddr string
    DebugAddr string
    ShutdownTimeout time.Duration

    // StatusHandler, if non-nil, is mounted at GET /status on the
    // public listener. cmd/jetstream constructs this via the web
    // package; tests can pass any http.Handler.
    StatusHandler http.Handler
}
```

In `New`, store the handler. The simplest path is to add a field on `Server`:

```go
type Server struct {
    cfg Config
    // ...existing fields...
    statusHandler http.Handler
}
```

Set it in `New`:

```go
s := &Server{cfg: cfg, logger: logger, metrics: metrics, statusHandler: cfg.StatusHandler}
```

In `publicMux`, register the route conditionally:

```go
func (s *Server) publicMux() http.Handler {
    mux := http.NewServeMux()
    mux.Handle("GET /{$}", s.metrics.InstrumentHandler("root", http.HandlerFunc(s.handleRoot)))
    if s.statusHandler != nil {
        mux.Handle("GET /status", s.metrics.InstrumentHandler("status", s.statusHandler))
        mux.Handle("HEAD /status", s.metrics.InstrumentHandler("status", s.statusHandler))
    }
    return mux
}
```

- [ ] **Step 10.4: Run tests**

```
just test ./internal/server
```

Expected: PASS.

- [ ] **Step 10.5: Commit**

```
git add internal/server/
git commit -m "server: optional /status route wired via Config.StatusHandler"
```

---

## Task 11: Wire collector + handler in `cmd/jetstream`

**Files:**
- Modify: `cmd/jetstream/main.go`
- Modify: `cmd/jetstream/serve_test.go` (add /status smoke)

- [ ] **Step 11.1: Construct collector + handler in `runServe`**

Find the block in `cmd/jetstream/main.go` where `srv := server.New(...)` is called. Just before that call, add:

```go
statusCollector, err := status.New(status.Options{
    Store:   metaStore,
    DataDir: dataDir,
})
if err != nil {
    return fmt.Errorf("serve: build status collector: %w", err)
}

statusHandler, err := web.New(web.Options{
    Snapshotter: statusCollector,
})
if err != nil {
    return fmt.Errorf("serve: build status handler: %w", err)
}
```

Add the field to the existing `server.Config{...}` literal:

```go
srv := server.New(server.Config{
    PublicAddr:      cmd.String("addr"),
    DebugAddr:       cmd.String("debug-addr"),
    ShutdownTimeout: cmd.Duration("shutdown-timeout"),
    StatusHandler:   statusHandler,
}, processLogger, metrics)
```

Add imports:

```go
"github.com/bluesky-social/jetstream-v2/internal/status"
"github.com/bluesky-social/jetstream-v2/internal/web"
```

- [ ] **Step 11.2: Build to confirm wiring compiles**

```
just build
```

Expected: success.

- [ ] **Step 11.3: Add end-to-end smoke test**

Locate `cmd/jetstream/serve_test.go` and look at how it spins up a real server. Append a test that hits `/status`:

```go
func TestServe_StatusEndpoint(t *testing.T) {
	t.Parallel()
	// Reuse whatever harness serve_test.go already provides — typically
	// a function that runs runServe in a goroutine against random ports
	// and returns the bound public URL. If the existing helper is named
	// runServeForTest, mirror its usage.
	publicURL := startServeForTest(t)

	resp, err := http.Get(publicURL + "/status")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "text/html; charset=utf-8", resp.Header.Get("Content-Type"))
	require.Contains(t, resp.Header.Get("Cache-Control"), "max-age=")
	require.NotEmpty(t, resp.Header.Get("X-Status-Generated-At"))

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), "jetstream")
	require.Contains(t, string(body), "Phase")
	require.Contains(t, string(body), "Backfill")
}
```

If the existing test file uses a different harness name, adapt to the existing pattern. Read the test file first; mimic the simplest existing test that spins up a server.

- [ ] **Step 11.4: Run cmd tests**

```
just test ./cmd/jetstream
```

Expected: PASS.

- [ ] **Step 11.5: Run full suite**

```
just lint test
```

Expected: PASS.

- [ ] **Step 11.6: Commit**

```
git add cmd/jetstream/
git commit -m "cmd/jetstream: mount /status on public listener"
```

---

## Task 12: Verify by running the binary

**Files:** none (manual verification step).

- [ ] **Step 12.1: Start a fresh jetstream**

In one terminal:

```
just clean
just run serve --addr :8080 --debug-addr :6060
```

- [ ] **Step 12.2: Hit /status**

In another terminal:

```
curl -i http://127.0.0.1:8080/status
```

Expected: HTTP/1.1 200 OK, `Content-Type: text/html; charset=utf-8`,
`Cache-Control: public, max-age=30`, an `X-Status-Generated-At` header,
and an HTML body containing "jetstream", "Phase", "Backfill",
"Live ingest", "Segments", "Metadata store".

- [ ] **Step 12.3: Visual check**

```
xdg-open http://127.0.0.1:8080/status
```

(or visit in a browser). Verify:
- Light/dark theme follows OS preference.
- Layout is sane on a narrow window (segment columns stack).
- All counts render (zeros are OK; that's just the initial state).

- [ ] **Step 12.4: Confirm caching**

```
for i in 1 2 3 4 5; do curl -s -o /dev/null -w "%{http_code} %{time_total}s\n" http://127.0.0.1:8080/status; done
```

Expected: first request may be slightly slower (cold cache); subsequent
requests within 30s should be near-instant (warm cache hit).

- [ ] **Step 12.5: Confirm method handling**

```
curl -i -X POST http://127.0.0.1:8080/status
```

Expected: HTTP/1.1 405 Method Not Allowed, `Allow: GET, HEAD`.

- [ ] **Step 12.6: Stop the server**

Ctrl-C in the server terminal. Verify it shuts down cleanly.

No commit for this task — purely a verification gate.

---

## Final checklist

- [ ] All 12 tasks complete.
- [ ] `just lint test` passes.
- [ ] `just test-race` passes.
- [ ] Manual verification (Task 12) passed.
- [ ] All commits authored on a single feature branch.
- [ ] No commits to `main` directly.

---

## Notes for the implementer

**Atmos types.** `atmos.DID`, `atmos.sync.ListReposEntry`, and
`atmos.repo.Commit` are defined in the `github.com/jcalabro/atmos`
module. If the test imports look wrong, check existing tests in
`internal/ingest/backfill/store_test.go` for the canonical import shape
and mimic them.

**Pebble UpperBound.** `IterOptions.UpperBound` is exclusive. We use
`nextLexBound("repo/")` = `"repo0"` (the byte after `/` is `0`),
correctly bounding above any `repo/<did>` key.

**Template func map ergonomics.** Go's `html/template` is strict about
type matching. The `humanInt64Cast` shim accepts `any` so a template
can call it on `uint32`/`uint64`/`int` without type-asserting in the
template — handy for `SegmentSummary.UniqueDIDCount` (uint32) and
`BlockCount` (uint32). For new fields, prefer adding type-specific
funcs rather than expanding the `any` switch.

**No favicon.** Browsers will request `/favicon.ico` and get a 404.
That's fine; we deliberately don't serve one.

**Error display.** The 503 page is intentionally minimal so curl users
see a readable response. Detailed error messages stay in logs (we
don't show internal errors on a public-internet endpoint).

**Future evolution paths.** The spec lists out-of-scope items:
`/status.json`, auto-refresh, operator-only `/debug/inspect-all`,
maintained backfill counters. Each of those is additive against this
foundation; nothing in this plan blocks them.
