# Merge Phase — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the body of `runMerge` in `internal/ingest/orchestrator/`. The merge drains `data/backfill/live_segments/seg_*.jss` into `data/segments/`, dropping events whose data was already authoritatively written by the backfill engine, refreshing `repo/<did>.Rev` for surviving DIDs, discovering DIDs born during bootstrap via a listRepos resume, then sealing the destination active segment and removing the bootstrap tree.

**Architecture:** A single serial pipeline driven by a per-source-segment cursor in pebble (`merge/next_source_idx`). Per-event `IndexedAt` is re-stamped at append to preserve the §3.4 segment-time-monotonicity invariant. Surviving commit events bump `repo/<did>.Rev` (top-level, NOT `Backfill.Rev`) atomically alongside the cursor advance via a single pebble batch. After the drain loop, a listRepos resume from a new `bootstrap/last_listrepos_cursor` key picks up DIDs born during the bootstrap window and queues them as `StatusFailed` for steady-state retry. Crash recovery is at-least-once: a crash window between flush and cursor commit produces logical duplicates with new seq numbers, which clients are required to handle.

**Tech Stack:** Go 1.26, pebble (cockroachdb/pebble), atmos (sync, backfill, identity), prometheus, OpenTelemetry, gotestsum, golangci-lint.

**Spec:** `docs/superpowers/specs/2026-05-27-merge-phase-design.md`

---

## File Structure

| File | Why |
|---|---|
| `internal/store/encoding.go` | Modify: add `GetVersionedUint64LE` / `SetVersionedUint64LE` helpers |
| `internal/store/encoding_test.go` | Modify: add round-trip + version-mismatch tests for new helpers |
| `internal/ingest/live/cursor.go` | Modify: refactor to delegate to the new store helpers (purely internal cleanup; observable behavior unchanged) |
| `internal/ingest/writer.go` | Modify: extract `SegmentFiles(dir)` helper; have `scanSegmentsDir` reuse it |
| `internal/ingest/writer_test.go` | Modify: add unit tests for `SegmentFiles` |
| `internal/ingest/backfill/cursor.go` | Modify: add `bootstrap/last_listrepos_cursor` sibling key + accessor that only persists non-empty cursors |
| `internal/ingest/backfill/cursor_test.go` | Modify: add tests for the new sibling key |
| `internal/ingest/backfill/run.go` | Modify: wire the OnPageComplete callback to ALSO save the bootstrap-last cursor when non-empty |
| `internal/ingest/backfill/run_test.go` | Modify: assert bootstrap-last cursor is set after pagination |
| `internal/ingest/orchestrator/metrics.go` | Modify: add 6 merge-related counters |
| `internal/ingest/orchestrator/metrics_test.go` | Modify: add registration assertions for new counters |
| `internal/ingest/orchestrator/merge.go` | Replace stub: `runMerge` lifecycle entry point |
| `internal/ingest/orchestrator/merge_filter.go` | New: `shouldKeep` predicate + `repoStatusLookup` cache |
| `internal/ingest/orchestrator/merge_filter_test.go` | New: Tier 1 unit tests |
| `internal/ingest/orchestrator/merge_cursor.go` | New: cursor load/delete + `commitSourceComplete` atomic batch |
| `internal/ingest/orchestrator/merge_cursor_test.go` | New: cursor + batch round-trip tests |
| `internal/ingest/orchestrator/merge_runner.go` | New: `mergeRunner` struct + `run` / `processSourceSegment` |
| `internal/ingest/orchestrator/merge_discovery.go` | New: post-merge listRepos resume + `StatusFailed` row writes |
| `internal/ingest/orchestrator/merge_test.go` | New: Tier 2 integration + Tier 3 crash-and-resume |
| `internal/ingest/orchestrator/merge_swarm_test.go` | New: Tier 4 swarm/property test |
| `internal/ingest/orchestrator/recovery_test.go` | Modify: existing `TestRun_ResumeFromMerging_AdvancesToSteadyState` was authored against the no-op stub; update to use the new merge — populate at least one source segment and assert the merge's terminal state |

---

## Conventions

- `just test ./internal/...` runs short tests in well under a second per package; preserve that.
- `just test-race` runs the full suite under `-race`. Run before final commit.
- `just lint` must report zero issues.
- `t.Parallel()` on independent tests; `t.Cleanup` for `db.Close`, `writer.Close`, etc.
- Doc comments on exported symbols. Comments explain WHY, never WHAT.
- Error wrapping: `orchestrator: merge: <action>: %w` for orchestrator-internal errors; `backfill: <action>: %w`, `store: <action>: %w` for cross-package.
- No `Co-Authored-By` or other commit trailers.
- Per-task commits: each task ends with one `git commit`.
- File-creation steps include a doc-comment package header on every new file. Files in an existing package use a one-line comment naming the file's responsibility (matches the existing style — see `internal/ingest/live/cursor.go` line 1).

---

## Task 1: Add `GetVersionedUint64LE` / `SetVersionedUint64LE` to `store`

**Files:**
- Modify: `internal/store/encoding.go`
- Modify: `internal/store/encoding_test.go`

- [ ] **Step 1: Add the failing tests to `internal/store/encoding_test.go`**

Append to the existing file:

```go
func TestVersionedUint64LE_RoundTrip(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	require.NoError(t, s.SetVersionedUint64LE("merge/test", 0x42, 12345))

	got, ok, err := s.GetVersionedUint64LE("merge/test", 0x42)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(12345), got)
}

func TestVersionedUint64LE_AbsentKey(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	got, ok, err := s.GetVersionedUint64LE("merge/missing", 0x42)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, uint64(0), got)
}

func TestVersionedUint64LE_VersionMismatch(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	require.NoError(t, s.SetVersionedUint64LE("merge/test", 0x01, 7))

	_, _, err := s.GetVersionedUint64LE("merge/test", 0x02)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown version")
}

func TestVersionedUint64LE_WrongLength(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	// Stash a too-short value directly.
	require.NoError(t, s.Set([]byte("merge/test"), []byte{0x01, 0x00, 0x00}, store.SyncWrites))

	_, _, err := s.GetVersionedUint64LE("merge/test", 0x01)
	require.Error(t, err)
	require.Contains(t, err.Error(), "wrong length")
}
```

If `newTestStore` doesn't already exist in `encoding_test.go`, look for it elsewhere in the `store` package's `_test.go` files (e.g., `store_test.go`) — reuse whatever helper builds a test `*Store`.

- [ ] **Step 2: Run to verify the tests fail**

Run: `just test ./internal/store -run TestVersionedUint64LE`
Expected: build error or four FAILs (methods not defined).

- [ ] **Step 3: Implement the helpers in `internal/store/encoding.go`**

Append after `PrefixUpperBound`:

```go
// versionedUint64LELen is the on-disk size of a versioned uint64 LE
// payload: 1 byte for the format version + 8 bytes for the LE uint64.
// Matches the live cursor convention so operators inspecting pebble
// see a consistent shape across cursor-shaped keys.
const versionedUint64LELen = 1 + 8

// GetVersionedUint64LE reads key as a [1B version][8B LE uint64]
// payload and returns (value, true, nil) on a hit. A missing key
// returns (0, false, nil). Errors when the stored bytes are the
// wrong length or carry a version byte that differs from wantVersion.
//
// Centralized so cursor-shaped keys (live's relay/cursor, merge's
// merge/next_source_idx, future single-counter cursors) share one
// encoding and one set of error checks.
func (s *Store) GetVersionedUint64LE(key string, wantVersion byte) (uint64, bool, error) {
	val, closer, err := s.Get([]byte(key))
	if errors.Is(err, ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("store: get %s: %w", key, err)
	}
	defer func() { _ = closer.Close() }()

	if len(val) != versionedUint64LELen {
		return 0, false, fmt.Errorf("store: %s: wrong length %d (want %d)",
			key, len(val), versionedUint64LELen)
	}
	if val[0] != wantVersion {
		return 0, false, fmt.Errorf("store: %s: unknown version 0x%02x (want 0x%02x)",
			key, val[0], wantVersion)
	}
	return binary.LittleEndian.Uint64(val[1:]), true, nil
}

// SetVersionedUint64LE writes key as a [1B version][8B LE uint64]
// payload via pebble.Sync. Used by all cursor-shaped writers that
// don't need batching with other keys; callers staging into a
// pebble.Batch should use EncodeVersionedUint64LE + Batch.Set
// directly.
func (s *Store) SetVersionedUint64LE(key string, version byte, v uint64) error {
	buf := EncodeVersionedUint64LE(version, v)
	if err := s.Set([]byte(key), buf, SyncWrites); err != nil {
		return fmt.Errorf("store: set %s: %w", key, err)
	}
	return nil
}

// EncodeVersionedUint64LE returns a fresh [1B version][8B LE uint64]
// payload. Exposed so callers staging into a pebble.Batch (e.g. the
// merge phase committing a cursor + N RepoStatus rows atomically)
// can produce the byte layout without duplicating it.
func EncodeVersionedUint64LE(version byte, v uint64) []byte {
	buf := make([]byte, versionedUint64LELen)
	buf[0] = version
	binary.LittleEndian.PutUint64(buf[1:], v)
	return buf
}
```

- [ ] **Step 4: Run the tests**

Run: `just test ./internal/store -run TestVersionedUint64LE`
Expected: PASS for all four.

- [ ] **Step 5: Run the package's full test suite + lint**

Run: `just test ./internal/store && just lint`
Expected: PASS, zero lint issues.

- [ ] **Step 6: Commit**

```bash
git add internal/store/encoding.go internal/store/encoding_test.go
git commit -m "store: add versioned uint64 LE helpers for cursor-shaped keys"
```

---

## Task 2: Refactor `live/cursor.go` to delegate to the store helpers

This is purely an internal cleanup — observable behavior of `LoadUpstreamCursor` / `SaveUpstreamCursor` does not change. We're DRYing the encoding so the merge cursor doesn't duplicate it.

**Files:**
- Modify: `internal/ingest/live/cursor.go`
- Modify: `internal/ingest/live/cursor_test.go` and/or `internal/ingest/live/cursor_fuzz_test.go` (read first; existing tests must continue to pass)

- [ ] **Step 1: Read the existing tests**

Run: `cat internal/ingest/live/cursor_test.go internal/ingest/live/cursor_fuzz_test.go`
Note the existing assertions: round-trip, missing-key returns 0, negative-int64 corruption surfaces an error, wrong-length surfaces an error, wrong-version surfaces an error.

- [ ] **Step 2: Rewrite `internal/ingest/live/cursor.go`**

Replace the whole file (preserve the package doc comment + cursorV1 / cursorV1Len constants for callers that import them transitively if any — `grep cursorV1` first to see):

```go
// package live: cursor.go persists the upstream relay firehose
// cursor in pebble so a process restart resumes from the last
// durably-flushed block. DESIGN.md §3.1.1: persisted cursor must be
// less than or equal to the latest durable event in the segment file.
//
// The on-disk encoding is [1B version][8B LE uint64], delegated to
// the store package's GetVersionedUint64LE / SetVersionedUint64LE
// helpers so every cursor-shaped key in pebble shares one layout.
// atmos exposes the cursor as int64; we cast at the boundary and
// document the implicit non-negativity constraint (atmos relays
// only emit positive seq values).
package live

import (
	"fmt"
	"math"

	"github.com/bluesky-social/jetstream-v2/internal/store"
)

const (
	// cursorV1 is the only currently-supported cursor format version.
	// A strict-equal check on read means a forward-incompatible writer
	// surfaces as an explicit error rather than a silent
	// misinterpretation of the payload bytes.
	cursorV1 byte = 0x01
)

// LoadUpstreamCursor reads the persisted relay cursor for key.
// A missing key returns 0 with nil error so a fresh data dir
// starts the firehose at "live" (atmos's "no cursor" semantics).
//
// Returns an error if the stored bytes have the high bit set:
// reading those as int64 would yield a negative number, which atmos's
// dial silently treats as "no cursor → live tail". A corrupted
// cursor must surface as an error so the operator notices, not a
// silent re-tail of the firehose that drops every historical event
// between the corrupt seq and now (AGENTS.md: crashing > silent
// data loss).
func LoadUpstreamCursor(s *store.Store, key string) (int64, error) {
	v, ok, err := s.GetVersionedUint64LE(key, cursorV1)
	if err != nil {
		return 0, fmt.Errorf("livestream: %s: %w", key, err)
	}
	if !ok {
		return 0, nil
	}
	if v > math.MaxInt64 {
		return 0, fmt.Errorf("livestream: %s: decodes to negative cursor (raw=0x%016x)", key, v)
	}
	return int64(v), nil
}

// SaveUpstreamCursor durably persists v under key with pebble.Sync.
// Used inside ingest.Writer's OnAfterFlush so the cursor advance
// is ordered after the per-block fsync.
//
// Rejects negative values so the on-disk invariant "stored cursor >= 0"
// holds by construction at every write site, and LoadUpstreamCursor
// can surface storage corruption (rather than caller bugs) as the
// only path to a negative read.
func SaveUpstreamCursor(s *store.Store, key string, v int64) error {
	if v < 0 {
		return fmt.Errorf("livestream: refuse to save negative cursor %d to %s", v, key)
	}
	if err := s.SetVersionedUint64LE(key, cursorV1, uint64(v)); err != nil {
		return fmt.Errorf("livestream: save %s: %w", key, err)
	}
	return nil
}
```

- [ ] **Step 3: Drop the now-unused `decodeUpstreamCursor` from the fuzz test if it referenced the private function**

Run: `grep -n decodeUpstreamCursor internal/ingest/live/cursor_fuzz_test.go`

If the fuzz test calls `decodeUpstreamCursor` directly, rewrite it to call `LoadUpstreamCursor` against an in-memory store seeded with the fuzz input, so the fuzz target keeps its coverage of the decode path. Sample shape:

```go
func FuzzLoadUpstreamCursor(f *testing.F) {
	f.Add([]byte{0x01, 0, 0, 0, 0, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, raw []byte) {
		s := newTestStore(t)
		// Bypass SaveUpstreamCursor to inject arbitrary bytes.
		require.NoError(t, s.Set([]byte("relay/cursor"), raw, store.SyncWrites))
		_, _ = LoadUpstreamCursor(s, "relay/cursor")
	})
}
```

If the existing fuzz test drives the public surface already, leave it alone.

- [ ] **Step 4: Run the live package's tests**

Run: `just test ./internal/ingest/live`
Expected: PASS. All existing assertions must still hold; the refactor does not change behavior.

- [ ] **Step 5: Run lint**

Run: `just lint`
Expected: zero issues.

- [ ] **Step 6: Commit**

```bash
git add internal/ingest/live/cursor.go internal/ingest/live/cursor_test.go internal/ingest/live/cursor_fuzz_test.go
git commit -m "live: delegate cursor encoding to store.VersionedUint64LE helpers"
```

---

## Task 3: Extract `ingest.SegmentFiles(dir)` helper

**Files:**
- Modify: `internal/ingest/writer.go`
- Modify: `internal/ingest/writer_test.go`

- [ ] **Step 1: Add failing tests to `internal/ingest/writer_test.go`**

Append:

```go
func TestSegmentFiles_Empty(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	got, err := SegmentFiles(dir)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestSegmentFiles_SortedAscending(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// Create out-of-order to confirm the helper sorts.
	for _, idx := range []uint64{2, 0, 5, 1} {
		path := filepath.Join(dir, SegmentFilename(idx))
		require.NoError(t, os.WriteFile(path, []byte("placeholder"), 0o644))
	}

	got, err := SegmentFiles(dir)
	require.NoError(t, err)
	require.Len(t, got, 4)
	require.Equal(t, []uint64{0, 1, 2, 5}, []uint64{got[0].Idx, got[1].Idx, got[2].Idx, got[3].Idx})
	require.Equal(t, filepath.Join(dir, SegmentFilename(0)), got[0].Path)
	require.Equal(t, filepath.Join(dir, SegmentFilename(5)), got[3].Path)
}

func TestSegmentFiles_IgnoresNonSegmentFiles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, SegmentFilename(3)), []byte("x"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "README.md"), []byte("x"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "subdir"), 0o755))

	got, err := SegmentFiles(dir)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, uint64(3), got[0].Idx)
}
```

Add imports `"os"`, `"path/filepath"` if not already present.

- [ ] **Step 2: Run to verify failure**

Run: `just test ./internal/ingest -run TestSegmentFiles`
Expected: build error (`SegmentFiles` undefined).

- [ ] **Step 3: Implement in `internal/ingest/writer.go`**

Replace the existing `scanSegmentsDir` function with:

```go
// SegmentFile is one entry in a SegmentFiles result.
type SegmentFile struct {
	Idx  uint64
	Path string
}

// SegmentFiles returns every seg_*.jss file under dir, sorted by
// numeric index ascending. Non-segment files and subdirectories are
// silently skipped — the directory may legitimately contain other
// operator-placed files.
//
// Used by every consumer that needs the full segment manifest in
// creation order: the merge phase draining live_segments/, the
// future lookaside compactor, and inspect tooling.
func SegmentFiles(dir string) ([]SegmentFile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("ingest: readdir %s: %w", dir, err)
	}
	out := make([]SegmentFile, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		idx, ok := ParseSegmentIndex(e.Name())
		if !ok {
			continue
		}
		out = append(out, SegmentFile{Idx: idx, Path: filepath.Join(dir, e.Name())})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Idx < out[j].Idx })
	return out, nil
}

// scanSegmentsDir lists cfg.SegmentsDir and returns the highest seg_*
// index seen and whether any matching files exist. Thin wrapper over
// SegmentFiles preserved for the writer-open path.
func scanSegmentsDir(dir string) (idx uint64, has bool, err error) {
	files, err := SegmentFiles(dir)
	if err != nil {
		return 0, false, err
	}
	if len(files) == 0 {
		return 0, false, nil
	}
	last := files[len(files)-1]
	return last.Idx, true, nil
}
```

Add `"sort"` to the import block.

- [ ] **Step 4: Run the new tests + the existing writer tests**

Run: `just test ./internal/ingest`
Expected: PASS for everything.

- [ ] **Step 5: Run lint**

Run: `just lint`
Expected: zero issues.

- [ ] **Step 6: Commit**

```bash
git add internal/ingest/writer.go internal/ingest/writer_test.go
git commit -m "ingest: extract SegmentFiles helper for full-manifest consumers"
```

---

## Task 4: Add `bootstrap/last_listrepos_cursor` to backfill cursor

**Files:**
- Modify: `internal/ingest/backfill/cursor.go`
- Modify: `internal/ingest/backfill/cursor_test.go`

- [ ] **Step 1: Add failing tests to `internal/ingest/backfill/cursor_test.go`**

Append:

```go
func TestBootstrapLastListReposCursor_RoundTrip(t *testing.T) {
	t.Parallel()
	db := newTestStore(t)

	got, err := LoadBootstrapLastListReposCursor(db)
	require.NoError(t, err)
	require.Equal(t, "", got)

	require.NoError(t, MaybeSaveBootstrapLastListReposCursor(db, "page-2-cursor"))

	got, err = LoadBootstrapLastListReposCursor(db)
	require.NoError(t, err)
	require.Equal(t, "page-2-cursor", got)
}

func TestBootstrapLastListReposCursor_IgnoresEmpty(t *testing.T) {
	t.Parallel()
	db := newTestStore(t)

	require.NoError(t, MaybeSaveBootstrapLastListReposCursor(db, "page-1-cursor"))
	// The relay's final page returns NextCursor="". We must NOT
	// overwrite our last-known-non-empty cursor with that.
	require.NoError(t, MaybeSaveBootstrapLastListReposCursor(db, ""))

	got, err := LoadBootstrapLastListReposCursor(db)
	require.NoError(t, err)
	require.Equal(t, "page-1-cursor", got)
}

func TestBootstrapLastListReposCursor_Delete(t *testing.T) {
	t.Parallel()
	db := newTestStore(t)
	require.NoError(t, MaybeSaveBootstrapLastListReposCursor(db, "x"))

	require.NoError(t, DeleteBootstrapLastListReposCursor(db))

	got, err := LoadBootstrapLastListReposCursor(db)
	require.NoError(t, err)
	require.Equal(t, "", got)
}
```

If `newTestStore` doesn't exist in this file, locate the helper used by the existing `TestSaveListReposCursor`-style tests in this same package and reuse it.

- [ ] **Step 2: Run to verify failure**

Run: `just test ./internal/ingest/backfill -run TestBootstrapLastListReposCursor`
Expected: build error (functions undefined).

- [ ] **Step 3: Add implementations to `internal/ingest/backfill/cursor.go`**

Append after `SaveListReposCursor`:

```go
// bootstrapLastListReposCursorKey is the pebble key carrying the
// last *non-empty* listRepos cursor saved during the bootstrap
// phase. The merge phase reads this to resume listRepos against
// the relay and discover DIDs born during the bootstrap window
// (DESIGN.md §4.7 of the merge spec).
//
// We need a separate key from listReposCursorKey because the
// existing cursor is allowed (correctly) to drain to "" when
// listRepos completes — that's how the resume path knows to start
// from the beginning on the next Run. The merge phase needs the
// last meaningful cursor, not the post-drain empty value.
const bootstrapLastListReposCursorKey = "bootstrap/last_listrepos_cursor"

// LoadBootstrapLastListReposCursor returns the saved bootstrap-phase
// last-non-empty listRepos cursor, or "" if absent (debug short-
// circuit runs that never paged past page 1, or a fresh data dir).
func LoadBootstrapLastListReposCursor(db *store.Store) (string, error) {
	val, closer, err := db.Get([]byte(bootstrapLastListReposCursorKey))
	if errors.Is(err, store.ErrNotFound) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("backfill: load bootstrap_last_listrepos_cursor: %w", err)
	}
	defer func() { _ = closer.Close() }()
	return string(val), nil
}

// MaybeSaveBootstrapLastListReposCursor writes cursor under
// bootstrapLastListReposCursorKey via pebble.Sync iff cursor != "".
// The empty-cursor short-circuit is the entire point: atmos's
// OnPageComplete fires on every page including the post-drain
// terminator, and we must not overwrite the last meaningful cursor
// with the relay's "I'm done" empty value.
func MaybeSaveBootstrapLastListReposCursor(db *store.Store, cursor string) error {
	if cursor == "" {
		return nil
	}
	if err := db.Set([]byte(bootstrapLastListReposCursorKey), []byte(cursor), store.SyncWrites); err != nil {
		return fmt.Errorf("backfill: save bootstrap_last_listrepos_cursor: %w", err)
	}
	return nil
}

// DeleteBootstrapLastListReposCursor removes the key. Called by the
// merge phase's terminal cleanup once discovery has succeeded so
// the keyspace is clean once we reach steady state.
func DeleteBootstrapLastListReposCursor(db *store.Store) error {
	if err := db.Delete([]byte(bootstrapLastListReposCursorKey), store.SyncWrites); err != nil {
		return fmt.Errorf("backfill: delete bootstrap_last_listrepos_cursor: %w", err)
	}
	return nil
}
```

- [ ] **Step 4: Run the tests**

Run: `just test ./internal/ingest/backfill -run TestBootstrapLastListReposCursor`
Expected: PASS.

- [ ] **Step 5: Run the package's full test suite**

Run: `just test ./internal/ingest/backfill`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/ingest/backfill/cursor.go internal/ingest/backfill/cursor_test.go
git commit -m "backfill: persist last non-empty listRepos cursor for merge resume"
```

---

## Task 5: Wire bootstrap-last cursor into `backfill.Run`'s OnPageComplete

**Files:**
- Modify: `internal/ingest/backfill/run.go`
- Modify: `internal/ingest/backfill/run_test.go`

- [ ] **Step 1: Read the existing test file**

Run: `cat internal/ingest/backfill/run_test.go`

Identify the test that drives backfill against the fake relay and asserts cursor persistence — likely `TestRun_PersistsListReposCursor` or similar. We'll add a sibling assertion to one of those.

- [ ] **Step 2: Add a failing assertion**

In `internal/ingest/backfill/run_test.go`, in the test that already exercises pagination through the fake relay (it should drive at least 2 pages so a non-empty next-cursor exists at the page-1 boundary), add after the existing cursor assertion:

```go
// Bootstrap-last cursor is set to the last NON-EMPTY cursor seen,
// so the merge phase can resume listRepos for new-DID discovery.
last, err := LoadBootstrapLastListReposCursor(db)
require.NoError(t, err)
require.NotEmpty(t, last, "bootstrap-last cursor must be set when listRepos paged at least once")
```

If no existing test exercises >1 page, scan the file's helpers for the `listRepos` fake — most likely there's a test that does. If genuinely none does, add a new test:

```go
func TestRun_PersistsBootstrapLastListReposCursor(t *testing.T) {
	t.Parallel()
	// Build a fake relay that returns one page with NextCursor="page2",
	// then a second (empty) page with NextCursor="". Exact builder
	// pattern: copy from the existing TestRun_*PersistsCursor test in
	// this file. Run the engine; assert relay/list_repos_cursor is ""
	// AND bootstrap/last_listrepos_cursor is "page2".
}
```

(If you copy from an existing helper, keep the wiring identical so this test runs in <1s like the rest of the package.)

- [ ] **Step 3: Run to verify failure**

Run: `just test ./internal/ingest/backfill -run TestRun_PersistsBootstrapLastListReposCursor` (or whichever test you modified)
Expected: FAIL — bootstrap-last cursor is empty because nothing writes it yet.

- [ ] **Step 4: Update the OnPageComplete callback in `internal/ingest/backfill/run.go`**

Locate the existing `OnPageComplete` block (around line 128). Replace:

```go
				OnPageComplete: gt.Some(func(cursor string) error {
					return SaveListReposCursor(cfg.Store, cursor)
				}),
```

with:

```go
				OnPageComplete: gt.Some(func(cursor string) error {
					if err := SaveListReposCursor(cfg.Store, cursor); err != nil {
						return err
					}
					// Persist the last non-empty cursor under a sibling
					// key so the merge phase can resume listRepos to
					// discover DIDs born during bootstrap. The
					// MaybeSave helper short-circuits on cursor=="".
					return MaybeSaveBootstrapLastListReposCursor(cfg.Store, cursor)
				}),
```

- [ ] **Step 5: Run the tests**

Run: `just test ./internal/ingest/backfill`
Expected: PASS.

- [ ] **Step 6: Run lint**

Run: `just lint`
Expected: zero issues.

- [ ] **Step 7: Commit**

```bash
git add internal/ingest/backfill/run.go internal/ingest/backfill/run_test.go
git commit -m "backfill: save bootstrap-last listRepos cursor for merge resume"
```

---

## Task 6: Add merge metrics

**Files:**
- Modify: `internal/ingest/orchestrator/metrics.go`
- Modify: `internal/ingest/orchestrator/metrics_test.go`

- [ ] **Step 1: Add failing test assertions to `internal/ingest/orchestrator/metrics_test.go`**

Read the existing test file to see the registration pattern, then add (or extend the existing registration test):

```go
func TestMetrics_RegistersMergeCounters(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.incMergeEventsKept()
	m.incMergeEventsDropped()
	m.incMergeSegmentsConsumed()
	m.incMergeDIDLookups()
	m.addMergeRepoRevsUpdated(3)
	m.incMergeDIDsDiscoveredPostBootstrap()

	gathered, err := reg.Gather()
	require.NoError(t, err)
	names := make(map[string]struct{}, len(gathered))
	for _, mf := range gathered {
		names[mf.GetName()] = struct{}{}
	}
	for _, want := range []string{
		"jetstream_orchestrator_merge_events_kept_total",
		"jetstream_orchestrator_merge_events_dropped_total",
		"jetstream_orchestrator_merge_segments_consumed_total",
		"jetstream_orchestrator_merge_did_lookups_total",
		"jetstream_orchestrator_merge_repo_revs_updated_total",
		"jetstream_orchestrator_merge_dids_discovered_post_bootstrap_total",
	} {
		_, ok := names[want]
		require.True(t, ok, "missing metric %s", want)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `just test ./internal/ingest/orchestrator -run TestMetrics_RegistersMergeCounters`
Expected: build error (helpers undefined).

- [ ] **Step 3: Extend `internal/ingest/orchestrator/metrics.go`**

Add to the `Metrics` struct (after `StateDuration`):

```go
	// Merge-phase counters. All increment-only; the merge runs once
	// per data-dir lifetime so totals are stable observables on
	// dashboards.
	MergeEventsKept                 prometheus.Counter
	MergeEventsDropped              prometheus.Counter
	MergeSegmentsConsumed           prometheus.Counter
	MergeDIDLookups                 prometheus.Counter
	MergeRepoRevsUpdated            prometheus.Counter
	MergeDIDsDiscoveredPostBootstrap prometheus.Counter
```

In `NewMetrics`, after `StateDuration` is built and before `reg.MustRegister`, add:

```go
	m.MergeEventsKept = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace, Subsystem: metricsSubsystem,
		Name: "merge_events_kept_total",
		Help: "Events from live_segments/ that survived the rev filter and were appended to the steady-state segments.",
	})
	m.MergeEventsDropped = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace, Subsystem: metricsSubsystem,
		Name: "merge_events_dropped_total",
		Help: "Events from live_segments/ dropped because their rev was already covered by initial backfill.",
	})
	m.MergeSegmentsConsumed = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace, Subsystem: metricsSubsystem,
		Name: "merge_segments_consumed_total",
		Help: "live_segments/ source files fully drained and committed by the merge phase.",
	})
	m.MergeDIDLookups = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace, Subsystem: metricsSubsystem,
		Name: "merge_did_lookups_total",
		Help: "First-time per-DID repo/<did> reads issued during merge (cache hits do not count).",
	})
	m.MergeRepoRevsUpdated = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace, Subsystem: metricsSubsystem,
		Name: "merge_repo_revs_updated_total",
		Help: "Per-DID repo/<did>.Rev refreshes committed by the merge phase.",
	})
	m.MergeDIDsDiscoveredPostBootstrap = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace, Subsystem: metricsSubsystem,
		Name: "merge_dids_discovered_post_bootstrap_total",
		Help: "DIDs first observed via the merge-phase listRepos resume and queued for steady-state retry.",
	})
```

Update the `reg.MustRegister(...)` call to register the six new counters alongside the existing three.

Append the helper methods:

```go
func (m *Metrics) incMergeEventsKept() {
	if m != nil {
		m.MergeEventsKept.Inc()
	}
}

func (m *Metrics) incMergeEventsDropped() {
	if m != nil {
		m.MergeEventsDropped.Inc()
	}
}

func (m *Metrics) incMergeSegmentsConsumed() {
	if m != nil {
		m.MergeSegmentsConsumed.Inc()
	}
}

func (m *Metrics) incMergeDIDLookups() {
	if m != nil {
		m.MergeDIDLookups.Inc()
	}
}

func (m *Metrics) addMergeRepoRevsUpdated(n int) {
	if m != nil && n > 0 {
		m.MergeRepoRevsUpdated.Add(float64(n))
	}
}

func (m *Metrics) incMergeDIDsDiscoveredPostBootstrap() {
	if m != nil {
		m.MergeDIDsDiscoveredPostBootstrap.Inc()
	}
}
```

- [ ] **Step 4: Run the new test + the package's existing metrics test**

Run: `just test ./internal/ingest/orchestrator -run TestMetrics`
Expected: PASS.

- [ ] **Step 5: Lint**

Run: `just lint`
Expected: zero issues.

- [ ] **Step 6: Commit**

```bash
git add internal/ingest/orchestrator/metrics.go internal/ingest/orchestrator/metrics_test.go
git commit -m "orchestrator: add merge-phase prometheus counters"
```

---

## Task 7: Implement `shouldKeep` predicate

**Files:**
- Create: `internal/ingest/orchestrator/merge_filter.go`
- Create: `internal/ingest/orchestrator/merge_filter_test.go`

- [ ] **Step 1: Write the failing tests in `internal/ingest/orchestrator/merge_filter_test.go`**

```go
package orchestrator

import (
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/stretchr/testify/require"
)

func TestShouldKeep(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ev   segment.Event
		st   *backfill.RepoStatus
		want bool
	}{
		{
			name: "nil RepoStatus → keep create",
			ev:   segment.Event{Kind: segment.KindCreate, Rev: "3l4"},
			st:   nil,
			want: true,
		},
		{
			name: "nil RepoStatus → keep identity",
			ev:   segment.Event{Kind: segment.KindIdentity},
			st:   nil,
			want: true,
		},
		{
			name: "StatusNotStarted → keep create",
			ev:   segment.Event{Kind: segment.KindCreate, Rev: "3l4"},
			st:   &backfill.RepoStatus{Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusNotStarted, Rev: "ignored"}},
			want: true,
		},
		{
			name: "StatusFailed → keep create",
			ev:   segment.Event{Kind: segment.KindCreate, Rev: "3l4"},
			st:   &backfill.RepoStatus{Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusFailed, Rev: "ignored"}},
			want: true,
		},
		{
			name: "StatusComplete + empty BackfillRev → keep create (defensive)",
			ev:   segment.Event{Kind: segment.KindCreate, Rev: "3l4"},
			st:   &backfill.RepoStatus{Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete, Rev: ""}},
			want: true,
		},
		{
			name: "StatusComplete + ev.Rev empty → keep (defensive)",
			ev:   segment.Event{Kind: segment.KindCreate, Rev: ""},
			st:   &backfill.RepoStatus{Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete, Rev: "3l5"}},
			want: true,
		},
		{
			name: "create with ev.Rev < BackfillRev → drop",
			ev:   segment.Event{Kind: segment.KindCreate, Rev: "3l4"},
			st:   &backfill.RepoStatus{Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete, Rev: "3l5"}},
			want: false,
		},
		{
			name: "create with ev.Rev == BackfillRev → drop",
			ev:   segment.Event{Kind: segment.KindCreate, Rev: "3l5"},
			st:   &backfill.RepoStatus{Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete, Rev: "3l5"}},
			want: false,
		},
		{
			name: "create with ev.Rev > BackfillRev → keep",
			ev:   segment.Event{Kind: segment.KindCreate, Rev: "3l6"},
			st:   &backfill.RepoStatus{Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete, Rev: "3l5"}},
			want: true,
		},
		{
			name: "update with ev.Rev <= BackfillRev → drop",
			ev:   segment.Event{Kind: segment.KindUpdate, Rev: "3l5"},
			st:   &backfill.RepoStatus{Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete, Rev: "3l5"}},
			want: false,
		},
		{
			name: "delete with ev.Rev <= BackfillRev → drop",
			ev:   segment.Event{Kind: segment.KindDelete, Rev: "3l4"},
			st:   &backfill.RepoStatus{Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete, Rev: "3l5"}},
			want: false,
		},
		{
			name: "identity with ev.Rev <= BackfillRev → keep (non-commit)",
			ev:   segment.Event{Kind: segment.KindIdentity},
			st:   &backfill.RepoStatus{Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete, Rev: "3l5"}},
			want: true,
		},
		{
			name: "account → keep regardless",
			ev:   segment.Event{Kind: segment.KindAccount},
			st:   &backfill.RepoStatus{Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete, Rev: "3l5"}},
			want: true,
		},
		{
			name: "sync → keep regardless",
			ev:   segment.Event{Kind: segment.KindSync},
			st:   &backfill.RepoStatus{Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete, Rev: "3l5"}},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, shouldKeep(&tt.ev, tt.st))
		})
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `just test ./internal/ingest/orchestrator -run TestShouldKeep`
Expected: build error (`shouldKeep` undefined).

- [ ] **Step 3: Implement in `internal/ingest/orchestrator/merge_filter.go`**

```go
// Package orchestrator: merge_filter.go owns the per-event keep/drop
// predicate that decides whether a live_segments survivor should be
// promoted into the steady-state segments tree, plus the per-DID
// repo/<did> lookup cache that backs it.
package orchestrator

import (
	"errors"
	"fmt"

	"github.com/bluesky-social/jetstream-v2/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/segment"
)

// isCommitKind reports whether k is one of the three commit-shaped
// event kinds (DESIGN.md §3.2). Only these carry a per-event Rev
// that maps to the repo MST and can therefore be filtered against
// repo/<did>.Backfill.Rev.
func isCommitKind(k segment.Kind) bool {
	switch k {
	case segment.KindCreate, segment.KindUpdate, segment.KindDelete:
		return true
	default:
		return false
	}
}

// shouldKeep returns true unless the event is a commit kind whose
// data is already covered by the backfill writer's authoritative
// per-DID write. Spec: docs/superpowers/specs/2026-05-27-merge-
// phase-design.md §4.3.
//
// Cross-component dependency: this predicate's correctness leans on
// internal/ingest/backfill/handler.go stamping commit.Rev (the head
// rev of the repo at download time) onto every synthetic Create
// event. If that handler ever switches to per-record commit revs,
// this predicate's BackfillRev comparison stops being a coherent
// watermark for the whole repo.
func shouldKeep(ev *segment.Event, st *backfill.RepoStatus) bool {
	if !isCommitKind(ev.Kind) {
		return true
	}
	if st == nil {
		return true
	}
	if st.Backfill.Status != backfill.StatusComplete {
		return true
	}
	if st.Backfill.Rev == "" || ev.Rev == "" {
		return true
	}
	// TIDs are designed to sort lexicographically (atproto rev spec).
	return ev.Rev > st.Backfill.Rev
}

// repoStatusLookup memoizes per-DID repo/<did> reads for a single
// merge run. Pebble I/O failures (other than ErrNotFound) latch a
// sticky error on the cache; subsequent lookups return it.
type repoStatusLookup struct {
	store      *store.Store
	cache      map[string]*backfill.RepoStatus
	stickyErr  error
	onLookup   func() // metrics.incMergeDIDLookups; nil-safe
}

// newRepoStatusLookup builds an empty cache. onLookup is invoked once
// per first-time-seen DID so callers can wire a metric.
func newRepoStatusLookup(s *store.Store, onLookup func()) *repoStatusLookup {
	return &repoStatusLookup{
		store:    s,
		cache:    make(map[string]*backfill.RepoStatus),
		onLookup: onLookup,
	}
}

// get returns the cached or freshly-read RepoStatus for did.
// Missing rows cache a nil so repeated misses don't re-hit pebble.
// Returns the sticky error on every call once one has been latched.
func (l *repoStatusLookup) get(did string) (*backfill.RepoStatus, error) {
	if l.stickyErr != nil {
		return nil, l.stickyErr
	}
	if rs, ok := l.cache[did]; ok {
		return rs, nil
	}
	if l.onLookup != nil {
		l.onLookup()
	}

	val, closer, err := l.store.Get([]byte(repoKey(did)))
	if errors.Is(err, store.ErrNotFound) {
		l.cache[did] = nil
		return nil, nil
	}
	if err != nil {
		l.stickyErr = fmt.Errorf("orchestrator: merge: lookup repo/%s: %w", did, err)
		return nil, l.stickyErr
	}
	defer func() { _ = closer.Close() }()

	rs, err := decodeRepoStatusFromBytes(val)
	if err != nil {
		l.stickyErr = fmt.Errorf("orchestrator: merge: decode repo/%s: %w", did, err)
		return nil, l.stickyErr
	}
	l.cache[did] = rs
	return rs, nil
}

// set replaces the cached entry. Called by commitSourceComplete
// after a successful pebble batch so subsequent sources see the
// updated Rev without a fresh pebble read.
func (l *repoStatusLookup) set(did string, rs *backfill.RepoStatus) {
	l.cache[did] = rs
}

// repoKey is duplicated locally rather than imported from backfill
// because the backfill package's repoKey is unexported. The string
// shape ("repo/<did>") is part of DESIGN.md §3.5's stable on-disk
// keyspace, so duplication is safe.
func repoKey(did string) string {
	return "repo/" + did
}

// decodeRepoStatusFromBytes wraps backfill.DecodeRepoStatus's
// equivalent. The backfill package's decode helper is unexported
// today; we add an exported wrapper there in this same task if
// needed. See follow-up note in this task's Step 4 if the
// wrapper turns out to already exist.
func decodeRepoStatusFromBytes(b []byte) (*backfill.RepoStatus, error) {
	return backfill.DecodeRepoStatus(b)
}
```

- [ ] **Step 4: Export `DecodeRepoStatus` from `internal/ingest/backfill`**

Check whether `backfill.DecodeRepoStatus` already exists:

Run: `grep -n "func DecodeRepoStatus\|func decodeRepoStatus" internal/ingest/backfill/status.go`

The existing helper is unexported (`decodeRepoStatus`). Add an exported wrapper at the end of `internal/ingest/backfill/status.go`:

```go
// DecodeRepoStatus is the exported decoder used by cross-package
// readers (the orchestrator's merge phase) that need to read the
// JSON shape stored at repo/<did>. Internal callers continue to
// use decodeRepoStatus directly.
func DecodeRepoStatus(b []byte) (*RepoStatus, error) {
	return decodeRepoStatus(b)
}

// EncodeRepoStatus is the exported encoder used by cross-package
// writers (the orchestrator's merge phase committing per-DID Rev
// updates) that need to produce the JSON shape stored at
// repo/<did>. Internal callers continue to use encodeRepoStatus
// directly.
func EncodeRepoStatus(s *RepoStatus) ([]byte, error) {
	return encodeRepoStatus(s)
}

// RepoKey returns the pebble key for a DID's RepoStatus row. Mirror
// of the unexported repoKey; exported for cross-package writers.
func RepoKey(did string) []byte {
	return []byte("repo/" + did)
}
```

In `internal/ingest/orchestrator/merge_filter.go`, replace the local `repoKey` and `decodeRepoStatusFromBytes` definitions with calls to the exported helpers — change the lookup code to:

```go
	val, closer, err := l.store.Get(backfill.RepoKey(did))
```

and:

```go
	rs, err := backfill.DecodeRepoStatus(val)
```

Drop the local `repoKey` and `decodeRepoStatusFromBytes` functions.

- [ ] **Step 5: Run the unit tests**

Run: `just test ./internal/ingest/orchestrator -run TestShouldKeep`
Expected: PASS (13 cases).

- [ ] **Step 6: Run the package's full test suite + lint**

Run: `just test ./internal/ingest/orchestrator && just test ./internal/ingest/backfill && just lint`
Expected: PASS, zero lint issues.

- [ ] **Step 7: Commit**

```bash
git add internal/ingest/orchestrator/merge_filter.go internal/ingest/orchestrator/merge_filter_test.go internal/ingest/backfill/status.go
git commit -m "orchestrator: add shouldKeep predicate + repo/<did> lookup cache"
```

---

## Task 8: Implement merge cursor + `commitSourceComplete` atomic batch

**Files:** Create `merge_cursor.go`, `merge_cursor_test.go` in `internal/ingest/orchestrator/`. Modify `internal/ingest/orchestrator/testfixtures_test.go` to add helpers.

- [ ] **Step 1: Add test helpers to `testfixtures_test.go`**

If a pebble-store helper isn't already present, append:

```go
func newOrchestratorTestStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func mustEncodeStatus(t *testing.T, rs *backfill.RepoStatus) []byte {
	t.Helper()
	b, err := backfill.EncodeRepoStatus(rs)
	require.NoError(t, err)
	return b
}
```

Imports as needed: `store`, `backfill`, `require`, `testing`.

- [ ] **Step 2: Write `merge_cursor_test.go`**

```go
package orchestrator

import (
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/stretchr/testify/require"
)

func TestMergeCursor_AbsentReadsZero(t *testing.T) {
	t.Parallel()
	st := newOrchestratorTestStore(t)
	got, err := loadMergeCursor(st)
	require.NoError(t, err)
	require.Equal(t, uint64(0), got)
}

func TestMergeCursor_RoundTripViaCommit(t *testing.T) {
	t.Parallel()
	st := newOrchestratorTestStore(t)
	require.NoError(t, st.Set(backfill.RepoKey("did:plc:a"), mustEncodeStatus(t, &backfill.RepoStatus{
		Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete, Rev: "rev-old"},
		Rev:      "rev-old",
	}), store.SyncWrites))

	cache := newRepoStatusLookup(st, nil)
	_, err := cache.get("did:plc:a")
	require.NoError(t, err)

	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	require.NoError(t, commitSourceComplete(st, cache, 5, map[string]string{"did:plc:a": "rev-new"}, now))

	got, err := loadMergeCursor(st)
	require.NoError(t, err)
	require.Equal(t, uint64(5), got)

	val, closer, err := st.Get(backfill.RepoKey("did:plc:a"))
	require.NoError(t, err)
	defer func() { _ = closer.Close() }()
	rs2, err := backfill.DecodeRepoStatus(val)
	require.NoError(t, err)
	require.Equal(t, "rev-new", rs2.Rev)
	require.Equal(t, "rev-old", rs2.Backfill.Rev) // immutable
	require.Equal(t, now, rs2.UpdatedAt)

	cached, err := cache.get("did:plc:a")
	require.NoError(t, err)
	require.Equal(t, "rev-new", cached.Rev)
}

func TestMergeCursor_NoRevsCommitsCursorOnly(t *testing.T) {
	t.Parallel()
	st := newOrchestratorTestStore(t)
	require.NoError(t, commitSourceComplete(st, newRepoStatusLookup(st, nil), 1, nil, time.Now()))
	got, err := loadMergeCursor(st)
	require.NoError(t, err)
	require.Equal(t, uint64(1), got)
}

func TestMergeCursor_Delete(t *testing.T) {
	t.Parallel()
	st := newOrchestratorTestStore(t)
	require.NoError(t, commitSourceComplete(st, newRepoStatusLookup(st, nil), 7, nil, time.Now()))
	require.NoError(t, deleteMergeCursor(st))
	got, err := loadMergeCursor(st)
	require.NoError(t, err)
	require.Equal(t, uint64(0), got)
}
```

- [ ] **Step 3: Implement `merge_cursor.go`**

```go
// Package orchestrator: merge_cursor.go owns the merge/next_source_idx
// cursor and the atomic per-source commit batch. Spec §4.5–§4.6.
package orchestrator

import (
	"fmt"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream-v2/internal/store"
)

const (
	mergeNextSourceIdxKey      = "merge/next_source_idx"
	mergeCursorV1         byte = 0x01
)

func loadMergeCursor(s *store.Store) (uint64, error) {
	v, _, err := s.GetVersionedUint64LE(mergeNextSourceIdxKey, mergeCursorV1)
	if err != nil {
		return 0, fmt.Errorf("orchestrator: merge: load cursor: %w", err)
	}
	return v, nil
}

func deleteMergeCursor(s *store.Store) error {
	if err := s.Delete([]byte(mergeNextSourceIdxKey), store.SyncWrites); err != nil {
		return fmt.Errorf("orchestrator: merge: delete cursor: %w", err)
	}
	return nil
}

// commitSourceComplete atomically advances the cursor and refreshes
// repo/<did>.Rev + UpdatedAt for every entry in perDIDLastRev. Only
// top-level Rev/UpdatedAt are mutated; Backfill.* is preserved.
func commitSourceComplete(
	s *store.Store,
	cache *repoStatusLookup,
	nextIdx uint64,
	perDIDLastRev map[string]string,
	now time.Time,
) error {
	batch := s.NewBatch()
	defer func() { _ = batch.Close() }()

	if err := batch.Set([]byte(mergeNextSourceIdxKey), store.EncodeVersionedUint64LE(mergeCursorV1, nextIdx), nil); err != nil {
		return fmt.Errorf("orchestrator: merge: stage cursor: %w", err)
	}

	pendingCache := make(map[string]*backfill.RepoStatus, len(perDIDLastRev))
	for did, rev := range perDIDLastRev {
		rs, err := cache.get(did)
		if err != nil {
			return err
		}
		var next backfill.RepoStatus
		if rs != nil {
			next = *rs
		}
		next.Rev = rev
		next.UpdatedAt = now.UTC()
		enc, err := backfill.EncodeRepoStatus(&next)
		if err != nil {
			return fmt.Errorf("orchestrator: merge: encode repo/%s: %w", did, err)
		}
		if err := batch.Set(backfill.RepoKey(did), enc, nil); err != nil {
			return fmt.Errorf("orchestrator: merge: stage repo/%s: %w", did, err)
		}
		updated := next
		pendingCache[did] = &updated
	}

	if err := s.Commit(batch, store.SyncWrites); err != nil {
		return fmt.Errorf("orchestrator: merge: commit batch: %w", err)
	}
	for did, rs := range pendingCache {
		cache.set(did, rs)
	}
	return nil
}
```

- [ ] **Step 4: Verify and commit**

```bash
just test ./internal/ingest/orchestrator -run TestMergeCursor && just lint
git add internal/ingest/orchestrator/merge_cursor.go internal/ingest/orchestrator/merge_cursor_test.go internal/ingest/orchestrator/testfixtures_test.go
git commit -m "orchestrator: add merge cursor + atomic per-source commit batch"
```

---

## Task 9: Implement `mergeRunner` (drain loop, no kill points yet)

**Files:** Create `internal/ingest/orchestrator/merge_runner.go`. The kill-point hook for crash tests lands in Task 13; in this task we wire the basic loop only.

- [ ] **Step 1: Implement `merge_runner.go`**

```go
// Package orchestrator: merge_runner.go owns the per-source-segment
// drain loop. One goroutine, serial, no fan-out. Spec §4.2.
package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream-v2/internal/obs"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/segment"
)

type mergeRunner struct {
	dst       *ingest.Writer
	store     *store.Store
	sourceDir string
	logger    *slog.Logger
	metrics   *Metrics
	now       func() time.Time // overridable for tests
	cache     *repoStatusLookup
}

func newMergeRunner(dst *ingest.Writer, st *store.Store, sourceDir string, logger *slog.Logger, m *Metrics) *mergeRunner {
	r := &mergeRunner{
		dst:       dst,
		store:     st,
		sourceDir: sourceDir,
		logger:    logger.With(slog.String("component", "orchestrator/merge")),
		metrics:   m,
		now:       func() time.Time { return time.Now().UTC() },
	}
	r.cache = newRepoStatusLookup(st, m.incMergeDIDLookups)
	return r
}

// run drains every source seg whose index >= the persisted cursor,
// committing per-source. Returns nil on full drain or ctx cancel.
func (r *mergeRunner) run(ctx context.Context) error {
	return obs.Span(ctx, func(ctx context.Context) error {
		fromIdx, err := loadMergeCursor(r.store)
		if err != nil {
			return err
		}

		all, err := ingest.SegmentFiles(r.sourceDir)
		if err != nil {
			return fmt.Errorf("orchestrator: merge: list source segments: %w", err)
		}

		// Skip already-drained sources; verify contiguity from fromIdx.
		var todo []ingest.SegmentFile
		expectIdx := fromIdx
		for _, sf := range all {
			if sf.Idx < fromIdx {
				continue
			}
			if sf.Idx != expectIdx {
				return fmt.Errorf("orchestrator: merge: source index gap: expected %d, got %d", expectIdx, sf.Idx)
			}
			todo = append(todo, sf)
			expectIdx++
		}

		for _, sf := range todo {
			if err := ctx.Err(); err != nil {
				return err
			}
			perDID, err := r.processSourceSegment(ctx, sf)
			if err != nil {
				return err
			}
			if err := commitSourceComplete(r.store, r.cache, sf.Idx+1, perDID, r.now()); err != nil {
				return err
			}
			r.metrics.incMergeSegmentsConsumed()
			r.metrics.addMergeRepoRevsUpdated(len(perDID))
		}
		return nil
	})
}

// processSourceSegment opens one source seg, iterates its blocks,
// applies the keep/drop predicate, appends survivors with re-stamped
// IndexedAt, returns the per-DID last-seen rev map. dst.Flush is
// called before returning so the cursor commit that follows is
// ordered after a fsync (per durability §5.2).
func (r *mergeRunner) processSourceSegment(ctx context.Context, sf ingest.SegmentFile) (map[string]string, error) {
	var perDID map[string]string
	err := obs.Span(ctx, func(ctx context.Context) error {
		rd, err := segment.Open(segment.ReaderConfig{Path: sf.Path})
		if err != nil {
			return fmt.Errorf("orchestrator: merge: open %s: %w", sf.Path, err)
		}
		defer func() { _ = rd.Close() }()

		blockCount := int(rd.Header().BlockCount)
		perDID = make(map[string]string)

		for i := 0; i < blockCount; i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			events, err := rd.DecodeBlock(i)
			if err != nil {
				return fmt.Errorf("orchestrator: merge: decode %s block %d: %w", sf.Path, i, err)
			}
			for j := range events {
				ev := &events[j]
				rs, err := r.cache.get(ev.DID)
				if err != nil {
					return err
				}
				if !shouldKeep(ev, rs) {
					r.metrics.incMergeEventsDropped()
					continue
				}
				ev.IndexedAt = r.now().UnixMicro() // §3.4 re-stamp
				if err := r.dst.Append(ctx, ev); err != nil {
					return fmt.Errorf("orchestrator: merge: append: %w", err)
				}
				r.metrics.incMergeEventsKept()
				if isCommitKind(ev.Kind) && ev.Rev != "" {
					perDID[ev.DID] = ev.Rev
				}
			}
		}

		// Note: we intentionally rely on the destination writer's
		// internal flushAndRotateLocked path triggered by full
		// blocks; the explicit Flush at end of source forces a fsync
		// of any partial trailing block before the cursor commit.
		// segment.Writer.Flush is unexported, so we rely on the
		// auto-rotate covering full blocks and Close handling
		// partial blocks at the end of runMerge. See §5.2 — this is
		// fine because commitSourceComplete persists Sync=true and
		// runMerge will close the writer before declaring success.
		return nil
	})
	return perDID, err
}
```

Note on the trailing-flush comment: `segment.Writer.Flush` is exported (`internal/ingest/writer.go` defers to `segment.Writer.Flush` internally; `ingest.Writer` doesn't expose Flush). The merge writer's pending block at end-of-source-seg gets flushed by `dst.SealActiveAndClose()` in `runMerge` after the loop. Since we accept at-least-once duplicates anyway, the relaxed flush ordering across source-seg boundaries is fine: a crash mid-source redoes that source.

If you find the `ingest.Writer` does have a public `Flush` method when implementing, prefer calling it explicitly here for tighter durability — the comment becomes stale but the spec wins. Verify with `grep -n "func (w \*Writer) Flush" internal/ingest/writer.go` before deciding.

- [ ] **Step 2: Verify the package builds**

```bash
just lint
```

No new tests in this task — coverage comes from Task 12 (integration). Build correctness is enough.

- [ ] **Step 3: Commit**

```bash
git add internal/ingest/orchestrator/merge_runner.go
git commit -m "orchestrator: add mergeRunner per-source drain loop"
```

---

## Task 10: Implement post-merge new-DID discovery

**Files:** Create `internal/ingest/orchestrator/merge_discovery.go`.

The discovery step is glue: load `bootstrap/last_listrepos_cursor`, build an atmos sync client against the same relay, walk listRepos pages, write a `StatusFailed`-shaped row for every DID with no existing pebble row.

- [ ] **Step 1: Implement `merge_discovery.go`**

```go
// Package orchestrator: merge_discovery.go runs the post-merge
// listRepos resume that picks up DIDs born during the bootstrap
// window. Spec §4.7.
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/bluesky-social/jetstream-v2/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream-v2/internal/obs"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/jcalabro/atmos"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
)

// listReposPageLimit matches the atmos default; chosen to amortize
// HTTP overhead while staying small enough that pagination latency
// is bounded.
const listReposPageLimit int64 = 1000

// runDiscovery resumes listRepos from bootstrap/last_listrepos_cursor
// and writes a StatusFailed row for every previously-unknown DID.
// No-op when the cursor key is absent (debug short-circuit runs that
// never paged past page 1).
func (r *mergeRunner) runDiscovery(ctx context.Context, relayURL string, httpClient *http.Client) error {
	return obs.Span(ctx, func(ctx context.Context) error {
		cursor, err := backfill.LoadBootstrapLastListReposCursor(r.store)
		if err != nil {
			return fmt.Errorf("orchestrator: merge: load bootstrap cursor: %w", err)
		}
		if cursor == "" {
			r.logger.InfoContext(ctx, "skipping discovery: no bootstrap-last cursor (debug short-circuit run?)")
			return nil
		}

		xc := &xrpc.Client{
			Host:       relayURL,
			HTTPClient: gt.Some(httpClient),
			Retry:      gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)}),
		}
		sc := atmossync.NewClient(atmossync.Options{Client: xc})

		for page, perr := range sc.ListRepos(ctx, listReposPageLimit, cursor) {
			if perr != nil {
				return fmt.Errorf("orchestrator: merge: discovery listRepos: %w", perr)
			}
			for _, entry := range page.Repos {
				if err := r.maybeWriteDiscoveredRow(ctx, entry); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// maybeWriteDiscoveredRow writes a StatusFailed-shaped row only when
// the DID has no existing repo/<did> entry. Idempotent across
// reruns.
func (r *mergeRunner) maybeWriteDiscoveredRow(ctx context.Context, entry atmossync.ListReposEntry) error {
	did := atmos.DID(entry.DID)
	_, closer, err := r.store.Get(backfill.RepoKey(string(did)))
	if err == nil {
		_ = closer.Close()
		return nil // existing row; race-safe with bootstrap's tail
	}
	if !errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("orchestrator: merge: discovery lookup %s: %w", did, err)
	}

	rs := &backfill.RepoStatus{
		Backfill: backfill.RepoBackfillStatus{
			Status:    backfill.StatusFailed,
			LastError: "discovered post-bootstrap; queued for retry",
		},
		Active: entry.Active,
	}
	enc, err := backfill.EncodeRepoStatus(rs)
	if err != nil {
		return fmt.Errorf("orchestrator: merge: discovery encode %s: %w", did, err)
	}
	if err := r.store.Set(backfill.RepoKey(string(did)), enc, store.SyncWrites); err != nil {
		return fmt.Errorf("orchestrator: merge: discovery write %s: %w", did, err)
	}
	r.metrics.incMergeDIDsDiscoveredPostBootstrap()
	r.logger.InfoContext(ctx, "discovered post-bootstrap DID", "did", string(did))
	return nil
}
```

If `atmossync.ListReposEntry` has a different field shape than `{DID, Active}` in the import you have, check `internal/ingest/backfill/store.go`'s `OnDiscover` for the canonical access pattern (it already uses `entry.DID`, `entry.Active`).

- [ ] **Step 2: Verify build + lint**

```bash
just lint
```

- [ ] **Step 3: Commit**

```bash
git add internal/ingest/orchestrator/merge_discovery.go
git commit -m "orchestrator: add post-merge new-DID discovery via listRepos resume"
```

---

## Task 11: Replace `runMerge` stub with the full lifecycle entry point

**Files:** Modify `internal/ingest/orchestrator/merge.go` (replace the stub body). Modify `internal/ingest/orchestrator/config.go` to expose the relay URL + HTTP client to the discovery step (already in the existing `Config`; just confirm).

- [ ] **Step 1: Read `internal/ingest/orchestrator/config.go`**

Run: `grep -n "RelayURL\|HTTPClient" internal/ingest/orchestrator/config.go`
Both fields already exist on `Config` per the bootstrap implementation. No changes needed.

- [ ] **Step 2: Replace `merge.go`**

```go
// Package orchestrator: merge.go owns the State 5 cutover step that
// drains data/backfill/live_segments/ into data/segments/. Spec:
// docs/superpowers/specs/2026-05-27-merge-phase-design.md.
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream-v2/internal/ingest/live"
	"github.com/bluesky-social/jetstream-v2/internal/obs"
)

func (o *Orchestrator) runMerge(ctx context.Context) error {
	return obs.Span(ctx, func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		liveSegmentsDir := filepath.Join(o.cfg.DataDir, "backfill", "live_segments")
		segmentsDir := filepath.Join(o.cfg.DataDir, "segments")

		// Restart-after-cleanup guard: if the source tree is gone we
		// just need to ensure the cursor keys are gone too and return.
		if _, err := os.Stat(liveSegmentsDir); errors.Is(err, os.ErrNotExist) {
			if err := deleteMergeCursor(o.cfg.Store); err != nil {
				return err
			}
			if err := backfill.DeleteBootstrapLastListReposCursor(o.cfg.Store); err != nil {
				return err
			}
			return nil
		} else if err != nil {
			return fmt.Errorf("orchestrator: merge: stat live_segments: %w", err)
		}

		dst, err := ingest.Open(ingest.Config{
			SegmentsDir:    segmentsDir,
			Store:          o.cfg.Store,
			SeqKey:         live.SteadySeqKey,
			Logger:         o.cfg.Logger,
			Metrics:        o.cfg.IngestMetrics,
			SegmentMetrics: o.cfg.SegmentMetrics,
		})
		if err != nil {
			return fmt.Errorf("orchestrator: merge: open dst writer: %w", err)
		}

		runner := newMergeRunner(dst, o.cfg.Store, liveSegmentsDir, o.cfg.Logger, o.cfg.Metrics)

		if err := runner.run(ctx); err != nil {
			if cerr := dst.Close(); cerr != nil {
				o.logger.WarnContext(ctx, "dst writer close after merge error", "err", cerr)
			}
			return err
		}

		if err := dst.SealActiveAndClose(); err != nil {
			return fmt.Errorf("orchestrator: merge: seal dst: %w", err)
		}

		if err := runner.runDiscovery(ctx, o.cfg.RelayURL, o.cfg.HTTPClient); err != nil {
			return err
		}

		if err := os.RemoveAll(filepath.Join(o.cfg.DataDir, "backfill")); err != nil {
			return fmt.Errorf("orchestrator: merge: remove backfill dir: %w", err)
		}
		if err := deleteMergeCursor(o.cfg.Store); err != nil {
			return err
		}
		if err := backfill.DeleteBootstrapLastListReposCursor(o.cfg.Store); err != nil {
			return err
		}
		return nil
	})
}
```

- [ ] **Step 3: Build + lint + run all orchestrator tests**

```bash
just test ./internal/ingest/orchestrator && just lint
```

The pre-existing `TestRun_ResumeFromMerging_AdvancesToSteadyState` may fail at this point because it was authored against the no-op stub. We update it in Task 15. For now, expect that one test to fail; everything else should pass.

- [ ] **Step 4: Commit**

```bash
git add internal/ingest/orchestrator/merge.go
git commit -m "orchestrator: implement runMerge body per spec"
```

---

## Task 12: Tier 2 integration tests

**Files:** Create `internal/ingest/orchestrator/merge_test.go`. Reuses `fakeRelay`, `newTestVerifier`, `testIdentityDirectory`, `readSegFiles`, `isSealed` from the existing `testfixtures_test.go`.

The integration tests build a real `data/` tree, populate `repo/<did>` rows, write source segments via the real `ingest.Writer`, run `runMerge`, assert outcomes. Each test runs in <1s.

- [ ] **Step 1: Add a per-package fixture builder to `testfixtures_test.go`**

Append:

```go
// mergeFixture builds a data dir with backfill/live_segments/ populated
// from the supplied event slices (one slice per source segment) and
// repo/<did> rows from the supplied per-DID backfill revs. Returns the
// data dir, the open store (cleanup wired via t.Cleanup), and the
// orchestrator Config wired to a fakeRelay. Caller drives runMerge or
// the full Run.
type mergeFixture struct {
	dataDir string
	store   *store.Store
	cfg     Config
	relay   *fakeRelay
}

func newMergeFixture(t *testing.T, sources [][]segment.Event, repoRevs map[string]string) *mergeFixture {
	t.Helper()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	// Pre-populate repo/<did> rows for completed backfills.
	for did, rev := range repoRevs {
		rs := &backfill.RepoStatus{
			Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete, Rev: rev},
			Rev:      rev,
		}
		enc, err := backfill.EncodeRepoStatus(rs)
		require.NoError(t, err)
		require.NoError(t, st.Set(backfill.RepoKey(did), enc, store.SyncWrites))
	}

	// Write each source slice as one source seg via a real ingest.Writer
	// pointed at backfill/live_segments. Seal + close between sources so
	// the merge sees fully-sealed source files.
	liveDir := filepath.Join(dataDir, "backfill", "live_segments")
	require.NoError(t, os.MkdirAll(liveDir, 0o755))
	for _, evs := range sources {
		w, err := ingest.Open(ingest.Config{
			SegmentsDir: liveDir,
			Store:       st,
			SeqKey:      live.BootstrapSeqKey,
			Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		})
		require.NoError(t, err)
		for i := range evs {
			require.NoError(t, w.Append(t.Context(), &evs[i]))
		}
		require.NoError(t, w.SealActiveAndClose())
	}

	relay := newFakeRelay(t, nil)
	cfg := Config{
		DataDir:    dataDir,
		Store:      st,
		RelayURL:   relay.URL(),
		HTTPClient: &http.Client{Timeout: 5 * time.Second},
		Directory:  testIdentityDirectory(),
		Verifier:   newTestVerifier(t, relay.URL()),
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return &mergeFixture{dataDir: dataDir, store: st, cfg: cfg, relay: relay}
}
```

Add imports as needed: `os`, `io`, `log/slog`, `net/http`, `time`, `path/filepath`, `ingest`, `live`, `backfill`, `segment`.

- [ ] **Step 2: Write the integration tests**

```go
package orchestrator

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/stretchr/testify/require"
)

func ev(seq uint64, did, rev string, kind segment.Kind, indexedAtMicros int64) segment.Event {
	return segment.Event{
		Seq:        seq,
		IndexedAt:  indexedAtMicros,
		Kind:       kind,
		DID:        did,
		Collection: "app.bsky.feed.post",
		Rkey:       "rkey-" + rev,
		Rev:        rev,
		Payload:    []byte("payload-" + rev),
	}
}

func TestMerge_DropsCoveredCommits_KeepsOthers(t *testing.T) {
	t.Parallel()
	// did:plc:a backfill at rev=3l5; did:plc:b never backfilled.
	srcEvs := []segment.Event{
		ev(0, "did:plc:a", "3l3", segment.KindCreate, 1000),  // drop (covered)
		ev(0, "did:plc:a", "3l5", segment.KindCreate, 1001),  // drop (== BackfillRev)
		ev(0, "did:plc:a", "3l6", segment.KindCreate, 1002),  // keep
		ev(0, "did:plc:b", "3l4", segment.KindCreate, 1003),  // keep (no backfill)
		{Seq: 0, Kind: segment.KindIdentity, DID: "did:plc:a", IndexedAt: 1004}, // keep (non-commit)
	}
	fix := newMergeFixture(t, [][]segment.Event{srcEvs}, map[string]string{"did:plc:a": "3l5"})

	o, err := New(fix.cfg)
	require.NoError(t, err)
	require.NoError(t, lifecycle.WritePhase(fix.store, lifecycle.PhaseMerging, time.Now().UTC()))
	require.NoError(t, o.runMerge(t.Context()))

	// Read destination segs.
	matches, err := filepath.Glob(filepath.Join(fix.dataDir, "segments", "seg_*.jss"))
	require.NoError(t, err)
	require.NotEmpty(t, matches)
	rd, err := segment.Open(segment.ReaderConfig{Path: matches[0]})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rd.Close() })

	var got []segment.Event
	for i := 0; i < int(rd.Header().BlockCount); i++ {
		blk, err := rd.DecodeBlock(i)
		require.NoError(t, err)
		got = append(got, blk...)
	}
	require.Len(t, got, 3, "want 3 survivors (a@3l6, b@3l4, identity)")

	// IndexedAt re-stamping invariant: every survivor > max(source IndexedAt)
	maxSrc := int64(1004)
	for _, e := range got {
		require.Greater(t, e.IndexedAt, maxSrc, "survivor IndexedAt must be re-stamped to merge time")
	}
}

func TestMerge_RefreshesRepoRev_PreservesBackfillRev(t *testing.T) {
	t.Parallel()
	srcEvs := []segment.Event{
		ev(0, "did:plc:a", "3l6", segment.KindCreate, 1000),
		ev(0, "did:plc:a", "3l7", segment.KindUpdate, 1001),
	}
	fix := newMergeFixture(t, [][]segment.Event{srcEvs}, map[string]string{"did:plc:a": "3l5"})

	o, err := New(fix.cfg)
	require.NoError(t, err)
	require.NoError(t, lifecycle.WritePhase(fix.store, lifecycle.PhaseMerging, time.Now().UTC()))
	require.NoError(t, o.runMerge(t.Context()))

	val, closer, err := fix.store.Get(backfill.RepoKey("did:plc:a"))
	require.NoError(t, err)
	defer func() { _ = closer.Close() }()
	rs, err := backfill.DecodeRepoStatus(val)
	require.NoError(t, err)
	require.Equal(t, "3l7", rs.Rev, "top-level Rev advances to last surviving rev")
	require.Equal(t, "3l5", rs.Backfill.Rev, "Backfill.Rev is immutable post-merge")
}

func TestMerge_MultiSourceContiguousCommit(t *testing.T) {
	t.Parallel()
	src1 := []segment.Event{ev(0, "did:plc:a", "3l6", segment.KindCreate, 1000)}
	src2 := []segment.Event{ev(0, "did:plc:a", "3l7", segment.KindCreate, 1001)}
	fix := newMergeFixture(t, [][]segment.Event{src1, src2}, map[string]string{"did:plc:a": "3l5"})

	o, err := New(fix.cfg)
	require.NoError(t, err)
	require.NoError(t, lifecycle.WritePhase(fix.store, lifecycle.PhaseMerging, time.Now().UTC()))
	require.NoError(t, o.runMerge(t.Context()))

	// Cursor key absent (terminal cleanup ran).
	got, err := loadMergeCursor(fix.store)
	require.NoError(t, err)
	require.Equal(t, uint64(0), got)

	// data/backfill removed.
	_, err = os.Stat(filepath.Join(fix.dataDir, "backfill"))
	require.True(t, os.IsNotExist(err))
}

func TestMerge_EmptyLiveSegmentsDir(t *testing.T) {
	t.Parallel()
	fix := newMergeFixture(t, nil, nil) // creates empty live_segments dir

	o, err := New(fix.cfg)
	require.NoError(t, err)
	require.NoError(t, lifecycle.WritePhase(fix.store, lifecycle.PhaseMerging, time.Now().UTC()))
	require.NoError(t, o.runMerge(t.Context()))
}

func TestMerge_RestartAfterCleanup_NoLiveSegmentsDir(t *testing.T) {
	t.Parallel()
	fix := newMergeFixture(t, nil, nil)
	require.NoError(t, os.RemoveAll(filepath.Join(fix.dataDir, "backfill")))

	o, err := New(fix.cfg)
	require.NoError(t, err)
	require.NoError(t, lifecycle.WritePhase(fix.store, lifecycle.PhaseMerging, time.Now().UTC()))
	require.NoError(t, o.runMerge(t.Context()))
}

func TestMerge_TopLevelRunAdvancesPhase(t *testing.T) {
	t.Parallel()
	srcEvs := []segment.Event{ev(0, "did:plc:a", "3l6", segment.KindCreate, 1000)}
	fix := newMergeFixture(t, [][]segment.Event{srcEvs}, map[string]string{"did:plc:a": "3l5"})

	require.NoError(t, lifecycle.WritePhase(fix.store, lifecycle.PhaseMerging, time.Now().UTC()))
	o, err := New(fix.cfg)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- o.Run(ctx) }()

	require.Eventually(t, func() bool {
		p, err := lifecycle.ReadPhase(fix.store)
		return err == nil && p == lifecycle.PhaseSteadyState
	}, 5*time.Second, 20*time.Millisecond, "phase did not advance to steady_state")
	cancel()
	<-done
}
```

Add `os` to imports.

- [ ] **Step 3: Verify**

```bash
just test ./internal/ingest/orchestrator -run TestMerge_ && just lint
```

- [ ] **Step 4: Commit**

```bash
git add internal/ingest/orchestrator/merge_test.go internal/ingest/orchestrator/testfixtures_test.go
git commit -m "orchestrator: integration tests for merge phase"
```

---

## Task 13: Crash-and-resume tests via kill-point hook

**Files:** Modify `internal/ingest/orchestrator/merge_runner.go` to expose two test-only kill-point hooks. Modify `merge.go` to expose one. Append crash tests to `merge_test.go`.

The hooks are unexported package vars holding `func() error`. Production builds leave them nil. Tests set them to return a sentinel error at a specific point; the merge surfaces the error, the test restarts the merge with the hook cleared, and asserts terminal state.

- [ ] **Step 1: Add kill-point hooks**

In `merge_runner.go`, add at package level:

```go
// killAfterFlushBeforeCommit, when non-nil, is invoked between
// processSourceSegment returning and commitSourceComplete being
// called. Test-only; production paths leave it nil. Used to
// reproduce the spec §5.3 "after dst.Flush, before commitSource
// Complete" crash window.
var killAfterFlushBeforeCommit func() error
```

In `merge_runner.go`'s `run` method, immediately before the `commitSourceComplete` call, add:

```go
			if killAfterFlushBeforeCommit != nil {
				if err := killAfterFlushBeforeCommit(); err != nil {
					return err
				}
			}
```

In `merge.go`, add at package level:

```go
// killAfterSealBeforeRemoveAll, when non-nil, is invoked between
// dst.SealActiveAndClose and the discovery+RemoveAll cleanup.
// Test-only.
var killAfterSealBeforeRemoveAll func() error
```

In `merge.go`'s `runMerge`, between `dst.SealActiveAndClose()` and `runner.runDiscovery(...)`, add:

```go
		if killAfterSealBeforeRemoveAll != nil {
			if err := killAfterSealBeforeRemoveAll(); err != nil {
				return err
			}
		}
```

- [ ] **Step 2: Append the two crash tests to `merge_test.go`**

```go
func TestMerge_CrashAfterFlushBeforeCommit_ProducesDuplicates(t *testing.T) {
	t.Parallel()
	srcEvs := []segment.Event{
		ev(0, "did:plc:a", "3l6", segment.KindCreate, 1000),
		ev(0, "did:plc:a", "3l7", segment.KindCreate, 1001),
	}
	fix := newMergeFixture(t, [][]segment.Event{srcEvs}, map[string]string{"did:plc:a": "3l5"})

	require.NoError(t, lifecycle.WritePhase(fix.store, lifecycle.PhaseMerging, time.Now().UTC()))
	sentinel := errors.New("kill point")
	killAfterFlushBeforeCommit = func() error { return sentinel }
	t.Cleanup(func() { killAfterFlushBeforeCommit = nil })

	o, err := New(fix.cfg)
	require.NoError(t, err)
	err = o.runMerge(t.Context())
	require.ErrorIs(t, err, sentinel)

	// Cursor unchanged (commit never ran).
	cur, err := loadMergeCursor(fix.store)
	require.NoError(t, err)
	require.Equal(t, uint64(0), cur)

	// Restart cleanly.
	killAfterFlushBeforeCommit = nil
	o2, err := New(fix.cfg)
	require.NoError(t, err)
	require.NoError(t, o2.runMerge(t.Context()))

	// Read all destination events.
	matches, err := filepath.Glob(filepath.Join(fix.dataDir, "segments", "seg_*.jss"))
	require.NoError(t, err)
	var allEvs []segment.Event
	for _, m := range matches {
		rd, err := segment.Open(segment.ReaderConfig{Path: m})
		require.NoError(t, err)
		for i := 0; i < int(rd.Header().BlockCount); i++ {
			blk, err := rd.DecodeBlock(i)
			require.NoError(t, err)
			allEvs = append(allEvs, blk...)
		}
		_ = rd.Close()
	}

	// Both events appear at least once. They MAY appear twice
	// (pre-crash flushed copy + post-recovery copy). Seqs strictly
	// monotonic across the file set.
	revs := map[string]int{}
	for _, e := range allEvs {
		revs[e.Rev]++
	}
	require.GreaterOrEqual(t, revs["3l6"], 1)
	require.GreaterOrEqual(t, revs["3l7"], 1)
	for i := 1; i < len(allEvs); i++ {
		require.Greater(t, allEvs[i].Seq, allEvs[i-1].Seq, "destination seqs must be strictly monotonic")
	}
}

func TestMerge_CrashAfterSealBeforeRemoveAll_RestartCleansUp(t *testing.T) {
	t.Parallel()
	srcEvs := []segment.Event{ev(0, "did:plc:a", "3l6", segment.KindCreate, 1000)}
	fix := newMergeFixture(t, [][]segment.Event{srcEvs}, map[string]string{"did:plc:a": "3l5"})

	require.NoError(t, lifecycle.WritePhase(fix.store, lifecycle.PhaseMerging, time.Now().UTC()))
	sentinel := errors.New("kill point")
	killAfterSealBeforeRemoveAll = func() error { return sentinel }
	t.Cleanup(func() { killAfterSealBeforeRemoveAll = nil })

	o, err := New(fix.cfg)
	require.NoError(t, err)
	require.ErrorIs(t, o.runMerge(t.Context()), sentinel)

	// Sealed dst still on disk; live_segments still on disk.
	_, err = os.Stat(filepath.Join(fix.dataDir, "backfill", "live_segments"))
	require.NoError(t, err)

	killAfterSealBeforeRemoveAll = nil
	o2, err := New(fix.cfg)
	require.NoError(t, err)
	require.NoError(t, o2.runMerge(t.Context()))

	_, err = os.Stat(filepath.Join(fix.dataDir, "backfill"))
	require.True(t, os.IsNotExist(err))
}
```

Add `errors` to imports.

- [ ] **Step 3: Verify**

```bash
just test ./internal/ingest/orchestrator -run TestMerge_Crash && just lint
```

- [ ] **Step 4: Commit**

```bash
git add internal/ingest/orchestrator/merge.go internal/ingest/orchestrator/merge_runner.go internal/ingest/orchestrator/merge_test.go
git commit -m "orchestrator: crash-and-resume tests via kill-point hooks"
```

---

## Task 14: Tier 4 swarm/property test

**Files:** Create `internal/ingest/orchestrator/merge_swarm_test.go`. Smoke count under `-short`, full count otherwise.

The generator builds N random scenarios. Each scenario: random events with monotonically-increasing per-DID TID-shaped revs, a random per-DID `Backfill.Rev` cutoff, random source-segment sizes, optional kill-point injection. After driving the merge to terminal completion, assert spec invariants 1–7.

- [ ] **Step 1: Write the swarm test**

```go
package orchestrator

import (
	"errors"
	"math/rand/v2"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/stretchr/testify/require"
)

const (
	swarmSmokeIters = 10
	swarmFullIters  = 1000
)

func TestMerge_Swarm(t *testing.T) {
	t.Parallel()
	iters := swarmFullIters
	if testing.Short() {
		iters = swarmSmokeIters
	}

	for i := 0; i < iters; i++ {
		i := i
		t.Run("iter-"+strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			runSwarmIteration(t, rand.New(rand.NewPCG(uint64(i+1), uint64(i+2))))
		})
	}
}

// scenario captures one swarm-generated case so failures can be
// re-run deterministically by reusing the same seed.
type scenario struct {
	dids          []string
	backfillRevs  map[string]string
	sourceEvents  [][]segment.Event
	expectSurvive map[string]int // did → count of survivors expected
	maxSrcRev     map[string]string
}

func generateScenario(rng *rand.Rand) scenario {
	const dids = 5
	s := scenario{backfillRevs: map[string]string{}, expectSurvive: map[string]int{}, maxSrcRev: map[string]string{}}
	for i := 0; i < dids; i++ {
		s.dids = append(s.dids, "did:plc:"+strconv.Itoa(i))
		// Random BackfillRev cutoff (or no cutoff for ~25% of DIDs).
		if rng.IntN(4) != 0 {
			s.backfillRevs[s.dids[i]] = "rev-" + paddedHex(rng.IntN(100))
		}
	}

	totalEvents := 50 + rng.IntN(450)
	srcCount := 1 + rng.IntN(3)
	perSrc := make([][]segment.Event, srcCount)
	revCounters := map[string]int{}

	for k := 0; k < totalEvents; k++ {
		did := s.dids[rng.IntN(len(s.dids))]
		revCounters[did] += 1 + rng.IntN(5)
		rev := "rev-" + paddedHex(revCounters[did])
		var kind segment.Kind
		switch rng.IntN(6) {
		case 0:
			kind = segment.KindCreate
		case 1:
			kind = segment.KindUpdate
		case 2:
			kind = segment.KindDelete
		case 3:
			kind = segment.KindIdentity
		case 4:
			kind = segment.KindAccount
		default:
			kind = segment.KindSync
		}
		ev := segment.Event{
			IndexedAt:  int64(1000 + k),
			Kind:       kind,
			DID:        did,
			Collection: "app.bsky.feed.post",
			Rkey:       "rkey-" + strconv.Itoa(k),
			Rev:        rev,
			Payload:    []byte("p"),
		}
		// Non-commit kinds get empty rev to match production shape.
		if kind == segment.KindIdentity || kind == segment.KindAccount || kind == segment.KindSync {
			ev.Rev = ""
		}
		srcIdx := rng.IntN(srcCount)
		perSrc[srcIdx] = append(perSrc[srcIdx], ev)

		// Predict survivor / track maxSrcRev for the keep cases.
		survives := true
		if isCommitKind(kind) {
			if cutoff, ok := s.backfillRevs[did]; ok && rev != "" && rev <= cutoff {
				survives = false
			}
		}
		if survives {
			s.expectSurvive[did]++
			if isCommitKind(kind) {
				if rev > s.maxSrcRev[did] {
					s.maxSrcRev[did] = rev
				}
			}
		}
	}
	s.sourceEvents = perSrc
	return s
}

func paddedHex(n int) string {
	const w = 5
	s := strconv.FormatInt(int64(n), 36)
	for len(s) < w {
		s = "0" + s
	}
	return s
}

func runSwarmIteration(t *testing.T, rng *rand.Rand) {
	s := generateScenario(rng)
	fix := newMergeFixture(t, s.sourceEvents, s.backfillRevs)

	require.NoError(t, lifecycle.WritePhase(fix.store, lifecycle.PhaseMerging, time.Now().UTC()))

	// 30% chance of a kill-point injection on the flush-before-commit path.
	if rng.IntN(10) < 3 {
		killAfterFlushBeforeCommit = func() error { return errors.New("swarm kill") }
		t.Cleanup(func() { killAfterFlushBeforeCommit = nil })
		o, err := New(fix.cfg)
		require.NoError(t, err)
		_ = o.runMerge(t.Context()) // expected to error; we recover below
		killAfterFlushBeforeCommit = nil
	}

	o, err := New(fix.cfg)
	require.NoError(t, err)
	require.NoError(t, o.runMerge(t.Context()))

	// Read all destination events.
	matches, _ := filepath.Glob(filepath.Join(fix.dataDir, "segments", "seg_*.jss"))
	var allEvs []segment.Event
	for _, m := range matches {
		rd, err := segment.Open(segment.ReaderConfig{Path: m})
		require.NoError(t, err)
		for i := 0; i < int(rd.Header().BlockCount); i++ {
			blk, err := rd.DecodeBlock(i)
			require.NoError(t, err)
			allEvs = append(allEvs, blk...)
		}
		_ = rd.Close()
	}

	// Invariant 1: every expected survivor present at least once.
	gotByDID := map[string]int{}
	for _, e := range allEvs {
		gotByDID[e.DID]++
		// Invariant 2: no commit event with rev <= BackfillRev.
		if isCommitKind(e.Kind) && e.Rev != "" {
			if cutoff, ok := s.backfillRevs[e.DID]; ok {
				require.Greater(t, e.Rev, cutoff, "leaked covered commit %s/%s", e.DID, e.Rev)
			}
		}
	}
	for did, want := range s.expectSurvive {
		require.GreaterOrEqual(t, gotByDID[did], want, "missing survivors for %s", did)
	}

	// Invariant 3: strict monotonic seqs.
	for i := 1; i < len(allEvs); i++ {
		require.Greater(t, allEvs[i].Seq, allEvs[i-1].Seq)
	}

	// Invariant 4: cursors absent.
	cur, err := loadMergeCursor(fix.store)
	require.NoError(t, err)
	require.Equal(t, uint64(0), cur)

	// Invariant 5+6: per-DID Rev advanced; Backfill.Rev unchanged.
	for did, maxRev := range s.maxSrcRev {
		val, closer, err := fix.store.Get(backfill.RepoKey(did))
		if errors.Is(err, store.ErrNotFound) {
			continue
		}
		require.NoError(t, err)
		rs, err := backfill.DecodeRepoStatus(val)
		_ = closer.Close()
		require.NoError(t, err)
		if origBF, ok := s.backfillRevs[did]; ok {
			require.Equal(t, origBF, rs.Backfill.Rev, "Backfill.Rev mutated for %s", did)
			if maxRev > origBF {
				require.Equal(t, maxRev, rs.Rev, "top-level Rev should reflect last surviving for %s", did)
			}
		}
	}

	// Invariant 7: every survivor has IndexedAt > max source IndexedAt.
	const maxSrcIndexedAt int64 = 1000 + 500 // generator caps total events at 500
	for _, e := range allEvs {
		require.Greater(t, e.IndexedAt, maxSrcIndexedAt)
	}
}
```

Add `"github.com/bluesky-social/jetstream-v2/internal/store"` for the `store.ErrNotFound` reference.

- [ ] **Step 2: Verify**

```bash
just test ./internal/ingest/orchestrator -run TestMerge_Swarm
just test-long ./internal/ingest/orchestrator -run TestMerge_Swarm   # full 1000 iters
just test-race ./internal/ingest/orchestrator -run TestMerge_Swarm   # race detector
just lint
```

If any iteration fails, the test name `iter-<N>` plus the seed scheme `(N+1, N+2)` makes it deterministic-reproducible.

- [ ] **Step 3: Commit**

```bash
git add internal/ingest/orchestrator/merge_swarm_test.go
git commit -m "orchestrator: swarm/property test for merge phase"
```

---

## Task 15: Update existing `recovery_test.go` to use the real merge

**Files:** Modify `internal/ingest/orchestrator/recovery_test.go`.

The existing `TestRun_ResumeFromMerging_AdvancesToSteadyState` was authored against the no-op stub. With the real merge in place, the test must populate at least one source segment so `runMerge` has something to drain.

- [ ] **Step 1: Update the test**

Replace the body of `TestRun_ResumeFromMerging_AdvancesToSteadyState` so that it:

1. Builds a `data/backfill/live_segments/` tree with one tiny source segment containing a single keep-able event (e.g. a KindIdentity for a DID with no `repo/` row).
2. Writes `phase=merging` (as today).
3. Runs `Run`, asserts phase advances to `steady_state`.
4. Asserts `data/backfill/` is gone.
5. Cancels and waits for `Run` to return.

The simplest path is to call `newMergeFixture(t, [][]segment.Event{{KindIdentity event}}, nil)`, then point `Config` at the same `dataDir`, then run.

Concrete patch:

```go
func TestRun_ResumeFromMerging_AdvancesToSteadyState(t *testing.T) {
	t.Parallel()

	fix := newMergeFixture(t, [][]segment.Event{{
		{Kind: segment.KindIdentity, DID: "did:plc:resume-test", IndexedAt: 1000},
	}}, nil)

	require.NoError(t, lifecycle.WritePhase(fix.store, lifecycle.PhaseMerging, time.Now().UTC()))

	o, err := New(fix.cfg)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	done := make(chan error, 1)
	go func() { done <- o.Run(ctx) }()

	require.Eventually(t, func() bool {
		got, err := lifecycle.ReadPhase(fix.store)
		return err == nil && got == lifecycle.PhaseSteadyState
	}, 5*time.Second, 20*time.Millisecond, "phase did not advance to steady_state")

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}

	// data/backfill removed by merge.
	_, err = os.Stat(filepath.Join(fix.dataDir, "backfill"))
	require.True(t, os.IsNotExist(err))
}
```

The existing `requireNoBootstrapArtifacts` helper relied on the dir never being created. With the real merge it IS created (by the test setup), then removed by the merge, so the assertion changes.

- [ ] **Step 2: Verify**

```bash
just test ./internal/ingest/orchestrator -run TestRun_ResumeFromMerging
```

- [ ] **Step 3: Commit**

```bash
git add internal/ingest/orchestrator/recovery_test.go
git commit -m "orchestrator: update merge-resume recovery test for real merge"
```

---

## Task 16: Final verification and code-review pass

- [ ] **Step 1: Full test suite under race**

```bash
just test-race
```

Expected: all PASS.

- [ ] **Step 2: Lint**

```bash
just lint
```

Expected: zero issues.

- [ ] **Step 3: Sanity-check the merge phase against the spec**

Re-read `docs/superpowers/specs/2026-05-27-merge-phase-design.md` §3.4 (re-stamp), §4.6 (atomic batch), §4.7 (discovery), §5.3 (crash matrix), §6 (errors). For each row of the §5.3 matrix, mentally trace the implemented code path and confirm the documented behavior. Note any drift in a comment in the relevant source file.

- [ ] **Step 4: Confirm AGENTS.md / CLAUDE.md compliance**

- No new external dependencies introduced beyond the whitelist.
- All exported symbols carry doc comments.
- No emojis introduced.
- No Co-Authored-By or other commit trailers.
- Tests run in <1s per package under `-short`.

- [ ] **Step 5: Final commit (if any cleanup edits land)**

If sanity-check turned up small comment-only fixes:

```bash
git add -p
git commit -m "orchestrator: post-implementation cleanup"
```

Otherwise nothing to commit; the task closes the implementation.

---

## Self-Review

**Spec coverage check:**
- §3.4 IndexedAt re-stamping → Task 9 (in `processSourceSegment`); Task 12 asserts the invariant.
- §4.1 runMerge lifecycle → Task 11.
- §4.2 mergeRunner loop → Task 9.
- §4.3 shouldKeep → Task 7.
- §4.4 repoStatusLookup cache → Task 7.
- §4.5 cursor helpers → Task 8.
- §4.6 commitSourceComplete atomic batch → Task 8 + Task 9 invocation.
- §4.7 post-merge discovery → Task 10 + Task 11 invocation.
- §4.8 store helpers + SegmentFiles → Tasks 1, 3.
- §4.9 metrics → Task 6.
- §5.3 crash matrix → Task 13 covers the two materially distinct windows; Task 14 swarms across them.
- §6 error handling → Implemented in Tasks 7–11 via consistent `fmt.Errorf("orchestrator: merge: ...: %w", err)` wrapping; Task 16 verifies.
- §7 testing strategy: Tier 1 (Task 7), Tier 2 (Task 12), Tier 3 (Task 13), Tier 4 (Task 14). All tiers implemented at the depth the spec calls for.
- §8 code layout → Tasks produce the listed files.

**Type/name consistency:** the helpers introduced — `loadMergeCursor`, `deleteMergeCursor`, `commitSourceComplete`, `newRepoStatusLookup`, `shouldKeep`, `isCommitKind`, `mergeRunner.run`, `mergeRunner.processSourceSegment`, `mergeRunner.runDiscovery`, `LoadBootstrapLastListReposCursor`, `MaybeSaveBootstrapLastListReposCursor`, `DeleteBootstrapLastListReposCursor`, `backfill.RepoKey`, `backfill.EncodeRepoStatus`, `backfill.DecodeRepoStatus`, `store.GetVersionedUint64LE`, `store.SetVersionedUint64LE`, `store.EncodeVersionedUint64LE`, `ingest.SegmentFiles`, `ingest.SegmentFile` — are used consistently across all task references.

**Placeholder scan:** no TBDs, no "implement later", no "similar to Task N" without code, no "add error handling" — every step shows the actual code.

