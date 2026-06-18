# inspect-all CLI + /status Enrichment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `jetstream inspect-all` CLI subcommand that prints a database-wide segment summary, and unify the same aggregation behind the `/status` HTTP page so both surfaces show network totals, per-tree rollups, and a per-collection breakdown.

**Architecture:** A new `internal/status.InspectAll` walks one or more segment-tree roots (sealed `segments/` and transient `backfill/live_segments/`), calls `segment.Inspect` per file, and folds the results into a `*SegmentAggregate` value type. The status `Collector` calls it during `/status` snapshot builds; `cmd/jetstream/inspect_all.go` calls it directly against a data dir for the CLI. Aggregation is single-threaded for v1; correctness first, parallelize later if profiling motivates it.

**Tech Stack:** Go 1.24+, `urfave/cli/v3` for the CLI, `html/template` for the status page, `testify/require` for tests, `gotestsum` runner via `just test`.

**Spec:** [`docs/superpowers/specs/2026-05-28-inspect-all-design.md`](../specs/2026-05-28-inspect-all-design.md)

---

## File Map

**Create:**
- `internal/status/inspect_all.go` — exports `InspectAll`, `InspectAllOptions`, `SegmentAggregate`, `TreeAggregate`, `CollectionAggregate`, `NetworkTotals`. Pure aggregation, no Pebble dependency. Also owns the moved `microsToTime` helper.
- `internal/status/inspect_all_test.go` — aggregation arithmetic tests.
- `cmd/jetstream/inspect_all.go` — CLI subcommand registration and renderer.
- `cmd/jetstream/inspect_all_test.go` — golden-text renderer test.
- `cmd/jetstream/format.go` — shared formatting helpers (`formatMicros`, `formatBytes`, `humanInt`) lifted out of `inspect_segment.go` so both renderers use them.
- `cmd/jetstream/testdata/inspect_all_basic.golden` — expected CLI text output.

**Modify:**
- `internal/status/snapshot.go` — replace `Segments`/`LiveSegs SegmentTreeStats` fields with `SegmentAggregate *SegmentAggregate`; delete the now-unused `SegmentTreeStats` type. Keep `SegmentSummary`.
- `internal/status/collect.go` — replace the two `collectSegmentTree` calls with one `InspectAll` call; delete `collectSegmentTree`, `buildSegmentSummary`, and `microsToTime` (the last one moves into `inspect_all.go`).
- `internal/status/collect_test.go` — update the `TestCollect_FreshDataDir` assertions to read from `snap.SegmentAggregate.Trees` instead of `snap.Segments` / `snap.LiveSegs`.
- `internal/web/handler_test.go` — update `newFixtureSnap` to populate `SegmentAggregate` instead of `Segments`/`LiveSegs`. Existing assertions stay.
- `internal/web/templates/status.html` — re-bind the `tree` sub-template invocations to `.SegmentAggregate.Trees`; add new "Network" and "Collections" top-level sections; render `Warnings` callout when non-empty; extend the `tree` sub-template with the new fields (events, blocks, seq range, indexed-at range).
- `cmd/jetstream/inspect_segment.go` — delete the local `formatMicros` and `errWriter`; reference the shared versions from `cmd/jetstream/format.go` (they move; no behavior change).
- `cmd/jetstream/main.go` — register `inspectAllCommand()` in the root command's `Commands` slice.

---

## Conventions for this plan

- **Run all tests:** `just test ./internal/status/... ./cmd/jetstream/... ./internal/web/...` from the repo root after each task. Use `just test` (the `-short` mode in justfile:60) unless a step says otherwise.
- **Lint:** `just lint` after each task. The repo uses golangci-lint v2.10.1.
- **Commit style:** match the repo's recent log (terse imperative, no "feat:" prefix). Examples from `git log`: `update DESIGN.md`, `implement collection counts in the footer and show it in the inspect-segment command`.
- **Skip-list for Bash tool calls:** use `Bash` for `git`, `just test`, `just lint`, `go build`. Prefer `Read`/`Edit`/`Write` for everything else.

---

### Task 1: Extract shared formatting helpers from inspect_segment.go

**Files:**
- Create: `cmd/jetstream/format.go`
- Modify: `cmd/jetstream/inspect_segment.go` (remove the local `formatMicros` and `errWriter`)

The current `inspect_segment.go` defines `formatMicros` (line 234) and `errWriter` (line 245). Both will be reused by the new `inspect-all` renderer. Pull them out into `format.go` along with two new helpers (`humanInt`, `formatBytes`) that we'll need but the segment renderer doesn't currently use.

- [ ] **Step 1: Create `cmd/jetstream/format.go`**

```go
package main

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// formatMicros formats a unix-microsecond timestamp as RFC3339 with
// six-digit fractional seconds in UTC. Zero -> the literal "0" so the
// renderer doesn't print a misleading 1970 timestamp on a fresh file.
func formatMicros(us int64) string {
	if us == 0 {
		return "0"
	}
	t := time.UnixMicro(us).UTC()
	return t.Format("2006-01-02T15:04:05.000000Z")
}

// formatBytes formats n as a base-1024 human-readable size. Mirrors
// internal/web/format.go::humanBytes so the CLI and HTML page agree.
func formatBytes(n int64) string {
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

// humanInt renders n with comma group separators ("1,234,567"). Mirrors
// internal/web/format.go::humanInt; intentionally duplicated rather
// than imported because internal/web imports internal/status and we
// don't want a CLI -> internal/web coupling for a six-line helper.
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

// errWriter accumulates a write error so renderers can be a sequence
// of bw.printf calls without an `if err != nil` after every one. The
// first error is sticky; subsequent writes are dropped.
type errWriter struct {
	w   io.Writer
	err error
}

func (e *errWriter) printf(format string, args ...any) {
	if e.err != nil {
		return
	}
	_, e.err = fmt.Fprintf(e.w, format, args...)
}
```

- [ ] **Step 2: Strip the moved code from `cmd/jetstream/inspect_segment.go`**

Delete two regions:

**Delete lines 231-256** (the `formatMicros` doc-comment, function, and the entire `errWriter` block at the bottom of the file). Inspect_segment.go's last code line after this edit becomes the closing brace of `renderInspection` (around line 229).

The `Edit` tool call:

```
old_string:
// formatMicros formats a unix-microsecond timestamp as RFC3339 with
// six-digit fractional seconds in UTC. Zero -> the literal "0" so the
// renderer doesn't print a misleading 1970 timestamp on a fresh file.
func formatMicros(us int64) string {
	if us == 0 {
		return "0"
	}
	t := time.UnixMicro(us).UTC()
	return t.Format("2006-01-02T15:04:05.000000Z")
}

// errWriter accumulates a write error so the renderer can be a sequence
// of bw.printf calls without an `if err != nil` after every one. The
// first error is sticky; subsequent writes are dropped.
type errWriter struct {
	w   io.Writer
	err error
}

func (e *errWriter) printf(format string, args ...any) {
	if e.err != nil {
		return
	}
	_, e.err = fmt.Fprintf(e.w, format, args...)
}

new_string: (empty)
```

- [ ] **Step 3: Drop the now-unused imports in `inspect_segment.go`**

The deletions remove the only `time` and `io` references in the file. Update the import block: `time` and `io` were only used by the moved functions. After this step the import block should be:

```go
import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/urfave/cli/v3"
)
```

If `goimports` reorganizes the list, accept its order.

- [ ] **Step 4: Build to confirm nothing else regressed**

Run: `go build ./cmd/jetstream/...`
Expected: clean build. (No tests added in this task; the existing inspect_segment_test.go already exercises `formatMicros` indirectly through `renderInspection`.)

- [ ] **Step 5: Run existing tests**

Run: `just test ./cmd/jetstream/...`
Expected: PASS for `inspect_segment_test.go` (and `main_test.go`, `serve_test.go`).

- [ ] **Step 6: Lint**

Run: `just lint`
Expected: clean. (Note: `formatBytes` and `humanInt` aren't called anywhere yet — they'll get their first callers in Task 6. golangci-lint v2 with default config does not enable `unused`/`deadcode` for symbols in main packages by default, so this should pass. If it does flag them, escalate to the user; do NOT add nolint pragmas.)

- [ ] **Step 7: Commit**

```bash
git add cmd/jetstream/format.go cmd/jetstream/inspect_segment.go
git commit -m "$(cat <<'EOF'
extract shared formatting helpers in cmd/jetstream

formatMicros and errWriter move out of inspect_segment.go into a new
format.go so the upcoming inspect-all renderer can reuse them. Adds
formatBytes and humanInt helpers that mirror internal/web/format.go;
intentionally duplicated rather than imported to keep the CLI off the
internal/web dependency.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2 PRE-CHECK

Before starting Task 2: nothing in `internal/status/inspect_all.go` references `segment` or `strings` yet (the placeholder helpers were removed). The file's imports are exactly `errors`, `fmt`, `io/fs`, `os`, `path/filepath`, `sort`, `time`, and `internal/ingest`. Task 3 will add the `segment` import alongside its first use.

---

### Task 2: Define SegmentAggregate and the empty-state InspectAll

**Files:**
- Create: `internal/status/inspect_all.go`
- Test: `internal/status/inspect_all_test.go`

This task lays down the value types and the trivial "no roots, or all-empty roots" path. The corrupt-file and full-fixture cases come in later tasks. TDD: write the empty-roots test first.

- [ ] **Step 1: Write the failing test for empty roots**

Create `internal/status/inspect_all_test.go`:

```go
package status_test

import (
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/status"
	"github.com/stretchr/testify/require"
)

func TestInspectAll_NoRoots(t *testing.T) {
	t.Parallel()
	agg, err := status.InspectAll(nil, status.InspectAllOptions{})
	require.NoError(t, err)
	require.NotNil(t, agg)
	require.Empty(t, agg.Trees)
	require.Empty(t, agg.Collections)
	require.Empty(t, agg.Warnings)
	require.Equal(t, status.NetworkTotals{}, agg.Network)
}

func TestInspectAll_MissingRoot(t *testing.T) {
	t.Parallel()
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	agg, err := status.InspectAll([]string{missing}, status.InspectAllOptions{})
	require.NoError(t, err)
	require.Len(t, agg.Trees, 1)
	require.Equal(t, missing, agg.Trees[0].Dir)
	require.Equal(t, 0, agg.Trees[0].SealedCount)
	require.Equal(t, 0, agg.Trees[0].ActiveCount)
	require.Nil(t, agg.Trees[0].LatestSegment)
	require.Empty(t, agg.Collections)
	require.Empty(t, agg.Warnings)
}

func TestInspectAll_EmptyExistingRoot(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	agg, err := status.InspectAll([]string{dir}, status.InspectAllOptions{})
	require.NoError(t, err)
	require.Len(t, agg.Trees, 1)
	require.Equal(t, dir, agg.Trees[0].Dir)
	require.Equal(t, 0, agg.Trees[0].SealedCount+agg.Trees[0].ActiveCount)
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/status/ -run 'TestInspectAll_(NoRoots|MissingRoot|EmptyExistingRoot)' -v`
Expected: FAIL — `undefined: status.InspectAll` (and the other types).

- [ ] **Step 3: Create `internal/status/inspect_all.go` with the value types and the empty-state implementation**

```go
// Package status — InspectAll aggregates segment files under one or
// more on-disk roots into a single rendering-agnostic value. It is
// the shared backbone of the /status HTTP page (via Collector.build)
// and the `jetstream inspect-all` CLI subcommand.
//
// InspectAll has no Pebble dependency: it walks the filesystem and
// calls segment.Inspect per file. The CLI can therefore call it
// against a data dir on a host where no jetstream serve process is
// running.
package status

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
)

// SegmentAggregate is the rendering-agnostic, database-wide view of
// all segment files under one or more roots. Built by InspectAll;
// consumed by Collector.build (for the /status snapshot) and the
// inspect-all CLI renderer.
type SegmentAggregate struct {
	// Trees is one entry per input root, in input order. A missing
	// or empty root yields an entry with zero counters.
	Trees []TreeAggregate

	// Collections is the per-NSID rollup across all trees. Sorted by
	// EventCount descending with NSID ascending as tiebreak.
	Collections []CollectionAggregate

	// Network is the database-wide rollup across all trees.
	Network NetworkTotals

	// Warnings is one entry per per-file Inspect failure that was
	// tolerated and excluded from the aggregates. The highest-idx
	// file in each tree may fail silently (rotation race) and does
	// not produce a warning. Format: "<path>: <err>".
	Warnings []string
}

// TreeAggregate is a per-root rollup. Replaces the old
// SegmentTreeStats; supersets it with the new aggregate counters.
type TreeAggregate struct {
	Dir               string
	SealedCount       int
	ActiveCount       int
	CompressedBytes   int64 // sum of block compressed sizes
	UncompressedBytes int64 // sum of block uncompressed sizes
	DiskBytes         int64 // sum of file sizes (incl. headers/footer/indexes)
	EventCount        uint64
	BlockCount        uint64
	OldestMTime       time.Time
	NewestMTime       time.Time
	MinSeq            uint64    // 0 if no records
	MaxSeq            uint64
	MinIndexedAt      time.Time // zero if no records
	MaxIndexedAt      time.Time
	LatestSegment     *SegmentSummary
}

// CollectionAggregate is one row per distinct NSID seen anywhere in
// the scanned trees. SegmentCount/BlockCount only count segments and
// blocks that actually mention this NSID.
type CollectionAggregate struct {
	NSID         string
	EventCount   uint64
	SegmentCount int
	BlockCount   uint64
}

// NetworkTotals is the database-wide rollup across all trees.
type NetworkTotals struct {
	Segments          int
	SealedSegments    int
	ActiveSegments    int
	Blocks            uint64
	Events            uint64
	Collections       int
	CompressedBytes   int64
	UncompressedBytes int64
	DiskBytes         int64
	MinSeq            uint64
	MaxSeq            uint64
	MinIndexedAt      time.Time
	MaxIndexedAt      time.Time
}

// InspectAllOptions controls the scan.
type InspectAllOptions struct {
	// SkipUnsealed skips the active-file frame walk for any segment
	// whose header checksum is zero. The file is still counted (size
	// + ActiveCount++) but no per-block or per-collection data is
	// folded in. Useful for fast operator surveys.
	SkipUnsealed bool
}

// InspectAll walks each root in roots, calls segment.Inspect on every
// seg_*.jss file, and folds the results into a *SegmentAggregate.
//
// A root that does not exist on disk yields a TreeAggregate with the
// dir set and all counters zero — not an error. A root that exists
// but cannot be readdir'd is fatal.
//
// Per-file segment.Inspect failures are tolerated and recorded in
// SegmentAggregate.Warnings; the failing file is excluded from
// aggregates. The single highest-idx file in each tree is allowed to
// fail silently to tolerate a rotation race during a live scan.
func InspectAll(roots []string, opts InspectAllOptions) (*SegmentAggregate, error) {
	agg := &SegmentAggregate{}
	collections := make(map[string]*CollectionAggregate)

	for _, root := range roots {
		tree, err := scanTree(root, opts, collections)
		if err != nil {
			return nil, err
		}
		agg.Trees = append(agg.Trees, tree)
	}

	agg.Collections = materializeCollections(collections)
	agg.Network = computeNetworkTotals(agg.Trees, len(agg.Collections))
	return agg, nil
}

// scanTree readdirs root, calls segment.Inspect on each seg_*.jss
// file, folds results into a TreeAggregate, and updates the shared
// collections map. Per-file errors update agg.Warnings via a closure
// passed in by the caller — but for v1 we keep warnings local and
// return them from scanTree, then merge in InspectAll.
func scanTree(root string, opts InspectAllOptions, collections map[string]*CollectionAggregate) (TreeAggregate, error) {
	tree := TreeAggregate{Dir: root}

	entries, err := os.ReadDir(root)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return tree, nil
		}
		return TreeAggregate{}, fmt.Errorf("status: readdir %s: %w", root, err)
	}

	type segFile struct {
		idx  uint64
		path string
		info os.FileInfo
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
			return TreeAggregate{}, fmt.Errorf("status: stat %s: %w", e.Name(), err)
		}
		files = append(files, segFile{idx: idx, path: filepath.Join(root, e.Name()), info: fi})
	}
	if len(files) == 0 {
		return tree, nil
	}
	sort.Slice(files, func(i, j int) bool { return files[i].idx < files[j].idx })

	tree.OldestMTime = files[0].info.ModTime()
	tree.NewestMTime = files[0].info.ModTime()
	for _, f := range files {
		mt := f.info.ModTime()
		if mt.Before(tree.OldestMTime) {
			tree.OldestMTime = mt
		}
		if mt.After(tree.NewestMTime) {
			tree.NewestMTime = mt
		}
	}

	// File folding will land in Task 4. For now: the per-file
	// folding (segment.Inspect, collections map, warnings) is a
	// no-op so the empty-state tests pass.
	_ = collections

	return tree, nil
}

// materializeCollections converts the scan-time map into a slice
// sorted by EventCount desc with NSID asc tiebreak.
func materializeCollections(m map[string]*CollectionAggregate) []CollectionAggregate {
	if len(m) == 0 {
		return nil
	}
	out := make([]CollectionAggregate, 0, len(m))
	for _, v := range m {
		out = append(out, *v)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].EventCount != out[j].EventCount {
			return out[i].EventCount > out[j].EventCount
		}
		return out[i].NSID < out[j].NSID
	})
	return out
}

// computeNetworkTotals sums the per-tree counters into a single
// NetworkTotals. Bounds (MinSeq/MinIndexedAt/MaxSeq/MaxIndexedAt) are
// only contributed by trees whose own counters are non-zero so empty
// trees do not pull min bounds to zero.
func computeNetworkTotals(trees []TreeAggregate, collectionCount int) NetworkTotals {
	tot := NetworkTotals{Collections: collectionCount}
	for _, t := range trees {
		tot.Segments += t.SealedCount + t.ActiveCount
		tot.SealedSegments += t.SealedCount
		tot.ActiveSegments += t.ActiveCount
		tot.Blocks += t.BlockCount
		tot.Events += t.EventCount
		tot.CompressedBytes += t.CompressedBytes
		tot.UncompressedBytes += t.UncompressedBytes
		tot.DiskBytes += t.DiskBytes

		if t.EventCount == 0 {
			continue
		}
		if tot.MinSeq == 0 || t.MinSeq < tot.MinSeq {
			tot.MinSeq = t.MinSeq
		}
		if t.MaxSeq > tot.MaxSeq {
			tot.MaxSeq = t.MaxSeq
		}
		if !t.MinIndexedAt.IsZero() && (tot.MinIndexedAt.IsZero() || t.MinIndexedAt.Before(tot.MinIndexedAt)) {
			tot.MinIndexedAt = t.MinIndexedAt
		}
		if t.MaxIndexedAt.After(tot.MaxIndexedAt) {
			tot.MaxIndexedAt = t.MaxIndexedAt
		}
	}
	return tot
}

```

(`microsToTime` already exists in `internal/status/collect.go`. Both files are in package `status`, so when Task 3 calls it from `inspect_all.go` the call resolves without redeclaration. Task 5 relocates the source-of-truth to `inspect_all.go` when collect.go is otherwise gutted.)


- [ ] **Step 4: Run the empty-state tests; expect PASS**

Run: `go test ./internal/status/ -run 'TestInspectAll_(NoRoots|MissingRoot|EmptyExistingRoot)' -v`
Expected: PASS for all three.

- [ ] **Step 5: Build and full-package test**

Run: `go build ./... && just test ./internal/status/...`
Expected: clean build, all status tests pass.

- [ ] **Step 6: Commit**

```bash
git add internal/status/inspect_all.go internal/status/inspect_all_test.go
git commit -m "$(cat <<'EOF'
add internal/status.InspectAll skeleton with empty-state tests

Lays down SegmentAggregate, TreeAggregate, CollectionAggregate, and
NetworkTotals value types plus the empty-roots / missing-root /
empty-dir code paths. Per-file segment.Inspect folding lands in a
follow-up commit; for now the function returns a well-formed
*SegmentAggregate with zero counters when no segments are found.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Fold per-file segment.Inspect results into the aggregate

**Files:**
- Modify: `internal/status/inspect_all.go` (add the per-file fold logic)
- Modify: `internal/status/inspect_all_test.go` (single-segment fixture test)

This task wires `segment.Inspect` into `scanTree` and folds the result into the running TreeAggregate plus the shared collections map. Single-segment fixture only — multi-segment, multi-tree, corrupt-file cases land in Task 4.

- [ ] **Step 1: Write the failing single-segment test**

Append to `internal/status/inspect_all_test.go`:

```go
import (
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/status"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/stretchr/testify/require"
)

// writeSealedSegment writes a deterministic sealed segment file at
// dir/seg_<idx>.jss containing the provided events and returns the
// path. The fixture mirrors segment/inspect_test.go::makeSealedFixture
// so we exercise the same writer/seal code paths the production
// pipeline uses.
func writeSealedSegment(t *testing.T, dir string, idx uint64, events []segment.Event) string {
	t.Helper()
	path := filepath.Join(dir, ingest.SegmentFilename(idx))
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 4})
	require.NoError(t, err)
	for i, ev := range events {
		full, err := w.Append(ev)
		require.NoError(t, err)
		if full {
			require.NoError(t, w.Flush())
		}
		if i == len(events)-1 && !full {
			require.NoError(t, w.Flush())
		}
	}
	_, err = w.Seal()
	require.NoError(t, err)
	return path
}

func TestInspectAll_SingleSegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	events := []segment.Event{
		{Seq: 10, IndexedAt: 1_700_000_000_000_000, Kind: segment.KindCreate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "a", Rev: "r1", Payload: []byte("p1")},
		{Seq: 11, IndexedAt: 1_700_000_000_500_000, Kind: segment.KindCreate, DID: "did:plc:bob", Collection: "app.bsky.feed.like", Rkey: "b", Rev: "r2", Payload: []byte("p2")},
		{Seq: 12, IndexedAt: 1_700_000_001_000_000, Kind: segment.KindCreate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "c", Rev: "r3", Payload: []byte("p3")},
	}
	writeSealedSegment(t, dir, 1, events)

	agg, err := status.InspectAll([]string{dir}, status.InspectAllOptions{})
	require.NoError(t, err)
	require.Empty(t, agg.Warnings)
	require.Len(t, agg.Trees, 1)

	tree := agg.Trees[0]
	require.Equal(t, dir, tree.Dir)
	require.Equal(t, 1, tree.SealedCount)
	require.Equal(t, 0, tree.ActiveCount)
	require.Equal(t, uint64(3), tree.EventCount)
	require.Greater(t, tree.BlockCount, uint64(0))
	require.Greater(t, tree.CompressedBytes, int64(0))
	require.Greater(t, tree.UncompressedBytes, int64(0))
	require.Greater(t, tree.DiskBytes, int64(0))
	require.Equal(t, uint64(10), tree.MinSeq)
	require.Equal(t, uint64(12), tree.MaxSeq)
	require.False(t, tree.MinIndexedAt.IsZero())
	require.False(t, tree.MaxIndexedAt.IsZero())
	require.True(t, tree.MaxIndexedAt.After(tree.MinIndexedAt) || tree.MaxIndexedAt.Equal(tree.MinIndexedAt))
	require.NotNil(t, tree.LatestSegment)
	require.Equal(t, uint64(1), tree.LatestSegment.Index)
	require.True(t, tree.LatestSegment.Sealed)

	require.Len(t, agg.Collections, 2)
	// Sorted by event count desc; post has 2, like has 1.
	require.Equal(t, "app.bsky.feed.post", agg.Collections[0].NSID)
	require.Equal(t, uint64(2), agg.Collections[0].EventCount)
	require.Equal(t, 1, agg.Collections[0].SegmentCount)
	require.Greater(t, agg.Collections[0].BlockCount, uint64(0))
	require.Equal(t, "app.bsky.feed.like", agg.Collections[1].NSID)
	require.Equal(t, uint64(1), agg.Collections[1].EventCount)

	require.Equal(t, 1, agg.Network.Segments)
	require.Equal(t, 1, agg.Network.SealedSegments)
	require.Equal(t, 0, agg.Network.ActiveSegments)
	require.Equal(t, uint64(3), agg.Network.Events)
	require.Equal(t, 2, agg.Network.Collections)
	require.Equal(t, uint64(10), agg.Network.MinSeq)
	require.Equal(t, uint64(12), agg.Network.MaxSeq)
}
```

The test imports were wrong in Task 2 — fix that file's import block to include all the new imports. Replace the existing imports with:

```go
import (
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/status"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/stretchr/testify/require"
)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/status/ -run TestInspectAll_SingleSegment -v`
Expected: FAIL — counters are zero, `agg.Trees[0].EventCount == 0` etc., because Task 2's `scanTree` didn't fold per-file results.

- [ ] **Step 3: Implement the per-file fold in `scanTree`**

Replace the entire `scanTree` function in `internal/status/inspect_all.go` and add a new `inspectFile` helper. The new file body (just the modified parts):

Add `"github.com/bluesky-social/jetstream-v2/segment"` to the import block.

Replace the existing `scanTree` and the trailing `_ = collections` block. The new `scanTree`:

```go
func scanTree(root string, opts InspectAllOptions, collections map[string]*CollectionAggregate) (TreeAggregate, []string, error) {
	tree := TreeAggregate{Dir: root}
	var warnings []string

	entries, err := os.ReadDir(root)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return tree, nil, nil
		}
		return TreeAggregate{}, nil, fmt.Errorf("status: readdir %s: %w", root, err)
	}

	type segFile struct {
		idx  uint64
		path string
		info os.FileInfo
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
			return TreeAggregate{}, nil, fmt.Errorf("status: stat %s: %w", e.Name(), err)
		}
		files = append(files, segFile{idx: idx, path: filepath.Join(root, e.Name()), info: fi})
	}
	if len(files) == 0 {
		return tree, nil, nil
	}
	sort.Slice(files, func(i, j int) bool { return files[i].idx < files[j].idx })

	tree.OldestMTime = files[0].info.ModTime()
	tree.NewestMTime = files[0].info.ModTime()

	tailIdx := len(files) - 1
	for i, f := range files {
		mt := f.info.ModTime()
		if mt.Before(tree.OldestMTime) {
			tree.OldestMTime = mt
		}
		if mt.After(tree.NewestMTime) {
			tree.NewestMTime = mt
		}

		ins, err := segment.Inspect(f.path)
		if err != nil {
			// Tail rotation race: silently skip.
			if i != tailIdx {
				warnings = append(warnings, fmt.Sprintf("%s: %v", f.path, err))
			}
			continue
		}

		tree.DiskBytes += ins.FileSize
		if ins.Sealed {
			tree.SealedCount++
		} else {
			tree.ActiveCount++
		}

		// SkipUnsealed: still count file size + ActiveCount above,
		// but skip per-block / per-collection folding.
		if !ins.Sealed && opts.SkipUnsealed {
			continue
		}

		foldInspection(&tree, ins, collections)

		// LatestSegment: highest-idx file that we successfully
		// inspected. Loop is in idx-asc order, so unconditional set.
		tree.LatestSegment = &SegmentSummary{
			Index:           f.idx,
			Sealed:          ins.Sealed,
			EventCount:      ins.TotalEvents,
			UniqueDIDCount:  ins.UniqueDIDCount,
			BlockCount:      uint32(len(ins.Blocks)),
			CollectionCount: len(ins.Collections),
			MinSeq:          ins.MinSeq,
			MaxSeq:          ins.MaxSeq,
			MinIndexedAt:    microsToTime(ins.MinIndexedAt),
			MaxIndexedAt:    microsToTime(ins.MaxIndexedAt),
			SizeBytes:       ins.FileSize,
		}
	}

	return tree, warnings, nil
}

// foldInspection accumulates one segment's Inspection into the
// running tree aggregate and updates the shared per-NSID collections
// map. Callers handle bookkeeping concerns (warnings, file sizing,
// LatestSegment) — this function only owns the per-block and
// per-collection arithmetic.
func foldInspection(tree *TreeAggregate, ins *segment.Inspection, collections map[string]*CollectionAggregate) {
	tree.EventCount += ins.TotalEvents
	tree.BlockCount += uint64(len(ins.Blocks))

	for _, b := range ins.Blocks {
		tree.CompressedBytes += int64(b.CompressedSize)
		tree.UncompressedBytes += int64(b.UncompressedSize)
	}

	if ins.TotalEvents > 0 {
		if tree.MinSeq == 0 || ins.MinSeq < tree.MinSeq {
			tree.MinSeq = ins.MinSeq
		}
		if ins.MaxSeq > tree.MaxSeq {
			tree.MaxSeq = ins.MaxSeq
		}
		minIA := microsToTime(ins.MinIndexedAt)
		maxIA := microsToTime(ins.MaxIndexedAt)
		if !minIA.IsZero() && (tree.MinIndexedAt.IsZero() || minIA.Before(tree.MinIndexedAt)) {
			tree.MinIndexedAt = minIA
		}
		if maxIA.After(tree.MaxIndexedAt) {
			tree.MaxIndexedAt = maxIA
		}
	}

	// Per-block per-NSID block contribution: walk
	// ins.BlockCollections once, count blocks per NSID-id, then
	// add into collections[NSID].BlockCount. This guarantees one
	// block-count increment per (segment, block) pair that contains
	// the NSID; no double-counting.
	blockCountsByID := make(map[uint32]uint64, len(ins.Collections))
	for _, ids := range ins.BlockCollections {
		for _, id := range ids {
			blockCountsByID[id]++
		}
	}

	for i, nsid := range ins.Collections {
		agg, ok := collections[nsid]
		if !ok {
			agg = &CollectionAggregate{NSID: nsid}
			collections[nsid] = agg
		}
		var events uint32
		if i < len(ins.CollectionEventCounts) {
			events = ins.CollectionEventCounts[i]
		}
		agg.EventCount += uint64(events)
		agg.SegmentCount++
		agg.BlockCount += blockCountsByID[uint32(i)]
	}
}
```

Update the call site in `InspectAll`:

```go
for _, root := range roots {
	tree, warns, err := scanTree(root, opts, collections)
	if err != nil {
		return nil, err
	}
	agg.Trees = append(agg.Trees, tree)
	agg.Warnings = append(agg.Warnings, warns...)
}
```

- [ ] **Step 4: Run the new test plus the empty-state tests**

Run: `go test ./internal/status/ -run TestInspectAll -v`
Expected: PASS for all four `TestInspectAll_*` tests.

- [ ] **Step 5: Run the full status package tests + lint**

Run: `just test ./internal/status/... && just lint`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add internal/status/inspect_all.go internal/status/inspect_all_test.go
git commit -m "$(cat <<'EOF'
fold segment.Inspect results into status.InspectAll

Adds the per-file fold logic: tree-level counters, per-NSID rollup
into the shared collections map, and the highest-idx LatestSegment
binding. Per-file Inspect failures are recorded in
SegmentAggregate.Warnings unless the failing file is the tail (which
is silently tolerated to absorb rotation races during a live scan).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: Add multi-tree, corrupt-file, and skip-unsealed coverage

**Files:**
- Modify: `internal/status/inspect_all_test.go` (additional table-driven cases)

The aggregator already supports these scenarios from Task 3. This task pins them with tests so future refactors can't silently regress them.

- [ ] **Step 1: Write the multi-segment + multi-tree test**

Append to `internal/status/inspect_all_test.go`:

```go
import (
	"os"
	// ...existing imports above
)

func TestInspectAll_MultiTreeMergesCollections(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	segDir := filepath.Join(dataDir, "segments")
	liveDir := filepath.Join(dataDir, "backfill", "live_segments")
	require.NoError(t, os.MkdirAll(segDir, 0o755))
	require.NoError(t, os.MkdirAll(liveDir, 0o755))

	// Seg 1 in segments/: 2 posts, 1 like.
	writeSealedSegment(t, segDir, 1, []segment.Event{
		{Seq: 1, IndexedAt: 1_700_000_000_000_000, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "x", Rev: "r1", Payload: []byte("p")},
		{Seq: 2, IndexedAt: 1_700_000_001_000_000, Kind: segment.KindCreate, DID: "did:plc:b", Collection: "app.bsky.feed.post", Rkey: "y", Rev: "r2", Payload: []byte("p")},
		{Seq: 3, IndexedAt: 1_700_000_002_000_000, Kind: segment.KindCreate, DID: "did:plc:c", Collection: "app.bsky.feed.like", Rkey: "z", Rev: "r3", Payload: []byte("p")},
	})
	// Seg 2 in segments/: 1 post, 1 follow.
	writeSealedSegment(t, segDir, 2, []segment.Event{
		{Seq: 4, IndexedAt: 1_700_000_003_000_000, Kind: segment.KindCreate, DID: "did:plc:d", Collection: "app.bsky.feed.post", Rkey: "w", Rev: "r4", Payload: []byte("p")},
		{Seq: 5, IndexedAt: 1_700_000_004_000_000, Kind: segment.KindCreate, DID: "did:plc:e", Collection: "app.bsky.graph.follow", Rkey: "v", Rev: "r5", Payload: []byte("p")},
	})
	// Seg 1 in backfill/live_segments/: 1 post (overlaps NSID with segments/).
	writeSealedSegment(t, liveDir, 1, []segment.Event{
		{Seq: 6, IndexedAt: 1_700_000_005_000_000, Kind: segment.KindCreate, DID: "did:plc:f", Collection: "app.bsky.feed.post", Rkey: "u", Rev: "r6", Payload: []byte("p")},
	})

	agg, err := status.InspectAll([]string{segDir, liveDir}, status.InspectAllOptions{})
	require.NoError(t, err)
	require.Empty(t, agg.Warnings)
	require.Len(t, agg.Trees, 2)

	// Per-tree counters.
	require.Equal(t, 2, agg.Trees[0].SealedCount)
	require.Equal(t, uint64(5), agg.Trees[0].EventCount)
	require.Equal(t, 1, agg.Trees[1].SealedCount)
	require.Equal(t, uint64(1), agg.Trees[1].EventCount)

	// Network totals.
	require.Equal(t, 3, agg.Network.Segments)
	require.Equal(t, uint64(6), agg.Network.Events)
	require.Equal(t, uint64(1), agg.Network.MinSeq)
	require.Equal(t, uint64(6), agg.Network.MaxSeq)
	require.Equal(t, 3, agg.Network.Collections)

	// Collections merge: post=4 (3 segs), like=1 (1 seg), follow=1 (1 seg).
	require.Len(t, agg.Collections, 3)
	require.Equal(t, "app.bsky.feed.post", agg.Collections[0].NSID)
	require.Equal(t, uint64(4), agg.Collections[0].EventCount)
	require.Equal(t, 3, agg.Collections[0].SegmentCount)
	// Tiebreak between like (1) and follow (1) is NSID asc -> follow before like.
	require.Equal(t, "app.bsky.feed.like", agg.Collections[2].NSID)
	require.Equal(t, "app.bsky.graph.follow", agg.Collections[1].NSID)
}
```

Note the asserted NSID tiebreak: `app.bsky.feed.like` and `app.bsky.graph.follow` both have `EventCount=1`; ASCII-sorted, `app.bsky.feed.like` < `app.bsky.graph.follow`, so `like` should appear at index 1, not 2. **Fix the test before running it** — invert the last two assertions:

```go
require.Equal(t, "app.bsky.feed.like", agg.Collections[1].NSID)
require.Equal(t, "app.bsky.graph.follow", agg.Collections[2].NSID)
```

- [ ] **Step 2: Run the test to verify it passes**

Run: `go test ./internal/status/ -run TestInspectAll_MultiTreeMergesCollections -v`
Expected: PASS. (Task 3 already implemented multi-tree fold; this test pins it.)

- [ ] **Step 3: Write the corrupt-non-tail-file test**

Append:

```go
func TestInspectAll_CorruptNonTailFileIsWarning(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Three segments. We'll corrupt #2 (middle); #3 is the tail.
	writeSealedSegment(t, dir, 1, []segment.Event{
		{Seq: 1, IndexedAt: 1_700_000_000_000_000, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "x", Rev: "r1", Payload: []byte("p")},
	})
	corruptPath := writeSealedSegment(t, dir, 2, []segment.Event{
		{Seq: 2, IndexedAt: 1_700_000_001_000_000, Kind: segment.KindCreate, DID: "did:plc:b", Collection: "app.bsky.feed.post", Rkey: "y", Rev: "r2", Payload: []byte("p")},
	})
	writeSealedSegment(t, dir, 3, []segment.Event{
		{Seq: 3, IndexedAt: 1_700_000_002_000_000, Kind: segment.KindCreate, DID: "did:plc:c", Collection: "app.bsky.feed.post", Rkey: "z", Rev: "r3", Payload: []byte("p")},
	})

	// Corrupt the middle file by overwriting its magic.
	f, err := os.OpenFile(corruptPath, os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte("XXXX"), 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	agg, err := status.InspectAll([]string{dir}, status.InspectAllOptions{})
	require.NoError(t, err)
	require.Len(t, agg.Trees, 1)
	require.Equal(t, 2, agg.Trees[0].SealedCount, "corrupt file should be excluded from sealed count")
	require.Equal(t, uint64(2), agg.Trees[0].EventCount, "corrupt file's events should be excluded")
	require.Len(t, agg.Warnings, 1)
	require.Contains(t, agg.Warnings[0], corruptPath)
}

func TestInspectAll_CorruptTailFileIsSilent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	writeSealedSegment(t, dir, 1, []segment.Event{
		{Seq: 1, IndexedAt: 1_700_000_000_000_000, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "x", Rev: "r1", Payload: []byte("p")},
	})
	tailPath := writeSealedSegment(t, dir, 2, []segment.Event{
		{Seq: 2, IndexedAt: 1_700_000_001_000_000, Kind: segment.KindCreate, DID: "did:plc:b", Collection: "app.bsky.feed.post", Rkey: "y", Rev: "r2", Payload: []byte("p")},
	})

	// Corrupt the tail file.
	f, err := os.OpenFile(tailPath, os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte("XXXX"), 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	agg, err := status.InspectAll([]string{dir}, status.InspectAllOptions{})
	require.NoError(t, err)
	require.Len(t, agg.Trees, 1)
	require.Equal(t, 1, agg.Trees[0].SealedCount, "tail corruption excludes only the tail")
	require.Equal(t, uint64(1), agg.Trees[0].EventCount)
	require.Empty(t, agg.Warnings, "tail rotation race should be silent")
}
```

- [ ] **Step 4: Run the corrupt-file tests**

Run: `go test ./internal/status/ -run 'TestInspectAll_Corrupt' -v`
Expected: PASS for both.

- [ ] **Step 5: Write the SkipUnsealed test**

For this, we need an unsealed (active) segment. Use `segment.New` + `Append` + `Flush` without a `Seal` call. The fixture is similar to `writeSealedSegment` but ends differently. Append:

```go
// writeActiveSegment writes a deterministic active (unsealed) segment
// file at dir/seg_<idx>.jss containing the provided events. The file's
// header.checksum field is left zero so segment.Inspect classifies it
// as active.
func writeActiveSegment(t *testing.T, dir string, idx uint64, events []segment.Event) string {
	t.Helper()
	path := filepath.Join(dir, ingest.SegmentFilename(idx))
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 4})
	require.NoError(t, err)
	for _, ev := range events {
		_, err := w.Append(ev)
		require.NoError(t, err)
	}
	require.NoError(t, w.Flush())
	// No Seal — leave the file active.
	return path
}

func TestInspectAll_SkipUnsealed(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	writeSealedSegment(t, dir, 1, []segment.Event{
		{Seq: 1, IndexedAt: 1_700_000_000_000_000, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "x", Rev: "r1", Payload: []byte("p")},
	})
	writeActiveSegment(t, dir, 2, []segment.Event{
		{Seq: 2, IndexedAt: 1_700_000_001_000_000, Kind: segment.KindCreate, DID: "did:plc:b", Collection: "app.bsky.feed.like", Rkey: "y", Rev: "r2", Payload: []byte("p")},
	})

	// Without skip: both files contribute.
	full, err := status.InspectAll([]string{dir}, status.InspectAllOptions{})
	require.NoError(t, err)
	require.Equal(t, 1, full.Trees[0].SealedCount)
	require.Equal(t, 1, full.Trees[0].ActiveCount)
	require.Equal(t, uint64(2), full.Trees[0].EventCount)
	require.Len(t, full.Collections, 2)

	// With skip: active file's events / collections are not folded.
	skipped, err := status.InspectAll([]string{dir}, status.InspectAllOptions{SkipUnsealed: true})
	require.NoError(t, err)
	require.Equal(t, 1, skipped.Trees[0].SealedCount)
	require.Equal(t, 1, skipped.Trees[0].ActiveCount, "active file should still be counted")
	require.Equal(t, uint64(1), skipped.Trees[0].EventCount, "active events excluded with SkipUnsealed")
	require.Len(t, skipped.Collections, 1, "active NSIDs excluded")
	require.Equal(t, "app.bsky.feed.post", skipped.Collections[0].NSID)
	require.Greater(t, skipped.Trees[0].DiskBytes, int64(0), "active file size still counted")
}
```

- [ ] **Step 6: Run the SkipUnsealed test**

Run: `go test ./internal/status/ -run TestInspectAll_SkipUnsealed -v`
Expected: PASS.

- [ ] **Step 7: Run the entire test file to confirm no regression**

Run: `just test ./internal/status/...`
Expected: PASS.

- [ ] **Step 8: Lint**

Run: `just lint`
Expected: clean.

- [ ] **Step 9: Commit**

```bash
git add internal/status/inspect_all_test.go
git commit -m "$(cat <<'EOF'
pin multi-tree, corruption, and skip-unsealed behavior in InspectAll

Adds tests for: collections merging across multiple roots, a
mid-stream corrupt segment producing a warning, a corrupt tail file
being silently tolerated (rotation race), and the SkipUnsealed
option excluding active-file events from aggregates while still
counting the file size.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: Wire SegmentAggregate end-to-end (Snapshot, Collector, template)

**Files:**
- Modify: `internal/status/snapshot.go` (replace `Segments`/`LiveSegs` with `*SegmentAggregate`; remove `SegmentTreeStats`)
- Modify: `internal/status/collect.go` (delete `collectSegmentTree`, `buildSegmentSummary`, `microsToTime`; replace with one `InspectAll` call)
- Modify: `internal/status/collect_test.go` (update `TestCollect_FreshDataDir`)
- Modify: `internal/web/handler_test.go` (update `newFixtureSnap` to populate `SegmentAggregate`)
- Modify: `internal/web/templates/status.html` (rebind tree sub-template; add Network and Collections sections; warnings callout)

This task keeps the build/tests green by changing the template in lockstep with the snapshot reshape. The template gains the new Network and Collections sections in the same commit so the existing handler tests pass under the new fixture.

- [ ] **Step 1: Update `snapshot.go`**

Read `internal/status/snapshot.go` first to confirm the current state. Then replace the `Snapshot` definition and remove `SegmentTreeStats`.

The new file:

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
	GeneratedAt      time.Time
	Process          ProcessInfo
	Phase            PhaseInfo
	Backfill         BackfillStats
	Live             LiveStats
	SegmentAggregate *SegmentAggregate
	Pebble           PebbleStats
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

This removes the `SegmentTreeStats` type entirely. Anything that referenced it must now use `*TreeAggregate` (which lives in inspect_all.go).

- [ ] **Step 2: Update `collect.go`**

Read `internal/status/collect.go` first. Then make these changes:

(a) Delete the `collectSegmentTree` function (lines 101-179) and `buildSegmentSummary` (lines 181-199).

(b) Move the `microsToTime` helper out of `collect.go` and into `inspect_all.go`. Cut these lines from collect.go:

```go
func microsToTime(us int64) time.Time {
	if us == 0 {
		return time.Time{}
	}
	return time.UnixMicro(us).UTC()
}
```

Paste them at the bottom of `internal/status/inspect_all.go`. The function is already used by Task 3's `foldInspection`; this just relocates the source-of-truth. Both files are in package `status`, so the move is symbol-neutral until the deletion-from-collect.go and addition-to-inspect_all.go land in the same commit.

(c) Replace the two `collectSegmentTree` calls in `build()` with a single `InspectAll` call. The relevant block becomes:

```go
roots := []string{
	filepath.Join(opts.DataDir, "segments"),
	filepath.Join(opts.DataDir, "backfill", "live_segments"),
}
agg, err := InspectAll(roots, InspectAllOptions{})
if err != nil {
	return nil, err
}
```

(d) Update the returned Snapshot construction:

```go
return &Snapshot{
	GeneratedAt:      now,
	Process:          collectProcess(now, startedAt),
	Phase:            phase,
	Backfill:         bf,
	Live:             liveStats,
	SegmentAggregate: agg,
	Pebble:           pdb,
}, nil
```

(e) Drop unused imports (`io/fs`, `sort`, and `github.com/bluesky-social/jetstream-v2/internal/ingest` if they were only used by the deleted helpers; `github.com/bluesky-social/jetstream-v2/segment` likewise). Keep `errors`, `fmt`, `filepath`, `os`, `runtime`, `time`, `cockroachdb/pebble`, `internal/ingest/backfill`, `internal/ingest/live`, `internal/lifecycle`, `internal/store`, `internal/version`. Verify with `goimports`/`go build`.

- [ ] **Step 3: Update `internal/status/collect_test.go::TestCollect_FreshDataDir`**

The test currently asserts `snap.Segments.Dir` and `snap.LiveSegs.Dir` (lines 46-49). Replace those four assertions with:

```go
require.NotNil(t, snap.SegmentAggregate)
require.Len(t, snap.SegmentAggregate.Trees, 2)
require.Equal(t, filepath.Join(dataDir, "segments"), snap.SegmentAggregate.Trees[0].Dir)
require.Equal(t, filepath.Join(dataDir, "backfill", "live_segments"), snap.SegmentAggregate.Trees[1].Dir)
require.Equal(t, 0, snap.SegmentAggregate.Trees[0].SealedCount+snap.SegmentAggregate.Trees[0].ActiveCount)
require.Equal(t, 0, snap.SegmentAggregate.Trees[1].SealedCount+snap.SegmentAggregate.Trees[1].ActiveCount)
```

- [ ] **Step 4: Update `internal/web/handler_test.go::newFixtureSnap`**

Read the file to confirm current state. Replace the `Segments: status.SegmentTreeStats{...}` and `LiveSegs: status.SegmentTreeStats{...}` blocks (lines 47-67 currently) with:

```go
SegmentAggregate: &status.SegmentAggregate{
	Trees: []status.TreeAggregate{
		{
			Dir:               "/tmp/segments",
			SealedCount:       5,
			ActiveCount:       1,
			CompressedBytes:   1024 * 1024,
			UncompressedBytes: 4 * 1024 * 1024,
			DiskBytes:         5 * 1024 * 1024,
			EventCount:        12345,
			BlockCount:        42,
			LatestSegment: &status.SegmentSummary{
				Index:           42,
				Sealed:          true,
				EventCount:      1234,
				UniqueDIDCount:  567,
				BlockCount:      8,
				CollectionCount: 3,
				MinSeq:          100,
				MaxSeq:          1233,
				MinIndexedAt:    time.Date(2026, 5, 24, 0, 0, 0, 0, time.UTC),
				MaxIndexedAt:    time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC),
				SizeBytes:       512 * 1024,
			},
		},
		{Dir: "/tmp/backfill/live_segments"},
	},
	Collections: []status.CollectionAggregate{
		{NSID: "app.bsky.feed.post", EventCount: 9000, SegmentCount: 5, BlockCount: 30},
		{NSID: "app.bsky.feed.like", EventCount: 3000, SegmentCount: 4, BlockCount: 10},
		{NSID: "app.bsky.graph.follow", EventCount: 345, SegmentCount: 2, BlockCount: 2},
	},
	Network: status.NetworkTotals{
		Segments:          6,
		SealedSegments:    5,
		ActiveSegments:    1,
		Events:            12345,
		Blocks:            42,
		Collections:       3,
		CompressedBytes:   1024 * 1024,
		UncompressedBytes: 4 * 1024 * 1024,
		DiskBytes:         5 * 1024 * 1024,
	},
},
```

- [ ] **Step 5: Update the HTML template (`internal/web/templates/status.html`)**

Read the file first to confirm current state. Three changes:

(a) **Rebind the existing tree sub-template invocations.** Replace the current "Segments" section (around lines 153-159) with a block that pulls trees by index from the aggregate:

```html
<section>
  <h2>Segments</h2>
  {{with .SegmentAggregate}}
  {{if .Warnings}}
  <div class="warnings" style="margin-bottom: 0.75rem; padding: 0.5rem 0.75rem; border-left: 3px solid var(--accent); background: var(--bar-bg);">
    <strong>{{len .Warnings}} warning(s):</strong>
    <ul style="margin: 0.25rem 0 0 1rem; padding: 0;">
      {{range .Warnings}}<li><code>{{.}}</code></li>{{end}}
    </ul>
  </div>
  {{end}}
  <div class="grid2">
    {{template "tree" dict "Stats" (index .Trees 0) "Now" $.Now "Title" "segments/"}}
    {{template "tree" dict "Stats" (index .Trees 1) "Now" $.Now "Title" "backfill/live_segments/"}}
  </div>
  {{end}}
</section>
```

(b) **Add a "Network" section before the "Segments" section.** Insert immediately above the section we just rewrote:

```html
<section>
  <h2>Network</h2>
  {{with .SegmentAggregate}}
  <dl>
    <dt>Segments</dt>
    <dd>{{.Network.Segments}} ({{.Network.SealedSegments}} sealed{{if gt .Network.ActiveSegments 0}} + {{.Network.ActiveSegments}} active{{end}})</dd>
    <dt>Blocks</dt>
    <dd>{{humanInt .Network.Blocks}}</dd>
    <dt>Events</dt>
    <dd>{{humanInt .Network.Events}}</dd>
    <dt>Collections</dt>
    <dd>{{.Network.Collections}} distinct NSIDs</dd>
    {{if gt .Network.Events 0}}
    <dt>Seq range</dt>
    <dd>[{{humanInt .Network.MinSeq}}, {{humanInt .Network.MaxSeq}}]</dd>
    {{if not .Network.MinIndexedAt.IsZero}}
    <dt>Indexed range</dt>
    <dd>{{.Network.MinIndexedAt.Format "2006-01-02 15:04:05"}} … {{.Network.MaxIndexedAt.Format "2006-01-02 15:04:05"}}</dd>
    {{end}}
    {{end}}
    <dt>Compressed</dt>
    <dd>{{humanBytes .Network.CompressedBytes}}</dd>
    <dt>Uncompressed</dt>
    <dd>{{humanBytes .Network.UncompressedBytes}}</dd>
    <dt>Disk usage</dt>
    <dd>{{humanBytes .Network.DiskBytes}}</dd>
  </dl>
  {{end}}
</section>
```

(c) **Add a "Collections" section after the "Segments" section.** Insert before the existing "Metadata store" section:

```html
<section>
  <h2>Collections</h2>
  {{with .SegmentAggregate}}
  {{if .Collections}}
  <table style="border-collapse: collapse; width: 100%;">
    <thead>
      <tr style="text-align: left; border-bottom: 1px solid var(--border);">
        <th style="padding: 0.25rem 0.5rem;">NSID</th>
        <th style="padding: 0.25rem 0.5rem; text-align: right;">Events</th>
        <th style="padding: 0.25rem 0.5rem; text-align: right;">Segments</th>
        <th style="padding: 0.25rem 0.5rem; text-align: right;">Blocks</th>
      </tr>
    </thead>
    <tbody>
    {{range .Collections}}
      <tr>
        <td style="padding: 0.25rem 0.5rem;"><code>{{.NSID}}</code></td>
        <td style="padding: 0.25rem 0.5rem; text-align: right; font-variant-numeric: tabular-nums;">{{humanInt .EventCount}}</td>
        <td style="padding: 0.25rem 0.5rem; text-align: right; font-variant-numeric: tabular-nums;">{{.SegmentCount}}</td>
        <td style="padding: 0.25rem 0.5rem; text-align: right; font-variant-numeric: tabular-nums;">{{humanInt .BlockCount}}</td>
      </tr>
    {{end}}
    </tbody>
  </table>
  {{else}}
  <p class="sub">No segments yet.</p>
  {{end}}
  {{end}}
</section>
```

(d) **Extend the existing `tree` sub-template.** The block at the bottom of the file currently only shows files / compressed / uncompressed. Add events, blocks, and seq range. Replace the existing `tree` template with:

```html
{{define "tree"}}
<div>
  <h3 style="margin-top: 0; font-size: 0.95rem; color: var(--muted); font-weight: 500;">{{.Title}}</h3>
  {{with .Stats}}
  <dl>
    <dt>Files</dt>
    <dd>{{.SealedCount}} sealed{{if gt .ActiveCount 0}} + {{.ActiveCount}} active{{end}}</dd>
    {{if gt .EventCount 0}}
    <dt>Events</dt>
    <dd>{{humanInt .EventCount}}</dd>
    <dt>Blocks</dt>
    <dd>{{humanInt .BlockCount}}</dd>
    <dt>Seq range</dt>
    <dd>[{{humanInt .MinSeq}}, {{humanInt .MaxSeq}}]</dd>
    {{end}}
    <dt>Compressed</dt>
    <dd>{{humanBytes .CompressedBytes}}</dd>
    <dt>Uncompressed</dt>
    <dd>{{humanBytes .UncompressedBytes}}</dd>
    {{if not .OldestMTime.IsZero}}
    <dt>Oldest</dt>
    <dd>{{relativeTime .OldestMTime $.Now}}</dd>
    <dt>Newest</dt>
    <dd>{{relativeTime .NewestMTime $.Now}}</dd>
    {{end}}
  </dl>
  {{with .LatestSegment}}
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
      {{if not .MinIndexedAt.IsZero}}
      <dt>Indexed range</dt>
      <dd>{{.MinIndexedAt.Format "2006-01-02 15:04:05"}}<br>… {{.MaxIndexedAt.Format "2006-01-02 15:04:05"}}</dd>
      {{end}}
      <dt>Size</dt>
      <dd>{{humanBytes .SizeBytes}}</dd>
    </dl>
  </div>
  {{end}}
  {{end}}
</div>
{{end}}
```

The crucial change is the outer `{{with .Stats}}` so the template tolerates a zero-value TreeAggregate (for the empty `live_segments/` case). The existing `humanInt64Cast` helper in `handler.go:62-74` already handles uint64 input.

- [ ] **Step 6: Build and run all affected tests**

Run: `go build ./... && just test ./internal/status/... ./internal/web/...`
Expected: PASS for all status and web tests. The existing `TestHandler_RendersOK` body assertions still match because:
- `1,234` comes from the new `LatestSegment.EventCount` (1234, humanInt'd).
- `567` from `LatestSegment.UniqueDIDCount`.
- `[100, 1,233]` from the per-segment Seq range.
- `2026-05-24` from `MinIndexedAt`.

The new "Network" section adds an `Events` line that prints `12,345` — does not collide with any existing assertion.

If a test fails with a template-execution error referring to `.Segments` or `.LiveSegs`, you missed a binding update; grep the template:

```bash
grep -n "\.Segments\|\.LiveSegs" internal/web/templates/status.html
```
Expected: only the literal string "Segments" (in the `<h2>` heading) — no template field references.

- [ ] **Step 7: Lint**

Run: `just lint`
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add internal/status/snapshot.go internal/status/collect.go internal/status/collect_test.go internal/web/handler_test.go internal/web/templates/status.html
git commit -m "$(cat <<'EOF'
wire SegmentAggregate into the /status pipeline and template

Snapshot.Segments and Snapshot.LiveSegs are replaced with a single
*SegmentAggregate populated from the new InspectAll. SegmentTreeStats
is removed; per-tree data lives on TreeAggregate. The HTML template
gains a Network totals section, a Collections table, a warnings
callout, and the existing tree sub-template now shows events / blocks
/ seq range alongside the byte totals.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: Build the inspect-all CLI renderer

**Files:**
- Create: `cmd/jetstream/inspect_all.go`
- Modify: `cmd/jetstream/main.go` (register the new command)

This task adds the `jetstream inspect-all` subcommand. The renderer is plain text, mirroring `inspect-segment`'s style. The renderer is unit-testable by handing it a `*SegmentAggregate` literal — that test is its own task (Task 7).

- [ ] **Step 1: Add `formatTime` helper to `cmd/jetstream/format.go`**

The renderer hands time.Time values to a formatter; we need a sibling of `formatMicros` for already-converted values. Append at the bottom of `cmd/jetstream/format.go`:

```go
// formatTime renders t in the same RFC3339-micros UTC format used by
// formatMicros. Zero time renders as "0" so a freshly initialized
// aggregate (no records yet) doesn't print a misleading 1970 stamp.
func formatTime(t time.Time) string {
	if t.IsZero() {
		return "0"
	}
	return t.UTC().Format("2006-01-02T15:04:05.000000Z")
}
```

- [ ] **Step 2: Create `cmd/jetstream/inspect_all.go`**

```go
package main

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/status"
	"github.com/urfave/cli/v3"
)

// inspectAllCommand wires up `jetstream inspect-all`. The command is a
// thin shell over status.InspectAll + renderInspectAll: aggregation
// lives in internal/status, this layer only owns CLI flag wiring and
// the text renderer.
//
// Mirrors inspectSegmentCommand: same file structure, same rendering
// conventions (plain text, RFC3339-micros UTC, comma-grouped counts,
// 1024-base byte sizes), same errWriter pattern.
func inspectAllCommand() *cli.Command {
	return &cli.Command{
		Name:  "inspect-all",
		Usage: "Print a database-wide summary of every segment file under --data-dir",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "data-dir",
				Usage:   "Path to the data directory; the inspected trees are <data-dir>/segments and <data-dir>/backfill/live_segments",
				Sources: cli.EnvVars("JETSTREAM_DATA_DIR"),
				Value:   "./data",
			},
			&cli.BoolFlag{
				Name:  "skip-unsealed",
				Usage: "Skip frame-walking active (unsealed) segments. Faster but excludes their events from aggregates; the file size is still counted.",
				Value: false,
			},
			&cli.IntFlag{
				Name:  "collections-truncate",
				Usage: "Truncate the per-collection table when distinct-NSID count exceeds this many rows (0 = no truncation).",
				Value: 100,
			},
		},
		Action: func(_ context.Context, cmd *cli.Command) error {
			truncate := cmd.Int("collections-truncate")
			if truncate < 0 {
				return fmt.Errorf("inspect-all: --collections-truncate must be >= 0, got %d", truncate)
			}

			dataDir := cmd.String("data-dir")
			roots := []string{
				filepath.Join(dataDir, "segments"),
				filepath.Join(dataDir, "backfill", "live_segments"),
			}
			agg, err := status.InspectAll(roots, status.InspectAllOptions{
				SkipUnsealed: cmd.Bool("skip-unsealed"),
			})
			if err != nil {
				return err
			}
			return renderInspectAll(cmd.Root().Writer, dataDir, agg, time.Now().UTC(), int(truncate))
		},
	}
}

// renderInspectAll writes the human + LLM-pasteable text report to w.
//
// Layout: header, network totals, per-tree summaries, per-collection
// table, optional warnings. Sections are blank-line separated.
// Numbers are decimal with comma group separators; bytes are 1024-base.
// Timestamps are RFC3339 micros UTC.
func renderInspectAll(w io.Writer, dataDir string, agg *status.SegmentAggregate, generatedAt time.Time, truncate int) error {
	bw := &errWriter{w: w}

	bw.printf("inspect-all\n")
	bw.printf("data-dir: %s\n", dataDir)
	bw.printf("generated: %s\n", formatTime(generatedAt))

	renderNetwork(bw, agg.Network)
	renderTrees(bw, agg.Trees)
	renderCollections(bw, agg.Collections, truncate)
	renderWarnings(bw, agg.Warnings)

	return bw.err
}

func renderNetwork(bw *errWriter, n status.NetworkTotals) {
	bw.printf("\nnetwork totals:\n")
	bw.printf("  segments:               %d (%d sealed, %d active)\n",
		n.Segments, n.SealedSegments, n.ActiveSegments)
	bw.printf("  blocks:                 %s\n", humanInt(n.Blocks))
	bw.printf("  events:                 %s\n", humanInt(n.Events))
	bw.printf("  collections:            %d distinct NSIDs\n", n.Collections)
	if n.Events > 0 {
		bw.printf("  seq range:              [%d, %d]\n", n.MinSeq, n.MaxSeq)
		bw.printf("  indexed_at range:       %s → %s\n",
			formatTime(n.MinIndexedAt), formatTime(n.MaxIndexedAt))
	}
	bw.printf("  payload (uncompressed): %s\n", formatBytes(n.UncompressedBytes))
	bw.printf("  payload (compressed):   %s\n", formatBytes(n.CompressedBytes))
	bw.printf("  disk usage:             %s\n", formatBytes(n.DiskBytes))
	if n.CompressedBytes > 0 {
		ratio := float64(n.UncompressedBytes) / float64(n.CompressedBytes)
		bw.printf("  compression ratio:      %.2fx\n", ratio)
	}
}

func renderTrees(bw *errWriter, trees []status.TreeAggregate) {
	bw.printf("\ntrees:\n")
	if len(trees) == 0 {
		bw.printf("  (none)\n")
		return
	}
	for i, t := range trees {
		bw.printf("  [%d] %s\n", i, t.Dir)
		if t.SealedCount+t.ActiveCount == 0 {
			bw.printf("        (empty)\n")
			continue
		}
		bw.printf("        files:        %d sealed + %d active\n", t.SealedCount, t.ActiveCount)
		bw.printf("        events:       %s\n", humanInt(t.EventCount))
		bw.printf("        blocks:       %s\n", humanInt(t.BlockCount))
		if t.EventCount > 0 {
			bw.printf("        seq range:    [%d, %d]\n", t.MinSeq, t.MaxSeq)
			bw.printf("        indexed_at:   %s → %s\n",
				formatTime(t.MinIndexedAt), formatTime(t.MaxIndexedAt))
		}
		if !t.OldestMTime.IsZero() {
			bw.printf("        oldest mtime: %s\n", formatTime(t.OldestMTime))
			bw.printf("        newest mtime: %s\n", formatTime(t.NewestMTime))
		}
		bw.printf("        compressed:   %s\n", formatBytes(t.CompressedBytes))
		bw.printf("        uncompressed: %s\n", formatBytes(t.UncompressedBytes))
		bw.printf("        disk:         %s\n", formatBytes(t.DiskBytes))
		if ls := t.LatestSegment; ls != nil {
			state := "active"
			if ls.Sealed {
				state = "sealed"
			}
			bw.printf("        latest:       idx=%d %s events=%s blocks=%d size=%s\n",
				ls.Index, state, humanInt(ls.EventCount), ls.BlockCount, formatBytes(ls.SizeBytes))
		}
	}
}

func renderCollections(bw *errWriter, cols []status.CollectionAggregate, truncate int) {
	bw.printf("\ncollections (%d distinct NSIDs):\n", len(cols))
	if len(cols) == 0 {
		bw.printf("  (none)\n")
		return
	}

	// Compute column widths from the rows we'll actually print so the
	// table aligns even if NSIDs vary in length. Min widths give a
	// readable header even on a tiny dataset.
	nsidW := len("NSID")
	for _, c := range cols {
		if len(c.NSID) > nsidW {
			nsidW = len(c.NSID)
		}
	}

	emit := func(idx int, c status.CollectionAggregate) {
		bw.printf("  [%3d] %-*s  events: %12s  segments: %5d  blocks: %12s\n",
			idx, nsidW, c.NSID, humanInt(c.EventCount), c.SegmentCount, humanInt(c.BlockCount))
	}

	n := len(cols)
	if truncate == 0 || n <= truncate {
		for i, c := range cols {
			emit(i, c)
		}
		return
	}
	half := truncate / 2
	for i := range half {
		emit(i, cols[i])
	}
	bw.printf("  ... (%d rows omitted) ...\n", n-2*half)
	for i := n - half; i < n; i++ {
		emit(i, cols[i])
	}
}

func renderWarnings(bw *errWriter, warns []string) {
	if len(warns) == 0 {
		return
	}
	bw.printf("\nwarnings (%d):\n", len(warns))
	for _, w := range warns {
		bw.printf("  %s\n", w)
	}
}
```

- [ ] **Step 3: Register the new command in `main.go`**

Read `cmd/jetstream/main.go` first. The `Commands` slice currently looks like:

```go
Commands: []*cli.Command{
	serveCommand(),
	versionCommand(),
	inspectSegmentCommand(),
},
```

Replace with:

```go
Commands: []*cli.Command{
	serveCommand(),
	versionCommand(),
	inspectSegmentCommand(),
	inspectAllCommand(),
},
```

- [ ] **Step 4: Build the binary to confirm everything links**

Run: `just build`
Expected: clean build at `bin/jetstream`.

- [ ] **Step 5: Smoke-test the help output**

Run: `bin/jetstream inspect-all --help`
Expected: usage text listing the three flags. No panic.

- [ ] **Step 6: Smoke-test against an empty data dir**

Run: `bin/jetstream inspect-all --data-dir /tmp/nonexistent-jetstream`
Expected: prints the header, network totals (all zeros), two tree entries each `(empty)`, `collections (0 distinct NSIDs):  (none)`, no warnings, exit 0.

- [ ] **Step 7: Run the existing test suite**

Run: `just test ./cmd/jetstream/...`
Expected: PASS for all existing tests (`inspect_segment_test.go`, `main_test.go`, `serve_test.go`). The new file has no tests yet; that's Task 7.

- [ ] **Step 8: Lint**

Run: `just lint`
Expected: clean.

- [ ] **Step 9: Commit**

```bash
git add cmd/jetstream/format.go cmd/jetstream/inspect_all.go cmd/jetstream/main.go
git commit -m "$(cat <<'EOF'
add jetstream inspect-all CLI subcommand

Wires up status.InspectAll behind a new inspect-all subcommand that
walks <data-dir>/segments and <data-dir>/backfill/live_segments and
prints a plain-text database-wide summary: network totals, per-tree
rollups, per-collection breakdown, and warnings if any. Mirrors
inspect-segment's style (no TUI library, RFC3339-micros UTC,
1024-base byte sizes, comma-grouped counts).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: Golden-text test for the inspect-all renderer

**Files:**
- Create: `cmd/jetstream/inspect_all_test.go`
- Create: `cmd/jetstream/testdata/inspect_all_basic.golden`

The renderer is pure (in -> string), so a single golden test is enough to lock the layout.

- [ ] **Step 1: Write the failing test**

Create `cmd/jetstream/inspect_all_test.go`:

```go
package main

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/status"
	"github.com/stretchr/testify/require"
)

var updateGolden = flag.Bool("update-golden", false, "rewrite golden files from current renderer output")

func TestRenderInspectAll_Basic(t *testing.T) {
	t.Parallel()

	agg := &status.SegmentAggregate{
		Trees: []status.TreeAggregate{
			{
				Dir:               "/data/segments",
				SealedCount:       3,
				ActiveCount:       1,
				CompressedBytes:   2 * 1024 * 1024,
				UncompressedBytes: 6 * 1024 * 1024,
				DiskBytes:         3 * 1024 * 1024,
				EventCount:        1234,
				BlockCount:        12,
				MinSeq:            10,
				MaxSeq:            1243,
				MinIndexedAt:      time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC),
				MaxIndexedAt:      time.Date(2026, 5, 28, 0, 0, 0, 0, time.UTC),
				OldestMTime:       time.Date(2026, 5, 27, 0, 1, 0, 0, time.UTC),
				NewestMTime:       time.Date(2026, 5, 28, 0, 1, 0, 0, time.UTC),
				LatestSegment: &status.SegmentSummary{
					Index:           4,
					Sealed:          false,
					EventCount:      300,
					UniqueDIDCount:  150,
					BlockCount:      3,
					CollectionCount: 2,
					MinSeq:          944,
					MaxSeq:          1243,
					SizeBytes:       768 * 1024,
				},
			},
			{Dir: "/data/backfill/live_segments"},
		},
		Collections: []status.CollectionAggregate{
			{NSID: "app.bsky.feed.post", EventCount: 900, SegmentCount: 4, BlockCount: 9},
			{NSID: "app.bsky.feed.like", EventCount: 300, SegmentCount: 2, BlockCount: 2},
			{NSID: "app.bsky.graph.follow", EventCount: 34, SegmentCount: 1, BlockCount: 1},
		},
		Network: status.NetworkTotals{
			Segments:          4,
			SealedSegments:    3,
			ActiveSegments:    1,
			Blocks:            12,
			Events:            1234,
			Collections:       3,
			CompressedBytes:   2 * 1024 * 1024,
			UncompressedBytes: 6 * 1024 * 1024,
			DiskBytes:         3 * 1024 * 1024,
			MinSeq:            10,
			MaxSeq:            1243,
			MinIndexedAt:      time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC),
			MaxIndexedAt:      time.Date(2026, 5, 28, 0, 0, 0, 0, time.UTC),
		},
		Warnings: []string{
			"/data/segments/seg_0000000002.jss: corrupt segment: bad magic \"XXXX\"",
		},
	}

	generatedAt := time.Date(2026, 5, 28, 12, 34, 56, 0, time.UTC)
	var buf bytes.Buffer
	require.NoError(t, renderInspectAll(&buf, "/data", agg, generatedAt, 100))

	goldenPath := filepath.Join("testdata", "inspect_all_basic.golden")
	if *updateGolden {
		require.NoError(t, os.MkdirAll("testdata", 0o755))
		require.NoError(t, os.WriteFile(goldenPath, buf.Bytes(), 0o644))
		return
	}
	want, err := os.ReadFile(goldenPath)
	require.NoError(t, err, "missing golden; rerun with -update-golden")
	require.Equal(t, string(want), buf.String())
}
```

- [ ] **Step 2: Generate the golden file**

Run: `go test ./cmd/jetstream/ -run TestRenderInspectAll_Basic -update-golden`
Expected: PASS, creates `cmd/jetstream/testdata/inspect_all_basic.golden`.

Read the generated file to confirm it looks right (you should see the network totals, both trees, the three-row collection table, and the warning).

- [ ] **Step 3: Run the test without the flag to verify it passes against the golden**

Run: `go test ./cmd/jetstream/ -run TestRenderInspectAll_Basic -v`
Expected: PASS.

- [ ] **Step 4: Run the full cmd/jetstream test suite**

Run: `just test ./cmd/jetstream/...`
Expected: all PASS.

- [ ] **Step 5: Lint**

Run: `just lint`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add cmd/jetstream/inspect_all_test.go cmd/jetstream/testdata/inspect_all_basic.golden
git commit -m "$(cat <<'EOF'
golden test for the inspect-all renderer

Locks the renderer's plain-text layout against a deterministic
*SegmentAggregate fixture covering a non-empty primary tree, an
empty live_segments tree, three sorted collections, and a single
warning. Use -update-golden to refresh after intentional layout
changes.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 8: Final repo-wide validation

**Files:**
- None (verification only)

End-of-feature gate. Confirms the new code interacts cleanly with the rest of the repo and nothing unrelated regressed.

- [ ] **Step 1: Build everything with race detector**

Run: `go build -race ./...`
Expected: clean build. (Race-build catches some accidental shared-state bugs the regular build misses.)

- [ ] **Step 2: Run the full short test suite**

Run: `just test`
Expected: all PASS. If any unrelated test fails, investigate before claiming done.

- [ ] **Step 3: Run race tests for the changed packages**

Run: `just test-race ./internal/status/... ./cmd/jetstream/... ./internal/web/...`
Expected: all PASS. Confirms `InspectAll` plus the existing `Collector` singleflight don't interact poorly under concurrent access.

- [ ] **Step 4: Final lint pass**

Run: `just lint`
Expected: clean.

- [ ] **Step 5: Manual smoke against a populated data dir if one is available**

If you have a local dev data dir with segments (e.g. `./data` from running the simulator):

Run: `bin/jetstream inspect-all --data-dir ./data | head -60`
Expected: real network totals, real collections, no warnings (or only rotation-tail warnings).

If no local data dir exists, this step is skipped — the unit + integration tests cover the behavior.

- [ ] **Step 6: Verify the /status page renders against a live process**

If you have the simulator running, start jetstream:

Run: `JETSTREAM_DATA_DIR=./data bin/jetstream serve`

Then in another shell: `curl -s http://localhost:8080/status | grep -A 5 'Network\|Collections'`
Expected: HTML containing the new sections.

If no simulator setup is handy, this step is skipped.

- [ ] **Step 7: No commit needed**

This task makes no source changes; the previous task commits are the final state.

---

## Self-review notes

**Spec coverage:**
- Package layout (internal/status, no public segment API growth) — Tasks 2, 3.
- Data shapes (`SegmentAggregate`, `TreeAggregate`, `CollectionAggregate`, `NetworkTotals`, `InspectAllOptions`) — Task 2.
- Snapshot reshape (`Segments`/`LiveSegs` removed, `*SegmentAggregate` added) — Task 5.
- Algorithm (per-tree iteration, error policy with tail tolerance, SkipUnsealed, foldInspection) — Tasks 3, 4.
- CLI surface (`--data-dir`, `--skip-unsealed`, `--collections-truncate`) — Task 6.
- Output layout (network totals, trees, collections, warnings) — Tasks 6, 7.
- /status integration (one InspectAll call replaces two collectSegmentTree calls; template gains Network and Collections sections; warnings callout) — Task 5.
- Testing (per-tree counters, multi-tree merge, missing root, corrupt non-tail, corrupt tail, SkipUnsealed) — Tasks 2, 3, 4.
- Renderer test (golden layout) — Task 7.
- Status integration test (Snapshot.SegmentAggregate populated and tree dirs match) — Task 5 step 3.

**Out-of-scope items, confirmed not in plan:**
- C/U/D counts (header format change, separate work).
- Database-wide unique-DID estimates.
- Concurrent segment scanning.
- Per-segment table in the CLI.

**Type consistency:** function signatures, field names, and method names match across tasks. `InspectAll`/`InspectAllOptions`/`SegmentAggregate`/`TreeAggregate`/`CollectionAggregate`/`NetworkTotals` are spelled consistently; `microsToTime` only exists on `internal/status`; `formatTime`/`formatBytes`/`humanInt`/`errWriter`/`formatMicros` only exist on `cmd/jetstream`.

**Placeholders:** none — every code block is complete and runnable.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-28-inspect-all.md`. Two execution options:

**1. Subagent-Driven (recommended)** — A fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?


