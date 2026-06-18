# inspect-all CLI + /status enrichment

## Summary

Add a new `jetstream inspect-all` CLI subcommand that walks every segment
file under a data directory and prints a database-wide summary (network
totals, per-tree rollups, per-collection breakdown). Unify the same
aggregation behind the existing `/status` HTTP endpoint so the HTML page
gains the same richer data.

Aggregation lives in `internal/status` so it's not a public API of the
`segment` package. It depends only on `segment` and stdlib so the CLI
can call it offline against a data directory while a separate
`jetstream serve` process holds Pebble.

## Motivation

`jetstream inspect-segment <path>` already gives a per-file view but
operators have no way to ask "what's in the whole database?" without
scripting around it. The /status page reports per-tree byte counts and a
single latest-segment summary, but says nothing about per-collection
counts, total events, or seq/indexed_at coverage across the database.

The witness prototype had a useful `inspect-all` CLI command for this
shape of question; this spec ports the idea to jetstream-v2 and lifts
the same aggregation into the live status surface.

## Non-goals

- **Creates / updates / deletes counts.** The sealed-segment header
  doesn't carry these fields; per-event op type lives inside compressed
  block bodies. Surfacing them would require a header format change and
  writer updates. Out of scope for this work.
- **Exact unique-DID counts.** Per-segment unique-DID counts exist in
  the header but they over-count when a DID is active in multiple
  segments. Bloom-filter union is approximate and adds complexity. We
  drop database-wide unique-DID estimates entirely; the per-segment
  count remains in the latest-segment summary.
- **Concurrent segment scanning.** Single-threaded aggregation is the
  v1 baseline. Correctness first; profile before parallelizing.
- **Per-segment table.** The CLI report does not list one row per
  segment file. Operators run `jetstream inspect-segment <path>` for
  per-file detail. Per-collection rollup is the unit of summary.

## Package layout

New file `internal/status/inspect_all.go` adds:

- Function: `InspectAll(roots []string, opts InspectAllOptions) (*SegmentAggregate, error)`
- Types: `SegmentAggregate`, `TreeAggregate`, `CollectionAggregate`,
  `NetworkTotals`, `InspectAllOptions`.

Dependency direction:

- `cmd/jetstream/inspect_all.go` (new) → `internal/status` (new function).
- `internal/status/collect.go` → calls `InspectAll` instead of two
  `collectSegmentTree` calls.
- `internal/status/inspect_all.go` → `segment`, `internal/ingest` (for
  `ParseSegmentIndex`), stdlib only. No `*store.Store`, no Pebble.

## Data shapes

```go
// SegmentAggregate is the rendering-agnostic, database-wide view of all
// segment files under one or more roots. Built by InspectAll; consumed
// by Collector.build (for the /status snapshot) and the inspect-all CLI.
type SegmentAggregate struct {
    Trees       []TreeAggregate       // one per root, in input order
    Collections []CollectionAggregate // sorted by Events desc, NSID asc tiebreak
    Network     NetworkTotals
    Warnings    []string              // "<path>: <err>" for tolerated per-file errors
}

// TreeAggregate is a per-root rollup. Replaces the old SegmentTreeStats;
// supersets it with new aggregate counters.
type TreeAggregate struct {
    Dir               string
    SealedCount       int
    ActiveCount       int
    CompressedBytes   int64           // sum of block compressed sizes
    UncompressedBytes int64           // sum of block uncompressed sizes
    DiskBytes         int64           // sum of file sizes (incl. headers/footers/indexes)
    EventCount        uint64
    BlockCount        uint64
    OldestMTime       time.Time
    NewestMTime       time.Time
    MinSeq            uint64          // 0 if no records
    MaxSeq            uint64
    MinIndexedAt      time.Time       // zero if no records
    MaxIndexedAt      time.Time
    LatestSegment     *SegmentSummary // nil if dir empty
}

// CollectionAggregate is one row per distinct NSID seen anywhere in the
// scanned trees.
type CollectionAggregate struct {
    NSID         string
    EventCount   uint64
    SegmentCount int    // number of segments that mention this NSID
    BlockCount   uint64 // total blocks across all segments that contain it
}

// NetworkTotals is the database-wide rollup across all trees.
type NetworkTotals struct {
    Segments          int
    SealedSegments    int
    ActiveSegments    int
    Blocks            uint64
    Events            uint64
    Collections       int    // == len(SegmentAggregate.Collections)
    CompressedBytes   int64
    UncompressedBytes int64
    DiskBytes         int64
    MinSeq            uint64
    MaxSeq            uint64
    MinIndexedAt      time.Time
    MaxIndexedAt      time.Time
}

type InspectAllOptions struct {
    SkipUnsealed bool // skip the active-file frame walk; sealed files only
}
```

`SegmentSummary` is the existing type from `internal/status/snapshot.go`,
unchanged. Keeping it preserves the latest-segment binding the HTML
template already uses.

## Snapshot reshape

`internal/status/snapshot.go::Snapshot` changes:

- Remove fields: `Segments SegmentTreeStats`, `LiveSegs SegmentTreeStats`.
- Remove type: `SegmentTreeStats` (subsumed by `TreeAggregate`).
- Add field: `SegmentAggregate *SegmentAggregate`.

The /status HTML template binds the existing "tree" sub-template to
`.SegmentAggregate.Trees[0]` and `.SegmentAggregate.Trees[1]` instead of
`.Segments` / `.LiveSegs`. Two new top-level sections render
`.SegmentAggregate.Network` and `.SegmentAggregate.Collections`.

## Algorithm

`InspectAll(roots, opts)`:

1. **For each root, in input order:**
   - `os.ReadDir(root)`. `fs.ErrNotExist` yields an empty
     `TreeAggregate{Dir: root}`, no error. Other read errors are fatal.
   - Filter entries to `seg_*.jss` via `ingest.ParseSegmentIndex`. Sort
     by index ascending. (`ParseSegmentIndex` is already used at
     `internal/status/collect.go:122`.)
   - For each file:
     - If `opts.SkipUnsealed`: read 12 bytes, check the checksum field.
       Zero means active; record `ActiveCount++` and `DiskBytes += info.Size()`,
       skip frame walk.
     - Otherwise call `segment.Inspect(path)`:
       - On error, record a warning (`"<path>: <err>"`) UNLESS this is
         the highest-idx file in the tree (rotation-race tolerance,
         matching the existing collector at `internal/status/collect.go:158-162`).
         File is excluded from aggregates.
       - On success, fold the `*Inspection` into the running tree aggregate
         and into a `map[string]*CollectionAggregate` shared across all trees.
   - Stash the highest-idx successful `Inspection` as the tree's
     `LatestSegment`.

2. **Folding rules:**
   - Tree-level counters accumulate by simple addition. Header fields
     are widened: `header.EventCount` (uint32) → `tree.EventCount`
     (uint64); same for `BlockCount`.
   - `MinSeq`/`MaxSeq`/`MinIndexedAt`/`MaxIndexedAt` accumulate only when
     the file's `TotalEvents > 0`. Empty segments don't pull bounds to
     zero. `MinIndexedAt`/`MaxIndexedAt` are converted from unix-micros
     to `time.Time` at the boundary.
   - For each NSID `i` in the file's `Collections[]`:
     - `agg := collections[nsid]`; create if missing.
     - `agg.EventCount += uint64(ins.CollectionEventCounts[i])`.
     - `agg.SegmentCount++` (one increment per segment that contains the NSID).
     - `agg.BlockCount` accumulates the count of blocks in this segment
       that mention the NSID. Compute by walking
       `ins.BlockCollections[][]uint32` once: for each block index `b`,
       for each collection-id `id` in that block, increment a local
       per-id counter; then add each counter into the corresponding
       `collections[Collections[id]].BlockCount`. This keeps the
       per-NSID block contribution at exactly one increment per
       (segment, block) pair that contains the NSID, with no
       double-counting.

3. **Finalize:**
   - Materialize the collections map to a slice; sort by `EventCount`
     desc with NSID asc tiebreak.
   - Compute `Network` totals from the per-tree slices.

4. **Errors policy summary:**
   - Fatal: root readdir error other than `ErrNotExist`.
   - Tolerated and recorded in `Warnings`: per-file `Inspect` error on
     non-tail files.
   - Tolerated and silent: per-file `Inspect` error on the
     highest-idx file (rotation race).

5. **Concurrency safety:** `segment.Inspect` opens its own files per
   call; the package documents goroutine safety
   (`segment/doc.go:18`, `segment/reader.go:45-50`). v1 is single-threaded
   regardless; documenting safety here so future parallelization is unblocked.

## CLI surface

`cmd/jetstream/inspect_all.go` registers a new subcommand under
`newApp`'s `Commands` slice (alongside `inspectSegmentCommand()`).

Flags:

- `--data-dir` (string, default `./data`, env `JETSTREAM_DATA_DIR`).
  Same env var as `serve` so operators don't have to think.
- `--skip-unsealed` (bool, default false). Fast-mode toggle.
- `--collections-truncate` (int, default 100; 0 = no truncation).
  Matches `inspect-segment`'s `--blocks-truncate` for parallel
  operator ergonomics.

The action computes:
```
roots := []string{
    filepath.Join(dataDir, "segments"),
    filepath.Join(dataDir, "backfill", "live_segments"),
}
agg, err := status.InspectAll(roots, status.InspectAllOptions{SkipUnsealed: ...})
```
then renders to `cmd.Root().Writer` using the same `errWriter` printf
pattern as `inspect_segment.go`. No external TUI library.

### Output layout

```
inspect-all
data-dir: ./data
generated: 2026-05-28T17:42:31.000000Z

network totals:
  segments:               1234 (1233 sealed, 1 active)
  blocks:                 58,231
  events:                 412,834,991
  collections:            42 distinct NSIDs
  seq range:              [1, 412834991]
  indexed_at range:       2025-01-04T00:00:00.000000Z → 2026-05-28T17:41:58.123456Z
  payload (uncompressed): 87.4 GiB
  payload (compressed):   31.2 GiB
  disk usage:             32.1 GiB
  compression ratio:      2.80x

trees:
  [0] segments/
        files:        1233 sealed + 0 active
        events:       411,200,000
        blocks:       58,112
        seq range:    [1, 411200000]
        indexed_at:   2025-01-04T00:00:00.000000Z → 2026-05-27T22:14:11.000000Z
        oldest mtime: 2025-01-04T00:00:01Z
        newest mtime: 2026-05-27T22:14:18Z
        compressed:   31.0 GiB
        uncompressed: 87.0 GiB
        disk:         31.9 GiB
        latest:       idx=1232 sealed events=320,000 blocks=12 size=26.1 MiB
  [1] backfill/live_segments/
        (empty)

collections (42 distinct NSIDs):
  [  0] app.bsky.feed.post              events: 251,034,118  segments: 1234  blocks: 47,210
  [  1] app.bsky.feed.like              events:  93,442,001  segments: 1180  blocks:  8,012
  ...

warnings (1):
  /home/jcalabro/data/segments/seg_0001033.jss: corrupt segment: bad magic "..."
```

Conventions:
- Counts: humanized with comma group separators.
- Bytes: KiB/MiB/GiB/TiB via a shared helper.
- Timestamps: RFC3339 micros UTC. Reuse `formatMicros` from
  `inspect_segment.go`; promote it to a shared helper (e.g.
  `cmd/jetstream/format.go`).
- Empty trees print `(empty)` instead of zero-row dump.
- `warnings:` section is omitted entirely when `len(Warnings) == 0`.
- Collections truncation: when `len(Collections) > truncate > 0`, print
  the first `truncate/2` rows, a `... (N rows omitted) ...` line, and
  the last `truncate/2` rows. Mirrors `inspect-segment`'s blocks
  truncation.

## /status integration

`internal/status/collect.go::build()` changes:

- Replace the two `collectSegmentTree` calls with one
  `InspectAll([segments, backfill/live_segments], InspectAllOptions{})`.
  No `SkipUnsealed` for the live page — accuracy matters and the cost is
  one frame walk per tree at most.
- Populate `Snapshot.SegmentAggregate` from the result.
- Remove `Snapshot.Segments` and `Snapshot.LiveSegs`.

`collectSegmentTree` and `buildSegmentSummary` are deleted. `microsToTime`
becomes part of the new `inspect_all.go` (the only remaining caller).

`internal/web/templates/status.html` changes:

- "Segments" section's two `{{template "tree"}}` invocations bind to
  `.SegmentAggregate.Trees` instead of `.Segments` / `.LiveSegs`. Use
  `index .SegmentAggregate.Trees 0` defensively in case the slice is
  shorter than expected (it shouldn't be — `InspectAll` always returns
  one TreeAggregate per input root).
- New top-level section before "Segments": "Network", with the
  database-wide totals (segments, blocks, events, seq/indexed_at ranges,
  byte totals, compression ratio).
- New top-level section after "Segments": "Collections", an HTML table
  with columns NSID / Events / Segments / Blocks. No truncation
  (operators can scroll); CLI handles terminal-friendly truncation.
- Warnings: when `len(.SegmentAggregate.Warnings) > 0`, render a small
  callout at the top of the "Segments" section listing them.
- The existing `tree` sub-template gains a few new `<dt>/<dd>` rows for
  the new fields (events, blocks, seq range, indexed-at range).

Cache behavior unchanged: `Collector` still wraps `build()` with the
same TTL/singleflight; CLI does its own one-shot scan and does not
share the cache.

## Testing

Three tests, focused.

1. **`internal/status/inspect_all_test.go`** — aggregation arithmetic.
   One table-driven test that builds a small fixture (2-3 sealed
   segments under a temp dir using `segment.Writer` + `segment.Seal`
   with known counts/NSIDs/seq ranges) and asserts:
   - Per-tree counters and network totals sum correctly.
   - `Collections[]` has the right NSIDs sorted by event count, with
     correct `SegmentCount` (an NSID present in 2 segments has
     `SegmentCount==2`).
   - Missing root yields empty `TreeAggregate`, no error.
   - One corrupt sealed file (truncated mid-footer) is excluded,
     captured in `Warnings`, others aggregate correctly.

2. **`cmd/jetstream/inspect_all_test.go`** — renderer. Build a
   `*SegmentAggregate` literal in code, render it, golden-compare via a
   testdata file. No filesystem.

3. **Extend `internal/status/collect_test.go`** — one new sub-test that
   asserts `Snapshot.SegmentAggregate` is non-nil and its
   `Trees[0].Dir` / `Trees[1].Dir` match the data-dir paths.
   Doesn't re-test arithmetic; confirms the wiring.

The existing `internal/web/handler_test.go` will need its fake
snapshot updated to the new field shape; no new assertions added there.

## Migration / rollout

Single PR. No data-format changes, no on-disk migration. The /status
JSON shape is not currently advertised as a stable contract (the
endpoint returns HTML, not JSON), so the `Segments`/`LiveSegs` field
removal is internally observable only via templates. After this PR:

- `jetstream inspect-segment` is unchanged.
- `jetstream inspect-all --data-dir ./data` is the new entry point.
- `/status` HTML page renders the new sections automatically.

## Open questions / future work

- **Concurrent scanning.** Single-threaded is the v1 baseline. If
  operators report slow `/status` refresh on large databases, add a
  worker pool inside `InspectAll`. The merge logic is already
  goroutine-safe in design (single-threaded merge from a results
  channel); the wiring is the only delta.
- **Optional per-segment table.** Could add `--segments=table` flag
  to `inspect-all` if operators ask for it. Deferred until asked.
- **Creates/updates/deletes.** A header format change would unlock
  database-wide C/U/D rollups. Tracked separately.
