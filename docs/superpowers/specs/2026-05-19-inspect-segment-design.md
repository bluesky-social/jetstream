# `jetstream inspect-segment` design

## Goal

Add a `jetstream inspect-segment <path>` subcommand that produces a single
plain-text report describing the structure and contents of a segment file.
The output must be readable by humans at a terminal *and* easy to paste into
an LLM, so we use one fixed-width text format — no colors, no ANSI escapes,
no JSON / YAML / table libraries.

The command must work on both **sealed** segment files (the post-`Seal`
format with a finalized header and footer) and **active** segment files
(unsealed, currently being written, with a zero-checksum header and no
footer). Active-file support is what makes the tool useful during
incidents: ops can look at a segment that's mid-write without stopping
the writer.

## Non-goals

- Decoding individual events. The on-disk metadata (header, block index,
  collection index, per-block stats) is enough to answer "what's in this
  segment?". Adding event-level dump would multiply output size by orders
  of magnitude and is best left to a separate tool if/when we want it.
- Repair, edit, or any write path. Inspect is read-only.
- Pretty multi-format output. One text format. No `--json` flag in v1.
- Streaming output. Segments are bounded (~256 MB target); the whole
  inspection result fits comfortably in memory.

## Surface

```
jetstream inspect-segment [flags] <path>
```

Flags (all optional):

| flag | default | meaning |
|---|---|---|
| `--blocks` | `table` | One of `summary`, `table`, `full`. `summary` skips the per-block list entirely. `table` prints the compact per-block table. `full` adds per-block collection NSIDs after each row. |
| `--blocks-truncate` | `100` | When `--blocks` ≠ `summary` and `BlockCount` exceeds this threshold, print only the first N/2 and last N/2 rows with a `... (N rows omitted) ...` marker between them. `0` disables truncation. |

Exit codes:

- `0` — success, including the case where a sealed file's checksum failed
  to verify but enough metadata could still be parsed to produce a report.
  The mismatch is surfaced as `checksum: invalid` in the output.
- `1` — the file could not be opened, was smaller than the 256-byte
  reserved header, did not start with the `jss0` magic, or any other
  unrecoverable parse failure.

## Active vs sealed detection

After opening the file we read the 256-byte fixed header and dispatch on
two fields:

1. Bytes 0..3 must equal `jss0` (`segmentMagic`). Anything else is a hard
   error: "not a jetstream segment file".
2. Bytes 4..11 (the xxh3 checksum field) determine state:
   - **Zero** ⇒ active (unsealed). The rest of the header is also zero by
     contract; the framed-block region runs from offset 256 to EOF.
   - **Non-zero** ⇒ sealed. Hand off to the existing `segment.Reader`.

This matches the contract `decodeHeader` already enforces — it explicitly
rejects zero-checksum input as "active file?".

## Public segment API additions

We add one entry point and one result type to the `segment` package, in a
new `segment/inspect.go`:

```go
// Inspection is the unified active+sealed view used by the
// inspect-segment CLI. All offset/size fields are zero where they don't
// apply (e.g. footer-section sizes on an active file).
type Inspection struct {
    Path     string
    FileSize int64
    Sealed   bool

    // Header is fully populated when Sealed. For active files only
    // Version is meaningful (currently always 0 by the active-file
    // invariant); the rest is zero.
    Header   Header

    // Blocks is the per-block info. Sealed: parsed from the on-disk
    // block index (cheap). Active: built by walking framed blocks
    // (decompresses every block).
    Blocks   []BlockInfo

    // Collections is the segment's NSID string table. Sealed-only
    // (the collection index is footer-only); empty for active files.
    Collections      []string
    // BlockCollections[i] is the sorted collection IDs in block i.
    // For active files this is built from the frame walk; for sealed
    // files it comes from the collection index.
    BlockCollections [][]uint32

    // Aggregates derived during inspection.
    TotalEvents    uint64
    UniqueDIDCount uint32
    MinSeq, MaxSeq uint64
    MinIndexedAt   int64
    MaxIndexedAt   int64

    // Footer-section sizes; zero for active files.
    BlockIndexBytes      uint64
    SegmentBloomBytes    uint64
    BlockBloomsBytes     uint64
    CollectionIndexBytes uint64
    PerBlockBloomBytes   uint32

    // ChecksumValid is true only when Sealed and the recomputed
    // xxh3 over header[12:] || footer matched the value stored in
    // header bytes 4..11. False on mismatch (the report still gets
    // produced — Inspect surfaces the mismatch rather than failing).
    ChecksumValid bool
}

// Inspect parses the segment file at path and returns a single
// snapshot suitable for the CLI text renderer. Inspect does its own
// checksum verification (rather than relying on Reader.Open's reject
// path) so corrupted-but-parsable files can still be inspected.
func Inspect(path string) (*Inspection, error)
```

### Sealed path

Use `Reader.Open` with `SkipChecksum: true`, then run our own xxh3
recomputation manually so we can record the result instead of failing.
We pull `Header()`, `Blocks()`, `Collections()`, and per-block collections
via `BlockCollections(i)` directly off the reader. Footer-section byte
sizes are derived from the header offsets:

```
BlockIndexBytes      = DIDBloomOffset       - BlockIndexOffset
SegmentBloomBytes    = BlockDIDBloomOffset  - DIDBloomOffset
BlockBloomsBytes     = CollectionIndexOffset - BlockDIDBloomOffset
CollectionIndexBytes = FileSize             - CollectionIndexOffset
PerBlockBloomBytes   = (BlockBloomsBytes - 8) / BlockCount
                       (zero when BlockCount == 0)
```

The `Reader` already exposes `perBlockBloomSize` internally; we either add
a small accessor or recompute from offsets. Recomputing is fine — the
math is unambiguous.

### Active path

There is no `Reader` for active files today. We need a frame walker. The
existing `seal.go::walkBlocks` and `readFrameAt` already do this work, but
they're attached to `*Writer` and assume a write-mode `*os.File`. We
refactor them out:

1. Move `readFrameAt` to a free function `readFrameAt(f *os.File, off,
   maxOff int64) ([]byte, int, error)` (it only needs an `io.ReaderAt`,
   really). Update `Writer.readFrameAt` to forward to it.
2. Move the per-frame iteration logic out of `walkBlocks` into a free
   function `walkActiveFrames(f *os.File, footerOffset int64, maxEvents
   int) (blockWalkResult, error)`. `Writer.walkBlocks` becomes a
   one-line wrapper that supplies `w.file` and `w.cfg.MaxEventsPerBlock`.

This refactor is mechanical, semantics-preserving, and gives the inspector
a clean entry point for active files without duplicating the wire-format
decode. Existing seal tests cover the wrapper.

For `Inspect` on an active file:

- Open `O_RDONLY`.
- Stat the file; if smaller than 256 bytes ⇒ `ErrCorruptSegment`.
- Read the header bytes; verify magic; confirm checksum == 0.
- Active files have no recorded "footer offset"; the framed-block region
  runs from 256 to EOF. Pass `fileSize` as the upper bound to
  `walkActiveFrames`.
- Translate `blockWalkResult` into the `Inspection` fields. Note: an
  active file's "Collections" slice is the in-progress string table the
  walk built; we record it but flag `Sealed: false` so the renderer can
  label the section appropriately.
- Aggregates (`TotalEvents`, `UniqueDIDCount`, etc.) come straight from
  the walk result.

If the walk encounters a torn frame at EOF (one that claims more bytes
than remain before EOF), we return the partial `Inspection` populated up
to the last clean frame, plus a non-nil error wrapping
`ErrCorruptSegment`. The CLI converts this into an exit-0 report with a
clear `WARNING: tail frame at offset N is truncated` line followed by
the rest of the report — operators want to see how much of an
mid-crash file is salvageable.

## CLI layer

New file `cmd/jetstream/inspect_segment.go` with two pieces:

1. `inspectSegmentCommand() *cli.Command` — flag wiring, calls into
   `segment.Inspect`, then into the renderer.
2. `renderInspection(w io.Writer, ins *segment.Inspection, blocksMode
   string, truncate int)` — writes the text report.

Wired into `newApp()` in `main.go` alongside `serveCommand` and
`versionCommand`.

### Output format

Plain ASCII. Sections separated by a blank line. Numbers are decimal
unless they're file offsets (always `0x` hex, zero-padded to 8 hex
digits — keeps columns aligned for any segment up to ~256 GB).
Timestamps are RFC3339 in UTC. Sizes are raw bytes, no humanization
(LLMs handle math fine; humans don't lose by reading "268435328 bytes"
once).

Sealed example:

```
file: /data/segments/000123.jss
size: 268435328 bytes
state: sealed
magic: jss0
version: 1
checksum: 0x9f3b1c2e7a55d901 (valid)

header summary:
  block_count:       128
  event_count:       524288
  unique_did_count:  42103
  seq range:         [1735000000, 1735524287]
  indexed_at range:  [2026-05-19T08:00:00.000000Z, 2026-05-19T08:14:23.451234Z]

footer layout (all offsets absolute; block_index_offset is also the footer start):
  block_index_offset:      0x10000100  block_index_size:       4608 bytes
  did_bloom_offset:        0x10001300  segment_bloom_size:     2048 bytes
  block_did_bloom_offset:  0x10001b00  per_block_blooms:       128 x 2056 bytes (incl. 8B region header)
  collection_index_offset: 0x10042100  collection_index_size:  1872 bytes
  end_of_file:             0x10042870

collections (3 distinct NSIDs):
  [  0] app.bsky.feed.post     blocks: 128
  [  1] app.bsky.feed.like     blocks: 121
  [  2] app.bsky.graph.follow  blocks:  89

blocks (128 total):
  idx       offset  comp_size  uncomp_size  events     min_seq     max_seq  cols
    0   0x00000100     163840       524288    4096  1735000000  1735004095     3
    1   0x00028108     163712       524288    4096  1735004096  1735008191     3
  ...
```

Active example (the differences):

```
file: /data/segments/active.jss
size: 17428992 bytes
state: active (unsealed; frame walk)
magic: jss0
version: -
checksum: 0x0 (active)

header summary:
  block_count:       4 (discovered via frame walk)
  event_count:       16384
  unique_did_count:  9112 (from walk; not durable until seal)
  seq range:         [1735524288, 1735540671]
  indexed_at range:  [2026-05-19T08:14:23.500000Z, 2026-05-19T08:14:51.001000Z]

footer layout: not present (active file)

collections discovered during walk (3):
  app.bsky.feed.post
  app.bsky.feed.like
  app.bsky.graph.follow

blocks (4 total):
  idx       offset  comp_size  uncomp_size  events     min_seq     max_seq  cols
  ...
```

The `--blocks=full` mode appends, after each row in the blocks table,
indented "collections:" lines naming the per-block NSIDs.

The `--blocks-truncate=N` behavior: when `BlockCount > N`, print the
first `N/2` rows, an indented `... (B rows omitted) ...` separator,
then the last `N/2` rows. Truncation never applies in `--blocks=full`
mode (the user opted into full detail).

## File layout

| file | role |
|---|---|
| `segment/inspect.go` | new — public `Inspect`, refactored `walkActiveFrames`, free-function `readFrameAt` |
| `segment/seal.go` | edit — `Writer.walkBlocks` becomes a thin wrapper around `walkActiveFrames`; `Writer.readFrameAt` becomes a thin wrapper around the free function |
| `segment/inspect_test.go` | new — see Testing |
| `cmd/jetstream/inspect_segment.go` | new — `inspectSegmentCommand` + `renderInspection` |
| `cmd/jetstream/inspect_segment_test.go` | new — CLI smoke test |
| `cmd/jetstream/main.go` | edit — register `inspectSegmentCommand()` in `newApp().Commands` |

## Testing

- **`segment/inspect_test.go`**:
  - Sealed roundtrip: build a writer with a deterministic event stream,
    seal, `Inspect`, assert every field matches what `Reader` reports
    independently. `ChecksumValid` must be true.
  - Active inspect: same writer, *without* sealing. `Inspect` reports
    correct block count, event count, seq range, and a non-empty
    collection set. `Sealed` is false.
  - Empty active file (just-created, header zeroed, no frames):
    Inspect succeeds, reports zero blocks, zero events, no error.
  - Corrupt sealed file: flip one byte inside the segment bloom region.
    `Inspect` succeeds, returns `ChecksumValid: false`, populates
    everything else.
  - Torn-tail active file: write a few blocks, append 4 garbage bytes
    at EOF (a torn frame-length prefix). `Inspect` returns the partial
    `Inspection` plus a wrapped `ErrCorruptSegment`.
- **`cmd/jetstream/inspect_segment_test.go`**:
  - Build a sealed fixture in `t.TempDir()`. Run `newApp().Run(ctx,
    []string{"jetstream", "inspect-segment", path})`. Capture stdout
    via the `cli.Command.Writer`. Assert presence of `state: sealed`,
    `checksum: 0x... (valid)`, `block_count:`, the blocks-table header,
    and the path itself.
  - Repeat with `--blocks=summary` — assert the blocks-table header is
    absent.
  - Repeat with a corrupted file — assert `(invalid)` appears next to
    the checksum and exit code is 0.
- **No new fuzz target.** The frame-walking code already gets fuzz
  coverage via `seal_fuzz_test.go` since it's the same code path now;
  the inspector is the same walk plus a presentation layer.

## Risks and trade-offs

- **`walkActiveFrames` is `O(file_size)` and decompresses every block.**
  On a ~256 MB segment with 64k blocks that's a real cost. Acceptable
  because (a) `inspect-segment` is an interactive ops tool, not a hot
  path, and (b) it's still cheaper than what `Seal` already does on
  every flush.
- **Output for very large block counts.** Mitigated by
  `--blocks-truncate`. Default of 100 keeps reports skim-able; ops
  can override.
- **Refactoring `walkBlocks` and `readFrameAt`.** The public surface of
  `segment` doesn't change for any existing consumer; the seal path
  keeps its current signatures via wrappers. Existing seal tests
  (`seal_test.go`, `seal_recovery_test.go`, `seal_fuzz_test.go`,
  `seal_swarm_test.go`) cover the refactor.
- **Active-file unique-DID count is not the same as the eventual
  sealed value** if the writer continues appending after we walk it.
  We label it `(from walk; not durable until seal)` in the output to
  make this explicit.
