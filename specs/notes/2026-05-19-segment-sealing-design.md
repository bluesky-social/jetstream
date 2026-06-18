# Segment File Sealing — Footer, Finalized Header, and Reader

**Date:** 2026-05-19
**Scope:** Second major slice of the `./segment` package. Adds the `Seal()` operation to `Writer`, finalizing an active segment per DESIGN.md §3.1.2: variable-length footer (block index, segment-level DID bloom, per-block DID blooms, collection block index), and a 256-byte fixed header containing an xxh3 checksum over the metadata. Adds a public `Reader` type that parses sealed metadata and exposes a block-list API for downstream callers (server, replicas, third-party tools).

## 1. Goals

1. Implement segment sealing exactly per DESIGN.md §3.1.2 / §3.1.3 / §3.1.4. After `Seal()`, a sealed segment is interrogatable as a self-describing immutable file with no out-of-band metadata.
2. Ship a public `Reader` type with a deliberately small block-list API. Higher-level iterators (`iter.Seq2`, DID/collection narrowing) compose on top of the primitives this slice ships.
3. Detect sealed-vs-active state at file open time. `Writer.New()` rejects sealed files with `ErrSegmentSealed`; `Reader.Open()` rejects active files.
4. Recover deterministically from a crash *during* seal (footer durable, header still zero) — and from any subsequent partial torn-tail state — using the existing `lastGoodOffset` truncation path.
5. Keep the segment package pure: no goroutines, no timers, no contexts, no metadata side-channels. Lifecycle (when to seal) lives in the future ingestion orchestrator.
6. Preserve the existing performance budget: `just test` runs the full segment test suite in well under one second.

## 2. Non-Goals

The following are explicitly out of scope for this slice:

- Re-sealing of already-sealed segments. Lookaside compaction (DESIGN.md §3.3.1) and timestamp imports (§8) both rewrite sealed segments; both land in their own slices. The footer format is designed so future re-seal is a clean primitive over today's encode/decode helpers.
- A `Reader.Events()` iterator. The block-list API is sufficient; iterators are a thin layer that can be added later without changing on-disk format or Reader internals.
- Lookaside file format and writer.
- `backfill_complete.log`.
- Pebble metadata coupling and the orchestrator that decides *when* to seal. Segment package never imports pebble.
- Replication.
- Server-side caching of `Reader` instances or sealed-segment blooms. That belongs to `./internal/server` in a future slice.
- Operator-supplied per-block expected-item knobs. Per-block blooms are sized off `MaxEventsPerBlock`; segment-level bloom is sized off the exact unique-DID count measured during the seal walk.

## 3. Architecture

### 3.1 Package Layout

The segment package keeps its single-package boundary discipline. New files (one-word names per project convention):

```
./segment/
  doc.go                  (existing; updated overview)
  event.go                (existing)
  errors.go               (existing; new sentinels added)
  block.go                (existing)
  zstd.go                 (existing)
  writer.go               (existing; gains w.Seal())

  header.go               (NEW) fixed-header encode/decode + xxh3 helper
  footer.go               (NEW) variable-length footer encode/decode
  bloom.go                (NEW) gloom adapter + per-block-blooms packed region
  collection.go           (NEW) collection block index encode/decode
  seal.go                 (NEW) the walk-and-write pass
  reader.go               (NEW) public Reader type, ReaderAt-driven, goroutine-safe

  header_test.go
  footer_test.go
  bloom_test.go
  collection_test.go
  seal_test.go            // append → seal → reader roundtrip property test
  seal_swarm_test.go      // swarm tests over seal feature axes
  seal_fuzz_test.go       // FuzzReadHeader, FuzzReadFooter, FuzzReadCollectionIndex
  seal_recovery_test.go   // partial-seal crash recovery
  reader_test.go          // concurrent reads, bloom narrowing, checksum behavior
  seal_bench_test.go      // developer-invoked benchmarks
  testdata/
    golden_seal.bin       // pinned encoded sealed-segment fixture
    fuzz/...              // checked-in fuzz corpus
```

Boundary discipline:

- `header.go`, `footer.go`, `bloom.go`, `collection.go` are pure encode/decode. Each takes/returns `[]byte`. None touches `os.File`.
- `seal.go` and `reader.go` are the only new files that perform I/O.
- The conceptual "Collection Block Index" of DESIGN.md §3.1.4 lives in `collection.go`; type names retain the `CollectionIndex` term for clarity, but the file is one word.

### 3.2 Concurrency Model

- `Writer`: still single-threaded, caller-serialized. `Seal()` runs synchronously on the calling goroutine. No internal goroutines.
- `Reader`: goroutine-safe for read operations. Holds an immutable parsed footer and an `*os.File` opened `O_RDONLY`. All file I/O goes through `os.File.ReadAt` (pread) so multiple goroutines can read concurrently with no shared mutable state. The blooms and collection index, once parsed, are read-only memory. zstd decompression uses package-level `blockDecoder`, which klauspost documents as goroutine-safe for `DecodeAll`.

### 3.3 Dependency Additions

Two new dependencies, both already on the PRACTICES.md whitelist:

- `github.com/jcalabro/gloom` — bloom filters for segment-level + per-block DID filtering.
- `github.com/zeebo/xxh3` — segment-level xxh3 checksum (DESIGN.md §3.1.2).

Both are small and focused; no transitive concerns of note.

## 4. Public API

### 4.1 Writer.Seal

```go
// Seal finalizes the active segment file: flushes any pending events,
// walks the file's framed-block region to gather per-block stats,
// writes the variable-length footer at end-of-file, patches the
// finalized 256-byte fixed header at offset 0, fsyncs, and closes
// the underlying file.
//
// Seal consumes the Writer. After a successful Seal, the writer is
// closed; any subsequent Append/Flush/Seal/Close call returns
// ErrClosed (Close is idempotent and returns nil).
//
// On failure, the file is left in a state from which the caller can
// recover by opening a fresh Writer at the same path:
//   - If the failure is before the footer is durable, the file is
//     untouched.
//   - If the failure is after the footer is durable but before the
//     header is patched, Seal explicitly truncates the partial
//     footer back off and fsyncs before returning, restoring the
//     active-state "last byte is the last good frame" invariant.
//   - The caller's stickyErr is latched in either case so a confused
//     caller cannot accidentally Seal twice into a torn file.
//
// Seal performs no goroutine work. It is suitable to call from any
// goroutine that already serializes access to this Writer.
func (w *Writer) Seal() (SealResult, error)

// SealResult is returned by Seal so the caller (orchestrator) can
// log/emit metrics without re-opening the file.
type SealResult struct {
    BlockCount     uint32
    EventCount     uint32 // total events across all blocks
    UniqueDIDCount uint32
    MinSeq         uint64
    MaxSeq         uint64
    MinIndexedAt   int64 // unix micros
    MaxIndexedAt   int64 // unix micros
    Checksum       uint64 // xxh3 over version..end-of-footer
    FooterOffset   uint64 // file offset where the footer begins
    FileSize       int64  // total file size after seal
}
```

### 4.2 Reader

```go
// ReaderConfig controls Reader.Open behavior. Path is required.
type ReaderConfig struct {
    // Path is the sealed segment file. Required.
    Path string

    // SkipChecksum disables the xxh3 verification performed by Open.
    // The default (false) computes xxh3 over (version..end-of-footer)
    // and compares against the value in the fixed header, returning
    // ErrChecksumMismatch on mismatch.
    //
    // Operators that have already verified the file via an out-of-band
    // mechanism (e.g., a checked SHA-256 from CDN download) may opt
    // out to save the cost of re-hashing the metadata region.
    SkipChecksum bool
}

// Open parses the sealed segment file at cfg.Path. On success, the
// returned Reader holds the parsed metadata in memory and an
// O_RDONLY file handle for on-demand block decode. Reader is
// goroutine-safe.
func Open(cfg ReaderConfig) (*Reader, error)

// Close releases the underlying file handle. Idempotent.
func (r *Reader) Close() error

// Header returns a copy of the parsed fixed header.
func (r *Reader) Header() Header

// Blocks returns a copy of the parsed block index. len == BlockCount.
func (r *Reader) Blocks() []BlockInfo

// SegmentBloom returns the segment-level DID bloom filter. Read-only.
// Callers must not mutate the returned filter.
func (r *Reader) SegmentBloom() *gloom.Filter

// BlockBloom reads and unmarshals the per-block DID bloom for the
// given block index. Each call performs one pread; there is no
// internal cache. Callers that want every block's bloom should
// prefer LoadAllBlockBlooms.
//
// Returns ErrBlockOutOfRange if idx >= BlockCount.
func (r *Reader) BlockBloom(idx int) (*gloom.Filter, error)

// LoadAllBlockBlooms is a convenience wrapper that calls BlockBloom
// for every block in order. Returns one filter per block.
func (r *Reader) LoadAllBlockBlooms() ([]*gloom.Filter, error)

// Collections returns the collection string table, indexed by
// NSID id. Read-only; callers must not mutate the returned slice.
func (r *Reader) Collections() []string

// BlockCollections returns the NSID ids present in the given block,
// sorted ascending. Returns ErrBlockOutOfRange if idx >= BlockCount.
func (r *Reader) BlockCollections(idx int) ([]uint32, error)

// DecodeBlock reads, decompresses, and decodes the block at the
// given index. Returns the decoded events in their stored order.
//
// Returns ErrBlockOutOfRange if idx >= BlockCount.
func (r *Reader) DecodeBlock(idx int) ([]Event, error)
```

### 4.3 Header and BlockInfo

```go
// Header is the parsed form of the 256-byte fixed header. All
// offsets are absolute file offsets.
type Header struct {
    Version                uint16
    BlockCount             uint32
    EventCount             uint32
    UniqueDIDCount         uint32
    MinSeq, MaxSeq         uint64
    MinIndexedAt           int64 // unix micros
    MaxIndexedAt           int64 // unix micros
    FooterOffset           uint64
    DIDBloomOffset         uint64
    BlockDIDBloomOffset    uint64
    CollectionIndexOffset  uint64
    BlockIndexOffset       uint64
    Checksum               uint64
}

// BlockInfo is one entry of the block index (DESIGN.md §3.1.2).
type BlockInfo struct {
    Offset           uint64 // file offset of the block's length prefix
    CompressedSize   uint32 // frame bytes; excludes the 8-byte length prefix
    UncompressedSize uint32
    EventCount       uint32
    MinSeq, MaxSeq   uint64
}
```

### 4.4 Errors

Existing sentinels gain a real producer (`ErrSegmentSealed`). New sentinels:

```go
// ErrChecksumMismatch is returned by Reader.Open when the file's
// xxh3 checksum disagrees with the value in its fixed header.
ErrChecksumMismatch = errors.New("segment: checksum mismatch")

// ErrInvalidFooter is returned by Reader.Open when the variable-
// length footer fails structural validation (truncated, lengths
// overrun the file, internal pointers don't agree).
ErrInvalidFooter = errors.New("segment: invalid footer")

// ErrInvalidBlockIndex is returned by Reader.Open when a block
// index entry's offset/size pair doesn't fit within the file.
ErrInvalidBlockIndex = errors.New("segment: invalid block index")

// ErrBlockOutOfRange is returned by Reader.DecodeBlock /
// BlockBloom / BlockCollections when the requested block index
// is past BlockCount.
ErrBlockOutOfRange = errors.New("segment: block index out of range")
```

All errors are `errors.Is`-compatible sentinels. Wrapped values use `fmt.Errorf("%w: %s", ErrX, detail)`.

## 5. On-Disk Format

This section locks down byte-level details that DESIGN.md §3.1.2 specifies at a high level.

### 5.1 Fixed Header (256 bytes)

```
offset  size  field
------  ----  ------------------------------------------------------------
0       4     magic                  "jss0"
4       8     checksum               uint64 LE  (xxh3 of version..end-of-footer)
12      2     version                uint16 LE  (currently 1)
14      4     block_count            uint32 LE
18      4     event_count            uint32 LE
22      4     unique_did_count       uint32 LE
26      8     min_seq                uint64 LE
34      8     max_seq                uint64 LE
42      8     min_indexed_at         int64  LE  (unix micros)
50      8     max_indexed_at         int64  LE  (unix micros)
58      8     footer_offset          uint64 LE  (file offset where footer starts)
66      8     did_bloom_offset       uint64 LE  (absolute file offset)
74      8     block_did_bloom_offset uint64 LE  (absolute file offset)
82      8     collection_index_offset uint64 LE  (absolute file offset)
90      8     block_index_offset     uint64 LE  (absolute file offset)
98      158   _reserved              zero bytes (future expansion)
```

Total: 256 bytes.

**Active vs. sealed detection:** the 8 checksum bytes at offset 4 are zero on an active file (initialized that way by `initializeNewSegment`) and non-zero on a sealed file. The 2^-64 probability of a sealed-segment xxh3 happening to be exactly zero is negligible at our scale (project lifetime: ~10^5 sealed segments, not 10^18) and is not defended against. If it ever occurs, the file would be misclassified as active on next open, which is operator-visible and recoverable by re-sealing.

### 5.2 Block Index

Per DESIGN.md §3.1.2, packed without an internal header (block_count comes from the fixed header):

```
per entry (36 bytes):
  offset:            uint64 LE  (absolute file offset of length prefix)
  compressed_size:   uint32 LE  (frame bytes, excludes the 8-byte length prefix)
  uncompressed_size: uint32 LE
  event_count:       uint32 LE
  min_seq:           uint64 LE
  max_seq:           uint64 LE
```

Total section size: `block_count * 36`. Uncompressed — entries are read-mostly and the count (~thousands per segment) doesn't justify a compress/decompress cycle.

### 5.3 Segment-Level DID Bloom

A `gloom.Filter.MarshalBinary()` blob, no length prefix. Its byte range is implied by the surrounding offsets in the fixed header (`did_bloom_offset` to `block_did_bloom_offset`).

Sized at seal time as `gloom.New(uniqueDIDCount, 0.001)`. The 0.001 (0.1%) FP rate matches the project's archive-scale sizing guidance: bloom memory across all segments is small relative to data, and false-positive cost at scan time (one extra pread per FP) is what we want to minimize.

### 5.4 Per-Block DID Blooms

Per DESIGN.md §3.1.3 — packed contiguously, indexed by multiplication:

```
header (8 bytes, uncompressed):
  block_count:      uint32 LE
  bloom_size_bytes: uint32 LE  (each bloom identical size)

body:
  block_count × bloom_size_bytes bytes
```

Each per-block bloom is `gloom.New(MaxEventsPerBlock, 0.001)`. All filters are constructed with identical parameters, so `MarshalBinary` returns identically-sized buffers; the seal pass asserts this (panic on mismatch — a violation indicates a bug in gloom or our usage).

`Reader.BlockBloom(idx)` does a single `pread(block_did_bloom_offset + 8 + idx*bloom_size_bytes, bloom_size_bytes)` followed by `gloom.UnmarshalBinary`. No internal cache.

### 5.5 Collection Block Index

Per DESIGN.md §3.1.4 — header uncompressed, body zstd-compressed:

```
header (16 bytes, uncompressed):
  collection_count:  uint32 LE  (unique NSIDs in segment)
  block_count:       uint32 LE
  bitmask_len:       uint32 LE  (ceil(collection_count / 8))
  uncompressed_size: uint32 LE  (length of decompressed body)

body (zstd frame, content checksum on):
  string table:
    collection_count entries:
      len:   uint8
      nsid:  [len]byte
  bitmasks:
    block_count × bitmask_len bytes
```

NSID IDs are assigned by string-table position (0-indexed) in first-seen order during the seal walk. Bit `n` set in block `i`'s bitmask means NSID `n` is present in block `i`. NSIDs are bounded to 255 bytes by the same `uint8` collection_len column used in blocks, so the `len: uint8` field is consistent.

### 5.6 Footer Layout End-to-End

```
[fixed header, 256 bytes]
[block 0 length prefix + frame]
[block 1 length prefix + frame]
...
[block N-1 length prefix + frame]
[footer:
  block index               (block_count × 36 bytes)
  segment-level DID bloom   (variable size; gloom MarshalBinary)
  per-block DID blooms      (8-byte header + block_count × bloom_size_bytes)
  collection block index    (16-byte header + zstd frame body)
]
EOF
```

`footer_offset` in the fixed header points at the first byte of the block index (the start of the footer). `block_index_offset == footer_offset`; the redundant field exists so the header's section-pointer set is symmetric and future-extensible.

`Reader.Open` parses the footer with at most four pread calls:

1. Fixed header (256 bytes at offset 0).
2. Block index (`block_count × 36` bytes at `block_index_offset`).
3. Segment-level DID bloom (`block_did_bloom_offset - did_bloom_offset` bytes at `did_bloom_offset`).
4. Collection block index header (16 bytes at `collection_index_offset`), then the zstd-compressed body.

The per-block blooms region is *not* read at Open. `BlockBloom(idx)` preads on demand.

## 6. The Seal Pass

### 6.1 Preconditions and Ordering

`Writer.Seal()`:

```
 1. Reject if w.closed                  (ErrClosed).
 2. Reject if w.stickyErr is set        (latched prior failure).
 3. flushLocked()                       (existing path; fsyncs the final pending block).
 4. Walk the framed-block region        (§6.2).
 5. Build footer sections in memory     (§6.3).
 6. Compute xxh3 over header[12:] + footerBytes (§6.5).
 7. Append footerBytes at EOF, fsync.
 8. WriteAt(headerBytes, 0), fsync.
 9. file.Close(), set w.closed = true.
10. Return SealResult.
```

### 6.2 The Walk Pass

Walk the framed-block region using the existing length-prefix framing (offsets `reservedHeaderBytes` through end-of-file at this stage, since the footer is not yet appended). For each block:

- pread the 8-byte length prefix → `compressed_size`.
- pread the frame bytes. Call a new unexported helper `decodeBlockCompressedSized(frame) ([]Event, int, error)` that wraps `decodeBlockCompressed` and additionally returns the decompressed-body length. The existing `decodeBlockCompressed` stays unchanged for callers that don't need the size; the new helper exists so `Seal` can populate `BlockInfo.UncompressedSize` without a second decode.
- Compute per-block stats:
  - `Offset` (the file offset of the length prefix).
  - `CompressedSize` (the frame length).
  - `UncompressedSize` (returned by the new helper).
  - `EventCount` (`len(events)`).
  - `MinSeq`, `MaxSeq` (single pass over the events).
- Per-block unique DIDs (`map[string]struct{}`, used to populate the per-block bloom).
- Per-block unique collections (`map[string]struct{}`, used to populate the bitmask after the global string table is finalized).
- Update segment-wide accumulators:
  - Global unique-DID set (`map[string]struct{}`).
  - Global unique-collection set, with first-seen ordering preserved (`map[string]uint32` plus an ordered slice).
  - Total event count, min/max seq, min/max indexed_at.

The walk uses the existing `decodeBlockCompressed`. Yes, this materializes events for fields (collection, payload) we don't directly need for the footer — the alternative is a duplicate "shallow" decoder, which is more code and divergence risk. Decoder throughput (~1 GB/s/core) means seal of a 256 MB segment runs in ~1 second of single-core CPU.

#### 6.2.1 Memory Footprint

The global unique-DID map is the dominant allocation. Worst case (initial-backfill seal of a segment full of unique DIDs at full network archive scale): ~10M DIDs × ~32 B/DID + map overhead ≈ 600-800 MB transient, released the moment Seal returns.

This is acceptable. DESIGN.md §1.1 already says the machine "would benefit from a fair bit of ram for initial backfill"; seal happens once per ~256 MB of data; the allocation is short-lived.

In steady-state operation (post-backfill), the same calculation gives a few thousand unique DIDs per ~256 MB of recent firehose traffic, so transient memory is in the low MB range — a non-issue.

### 6.3 Building Footer Sections

After the walk, all four sections are built in memory:

1. **Block index**: encode the `[]BlockInfo` slice (length is now known) as `block_count × 36` packed bytes. Emit into a writer-owned scratch buffer.
2. **Segment-level DID bloom**: `gloom.New(uniqueDIDCount, 0.001)`, `Add` every DID from the global map, `MarshalBinary()` once.
3. **Per-block DID blooms**: construct `block_count` independent `gloom.New(MaxEventsPerBlock, 0.001)` filters, `Add` each block's DIDs into its respective filter. Marshal the first one, record `bloom_size_bytes`. Marshal each subsequent filter and assert `len == bloom_size_bytes` (panic on mismatch). Emit the 8-byte header followed by the concatenated marshaled blooms.
4. **Collection block index**: build the string table from the global ordered slice, compute `bitmask_len = ceil(collection_count / 8)`, build per-block bitmasks setting the bit for each NSID id present in that block. Concatenate string table bytes + bitmask bytes (uncompressed body), then zstd-compress with `blockEncoder.EncodeAll`. Emit the 16-byte header followed by the zstd frame.

All four sections are concatenated in order into a single `footerBytes` buffer.

### 6.4 Writing Footer + Header

```
footerOffset := currentFileSize  // == reservedHeaderBytes + sum(framed-block-sizes)
file.Write(footerBytes)          // single write call → at most one torn frame on partial write
file.Sync()
file.WriteAt(headerBytes, 0)     // single pwrite at offset 0
file.Sync()
file.Close()
```

The footer goes out in one `Write` call; the header patch is `WriteAt(headerBytes, 0)` (no seek state change).

We do not fsync the parent directory at seal time. The dirent for this file was already made durable by `New()`'s `syncParentDir` call when the segment was first created. The seal step modifies the file's inode-attached data only (footer bytes are appended, header bytes at offset 0 are overwritten). `fsync(file)` is the correct durability scope. (A future seal-via-rename design would need a parent-dir fsync; we are not doing that.)

### 6.5 Computing the xxh3 Checksum

The checksum spans `version..end-of-footer`:

- Bytes 12..255 of the fixed header (244 bytes — version through reserved padding).
- All footer bytes.

Using a streaming `xxh3.New()`:

```go
h := xxh3.New()
h.Write(headerBytes[12:])
h.Write(footerBytes)
checksum := h.Sum64()
binary.LittleEndian.PutUint64(headerBytes[4:12], checksum)
```

Build order:

1. Build the footer in memory.
2. Determine `footerOffset` (current file size, equivalently `reservedHeaderBytes + sum(framed block sizes)`).
3. Build `headerBytes` with all offsets populated, `checksum` field zeroed temporarily.
4. Compute xxh3 over `headerBytes[12:]` + `footerBytes`.
5. Patch checksum bytes into `headerBytes[4:12]`.

### 6.6 Failure Modes

| Step | State on failure | Recovery |
|---|---|---|
| Pending flush (step 3) | Unchanged from last successful Flush | Operator retries Seal; flushLocked picks up the still-pending block. |
| Walk read (step 4) | File unchanged | Operator retries. |
| Footer Write (step 7) | Partial footer past last good frame: bytes that don't parse as a valid frame | Next `New()` truncates via `lastGoodOffset`. File is back to active. |
| Footer fsync (step 7) | Same as above | Same. |
| Header WriteAt (step 8) | Footer durable, header still zero | **Seal explicitly truncates the footer back off**: `f.Truncate(footerOffset); f.Sync()`. File is restored to active state. stickyErr is latched. |
| Header fsync (step 8) | Both writes durable; fsync failed (rare; on some kernel/fs combos may indicate the bytes haven't actually hit the platter) | Latch stickyErr, return. Next `Open()` will detect a sealed file with a checksum that may or may not match; operator can choose to delete and re-derive (replication) or trust it. |

The header-write-failure recovery (step 8a) is the most subtle case and is documented inline in `seal.go` with a multi-paragraph comment. The recovery is deterministic: truncate + fsync restores the active-state invariant ("last byte is the last good frame").

In the (rare) header-fsync-fails case, we cannot truncate without potentially destroying data the kernel has already promised; we leave the bytes on disk and rely on the checksum check at next Open to surface any corruption.

### 6.7 Idempotency

`Seal()` consumes the writer; calling it twice on the same `Writer` returns `ErrClosed` on the second call. Across processes, a successful Seal is idempotent (next `New()` returns `ErrSegmentSealed`); a partial Seal followed by recovery + retry from a fresh Writer is idempotent (recovery truncates the partial footer; retry produces equivalent on-disk state).

## 7. Reader

### 7.1 Open

```go
func Open(cfg ReaderConfig) (*Reader, error)
```

1. Validate `cfg.Path != ""`.
2. `os.OpenFile(path, O_RDONLY, 0)`.
3. Stat → file size.
4. pread the 256-byte fixed header at offset 0.
5. Validate magic == "jss0".
6. Validate checksum != 0 (else this is an active file → return `ErrCorruptSegment`).
7. Validate version == 1.
8. Validate every offset field fits within the file size and they are in the expected order: `block_index_offset == footer_offset; footer_offset > reservedHeaderBytes; did_bloom_offset > block_index_offset; ...`. Mismatch → `ErrInvalidFooter`.
9. pread the block index, segment-level bloom, collection-index header + body. Parse each.
10. Validate every block index entry's `offset + 8 + compressed_size <= footer_offset`. Mismatch → `ErrInvalidBlockIndex`.
11. If `!cfg.SkipChecksum`: re-pread the bytes from offset 12 to end-of-footer and compute xxh3, compare against `header.Checksum`. Mismatch → `ErrChecksumMismatch`.
12. Return a `*Reader` carrying parsed metadata + file handle.

The checksum-verification pread is bounded by the metadata size (small relative to data: a few MB on a 256 MB segment), runs once per Open. Hot paths that have already verified the file (e.g., after a CDN download with checked SHA-256) can pass `SkipChecksum: true`.

### 7.2 Block Decode

```go
func (r *Reader) DecodeBlock(idx int) ([]Event, error)
```

1. Bounds-check `idx`. Out-of-range → `ErrBlockOutOfRange`.
2. Look up the `BlockInfo`.
3. pread `compressed_size` bytes at `offset + 8` (skip the length prefix; we already have `compressed_size` from the index).
4. `decodeBlockCompressed(frame)` → `[]Event`.
5. Return.

No internal cache. Multiple goroutines may call concurrently; each pread is a single syscall on a goroutine-local buffer.

### 7.3 Bloom and Collection Accessors

- `SegmentBloom()`: O(1), returns the cached parsed filter.
- `BlockBloom(idx)`: pread `bloom_size_bytes` at `block_did_bloom_offset + 8 + idx*bloom_size_bytes`, `gloom.UnmarshalBinary`. One syscall per call.
- `LoadAllBlockBlooms()`: simple loop over `BlockBloom(i)`. No coalescing.
- `Collections()`: O(1), returns the cached string slice.
- `BlockCollections(idx)`: O(collection_count) per call (walks the bitmask). Idx is bounds-checked.

### 7.4 Concurrency

All public methods are safe to call from multiple goroutines. The Reader's internal state is read-only after Open. `os.File.ReadAt` is goroutine-safe on Linux (via pread). The package-level `blockDecoder` is goroutine-safe for `DecodeAll` per klauspost docs.

Tests assert `-race` cleanliness under concurrent `DecodeBlock` calls.

## 8. Sealed-vs-Active Detection

`Writer.New()` is updated to detect sealed files and reject them:

```go
// In resumeExistingSegment, after the magic check and before
// lastGoodOffset:
var checksumBuf [8]byte
if _, err := f.ReadAt(checksumBuf[:], 4); err != nil { ... }
if binary.LittleEndian.Uint64(checksumBuf[:]) != 0 {
    return fmt.Errorf("%w: %s", ErrSegmentSealed, path)
}
```

This replaces the kaizen-comment-only stub at `writer.go:188-192`. The corresponding kaizen note in `writer_test.go:70-73` is replaced by a real `TestNewRejectsSealedFile` that builds a sealed segment via the public `Seal()` API and verifies New rejects it.

## 9. Recovery from Crash During Seal

The existing `lastGoodOffset` walk handles the post-crash state in two cases:

1. **Crash before footer fsync**: trailing bytes do not parse as a valid `[uint64 len][zstd frame]` pair (because the first 8 bytes of the footer are the block index's first entry, which is a uint64 file offset interpreted as a length prefix; that length will overrun the file). `lastGoodOffset` returns the end of the last good frame, and `resumeExistingSegment` truncates.
2. **Crash between footer fsync and header pwrite**: same — trailing footer bytes look like a torn frame from the framing walker's perspective; truncated identically.

Both branches in `lastGoodOffset` (length prefix promises more bytes than the file holds; integer-overflow on hostile input) cover the cases. We add explicit recovery integration tests that build these states deterministically and verify the next Writer.New() restores the file to a clean active state.

## 10. Testing Strategy

The existing test suite runs in 228ms across 114 tests under `just test` (sub-second budget, AGENTS.md). Sealing tests are sized to fit the same budget while preserving meaningful coverage.

### 10.1 Unit Tests (`header_test.go`, `footer_test.go`, `bloom_test.go`, `collection_test.go`)

Pure-Go, no filesystem. Each new format file gets:

- Encode → decode roundtrip with hand-crafted small inputs.
- Field-offset regression: examine encoded bytes byte-by-byte to catch accidental field reorders.
- Decode rejects: each malformed input independently (truncated header, version too high, offset overruns file size, oversized lengths in collection index, etc.).

`testing/quick`-style property test for any encode/decode pair (block index entries, collection bitmasks).

### 10.2 Roundtrip Property Test (`seal_test.go`)

Generators reuse `block_test.go` event generators. Each iteration:

1. Build a Writer with small `MaxEventsPerBlock` (2-16).
2. Append `n` random events (`n` in [1, 100], drawn uniformly), with periodic flushes.
3. Call `Seal()`, capture `SealResult`.
4. `Open` the result.
5. Verify:
   - Header fields match `SealResult`.
   - `Reader.Blocks()` returns one entry per flushed block, in order.
   - For every event in the original stream, `Reader.SegmentBloom().Test(event.DID) == true`.
   - For every event, the per-block bloom for its block returns true.
   - `Reader.DecodeBlock(idx)` over every block reproduces the original events in order.
   - `Reader.Collections()` is the unique set of collections; `Reader.BlockCollections(idx)` matches each block's actual collection set.

`-short` runs ~30 iterations; `just test-long` runs ~500. Each iteration is bounded by ~1ms of work (single-digit small blocks).

### 10.3 Swarm Test (`seal_swarm_test.go`)

Independent feature axes, ~p=0.5 each:

1. Single-block segment vs. many-block segment.
2. All-same-DID vs. all-distinct-DIDs vs. heavy-tail DID distribution.
3. Single collection vs. dozens.
4. Tiny payloads vs. large.
5. Default `MaxEventsPerBlock` vs. small (16) vs. large (32K).
6. Block-count edge cases: 1, 2, exactly the bitmask boundary (8 collections), exactly 64 blocks, large counts.
7. Custom FP rates parameterized in [0.001, 0.01].

Each iteration: append → seal → open → walk blocks → verify roundtrip. `-short`: ~30 iters; `-long`: matches existing 1000-iter swarm pattern.

### 10.4 Fuzz Tests (`seal_fuzz_test.go`)

```go
func FuzzReadHeader(f *testing.F)
func FuzzReadFooter(f *testing.F)             // block index + bloom region + collection index
func FuzzReadCollectionIndex(f *testing.F)
```

Contract for each: must not panic, must not read past end of input, must not allocate unbounded memory. Returns valid output or an error.

Seed corpus from valid encoded fixtures + deliberately-truncated/malformed inputs. CI's seed-corpus pass runs in microseconds; `-fuzztime=10s` per target only when invoked explicitly.

### 10.5 Golden Bytes (`seal_test.go` + `testdata/golden_seal.bin`)

A deterministic events stream sealed with fixed `MaxEventsPerBlock` and FP rates → byte-compared against `testdata/golden_seal.bin`. Catches accidental layout changes. Updatable via `go test -run TestSealGolden -update`.

### 10.6 Reader Integration (`reader_test.go`)

Filesystem tests against `t.TempDir()`:

- Open a sealed segment, read back every block, verify event identity.
- `Reader.SegmentBloom().Test(did)` returns true for known DIDs.
- `Reader.BlockBloom(idx)` returns true for that block's DIDs.
- `Reader.LoadAllBlockBlooms()` returns blooms equivalent to N independent `BlockBloom(idx)` calls.
- `Reader.Collections()` returns NSIDs in their assigned ID order; `Reader.BlockCollections(idx)` matches what was actually in the block.
- `DecodeBlock(idx)` for every valid idx; `DecodeBlock` returns `ErrBlockOutOfRange` past the end.
- **Concurrent decode**: 10 goroutines × 100 iterations, random block indices. Bounded so `-race` runs in milliseconds.
- **Checksum verification**: corrupt one byte of the footer, expect `ErrChecksumMismatch` on Open.
- **`SkipChecksum: true`**: same corrupted file opens successfully.
- Reader rejects an active file (zero checksum) with `ErrCorruptSegment`.

### 10.7 Recovery Tests (`seal_recovery_test.go`)

- **Crash after footer fsync, before header pwrite**: build a sealed segment, manually re-zero header bytes 4..256. `New()` reopens: `lastGoodOffset` returns the offset before the partial footer, file is truncated, follow-up Append + Seal produces a valid sealed file.
- **Crash after partial footer write**: write a partial footer (some leading bytes). Same recovery — truncate, reseal, verify.
- **Header-WriteAt-fails-truncate-the-footer-back-off**: inject a failure on the header pwrite (close the underlying file descriptor behind the Writer's back, as `TestStickyErrorIsLatchedAcrossFlushAndClose` does). Observe the file size returns to footerOffset, `stickyErr` is latched, subsequent Seal returns `ErrClosed`. A *new* Writer opening the same path resumes as active and can be Sealed cleanly.
- **`New()` rejects a sealed file** with `ErrSegmentSealed`. Replaces the kaizen-comment-only test at `writer_test.go:74`.

### 10.8 Race Detector

All tests run under `-race` via CI's `just test-race`. Reader's concurrent-decode test is the gating coverage for goroutine-safety claims.

### 10.9 Benchmarks (`seal_bench_test.go`)

Developer-invoked, no CI gating:

```go
BenchmarkSeal              // append N events, then Seal; reports MB/s of seal walk
BenchmarkReaderOpen        // Open + checksum verification
BenchmarkReaderOpenNoVerify
BenchmarkBlockBloom        // pread + UnmarshalBinary per call
BenchmarkLoadAllBlockBlooms
BenchmarkDecodeBlockSealed // decompress + decode through Reader
```

### 10.10 Performance Budget

Estimated under `-short`:
- Unit tests: ~30 tests × ~5μs = negligible.
- Property test (~30 iters): ~30ms.
- Swarm (~30 iters): ~30ms.
- Reader integration (~15 tests): ~150ms.
- Recovery (~5 tests): ~50ms.

Total addition to segment package: ~250-300ms. Total segment-package runtime: ~500ms. Suite total well under 1s.

Generator helpers reuse RNG sources and pre-allocate event slices. Reader integration tests share fixture builders to avoid re-encoding the same sealed segment in every test.

## 11. Open Questions Deferred

- **Block size and segment size tuning.** Still open per DESIGN.md §3.2. Will be informed by post-deployment measurements; format is not affected.
- **Compaction (re-seal)**. Footer format here is intentionally amenable to re-seal as a clean primitive: rebuild block index entries for any rewritten blocks, rebuild blooms and collection index from a final walk, regenerate header + checksum, write to a temp file, atomic rename. That slice will compose on top of these encode/decode helpers; nothing new is needed at the format level.

## 12. Dependency Additions

Two new dependencies, both whitelisted in PRACTICES.md:

- `github.com/jcalabro/gloom`
- `github.com/zeebo/xxh3`
