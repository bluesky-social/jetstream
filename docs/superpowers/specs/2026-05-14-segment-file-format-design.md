# Segment File Format — Initial Slice

**Date:** 2026-05-14
**Scope:** First implementation slice of the jetstream segment file format. Covers in-memory event buffering, columnar block encoding, length-prefixed block flush to disk with fsync, and a complementary block decoder used for testing and (later) reading. Excludes sealing, reading, crash recovery, time-based flush triggers, and any coupling to the pebble metadata store.

## 1. Goals

1. Implement the segment block wire format exactly as specified in `DESIGN.md` §3.1.2 and §3.2.
2. Ship a single Go package, `./segment`, exposing a focused public API for *writing* segment files. Reading comes in a later slice via the same package.
3. Keep the writer pure: no goroutines, no timers, no contexts, no metadata side-channels. The future ingestion orchestrator owns all that.
4. Test the format rigorously — unit, roundtrip property, swarm, fuzz, golden bytes, writer integration, plus benchmarks — because every other layer of the system trusts that bytes written here can be read back identically.

## 2. Non-Goals

The following are explicitly out of scope for this slice and will be addressed in future work:

- Sealing (writing the 256-byte fixed header, building the variable-length footer with block index / DID blooms / collection block index, and rotating to a new active file).
- Reading (a public `Reader` type — only an unexported `decodeBlock` ships, used by tests).
- Crash recovery (walking length prefixes from offset 256 to validate prior blocks and truncate any partial trailing block).
- Time-based flush triggers (driven externally by the ingestion orchestrator).
- Pebble metadata coupling. The segment package never imports pebble.
- Lookaside file format and writer.
- `backfill_complete.log` format and writer.
- Replication.

## 3. Architecture

### 3.1 Package Layout

A single top-level `./segment` package, public so external consumers (replicas, third-party clients that want to read segment files directly) can import it without a `pkg/` URL detour.

```
./segment/
  doc.go                  // package overview godoc
  event.go                // Event, Kind constants
  errors.go               // sentinel errors
  block.go                // unexported encodeBlock / decodeBlock
  writer.go               // Config, Writer, Append/Flush/Close/Pending/Cap

  event_test.go
  block_test.go           // unit + roundtrip property tests
  block_golden_test.go    // golden-bytes pinning of wire format
  block_swarm_test.go     // swarm tests over feature axes
  block_fuzz_test.go      // FuzzDecodeBlock
  block_bench_test.go     // benchmarks for encode / decode / append / flush
  writer_test.go          // integration tests against t.TempDir()
  testdata/
    golden_block.bin      // pinned encoded fixture
    fuzz/...              // checked-in fuzz corpus
```

Boundary discipline despite the single package:

- `Event`, `Kind` (and its constants), `Config`, `Writer`, and the methods on `Writer` are exported.
- `encodeBlock` and `decodeBlock` are unexported pure functions with no I/O. Tests in the same package call them directly. External read access lands in a future `Reader` type.
- Nothing in `block.go` touches `os.File`. Nothing in `writer.go` touches the columnar layout. File-level boundaries enforce the conceptual split.

### 3.2 Concurrency Model

The `Writer` is **not** goroutine-safe. The caller (the future ingestion pipeline) owns serialization. This matches `DESIGN.md`'s "one active segment at a time" invariant, keeps the implementation simple, and makes test reasoning easier. The constraint is documented on the type.

## 4. Public API

### 4.1 Event and Kind

```go
package segment

// Kind discriminates which firehose event type a row represents.
// Values match DESIGN.md §3.2.
type Kind uint8

const (
    KindCreate   Kind = 1
    KindUpdate   Kind = 2
    KindDelete   Kind = 3
    KindIdentity Kind = 4
    KindAccount  Kind = 5
    KindSync     Kind = 6
)

// Event is one row inside a segment block.
//
// Variable-length fields are constrained to fit in their on-disk
// length columns:
//   DID:        up to 65535 bytes (uint16 column)
//   Collection: up to 255 bytes   (uint8 column)
//   Rkey:       up to 255 bytes   (uint8 column)
//   Rev:        up to 255 bytes   (uint8 column)
//   Payload:    up to MaxUint32 bytes (uint32 column)
//
// IndexedAt and RenderedAt are unix microseconds. RenderedAt == 0
// means "no operator-supplied timestamp" (DESIGN.md §3.2).
//
// For non-commit kinds (Identity, Account, Sync), Collection, Rkey,
// Rev, and Payload are typically empty / nil. The encoder accepts
// any combination; emptiness is not enforced as an invariant of the
// segment package.
type Event struct {
    Seq        uint64
    IndexedAt  int64
    RenderedAt int64
    Kind       Kind

    DID        string
    Collection string
    Rkey       string
    Rev        string

    Payload    []byte // raw drisl (the DAG-CBOR subset used by atproto)
}
```

### 4.2 Config and Writer

```go
// Config controls writer behavior. Path is required; other fields
// pick up defaults if zero.
type Config struct {
    // Path is the segment file. Required.
    Path string

    // MaxEventsPerBlock triggers a "block full" signal from Append.
    // Default 4096 (DESIGN.md §3.2). Must be >= 1.
    MaxEventsPerBlock int
}

// Writer encodes events into the active segment file.
//
// Writer is not safe for concurrent use; the caller serializes
// access. The writer performs no goroutines, timers, or context
// management. Time-based flush triggers and pebble metadata
// coupling live in the ingestion orchestrator that composes
// Writer with the rest of the system.
type Writer struct { /* unexported */ }

// New opens or creates the active segment at cfg.Path.
//
// If the file does not exist, New creates it and writes 256 zero
// bytes (the reserved header region per DESIGN.md §3.1.2).
//
// If the file exists, New stats it and validates:
//   - file size >= 256 bytes (else ErrCorruptSegment);
//   - first 4 bytes are not the sealed magic "jss0" (else
//     ErrSegmentSealed). Sealed-file handling lands in a later slice.
// On success, New seeks to end-of-file and is ready to append.
//
// cfg is validated; invalid values return ErrInvalidConfig.
func New(cfg Config) (*Writer, error)

// Append validates ev and adds it to the in-memory pending block.
// It performs no disk I/O.
//
// The returned bool is true when the pending block has reached
// MaxEventsPerBlock and the caller must call Flush before the next
// Append. The caller is free to ignore the bool only at the cost of
// receiving ErrBufferFull from the next Append.
//
// Errors:
//   - ErrFieldTooLong: a string or Payload field exceeds its column width.
//   - ErrInvalidKind:  Kind is outside [1, 6].
//   - ErrClosed:       the writer was already closed.
//   - ErrBufferFull:   the pending block is at capacity and this
//                      call would exceed it; caller must Flush.
// On error, the buffer is unchanged.
func (w *Writer) Append(ev Event) (full bool, err error)

// Flush encodes the pending block, writes it to the file as
// [uint64 LE compressed_len][zstd frame], and fsyncs before
// returning. No-op if the pending buffer is empty.
//
// Flush is the only method that performs disk I/O on the hot path.
// Errors propagate from os.File.Write, os.File.Sync, or the zstd
// encoder.
func (w *Writer) Flush() error

// Close flushes any pending block and closes the file. Close is
// idempotent; repeated calls return nil without re-flushing.
func (w *Writer) Close() error

// Pending returns the number of events buffered but not yet flushed.
func (w *Writer) Pending() int

// Cap returns Config.MaxEventsPerBlock.
func (w *Writer) Cap() int
```

### 4.3 Errors

```go
var (
    ErrInvalidConfig  = errors.New("segment: invalid config")
    ErrCorruptSegment = errors.New("segment: file is smaller than reserved header")
    ErrSegmentSealed  = errors.New("segment: file is already sealed")
    ErrFieldTooLong   = errors.New("segment: event field exceeds column width")
    ErrInvalidKind    = errors.New("segment: kind out of range")
    ErrBufferFull     = errors.New("segment: pending block is at capacity; flush required")
    ErrClosed         = errors.New("segment: writer is closed")
)
```

All errors are `errors.Is`-compatible sentinels. Wrapped values use `fmt.Errorf("%w: %s", ErrX, detail)` when carrying additional context.

## 5. Internal Data Flow

### 5.1 Writer State

```go
type Writer struct {
    cfg     Config
    file    *os.File
    encoder *zstd.Encoder // klauspost/compress, default level, reused
    pending pendingBlock
    closed  bool
}

type pendingBlock struct {
    seq        []uint64
    indexedAt  []int64
    renderedAt []int64
    kind       []uint8
    didLen     []uint16
    collLen    []uint8
    rkeyLen    []uint8
    revLen     []uint8
    eventLen   []uint32

    dids        []byte // concatenation of DID bytes
    collections []byte
    rkeys       []byte
    revs        []byte
    payloads    []byte
}
```

Parallel column slices (rather than a slice of `Event`) so the encoder iterates each column once with a tight loop and no per-event indirection. After `Flush`, every slice is reset via `s = s[:0]` to retain capacity, so steady-state operation is allocation-free on the column slices.

Memory cost for one pending block at 4096 events with average atproto event sizes (~32 B DID, ~500 B payload) is ~2.5 MB. No pool needed.

### 5.2 Append Path

1. Reject if `closed` (`ErrClosed`).
2. Reject if `len(pending.seq) >= cfg.MaxEventsPerBlock` (`ErrBufferFull`).
3. `validate(ev)`: check `Kind ∈ [1, 6]`, `len(DID) ≤ MaxUint16`, the three `uint8` length fields fit, `len(Payload) ≤ MaxUint32`. Pure function, easy to property-test.
4. Append every column in order — fixed-size lengths and variable-length byte concatenations.
5. Return `full = (len(pending.seq) == cfg.MaxEventsPerBlock)`.

`Append` performs no disk I/O.

### 5.3 Flush Path

1. Reject if `closed` (`ErrClosed`).
2. No-op if `len(pending.seq) == 0`.
3. Encode the columnar body into a `[]byte` (see §6).
4. Compress with the reused `zstd.Encoder` at the default level (klauspost's `zstd.SpeedDefault`, which is zstd level 3 — the format default).
5. Write the framing `[uint64 LE compressed_len][zstd frame]` to the file in a single `Write` call (one buffer, one syscall).
6. `f.Sync()`.
7. Reset every column slice in `pending` via `s = s[:0]`.

The fsync ordering matches DESIGN.md §3.5: fsync the segment block first, *then* the orchestrator commits its pebble batch. The orchestrator does not exist in this slice; the segment package's contract is that `Flush` returns successfully only once the block is durable on disk.

### 5.4 Close Path

1. Idempotent: if already closed, return nil.
2. If `pending.seq` is non-empty, `Flush`. Propagate any error.
3. `f.Close()`.
4. Mark closed.

The `zstd.Encoder` is `Close`-able too; we close it after the file.

## 6. Block Wire Format

Per DESIGN.md §3.2, with `did_len` widened to `uint16` (per the same doc, current revision). The byte layout of the **uncompressed** block body, before being wrapped in a single zstd frame, is:

```
[uint32 LE]  event_count

[ event_count × uint64 LE ]  seq[]
[ event_count × int64  LE ]  indexed_at[]
[ event_count × int64  LE ]  rendered_at[]
[ event_count ×  uint8    ]  kind[]
[ event_count ×  uint8    ]  collection_len[]
[ event_count × uint16 LE ]  did_len[]
[ event_count ×  uint8    ]  rkey_len[]
[ event_count ×  uint8    ]  rev_len[]
[ event_count × uint32 LE ]  event_len[]

[ Σ collection_len bytes ]   collections[]
[ Σ did_len        bytes ]   dids[]
[ Σ rkey_len       bytes ]   rkeys[]
[ Σ rev_len        bytes ]   revs[]
[ Σ event_len      bytes ]   payloads[]
```

The body is fed through `zstd.Encoder.EncodeAll` to produce a single zstd frame with content checksums enabled. The writer prefixes that frame with `[uint64 LE compressed_len]` before appending to the file.

`decodeBlock` is the exact inverse, reading the count, then each fixed-size column with batched `binary.LittleEndian.Uint*` calls, then walking the four length columns to slice the variable-length blobs into per-event byte slices. It returns `[]Event`. On any short read or inconsistency it returns an unexported sentinel `errTruncatedBlock`. The decoder's error surface stays unexported while the decoder itself is unexported; it gets promoted to a public error in the future slice that ships a `Reader`.

For non-commit kinds with no payload, the encoder writes `event_len = 0` and emits zero payload bytes. The decoder returns `Payload = nil` (not `[]byte{}`) so the roundtrip property holds: `decode(encode(events)) == events`.

## 7. Testing Strategy

### 7.1 Unit Tests (`event_test.go`, `block_test.go`)

Pure-Go tests with no filesystem. Disk-touching tests live in §7.6.

- Encode → decode roundtrip on a small hand-crafted block (3-5 events, mixed Kinds).
- Validation rejects each malformed input independently: `len(DID) > 65535`, oversized Collection / Rkey / Rev / Payload, `Kind == 0`, `Kind == 7`.
- `Append` returns `ErrFieldTooLong` for each oversized field, with the buffer unchanged after error.
- `Append` returns `ErrInvalidKind` for `Kind == 0` and `Kind == 7`.
- `Cap()` returns the configured `MaxEventsPerBlock`.
- `Pending()` returns the right value after each `Append` and resets to 0 after `Flush`.

### 7.2 Roundtrip Property Test (`block_test.go`)

Using `testing/quick` (no new dependency). Generators:

- `genEvent(rand)` — Event with Kind in [1,6], random `uint64` Seq, random `int64` timestamps (including 0 RenderedAt), DIDs drawn from a length distribution centered at 32 B with a long tail to 65535, Collection / Rkey / Rev with realistic length distributions, Payload with a long-tail distribution centered around ~500 B.
- `genBlock(rand, n)` — `n` events, `n` drawn uniformly in [1, 4096].

Assertion: `decodeBlock(encodeBlock(events)) == events` byte-for-byte (deep-equal). ~1000 iterations per `go test`.

### 7.3 Swarm Test (`block_swarm_test.go`)

Each iteration independently flips each of the following axes with `p = 0.5`. If zero axes end up enabled, force one on (the all-default case is already covered by §7.2). Subset size is therefore ~Binomial(n, 0.5), giving genuine wide coverage including sparse and dense feature combinations.

Axes:

1. Tiny payloads (all 0–10 B).
2. Huge payloads (centered near `MaxUint32`, bounded to a few MB to keep tests fast).
3. Empty optional fields (all of Collection / Rkey / Rev empty — non-commit kinds).
4. Max-length DIDs (every DID exactly 65535 bytes).
5. Single-event blocks vs. max-event blocks (4096).
6. All same Kind vs. every Kind represented.
7. Repeated identical events (entire block is N copies — stresses zstd repeat detection).
8. Mostly-zero columns (Seq=0, IndexedAt=0, RenderedAt=0 for most rows).
9. Bytes that look like length prefixes inside payloads (long zero runs; values that could confuse a buggy decoder).

Each iteration: pick subset, generate block, encode, decode, assert equality. ~500 iterations.

### 7.4 Fuzz Test (`block_fuzz_test.go`)

```go
func FuzzDecodeBlock(f *testing.F) {
    // Seed with valid encoded fixtures, plus edge cases.
    f.Fuzz(func(t *testing.T, data []byte) {
        // Contract: decodeBlock MUST NOT panic, MUST NOT read past
        // end of input, MUST NOT allocate unbounded memory. Returns
        // either a valid []Event or an error.
        _, _ = decodeBlock(data)
    })
}
```

The decoder validates `event_count` against the available input length before allocating, so a malicious header claiming `event_count = 1<<31` cannot provoke a 16 GB allocation.

Two targets ship:

- `FuzzDecodeBlock` — bare uncompressed body bytes (the columnar split alone).
- `FuzzDecodeBlockFromCompressed` — full `[uint64 len][zstd frame]` framing (the full read pipeline).

CI runs the fuzz targets with a short time budget (`-fuzztime=10s` per target). The corpus accumulated in `testdata/fuzz/...` is checked in.

### 7.5 Golden Bytes (`block_golden_test.go`)

A single deterministic `[]Event` is encoded and compared byte-for-byte against `testdata/golden_block.bin`. Any accidental layout change (column reorder, endianness flip, missing field) breaks the test loudly with a diff. The fixture is regenerated by `go test -run TestGolden -update`, and regeneration shows up as a diff in the PR for explicit reviewer approval.

### 7.6 Writer Integration (`writer_test.go`)

Tests touch disk via `t.TempDir()`. Smaller suite than the block tests because most format risk is covered above. The roundtrip test reads the file bytes directly with `os.ReadFile` and walks the `[uint64 LE len][zstd frame]` framing using a small test helper, then calls `decodeBlock` per frame; it does not use a public `Reader` because none ships in this slice.

- `New` on a missing file creates a 256-byte zero-prefixed file.
- `New` on an existing active file resumes at end-of-file.
- `New` on a sealed file (first 4 bytes `"jss0"`) returns `ErrSegmentSealed`.
- `New` on a file smaller than 256 bytes returns `ErrCorruptSegment`.
- `Append` after `Close` returns `ErrClosed`.
- `Append` after the buffer hits `Cap()` returns `ErrBufferFull`.
- `Flush` on empty buffer is a no-op and leaves the file unchanged.
- Append → Flush → reopen → walk length prefixes → decode each block reproduces the original events.
- Multiple flushes produce multiple length-prefixed blocks at the expected file offsets.
- Buffer reuse: after a flush, `Pending()` is 0 and the next Append starts a fresh block.
- `Close` with non-empty pending flushes the pending block; `Close` with empty pending just closes the file.
- `Close` is idempotent.

### 7.7 Race Detector

All tests run under `-race` via CI's `just test-race`. Benchmarks are not run under `-race` (the race detector substantially distorts performance numbers and benchmarks are developer-invoked, not CI-gated). The package isn't internally concurrent, but the race detector catches accidental shared state if a future change introduces a goroutine.

### 7.8 Benchmarks (`block_bench_test.go`)

Developer-invokes-it-when-they-want-to-measure benchmarks. No CI gating in this slice.

```go
func BenchmarkEncodeBlock(b *testing.B) {
    // Sub-benchmarks at realistic block sizes:
    //   - 256 events  (small block)
    //   - 4096 events (default)
    //   - 4096 events with all payloads = 0 bytes (low-compression worst case)
    //   - 4096 events with identical payloads (best compression case)
    // Reports B/op, allocs/op, and bytes/sec on the encoded body.
}

func BenchmarkDecodeBlock(b *testing.B) {
    // Mirror cases against pre-encoded fixtures so we measure decode
    // in isolation, not encode+decode.
}

func BenchmarkAppend(b *testing.B) {
    // Append-only path with a writer that never flushes; measures
    // per-event amortized cost of column-split + slice-append.
}

func BenchmarkFlushToTmpfs(b *testing.B) {
    // 4096 Appends + Flush including fsync. On Linux CI runners
    // t.TempDir() is tmpfs, so this measures CPU + zstd time, not
    // real-disk latency. Documented in a comment.
}
```

## 8. Open Questions Deferred to Later Slices

- **Block size tuning.** DESIGN.md §3.2 acknowledges 4096 events / 256 MB segments are educated guesses. Will be measured against the production firehose once we're connected.
- **Crash recovery.** A future `Recover` function will walk length prefixes from offset 256, validate each compressed frame decodes cleanly, and truncate any partial trailing block. Will be tested with deliberate byte-level corruption injection.

## 9. Dependencies

This slice introduces one new dependency, already on the PRACTICES.md whitelist:

- `github.com/klauspost/compress/zstd` — encoder and decoder for the zstd frame.

No other new dependencies. `testing/quick` is stdlib. Go fuzzing is stdlib. `github.com/stretchr/testify` is already in `go.mod`.
