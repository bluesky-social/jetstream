# Segment File Sealing — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `Writer.Seal()` to finalize active segments per DESIGN.md §3.1.2 (variable-length footer + 256-byte fixed header with xxh3 checksum), and ship a public `Reader` type that parses sealed metadata and exposes a block-list API.

**Architecture:** Six new files in `./segment` split by responsibility: `header.go`, `footer.go`, `bloom.go`, `collection.go` are pure encode/decode; `seal.go` orchestrates the seal walk-and-write pass; `reader.go` ships a goroutine-safe Reader driven by `os.File.ReadAt` (pread). The walk pass decompresses every block once at seal time to gather per-block stats — DIDs, collections, byte ranges, min/max seq — then builds the four footer sections in memory and writes them in one `Write`, then patches the finalized header in one `WriteAt`. After Seal, the writer is consumed.

**Tech Stack:** Go 1.26, `github.com/klauspost/compress/zstd` (already a dep), `github.com/jcalabro/gloom` (new), `github.com/zeebo/xxh3` (new), stdlib `testing` + `testing/quick` + Go 1.18+ fuzzing, `github.com/stretchr/testify` (already a dep).

**Spec:** `docs/superpowers/specs/2026-05-19-segment-sealing-design.md`

**Note on imports:** Several tasks say "append to file X" and show a code block that includes an `import (...)` declaration. Go allows multiple `import` blocks per file, but `gofmt` (and our `just lint` step) prefers a single block. When appending, merge the new imports into the file's existing `import (...)` block rather than adding a second one.

---

## File Structure Overview

Files this plan creates or modifies:

- **Modify:** `go.mod`, `go.sum` (add `gloom` and `xxh3`)
- **Modify:** `segment/errors.go` (add four new sentinels)
- **Modify:** `segment/zstd.go` (add `decodeBlockCompressedSized` helper)
- **Modify:** `segment/writer.go` (sealed-file detection in `resumeExistingSegment`, `Writer.Seal()` method)
- **Modify:** `segment/writer_test.go` (replace kaizen-comment-only tests with real ones)
- **Modify:** `segment/doc.go` (update package overview)
- **Create:** `segment/header.go` — fixed-header encode/decode + xxh3 helper
- **Create:** `segment/header_test.go`
- **Create:** `segment/footer.go` — block index encode/decode
- **Create:** `segment/footer_test.go`
- **Create:** `segment/bloom.go` — per-block-blooms packed-region encode/decode
- **Create:** `segment/bloom_test.go`
- **Create:** `segment/collection.go` — collection block index encode/decode
- **Create:** `segment/collection_test.go`
- **Create:** `segment/seal.go` — `Writer.Seal()` body + walk pass
- **Create:** `segment/seal_test.go` — happy path + roundtrip property test + golden
- **Create:** `segment/seal_swarm_test.go`
- **Create:** `segment/seal_fuzz_test.go`
- **Create:** `segment/seal_recovery_test.go`
- **Create:** `segment/reader.go` — public Reader type
- **Create:** `segment/reader_test.go`
- **Create:** `segment/seal_bench_test.go`
- **Create:** `segment/testdata/golden_seal.bin`

Each file has a single responsibility:
- `header.go`, `footer.go`, `bloom.go`, `collection.go` never touch `os.File`.
- `seal.go` and `reader.go` are the only new I/O-performing files.

---

## Conventions

- `just test ./segment` — run package tests (must remain sub-second).
- `just lint` — must report 0 issues.
- `just test-race` — full module under race; required for final verification.
- `just test-long` — full suite including 1000-iter swarm; required for final verification.
- `t.Parallel()` on independent tests; `t.Cleanup` for cleanup.
- Doc comments on every exported symbol; explain WHY for non-obvious decisions.
- Error wrapping: `segment: <action>: %w` (matches existing convention in `writer.go`).
- Commit messages: subject line only, prefixed `segment:` (matches existing repo style). No `Co-Authored-By` trailer.
- Defer `_ = file.Close()` in tests; never bare `defer file.Close()` (the lint config flags unchecked errors).

---

## Task 1: Add `gloom` and `xxh3` dependencies

**Files:**
- Modify: `go.mod`, `go.sum`

- [ ] **Step 1: Add the dependencies**

Run:
```bash
go get github.com/jcalabro/gloom@latest
go get github.com/zeebo/xxh3@latest
go mod tidy
```

Expected: `go.mod` gains require lines for both packages; `go.sum` gains hash entries.

- [ ] **Step 2: Confirm it builds**

Run:
```bash
just lint
```

Expected: PASS (no source files have been added yet).

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "deps: add gloom and xxh3 for segment sealing"
```

---

## Task 2: Add new error sentinels

**Files:**
- Modify: `segment/errors.go`

- [ ] **Step 1: Append the new sentinels**

Open `segment/errors.go` and add the four new sentinels alongside the existing ones:

```go
// ErrChecksumMismatch is returned by Reader.Open when the file's
// xxh3 checksum disagrees with the value in its fixed header. The
// likely contributing factors are bit rot, a partial CDN download,
// or replication corruption.
ErrChecksumMismatch = errors.New("segment: checksum mismatch")

// ErrInvalidFooter is returned by Reader.Open when the variable-
// length footer fails structural validation: a section length
// would overrun the file, internal pointers don't agree, or a
// length-prefixed sub-region is truncated.
ErrInvalidFooter = errors.New("segment: invalid footer")

// ErrInvalidBlockIndex is returned by Reader.Open when a block
// index entry's offset/size pair doesn't fit within the file.
ErrInvalidBlockIndex = errors.New("segment: invalid block index")

// ErrBlockOutOfRange is returned by Reader.DecodeBlock,
// Reader.BlockBloom, and Reader.BlockCollections when the requested
// block index is past BlockCount.
ErrBlockOutOfRange = errors.New("segment: block index out of range")
```

- [ ] **Step 2: Confirm it builds**

Run:
```bash
just lint
just test ./segment
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add segment/errors.go
git commit -m "segment: add Reader-side error sentinels"
```

---

## Task 3: `Header` type + on-disk encode/decode

**Files:**
- Create: `segment/header.go`
- Create: `segment/header_test.go`

The fixed header is 256 bytes total, with a defined field layout in DESIGN.md §3.1.2 / spec §5.1. Bytes 0..3 are the magic `"jss0"` (already written at New time); bytes 4..11 are the xxh3 checksum (zero on active, non-zero on sealed); bytes 12..97 are the metadata fields; bytes 98..255 are reserved padding.

This task ships pure encode/decode plus a streaming xxh3 helper. No I/O.

- [ ] **Step 1: Write the failing tests**

Create `segment/header_test.go`:

```go
package segment

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeHeaderRoundtrip(t *testing.T) {
	t.Parallel()

	h := Header{
		Version:               1,
		BlockCount:            42,
		EventCount:            123_456,
		UniqueDIDCount:        9_999,
		MinSeq:                1,
		MaxSeq:                123_456,
		MinIndexedAt:          1_700_000_000_000_000,
		MaxIndexedAt:          1_700_000_001_000_000,
		FooterOffset:          1 << 20,
		DIDBloomOffset:        (1 << 20) + 1024,
		BlockDIDBloomOffset:   (1 << 20) + 4096,
		CollectionIndexOffset: (1 << 20) + 8192,
		BlockIndexOffset:      1 << 20, // == FooterOffset by spec invariant
		Checksum:              0xDEADBEEFCAFEBABE,
	}

	buf := encodeHeader(h)
	require.Len(t, buf, reservedHeaderBytes)

	got, err := decodeHeader(buf)
	require.NoError(t, err)
	require.Equal(t, h, got)
}

func TestEncodeHeaderFieldOffsets(t *testing.T) {
	t.Parallel()

	// Pin every field offset by hand so accidental field reorders fail
	// loudly. The byte-layout is contractual with sealed files on disk
	// and with replicas; reorders are silently file-format-incompatible
	// otherwise.
	h := Header{
		Version:               1,
		BlockCount:            2,
		EventCount:            3,
		UniqueDIDCount:        4,
		MinSeq:                5,
		MaxSeq:                6,
		MinIndexedAt:          7,
		MaxIndexedAt:          8,
		FooterOffset:          9,
		DIDBloomOffset:        10,
		BlockDIDBloomOffset:   11,
		CollectionIndexOffset: 12,
		BlockIndexOffset:      9, // matches FooterOffset
		Checksum:              0xAABBCCDDEEFF0011,
	}
	buf := encodeHeader(h)

	require.Equal(t, []byte("jss0"), buf[0:4], "magic at offset 0")
	require.Equal(t, uint64(0xAABBCCDDEEFF0011),
		binary.LittleEndian.Uint64(buf[4:12]), "checksum at offset 4")
	require.Equal(t, uint16(1), binary.LittleEndian.Uint16(buf[12:14]),
		"version at offset 12")
	require.Equal(t, uint32(2), binary.LittleEndian.Uint32(buf[14:18]),
		"block_count at offset 14")
	require.Equal(t, uint32(3), binary.LittleEndian.Uint32(buf[18:22]),
		"event_count at offset 18")
	require.Equal(t, uint32(4), binary.LittleEndian.Uint32(buf[22:26]),
		"unique_did_count at offset 22")
	require.Equal(t, uint64(5), binary.LittleEndian.Uint64(buf[26:34]),
		"min_seq at offset 26")
	require.Equal(t, uint64(6), binary.LittleEndian.Uint64(buf[34:42]),
		"max_seq at offset 34")
	require.Equal(t, int64(7), int64(binary.LittleEndian.Uint64(buf[42:50])),
		"min_indexed_at at offset 42")
	require.Equal(t, int64(8), int64(binary.LittleEndian.Uint64(buf[50:58])),
		"max_indexed_at at offset 50")
	require.Equal(t, uint64(9), binary.LittleEndian.Uint64(buf[58:66]),
		"footer_offset at offset 58")
	require.Equal(t, uint64(10), binary.LittleEndian.Uint64(buf[66:74]),
		"did_bloom_offset at offset 66")
	require.Equal(t, uint64(11), binary.LittleEndian.Uint64(buf[74:82]),
		"block_did_bloom_offset at offset 74")
	require.Equal(t, uint64(12), binary.LittleEndian.Uint64(buf[82:90]),
		"collection_index_offset at offset 82")
	require.Equal(t, uint64(9), binary.LittleEndian.Uint64(buf[90:98]),
		"block_index_offset at offset 90")

	// Reserved padding (bytes 98..256) must be zero so future
	// extensions can land without breaking older readers.
	for i := 98; i < reservedHeaderBytes; i++ {
		require.Zerof(t, buf[i], "reserved byte %d must be zero", i)
	}
}

func TestDecodeHeaderRejectsShort(t *testing.T) {
	t.Parallel()

	_, err := decodeHeader(make([]byte, reservedHeaderBytes-1))
	require.True(t, errors.Is(err, ErrInvalidFooter))
}

func TestDecodeHeaderRejectsBadMagic(t *testing.T) {
	t.Parallel()

	buf := make([]byte, reservedHeaderBytes)
	copy(buf, []byte("XXXX"))
	_, err := decodeHeader(buf)
	require.True(t, errors.Is(err, ErrCorruptSegment))
}

func TestDecodeHeaderRejectsZeroChecksum(t *testing.T) {
	t.Parallel()

	// Zero checksum at offset 4 means "active file" by our active/sealed
	// convention. decodeHeader is only ever called on what should be a
	// sealed file, so a zero checksum is an unambiguous error.
	buf := make([]byte, reservedHeaderBytes)
	copy(buf, segmentMagic)
	binary.LittleEndian.PutUint16(buf[12:14], 1) // version
	_, err := decodeHeader(buf)
	require.True(t, errors.Is(err, ErrCorruptSegment))
}

func TestDecodeHeaderRejectsBadVersion(t *testing.T) {
	t.Parallel()

	buf := make([]byte, reservedHeaderBytes)
	copy(buf, segmentMagic)
	binary.LittleEndian.PutUint64(buf[4:12], 0xCAFE) // non-zero checksum
	binary.LittleEndian.PutUint16(buf[12:14], 99)    // future version
	_, err := decodeHeader(buf)
	require.True(t, errors.Is(err, ErrInvalidFooter))
}

func TestXxh3HeaderFooter(t *testing.T) {
	t.Parallel()

	headerBytes := bytes.Repeat([]byte{0xAB}, reservedHeaderBytes)
	footerBytes := []byte("hello, footer")
	got := xxh3HeaderFooter(headerBytes, footerBytes)
	require.NotZero(t, got)

	// Streaming computes the same value as feeding both regions in
	// one go. Idempotent regression in case we ever switch the
	// streaming model.
	again := xxh3HeaderFooter(headerBytes, footerBytes)
	require.Equal(t, got, again)
}
```

- [ ] **Step 2: Run tests, expect FAIL**

Run:
```bash
just test ./segment
```

Expected: build failure: `undefined: Header`, `undefined: encodeHeader`, etc.

- [ ] **Step 3: Implement `header.go`**

Create `segment/header.go`:

```go
package segment

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/zeebo/xxh3"
)

// Header is the parsed form of the 256-byte fixed header at offset 0
// of every sealed segment file. See DESIGN.md §3.1.2 for the wire
// layout, and the spec §5.1 for byte-offset details.
//
// All offsets are absolute file offsets.
type Header struct {
	Version               uint16
	BlockCount            uint32
	EventCount            uint32
	UniqueDIDCount        uint32
	MinSeq, MaxSeq        uint64
	MinIndexedAt          int64 // unix micros
	MaxIndexedAt          int64 // unix micros
	FooterOffset          uint64
	DIDBloomOffset        uint64
	BlockDIDBloomOffset   uint64
	CollectionIndexOffset uint64
	BlockIndexOffset      uint64
	Checksum              uint64
}

// currentHeaderVersion is the only version produced by this package.
// decodeHeader accepts only this exact version; older or newer files
// land via a future migration path.
const currentHeaderVersion uint16 = 1

// encodeHeader serializes h into a freshly-allocated 256-byte slice.
// Bytes 0..3 are the magic; bytes 4..11 are the checksum; bytes
// 12..97 are the metadata fields; bytes 98..255 are zero-filled
// reserved padding.
func encodeHeader(h Header) []byte {
	buf := make([]byte, reservedHeaderBytes)
	copy(buf[0:4], segmentMagic)
	le := binary.LittleEndian
	le.PutUint64(buf[4:12], h.Checksum)
	le.PutUint16(buf[12:14], h.Version)
	le.PutUint32(buf[14:18], h.BlockCount)
	le.PutUint32(buf[18:22], h.EventCount)
	le.PutUint32(buf[22:26], h.UniqueDIDCount)
	le.PutUint64(buf[26:34], h.MinSeq)
	le.PutUint64(buf[34:42], h.MaxSeq)
	le.PutUint64(buf[42:50], uint64(h.MinIndexedAt))
	le.PutUint64(buf[50:58], uint64(h.MaxIndexedAt))
	le.PutUint64(buf[58:66], h.FooterOffset)
	le.PutUint64(buf[66:74], h.DIDBloomOffset)
	le.PutUint64(buf[74:82], h.BlockDIDBloomOffset)
	le.PutUint64(buf[82:90], h.CollectionIndexOffset)
	le.PutUint64(buf[90:98], h.BlockIndexOffset)
	// buf[98:256] is left zero — reserved for future expansion.
	return buf
}

// decodeHeader parses a 256-byte slice into Header. It validates:
//   - length == reservedHeaderBytes
//   - magic == "jss0"
//   - checksum field is non-zero (zero means "active file"; our
//     contract is that decodeHeader is only ever fed sealed-file bytes)
//   - version == currentHeaderVersion
//
// On any failure decodeHeader returns a sentinel-wrapped error.
func decodeHeader(buf []byte) (Header, error) {
	if len(buf) != reservedHeaderBytes {
		return Header{}, fmt.Errorf("%w: header is %d bytes, want %d",
			ErrInvalidFooter, len(buf), reservedHeaderBytes)
	}
	if !bytes.Equal(buf[0:4], segmentMagic) {
		return Header{}, fmt.Errorf("%w: bad magic %q",
			ErrCorruptSegment, buf[0:4])
	}
	le := binary.LittleEndian
	checksum := le.Uint64(buf[4:12])
	if checksum == 0 {
		return Header{}, fmt.Errorf("%w: checksum is zero (active file?)",
			ErrCorruptSegment)
	}
	version := le.Uint16(buf[12:14])
	if version != currentHeaderVersion {
		return Header{}, fmt.Errorf("%w: header version %d, want %d",
			ErrInvalidFooter, version, currentHeaderVersion)
	}
	h := Header{
		Version:               version,
		BlockCount:            le.Uint32(buf[14:18]),
		EventCount:            le.Uint32(buf[18:22]),
		UniqueDIDCount:        le.Uint32(buf[22:26]),
		MinSeq:                le.Uint64(buf[26:34]),
		MaxSeq:                le.Uint64(buf[34:42]),
		MinIndexedAt:          int64(le.Uint64(buf[42:50])),
		MaxIndexedAt:          int64(le.Uint64(buf[50:58])),
		FooterOffset:          le.Uint64(buf[58:66]),
		DIDBloomOffset:        le.Uint64(buf[66:74]),
		BlockDIDBloomOffset:   le.Uint64(buf[74:82]),
		CollectionIndexOffset: le.Uint64(buf[82:90]),
		BlockIndexOffset:      le.Uint64(buf[90:98]),
		Checksum:              checksum,
	}
	return h, nil
}

// xxh3HeaderFooter computes xxh3 over headerBytes[12:] followed by
// footerBytes — i.e. version through end-of-footer per DESIGN.md
// §3.1.2. The magic and the checksum field itself are excluded so a
// reader can verify the file's integrity without first knowing the
// checksum value.
//
// Callers pass headerBytes with the checksum field zeroed; the
// computed value is what they store back into bytes 4..11 of the
// finalized header.
func xxh3HeaderFooter(headerBytes, footerBytes []byte) uint64 {
	h := xxh3.New()
	_, _ = h.Write(headerBytes[12:])
	_, _ = h.Write(footerBytes)
	return h.Sum64()
}
```

- [ ] **Step 4: Run tests, expect PASS**

Run:
```bash
just test ./segment
```

Expected: PASS, all subtests pass.

- [ ] **Step 5: Commit**

```bash
git add segment/header.go segment/header_test.go
git commit -m "segment: add fixed header encode/decode and xxh3 helper"
```

---

## Task 4: Block index encode/decode

**Files:**
- Create: `segment/footer.go`
- Create: `segment/footer_test.go`

The block index is `block_count × 36 bytes`, packed without an internal header. `block_count` comes from the fixed header; the index is read by computing offsets directly.

- [ ] **Step 1: Write the failing tests**

Create `segment/footer_test.go`:

```go
package segment

import (
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeBlockIndexRoundtrip(t *testing.T) {
	t.Parallel()

	infos := []BlockInfo{
		{
			Offset:           reservedHeaderBytes,
			CompressedSize:   1024,
			UncompressedSize: 4096,
			EventCount:       16,
			MinSeq:           1,
			MaxSeq:           16,
		},
		{
			Offset:           reservedHeaderBytes + 1024 + 8,
			CompressedSize:   2048,
			UncompressedSize: 8192,
			EventCount:       32,
			MinSeq:           17,
			MaxSeq:           48,
		},
	}

	buf := encodeBlockIndex(infos)
	require.Len(t, buf, len(infos)*blockIndexEntrySize)

	got, err := decodeBlockIndex(buf, uint32(len(infos)))
	require.NoError(t, err)
	require.Equal(t, infos, got)
}

func TestEncodeBlockIndexEntryFieldOffsets(t *testing.T) {
	t.Parallel()

	// Pin field offsets within a single 36-byte entry. Same rationale
	// as the header field-offset test: silent reorder = silent file
	// incompatibility.
	info := BlockInfo{
		Offset:           0x0102030405060708,
		CompressedSize:   0x11121314,
		UncompressedSize: 0x21222324,
		EventCount:       0x31323334,
		MinSeq:           0x4142434445464748,
		MaxSeq:           0x5152535455565758,
	}
	buf := encodeBlockIndex([]BlockInfo{info})
	require.Len(t, buf, blockIndexEntrySize)

	require.Equal(t, uint64(0x0102030405060708),
		binary.LittleEndian.Uint64(buf[0:8]))
	require.Equal(t, uint32(0x11121314),
		binary.LittleEndian.Uint32(buf[8:12]))
	require.Equal(t, uint32(0x21222324),
		binary.LittleEndian.Uint32(buf[12:16]))
	require.Equal(t, uint32(0x31323334),
		binary.LittleEndian.Uint32(buf[16:20]))
	require.Equal(t, uint64(0x4142434445464748),
		binary.LittleEndian.Uint64(buf[20:28]))
	require.Equal(t, uint64(0x5152535455565758),
		binary.LittleEndian.Uint64(buf[28:36]))
}

func TestDecodeBlockIndexRejectsLengthMismatch(t *testing.T) {
	t.Parallel()

	// One entry's worth of bytes, but caller asks for two.
	buf := make([]byte, blockIndexEntrySize)
	_, err := decodeBlockIndex(buf, 2)
	require.True(t, errors.Is(err, ErrInvalidBlockIndex))
}

func TestDecodeBlockIndexZeroEntries(t *testing.T) {
	t.Parallel()

	got, err := decodeBlockIndex(nil, 0)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestEncodeBlockIndexProperty(t *testing.T) {
	t.Parallel()

	r := rand.New(rand.NewPCG(1, 2))
	for range 200 {
		n := 1 + r.IntN(64)
		infos := make([]BlockInfo, n)
		for i := range infos {
			infos[i] = BlockInfo{
				Offset:           r.Uint64(),
				CompressedSize:   r.Uint32(),
				UncompressedSize: r.Uint32(),
				EventCount:       r.Uint32(),
				MinSeq:           r.Uint64(),
				MaxSeq:           r.Uint64(),
			}
		}
		buf := encodeBlockIndex(infos)
		got, err := decodeBlockIndex(buf, uint32(n))
		require.NoError(t, err)
		require.Equal(t, infos, got)
	}
}
```

- [ ] **Step 2: Run tests, expect FAIL**

Run:
```bash
just test ./segment
```

Expected: build failure: `undefined: BlockInfo`, `undefined: encodeBlockIndex`, etc.

- [ ] **Step 3: Implement `footer.go`**

Create `segment/footer.go`:

```go
package segment

import (
	"encoding/binary"
	"fmt"
)

// BlockInfo is one entry of the block index (DESIGN.md §3.1.2). It
// describes a block's location and bounds within a sealed segment
// file.
type BlockInfo struct {
	// Offset is the absolute file offset of the block's 8-byte
	// length prefix. The compressed frame begins at Offset + 8.
	Offset uint64

	// CompressedSize is the number of bytes in the zstd frame,
	// excluding the 8-byte length prefix.
	CompressedSize uint32

	// UncompressedSize is the size of the columnar block body
	// before compression.
	UncompressedSize uint32

	// EventCount is the number of events in this block.
	EventCount uint32

	// MinSeq, MaxSeq bound the seq column. MaxSeq >= MinSeq.
	MinSeq, MaxSeq uint64
}

// blockIndexEntrySize is the wire-format size of one block index
// entry: 8 + 4 + 4 + 4 + 8 + 8 = 36 bytes.
const blockIndexEntrySize = 36

// encodeBlockIndex serializes the given infos into a freshly-allocated
// slice of length len(infos) * blockIndexEntrySize. Entries are
// emitted in argument order (which is also the on-disk block order).
func encodeBlockIndex(infos []BlockInfo) []byte {
	buf := make([]byte, len(infos)*blockIndexEntrySize)
	le := binary.LittleEndian
	for i, info := range infos {
		off := i * blockIndexEntrySize
		le.PutUint64(buf[off+0:off+8], info.Offset)
		le.PutUint32(buf[off+8:off+12], info.CompressedSize)
		le.PutUint32(buf[off+12:off+16], info.UncompressedSize)
		le.PutUint32(buf[off+16:off+20], info.EventCount)
		le.PutUint64(buf[off+20:off+28], info.MinSeq)
		le.PutUint64(buf[off+28:off+36], info.MaxSeq)
	}
	return buf
}

// decodeBlockIndex parses count entries out of buf. buf must be
// exactly count * blockIndexEntrySize bytes; mismatch is reported
// as ErrInvalidBlockIndex.
func decodeBlockIndex(buf []byte, count uint32) ([]BlockInfo, error) {
	want := int(count) * blockIndexEntrySize
	if len(buf) != want {
		return nil, fmt.Errorf("%w: have %d bytes, want %d (count=%d)",
			ErrInvalidBlockIndex, len(buf), want, count)
	}
	if count == 0 {
		return nil, nil
	}
	infos := make([]BlockInfo, count)
	le := binary.LittleEndian
	for i := range infos {
		off := i * blockIndexEntrySize
		infos[i] = BlockInfo{
			Offset:           le.Uint64(buf[off+0 : off+8]),
			CompressedSize:   le.Uint32(buf[off+8 : off+12]),
			UncompressedSize: le.Uint32(buf[off+12 : off+16]),
			EventCount:       le.Uint32(buf[off+16 : off+20]),
			MinSeq:           le.Uint64(buf[off+20 : off+28]),
			MaxSeq:           le.Uint64(buf[off+28 : off+36]),
		}
	}
	return infos, nil
}
```

- [ ] **Step 4: Run tests, expect PASS**

Run:
```bash
just test ./segment
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add segment/footer.go segment/footer_test.go
git commit -m "segment: add block index encode/decode"
```

---

## Task 5: Per-block blooms region encode/decode

**Files:**
- Create: `segment/bloom.go`
- Create: `segment/bloom_test.go`

The per-block-blooms region has an 8-byte uncompressed header (`block_count`, `bloom_size_bytes` — both `uint32 LE`) followed by `block_count * bloom_size_bytes` bytes of packed marshaled gloom filters.

This task ships the region-level encode/decode. It also pins the invariant that all marshaled filters with identical parameters produce identically-sized buffers — that's what makes O(1) indexing possible.

- [ ] **Step 1: Write the failing tests**

Create `segment/bloom_test.go`:

```go
package segment

import (
	"errors"
	"testing"

	"github.com/jcalabro/gloom"
	"github.com/stretchr/testify/require"
)

// fixedSizeBlooms returns n filters all built with the same
// parameters; their MarshalBinary outputs must be identical-length.
func fixedSizeBlooms(t *testing.T, n int) []*gloom.Filter {
	t.Helper()
	out := make([]*gloom.Filter, n)
	for i := range out {
		out[i] = gloom.New(perBlockBloomCapacity, perBlockBloomFPRate)
	}
	return out
}

func TestEncodeBlockBloomsRegionRoundtrip(t *testing.T) {
	t.Parallel()

	filters := fixedSizeBlooms(t, 4)
	for i, f := range filters {
		// Distinct content so we can verify per-index decode.
		f.AddString("did:plc:" + string(rune('a'+i)))
	}

	region, sizeBytes, err := encodeBlockBloomsRegion(filters)
	require.NoError(t, err)
	require.Greater(t, sizeBytes, uint32(0))

	for i, want := range filters {
		got, err := decodeBlockBloomFromRegion(region, sizeBytes, i)
		require.NoError(t, err)
		require.Equal(t, want.Test([]byte("did:plc:"+string(rune('a'+i)))),
			got.Test([]byte("did:plc:"+string(rune('a'+i)))))
	}
}

func TestEncodeBlockBloomsRegionEmpty(t *testing.T) {
	t.Parallel()

	region, sizeBytes, err := encodeBlockBloomsRegion(nil)
	require.NoError(t, err)
	require.Zero(t, sizeBytes)
	// 8-byte header only: block_count=0, bloom_size_bytes=0.
	require.Len(t, region, 8)
}

func TestEncodeBlockBloomsRegionRejectsMixedSizes(t *testing.T) {
	t.Parallel()

	mixed := []*gloom.Filter{
		gloom.New(64, 0.01),
		gloom.New(64_000, 0.01), // far larger; different MarshalBinary length
	}
	_, _, err := encodeBlockBloomsRegion(mixed)
	require.Error(t, err)
	require.Contains(t, err.Error(), "size mismatch")
}

func TestDecodeBlockBloomFromRegionRejectsOutOfRange(t *testing.T) {
	t.Parallel()

	filters := fixedSizeBlooms(t, 2)
	region, sizeBytes, err := encodeBlockBloomsRegion(filters)
	require.NoError(t, err)

	_, err = decodeBlockBloomFromRegion(region, sizeBytes, 2)
	require.True(t, errors.Is(err, ErrBlockOutOfRange))
}

func TestDecodeBlockBloomsRegionHeaderRejectsShort(t *testing.T) {
	t.Parallel()

	_, _, err := decodeBlockBloomsRegionHeader([]byte{0x01, 0x02})
	require.True(t, errors.Is(err, ErrInvalidFooter))
}

func TestDecodeBlockBloomsRegionHeaderRoundtrip(t *testing.T) {
	t.Parallel()

	filters := fixedSizeBlooms(t, 3)
	region, sizeBytes, err := encodeBlockBloomsRegion(filters)
	require.NoError(t, err)

	count, sz, err := decodeBlockBloomsRegionHeader(region[:blockBloomsRegionHeaderSize])
	require.NoError(t, err)
	require.EqualValues(t, len(filters), count)
	require.Equal(t, sizeBytes, sz)
}
```

- [ ] **Step 2: Run tests, expect FAIL**

Run:
```bash
just test ./segment
```

Expected: build failure: `undefined: encodeBlockBloomsRegion`, etc.

- [ ] **Step 3: Implement `bloom.go`**

Create `segment/bloom.go`:

```go
package segment

import (
	"encoding/binary"
	"fmt"

	"github.com/jcalabro/gloom"
)

// Bloom-filter sizing knobs. Both apply to DID blooms (segment-level
// and per-block) per DESIGN.md §3.1.3 and the project FP-rate guidance
// in PRACTICES.md/DESIGN.md.
//
// The 0.001 (0.1%) false-positive rate balances on-disk size
// (negligible relative to a ~256 MB segment) against scan-time false
// positives (each FP costs a full block decompress + column scan,
// which is meaningfully expensive). PRACTICES.md and DESIGN.md
// document RAM as cheap on these servers, so we don't penny-pinch.
const (
	perBlockBloomFPRate = 0.001

	// segmentBloomFPRate matches the per-block rate. Memory cost
	// across all segments is bounded; FP cost is one extra pread to
	// load per-block-blooms, which is also small but measurable. Same
	// rate keeps the trade-off symmetric.
	segmentBloomFPRate = 0.001

	// perBlockBloomCapacity is the expected-items count we feed gloom
	// when constructing per-block filters. Per the spec §5.4, all
	// per-block filters are sized identically so the region is
	// indexable by multiplication; we use the writer's configured
	// MaxEventsPerBlock as the upper bound. The actual cardinality of
	// unique DIDs in a block is always ≤ MaxEventsPerBlock, so the
	// configured FP rate is an upper bound on the realized rate.
	//
	// Callers that need a different cap (e.g., compaction with a
	// rebuilt block) should pass an explicit capacity; for now this
	// constant is what Seal uses. We tie it to DefaultMaxEventsPerBlock
	// because that's the writer default; the seal-time call is
	// parameterized so an alternate cap is supported.
	perBlockBloomCapacity = uint64(DefaultMaxEventsPerBlock)
)

// blockBloomsRegionHeaderSize is the 8-byte uncompressed header that
// precedes the packed per-block filters: block_count (uint32 LE) +
// bloom_size_bytes (uint32 LE).
const blockBloomsRegionHeaderSize = 8

// encodeBlockBloomsRegion serializes the per-block blooms region
// (DESIGN.md §3.1.3, spec §5.4). Every filter must marshal to an
// identical length; this is the invariant that lets the reader index
// blooms by multiplication. We assert it here so a violation is loud
// rather than a silent on-disk corruption.
//
// Returns the encoded region and the per-bloom size in bytes (which
// the caller stores in the header for cross-region offset math).
func encodeBlockBloomsRegion(filters []*gloom.Filter) ([]byte, uint32, error) {
	header := make([]byte, blockBloomsRegionHeaderSize)
	if len(filters) == 0 {
		// Header carries (block_count=0, bloom_size_bytes=0) and no body.
		return header, 0, nil
	}

	first, err := filters[0].MarshalBinary()
	if err != nil {
		return nil, 0, fmt.Errorf("segment: marshal block bloom 0: %w", err)
	}
	sizeBytes := uint32(len(first))

	body := make([]byte, len(filters)*int(sizeBytes))
	copy(body[:sizeBytes], first)

	for i := 1; i < len(filters); i++ {
		marshaled, err := filters[i].MarshalBinary()
		if err != nil {
			return nil, 0, fmt.Errorf("segment: marshal block bloom %d: %w", i, err)
		}
		if uint32(len(marshaled)) != sizeBytes {
			return nil, 0, fmt.Errorf(
				"segment: per-block bloom size mismatch at block %d: got %d, want %d",
				i, len(marshaled), sizeBytes)
		}
		copy(body[i*int(sizeBytes):(i+1)*int(sizeBytes)], marshaled)
	}

	le := binary.LittleEndian
	le.PutUint32(header[0:4], uint32(len(filters)))
	le.PutUint32(header[4:8], sizeBytes)

	return append(header, body...), sizeBytes, nil
}

// decodeBlockBloomsRegionHeader reads the 8-byte region header and
// returns (block_count, bloom_size_bytes). The full region body
// follows on disk and is read by the caller via pread on demand.
func decodeBlockBloomsRegionHeader(buf []byte) (uint32, uint32, error) {
	if len(buf) < blockBloomsRegionHeaderSize {
		return 0, 0, fmt.Errorf("%w: bloom region header is %d bytes, want %d",
			ErrInvalidFooter, len(buf), blockBloomsRegionHeaderSize)
	}
	le := binary.LittleEndian
	count := le.Uint32(buf[0:4])
	size := le.Uint32(buf[4:8])
	return count, size, nil
}

// decodeBlockBloomFromRegion returns the bloom for block idx given a
// region body (everything after the 8-byte header) and the per-bloom
// size. Most callers will not have the whole body in memory; they use
// the (offset, size) math directly via pread. This helper exists for
// the in-memory roundtrip path used by tests.
func decodeBlockBloomFromRegion(region []byte, sizeBytes uint32, idx int) (*gloom.Filter, error) {
	if idx < 0 {
		return nil, fmt.Errorf("%w: idx %d < 0", ErrBlockOutOfRange, idx)
	}
	body := region[blockBloomsRegionHeaderSize:]
	start := idx * int(sizeBytes)
	end := start + int(sizeBytes)
	if end > len(body) {
		return nil, fmt.Errorf("%w: idx %d past region (size %d)",
			ErrBlockOutOfRange, idx, len(body)/int(sizeBytes))
	}
	f, err := gloom.UnmarshalBinary(body[start:end])
	if err != nil {
		return nil, fmt.Errorf("segment: unmarshal block bloom %d: %w", idx, err)
	}
	return f, nil
}
```

- [ ] **Step 4: Run tests, expect PASS**

Run:
```bash
just test ./segment
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add segment/bloom.go segment/bloom_test.go
git commit -m "segment: add per-block blooms region encode/decode"
```

---

## Task 6: Collection block index encode/decode

**Files:**
- Create: `segment/collection.go`
- Create: `segment/collection_test.go`

The collection block index has a 16-byte uncompressed header and a zstd-compressed body containing a string table + per-block bitmasks. NSID IDs are assigned in first-seen order during the seal walk.

- [ ] **Step 1: Write the failing tests**

Create `segment/collection_test.go`:

```go
package segment

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeCollectionIndexRoundtrip(t *testing.T) {
	t.Parallel()

	idx := CollectionIndex{
		StringTable: []string{
			"app.bsky.feed.post",
			"app.bsky.feed.like",
			"app.bsky.graph.follow",
		},
		BlockBitmasks: [][]uint32{
			{0, 1},    // block 0 has post + like
			{0, 2},    // block 1 has post + follow
			{1, 2},    // block 2 has like + follow
		},
	}

	buf, err := encodeCollectionIndex(idx)
	require.NoError(t, err)

	got, err := decodeCollectionIndex(buf)
	require.NoError(t, err)
	require.Equal(t, idx.StringTable, got.StringTable)
	require.Len(t, got.BlockBitmasks, len(idx.BlockBitmasks))
	for i := range idx.BlockBitmasks {
		require.ElementsMatch(t, idx.BlockBitmasks[i], got.BlockBitmasks[i],
			"block %d", i)
	}
}

func TestEncodeCollectionIndexEmpty(t *testing.T) {
	t.Parallel()

	// Empty segment is meaningless for sealing in production, but the
	// encoder must not panic on it. Round-trips to an equivalent empty
	// index.
	idx := CollectionIndex{StringTable: nil, BlockBitmasks: nil}
	buf, err := encodeCollectionIndex(idx)
	require.NoError(t, err)
	got, err := decodeCollectionIndex(buf)
	require.NoError(t, err)
	require.Empty(t, got.StringTable)
	require.Empty(t, got.BlockBitmasks)
}

func TestEncodeCollectionIndexBitmaskBoundary(t *testing.T) {
	t.Parallel()

	// Test the bitmask byte boundary: 8 collections produce
	// bitmask_len == 1; 9 produce bitmask_len == 2.
	idx := CollectionIndex{
		StringTable: []string{"a", "b", "c", "d", "e", "f", "g", "h"},
		BlockBitmasks: [][]uint32{
			{0, 7},
			{1, 6},
		},
	}
	buf, err := encodeCollectionIndex(idx)
	require.NoError(t, err)

	got, err := decodeCollectionIndex(buf)
	require.NoError(t, err)
	require.Equal(t, idx.StringTable, got.StringTable)
	require.ElementsMatch(t, idx.BlockBitmasks[0], got.BlockBitmasks[0])
	require.ElementsMatch(t, idx.BlockBitmasks[1], got.BlockBitmasks[1])

	idx2 := CollectionIndex{
		StringTable:   append([]string(nil), idx.StringTable...),
		BlockBitmasks: [][]uint32{{8}},
	}
	idx2.StringTable = append(idx2.StringTable, "i")
	buf2, err := encodeCollectionIndex(idx2)
	require.NoError(t, err)
	got2, err := decodeCollectionIndex(buf2)
	require.NoError(t, err)
	require.ElementsMatch(t, []uint32{8}, got2.BlockBitmasks[0])
}

func TestDecodeCollectionIndexRejectsShortHeader(t *testing.T) {
	t.Parallel()

	_, err := decodeCollectionIndex([]byte{0x01, 0x02})
	require.True(t, errors.Is(err, ErrInvalidFooter))
}

func TestEncodeCollectionIndexRejectsOversizedNSID(t *testing.T) {
	t.Parallel()

	long := make([]byte, 256)
	for i := range long {
		long[i] = 'a'
	}
	idx := CollectionIndex{
		StringTable:   []string{string(long)},
		BlockBitmasks: [][]uint32{{0}},
	}
	_, err := encodeCollectionIndex(idx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds")
}
```

- [ ] **Step 2: Run tests, expect FAIL**

Run:
```bash
just test ./segment
```

Expected: build failure: `undefined: CollectionIndex`, etc.

- [ ] **Step 3: Implement `collection.go`**

Create `segment/collection.go`:

```go
package segment

import (
	"encoding/binary"
	"fmt"
	"math"
)

// CollectionIndex is the parsed form of the collection block index
// (DESIGN.md §3.1.4, spec §5.5). StringTable is the unique NSIDs in
// first-seen order; the index of each NSID in StringTable is its
// "collection ID". BlockBitmasks[i] is the sorted, deduplicated set
// of collection IDs present in block i.
//
// On disk the bitmasks are stored as packed-byte bit arrays of size
// ceil(len(StringTable) / 8); we decode them into []uint32 of IDs
// here so callers don't have to do bit math.
type CollectionIndex struct {
	StringTable   []string
	BlockBitmasks [][]uint32
}

// collectionIndexHeaderSize is the uncompressed 16-byte header that
// precedes the zstd-compressed body.
const collectionIndexHeaderSize = 16

// encodeCollectionIndex serializes idx with the spec §5.5 wire layout.
// The body is zstd-compressed; the 16-byte header is uncompressed.
//
// Returns ErrFieldTooLong wrapped if any NSID exceeds the on-disk
// uint8 length column.
func encodeCollectionIndex(idx CollectionIndex) ([]byte, error) {
	collectionCount := len(idx.StringTable)
	blockCount := len(idx.BlockBitmasks)
	if collectionCount > math.MaxUint32 || blockCount > math.MaxUint32 {
		return nil, fmt.Errorf("%w: collection or block count overflows uint32",
			ErrInvalidFooter)
	}
	bitmaskLen := (collectionCount + 7) / 8

	// Build the uncompressed body: string table + per-block bitmasks.
	var body []byte
	for i, nsid := range idx.StringTable {
		if len(nsid) > math.MaxUint8 {
			return nil, fmt.Errorf("%w: NSID %d exceeds %d bytes",
				ErrFieldTooLong, i, math.MaxUint8)
		}
		body = append(body, uint8(len(nsid)))
		body = append(body, nsid...)
	}
	for blockIdx, ids := range idx.BlockBitmasks {
		mask := make([]byte, bitmaskLen)
		for _, id := range ids {
			if int(id) >= collectionCount {
				return nil, fmt.Errorf(
					"%w: block %d references collection id %d (table has %d)",
					ErrInvalidFooter, blockIdx, id, collectionCount)
			}
			mask[id/8] |= 1 << (id % 8)
		}
		body = append(body, mask...)
	}

	bodyZstd := blockEncoder.EncodeAll(body, nil)

	header := make([]byte, collectionIndexHeaderSize)
	le := binary.LittleEndian
	le.PutUint32(header[0:4], uint32(collectionCount))
	le.PutUint32(header[4:8], uint32(blockCount))
	le.PutUint32(header[8:12], uint32(bitmaskLen))
	le.PutUint32(header[12:16], uint32(len(body)))

	return append(header, bodyZstd...), nil
}

// decodeCollectionIndex parses the on-disk bytes into a CollectionIndex.
func decodeCollectionIndex(buf []byte) (CollectionIndex, error) {
	if len(buf) < collectionIndexHeaderSize {
		return CollectionIndex{}, fmt.Errorf(
			"%w: collection index header is %d bytes, want >=%d",
			ErrInvalidFooter, len(buf), collectionIndexHeaderSize)
	}

	le := binary.LittleEndian
	collectionCount := le.Uint32(buf[0:4])
	blockCount := le.Uint32(buf[4:8])
	bitmaskLen := le.Uint32(buf[8:12])
	uncompressedSize := le.Uint32(buf[12:16])

	wantBitmaskLen := (collectionCount + 7) / 8
	if bitmaskLen != wantBitmaskLen {
		return CollectionIndex{}, fmt.Errorf(
			"%w: bitmask_len %d, want %d for collection_count %d",
			ErrInvalidFooter, bitmaskLen, wantBitmaskLen, collectionCount)
	}

	body, err := blockDecoder.DecodeAll(buf[collectionIndexHeaderSize:], nil)
	if err != nil {
		return CollectionIndex{}, fmt.Errorf("segment: collection index decompress: %w", err)
	}
	if uint32(len(body)) != uncompressedSize {
		return CollectionIndex{}, fmt.Errorf(
			"%w: collection body decompressed to %d bytes, header claimed %d",
			ErrInvalidFooter, len(body), uncompressedSize)
	}

	off := 0
	stringTable := make([]string, collectionCount)
	for i := range stringTable {
		if off+1 > len(body) {
			return CollectionIndex{}, fmt.Errorf(
				"%w: truncated NSID length at i=%d", ErrInvalidFooter, i)
		}
		strLen := int(body[off])
		off++
		if off+strLen > len(body) {
			return CollectionIndex{}, fmt.Errorf(
				"%w: truncated NSID body at i=%d", ErrInvalidFooter, i)
		}
		// Copy: the body buffer is private to this call but the result
		// outlives it; we can't alias.
		stringTable[i] = string(body[off : off+strLen])
		off += strLen
	}

	bitmasks := make([][]uint32, blockCount)
	for i := range bitmasks {
		if off+int(bitmaskLen) > len(body) {
			return CollectionIndex{}, fmt.Errorf(
				"%w: truncated bitmask at block %d", ErrInvalidFooter, i)
		}
		mask := body[off : off+int(bitmaskLen)]
		off += int(bitmaskLen)
		var ids []uint32
		for byteIdx, b := range mask {
			for bit := 0; bit < 8 && b != 0; bit++ {
				if b&(1<<bit) != 0 {
					id := uint32(byteIdx*8 + bit)
					if id < collectionCount {
						ids = append(ids, id)
					}
				}
			}
		}
		bitmasks[i] = ids
	}

	if off != len(body) {
		return CollectionIndex{}, fmt.Errorf(
			"%w: trailing bytes in collection body (off=%d, len=%d)",
			ErrInvalidFooter, off, len(body))
	}

	return CollectionIndex{
		StringTable:   stringTable,
		BlockBitmasks: bitmasks,
	}, nil
}
```

- [ ] **Step 4: Run tests, expect PASS**

Run:
```bash
just test ./segment
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add segment/collection.go segment/collection_test.go
git commit -m "segment: add collection block index encode/decode"
```

---

## Task 7: `decodeBlockCompressedSized` helper

**Files:**
- Modify: `segment/zstd.go`

The seal walk needs both the decoded events *and* the uncompressed body length per block (to populate `BlockInfo.UncompressedSize`). Adding a tiny variant of `decodeBlockCompressed` that returns the size avoids decoding twice.

- [ ] **Step 1: Append the helper to `segment/zstd.go`**

Add after `decodeBlockCompressed`:

```go
// decodeBlockCompressedSized is like decodeBlockCompressed but also
// returns the decompressed body length. Seal needs the size to
// populate BlockInfo.UncompressedSize for the block index without a
// second decompress.
//
// The buffer-aliasing contract from decodeBlock applies: the returned
// events alias the (private) decompressed body for their string and
// payload columns. Callers that need to retain string fields beyond
// the events' lifetime must clone.
func decodeBlockCompressedSized(frame []byte) ([]Event, int, error) {
	body, err := blockDecoder.DecodeAll(frame, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("segment: zstd decompress: %w", err)
	}
	events, err := decodeBlock(body)
	if err != nil {
		return nil, 0, err
	}
	return events, len(body), nil
}
```

- [ ] **Step 2: Confirm it builds**

Run:
```bash
just test ./segment
```

Expected: PASS (no new tests yet; helper is just compiled).

- [ ] **Step 3: Add a regression test**

Append to `segment/block_test.go` (or wherever the existing block-level tests live; `block_test.go` is the right home):

```go
func TestDecodeBlockCompressedSizedReturnsBodyLen(t *testing.T) {
	t.Parallel()

	events := []Event{
		{Seq: 1, Kind: KindCreate, DID: "did:plc:a", Payload: []byte("p1")},
		{Seq: 2, Kind: KindCreate, DID: "did:plc:b", Payload: []byte("p2")},
	}
	frame, err := encodeBlockCompressed(events)
	require.NoError(t, err)

	gotEvents, gotSize, err := decodeBlockCompressedSized(frame)
	require.NoError(t, err)
	require.Len(t, gotEvents, len(events))

	// Cross-check: the body length should equal what encodeBlock
	// produced before zstd-wrapping.
	body, err := encodeBlock(events)
	require.NoError(t, err)
	require.Equal(t, len(body), gotSize)
}
```

- [ ] **Step 4: Run tests, expect PASS**

Run:
```bash
just test ./segment
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add segment/zstd.go segment/block_test.go
git commit -m "segment: add decodeBlockCompressedSized helper for seal walk"
```

---

## Task 8: Sealed-vs-active detection in `New()`

**Files:**
- Modify: `segment/writer.go`
- Modify: `segment/writer_test.go`

This task replaces the kaizen-comment-only stub in `resumeExistingSegment`. After the magic check, we read 8 bytes at offset 4; non-zero is a sealed file, return `ErrSegmentSealed`. The corresponding kaizen comment in `writer_test.go` is replaced by a real test that builds an obviously-sealed file (we don't have `Seal()` yet, so we hand-construct a file with non-zero checksum bytes — a valid test fixture for the detection logic in isolation).

- [ ] **Step 1: Replace the kaizen comment in `resumeExistingSegment`**

Open `segment/writer.go`. Find:

```go
	// Sealed-vs-active is intentionally not checked here: in this slice
	// every successfully-initialized segment carries segmentMagic at
	// offset 0, and the sealed marker is the (future) checksum trailer.
	// kaizen: once the trailer format lands, this function should also
	// reject sealed segments with ErrSegmentSealed.
```

Replace with:

```go
	// Sealed-vs-active detection: bytes 4..11 are zero on an active
	// file (initializeNewSegment writes only the magic into the
	// reserved 256-byte header) and non-zero on a sealed file (Seal
	// patches in the xxh3 checksum). DESIGN.md §3.1.2 names this the
	// "checksum at offset 4" signal; spec §8 documents the convention.
	var checksumBuf [8]byte
	if _, err := f.ReadAt(checksumBuf[:], 4); err != nil {
		return fmt.Errorf("segment: read checksum: %w", err)
	}
	if binary.LittleEndian.Uint64(checksumBuf[:]) != 0 {
		return fmt.Errorf("%w: %s", ErrSegmentSealed, path)
	}
```

- [ ] **Step 2: Replace the kaizen-comment-only test**

Open `segment/writer_test.go`. Find `TestNewRejectsBadMagic` (around line 74) and the kaizen comment above it. Remove the kaizen comment block and add a new test below `TestNewRejectsBadMagic`:

```go
// TestNewRejectsSealedFile verifies that the sealed-vs-active
// detection logic in resumeExistingSegment returns ErrSegmentSealed
// for a file whose checksum bytes (offset 4..11) are non-zero. We
// build the fixture by hand here rather than via the public Seal API
// because Seal lives in a sibling package file and we want this test
// to cover detection in isolation; an end-to-end test that round-
// trips through Seal lives in seal_test.go.
func TestNewRejectsSealedFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	header := make([]byte, reservedHeaderBytes)
	copy(header, segmentMagic)
	// Any non-zero value at offset 4..11 trips the detection.
	binary.LittleEndian.PutUint64(header[4:12], 0xCAFEBABE)
	require.NoError(t, os.WriteFile(path, header, 0o644))

	_, err := New(Config{Path: path})
	require.True(t, errors.Is(err, ErrSegmentSealed))
}
```

- [ ] **Step 3: Run tests, expect PASS**

Run:
```bash
just test ./segment
```

Expected: PASS, all subtests including the new one.

- [ ] **Step 4: Confirm lint**

Run:
```bash
just lint
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add segment/writer.go segment/writer_test.go
git commit -m "segment: detect sealed files in New() via checksum at offset 4"
```

---

## Task 9: `SealResult` type + skeleton `Seal()` (errors only)

**Files:**
- Create: `segment/seal.go`
- Create: `segment/seal_test.go`

This task ships the public surface and the precondition checks. The actual walk-and-write happens in Task 10. Splitting these means we can verify the error-path behavior independently of the happy path.

- [ ] **Step 1: Write the failing tests**

Create `segment/seal_test.go`:

```go
package segment

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSealAfterCloseReturnsErrClosed(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	_, err = w.Seal()
	require.True(t, errors.Is(err, ErrClosed))
}

func TestSealOnEmptyWriterProducesValidFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path})
	require.NoError(t, err)

	res, err := w.Seal()
	require.NoError(t, err)
	require.EqualValues(t, 0, res.BlockCount)
	require.EqualValues(t, 0, res.EventCount)
	require.NotZero(t, res.Checksum)
	require.Greater(t, res.FileSize, int64(reservedHeaderBytes))

	// Subsequent calls report ErrClosed.
	_, err = w.Seal()
	require.True(t, errors.Is(err, ErrClosed))

	// Close after Seal is a no-op.
	require.NoError(t, w.Close())
}

func TestSealAfterStickyErrIsRejected(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 4})
	require.NoError(t, err)
	_, err = w.Append(Event{Seq: 1, Kind: KindCreate, DID: "d"})
	require.NoError(t, err)

	// Closing the underlying file behind the writer's back makes the
	// next Write fail; flushLocked latches stickyErr. Seal must surface
	// the same latched error rather than partially succeed.
	require.NoError(t, w.file.Close())

	flushErr := w.Flush()
	require.Error(t, flushErr)

	_, err = w.Seal()
	require.ErrorIs(t, err, flushErr)
}
```

- [ ] **Step 2: Run tests, expect FAIL**

Run:
```bash
just test ./segment
```

Expected: build failure: `undefined: SealResult`, `Writer has no method Seal`.

- [ ] **Step 3: Implement the seal skeleton**

Create `segment/seal.go`:

```go
package segment

import (
	"encoding/binary"
	"fmt"
	"os"
)

// SealResult is returned by Writer.Seal so the caller (orchestrator)
// can log/emit metrics without re-reading the file.
type SealResult struct {
	BlockCount     uint32
	EventCount     uint32
	UniqueDIDCount uint32
	MinSeq         uint64
	MaxSeq         uint64
	MinIndexedAt   int64
	MaxIndexedAt   int64
	Checksum       uint64
	FooterOffset   uint64
	FileSize       int64
}

// Seal finalizes the active segment file: flushes any pending events,
// walks the on-disk frames to gather per-block stats, writes the
// variable-length footer at end-of-file, patches the finalized
// 256-byte fixed header at offset 0, fsyncs, and closes the file.
//
// Seal consumes the Writer. After a successful Seal, the writer is
// closed and any subsequent Append/Flush/Seal/Close call returns
// ErrClosed (Close is idempotent and returns nil).
//
// On failure, the file is left in a state from which the caller can
// recover by opening a fresh Writer at the same path:
//   - Failure before the footer is durable: the file is untouched.
//   - Failure after the footer is durable but before the header is
//     patched: Seal explicitly truncates the partial footer back off
//     before returning, restoring the active-state "last byte is the
//     last good frame" invariant. The detail is documented in
//     sealAfterFlush.
//
// Seal performs no goroutine work. It is safe to call from any
// goroutine that already serializes access to this Writer.
func (w *Writer) Seal() (SealResult, error) {
	if w.closed {
		return SealResult{}, ErrClosed
	}
	if w.stickyErr != nil {
		return SealResult{}, w.stickyErr
	}
	// Flush pending. flushLocked is a no-op if pending is empty.
	if err := w.flushLocked(); err != nil {
		return SealResult{}, err
	}
	res, err := w.sealAfterFlush()
	if err != nil {
		return SealResult{}, err
	}
	// Mark the writer closed: Seal is terminal. The underlying file
	// has already been Close()d by sealAfterFlush.
	w.closed = true
	return res, nil
}

// sealAfterFlush is the body of Seal after flushLocked has fsynced
// any pending block. It performs the walk, builds the footer, writes
// it, patches the header, fsyncs, and closes the file.
//
// Implementation lands in Task 10.
func (w *Writer) sealAfterFlush() (SealResult, error) {
	return SealResult{}, fmt.Errorf("segment: sealAfterFlush not yet implemented")
}

// activeFileSize returns the current on-disk size of the writer's
// file. Used by sealAfterFlush to compute footer_offset.
func (w *Writer) activeFileSize() (int64, error) {
	info, err := w.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("segment: stat for seal: %w", err)
	}
	return info.Size(), nil
}

// truncateFooterTail truncates the file back to footerOffset and
// fsyncs. Used by sealAfterFlush when the header pwrite fails: the
// footer is on disk but unreferenced by a finalized header, leaving
// the file in an ambiguous state. Truncating restores the active-
// state invariant ("last byte is the last good frame"), so the next
// Writer.New() can resume cleanly and the operator can re-call Seal.
//
// We use file.Truncate (which acts on the inode regardless of file
// position) and fsync immediately so a second crash before any
// further writes cannot resurrect the truncated bytes. We don't fsync
// the parent directory: only the file's contents and size are
// changing, both inode-attached.
func (w *Writer) truncateFooterTail(footerOffset int64) error {
	if err := w.file.Truncate(footerOffset); err != nil {
		return fmt.Errorf("segment: truncate footer: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("segment: fsync truncated file: %w", err)
	}
	return nil
}

// readFrameAt reads a single [uint64 LE compressed_len][zstd frame]
// pair starting at fileOffset. Used by the seal walk in Task 10. We
// keep it here so the seal-only seek logic is co-located.
func (w *Writer) readFrameAt(fileOffset int64) (frame []byte, frameLen int, err error) {
	var lenBuf [8]byte
	if _, err := w.file.ReadAt(lenBuf[:], fileOffset); err != nil {
		return nil, 0, fmt.Errorf("segment: read frame length at %d: %w",
			fileOffset, err)
	}
	frameSize := binary.LittleEndian.Uint64(lenBuf[:])
	frame = make([]byte, frameSize)
	if _, err := w.file.ReadAt(frame, fileOffset+8); err != nil {
		return nil, 0, fmt.Errorf("segment: read frame body at %d: %w",
			fileOffset+8, err)
	}
	return frame, len(lenBuf) + int(frameSize), nil
}

// closeFile closes the writer's underlying file. Used by sealAfterFlush
// at the end of the happy path. The standalone helper exists so
// sealAfterFlush can call it once at end-of-success without polluting
// the error-path control flow.
func (w *Writer) closeFile() error {
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("segment: close after seal: %w", err)
	}
	return nil
}

// avoid an unused-import warning while sealAfterFlush is a stub.
var _ = os.O_RDONLY
```

- [ ] **Step 4: Run tests, expect FAIL on the empty-writer happy-path test**

Run:
```bash
just test ./segment -run TestSeal
```

Expected: `TestSealAfterCloseReturnsErrClosed` passes, `TestSealAfterStickyErrIsRejected` passes, `TestSealOnEmptyWriterProducesValidFile` FAILS at `sealAfterFlush not yet implemented`.

- [ ] **Step 5: Commit**

```bash
git add segment/seal.go segment/seal_test.go
git commit -m "segment: scaffold Seal() preconditions and SealResult"
```

---

## Task 10: `sealAfterFlush` happy path

**Files:**
- Modify: `segment/seal.go`

This is the substantive task: walk the on-disk frames, build the four footer sections, compute xxh3, write the footer, patch the header, fsync, close.

- [ ] **Step 1: Replace the stub `sealAfterFlush`**

Open `segment/seal.go`. Replace the `sealAfterFlush` stub with the full implementation:

```go
import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/jcalabro/gloom"
)

// sealAfterFlush walks the active file's frames to gather per-block
// stats, builds the variable-length footer in memory, writes it at
// EOF, patches the finalized fixed header at offset 0, fsyncs, and
// closes the file.
//
// Failure paths and recovery:
//   - Walk fails: file is untouched, error is returned. The next
//     Writer.New() resumes cleanly.
//   - Footer Write fails: a partial footer past the last good frame
//     is written. Those bytes do not parse as a frame length prefix
//     (because the first 8 bytes of the footer are the block index,
//     which is a uint64 file offset, which when interpreted as a
//     length always overruns the file). lastGoodOffset truncates them
//     on the next Writer.New(). Seal latches stickyErr and returns.
//   - Footer Sync fails: same as above; the footer bytes may or may
//     not be durable, but the recovery path is identical.
//   - Header WriteAt fails: the footer is durable but the header is
//     still zero. We explicitly truncate the footer back off here so
//     the file is restored to the active-state invariant. stickyErr
//     latched.
//   - Header Sync fails: both writes are durable per the kernel; we
//     leave the file as-is and rely on the Reader.Open checksum check
//     at next open to detect any corruption. stickyErr latched.
func (w *Writer) sealAfterFlush() (SealResult, error) {
	footerOffset, err := w.activeFileSize()
	if err != nil {
		return SealResult{}, err
	}

	walk, err := w.walkBlocks(footerOffset)
	if err != nil {
		return SealResult{}, err
	}

	footerBytes, header, err := buildFooter(walk, w.cfg.MaxEventsPerBlock, footerOffset)
	if err != nil {
		return SealResult{}, err
	}

	headerBytes := encodeHeader(header)
	checksum := xxh3HeaderFooter(headerBytes, footerBytes)
	header.Checksum = checksum
	binary.LittleEndian.PutUint64(headerBytes[4:12], checksum)

	// Footer write.
	if _, err := w.file.WriteAt(footerBytes, footerOffset); err != nil {
		w.stickyErr = fmt.Errorf("segment: write footer: %w", err)
		return SealResult{}, w.stickyErr
	}
	if err := w.file.Sync(); err != nil {
		w.stickyErr = fmt.Errorf("segment: fsync footer: %w", err)
		return SealResult{}, w.stickyErr
	}

	// Header pwrite.
	if _, err := w.file.WriteAt(headerBytes, 0); err != nil {
		// Footer is durable but header is zero. Truncate the footer
		// back off so the file is restored to active-state invariants.
		// We swallow the truncate error in favor of surfacing the
		// original WriteAt failure, but log it via the wrapped error.
		writeErr := fmt.Errorf("segment: write header: %w", err)
		if truncErr := w.truncateFooterTail(footerOffset); truncErr != nil {
			w.stickyErr = fmt.Errorf("%w (also: %v)", writeErr, truncErr)
		} else {
			w.stickyErr = writeErr
		}
		return SealResult{}, w.stickyErr
	}
	if err := w.file.Sync(); err != nil {
		// Both writes happened but we couldn't confirm durability of
		// the header. Don't truncate: the bytes may already be durable
		// and truncating could destroy a valid sealed file. The Reader
		// will detect corruption on Open via the xxh3 check.
		w.stickyErr = fmt.Errorf("segment: fsync sealed file: %w", err)
		return SealResult{}, w.stickyErr
	}

	if err := w.closeFile(); err != nil {
		w.stickyErr = err
		return SealResult{}, err
	}

	stat, _ := w.fileStatAfterClose() // best-effort for FileSize reporting
	return SealResult{
		BlockCount:     header.BlockCount,
		EventCount:     header.EventCount,
		UniqueDIDCount: header.UniqueDIDCount,
		MinSeq:         header.MinSeq,
		MaxSeq:         header.MaxSeq,
		MinIndexedAt:   header.MinIndexedAt,
		MaxIndexedAt:   header.MaxIndexedAt,
		Checksum:       checksum,
		FooterOffset:   header.FooterOffset,
		FileSize:       stat,
	}, nil
}

// fileStatAfterClose stats the sealed file by path so SealResult can
// report FileSize. It's "best-effort": a stat error is silently
// reported as 0 because the seal itself already succeeded.
func (w *Writer) fileStatAfterClose() (int64, error) {
	info, err := osStat(w.cfg.Path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// blockWalkResult holds the per-block info and segment-wide
// accumulators gathered during the seal walk.
type blockWalkResult struct {
	infos []BlockInfo

	// Segment-wide accumulators.
	totalEventCount uint32
	uniqueDIDs      map[string]struct{}
	minSeq          uint64
	maxSeq          uint64
	minIndexedAt    int64
	maxIndexedAt    int64

	// Per-block bloom inputs and collection IDs.
	perBlockDIDs        []map[string]struct{}
	perBlockCollections [][]uint32

	// Global collection string table in first-seen order.
	collectionStringTable []string
	collectionIDByName    map[string]uint32

	// Did we see any events at all? minSeq/minIndexedAt are only
	// meaningful when this is true.
	sawAny bool
}

// walkBlocks walks the framed-block region of the active file from
// reservedHeaderBytes to footerOffset, decompressing each frame and
// gathering per-block stats.
func (w *Writer) walkBlocks(footerOffset int64) (blockWalkResult, error) {
	res := blockWalkResult{
		uniqueDIDs:         map[string]struct{}{},
		collectionIDByName: map[string]uint32{},
	}
	off := int64(reservedHeaderBytes)
	for off < footerOffset {
		frame, frameSize, err := w.readFrameAt(off)
		if err != nil {
			return blockWalkResult{}, err
		}

		events, uncompressedSize, err := decodeBlockCompressedSized(frame)
		if err != nil {
			return blockWalkResult{}, fmt.Errorf("segment: decode block at %d: %w",
				off, err)
		}

		info := BlockInfo{
			Offset:           uint64(off),
			CompressedSize:   uint32(len(frame)),
			UncompressedSize: uint32(uncompressedSize),
			EventCount:       uint32(len(events)),
		}
		blockDIDs := map[string]struct{}{}
		blockCollections := map[uint32]struct{}{}

		for i, ev := range events {
			if i == 0 {
				info.MinSeq = ev.Seq
				info.MaxSeq = ev.Seq
			}
			if ev.Seq < info.MinSeq {
				info.MinSeq = ev.Seq
			}
			if ev.Seq > info.MaxSeq {
				info.MaxSeq = ev.Seq
			}

			if !res.sawAny {
				res.minSeq = ev.Seq
				res.maxSeq = ev.Seq
				res.minIndexedAt = ev.IndexedAt
				res.maxIndexedAt = ev.IndexedAt
				res.sawAny = true
			} else {
				if ev.Seq < res.minSeq {
					res.minSeq = ev.Seq
				}
				if ev.Seq > res.maxSeq {
					res.maxSeq = ev.Seq
				}
				if ev.IndexedAt < res.minIndexedAt {
					res.minIndexedAt = ev.IndexedAt
				}
				if ev.IndexedAt > res.maxIndexedAt {
					res.maxIndexedAt = ev.IndexedAt
				}
			}

			// DIDs may be empty for some kinds, but we add the empty
			// string to the bloom regardless — it's a no-op for the
			// bloom if every event has a DID, and a tiny bit of
			// noise otherwise. In practice every kind in the firehose
			// carries a DID.
			if ev.DID != "" {
				if _, ok := res.uniqueDIDs[ev.DID]; !ok {
					// Clone the string: ev.DID aliases the decompressed
					// frame, which goes out of scope at the end of this
					// loop iteration.
					did := string([]byte(ev.DID))
					res.uniqueDIDs[did] = struct{}{}
					blockDIDs[did] = struct{}{}
				} else if _, ok := blockDIDs[ev.DID]; !ok {
					blockDIDs[string([]byte(ev.DID))] = struct{}{}
				}
			}
			if ev.Collection != "" {
				id, ok := res.collectionIDByName[ev.Collection]
				if !ok {
					if uint64(len(res.collectionStringTable)) >= math.MaxUint32 {
						return blockWalkResult{}, fmt.Errorf(
							"%w: too many distinct collections", ErrInvalidFooter)
					}
					col := string([]byte(ev.Collection))
					id = uint32(len(res.collectionStringTable))
					res.collectionStringTable = append(res.collectionStringTable, col)
					res.collectionIDByName[col] = id
				}
				blockCollections[id] = struct{}{}
			}
		}

		res.infos = append(res.infos, info)
		res.perBlockDIDs = append(res.perBlockDIDs, blockDIDs)

		ids := make([]uint32, 0, len(blockCollections))
		for id := range blockCollections {
			ids = append(ids, id)
		}
		// Sort ids ascending for deterministic bitmask emission and
		// downstream BlockCollections() output.
		sortUint32(ids)
		res.perBlockCollections = append(res.perBlockCollections, ids)

		res.totalEventCount += uint32(len(events))
		off += int64(frameSize)
	}
	return res, nil
}

// buildFooter assembles the four footer sections in the layout
// specified in DESIGN.md §3.1.2 and spec §5.6 and returns them
// concatenated, plus the partially-populated Header (Checksum left
// zero; the caller fills it in after computing xxh3).
func buildFooter(walk blockWalkResult, maxEventsPerBlock int, footerOffset int64) ([]byte, Header, error) {
	// 1. Block index.
	blockIndexBytes := encodeBlockIndex(walk.infos)

	// 2. Segment-level DID bloom.
	segmentBloom := gloom.New(uint64(len(walk.uniqueDIDs)), segmentBloomFPRate)
	for did := range walk.uniqueDIDs {
		segmentBloom.AddString(did)
	}
	segmentBloomBytes, err := segmentBloom.MarshalBinary()
	if err != nil {
		return nil, Header{}, fmt.Errorf("segment: marshal segment bloom: %w", err)
	}

	// 3. Per-block DID blooms.
	perBlockFilters := make([]*gloom.Filter, len(walk.perBlockDIDs))
	for i, dids := range walk.perBlockDIDs {
		f := gloom.New(uint64(maxEventsPerBlock), perBlockBloomFPRate)
		for did := range dids {
			f.AddString(did)
		}
		perBlockFilters[i] = f
	}
	perBlockBloomsRegion, perBlockBloomSize, err := encodeBlockBloomsRegion(perBlockFilters)
	if err != nil {
		return nil, Header{}, err
	}
	_ = perBlockBloomSize // recorded inside the region header

	// 4. Collection block index.
	colIdx := CollectionIndex{
		StringTable:   walk.collectionStringTable,
		BlockBitmasks: walk.perBlockCollections,
	}
	collectionIndexBytes, err := encodeCollectionIndex(colIdx)
	if err != nil {
		return nil, Header{}, err
	}

	// Concatenate in spec order. Track section offsets so the header
	// can record absolute file positions for each.
	footer := make([]byte, 0,
		len(blockIndexBytes)+len(segmentBloomBytes)+
			len(perBlockBloomsRegion)+len(collectionIndexBytes))

	blockIndexOffset := uint64(footerOffset)
	footer = append(footer, blockIndexBytes...)
	didBloomOffset := blockIndexOffset + uint64(len(blockIndexBytes))
	footer = append(footer, segmentBloomBytes...)
	blockDIDBloomOffset := didBloomOffset + uint64(len(segmentBloomBytes))
	footer = append(footer, perBlockBloomsRegion...)
	collectionIndexOffset := blockDIDBloomOffset + uint64(len(perBlockBloomsRegion))
	footer = append(footer, collectionIndexBytes...)

	header := Header{
		Version:               currentHeaderVersion,
		BlockCount:            uint32(len(walk.infos)),
		EventCount:            walk.totalEventCount,
		UniqueDIDCount:        uint32(len(walk.uniqueDIDs)),
		MinSeq:                walk.minSeq,
		MaxSeq:                walk.maxSeq,
		MinIndexedAt:          walk.minIndexedAt,
		MaxIndexedAt:          walk.maxIndexedAt,
		FooterOffset:          uint64(footerOffset),
		DIDBloomOffset:        didBloomOffset,
		BlockDIDBloomOffset:   blockDIDBloomOffset,
		CollectionIndexOffset: collectionIndexOffset,
		BlockIndexOffset:      blockIndexOffset,
		// Checksum: filled in by caller after xxh3.
	}
	return footer, header, nil
}

// sortUint32 sorts in ascending order. Pulled out as a tiny helper
// because the std slices.Sort generic instantiation is overkill for a
// single use here, and a hand-coded insertion sort is fine for the
// small N we see (~tens of collections per block).
func sortUint32(s []uint32) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}

// osStat is a function variable so tests can substitute. Real builds
// use os.Stat.
var osStat = osStatReal
```

At the bottom of the file (or in a small block after the existing imports), add:

```go
// osStatReal is the production implementation. We split it out
// behind a variable so seal_test.go can assert on FileSize without
// wiring through the real fs.
func osStatReal(name string) (osFileInfo, error) {
	info, err := os.Stat(name)
	if err != nil {
		return nil, err
	}
	return info, nil
}

// osFileInfo is a small interface so the var-of-fn pattern doesn't
// require importing os in seal_test.go.
type osFileInfo interface {
	Size() int64
}
```

Remove the `var _ = os.O_RDONLY` line that was added in Task 9.

- [ ] **Step 2: Run tests**

Run:
```bash
just test ./segment
```

Expected: `TestSealOnEmptyWriterProducesValidFile` PASSES; the other Seal tests stay green.

- [ ] **Step 3: Add a happy-path roundtrip test**

Append to `segment/seal_test.go`:

```go
import (
	"encoding/binary"
	"os"
)

func TestSealRoundtripSmallStream(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)

	events := []Event{
		{Seq: 1, IndexedAt: 100, Kind: KindCreate, DID: "did:plc:a",
			Collection: "app.bsky.feed.post", Rkey: "k1", Rev: "v1",
			Payload: []byte("p1")},
		{Seq: 2, IndexedAt: 200, Kind: KindCreate, DID: "did:plc:b",
			Collection: "app.bsky.feed.like", Rkey: "k2", Rev: "v2",
			Payload: []byte("p2")},
		{Seq: 3, IndexedAt: 300, Kind: KindUpdate, DID: "did:plc:a",
			Collection: "app.bsky.feed.post", Rkey: "k1", Rev: "v3",
			Payload: []byte("p3")},
	}
	for _, ev := range events {
		_, err := w.Append(ev)
		require.NoError(t, err)
	}

	res, err := w.Seal()
	require.NoError(t, err)
	require.EqualValues(t, 2, res.BlockCount)
	require.EqualValues(t, 3, res.EventCount)
	require.EqualValues(t, 2, res.UniqueDIDCount)
	require.EqualValues(t, 1, res.MinSeq)
	require.EqualValues(t, 3, res.MaxSeq)
	require.EqualValues(t, 100, res.MinIndexedAt)
	require.EqualValues(t, 300, res.MaxIndexedAt)
	require.NotZero(t, res.Checksum)

	// Verify the on-disk header reflects the same values.
	f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	headerBytes := make([]byte, reservedHeaderBytes)
	_, err = f.ReadAt(headerBytes, 0)
	require.NoError(t, err)
	require.Equal(t, segmentMagic, headerBytes[0:4])
	require.NotZero(t, binary.LittleEndian.Uint64(headerBytes[4:12]))
	require.EqualValues(t, 1,
		binary.LittleEndian.Uint16(headerBytes[12:14]))
	require.EqualValues(t, 2,
		binary.LittleEndian.Uint32(headerBytes[14:18]))
}
```

- [ ] **Step 4: Run tests, expect PASS**

Run:
```bash
just test ./segment -run TestSeal
```

Expected: PASS.

- [ ] **Step 5: Lint**

Run:
```bash
just lint
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add segment/seal.go segment/seal_test.go
git commit -m "segment: implement Seal walk-and-write happy path"
```

---

## Task 11: Sealed-file detection round-trips through Seal

**Files:**
- Modify: `segment/seal_test.go`

`TestNewRejectsSealedFile` (Task 8) used a hand-built fixture; now that Seal exists, add the round-trip test.

- [ ] **Step 1: Append the test**

Append to `segment/seal_test.go`:

```go
func TestNewRejectsRealSealedFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	_, err = w.Append(Event{Seq: 1, Kind: KindCreate, DID: "did:plc:a"})
	require.NoError(t, err)
	_, err = w.Seal()
	require.NoError(t, err)

	_, err = New(Config{Path: path})
	require.True(t, errors.Is(err, ErrSegmentSealed))
}
```

- [ ] **Step 2: Run tests, expect PASS**

Run:
```bash
just test ./segment -run TestNewRejectsRealSealedFile
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add segment/seal_test.go
git commit -m "segment: round-trip ErrSegmentSealed against real sealed file"
```

---

## Task 12: Crash-during-seal recovery tests

**Files:**
- Create: `segment/seal_recovery_test.go`

Three scenarios per the spec §6.6:
1. Footer durable, header still zero → `New()` truncates the partial footer via `lastGoodOffset`.
2. Partial footer write (some leading footer bytes) → same recovery.
3. Header WriteAt fails (simulated by closing the underlying fd) → Seal truncates footer back off, latches stickyErr, next `New()` resumes cleanly.

- [ ] **Step 1: Create the recovery test file**

Create `segment/seal_recovery_test.go`:

```go
package segment

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRecoveryFromCrashAfterFooterFsyncBeforeHeaderPwrite simulates a
// crash where the footer is durable but the header is still zero-
// filled. The first 8 bytes of the footer are the block index's
// first entry (a uint64 file offset interpreted as a length prefix
// by lastGoodOffset); that interpreted "length" overruns the file,
// so lastGoodOffset truncates the trailing bytes back off.
func TestRecoveryFromCrashAfterFooterFsyncBeforeHeaderPwrite(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	// Write and seal a normal segment so the file has a real footer.
	w, err := New(Config{Path: path, MaxEventsPerBlock: 4})
	require.NoError(t, err)
	for i := 1; i <= 3; i++ {
		_, err = w.Append(Event{
			Seq: uint64(i), Kind: KindCreate,
			DID: "did:plc:a", Collection: "app.bsky.feed.post",
		})
		require.NoError(t, err)
	}
	_, err = w.Seal()
	require.NoError(t, err)

	// Roll back the header to active-state by re-zeroing bytes 4..256.
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	require.NoError(t, err)
	zero := make([]byte, reservedHeaderBytes-4)
	_, err = f.WriteAt(zero, 4)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	preInfo, err := os.Stat(path)
	require.NoError(t, err)
	preSize := preInfo.Size()

	// Reopen — torn footer must be truncated.
	w2, err := New(Config{Path: path})
	require.NoError(t, err)
	require.NoError(t, w2.Close())

	postInfo, err := os.Stat(path)
	require.NoError(t, err)
	require.Less(t, postInfo.Size(), preSize,
		"reopen must shrink the file by truncating the orphaned footer")
}

// TestRecoveryFromPartialFooterWrite simulates a crash where some
// leading footer bytes were written but the footer fsync didn't
// complete. The torn-tail recovery path handles this identically to
// the full-footer case: lastGoodOffset can't parse the bytes as a
// frame and truncates them.
func TestRecoveryFromPartialFooterWrite(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 4})
	require.NoError(t, err)
	for i := 1; i <= 2; i++ {
		_, err = w.Append(Event{
			Seq: uint64(i), Kind: KindCreate, DID: "did:plc:a",
		})
		require.NoError(t, err)
	}
	require.NoError(t, w.Flush())
	require.NoError(t, w.Close())

	preInfo, err := os.Stat(path)
	require.NoError(t, err)
	preSize := preInfo.Size()

	// Append some plausible partial-footer bytes.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o644)
	require.NoError(t, err)
	_, err = f.Write(make([]byte, 256))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	w2, err := New(Config{Path: path})
	require.NoError(t, err)
	require.NoError(t, w2.Close())

	postInfo, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, preSize, postInfo.Size(),
		"recovery should truncate back to the pre-partial-footer size")
}

// TestSealHeaderWriteFailureTruncatesFooterBackOff covers the most
// subtle recovery path. We force the header pwrite to fail by
// closing the underlying file descriptor behind the writer's back
// after the footer is durable but before the header pwrite happens.
// Seal must explicitly truncate the footer so the file is restored
// to its pre-Seal state.
func TestSealHeaderWriteFailureTruncatesFooterBackOff(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 4})
	require.NoError(t, err)
	_, err = w.Append(Event{
		Seq: 1, Kind: KindCreate, DID: "did:plc:a",
		Collection: "app.bsky.feed.post", Rkey: "k", Rev: "v",
	})
	require.NoError(t, err)
	require.NoError(t, w.Flush())

	preInfo, err := os.Stat(path)
	require.NoError(t, err)
	preSize := preInfo.Size()

	// Force the next WriteAt to fail. Closing the fd makes both the
	// footer write *and* the header write fail. We're testing the
	// header-write-failure path specifically by checking that the
	// file is restored to preSize, but the footer write may have
	// failed first (in which case there's nothing to undo).
	require.NoError(t, w.file.Close())

	_, sealErr := w.Seal()
	require.Error(t, sealErr)

	// Whichever step failed, the file must be back to preSize: either
	// because the footer write failed first (nothing to undo) or
	// because the header write failed and Seal truncated the footer.
	postInfo, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, preSize, postInfo.Size(),
		"Seal must leave the file at pre-Seal size on any I/O failure")

	// A fresh Writer must reopen the file as active and seal cleanly.
	w2, err := New(Config{Path: path})
	require.NoError(t, err)
	res, err := w2.Seal()
	require.NoError(t, err)
	require.EqualValues(t, 1, res.EventCount)
	require.True(t, errors.Is(w2.Close(), nil) || w2.Close() == nil)
}
```

- [ ] **Step 2: Run tests, expect PASS**

Run:
```bash
just test ./segment -run TestRecovery
just test ./segment -run TestSealHeader
```

Expected: PASS.

- [ ] **Step 3: Confirm lint**

Run:
```bash
just lint
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add segment/seal_recovery_test.go
git commit -m "segment: test crash-during-seal recovery paths"
```

---

## Task 13: `Reader.Open` (parse + checksum)

**Files:**
- Create: `segment/reader.go`
- Create: `segment/reader_test.go`

This task ships `Open`, `Close`, `Header()`, `Blocks()`, `SegmentBloom()`, `Collections()`, `BlockCollections()`. Block decode and per-block bloom land in subsequent tasks so we can see the failure-mode test surface clearly.

- [ ] **Step 1: Write the failing tests**

Create `segment/reader_test.go`:

```go
package segment

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func sealedSegmentForReader(t *testing.T, dir string, events []Event, maxPerBlock int) string {
	t.Helper()
	path := filepath.Join(dir, "seg.jss")
	w, err := New(Config{Path: path, MaxEventsPerBlock: maxPerBlock})
	require.NoError(t, err)
	for _, ev := range events {
		_, err := w.Append(ev)
		require.NoError(t, err)
	}
	_, err = w.Seal()
	require.NoError(t, err)
	return path
}

func TestReaderOpenSucceeds(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	events := []Event{
		{Seq: 1, IndexedAt: 100, Kind: KindCreate, DID: "did:plc:a",
			Collection: "app.bsky.feed.post", Rkey: "k1", Rev: "v1"},
		{Seq: 2, IndexedAt: 200, Kind: KindCreate, DID: "did:plc:b",
			Collection: "app.bsky.feed.like", Rkey: "k2", Rev: "v2"},
	}
	path := sealedSegmentForReader(t, dir, events, 2)

	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	h := r.Header()
	require.EqualValues(t, 1, h.BlockCount)
	require.EqualValues(t, 2, h.EventCount)
	require.EqualValues(t, 2, h.UniqueDIDCount)

	infos := r.Blocks()
	require.Len(t, infos, 1)
	require.EqualValues(t, 2, infos[0].EventCount)

	bloom := r.SegmentBloom()
	require.NotNil(t, bloom)
	require.True(t, bloom.TestString("did:plc:a"))
	require.True(t, bloom.TestString("did:plc:b"))

	cols := r.Collections()
	require.ElementsMatch(t,
		[]string{"app.bsky.feed.post", "app.bsky.feed.like"}, cols)

	got0, err := r.BlockCollections(0)
	require.NoError(t, err)
	require.Len(t, got0, 2)
}

func TestReaderOpenChecksumMismatch(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := sealedSegmentForReader(t, dir,
		[]Event{{Seq: 1, Kind: KindCreate, DID: "did:plc:a"}}, 4)

	// Flip a single byte at the *very last* offset of the file (inside
	// the collection index zstd body). The xxh3 over (version..end-of-
	// footer) must change, but the active/sealed marker (offset 4..11)
	// is untouched so we still take the sealed-file Open path.
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	require.NoError(t, err)
	info, err := f.Stat()
	require.NoError(t, err)
	last := info.Size() - 1
	var b [1]byte
	_, err = f.ReadAt(b[:], last)
	require.NoError(t, err)
	b[0] ^= 0xFF
	_, err = f.WriteAt(b[:], last)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	_, err = Open(ReaderConfig{Path: path})
	require.True(t, errors.Is(err, ErrChecksumMismatch))
}

func TestReaderOpenSkipChecksum(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := sealedSegmentForReader(t, dir,
		[]Event{{Seq: 1, Kind: KindCreate, DID: "did:plc:a"}}, 4)

	// Same corruption as the previous test.
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	require.NoError(t, err)
	info, err := f.Stat()
	require.NoError(t, err)
	last := info.Size() - 1
	var b [1]byte
	_, err = f.ReadAt(b[:], last)
	require.NoError(t, err)
	b[0] ^= 0xFF
	_, err = f.WriteAt(b[:], last)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	r, err := Open(ReaderConfig{Path: path, SkipChecksum: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })
}

func TestReaderOpenRejectsActiveFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	// Create an active segment but don't seal it.
	w, err := New(Config{Path: path})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	_, err = Open(ReaderConfig{Path: path})
	require.True(t, errors.Is(err, ErrCorruptSegment))
}

func TestReaderOpenRejectsMissingPath(t *testing.T) {
	t.Parallel()

	_, err := Open(ReaderConfig{})
	require.True(t, errors.Is(err, ErrInvalidConfig))
}

func TestReaderCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := sealedSegmentForReader(t, dir,
		[]Event{{Seq: 1, Kind: KindCreate, DID: "did:plc:a"}}, 4)
	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	require.NoError(t, r.Close())
	require.NoError(t, r.Close())
}
```

- [ ] **Step 2: Run tests, expect FAIL**

Run:
```bash
just test ./segment -run TestReader
```

Expected: build failure: `undefined: Open`.

- [ ] **Step 3: Implement `reader.go`**

Create `segment/reader.go`:

```go
package segment

import (
	"fmt"
	"os"
	"sync/atomic"

	"github.com/jcalabro/gloom"
)

// ReaderConfig controls Reader.Open behavior. Path is required.
type ReaderConfig struct {
	// Path is the sealed segment file. Required.
	Path string

	// SkipChecksum disables the xxh3 verification performed by Open.
	// The default (false) computes xxh3 over (version..end-of-footer)
	// and compares against the value in the fixed header, returning
	// ErrChecksumMismatch on mismatch.
	//
	// Operators that have already verified the file via an out-of-
	// band mechanism (e.g., a checked SHA-256 from a CDN download)
	// may opt out to save the cost of re-hashing the metadata region.
	SkipChecksum bool
}

// Reader provides goroutine-safe read access to a sealed segment
// file. After Open, the file's metadata (header, block index,
// segment bloom, collection index) is parsed and held in memory;
// per-block decode and per-block-bloom load happen on demand via
// pread, so multiple goroutines may call DecodeBlock and BlockBloom
// concurrently with no shared mutable state.
//
// Close releases the file handle. It is idempotent.
type Reader struct {
	path string
	file *os.File

	header           Header
	blocks           []BlockInfo
	segmentBloom     *gloom.Filter
	collectionIndex  CollectionIndex
	perBlockBloomSize uint32

	closed atomic.Bool
}

// Open parses the sealed segment file at cfg.Path. On success, the
// returned Reader holds the parsed metadata in memory and an
// O_RDONLY file handle for on-demand block decode.
//
// Open performs at most four pread calls on the metadata region of
// the file: the fixed header, the block index, the segment-level
// bloom, and the collection-index header+body. The per-block-blooms
// region is not read at Open; BlockBloom preads on demand.
func Open(cfg ReaderConfig) (*Reader, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("%w: ReaderConfig.Path is required", ErrInvalidConfig)
	}

	f, err := os.OpenFile(cfg.Path, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("segment: open %s: %w", cfg.Path, err)
	}
	success := false
	defer func() {
		if !success {
			_ = f.Close()
		}
	}()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("segment: stat %s: %w", cfg.Path, err)
	}
	fileSize := info.Size()
	if fileSize < int64(reservedHeaderBytes) {
		return nil, fmt.Errorf("%w: %s is %d bytes",
			ErrCorruptSegment, cfg.Path, fileSize)
	}

	// 1. Fixed header.
	headerBytes := make([]byte, reservedHeaderBytes)
	if _, err := f.ReadAt(headerBytes, 0); err != nil {
		return nil, fmt.Errorf("segment: read header: %w", err)
	}
	header, err := decodeHeader(headerBytes)
	if err != nil {
		return nil, err
	}
	if err := validateHeaderOffsets(header, uint64(fileSize)); err != nil {
		return nil, err
	}

	// 2. Block index.
	blockIndexLen := int64(header.BlockCount) * blockIndexEntrySize
	blockIndexBytes := make([]byte, blockIndexLen)
	if blockIndexLen > 0 {
		if _, err := f.ReadAt(blockIndexBytes, int64(header.BlockIndexOffset)); err != nil {
			return nil, fmt.Errorf("segment: read block index: %w", err)
		}
	}
	blocks, err := decodeBlockIndex(blockIndexBytes, header.BlockCount)
	if err != nil {
		return nil, err
	}
	if err := validateBlockOffsets(blocks, header.FooterOffset); err != nil {
		return nil, err
	}

	// 3. Segment-level DID bloom.
	segmentBloomLen := int64(header.BlockDIDBloomOffset - header.DIDBloomOffset)
	segmentBloomBytes := make([]byte, segmentBloomLen)
	if segmentBloomLen > 0 {
		if _, err := f.ReadAt(segmentBloomBytes, int64(header.DIDBloomOffset)); err != nil {
			return nil, fmt.Errorf("segment: read segment bloom: %w", err)
		}
	}
	var segmentBloom *gloom.Filter
	if segmentBloomLen > 0 {
		segmentBloom, err = gloom.UnmarshalBinary(segmentBloomBytes)
		if err != nil {
			return nil, fmt.Errorf("segment: unmarshal segment bloom: %w", err)
		}
	}

	// 4. Per-block blooms region header (8 bytes).
	bloomRegionHeader := make([]byte, blockBloomsRegionHeaderSize)
	if _, err := f.ReadAt(bloomRegionHeader, int64(header.BlockDIDBloomOffset)); err != nil {
		return nil, fmt.Errorf("segment: read bloom region header: %w", err)
	}
	regionCount, perBlockSize, err := decodeBlockBloomsRegionHeader(bloomRegionHeader)
	if err != nil {
		return nil, err
	}
	if regionCount != header.BlockCount {
		return nil, fmt.Errorf("%w: bloom region count %d, header block_count %d",
			ErrInvalidFooter, regionCount, header.BlockCount)
	}

	// 5. Collection index.
	colHeader := make([]byte, collectionIndexHeaderSize)
	if _, err := f.ReadAt(colHeader, int64(header.CollectionIndexOffset)); err != nil {
		return nil, fmt.Errorf("segment: read collection index header: %w", err)
	}
	collectionLen := int64(fileSize) - int64(header.CollectionIndexOffset)
	if collectionLen < int64(collectionIndexHeaderSize) {
		return nil, fmt.Errorf("%w: collection index region too small", ErrInvalidFooter)
	}
	collectionBytes := make([]byte, collectionLen)
	if _, err := f.ReadAt(collectionBytes, int64(header.CollectionIndexOffset)); err != nil {
		return nil, fmt.Errorf("segment: read collection index: %w", err)
	}
	colIdx, err := decodeCollectionIndex(collectionBytes)
	if err != nil {
		return nil, err
	}

	// 6. Optional checksum verification.
	if !cfg.SkipChecksum {
		footerLen := fileSize - int64(header.FooterOffset)
		footerBytes := make([]byte, footerLen)
		if _, err := f.ReadAt(footerBytes, int64(header.FooterOffset)); err != nil {
			return nil, fmt.Errorf("segment: read footer for checksum: %w", err)
		}
		// Header bytes with the checksum field zeroed: that's how it
		// looked when seal computed xxh3.
		headerForHash := make([]byte, reservedHeaderBytes)
		copy(headerForHash, headerBytes)
		for i := 4; i < 12; i++ {
			headerForHash[i] = 0
		}
		got := xxh3HeaderFooter(headerForHash, footerBytes)
		if got != header.Checksum {
			return nil, fmt.Errorf("%w: computed=%x, header=%x",
				ErrChecksumMismatch, got, header.Checksum)
		}
	}

	r := &Reader{
		path:              cfg.Path,
		file:              f,
		header:            header,
		blocks:            blocks,
		segmentBloom:      segmentBloom,
		collectionIndex:   colIdx,
		perBlockBloomSize: perBlockSize,
	}
	success = true
	return r, nil
}

// Close releases the underlying file handle. Idempotent.
func (r *Reader) Close() error {
	if !r.closed.CompareAndSwap(false, true) {
		return nil
	}
	return r.file.Close()
}

// Header returns a copy of the parsed fixed header.
func (r *Reader) Header() Header { return r.header }

// Blocks returns a copy of the parsed block index. len == BlockCount.
func (r *Reader) Blocks() []BlockInfo {
	out := make([]BlockInfo, len(r.blocks))
	copy(out, r.blocks)
	return out
}

// SegmentBloom returns the segment-level DID bloom filter. Read-only.
// Callers must not mutate the returned filter.
func (r *Reader) SegmentBloom() *gloom.Filter { return r.segmentBloom }

// Collections returns the collection string table, indexed by NSID
// id. The returned slice is a fresh copy; mutation is harmless.
func (r *Reader) Collections() []string {
	out := make([]string, len(r.collectionIndex.StringTable))
	copy(out, r.collectionIndex.StringTable)
	return out
}

// BlockCollections returns the NSID ids present in the given block,
// sorted ascending.
func (r *Reader) BlockCollections(idx int) ([]uint32, error) {
	if idx < 0 || idx >= len(r.collectionIndex.BlockBitmasks) {
		return nil, fmt.Errorf("%w: idx %d, BlockCount %d",
			ErrBlockOutOfRange, idx, len(r.collectionIndex.BlockBitmasks))
	}
	src := r.collectionIndex.BlockBitmasks[idx]
	out := make([]uint32, len(src))
	copy(out, src)
	return out, nil
}

// validateHeaderOffsets checks that every offset in the parsed header
// fits within the file and that the section ordering matches the
// spec. Returns ErrInvalidFooter on any violation.
func validateHeaderOffsets(h Header, fileSize uint64) error {
	if h.FooterOffset < uint64(reservedHeaderBytes) {
		return fmt.Errorf("%w: footer_offset %d < reserved header",
			ErrInvalidFooter, h.FooterOffset)
	}
	if h.FooterOffset > fileSize {
		return fmt.Errorf("%w: footer_offset %d > file size %d",
			ErrInvalidFooter, h.FooterOffset, fileSize)
	}
	if h.BlockIndexOffset != h.FooterOffset {
		return fmt.Errorf("%w: block_index_offset %d != footer_offset %d",
			ErrInvalidFooter, h.BlockIndexOffset, h.FooterOffset)
	}
	if h.DIDBloomOffset < h.BlockIndexOffset ||
		h.BlockDIDBloomOffset < h.DIDBloomOffset ||
		h.CollectionIndexOffset < h.BlockDIDBloomOffset ||
		h.CollectionIndexOffset > fileSize {
		return fmt.Errorf("%w: footer section offsets out of order", ErrInvalidFooter)
	}
	return nil
}

// validateBlockOffsets verifies every block's [offset, offset+8+size]
// range fits before the footer.
func validateBlockOffsets(blocks []BlockInfo, footerOffset uint64) error {
	for i, b := range blocks {
		end := b.Offset + 8 + uint64(b.CompressedSize)
		if b.Offset < uint64(reservedHeaderBytes) || end > footerOffset {
			return fmt.Errorf(
				"%w: block %d range [%d, %d) outside [%d, %d)",
				ErrInvalidBlockIndex, i, b.Offset, end,
				reservedHeaderBytes, footerOffset)
		}
	}
	return nil
}
```

- [ ] **Step 4: Run tests, expect PASS**

Run:
```bash
just test ./segment -run TestReader
```

Expected: PASS for all six tests.

- [ ] **Step 5: Confirm full suite still passes**

Run:
```bash
just test ./segment
```

Expected: PASS.

- [ ] **Step 6: Lint**

Run:
```bash
just lint
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add segment/reader.go segment/reader_test.go
git commit -m "segment: add Reader.Open with footer parsing and checksum"
```

---

## Task 14: `Reader.DecodeBlock`

**Files:**
- Modify: `segment/reader.go`
- Modify: `segment/reader_test.go`

- [ ] **Step 1: Append the failing test**

Append to `segment/reader_test.go`:

```go
func TestReaderDecodeBlockReturnsEvents(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	events := []Event{
		{Seq: 1, IndexedAt: 100, Kind: KindCreate, DID: "did:plc:a",
			Collection: "app.bsky.feed.post", Rkey: "k1", Rev: "v1",
			Payload: []byte("p1")},
		{Seq: 2, IndexedAt: 200, Kind: KindCreate, DID: "did:plc:b",
			Collection: "app.bsky.feed.like", Rkey: "k2", Rev: "v2",
			Payload: []byte("p2")},
		{Seq: 3, IndexedAt: 300, Kind: KindUpdate, DID: "did:plc:a",
			Collection: "app.bsky.feed.post", Rkey: "k1", Rev: "v3",
			Payload: []byte("p3")},
	}
	path := sealedSegmentForReader(t, dir, events, 2)

	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	got0, err := r.DecodeBlock(0)
	require.NoError(t, err)
	require.Len(t, got0, 2)
	require.True(t, eventsEqual(events[0], got0[0]))
	require.True(t, eventsEqual(events[1], got0[1]))

	got1, err := r.DecodeBlock(1)
	require.NoError(t, err)
	require.Len(t, got1, 1)
	require.True(t, eventsEqual(events[2], got1[0]))

	_, err = r.DecodeBlock(2)
	require.True(t, errors.Is(err, ErrBlockOutOfRange))

	_, err = r.DecodeBlock(-1)
	require.True(t, errors.Is(err, ErrBlockOutOfRange))
}
```

- [ ] **Step 2: Run tests, expect FAIL**

Run:
```bash
just test ./segment -run TestReaderDecodeBlock
```

Expected: build failure: `Reader has no method DecodeBlock`.

- [ ] **Step 3: Implement `DecodeBlock` in `reader.go`**

Append to `segment/reader.go`:

```go
// DecodeBlock reads, decompresses, and decodes the block at the
// given index. Returns the decoded events in their stored order.
//
// Multiple goroutines may call DecodeBlock concurrently; each call
// is a fresh pread + decompress + decode with no shared mutable
// state.
func (r *Reader) DecodeBlock(idx int) ([]Event, error) {
	if idx < 0 || idx >= len(r.blocks) {
		return nil, fmt.Errorf("%w: idx %d, BlockCount %d",
			ErrBlockOutOfRange, idx, len(r.blocks))
	}
	b := r.blocks[idx]
	frame := make([]byte, b.CompressedSize)
	// The block index records the offset of the 8-byte length prefix;
	// the frame body starts 8 bytes later.
	if _, err := r.file.ReadAt(frame, int64(b.Offset)+8); err != nil {
		return nil, fmt.Errorf("segment: read block %d frame: %w", idx, err)
	}
	events, _, err := decodeBlockCompressedSized(frame)
	if err != nil {
		return nil, fmt.Errorf("segment: decode block %d: %w", idx, err)
	}
	return events, nil
}
```

- [ ] **Step 4: Run tests, expect PASS**

Run:
```bash
just test ./segment -run TestReaderDecodeBlock
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add segment/reader.go segment/reader_test.go
git commit -m "segment: add Reader.DecodeBlock"
```

---

## Task 15: `Reader.BlockBloom` + `LoadAllBlockBlooms`

**Files:**
- Modify: `segment/reader.go`
- Modify: `segment/reader_test.go`

- [ ] **Step 1: Append the failing tests**

Append to `segment/reader_test.go`:

```go
func TestReaderBlockBloom(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	events := []Event{
		{Seq: 1, Kind: KindCreate, DID: "did:plc:alice"},
		{Seq: 2, Kind: KindCreate, DID: "did:plc:bob"},
		{Seq: 3, Kind: KindCreate, DID: "did:plc:carol"},
		{Seq: 4, Kind: KindCreate, DID: "did:plc:dave"},
	}
	path := sealedSegmentForReader(t, dir, events, 2)

	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	b0, err := r.BlockBloom(0)
	require.NoError(t, err)
	require.True(t, b0.TestString("did:plc:alice"))
	require.True(t, b0.TestString("did:plc:bob"))

	b1, err := r.BlockBloom(1)
	require.NoError(t, err)
	require.True(t, b1.TestString("did:plc:carol"))
	require.True(t, b1.TestString("did:plc:dave"))

	_, err = r.BlockBloom(2)
	require.True(t, errors.Is(err, ErrBlockOutOfRange))

	_, err = r.BlockBloom(-1)
	require.True(t, errors.Is(err, ErrBlockOutOfRange))
}

func TestReaderLoadAllBlockBlooms(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	events := []Event{
		{Seq: 1, Kind: KindCreate, DID: "did:plc:a"},
		{Seq: 2, Kind: KindCreate, DID: "did:plc:b"},
		{Seq: 3, Kind: KindCreate, DID: "did:plc:c"},
	}
	path := sealedSegmentForReader(t, dir, events, 1)

	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	blooms, err := r.LoadAllBlockBlooms()
	require.NoError(t, err)
	require.Len(t, blooms, 3)

	require.True(t, blooms[0].TestString("did:plc:a"))
	require.True(t, blooms[1].TestString("did:plc:b"))
	require.True(t, blooms[2].TestString("did:plc:c"))

	// Cross-check: each filter equals the one BlockBloom returns.
	for i := range blooms {
		single, err := r.BlockBloom(i)
		require.NoError(t, err)
		require.Equal(t,
			blooms[i].TestString("did:plc:a"),
			single.TestString("did:plc:a"))
	}
}
```

- [ ] **Step 2: Run tests, expect FAIL**

Run:
```bash
just test ./segment -run TestReaderBlockBloom
just test ./segment -run TestReaderLoadAllBlockBlooms
```

Expected: build failure.

- [ ] **Step 3: Implement in `reader.go`**

Append:

```go
// BlockBloom reads and unmarshals the per-block DID bloom for the
// given block index. Each call performs one pread; there is no
// internal cache. Callers that want every block's bloom should
// prefer LoadAllBlockBlooms.
//
// Multiple goroutines may call BlockBloom concurrently.
func (r *Reader) BlockBloom(idx int) (*gloom.Filter, error) {
	if idx < 0 || idx >= len(r.blocks) {
		return nil, fmt.Errorf("%w: idx %d, BlockCount %d",
			ErrBlockOutOfRange, idx, len(r.blocks))
	}
	if r.perBlockBloomSize == 0 {
		// Empty segment (no blocks ever appended). The bounds check
		// above already short-circuits in that case, but keep the
		// guard explicit.
		return nil, fmt.Errorf("%w: no per-block blooms in this segment",
			ErrBlockOutOfRange)
	}
	off := int64(r.header.BlockDIDBloomOffset) +
		int64(blockBloomsRegionHeaderSize) +
		int64(idx)*int64(r.perBlockBloomSize)
	buf := make([]byte, r.perBlockBloomSize)
	if _, err := r.file.ReadAt(buf, off); err != nil {
		return nil, fmt.Errorf("segment: read block %d bloom: %w", idx, err)
	}
	f, err := gloom.UnmarshalBinary(buf)
	if err != nil {
		return nil, fmt.Errorf("segment: unmarshal block %d bloom: %w", idx, err)
	}
	return f, nil
}

// LoadAllBlockBlooms calls BlockBloom for every block in order.
// Equivalent to a hand-written loop; provided for callers that want
// a single call.
func (r *Reader) LoadAllBlockBlooms() ([]*gloom.Filter, error) {
	out := make([]*gloom.Filter, len(r.blocks))
	for i := range out {
		f, err := r.BlockBloom(i)
		if err != nil {
			return nil, err
		}
		out[i] = f
	}
	return out, nil
}
```

- [ ] **Step 4: Run tests, expect PASS**

Run:
```bash
just test ./segment -run TestReaderBlockBloom
just test ./segment -run TestReaderLoadAllBlockBlooms
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add segment/reader.go segment/reader_test.go
git commit -m "segment: add Reader.BlockBloom and LoadAllBlockBlooms"
```

---

## Task 16: Concurrent reader test

**Files:**
- Modify: `segment/reader_test.go`

- [ ] **Step 1: Append the test**

```go
import "sync"

func TestReaderConcurrentDecodeBlock(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	var events []Event
	for i := 1; i <= 64; i++ {
		events = append(events, Event{
			Seq: uint64(i), Kind: KindCreate,
			DID: "did:plc:" + string(rune('a'+(i%26))),
		})
	}
	path := sealedSegmentForReader(t, dir, events, 4)

	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	const goroutines = 10
	const itersPerGoroutine = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	errs := make(chan error, goroutines*itersPerGoroutine)

	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < itersPerGoroutine; i++ {
				idx := (g + i) % len(r.Blocks())
				if _, err := r.DecodeBlock(idx); err != nil {
					errs <- err
					return
				}
				if _, err := r.BlockBloom(idx); err != nil {
					errs <- err
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}
```

- [ ] **Step 2: Run under `-race`**

Run:
```bash
go test -race -count=1 -short ./segment -run TestReaderConcurrentDecodeBlock
```

Expected: PASS, no race output.

- [ ] **Step 3: Run the full segment package under -race**

Run:
```bash
go test -race -count=1 -short ./segment
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add segment/reader_test.go
git commit -m "segment: test Reader concurrent DecodeBlock and BlockBloom"
```

---

## Task 17: Roundtrip property test

**Files:**
- Modify: `segment/seal_test.go`

- [ ] **Step 1: Append the test**

```go
import "math/rand/v2"

func TestSealReaderRoundtripProperty(t *testing.T) {
	t.Parallel()

	iters := 30
	if !testing.Short() {
		iters = 500
	}

	r := rand.New(rand.NewPCG(42, 99))
	for it := range iters {
		dir := t.TempDir()
		path := filepath.Join(dir, "seg.jss")

		nEvents := 1 + r.IntN(50)
		maxPerBlock := 1 + r.IntN(8)

		w, err := New(Config{Path: path, MaxEventsPerBlock: maxPerBlock})
		require.NoErrorf(t, err, "iter %d", it)
		var events []Event
		for i := 0; i < nEvents; i++ {
			ev := Event{
				Seq:        uint64(i + 1),
				IndexedAt:  int64(it*1000 + i),
				Kind:       Kind(1 + r.IntN(6)),
				DID:        "did:plc:" + string(rune('a'+r.IntN(26))),
				Collection: "app.bsky.feed." + string(rune('a'+r.IntN(5))),
				Rkey:       "k" + string(rune('a'+r.IntN(26))),
				Rev:        "rev" + string(rune('a'+r.IntN(26))),
				Payload:    []byte{byte(r.IntN(256))},
			}
			events = append(events, ev)
			full, err := w.Append(ev)
			require.NoErrorf(t, err, "iter %d append %d", it, i)
			if full {
				require.NoError(t, w.Flush())
			}
		}

		res, err := w.Seal()
		require.NoErrorf(t, err, "iter %d seal", it)
		require.EqualValues(t, len(events), res.EventCount)

		rdr, err := Open(ReaderConfig{Path: path})
		require.NoErrorf(t, err, "iter %d open", it)

		// Verify segment-level bloom: every DID we put in must come back
		// true. (False positives are allowed; we don't assert on them.)
		for _, ev := range events {
			require.Truef(t, rdr.SegmentBloom().TestString(ev.DID),
				"iter %d DID %q missing from segment bloom", it, ev.DID)
		}

		// Walk every block via DecodeBlock and reassemble the stream.
		var got []Event
		for i := range rdr.Blocks() {
			evs, err := rdr.DecodeBlock(i)
			require.NoErrorf(t, err, "iter %d block %d", it, i)
			got = append(got, evs...)
		}
		require.Lenf(t, got, len(events), "iter %d", it)
		for i := range events {
			require.Truef(t, eventsEqual(events[i], got[i]),
				"iter %d event %d mismatch", it, i)
		}

		require.NoError(t, rdr.Close())
	}
}
```

- [ ] **Step 2: Run tests, expect PASS**

Run:
```bash
just test ./segment -run TestSealReaderRoundtripProperty
```

Expected: PASS, well under 1s.

- [ ] **Step 3: Run under `-long` to confirm full count is sane**

Run:
```bash
just test-long ./segment -run TestSealReaderRoundtripProperty
```

Expected: PASS, well under a few seconds.

- [ ] **Step 4: Commit**

```bash
git add segment/seal_test.go
git commit -m "segment: add Seal+Reader roundtrip property test"
```

---

## Task 18: Swarm test

**Files:**
- Create: `segment/seal_swarm_test.go`

- [ ] **Step 1: Create the swarm test**

Create `segment/seal_swarm_test.go`:

```go
package segment

import (
	"math/rand/v2"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSealSwarm explores feature-axis combinations to surface
// interactions the deterministic tests miss. Each iteration flips
// each axis with p=0.5 (forcing at least one if all roll false), then
// builds a sealed segment under the resulting profile and round-trips
// it through a Reader.
func TestSealSwarm(t *testing.T) {
	t.Parallel()

	iters := 30
	if !testing.Short() {
		iters = 1000
	}

	r := rand.New(rand.NewPCG(7, 11))

	for it := range iters {
		axes := struct {
			singleBlock      bool
			manyBlocks       bool
			allSameDID       bool
			heavyTailDIDs    bool
			singleCollection bool
			manyCollections  bool
			tinyPayloads     bool
			largePayloads    bool
		}{
			singleBlock:      r.Float64() < 0.5,
			manyBlocks:       r.Float64() < 0.5,
			allSameDID:       r.Float64() < 0.5,
			heavyTailDIDs:    r.Float64() < 0.5,
			singleCollection: r.Float64() < 0.5,
			manyCollections:  r.Float64() < 0.5,
			tinyPayloads:     r.Float64() < 0.5,
			largePayloads:    r.Float64() < 0.5,
		}

		// Force at least one axis on so we don't repeat the all-defaults
		// case (which the property test in Task 17 already covers).
		any := axes.singleBlock || axes.manyBlocks || axes.allSameDID ||
			axes.heavyTailDIDs || axes.singleCollection || axes.manyCollections ||
			axes.tinyPayloads || axes.largePayloads
		if !any {
			axes.manyBlocks = true
		}

		maxPerBlock := 4
		if axes.singleBlock {
			maxPerBlock = 64
		}
		nEvents := 4 + r.IntN(20)
		if axes.manyBlocks {
			nEvents = 16 + r.IntN(48)
			maxPerBlock = 2
		}

		dids := []string{"did:plc:a", "did:plc:b", "did:plc:c", "did:plc:d"}
		colls := []string{"a", "b", "c"}
		payloadSize := 4
		if axes.tinyPayloads {
			payloadSize = 1
		}
		if axes.largePayloads {
			payloadSize = 256
		}

		dir := t.TempDir()
		path := filepath.Join(dir, "seg.jss")

		w, err := New(Config{Path: path, MaxEventsPerBlock: maxPerBlock})
		require.NoErrorf(t, err, "iter %d", it)

		var events []Event
		for i := 0; i < nEvents; i++ {
			did := dids[r.IntN(len(dids))]
			if axes.allSameDID {
				did = dids[0]
			} else if axes.heavyTailDIDs {
				if r.Float64() < 0.9 {
					did = dids[0]
				}
			}
			coll := "app.bsky.feed." + colls[r.IntN(len(colls))]
			if axes.singleCollection {
				coll = "app.bsky.feed.post"
			} else if axes.manyCollections {
				coll = "app.bsky.feed." + string(rune('a'+r.IntN(20)))
			}
			payload := make([]byte, payloadSize)
			for j := range payload {
				payload[j] = byte(r.IntN(256))
			}
			ev := Event{
				Seq: uint64(i + 1), IndexedAt: int64(i),
				Kind: Kind(1 + r.IntN(6)),
				DID:  did, Collection: coll, Rkey: "k", Rev: "rev",
				Payload: payload,
			}
			events = append(events, ev)
			full, err := w.Append(ev)
			require.NoErrorf(t, err, "iter %d append %d", it, i)
			if full {
				require.NoError(t, w.Flush())
			}
		}

		_, err = w.Seal()
		require.NoErrorf(t, err, "iter %d seal", it)

		rdr, err := Open(ReaderConfig{Path: path})
		require.NoErrorf(t, err, "iter %d open", it)

		var got []Event
		for i := range rdr.Blocks() {
			evs, err := rdr.DecodeBlock(i)
			require.NoErrorf(t, err, "iter %d block %d", it, i)
			got = append(got, evs...)
		}
		require.Lenf(t, got, len(events), "iter %d", it)
		for i := range events {
			require.Truef(t, eventsEqual(events[i], got[i]),
				"iter %d event %d mismatch (axes=%+v)", it, i, axes)
		}

		require.NoError(t, rdr.Close())
	}
}
```

- [ ] **Step 2: Run under `-short`**

Run:
```bash
just test ./segment -run TestSealSwarm
```

Expected: PASS, sub-second.

- [ ] **Step 3: Run `-long`**

Run:
```bash
just test-long ./segment -run TestSealSwarm
```

Expected: PASS, a few seconds.

- [ ] **Step 4: Commit**

```bash
git add segment/seal_swarm_test.go
git commit -m "segment: add seal swarm test"
```

---

## Task 19: Fuzz tests

**Files:**
- Create: `segment/seal_fuzz_test.go`

- [ ] **Step 1: Create the fuzz file**

Create `segment/seal_fuzz_test.go`:

```go
package segment

import "testing"

// FuzzReadHeader ensures decodeHeader never panics or reads past the
// input regardless of how malformed the input is. It returns either
// a valid Header or a sentinel-wrapped error.
func FuzzReadHeader(f *testing.F) {
	// Seed with a few well-formed and obviously malformed inputs.
	good := encodeHeader(Header{
		Version:  1,
		Checksum: 0xDEADBEEF,
	})
	f.Add(good)
	f.Add(make([]byte, reservedHeaderBytes))
	f.Add([]byte{}) // truncated
	f.Add([]byte("jss0"))

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = decodeHeader(data)
	})
}

// FuzzReadBlockIndex covers decodeBlockIndex over arbitrary byte
// inputs paired with arbitrary count values. Output: valid []BlockInfo
// or an error.
func FuzzReadBlockIndex(f *testing.F) {
	f.Add([]byte{}, uint32(0))
	good := encodeBlockIndex([]BlockInfo{{Offset: 256, CompressedSize: 1, EventCount: 1}})
	f.Add(good, uint32(1))

	f.Fuzz(func(t *testing.T, data []byte, count uint32) {
		// Cap count so a hostile value can't drive an enormous
		// allocation in the fuzz target itself.
		if count > 1<<16 {
			t.Skip()
		}
		_, _ = decodeBlockIndex(data, count)
	})
}

// FuzzReadCollectionIndex covers decodeCollectionIndex.
func FuzzReadCollectionIndex(f *testing.F) {
	good, err := encodeCollectionIndex(CollectionIndex{
		StringTable:   []string{"app.bsky.feed.post"},
		BlockBitmasks: [][]uint32{{0}},
	})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(good)
	f.Add([]byte{})
	f.Add(make([]byte, collectionIndexHeaderSize))

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = decodeCollectionIndex(data)
	})
}
```

- [ ] **Step 2: Run the seed corpus**

Run:
```bash
just test ./segment -run "Fuzz"
```

Expected: PASS (seed corpus only — this verifies the fuzz targets compile and the seeds round-trip).

- [ ] **Step 3: Run a short fuzz pass for each**

Run:
```bash
go test -fuzz=FuzzReadHeader -fuzztime=2s ./segment
go test -fuzz=FuzzReadBlockIndex -fuzztime=2s ./segment
go test -fuzz=FuzzReadCollectionIndex -fuzztime=2s ./segment
```

Expected: each runs without panics, prints something like `fuzz: elapsed: 2s, execs: ...`.

- [ ] **Step 4: Commit**

```bash
git add segment/seal_fuzz_test.go
git commit -m "segment: add fuzz tests for header/blockindex/collection decoders"
```

---

## Task 20: Golden bytes for seal

**Files:**
- Modify: `segment/seal_test.go`
- Create: `segment/testdata/golden_seal.bin`

- [ ] **Step 1: Add the golden test (with -update support)**

Append to `segment/seal_test.go`:

```go
import "flag"

var updateGolden = flag.Bool("update", false, "update golden test fixtures")

// TestSealGolden pins the byte-exact output of a deterministic seal.
// Any accidental layout change breaks this test loudly. Regenerate
// the fixture with: go test -run TestSealGolden -update ./segment
func TestSealGolden(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)

	events := []Event{
		{Seq: 1, IndexedAt: 100, RenderedAt: 0, Kind: KindCreate,
			DID: "did:plc:a", Collection: "app.bsky.feed.post",
			Rkey: "k1", Rev: "v1", Payload: []byte("p1")},
		{Seq: 2, IndexedAt: 200, RenderedAt: 250, Kind: KindCreate,
			DID: "did:plc:b", Collection: "app.bsky.feed.like",
			Rkey: "k2", Rev: "v2", Payload: []byte("p2")},
	}
	for _, ev := range events {
		_, err := w.Append(ev)
		require.NoError(t, err)
	}
	_, err = w.Seal()
	require.NoError(t, err)

	got, err := os.ReadFile(path)
	require.NoError(t, err)

	goldenPath := filepath.Join("testdata", "golden_seal.bin")
	if *updateGolden {
		require.NoError(t, os.MkdirAll(filepath.Dir(goldenPath), 0o755))
		require.NoError(t, os.WriteFile(goldenPath, got, 0o644))
		t.Logf("updated %s (%d bytes)", goldenPath, len(got))
		return
	}
	want, err := os.ReadFile(goldenPath)
	require.NoErrorf(t, err, "missing golden file; run with -update")
	require.Equal(t, want, got)
}
```

- [ ] **Step 2: Generate the fixture**

Run:
```bash
go test -count=1 -run TestSealGolden -update ./segment
```

Expected: log message "updated segment/testdata/golden_seal.bin".

- [ ] **Step 3: Run without -update, expect PASS**

Run:
```bash
just test ./segment -run TestSealGolden
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add segment/seal_test.go segment/testdata/golden_seal.bin
git commit -m "segment: add golden-bytes pinning for sealed file format"
```

---

## Task 21: Benchmarks

**Files:**
- Create: `segment/seal_bench_test.go`

- [ ] **Step 1: Create the benchmark file**

Create `segment/seal_bench_test.go`:

```go
package segment

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func benchEvents(n int) []Event {
	out := make([]Event, n)
	for i := range out {
		out[i] = Event{
			Seq: uint64(i + 1), IndexedAt: int64(i),
			Kind: KindCreate,
			DID:  "did:plc:" + string(rune('a'+(i%26))),
			Collection: "app.bsky.feed.post",
			Rkey:       "k", Rev: "rev",
			Payload:    make([]byte, 512),
		}
	}
	return out
}

func BenchmarkSeal(b *testing.B) {
	events := benchEvents(4096)

	for i := 0; i < b.N; i++ {
		dir := b.TempDir()
		path := filepath.Join(dir, "seg.jss")
		w, err := New(Config{Path: path, MaxEventsPerBlock: 4096})
		require.NoError(b, err)
		for _, ev := range events {
			if _, err := w.Append(ev); err != nil {
				b.Fatal(err)
			}
		}
		if _, err := w.Seal(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReaderOpen(b *testing.B) {
	events := benchEvents(1024)
	dir := b.TempDir()
	path := filepath.Join(dir, "seg.jss")
	w, err := New(Config{Path: path, MaxEventsPerBlock: 256})
	require.NoError(b, err)
	for _, ev := range events {
		if _, err := w.Append(ev); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := w.Seal(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := Open(ReaderConfig{Path: path})
		if err != nil {
			b.Fatal(err)
		}
		_ = r.Close()
	}
}

func BenchmarkReaderOpenNoVerify(b *testing.B) {
	events := benchEvents(1024)
	dir := b.TempDir()
	path := filepath.Join(dir, "seg.jss")
	w, err := New(Config{Path: path, MaxEventsPerBlock: 256})
	require.NoError(b, err)
	for _, ev := range events {
		if _, err := w.Append(ev); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := w.Seal(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := Open(ReaderConfig{Path: path, SkipChecksum: true})
		if err != nil {
			b.Fatal(err)
		}
		_ = r.Close()
	}
}

func BenchmarkBlockBloom(b *testing.B) {
	events := benchEvents(1024)
	dir := b.TempDir()
	path := filepath.Join(dir, "seg.jss")
	w, err := New(Config{Path: path, MaxEventsPerBlock: 64})
	require.NoError(b, err)
	for _, ev := range events {
		if _, err := w.Append(ev); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := w.Seal(); err != nil {
		b.Fatal(err)
	}
	r, err := Open(ReaderConfig{Path: path})
	require.NoError(b, err)
	b.Cleanup(func() { _ = r.Close() })

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := r.BlockBloom(i % len(r.Blocks()))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeBlockSealed(b *testing.B) {
	events := benchEvents(4096)
	dir := b.TempDir()
	path := filepath.Join(dir, "seg.jss")
	w, err := New(Config{Path: path, MaxEventsPerBlock: 4096})
	require.NoError(b, err)
	for _, ev := range events {
		if _, err := w.Append(ev); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := w.Seal(); err != nil {
		b.Fatal(err)
	}
	r, err := Open(ReaderConfig{Path: path})
	require.NoError(b, err)
	b.Cleanup(func() { _ = r.Close() })

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := r.DecodeBlock(0)
		if err != nil {
			b.Fatal(err)
		}
	}
}
```

- [ ] **Step 2: Confirm benchmarks build and run briefly**

Run:
```bash
go test -bench=BenchmarkSeal -benchtime=1x -run=^$ ./segment
go test -bench=BenchmarkReaderOpen -benchtime=1x -run=^$ ./segment
go test -bench=BenchmarkBlockBloom -benchtime=1x -run=^$ ./segment
go test -bench=BenchmarkDecodeBlockSealed -benchtime=1x -run=^$ ./segment
```

Expected: each prints a benchmark result line; no errors.

- [ ] **Step 3: Commit**

```bash
git add segment/seal_bench_test.go
git commit -m "segment: add benchmarks for Seal, Reader.Open, BlockBloom, DecodeBlock"
```

---

## Task 22: Update package doc

**Files:**
- Modify: `segment/doc.go`

- [ ] **Step 1: Replace the package doc**

Open `segment/doc.go` and replace its contents with:

```go
// Package segment implements the jetstream segment file format: a
// columnar, zstd-compressed, length-prefixed binary log of atproto
// firehose events. Every active segment can be sealed into an
// immutable, self-describing file with a footer (block index,
// segment-level DID bloom, per-block DID blooms, collection block
// index) and a finalized 256-byte header carrying an xxh3 checksum
// over the metadata.
//
// The package is split by responsibility: event.go, block.go, and
// zstd.go define the row layout and block wire format; writer.go owns
// the active-segment file state machine (append, flush, fsync, seal);
// header.go, footer.go, bloom.go, and collection.go are pure
// encode/decode for footer sub-formats; seal.go orchestrates the
// seal walk-and-write pass; reader.go ships a goroutine-safe public
// Reader for sealed files.
//
// Writer is not safe for concurrent use; callers serialize access.
// Reader is safe for concurrent reads.
//
// The package contains no goroutines, timers, or context plumbing;
// lifecycle (time-based flushes, when to seal, graceful shutdown,
// metadata coupling) is the responsibility of the ingestion
// orchestrator that composes Writer with the rest of the system.
package segment
```

- [ ] **Step 2: Lint and test**

Run:
```bash
just lint
just test ./segment
```

Expected: PASS for both.

- [ ] **Step 3: Commit**

```bash
git add segment/doc.go
git commit -m "segment: refresh package doc to cover sealing and Reader"
```

---

## Task 23: Final verification

**Files:** none (verification only).

- [ ] **Step 1: Full lint**

Run:
```bash
just lint
```

Expected: PASS.

- [ ] **Step 2: Full short test (the budget gate)**

Run:
```bash
just test
```

Expected: PASS, total under 1s, segment package under ~500ms.

- [ ] **Step 3: Full long test (1000-iter swarm)**

Run:
```bash
just test-long
```

Expected: PASS, total under ~30s.

- [ ] **Step 4: Race detector**

Run:
```bash
just test-race
```

Expected: PASS, no race output.

- [ ] **Step 5: Confirm no kaizen comments left referring to seal**

Run:
```bash
grep -rn "kaizen.*seal\|future.*seal\|future Seal\|future trailer" segment/
```

Expected: empty output (the original kaizen comments in `writer.go` and `writer_test.go` were replaced in Task 8).

- [ ] **Step 6: Spot-check the public API surface**

Run:
```bash
go doc ./segment | grep -E "^(func|type|var) " | sort
```

Expected output includes:

```
func New(cfg Config) (*Writer, error)
func Open(cfg ReaderConfig) (*Reader, error)
type BlockInfo struct{ ... }
type Config struct{ ... }
type Event struct{ ... }
type Header struct{ ... }
type Kind uint8
type Reader struct{ ... }
type ReaderConfig struct{ ... }
type SealResult struct{ ... }
type Writer struct{ ... }
var ErrBlockOutOfRange = errors.New(...)
var ErrBufferFull = errors.New(...)
var ErrChecksumMismatch = errors.New(...)
var ErrClosed = errors.New(...)
var ErrCorruptSegment = errors.New(...)
var ErrFieldTooLong = errors.New(...)
var ErrInvalidBlockIndex = errors.New(...)
var ErrInvalidConfig = errors.New(...)
var ErrInvalidFooter = errors.New(...)
var ErrInvalidKind = errors.New(...)
var ErrSegmentSealed = errors.New(...)
```

Plus the methods: `Writer.Append/Flush/Close/Seal/Pending/Cap`, `Reader.Header/Blocks/SegmentBloom/BlockBloom/LoadAllBlockBlooms/Collections/BlockCollections/DecodeBlock/Close`.

- [ ] **Step 7: No commit needed**

This task is a verification gate. If anything fails, address it via a follow-up task and re-run the gates.

---

## Self-Review Checklist

Done by the plan author after writing; engineers executing the plan can skip this section.

- [x] Spec coverage: every public symbol in spec §4 has a task that produces it. `Writer.Seal` (Task 9-10), Reader public surface (Tasks 13-15), `Header`/`BlockInfo`/`SealResult` types (Tasks 3, 4, 9), all four new errors (Task 2), per-block bloom region encode/decode (Task 5), collection index encode/decode (Task 6).
- [x] Format: header layout (Task 3), block index (Task 4), per-block blooms region (Task 5), collection index (Task 6), all match spec §5.
- [x] Seal pass details: walk → build footer → compute xxh3 → write footer → patch header → fsync → close (Tasks 9-10). Recovery for header-write-fail truncates the footer back off (Task 10, lines in `sealAfterFlush`).
- [x] Sealed-vs-active detection (Task 8) and `New()` `ErrSegmentSealed` round-trip via real Seal (Task 11).
- [x] Crash-during-seal recovery tests (Task 12) cover all three cases from spec §6.6.
- [x] Reader concurrency: `os.File.ReadAt` based (Task 13 implementation), `-race` test (Task 16).
- [x] Tests stay sub-second under `-short` per the AGENTS.md budget (Task 23 step 2).
- [x] Fuzz tests don't run unbounded under `just test` (Task 19 — fuzz seed corpus runs in microseconds; `-fuzztime` is only used when invoked explicitly).
- [x] Every code step shows the actual code, not "fill in details". Every test shows the assertions. No "TBD"s.
- [x] Type and method names are consistent across tasks: `Header`, `BlockInfo`, `SealResult`, `Reader`, `ReaderConfig`, `CollectionIndex`, `Open`, `BlockBloom`, `LoadAllBlockBlooms`, `BlockCollections`, `DecodeBlock`.
- [x] Helpers introduced in early tasks (`decodeBlockCompressedSized`, `encodeBlockBloomsRegion`, `decodeBlockBloomsRegionHeader`, `encodeCollectionIndex`, `xxh3HeaderFooter`, `validateHeaderOffsets`, `validateBlockOffsets`) are referenced by later tasks under the same name.
