# `jetstream inspect-segment` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `jetstream inspect-segment <path>` subcommand that prints a single plain-text report describing a segment file's structure and contents — works on both sealed and active (unsealed) files.

**Architecture:** Refactor the existing seal-time frame walker out of `*Writer` into a free function. Add a public `segment.Inspect(path) (*Inspection, error)` that handles both file states (active: walk frames; sealed: lean on `segment.Reader` plus a separate xxh3 verification). New CLI subcommand renders the `Inspection` value as a single fixed-width text report.

**Tech Stack:** Go 1.x, `urfave/cli/v3`, `stretchr/testify`, `zeebo/xxh3` (already vendored), the existing `segment` package.

---

## File Structure

| file | role |
|---|---|
| `segment/inspect.go` | new — public `Inspect` and `Inspection`, refactored free-function `walkActiveFrames` and `readFrameAt`, the active-file path |
| `segment/inspect_test.go` | new — sealed roundtrip, active walk, corrupt sealed file, torn tail |
| `segment/seal.go` | edit — `Writer.walkBlocks` becomes a wrapper around the free `walkActiveFrames`; `Writer.readFrameAt` becomes a wrapper around the free `readFrameAt` |
| `cmd/jetstream/inspect_segment.go` | new — `inspectSegmentCommand()` and the text renderer |
| `cmd/jetstream/inspect_segment_test.go` | new — CLI smoke test |
| `cmd/jetstream/main.go` | edit — register the new command in `newApp().Commands` |

---

## Task 1: Refactor `readFrameAt` off `*Writer`

The active-file path needs to read frames from a read-only `*os.File`, but the existing `Writer.readFrameAt` requires the writer's `*os.File`. Pull the function body into a free function so both `Writer` and the new `Inspect` path can call it.

**Files:**
- Modify: `segment/seal.go` (the `Writer.readFrameAt` method, currently at the end of the file)
- Test: existing `segment/seal_test.go` and friends already exercise this path; no new test needed for the refactor itself.

- [ ] **Step 1: Replace the `Writer.readFrameAt` method with a free function plus a thin wrapper**

In `segment/seal.go`, find this block at the end of the file:

```go
// readFrameAt reads a single [uint64 LE compressed_len][zstd frame]
// pair starting at fileOffset, bounded by maxOffset so a torn or
// hostile length prefix can't drive an unbounded allocation.
// Used by the seal walk.
func (w *Writer) readFrameAt(fileOffset, maxOffset int64) (frame []byte, frameLen int, err error) {
	var lenBuf [8]byte
	if _, err := w.file.ReadAt(lenBuf[:], fileOffset); err != nil {
		return nil, 0, fmt.Errorf("segment: read frame length at %d: %w",
			fileOffset, err)
	}
	frameSize := binary.LittleEndian.Uint64(lenBuf[:])
	// Reject frames that would extend past the (active-state) end of
	// the framed-block region. The writer's own resumeExistingSegment
	// path already truncates torn tails, so this should never fire on
	// a well-behaved file; treat a hit here as corruption.
	remaining := uint64(maxOffset - fileOffset - int64(len(lenBuf)))
	if frameSize > remaining {
		return nil, 0, fmt.Errorf(
			"%w: frame at %d claims %d bytes, only %d remain before footer",
			ErrCorruptSegment, fileOffset, frameSize, remaining)
	}
	frame = make([]byte, frameSize)
	if _, err := w.file.ReadAt(frame, fileOffset+8); err != nil {
		return nil, 0, fmt.Errorf("segment: read frame body at %d: %w",
			fileOffset+8, err)
	}
	return frame, len(lenBuf) + int(frameSize), nil
}
```

Replace it with:

```go
// readFrameAt reads a single [uint64 LE compressed_len][zstd frame]
// pair starting at fileOffset, bounded by maxOffset so a torn or
// hostile length prefix can't drive an unbounded allocation.
// Used by the seal walk and by Inspect's active-file path.
func readFrameAt(f io.ReaderAt, fileOffset, maxOffset int64) (frame []byte, frameLen int, err error) {
	var lenBuf [8]byte
	if _, err := f.ReadAt(lenBuf[:], fileOffset); err != nil {
		return nil, 0, fmt.Errorf("segment: read frame length at %d: %w",
			fileOffset, err)
	}
	frameSize := binary.LittleEndian.Uint64(lenBuf[:])
	// Reject frames that would extend past the active-state end of
	// the framed-block region. The writer's own resumeExistingSegment
	// path already truncates torn tails, so a hit on a well-behaved
	// file means corruption.
	remaining := uint64(maxOffset - fileOffset - int64(len(lenBuf)))
	if frameSize > remaining {
		return nil, 0, fmt.Errorf(
			"%w: frame at %d claims %d bytes, only %d remain before footer",
			ErrCorruptSegment, fileOffset, frameSize, remaining)
	}
	frame = make([]byte, frameSize)
	if _, err := f.ReadAt(frame, fileOffset+8); err != nil {
		return nil, 0, fmt.Errorf("segment: read frame body at %d: %w",
			fileOffset+8, err)
	}
	return frame, len(lenBuf) + int(frameSize), nil
}

// (no Writer.readFrameAt wrapper — walkBlocks calls the free function directly.)
```

Then update the one caller inside `walkBlocks` (about 20 lines up in the same file). Find:

```go
		frame, frameSize, err := w.readFrameAt(off, footerOffset)
```

Replace with:

```go
		frame, frameSize, err := readFrameAt(w.file, off, footerOffset)
```

Also add `io` to the import block at the top of `segment/seal.go`. The import group currently reads:

```go
import (
	"encoding/binary"
	"fmt"
	"math"
	"os"

	"github.com/jcalabro/gloom"
)
```

Update to:

```go
import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/jcalabro/gloom"
)
```

- [ ] **Step 2: Run the seal tests to confirm the refactor is semantics-preserving**

Run: `just test ./segment`
Expected: PASS. The seal tests already exercise the walk path (`TestSealRoundtripSmallStream`, etc.).

- [ ] **Step 3: Commit**

```bash
git add segment/seal.go
git commit -m "$(cat <<'EOF'
segment: lift readFrameAt off *Writer into a free function

Upcoming Inspect() path needs to walk frames against a read-only
*os.File. The function only ever needed io.ReaderAt; expose that.
EOF
)"
```

---

## Task 2: Refactor `walkBlocks` so the per-frame walk is a free function

Same goal as Task 1: the inspector needs to walk frames without owning a `*Writer`. We split the loop body into `walkActiveFrames` and leave `Writer.walkBlocks` as a one-line wrapper that supplies its `file` and `cfg.MaxEventsPerBlock`.

**Files:**
- Modify: `segment/seal.go` (the `walkBlocks` method, ~lines 205-314)

- [ ] **Step 1: Replace `Writer.walkBlocks` with a wrapper plus a free function**

In `segment/seal.go`, find the `walkBlocks` method (it begins with `func (w *Writer) walkBlocks(footerOffset int64) (blockWalkResult, error) {`).

Replace the whole method with:

```go
// walkBlocks walks the framed-block region of the active file from
// reservedHeaderBytes to footerOffset. Wrapper around walkActiveFrames
// so the seal path keeps its existing call shape.
func (w *Writer) walkBlocks(footerOffset int64) (blockWalkResult, error) {
	return walkActiveFrames(w.file, footerOffset)
}

// walkActiveFrames walks the framed-block region of a segment file
// from reservedHeaderBytes to maxOffset, decompressing each frame
// and gathering per-block stats. Used by Writer.Seal during sealing
// and by Inspect when reporting on an active (unsealed) file.
//
// On a torn tail (a frame whose length prefix points past maxOffset)
// the partial blockWalkResult accumulated up to that frame is
// returned alongside an ErrCorruptSegment-wrapped error so callers
// that want the partial result can recover it.
func walkActiveFrames(f io.ReaderAt, maxOffset int64) (blockWalkResult, error) {
	res := blockWalkResult{
		uniqueDIDs:         map[string]struct{}{},
		collectionIDByName: map[string]uint32{},
	}
	off := int64(reservedHeaderBytes)
	for off < maxOffset {
		frame, frameSize, err := readFrameAt(f, off, maxOffset)
		if err != nil {
			return res, err
		}

		events, uncompressedSize, err := decodeBlockCompressedSized(frame)
		if err != nil {
			return res, fmt.Errorf("segment: decode block at %d: %w",
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

			// DIDs may be empty for some kinds (Identity, Account,
			// Sync events sometimes carry no DID per atproto spec).
			// Skip empty DIDs from both bloom filters and the unique-
			// DID count: an empty bloom hit is meaningless, and the
			// uniqueDIDCount in the header should reflect real DIDs.
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
						return res, fmt.Errorf(
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
		sortUint32(ids)
		res.perBlockCollections = append(res.perBlockCollections, ids)

		res.totalEventCount += uint32(len(events))
		off += int64(frameSize)
	}
	return res, nil
}
```

Note the only behavioural change vs the old code: on `readFrameAt` error or `decodeBlockCompressedSized` error we now return `res` (partial) instead of a zero `blockWalkResult{}`. This is what gives the inspector a partial view on a torn tail. It does *not* change the seal path's behavior, because `Writer.Seal` returns immediately on any error — it never inspects `res` on the error path.

- [ ] **Step 2: Run the seal tests**

Run: `just test ./segment`
Expected: PASS. Seal tests still hit the same code path, including `seal_recovery_test.go` which exercises the corrupt-frame branch.

- [ ] **Step 3: Commit**

```bash
git add segment/seal.go
git commit -m "$(cat <<'EOF'
segment: lift walkBlocks loop into walkActiveFrames

Free function takes io.ReaderAt + a max-offset upper bound, so the
upcoming Inspect path can reuse the same wire-format decode for
active files without depending on a *Writer. Writer.walkBlocks is
now a one-line wrapper.

On torn-tail or decode error the function now returns the partial
walk result alongside the error, which Inspect uses to report on
salvageable bytes. The seal path still treats any error as terminal.
EOF
)"
```

---

## Task 3: Add `Inspection` type and `Inspect()` for sealed files

Sealed-file path lands first because it can lean on the existing `Reader.Open`. Active-file path comes next, on top of the same surface.

**Files:**
- Create: `segment/inspect.go`
- Test: `segment/inspect_test.go`

- [ ] **Step 1: Write the failing sealed-roundtrip test**

Create `segment/inspect_test.go`:

```go
package segment

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// makeSealedFixture builds a deterministic sealed segment file in dir
// and returns the path and the SealResult so tests can cross-check.
func makeSealedFixture(t *testing.T, dir string) (string, SealResult) {
	t.Helper()
	path := filepath.Join(dir, "seg.jss")
	w, err := New(Config{Path: path, MaxEventsPerBlock: 4})
	require.NoError(t, err)
	events := []Event{
		{Seq: 1, IndexedAt: 1_000_000, Kind: KindCreate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "a", Rev: "r1", Payload: []byte("p1")},
		{Seq: 2, IndexedAt: 1_000_001, Kind: KindCreate, DID: "did:plc:bob", Collection: "app.bsky.feed.post", Rkey: "b", Rev: "r2", Payload: []byte("p2")},
		{Seq: 3, IndexedAt: 1_000_002, Kind: KindCreate, DID: "did:plc:carol", Collection: "app.bsky.feed.like", Rkey: "c", Rev: "r3", Payload: []byte("p3")},
		{Seq: 4, IndexedAt: 1_000_003, Kind: KindUpdate, DID: "did:plc:alice", Collection: "app.bsky.feed.like", Rkey: "d", Rev: "r4", Payload: []byte("p4")},
		{Seq: 5, IndexedAt: 1_000_004, Kind: KindCreate, DID: "did:plc:dan", Collection: "app.bsky.graph.follow", Rkey: "e", Rev: "r5", Payload: []byte("p5")},
	}
	for _, ev := range events {
		_, err := w.Append(ev)
		require.NoError(t, err)
	}
	res, err := w.Seal()
	require.NoError(t, err)
	return path, res
}

func TestInspect_SealedRoundtrip(t *testing.T) {
	t.Parallel()

	path, sealRes := makeSealedFixture(t, t.TempDir())

	ins, err := Inspect(path)
	require.NoError(t, err)
	require.NotNil(t, ins)

	require.Equal(t, path, ins.Path)
	require.True(t, ins.Sealed)
	require.True(t, ins.ChecksumValid)
	require.Equal(t, sealRes.BlockCount, ins.Header.BlockCount)
	require.Equal(t, sealRes.EventCount, ins.Header.EventCount)
	require.Equal(t, sealRes.UniqueDIDCount, ins.Header.UniqueDIDCount)
	require.Equal(t, sealRes.MinSeq, ins.Header.MinSeq)
	require.Equal(t, sealRes.MaxSeq, ins.Header.MaxSeq)
	require.Equal(t, sealRes.Checksum, ins.Header.Checksum)
	require.Equal(t, sealRes.FileSize, ins.FileSize)

	require.EqualValues(t, sealRes.BlockCount, len(ins.Blocks))
	// Three distinct collections in the fixture:
	require.ElementsMatch(t,
		[]string{"app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.graph.follow"},
		ins.Collections)
	require.Len(t, ins.BlockCollections, len(ins.Blocks))

	require.EqualValues(t, sealRes.EventCount, ins.TotalEvents)
	require.NotZero(t, ins.BlockIndexBytes)
	require.NotZero(t, ins.SegmentBloomBytes)
	require.NotZero(t, ins.BlockBloomsBytes)
	require.NotZero(t, ins.CollectionIndexBytes)
	require.NotZero(t, ins.PerBlockBloomBytes)

	// Section sizes should sum (with the per-block-blooms region's
	// 8-byte sub-header) to the size of the on-disk footer.
	totalFooter := ins.BlockIndexBytes + ins.SegmentBloomBytes +
		ins.BlockBloomsBytes + ins.CollectionIndexBytes
	require.EqualValues(t, uint64(ins.FileSize)-ins.Header.FooterOffset, totalFooter)
}

func TestInspect_CorruptSealedReportsInvalidChecksum(t *testing.T) {
	t.Parallel()

	path, _ := makeSealedFixture(t, t.TempDir())

	// Flip a byte inside the segment-level bloom region so the metadata
	// stays structurally parseable but the xxh3 won't match.
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	// Pick a byte safely inside the bloom region; +4 keeps us off any
	// bloom-internal length prefix.
	ins0, err := Inspect(path)
	require.NoError(t, err)
	corruptOff := int64(ins0.Header.DIDBloomOffset) + 4
	var b [1]byte
	_, err = f.ReadAt(b[:], corruptOff)
	require.NoError(t, err)
	b[0] ^= 0xff
	_, err = f.WriteAt(b[:], corruptOff)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	ins, err := Inspect(path)
	require.NoError(t, err)
	require.True(t, ins.Sealed)
	require.False(t, ins.ChecksumValid)
	// Metadata still parsed:
	require.NotZero(t, ins.Header.BlockCount)
	require.Equal(t, len(ins.Blocks), int(ins.Header.BlockCount))
}

func TestInspect_NotASegmentFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "garbage.jss")
	require.NoError(t, os.WriteFile(path, []byte("not-a-segment-file"), 0o644))
	_, err := Inspect(path)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrCorruptSegment))
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `just test ./segment -run TestInspect`
Expected: FAIL with "undefined: Inspect" (compile error).

- [ ] **Step 3: Implement `Inspect` and `Inspection` for the sealed path**

Create `segment/inspect.go`:

```go
// Package segment — Inspection surface used by the inspect-segment
// CLI. Active-file support lives in this same file; both paths
// produce the same Inspection value so the renderer is one code
// path.
package segment

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

// Inspection is the unified active+sealed view of a segment file
// produced by Inspect. All offset/size fields are zero where they
// don't apply (e.g. footer-section sizes on an active file).
type Inspection struct {
	Path     string
	FileSize int64
	Sealed   bool

	// Header is fully populated when Sealed. For active files the
	// fields are zero; the ones that are still meaningful (block
	// counts, seq/indexed_at ranges, etc.) live on the dedicated
	// fields below.
	Header Header

	// Blocks is the per-block info. Sealed: parsed from the on-disk
	// block index (cheap). Active: built by walking framed blocks
	// (decompresses every block).
	Blocks []BlockInfo

	// Collections is the segment's NSID string table. For sealed
	// files this comes from the on-disk collection index. For active
	// files it's the table accumulated during the frame walk in
	// first-seen order (and is therefore stable as long as the writer
	// hasn't appended more events since the inspect started).
	Collections []string
	// BlockCollections[i] is the sorted collection IDs in block i.
	BlockCollections [][]uint32

	// Aggregates derived during inspection. For sealed files these
	// match the corresponding Header fields; for active files they
	// come from the frame walk.
	TotalEvents    uint64
	UniqueDIDCount uint32
	MinSeq, MaxSeq uint64
	MinIndexedAt   int64
	MaxIndexedAt   int64

	// Footer-section sizes; zero for active files. The math is:
	//   BlockIndexBytes      = DIDBloomOffset       - BlockIndexOffset
	//   SegmentBloomBytes    = BlockDIDBloomOffset  - DIDBloomOffset
	//   BlockBloomsBytes     = CollectionIndexOffset - BlockDIDBloomOffset
	//   CollectionIndexBytes = FileSize             - CollectionIndexOffset
	BlockIndexBytes      uint64
	SegmentBloomBytes    uint64
	BlockBloomsBytes     uint64
	CollectionIndexBytes uint64

	// PerBlockBloomBytes is the size in bytes of each per-block
	// bloom filter. Zero for active files and for sealed files with
	// zero blocks.
	PerBlockBloomBytes uint32

	// ChecksumValid is true only when Sealed and the recomputed
	// xxh3 over header[12:]||footer matched the value in the header
	// checksum field. False on mismatch (the report still gets
	// produced — Inspect surfaces the mismatch rather than failing).
	ChecksumValid bool

	// PartialError is populated when the active-file frame walk hit
	// a torn tail or decode error. Inspection is still returned with
	// everything that could be parsed up to the failure; PartialError
	// carries the wrapped sentinel so callers can decide how to
	// surface it.
	PartialError error
}

// Inspect parses the segment file at path and returns a single
// snapshot suitable for the CLI text renderer. Inspect handles both
// sealed and active files and does its own checksum verification
// (rather than relying on Reader.Open's reject path) so corrupted-
// but-parsable files can still be inspected.
//
// Returns a non-nil error only when the file cannot be opened, is
// shorter than the 256-byte reserved header, or does not start with
// the 'jss0' magic. A torn tail in an active file is reported via
// Inspection.PartialError, not via the function return.
func Inspect(path string) (*Inspection, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("segment: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("segment: stat %s: %w", path, err)
	}
	fileSize := stat.Size()
	if fileSize < int64(reservedHeaderBytes) {
		return nil, fmt.Errorf("%w: %s is %d bytes (need >= %d for header)",
			ErrCorruptSegment, path, fileSize, reservedHeaderBytes)
	}

	headerBytes := make([]byte, reservedHeaderBytes)
	if _, err := f.ReadAt(headerBytes, 0); err != nil {
		return nil, fmt.Errorf("segment: read header: %w", err)
	}
	if string(headerBytes[0:4]) != string(segmentMagic) {
		return nil, fmt.Errorf("%w: bad magic %q (want %q)",
			ErrCorruptSegment, headerBytes[0:4], segmentMagic)
	}

	checksum := binary.LittleEndian.Uint64(headerBytes[4:12])
	if checksum == 0 {
		return inspectActive(path, f, fileSize)
	}
	return inspectSealed(path, fileSize, headerBytes)
}

func inspectSealed(path string, fileSize int64, headerBytes []byte) (*Inspection, error) {
	header, err := decodeHeader(headerBytes)
	if err != nil {
		return nil, err
	}

	// Open via the public Reader, but skip the checksum check there
	// so we can compute it ourselves and surface the result.
	r, err := Open(ReaderConfig{Path: path, SkipChecksum: true})
	if err != nil {
		return nil, err
	}
	defer func() { _ = r.Close() }()

	blocks := r.Blocks()
	collections := r.Collections()
	perBlockCollections := make([][]uint32, len(blocks))
	for i := range blocks {
		ids, err := r.BlockCollections(i)
		if err != nil {
			return nil, err
		}
		perBlockCollections[i] = ids
	}

	checksumValid, err := verifySealedChecksum(path, fileSize, headerBytes, header)
	if err != nil {
		return nil, err
	}

	blockIndexBytes := header.DIDBloomOffset - header.BlockIndexOffset
	segmentBloomBytes := header.BlockDIDBloomOffset - header.DIDBloomOffset
	blockBloomsBytes := header.CollectionIndexOffset - header.BlockDIDBloomOffset
	collectionIndexBytes := uint64(fileSize) - header.CollectionIndexOffset

	var perBlockBloomBytes uint32
	if header.BlockCount > 0 && blockBloomsBytes >= blockBloomsRegionHeaderSize {
		perBlockBloomBytes = uint32((blockBloomsBytes - blockBloomsRegionHeaderSize) / uint64(header.BlockCount))
	}

	return &Inspection{
		Path:                 path,
		FileSize:             fileSize,
		Sealed:               true,
		Header:               header,
		Blocks:               blocks,
		Collections:          collections,
		BlockCollections:     perBlockCollections,
		TotalEvents:          uint64(header.EventCount),
		UniqueDIDCount:       header.UniqueDIDCount,
		MinSeq:               header.MinSeq,
		MaxSeq:               header.MaxSeq,
		MinIndexedAt:         header.MinIndexedAt,
		MaxIndexedAt:         header.MaxIndexedAt,
		BlockIndexBytes:      blockIndexBytes,
		SegmentBloomBytes:    segmentBloomBytes,
		BlockBloomsBytes:     blockBloomsBytes,
		CollectionIndexBytes: collectionIndexBytes,
		PerBlockBloomBytes:   perBlockBloomBytes,
		ChecksumValid:        checksumValid,
	}, nil
}

// verifySealedChecksum recomputes the xxh3 over header[12:]||footer
// and reports whether it matches the value embedded in the header.
// Reads the footer via pread.
func verifySealedChecksum(path string, fileSize int64, headerBytes []byte, header Header) (bool, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return false, fmt.Errorf("segment: reopen for checksum: %w", err)
	}
	defer func() { _ = f.Close() }()

	footerLen := fileSize - int64(header.FooterOffset)
	if footerLen <= 0 {
		return false, fmt.Errorf("%w: footer length is %d", ErrInvalidFooter, footerLen)
	}
	footerBytes := make([]byte, footerLen)
	if _, err := f.ReadAt(footerBytes, int64(header.FooterOffset)); err != nil {
		return false, fmt.Errorf("segment: read footer for checksum: %w", err)
	}

	// Header bytes with the checksum field zeroed.
	headerForHash := make([]byte, reservedHeaderBytes)
	copy(headerForHash, headerBytes)
	for i := 4; i < 12; i++ {
		headerForHash[i] = 0
	}
	got := xxh3HeaderFooter(headerForHash, footerBytes)
	return got == header.Checksum, nil
}

// inspectActive walks the framed-block region of an active (unsealed)
// file. Returns a populated Inspection plus, on a torn tail or decode
// failure, a non-nil PartialError.
func inspectActive(path string, f *os.File, fileSize int64) (*Inspection, error) {
	walk, walkErr := walkActiveFrames(f, fileSize)
	// Translate walk into Inspection. We do this even on walkErr so
	// the partial result is reachable.
	ins := &Inspection{
		Path:             path,
		FileSize:         fileSize,
		Sealed:           false,
		Blocks:           walk.infos,
		Collections:      walk.collectionStringTable,
		BlockCollections: walk.perBlockCollections,
		TotalEvents:      uint64(walk.totalEventCount),
		UniqueDIDCount:   uint32(len(walk.uniqueDIDs)),
		MinSeq:           walk.minSeq,
		MaxSeq:           walk.maxSeq,
		MinIndexedAt:     walk.minIndexedAt,
		MaxIndexedAt:     walk.maxIndexedAt,
	}
	if walkErr != nil {
		// Distinguish "definitely corrupt" from "ran out of file mid-frame".
		// Both surface as ErrCorruptSegment via readFrameAt; downstream
		// callers don't currently need finer granularity.
		_ = errors.Is // keep errors imported for future expansion
		ins.PartialError = walkErr
	}
	return ins, nil
}
```

- [ ] **Step 4: Run the sealed tests to verify pass**

Run: `just test ./segment -run TestInspect`
Expected: PASS for `TestInspect_SealedRoundtrip`, `TestInspect_CorruptSealedReportsInvalidChecksum`, `TestInspect_NotASegmentFile`.

- [ ] **Step 5: Commit**

```bash
git add segment/inspect.go segment/inspect_test.go
git commit -m "$(cat <<'EOF'
segment: add Inspect() for sealed files

Inspect parses both metadata and footer-section sizes and surfaces
checksum mismatches via Inspection.ChecksumValid rather than
rejecting the file outright, so an operator can inspect a corrupted
segment to decide whether it's salvageable.
EOF
)"
```

---

## Task 4: Add active-file Inspect coverage

The `Inspect` code path for active files already landed in Task 3. Now we add the tests that exercise it.

**Files:**
- Modify: `segment/inspect_test.go`

- [ ] **Step 1: Write the failing active-file tests**

Append to `segment/inspect_test.go`:

```go
func TestInspect_ActiveFileWithBlocks(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "active.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	events := []Event{
		{Seq: 10, IndexedAt: 2_000_000, Kind: KindCreate, DID: "did:plc:x", Collection: "app.bsky.feed.post", Rkey: "1", Rev: "r"},
		{Seq: 11, IndexedAt: 2_000_001, Kind: KindCreate, DID: "did:plc:y", Collection: "app.bsky.feed.post", Rkey: "2", Rev: "r"},
		{Seq: 12, IndexedAt: 2_000_002, Kind: KindUpdate, DID: "did:plc:x", Collection: "app.bsky.feed.like", Rkey: "3", Rev: "r"},
		{Seq: 13, IndexedAt: 2_000_003, Kind: KindCreate, DID: "did:plc:z", Collection: "app.bsky.feed.like", Rkey: "4", Rev: "r"},
	}
	for _, ev := range events {
		_, err := w.Append(ev)
		require.NoError(t, err)
	}
	require.NoError(t, w.Flush())
	// Deliberately do NOT seal: the file is now an active segment with
	// two flushed blocks of size 2 each.

	ins, err := Inspect(path)
	require.NoError(t, err)
	require.False(t, ins.Sealed)
	require.False(t, ins.ChecksumValid) // active = no checksum
	require.Nil(t, ins.PartialError)
	require.EqualValues(t, 4, ins.TotalEvents)
	require.Len(t, ins.Blocks, 2)
	require.Equal(t, uint64(10), ins.MinSeq)
	require.Equal(t, uint64(13), ins.MaxSeq)
	require.ElementsMatch(t,
		[]string{"app.bsky.feed.post", "app.bsky.feed.like"},
		ins.Collections)
	require.Len(t, ins.BlockCollections, 2)
}

func TestInspect_ActiveFileEmpty(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.jss")

	w, err := New(Config{Path: path})
	require.NoError(t, err)
	require.NoError(t, w.Close()) // no events appended; file is just the 256B header

	ins, err := Inspect(path)
	require.NoError(t, err)
	require.False(t, ins.Sealed)
	require.Empty(t, ins.Blocks)
	require.EqualValues(t, 0, ins.TotalEvents)
}

func TestInspect_ActiveFileTornTailReportsPartialError(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "torn.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	for _, seq := range []uint64{1, 2} {
		_, err := w.Append(Event{Seq: seq, Kind: KindCreate, DID: "d", Collection: "c", Rkey: "r", Rev: "rv"})
		require.NoError(t, err)
	}
	require.NoError(t, w.Flush())
	require.NoError(t, w.Close())

	// Append 4 garbage bytes that look like the start of a length
	// prefix but truncate before its 8 bytes are complete. The frame
	// walker should see an unread length prefix and surface it as a
	// torn tail.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0)
	require.NoError(t, err)
	_, err = f.Write([]byte{0xff, 0xff, 0xff, 0xff})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	ins, err := Inspect(path)
	require.NoError(t, err) // partial errors are surfaced via PartialError, not return
	require.False(t, ins.Sealed)
	require.NotNil(t, ins.PartialError)
	// The first (clean) block must still be visible.
	require.GreaterOrEqual(t, len(ins.Blocks), 1)
}
```

- [ ] **Step 2: Run the tests**

Run: `just test ./segment -run TestInspect`
Expected: PASS. The active path was already implemented in Task 3; these tests verify it.

- [ ] **Step 3: Commit**

```bash
git add segment/inspect_test.go
git commit -m "$(cat <<'EOF'
segment: cover Inspect() active-file paths

Empty file, populated active file with two flushed blocks, and a
torn tail past the last clean frame.
EOF
)"
```

---

## Task 5: Render an `Inspection` as plain text

The renderer is pure: takes an `*Inspection`, a `--blocks` mode, and a `--blocks-truncate` value; writes to an `io.Writer`. Easy to test in isolation, no I/O dependency.

**Files:**
- Create: `cmd/jetstream/inspect_segment.go`
- Test: `cmd/jetstream/inspect_segment_test.go`

- [ ] **Step 1: Write the failing renderer test**

Create `cmd/jetstream/inspect_segment_test.go`:

```go
package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/stretchr/testify/require"
)

// makeSealedFixture builds a minimal sealed segment for the CLI tests.
func makeSealedFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	for _, seq := range []uint64{1, 2, 3, 4} {
		_, err := w.Append(segment.Event{
			Seq:        seq,
			IndexedAt:  int64(1_700_000_000_000_000) + int64(seq),
			Kind:       segment.KindCreate,
			DID:        "did:plc:demo",
			Collection: "app.bsky.feed.post",
			Rkey:       "r",
			Rev:        "v",
			Payload:    []byte("p"),
		})
		require.NoError(t, err)
	}
	_, err = w.Seal()
	require.NoError(t, err)
	return path
}

func TestRenderInspection_SealedTableMode(t *testing.T) {
	t.Parallel()

	path := makeSealedFixture(t)
	ins, err := segment.Inspect(path)
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, renderInspection(&buf, ins, "table", 100))

	out := buf.String()
	require.Contains(t, out, "state: sealed")
	require.Contains(t, out, "checksum:")
	require.Contains(t, out, "(valid)")
	require.Contains(t, out, "block_count:")
	require.Contains(t, out, "footer layout")
	require.Contains(t, out, "blocks (2 total)")
	require.Contains(t, out, "app.bsky.feed.post")
}

func TestRenderInspection_SummaryModeOmitsBlockTable(t *testing.T) {
	t.Parallel()

	path := makeSealedFixture(t)
	ins, err := segment.Inspect(path)
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, renderInspection(&buf, ins, "summary", 100))
	out := buf.String()
	require.Contains(t, out, "state: sealed")
	require.NotContains(t, out, "blocks (")
}

func TestRenderInspection_FullModeListsPerBlockCollections(t *testing.T) {
	t.Parallel()

	path := makeSealedFixture(t)
	ins, err := segment.Inspect(path)
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, renderInspection(&buf, ins, "full", 100))
	out := buf.String()
	require.Contains(t, out, "collections:")
	require.Contains(t, out, "app.bsky.feed.post")
}

func TestRenderInspection_CorruptChecksumLabelled(t *testing.T) {
	t.Parallel()

	path := makeSealedFixture(t)

	// Corrupt the segment-level bloom region.
	ins0, err := segment.Inspect(path)
	require.NoError(t, err)
	off := int64(ins0.Header.DIDBloomOffset) + 4
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	var b [1]byte
	_, err = f.ReadAt(b[:], off)
	require.NoError(t, err)
	b[0] ^= 0xff
	_, err = f.WriteAt(b[:], off)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	ins, err := segment.Inspect(path)
	require.NoError(t, err)
	require.False(t, ins.ChecksumValid)

	var buf bytes.Buffer
	require.NoError(t, renderInspection(&buf, ins, "table", 100))
	require.Contains(t, buf.String(), "(invalid)")
}

func TestRenderInspection_ActiveFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "active.jss")
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	for _, seq := range []uint64{1, 2} {
		_, err := w.Append(segment.Event{Seq: seq, Kind: segment.KindCreate, DID: "d", Collection: "c", Rkey: "r", Rev: "v"})
		require.NoError(t, err)
	}
	require.NoError(t, w.Flush())

	ins, err := segment.Inspect(path)
	require.NoError(t, err)
	var buf bytes.Buffer
	require.NoError(t, renderInspection(&buf, ins, "table", 100))

	out := buf.String()
	require.Contains(t, out, "state: active")
	require.Contains(t, out, "footer layout: not present (active file)")
}

func TestRenderInspection_BlockTruncation(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "many.jss")
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 1})
	require.NoError(t, err)
	for seq := uint64(1); seq <= 20; seq++ {
		_, err := w.Append(segment.Event{Seq: seq, Kind: segment.KindCreate, DID: "d", Collection: "c", Rkey: "r", Rev: "v"})
		require.NoError(t, err)
	}
	_, err = w.Seal()
	require.NoError(t, err)

	ins, err := segment.Inspect(path)
	require.NoError(t, err)
	require.EqualValues(t, 20, ins.Header.BlockCount)

	var buf bytes.Buffer
	require.NoError(t, renderInspection(&buf, ins, "table", 6))
	out := buf.String()
	require.Contains(t, out, "rows omitted")
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `just test ./cmd/jetstream -run TestRenderInspection`
Expected: FAIL with "undefined: renderInspection".

- [ ] **Step 3: Implement the renderer**

Create `cmd/jetstream/inspect_segment.go`:

```go
package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/urfave/cli/v3"
)

// inspectSegmentCommand wires up `jetstream inspect-segment <path>`.
//
// The command is a thin shell over segment.Inspect + renderInspection:
// all parsing and aggregation lives in the segment package; this layer
// only owns CLI flag wiring and the text renderer.
func inspectSegmentCommand() *cli.Command {
	return &cli.Command{
		Name:      "inspect-segment",
		Usage:     "Print a plain-text summary of a sealed or active segment file",
		ArgsUsage: "<path>",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "blocks",
				Usage: "Per-block detail level: summary | table | full",
				Value: "table",
			},
			&cli.IntFlag{
				Name:  "blocks-truncate",
				Usage: "Truncate the per-block table when block_count exceeds this many rows (0 = no truncation)",
				Value: 100,
			},
		},
		Action: func(_ context.Context, cmd *cli.Command) error {
			args := cmd.Args()
			if args.Len() != 1 {
				return fmt.Errorf("inspect-segment: expected exactly one path argument, got %d", args.Len())
			}
			path := args.First()

			mode := cmd.String("blocks")
			switch mode {
			case "summary", "table", "full":
			default:
				return fmt.Errorf("inspect-segment: --blocks must be one of summary|table|full, got %q", mode)
			}
			truncate := cmd.Int("blocks-truncate")
			if truncate < 0 {
				return fmt.Errorf("inspect-segment: --blocks-truncate must be >= 0, got %d", truncate)
			}

			ins, err := segment.Inspect(path)
			if err != nil {
				return err
			}
			return renderInspection(cmd.Root().Writer, ins, mode, int(truncate))
		},
	}
}

// renderInspection writes the human + LLM-pasteable text report for ins to w.
//
// Layout: header summary, footer layout, collections, blocks. Sections are
// blank-line separated. Numbers are decimal except absolute file offsets
// (always 0x-hex). Timestamps are RFC3339 micros in UTC.
func renderInspection(w io.Writer, ins *segment.Inspection, blocksMode string, blocksTruncate int) error {
	bw := &errWriter{w: w}

	bw.printf("file: %s\n", ins.Path)
	bw.printf("size: %d bytes\n", ins.FileSize)
	if ins.Sealed {
		bw.printf("state: sealed\n")
	} else {
		bw.printf("state: active (unsealed; frame walk)\n")
	}
	bw.printf("magic: jss0\n")
	if ins.Sealed {
		bw.printf("version: %d\n", ins.Header.Version)
		valid := "valid"
		if !ins.ChecksumValid {
			valid = "invalid"
		}
		bw.printf("checksum: 0x%016x (%s)\n", ins.Header.Checksum, valid)
	} else {
		bw.printf("version: -\n")
		bw.printf("checksum: 0x0 (active)\n")
	}

	if ins.PartialError != nil {
		bw.printf("\nWARNING: partial inspection — %v\n", ins.PartialError)
	}

	bw.printf("\nheader summary:\n")
	if ins.Sealed {
		bw.printf("  block_count:       %d\n", ins.Header.BlockCount)
		bw.printf("  event_count:       %d\n", ins.Header.EventCount)
		bw.printf("  unique_did_count:  %d\n", ins.Header.UniqueDIDCount)
	} else {
		bw.printf("  block_count:       %d (discovered via frame walk)\n", len(ins.Blocks))
		bw.printf("  event_count:       %d (from walk)\n", ins.TotalEvents)
		bw.printf("  unique_did_count:  %d (from walk; not durable until seal)\n", ins.UniqueDIDCount)
	}
	bw.printf("  seq range:         [%d, %d]\n", ins.MinSeq, ins.MaxSeq)
	bw.printf("  indexed_at range:  [%s, %s]\n",
		formatMicros(ins.MinIndexedAt), formatMicros(ins.MaxIndexedAt))

	bw.printf("\n")
	if ins.Sealed {
		bw.printf("footer layout (all offsets absolute; block_index_offset is also the footer start):\n")
		bw.printf("  block_index_offset:      0x%016x  block_index_size:       %d bytes\n",
			ins.Header.BlockIndexOffset, ins.BlockIndexBytes)
		bw.printf("  did_bloom_offset:        0x%016x  segment_bloom_size:     %d bytes\n",
			ins.Header.DIDBloomOffset, ins.SegmentBloomBytes)
		bw.printf("  block_did_bloom_offset:  0x%016x  per_block_blooms:       %d x %d bytes (incl. 8B region header)\n",
			ins.Header.BlockDIDBloomOffset, ins.Header.BlockCount, ins.PerBlockBloomBytes)
		bw.printf("  collection_index_offset: 0x%016x  collection_index_size:  %d bytes\n",
			ins.Header.CollectionIndexOffset, ins.CollectionIndexBytes)
		bw.printf("  end_of_file:             0x%016x\n", uint64(ins.FileSize))
	} else {
		bw.printf("footer layout: not present (active file)\n")
	}

	bw.printf("\ncollections (%d distinct NSIDs):\n", len(ins.Collections))
	if len(ins.Collections) == 0 {
		bw.printf("  (none)\n")
	} else {
		// Count appearances per collection across blocks for sealed files.
		counts := make([]int, len(ins.Collections))
		for _, ids := range ins.BlockCollections {
			for _, id := range ids {
				if int(id) < len(counts) {
					counts[id]++
				}
			}
		}
		nsidWidth := 0
		for _, n := range ins.Collections {
			if len(n) > nsidWidth {
				nsidWidth = len(n)
			}
		}
		for i, n := range ins.Collections {
			bw.printf("  [%3d] %-*s  blocks: %d\n", i, nsidWidth, n, counts[i])
		}
	}

	if blocksMode == "summary" {
		return bw.err
	}

	bw.printf("\nblocks (%d total):\n", len(ins.Blocks))
	bw.printf("  idx       offset  comp_size  uncomp_size  events     min_seq     max_seq  cols\n")

	emitRow := func(i int) {
		b := ins.Blocks[i]
		cols := 0
		if i < len(ins.BlockCollections) {
			cols = len(ins.BlockCollections[i])
		}
		bw.printf("  %3d  0x%010x  %9d  %11d  %6d  %10d  %10d  %4d\n",
			i, b.Offset, b.CompressedSize, b.UncompressedSize,
			b.EventCount, b.MinSeq, b.MaxSeq, cols)
		if blocksMode == "full" && i < len(ins.BlockCollections) && len(ins.BlockCollections[i]) > 0 {
			names := make([]string, 0, len(ins.BlockCollections[i]))
			for _, id := range ins.BlockCollections[i] {
				if int(id) < len(ins.Collections) {
					names = append(names, ins.Collections[id])
				}
			}
			bw.printf("       collections: %s\n", strings.Join(names, ", "))
		}
	}

	n := len(ins.Blocks)
	if blocksTruncate == 0 || blocksMode == "full" || n <= blocksTruncate {
		for i := range ins.Blocks {
			emitRow(i)
		}
	} else {
		half := blocksTruncate / 2
		for i := 0; i < half; i++ {
			emitRow(i)
		}
		bw.printf("  ... (%d rows omitted) ...\n", n-2*half)
		for i := n - half; i < n; i++ {
			emitRow(i)
		}
	}

	return bw.err
}

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
// of bw.printf calls without an `if err != nil { return err }` after
// every one. The first error is sticky; subsequent writes are dropped.
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

- [ ] **Step 4: Run the renderer tests**

Run: `just test ./cmd/jetstream -run TestRenderInspection`
Expected: PASS for all six tests.

- [ ] **Step 5: Commit**

```bash
git add cmd/jetstream/inspect_segment.go cmd/jetstream/inspect_segment_test.go
git commit -m "$(cat <<'EOF'
cmd/jetstream: render Inspection as plain text

Single fixed-width report covering header, footer layout,
collections, and per-block table. --blocks=summary|table|full
controls detail; --blocks-truncate caps the row count for big
segments.
EOF
)"
```

---

## Task 6: Wire `inspect-segment` into the root command

**Files:**
- Modify: `cmd/jetstream/main.go` (the `newApp` function, ~line 113)
- Test: `cmd/jetstream/main_test.go` (add a help-runs test)

- [ ] **Step 1: Write the failing test**

Append to `cmd/jetstream/main_test.go`:

```go
func TestNewApp_InspectSegmentHelpDoesNotError(t *testing.T) {
	t.Parallel()

	err := newApp().Run(t.Context(), []string{"jetstream", "inspect-segment", "--help"})
	require.NoError(t, err)
}

func TestNewApp_InspectSegmentRunsAgainstSealedFile(t *testing.T) {
	t.Parallel()

	path := makeSealedFixture(t)

	var buf bytes.Buffer
	app := newApp()
	app.Writer = &buf

	err := app.Run(t.Context(), []string{"jetstream", "inspect-segment", path})
	require.NoError(t, err)

	out := buf.String()
	require.Contains(t, out, "state: sealed")
	require.Contains(t, out, path)
}
```

(`makeSealedFixture` is already defined in `inspect_segment_test.go`, same package.)

- [ ] **Step 2: Run to verify failure**

Run: `just test ./cmd/jetstream -run TestNewApp_InspectSegment`
Expected: FAIL — "Command 'inspect-segment' not found".

- [ ] **Step 3: Register the command**

In `cmd/jetstream/main.go`, find:

```go
		Commands: []*cli.Command{
			serveCommand(),
			versionCommand(),
		},
```

Replace with:

```go
		Commands: []*cli.Command{
			serveCommand(),
			versionCommand(),
			inspectSegmentCommand(),
		},
```

- [ ] **Step 4: Run the tests**

Run: `just test ./cmd/jetstream`
Expected: PASS for the new tests and all existing ones.

- [ ] **Step 5: Run the full suite for confidence**

Run: `just`
Expected: lint clean + all tests pass.

- [ ] **Step 6: Commit**

```bash
git add cmd/jetstream/main.go cmd/jetstream/main_test.go
git commit -m "$(cat <<'EOF'
cmd/jetstream: register inspect-segment subcommand

Plain-text summary of a sealed or active segment file:
   jetstream inspect-segment ./data/segments/000123.jss
EOF
)"
```

---

## Task 7: End-to-end smoke test against a real binary

A final sanity check: build the binary and run it against a freshly-sealed segment in a tmp dir to make sure nothing went wrong in the wiring layer.

**Files:**
- Modify: `cmd/jetstream/inspect_segment_test.go` (add one test)

- [ ] **Step 1: Add an integration smoke test that invokes the built CLI shape**

Append to `cmd/jetstream/inspect_segment_test.go`:

```go
func TestInspectSegmentCommand_EndToEndAgainstRealFile(t *testing.T) {
	t.Parallel()

	path := makeSealedFixture(t)

	var buf bytes.Buffer
	app := newApp()
	app.Writer = &buf

	err := app.Run(t.Context(), []string{
		"jetstream", "inspect-segment",
		"--blocks=full",
		"--blocks-truncate=0",
		path,
	})
	require.NoError(t, err)

	out := buf.String()
	require.Contains(t, out, "state: sealed")
	require.Contains(t, out, "(valid)")
	require.Contains(t, out, "blocks (")
	require.Contains(t, out, "collections:")
}

func TestInspectSegmentCommand_RejectsBadFlag(t *testing.T) {
	t.Parallel()
	path := makeSealedFixture(t)

	app := newApp()
	app.Writer = new(bytes.Buffer)
	err := app.Run(t.Context(), []string{
		"jetstream", "inspect-segment", "--blocks=foo", path,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "--blocks")
}

func TestInspectSegmentCommand_RejectsMissingArg(t *testing.T) {
	t.Parallel()
	app := newApp()
	app.Writer = new(bytes.Buffer)
	err := app.Run(t.Context(), []string{"jetstream", "inspect-segment"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected exactly one path argument")
}
```

- [ ] **Step 2: Run the tests**

Run: `just test ./cmd/jetstream -run TestInspectSegmentCommand`
Expected: PASS.

- [ ] **Step 3: Run the lint + full suite one more time**

Run: `just`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add cmd/jetstream/inspect_segment_test.go
git commit -m "$(cat <<'EOF'
cmd/jetstream: end-to-end coverage for inspect-segment CLI

Sealed file with --blocks=full, bad flag rejection, missing arg.
EOF
)"
```

---

## Self-review

**1. Spec coverage:**
- "Active vs sealed detection on magic + checksum field" → Task 3 (`Inspect` dispatch on checksum == 0).
- "Refactor walkBlocks/readFrameAt out of *Writer" → Tasks 1, 2.
- "`Inspection` struct with all required fields" → Task 3 (struct), Task 3/4 (fields populated).
- "Sealed reader path with own checksum verification" → Task 3 (`verifySealedChecksum`).
- "Active path returns partial result on torn tail" → Task 3 (`inspectActive` carries `walkErr` into `PartialError`); Task 4 (test).
- "Plain-text renderer with header, footer, collections, blocks" → Task 5.
- "`--blocks` summary/table/full and `--blocks-truncate`" → Task 5 (renderer + flag handling).
- "Wired into newApp" → Task 6.
- "Sealed roundtrip / corrupt sealed / empty active / torn tail / CLI smoke tests" → Tasks 3, 4, 5, 6, 7.

**2. Placeholder scan:** No "TBD" or "implement later". Every code step has the actual code.

**3. Type consistency:** `Inspection`, `Inspect`, `inspectSealed`, `inspectActive`, `verifySealedChecksum`, `walkActiveFrames`, `readFrameAt`, `renderInspection`, `inspectSegmentCommand`, `formatMicros`, `errWriter` are all referenced consistently across tasks. `--blocks` values `summary|table|full` match between renderer impl, command flag validation, and tests. `Inspection.PartialError` is set in Task 3 and asserted in Task 4. `BlockIndexBytes` and friends are populated in Task 3 sealed path, declared in struct in Task 3.
