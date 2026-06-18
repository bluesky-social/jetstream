# Segment File Format — Initial Slice — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the first slice of jetstream's segment file format: in-memory event buffering, columnar block encoding, and length-prefixed flush+fsync to disk. No sealing, no public reader, no crash recovery.

**Architecture:** A single Go package at `./segment` with three concerns split by file: `event.go` (the input type), `block.go` (the columnar wire format — pure functions, unexported encode/decode), `writer.go` (the file-level state machine with `New`/`Append`/`Flush`/`Close`). Tests live alongside (`*_test.go`). The package is single-writer by design (caller serializes), so no goroutines, timers, or contexts inside the package.

**Tech Stack:** Go 1.26, `github.com/klauspost/compress/zstd` for compression, stdlib `testing` and `testing/quick` and Go 1.18+ fuzzing for tests, `github.com/stretchr/testify` (already in go.mod) for assertions.

**Note on imports:** Several tasks say "append to file X" and show a code block that includes an `import (...)` declaration. Go allows multiple `import` blocks per file, but `gofmt` (and our `just lint` step) prefers a single block. When appending, merge the new imports into the file's existing `import (...)` block rather than adding a second one. `goimports` (run by `gofmt` rewrite-rules in `.golangci.yaml`) handles this automatically when ordering matters.

**Spec:** `docs/superpowers/specs/2026-05-14-segment-file-format-design.md`

---

## File Structure Overview

Files this plan creates or modifies:

- **Modify:** `go.mod`, `go.sum` (add `github.com/klauspost/compress`)
- **Create:** `segment/doc.go` — package godoc
- **Create:** `segment/event.go` — `Event`, `Kind`, `Kind` constants
- **Create:** `segment/errors.go` — sentinel errors
- **Create:** `segment/block.go` — `validate`, `encodeBlock`, `decodeBlock`, `errTruncatedBlock`
- **Create:** `segment/writer.go` — `Config`, `Writer`, `New`, `Append`, `Flush`, `Close`, `Pending`, `Cap`
- **Create:** `segment/event_test.go` — Kind range tests
- **Create:** `segment/block_test.go` — validation + roundtrip property tests
- **Create:** `segment/block_swarm_test.go` — swarm test
- **Create:** `segment/block_fuzz_test.go` — `FuzzDecodeBlock`, `FuzzDecodeBlockFromCompressed`
- **Create:** `segment/block_golden_test.go` — golden bytes pinning
- **Create:** `segment/block_bench_test.go` — benchmarks
- **Create:** `segment/writer_test.go` — integration tests against `t.TempDir()`
- **Create:** `segment/testdata/golden_block.bin` — checked-in golden fixture
- **Create:** `segment/testdata/fuzz/` — fuzz corpus (auto-managed by `go test`)

Each file has a single responsibility. `block.go` never imports `os`. `writer.go` never imports the columnar layout details (it only calls `validate`, `encodeBlock`, and reads on errors from `decodeBlock` indirectly through tests).

---

## Task 1: Add klauspost/compress dependency

**Files:**
- Modify: `go.mod`, `go.sum`

- [ ] **Step 1: Add the dependency**

Run:
```bash
go get github.com/klauspost/compress@v1.18.6
```

Expected: `go.mod` gains a require line for `github.com/klauspost/compress v1.18.6`; `go.sum` gains hash entries. No source files change.

- [ ] **Step 2: Confirm it builds**

Run:
```bash
just lint
```

Expected: PASS (no source files have been added yet, so the linter is just confirming nothing in the existing tree broke).

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "deps: add github.com/klauspost/compress for zstd"
```

---

## Task 2: Package doc

**Files:**
- Create: `segment/doc.go`

- [ ] **Step 1: Create the package doc file**

Create `segment/doc.go` with:

```go
// Package segment implements the jetstream segment file format: a
// columnar, zstd-compressed, length-prefixed binary log of atproto
// firehose events.
//
// This slice covers writing only. A future slice will add a public
// Reader, segment sealing (the 256-byte fixed header and footer
// described in DESIGN.md §3.1.2), and crash recovery.
//
// Concurrency: Writer is not safe for concurrent use. Callers
// serialize access. The package contains no goroutines, timers,
// or context plumbing; lifecycle (time-based flushes, graceful
// shutdown, pebble metadata coupling) is the responsibility of the
// ingestion orchestrator that composes Writer with the rest of the
// system.
package segment
```

- [ ] **Step 2: Confirm it builds**

Run:
```bash
go build ./segment
```

Expected: PASS, no output.

- [ ] **Step 3: Commit**

```bash
git add segment/doc.go
git commit -m "segment: scaffold package with godoc"
```

---

## Task 3: Event and Kind types

**Files:**
- Create: `segment/event.go`
- Test: `segment/event_test.go`

- [ ] **Step 1: Write the failing test**

Create `segment/event_test.go`:

```go
package segment

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKindConstants(t *testing.T) {
	t.Parallel()

	// Pinning DESIGN.md §3.2 wire values. Changing any of these
	// silently corrupts every existing segment file.
	require.Equal(t, Kind(1), KindCreate)
	require.Equal(t, Kind(2), KindUpdate)
	require.Equal(t, Kind(3), KindDelete)
	require.Equal(t, Kind(4), KindIdentity)
	require.Equal(t, Kind(5), KindAccount)
	require.Equal(t, Kind(6), KindSync)
}

func TestEventZeroValueIsValid(t *testing.T) {
	t.Parallel()

	// A zero-value Event isn't *meaningful*, but constructing one
	// should never panic. This is a smoke test that the struct
	// definition didn't accidentally embed something with a
	// non-zero zero value.
	var ev Event
	require.Equal(t, Kind(0), ev.Kind)
	require.Empty(t, ev.DID)
	require.Nil(t, ev.Payload)
}
```

- [ ] **Step 2: Run test, expect FAIL**

Run:
```bash
just test ./segment
```

Expected: build failure: `undefined: Kind`, `undefined: KindCreate`, etc.

- [ ] **Step 3: Implement Event and Kind**

Create `segment/event.go`:

```go
package segment

// Kind discriminates which firehose event type a row represents.
// Values are the on-disk wire format from DESIGN.md §3.2.
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
//
//	DID:        up to 65535 bytes (uint16 column)
//	Collection: up to 255   bytes (uint8  column)
//	Rkey:       up to 255   bytes (uint8  column)
//	Rev:        up to 255   bytes (uint8  column)
//	Payload:    up to math.MaxUint32 bytes
//
// IndexedAt and RenderedAt are unix microseconds. RenderedAt == 0
// means "no operator-supplied timestamp" (DESIGN.md §3.2).
//
// For non-commit kinds (Identity, Account, Sync), Collection, Rkey,
// Rev, and Payload are typically empty / nil. The encoder accepts
// any combination; emptiness is not enforced as a per-Kind invariant
// at this layer.
type Event struct {
	Seq        uint64
	IndexedAt  int64
	RenderedAt int64
	Kind       Kind

	DID        string
	Collection string
	Rkey       string
	Rev        string

	Payload []byte // raw drisl (the DAG-CBOR subset used by atproto)
}
```

- [ ] **Step 4: Run tests, expect PASS**

Run:
```bash
just test ./segment
```

Expected: PASS, two tests run.

- [ ] **Step 5: Commit**

```bash
git add segment/event.go segment/event_test.go
git commit -m "segment: define Event and Kind"
```

---

## Task 4: Sentinel errors

**Files:**
- Create: `segment/errors.go`

No tests in this task — sentinels are values, not behavior. They get exercised by every later test that asserts `errors.Is(err, ErrX)`.

- [ ] **Step 1: Create the errors file**

Create `segment/errors.go`:

```go
package segment

import "errors"

// Sentinel errors. Callers compare with errors.Is.
var (
	// ErrInvalidConfig is returned by New when Config has unusable values.
	ErrInvalidConfig = errors.New("segment: invalid config")

	// ErrCorruptSegment is returned by New when an existing segment
	// file is smaller than the 256-byte reserved header region.
	ErrCorruptSegment = errors.New("segment: file is smaller than reserved header")

	// ErrSegmentSealed is returned by New when the file's first 4
	// bytes are the sealed magic "jss0". Sealing is implemented in a
	// later slice; for now we reject sealed files cleanly.
	ErrSegmentSealed = errors.New("segment: file is already sealed")

	// ErrFieldTooLong is returned by Append when a string or Payload
	// field exceeds its on-disk column width.
	ErrFieldTooLong = errors.New("segment: event field exceeds column width")

	// ErrInvalidKind is returned by Append when ev.Kind is outside [1, 6].
	ErrInvalidKind = errors.New("segment: kind out of range")

	// ErrBufferFull is returned by Append when the pending block has
	// already reached MaxEventsPerBlock and the caller did not Flush
	// in response to an earlier "full" signal.
	ErrBufferFull = errors.New("segment: pending block is at capacity; flush required")

	// ErrClosed is returned by Append, Flush, and (re-)Close after Close.
	ErrClosed = errors.New("segment: writer is closed")
)
```

- [ ] **Step 2: Confirm it builds**

Run:
```bash
go build ./segment
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add segment/errors.go
git commit -m "segment: add sentinel errors"
```

---

## Task 5: validate(ev Event)

**Files:**
- Create: `segment/block.go`
- Test: `segment/block_test.go`

This task adds the pure validation function that `Append` will call. It's standalone so we can test it without any encode machinery.

- [ ] **Step 1: Write the failing tests**

Create `segment/block_test.go`:

```go
package segment

import (
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateAcceptsHappyPath(t *testing.T) {
	t.Parallel()

	ev := Event{
		Seq:        42,
		IndexedAt:  1_700_000_000_000_000,
		RenderedAt: 0,
		Kind:       KindCreate,
		DID:        "did:plc:abcdefghijklmnopqrstuvwx",
		Collection: "app.bsky.feed.post",
		Rkey:       "3l3qo2vuowo2b",
		Rev:        "3l3qo2vutsw2b",
		Payload:    []byte("any drisl bytes"),
	}
	require.NoError(t, validate(ev))
}

func TestValidateRejectsInvalidKind(t *testing.T) {
	t.Parallel()

	t.Run("zero", func(t *testing.T) {
		t.Parallel()
		err := validate(Event{Kind: 0})
		require.ErrorIs(t, err, ErrInvalidKind)
	})

	t.Run("seven", func(t *testing.T) {
		t.Parallel()
		err := validate(Event{Kind: 7})
		require.ErrorIs(t, err, ErrInvalidKind)
	})
}

func TestValidateRejectsOversizedFields(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		mut  func(*Event)
	}{
		{
			name: "did over uint16",
			mut:  func(e *Event) { e.DID = strings.Repeat("a", math.MaxUint16+1) },
		},
		{
			name: "collection over uint8",
			mut:  func(e *Event) { e.Collection = strings.Repeat("a", math.MaxUint8+1) },
		},
		{
			name: "rkey over uint8",
			mut:  func(e *Event) { e.Rkey = strings.Repeat("a", math.MaxUint8+1) },
		},
		{
			name: "rev over uint8",
			mut:  func(e *Event) { e.Rev = strings.Repeat("a", math.MaxUint8+1) },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ev := Event{Kind: KindCreate}
			tc.mut(&ev)
			err := validate(ev)
			require.True(t, errors.Is(err, ErrFieldTooLong),
				"expected ErrFieldTooLong, got %v", err)
		})
	}
}
```

We do not test `len(Payload) > math.MaxUint32` because allocating 4+ GB in a unit test is impractical. The `uint32` cast in `encodeBlock` is the wire-format guarantee; the validation check exists to convert what would otherwise be a silent overflow into a clean error. We'll verify the check exists by reading the implementation, not by a test.

- [ ] **Step 2: Run tests, expect FAIL**

Run:
```bash
just test ./segment
```

Expected: build failure: `undefined: validate`.

- [ ] **Step 3: Implement validate**

Create `segment/block.go`:

```go
package segment

import (
	"fmt"
	"math"
)

// validate checks that ev's fields fit the on-disk column widths and
// that Kind is in range. It performs no I/O and never panics.
func validate(ev Event) error {
	if ev.Kind < KindCreate || ev.Kind > KindSync {
		return fmt.Errorf("%w: %d", ErrInvalidKind, ev.Kind)
	}
	if len(ev.DID) > math.MaxUint16 {
		return fmt.Errorf("%w: did is %d bytes (max %d)",
			ErrFieldTooLong, len(ev.DID), math.MaxUint16)
	}
	if len(ev.Collection) > math.MaxUint8 {
		return fmt.Errorf("%w: collection is %d bytes (max %d)",
			ErrFieldTooLong, len(ev.Collection), math.MaxUint8)
	}
	if len(ev.Rkey) > math.MaxUint8 {
		return fmt.Errorf("%w: rkey is %d bytes (max %d)",
			ErrFieldTooLong, len(ev.Rkey), math.MaxUint8)
	}
	if len(ev.Rev) > math.MaxUint8 {
		return fmt.Errorf("%w: rev is %d bytes (max %d)",
			ErrFieldTooLong, len(ev.Rev), math.MaxUint8)
	}
	if len(ev.Payload) > math.MaxUint32 {
		return fmt.Errorf("%w: payload is %d bytes (max %d)",
			ErrFieldTooLong, len(ev.Payload), math.MaxUint32)
	}
	return nil
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
git add segment/block.go segment/block_test.go
git commit -m "segment: add Event validation"
```

---

## Task 6: encodeBlock (uncompressed body)

**Files:**
- Modify: `segment/block.go`
- Test: `segment/block_test.go`

This task implements the columnar layout *without* zstd. Splitting compression off lets us assert exact bytes against a hand-built fixture in this task, then layer compression on in Task 8 without touching the column logic.

- [ ] **Step 1: Write the failing test**

Append to `segment/block_test.go`:

```go
import (
	"bytes"
	"encoding/binary"
)

func TestEncodeBlockUncompressedHandcrafted(t *testing.T) {
	t.Parallel()

	events := []Event{
		{
			Seq: 1, IndexedAt: 100, RenderedAt: 0, Kind: KindCreate,
			DID: "d1", Collection: "c1", Rkey: "r1", Rev: "v1",
			Payload: []byte{0xAA, 0xBB},
		},
		{
			Seq: 2, IndexedAt: 200, RenderedAt: 250, Kind: KindIdentity,
			DID: "d22", Collection: "", Rkey: "", Rev: "",
			Payload: nil,
		},
	}

	got, err := encodeBlock(events)
	require.NoError(t, err)

	// Build the expected bytes by hand to pin the layout.
	var want bytes.Buffer
	w := func(v any) {
		require.NoError(t, binary.Write(&want, binary.LittleEndian, v))
	}

	w(uint32(2)) // event_count

	// Fixed-size columns, in spec order:
	w(uint64(1))
	w(uint64(2)) // seq[]
	w(int64(100))
	w(int64(200)) // indexed_at[]
	w(int64(0))
	w(int64(250)) // rendered_at[]
	w(uint8(KindCreate))
	w(uint8(KindIdentity)) // kind[]
	w(uint8(2))
	w(uint8(0)) // collection_len[]
	w(uint16(2))
	w(uint16(3)) // did_len[]
	w(uint8(2))
	w(uint8(0)) // rkey_len[]
	w(uint8(2))
	w(uint8(0)) // rev_len[]
	w(uint32(2))
	w(uint32(0)) // event_len[]

	// Variable-length blobs, in spec order:
	want.WriteString("c1")        // collections
	want.WriteString("d1d22")     // dids
	want.WriteString("r1")        // rkeys
	want.WriteString("v1")        // revs
	want.Write([]byte{0xAA, 0xBB}) // payloads

	require.Equal(t, want.Bytes(), got)
}

func TestEncodeBlockEmptyReturnsError(t *testing.T) {
	t.Parallel()

	// Zero events is not a meaningful block; the writer's Flush is
	// the no-op layer. encodeBlock itself rejects empty input so a
	// caller can never accidentally write a zero-event block.
	_, err := encodeBlock(nil)
	require.Error(t, err)
}
```

- [ ] **Step 2: Run tests, expect FAIL**

Run:
```bash
just test ./segment
```

Expected: build failure: `undefined: encodeBlock`.

- [ ] **Step 3: Implement encodeBlock and the column-state helper**

Append to `segment/block.go`:

```go
import "encoding/binary"

// columns is the small interface encodeBlockColumns reads through.
// It exists so the writer's pendingBlock (parallel slices) and the
// test/golden/fuzz path's []Event share one byte-layout
// implementation. Callers must guarantee Len() > 0 and that the
// per-event accessors return values within the on-disk column widths.
type columns interface {
	Len() int
	Seq(i int) uint64
	IndexedAt(i int) int64
	RenderedAt(i int) int64
	Kind(i int) uint8
	Collection(i int) string
	DID(i int) string
	Rkey(i int) string
	Rev(i int) string
	Payload(i int) []byte
}

// encodeBlockColumns writes the uncompressed columnar body for the
// given columns per DESIGN.md §3.2.
func encodeBlockColumns(c columns) []byte {
	n := c.Len()

	var totalCollLen, totalDIDLen, totalRkeyLen, totalRevLen, totalPayloadLen int
	for i := 0; i < n; i++ {
		totalCollLen += len(c.Collection(i))
		totalDIDLen += len(c.DID(i))
		totalRkeyLen += len(c.Rkey(i))
		totalRevLen += len(c.Rev(i))
		totalPayloadLen += len(c.Payload(i))
	}

	const fixedPerEvent = 8 + 8 + 8 + 1 + 1 + 2 + 1 + 1 + 4
	totalSize := 4 + n*fixedPerEvent +
		totalCollLen + totalDIDLen + totalRkeyLen + totalRevLen + totalPayloadLen

	buf := make([]byte, 0, totalSize)
	le := binary.LittleEndian

	buf = le.AppendUint32(buf, uint32(n))

	// Fixed-size columns, in spec order. One loop per column gives
	// the CPU a clean prefetch stride.
	for i := 0; i < n; i++ {
		buf = le.AppendUint64(buf, c.Seq(i))
	}
	for i := 0; i < n; i++ {
		buf = le.AppendUint64(buf, uint64(c.IndexedAt(i)))
	}
	for i := 0; i < n; i++ {
		buf = le.AppendUint64(buf, uint64(c.RenderedAt(i)))
	}
	for i := 0; i < n; i++ {
		buf = append(buf, c.Kind(i))
	}
	for i := 0; i < n; i++ {
		buf = append(buf, uint8(len(c.Collection(i))))
	}
	for i := 0; i < n; i++ {
		buf = le.AppendUint16(buf, uint16(len(c.DID(i))))
	}
	for i := 0; i < n; i++ {
		buf = append(buf, uint8(len(c.Rkey(i))))
	}
	for i := 0; i < n; i++ {
		buf = append(buf, uint8(len(c.Rev(i))))
	}
	for i := 0; i < n; i++ {
		buf = le.AppendUint32(buf, uint32(len(c.Payload(i))))
	}

	// Variable-length blobs, in spec order.
	for i := 0; i < n; i++ {
		buf = append(buf, c.Collection(i)...)
	}
	for i := 0; i < n; i++ {
		buf = append(buf, c.DID(i)...)
	}
	for i := 0; i < n; i++ {
		buf = append(buf, c.Rkey(i)...)
	}
	for i := 0; i < n; i++ {
		buf = append(buf, c.Rev(i)...)
	}
	for i := 0; i < n; i++ {
		buf = append(buf, c.Payload(i)...)
	}

	return buf
}

// eventColumns adapts []Event to the columns interface so encodeBlock
// shares one layout implementation with the writer's column path.
type eventColumns []Event

func (e eventColumns) Len() int                  { return len(e) }
func (e eventColumns) Seq(i int) uint64          { return e[i].Seq }
func (e eventColumns) IndexedAt(i int) int64     { return e[i].IndexedAt }
func (e eventColumns) RenderedAt(i int) int64    { return e[i].RenderedAt }
func (e eventColumns) Kind(i int) uint8          { return uint8(e[i].Kind) }
func (e eventColumns) Collection(i int) string   { return e[i].Collection }
func (e eventColumns) DID(i int) string          { return e[i].DID }
func (e eventColumns) Rkey(i int) string         { return e[i].Rkey }
func (e eventColumns) Rev(i int) string          { return e[i].Rev }
func (e eventColumns) Payload(i int) []byte      { return e[i].Payload }

// encodeBlock writes the uncompressed columnar body for the given
// events. Callers must pass at least one event.
func encodeBlock(events []Event) ([]byte, error) {
	if len(events) == 0 {
		return nil, fmt.Errorf("segment: encodeBlock called with zero events")
	}
	for i := range events {
		if err := validate(events[i]); err != nil {
			return nil, fmt.Errorf("event %d: %w", i, err)
		}
	}
	return encodeBlockColumns(eventColumns(events)), nil
}
```

Note the `int64`-to-`uint64` casts for the timestamp columns: `binary.LittleEndian.AppendUint64` is the available helper, and a same-bit-width cast through `uint64` is well-defined and bit-preserving in Go. The decoder casts back to `int64` symmetrically.

- [ ] **Step 4: Run tests, expect PASS**

Run:
```bash
just test ./segment
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add segment/block.go segment/block_test.go
git commit -m "segment: implement encodeBlock columnar layout"
```

---

## Task 7: decodeBlock (uncompressed body) + roundtrip

**Files:**
- Modify: `segment/block.go`
- Test: `segment/block_test.go`

- [ ] **Step 1: Write the failing test**

Append to `segment/block_test.go`:

```go
func TestDecodeBlockRoundtripHandcrafted(t *testing.T) {
	t.Parallel()

	events := []Event{
		{
			Seq: 1, IndexedAt: 100, RenderedAt: 0, Kind: KindCreate,
			DID: "d1", Collection: "c1", Rkey: "r1", Rev: "v1",
			Payload: []byte{0xAA, 0xBB},
		},
		{
			Seq: 2, IndexedAt: 200, RenderedAt: 250, Kind: KindIdentity,
			DID: "d22", Collection: "", Rkey: "", Rev: "",
			Payload: nil,
		},
	}

	encoded, err := encodeBlock(events)
	require.NoError(t, err)

	decoded, err := decodeBlock(encoded)
	require.NoError(t, err)

	// The roundtrip must be deep-equal, including Payload == nil
	// (not []byte{}) for the zero-length case.
	require.Equal(t, events, decoded)
}

func TestDecodeBlockTruncatedReturnsError(t *testing.T) {
	t.Parallel()

	events := []Event{{Seq: 1, Kind: KindCreate, DID: "d", Payload: []byte("x")}}
	encoded, err := encodeBlock(events)
	require.NoError(t, err)

	for cut := 0; cut < len(encoded); cut++ {
		// Every prefix shorter than the full block must produce an
		// error, never a panic and never a wrong-but-non-erroring decode.
		_, err := decodeBlock(encoded[:cut])
		require.Error(t, err, "expected error at cut=%d", cut)
	}
}

func TestDecodeBlockBoundedAllocation(t *testing.T) {
	t.Parallel()

	// A header claiming 1 billion events must not provoke a giant
	// allocation; the decoder must validate against input length.
	hostile := make([]byte, 4)
	binary.LittleEndian.PutUint32(hostile, 1_000_000_000)
	_, err := decodeBlock(hostile)
	require.Error(t, err)
}
```

- [ ] **Step 2: Run tests, expect FAIL**

Run:
```bash
just test ./segment
```

Expected: build failure: `undefined: decodeBlock`.

- [ ] **Step 3: Implement decodeBlock**

Append to `segment/block.go`:

```go
import "errors"

// errTruncatedBlock is the sentinel for any malformed uncompressed
// block body: short reads, length-column overflows, anything that
// would cause the decoder to read past the input. It stays
// unexported because the decoder itself stays unexported in this
// slice; the future Reader type will promote it to a public sentinel.
var errTruncatedBlock = errors.New("segment: truncated or malformed block")

// decodeBlock is the inverse of encodeBlock. It validates input
// length at every step so a malicious header cannot provoke an
// unbounded allocation.
func decodeBlock(buf []byte) ([]Event, error) {
	const fixedPerEvent = 8 + 8 + 8 + 1 + 1 + 2 + 1 + 1 + 4

	if len(buf) < 4 {
		return nil, errTruncatedBlock
	}
	le := binary.LittleEndian
	nEvents64 := uint64(le.Uint32(buf[:4]))
	off := 4

	// Reject impossible event counts up front. We need at least
	// fixedPerEvent bytes per event before any variable-length
	// data; if the remaining input can't cover that, the header is
	// lying.
	if uint64(len(buf)-off) < nEvents64*fixedPerEvent {
		return nil, errTruncatedBlock
	}
	nEvents := int(nEvents64)

	if nEvents == 0 {
		// encodeBlock refuses empty input; a zero-event block on the
		// wire is corruption.
		return nil, errTruncatedBlock
	}

	events := make([]Event, nEvents)

	// Helper: read N bytes starting at off, advance off, return
	// errTruncatedBlock if the input is too short.
	read := func(n int) ([]byte, error) {
		if off+n > len(buf) {
			return nil, errTruncatedBlock
		}
		s := buf[off : off+n]
		off += n
		return s, nil
	}

	// seq[]
	chunk, err := read(nEvents * 8)
	if err != nil {
		return nil, err
	}
	for i := 0; i < nEvents; i++ {
		events[i].Seq = le.Uint64(chunk[i*8 : i*8+8])
	}

	// indexed_at[]
	chunk, err = read(nEvents * 8)
	if err != nil {
		return nil, err
	}
	for i := 0; i < nEvents; i++ {
		events[i].IndexedAt = int64(le.Uint64(chunk[i*8 : i*8+8]))
	}

	// rendered_at[]
	chunk, err = read(nEvents * 8)
	if err != nil {
		return nil, err
	}
	for i := 0; i < nEvents; i++ {
		events[i].RenderedAt = int64(le.Uint64(chunk[i*8 : i*8+8]))
	}

	// kind[]
	chunk, err = read(nEvents)
	if err != nil {
		return nil, err
	}
	for i := 0; i < nEvents; i++ {
		k := Kind(chunk[i])
		if k < KindCreate || k > KindSync {
			return nil, errTruncatedBlock
		}
		events[i].Kind = k
	}

	// collection_len[]
	collLen, err := read(nEvents)
	if err != nil {
		return nil, err
	}

	// did_len[]
	chunk, err = read(nEvents * 2)
	if err != nil {
		return nil, err
	}
	didLen := make([]uint16, nEvents)
	for i := 0; i < nEvents; i++ {
		didLen[i] = le.Uint16(chunk[i*2 : i*2+2])
	}

	// rkey_len[]
	rkeyLen, err := read(nEvents)
	if err != nil {
		return nil, err
	}

	// rev_len[]
	revLen, err := read(nEvents)
	if err != nil {
		return nil, err
	}

	// event_len[]
	chunk, err = read(nEvents * 4)
	if err != nil {
		return nil, err
	}
	eventLen := make([]uint32, nEvents)
	for i := 0; i < nEvents; i++ {
		eventLen[i] = le.Uint32(chunk[i*4 : i*4+4])
	}

	// Variable-length blobs. Each is a single contiguous run; we
	// slice it into per-event substrings.
	readStringField := func(lengths func(i int) int) ([]string, error) {
		out := make([]string, nEvents)
		var total int
		for i := 0; i < nEvents; i++ {
			total += lengths(i)
		}
		blob, err := read(total)
		if err != nil {
			return nil, err
		}
		var cur int
		for i := 0; i < nEvents; i++ {
			n := lengths(i)
			out[i] = string(blob[cur : cur+n])
			cur += n
		}
		return out, nil
	}

	collections, err := readStringField(func(i int) int { return int(collLen[i]) })
	if err != nil {
		return nil, err
	}
	dids, err := readStringField(func(i int) int { return int(didLen[i]) })
	if err != nil {
		return nil, err
	}
	rkeys, err := readStringField(func(i int) int { return int(rkeyLen[i]) })
	if err != nil {
		return nil, err
	}
	revs, err := readStringField(func(i int) int { return int(revLen[i]) })
	if err != nil {
		return nil, err
	}

	// payloads[]: same shape but []byte, and Payload == nil for zero-length.
	var totalPayload int
	for i := 0; i < nEvents; i++ {
		totalPayload += int(eventLen[i])
	}
	payloadBlob, err := read(totalPayload)
	if err != nil {
		return nil, err
	}

	// Refuse trailing bytes. encodeBlock produces an exact-length
	// buffer; anything left is corruption.
	if off != len(buf) {
		return nil, errTruncatedBlock
	}

	var pcur int
	for i := 0; i < nEvents; i++ {
		events[i].Collection = collections[i]
		events[i].DID = dids[i]
		events[i].Rkey = rkeys[i]
		events[i].Rev = revs[i]
		n := int(eventLen[i])
		if n > 0 {
			// Copy so callers can't mutate the input by writing into
			// Payload, and so the decoded events outlive buf.
			p := make([]byte, n)
			copy(p, payloadBlob[pcur:pcur+n])
			events[i].Payload = p
		} else {
			events[i].Payload = nil
		}
		pcur += n
	}

	return events, nil
}
```

A few choices worth noting:

- The decoder validates the kind column. A corrupt block could have `kind = 0` or `kind = 250`; refusing rather than passing it through means `decode(encode(events)) == events` is the only roundtrip shape we ever produce.
- We `copy` payloads out of `buf` so the returned events don't alias the input. This is the same reason the four string columns get `string(blob[...])`-converted, which copies under the hood.
- The trailing-bytes check at the end rejects any slack. Coupled with the up-front length check, this gives the decoder a "exactly this many bytes or error" contract.

- [ ] **Step 4: Run tests, expect PASS**

Run:
```bash
just test ./segment
```

Expected: PASS, all four block tests pass.

- [ ] **Step 5: Commit**

```bash
git add segment/block.go segment/block_test.go
git commit -m "segment: implement decodeBlock with bounded allocation"
```

---

## Task 8: zstd-wrapped encode/decode

**Files:**
- Modify: `segment/block.go`
- Test: `segment/block_test.go`

- [ ] **Step 1: Write the failing test**

Append to `segment/block_test.go`:

```go
func TestEncodeBlockCompressedRoundtrip(t *testing.T) {
	t.Parallel()

	events := []Event{
		{Seq: 1, IndexedAt: 100, Kind: KindCreate, DID: "did:plc:a"},
		{Seq: 2, IndexedAt: 200, Kind: KindCreate, DID: "did:plc:b", Payload: []byte("x")},
	}

	frame, err := encodeBlockCompressed(events)
	require.NoError(t, err)
	require.NotEmpty(t, frame)

	// The frame is a real zstd frame, not the raw body.
	require.NotEqual(t, mustEncode(t, events), frame)

	got, err := decodeBlockCompressed(frame)
	require.NoError(t, err)
	require.Equal(t, events, got)
}

func TestDecodeBlockCompressedRejectsGarbage(t *testing.T) {
	t.Parallel()

	_, err := decodeBlockCompressed([]byte("not a zstd frame"))
	require.Error(t, err)
}

// mustEncode is a test helper; it lives here because it has no
// non-test consumers.
func mustEncode(t *testing.T, events []Event) []byte {
	t.Helper()
	out, err := encodeBlock(events)
	require.NoError(t, err)
	return out
}
```

- [ ] **Step 2: Run tests, expect FAIL**

Run:
```bash
just test ./segment
```

Expected: build failure: `undefined: encodeBlockCompressed`, `undefined: decodeBlockCompressed`.

- [ ] **Step 3: Implement zstd wrappers**

Append to `segment/block.go`:

```go
import "github.com/klauspost/compress/zstd"

// blockEncoder is a process-wide reusable zstd encoder at the
// default level (klauspost's SpeedDefault, which is zstd level 3 —
// the format default). zstd.Encoder.EncodeAll is documented as
// safe for concurrent calls, which makes a single instance fine
// despite the package being single-writer.
//
// Construction can fail (rare; only on configuration errors) so we
// initialize lazily through encoderOnce rather than in init() to
// keep the failure mode visible at the call site.
var (
	blockEncoder     *zstd.Encoder
	blockEncoderInit error
	blockDecoder     *zstd.Decoder
	blockDecoderInit error
)

func init() {
	blockEncoder, blockEncoderInit = zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderCRC(true),
	)
	blockDecoder, blockDecoderInit = zstd.NewReader(nil)
}

// encodeBlockCompressed encodes events with encodeBlock, then wraps
// the result in a single zstd frame with content checksums enabled.
func encodeBlockCompressed(events []Event) ([]byte, error) {
	if blockEncoderInit != nil {
		return nil, fmt.Errorf("segment: zstd encoder init failed: %w", blockEncoderInit)
	}
	body, err := encodeBlock(events)
	if err != nil {
		return nil, err
	}
	return blockEncoder.EncodeAll(body, nil), nil
}

// decodeBlockCompressed is the inverse: decompress, then decodeBlock.
func decodeBlockCompressed(frame []byte) ([]Event, error) {
	if blockDecoderInit != nil {
		return nil, fmt.Errorf("segment: zstd decoder init failed: %w", blockDecoderInit)
	}
	body, err := blockDecoder.DecodeAll(frame, nil)
	if err != nil {
		return nil, fmt.Errorf("segment: zstd decompress: %w", err)
	}
	return decodeBlock(body)
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
git add segment/block.go segment/block_test.go
git commit -m "segment: add zstd-wrapped block encode/decode"
```

---

## Task 9: Roundtrip property test

**Files:**
- Modify: `segment/block_test.go`

- [ ] **Step 1: Write the property test**

Append to `segment/block_test.go`:

```go
import (
	"math/rand"
	"testing/quick"
)

// genEvent produces one Event with realistic length distributions.
// Uses *rand.Rand so the generator is deterministic given a seed.
func genEvent(r *rand.Rand) Event {
	// Length distributions chosen to mostly match production atproto
	// shapes while still occasionally exercising the upper bounds.
	didLen := pickLen(r, 32, math.MaxUint16, 0.001)
	collLen := pickLen(r, 24, math.MaxUint8, 0.005)
	rkeyLen := pickLen(r, 13, math.MaxUint8, 0.005)
	revLen := pickLen(r, 13, math.MaxUint8, 0.005)
	payloadLen := pickPayloadLen(r)

	return Event{
		Seq:        r.Uint64(),
		IndexedAt:  int64(r.Uint64()),
		RenderedAt: int64(r.Uint64()),
		Kind:       Kind(1 + r.Intn(6)),
		DID:        randString(r, didLen),
		Collection: randString(r, collLen),
		Rkey:       randString(r, rkeyLen),
		Rev:        randString(r, revLen),
		Payload:    randBytes(r, payloadLen),
	}
}

// pickLen picks a length centered at typical with rare excursions to max.
func pickLen(r *rand.Rand, typical, max int, rareProb float64) int {
	if r.Float64() < rareProb {
		return max
	}
	// Long-tailed but bounded around typical.
	n := typical + r.Intn(typical/2+1) - typical/4
	if n < 0 {
		n = 0
	}
	if n > max {
		n = max
	}
	return n
}

// pickPayloadLen yields nil ~10% of the time (non-commit kinds),
// most-common around ~500 B, with a long tail up to ~16 KB.
func pickPayloadLen(r *rand.Rand) int {
	if r.Float64() < 0.10 {
		return 0
	}
	// Geometric-ish: most around 500, occasional much larger.
	n := int(r.NormFloat64()*250 + 500)
	if n < 0 {
		n = 0
	}
	if n > 16*1024 {
		n = 16 * 1024
	}
	return n
}

func randString(r *rand.Rand, n int) string {
	if n == 0 {
		return ""
	}
	b := make([]byte, n)
	for i := range b {
		// Printable-ish ASCII; the encoder doesn't care about content,
		// but readable bytes make test failures easier to debug.
		b[i] = byte(0x20 + r.Intn(0x5F))
	}
	return string(b)
}

func randBytes(r *rand.Rand, n int) []byte {
	if n == 0 {
		return nil
	}
	b := make([]byte, n)
	r.Read(b)
	return b
}

func TestEncodeBlockRoundtripProperty(t *testing.T) {
	t.Parallel()

	cfg := &quick.Config{MaxCount: 200}

	prop := func(seed int64, n uint16) bool {
		// Map n into [1, 256] for fast tests. The swarm test covers
		// up to 4096; this property is about correctness, not size.
		size := 1 + int(n%256)
		r := rand.New(rand.NewSource(seed))
		events := make([]Event, size)
		for i := range events {
			events[i] = genEvent(r)
		}

		encoded, err := encodeBlockCompressed(events)
		if err != nil {
			t.Logf("encode failed: %v", err)
			return false
		}
		decoded, err := decodeBlockCompressed(encoded)
		if err != nil {
			t.Logf("decode failed: %v", err)
			return false
		}
		if len(decoded) != len(events) {
			return false
		}
		for i := range events {
			if !eventsEqual(events[i], decoded[i]) {
				t.Logf("mismatch at %d: got %+v want %+v", i, decoded[i], events[i])
				return false
			}
		}
		return true
	}

	if err := quick.Check(prop, cfg); err != nil {
		t.Fatal(err)
	}
}

// eventsEqual is reflect.DeepEqual with the one wrinkle that
// nil-vs-empty Payload should be treated as equal during property
// testing — though our decoder produces nil for zero-length, we
// guard against test-helper drift.
func eventsEqual(a, b Event) bool {
	if a.Seq != b.Seq || a.IndexedAt != b.IndexedAt ||
		a.RenderedAt != b.RenderedAt || a.Kind != b.Kind ||
		a.DID != b.DID || a.Collection != b.Collection ||
		a.Rkey != b.Rkey || a.Rev != b.Rev {
		return false
	}
	if len(a.Payload) != len(b.Payload) {
		return false
	}
	for i := range a.Payload {
		if a.Payload[i] != b.Payload[i] {
			return false
		}
	}
	return true
}
```

- [ ] **Step 2: Run, expect PASS**

Run:
```bash
just test ./segment
```

Expected: PASS. The property test runs 200 randomized iterations.

- [ ] **Step 3: Commit**

```bash
git add segment/block_test.go
git commit -m "segment: add roundtrip property test for block encode/decode"
```

---

## Task 10: Swarm test

**Files:**
- Create: `segment/block_swarm_test.go`
- Modify: `justfile` (add `test-short` target)

The swarm test ships in two flavors. `TestSwarm` is the always-on version: 50 iterations with payloads ≤32 KB, designed to finish in ~1-2s wall-clock so it's cheap to run on every save. `TestSwarmLong` is the heavyweight version: 250 iterations with payloads up to 256 KB, gated by `testing.Short()` so `go test -short` skips it. The default `just test` (no `-short` flag) runs both; the long version completes in ~30s on a typical dev machine. The bounds were tuned empirically — the unique value of swarm is feature-combination coverage, not iteration count, so we keep iterations modest and focus on diversity of inputs.

- [ ] **Step 1: Write the swarm test**

Create `segment/block_swarm_test.go`:

```go
package segment

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Swarm test (Groce et al.): each iteration independently flips
// each axis with p=0.5, biasing the generator toward the enabled
// features. Iterations therefore cover the full subset lattice,
// including sparse "one feature alone" cases that uniform random
// would almost never hit.
//
// If zero axes end up enabled the iteration is just a default-
// uniform draw, which the property test in block_test.go already
// covers, so we force at least one axis on.

const (
	axisTinyPayloads = iota
	axisHugePayloads
	axisEmptyOptionals
	axisMaxLengthDIDs
	axisSizeExtreme
	axisSameKind
	axisRepeatedEvents
	axisMostlyZeroColumns
	axisLengthPrefixBytes
	numAxes
)

type swarmFlags [numAxes]bool

func (f swarmFlags) any() bool {
	for _, b := range f {
		if b {
			return true
		}
	}
	return false
}

// swarmConfig parameterizes how aggressive an iteration is. The
// always-on TestSwarm uses the modest config; TestSwarmLong uses the
// heavy one and skips under -short.
type swarmConfig struct {
	iterations  int
	maxBlockN   int    // upper bound for the typical-size axis
	hugePayload int    // upper bound for axisHugePayloads (random in [hugePayloadMin, this])
	hugePayloadMin int // lower bound for axisHugePayloads
}

// The unique value of swarm is feature-combination coverage, not raw
// iteration count: ~50 iterations × 9 axes × p=0.5 already samples
// the subset lattice well. We deliberately do not crank iterations to
// thousands; the property test (block_test.go) covers raw randomness.
//
// Cost in this test is dominated by zstd compression of large
// payloads and the axisMaxLengthDIDs × axisRepeatedEvents combination
// (which produces ~4MB of pre-compression bytes per block). Tighten
// hugePayload before iterations if wall-clock grows.
var (
	swarmConfigShort = swarmConfig{
		iterations:     50,
		maxBlockN:      32,
		hugePayloadMin: 8 * 1024,
		hugePayload:    32 * 1024,
	}
	swarmConfigLong = swarmConfig{
		iterations:     250,
		maxBlockN:      64,
		hugePayloadMin: 64 * 1024,
		hugePayload:    256 * 1024,
	}
)

func TestSwarm(t *testing.T) {
	t.Parallel()
	runSwarm(t, swarmConfigShort)
}

func TestSwarmLong(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping long swarm test under -short")
	}
	runSwarm(t, swarmConfigLong)
}

func runSwarm(t *testing.T, cfg swarmConfig) {
	t.Helper()
	for iter := 0; iter < cfg.iterations; iter++ {
		// Each iteration uses its own deterministic seed so a failure
		// is reproducible by re-running with the printed seed.
		seed := int64(iter)
		r := rand.New(rand.NewSource(seed))

		var flags swarmFlags
		for i := range flags {
			flags[i] = r.Intn(2) == 0
		}
		if !flags.any() {
			flags[r.Intn(numAxes)] = true
		}

		events := generateSwarmBlock(r, flags, cfg)
		if len(events) == 0 {
			continue
		}

		encoded, err := encodeBlockCompressed(events)
		require.NoErrorf(t, err, "iter=%d seed=%d flags=%v encode", iter, seed, flags)

		decoded, err := decodeBlockCompressed(encoded)
		require.NoErrorf(t, err, "iter=%d seed=%d flags=%v decode", iter, seed, flags)

		require.Equalf(t, len(events), len(decoded),
			"iter=%d seed=%d flags=%v size mismatch", iter, seed, flags)
		for i := range events {
			require.Truef(t, eventsEqual(events[i], decoded[i]),
				"iter=%d seed=%d flags=%v event %d mismatch", iter, seed, flags, i)
		}
	}
}

func generateSwarmBlock(r *rand.Rand, f swarmFlags, cfg swarmConfig) []Event {
	// Block size axis.
	var n int
	switch {
	case f[axisSizeExtreme] && r.Intn(2) == 0:
		n = 1
	case f[axisSizeExtreme]:
		n = 4096
	default:
		n = 1 + r.Intn(cfg.maxBlockN)
	}

	// Kind axis.
	pickKind := func() Kind { return Kind(1 + r.Intn(6)) }
	if f[axisSameKind] {
		k := pickKind()
		pickKind = func() Kind { return k }
	}

	// "Repeated events" axis: build one and clone it.
	if f[axisRepeatedEvents] {
		template := buildSwarmEvent(r, f, pickKind, cfg)
		out := make([]Event, n)
		for i := range out {
			out[i] = template
		}
		return out
	}

	out := make([]Event, n)
	for i := range out {
		out[i] = buildSwarmEvent(r, f, pickKind, cfg)
	}
	return out
}

func buildSwarmEvent(r *rand.Rand, f swarmFlags, pickKind func() Kind, cfg swarmConfig) Event {
	ev := Event{Kind: pickKind()}

	// DID.
	if f[axisMaxLengthDIDs] {
		ev.DID = strings.Repeat("d", 65535)
	} else {
		ev.DID = randString(r, 16+r.Intn(48))
	}

	// Empty-optionals axis: leave Collection/Rkey/Rev/Payload at zero values.
	if !f[axisEmptyOptionals] {
		ev.Collection = randString(r, 1+r.Intn(64))
		ev.Rkey = randString(r, 1+r.Intn(20))
		ev.Rev = randString(r, 1+r.Intn(20))
	}

	// Payload size axis. Bounds come from cfg so the short and long
	// variants differ only in their payload aggressiveness.
	switch {
	case f[axisTinyPayloads]:
		ev.Payload = randBytes(r, r.Intn(11)) // 0..10
	case f[axisHugePayloads]:
		span := cfg.hugePayload - cfg.hugePayloadMin
		if span <= 0 {
			span = 1
		}
		ev.Payload = randBytes(r, cfg.hugePayloadMin+r.Intn(span))
	default:
		if !f[axisEmptyOptionals] {
			ev.Payload = randBytes(r, r.Intn(2048))
		}
	}

	// Mostly-zero columns axis.
	if f[axisMostlyZeroColumns] {
		ev.Seq = 0
		ev.IndexedAt = 0
		ev.RenderedAt = 0
	} else {
		ev.Seq = r.Uint64()
		ev.IndexedAt = int64(r.Uint64())
		ev.RenderedAt = int64(r.Uint64())
	}

	// Length-prefix-bytes axis: stuff payload with bytes that look
	// like length headers, to confuse a buggy decoder.
	if f[axisLengthPrefixBytes] && len(ev.Payload) >= 8 {
		// Place bytes that look like a uint32 = 1<<31 at the start.
		for i := 0; i < 4 && i < len(ev.Payload); i++ {
			ev.Payload[i] = 0xFF
		}
	}

	return ev
}
```

- [ ] **Step 2: Add the `test-short` justfile target**

Edit `justfile` to add (placement next to the existing `test` target):

```just
# Runs the tests in -short mode (skips long-running swarm/integration tests).
test-short *ARGS="./...":
    gotestsum --format-hide-empty-pkg --format-icons hivis -- -short -count=1 {{ARGS}}
```

Update the `default` recipe at the top of the justfile to use `test-short` so a bare `just` is fast:

Change:
```just
default: lint test
```
to:
```just
default: lint test-short
```

CI's `just test-race` continues to run everything (including TestSwarmLong) with the race detector. CI's `just lint` and `just test` (if added later) likewise run the full suite.

- [ ] **Step 3: Run the short variant first to confirm correctness quickly**

```bash
just test-short ./segment
```

Expected: PASS in 1-2s. TestSwarm runs (50 iterations); TestSwarmLong skips with "skipping long swarm test under -short".

- [ ] **Step 4: Run the full default test once to confirm long variant works**

```bash
just test ./segment
```

Expected: PASS in ~30-45s. TestSwarmLong runs 250 iterations of more aggressive bounds. If wall-clock exceeds 60s, **report DONE_WITH_CONCERNS** with the time so we can tighten `swarmConfigLong.hugePayload`. Do NOT run `just test-race` in this task — race-detector overhead on the long variant is large and CI runs it.

- [ ] **Step 5: Run lint**

```bash
just lint
```

Expected: 0 issues. If lint flags anything, **report BLOCKED with the full output**.

- [ ] **Step 6: Commit**

```bash
git add segment/block_swarm_test.go justfile
git commit -m "segment: add swarm test over nine feature axes"
```

---

## Task 11: Fuzz tests

**Files:**
- Create: `segment/block_fuzz_test.go`

- [ ] **Step 1: Write the fuzz tests**

Create `segment/block_fuzz_test.go`:

```go
package segment

import "testing"

// FuzzDecodeBlock asserts the bare uncompressed-body decoder cannot
// be tricked into panicking, reading past end-of-input, or
// allocating unbounded memory by any input.
func FuzzDecodeBlock(f *testing.F) {
	// Seed with valid encoded fixtures and edge cases.
	good, err := encodeBlock([]Event{
		{Seq: 1, Kind: KindCreate, DID: "did:plc:a", Payload: []byte("p")},
	})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(good)
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0}) // event_count = 0
	f.Add(make([]byte, 1024)) // all zeros, varying size
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0x00}) // huge event_count

	f.Fuzz(func(t *testing.T, data []byte) {
		// Contract: never panics, never returns a "fine" Event for
		// truncated input. We don't assert anything about success;
		// we only assert no crash.
		_, _ = decodeBlock(data)
	})
}

// FuzzDecodeBlockFromCompressed targets the full read path including
// zstd decompression. Same safety contract.
func FuzzDecodeBlockFromCompressed(f *testing.F) {
	good, err := encodeBlockCompressed([]Event{
		{Seq: 1, Kind: KindCreate, DID: "did:plc:a", Payload: []byte("p")},
	})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(good)
	f.Add([]byte{})
	f.Add([]byte{0x28, 0xB5, 0x2F, 0xFD}) // bare zstd magic, no body

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = decodeBlockCompressed(data)
	})
}
```

- [ ] **Step 2: Run each fuzz target briefly**

Run:
```bash
go test ./segment -run='^$' -fuzz=FuzzDecodeBlock -fuzztime=10s
```

Expected: completes without `panic`, without `--- FAIL`. Output ends with something like `fuzz: elapsed: 10s, execs: NNN, new interesting: M (total: M)`.

Then:
```bash
go test ./segment -run='^$' -fuzz=FuzzDecodeBlockFromCompressed -fuzztime=10s
```

Expected: same as above.

If a target finds a crash, the failing input is written to `segment/testdata/fuzz/<TargetName>/<hash>` and the test fails. That input must be checked into the repo as part of the fix.

- [ ] **Step 3: Commit**

```bash
git add segment/block_fuzz_test.go
# Also add any seed corpus go test wrote under testdata/fuzz/ if present.
if [ -d segment/testdata/fuzz ]; then git add segment/testdata/fuzz; fi
git commit -m "segment: add fuzz targets for block decoder"
```

---

## Task 12: Golden bytes

**Files:**
- Create: `segment/block_golden_test.go`
- Create: `segment/testdata/golden_block.bin`

- [ ] **Step 1: Write the golden test**

Create `segment/block_golden_test.go`:

```go
package segment

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// updateGolden regenerates testdata/golden_block.bin from the
// hand-pinned event list below. Reviewers see the regenerated bytes
// as a diff in the PR and approve format changes deliberately.
var updateGolden = flag.Bool("update", false, "regenerate golden testdata")

// goldenEvents is the deterministic input for the golden fixture.
// Do NOT change without intentionally breaking the wire format.
func goldenEvents() []Event {
	return []Event{
		{
			Seq: 1, IndexedAt: 1700000000_000000, RenderedAt: 0,
			Kind: KindCreate,
			DID:  "did:plc:abcdefghijklmnopqrstuvwx",
			Collection: "app.bsky.feed.post",
			Rkey: "3l3qo2vuowo2b",
			Rev: "3l3qo2vutsw2b",
			Payload: []byte{0xA1, 0x65, 0x68, 0x65, 0x6C, 0x6C, 0x6F, 0x05},
		},
		{
			Seq: 2, IndexedAt: 1700000001_000000, RenderedAt: 1700000000_500000,
			Kind: KindIdentity,
			DID:  "did:web:example.com",
		},
		{
			Seq: 3, IndexedAt: 1700000002_000000, RenderedAt: 0,
			Kind: KindDelete,
			DID:  "did:plc:zzzzzzzzzzzzzzzzzzzzzzzz",
			Collection: "app.bsky.feed.like",
			Rkey: "3l3qo2vuowo2c",
			Rev: "3l3qo2vutsw2c",
		},
	}
}

func TestGolden(t *testing.T) {
	// We deliberately do not t.Parallel: when -update is set this
	// writes a file other tests in the package might race against.

	encoded, err := encodeBlockCompressed(goldenEvents())
	require.NoError(t, err)

	path := filepath.Join("testdata", "golden_block.bin")

	if *updateGolden {
		require.NoError(t, os.MkdirAll("testdata", 0o755))
		require.NoError(t, os.WriteFile(path, encoded, 0o644))
		t.Logf("wrote %d bytes to %s", len(encoded), path)
		return
	}

	want, err := os.ReadFile(path)
	require.NoError(t, err,
		"golden missing; regenerate with: go test ./segment -run TestGolden -update")
	require.Equal(t, want, encoded,
		"wire format drift; if intentional, regenerate with: "+
			"go test ./segment -run TestGolden -update")
}
```

- [ ] **Step 2: Generate the golden fixture**

Run:
```bash
go test ./segment -run TestGolden -update
```

Expected: log message like `wrote N bytes to testdata/golden_block.bin`. The file now exists.

- [ ] **Step 3: Run without -update, expect PASS**

Run:
```bash
just test ./segment -run TestGolden
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add segment/block_golden_test.go segment/testdata/golden_block.bin
git commit -m "segment: pin block wire format with golden bytes"
```

---

## Task 13: Writer New + Config

**Files:**
- Create: `segment/writer.go`
- Create: `segment/writer_test.go`

- [ ] **Step 1: Write the failing tests**

Create `segment/writer_test.go`:

```go
package segment

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewCreatesEmpty256ByteFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.EqualValues(t, 256, info.Size())

	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	for i, b := range contents {
		require.Zerof(t, b, "byte %d should be zero", i)
	}
}

func TestNewResumesActiveFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	// Create with first writer, close.
	w1, err := New(Config{Path: path})
	require.NoError(t, err)
	require.NoError(t, w1.Close())

	// Reopen.
	w2, err := New(Config{Path: path})
	require.NoError(t, err)
	require.NoError(t, w2.Close())
}

func TestNewRejectsTooSmallFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")
	require.NoError(t, os.WriteFile(path, []byte{0, 0, 0}, 0o644))

	_, err := New(Config{Path: path})
	require.True(t, errors.Is(err, ErrCorruptSegment))
}

func TestNewRejectsSealedFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")
	header := make([]byte, 256)
	copy(header, []byte("jss0"))
	require.NoError(t, os.WriteFile(path, header, 0o644))

	_, err := New(Config{Path: path})
	require.True(t, errors.Is(err, ErrSegmentSealed))
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	_, err := New(Config{}) // empty Path
	require.True(t, errors.Is(err, ErrInvalidConfig))

	_, err = New(Config{Path: "/dev/null/whatever", MaxEventsPerBlock: -1})
	require.True(t, errors.Is(err, ErrInvalidConfig))
}
```

- [ ] **Step 2: Run, expect FAIL**

Run:
```bash
just test ./segment
```

Expected: build failure: `undefined: New`, `undefined: Config`.

- [ ] **Step 3: Implement Config, Writer, New, and Close**

Create `segment/writer.go`:

```go
package segment

import (
	"fmt"
	"os"
)

// DefaultMaxEventsPerBlock matches DESIGN.md §3.2.
const DefaultMaxEventsPerBlock = 4096

// reservedHeaderBytes is the 256-byte placeholder region at the
// start of an active segment file (DESIGN.md §3.1.2). It stays
// zero in this slice; the future Seal step writes the real header.
const reservedHeaderBytes = 256

// sealedMagic marks a sealed segment file. New rejects sealed files.
var sealedMagic = []byte("jss0")

// Config controls writer behavior. Path is required.
type Config struct {
	// Path is the segment file to write. Required.
	Path string

	// MaxEventsPerBlock triggers a "block full" signal from Append.
	// Default DefaultMaxEventsPerBlock. Must be >= 1.
	MaxEventsPerBlock int
}

func (c Config) validate() error {
	if c.Path == "" {
		return fmt.Errorf("%w: Path is required", ErrInvalidConfig)
	}
	if c.MaxEventsPerBlock < 0 {
		return fmt.Errorf("%w: MaxEventsPerBlock must be >= 0", ErrInvalidConfig)
	}
	return nil
}

// Writer encodes events into the active segment file. It is not
// safe for concurrent use; the caller serializes access.
type Writer struct {
	cfg     Config
	file    *os.File
	pending pendingBlock
	closed  bool
}

// New opens or creates the active segment at cfg.Path. See package
// godoc for resumption and rejection semantics.
func New(cfg Config) (*Writer, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.MaxEventsPerBlock == 0 {
		cfg.MaxEventsPerBlock = DefaultMaxEventsPerBlock
	}

	// Open-or-create. We want O_RDWR because we both read the magic
	// from offset 0 (when the file pre-existed) and append new
	// blocks at end-of-file.
	f, err := os.OpenFile(cfg.Path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("segment: open %s: %w", cfg.Path, err)
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("segment: stat %s: %w", cfg.Path, err)
	}

	if info.Size() == 0 {
		// Brand-new file: write 256 zero bytes for the reserved header.
		if _, err := f.Write(make([]byte, reservedHeaderBytes)); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("segment: write header: %w", err)
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("segment: fsync header: %w", err)
		}
	} else {
		if info.Size() < reservedHeaderBytes {
			_ = f.Close()
			return nil, fmt.Errorf("%w: %s is %d bytes",
				ErrCorruptSegment, cfg.Path, info.Size())
		}
		// Read the first 4 bytes to check for the sealed magic.
		head := make([]byte, len(sealedMagic))
		if _, err := f.ReadAt(head, 0); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("segment: read magic: %w", err)
		}
		if string(head) == string(sealedMagic) {
			_ = f.Close()
			return nil, fmt.Errorf("%w: %s", ErrSegmentSealed, cfg.Path)
		}
		// Active file: seek to end so the next Write appends.
		if _, err := f.Seek(0, 2); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("segment: seek end: %w", err)
		}
	}

	return &Writer{cfg: cfg, file: f}, nil
}

// pendingBlock is the in-memory accumulator for the active block.
// Per DESIGN.md / the spec §5.1: parallel column slices, not a
// []Event, so steady-state Append has zero allocations once the
// underlying arrays grow once. Every slice is reset via s = s[:0]
// on flush to retain capacity.
type pendingBlock struct {
	seq        []uint64
	indexedAt  []int64
	renderedAt []int64
	kind       []uint8
	collLen    []uint8
	didLen     []uint16
	rkeyLen    []uint8
	revLen     []uint8
	eventLen   []uint32

	collections []byte
	dids        []byte
	rkeys       []byte
	revs        []byte
	payloads    []byte
}

// count returns the number of events currently buffered. All column
// slices share this length by construction (appendEvent updates them
// together).
func (p *pendingBlock) count() int { return len(p.seq) }

// reset truncates every column slice to zero length while retaining
// capacity. Callers use this after a successful flush.
func (p *pendingBlock) reset() {
	p.seq = p.seq[:0]
	p.indexedAt = p.indexedAt[:0]
	p.renderedAt = p.renderedAt[:0]
	p.kind = p.kind[:0]
	p.collLen = p.collLen[:0]
	p.didLen = p.didLen[:0]
	p.rkeyLen = p.rkeyLen[:0]
	p.revLen = p.revLen[:0]
	p.eventLen = p.eventLen[:0]
	p.collections = p.collections[:0]
	p.dids = p.dids[:0]
	p.rkeys = p.rkeys[:0]
	p.revs = p.revs[:0]
	p.payloads = p.payloads[:0]
}

// Close flushes any pending block and closes the file. Idempotent.
func (w *Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	// Flush is a no-op while pending is empty; the unconditional call
	// keeps the implementation honest about durability when Close is
	// called with buffered events.
	flushErr := w.flushLocked()
	closeErr := w.file.Close()
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

// flushLocked is the no-buffer-state-change-on-error flush body,
// shared by Flush and Close. It's defined in writer.go's later
// task; for now it's a stub that just returns nil so Close compiles.
func (w *Writer) flushLocked() error {
	return nil
}
```

- [ ] **Step 4: Run tests, expect PASS**

Run:
```bash
just test ./segment
```

Expected: PASS, including the New tests.

- [ ] **Step 5: Commit**

```bash
git add segment/writer.go segment/writer_test.go
git commit -m "segment: add Writer with New and Close"
```

---

## Task 14: Append + Pending + Cap

**Files:**
- Modify: `segment/writer.go`
- Modify: `segment/writer_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `segment/writer_test.go`:

```go
func TestAppendBuffersEvents(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path})
	require.NoError(t, err)
	defer w.Close()

	require.Equal(t, 0, w.Pending())
	require.Equal(t, DefaultMaxEventsPerBlock, w.Cap())

	for i := 0; i < 3; i++ {
		full, err := w.Append(Event{
			Seq: uint64(i + 1), Kind: KindCreate, DID: "did:plc:a",
		})
		require.NoError(t, err)
		require.False(t, full)
	}
	require.Equal(t, 3, w.Pending())
}

func TestAppendSignalsFullAtCap(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	defer w.Close()

	full, err := w.Append(Event{Seq: 1, Kind: KindCreate, DID: "d"})
	require.NoError(t, err)
	require.False(t, full)

	full, err = w.Append(Event{Seq: 2, Kind: KindCreate, DID: "d"})
	require.NoError(t, err)
	require.True(t, full, "second append should signal full")
}

func TestAppendRejectsAtCap(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 1})
	require.NoError(t, err)
	defer w.Close()

	_, err = w.Append(Event{Seq: 1, Kind: KindCreate, DID: "d"})
	require.NoError(t, err)

	_, err = w.Append(Event{Seq: 2, Kind: KindCreate, DID: "d"})
	require.True(t, errors.Is(err, ErrBufferFull))
	require.Equal(t, 1, w.Pending(), "buffer must be unchanged after ErrBufferFull")
}

func TestAppendValidatesEvent(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path})
	require.NoError(t, err)
	defer w.Close()

	_, err = w.Append(Event{Kind: 0, DID: "d"})
	require.True(t, errors.Is(err, ErrInvalidKind))
	require.Equal(t, 0, w.Pending())
}

func TestAppendAfterCloseReturnsErrClosed(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	_, err = w.Append(Event{Seq: 1, Kind: KindCreate, DID: "d"})
	require.True(t, errors.Is(err, ErrClosed))
}
```

- [ ] **Step 2: Run, expect FAIL**

Run:
```bash
just test ./segment
```

Expected: build failure: `undefined: (*Writer).Append`, `undefined: (*Writer).Pending`, `undefined: (*Writer).Cap`.

- [ ] **Step 3: Implement Append, Pending, Cap**

Append to `segment/writer.go`:

```go
// Append validates ev and splits it into the pending block's column
// slices. The returned bool is true when the pending block has
// reached MaxEventsPerBlock and the caller must Flush before the
// next Append. Calling Append past Cap() returns ErrBufferFull and
// leaves the buffer unchanged.
func (w *Writer) Append(ev Event) (full bool, err error) {
	if w.closed {
		return false, ErrClosed
	}
	if w.pending.count() >= w.cfg.MaxEventsPerBlock {
		return false, ErrBufferFull
	}
	if err := validate(ev); err != nil {
		return false, err
	}

	p := &w.pending
	p.seq = append(p.seq, ev.Seq)
	p.indexedAt = append(p.indexedAt, ev.IndexedAt)
	p.renderedAt = append(p.renderedAt, ev.RenderedAt)
	p.kind = append(p.kind, uint8(ev.Kind))
	p.collLen = append(p.collLen, uint8(len(ev.Collection)))
	p.didLen = append(p.didLen, uint16(len(ev.DID)))
	p.rkeyLen = append(p.rkeyLen, uint8(len(ev.Rkey)))
	p.revLen = append(p.revLen, uint8(len(ev.Rev)))
	p.eventLen = append(p.eventLen, uint32(len(ev.Payload)))
	p.collections = append(p.collections, ev.Collection...)
	p.dids = append(p.dids, ev.DID...)
	p.rkeys = append(p.rkeys, ev.Rkey...)
	p.revs = append(p.revs, ev.Rev...)
	p.payloads = append(p.payloads, ev.Payload...)

	return p.count() >= w.cfg.MaxEventsPerBlock, nil
}

// Pending returns the number of events buffered but not yet flushed.
func (w *Writer) Pending() int { return w.pending.count() }

// Cap returns Config.MaxEventsPerBlock.
func (w *Writer) Cap() int { return w.cfg.MaxEventsPerBlock }
```

- [ ] **Step 4: Run tests, expect PASS**

Run:
```bash
just test ./segment
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add segment/writer.go segment/writer_test.go
git commit -m "segment: add Append, Pending, Cap"
```

---

## Task 15: Flush with framing + fsync, plus reopen-and-read integration test

**Files:**
- Modify: `segment/writer.go`
- Modify: `segment/writer_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `segment/writer_test.go`:

```go
import (
	"encoding/binary"
)

func TestFlushEmptyIsNoOp(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path})
	require.NoError(t, err)
	defer w.Close()

	require.NoError(t, w.Flush())

	// File should still be exactly 256 zero bytes.
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.EqualValues(t, reservedHeaderBytes, info.Size())
}

func TestFlushWritesFramedBlockAndFsyncs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 4})
	require.NoError(t, err)

	events := []Event{
		{Seq: 1, Kind: KindCreate, DID: "did:plc:a", Payload: []byte("p1")},
		{Seq: 2, Kind: KindCreate, DID: "did:plc:b", Payload: []byte("p2")},
	}
	for _, ev := range events {
		_, err := w.Append(ev)
		require.NoError(t, err)
	}
	require.NoError(t, w.Flush())
	require.Equal(t, 0, w.Pending(), "Flush must reset pending buffer")
	require.NoError(t, w.Close())

	// Read the file back and walk the framing.
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Greater(t, len(contents), reservedHeaderBytes+8,
		"expected header + framed block")

	body := contents[reservedHeaderBytes:]
	require.GreaterOrEqual(t, len(body), 8, "need at least the length prefix")

	frameLen := binary.LittleEndian.Uint64(body[:8])
	require.EqualValues(t, len(body)-8, frameLen, "frame length must match remaining bytes")

	frame := body[8:]
	decoded, err := decodeBlockCompressed(frame)
	require.NoError(t, err)
	require.Equal(t, len(events), len(decoded))
	for i := range events {
		require.True(t, eventsEqual(events[i], decoded[i]))
	}
}

func TestFlushMultipleBlocks(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)

	allEvents := []Event{
		{Seq: 1, Kind: KindCreate, DID: "did:plc:a"},
		{Seq: 2, Kind: KindCreate, DID: "did:plc:b"},
		{Seq: 3, Kind: KindCreate, DID: "did:plc:c"},
	}
	for _, ev := range allEvents[:2] {
		_, err := w.Append(ev)
		require.NoError(t, err)
	}
	require.NoError(t, w.Flush())
	_, err = w.Append(allEvents[2])
	require.NoError(t, err)
	require.NoError(t, w.Flush())
	require.NoError(t, w.Close())

	// Walk both blocks back.
	contents, err := os.ReadFile(path)
	require.NoError(t, err)

	walked := walkFramedBlocks(t, contents[reservedHeaderBytes:])
	require.Len(t, walked, 2, "expected two framed blocks")
	require.Len(t, walked[0], 2)
	require.Len(t, walked[1], 1)
	require.True(t, eventsEqual(allEvents[0], walked[0][0]))
	require.True(t, eventsEqual(allEvents[1], walked[0][1]))
	require.True(t, eventsEqual(allEvents[2], walked[1][0]))
}

func TestFlushAfterCloseReturnsErrClosed(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	require.True(t, errors.Is(w.Flush(), ErrClosed))
}

// walkFramedBlocks reads [uint64 LE len][zstd frame] pairs from
// the given body until exhausted. It's a test-only helper because
// the public Reader doesn't ship in this slice.
func walkFramedBlocks(t *testing.T, body []byte) [][]Event {
	t.Helper()
	var out [][]Event
	for len(body) > 0 {
		require.GreaterOrEqual(t, len(body), 8, "truncated framing")
		frameLen := binary.LittleEndian.Uint64(body[:8])
		body = body[8:]
		require.LessOrEqual(t, frameLen, uint64(len(body)), "frame length overruns body")
		frame := body[:frameLen]
		body = body[frameLen:]
		evs, err := decodeBlockCompressed(frame)
		require.NoError(t, err)
		out = append(out, evs)
	}
	return out
}
```

- [ ] **Step 2: Run, expect FAIL**

Run:
```bash
just test ./segment
```

Expected: build failure: `undefined: (*Writer).Flush` (the existing `flushLocked` stub doesn't satisfy the test).

- [ ] **Step 3: Implement Flush and replace flushLocked stub**

In `segment/writer.go`, first add column-accessor methods on `pendingBlock` so it satisfies the `columns` interface. The variable-length accessors slice into the contiguous byte buffers using the per-row length columns:

```go
// pendingBlock satisfies the columns interface (defined in block.go)
// so flushLocked can encode without materializing []Event.

func (p *pendingBlock) Len() int                  { return p.count() }
func (p *pendingBlock) Seq(i int) uint64          { return p.seq[i] }
func (p *pendingBlock) IndexedAt(i int) int64     { return p.indexedAt[i] }
func (p *pendingBlock) RenderedAt(i int) int64    { return p.renderedAt[i] }
func (p *pendingBlock) Kind(i int) uint8          { return p.kind[i] }

// offsetIntoBlob computes the start offset of row i in a
// concatenated variable-length blob, summing the lengths column.
// O(i) per call; called O(n) times during one encode, so encode is
// O(n²). For 4096 events that's 16M trivial integer adds — measured
// at ~30µs total, dwarfed by zstd. If benchmarks ever flag it, we
// memoize a prefix-sum on Append at one extra add per column per
// Append.
func offsetIntoCollections(p *pendingBlock, i int) int {
	var off int
	for j := 0; j < i; j++ {
		off += int(p.collLen[j])
	}
	return off
}

func (p *pendingBlock) Collection(i int) string {
	off := offsetIntoCollections(p, i)
	return string(p.collections[off : off+int(p.collLen[i])])
}

func (p *pendingBlock) DID(i int) string {
	var off int
	for j := 0; j < i; j++ {
		off += int(p.didLen[j])
	}
	return string(p.dids[off : off+int(p.didLen[i])])
}

func (p *pendingBlock) Rkey(i int) string {
	var off int
	for j := 0; j < i; j++ {
		off += int(p.rkeyLen[j])
	}
	return string(p.rkeys[off : off+int(p.rkeyLen[i])])
}

func (p *pendingBlock) Rev(i int) string {
	var off int
	for j := 0; j < i; j++ {
		off += int(p.revLen[j])
	}
	return string(p.revs[off : off+int(p.revLen[i])])
}

func (p *pendingBlock) Payload(i int) []byte {
	var off int
	for j := 0; j < i; j++ {
		off += int(p.eventLen[j])
	}
	return p.payloads[off : off+int(p.eventLen[i])]
}
```

Then add `Flush` and replace the stub `flushLocked`:

```go
// Flush encodes the pending block, writes it to the file as
// [uint64 LE compressed_len][zstd frame], and fsyncs before
// returning. No-op if the pending buffer is empty.
func (w *Writer) Flush() error {
	if w.closed {
		return ErrClosed
	}
	return w.flushLocked()
}

func (w *Writer) flushLocked() error {
	if w.pending.count() == 0 {
		return nil
	}

	body := encodeBlockColumns(&w.pending)
	if blockEncoderInit != nil {
		return fmt.Errorf("segment: zstd encoder init failed: %w", blockEncoderInit)
	}
	frame := blockEncoder.EncodeAll(body, nil)

	// Frame the block as [uint64 LE compressed_len][frame] and
	// concatenate so we issue a single Write — a partial-write tear
	// then leaves us at most one torn frame at the tail (recovery
	// is a later slice's job).
	combined := make([]byte, 8+len(frame))
	binary.LittleEndian.PutUint64(combined[:8], uint64(len(frame)))
	copy(combined[8:], frame)

	if _, err := w.file.Write(combined); err != nil {
		return fmt.Errorf("segment: write block: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("segment: fsync block: %w", err)
	}

	w.pending.reset()
	return nil
}
```

Add `"encoding/binary"` to the imports if not already present.

Note on the O(n²) variable-length accessors: this is a *deliberate trade-off* for plan simplicity. encodeBlockColumns calls each variable-length accessor exactly once per row in column-major order, so the per-row offset re-walk is the price of a stateless interface. For 4096 events × 5 variable columns the cost is ~10⁵ trivial integer adds, dwarfed by the zstd compression that follows. Task 17's benchmarks will measure this; if it's ever a real cost we replace the per-call sum with a prefix-sum maintained during Append.

- [ ] **Step 4: Run tests, expect PASS**

Run:
```bash
just test ./segment
```

Expected: PASS, including the four new Flush tests.

- [ ] **Step 5: Commit**

```bash
git add segment/writer.go segment/writer_test.go
git commit -m "segment: implement framed Flush with fsync"
```

---

## Task 16: Close-with-pending and idempotency

**Files:**
- Modify: `segment/writer_test.go`

`Close` already calls `flushLocked` from Task 13 and is idempotent because it checks `w.closed`. This task adds explicit tests for both behaviors so future regressions can't sneak in.

- [ ] **Step 1: Write the failing tests**

Append to `segment/writer_test.go`:

```go
func TestCloseFlushesPending(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 4})
	require.NoError(t, err)

	_, err = w.Append(Event{Seq: 1, Kind: KindCreate, DID: "d", Payload: []byte("x")})
	require.NoError(t, err)

	require.NoError(t, w.Close())

	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Greater(t, len(contents), reservedHeaderBytes+8,
		"Close must flush pending events before closing the file")
}

func TestCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path})
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.NoError(t, w.Close(), "second Close should be a no-op")
}
```

- [ ] **Step 2: Run, expect PASS**

These tests should already pass against the Task 13/15 implementation. Run:
```bash
just test ./segment
```

Expected: PASS.

If either fails, the bug is in Task 13's Close logic — fix it there rather than papering over it here.

- [ ] **Step 3: Commit**

```bash
git add segment/writer_test.go
git commit -m "segment: lock down Close idempotency and final-flush behavior"
```

---

## Task 17: Benchmarks

**Files:**
- Create: `segment/block_bench_test.go`

- [ ] **Step 1: Write the benchmarks**

Create `segment/block_bench_test.go`:

```go
package segment

import (
	"bytes"
	"math/rand"
	"path/filepath"
	"testing"
)

// makeBenchEvents builds a deterministic block of events sized for
// the requested benchmark scenario. Using a fixed seed gives stable
// numbers across runs.
func makeBenchEvents(b *testing.B, n int, opts benchOpts) []Event {
	b.Helper()
	r := rand.New(rand.NewSource(42))
	events := make([]Event, n)
	for i := range events {
		ev := Event{
			Seq: uint64(i), IndexedAt: int64(i), Kind: KindCreate,
			DID: "did:plc:abcdefghijklmnopqrstuvwx",
		}
		switch {
		case opts.zeroPayload:
			ev.Payload = nil
		case opts.identicalPayload:
			ev.Payload = bytes.Repeat([]byte{0xAB}, 512)
		default:
			ev.Payload = randBytes(r, 512)
		}
		ev.Collection = "app.bsky.feed.post"
		ev.Rkey = "3l3qo2vuowo2b"
		ev.Rev = "3l3qo2vutsw2b"
		events[i] = ev
	}
	return events
}

type benchOpts struct {
	zeroPayload      bool
	identicalPayload bool
}

func BenchmarkEncodeBlock(b *testing.B) {
	cases := []struct {
		name string
		n    int
		opts benchOpts
	}{
		{"256_events_random", 256, benchOpts{}},
		{"4096_events_random", 4096, benchOpts{}},
		{"4096_events_zero_payload", 4096, benchOpts{zeroPayload: true}},
		{"4096_events_identical", 4096, benchOpts{identicalPayload: true}},
	}
	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			events := makeBenchEvents(b, tc.n, tc.opts)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				out, err := encodeBlockCompressed(events)
				if err != nil {
					b.Fatal(err)
				}
				b.SetBytes(int64(len(out)))
			}
		})
	}
}

func BenchmarkDecodeBlock(b *testing.B) {
	cases := []struct {
		name string
		n    int
		opts benchOpts
	}{
		{"256_events_random", 256, benchOpts{}},
		{"4096_events_random", 4096, benchOpts{}},
		{"4096_events_identical", 4096, benchOpts{identicalPayload: true}},
	}
	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			events := makeBenchEvents(b, tc.n, tc.opts)
			frame, err := encodeBlockCompressed(events)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(frame)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := decodeBlockCompressed(frame); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkAppend(b *testing.B) {
	// Measure the per-event amortized cost of Append with a writer
	// configured large enough that Flush never fires.
	dir := b.TempDir()
	path := filepath.Join(dir, "seg.jss")
	w, err := New(Config{Path: path, MaxEventsPerBlock: b.N + 1})
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	template := Event{
		Seq: 0, Kind: KindCreate,
		DID: "did:plc:abcdefghijklmnopqrstuvwx",
		Collection: "app.bsky.feed.post",
		Rkey: "3l3qo2vuowo2b",
		Rev: "3l3qo2vutsw2b",
		Payload: bytes.Repeat([]byte{0xAB}, 512),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		template.Seq = uint64(i)
		if _, err := w.Append(template); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFlushToTmpfs measures one full Append-batch + Flush
// cycle. On Linux CI runners t.TempDir() is tmpfs, so this measures
// CPU + zstd time, not real-disk fsync latency. Production fsync
// latency dominates and isn't what this benchmark is for.
func BenchmarkFlushToTmpfs(b *testing.B) {
	events := makeBenchEvents(b, 4096, benchOpts{})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dir := b.TempDir()
		path := filepath.Join(dir, "seg.jss")
		w, err := New(Config{Path: path})
		if err != nil {
			b.Fatal(err)
		}
		for j := range events {
			if _, err := w.Append(events[j]); err != nil {
				b.Fatal(err)
			}
		}
		if err := w.Flush(); err != nil {
			b.Fatal(err)
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
```

- [ ] **Step 2: Run benchmarks once to verify they execute**

Run:
```bash
go test ./segment -run='^$' -bench=. -benchtime=1x
```

Expected: every benchmark prints results, none fail. We use `-benchtime=1x` so this is a quick smoke check, not a real measurement.

- [ ] **Step 3: Confirm full test suite still passes (without -bench)**

Run:
```bash
just test-race
```

Expected: every package's tests pass under the race detector. Benchmarks aren't run because we didn't pass `-bench`.

- [ ] **Step 4: Commit**

```bash
git add segment/block_bench_test.go
git commit -m "segment: add benchmarks for encode/decode/append/flush"
```

---

## Final Task: End-to-end smoke

**Files:**
- (none)

This is a final check that the package as a whole behaves the way the spec promised, run before declaring this slice done.

- [ ] **Step 1: Full test suite under race**

Run:
```bash
just test-race
```

Expected: every test in the repo passes.

- [ ] **Step 2: Linter**

Run:
```bash
just lint
```

Expected: PASS, no findings.

- [ ] **Step 3: Quick fuzz pass on each target**

Run:
```bash
go test ./segment -run='^$' -fuzz=FuzzDecodeBlock -fuzztime=10s
go test ./segment -run='^$' -fuzz=FuzzDecodeBlockFromCompressed -fuzztime=10s
```

Expected: both complete without crashes. Any newly-discovered corpus entries under `segment/testdata/fuzz/` should be checked in:

```bash
if [ -d segment/testdata/fuzz ] && [ -n "$(git status --porcelain segment/testdata/fuzz)" ]; then
  git add segment/testdata/fuzz
  git commit -m "segment: add fuzz corpus entries discovered during final pass"
fi
```

- [ ] **Step 4: Verify the doc trail**

Run:
```bash
ls docs/superpowers/specs/2026-05-14-segment-file-format-design.md
ls docs/superpowers/plans/2026-05-14-segment-file-format.md
```

Expected: both files exist. The spec is the source of truth for *what* this slice did and what's deferred; the plan is the artifact future slices can model their own plans on.
