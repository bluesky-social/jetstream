# getTombstones Compaction Overlay — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `network.bsky.jetstream.getTombstones` XRPC query that serves the in-memory delete/update/account-deletion tombstone overlay as a precomputed, compact, zstd-compressed binary blob, so a future client can suppress superseded record rows during backfill replay.

**Architecture:** A new transport-agnostic `internal/overlay` package owns (a) a pure encoder `Snapshot → []byte` (custom columnar + dictionary + delta-varint, whole body zstd-framed) and (b) a `Cache` that holds the latest immutable compressed `*Blob`, rebuilt on compaction passes and a coalescing ticker. A handler in `internal/xrpcapi` serves the cached blob verbatim. runtime.go wires the cache to the existing `tombstone.Set` and `OnCompactionPass` hook.

**Tech Stack:** Go; `github.com/klauspost/compress/zstd` (already a dependency); `github.com/zeebo/xxh3` (already used in `segment/header.go`); atmos `xrpcserver`; prometheus client; existing `internal/tombstone`, `internal/manifest`, `internal/oracle` packages.

---

## Background the implementer needs

Read these before starting:

- **`docs/superpowers/specs/2026-06-15-getTombstones-overlay-design.md`** — the full design. Especially §3 (consistency), §4 (wire format), §5.4 (why a stale blob is stale-but-coherent, not incorrect).
- **`DESIGN.md` §3.3** — the compaction model this completes.
- **`internal/tombstone/tombstone.go`** — the data we serialize. Key types:
  - `RecordKey{DID, Collection, Rkey string}`
  - `DIDTombstone{Seq uint64, Reason string}` (Reason is `"account"` or `"sync"`)
  - `Snapshot{Records map[RecordKey]uint64, DIDs map[string]DIDTombstone}`
  - `(*Set).SnapshotRange(lowExclusive, highInclusive uint64) Snapshot` — returns tombstones in `(low, high]`.
  - `(Snapshot).Empty() bool`, `(Snapshot).ShouldDrop(*segment.Event) (bool, string)`.
- **`internal/xrpcapi/getsegment.go`, `getsegment_test.go`, `server.go`, `checksum.go`, `testsupport_test.go`** — the handler + test patterns to mirror.
- **`segment/zstd.go`** — the zstd encoder/decoder idiom (`zstd.NewWriter` with `SpeedDefault` + `WithEncoderCRC(true)`; `EncodeAll`/`DecodeAll`; the `WithDecoderMaxMemory` zstd-bomb cap).
- **`internal/oracle/compacted.go`, `compacted_test.go`** — the oracle suppression model we extend.

### Conventions (from AGENTS.md)

- Use `just` recipes, not raw `go test`. One package: `just test ./internal/overlay`. One test: `just test ./internal/overlay -run TestName`. Fuzz: `just fuzz 30s ./internal/overlay`. Benchmarks: `just bench ./internal/overlay`. Lint+modernize via `just`. The default `just` target runs short tests.
- Tests must be fast (<1s per package in short mode). Heavy oracle paths gate behind `testing.Short()` and run under `just test-long` / `just oracle`.
- Never crash on external/decoded input; bound all lengths; loud crash only on internal corruption.
- Exported symbols get a docstring; comments explain *why*, not *what*.
- Commit frequently. Branch off `main` first (do not commit straight to `main`).

### NOTE on the post-commit hook

This repo has a hook that runs `git reset HEAD^` after a commit in some flows (observed during spec authoring: it un-records the commit but keeps changes in the working tree, so the *next* commit re-captures them and granularity collapses). After each `git commit` in this plan, run `git log --oneline -1` to confirm the commit actually landed; if HEAD did not advance, re-run the commit. Do **not** use `--no-verify` to bypass hooks.

### Task 0: Create the working branch

- [ ] **Step 1: Branch off main**

```bash
git checkout main && git pull --ff-only origin main
git checkout -b feat/gettombstones-overlay
```

- [ ] **Step 2: File the two follow-up issues now** (referenced by later commits/DESIGN.md)

```bash
gh issue create -t "client: jetstream v2 tombstone-overlay decoder + suppression applier" -b "$(cat <<'EOF'
## Context
`network.bsky.jetstream.getTombstones` (see docs/superpowers/specs/2026-06-15-getTombstones-overlay-design.md) ships the server-side overlay blob. There is no v2 client yet. The decoder + `ShouldDrop`-based suppression applier the oracle test contains (internal/oracle) is the reference implementation a real client library must mirror.

## Definition of done
A Go client decoder for the `jsto` v1 format that bounds all lengths against the buffer (hostile input), rejects unknown versions and trailing garbage, and an applier that suppresses segment rows per the §3.1 rule. Round-trips against the server encoder.

## Notes
Format spec: DESIGN.md §3.3 binary diagram. Test decoder lives in internal/overlay tests + internal/oracle today.
EOF
)" -l enhancement

gh issue create -t "subscribe/client: query-plan negotiation (segment list + cache directives + subscribe cursor = overlay maxSeq)" -b "$(cat <<'EOF'
## Context
The overlay endpoint reports W (watermark) and M (maxSeq). For gapless, leak-free coverage the future query plan MUST set the client's /subscribe cursor to the blob's actual M (NOT now()), and mark any segment that could still be rewritten under W as must-revalidate so a stale CDN/edge copy can't be spliced under the overlay. See design §3.3/§3.4 and §5.4.

## Definition of done
A negotiated plan handed to clients: {overlay (W,M), ordered segment list, per-segment cache directives, subscribe cursor = M}. Oracle seam test asserts no gap / no double-suppression at the W and M boundaries.

## Notes
This is the consumer half; the endpoint half is server-only and shipped separately.
EOF
)" -l enhancement
```

Record the two issue numbers; later steps reference them as `#CLIENT` and `#PLAN`.

- [ ] **Step 3: Commit nothing yet** — proceed to Task 1.

---

## File structure

**Create:**
- `lexicons/network/bsky/jetstream/getTombstones.json` — lexicon (parameterless query, octet-stream).
- `internal/overlay/doc.go` — package doc.
- `internal/overlay/format.go` — wire-format constants + the pure `Encode(snap, W, M) []byte` and a `decodeForTest` (round-trip reference).
- `internal/overlay/format_test.go` — encoder unit + property + fuzz + determinism tests.
- `internal/overlay/cache.go` — `Blob`, `Cache`, rebuild triggers.
- `internal/overlay/cache_test.go` — concurrency + debounce tests.
- `internal/overlay/metrics.go` — prometheus metrics.
- `internal/overlay/bench_test.go` — columnar-vs-flat compressibility benchmark.
- `internal/xrpcapi/gettombstones.go` — handler.
- `internal/xrpcapi/gettombstones_test.go` — handler integration tests.
- `internal/oracle/overlay.go` — overlay reconstruction + end-to-end check.
- `internal/oracle/overlay_test.go` — oracle determinism + no-permanent-tombstone + seam tests.
- `testing/mutation/mutants/m0XX-*.patch` (×4) — mutation campaign mutants.

**Modify:**
- `internal/xrpcapi/server.go` — register the new handler; thread an `OverlaySource`.
- `internal/jetstreamd/runtime.go` — construct `overlay.Cache`, hook `OnCompactionPass`, start the ticker, pass to xrpcapi.
- `internal/jetstreamd/runtime.go` (or its lifecycle goroutine group) — run the cache ticker under graceful shutdown.
- `api/jetstream/jetstreamgettombstones.go` — generated by lexgen (do not hand-edit).
- `DESIGN.md` §3.3 — wire-format diagram + coverage contract.
- `testing/mutation/RESULTS.md` and the mutant catalog — record the 4 new mutants.

---

## Task 1: Lexicon + generated types

**Files:**
- Create: `lexicons/network/bsky/jetstream/getTombstones.json`
- Generated: `api/jetstream/jetstreamgettombstones.go`

- [ ] **Step 1: Write the lexicon**

`lexicons/network/bsky/jetstream/getTombstones.json`:

```json
{
  "lexicon": 1,
  "id": "network.bsky.jetstream.getTombstones",
  "defs": {
    "main": {
      "type": "query",
      "description": "Download the current compaction overlay: the set of record deletions, record updates, and account/sync DID tombstones above the server's compaction watermark, as a compact zstd-compressed binary blob. A client overlays these on segment-file rows during backfill replay to suppress superseded records. The body is self-describing (see DESIGN.md section 3.3): a fixed 'jsto' framing carrying the watermark and maxSeq, followed by a single zstd frame. This endpoint is not CDN-cacheable in practice because the overlay changes continuously; it emits a strong ETag only for opportunistic revalidation.",
      "output": {
        "encoding": "application/octet-stream"
      }
    }
  }
}
```

- [ ] **Step 2: Generate Go types**

Run: `just lexgen`
Expected: creates `api/jetstream/jetstreamgettombstones.go` with a `JetstreamGetTombstones(ctx, c, ) ([]byte, error)` client helper (octet-stream → `QueryRaw`), no compile errors. Confirm with `git status` that the file appeared.

- [ ] **Step 3: Build to confirm generated code compiles**

Run: `just build` (or `go build ./...` if no build recipe)
Expected: success.

- [ ] **Step 4: Commit**

```bash
git add lexicons/network/bsky/jetstream/getTombstones.json api/jetstream/jetstreamgettombstones.go
git commit -m "lexicon: add network.bsky.jetstream.getTombstones query

Parameterless octet-stream query for the compaction overlay blob.
Refs #PLAN"
git log --oneline -1   # confirm HEAD advanced (see post-commit hook note)
```

---

## Task 2: Wire-format constants + encoder (pure function)

**Files:**
- Create: `internal/overlay/doc.go`, `internal/overlay/format.go`
- Test: `internal/overlay/format_test.go`

The encoder is a pure function with no I/O, so we TDD it via round-trip: write a `decodeForTest` that inverts `Encode`, and assert `decode(encode(x)) == x`. The test decoder is the reference for the future client (issue #CLIENT).

- [ ] **Step 1: Write the package doc**

`internal/overlay/doc.go`:

```go
// Package overlay builds and caches the compaction overlay blob served
// by network.bsky.jetstream.getTombstones. The blob is a compact,
// zstd-compressed binary serialization of the in-memory tombstone set
// (internal/tombstone) covering the seq range (W, M], where W is the
// compaction watermark and M is the highest seq folded in.
//
// The blob is precomputed and immutable once published: every reader
// shares the same backing bytes with zero per-request CPU. A cached
// blob may lag the live tip by one rebuild interval, but it is never
// invalid data — it is an atomic point-in-time snapshot that reports
// the exact W and M it covers, and the query-plan contract resumes the
// live tail from that M so coverage stays gapless. See
// docs/superpowers/specs/2026-06-15-getTombstones-overlay-design.md §5.4.
package overlay
```

- [ ] **Step 2: Write the failing round-trip + determinism test**

`internal/overlay/format_test.go`:

```go
package overlay

import (
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/tombstone"
	"github.com/stretchr/testify/require"
)

func sampleSnapshot() tombstone.Snapshot {
	return tombstone.Snapshot{
		Records: map[tombstone.RecordKey]uint64{
			{DID: "did:plc:aaa", Collection: "app.bsky.feed.post", Rkey: "r1"}: 110,
			{DID: "did:plc:aaa", Collection: "app.bsky.feed.like", Rkey: "r2"}: 130,
			{DID: "did:plc:bbb", Collection: "app.bsky.feed.post", Rkey: "r3"}: 150,
		},
		DIDs: map[string]tombstone.DIDTombstone{
			"did:plc:ccc": {Seq: 120, Reason: "account"},
			"did:plc:ddd": {Seq: 140, Reason: "sync"},
		},
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	t.Parallel()
	const W, M = uint64(100), uint64(150)
	blob := Encode(sampleSnapshot(), W, M)

	gotW, gotM, gotSnap, err := decodeForTest(blob)
	require.NoError(t, err)
	require.Equal(t, W, gotW)
	require.Equal(t, M, gotM)
	require.Equal(t, sampleSnapshot(), gotSnap)
}

func TestEncodeDeterministic(t *testing.T) {
	t.Parallel()
	a := Encode(sampleSnapshot(), 100, 150)
	b := Encode(sampleSnapshot(), 100, 150)
	require.Equal(t, a, b, "same snapshot must produce byte-identical blobs")
}

func TestEncodeEmpty(t *testing.T) {
	t.Parallel()
	blob := Encode(tombstone.Snapshot{
		Records: map[tombstone.RecordKey]uint64{},
		DIDs:    map[string]tombstone.DIDTombstone{},
	}, 200, 200)
	w, m, snap, err := decodeForTest(blob)
	require.NoError(t, err)
	require.Equal(t, uint64(200), w)
	require.Equal(t, uint64(200), m)
	require.True(t, snap.Empty())
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `just test ./internal/overlay -run TestEncode`
Expected: FAIL — `Encode`/`decodeForTest` undefined (compile error).

- [ ] **Step 4: Implement the format**

`internal/overlay/format.go`. The layout matches DESIGN.md §3.3. All integers little-endian; varint = `encoding/binary` `Uvarint`/`PutUvarint`. Reason enum: `account=1`, `sync=2`.

```go
package overlay

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/bluesky-social/jetstream-v2/internal/tombstone"
	"github.com/klauspost/compress/zstd"
)

const (
	magic        = "jsto"
	formatVer    = uint16(1)
	reasonAcct   = uint8(1)
	reasonSync   = uint8(2)
	frameHdrSize = 4 + 2 + 2 + 8 + 8 + 8 // magic, ver, flags, W, M, body_len

	// maxDecodedOverlayBytes caps the decoder's decompressed size to
	// stop a hostile/corrupt frame from ballooning memory (zstd bomb),
	// mirroring segment/zstd.go. The overlay is bounded by the tombstone
	// cap (~32M entries); 1 GiB leaves generous headroom.
	maxDecodedOverlayBytes uint64 = 1 << 30
)

var (
	overlayEncoder *zstd.Encoder
	overlayDecoder *zstd.Decoder
)

func init() {
	var err error
	overlayEncoder, err = zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderCRC(true),
	)
	if err != nil {
		panic(fmt.Sprintf("overlay: zstd encoder init: %v", err))
	}
	overlayDecoder, err = zstd.NewReader(nil,
		zstd.WithDecoderMaxMemory(maxDecodedOverlayBytes),
	)
	if err != nil {
		panic(fmt.Sprintf("overlay: zstd decoder init: %v", err))
	}
}

func reasonCode(s string) uint8 {
	if s == "sync" {
		return reasonSync
	}
	return reasonAcct // tombstone.observeLocked only ever sets "account" or "sync"
}

func reasonString(c uint8) (string, error) {
	switch c {
	case reasonAcct:
		return "account", nil
	case reasonSync:
		return "sync", nil
	default:
		return "", fmt.Errorf("overlay: unknown reason code %d", c)
	}
}

// Encode serializes snap into the jsto v1 wire format: a fixed framing
// carrying W and M, then a single zstd frame holding dictionary tables
// and columnar, delta-varint tombstone sections. Pure and deterministic:
// the same snapshot always yields identical bytes.
func Encode(snap tombstone.Snapshot, w, m uint64) []byte {
	body := encodeBody(snap, w)
	frame := overlayEncoder.EncodeAll(body, nil)

	out := make([]byte, frameHdrSize+len(frame))
	copy(out[0:4], magic)
	binary.LittleEndian.PutUint16(out[4:6], formatVer)
	binary.LittleEndian.PutUint16(out[6:8], 0) // flags reserved
	binary.LittleEndian.PutUint64(out[8:16], w)
	binary.LittleEndian.PutUint64(out[16:24], m)
	binary.LittleEndian.PutUint64(out[24:32], uint64(len(frame)))
	copy(out[frameHdrSize:], frame)
	return out
}

// encodeBody builds the uncompressed columnar body. seqBase is W (the
// first seq delta in every group/section is seq-W).
func encodeBody(snap tombstone.Snapshot, seqBase uint64) []byte {
	// Build dictionaries with deterministic ordering: DIDs sorted, then
	// collections sorted. Record tombstones are grouped by DID.
	type recEntry struct {
		coll string
		rkey string
		seq  uint64
	}
	byDID := make(map[string][]recEntry)
	didSet := make(map[string]struct{})
	collSet := make(map[string]struct{})
	for k, seq := range snap.Records {
		byDID[k.DID] = append(byDID[k.DID], recEntry{coll: k.Collection, rkey: k.Rkey, seq: seq})
		didSet[k.DID] = struct{}{}
		collSet[k.Collection] = struct{}{}
	}
	for did := range snap.DIDs {
		didSet[did] = struct{}{}
	}

	dids := sortedKeys(didSet)
	colls := sortedKeys(collSet)
	didID := indexMap(dids)
	collID := indexMap(colls)

	var buf []byte
	appendStringTable := func(items []string) {
		buf = appendUvarint(buf, uint64(len(items)))
		for _, s := range items {
			buf = appendUvarint(buf, uint64(len(s)))
			buf = append(buf, s...)
		}
	}
	appendStringTable(dids)
	appendStringTable(colls)

	// Record tombstones, grouped by didID ascending; within a group,
	// entries sorted by seq and seq delta-encoded (first delta vs W).
	groupDIDs := make([]string, 0, len(byDID))
	for did := range byDID {
		groupDIDs = append(groupDIDs, did)
	}
	sort.Slice(groupDIDs, func(i, j int) bool { return didID[groupDIDs[i]] < didID[groupDIDs[j]] })

	buf = appendUvarint(buf, uint64(len(groupDIDs)))
	for _, did := range groupDIDs {
		entries := byDID[did]
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].seq != entries[j].seq {
				return entries[i].seq < entries[j].seq
			}
			if entries[i].coll != entries[j].coll {
				return entries[i].coll < entries[j].coll
			}
			return entries[i].rkey < entries[j].rkey
		})
		buf = appendUvarint(buf, uint64(didID[did]))
		buf = appendUvarint(buf, uint64(len(entries)))
		prev := seqBase
		for _, e := range entries {
			buf = appendUvarint(buf, uint64(collID[e.coll]))
			buf = appendUvarint(buf, uint64(len(e.rkey)))
			buf = append(buf, e.rkey...)
			buf = appendUvarint(buf, e.seq-prev)
			prev = e.seq
		}
	}

	// DID tombstones, ascending by didID, seq delta-encoded vs W then prev.
	didTombDIDs := make([]string, 0, len(snap.DIDs))
	for did := range snap.DIDs {
		didTombDIDs = append(didTombDIDs, did)
	}
	sort.Slice(didTombDIDs, func(i, j int) bool { return didID[didTombDIDs[i]] < didID[didTombDIDs[j]] })

	buf = appendUvarint(buf, uint64(len(didTombDIDs)))
	prev := seqBase
	for _, did := range didTombDIDs {
		ts := snap.DIDs[did]
		buf = appendUvarint(buf, uint64(didID[did]))
		buf = appendUvarint(buf, ts.Seq-prev)
		buf = append(buf, reasonCode(ts.Reason))
		prev = ts.Seq
	}
	return buf
}

func sortedKeys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func indexMap(items []string) map[string]int {
	m := make(map[string]int, len(items))
	for i, s := range items {
		m[s] = i
	}
	return m
}

func appendUvarint(b []byte, v uint64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	return append(b, tmp[:n]...)
}

// errMalformed is returned by the decoder for any structurally invalid
// blob. The decoder never panics on hostile input (issue #CLIENT relies
// on this contract).
var errMalformed = errors.New("overlay: malformed blob")
```

- [ ] **Step 5: Implement `decodeForTest` (reference decoder)**

Append to `format.go` (it is exported-for-test-only via lowercase name; the real client decoder is issue #CLIENT). It must bound every length read against the remaining buffer and return `errMalformed` rather than panic.

```go
// decodeForTest inverts Encode. It is the reference decoder used by
// tests and the oracle; it bounds all lengths against the buffer and
// never panics on malformed input. The production client decoder
// (issue #CLIENT) will mirror this logic.
func decodeForTest(blob []byte) (w, m uint64, snap tombstone.Snapshot, err error) {
	if len(blob) < frameHdrSize {
		return 0, 0, tombstone.Snapshot{}, fmt.Errorf("%w: short header", errMalformed)
	}
	if string(blob[0:4]) != magic {
		return 0, 0, tombstone.Snapshot{}, fmt.Errorf("%w: bad magic", errMalformed)
	}
	if ver := binary.LittleEndian.Uint16(blob[4:6]); ver != formatVer {
		return 0, 0, tombstone.Snapshot{}, fmt.Errorf("%w: unsupported version %d", errMalformed, ver)
	}
	w = binary.LittleEndian.Uint64(blob[8:16])
	m = binary.LittleEndian.Uint64(blob[16:24])
	bodyLen := binary.LittleEndian.Uint64(blob[24:32])
	if uint64(len(blob)-frameHdrSize) != bodyLen {
		return 0, 0, tombstone.Snapshot{}, fmt.Errorf("%w: body length mismatch", errMalformed)
	}
	body, derr := overlayDecoder.DecodeAll(blob[frameHdrSize:], nil)
	if derr != nil {
		return 0, 0, tombstone.Snapshot{}, fmt.Errorf("%w: zstd: %v", errMalformed, derr)
	}

	snap = tombstone.Snapshot{
		Records: map[tombstone.RecordKey]uint64{},
		DIDs:    map[string]tombstone.DIDTombstone{},
	}
	c := &cursor{buf: body}

	dids, err := c.stringTable()
	if err != nil {
		return 0, 0, tombstone.Snapshot{}, err
	}
	colls, err := c.stringTable()
	if err != nil {
		return 0, 0, tombstone.Snapshot{}, err
	}

	groupCount, err := c.uvarint()
	if err != nil {
		return 0, 0, tombstone.Snapshot{}, err
	}
	for g := uint64(0); g < groupCount; g++ {
		didIdx, err := c.uvarint()
		if err != nil {
			return 0, 0, tombstone.Snapshot{}, err
		}
		if didIdx >= uint64(len(dids)) {
			return 0, 0, tombstone.Snapshot{}, fmt.Errorf("%w: did index out of range", errMalformed)
		}
		entryCount, err := c.uvarint()
		if err != nil {
			return 0, 0, tombstone.Snapshot{}, err
		}
		prev := w
		for e := uint64(0); e < entryCount; e++ {
			collIdx, err := c.uvarint()
			if err != nil {
				return 0, 0, tombstone.Snapshot{}, err
			}
			if collIdx >= uint64(len(colls)) {
				return 0, 0, tombstone.Snapshot{}, fmt.Errorf("%w: coll index out of range", errMalformed)
			}
			rkey, err := c.lenBytes()
			if err != nil {
				return 0, 0, tombstone.Snapshot{}, err
			}
			delta, err := c.uvarint()
			if err != nil {
				return 0, 0, tombstone.Snapshot{}, err
			}
			seq := prev + delta
			prev = seq
			snap.Records[tombstone.RecordKey{DID: dids[didIdx], Collection: colls[collIdx], Rkey: string(rkey)}] = seq
		}
	}

	didTombCount, err := c.uvarint()
	if err != nil {
		return 0, 0, tombstone.Snapshot{}, err
	}
	prev := w
	for i := uint64(0); i < didTombCount; i++ {
		didIdx, err := c.uvarint()
		if err != nil {
			return 0, 0, tombstone.Snapshot{}, err
		}
		if didIdx >= uint64(len(dids)) {
			return 0, 0, tombstone.Snapshot{}, fmt.Errorf("%w: did index out of range", errMalformed)
		}
		delta, err := c.uvarint()
		if err != nil {
			return 0, 0, tombstone.Snapshot{}, err
		}
		rc, err := c.byteVal()
		if err != nil {
			return 0, 0, tombstone.Snapshot{}, err
		}
		reason, err := reasonString(rc)
		if err != nil {
			return 0, 0, tombstone.Snapshot{}, err
		}
		seq := prev + delta
		prev = seq
		snap.DIDs[dids[didIdx]] = tombstone.DIDTombstone{Seq: seq, Reason: reason}
	}

	if c.off != len(c.buf) {
		return 0, 0, tombstone.Snapshot{}, fmt.Errorf("%w: trailing bytes", errMalformed)
	}
	return w, m, snap, nil
}

type cursor struct {
	buf []byte
	off int
}

func (c *cursor) uvarint() (uint64, error) {
	v, n := binary.Uvarint(c.buf[c.off:])
	if n <= 0 {
		return 0, fmt.Errorf("%w: bad uvarint", errMalformed)
	}
	c.off += n
	return v, nil
}

func (c *cursor) byteVal() (uint8, error) {
	if c.off >= len(c.buf) {
		return 0, fmt.Errorf("%w: eof reading byte", errMalformed)
	}
	b := c.buf[c.off]
	c.off++
	return b, nil
}

func (c *cursor) lenBytes() ([]byte, error) {
	n, err := c.uvarint()
	if err != nil {
		return nil, err
	}
	if uint64(c.off)+n > uint64(len(c.buf)) {
		return nil, fmt.Errorf("%w: length exceeds buffer", errMalformed)
	}
	b := c.buf[c.off : c.off+int(n)]
	c.off += int(n)
	return b, nil
}

func (c *cursor) stringTable() ([]string, error) {
	count, err := c.uvarint()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, count)
	for i := uint64(0); i < count; i++ {
		b, err := c.lenBytes()
		if err != nil {
			return nil, err
		}
		out = append(out, string(b))
	}
	return out, nil
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `just test ./internal/overlay -run TestEncode`
Expected: PASS for `TestEncodeDecodeRoundTrip`, `TestEncodeDeterministic`, `TestEncodeEmpty`.

- [ ] **Step 7: Commit**

```bash
git add internal/overlay/doc.go internal/overlay/format.go internal/overlay/format_test.go
git commit -m "overlay: jsto v1 wire format encoder + reference decoder

Columnar + dictionary + delta-varint body, zstd-framed; W/M in the
uncompressed framing. Pure, deterministic encoder. Refs #PLAN"
git log --oneline -1
```

---

## Task 3: Encoder property + fuzz + decode-safety tests

**Files:**
- Test: `internal/overlay/format_test.go` (extend)

- [ ] **Step 1: Add the property test (round-trip fidelity over random snapshots)**

Append to `format_test.go`:

```go
import (
	"testing/quick"
	// ... existing imports
)

func TestEncodeRoundTripProperty(t *testing.T) {
	t.Parallel()
	f := func(seed uint32) bool {
		snap, w, m := randomSnapshot(seed)
		blob := Encode(snap, w, m)
		gw, gm, gs, err := decodeForTest(blob)
		if err != nil || gw != w || gm != m {
			return false
		}
		return snapshotsEqual(snap, gs)
	}
	require.NoError(t, quick.Check(f, &quick.Config{MaxCount: 500}))
}
```

Add deterministic-from-seed helpers (no `math/rand` global; derive from the seed so failures reproduce). Cover adversarial cases: empty rkey, long rkey, non-UTF8/NUL bytes in rkey, a DID present in both `Records` and `DIDs`, seqs equal to `W+1` and to `M`:

```go
func randomSnapshot(seed uint32) (tombstone.Snapshot, uint64, uint64) {
	r := uint64(seed)*2862933555777941757 + 3037000493 // splitmix-ish
	next := func() uint64 { r ^= r >> 12; r ^= r << 25; r ^= r >> 27; return r * 2685821657736338717 }

	w := next() % 1000
	snap := tombstone.Snapshot{
		Records: map[tombstone.RecordKey]uint64{},
		DIDs:    map[string]tombstone.DIDTombstone{},
	}
	maxSeq := w
	nDID := int(next()%5) + 1
	rkeys := []string{"", "rk", string([]byte{0x00, 0xff, 0x80}), "aaaaaaaaaaaaaaaaaaaa"}
	colls := []string{"app.bsky.feed.post", "app.bsky.feed.like", "x"}
	for i := 0; i < nDID; i++ {
		did := "did:plc:" + string(rune('a'+i))
		nRec := int(next() % 4)
		for j := 0; j < nRec; j++ {
			seq := w + 1 + next()%500
			if seq > maxSeq {
				maxSeq = seq
			}
			snap.Records[tombstone.RecordKey{
				DID: did, Collection: colls[next()%uint64(len(colls))], Rkey: rkeys[next()%uint64(len(rkeys))],
			}] = seq
		}
		if next()%2 == 0 {
			seq := w + 1 + next()%500
			if seq > maxSeq {
				maxSeq = seq
			}
			reason := "account"
			if next()%2 == 0 {
				reason = "sync"
			}
			snap.DIDs[did] = tombstone.DIDTombstone{Seq: seq, Reason: reason}
		}
	}
	return snap, w, maxSeq
}

func snapshotsEqual(a, b tombstone.Snapshot) bool {
	if len(a.Records) != len(b.Records) || len(a.DIDs) != len(b.DIDs) {
		return false
	}
	for k, v := range a.Records {
		if b.Records[k] != v {
			return false
		}
	}
	for k, v := range a.DIDs {
		if b.DIDs[k] != v {
			return false
		}
	}
	return true
}
```

- [ ] **Step 2: Run the property test**

Run: `just test ./internal/overlay -run TestEncodeRoundTripProperty`
Expected: PASS. If it fails, the seed in the failure output reproduces it deterministically — fix the encoder/decoder, do not weaken the test.

- [ ] **Step 3: Add the decode-safety fuzz target**

```go
func FuzzDecodeForTest(f *testing.F) {
	f.Add(Encode(sampleSnapshot(), 100, 150))
	f.Add([]byte("jsto"))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, blob []byte) {
		// Must never panic; any structurally invalid blob returns an error.
		_, _, _, _ = decodeForTest(blob)
	})
}
```

- [ ] **Step 4: Run the fuzzer briefly**

Run: `just fuzz 30s ./internal/overlay`
Expected: no panics, no new crashers. (CI runs short; this is a local gate.)

- [ ] **Step 5: Commit**

```bash
git add internal/overlay/format_test.go
git commit -m "overlay: property + decode-safety fuzz tests for jsto format

Round-trip over random snapshots (adversarial rkeys, dup DIDs, W/M-edge
seqs); fuzz the decoder for panic-freedom on hostile input. Refs #PLAN"
git log --oneline -1
```

---

## Task 4: Metrics

**Files:**
- Create: `internal/overlay/metrics.go`

Mirror the nil-safe metrics pattern from `internal/ingest/orchestrator/metrics.go` (a nil `*Metrics` is valid; every method is a no-op).

- [ ] **Step 1: Implement metrics**

`internal/overlay/metrics.go`:

```go
package overlay

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricsNamespace = "jetstream"
	metricsSubsystem = "overlay"
)

// Metrics owns the prometheus state for the overlay cache. A nil
// *Metrics is valid: every method is a no-op so tests can skip
// registration.
type Metrics struct {
	BlobBytes      prometheus.Gauge
	BuildRecords   prometheus.Gauge
	BuildDIDs      prometheus.Gauge
	Rebuilds       prometheus.Counter
	RebuildSeconds prometheus.Histogram
	Requests       prometheus.Counter
	ServeBytes     prometheus.Counter
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		BlobBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "blob_bytes", Help: "Size of the current overlay blob in bytes.",
		}),
		BuildRecords: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "build_records", Help: "Record tombstones in the current overlay blob.",
		}),
		BuildDIDs: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "build_dids", Help: "DID tombstones in the current overlay blob.",
		}),
		Rebuilds: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "rebuilds_total", Help: "Total overlay blob rebuilds.",
		}),
		RebuildSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "rebuild_duration_seconds", Help: "Overlay blob build (encode+compress) latency.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 12),
		}),
		Requests: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "requests_total", Help: "Total getTombstones requests served.",
		}),
		ServeBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "serve_bytes_total", Help: "Total overlay bytes written to clients.",
		}),
	}
	reg.MustRegister(m.BlobBytes, m.BuildRecords, m.BuildDIDs, m.Rebuilds, m.RebuildSeconds, m.Requests, m.ServeBytes)
	return m
}

func (m *Metrics) observeBuild(d time.Duration, blobBytes, records, dids int) {
	if m == nil {
		return
	}
	m.Rebuilds.Inc()
	m.RebuildSeconds.Observe(d.Seconds())
	m.BlobBytes.Set(float64(blobBytes))
	m.BuildRecords.Set(float64(records))
	m.BuildDIDs.Set(float64(dids))
}

func (m *Metrics) observeServe(n int) {
	if m == nil {
		return
	}
	m.Requests.Inc()
	m.ServeBytes.Add(float64(n))
}
```

- [ ] **Step 2: Build**

Run: `just build`
Expected: success (no test yet; metrics are exercised via the cache tests).

- [ ] **Step 3: Commit**

```bash
git add internal/overlay/metrics.go
git commit -m "overlay: prometheus metrics for blob size, rebuilds, serves

Refs #PLAN"
git log --oneline -1
```

---

## Task 5: The cache (Blob, build, rebuild triggers)

**Files:**
- Create: `internal/overlay/cache.go`
- Test: `internal/overlay/cache_test.go`

The cache needs a watermark source and a snapshot source. To keep `overlay` transport- and store-agnostic, define small interfaces it depends on (the concrete `*tombstone.Set` and a watermark closure satisfy them).

- [ ] **Step 1: Write the failing cache test**

`internal/overlay/cache_test.go`:

```go
package overlay

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/tombstone"
	"github.com/stretchr/testify/require"
)

// fakeSource is a tombstone source + watermark for tests.
type fakeSource struct {
	mu   sync.Mutex
	set  *tombstone.Set
	wm   uint64
	gen  atomic.Uint64 // bumps on every mutation, drives dirtiness
}

func (f *fakeSource) SnapshotRange(low, high uint64) tombstone.Snapshot {
	return f.set.SnapshotRange(low, high)
}
func (f *fakeSource) Watermark() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.wm
}
func (f *fakeSource) Dirty() uint64 { return f.gen.Load() }

func TestCacheBuildsOnFirstAccess(t *testing.T) {
	t.Parallel()
	set := tombstone.New()
	require.NoError(t, set.Observe(&segEvt(t, 110, "did:plc:a", "app.bsky.feed.post", "r1")))
	src := &fakeSource{set: set, wm: 100}
	src.gen.Store(1)

	c := NewCache(src, nil)
	blob := c.Current()
	require.NotNil(t, blob)
	require.Equal(t, uint64(100), blob.Watermark)
	require.Equal(t, uint64(110), blob.MaxSeq)
	require.NotEmpty(t, blob.ETag)

	w, m, snap, err := decodeForTest(blob.Bytes)
	require.NoError(t, err)
	require.Equal(t, uint64(100), w)
	require.Equal(t, uint64(110), m)
	require.Len(t, snap.Records, 1)
}

func TestCacheRebuildOnlyWhenDirty(t *testing.T) {
	t.Parallel()
	set := tombstone.New()
	src := &fakeSource{set: set, wm: 0}
	src.gen.Store(1)
	c := NewCache(src, nil)

	first := c.Current()
	// No mutation -> Rebuild is a no-op, same blob pointer.
	c.Rebuild()
	require.Same(t, first, c.Current())

	// Mutate -> dirty -> rebuild swaps.
	require.NoError(t, set.Observe(&segEvt(t, 5, "did:plc:a", "c", "r")))
	src.gen.Add(1)
	c.Rebuild()
	require.NotSame(t, first, c.Current())
}

func TestCacheConcurrentServeAndRebuild(t *testing.T) {
	t.Parallel()
	set := tombstone.New()
	src := &fakeSource{set: set, wm: 0}
	src.gen.Store(1)
	c := NewCache(src, nil)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					b := c.Current()
					_, _, _, err := decodeForTest(b.Bytes)
					require.NoError(t, err)
				}
			}
		}()
	}
	for i := 0; i < 200; i++ {
		require.NoError(t, set.Observe(&segEvt(t, uint64(i+1), "did:plc:a", "c", "r"+string(rune('a'+i%26)))))
		src.gen.Add(1)
		c.Rebuild()
	}
	close(stop)
	wg.Wait()
}
```

Add a small `segEvt` helper to `cache_test.go` (a `segment.Event` value; `set.Observe` takes `*segment.Event`):

```go
import "github.com/bluesky-social/jetstream-v2/segment"

func segEvt(t *testing.T, seq uint64, did, coll, rkey string) segment.Event {
	t.Helper()
	return segment.Event{Seq: seq, Kind: segment.KindDelete, DID: did, Collection: coll, Rkey: rkey}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `just test ./internal/overlay -run TestCache`
Expected: FAIL — `NewCache`, `Cache.Current`, `Cache.Rebuild`, `Blob` undefined.

- [ ] **Step 3: Implement the cache**

`internal/overlay/cache.go`:

```go
package overlay

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/tombstone"
	"github.com/zeebo/xxh3"
)

// Source supplies the data the overlay blob is built from. The concrete
// *tombstone.Set plus a watermark accessor satisfy it via a thin adapter
// constructed in runtime.go.
type Source interface {
	// SnapshotRange returns tombstones in (lowExclusive, highInclusive].
	SnapshotRange(lowExclusive, highInclusive uint64) tombstone.Snapshot
	// Watermark is the current compaction/seq (the blob's W floor).
	Watermark() uint64
	// Dirty returns a value that changes whenever the underlying set
	// mutates. The cache rebuilds only when this differs from the value
	// captured at the last build.
	Dirty() uint64
}

// Blob is one immutable, serialized overlay. Never mutated after publish;
// concurrent readers share Bytes.
type Blob struct {
	Bytes      []byte
	ETag       string
	Watermark  uint64
	MaxSeq     uint64
	NumRecords int
	NumDIDs    int
	dirtyAt    uint64 // Source.Dirty() value this blob was built from
}

// Cache holds the latest published *Blob and rebuilds it from Source.
type Cache struct {
	src     Source
	metrics *Metrics

	mu  sync.RWMutex
	cur *Blob
}

// NewCache builds an initial blob immediately so Current never returns nil.
func NewCache(src Source, m *Metrics) *Cache {
	c := &Cache{src: src, metrics: m}
	c.cur = c.build()
	return c
}

// Current returns the latest published blob. Safe for concurrent use; the
// returned *Blob is immutable.
func (c *Cache) Current() *Blob {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cur
}

// Rebuild rebuilds and publishes a new blob if the source has changed
// since the last build; otherwise it is a cheap no-op. Called from the
// compaction-pass hook and the background ticker.
func (c *Cache) Rebuild() {
	c.mu.RLock()
	prev := c.cur
	c.mu.RUnlock()
	if prev != nil && c.src.Dirty() == prev.dirtyAt {
		return
	}
	next := c.build()
	c.mu.Lock()
	c.cur = next
	c.mu.Unlock()
}

// build snapshots the source and serializes a new blob. The snapshot is
// taken under the set's own lock (inside SnapshotRange) and released
// before encode+compress, so a slow build never blocks ingestion.
func (c *Cache) build() *Blob {
	start := time.Now()
	dirty := c.src.Dirty()
	w := c.src.Watermark()
	snap := c.src.SnapshotRange(w, ^uint64(0))

	m := w
	for _, seq := range snap.Records {
		if seq > m {
			m = seq
		}
	}
	for _, ts := range snap.DIDs {
		if ts.Seq > m {
			m = ts.Seq
		}
	}

	bytes := Encode(snap, w, m)
	blob := &Blob{
		Bytes:      bytes,
		ETag:       fmt.Sprintf("%q", fmt.Sprintf("%016x", xxh3.Hash(bytes))),
		Watermark:  w,
		MaxSeq:     m,
		NumRecords: len(snap.Records),
		NumDIDs:    len(snap.DIDs),
		dirtyAt:    dirty,
	}
	c.metrics.observeBuild(time.Since(start), len(bytes), blob.NumRecords, blob.NumDIDs)
	return blob
}

// RunTicker rebuilds on a coalescing interval until ctx is cancelled. It
// bounds blob staleness to ~interval; Rebuild itself skips when the source
// is unchanged, so an idle firehose costs nothing. Run as a lifecycle
// goroutine.
func (c *Cache) RunTicker(ctx context.Context, interval time.Duration) error {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			c.Rebuild()
		}
	}
}

func (c *Cache) observeServe(n int) { c.metrics.observeServe(n) }
```

Note: `dirtyAt` captured *before* the snapshot means a mutation racing the build only risks one extra (harmless) rebuild next tick — never a missed one.

- [ ] **Step 4: Run cache tests under the race detector**

Run: `just test ./internal/overlay -run TestCache` (the project's `test` recipe runs `-race`; if unsure, `go test -race ./internal/overlay -run TestCache`)
Expected: PASS, no race warnings.

- [ ] **Step 5: Commit**

```bash
git add internal/overlay/cache.go internal/overlay/cache_test.go
git commit -m "overlay: in-memory blob cache with dirty-driven rebuild

Immutable published blob shared by all readers; Rebuild is a no-op when
the source is unchanged; RunTicker bounds staleness. Refs #PLAN"
git log --oneline -1
```

---

## Task 6: Compressibility benchmark (the §4 columnar-vs-flat gate)

**Files:**
- Create: `internal/overlay/bench_test.go`

This produces the number that decides whether to keep columnar. Implement a minimal flat encoder *in the benchmark file only* (sorted length-prefixed rows + zstd) to compare against `Encode`.

- [ ] **Step 1: Write the benchmark**

`internal/overlay/bench_test.go`:

```go
package overlay

import (
	"encoding/binary"
	"fmt"
	"sort"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/tombstone"
)

// realisticSnapshot builds n record tombstones with a Zipfian-ish DID skew
// (a few hot DIDs), a small set of hot collections, and TID-shaped rkeys.
func realisticSnapshot(n int, w uint64) (tombstone.Snapshot, uint64) {
	colls := []string{"app.bsky.feed.like", "app.bsky.feed.post", "app.bsky.graph.follow", "app.bsky.feed.repost"}
	snap := tombstone.Snapshot{Records: map[tombstone.RecordKey]uint64{}, DIDs: map[string]tombstone.DIDTombstone{}}
	m := w
	r := uint64(0x9e3779b97f4a7c15)
	next := func() uint64 { r ^= r >> 12; r ^= r << 25; r ^= r >> 27; return r * 2685821657736338717 }
	nDID := n/20 + 1
	for i := 0; i < n; i++ {
		didN := next() % uint64(nDID)
		if next()%3 == 0 { // skew: a third land on the 16 hottest DIDs
			didN %= 16
		}
		did := fmt.Sprintf("did:plc:%013x", didN)
		rkey := fmt.Sprintf("3k%011x", next()%0xffffffffff)
		seq := w + 1 + next()%uint64(n*4+1)
		if seq > m {
			m = seq
		}
		snap.Records[tombstone.RecordKey{DID: did, Collection: colls[next()%uint64(len(colls))], Rkey: rkey}] = seq
	}
	return snap, m
}

// encodeFlat is the alternative layout: sorted length-prefixed rows, zstd.
func encodeFlat(snap tombstone.Snapshot, w, m uint64) []byte {
	type row struct{ did, coll, rkey string; seq uint64 }
	rows := make([]row, 0, len(snap.Records))
	for k, seq := range snap.Records {
		rows = append(rows, row{k.DID, k.Collection, k.Rkey, seq})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].did != rows[j].did {
			return rows[i].did < rows[j].did
		}
		if rows[i].coll != rows[j].coll {
			return rows[i].coll < rows[j].coll
		}
		return rows[i].rkey < rows[j].rkey
	})
	var body []byte
	for _, r := range rows {
		body = appendUvarint(body, uint64(len(r.did)))
		body = append(body, r.did...)
		body = appendUvarint(body, uint64(len(r.coll)))
		body = append(body, r.coll...)
		body = appendUvarint(body, uint64(len(r.rkey)))
		body = append(body, r.rkey...)
		body = appendUvarint(body, r.seq)
	}
	frame := overlayEncoder.EncodeAll(body, nil)
	hdr := make([]byte, frameHdrSize)
	binary.LittleEndian.PutUint64(hdr[24:32], uint64(len(frame)))
	return append(hdr, frame...)
}

func BenchmarkEncodeColumnarVsFlat(b *testing.B) {
	for _, n := range []int{1_000, 100_000, 1_000_000} {
		snap, m := realisticSnapshot(n, 1_000_000)
		b.Run(fmt.Sprintf("columnar/%d", n), func(b *testing.B) {
			var sz int
			for i := 0; i < b.N; i++ {
				sz = len(Encode(snap, 1_000_000, m))
			}
			b.ReportMetric(float64(sz), "wire_bytes")
			b.ReportMetric(float64(sz)/float64(n), "bytes/tombstone")
		})
		b.Run(fmt.Sprintf("flat/%d", n), func(b *testing.B) {
			var sz int
			for i := 0; i < b.N; i++ {
				sz = len(encodeFlat(snap, 1_000_000, m))
			}
			b.ReportMetric(float64(sz), "wire_bytes")
			b.ReportMetric(float64(sz)/float64(n), "bytes/tombstone")
		})
	}
}
```

- [ ] **Step 2: Run the benchmark and record the result**

Run: `just bench ./internal/overlay` (or `go test -bench=BenchmarkEncodeColumnarVsFlat -benchmem ./internal/overlay`)
Expected: completes; compare `wire_bytes` for columnar vs flat at each size.

- [ ] **Step 3: Decide and document**

If columnar wins on `wire_bytes` (expected, especially at 1M), keep it and record the numbers in the spec's §9 / a short note in the commit body. If flat wins or ties within ~5% at all sizes, **stop and report to the user** before ripping out columnar — that is a design-affecting result worth a checkpoint.

- [ ] **Step 4: Commit**

```bash
git add internal/overlay/bench_test.go
git commit -m "overlay: columnar-vs-flat compressibility benchmark

Records wire bytes/tombstone at 1k/100k/1M on a realistic skewed
distribution; gates the layout choice. Refs #PLAN"
git log --oneline -1
```

---

## Task 7: XRPC handler + registration

**Files:**
- Create: `internal/xrpcapi/gettombstones.go`
- Test: `internal/xrpcapi/gettombstones_test.go`
- Modify: `internal/xrpcapi/server.go`

The handler serves the cached blob via `RawQuery` (it writes raw bytes, like getSegment writes raw file content but simpler — no Range needed; the body is small enough and changes constantly).

- [ ] **Step 1: Add an OverlaySource interface + handler**

`internal/xrpcapi/gettombstones.go`:

```go
package xrpcapi

import (
	"context"
	"net/http"

	"github.com/bluesky-social/jetstream-v2/internal/overlay"
	"github.com/jcalabro/atmos/xrpcserver"
)

// OverlaySource is the read-only surface the getTombstones handler needs.
// *overlay.Cache satisfies it; tests pass a fake.
type OverlaySource interface {
	Current() *overlay.Blob
}

func newGetTombstonesHandler(src OverlaySource) xrpcserver.Handler {
	return xrpcserver.RawQuery(func(ctx context.Context, p xrpcserver.Params, w http.ResponseWriter) error {
		blob := src.Current()
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("ETag", blob.ETag)
		// Not CDN-cacheable in practice (the overlay changes continuously);
		// no-cache lets the occasional If-None-Match revalidation still 304.
		w.Header().Set("Cache-Control", "no-cache")
		// Surface W/M as headers so the future query plan can read the
		// coverage envelope without decompressing the body.
		w.Header().Set("Jetstream-Overlay-Watermark", itoa(blob.Watermark))
		w.Header().Set("Jetstream-Overlay-Max-Seq", itoa(blob.MaxSeq))
		_, _ = w.Write(blob.Bytes)
		return nil
	})
}

func itoa(v uint64) string {
	return strconv.FormatUint(v, 10)
}
```

(add `"strconv"` to imports.)

NOTE: `RawQuery`'s callback signature is `func(ctx, p Params, w http.ResponseWriter) error` per `atmos/xrpcserver/handler.go`. Confirm the exact signature in the vendored version (`go doc github.com/jcalabro/atmos/xrpcserver.RawQuery`) and match it.

- [ ] **Step 2: Register the route in server.go**

In `internal/xrpcapi/server.go`, thread an `OverlaySource` into the constructors. Modify `Server`, `New`, `NewWithReady`, `NewWithReadyAndCache`:

```go
type Server struct {
	src     SegmentSource
	overlay OverlaySource
	logger  *slog.Logger
	xrpc    *xrpcserver.Server
}
```

Add an `overlay OverlaySource` parameter to `NewWithReadyAndCache` (the lowest constructor) and pass it through from the others. Register inside `NewWithReadyAndCache`, after the listSegments registration:

```go
	if overlay != nil {
		s.xrpc.HandleQuery("network.bsky.jetstream.getTombstones", withReady(ready, newGetTombstonesHandler(overlay)))
	}
```

Guarding on non-nil keeps existing tests (which pass no overlay) working: update `New`/`NewWithReady`/`NewWithReadyAndCache` so the existing 2-arg/3-arg/4-arg call sites still compile. Simplest: add the overlay as the *last* parameter with the existing constructors passing `nil`, and add one new constructor the runtime uses. Concretely:

```go
func New(src SegmentSource, logger *slog.Logger) *Server {
	return NewWithReadyAndCacheAndOverlay(src, logger, nil, 0, nil)
}
func NewWithReady(src SegmentSource, logger *slog.Logger, ready ReadyFunc) *Server {
	return NewWithReadyAndCacheAndOverlay(src, logger, ready, 0, nil)
}
func NewWithReadyAndCache(src SegmentSource, logger *slog.Logger, ready ReadyFunc, cacheMaxAge time.Duration) *Server {
	return NewWithReadyAndCacheAndOverlay(src, logger, ready, cacheMaxAge, nil)
}
func NewWithReadyAndCacheAndOverlay(src SegmentSource, logger *slog.Logger, ready ReadyFunc, cacheMaxAge time.Duration, ov OverlaySource) *Server {
	// ... existing body, set s.overlay = ov, register getTombstones when ov != nil ...
}
```

- [ ] **Step 3: Write the failing handler test**

`internal/xrpcapi/gettombstones_test.go`:

```go
package xrpcapi

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/overlay"
	"github.com/bluesky-social/jetstream-v2/internal/tombstone"
	"github.com/stretchr/testify/require"
)

type fakeOverlay struct{ blob *overlay.Blob }

func (f *fakeOverlay) Current() *overlay.Blob { return f.blob }

func newOverlayTestServer(t *testing.T, snap tombstone.Snapshot, w, m uint64) *Server {
	t.Helper()
	s, _ := newTestServer(t, 1)
	blob := &overlay.Blob{
		Bytes:     overlay.Encode(snap, w, m),
		ETag:      `"abc123"`,
		Watermark: w, MaxSeq: m,
	}
	return NewWithReadyAndCacheAndOverlay(s.src, s.logger, nil, 0, &fakeOverlay{blob: blob})
}

func tombURL(base string) string {
	return base + "/xrpc/network.bsky.jetstream.getTombstones"
}

func TestGetTombstones_ServesBlob(t *testing.T) {
	t.Parallel()
	snap := tombstone.Snapshot{
		Records: map[tombstone.RecordKey]uint64{
			{DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r1"}: 110,
		},
		DIDs: map[string]tombstone.DIDTombstone{},
	}
	s := newOverlayTestServer(t, snap, 100, 110)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	resp := doGet(t, tombURL(ts.URL))
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/octet-stream", resp.Header.Get("Content-Type"))
	require.Equal(t, `"abc123"`, resp.Header.Get("ETag"))
	require.Equal(t, "100", resp.Header.Get("Jetstream-Overlay-Watermark"))
	require.Equal(t, "110", resp.Header.Get("Jetstream-Overlay-Max-Seq"))

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, overlay.Encode(snap, 100, 110), body)
}

func TestGetTombstones_EmptyOverlay(t *testing.T) {
	t.Parallel()
	s := newOverlayTestServer(t, tombstone.Snapshot{
		Records: map[tombstone.RecordKey]uint64{}, DIDs: map[string]tombstone.DIDTombstone{},
	}, 200, 200)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	resp := doGet(t, tombURL(ts.URL))
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NotEmpty(t, body, "empty overlay still has framing + zstd frame")
}

func TestGetTombstones_ReadinessGate(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, 1)
	blob := &overlay.Blob{Bytes: overlay.Encode(tombstone.Snapshot{
		Records: map[tombstone.RecordKey]uint64{}, DIDs: map[string]tombstone.DIDTombstone{}}, 0, 0), ETag: `"x"`}
	gated := NewWithReadyAndCacheAndOverlay(s.src, s.logger, func(_ context.Context) error {
		return errors.New("bootstrap in progress")
	}, 0, &fakeOverlay{blob: blob})
	ts := httptest.NewServer(gated.Handler())
	defer ts.Close()

	resp := doGet(t, tombURL(ts.URL))
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}
```

(add `"context"` and `"errors"` imports.)

- [ ] **Step 4: Run to verify failure, then make it pass**

Run: `just test ./internal/xrpcapi -run TestGetTombstones`
Expected: FAIL first (undefined `NewWithReadyAndCacheAndOverlay`), then PASS after Step 2's edits compile.

- [ ] **Step 5: Run the whole xrpcapi suite to confirm no regressions**

Run: `just test ./internal/xrpcapi`
Expected: PASS (existing getSegment/listSegments/server tests still green).

- [ ] **Step 6: Commit**

```bash
git add internal/xrpcapi/gettombstones.go internal/xrpcapi/gettombstones_test.go internal/xrpcapi/server.go
git commit -m "xrpcapi: serve getTombstones overlay blob

RawQuery handler writes the cached blob with octet-stream, ETag, and
W/M headers; registered behind the readiness gate. Refs #PLAN"
git log --oneline -1
```

---

## Task 8: Wire the cache into runtime.go

**Files:**
- Modify: `internal/jetstreamd/runtime.go`

The runtime already has `tombstones := tombstone.New()` (line ~213) and an `OnCompactionPass` callback in the orchestrator config (line ~304). We add a watermark accessor, build the cache, hook rebuild into `OnCompactionPass`, start the ticker, and pass the cache to xrpcapi.

- [ ] **Step 1: Add a source adapter**

The cache needs `Source{SnapshotRange, Watermark, Dirty}`. `*tombstone.Set` already has `SnapshotRange`. Add `Watermark` and `Dirty`:
- **Watermark:** read `compaction/seq` from the store. The helper `loadCompactionWatermark` is in `internal/ingest/orchestrator` (unexported). Rather than export it, read the key directly via the store in the adapter. Check how the store exposes a uint64 get (`grep -n "func (s \*Store)" internal/store/*.go`); the watermark key is the constant `"compaction/seq"`.
- **Dirty:** `tombstone.Set` has no change counter today. Add one: a `dirty atomic.Uint64` incremented in `Observe`, `Evict`, and `Replace`, exposed via `func (s *Set) Dirty() uint64`. This is a tiny, safe addition (kaizen — the set is the right owner of its own change signal).

Add to `internal/tombstone/tombstone.go`:

```go
// in the Set struct:
	dirty atomic.Uint64

// new method:
// Dirty returns a monotonically increasing value that changes on every
// mutation (Observe, Evict, Replace). The overlay cache uses it to skip
// rebuilds when the set is unchanged.
func (s *Set) Dirty() uint64 { return s.dirty.Load() }
```

Increment `s.dirty.Add(1)` at the end of `Observe`, `Evict`, and `Replace` (inside the lock is fine). Add `"sync/atomic"` to imports.

Write a tombstone unit test for it in `internal/tombstone/tombstone_test.go`:

```go
func TestSetDirtyChangesOnMutation(t *testing.T) {
	t.Parallel()
	s := New()
	d0 := s.Dirty()
	require.NoError(t, s.Observe(&segment.Event{Seq: 1, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r"}))
	require.NotEqual(t, d0, s.Dirty())
	d1 := s.Dirty()
	s.Evict(1)
	require.NotEqual(t, d1, s.Dirty())
}
```

Run: `just test ./internal/tombstone -run TestSetDirty` → PASS.

- [ ] **Step 2: Build the adapter + cache in runtime.go**

In `internal/jetstreamd/runtime.go`, near the other subsystem construction (after `tombstones := tombstone.New()` and after `metaStore` exists), add:

```go
overlayMetrics := overlay.NewMetrics(metrics.Registry)
overlayCache := overlay.NewCache(overlaySource{set: tombstones, store: metaStore}, overlayMetrics)
rt.overlayCache = overlayCache
```

Define the adapter (in runtime.go or a small new file `internal/jetstreamd/overlay_source.go`):

```go
type overlaySource struct {
	set   *tombstone.Set
	store *store.Store
}

func (o overlaySource) SnapshotRange(low, high uint64) tombstone.Snapshot {
	return o.set.SnapshotRange(low, high)
}
func (o overlaySource) Dirty() uint64 { return o.set.Dirty() }
func (o overlaySource) Watermark() uint64 {
	v, ok, err := o.store.GetUint64("compaction/seq") // confirm the exact store API
	if err != nil || !ok {
		return 0
	}
	return v
}
```

Confirm the store getter name/signature with `go doc github.com/bluesky-social/jetstream-v2/internal/store.Store` and adjust. If only a `[]byte` getter exists, decode with `binary.BigEndian`/`LittleEndian` exactly as `loadCompactionWatermark` does (read that function to match the encoding precisely — getting endianness wrong silently corrupts W).

- [ ] **Step 3: Hook rebuild into OnCompactionPass**

The orchestrator config's `OnCompactionPass` currently forwards to `opts.OnCompactionPass`. Wrap it so the overlay rebuilds after every pass (the set was just evicted, W advanced):

```go
OnCompactionPass: func(result orchestrator.CompactionPassResult) {
	overlayCache.Rebuild()
	if opts.OnCompactionPass != nil {
		opts.OnCompactionPass(CompactionPassResult{Watermark: result.Watermark, Err: result.Err})
	}
},
```

- [ ] **Step 4: Pass the cache to xrpcapi**

Change the xrpc construction (line ~346) from `NewWithReadyAndCache(...)` to `NewWithReadyAndCacheAndOverlay(mft, processLogger, readyFn, opts.SegmentCacheMaxAge, overlayCache)`.

- [ ] **Step 5: Start the ticker under lifecycle**

Find where runtime starts background goroutines under graceful shutdown (look for the errgroup / lifecycle manager in `Run`). Add the ticker, e.g.:

```go
g.Go(func() error {
	err := r.overlayCache.RunTicker(gctx, overlayRebuildInterval)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
})
```

Define `const overlayRebuildInterval = 2 * time.Second` near the top of the package (matches spec §5.5 default). Add an `overlayCache *overlay.Cache` field to the `Runtime` struct.

NOTE: the cache only serves meaningful data in steady state (the set is populated then). Before steady state, `Watermark()` returns 0 and the set is empty, so the blob is a valid empty overlay; the xrpcapi readiness gate already 503s the endpoint until steady state, so no stale-but-wrong data is served. Starting the ticker early is harmless (cheap no-op rebuilds on an empty set).

- [ ] **Step 6: Build and run the daemon package tests**

Run: `just build` then `just test ./internal/jetstreamd`
Expected: success / PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/tombstone/tombstone.go internal/tombstone/tombstone_test.go internal/jetstreamd/
git commit -m "jetstreamd: wire overlay cache to tombstone set and compaction

Add Set.Dirty() change signal; rebuild the overlay blob on each
compaction pass and on a 2s coalescing ticker; serve via getTombstones.
Refs #PLAN"
git log --oneline -1
```

---

## Task 9: Oracle end-to-end determinism test

**Files:**
- Create: `internal/oracle/overlay.go`, `internal/oracle/overlay_test.go`

This is the correctness keystone (spec §7.5). It proves the three coverage ranges stitch together: reconstruct the client's would-be output from (segments ≤ W) + (overlay (W,M]) + (live (M,∞)) and assert it equals ground truth.

First, understand the existing oracle harness: read `internal/oracle/harness_test.go` and `compacted_test.go` to see how `[]ObservedEvent` and the watermark are obtained from a simulator run. The new test reuses that machinery.

- [ ] **Step 1: Implement the reconstruction checker**

`internal/oracle/overlay.go`:

```go
package oracle

import (
	"fmt"

	"github.com/bluesky-social/jetstream-v2/internal/tombstone"
	"github.com/bluesky-social/jetstream-v2/segment"
)

// CheckOverlayReconstruction verifies the full client coverage contract:
// given all observed events, a compaction watermark W, the overlay
// snapshot the server would serve (covering (W, M]), and M, the set of
// rows a client would EMIT must exactly equal the ground-truth live set.
//
//   - segments cover seq <= W (already physically compacted: superseded
//     create/update rows removed)
//   - overlay covers (W, M]
//   - live tail covers (M, inf)
//
// A client emits a create/update row unless a tombstone with strictly
// greater seq supersedes it. We reconstruct that emission decision from
// the three sources and compare against an independent ground-truth fold.
func CheckOverlayReconstruction(events []ObservedEvent, w, m uint64, overlaySnap tombstone.Snapshot) error {
	// Ground truth: the final live set of (did,collection,rkey) records
	// after applying every delete/update/account/sync across ALL seqs.
	ground := groundTruthLive(events)

	// Reconstructed client emission:
	//   - record/update rows with seq <= W are already gone from segments
	//     (compaction removed superseded ones; survivors are the latest).
	//   - rows in (W, M]: emitted unless overlaySnap supersedes them.
	//   - rows in (M, inf): emitted unless a live tombstone supersedes them;
	//     the client gets live tombstones over /subscribe from cursor=M.
	// We model "client output" by folding live tombstones for (M,inf)
	// ourselves (that is what the live tail delivers) and overlaySnap for
	// (W,M], and segment physical-compaction for <=W.
	liveTomb, err := tombstone.FoldRange(events, m, ^uint64(0))
	if err != nil {
		return err
	}

	emitted := make(map[tombstone.RecordKey]uint64)
	for i := range events {
		ev := &events[i]
		if ev.Kind != segment.KindCreate && ev.Kind != segment.KindUpdate {
			continue
		}
		key := tombstone.RecordKey{DID: ev.DID, Collection: ev.Collection, Rkey: ev.Rkey}
		switch {
		case ev.Seq <= w:
			// Survives in segments only if it is the ground-truth-latest;
			// compaction removed superseded <=W rows. Model that by
			// deferring to the ground-truth check below (segment content
			// is validated by CheckCompacted separately).
			if gseq, ok := ground[key]; ok && gseq == ev.Seq {
				emitted[key] = maxU64(emitted[key], ev.Seq)
			}
		case ev.Seq <= m:
			if drop, _ := overlaySnap.ShouldDrop(ev); !drop {
				emitted[key] = maxU64(emitted[key], ev.Seq)
			}
		default: // ev.Seq > m
			if drop, _ := liveTomb.ShouldDrop(ev); !drop {
				emitted[key] = maxU64(emitted[key], ev.Seq)
			}
		}
	}

	// Every emitted key must be live in ground truth at the same seq, and
	// every ground-truth-live key must be emitted. Exact equality.
	for key, seq := range emitted {
		gseq, ok := ground[key]
		if !ok {
			return fmt.Errorf("oracle overlay: emitted a record that ground truth deleted: %v seq=%d", key, seq)
		}
		if gseq != seq {
			return fmt.Errorf("oracle overlay: emitted stale version %v seq=%d (live seq=%d)", key, seq, gseq)
		}
	}
	for key, gseq := range ground {
		if _, ok := emitted[key]; !ok {
			return fmt.Errorf("oracle overlay: failed to emit a live record: %v seq=%d", key, gseq)
		}
	}
	return nil
}

// groundTruthLive folds the entire event stream into the set of records
// that are live at the end, mapping key -> latest create/update seq. A
// delete, account-delete, or sync at a higher seq removes the record.
func groundTruthLive(events []ObservedEvent) map[tombstone.RecordKey]uint64 {
	type rec struct {
		seq  uint64
		live bool
	}
	latest := make(map[tombstone.RecordKey]*rec)
	// DID-level kills: did -> highest account-delete/sync seq.
	didKill := make(map[string]uint64)
	for i := range events {
		ev := &events[i]
		switch ev.Kind {
		case segment.KindCreate, segment.KindUpdate:
			key := tombstone.RecordKey{DID: ev.DID, Collection: ev.Collection, Rkey: ev.Rkey}
			r := latest[key]
			if r == nil {
				r = &rec{}
				latest[key] = r
			}
			if ev.Seq >= r.seq {
				r.seq = ev.Seq
				r.live = true
			}
		case segment.KindDelete:
			key := tombstone.RecordKey{DID: ev.DID, Collection: ev.Collection, Rkey: ev.Rkey}
			r := latest[key]
			if r == nil {
				r = &rec{}
				latest[key] = r
			}
			if ev.Seq >= r.seq {
				r.seq = ev.Seq
				r.live = false
			}
		case segment.KindSync:
			if ev.Seq > didKill[ev.DID] {
				didKill[ev.DID] = ev.Seq
			}
		case segment.KindAccount:
			deleted, _ := oracleAccountDeleted(ev.Payload)
			if deleted && ev.Seq > didKill[ev.DID] {
				didKill[ev.DID] = ev.Seq
			}
		}
	}
	out := make(map[tombstone.RecordKey]uint64)
	for key, r := range latest {
		if r.live && didKill[key.DID] <= r.seq {
			out[key] = r.seq
		}
	}
	return out
}

func maxU64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
```

NOTE on `ObservedEvent`: confirm its fields (`Seq, Kind, DID, Collection, Rkey, Payload`) against the existing `compacted.go`/harness; they match the usage above. If the harness exposes events differently, adapt the field access — the algorithm is unchanged.

- [ ] **Step 2: Write the unit-level tests for the checker (fast, no simulator)**

`internal/oracle/overlay_test.go`:

```go
package oracle

import (
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/tombstone"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/stretchr/testify/require"
)

// overlayFor builds the snapshot the server would serve for (w, m].
func overlayFor(t *testing.T, events []ObservedEvent, w, m uint64) tombstone.Snapshot {
	t.Helper()
	snap, err := tombstone.FoldRange(events, w, m)
	require.NoError(t, err)
	return snap
}

func TestOverlayReconstruction_SuppressesDeletedRecord(t *testing.T) {
	t.Parallel()
	events := []ObservedEvent{
		{Seq: 50, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r"},
		{Seq: 120, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r"},
	}
	// W=100: create@50 is <=W (segment), delete@120 is in (100,150] overlay.
	require.NoError(t, CheckOverlayReconstruction(events, 100, 150, overlayFor(t, events, 100, 150)))
}

func TestOverlayReconstruction_NoPermanentTombstone(t *testing.T) {
	t.Parallel()
	// Account deleted@100, reactivated implicitly, posts again@200.
	events := []ObservedEvent{
		{Seq: 60, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "old"},
		{Seq: 100, Kind: segment.KindAccount, DID: "did:plc:a", Payload: oracleAccountPayload(t, false, "deleted")},
		{Seq: 200, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "new"},
	}
	// W=150, M=250: account-delete@100 is <=W (already compacted out of
	// segments). post@200 must survive: it is newer than the deletion.
	err := CheckOverlayReconstruction(events, 150, 250, overlayFor(t, events, 150, 250))
	require.NoError(t, err, "reactivated account's newer record must be emitted")
}

func TestOverlayReconstruction_SeamBoundaries(t *testing.T) {
	t.Parallel()
	// Tombstone exactly at M (in overlay) and just above M (live).
	events := []ObservedEvent{
		{Seq: 10, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r1"},
		{Seq: 150, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r1"},
		{Seq: 80, Kind: segment.KindCreate, DID: "did:plc:b", Collection: "c", Rkey: "r2"},
		{Seq: 151, Kind: segment.KindDelete, DID: "did:plc:b", Collection: "c", Rkey: "r2"},
	}
	// W=100, M=150: delete@150 in overlay; delete@151 over live tail.
	require.NoError(t, CheckOverlayReconstruction(events, 100, 150, overlayFor(t, events, 100, 150)))
}
```

(`oracleAccountPayload` already exists in `compacted_test.go`; it is in the same package.)

- [ ] **Step 3: Run the checker tests**

Run: `just test ./internal/oracle -run TestOverlayReconstruction`
Expected: PASS. If `NoPermanentTombstone` fails, the bug is real — investigate before weakening it.

- [ ] **Step 4: Wire into the seeded-simulator oracle (non-short)**

Add a non-short test that drives the existing simulator harness through deletes/updates/account-deletes/a sync divergence, runs a compaction pass to advance W, fetches the *real* overlay blob from the running server's `getTombstones` (or from the in-process `overlay.Cache`), decodes it with the production decoder path, and calls `CheckOverlayReconstruction`. Gate with `if testing.Short() { t.Skip(...) }`. Model it on the existing restart/compaction oracle in `harness_test.go` (reuse its setup helpers; do not duplicate simulator wiring).

Because the existing harness wiring is not fully visible in this plan, this step is: **read `internal/oracle/harness_test.go`, find the helper that returns observed events + the live `Runtime`/server, and add a `TestOracle_OverlayReconstruction` that calls `CheckOverlayReconstruction` with the served blob decoded via `overlay` (expose a package-level `Decode` in `internal/overlay` if the test needs it — promote `decodeForTest` to an exported `Decode` at that point, since the oracle is a legitimate non-test consumer).**

- [ ] **Step 5: Run the non-short oracle**

Run: `just test-long ./internal/oracle -run TestOracle_OverlayReconstruction -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/oracle/overlay.go internal/oracle/overlay_test.go internal/overlay/
git commit -m "oracle: end-to-end overlay reconstruction determinism check

Reconstruct client emission from segments(<=W)+overlay((W,M])+live((M,inf))
and assert exact equality with ground-truth live set; covers
no-permanent-tombstone and seam boundaries. Refs #PLAN"
git log --oneline -1
```

---

## Task 10: Mutation campaign mutants

**Files:**
- Create: `testing/mutation/mutants/m0XX-*.patch` (×4)
- Modify: `testing/mutation/RESULTS.md` + mutant catalog

Read `docs/superpowers/specs/2026-06-12-oracle-mutation-campaign-design.md` and `testing/mutation/RESULTS.md` first to match the existing mutant format (single-edit `.patch`, documented failure mode + predicted tier). Find the next free `m0XX` numbers (`ls testing/mutation/mutants/`).

- [ ] **Step 1: Create the four mutants**

Each is a single-line diff that introduces a real bug this feature's tests must catch. Generate each by making the edit, `git diff > testing/mutation/mutants/mNNN-name.patch`, then reverting the edit. The four:

1. **`ShouldDrop` off-by-one** — in `internal/tombstone/tombstone.go`, change `ts.Seq > ev.Seq` to `ts.Seq >= ev.Seq` (masks a live record at the boundary). Predicted kill: `internal/oracle` overlay reconstruction (seam test) + `compacted` checks.
2. **Encoder drops DID-tombstone section** — in `internal/overlay/format.go`, change the DID-tombstone count to always write 0 (`buf = appendUvarint(buf, 0)` and skip the loop). Predicted kill: `internal/overlay` round-trip property test + oracle (account/sync deletions leak).
3. **Encoder wrong seq base** — in `encodeBody`, change record `prev := seqBase` to `prev := uint64(0)` (delta decode produces wrong seqs). Predicted kill: `internal/overlay` round-trip test.
4. **Cache reports stale M** — in `internal/overlay/cache.go` `build`, change `MaxSeq: m` to `MaxSeq: w` (under-reports coverage, would cause the future query plan to set cursor below real coverage → seam gap). Predicted kill: `internal/overlay` cache test (asserts MaxSeq) + oracle seam test.

- [ ] **Step 2: Verify each mutant is killed by the campaign driver**

Run: `just mutation-campaign` (per AGENTS.md; never apply patches outside the driver)
Expected: all four new mutants reported as KILLED by the predicted tier.

- [ ] **Step 3: Update the scorecard**

Edit `testing/mutation/RESULTS.md` and the mutant catalog to add the four entries with their production failure mode and the tier that killed them, matching the existing table format.

- [ ] **Step 4: Commit**

```bash
git add testing/mutation/
git commit -m "test(mutation): overlay encoder/cache/ShouldDrop mutants

Four single-edit bugs (off-by-one suppression, dropped DID section,
wrong seq base, stale M) the overlay + oracle tests must kill. Refs #PLAN"
git log --oneline -1
```

---

## Task 11: Update DESIGN.md §3.3

**Files:**
- Modify: `DESIGN.md` (§3.3, around lines 337–364)

- [ ] **Step 1: Add the wire-format diagram + coverage contract**

In §3.3, after the existing paragraph that ends "...expose this same structure to backfill clients so they can suppress rows newer than the physical compaction watermark." (line ~351), add a new subsection documenting the now-shipped endpoint. Insert the `jsto` binary-format diagram from the spec (§4), the `W`/`M` coverage contract (the three-range table), and a sentence that the **client decoder and query-plan cursor selection are future work** (reference issues #CLIENT and #PLAN). Update the "Future read-overlay work" bullets (steps 3, 5–7, lines ~357–364) to note that the server endpoint now exists (`network.bsky.jetstream.getTombstones`) and that the remaining gap is the client consumer.

Keep the prose consistent with the existing DESIGN.md voice (terse, present-tense, "we").

- [ ] **Step 2: Re-read the edited section for accuracy**

Confirm: the diagram matches `internal/overlay/format.go` exactly (field order, types, reason enum values); the watermark/maxSeq semantics match the code; no contradiction with the existing compaction prose.

- [ ] **Step 3: Commit**

```bash
git add DESIGN.md
git commit -m "design: document getTombstones overlay format and coverage contract

Add the jsto v1 wire-format diagram and the segments/overlay/live
coverage contract to 3.3; note client decoder + query plan as future
work (#CLIENT, #PLAN)."
git log --oneline -1
```

---

## Task 12: Full verification sweep

**Files:** none (verification only)

- [ ] **Step 1: Full short test suite + race**

Run: `just test` (default target: short tests with `-race`)
Expected: all green.

- [ ] **Step 2: Lint + modernize**

Run: `just lint` then `just modernize`
Expected: clean; if modernize rewrites anything, review and commit it.

- [ ] **Step 3: Non-short oracle + restart coverage**

Run: `just test-long ./internal/oracle -run TestOracle -v`
Expected: green (the overlay reconstruction test and existing restart/compaction tests).

- [ ] **Step 4: Heavier oracle sweep (smoke)**

Run: `just oracle` (and optionally `just oracle-sweep`)
Expected: green; this catches any nondeterminism in the overlay path under stress.

- [ ] **Step 5: Confirm the endpoint serves end to end**

Bring up the local stack against the simulator (per README / `just run` against `cmd/simulator`), wait for steady state, and `curl -i http://localhost:8080/xrpc/network.bsky.jetstream.getTombstones`. Expected: 200, `Content-Type: application/octet-stream`, `ETag` present, `Jetstream-Overlay-Watermark`/`-Max-Seq` headers present, a non-empty body that the `internal/overlay` decoder round-trips. (If the local stack cannot reach steady state quickly, state that you could not manually exercise it and rely on the oracle E2E test instead — do not claim manual verification you did not do.)

- [ ] **Step 6: Final commit if anything changed during the sweep**

```bash
git add -A
git commit -m "overlay: lint/modernize cleanup from verification sweep"
git log --oneline -1
```

---

## Self-review notes (for the implementer)

- **Spec coverage:** §2 wire format → Task 2; §4 columnar gate → Task 6; §5.2–5.5 cache/triggers → Tasks 4–5, 8; §5.7 compaction-disabled → handled by readiness gate + empty-blob (Task 8 Step 5 note); §5.8 metrics → Task 4; §7.1 encoder tests → Tasks 2–3; §7.2 concurrency → Task 5; §7.3 handler → Task 7; §7.4 benchmark → Task 6; §7.5 oracle → Task 9; §7.6 mutants → Task 10; DESIGN.md → Task 11; follow-up issues → Task 0.
- **Type consistency:** `Encode(snap, w, m)`, `decodeForTest`/`Decode`, `Blob{Bytes,ETag,Watermark,MaxSeq,NumRecords,NumDIDs}`, `Source{SnapshotRange,Watermark,Dirty}`, `OverlaySource{Current}`, `NewWithReadyAndCacheAndOverlay` — names are used identically across tasks.
- **Unverified API surfaces to confirm at implementation time** (flagged inline, not guessed): exact `xrpcserver.RawQuery` callback signature; the `store.Store` uint64 getter name + watermark encoding (match `loadCompactionWatermark`); the `internal/oracle` harness helper that yields observed events + a live server; the lexgen output filename. These are explicitly called out in their tasks with a `go doc`/grep instruction rather than assumed.
```
