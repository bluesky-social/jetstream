# getBlock XRPC Endpoint Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `network.bsky.jetstream.getBlock`, an XRPC query that serves one sealed-segment block by index as its raw stored zstd frame, CDN-cacheable with a strong immutable ETag.

**Architecture:** A new `segment.ReadBlockFrame` primitive does the bytes-only read (header already in hand → one index-entry pread → one frame pread, no decompression). A new `xrpcapi.getBlockHandler` resolves the segment path via the in-memory manifest (`SegmentByIdx`), then opens the file fresh and derives the ETag, block count, and block bytes from that single fd — the same generation-pinning idiom `getSegment` uses to make a concurrent compaction rewrite unable to splice file generations. Observability (Prometheus metrics + a per-request trace span) and wiring round it out, followed by an oracle-backed end-to-end test.

**Tech Stack:** Go; `github.com/jcalabro/atmos/xrpcserver` (XRPC), `github.com/prometheus/client_golang` (metrics), OpenTelemetry via the project's `internal/obs` wrapper (tracing), `lexgen` (lexicon codegen), `github.com/stretchr/testify/require` (tests).

## Global Constraints

- Module path: `github.com/bluesky-social/jetstream-v2`. Segment-format logic lives in package `segment`; only `internal/xrpcapi` depends on atmos.
- Block frame on the wire excludes the 8-byte length prefix; `Content-Length` carries the length.
- ETag format: `"<segment-xxh3-hex>:<blockIndex>"` where the hex is `checksumHex(checksum)` (16-char, `%016x`). Strong validator (double-quoted).
- ETag, block count, and block bytes for a request MUST all come from the single freshly-opened fd, never the in-memory manifest. The manifest is used only for `SegmentByIdx` path resolution. (Corruption guard — crash/error over corruption.)
- Cache-Control reuses the existing `cacheControlHeader(cacheMaxAge)` helper / `--segment-cache-max-age` flag. No new CLI flags.
- Error names exactly `SegmentNotFound` and `BlockNotFound` (match the lexicon), not generic `NotFound`.
- Prometheus namespace `jetstream`. Metrics types are nil-safe (every method a no-op on a nil receiver), mirroring `overlay.Metrics`.
- TDD: write the failing test first, watch it fail, implement minimally, watch it pass, commit. Run `gofmt`/`goimports` before each commit; the tree must build (`go build ./...`) and vet clean.

---

### Task 1: `segment.ReadBlockFrame` primitive

Bytes-only single-block read in the `segment` package, so `xrpcapi` never needs to know the on-disk frame layout. Sibling of `Reader.DecodeBlock`, but returns the raw stored frame with no decompression and reads only the fixed header + one index entry + the frame.

**Files:**
- Create: `segment/blockframe.go`
- Test: `segment/blockframe_test.go`

**Interfaces:**
- Consumes (existing, verified): `Header` struct with fields `BlockCount uint32`, `BlockIndexOffset uint64`, `FooterOffset uint64`, `Checksum uint64` (`segment/header.go:17`); `func ReadSealedHeader(r io.ReaderAt) (Header, error)` (`segment/header.go:116`); `const blockIndexEntrySize = 52` (`segment/footer.go:39`); `BlockInfo` fields `Offset uint64`, `CompressedSize uint32` (`segment/footer.go`); `ReservedHeaderBytes` (`segment/writer.go`); sentinel errors `ErrBlockOutOfRange`, `ErrInvalidBlockIndex` (`segment/errors.go`). Block-index entry on-disk layout: at `off`, `Offset=u64[off:off+8]`, `CompressedSize=u32[off+8:off+12]` (`segment/footer.go` `encodeBlockIndex`). The frame body begins at `Offset+8` (skips the 8-byte length prefix), confirmed by `Reader.DecodeBlock` (`segment/reader.go:310`).
- Produces (later tasks rely on this exact signature): `func ReadBlockFrame(r io.ReaderAt, hdr Header, idx int) ([]byte, error)` — returns the raw zstd frame bytes for block `idx` (no length prefix). Returns `ErrBlockOutOfRange` (wrapped) when `idx < 0 || idx >= int(hdr.BlockCount)`.

- [ ] **Step 1: Write the failing tests**

Create `segment/blockframe_test.go`. The test builds a real multi-block sealed segment with the existing writer (small `MaxEventsPerBlock` to force several blocks), then asserts `ReadBlockFrame` returns bytes byte-identical to the frame as read by an independent file slice, and that decoding those bytes yields the same events as `Reader.DecodeBlock`.

```go
package segment

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// buildMultiBlockSegment writes a sealed segment with blockCount blocks of
// perBlock events each and returns its path.
func buildMultiBlockSegment(t *testing.T, perBlock, blockCount int) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg_test.jss")
	w, err := New(Config{Path: path, MaxEventsPerBlock: perBlock})
	require.NoError(t, err)
	var seq uint64
	for b := 0; b < blockCount; b++ {
		for i := 0; i < perBlock; i++ {
			seq++
			_, err = w.Append(Event{
				Seq:        seq,
				IndexedAt:  int64(1_730_000_000_000_000 + seq*1_000),
				Kind:       KindCreate,
				DID:        "did:plc:test",
				Collection: "app.bsky.feed.post",
				Rkey:       "rkey",
				Rev:        "rev",
				Payload:    []byte{0xa0},
			})
			require.NoError(t, err)
		}
	}
	_, err = w.Seal()
	require.NoError(t, err)
	return path
}

func TestReadBlockFrame_MatchesOnDiskAndDecodes(t *testing.T) {
	path := buildMultiBlockSegment(t, 2, 3) // 3 blocks, 2 events each

	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	hdr, err := ReadSealedHeader(f)
	require.NoError(t, err)
	require.Equal(t, uint32(3), hdr.BlockCount)

	// Reader for cross-checking decoded events + raw offsets.
	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	defer func() { _ = r.Close() }()
	infos := r.Blocks()

	for idx := 0; idx < int(hdr.BlockCount); idx++ {
		frame, err := ReadBlockFrame(f, hdr, idx)
		require.NoError(t, err)

		// Byte-identical to an independent slice read of [Offset+8, +CompressedSize).
		want := make([]byte, infos[idx].CompressedSize)
		_, err = f.ReadAt(want, int64(infos[idx].Offset)+8)
		require.NoError(t, err)
		require.Equal(t, want, frame, "block %d frame bytes", idx)

		// Decodes to the same events as DecodeBlock.
		gotEvents, _, err := decodeBlockCompressedSized(frame)
		require.NoError(t, err)
		wantEvents, err := r.DecodeBlock(idx)
		require.NoError(t, err)
		require.Equal(t, wantEvents, gotEvents, "block %d events", idx)
	}
}

func TestReadBlockFrame_OutOfRange(t *testing.T) {
	path := buildMultiBlockSegment(t, 2, 2)
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	hdr, err := ReadSealedHeader(f)
	require.NoError(t, err)

	_, err = ReadBlockFrame(f, hdr, -1)
	require.ErrorIs(t, err, ErrBlockOutOfRange)
	_, err = ReadBlockFrame(f, hdr, int(hdr.BlockCount))
	require.ErrorIs(t, err, ErrBlockOutOfRange)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./segment/ -run TestReadBlockFrame -v`
Expected: compile error / FAIL — `undefined: ReadBlockFrame`.

- [ ] **Step 3: Implement `ReadBlockFrame`**

Create `segment/blockframe.go`. Read one 52-byte index entry at `BlockIndexOffset + idx*52`, decode `Offset`+`CompressedSize` from it, bounds-check against the footer, then read the frame. No decompression, no bloom/collection parsing.

```go
package segment

import (
	"encoding/binary"
	"fmt"
	"io"
)

// ReadBlockFrame reads the raw, stored zstd frame for block idx using only the
// already-read fixed header — no footer/bloom/collection parsing and no
// decompression. The returned bytes exclude the 8-byte length prefix, i.e. they
// are exactly the [block_len]byte frame the writer appended.
//
// r is the fd for the sealed segment file; hdr is its fixed header as returned
// by ReadSealedHeader. Returns ErrBlockOutOfRange when idx is out of range.
//
// All offsets are validated against hdr.FooterOffset before any read keyed off
// them, so a corrupt/hostile block-index entry cannot drive an out-of-bounds or
// oversized allocation/read.
func ReadBlockFrame(r io.ReaderAt, hdr Header, idx int) ([]byte, error) {
	if idx < 0 || idx >= int(hdr.BlockCount) {
		return nil, fmt.Errorf("%w: idx %d, block_count %d",
			ErrBlockOutOfRange, idx, hdr.BlockCount)
	}

	entry := make([]byte, blockIndexEntrySize)
	entryOff := int64(hdr.BlockIndexOffset) + int64(idx)*blockIndexEntrySize
	if _, err := r.ReadAt(entry, entryOff); err != nil {
		return nil, fmt.Errorf("segment: read block %d index entry: %w", idx, err)
	}
	le := binary.LittleEndian
	offset := le.Uint64(entry[0:8])
	compressedSize := le.Uint32(entry[8:12])

	// Validate the frame range lies within [ReservedHeaderBytes, FooterOffset),
	// mirroring validateBlockOffsets. end = offset + 8 (length prefix) + size.
	if uint64(compressedSize) > hdr.FooterOffset {
		return nil, fmt.Errorf("%w: block %d compressed_size %d > footer_offset %d",
			ErrInvalidBlockIndex, idx, compressedSize, hdr.FooterOffset)
	}
	end := offset + 8 + uint64(compressedSize)
	if offset < uint64(ReservedHeaderBytes) || end > hdr.FooterOffset {
		return nil, fmt.Errorf("%w: block %d range [%d, %d) outside [%d, %d)",
			ErrInvalidBlockIndex, idx, offset, end, ReservedHeaderBytes, hdr.FooterOffset)
	}

	frame := make([]byte, compressedSize)
	if _, err := r.ReadAt(frame, int64(offset)+8); err != nil {
		return nil, fmt.Errorf("segment: read block %d frame: %w", idx, err)
	}
	return frame, nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./segment/ -run TestReadBlockFrame -v`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
gofmt -w segment/blockframe.go segment/blockframe_test.go
git add segment/blockframe.go segment/blockframe_test.go
git commit -m "segment: add ReadBlockFrame bytes-only single-block read"
```

---

### Task 2: Lexicon definition

Add the `getBlock` lexicon and regenerate. Like `getSegment`/`getTombstones`, it is a raw octet-stream query with no JSON output schema, so codegen produces no handler-relevant struct; the handler is hand-written. Regenerating keeps generated artifacts consistent.

**Files:**
- Create: `lexicons/network/bsky/jetstream/getBlock.json`
- (Possibly regenerated, do not hand-edit): `api/jetstream/*`

**Interfaces:**
- Consumes: existing lexgen config `lexgen.json` and the Makefile/go:generate target used for the other three lexicons.
- Produces: the published NSID `network.bsky.jetstream.getBlock` with params `segment` (string, required) and `blockIndex` (integer, required, minimum 0), output `application/octet-stream`, errors `SegmentNotFound`, `BlockNotFound`.

- [ ] **Step 1: Create the lexicon JSON**

Create `lexicons/network/bsky/jetstream/getBlock.json`:

```json
{
  "lexicon": 1,
  "id": "network.bsky.jetstream.getBlock",
  "defs": {
    "main": {
      "type": "query",
      "description": "Download a single sealed-segment block by index. Returns the raw zstd-compressed block frame exactly as stored on disk (no 8-byte length prefix; Content-Length carries the length). The response is immutable for a given ETag and is CDN-cacheable. Clients fetch the blocks named by a query plan and decode each frame with the standard block decoder.",
      "parameters": {
        "type": "params",
        "required": ["segment", "blockIndex"],
        "properties": {
          "segment": {
            "type": "string",
            "description": "The sealed segment filename, e.g. seg_000000002a.jss (same value returned by listSegments and accepted by getSegment)."
          },
          "blockIndex": {
            "type": "integer",
            "minimum": 0,
            "description": "Zero-based block index within the segment. Must be < the segment's block_count."
          }
        }
      },
      "output": { "encoding": "application/octet-stream" },
      "errors": [
        { "name": "SegmentNotFound", "description": "No sealed segment exists for the given name." },
        { "name": "BlockNotFound", "description": "blockIndex is >= the segment's block_count." }
      ]
    }
  }
}
```

- [ ] **Step 2: Find the codegen target**

Run: `grep -rn "lexgen" Makefile* 2>/dev/null; grep -rn "go:generate" --include=*.go . | grep -i lex`
Expected: locate the generate command (e.g. a `make lexgen` target or a `//go:generate` directive).

- [ ] **Step 3: Regenerate lexicon types**

Run the discovered command (e.g. `make lexgen` or `go generate ./...`).
Expected: command succeeds; `git status` shows only additive/no changes under `api/`. If it produces no diff (expected for a schema-less octet-stream query), that is fine.

- [ ] **Step 4: Verify the build still compiles**

Run: `go build ./...`
Expected: success.

- [ ] **Step 5: Commit**

```bash
git add lexicons/network/bsky/jetstream/getBlock.json api/
git commit -m "lexicon: add network.bsky.jetstream.getBlock"
```

---

### Task 3: `xrpcapi.Metrics` (Prometheus)

The `internal/xrpcapi` package has no metrics today. Add a nil-safe `Metrics` type (subsystem `getblock`) following the `overlay.Metrics` pattern. The handler (Task 4) consumes these exact field/method names.

**Files:**
- Create: `internal/xrpcapi/metrics.go`
- Test: `internal/xrpcapi/metrics_test.go`

**Interfaces:**
- Consumes: `github.com/prometheus/client_golang/prometheus`.
- Produces:
  - `type Metrics struct { ... }` (unexported fields).
  - `func NewMetrics(reg prometheus.Registerer) *Metrics` — registers and returns; mirrors `overlay.NewMetrics`.
  - `func (m *Metrics) observeServe(result string, bytes int, seconds float64)` — nil-safe; increments `getblock_requests_total{result}`, adds to `getblock_served_bytes_total` (only when bytes > 0), observes `getblock_duration_seconds`.
  - Result label constants: `resultOK = "ok"`, `resultNotFound = "not_found"`, `resultBadRequest = "bad_request"`, `resultError = "error"`.

- [ ] **Step 1: Write the failing test**

Create `internal/xrpcapi/metrics_test.go`:

```go
package xrpcapi

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestMetrics_NilSafe(t *testing.T) {
	var m *Metrics
	require.NotPanics(t, func() { m.observeServe(resultOK, 123, 0.001) })
}

func TestMetrics_ObserveServe(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	m.observeServe(resultOK, 100, 0.002)
	m.observeServe(resultNotFound, 0, 0.001)

	mfs, err := reg.Gather()
	require.NoError(t, err)

	got := map[string]bool{}
	for _, mf := range mfs {
		got[mf.GetName()] = true
	}
	require.True(t, got["jetstream_getblock_requests_total"])
	require.True(t, got["jetstream_getblock_served_bytes_total"])
	require.True(t, got["jetstream_getblock_duration_seconds"])
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/xrpcapi/ -run TestMetrics -v`
Expected: compile error — `undefined: Metrics`, `NewMetrics`, `resultOK`, etc.

- [ ] **Step 3: Implement the metrics type**

Create `internal/xrpcapi/metrics.go`:

```go
package xrpcapi

import "github.com/prometheus/client_golang/prometheus"

const (
	metricsNamespace = "jetstream"
	metricsSubsystem = "getblock"

	resultOK         = "ok"
	resultNotFound   = "not_found"
	resultBadRequest = "bad_request"
	resultError      = "error"
)

// Metrics owns the prometheus state for getBlock. A nil *Metrics is valid:
// every method is a no-op, so tests and the zero-config server can skip
// registration.
type Metrics struct {
	requests   *prometheus.CounterVec
	servedByte prometheus.Counter
	duration   prometheus.Histogram
}

// NewMetrics registers and returns the getBlock metrics on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "requests_total", Help: "getBlock requests served, by outcome.",
		}, []string{"result"}),
		servedByte: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "served_bytes_total", Help: "Block frame bytes written to clients.",
		}),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "duration_seconds", Help: "getBlock handler wall-clock latency.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 12),
		}),
	}
	reg.MustRegister(m.requests, m.servedByte, m.duration)
	return m
}

// observeServe records one request outcome. Nil-safe.
func (m *Metrics) observeServe(result string, bytes int, seconds float64) {
	if m == nil {
		return
	}
	m.requests.WithLabelValues(result).Inc()
	if bytes > 0 {
		m.servedByte.Add(float64(bytes))
	}
	m.duration.Observe(seconds)
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test ./internal/xrpcapi/ -run TestMetrics -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
gofmt -w internal/xrpcapi/metrics.go internal/xrpcapi/metrics_test.go
git add internal/xrpcapi/metrics.go internal/xrpcapi/metrics_test.go
git commit -m "xrpcapi: add nil-safe getBlock Prometheus metrics"
```

---

### Task 4: `getBlockHandler` + Config refactor + wiring

Implement the handler, register the NSID, and wire metrics/tracer through the server constructor. The constructor today telescopes (`New` → `NewWithReady` → `NewWithReadyAndCache` → `NewWithReadyAndCacheAndOverlay`); adding two more positional args worsens this, so collapse them into a single `New(Config)`. Five call sites change (`runtime.go` + four test files).

**Files:**
- Create: `internal/xrpcapi/getblock.go`
- Modify: `internal/xrpcapi/server.go` (replace telescoping constructors with `New(Config)`; register `getBlock`)
- Modify: `internal/jetstreamd/runtime.go:384` (call `New(Config{...})`; construct + pass `*xrpcapi.Metrics` and tracer)
- Modify: `internal/xrpcapi/getsegment_test.go:48`, `internal/xrpcapi/server_test.go:39`, `internal/xrpcapi/gettombstones_test.go:29,83`, `internal/xrpcapi/testsupport_test.go:64-73` (migrate to `New(Config)`)
- Test: `internal/xrpcapi/getblock_test.go`

**Interfaces:**
- Consumes: `segment.ReadBlockFrame` (Task 1); `segment.ReadSealedHeader`; `xrpcapi.Metrics`/`NewMetrics`/result constants (Task 3); `SegmentSource.SegmentByIdx(idx uint64) (manifest.SegmentFileRef, bool)` with `SegmentFileRef.Path` (`internal/xrpcapi/server.go:21`, `internal/manifest`); `ingest.ParseSegmentIndex(name string) (uint64, bool)`; `checksumHex(uint64) string`; `cacheControlHeader(time.Duration) string` (`internal/xrpcapi/getsegment.go:93`); atmos `xrpcserver.Request` with `r.Params.String/Int64/Has` and `r.HTTPReq`; `xrpcserver.InvalidRequest`, `xrpcserver.InternalError`; `xrpc.Error`; `obs.Tracer(name string) trace.Tracer` (`internal/obs/tracing.go:92`) and `go.opentelemetry.io/otel/trace` + `go.opentelemetry.io/otel/attribute`.
- Produces:
  - `type Config struct { Src SegmentSource; Logger *slog.Logger; Ready ReadyFunc; CacheMaxAge time.Duration; Overlay OverlaySource; Metrics *Metrics; Tracer trace.Tracer }`
  - `func New(cfg Config) *Server`
  - `type getBlockHandler struct { ... }` implementing `xrpcserver.Handler`.

- [ ] **Step 1: Write the failing handler tests**

First extend the test fixture to produce multi-block segments, then add `getblock_test.go`. Append to `internal/xrpcapi/testsupport_test.go` a helper that writes a segment with a caller-chosen block count (the existing `writeSealedSegment` makes a single 4-event block):

```go
// writeSealedSegmentBlocks writes a sealed segment at index idx with blockCount
// blocks of perBlock events each (seq starting at seqStart) and returns its path.
func writeSealedSegmentBlocks(t *testing.T, dir string, idx, seqStart uint64, perBlock, blockCount int) string {
	t.Helper()
	path := filepath.Join(dir, ingest.SegmentFilename(idx))
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: perBlock})
	require.NoError(t, err)
	seq := seqStart
	for b := 0; b < blockCount; b++ {
		for i := 0; i < perBlock; i++ {
			_, err = w.Append(segment.Event{
				Seq: seq, IndexedAt: int64(1_730_000_000_000_000 + seq*1_000),
				Kind: segment.KindCreate, DID: "did:plc:test",
				Collection: "app.bsky.feed.post", Rkey: "rkey", Rev: "rev",
				Payload: []byte{0xa0},
			})
			require.NoError(t, err)
			seq++
		}
	}
	_, err = w.Seal()
	require.NoError(t, err)
	return path
}
```

Create `internal/xrpcapi/getblock_test.go`:

```go
package xrpcapi

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// newBlockTestServer seeds one sealed segment (idx 0) with the given block
// shape and returns a running httptest server + segment path. Mirrors the
// inline httptest.NewServer(s.Handler()) pattern used by the other tests.
func newBlockTestServer(t *testing.T, perBlock, blockCount int) (*httptest.Server, string) {
	t.Helper()
	dir := t.TempDir()
	path := writeSealedSegmentBlocks(t, dir, 0, 1, perBlock, blockCount)
	m, err := manifest.Open(manifest.Options{SegmentsDir: dir, Logger: slog.Default()})
	require.NoError(t, err)
	srv := New(Config{Src: m, Logger: slog.Default()})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, path
}

func blockURL(base, segName string, idx int) string {
	return base + "/xrpc/network.bsky.jetstream.getBlock?segment=" + segName +
		"&blockIndex=" + fmt.Sprint(idx)
}

func TestGetBlock_BytesMatchOnDisk(t *testing.T) {
	ts, path := newBlockTestServer(t, 2, 3)
	segName := ingest.SegmentFilename(0)

	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	hdr, err := segment.ReadSealedHeader(f)
	require.NoError(t, err)
	require.Equal(t, uint32(3), hdr.BlockCount)

	for idx := 0; idx < 3; idx++ {
		resp := doGet(t, blockURL(ts.URL, segName, idx))
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, "application/octet-stream", resp.Header.Get("Content-Type"))
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		want, err := segment.ReadBlockFrame(f, hdr, idx)
		require.NoError(t, err)
		require.Equal(t, want, body, "block %d", idx)

		require.Equal(t, fmt.Sprintf("%q", checksumHex(hdr.Checksum)+":"+fmt.Sprint(idx)),
			resp.Header.Get("ETag"))
	}
}

func TestGetBlock_ETagDiffersPerBlock(t *testing.T) {
	ts, _ := newBlockTestServer(t, 2, 2)
	segName := ingest.SegmentFilename(0)
	e0 := doGet(t, blockURL(ts.URL, segName, 0)).Header.Get("ETag")
	e1 := doGet(t, blockURL(ts.URL, segName, 1)).Header.Get("ETag")
	require.NotEqual(t, e0, e1)
}

func TestGetBlock_NotModified(t *testing.T) {
	ts, _ := newBlockTestServer(t, 2, 2)
	segName := ingest.SegmentFilename(0)
	url := blockURL(ts.URL, segName, 0)
	etag := doGet(t, url).Header.Get("ETag")
	resp := doGetWith(t, url, func(r *http.Request) { r.Header.Set("If-None-Match", etag) })
	require.Equal(t, http.StatusNotModified, resp.StatusCode)
}

func TestGetBlock_Errors(t *testing.T) {
	ts, _ := newBlockTestServer(t, 2, 2)
	segName := ingest.SegmentFilename(0)

	// malformed segment name -> 400
	require.Equal(t, http.StatusBadRequest,
		doGet(t, ts.URL+"/xrpc/network.bsky.jetstream.getBlock?segment=nope&blockIndex=0").StatusCode)
	// missing blockIndex -> 400
	require.Equal(t, http.StatusBadRequest,
		doGet(t, ts.URL+"/xrpc/network.bsky.jetstream.getBlock?segment="+segName).StatusCode)
	// negative blockIndex -> 400
	require.Equal(t, http.StatusBadRequest,
		doGet(t, blockURL(ts.URL, segName, -1)).StatusCode)
	// unknown segment -> 404 SegmentNotFound
	missing := ingest.SegmentFilename(99)
	require.Equal(t, http.StatusNotFound, doGet(t, blockURL(ts.URL, missing, 0)).StatusCode)
	// blockIndex == block_count -> 404 BlockNotFound
	require.Equal(t, http.StatusNotFound, doGet(t, blockURL(ts.URL, segName, 2)).StatusCode)
}
```

Convention confirmed (verified against the existing tests): `doGet(t, url)` / `doGetWith(t, url, fn)` in `testsupport_test.go` issue context-bound GETs against a full URL via `http.DefaultClient`; each test stands up its own server inline with `ts := httptest.NewServer(srv.Handler())`. The `newBlockTestServer` helper above follows that pattern exactly — no shared helper is added.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/xrpcapi/ -run TestGetBlock -v`
Expected: compile error — `New(Config)` and `getBlock` route do not exist yet.

- [ ] **Step 3: Refactor the constructor to `New(Config)`**

In `internal/xrpcapi/server.go`, replace the four telescoping constructors with a single `Config`-based one and register all four NSIDs (including `getBlock`):

```go
// Config holds the dependencies for the XRPC server. Zero values are valid:
// a nil Logger defaults to slog.Default(); a nil Ready disables the readiness
// gate; a zero CacheMaxAge disables segment/block caching; a nil Overlay omits
// getTombstones; nil Metrics/Tracer make getBlock observability no-ops.
type Config struct {
	Src         SegmentSource
	Logger      *slog.Logger
	Ready       ReadyFunc
	CacheMaxAge time.Duration
	Overlay     OverlaySource
	Metrics     *Metrics
	Tracer      trace.Tracer
}

// New constructs the XRPC server and registers all jetstream NSIDs.
func New(cfg Config) *Server {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{src: cfg.Src, logger: logger, xrpc: &xrpcserver.Server{}, overlay: cfg.Overlay}
	s.xrpc.HandleQuery("network.bsky.jetstream.getSegment", withReady(cfg.Ready, &getSegmentHandler{
		src: cfg.Src, logger: logger, cacheMaxAge: cfg.CacheMaxAge,
	}))
	s.xrpc.HandleQuery("network.bsky.jetstream.getBlock", withReady(cfg.Ready, &getBlockHandler{
		src: cfg.Src, logger: logger, cacheMaxAge: cfg.CacheMaxAge,
		metrics: cfg.Metrics, tracer: cfg.Tracer,
	}))
	s.xrpc.HandleQuery("network.bsky.jetstream.listSegments", withReady(cfg.Ready, newListSegmentsHandler(cfg.Src)))
	if cfg.Overlay != nil {
		s.xrpc.HandleQuery("network.bsky.jetstream.getTombstones", withReady(cfg.Ready, newGetTombstonesHandler(cfg.Overlay)))
	}
	return s
}
```

Add the import `"go.opentelemetry.io/otel/trace"` to `server.go`. Delete the old `New`, `NewWithReady`, `NewWithReadyAndCache`, `NewWithReadyAndCacheAndOverlay` functions.

- [ ] **Step 4: Implement the handler**

Create `internal/xrpcapi/getblock.go`:

```go
package xrpcapi

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/atmos/xrpcserver"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// getBlockHandler serves one sealed-segment block as its raw stored zstd frame.
// Like getSegmentHandler it implements xrpcserver.Handler directly so it can use
// http.ServeContent for conditional/Range handling. The block bytes, block
// count, and ETag are all derived from a single freshly-opened fd — never the
// manifest — so a concurrent compaction rewrite cannot splice generations.
type getBlockHandler struct {
	src         SegmentSource
	logger      *slog.Logger
	cacheMaxAge time.Duration
	metrics     *Metrics
	tracer      trace.Tracer
}

func (h *getBlockHandler) ServeXRPC(ctx context.Context, w http.ResponseWriter, r *xrpcserver.Request) error {
	start := time.Now()
	var span trace.Span
	if h.tracer != nil {
		ctx, span = h.tracer.Start(ctx, "getBlock")
		defer span.End()
	}
	result := resultError
	served := 0
	defer func() { h.metrics.observeServe(result, served, time.Since(start).Seconds()) }()

	fail := func(res string, err error) error {
		result = res
		if span != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		return err
	}

	name, err := r.Params.String("segment")
	if err != nil {
		return fail(resultBadRequest, err)
	}
	idx, ok := ingest.ParseSegmentIndex(name)
	if !ok {
		return fail(resultBadRequest, xrpcserver.InvalidRequest("malformed segment name"))
	}
	blockIdx64, err := r.Params.Int64("blockIndex")
	if err != nil {
		return fail(resultBadRequest, err)
	}
	if blockIdx64 < 0 {
		return fail(resultBadRequest, xrpcserver.InvalidRequest("blockIndex must be >= 0"))
	}
	blockIdx := int(blockIdx64)
	if span != nil {
		span.SetAttributes(attribute.Int64("segment.idx", int64(idx)),
			attribute.Int("block.index", blockIdx))
	}

	ref, ok := h.src.SegmentByIdx(idx)
	if !ok {
		return fail(resultNotFound, &xrpc.Error{
			StatusCode: http.StatusNotFound, Name: "SegmentNotFound", Message: "segment not found",
		})
	}

	f, err := os.Open(ref.Path)
	if err != nil {
		h.logger.Error("getBlock: open sealed file failed",
			slog.String("name", name), slog.String("path", ref.Path), slog.Any("err", err))
		return fail(resultError, xrpcserver.InternalError("failed to open segment"))
	}
	defer func() { _ = f.Close() }()

	hdr, err := segment.ReadSealedHeader(f)
	if err != nil {
		h.logger.Error("getBlock: read sealed header failed",
			slog.String("name", name), slog.String("path", ref.Path), slog.Any("err", err))
		return fail(resultError, xrpcserver.InternalError("failed to read segment header"))
	}
	if blockIdx >= int(hdr.BlockCount) {
		return fail(resultNotFound, &xrpc.Error{
			StatusCode: http.StatusNotFound, Name: "BlockNotFound", Message: "block index out of range",
		})
	}

	frame, err := segment.ReadBlockFrame(f, hdr, blockIdx)
	if err != nil {
		h.logger.Error("getBlock: read block frame failed",
			slog.String("name", name), slog.Int("block", blockIdx), slog.Any("err", err))
		return fail(resultError, xrpcserver.InternalError("failed to read block"))
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", fmt.Sprintf("%q", checksumHex(hdr.Checksum)+":"+fmt.Sprint(blockIdx)))
	w.Header().Set("Cache-Control", cacheControlHeader(h.cacheMaxAge))

	result = resultOK
	served = len(frame)
	if span != nil {
		span.SetAttributes(attribute.Int("block.compressed_size", served))
		span.SetStatus(codes.Ok, "")
	}

	// ServeContent handles If-None-Match->304, Range, and Content-Length. After
	// this point the response may be partially written, so per the Handler
	// contract we return nil.
	http.ServeContent(w, r.HTTPReq, name, ref.ModTime, bytes.NewReader(frame))
	return nil
}
```

Note: `SegmentFileRef.ModTime` is confirmed present (`internal/manifest/manifest.go:114`), so `ref.ModTime` is correct.

- [ ] **Step 5: Migrate the constructor call sites**

In `internal/jetstreamd/runtime.go` around line 384, construct metrics + tracer and switch to `New(Config{...})`:

```go
	xrpcMetrics := xrpcapi.NewMetrics(metrics.Registry)
	xrpcSrv := xrpcapi.New(xrpcapi.Config{
		Src:    mft,
		Logger: processLogger,
		Ready: func(ctx context.Context) error {
			if !lifecycle.IsSteadyState(metaStore) {
				return errors.New("bootstrap in progress")
			}
			if err := mft.Wait(ctx); err != nil {
				return fmt.Errorf("manifest warming up: %w", err)
			}
			return nil
		},
		CacheMaxAge: opts.SegmentCacheMaxAge,
		Overlay:     overlayCache,
		Metrics:     xrpcMetrics,
		Tracer:      obs.Tracer("xrpcapi"),
	})
```

Migrate the test call sites to `New(Config)`:
- `internal/xrpcapi/testsupport_test.go:64-73` `newTestServer` (currently `return New(m, slog.Default()), dir`): change to `return New(Config{Src: m, Logger: slog.Default()}), dir`
- `internal/xrpcapi/getsegment_test.go:48`: `New(Config{Src: s.src, Logger: s.logger, CacheMaxAge: time.Hour})`
- `internal/xrpcapi/server_test.go:39`: `New(Config{Src: s.src, Logger: s.logger, Ready: func(_ context.Context) error { ... }})`
- `internal/xrpcapi/gettombstones_test.go:29,83`: `New(Config{Src: s.src, Logger: s.logger, Overlay: &fakeOverlay{...}})` and the gated variant with `Ready:`.

- [ ] **Step 6: Run the package tests + build**

Run: `go build ./... && go test ./internal/xrpcapi/ ./internal/jetstreamd/ -v`
Expected: PASS — all new getBlock tests and the migrated existing tests.

- [ ] **Step 7: Commit**

```bash
gofmt -w internal/xrpcapi/ internal/jetstreamd/runtime.go
goimports -w internal/xrpcapi/ internal/jetstreamd/runtime.go
git add internal/xrpcapi/ internal/jetstreamd/runtime.go
git commit -m "xrpcapi: add getBlock handler; collapse constructors into New(Config)"
```

---

### Task 5: Oracle end-to-end verification

Add an end-to-end test that drives the real server against the simulator and checks every served block against the oracle's independent decode. This is the headline correctness test.

**Files:**
- Test: `cmd/simulator/getblock_e2e_test.go` (new; alongside the existing `e2e_test.go` / `e2e_helpers_test.go`)

**Interfaces:**
- Consumes: existing e2e helpers (`buildJetstreamForTest`, the simulator world setup, subprocess spawn, `freePortAddr` — see `cmd/simulator/e2e_helpers_test.go` and `e2e_test.go`); `segment.Open`/`Reader.DecodeBlock`/`Reader.Blocks`/`ReadSealedHeader` for the oracle-side decode; the `listSegments` and `getBlock` HTTP routes.
- Produces: a `go test`-gated end-to-end assertion; no production code.

- [ ] **Step 1: Study the existing e2e harness**

Run: `sed -n '1,80p' cmd/simulator/e2e_test.go; sed -n '1,80p' cmd/simulator/e2e_helpers_test.go`
Expected: identify how the test (a) reaches steady state, (b) learns the server's public address, and (c) finds the data dir (segments live at `<data-dir>/segments`). Reuse these exact helpers.

- [ ] **Step 2: Write the e2e test**

Create `cmd/simulator/getblock_e2e_test.go`. Adapt setup to the harness discovered in Step 1 (the skeleton below shows the assertion logic — match the existing test's bootstrap/steady-state/addr/data-dir plumbing rather than re-inventing it):

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func TestEndToEnd_GetBlockMatchesOracle(t *testing.T) {
	if testing.Short() {
		t.Skip("heavy e2e test")
	}

	// --- Reuse the existing harness to reach steady state. ---
	// addr, dataDir := startJetstreamAgainstSimulator(t)  // per Step 1 helpers

	// 1. Enumerate sealed segments via listSegments.
	listResp, err := http.Get("http://" + addr + "/xrpc/network.bsky.jetstream.listSegments?limit=1000")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, listResp.StatusCode)
	var list struct {
		Segments []struct {
			Name  string `json:"name"`
			Index int64  `json:"index"`
		} `json:"segments"`
	}
	require.NoError(t, json.NewDecoder(listResp.Body).Decode(&list))
	_ = listResp.Body.Close()
	require.NotEmpty(t, list.Segments, "expected at least one sealed segment")

	for _, seg := range list.Segments {
		segPath := filepath.Join(dataDir, "segments", seg.Name)

		// Oracle side: open the file independently and decode every block.
		r, err := segment.Open(segment.ReaderConfig{Path: segPath})
		require.NoError(t, err)
		blockCount := len(r.Blocks())

		f, err := os.Open(segPath)
		require.NoError(t, err)
		hdr, err := segment.ReadSealedHeader(f)
		require.NoError(t, err)

		for idx := 0; idx < blockCount; idx++ {
			url := fmt.Sprintf("http://%s/xrpc/network.bsky.jetstream.getBlock?segment=%s&blockIndex=%d",
				addr, seg.Name, idx)
			resp, err := http.Get(url)
			require.NoError(t, err)
			require.Equalf(t, http.StatusOK, resp.StatusCode, "%s block %d", seg.Name, idx)
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()

			// (a) served bytes == raw stored frame.
			wantFrame, err := segment.ReadBlockFrame(f, hdr, idx)
			require.NoError(t, err)
			require.Equalf(t, wantFrame, body, "%s block %d frame bytes", seg.Name, idx)

			// (b) ETag == "checksum:idx".
			require.Equalf(t,
				fmt.Sprintf("%q", fmt.Sprintf("%016x:%d", hdr.Checksum, idx)),
				resp.Header.Get("ETag"), "%s block %d etag", seg.Name, idx)

			// (c) second request with If-None-Match -> 304.
			req, _ := http.NewRequest(http.MethodGet, url, nil)
			req.Header.Set("If-None-Match", resp.Header.Get("ETag"))
			resp2, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			require.Equalf(t, http.StatusNotModified, resp2.StatusCode, "%s block %d revalidate", seg.Name, idx)
			_ = resp2.Body.Close()

			// (d) served frame decodes to exactly the oracle's events for this block.
			gotEvents, _, err := segmentDecodeFrame(t, wantFrame) // tiny helper below
			require.NoError(t, err)
			oracleEvents, err := r.DecodeBlock(idx)
			require.NoError(t, err)
			require.Equalf(t, oracleEvents, gotEvents, "%s block %d decoded events", seg.Name, idx)
		}

		// Negative: blockIndex == blockCount -> 404 BlockNotFound.
		nf, err := http.Get(fmt.Sprintf(
			"http://%s/xrpc/network.bsky.jetstream.getBlock?segment=%s&blockIndex=%d",
			addr, seg.Name, blockCount))
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, nf.StatusCode)
		_ = nf.Body.Close()

		_ = f.Close()
		_ = r.Close()
	}

	// Negative: unknown segment -> 404 SegmentNotFound.
	missing := list.Segments[len(list.Segments)-1].Name // mutate to a non-existent index
	_ = sort.Strings
	unknown, err := http.Get("http://" + addr +
		"/xrpc/network.bsky.jetstream.getBlock?segment=seg_zzzzzzzzzz.jss&blockIndex=0")
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, unknown.StatusCode)
	_ = unknown.Body.Close()
	_ = missing
}
```

Note: `segmentDecodeFrame` decodes a raw frame the same way the client will. If the `segment` package's frame decoder (`decodeBlockCompressedSized`) is unexported, decode via a `segment.Reader` instead (compare `r.DecodeBlock(idx)` to itself is trivial — so for the decode-equivalence check, prefer asserting the served bytes equal `ReadBlockFrame` output, which Task 1 already proves decodes identically). If no exported frame-decode entrypoint exists, drop sub-check (d) here (Task 1 covers decode equivalence) and keep (a)/(b)/(c) plus the negatives. Decide based on what `segment` exports.

- [ ] **Step 3: Run the e2e test**

Run: `go test ./cmd/simulator/ -run TestEndToEnd_GetBlockMatchesOracle -v`
Expected: PASS. If it skips under `-short`, run without `-short`.

- [ ] **Step 4: Full suite + vet**

Run: `go build ./... && go vet ./... && go test ./...`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
gofmt -w cmd/simulator/getblock_e2e_test.go
git add cmd/simulator/getblock_e2e_test.go
git commit -m "test: e2e oracle verification of getBlock against simulator"
```

---

## Self-Review

**Spec coverage:**
- Lexicon (spec §3) → Task 2. ✓
- Wire contract: body = raw frame no prefix, ETag `checksum:idx`, Cache-Control reuse (spec §4) → Tasks 1 (bytes) + 4 (headers). ✓
- Handler flow, open-fresh generation pinning, bounds checks (spec §5) → Task 4 + Task 1's offset validation. ✓
- `segment.ReadBlockFrame` primitive (spec §5) → Task 1. ✓
- Wiring/registration behind readiness gate (spec §6) → Task 4. ✓
- Metrics + per-request span (spec §7) → Task 3 (metrics) + Task 4 (span). ✓
- Testing: unit, happy, caching/conditional/range, sad, round-trip, oracle e2e (spec §8) → Tasks 1, 4, 5. ✓
- Non-goals (active blocks, batch, manifest changes, fd-pinning) → not implemented, as intended. ✓

**Placeholder scan:** No TBD/TODO. Two deliberate "adapt to existing harness" notes (Task 4 Step 1a, Task 5 Steps 1–2) point at concrete code to read first, with exact fallbacks specified — they are discovery steps, not unspecified work.

**Type consistency:** `ReadBlockFrame(io.ReaderAt, Header, int) ([]byte, error)` used identically in Tasks 1, 4, 5. `Config` fields and `New(Config)` consistent across Tasks 4 call sites. `observeServe(result, bytes, seconds)` + result constants consistent between Tasks 3 and 4. ETag string `fmt.Sprintf("%q", checksumHex(c)+":"+fmt.Sprint(idx))` matches the test's expected `%q` of `%016x:%d`.

**Verified against source while writing:** `SegmentFileRef.ModTime` exists (`manifest.go:114`); `doGet`/`doGetWith` take a full URL and tests stand up `httptest.NewServer(srv.Handler())` inline; `newTestServer` currently calls `New(m, slog.Default())` (migrated in Task 4). **Risk flagged for the implementer:** whether `segment` exports a frame-decode entrypoint affects Task 5 sub-check (d) — explicit fallback specified (rely on Task 1's decode-equivalence proof + keep checks a/b/c).
