# getBlock XRPC Endpoint Design

Date: 2026-06-16
Author: jcalabro (with Claude)
Status: Approved for planning

## 1. Summary

Add a new XRPC query, `network.bsky.jetstream.getBlock`, that serves a single
sealed-segment block by index as an `application/octet-stream`. It returns the
raw zstd block frame exactly as stored on disk (the `[block_len]byte` ZSTD frame
*without* the 8-byte length prefix), with a strong, immutable ETag.

This is a CDN-cacheable primitive. "Batching" is intentionally **client-side**:
a client fans out N concurrent cacheable GETs over HTTP/2 rather than issuing one
large batch request. The upcoming "query plan" endpoint is what hands a client
the list of `(segment, blockIndex)` pairs to fetch; `getBlock` is the cheap,
amplifiable primitive that makes serving such a plan inexpensive at scale.

### Motivating use case

Small atproto collections (e.g. `standard.site`, a few thousand records) appear
in only a handful of blocks across the archive. Downloading whole 256 MB sealed
segment files to replay such a collection is enormously wasteful. `getBlock`
lets a client download exactly the blocks it needs. Because the response is
immutable and cacheable, a popular small-collection replay amplifies across the
CDN: the first client warms the cache, the rest never hit origin.

Large collections (`app.bsky.feed.post`, `app.bsky.feed.like`) appear in
essentially every block of every segment; for those, downloading the whole
sealed file via `getSegment` remains the better, more cache-friendly choice.
`getBlock` does not change that; it complements it.

## 2. Goals and Non-Goals

### Goals

- Serve one sealed-segment block by `(segment name, block index)`.
- Return the smallest possible payload: the raw stored zstd frame, no
  server-side decompression, no re-framing.
- Be CDN-cacheable with a strong, immutable ETag and the existing
  segment cache-control policy.
- Be a hot-path-efficient read: minimal syscalls, no decompression, no
  bloom/collection-index parsing.
- Match the correctness discipline of the existing `getSegment` handler so a
  concurrent compaction rewrite can never splice two file generations together.
- Emit metrics and a per-request trace span in the project's house style.
- Be exhaustively tested, including end-to-end verification against the
  oracle simulator's independent ground-truth model.

### Non-Goals (YAGNI)

- **Active-segment blocks.** v1 serves sealed segments only. The active segment
  has no footer (no block index, no per-block blooms, no collection index), so
  precise per-collection selection is impossible there, and its bytes are
  covered by the live websocket tail. Designed so active blocks can be added
  later without a breaking change (see §9).
- **Batch POST procedure.** Client-side fan-out of cacheable GETs is preferred
  for the CDN-amplification goal. A batch endpoint is explicitly not built.
- **Block-metadata discovery.** `getBlock` serves *by index*; it does not tell a
  client which blocks to fetch. That is the future query-plan endpoint's job,
  built on the manifest's already-resident block metadata.
- **Manifest changes.** `getBlock` requires no changes to the manifest. The
  manifest already resolves a segment index to a path in memory; serving block
  bytes is a disk read no manifest design avoids. fd-pinning / LRU fd caching is
  captured as future work tied to the query-plan endpoint (see §9), not built
  here.

## 3. Lexicon

New file: `lexicons/network/bsky/jetstream/getBlock.json`

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

Codegen: `getBlock` is a raw octet-stream query with no JSON output schema, so
(like `getSegment` / `getTombstones`) it generates no meaningful output struct.
Re-running lexgen after adding the file is harmless and keeps generated artifacts
consistent; the handler is hand-written.

## 4. Wire Contract

### Request

```
GET /xrpc/network.bsky.jetstream.getBlock?segment=seg_000000002a.jss&blockIndex=7
```

### Response (200)

```
Content-Type: application/octet-stream
ETag: "a1b2c3d4e5f6a7b8:7"
Cache-Control: public, max-age=<segment-cache-max-age>   (or "public, no-cache" when unset)
Accept-Ranges: bytes
Content-Length: <compressed frame length>

<raw zstd block frame bytes>
```

- **Body**: exactly `BlockInfo.CompressedSize` bytes — the ZSTD frame as stored,
  *excluding* the leading 8-byte length prefix. The frame carries its own zstd
  content checksum, so a client detects corruption on decode.
- **ETag**: `"<segment-xxh3-hex>:<blockIndex>"`. The 16-char hex is the same
  segment checksum `getSegment` and `listSegments` expose (`checksumHex`). The
  `:<blockIndex>` suffix makes the validator unique per block within a segment.
  This is a *strong* validator (RFC 9110: value wrapped in double quotes).
- **Cache-Control**: reuses the existing `--segment-cache-max-age`
  (`JETSTREAM_SEGMENT_CACHE_MAX_AGE`) flag via the existing `cacheControlHeader`
  helper. No new config. A block is exactly as immutable / as rewritable as its
  segment, so it shares the segment's caching policy.

### Why this ETag is correct (immutability argument)

The segment xxh3 covers the header + footer (metadata), not the block bodies.
But compaction is the only thing that rewrites a sealed segment, and when it
drops rows from a block, that block's `event_count` changes, which changes the
footer's block index, which changes the segment checksum. zstd encoding is
deterministic, so identical block input produces identical frame bytes.
Therefore:

- **different block bytes ⇒ different segment checksum ⇒ different ETag** (always).
  The dangerous direction — different bytes served under the same ETag, which
  would poison a cache — is impossible.
- identical block bytes under a new checksum (after an unrelated rewrite
  elsewhere in the segment) merely causes one redundant CDN refetch. Harmless.

Block *numbering* is stable across compaction generations: DESIGN.md §3.3
guarantees rewrites preserve block topology, and an emptied block remains present
as an `event_count=0` block. So `blockIndex` addresses the same logical block
across generations.

### Errors

| Condition | XRPC error | HTTP status |
|---|---|---|
| Missing/empty `segment` param | InvalidRequest | 400 |
| Malformed segment name (fails `ingest.ParseSegmentIndex`) | InvalidRequest | 400 |
| Missing / non-integer / negative `blockIndex` | InvalidRequest | 400 |
| No sealed segment for that index | `SegmentNotFound` | 404 |
| `blockIndex >= block_count` | `BlockNotFound` | 404 |
| Manifest knows the segment but file open/read fails (rotation/deletion race) | InternalServerError | 500 |
| Server not in steady state / manifest warming | ServiceUnavailable | 503 |

The error *names* `SegmentNotFound` and `BlockNotFound` must match the lexicon's
declared names exactly (not the generic `NotFound`), mirroring how `getSegment`
returns a literal `&xrpc.Error{Name: "SegmentNotFound"}` so clients matching on
the published name work.

## 5. Handler Design

New file: `internal/xrpcapi/getblock.go`. Like `getSegment`, it implements
`xrpcserver.Handler` directly (not `xrpcserver.Query`/`RawQuery`) so it can drive
`http.ServeContent` for Range / conditional handling.

```go
type getBlockHandler struct {
    src         SegmentSource
    logger      *slog.Logger
    cacheMaxAge time.Duration
    metrics     *Metrics // nil-safe
    tracer      trace.Tracer
}
```

### Request flow

1. Parse `segment` → `idx` via `ingest.ParseSegmentIndex`; 400 on malformed.
2. Parse `blockIndex` (int64, `>= 0`); 400 on missing/non-integer/negative.
3. `src.SegmentByIdx(idx)` → path. Not found ⇒ `SegmentNotFound` (404).
   This is the **only** manifest interaction: an in-memory path lookup.
4. **Open the file fresh** (`os.Open(ref.Path)`). All of the following come from
   *this one fd*, never the manifest — this is the generation-pinning rule:
   - `segment.ReadSealedHeader(f)` (one 256-byte pread) → `Header`. Validates
     magic/version, so a corrupt/foreign file errors instead of serving a
     confident-but-wrong ETag. Gives us `Checksum` (ETag), `BlockCount`
     (range check), and `BlockIndexOffset`.
   - Range-check `blockIndex < BlockCount`; else `BlockNotFound` (404).
   - Read one 52-byte block-index entry at `BlockIndexOffset + blockIndex*52`
     → `BlockInfo{Offset, CompressedSize, ...}`, with bounds checks so a corrupt
     index can't drive an out-of-bounds or oversized read.
   - Read `CompressedSize` bytes at `Offset + 8` (skip the length prefix) → frame.
5. Set headers (Content-Type, ETag from this fd's checksum + blockIndex,
   Cache-Control), then serve the frame bytes via `http.ServeContent` over a
   `bytes.Reader` for free `If-None-Match`→304, Range→206, and Content-Length.
   Per the `xrpcserver.Handler` contract, return `nil` after `ServeContent`.

Header/index reads happen **before** writing any response byte, so any failure
becomes a clean XRPC error envelope rather than a corrupt partial 200.

### Why open-fresh, not manifest offsets (corruption guard)

`rename(2)` is atomic on POSIX, so `os.Open(path)` returns either the fully-old
or fully-new inode — never torn. A freshly-opened reader is therefore internally
self-consistent: its header checksum, block-index offsets, and block bytes all
belong to one generation. If we instead took the offset from the in-memory
manifest and pread it against a separately-opened file, a concurrent compaction
rename→refresh could splice a stale offset onto a new file generation — silent
data corruption. This is the exact hazard `getSegment` already documents
("Download validators come from the fd we actually serve, never the manifest").
We follow the same idiom. Per the project directive: crash (or error) over
corruption.

This open-fresh cost is bounded by inspection to a few microseconds on a hot
dentry: one `open()` plus two page-cache-resident preads (256 B + 52 B) on top of
the unavoidable frame pread. `getSegment` already pays this.

### New segment-package primitive

To keep segment-format knowledge out of `xrpcapi` (preserving the existing
layering where only `xrpcapi` depends on atmos and the format lives in
`segment`), add a focused helper to the `segment` package:

```go
// ReadBlockFrame reads the raw, stored zstd frame for block idx (without the
// 8-byte length prefix) using only the fixed header — no footer/bloom/collection
// parsing and no decompression. r is the fd for the sealed file; hdr is its
// already-read fixed header. Returns ErrBlockOutOfRange if idx >= hdr.BlockCount.
func ReadBlockFrame(r io.ReaderAt, hdr Header, idx int) ([]byte, error)
```

This is the raw-bytes sibling of `Reader.DecodeBlock`. It does the index-entry
pread + frame pread + bounds checks, and is independently unit-tested. The
handler calls `ReadSealedHeader` then `ReadBlockFrame`, avoiding the full
`segment.Open` (which also zstd-decodes the collection index and loads blooms —
all unnecessary here).

## 6. Wiring

- Register in `internal/xrpcapi/server.go` alongside the others:
  `s.xrpc.HandleQuery("network.bsky.jetstream.getBlock", withReady(ready, &getBlockHandler{...}))`,
  behind the same readiness gate (steady-state + manifest-warm) the other NSIDs use.
- Plumb `cacheMaxAge` (already threaded as `opts.SegmentCacheMaxAge`) and a new
  `*xrpcapi.Metrics` + tracer through the `NewWithReadyAndCacheAndOverlay`
  constructor. The constructor already takes the cache age; add the metrics/
  tracer dependency in the same call site in `internal/jetstreamd/runtime.go`
  (around the existing `xrpcSrv := xrpcapi.NewWithReadyAndCacheAndOverlay(...)`).
- No new CLI flags.

## 7. Metrics and Tracing

House style: Prometheus, namespace `jetstream`. Add a new `xrpcapi` metrics type
(the package has none today) in `internal/xrpcapi/metrics.go`, subsystem
`getblock`, nil-safe like `overlay.Metrics`:

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `jetstream_getblock_requests_total` | Counter | `result=ok\|not_found\|bad_request\|error` | requests served by outcome |
| `jetstream_getblock_served_bytes_total` | Counter | — | frame bytes written to clients (200s) |
| `jetstream_getblock_duration_seconds` | Histogram | — | handler wall-clock latency |

Constructed via `NewMetrics(reg prometheus.Registerer)` and registered from
`runtime.go` against `metrics.Registry`, exactly like `manifest.NewMetrics` /
`overlay.NewMetrics`.

Tracing: wrap the handler body in a per-request span (the operator samples at
their collector, so no in-process sampling). Use the `obs` tracer
(`obs.Tracer("xrpcapi")`); set attributes `segment.idx`, `block.index`,
`block.compressed_size`, and `result`, and record errors on the span. This is a
deliberate, operator-sanctioned exception to the `obs.Span` hot-path rule (which
`getSegment` follows by emitting no span); it is justified because per-block
fetch latency is a primary SLI for the query-plan workload.

## 8. Testing

TDD: tests written before implementation. Use the existing
`internal/xrpcapi/testsupport_test.go` harness (`newTestServer`,
`writeSealedSegment`, `doGet`, `doGetWith`, `rawFile`). Extend the fixture writer
to produce a segment with **multiple blocks** (append > MaxEventsPerBlock events,
or configure a small `MaxEventsPerBlock`) so block-index addressing is exercised.

### Unit: `segment.ReadBlockFrame`

- Returns bytes byte-identical to the frame slice extracted directly from the
  file (`BlockInfo.Offset+8 .. +CompressedSize`).
- The returned frame decodes (via the standard decoder) to the same events as
  `Reader.DecodeBlock(idx)` for that index.
- `idx >= BlockCount` ⇒ `ErrBlockOutOfRange`; `idx < 0` ⇒ error.
- An `event_count=0` (compaction-emptied) block round-trips and decodes to zero
  events.

### Handler happy paths

- 200 + body byte-identical to the on-disk frame; correct Content-Type.
- Multi-block segment: request every block index 0..N-1, each returns the
  matching frame.
- ETag equals `"<checksum-hex>:<blockIndex>"`; ETags differ across block
  indices within the same segment.
- `Cache-Control` reflects `--segment-cache-max-age` (set and unset).

### Caching / conditional / range

- `If-None-Match` with the served ETag ⇒ 304, empty body.
- `Range: bytes=0-` and a mid-frame range ⇒ 206 with correct
  Content-Range/length.

### Sad paths

- Missing `segment` ⇒ 400; malformed name ⇒ 400.
- Missing / non-integer / negative `blockIndex` ⇒ 400.
- Unknown segment ⇒ 404 with error name exactly `SegmentNotFound`.
- `blockIndex == BlockCount` and `blockIndex` far past end ⇒ 404 with error name
  exactly `BlockNotFound`.
- Not-ready (readiness gate fails) ⇒ 503.
- File deleted after manifest lookup (rotation/deletion race) ⇒ 500.

### Round-trip equivalence

- For a given segment, concatenating the decoded events of every `getBlock`
  frame (0..N-1) equals the events from `getSegment` decoded whole. Proves
  getBlock is a faithful partition of the segment.

### End-to-end oracle verification (the strongest test)

The oracle simulator (`cmd/simulator`, `internal/oracle`) maintains an
independent ground-truth model and already decodes every block of every sealed
segment (`oracle/segments.go` walks `Header.BlockCount`, calling
`Reader.DecodeBlock(i)`). Block composition is deterministic given the world
seed. Add an e2e test (in the `cmd/simulator` e2e suite, gated like the existing
heavy test) that, after the server reaches steady state:

1. Enumerates sealed segments via `listSegments`.
2. For each segment × each block index: fetches the frame via `getBlock` over
   HTTP, decodes it with the standard block decoder.
3. Asserts the decoded event set equals what the oracle computes for that exact
   `(segment, blockIndex)` (i.e. equals `segment.Reader.DecodeBlock` for that
   block — the oracle's own read path), proving served bytes are byte-faithful
   and decode to precisely the archive's contents for that block.
4. Asserts the ETag equals `checksum:blockIndex` and that a second
   `If-None-Match` request 304s.
5. Negative: `blockIndex == BlockCount` ⇒ `BlockNotFound`; unknown segment ⇒
   `SegmentNotFound`.

This catches off-by-one block addressing, frame-slicing errors, and
generation-splice regressions end-to-end against a realistic simulated network.

## 9. Future Work (explicitly out of scope here)

- **Active-segment flushed blocks.** Individual flushed blocks in the active
  segment are immutable once fsynced (appends never rewrite prior blocks; seal
  only appends a footer + backfills the header). They could be exposed later via
  a distinct cache-validator scheme. The active segment lacks a footer, so
  precise per-collection selection there is not possible; recency is already
  covered by the live websocket. The `getBlock` lexicon does not preclude adding
  this.
- **Manifest as authoritative generation owner (fd-pinning / LRU fd cache).**
  To eliminate the per-request `open()` + header pread on a very high-QPS byte
  path, the manifest could hold refcounted open fds per sealed segment, swapped
  atomically (under one write lock) with offsets + checksum on seal/compaction,
  letting `getBlock` do a single frame pread with the ETag taken from the
  manifest checksum. At ~7000 sealed files post-backfill this is operationally
  feasible but implies thousands of persistent fds (raised `RLIMIT_NOFILE`,
  best-effort self-raise at startup with a loud warning if the hard limit is too
  low, and refcounted-close lifecycle). A bounded LRU-of-hot-fds variant
  (cold/miss = open-fresh with checksum verify) caps fds. This work is deferred
  to and designed alongside the query-plan endpoint, where it pays off across
  many block fetches; it is **not** needed for correctness and is not built here.
  Correctness must degrade to performance, never to corruption: a too-low fd
  limit falls back to open-fresh.

## 10. Open Questions

None blocking. Decisions settled during design:

- Scope: sealed segments only (active deferred).
- Shape: single-block GET query (no batch POST); batching is client-side.
- Payload: raw stored zstd frame, no length prefix.
- Manifest: untouched; used only for `SegmentByIdx` path resolution.
- Read path: open-fresh idiom (matches `getSegment`).
- Observability: Prometheus metrics + per-request trace span (operator samples).
