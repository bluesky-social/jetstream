# Subscriber cursor replay — design

> ⚠ SUPERSEDED VALUES (2026-06-30). This note states the v2/v1 cursor
> disambiguation floor as `1.5e15` throughout (the table at "Wire contract", the
> "Risk notes", and the Constraints). The value that actually ships is **`1e15`**
> (`CursorSeqMaxThreshold = 1_000_000_000_000_000`, `internal/subscribe/cursor.go`;
> `docs/README.md` §5.1 is authoritative). Read every `1.5e15` here as `1e15`. The
> argument still holds at `1e15`: `1e15` unix-micros is 2001-09-09, an order of
> magnitude below any current timestamp (~1.75e15) and the 36h lookback floor, and
> a 1-based v2 seq counter does not approach `1e15` for centuries at any realistic
> ingest rate — so the two namespaces remain provably non-overlapping. (The note's
> "predates atproto ~Jan 2017" wording was specific to 1.5e15 and does not apply to
> the shipped 1e15; the bound is the timestamp/seq order-of-magnitude gap, not the
> 2017 anchor.) Also note v2 `/subscribe-v2` now REJECTS a too-old below-floor seq
> cursor with HTTP 400 rather than the "no window cap" replay this note describes;
> v1 still clamps. This note is kept as the reasoning trail only.

## Problem

Today the `/subscribe` handler only delivers live events: a connecting
client misses everything that arrived before its websocket was open.
Jetstream v1 supports a `?cursor=<unix_micros>` query param that
replays the last 24 hours of data; v2 must preserve that contract for
existing v1 clients.

Separately, v2 has its own monotonic 64-bit `seq` cursor (DESIGN.md
§2). v2-aware clients should be able to pass `?cursor=<seq>` to
resume from an exact event boundary, with no time-window cap, against
the full archive.

Both forms must be supported on the same `?cursor=` query parameter
and over the same `/subscribe` endpoint, with no new wire surface that
existing v1 clients can fail to send.

## Constraints

- v1 wire compatibility is non-negotiable. Existing v1 clients send
  `?cursor=<unix_micros>` and expect to resume from that point. They
  must continue working unchanged.
- Cursor disambiguation must be safe for any deployment that's
  realistic in the next several human generations. A single fixed
  wire-protocol constant is acceptable provided no plausible value of
  either cursor type can ever cross it (see "Risk notes" for the
  forever-fixed `1.5e15` floor and its bounding argument).
- The seek path must be cheap. Thousands of concurrent subscribers
  may hold cursors; cursor → start-position resolution must do zero
  zstd decompression and zero disk I/O on the in-memory path.
- The replay → live cutover must be lossless under at-least-once
  semantics. No event with a seq the subscriber would otherwise
  receive may be silently dropped during cutover. Duplicates are
  fine; gaps are not.
- Memory must stay bounded as concurrent cursor connections grow.
  No per-connection ring buffers that scale with replay duration.

## Wire contract

### Inbound: `?cursor=<integer>`

A single integer-valued query parameter. The parser disambiguates
between v2 seq numbers and legacy unix-micros timestamps using the
server's current `maxSeq`:

| value `v` | classification |
|---|---|
| empty / missing | live-only (no replay) |
| `v < 0` | `400 invalid cursor` |
| `0 ≤ v ≤ currentMaxSeq` | v2 seq cursor |
| `currentMaxSeq < v < 1.5e15` | future seq → live-only |
| `v ≥ 1.5e15` | legacy unix-micros timestamp |

The `1.5e15` floor is a documented wire-protocol constant: it predates
atproto (~Jan 2017 in unix micros), and a v2 seq number that large
won't exist for tens of thousands of years at any realistic ingest
rate. Anything in the ambiguous middle (between current max seq and
the timestamp floor) is treated as a future seq and resolves to
live-only, mirroring v1's existing "future cursor → just live tail"
treatment for timestamps. The disambiguator never silently
misclassifies a real cursor: a real seq is `≤ currentMaxSeq` by
definition, and a real timestamp is `≥ 1.5e15`.

`currentMaxSeq` is read from the writer's running counter at parse
time, not pinned to startup, so the seq classification window grows
as the instance ingests. This makes the disambiguation correct for
the lifetime of the deployment.

### Outbound JSON

Every event carries both:

- `time_us` — legacy v1 indexed_at timestamp in unix micros
  (already present today)
- `cursor` — the v2 seq number assigned at ingest time
  (new field, always present)

v1 clients that ignore unknown JSON fields keep using `time_us`. v2-
aware clients use `cursor` to resume. No `extended=true`
interleaving in this design — that's a separate work item per
DESIGN.md §5.2.

### Out-of-window legacy cursor

If a legacy timestamp cursor is older than `now - lookbackWindow`
(default 36h, configurable), we silently start at the oldest event
in the window. This matches v1's pebble-range-scan behavior (its
range scan simply finds nothing older than the TTL boundary and
starts at the oldest available row). No error; no warning log
beyond the structured request log line.

v2 seq cursors can replay from the beginning of the sealed
archive. But the live `/subscribe-v2` lookback floor is enforced:
a v2 seq cursor that resolves below the lookback floor is rejected
with a pre-upgrade HTTP 400 ("cursor too old", carrying the floor
seq), NOT silently clamped (the v1 timestamp path above still
clamps). This is the explicit signal a paginated backfill client
keys on to re-backfill from its last seq rather than silently
dropping the gap — see the 2026-06-28 drop-client-tombstones
design §14. (Replaying the sealed archive itself goes through
paginated `planBackfill` + segment downloads, not the live
websocket, so "from the beginning" is not bounded by the floor.)

## Configuration

New CLI flag and env var on `cmd/jetstream`:

- Flag: `--legacy-cursor-lookback-window`
- Env: `JETSTREAM_LEGACY_CURSOR_LOOKBACK_WINDOW`
- Default: `36h`

Plumbed into the subscribe handler at startup. Applies only to
legacy timestamp cursors.

## Architecture

A new package `internal/replay` owns the read-side history
reconstruction. It's a peer of `internal/ingest` (writer side) and
`internal/subscribe` (wire framing): subscribe owns connection
lifecycle and live framing; ingest owns segment authoring; replay
owns cursor → events history.

**Import direction:** `subscribe` imports `replay`; `replay` does
NOT import `subscribe`. To make this work without circular imports,
the `Filter` predicate and the broadcaster interface used by the
replayer are defined as small interfaces inside `replay`:

```go
// In internal/replay/types.go — what replay needs from a filter and a
// broadcaster, narrow enough that subscribe.Filter and
// subscribe.Broadcaster satisfy them by structural typing.

type EventFilter interface {
    Wants(*segment.Event) bool
}

type SubscriberRegistrar interface {
    SubscribeFromSeq(minSeq uint64) (<-chan *segment.Event, <-chan struct{}, func())
}
```

`subscribe.Filter` already has the right `Wants` method;
`subscribe.Broadcaster` will gain `SubscribeFromSeq`. Both satisfy
the interfaces with no wrapper code.

The writer's `SnapshotPendingAndRegister` is the one place where
ingest needs to call into a broadcaster, which it already does
today via `cfg.OnEvent`. The new entry point takes the same
`SubscriberRegistrar` interface, so ingest doesn't need to import
subscribe either.

```
                  ┌──────────────────────┐
                  │  internal/subscribe  │  <- wire framing, filter,
                  │      (handler.go)    │     conn lifecycle
                  └──────────┬───────────┘
                             │
                  ┌──────────▼───────────┐
                  │   internal/replay    │  <- cursor parsing, manifest,
                  │  (replayer.go,       │     segment streaming, pivot-
                  │   manifest.go,       │     seq cutover
                  │   cursor.go)         │
                  └──────┬──────┬────────┘
                         │      │
       ┌─────────────────▼┐    ┌▼──────────────────┐
       │ segment (reader, │    │ ingest (writer    │
       │  header, footer) │    │  Snapshot+Register)│
       └──────────────────┘    └────────────────────┘
```

### Manifest (`internal/replay/manifest.go`)

In-memory index of every sealed segment. Always resident; never
spills to disk.

```go
type SegmentSummary struct {
    Path       string
    Index      uint32              // base-36 counter from filename
    Header     segment.Header      // parsed 256-byte fixed header
    BlockIndex []segment.BlockInfo // parsed per-block index
}

type Manifest struct {
    mu        sync.RWMutex
    byIndex   []*SegmentSummary  // sorted by Index, gap-free
    byMinTime []*SegmentSummary  // sorted by Header.MinIndexedAt
    minSeq    uint64
    maxSeq    uint64
}
```

Two sorted views: `byIndex` for v2 seq cursors (segments are
seq-monotonic by construction); `byMinTime` for legacy timestamp
cursors (filename order ≈ time order under nominal conditions, but
sorting once eliminates clock-jitter edge cases).

#### Lifecycle

- **Startup:** scan `data/segments/` once. Parallel across N
  goroutines (default 16). For each `.jss` file: `Open()` the file,
  `pread(0, 256)` the fixed header, `pread(headerOff, 36×blockCount)`
  the block index, decode both, close. Skip checksum — manifest
  isn't consuming the data. Skip blooms and collection index — not
  needed for cursor resolution.

  Napkin math for a 20,000-segment archive on NVMe:
  per-file ~120 µs cold / ~30 µs warm; parallelized at 16
  goroutines, ~150–300 ms cold and ~50–100 ms warm. Total bytes
  read ~50 MB; resident memory ~50 MB. Comfortable.

- **Steady state:** the ingest writer notifies the manifest via a
  callback when it seals a segment. Manifest opens the new file,
  parses header + block index, releases the handle, builds a fresh
  sorted view, and atomically swaps it in. RW lock for the swap;
  `Lookup` calls take RLock and copy out a slice header — no
  hot-path contention.

- **Compaction:** out of scope. When lookaside compaction (DESIGN.md
  §3.3.1) lands, it'll need to invalidate the affected summary;
  cursor replay isn't running during compaction's brief window
  anyway.

#### Lookup API

```go
// Snapshot view returned to readers. Immutable; safe to retain across
// mutex drops.
type ManifestSnapshot struct {
    SegmentsByIndex   []*SegmentSummary
    SegmentsByMinTime []*SegmentSummary
    MinSeq, MaxSeq    uint64
}

func (m *Manifest) Snapshot() ManifestSnapshot

// Resolve cursor (seq) to (segment index, block index, event offset).
// Pure in-memory binary search using BlockInfo.MinSeq/MaxSeq.
// Zero I/O. ok=false means seq is past the manifest's MaxSeq (live).
func (s ManifestSnapshot) ResolveSeq(seq uint64) (segIdx, blockIdx, evtOffset int, ok bool)

// Resolve cursor (timestamp µs) to (segment index, block index).
// Binary searches the byMinTime view for the segment whose
// [MinIndexedAt, MaxIndexedAt] contains tsMicros (or the next segment
// after, if tsMicros falls in a gap). Always returns blockIdx=0; the
// replayer linearly decompresses blocks within that segment to find
// the timestamp boundary. Block-level timestamp resolution is the
// future format-bump optimization.
//
// lookbackFloor clamps the cursor up to max(tsMicros, lookbackFloor)
// before resolution.
//
// ok=false means the resolved timestamp is past the manifest's
// MaxIndexedAt — caller should treat as live-only.
func (s ManifestSnapshot) ResolveTimestamp(tsMicros, lookbackFloor int64) (segIdx, blockIdx int, ok bool)
```

`ResolveSeq` is `O(log N)` over segments (segments don't overlap in
seq) followed by `O(log K)` over blocks. Both are pure-CPU
binary searches over in-memory slices.

`ResolveTimestamp` is `O(log N)` over segments, then returns block 0:
the replayer linearly decompresses blocks within the candidate
segment to find the timestamp boundary. Worst case ~64 blocks ×
~10 ms decompress = ~640 ms; in practice the boundary lands in 1–2
blocks. Acceptable for the initial implementation.

**Future format bump (pre-production, out of scope here):** add
`MinIndexedAt`/`MaxIndexedAt` (16 bytes) to `BlockInfo`. Footer
grows from 36 → 52 bytes per block — ~1 MB extra per 64k-block
segment, trivial. Then `ResolveTimestamp` resolves to a specific
block in `O(log K)` with zero zstd decompression. Tracked as a
future work item; not blocking shipping the cursor feature.

### Replayer (`internal/replay/replayer.go`)

```go
// EmitFunc receives one historical event during replay. The handler
// passes a callback that applies its filter, encodes, and writes one
// websocket frame. Returning an error aborts replay (typically because
// the websocket write failed).
type EmitFunc func(*segment.Event) error

// Run streams historical events for the connection's cursor and returns
// the live-tail subscriber handles for the caller to drive after
// replay. emit is the same per-frame callback the live tail uses.
//
// On a CursorLive request, no historical events are streamed and the
// caller is registered immediately at currentMaxSeq+1.
//
// On any cursor that resolves to historical events, the returned
// (subCh, doneCh, unsubscribe) are valid only after Run returns nil:
// the Replayer registers them atomically with its pending-block
// snapshot under the writer's lock during step 4 (see "Steps" below).
func (r *Replayer) Run(
    ctx context.Context,
    cursor CursorRequest,
    filter EventFilter,
    emit EmitFunc,
    registrar SubscriberRegistrar,
) (subCh <-chan *segment.Event, doneCh <-chan struct{}, unsubscribe func(), err error)
```

#### Steps

1. **Resolve cursor → start position** via the manifest snapshot.
   For a `liveOnly` cursor (empty, future seq, etc.), short-circuit
   to step 4.

2. **Stream sealed segments** from `startSegIdx` forward. For each
   segment: open via an LRU of file handles (default cap 64,
   configurable), iterate blocks from `startBlockIdx` (only on
   first segment; subsequent segments start at block 0),
   `DecodeBlock()` each, walk events, apply `Filter.Wants()`, call
   `emit()` for matches. Skip events with `seq < cursorSeq` when
   starting mid-block. Track `lastSentSeq`.

3. **Stream the active segment's flushed-to-disk blocks.** The
   currently-active (unsealed) segment file is on disk too. The
   writer flushes blocks to it but doesn't update a footer.
   The writer exposes `ActiveSegmentSnapshot()` that returns: path,
   current block count, per-block summary derived from the writer's
   in-memory accounting. Replay reads via `pread` with no checksum
   verification (file is mid-flight; the writer is the source of
   truth). Skip blocks already covered by sealed-segment streaming.
   Stream events as in step 2.

4. **Atomic snapshot-and-register cutover.** Call
   `writer.SnapshotPendingAndRegister(registrar)`
   which, **under the writer's pending-block mutex**:
   - Copies the writer's in-memory pending events (≤4096 entries).
   - Reads `nextSeq` → this is the `pivotSeq`.
   - Calls `registrar.SubscribeFromSeq(pivotSeq)`, returning the
     subscriber's `subCh`/`doneCh`/`unsubscribe`.
   - Returns the snapshot, pivotSeq, and subscriber handles.

   The writer's mutex is held for one slice copy + one int read +
   one map insert. Microseconds. The atomicity of snapshot + register
   is what guarantees no event slips through: events with
   `seq < pivotSeq` are in the snapshot; events with
   `seq ≥ pivotSeq` go through the broadcaster.

5. **Drain the pending-block snapshot.** Stream events from the
   snapshot with `seq > lastSentSeq`. Apply filter, emit. The
   snapshot is freed at the end of this step.

6. **Return** `subCh`, `doneCh`, `unsubscribe` to the handler. The
   handler enters its existing live-tail write loop. Every event
   delivered by the broadcaster to this subscriber has
   `seq ≥ pivotSeq`, so no dedup required downstream.

#### Backpressure

If the client can't keep up during replay, `emit` returns an error
from `conn.Write` (5-second timeout per frame, same as live path).
Replayer aborts, caller unsubscribes if registered, closes the
connection. Same policy as the existing live-only path.

#### Memory

Per-connection memory during replay:
- One open `segment.Reader` at a time (the LRU is shared across
  connections).
- One pending-block snapshot (≤4096 events × event size, transient).
- Standard broadcaster per-subscriber channel buffer (existing).

Total: bounded, sub-MB per connection during replay. Drops to the
existing live-only memory profile after step 5.

### Cursor parsing (`internal/replay/cursor.go`)

Pure parser. No I/O. Takes `currentMaxSeq` as input so disambiguation
is testable without spinning up a manifest.

```go
type CursorRequest struct {
    Kind      CursorKind  // CursorLive | CursorSeq | CursorTimestamp
    Seq       uint64      // valid when Kind == CursorSeq
    TimestampMicros int64 // valid when Kind == CursorTimestamp
}

func ParseCursor(raw string, currentMaxSeq uint64) (CursorRequest, error)
```

Error path returns wrapped `ErrInvalidCursor` for `400` responses.

## Wiring changes

### `internal/subscribe/broadcaster.go`

Add `SubscribeFromSeq(minSeq uint64) (subCh, doneCh, unsubscribe)`:
the existing `Subscribe()` becomes `SubscribeFromSeq(0)` (every
event matches). Per-subscriber filter checks
`if evt.Seq < minSeq { continue }` before the channel send.
Existing slow-drop behavior unchanged.

### `internal/ingest/.../writer.go`

Add `ActiveSegmentSnapshot() (path string, blockCount int, blockSummaries []ActiveBlockInfo)`
— derives a Reader-equivalent summary from the writer's in-memory
counters without touching the active segment file.

Add `SnapshotPendingAndRegister(r SubscriberRegistrar)` — under the
existing pending-block mutex, copies pending events and atomically
calls `r.SubscribeFromSeq(nextSeq)`. Returns
`(snapshot []*segment.Event, pivotSeq uint64, subCh <-chan *segment.Event, doneCh <-chan struct{}, unsubscribe func(), err error)`.

The `SubscriberRegistrar` interface here is defined locally inside
the ingest package (single method, structurally identical to the one
in `internal/replay`). Go's structural interface satisfaction means
the same `*subscribe.Broadcaster` value satisfies both interfaces
with no wrapper. This keeps ingest from importing either replay or
subscribe — no new edges in the dependency graph beyond the existing
"ingest publishes to broadcaster" coupling that already lives behind
`cfg.OnEvent`.

### `internal/subscribe/handler.go`

Two changes:
1. After `ParseQuery`, call `ParseCursor(values.Get("cursor"), manifest.MaxSeq())`.
2. Replace `broadcaster.Subscribe()` with `replay.Run(...)` which
   handles both cases internally and returns the subscriber handles.

The existing options_update reader, requireHello flow, ping ticker,
and write loop are unchanged.

### `cmd/jetstream/main.go`

- New `--legacy-cursor-lookback-window` flag (default `36h`).
- Construct the manifest at startup, before the broadcaster, before
  starting `live.Consumer`. Plumb manifest into the writer (for the
  seal callback) and into the replay package.

## Testing

### Unit

- `internal/replay/cursor_test.go`: pin `ParseCursor` against every
  edge of the disambiguation table — empty, negative, zero, seq at
  exactly currentMaxSeq, seq one above currentMaxSeq, future seq
  in the gap, exactly `1.5e15`, real-world unix-micros from 2024+,
  malformed integers, integer overflow.

- `internal/replay/manifest_test.go`: build a synthetic data dir
  with N sealed segments, verify Snapshot returns sorted views,
  verify ResolveSeq lands in the correct (segIdx, blockIdx),
  verify ResolveTimestamp clamps to lookbackFloor, verify the
  seal-callback path appends a new summary.

- `internal/replay/replayer_test.go`: feed a synthetic manifest and
  in-memory writer, exercise:
  - liveOnly short-circuit
  - seq cursor inside one sealed segment
  - seq cursor spanning multiple sealed segments
  - seq cursor that lands in the active segment's flushed blocks
  - seq cursor that lands in pending block (cutover snapshot)
  - timestamp cursor older than the lookback window (clamps)
  - timestamp cursor inside the window
  - filter applied during replay

- `internal/ingest/.../writer_test.go`: extend with a
  `SnapshotPendingAndRegister` test that exercises the atomicity:
  spawn N goroutines hammering Publish, one calling
  SnapshotPendingAndRegister, assert no event has
  `seq < pivotSeq` arriving on the subscriber channel and no event
  with `seq ≥ pivotSeq` is missing.

### Integration (cmd/jetstream)

`cmd/jetstream/serve_test.go`: extend the existing serve harness
with a cursor-resume scenario — start, ingest a few synthetic events
into segments, restart, connect with a cursor, verify the historical
events arrive on the websocket before the live cutover.

### Manual smoke test

```sh
# In one shell:
just clean
just run serve

# Wait for steady-state. Connect without cursor:
websocat ws://localhost:8080/subscribe

# Note a few `time_us` and `cursor` values from the JSON.

# Reconnect with the v1-style cursor (timestamp from the past minute):
websocat "ws://localhost:8080/subscribe?cursor=<recent_time_us>"

# Reconnect with the v2-style cursor (recent seq):
websocat "ws://localhost:8080/subscribe?cursor=<recent_seq>"

# Reconnect with an out-of-window timestamp (e.g. 1 day ago when
# window is 1h):
websocat "ws://localhost:8080/subscribe?cursor=<24h_ago_micros>"
# Expect: stream starts at oldest event in window, no error.
```

## Out of scope

- Replay rate limiting (DESIGN.md §7) — handled at broadcaster /
  per-IP layer separately.
- Extended-mode `?extended=true` payload (DESIGN.md §5.2) — separate
  work item; the cursor mechanism is format-agnostic.
- Per-block timestamp index in segment footer — future format bump,
  pre-production. Tracked as a future-work hook in the manifest
  package's doc.go.
- HTTP segment download API for client-driven backfill (DESIGN.md
  §5) — separate work item; the manifest is reusable for that
  feature too.
- Lookaside compaction interaction (DESIGN.md §3.3.1) — manifest
  refresh on compaction is wired when compaction itself lands.
- Replication protocol cursor handling (DESIGN.md §6) — separate
  work item.

## Risk notes

- The `1.5e15` disambiguation floor is a forever-fixed wire
  constant. If we ever truly outgrow uint64 seq (won't happen at
  realistic rates within thousands of years), or if some future
  protocol bug emits a `time_us` < 1.5e15 (won't happen — atproto
  postdates that floor by 6+ years), the disambiguation breaks.
  Mitigation: documented in the parser and asserted in tests; any
  future change to seq generation that approaches 1e15 should
  trigger a wire-protocol revision discussion.

- The atomic SnapshotPendingAndRegister couples the writer and
  broadcaster behind one lock. If either mutex's hold time grows
  (e.g. broadcaster.Subscribe starts doing real work), the cursor
  connect path slows. Mitigation: both ops are pointer/map writes
  by construction; tests pin the critical-section bounds with
  benchmarks.

- Per-segment file-handle LRU has a default cap of 64. Under
  hostile load (every subscriber asking for a different cold
  segment) this thrashes open()/close(). Mitigation: cap is
  configurable; metrics expose hit/miss ratios so operators can
  tune.

- Active-segment streaming reads a mid-flight file. The reader
  uses the writer's in-memory block summary (not a footer scan), so
  a torn write at the tail can't cause a misread — but a crash
  between writer fsync and pebble batch commit could leave a block
  the writer's accounting forgot about. Mitigation: replay only
  reads blocks the writer's accounting reports as durable; on
  startup the writer's recovery path (DESIGN.md §3.1.1) trims any
  partial tail before serving cursor connections.
