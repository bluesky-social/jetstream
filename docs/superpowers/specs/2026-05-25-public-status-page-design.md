# Public status page

## Problem

Operators and external observers have no view into a running jetstream's
state today. We have an `inspect-segment` CLI for forensic per-file
analysis, but nothing that shows aggregate state: what phase is the
process in, how far along is backfill, how big is the on-disk archive,
where is the upstream cursor, and so on. An attempt to add an offline
`inspect-all` CLI ran into pebble's exclusive-lock requirement: a CLI
can't open the metadata DB while jetstream holds it. Stopping the
process to inspect it is a non-starter for production.

The mission of atproto is open infrastructure on the public web. Other
public archives (Mastodon, Wikipedia, archive.org) publish their state
as a matter of course. A public status page on the same listener that
serves the protocol surface fits that posture and gives operators a
single URL to reference when answering "is jetstream healthy."

## Goals

- A public-internet-safe HTML page on the existing `:8080` listener
  showing aggregate process state.
- One page, parameterless route at `GET /status`.
- Cheap per-request: cache-driven, no per-request work that scales with
  data size.
- No coupling to runtime subsystems beyond the metadata store and the
  data directory layout.
- Cleanly separated data-gathering and rendering, so future surfaces
  (JSON, Prometheus, CLI) reuse the snapshot logic.

## Non-goals

- JSON or Prometheus output (deferred — straightforward to add later).
- Per-DID, per-segment, or per-collection drilldown.
- Per-collection record counts (would require decompressing every
  block in every segment file).
- JavaScript, NPM, build-time toolchains. Vanilla HTML and inline CSS
  only.
- Auto-refreshing UI.
- Operator-only debug surfaces. Those continue to live on `:6060`.
- A favicon (one less round-trip surface).

## Architecture

Two new packages plus one small orchestrator change.

### `internal/status` — data gathering

Owns reading the metadata store and statting the segment trees to
produce a typed snapshot.

```
internal/status/
  doc.go
  collector.go        // Collector, Options, cache + singleflight
  snapshot.go         // Snapshot type + sub-types
  collect.go          // Snapshot-building functions
  collector_test.go
  collect_test.go
```

Public API:

```go
type Options struct {
    Store    *store.Store
    DataDir  string
    TTL      time.Duration  // default 30s
    NegTTL   time.Duration  // default 1s
    Now      func() time.Time // overridable for tests
}

type Collector struct { /* ... */ }

func New(opts Options) (*Collector, error)
func (c *Collector) Snapshot(ctx context.Context) (*Snapshot, error)
```

`Collector` is safe for concurrent use. Internal state:
- `mu sync.Mutex` guarding `cached *cacheEntry`
- `sf singleflight.Group` (golang.org/x/sync/singleflight)
- A `cacheEntry` holds `{snap *Snapshot, err error, expiresAt time.Time}`
- `Snapshot` is treated as immutable after construction; concurrent
  readers see the same pointer

Cache key for singleflight: literal `"status"` (only one snapshot ever
in flight). `Snapshot()` returns the cached value if not expired,
otherwise calls `sf.Do("status", ...)` to build a new one. The build
function honors ctx cancellation between phases of the gather but does
not abort mid-pebble-iter (the iter completes; ctx is checked between
sections).

Negative caching: if a build returns an error, the error is cached for
`NegTTL` so a transient pebble hiccup isn't hammered. After the
negative window, the next request retries.

### `internal/web` — rendering

Owns templates, CSS, and the HTTP handler.

```
internal/web/
  doc.go
  handler.go            // Handler, ServeHTTP
  render.go             // template execution helpers
  format.go             // bytes->human, time->relative, percent
  templates/status.html  // includes inline <style>; both embedded
  handler_test.go
  format_test.go
```

Public API:

```go
type Snapshotter interface {
    Snapshot(ctx context.Context) (*status.Snapshot, error)
}

type Handler struct { /* ... */ }

func New(s Snapshotter) (*Handler, error)
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request)
```

`web.New` parses templates at construction time so a malformed template
fails at process startup, not on first request. Templates are embedded
via `//go:embed` so the binary is self-contained.

`web` imports `status` only for the `Snapshot` type. The dependency
points one way; `status` does not import `web`.

### `internal/server` — wiring

`server.Config` gains one field:

```go
StatusHandler http.Handler  // optional; if nil, /status is not registered
```

`publicMux()` mounts `GET /status` if the handler is non-nil. Server
imports neither `status` nor `web` directly; the handler is injected as
an interface value. Existing tests don't change unless they want to
exercise the new route.

### `cmd/jetstream` — wiring

After `metaStore` opens (so we share the live pebble handle):

```go
collector, err := status.New(status.Options{
    Store:   metaStore,
    DataDir: dataDir,
    TTL:     30 * time.Second,
})
if err != nil { return err }

statusHandler, err := web.New(collector)
if err != nil { return err }

srv := server.New(server.Config{
    ...,
    StatusHandler: statusHandler,
}, processLogger, metrics)
```

The collector lifetime tracks the process lifetime. No Close() needed —
no goroutines, no background work; the cache is purely demand-driven.

## Data model

```go
package status

type Snapshot struct {
    GeneratedAt time.Time
    Process     ProcessInfo
    Phase       PhaseInfo
    Backfill    BackfillStats
    Live        LiveStats
    Segments    SegmentTreeStats   // for "<dataDir>/segments/"
    LiveSegs    SegmentTreeStats   // for "<dataDir>/backfill/live_segments/"
    Pebble      PebbleStats
}

type ProcessInfo struct {
    Version   string
    Commit    string
    BuiltAt   string
    StartedAt time.Time
    Uptime    time.Duration
    GoVersion string
}

type PhaseInfo struct {
    Phase          lifecycle.Phase   // "" on a fresh data dir; rendered as "starting"
    PhaseEnteredAt time.Time         // zero if no phase/entered_at key
}

type BackfillStats struct {
    TotalDIDs       uint64
    Discovered      uint64    // status == not_started
    Complete        uint64    // status == complete
    Failed          uint64    // status == failed
    PercentComplete float64
    ListReposCursor string    // opaque relay cursor; useful only as "we're enumerating"
}

type LiveStats struct {
    UpstreamCursor int64    // relay/cursor
    NextSeq        uint64   // seq/next
    BootstrapSeq   uint64   // live_segments/seq/next; zero in steady state
    EventsAppended uint64   // alias for NextSeq, displayed as event count
}

type SegmentTreeStats struct {
    Dir                  string
    SealedCount          int
    ActiveCount          int       // 0 or 1
    CompressedBytes      int64     // sum of file sizes on disk
    UncompressedBytes    int64     // sum of block headers' UncompressedSize
    OldestMTime          time.Time
    NewestMTime          time.Time
    LatestSegment        *SegmentSummary  // nil if directory is empty or read failed
}

type SegmentSummary struct {
    Index            uint64
    Sealed           bool
    EventCount       uint64
    UniqueDIDCount   uint32
    BlockCount       uint32
    CollectionCount  int
    MinSeq, MaxSeq   uint64
    MinIndexedAt     time.Time
    MaxIndexedAt     time.Time
    SizeBytes        int64
}

type PebbleStats struct {
    DiskBytes      int64                // sum of meta.pebble/ tree
    KeyspaceCounts map[string]uint64    // by prefix; see below
}
```

`PebbleStats.KeyspaceCounts` covers these prefixes:

- `repo/`         (per-DID backfill rows)
- `sync/chain/`   (atmos chain state)
- `sync/host/`    (atmos hosting state)
- `relay/`        (cursors and singletons)

`sync/identity/` is excluded from the public surface.

## Snapshot building

The collector executes the following sequence on a cache miss. Each
step is independent; a failure in one section returns an error from
the whole build (we don't render partial snapshots — fewer rendering
edge cases).

1. **Process info.** `version.Get()`, `runtime.Version()`, the
   collector's `StartedAt` (captured in `New`), `Uptime = Now() -
   StartedAt`. No I/O.

2. **Phase.** `lifecycle.ReadPhase(store)` and a new
   `lifecycle.ReadPhaseEnteredAt(store)`. Both tolerate ErrNotFound and
   return zero values.

3. **Live cursors.** `live.LoadUpstreamCursor(store, live.CursorKey)`,
   plus direct reads of `seq/next` and `live_segments/seq/next` (8-byte
   little-endian uint64 values; reuse the existing `loadNextSeq` decode
   logic from `internal/ingest/writer.go` — extracted into a shared
   helper).

4. **Backfill counts.** Range scan over `repo/`. The decoder is the
   existing `backfill.decodeRepoStatus`; we count by `Backfill.Status`.
   We expose a `backfill.CountStatuses(store) (Counts, error)` helper
   so this package doesn't reach into `repoKeyPrefix` or the encoding
   directly. The list-repos cursor comes from
   `backfill.LoadListReposCursor(store)`.

5. **Segment tree stats.** For each of the two trees:
   - `os.ReadDir` + filter by the existing `parseSegmentIndex` helper
     (extracted to a shared `internal/ingest/filename.go` export, or
     re-implemented in the collector — see "Code reuse" below).
   - For each file: `os.Stat` for size + mtime.
   - Active vs. sealed: open the highest-index file, read 12 bytes of
     header, check the checksum field — zero means active. (Reuses the
     same active/sealed distinction `segment.Inspect` makes.)
   - Uncompressed bytes and `LatestSegment`: open the highest-index
     file once and call `segment.Inspect` to get block-header sums and
     the segment summary. (Inspect on a sealed file is cheap — index
     parse only, no decompression. On an active file it does walk
     framed blocks; we accept that cost for the latest one only.) For
     uncompressed bytes across the whole tree, we read each file's
     block index without instantiating a full Inspection — we'll add a
     lighter `segment.QuickStats(path) (QuickStats, error)` that
     returns just `{Compressed, Uncompressed, Sealed, FileSize}`.

6. **Pebble stats.** `meta.pebble/` tree size via
   `filepath.WalkDir`+`Stat`. Keyspace counts via separate range scans
   per prefix using `pebble.NewIter` with key-only iteration (no value
   reads).

`status.Collect` is structured as a sequence of small pure functions
(`collectProcess`, `collectPhase`, `collectLive`, `collectBackfill`,
`collectSegmentTree`, `collectPebble`) that each return a sub-struct
or an error. The top-level `build` function composes them, returning
the first error.

## Rendering

One template, `templates/status.html`. Inline CSS via a `<style>` block
(small enough to not warrant a separate file; an external CSS file
would mean a second cacheable response and we want to keep the surface
minimal). `html/template` auto-escapes all interpolated values.

Page sections, in order:

1. **Header** — service name, version + commit, "generated N seconds
   ago", uptime.
2. **Phase** — current phase, "in this phase for X" (or "since process
   start" if `PhaseEnteredAt` is zero).
3. **Backfill** — progress bar, counts, list-repos cursor (truncated
   for display).
4. **Live ingest** — upstream cursor, next seq, bootstrap seq if
   non-zero.
5. **Segments** — two columns side-by-side (`segments/` and
   `backfill/live_segments/`). Each column shows counts, compressed,
   uncompressed, oldest/newest mtime, and a "Latest segment" sub-card
   with the segment summary if present.
6. **Metadata store** — pebble disk size and keyspace counts.

CSS uses CSS Grid with a single media query for narrow screens (the two
segment columns stack on mobile). No fonts loaded from the network;
system font stack only.

### Response shape

- `Content-Type: text/html; charset=utf-8`
- `Cache-Control: public, max-age=30` (matches default TTL; the value
  is read from the configured TTL, not hard-coded)
- `X-Status-Generated-At: <RFC3339Nano>` reflecting the actual snapshot
  time (may be older than the request if cached)
- `200 OK` on success
- `503 Service Unavailable` on collector error, with a minimal but
  still HTML body so curl users see a readable message

Methods: `GET` and `HEAD` only. Other methods get 405. `HEAD` is
auto-handled by the stdlib once `GET` is registered.

## Orchestrator change

`lifecycle.WritePhase` is extended to also persist a transition
timestamp atomically with the phase value. This is required for
`Phase.PhaseEnteredAt` to be meaningful; without it, the rendered "in
phase X for 3d 1h" line would just be "since process start," which is
misleading after a restart.

```go
// lifecycle/phase.go

const (
    phaseKey          = "phase"
    phaseEnteredAtKey = "phase/entered_at"
)

func WritePhase(s *store.Store, p Phase, enteredAt time.Time) error {
    // batch: set phase + phase/entered_at (RFC3339Nano), commit Sync
}

func ReadPhaseEnteredAt(s *store.Store) (time.Time, error) {
    // ErrNotFound -> zero time, nil error
}
```

The single-key atomic write is replaced by a two-key pebble batch
committed with `store.SyncWrites`. Both keys move together; a crash
between them is impossible by construction.

Three call sites in the orchestrator each gain a `time.Now().UTC()`
argument:

1. `orchestrator.go` `Run`: the fresh-data-dir initial-write of
   `PhaseBootstrap` (`if phase == ""` branch).
2. `states.go` `writeMergingPhase`: the bootstrap → merging transition
   (commit point #1).
3. `states.go` `writeSteadyStatePhase`: the merging → steady_state
   transition (commit point #2).

On a recovery-path restart in PhaseMerging or PhaseSteadyState (no
transition occurred), we do NOT rewrite the timestamp — the existing
`phase/entered_at` stands. The orchestrator already distinguishes
"we just transitioned" from "we resumed in this phase" via the call
sites; only the transition call sites pass the timestamp.

## Caching, concurrency, costs

**Cache hit cost:** one mutex acquire + one struct read + one time
comparison. Sub-microsecond.

**Cache miss cost (estimated):**
- Process info: zero I/O.
- Phase + cursors + seq counters: 4 pebble Gets, ~µs each.
- Backfill counts: range scan over `repo/` (30M keys at full network
  scale). Pebble's key-only iteration is fast but not free; if this is
  too slow we switch to a maintained counter. See "Risks."
- Segment tree stats: `os.ReadDir` + N `os.Stat` (a few thousand
  files at most) + N `segment.QuickStats` (one open + one ReadAt of
  the block index per file). Should fit in tens of ms even with
  thousands of files.
- Latest segment summary: one `segment.Inspect` call on the
  highest-index file.
- Pebble keyspace counts: one range scan per prefix using key-only
  iteration.
- Pebble disk size: one `filepath.WalkDir` over `meta.pebble/`.

Total budget: target <200ms on a fully-populated data dir at full
network scale. Negative cache (1s) absorbs cold-cache failures
gracefully; the 30s TTL means the worst case for a popular endpoint is
one expensive collection per 30s window plus singleflight-collapsed
concurrent waiters.

**Concurrency:**
- Multiple concurrent `/status` requests on a cold cache: all but one
  block in `singleflight.Do`. The one that runs builds the snapshot,
  populates the cache, and returns the value to all waiters.
- Concurrent reads from a warm cache: each request takes the mutex,
  reads the pointer, releases. No blocking.
- Phase transition mid-collection: a snapshot reflects pebble state at
  the moment each section runs, not a single atomic snapshot. Brief
  cross-section inconsistency is acceptable; it heals on next refresh.

## Abuse hardening

- The route is parameterless: no request input affects backend cost.
- The cache (default 30s) is the primary load-shedding mechanism.
  Sustained 1k RPS becomes one collection per 30s plus warm-cache
  reads.
- `singleflight` collapses cold-cache stampedes to a single backend
  call.
- `ReadHeaderTimeout: 10s` (already configured on the server) bounds
  slowloris.
- Response body is small (~few KB). No slow-read amplification.
- No links to operator-only endpoints. `:6060` URLs are not surfaced.
- Output is HTML through `html/template`; all interpolation is escaped
  by default. We add a golden test that injects `<script>` into a
  string field and verifies it's escaped.

## Code reuse

These existing helpers move or get exported:

- `internal/ingest.parseSegmentIndex` / `segmentFilename` — already
  package-private. Either we export them or `status` re-implements the
  trivial parse. Lean toward exporting (one shared truth for the
  filename grammar); add `ingest.ParseSegmentIndex` and
  `ingest.SegmentFilename`.

- `internal/ingest.loadNextSeq` — currently package-private. Extract
  the 8-byte LE decoding into a shared `store.GetUint64LE(key)` helper
  on `*store.Store`, since the encoding is a property of "how we
  store uint64s in pebble," not of ingest.

- `segment` — gain a `QuickStats(path)` that returns
  `{Compressed, Uncompressed, Sealed, FileSize}` without instantiating
  a full Reader. Used by both per-segment-tree stats and any future
  consumer that wants cheap aggregate sizes.

- `internal/ingest/backfill` — gain `CountStatuses(store)` that
  range-scans `repo/` and returns the four counts. Exposes the count
  without exposing the storage encoding to `status`.

- `internal/lifecycle` — gain `ReadPhaseEnteredAt`. `WritePhase`
  signature changes (see Orchestrator change).

## Error handling

- `Collector.New` returns an error if `Options.Store` is nil or
  `Options.DataDir` is empty.
- `web.New` returns an error if template parsing fails.
- `Collector.Snapshot` returns an error if any sub-collector fails;
  the error is cached for `NegTTL`.
- `Handler.ServeHTTP` returns 503 on collector error with an HTML body.
  The error message in the body is generic ("status temporarily
  unavailable"); details go to logs only.
- Filesystem-not-found cases (segments dir doesn't exist on a fresh
  install) are NOT errors — they yield zero-count tree stats.
- Latest-segment header read failures (file race during rotation,
  partial write) leave `LatestSegment` nil and continue. The template
  omits that subsection.
- Corrupt phase value in pebble: bubbles up as an error from
  `lifecycle.ReadPhase` (existing behavior). `Collector.Snapshot`
  returns it; handler returns 503. We prefer crashing the request over
  showing wrong data, consistent with PRACTICES.md.

## Testing

### `internal/status`

- Empty store: `Collector.Snapshot` succeeds, returns zeroed counts.
- Mid-backfill fixture: write a handful of `repo/<did>` rows in
  various states + a `relay/cursor` + a `phase` value; assert
  `Backfill` counts and `Live.UpstreamCursor` match.
- Phase entered-at: write `phase/entered_at`, verify it's exposed;
  missing key yields zero time.
- Cache TTL: with `Now` overridden, advance time and verify a second
  call inside the TTL returns the same `*Snapshot` pointer; advance
  past the TTL and verify a fresh build runs.
- Negative cache: inject an error from a fake collector, verify
  consecutive calls within `NegTTL` see the error without re-invoking
  the gather.
- Concurrency: 100 concurrent `Snapshot` calls on a cold cache
  produce exactly one collection. (Use a counter-wrapped fake gather.)
- Segment tree stats: synthetic `segments/` dir with 3 sealed + 1
  active file; verify counts, sums, and `LatestSegment`.

### `internal/web`

- Golden file: render against a fixed `Snapshot` fixture, compare
  against a checked-in golden HTML file.
- XSS smoke: a `Snapshot` with `<script>alert(1)</script>` injected
  into the list-repos cursor field renders as escaped text.
- Missing optional fields: `LatestSegment == nil` renders without
  errors and without the latest-segment subsection.
- Method handling: POST returns 405.
- Response headers: `Cache-Control`, `X-Status-Generated-At`,
  `Content-Type` all present and correct.

### `internal/server`

- Mounting: with `StatusHandler` set, `/status` returns 200; with it
  nil, returns 404.
- End-to-end: `serve_test.go`-style harness verifies a real running
  server answers `/status` with the expected content-type and the
  correct cache header.

### `internal/lifecycle`

- `WritePhase` with the new signature writes both keys atomically; a
  reader sees both or (in a never-written case) neither.
- `ReadPhaseEnteredAt` returns zero time on missing key.
- Round-trip: write, read, compare with truncation to the encoded
  precision.

### `cmd/jetstream`

- Existing `serve_test.go` extended: hit `/status` against the live
  test server, verify it returns 200 and the body contains expected
  marker strings (`"Phase"`, `"Backfill"`).

## Risks

**Backfill range scan at scale.** Counting 30M `repo/<did>` rows on
every cache miss may be too slow. Mitigations:
1. Measure first against a populated fixture.
2. If too slow, switch to a maintained counter — backfill metrics
   already track per-status increments; we'd add durable counters in
   pebble singletons (`repo/count/total`, `repo/count/complete`, etc.)
   updated in the same batch as the per-DID writes.

**Filesystem-snapshot inconsistency.** During a segment rotation,
`os.ReadDir` could see a new active file that hasn't been opened yet.
The collector tolerates this (file might be 0 bytes, header read may
return ErrCorrupt). We log and continue with `LatestSegment = nil`.

**Pebble keyspace counts on cold L0.** The first range scan after a
process restart hits cold pebble blocks. The 30s TTL absorbs this; the
first request after restart pays the cost, the next 29s of requests
use the cache.

## Out of scope (next steps)

- `/status.json` for machine consumers, fed by the same Collector.
- Auto-refresh via vanilla JS (`fetch('/status.json')` every N
  seconds).
- Operator-only `/debug/inspect-all` on `:6060` for full per-segment
  drilldowns.
- Maintained backfill counters in pebble (only if the range-scan
  measurement justifies it).
