# Live Firehose Consumer During Backfill

**Date:** 2026-05-21
**Scope:** Stand up a generic firehose-to-segments consumer that runs concurrently with the backfill engine during the bootstrap phase. Writes its segment files to `data/backfill/live_segments/`. Persists the upstream relay cursor in pebble so a restarted process resumes without losing events. Adds a `phase` lifecycle key so a started server can decide whether to launch the live consumer at all.

This is the smallest slice of DESIGN.md §4.1 step 1 ("the live firehose consumer") that lands real bytes on disk, and is structured so the same `Consumer` type can be redeployed against `data/segments/` once the merge phase (DESIGN.md §4.2) is implemented.

## 1. Goals

1. While the backfill engine runs, also subscribe to the upstream relay's `com.atproto.sync.subscribeRepos` and write every event into segment files at `<data-dir>/backfill/live_segments/seg_<base36>.jss`.
2. Persist the upstream firehose cursor durably in pebble at the per-block boundary, with the DESIGN.md §3.1.1 invariant that the persisted cursor is always ≤ the last durable event in the active segment.
3. Resume cleanly across process restarts: subscribe from the persisted cursor, rely on at-least-once delivery to cover the overlap.
4. Store every event type DESIGN.md §4.4 names (`#commit`, `#identity`, `#account`, `#sync`) so the live_segments archive is faithful to what crossed the wire.
5. Persist a `phase` lifecycle marker so the server can refuse to start the live_segments consumer once steady-state has been reached. A future PR (the merge step) flips this marker.
6. Structure the new package so steady-state reuse requires changing only `cmd/jetstream` wiring — the consumer itself never names "backfill" or "live_segments".
7. Preserve `just test` under a second: the integration test is a scripted in-process WebSocket server, not a real network dependency.

## 2. Non-Goals

- The merge phase (DESIGN.md §4.2). No code rewrites events from `live_segments/` into `segments/` or compares revs against `repo/<did>.BackfillRev`. That's a follow-up PR.
- Steady-state phase plumbing. Hitting `phase = steady_state` in this PR is an explicit "not implemented" error, not silent fallback.
- Sync 1.1 / `#sync` resync handling. atmos v0.0.16 doesn't implement full sync 1.1. We archive `#sync` events into the segment file as `KindSync` with the CBOR payload preserved, but we do not act on them — atmos's auto-resync is disabled. The user is landing full sync 1.1 in atmos separately; this consumer will pick it up by upgrading the dependency and removing the opt-out.
- Lookaside file writes (DESIGN.md §3.3). `KindUpdate`, `KindDelete`, and account suppressions still get written into the segment file as ordinary events; the lookaside is a future PR.
- Block-time-based flush (the "or 30 seconds, whichever first" branch in DESIGN.md §3.1.1). The firehose generates plenty of traffic to fill 4096-event blocks quickly; if it stalls long enough to matter, that's a separate observability concern.
- Identity / handle resolution caching. The consumer just archives bytes.
- Replication. Out of scope.

## 3. Architecture

### 3.1 Package Layout

```
internal/livestream/                (NEW)
  doc.go                            package overview tying back to DESIGN.md §4.1
  config.go                         Config + validate()
  consumer.go                       Consumer (Open/Run/Close)
  cursor.go                         load/save the upstream relay cursor
  events.go                         pure ConvertEvent: streaming.Event → []segment.Event
  metrics.go                        Prometheus counters/gauges, nil-safe
  errors.go                         sentinel errors

  consumer_test.go                  integration: scripted ws server + tempdir + real pebble
  cursor_test.go                    round-trip + missing-key
  events_test.go                    table-driven + property + fuzz coverage
  events_swarm_test.go              random sequences, panic/invariant checks
  metrics_test.go                   registration round-trip

internal/lifecycle/                 (NEW)
  doc.go                            phase semantics
  phase.go                          Phase type, ReadPhase / WritePhase
  phase_test.go                     round-trip + unknown-value rejection

internal/ingest/
  config.go                         MODIFIED: Config gains SeqKey,
                                    OnAfterFlush
  writer.go                         MODIFIED: seqNextKey becomes per-Config;
                                    flushAndRotateLocked invokes OnAfterFlush
                                    after the seq save

cmd/jetstream/
  main.go                           MODIFIED: read phase, write bootstrap on
                                    fresh dir, refuse to start in steady_state,
                                    start livestream.Consumer alongside backfill
```

Boundary discipline:

- `internal/livestream` imports `internal/ingest`, `internal/store`, `segment`, `internal/obs`, and `github.com/jcalabro/atmos/streaming`. Nothing else.
- `internal/lifecycle` imports `internal/store`. Nothing else. Lives outside `internal/store` because `store/store.go`'s package doc explicitly keeps that package keyspace-agnostic.
- `internal/ingest`'s public surface gains two optional fields. Default values keep existing callers (the backfill writer) byte-for-byte compatible.

### 3.2 Concurrency Model

Three sibling goroutines under the existing `errgroup` in `cmd/jetstream`:

1. HTTP server (already there).
2. Backfill engine (already there).
3. `livestream.Consumer.Run` (new).

The live consumer owns its own `*ingest.Writer` (pointed at `data/backfill/live_segments/`). The backfill writer (pointed at `data/segments/`) is unchanged. The two writers are wholly independent: separate directories, separate active segments, separate seq counters.

There is no shared mutable state between the consumer and the backfill engine. Both write to the same pebble database, but to disjoint key prefixes (`seq/next`, `relay/list_repos_cursor`, `repo/<did>` for backfill; `live_segments/seq/next` and `relay/cursor` for live).

`atmos/streaming.Client.Events(ctx)` returns an `iter.Seq2[[]Event, error]`; the consumer drives it on its own goroutine and translates each batch into `ingest.Writer.Append` calls. Internally the writer takes a single mutex, so concurrent appends are serialized; this consumer is a single producer per writer, so contention is none.

### 3.3 Data Flow

```
atmos streaming.Client.Events(ctx)        (consumer goroutine)
  └─ for batch, err := range it:
       ├─ if err != nil: log.Warn; continue   // atmos auto-reconnects
       └─ for evt := range batch:
            └─ segEvts := ConvertEvent(evt, time.Now().UnixMicro())
                 (1 event per record op for #commit, exactly 1 for non-commit)
            └─ for i := range segEvts:
                 └─ ingest.Writer.Append(&segEvts[i])
                      └─ inside writer:
                          ├─ seg.Append(ev) → may report block full
                          └─ on full:
                              ├─ seg.Flush()                   // fsync block
                              ├─ pebble.Set("live_segments/seq/next", Sync)
                              └─ Config.OnAfterFlush(ctx)      // NEW hook
                                   └─ Consumer.persistUpstream()
                                        └─ pebble.Set("relay/cursor", Sync)
            // After ALL ops for evt have been Append'd:
            └─ Consumer.lastUpstream.Store(evt.Seq)
```

Read the durability invariant in §3.5; the placement of `lastUpstream.Store(evt.Seq)` *after* the inner per-op loop is what upholds it.

### 3.4 `internal/ingest` Extension

We add two optional fields to `ingest.Config` and route them through the writer. Both default to today's behavior.

```go
type Config struct {
    // ... existing fields ...

    // SeqKey is the pebble key holding the writer's seq counter.
    // Default "seq/next" preserves backfill writer behavior. The
    // live_segments writer uses "live_segments/seq/next" so the two
    // counters do not collide.
    SeqKey string

    // OnAfterFlush, if non-nil, runs after each block flush has
    // completed: segment.Flush has fsynced, and SeqKey has been
    // pebble.Sync'd. Errors propagate up through Append. Used by the
    // live consumer to advance "relay/cursor" with the same per-block
    // cadence as seq/next. A nil hook is a no-op.
    OnAfterFlush func(ctx context.Context) error
}
```

The constant `seqNextKey = "seq/next"` becomes the default for `SeqKey`. `loadNextSeq` and `saveNextSeq` already take a `*store.Store`; they are extended to take a key and the writer holds it on the struct.

`flushAndRotateLocked` gains exactly one extra block after `saveNextSeq`:

```go
if w.cfg.OnAfterFlush != nil {
    if err := w.cfg.OnAfterFlush(ctx); err != nil {
        span.RecordError(err)
        return fmt.Errorf("ingest: on_after_flush: %w", err)
    }
}
```

Two `pebble.Sync` calls per block (seq/next and the consumer's cursor) is acceptable. Block flushes fire at most once per 4096 events; the durability story is identical to a single batched commit, and the simpler code is worth the trivially-larger fsync cost.

### 3.5 Durability Invariant

DESIGN.md §3.1.1 demands: *the persisted upstream cursor is always less than or equal to the the latest durable event in the segment file.*

Translated to this consumer:

1. Atmos delivers a batch of decoded `streaming.Event`s.
2. For each event, we compute its `[]segment.Event` (1 for non-commits; N ops for a commit) and call `Append` for each.
3. We update `Consumer.lastUpstream` (an `atomic.Int64` holding the last upstream seq whose ops have *all* been buffered into the active segment) **only after** every op of that event has returned from `Append`. Type is `int64` to match atmos's `streaming.Event.Seq`. DESIGN.md §3.5 names `relay/cursor` as `uint64` on disk; we encode the int64 as little-endian bytes and document the implicit non-negativity constraint (atmos relays only emit positive seqs).
4. When `Append` triggers a block flush, the writer first `fsync`s the block, then `pebble.Sync`s `seq/next`, then calls `OnAfterFlush` which `pebble.Sync`s `relay/cursor` with the value of `Consumer.lastUpstream.Load()`.

Why this works:

- A flush triggered mid-commit (op 3 of 7 fills the block) will read `lastUpstream` as the *previous* commit's seq — not the in-progress commit. On recovery the in-progress commit replays from the upstream relay; the already-buffered ops 1..3 are duplicated as ops 1..3+1..7 in the segments. Per DESIGN.md §1 the system guarantees at-least-once delivery and downstream consumers must be idempotent, so this is correct. The cursor never points past durable events.
- A flush triggered at exactly an event boundary reads `lastUpstream` equal to that event's seq, which is correct.
- A clean shutdown flushes and persists the cursor before returning, so no events are lost (just possibly duplicated on restart).

### 3.6 The `phase` Lifecycle Key

`internal/lifecycle/phase.go`:

```go
package lifecycle

type Phase string

const (
    PhaseBootstrap   Phase = "bootstrap"
    PhaseSteadyState Phase = "steady_state"
)

const phaseKey = "phase"

// ReadPhase returns the persisted phase, or "" if absent.
// Returns an error if the value is non-empty but unrecognized.
func ReadPhase(s *store.Store) (Phase, error)

// WritePhase persists p with pebble.Sync.
func WritePhase(s *store.Store, p Phase) error
```

Unknown values return an error from `ReadPhase` rather than mapping to a known phase. Per PRACTICES.md "silent fallbacks are often a mistake" — if we ever read garbage we want to crash and investigate, not assume the safe default.

`cmd/jetstream/runServe` consults the phase before starting goroutines:

```go
phase, err := lifecycle.ReadPhase(metaStore)
if err != nil { return err }

if phase == "" {
    // Fresh data dir or upgrade from a pre-phase build. Both are
    // bootstrap.
    phase = lifecycle.PhaseBootstrap
    if err := lifecycle.WritePhase(metaStore, phase); err != nil {
        return err
    }
}

switch phase {
case lifecycle.PhaseBootstrap:
    g.Go(...backfill...)
    g.Go(...liveConsumer...)
case lifecycle.PhaseSteadyState:
    return errors.New("serve: steady-state phase not yet supported; the merge step has not been implemented")
default:
    // Unreachable: ReadPhase rejected unknown values.
    return fmt.Errorf("serve: unhandled phase %q", phase)
}
```

The "fresh data dir" upgrade write is itself written with `pebble.Sync`. A crash between `ReadPhase` returning empty and `WritePhase` persisting bootstrap leaves the next start in exactly the same state — re-run with `phase = ""`. Idempotent.

### 3.7 Future Reuse for Steady State

When the merge step lands, `cmd/jetstream` will:

1. Detect `phase == PhaseBootstrap` *and* a "backfill is complete" signal. The shape of that signal is for the merge PR to decide; a likely path is a sibling `backfill_done` key in `internal/lifecycle`.
2. Stop the live consumer cleanly. Read `relay/cursor` from pebble.
3. Run the merge: read each `live_segments/seg_*.jss`, decompress its blocks, and re-Append them to the main segments writer (gated on `repo/<did>.BackfillRev` per DESIGN.md §4.2).
4. Delete `data/backfill/live_segments/` and the `live_segments/seq/next` pebble key.
5. `WritePhase(PhaseSteadyState)`.
6. Start a new `livestream.Consumer` pointing at `data/segments/` with `SeqKey = "seq/next"` and the same `relay/cursor`.

Step 6 is the payoff: the live consumer code does not change. Only the wiring in `cmd/jetstream` does.

## 4. Public API

### 4.1 `livestream.Config`

```go
type Config struct {
    // SegmentsDir is the directory where the consumer writes seg_*.jss.
    // For this PR: "<data-dir>/backfill/live_segments".
    // Future steady-state reuse: "<data-dir>/segments".
    SegmentsDir string

    // Store is the shared metadata pebble db.
    Store *store.Store

    // SeqKey is the pebble key for the segment writer's seq counter.
    // For this PR: "live_segments/seq/next".
    SeqKey string

    // CursorKey is the pebble key for the upstream relay cursor.
    // For both phases: "relay/cursor".
    CursorKey string

    // RelayURL is the upstream relay base URL (e.g. https://bsky.network).
    // The consumer derives the WebSocket URL by replacing the scheme and
    // appending /xrpc/com.atproto.sync.subscribeRepos.
    RelayURL string

    // Logger is required.
    Logger *slog.Logger

    // Metrics is optional; nil means no /metrics counters incrementing.
    Metrics *Metrics

    // MaxSegmentBytes / MaxEventsPerBlock forwarded to ingest.Config.
    // Zero means defaults from internal/ingest.
    MaxSegmentBytes   int64
    MaxEventsPerBlock int

    // now is overridable for tests; production code uses time.Now.
    now func() time.Time
}
```

### 4.2 `livestream.Consumer`

```go
// Consumer pumps the upstream firehose into a directory of segment
// files. Goroutine-safe to construct, single-producer Run.
type Consumer struct { /* unexported */ }

// Open initializes the consumer's writer and validates config. Does
// not subscribe to the firehose; that happens in Run.
func Open(cfg Config) (*Consumer, error)

// Run subscribes to the relay and pumps events until ctx is cancelled
// or atmos returns a fatal error. Returns nil on clean cancellation.
func (c *Consumer) Run(ctx context.Context) error

// Close flushes any pending block, persists the cursor, and closes
// the underlying writer. Idempotent.
func (c *Consumer) Close() error

// LastUpstreamSeq returns the highest upstream seq whose ops have all
// been buffered into the active segment. Note this is the in-memory
// value, not the persisted relay/cursor — the persisted cursor lags
// by at most one in-flight block. Reported for tests and the future
// merge orchestrator that needs to know where to resume the steady-
// state consumer from.
func (c *Consumer) LastUpstreamSeq() int64
```

### 4.3 `livestream.ConvertEvent`

```go
// ConvertEvent translates one atmos streaming.Event into zero or more
// segment.Events. Each #commit op produces one segment.Event;
// #identity / #account / #sync each produce exactly one. #info, label
// frames, and unknown event types produce zero (caller logs+skips).
//
// indexedAt is the wall-clock instant the consumer observed the
// event, in unix microseconds. All segment.Events derived from the
// same upstream event share the same indexedAt.
//
// Pure: no I/O, no allocation beyond the result slice and CBOR
// marshalling. Safe to call from tests against arbitrary events.
func ConvertEvent(evt streaming.Event, indexedAt int64) ([]segment.Event, error)
```

Mapping table:

| Upstream                 | Output                                                         |
| ------------------------ | -------------------------------------------------------------- |
| `#commit` op (Create)    | 1× `segment.Event{Kind: KindCreate, Payload: op.BlockData()}`  |
| `#commit` op (Update)    | 1× `segment.Event{Kind: KindUpdate, Payload: op.BlockData()}`  |
| `#commit` op (Delete)    | 1× `segment.Event{Kind: KindDelete, Payload: nil}`             |
| `#commit` op (other)     | error returned (forward-compat: this is a relay schema bug)    |
| `#identity`              | 1× `segment.Event{Kind: KindIdentity, Payload: cbor}`          |
| `#account`               | 1× `segment.Event{Kind: KindAccount, Payload: cbor}`           |
| `#sync`                  | 1× `segment.Event{Kind: KindSync, Rev: ev.Sync.Rev, Payload: cbor}` |
| `#info`                  | empty slice                                                    |
| label frame              | empty slice                                                    |
| any decode error         | error returned                                                 |

For non-commit events, `Payload` is the result of `evt.<Type>.MarshalCBOR()`. atmos's generated types implement this. `DID`, `Collection`, `Rkey`, `Rev` are populated where the source event has them; for `#identity` and `#account` the DID is from the event itself, Collection/Rkey/Rev are empty.

`Seq` is left zero on the returned events; `ingest.Writer.Append` allocates the value.

### 4.4 atmos client construction

```go
xc := &xrpc.Client{
    Host:       cfg.RelayURL,
    HTTPClient: gt.Some(xrpc.NewHTTPClient(2 * time.Minute)),
}

wsURL := deriveSubscribeReposURL(cfg.RelayURL)
//   "https://bsky.network" → "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"

cur, err := loadUpstreamCursor(cfg.Store, cfg.CursorKey)
if err != nil { return err }

opts := streaming.Options{
    URL:         wsURL,
    Cursor:      gt.Some(cur),
    SyncClient:  gt.Some[*sync.Client](nil),  // disable auto-resync; out of scope
    OnReconnect: gt.Some(onReconnect),
    BatchSize:   gt.Some(50),                 // atmos default; explicit for clarity
    BatchTimeout: gt.Some(500 * time.Millisecond),
}

client, err := streaming.NewClient(opts)
```

`deriveSubscribeReposURL` is a small helper in `consumer.go`. Tests cover the URL derivation cases (https → wss, http → ws, trailing slash, missing scheme rejected).

## 5. Testing Strategy

Following PRACTICES.md (integration > unit; fuzz/property where invariants exist; tests must run fast).

### 5.1 `events_test.go` — `ConvertEvent`

Table-driven cases for every upstream event type. Asserts:

- Produced `segment.Event` count.
- Per-event `Kind`, `DID`, `Collection`, `Rkey`, `Rev`.
- For commits: `Payload` byte-equals the operation's `BlockData()` (or nil for deletes).
- For non-commits: `Payload` byte-decodes back to a struct equal to the source.
- `IndexedAt` equals the input value across all returned events.
- `Seq` is zero on every returned event.

### 5.2 `events_swarm_test.go` — `ConvertEvent`

A swarm-style test that builds random sequences of mixed events (commits with random op counts, identity, account, sync, info, malformed frames). Asserts:

- Never panics.
- Every returned `segment.Event` has a non-empty `DID` and a valid `Kind`.
- `Collection`, `Rkey`, `Rev`, `DID` lengths fit their on-disk column widths.
- Errors are returned, not swallowed, for malformed input.

A small fuzz target (`FuzzConvertEvent_Commit`) runs random byte sequences through CAR decoding paths so we surface panics on adversarial input.

### 5.3 `cursor_test.go`

- `LoadUpstreamCursor` on empty store returns 0.
- Save / load round-trips a 64-bit value.
- Saving a smaller value than current is allowed (supports operator manual rewind for debugging) but is not tested as a semantic guarantee — this is just a kv pair.

### 5.4 `consumer_test.go` — integration

A single integration test, no real network. Stand up an `httptest.Server` on a free port whose handler:

1. Upgrades to a WebSocket via `coder/websocket` (matching atmos's client).
2. Reads `?cursor=` from the request URL.
3. Sends a scripted sequence of CBOR-encoded firehose frames: 3 commits (with mixed Create/Update/Delete ops, total 8 ops), 1 `#identity`, 1 `#account`, 1 `#sync`, 1 unknown frame.
4. After the script finishes, holds the connection open until the client closes.

Test body:

1. `t.TempDir()` for the data dir.
2. Open a `store.Store`.
3. Build `livestream.Config` pointing at the fake server, with `MaxSegmentBytes = 4 << 10` and `MaxEventsPerBlock = 3` so we exercise multiple block flushes and (later) a rotation.
4. `Open` the consumer; run `Run` in a goroutine with a context bounded by `t.Context()`.
5. Wait for `c.LastUpstreamSeq()` to reach the last scripted seq with a 1-second poll loop.
6. Cancel the context, await `Run` return.
7. Re-open the segments dir with `segment.Reader`. Assert the on-disk events match the scripted ones in order, with Kinds and Payloads as expected.
8. Re-read `relay/cursor` from pebble and assert it equals the last scripted seq, and is ≤ the highest event in the segment file.

A second sub-test asserts crash recovery:

1. Identical setup, but the fake server sends 5 events then panics the websocket read mid-batch (closes with a 1006 / abnormal-closure).
2. Cancel ctx after seeing 3 events durable in the segment.
3. Re-open the consumer with the same config; the second fake server starts at `?cursor=N+1` where N is the persisted cursor.
4. Assert the cursor sent by the second connection is ≤ the seq of the last durable event.

### 5.5 `cmd/jetstream/serve_test.go` — phase gate

Add a sub-test that:

1. Pre-populates the data dir with a `phase = "steady_state"` pebble key.
2. Runs `runServe` with a context that immediately cancels.
3. Asserts the returned error mentions "steady-state phase not yet supported".

A second sub-test verifies the fresh-dir bootstrap: empty pebble → `runServe` writes `phase = bootstrap` before returning. (Drive it with `runServe` cancelled immediately; check the key.)

### 5.6 `phase_test.go`

- Empty store: `ReadPhase` returns `("", nil)`.
- Round-trip both known values.
- Pre-populate with `"banana"`: `ReadPhase` returns an error.

### 5.7 Test-budget discipline

- `consumer_test.go` integration runs entirely in-process (`httptest.Server`, real `coder/websocket`, real pebble in `t.TempDir()`). Target wall-clock: < 200ms.
- Swarm test bound at 1k iterations under `just test-long`, 50 under `just test`.
- No real network, no DNS. The fake firehose lives in the test file.

## 6. Observability

Following the patterns in `internal/ingest/metrics.go` and `internal/backfill/metrics.go`:

```go
// internal/livestream/metrics.go
type Metrics struct {
    EventsReceived       prometheus.Counter        // labelled by atmos event type
    EventsConverted      prometheus.Counter        // segment.Events emitted
    UpstreamCursor       prometheus.Gauge          // last-persisted relay/cursor
    UpstreamLag          prometheus.Gauge          // optional, future: time since last event
    Reconnects           prometheus.Counter
    DecodeErrors         prometheus.Counter
}
```

Tracing: one span per `ConvertEvent` call would explode at firehose rates (~1k events/sec). Instead, a span per atmos batch (`livestream.batch`) plus the existing `ingest.flush_block` span when a flush trips. Reconnect attempts log at WARN with attempt count and delay.

slog at INFO on Run start ("livestream: subscribing", relay URL, starting cursor) and on clean shutdown ("livestream: stopped", final cursor, lastUpstreamSeq).

## 7. Wiring in `cmd/jetstream/main.go`

Pseudocode-level diff:

```go
func runServe(ctx context.Context, cmd *cli.Command) error {
    // ... existing logger / tracing / metrics setup ...
    // ... existing metaStore Open ...

    phase, err := lifecycle.ReadPhase(metaStore)
    if err != nil { return fmt.Errorf("serve: read phase: %w", err) }
    if phase == "" {
        phase = lifecycle.PhaseBootstrap
        if err := lifecycle.WritePhase(metaStore, phase); err != nil {
            return fmt.Errorf("serve: write phase: %w", err)
        }
    }

    if phase == lifecycle.PhaseSteadyState {
        return errors.New("serve: steady-state phase not yet supported")
    }

    // ... existing ingest.Open for the backfill writer ...

    liveConsumer, err := livestream.Open(livestream.Config{
        SegmentsDir:       filepath.Join(dataDir, "backfill", "live_segments"),
        Store:             metaStore,
        SeqKey:            "live_segments/seq/next",
        CursorKey:         "relay/cursor",
        RelayURL:          cmd.String("relay-url"),
        Logger:            logger,
        Metrics:           livestream.NewMetrics(metrics.Registry),
    })
    if err != nil { return fmt.Errorf("livestream open: %w", err) }
    defer func() {
        if cerr := liveConsumer.Close(); cerr != nil {
            logger.Error("close live consumer", "err", cerr)
        }
    }()

    // ... existing srv := server.New ...
    // ... existing signal.NotifyContext ...

    g, gctx := errgroup.WithContext(runCtx)
    g.Go(func() error { return srv.Run(gctx) })
    g.Go(func() error { return backfill.Run(gctx, /* ... */) })
    g.Go(func() error { return liveConsumer.Run(gctx) })

    // ... existing graceful shutdown ...
}
```

The new flag situation: zero. `--relay-url` is already on `serve` and is shared between the backfill engine and the live consumer.

## 8. Failure Modes and Recovery

| Failure                                 | Behavior                                                                                                                                  |
| --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| Network error mid-stream                | atmos's `streaming.Client` reconnects with exponential backoff. Cursor is preserved across reconnects (it's our pebble key, not atmos's). |
| Decode error on a frame                 | Logged at WARN with frame bytes; metrics increment; iteration continues.                                                                  |
| Unknown event type                      | Skipped silently (forward compat per atmos's existing behavior).                                                                          |
| `pebble.Set("relay/cursor", Sync)` fail | Returned from `OnAfterFlush`, propagates up through `ingest.Writer.Append`, propagates up through `Consumer.Run`, errgroup tears down.    |
| `segment.Flush` fsync fail              | Same path; errgroup tears down. PRACTICES.md: "crashing is preferred over data corruption."                                               |
| Crash mid-block                         | On restart: `ScanMaxSeq` reconciles `live_segments/seq/next`; `relay/cursor` ≤ last durable event; resume firehose with overlap.          |
| Clean shutdown                          | `Consumer.Close` flushes the active segment (no-op if empty), then writes the latest cursor with sync, then closes pebble + writer.       |

## 9. Risks and Open Questions

1. **Two `pebble.Sync` calls per block flush.** Acceptable; flushes are at most once per 4096 events. If profiling shows it bites, we can refactor to a single `pebble.Batch` (sync once, atomic across keys) — but the durability invariant holds either way.
2. **`CursorKey` pebble key naming.** I went with `relay/cursor` (matches DESIGN.md §3.5). The future steady-state consumer reuses the same key — this is desirable, since the merge step seamlessly hands cursor ownership over.
3. **`SeqKey` parallel-track collision risk.** Two writers, two seq counters. `ingest.Writer.Open` is the only place these counters get manipulated; tests cover correct restart behavior with a non-default `SeqKey`. The risk is operator confusion ("which seq is which?") — a one-line comment near `Config.SeqKey` plus the metrics labels (`segment_dir="live_segments"` vs. `"segments"`) handles that.
4. **atmos batch timing.** Default 50 events / 500ms. At firehose rates (~1k events/sec) that's ~50ms-batches, which is fine. We do not adjust.
5. **`#sync` events archived but not actioned.** Acceptable for now per the user's note. When atmos lands full sync 1.1, removing the `SyncClient: gt.Some[*sync.Client](nil)` opt-out is the only consumer-side change.
6. **Replicas (DESIGN.md §6).** This consumer is a leader-only construct: it speaks `subscribeRepos` to a relay. Replicas will have their own consumer that speaks the extended-mode jetstream WebSocket. That's a future PR; the `livestream` package may end up with a sibling.
