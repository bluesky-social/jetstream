# Backfill Bootstrap (PR 1): Wire the atmos Backfill Engine

## 1. Summary

Wire `cmd/jetstream serve` to drive the atmos `backfill.Engine` end-to-end:
enumerate the upstream relay's `com.atproto.sync.listRepos`, download each
repo from the account's PDS, and persist per-DID lifecycle state in pebble
at `repo/<did>` per [DESIGN.md §3.5](../../../DESIGN.md). Survive restart
by re-walking listRepos and skipping DIDs already at `StateComplete`.

This is the first slice of [DESIGN.md §4.1 Bootstrap Phase](../../../DESIGN.md).
Segment file writing, the live firehose consumer, the merge phase, and
`backfill_complete.log` are explicitly out of scope and land in follow-up PRs.

`Handler.HandleRepo` is a no-op + log + counter. The point of this PR is to
prove the engine wiring works: relay enumeration, PDS resolution, repo
download, signature verification, and durable per-DID state.

## 2. Context

`internal/backfill` was deleted in commit `92c1a90` ahead of a redesign
against atmos v0.0.15. The prior implementation maintained its own bespoke
state machine with `Phase` enums; the new implementation delegates that
logic to atmos and stores only what DESIGN.md §3.5 prescribes.

The build is currently broken: `cmd/jetstream/main.go` and
`cmd/jetstream/serve_test.go` still import the deleted package. This PR
fixes that.

## 3. Goals

1. `serve` brings up the HTTP listeners and runs the atmos backfill engine
   in a sibling errgroup goroutine. Either failing cancels the other.
2. Per-DID state is persisted to `<data-dir>/meta.pebble/repo/<did>` as
   the §3.5 `RepoStatus` JSON.
3. Restart in the middle of backfill resumes from where it left off.
4. Restart after backfill is complete is a fast no-op.
5. Repos download via the account's PDS (atmos `Directory` resolution),
   not the relay, with commit signature verification.

## 4. Non-Goals

These are deliberately deferred to keep this PR small:

- Writing repo records to segment files (`HandleRepo` is a no-op).
- The live firehose consumer for `backfill/live_shards/`.
- The merge phase (DESIGN.md §4.2).
- `backfill_complete.log` (DESIGN.md §3.5) — separate append-only file,
  introduced when replication lands.
- `relay/cursor` durability — there's no firehose consumer yet, so no
  cursor to persist.
- Returning HTTP 503 from the public listener while backfill is in
  flight (DESIGN.md §4.1). For now the server stays up and serves
  whatever it serves today.
- Replication, timestamp import, lookaside compaction — far future.

## 5. Architecture

### 5.1 Package Layout

A single new package `internal/backfill`:

```
internal/backfill/
  doc.go         package overview, DESIGN.md cross-refs
  run.go         Run(ctx, Config) — entry point; constructs deps and drives the engine
  status.go      RepoStatus / RepoBackfillStatus / Status types per DESIGN.md §3.5
  store.go       atmos backfill.Store impl backed by *store.Store; key = "repo/<did>"
  handler.go     no-op atmos backfill.Handler (logs + counts)
  metrics.go     Prometheus counters/gauges, registered on the shared registry
  store_test.go
  handler_test.go
  run_test.go
```

### 5.2 Engine Lifecycle

The atmos `backfill.Engine` is single-shot: one `Run` call paginates
listRepos to completion and returns. We use that property as the resume
mechanism. On every process start:

1. Open pebble at `<data-dir>/meta.pebble`.
2. Construct a fresh `Engine` with our `Store` and `Handler`.
3. Call `engine.Run(ctx)`.
4. The engine's producer paginates listRepos; for each entry it calls
   `Store.Lookup`, sees `StateComplete` for already-finished DIDs, skips
   them, dispatches the rest to workers.
5. When the producer drains and workers idle, `Run` returns nil. Our
   goroutine returns nil. The HTTP server keeps running.

There is no "phase" state machine in our code. atmos owns it.

### 5.3 Pebble Keyspace

Per DESIGN.md §3.5:

- `repo/<did>` → JSON-encoded `RepoStatus`

`RepoStatus` matches §3.5 verbatim, with one addition required by atmos:

```go
// status.go

type Status string

const (
    StatusNotStarted Status = "not_started"
    StatusComplete   Status = "complete"
    StatusFailed     Status = "failed"
)

type RepoBackfillStatus struct {
    Status      Status    `json:"status"`
    Rev         string    `json:"rev,omitempty"`
    Attempts    int       `json:"attempts,omitempty"`
    LastError   string    `json:"last_error,omitempty"`
    StartedAt   time.Time `json:"started_at,omitempty"`
    CompletedAt time.Time `json:"completed_at,omitempty"`
}

type RepoStatus struct {
    Backfill    RepoBackfillStatus `json:"backfill"`
    PDS         string             `json:"pds,omitempty"`
    Rev         string             `json:"rev,omitempty"`
    UpdatedAt   time.Time          `json:"updated_at,omitempty"`
    RecordCount int64              `json:"record_count,omitempty"`
    TotalBytes  int64              `json:"total_bytes,omitempty"`

    // Active is the last-observed listRepos.Active value. atmos requires
    // this on every row so the engine can detect liveness flips without
    // an extra round-trip. DESIGN.md §3.5 doesn't pin a JSON tag for it
    // (because the original draft didn't anticipate the atmos active-flip
    // callback) — recording it here keeps the §3.5 RepoStatus shape stable
    // and adds the one field atmos needs.
    Active bool `json:"active"`
}
```

This PR populates only `Backfill.{Status, Rev, Attempts, LastError,
StartedAt, CompletedAt}` and `Active`. The other fields stay zero —
they'll be populated by steady-state ingest (segment writing, live
firehose). Storing the wider struct now avoids a forced schema migration.

The `not_started` value is what `OnDiscover` writes; we don't need a
separate "discovered" disk value because the row's mere existence at
`not_started` indicates the engine has seen it. atmos's `StateDiscovered`
maps to our `StatusNotStarted` on Lookup.

### 5.4 Store Implementation (`store.go`)

The atmos `backfill.Store` interface has 5 methods. All writes are
durable (`pebble.Sync`) before returning, per atmos's contract.

```go
type Store struct {
    db      *store.Store
    metrics *Metrics // optional
}

func New(db *store.Store, metrics *Metrics) *Store
```

Key construction is a private helper `repoKey(did atmos.DID) []byte`
that returns `[]byte("repo/" + did)`.

**`Lookup(ctx, did)`** — point-Get on `repo/<did>`, decode JSON,
project to `atmos.StoreEntry{State, Active}`:

| Disk state | Returned `State` |
|---|---|
| no row | `StateUnknown` |
| `Backfill.Status == StatusNotStarted` | `StateDiscovered` |
| `Backfill.Status == StatusComplete` | `StateComplete` |
| `Backfill.Status == StatusFailed` | `StateFailed` |

`Active` comes straight off the row.

**`OnDiscover(ctx, entry)`** — writes a fresh `RepoStatus`:

```go
RepoStatus{
    Backfill: RepoBackfillStatus{
        Status:    StatusNotStarted,
        StartedAt: time.Now().UTC(),
    },
    Active: entry.Active,
}
```

**`OnUpdate(ctx, entry)`** — read-modify-write the existing row to flip
`Active`. atmos guarantees no concurrent callbacks for the same DID, so
RMW is safe without a transaction. If the row somehow doesn't exist, we
treat it as a corrupted-state error and return it; that aborts the Run.

**`OnComplete(ctx, did, commit)`** — RMW the row:

```go
status.Backfill.Status      = StatusComplete
status.Backfill.Rev          = commit.Rev
status.Backfill.CompletedAt  = time.Now().UTC()
status.Rev                   = commit.Rev
status.UpdatedAt             = time.Now().UTC()
```

**`OnFail(ctx, did, err, attempts)`** — RMW the row:

```go
status.Backfill.Status     = StatusFailed
status.Backfill.LastError  = err.Error()
status.Backfill.Attempts   = attempts
```

Notes on `OnFail` semantics:

- `attempts` is the count for the current Run only (atmos passes
  `initial + retries` from the current `processRepo` call). We
  overwrite rather than accumulate across Runs; `LastError` is the
  most recent failure and `Attempts` is the most recent attempt
  count. This matches DESIGN.md §6.3, which calls out that
  `Attempts` resetting on failover is an acceptable cosmetic
  regression.
- `Backfill.StartedAt` is left as written by `OnDiscover` (or by a
  prior Run's `OnDiscover`). It's the wall-clock time the engine
  first saw the DID; it doesn't reset on failure.
- `OnFail` does NOT clear `CompletedAt` or `Rev` from a prior
  successful run, so a repo that completed once and later fails
  (shouldn't happen within a single Run, but is conceivable across
  Runs) keeps its historical rev. Within this PR a Run never
  retries a `StateComplete` DID, so this is mostly defensive.

### 5.5 Engine Wiring (`run.go`)

```go
type Config struct {
    Store    *store.Store
    RelayURL string
    Logger   *slog.Logger
    Metrics  *Metrics  // optional; nil = no metrics
}

func Run(ctx context.Context, cfg Config) error
```

`Run` constructs:

1. **Shared HTTP client.** `&http.Client{Timeout: 30 * time.Second}`
   with the default transport. Passed to the relay xrpc client, the
   identity resolver, and `Options.HTTPClient` so the per-PDS pool
   reuses the same transport / connection cache.

2. **Relay xrpc client.**
   ```go
   xc := &xrpc.Client{
       Host:       cfg.RelayURL,
       HTTPClient: gt.Some(httpClient),
       Retry:      gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)}),
   }
   ```
   Per the atmos `Options.SyncClient` doc comment: the engine has its
   own retry/backoff loop, so xrpc retries must be disabled to avoid
   compounding.

3. **Identity directory.**
   ```go
   dir := &identity.Directory{
       Resolver: &identity.DefaultResolver{
           HTTPClient: gt.Some(httpClient),
       },
       Cache: identity.NewLRUCache(100_000, 24 * time.Hour),
   }
   ```
   100k DID cache covers most of the network. TTL is generous because
   PLC ops are rare and atmos's `Purge` will be needed only for
   key-rotation cases (which don't apply to bulk download).

4. **Sync client (relay).**
   ```go
   sc := sync.NewClient(sync.Options{
       Client:    xc,
       Directory: gt.Some(dir),
   })
   ```

5. **Engine.**
   ```go
   eng := backfill.NewEngine(backfill.Options{
       SyncClient: sc,
       Store:      ourStore,
       Handler:    ourHandler,
       Directory:  gt.Some(dir),
       HTTPClient: gt.Some(httpClient),
       OnError:    gt.Some(func(did atmos.DID, err error) { ... }),
       OnProgress: gt.Some(func(s backfill.Stats) { ... }),
   })
   return eng.Run(ctx)
   ```

The `OnError` callback logs at WARN with the DID and error. The
`OnProgress` callback fires after each successful repo and bumps the
`completed_total` Prometheus counter; every 1000 callbacks it logs
at INFO with the current count.

### 5.6 Handler (`handler.go`)

```go
type LogHandler struct {
    logger *slog.Logger
}

func (h *LogHandler) HandleRepo(ctx context.Context, did atmos.DID, r *repo.Repo, commit *repo.Commit) error {
    h.logger.Debug("backfill: repo handled",
        "did", did,
        "rev", commit.Rev,
    )
    return nil
}
```

No segment writes. No record walks. A future PR replaces this with the
real segment-writer-backed handler.

### 5.7 Metrics (`metrics.go`)

A small `Metrics` struct with Prometheus counters/gauges, constructed
against the shared `*prometheus.Registry` exposed by `internal/obs`:

```go
type Metrics struct {
    Discovered   prometheus.Counter         // jetstream_backfill_discovered_total
    Completed    prometheus.Counter         // jetstream_backfill_completed_total
    Failed       prometheus.Counter         // jetstream_backfill_failed_total
    ActiveFlips  prometheus.Counter         // jetstream_backfill_active_flips_total
    OnFailErrors prometheus.Counter         // jetstream_backfill_on_fail_store_errors_total
}

func NewMetrics(reg prometheus.Registerer) *Metrics
```

Counters are bumped from inside `Store.On*` and from the engine's
`OnError` / `OnProgress` callbacks. `nil` `*Metrics` is a no-op
everywhere — useful for tests.

### 5.8 Restart Behavior

Three cases, all driven by atmos's `Lookup`-then-skip pattern:

1. **First startup.** Pebble is empty. Every `Lookup` returns
   `StateUnknown` → engine fires `OnDiscover` → row written at
   `not_started` → engine downloads → `OnComplete` writes `complete`.

2. **Restart mid-backfill.** Pebble has a mix of rows. `Lookup`
   reports the disk state. `Complete` rows skip download. `Discovered`
   (i.e. `not_started`) and `Failed` rows are re-dispatched and
   re-attempted. At-least-once is fine — `HandleRepo` is a no-op, so
   re-running it is free.

3. **Restart after completion.** Every row is `complete`. Engine walks
   listRepos, every `Lookup` says `StateComplete`, no work is
   dispatched, `Run` returns nil immediately. Total cost is one
   `listRepos` enumeration plus one pebble `Get` per DID.

## 6. cmd/jetstream Wiring

`cmd/jetstream/main.go` already has the errgroup slot for backfill —
this PR only updates the call signature and types:

```go
g.Go(func() error {
    return backfill.Run(gctx, backfill.Config{
        Store:    metaStore,
        RelayURL: cmd.String("relay-url"),
        Logger:   logger,
        Metrics:  backfill.NewMetrics(metrics.Registry),
    })
})
```

The existing `seedMetrics := backfill.NewSeedMetrics(...)` line is
replaced with the new `NewMetrics` constructor. The `gt.Some(logger)`
wrapping is dropped since the new `Config` makes the logger
non-optional (always required — there's no sensible default for an
ingestion service that needs to surface failure modes).

## 7. Tests

Three test files in the new package:

### 7.1 `store_test.go`

Direct unit tests against a real pebble in `t.TempDir()`:

- `Lookup` on missing key returns `StateUnknown`, `Active=false`, no
  error.
- `OnDiscover` writes `not_started`. `Lookup` afterwards returns
  `StateDiscovered`.
- `OnComplete` writes `complete` with the commit rev. `Lookup`
  afterwards returns `StateComplete`. `RepoStatus.Rev` equals the
  commit rev.
- `OnFail` writes `failed` with the error string and attempts count.
  `Lookup` afterwards returns `StateFailed`.
- `OnUpdate` flips `Active` without changing `Backfill.Status`.
- Round-trip: `OnDiscover(active=true)` then `OnUpdate(active=false)`
  preserves `Backfill.Status == not_started` but flips `Active`.
- `OnComplete` preserves any pre-existing top-level fields (i.e. RMW
  doesn't clobber a hypothetical `RecordCount` set by a future PR).
  Implemented by writing a row with `RecordCount=42` directly via
  pebble, then calling `OnComplete`, then asserting `RecordCount==42`
  on read-back.

### 7.2 `handler_test.go`

Trivial: construct a `LogHandler` with a discarding logger, call
`HandleRepo`, assert nil error.

### 7.3 `run_test.go`

End-to-end against `httptest.NewServer`:

- Build a stub relay that serves:
  - `com.atproto.sync.listRepos` → two pages (3 DIDs total) then empty.
- Build a stub PDS (separate `httptest.NewServer`) that serves:
  - `com.atproto.sync.getRepo` → a real CAR built via `atmos/repo` for
    the requested DID.
- Build a stub PLC that serves DID documents pointing at the stub PDS.
- Drive `backfill.Run` with the real engine.
- Assert: all 3 DIDs reach `StatusComplete` in pebble.
- Re-run on the same data dir: `Run` returns nil quickly, no
  `getRepo` requests fired (assert via request counter on the PDS
  stub).
- Failure case: PDS returns 500. The DID lands at `StatusFailed`
  with the error captured. A subsequent run re-attempts.

### 7.4 `cmd/jetstream/serve_test.go` rewrite

The deleted test referenced `backfill.CountRepos`, `GetBootstrapState`,
`PhaseSeed`, `PhaseComplete` — none of which exist in the new design.
Replace with a wiring smoke test:

- Stub relay with two DIDs.
- Stub PDS serving real CARs.
- Run `serve`, wait for both pages to be served.
- Cancel context, wait for `Run` to return.
- Re-open pebble, assert both DIDs landed at `StatusComplete`.

(Keep this test intentionally simple — exhaustive coverage lives in
`internal/backfill/run_test.go` where the harness is more direct.)

## 8. Failure Modes

- **listRepos returns transient 5xx.** Atmos's xrpc layer surfaces it
  as a transient error; the engine's retry loop handles it; on
  retry-exhaustion, `producerLoop` returns the error from `Run`,
  which propagates up through our `Run` and cancels the errgroup.
  The HTTP server shuts down. This is acceptable: a backfill that
  can't enumerate the network can't proceed.

- **PDS returns 5xx for a single repo.** Atmos retries with backoff.
  After exhaustion, the DID lands at `StatusFailed` and the Run
  continues. A future Run re-attempts.

- **DID doc resolution fails.** Atmos falls back to the relay
  `getRepo` (per the atmos `syncClientForRepo` logic). Slower but
  still correct.

- **Pebble write fails.** atmos surfaces as a Run-aborting error.
  Process exits with the error; supervisor restarts; resume picks up.

- **Process killed mid-block.** Worst case is one in-flight repo
  download is dropped; the row stays at `not_started` and gets
  re-attempted on next Run.

## 9. Open Questions

None for this PR. All resolved during brainstorming:

- Handler scope: no-op + log + counter.
- Directory: yes, resolve to PDS (faster + signature verification).
- Schema: full DESIGN.md §3.5 shape, partially populated.
- Stale `serve_test.go`: delete, write a new one.
- Server gating: no 503 — server stays up.
- Test strategy: stub HTTP relay + real engine.

## 10. References

- [DESIGN.md §3.5 Metadata Store](../../../DESIGN.md)
- [DESIGN.md §4.1 Bootstrap Phase](../../../DESIGN.md)
- atmos backfill package: `/home/jcalabro/go/src/github.com/jcalabro/atmos/backfill/`
- atmos sync package: `/home/jcalabro/go/src/github.com/jcalabro/atmos/sync/`
- atmos identity package: `/home/jcalabro/go/src/github.com/jcalabro/atmos/identity/`
