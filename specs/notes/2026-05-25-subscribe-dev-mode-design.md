# /subscribe dev-mode flag — design

## Problem

The `/subscribe` handler is gated on `lifecycle.IsSteadyState(store)` and
returns 503 until backfill drains, which takes ~a day. There's no way to
exercise the live-tail wire format end-to-end against the real upstream
firehose during local development without waiting out a full backfill.

## Constraints

- Developer convenience only. Not a supported deployment mode.
- Must exercise the production code path: real upstream firehose → real
  `live.Consumer` → real `Broadcaster` → real handler → real wire encoder.
  No synthetic event injection, no in-memory shortcuts.
- Disk writes are fine. The dev-mode path may freely create segment
  files and pebble entries; operators are expected to `just clean`
  between runs.
- Loud at startup. Impossible to forget the flag is on.

## Design

Three code changes plus tests.

### 1. `internal/subscribe`: introduce `SteadyStateGate`

Replace the handler's `*store.Store` parameter with a small interface:

```go
type SteadyStateGate interface { IsReady() bool }
```

Provide two implementations in the same package:

- `LifecycleGate{Store *store.Store}` — wraps `lifecycle.IsSteadyState`.
  This is what prod uses.
- `OpenGate{}` — always returns true. This is what `--dev-mode` uses.

The handler stops importing `internal/store` and `internal/lifecycle`
directly, which makes `handler_test.go` simpler (the
`newSteadyStateStore` helper goes away in favor of stub gates).

### 2. `cmd/jetstream/main.go`: `--dev-mode` flag

New `cli.BoolFlag`:

- Name: `dev-mode`
- Env: `JETSTREAM_DEV_MODE`
- Default: `false`

In `runServe`, after building the broadcaster:

```go
var gate subscribe.SteadyStateGate
if cmd.Bool("dev-mode") {
    logger.Warn("DEV MODE ENABLED — /subscribe gate is open during bootstrap; do not use in production")
    gate = subscribe.OpenGate{}
} else {
    gate = subscribe.LifecycleGate{Store: metaStore}
}
```

The gate is then passed to `subscribe.NewHandler` in place of the store.

### 3. `internal/ingest/orchestrator/bootstrap.go`: always publish to broadcaster

In `runBootstrap`, the bootstrap-time `live.Open` call (lines 66-77)
omits `OnEvent`. Add it:

```go
OnEvent: o.cfg.OnEvent,
```

This is unconditional — both prod and dev. In prod, the gate keeps
subscribers out, so `Publish` runs against an empty subscriber map;
the cost is one `RLock`/empty-map iteration plus one metrics
increment per event. At firehose volume this is noise. The benefit
is that bootstrap-live and steady-state-live are now wired
identically, eliminating a dev/prod divergence that would otherwise
hide bugs.

## Testing

### Unit

- `internal/subscribe/handler_test.go`: replace `newSteadyStateStore`
  with a stub gate. Existing test cases (`RejectsWhenNotSteadyState`,
  `HappyPath_DeliversIdentityEvent`, `SyncEventNotEmitted`) move to
  the stub-gate construction. Rename `RejectsWhenNotSteadyState` to
  `RejectsWhenGateClosed`.
- New `gate_test.go`: pin `LifecycleGate.IsReady` against each
  `lifecycle.Phase` value plus the empty-key case, and pin
  `OpenGate.IsReady` returning true.

### Integration (cmd/jetstream)

New test in `cmd/jetstream/serve_test.go` modeled on
`TestServe_StartsInSteadyStatePhase`:

- Fresh data dir, no pre-seeded phase. Phase will be `bootstrap`.
- Fake relay that:
  - Serves an empty `listRepos` page so backfill drains immediately
    (no DIDs to download).
  - Accepts the `subscribeRepos` websocket and writes one synthetic
    `#identity` frame to it.
- `serve` with `--dev-mode --addr=127.0.0.1:0`.
- After the public listener is up, dial `ws://127.0.0.1:<port>/subscribe`.
- Assert: 101 Switching Protocols (not 503), and one identity frame
  received within a short timeout.

This covers both the gate flip (101 instead of 503 in PhaseBootstrap)
and the new bootstrap-time `OnEvent` wiring (event reaches the
subscriber during bootstrap, not just after merge).

A counterpart `TestServe_SubscribeRejectsWithoutDevMode` confirms the
default returns 503.

### Manual smoke test

```sh
just clean
just run serve --dev-mode
# in another shell:
websocat ws://localhost:8080/subscribe
# expect: 101 Switching Protocols, then JSON frames as the
# bootstrap-time live consumer connects to bsky.network and forwards
# events.
```

## Out of scope

- True zero-disk operation (would require a writer-less `live.Consumer`
  variant — large change, no current need).
- Promoting dev-mode to a supported `live_only` deployment phase.
- Surfacing dev-mode in the status page or in metrics.

## Risk notes

- The unconditional `OnEvent` wiring in bootstrap is the only change
  that touches prod behavior. Mitigation: the gate keeps subscribers
  out in prod, so `Publish` runs against an empty map; cost is
  empirically negligible at firehose volume.
- `--dev-mode` must never be set in prod. Mitigation: loud
  `logger.Warn` at startup; default is `false`; this spec and the
  flag's `Usage` string both say "developer only".
