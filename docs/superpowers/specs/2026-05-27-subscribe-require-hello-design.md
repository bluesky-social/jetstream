# `requireHello` Support in `/subscribe` — Design

**Date:** 2026-05-27
**Status:** Approved (pre-implementation)
**Scope:** v1 wire-compat addition to the existing `/subscribe` websocket handler in `internal/subscribe`.

## Problem

Jetstream v1 supports a `?requireHello=true` query param on `/subscribe`. When set, the server must NOT deliver any events to the client until the client sends a valid `options_update` `SubscriberSourcedMessage` over the websocket. v1 README §"Options Updates":

> Additionally, a client can connect with `?requireHello=true` in the query params to pause replay/live-tail until the first Options Update message is sent by the client over the socket.
>
> Invalid Options Updates in `requireHello` mode or normal operating mode will result in the client being disconnected.

v2 already implements `wantedDids` / `wantedCollections` / `maxMessageSizeBytes` / mid-stream `options_update`; `requireHello` is the last v1 query-param gap on the live-tail path. Without it, v1 clients that rely on hello-gating (typically to set their initial filter atomically before any events are sent) cannot port to v2 without behavior changes.

## Goals

- Match v1 behavior exactly:
  - `?requireHello=true` blocks event delivery until the first valid `options_update`.
  - The string comparison is `requireHello == "true"` (case-sensitive). Any other value, including `"false"`, `"True"`, `"1"`, or absence, is treated as false.
  - An invalid `options_update` while waiting (or after) terminates the connection with a websocket close.
  - No timeout — the wait lasts until the client sends hello OR disconnects.
- Keep the change scoped to `internal/subscribe/handler.go` plus tests. No broadcaster changes, no new metrics, no new types.
- Robust under the abuse / edge cases v1 doesn't enumerate: client never sends, client sends binary, client sends unknown type then a valid update, client sends multiple valid updates, multiple goroutines racing on the hello signal.

## Non-goals

- Cursor replay (no v2 implementation yet; `requireHello` only gates live-tail in this spec).
- Zstd compression / `Socket-Encoding` header (same — out of scope for v1-compat parity work).
- Any change to the `options_update` parser, the filter type, or the broadcaster.

## Architecture

### Wire surface

A new query parameter, `requireHello`. Truthy iff the literal string equals `"true"`. This intentionally matches v1's `c.Request().URL.Query().Get("requireHello") == "true"` and is locked down by a small unit test enumerating the parse rules. v1's behavior is "not great taste" (typed booleans would be nicer), but it IS the wire contract — existing clients that send `requireHello=true` and expect blocking, or omit the param and expect normal flow, must keep working unchanged.

A bad value (e.g. `requireHello=garbage`) is silently treated as false, matching v1. No 400 error, no log warning at `Warn` level. (We may emit a single Debug-level log if we want, but that's an implementation detail.)

### Control flow change in `serve`

Today, `serve` runs in this order:

1. Readiness gate (`lifecycle.IsSteadyState`).
2. Parse query string → `initialFilter`.
3. `websocket.Accept`.
4. `conn.SetReadLimit(MaxSubscriberMessageBytes)`.
5. Initialize per-connection `filterPtr atomic.Pointer[Filter]`.
6. `broadcaster.Subscribe()` → returns `subCh, doneCh, unsubscribe`.
7. Build `ctx, cancel := context.WithCancel(r.Context())`.
8. Start the reader goroutine (parses incoming `SubscriberSourcedMessage`s).
9. Writer loop selecting on ctx, doneCh, ping ticker, subCh.

After this change, the order becomes:

1. Readiness gate.
2. Parse query string → `initialFilter`, `requireHello` bool.
3. `websocket.Accept`.
4. `conn.SetReadLimit`.
5. Initialize `filterPtr`.
6. Build `ctx, cancel`.
7. Allocate `helloCh chan struct{}` and a `sync.Once` for safe close.
8. Start the reader goroutine, which now ALSO closes `helloCh` (via the `Once`) on the first valid `options_update` if `requireHello` was set.
9. **If `requireHello`: `select { case <-helloCh: case <-ctx.Done(): return }`** — pre-Subscribe wait.
10. `broadcaster.Subscribe()` (now happens AFTER hello, if hello was required).
11. Writer loop unchanged.

Critical correctness points:

- **Subscribe is delayed**, so the broadcaster never queues an event for this connection during the wait. No drain logic, no risk of bounded-channel self-eviction, no need for a "discard mode" flag.
- **Reader goroutine starts BEFORE the wait**, because that's the goroutine that signals hello.
- **The reader's existing termination paths** (bad envelope JSON, bad payload JSON, `ParseUpdatePayload` error, oversize close, plain `conn.Read` error) all already call `cancel()`. Combined with the `<-ctx.Done()` arm of the wait `select`, that gives us "invalid update during requireHello disconnects" for free.
- **`sync.Once`-guarded close**: a chatty v1 client that sends `options_update` more than once must not panic on a double-close. `Once` makes the signal idempotent.
- **`requireHello=false` AND no param**: skip the wait entirely. Subscribe immediately, exactly like today.

### Pseudo-code

```go
ctx, cancel := context.WithCancel(r.Context())
defer cancel()

helloCh := make(chan struct{})
var helloOnce sync.Once
signalHello := func() { helloOnce.Do(func() { close(helloCh) }) }

go readerLoop(ctx, conn, &filterPtr, m, logger, requireHello, signalHello, cancel)

if requireHello {
    select {
    case <-helloCh:
        // proceed
    case <-ctx.Done():
        return
    }
}

subCh, doneCh, unsubscribe := broadcaster.Subscribe()
defer unsubscribe()

// existing writer loop
```

The reader goroutine signature gains `signalHello func()`. The reader calls it once after `filterPtr.Store(newFilter)` succeeds, only on the `options_update` happy path. (`Once` makes it safe to call unconditionally too, but only-on-happy-path is clearer and matches v1's "valid options update releases the wait" rule.)

### Pre-hello ping behavior

v1 doesn't ping during the hello wait — the ping ticker lives inside the post-hello loop. We match exactly: no `ping` until after hello. The TCP connection stays alive as long as the kernel and any intermediate proxy keep it open; that's v1's contract. If a client connects with `requireHello=true` and never sends, the connection sits open until either side drops it. v1 has the same behavior, and operators of v1 deployments accept it.

(If we ever want a hello-wait timeout, that's a separate change with its own metric. Not in this spec.)

### Idle-proxy concern

A possible operational concern is that load balancers / reverse proxies may close idle TCP connections after some seconds-to-minutes window, and v1's no-ping-during-hello design would inherit the same exposure. Since v1 has the same shape and operators have lived with it, we don't add a workaround. We document the behavior in the doc.go V1 PARITY list so future operators see it.

## Testing

Three integration tests through `httptest.NewServer` + `coder/websocket`, plus one micro-test for the `requireHello` query parse rule.

### Integration tests (added to `internal/subscribe/handler_test.go`)

1. **`TestHandler_RequireHello_BlocksUntilOptionsUpdate`**

   - Connect with `?requireHello=true`.
   - Publish a matching event into the broadcaster.
   - Assert the client receives nothing within ~100ms (read with a short deadline; expect `context.DeadlineExceeded`).
   - Send a valid `options_update` JSON frame matching the published event's filter.
   - Publish another matching event.
   - Assert the client receives that second event (and only that one — the pre-hello publish must not be queued and replayed).

2. **`TestHandler_RequireHello_InvalidUpdateDisconnects`**

   - Connect with `?requireHello=true`.
   - Send a malformed JSON text frame (e.g. `{`).
   - Assert the next read returns a websocket close error with `StatusInvalidFramePayloadData` (matching the existing "bad SubscriberSourcedMessage envelope" close).
   - Repeat for a well-formed envelope with a bad payload (`{"type":"options_update","payload":"not-json"}` → `StatusInvalidFramePayloadData`) and for a well-formed payload that fails `ParseUpdatePayload` (e.g. an invalid DID → `StatusPolicyViolation`).
   - These test the "invalid update during requireHello disconnects" rule.

3. **`TestHandler_RequireHello_FalseHasNoEffect`**

   - Subtest: `requireHello=false`. Publish, assert event arrives without sending hello.
   - Subtest: `requireHello=garbage`. Same.
   - Subtest: param absent. Same. (This is "default behavior unchanged".)

   This locks down the "exactly the literal string `true`" parse rule.

4. **`TestHandler_RequireHello_NoLeakOnClientDisconnect`** *(robustness)*

   - Connect with `?requireHello=true`, immediately close the client side without sending hello.
   - Wait briefly (poll-with-deadline) and assert the handler returns and `unsubscribe` was never called against the broadcaster (the broadcaster's subscriber count returns to baseline, OR — if subscriber count isn't observable — assert via metric / log that the handler exited cleanly without a panic).
   - This catches regressions where the hello wait blocks forever on a closed connection.

5. **`TestHandler_RequireHello_MultipleUpdatesDoNotPanic`** *(robustness)*

   - Connect with `?requireHello=true`.
   - Send two valid `options_update` frames back-to-back.
   - Assert no panic, hello fires once, normal event flow proceeds. This locks down the `sync.Once` guard.

### Unit test (added to `handler_test.go` or a new tiny test)

6. **`TestParseRequireHello`** — table-driven:

   | input          | want  |
   |----------------|-------|
   | `"true"`       | true  |
   | `"True"`       | false |
   | `"TRUE"`       | false |
   | `"false"`      | false |
   | `"1"`          | false |
   | `""`           | false |
   | `"yes"`        | false |
   | `"true "`      | false |

   Locks down the "exactly the literal string `true`" rule and prevents accidental "be liberal in what you accept" drift.

### Race coverage

All integration tests run under the existing `go test -race` invocation. The new code involves a `sync.Once` plus a `chan struct{}` close — both intrinsically race-safe — so no additional race-specific test is needed. The existing `-race` job will catch any accidental misuse (e.g., reading from `helloCh` in the writer loop without ordering through the close).

## Metrics

No new metrics. The existing `OptionsUpdates` and `OptionsUpdateErrors{reason}` counters already cover the `options_update` path; a `requireHello` connection's first hello will simply increment `OptionsUpdates` once. Operators can correlate with `subscribe_active` if they need to spot stuck-pre-hello connections.

If we later observe stuck-pre-hello connections in production, we can add a dedicated `subscribe_pre_hello_waiting` gauge in a follow-up. YAGNI for now.

## Documentation

Add `requireHello` to the V1 PARITY block at the top of `internal/subscribe/doc.go`:

> - `?requireHello=true` blocks event delivery until the first valid `options_update`. Implemented by delaying the broadcaster Subscribe call until after hello (matches v1's user-visible contract; the internal queueing differs).

The "Out of scope for this v1-compat surface" list in doc.go currently mentions `requireHello` as future work. Remove it from that list once implemented.

## Implementation file map

- `internal/subscribe/handler.go` — primary changes:
  - Parse `requireHello` from `values` after the existing query parse.
  - Add `helloCh`/`sync.Once`/`signalHello` setup before reader goroutine.
  - Pass `signalHello` and `requireHello` into the reader goroutine; signal on the happy `options_update` path.
  - Move `broadcaster.Subscribe()` and its `defer unsubscribe()` to AFTER the hello wait.
  - Add a `parseRequireHello(values url.Values) bool` helper near the existing `truncateCloseReason` helper.
- `internal/subscribe/handler_test.go` — five new tests (3 happy/contract, 2 robustness, 1 parse-rule unit table).
- `internal/subscribe/doc.go` — V1 PARITY block update.

## Risks & mitigations

| Risk | Mitigation |
|------|------------|
| Connection leak if hello wait blocks forever after client disconnect | The wait already selects on `ctx.Done()`. The reader goroutine's `conn.Read` returns an error when the client closes, which `cancel()`s the context. Tested by `TestHandler_RequireHello_NoLeakOnClientDisconnect`. |
| Double-close panic on chatty client | `sync.Once` guards the channel close. Tested by `TestHandler_RequireHello_MultipleUpdatesDoNotPanic`. |
| Reader goroutine signals hello before `ctx` is set up | `ctx` is set up before the reader goroutine starts; the goroutine receives it as a parameter. Code-review point, not a test. |
| Pre-hello idle proxy timeout closes the connection | Documented behavior matching v1. Operators of v1 deployments accept this. |
| A client sends a bad first frame (binary, unknown type, etc.) and then a valid update — does hello still fire? | Yes: the reader goroutine ignores binary and unknown-type frames (existing behavior), and continues to read. The next valid `options_update` fires hello. Tested implicitly by the chained-update flow in `TestHandler_RequireHello_BlocksUntilOptionsUpdate`. |

## Out of scope (explicit non-list, for clarity)

- `?cursor=` replay — separate v2-native feature.
- `?compress=true` / zstd — separate v2-native feature.
- A timeout on the hello wait — not in v1, not in this spec.
- A `subscribe_pre_hello_waiting` gauge — YAGNI.
- Any change to the broadcaster's bounded-channel semantics — none needed; we delay subscribing instead.
