# Subscribe — v1-compatible filtering

## Problem

`internal/subscribe` currently exposes `GET /subscribe` as a websocket
that fans every live event out to every connected client. Jetstream v1
exposes the same endpoint with three filtering knobs — `wantedCollections`,
`wantedDids`, `maxMessageSizeBytes` — and a small subscriber-sourced-message
machinery (`options_update`) that lets a connected client change those
filters mid-stream. v1 documents this as the public wire contract; existing
clients depend on it.

v2's `serve` ignores the query string entirely and the reader goroutine
drains client→server frames without parsing them. A client that connects to
v2 with `wantedCollections=app.bsky.feed.post` receives every collection's
events, indistinguishable from a buggy v1 instance. We need to port the v1
filtering contract over so existing v1 clients work unchanged against v2.

## Goals

- A v2 `/subscribe` that honors the v1 query parameters
  `wantedCollections`, `wantedDids`, and `maxMessageSizeBytes` exactly per
  the [v1 README](https://github.com/bluesky-social/jetstream/blob/main/README.md).
- v1-compatible `options_update` over the websocket so clients can change
  filters mid-stream.
- Strict v1 wire-contract parity: clients that work against v1 should work
  against v2 without code changes for the in-scope feature surface.
- Cohesive packaging: filter logic in one new file
  (`internal/subscribe/filter.go`) with thorough unit + fuzz coverage.
- Integration tests through the real websocket handler that pin down the
  v1-contract subtleties (identity/account bypass `wantedCollections`,
  oversize coercion of `maxMessageSizeBytes`, etc.).

## Non-goals

- `cursor` replay. v2 doesn't have a replay path wired yet; this spec
  silently ignores the query param to avoid breaking v1 clients that send
  it. Revisit when replay lands.
- `compress=true` / `Socket-Encoding: zstd`. The zstd dictionary and
  compression machinery are their own change; clients negotiate down to
  plain JSON.
- `requireHello=true`. Acknowledged as a v1 surface; we accept the query
  param value but always proceed without waiting on a hello message. v1
  clients that set it get a live tail immediately rather than blocking.
- New v2-native endpoints / non-legacy wire formats. Future v2 endpoints
  will sit alongside `/subscribe` in this package or a sibling; that's a
  separate spec.
- Changing the broadcaster's fan-out semantics. The broadcaster stays a
  protocol-agnostic pub/sub.
- Per-IP rate limiting. v1 has it (`rate.Limiter`); v2 doesn't yet, and
  this isn't the change to add it.

## Architecture

One new file plus modifications to the existing handler.

```
internal/subscribe/
├── broadcaster.go           (unchanged — generic fan-out)
├── config.go                (unchanged)
├── doc.go                   (rewritten — drops "filtering deferred"
│                              disclaimer, adds v1-compat paragraph)
├── encoder.go               (unchanged)
├── errors.go                (extended — adds ErrInvalidOptions)
├── filter.go                (NEW)
├── filter_test.go           (NEW)
├── filter_fuzz_test.go      (NEW)
├── handler.go               (modified — parse query on connect,
│                              per-conn atomic.Pointer[Filter], reader
│                              parses SubscriberSourcedMessage,
│                              writer loop filters + size-checks)
├── handler_test.go          (extended)
├── metrics.go               (extended — three new counters + one labeled)
└── metrics_test.go          (extended)
```

### Why one file in `internal/subscribe`

We considered a `internal/subscribe/legacy/` subpackage to mark the v1
surface as separable from future v2-native endpoints. We rejected this for
the scope of this change: handler.go and encoder.go are already v1-specific
in everything but name (the encoder docstring literally says "Jetstream v1
JSON wire format"); a one-file `legacy` package would either be arbitrary
or grow to the right shape via a much larger move. When v2-native
endpoints land they'll sit alongside the current files in this package or
in a sibling — a future packaging decision informed by what those
endpoints actually look like.

### `filter.go` — types and behavior

**`type Filter struct`** — value type, exported. Fields are unexported;
behavior reads/writes through methods so the zero-value semantics are
under the package's control. Internal shape mirrors v1:

```go
type Filter struct {
    wantedDIDs           map[string]struct{} // nil = match-all
    wantedCollections    *wantedCollections   // nil = match-all
    maxMessageSizeBytes  uint32               // 0 = no cap
}

type wantedCollections struct {
    fullPaths map[string]struct{}
    prefixes  []string // each ends in "." (e.g. "app.bsky.graph.")
}
```

**`func ParseQuery(q url.Values) (*Filter, error)`** — turns a `url.Values`
into a validated `*Filter` or an error wrapping `ErrInvalidOptions`. Empty
input yields a match-all filter. Caps: ≤100 collection patterns, ≤10 000
DIDs (v1 parity).

**`func ParseUpdatePayload(p UpdatePayload) (*Filter, error)`** — same
validation, applied to the JSON struct from `options_update`.

**`(*Filter).Wants(evt *segment.Event) bool`** — the predicate.

- `wantedDIDs`: applies to all event kinds. If non-empty and `evt.DID` not
  in the set, drop.
- `wantedCollections`: applies *only* to commit events
  (`KindCreate`/`KindUpdate`/`KindDelete`). Identity and account events
  bypass it. (`KindSync` is rejected upstream by `Encode` returning
  `errSkipEvent`, so `Wants` doesn't need to special-case it. We special-
  case it defensively anyway by treating non-commit events as not subject
  to collection filtering.)
- Prefix matching: a query value ending in `.*` is stored with the trailing
  `*` stripped. Match is `strings.HasPrefix(collection, prefix)`. Full
  paths are checked first via map lookup.

**`(*Filter).MaxMessageSizeBytes() uint32`** — accessor used by the
handler post-encode to drop oversize frames. Lives outside `Wants` because
size enforcement applies to encoded byte length, which `Wants` doesn't
have access to.

### Wire types

Public types in `filter.go`:

```go
type SubscriberSourcedMessage struct {
    Type    string          `json:"type"`
    Payload json.RawMessage `json:"payload"`
}

type UpdatePayload struct {
    WantedCollections   []string `json:"wantedCollections"`
    WantedDIDs          []string `json:"wantedDids"`
    MaxMessageSizeBytes int      `json:"maxMessageSizeBytes"`
}

const SubMessageTypeOptionsUpdate = "options_update"

const (
    MaxWantedCollections      = 100
    MaxWantedDIDs             = 10_000
    MaxSubscriberMessageBytes = 10_000_000 // 10 MB, v1 parity (decimal, not MiB)
)
```

`json.RawMessage` for `Payload` matches v1 — lets the dispatch layer pick
the type before decoding the inner payload, so an unknown `type` doesn't
fail the outer parse.

### Errors

`errors.go` adds:

```go
var ErrInvalidOptions = errors.New("subscribe: invalid options")
```

`ParseQuery` and `ParseUpdatePayload` return wrapped errors via
`fmt.Errorf("%w: <detail>", ErrInvalidOptions)`. Detail strings are terse
but informative (`"invalid collection: app.bsky.foo*"`,
`"too many wanted DIDs: 12345 > 10000"`) so the HTTP 400 body and the
websocket close reason carry useful diagnostics.

### `maxMessageSizeBytes` parsing — v1 parity (deliberate)

This is the one place v2's house style ("silent fallbacks are often a
mistake; crashing is preferred over data corruption" — `CLAUDE.md`)
intentionally yields to v1 wire-contract compatibility.

v1's `ParseMaxMessageSizeBytes` silently coerces empty, malformed, or
negative values to 0. The v1 README documents this as the contract:

> `maxMessageSizeBytes` — The maximum size of a payload that this client
> would like to receive. Zero means no limit, negative values are treated
> as zero. (Default "0" or empty = no maximum size)

Existing v1 clients send `"0"`, `""`, and (occasionally) garbage and rely
on this exact coercion. Strict-on-parse v2 behavior would silently break
such clients with HTTP 400.

We match v1 exactly. Two helpers in `filter.go`:

```go
// parseMaxMsgSizeQuery: empty/malformed/negative -> 0. V1 PARITY.
func parseMaxMsgSizeQuery(s string) uint32

// clampMaxMsgSize: negative int -> 0. V1 PARITY.
func clampMaxMsgSize(n int) uint32
```

The doc comment on each helper cites the v1 README contract verbatim, so
`git blame` makes the rationale durable. `TestParseMaxMsgSize_V1Compat`
locks the behavior table down:

| Input                       | `parseMaxMsgSizeQuery` | `clampMaxMsgSize` |
|---                          |---                     |---                |
| `""` / not provided         | 0                      | n/a               |
| `"0"` / `0`                 | 0                      | 0                 |
| `"-1"` / `-1`               | 0                      | 0                 |
| `"abc"`                     | 0                      | n/a               |
| `"1000000"` / `1000000`     | 1000000                | 1000000           |
| `"99999999999"` (overflow)  | 0                      | n/a               |

This file documents the v1-compat behavior, the helper docstrings
duplicate it, `internal/subscribe/doc.go` has a short paragraph
referencing it, and the test pins it down — four redundant surfaces on
purpose. Future readers tempted to "fix" the silent-coerce behavior have
to overcome four signals before they regress the contract.

### Handler changes

`internal/subscribe/handler.go` gains three responsibilities. State
ownership stays per-connection; nothing about the broadcaster changes.

**1. Parse-on-connect.** Before `websocket.Accept`, call
`ParseQuery(r.URL.Query())`. On error: `http.Error(w, err.Error(),
http.StatusBadRequest)` and return — same as v1, error reaches the client
*before* the upgrade so curl/websocat see an HTTP 400 with a useful body.

**2. Per-connection filter state.** The connection holds an
`atomic.Pointer[Filter]`. Initial parse populates it; the reader goroutine
swaps it on a valid `options_update`; the writer loop loads it on each
event.

`atomic.Pointer` (not `sync.RWMutex`) for two reasons:

- Hot path is one load per delivered event. `Pointer.Load()` is wait-free;
  `RWMutex.RLock` is not.
- `*Filter` is treated as immutable once published. Updates allocate a
  fresh `*Filter` and Store it. No partial-update visibility.

**3. SubscriberSourcedMessage handling.** The reader goroutine — currently
"drain and discard" — becomes a parser:

```go
go func() {
    defer cancel()
    for {
        msgType, payload, rerr := conn.Read(ctx)
        if rerr != nil {
            return
        }
        if msgType != websocket.MessageText {
            continue // ignore binary; v1 does the same
        }
        if len(payload) > MaxSubscriberMessageBytes {
            terminate(conn, "subscriber message too large")
            return
        }
        var msg SubscriberSourcedMessage
        if err := json.Unmarshal(payload, &msg); err != nil {
            terminate(conn, "bad subscriber message envelope")
            return
        }
        switch msg.Type {
        case SubMessageTypeOptionsUpdate:
            var update UpdatePayload
            if err := json.Unmarshal(msg.Payload, &update); err != nil {
                terminate(conn, "bad options_update payload")
                return
            }
            newFilter, err := ParseUpdatePayload(update)
            if err != nil {
                terminate(conn, err.Error())
                return
            }
            filterPtr.Store(newFilter)
        default:
            // v1 logs a warning and ignores unknown types. Match that.
            logger.Warn("unknown subscriber message type", "type", msg.Type)
        }
    }
}()
```

Where `terminate` sends a websocket close frame with the given reason and
cancels the connection context. This kills the writer loop too via
`<-ctx.Done()`.

**4. Writer-loop modification.** The existing event branch becomes:

```go
case evt := <-subCh:
    f := filterPtr.Load()
    if !f.Wants(evt) {
        m.incEventsFiltered()
        continue
    }
    body, eerr := Encode(evt)
    if errors.Is(eerr, errSkipEvent) {
        m.incEventsSkippedSync()
        continue
    }
    if eerr != nil {
        // ... existing error handling ...
        continue
    }
    if max := f.MaxMessageSizeBytes(); max > 0 && uint32(len(body)) > max {
        m.incEventsOversize()
        continue
    }
    // ... existing write ...
```

Filter check happens *before* `Encode` — `Encode` is the expensive step
(CBOR→JSON), no point doing it for filtered-out events. Max-size check
happens *after* `Encode` because it's the encoded size that matters.

### Filter location: handler, not broadcaster

Considered: have the broadcaster reach into a per-subscriber predicate and
skip channel-send for non-matches. Rejected: turns the single-publisher
hot path into O(subs × filter-cost) work and adds RWMutex contention to
each Publish. With the current 16 384-slot per-subscriber buffer and
~500 evt/s steady state, a narrow subscriber that filters out most events
still has 30+ seconds of headroom — the cost of "send to channel and let
handler discard" is real but well within budget. v1 does filter inside the
emitter under a per-subscriber lock; v2's broadcaster is shaped
differently (single-publisher snapshot fan-out) and benefits from staying
that way.

### Mid-stream filter races

When `options_update` arrives while the writer loop has already received
an event from `subCh`, the writer loop loads the new filter pointer at
the top of the next iteration. Up to one stale-filter event can slip
through after the Store. This is not a defect — v1 has the same property
under its `sub.lk` mutex, and the alternative (queue-drain barrier) adds
complexity for a property no client cares about. Documented in the
doc comment on the writer loop.

## Wire contract — v1-compat behaviors documented

These four points are all parts of the v1 contract that diverge from
"what we'd write from scratch." Each is locked down by tests and called
out in code comments.

1. **`maxMessageSizeBytes` silent coercion** — empty/malformed/negative
   becomes 0 (no cap). Documented in §"`maxMessageSizeBytes` parsing"
   above. Tests: `TestParseMaxMsgSize_V1Compat`,
   `TestHandler_Filter_MaxMessageSize_DropsOversize`.
2. **Identity and account events bypass `wantedCollections`** — they
   always pass the collection filter regardless of subscriber preference,
   per v1 README: "Regardless of desired collections, all subscribers
   receive Account and Identity events." They DO still respect
   `wantedDids`. Tests: `TestWants_KindRules`,
   `TestHandler_Filter_IdentityBypassesCollectionFilter`,
   `TestHandler_Filter_IdentityRespectsDIDFilter`.
3. **`Sync` events deliberately not emitted** — pre-existing behavior
   from `encoder.go`; v1 didn't emit them either. Test:
   `TestHandler_SyncEventNotEmitted` (existing).
4. **Unknown `SubscriberSourcedMessage.Type` silently ignored** — v1
   logs a warning and keeps the connection open. Test:
   `TestHandler_OptionsUpdate_UnknownTypeIgnored`.

## Test plan

Three layers, each in its own file. Total target runtime under 1s for
`just test` (the `-short` everyday loop), per AGENTS.md.

### Unit tests — `filter_test.go`

Pure-function tests. No I/O, no goroutines.

- `TestWantsCollection` — table-driven: full-path match, `app.bsky.*`
  prefix, `app.bsky.graph.*` prefix, multi-prefix subscriber, no-match,
  nil filter (match-all), empty-string collection.
- `TestWantsDID` — table-driven: in-set, not-in-set, nil filter
  (match-all), empty-string DID edge case.
- `TestWants_KindRules` — locks down the v1-contract subtleties:
  - Identity event with `wantedCollections` set → delivered.
  - Account event with `wantedCollections` set → delivered.
  - Identity event with `wantedDIDs` set, DID not in set → dropped.
  - Account event with `wantedDIDs` set, DID in set → delivered.
  - Commit event with both filters set, only collection matches → dropped.
- `TestParseQuery_HappyPaths` — empty (match-all), single collection,
  multiple collections, prefix collection, multiple DIDs, all combined.
- `TestParseQuery_Errors` — every error path returns
  `ErrInvalidOptions`-wrapped: bad NSID, bad DID, prefix not at NSID
  boundary (e.g. `app.bsky.fo*`), >100 collections, >10 000 DIDs.
- `TestParseUpdatePayload_HappyAndErrors` — same pattern applied to the
  JSON struct.
- `TestParseMaxMsgSize_V1Compat` — the table from
  §"`maxMessageSizeBytes` parsing" above. The regression-prevention test
  for the v1-parity decision; doc comment cites the v1 README contract.
- `TestFilter_Wants_MaxMsgSizeAccessor` — Wants does NOT enforce
  `maxMessageSizeBytes`; the accessor reports it correctly. Pins down
  the design choice that size enforcement lives in the handler, not the
  predicate.

### Fuzz tests — `filter_fuzz_test.go`

Runs in `just test-long` / CI extended runs, not the everyday loop.
AGENTS.md emphasizes fuzz for untrusted input — the query string and the
websocket payload are exactly that.

- `FuzzParseQuery` — corpus seeded with realistic inputs (a couple of
  valid + a couple of invalid). Body: build `url.Values` from random
  `[]byte` (split on a separator), call `ParseQuery`. Invariants: never
  panic; if `err == nil`, `Wants` never panics on a constructed event
  with random DID and collection.
- `FuzzParseUpdatePayload` — fuzz on a JSON payload directly. Same
  invariants.

### Integration tests — `handler_test.go` (extends existing file)

Through the real websocket handler via `httptest.NewServer`. Each test
follows the existing `newSteadyStateStore` pattern.

- `TestHandler_Filter_RejectsInvalidQuery` — connect with
  `wantedCollections=app.bsky.foo*` (illegal prefix); assert HTTP 400
  *before* websocket upgrade, body contains "invalid collection".
- `TestHandler_Filter_TooManyDIDs` — assert HTTP 400.
- `TestHandler_Filter_TooManyCollections` — assert HTTP 400.
- `TestHandler_Filter_WantedCollections_DeliversMatching` — connect with
  `wantedCollections=app.bsky.feed.post`; publish a like and a post;
  assert only the post arrives.
- `TestHandler_Filter_WantedCollections_PrefixMatch` — connect with
  `wantedCollections=app.bsky.graph.*`; publish a follow and a post;
  assert only the follow arrives.
- `TestHandler_Filter_WantedDIDs_DeliversMatching` — assert per-DID
  filtering.
- `TestHandler_Filter_IdentityBypassesCollectionFilter` — connect with
  `wantedCollections=app.bsky.feed.post`; publish identity; assert
  identity arrives. Locks down the v1-contract subtlety.
- `TestHandler_Filter_IdentityRespectsDIDFilter` — connect with
  `wantedDids=did:plc:want`; publish identity for `did:plc:other`;
  assert nothing arrives within timeout.
- `TestHandler_Filter_MaxMessageSize_DropsOversize` — connect with
  `maxMessageSizeBytes=100`; publish a large commit; assert nothing
  arrives. Then publish a small commit; assert it arrives. Sub-test for
  `maxMessageSizeBytes=-1` and `maxMessageSizeBytes=` (empty), both
  producing match-all behavior — the v1-coercion regression guard.
- `TestHandler_OptionsUpdate_ChangesFilter` — connect with no filter;
  publish post → assert delivery; send `options_update` narrowing to
  `app.bsky.feed.like`; publish post + like → assert only like arrives.
- `TestHandler_OptionsUpdate_InvalidPayloadDisconnects` — send malformed
  JSON; assert websocket closes with the reason. Send a valid envelope
  with bad NSID; assert close.
- `TestHandler_OptionsUpdate_OversizePayload` — send a payload larger than
  `MaxSubscriberMessageBytes` (10 MB, v1 parity); assert disconnect.
- `TestHandler_OptionsUpdate_UnknownTypeIgnored` — send
  `{"type":"unknown","payload":null}`; assert connection stays open and
  subsequent events still flow.
- `TestHandler_NoFilter_AllEventsDelivered` — sanity test that the
  existing happy path still works.

**Out of scope.** No new swarm test (per the brainstorming choice); the
existing `broadcaster_swarm_test.go` already exercises the
concurrency-sensitive surface of the broadcaster, and filter state lives
entirely behind a per-connection `atomic.Pointer` so it doesn't add new
concurrency surface to swarm-test.

## Metrics

Three new counters in `metrics.go`, namespaced consistently with existing
`subscribe_*` counters.

- `subscribe_events_filtered_total` — incremented when `Wants` returns
  false. Counts the savings from filtering. No labels.
- `subscribe_events_oversize_total` — incremented when
  `MaxMessageSizeBytes` drops a frame post-encode. Indicates clients are
  receiving smaller-than-they-want streams.
- `subscribe_options_updates_total` — incremented on each successful
  `options_update`. Useful signal for "are clients actually using the
  dynamic filter API."

One label-bearing counter:

- `subscribe_options_update_errors_total{reason}` — incremented on each
  rejected client message. `reason` is one low-cardinality label:
  `oversize`, `bad_envelope_json`, `bad_payload_json`, `invalid_options`.
  Cardinality ≤4.

`metrics_test.go` extends with one assertion per new counter that the
increment helper increments the underlying Prom counter.

## Implementation order

1. `filter.go` + `filter_test.go` — pure logic, unit tests, the
   `TestParseMaxMsgSize_V1Compat` table. No handler changes yet. Green
   checkpoint.
2. `filter_fuzz_test.go` — corpus + invariants. Run briefly locally to
   confirm no panics. Green checkpoint.
3. Handler integration: parse-on-connect, `atomic.Pointer`, writer-loop
   filter check, max-size check post-encode. Existing tests stay green.
   New integration tests for the connect-time path land here. Green
   checkpoint.
4. `options_update` reader: parse `SubscriberSourcedMessage`, dispatch,
   `Filter.Store` on success, terminate on failure. Integration tests
   for the dynamic path land here. Green checkpoint.
5. Metrics: new counters + wiring + extend `metrics_test.go`. Green
   checkpoint.
6. `doc.go` rewrite: drop the "filtering deferred" disclaimer; add the
   v1-compat paragraph documenting the silent-coercion, identity-bypass,
   sync-skip, and unknown-type-ignored behaviors with rationale.

## Risks

- **`coder/websocket` close-with-reason API**: differs from
  `gorilla/websocket`. If the library doesn't support sending a custom
  close reason, fall back to `Close(StatusPolicyViolation, "")` and put
  the reason in a logged warn line. Either way the v1-compatible
  close-status code is what matters for clients reacting programmatically.
  Surfaces in implementation step 4.

- **NSID validator availability**: the v1 server uses
  `bluesky-social/indigo`'s `syntax.ParseNSID`. v2's allow-list of deps
  does not include indigo (uses `jcalabro/atmos`). If `atmos` doesn't
  expose an equivalent, we add a 20-line local validator covering the
  v1 parser's actual rules (RFC-light NSID validation: dot-separated,
  lowercase, valid character set per atproto spec). Surfaces in
  implementation step 1; if local, lives next to `filter.go` with its
  own table-driven test.

- **DID validator availability**: same shape as NSID. Same fallback.

## Open questions

None. Ambiguities resolved during brainstorming:

- Filter location: handler, not broadcaster. (§"Filter location" above.)
- v1 parity vs strict-on-parse for `maxMessageSizeBytes`: parity wins.
  (§"`maxMessageSizeBytes` parsing" above.)
- Identity/account collection-filter bypass: yes, match v1.
  (§"Wire contract" above.)
- Unknown message type: silently ignore, log warn. (§"Wire contract"
  above.)
- Packaging: single `filter.go` in `internal/subscribe`, no `legacy/`
  subpackage. (§"Why one file" above.)
