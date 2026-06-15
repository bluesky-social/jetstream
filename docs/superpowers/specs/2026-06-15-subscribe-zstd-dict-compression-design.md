# subscribe: v1 custom zstd-dictionary compression on `/subscribe`

- **Issue:** #3
- **Date:** 2026-06-15
- **Status:** Approved (brainstorming)

## Summary

Re-enable the Jetstream v1 custom-zstd-dictionary compression scheme on
`/subscribe`, which v2 currently rejects with a 400. A client opts in via
`?compress=true` or a `Socket-Encoding: zstd` request header and receives
websocket **BinaryMessage** frames, each a zstd frame compressed with the
v1 custom dictionary (113 KB, dict ID 1612007021). Non-opted clients are
unaffected: they continue to receive **TextMessage** JSON, with RFC 7692
permessage-deflate negotiated when offered.

This scheme is **not preferred**. Standard RFC 7692 permessage-deflate is
the recommended compression path for new consumers. The custom dictionary
exists solely for backwards compatibility with v1 clients, and code/doc
comments must say so plainly.

## Motivation

v1 supported a custom zstd dictionary trained on the atproto firehose JSON,
giving a better ratio than generic deflate on this small-message, highly
repetitive stream. v1 clients (including those using the v1 client library's
`Compress` option) send `Socket-Encoding: zstd` / `?compress=true` and expect
dictionary-compressed binary frames. v2 today fails these clients loudly with
a 400. Supporting the scheme lets v1 consumers migrate to v2 unchanged.

## Background: the v1 implementation

From `github.com/bluesky-social/jetstream`:

- **Dictionary**: `pkg/models/zstd_dictionary`, embedded via `//go:embed`
  into `models.ZSTDDictionary`. 113 KB, Zstandard dictionary ID 1612007021.
- **Encoder** (`pkg/consumer/consumer.go:76`):
  `zstd.NewWriter(nil, zstd.WithEncoderDict(models.ZSTDDictionary),
  zstd.WithWindowSize(1<<17), zstd.WithEncoderConcurrency(1))`, used via
  `EncodeAll`.
- **Framing** (`pkg/server/server.go:268`): compressed events are sent as
  `websocket.BinaryMessage`; uncompressed events as `websocket.TextMessage`.
- **Opt-in** (`pkg/server/server.go:82`): `Socket-Encoding: zstd` header OR
  `?compress=true`. Fixed for the lifetime of the stream.
- **Client decode** (`pkg/client/client.go:71`):
  `zstd.NewReader(nil, zstd.WithDecoderDicts(models.ZSTDDictionary))`.
- **maxMessageSizeBytes** (`pkg/server/subscriber.go:69`): in v1 the size
  cap is compared against the **compressed** byte length.

## v2 current state

- `internal/subscribe/handler.go:91` rejects `?compress=true` /
  `Socket-Encoding: zstd` with a 400.
- `handler.go:189` always negotiates `CompressionContextTakeover`
  (permessage-deflate) on Accept when the client offers it.
- `handler.go:404` writes every frame as `MessageText`.
- The hot ring `Entry` (`entry.go`) lazily memoizes the JSON encoding once
  per event via `sync.Once`, shared across all caught-up subscribers; there
  are two variants, `Encoded()` (v1 shape) and `EncodedExtended()`.
- `doc.go` "Cursor replay and compression" documents the scheme as NOT
  supported.

## Design

### 1. Dictionary embedding

Copy the v1 dictionary file verbatim into the `subscribe` package as
`internal/subscribe/zstd_dictionary`, embedded via `//go:embed` in a new
small file (e.g. `dictionary.go`):

```go
//go:embed zstd_dictionary
var zstdDictionary []byte
```

Rationale for placing it in `subscribe` rather than a shared `models`
package: v2 has no `models` package, and the dictionary is only consumed by
the subscribe encode path. Keeping it package-local keeps the dependency
graph tight.

### 2. Shared encoder

A package-level `*zstd.Encoder` built once at package init (or via a guarded
`sync.Once` / package var) with exactly v1's configuration:

```go
zstd.NewWriter(nil,
    zstd.WithEncoderDict(zstdDictionary),
    zstd.WithWindowSize(1<<17),
    zstd.WithEncoderConcurrency(1))
```

`EncodeAll(src, nil)` is goroutine-safe on a shared `*zstd.Encoder`, so a
single shared encoder serves all subscribers. `klauspost/compress` is already
a whitelisted dependency and already used in `segment/`.

A failure to construct the encoder (corrupt embedded dictionary) is a
programmer/build error, not runtime input — fail loud at construction
(panic at init or surface an error through `NewHandler`'s existing
panic-on-misconfiguration path).

### 3. Per-Entry memoization

Add two lazily-memoized fields to `Entry`, mirroring the existing JSON pair:

```go
compressedOnce  sync.Once
compressed      []byte
compressedErr   error

compressedExtendedOnce sync.Once
compressedExtended     []byte
compressedExtendedErr  error
```

With accessors `Compressed()` and `CompressedExtended()` that:

1. obtain the memoized JSON body via `Encoded()` / `EncodedExtended()`
   (propagating `errSkipEvent` / encode errors unchanged), then
2. `encoder.EncodeAll(body, nil)` once, caching the result.

This guarantees one JSON encode and one compression per event regardless of
how many zstd subscribers are caught up at the tip. The compressed bytes are
**not** counted in `approxBytes()` for the same reason the JSON encoding is
excluded today (bounded by the same ring; counting would double-count).

### 4. Opt-in resolution and mutual exclusion with deflate

In `serve`, replace the rejection block at `handler.go:91` with opt-in
detection:

```go
wantZstd := values.Get("compress") == "true" ||
    strings.Contains(r.Header.Get("Socket-Encoding"), "zstd")
```

**Mutual exclusion:** if `wantZstd` AND the client's request offers
permessage-deflate (its `Sec-WebSocket-Extensions` header contains
`permessage-deflate`), reject with a 400:

> "choose one compression scheme: custom zstd (compress=true /
> Socket-Encoding: zstd) or RFC 7692 permessage-deflate, not both"

This rejects only when the client *actively offers deflate alongside* the
zstd opt-in. A zstd client that does not offer deflate connects normally.

On `websocket.Accept`, select the compression mode by opt-in:

- `wantZstd` → `CompressionMode: websocket.CompressionDisabled` (we do our
  own framing; deflate must not also run).
- otherwise → `CompressionMode: websocket.CompressionContextTakeover`
  (unchanged from today).

Note: parsing the offered extensions is a header inspection only; we do not
re-implement RFC 7692 negotiation. `coder/websocket` still owns negotiation
for the non-zstd path.

### 5. Subscriber loop framing

`runSubscriberLoop` gains a `compress bool` parameter, resolved once in
`serve` and fixed for the connection lifetime (matching v1). Per delivered
event:

- `compress` → call `Compressed()` / `CompressedExtended()`, write
  `websocket.MessageBinary`.
- otherwise → existing `Encoded()` / `EncodedExtended()` path, write
  `websocket.MessageText`.

The `errSkipEvent` and encode-error handling is identical for both paths
(the compressed accessor surfaces the same sentinels from the underlying
JSON encode).

### 6. maxMessageSizeBytes semantics

The size cap is compared against the **uncompressed** JSON body length, even
for zstd clients — i.e. the check stays where it is today (`handler.go:398`,
against `body`), evaluated before swapping in compressed bytes.

This is a deliberate, documented divergence from v1, which compared the
compressed length. Rationale: the cap exists to bound the logical record
size a client is willing to receive; comparing against unpredictable
compressed size would let a multi-megabyte record slip through a small cap
(and vice versa). A code comment records the divergence.

### 7. Documentation

- Doc comment at the encoder/dictionary: this scheme is for v1 back-compat
  only and is **not preferred**; new consumers should use RFC 7692
  permessage-deflate.
- Doc comment at the handler opt-in / mutual-exclusion block explaining the
  reject-both rule.
- Rewrite `doc.go` "Cursor replay and compression": from "NOT supported /
  rejected with a 400" to "supported-but-discouraged custom zstd dictionary
  scheme", documenting opt-in triggers, binary framing, the dictionary, the
  mutual-exclusion 400, and the recommendation to prefer permessage-deflate.

## Testing (TDD)

Rewrite the existing rejection tests in `handler_test.go`
(`TestHandler_RejectsCompressQueryParam`,
`TestHandler_RejectsCompressQueryParam` header variant) into acceptance
tests. New / changed coverage:

1. **Round-trip via query param** — connect with `?compress=true`, no
   deflate offer; assert frames arrive as binary and decode (with a
   `zstd.NewReader(nil, WithDecoderDicts(dict))`) to byte-identical JSON
   versus the uncompressed path. Cover both v1 and `extended=true` shapes.
2. **Round-trip via header** — same, opting in with `Socket-Encoding: zstd`.
3. **compress=false** — stays on text frames (existing
   `TestHandler_AllowsCompressFalse` retained).
4. **No opt-in, deflate offered** — permessage-deflate negotiated, text
   frames (existing `TestHandler_NegotiatesCompression_*` retained).
5. **Both at once → 400** — `?compress=true` with a permessage-deflate
   `Sec-WebSocket-Extensions` offer returns HTTP 400 with the
   choose-one message.
6. **Entry memoization** — a unit test asserting `Compressed()` runs the
   encode at most once (reuse the existing `Entry` test harness with an
   injected encode fn / call counter) and that it decodes to the same JSON
   as `Encoded()`.

Tests must stay fast (package suite < 1s per AGENTS.md). The dictionary
encode/decode of a handful of small events is microseconds; no concern.

## Out of scope (kaizen / follow-ups)

- **cmd/client decoder** — server-side only this pass; the load-test client
  does not learn to decode zstd. File a follow-up issue if end-to-end
  round-trip validation through `cmd/client` is wanted later.
- No change to the archive/segment compression path; this is purely the live
  `/subscribe` wire encoding.

## Risks

- **Double-compression CPU waste** — avoided by `CompressionDisabled` for
  zstd clients (mutual exclusion enforced at handshake).
- **Shared encoder contention** — `EncodeAll` on a shared encoder with
  `EncoderConcurrency(1)` serializes internally; at v2 fan-out scale the
  encode happens once per event (memoized), not once per subscriber, so
  contention is bounded by event rate, not connection count.
- **Dictionary drift** — the file is copied verbatim and its dict ID
  (1612007021) is pinned in this spec; a golden test decoding a known frame
  guards against accidental replacement.
