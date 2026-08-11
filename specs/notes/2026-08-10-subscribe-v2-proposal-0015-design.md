# /subscribe-v2: adopt XRPC JSON subscription framing (proposal 0015) — design (2026-08-10)

> **Historical design:** the implemented `network.bsky.jetstream.subscribeEvents`
> contract no longer carries the draft's `recordCbor` field. `record` is the
> atproto JSON data-model value; DRISL defines its canonical DAG-CBOR encoding,
> so transmitting both representations was redundant. See the authoritative
> lexicon and `docs/README.md` §5.2 for the current wire.

Issue: bluesky-social/jetstream#318. Prereq landed: atmos v0.3.2
(jcalabro/atmos#6) implements the 0015 subprotocols for both the streaming
client and `xrpcserver.HandleSubscription`; jetstream is pinned to it as of
commit 6083fd7.

## 1. What proposal 0015 requires (draft as of 2026-08-10)

Re-read from source (`bluesky-social/proposals/0015-json-subscriptions`):

- **Versioned subprotocols** negotiated via `Sec-WebSocket-Protocol`:
  `xrpc.v0.cbor` (legacy, unchanged), `xrpc.v1.json` (one self-describing
  JSON object per **text** frame), `xrpc.v1.cbor` (specified, not shipping).
- **v1 framing**: exactly one object per frame, discriminated by `$type`,
  two frame types:
  - `{"$type":"message","payload":{"$type":"<nsid>#<fragment>", ...}}` —
    the payload IS the lexicon message, decodable by standard lex tooling
    with no unwrapping.
  - `{"$type":"error","error":"FutureCursor","message":"..."}` — `error`
    required (bare type name), `message` optional; the stream closes
    immediately after an error frame.
- **Negotiation**: client offers tokens in preference order; server echoes
  its selection; no recognized offer → the connection falls back to the
  lexicon-declared `subprotocol` (a new optional field on `subscription`
  defs), which itself defaults to `xrpc.v0.cbor`. Servers MUST support at
  minimum the lexicon-declared subprotocol.
- **Compression**: permessage-deflate is *recommended*, not mandated.
- Jetstream v2 is the named driving use case; the intent is that a
  JSON-native stream declares `subprotocol: "xrpc.v1.json"` so even
  unnegotiated connections receive JSON.

Consequence for jetstream: we declare `xrpc.v1.json` as the lexicon default
and support **only** that token. Negotiation then degenerates to a header
echo — every connection, negotiated or not, receives identical framing.
There is no per-connection codec switch, ever. (We do not offer
`xrpc.v0.cbor`: jetstream's payloads are JSON-native and a CBOR encoding of
them is a new wire contract nobody asked for; the spec's "must support the
lexicon default" is satisfied by v1.json itself.)

## 2. What we serve today (delta inventory)

Current `/subscribe-v2` wire (authoritative: `internal/subscribe/encoder.go`,
`docs/README.md` §5.2, `specs/client.md`):

- Bare event objects, one per text frame, **no envelope, no `$type`**:
  `{did, time_us, cursor, kind, seq, commit|account|identity|sync}`.
  `cursor` and `seq` carry the same value (v1 wire parity kept `cursor`).
- No in-stream error frames are ever sent. All error signaling is
  pre-upgrade HTTP (400 with substring markers `"cursor too old"` /
  `"unknown zstd dictionary id"`, both duplicated client-side and pinned by
  `internal/client/live_subscribe_contract_test.go`) or websocket close
  codes (`StatusPolicyViolation` from the slow detector, `StatusGoingAway`
  on shutdown).
- No `Sec-WebSocket-Protocol` handling anywhere.
- Compression: dict-zstd only (`?zstdDictionary=<id>` → binary frames, one
  zstd frame per event, shared per-event memoized compression);
  permessage-deflate deliberately never negotiated (#294, measured 2.3x
  server CPU vs shared zstd at 200 subscribers).
- Client→server frames: `SubscriberSourcedMessage` (`options_update`) +
  `?requireHello=true`, served by a reader goroutine
  (`handler.go` `runReader`). This exists on BOTH endpoints today.
  (DECIDED: dropped from v2 — see §13.)
- The bundled Go client (`internal/client/live.go`, `livedecode.go`) has its
  own dialer/decoder (does not use atmos streaming); it substring-matches
  the two 400 markers, dedups on seq, and re-enters backfill on
  cursor-too-old.

`/subscribe` (v1) is wire-frozen and completely untouched by this work.

## 3. Architecture decision: where the 0015 logic lives

Two credible options. This is the biggest fork in the design.

### Option A — keep the bespoke handler, conform to the wire spec (recommended)

`internal/subscribe/handler.go` keeps ownership of upgrade, reader
goroutine, pull loop, slow detector, Tail registry, ping ticker, and
compression. We add:

- **Negotiation** (~30 lines): exact-match intersect of the client's
  `Sec-WebSocket-Protocol` offer with `{"xrpc.v1.json"}`, passed to
  `websocket.AcceptOptions.Subprotocols`. The exact-match intersection
  (not the raw supported set) matters because coder/websocket matches
  EqualFold and echoes the *client's* casing — atmos hit and solved the
  same issue (`xrpcserver/subscription.go:162-183`); we copy the pattern.
  No echo → connection proceeds on the lexicon default, which is the same
  framing. Zero per-connection state.
- **Framing folded into the encoder**: `EncodeV2` output becomes the full
  frame `{"$type":"message","payload":{<event with $type>}}`. Since the
  envelope has no per-subscriber content, the **entire frame** (and its
  zstd-compressed twin) stays memoized once per event in `Entry` — the
  measured fanout economics (compress once, fan out) are preserved
  unchanged.
- **Error frames** (~20 lines): tiny append helpers mirroring atmos's
  `appendV1JSONErrorFrame`, used at the two places the server terminates a
  stream for cause (slow detector, internal read failure), followed by the
  existing close. Ordering is trivial because the per-connection write loop
  is single-goroutine — no writeGate needed.

Cost: jetstream re-implements ~50 lines of spec logic that atmos also has.
Mitigation: contract tests pin our frames byte-compatible with atmos's
parser (`streaming.parseV1JSONEnvelope` semantics), and a cross-stack test
can decode our frames with atmos's generated union types.

### Option B — adopt `xrpcserver.HandleSubscription`

This was the stated intent in the atmos PR ("jetstream as the first
consumer"). Having now read both sides closely, the impedance mismatches
are substantial:

| jetstream needs | xrpcserver today |
|---|---|
| client→server `options_update` frames (moot after decision 4: v2 drops them) | `CloseRead` closes the conn with `StatusPolicyViolation` on ANY client data frame |
| encode-once/compress-once fanout (Entry memoization; measured: the reason zstd beats deflate 2.3x at c=200) | `Stream.Send(Message)` re-encodes envelope+payload per subscriber per event (~600 B alloc+copy × subscribers × event rate) |
| dict-zstd binary frames | no raw/binary send path; framing is hardwired text-JSON / binary-CBOR |
| 30s pings | `Stream` doesn't expose Ping (CloseRead's reader handles pongs, but we originate pings) |
| `OriginPatterns: ["*"]` (public data, browser consumers) | `websocket.Accept` default rejects cross-origin browser requests; not configurable |
| pre-upgrade 400s with specific bodies | `Validate` hook exists and works (writes XRPC error envelope) ✓ |

Making B work means extending atmos with: `Stream.SendRaw` (pre-framed,
possibly pre-compressed bytes, bypassing the $type agreement check),
`Stream.Ping`, an `AcceptOptions`/origin passthrough, and a "keep the read
side open, hand me client messages" mode. That's four API extensions whose
only consumer pierces the abstraction precisely where it adds value —
negotiation and framing — which for a single-subprotocol endpoint is a
header echo and a string constant. The remaining value (terminal-ordering
writeGate) solves a concurrency problem jetstream doesn't have (one writer
goroutine per conn).

**Recommendation: A.** The fanout architecture is the measured crown jewel
of this endpoint; wrapping it in an abstraction we must pierce four ways is
worse engineering than 50 lines of well-tested duplication. atmos
`HandleSubscription` remains the right tool for its intended shape
(server-push lexicon streams with modest fanout) and jetstream still
consumes atmos for the lexicon `subprotocol` field, lexgen types, and — in
tests — as an independent conformance decoder.

If we later want more services on one implementation, the extension list
above is the concrete backlog for atmos.

## 4. Routing

**DECIDED (Jim, 2026-08-10): only `/xrpc/network.bsky.jetstream.subscribe`.
The `/subscribe-v2` path is removed** — v2 has no deployed users, and one
canonical path is what lexicon tooling derives from the published schema.

Today `/xrpc/` routes wholesale to the atmos xrpcserver mux
(`runtime.go`), but Go 1.22 mux precedence lets the more specific
`GET /xrpc/network.bsky.jetstream.subscribe` pattern win, so the bespoke
handler mounts there cleanly while the rest of `/xrpc/` keeps flowing to
xrpcserver.

Every dial site changes in the same series: `internal/client/live.go`
`subscribeURL`, `cmd/client` (loadtest/subscribe), `testing/dicttrain`,
the oracle Part B harness (`dialSubscribeV2` and friends), and all docs.
The `/subscribe` (v1) path is untouched.

## 5. The lexicon

New file `lexicons/network/bsky/jetstream/subscribe.json`. Two candidate
payload shapes; this is the second big decision.

### Filtering model (Jim's meeting notes, 2026-08-10 iteration)

v1's filtering contract is the pain point this lexicon fixes. In v1,
`wantedDids` is clear (applies to every event kind), but
`wantedCollections=app.bsky.feed.like` delivers like commits PLUS all
account/identity events **for every user on the network** — and there is
no way to say "commits only." Consumers can't tell from the parameters
what stream they actually built.

The new model: **three orthogonal filter axes, each an independent
AND-composed predicate**. Unset means match-all on that axis; "one big
stream" is simply no parameters.

1. `kinds` — event types (`commit`, `identity`, `account`, `sync`).
2. `dids` — repo DIDs; applies to all event kinds.
3. `collections` — collection NSIDs / `<prefix>.*` patterns; only commit
   events *have* a collection, so this axis only constrains commits.

Formally:

```
deliver(evt) =
      (kinds unset       OR evt.kind ∈ kinds)
  AND (dids unset        OR evt.did ∈ dids)
  AND (evt.kind ≠ commit OR collections unset
                         OR matches(evt.collection, collections))
```

Note the third clause: `collections` never *drops* a non-commit event —
excluding account/identity/sync is `kinds`' job. This is what makes the
axes orthogonal, and it is deliberately NOT "setting collections implies
commits-only": one parameter silently rewriting another's default is the
kind of coupling that made v1 confusing, and the v1 use case
"collection-scoped consumer still sees account deletions so it can purge
dead accounts' records" (load-bearing per v1's own docs) must stay
expressible — it is now `collections=X` with `kinds` unset, and the
consumer who wants only the commits says `kinds=commit&collections=X`.

Composition examples (the consumer-facing story):

| Parameters | Stream |
|---|---|
| *(none)* | everything — one big stream |
| `kinds=commit` | all commits, no account/identity/sync |
| `collections=app.bsky.feed.like` | all like commits + account/identity/sync for everyone (the v1 shape, now explicit and opt-in) |
| `kinds=commit&collections=app.bsky.feed.like` | only like commits — the v1 pain point, solved |
| `dids=did:plc:X` | every event about X |
| `dids=did:plc:X&kinds=account,identity` | X's account/identity events only |
| `dids=did:plc:X&collections=app.bsky.graph.follow` | X's follows + X's account/identity/sync |

Validation (pre-upgrade HTTP 400 `InvalidRequest`, crash-loud at the API
boundary):

- Unknown `kinds` value → 400. Deterministic and explicit when a newer
  client names a kind this server predates; silently-never-matching
  would be a silent fallback.
- Duplicate values → deduped, capped post-dedupe (v1 forgiveness kept).
- `collections` set while `kinds` is set and excludes `commit` → 400:
  the collections filter would be provably inert, which is a client bug;
  tell the developer immediately rather than serve a stream they
  misunderstand.
- Malformed DID / collection pattern → 400 (as today).

Kept from the earlier draft, unchanged in behavior: a commit with an
empty collection bypasses the collections filter (never silently drop
data the filter can't classify), and `maxMessageSizeBytes` stays as a
size gate — it is not a semantic filter axis.

Naming: **the `wanted` prefix is dropped everywhere** (`dids`,
`collections`) — clean, minimal, simple. The event-type axis is `kinds`,
matching jetstream's historical vocabulary; on the wire the kind is
carried by the payload `$type` fragment, and the `kinds` values are
exactly those fragment names.

Operational note (consequence of dropping `options_update`): filters are
URL-only now. A max-size `dids` set (10,000 DIDs ≈ 420 KB of query
string) fits Go's default 1 MB header cap (the public listener doesn't
lower it) but far exceeds common CDN/proxy URL limits (8–64 KB). For the
Bluesky-hosted instance behind a CDN, big-filter consumers either
connect straight to origin or the practical cap is the CDN's; if a real
>URL-length use case materializes, it needs a new mechanism (e.g. POST a
filter, get a handle) — not subscriber-sourced frames.

### Shape 1 — per-kind message union (recommended)

Lexicon-idiomatic, mirrors `com.atproto.sync.subscribeRepos`: the message
schema is a union of per-kind refs, each message carries the jetstream
envelope fields directly, and consumers dispatch on the payload `$type`
exactly the way every other lexicon stream works. The `kind` discriminator
and the duplicated `cursor` field die (their job is now done by `$type`
and `seq`).

Draft:

```json
{
  "lexicon": 1,
  "id": "network.bsky.jetstream.subscribe",
  "defs": {
    "main": {
      "type": "subscription",
      "description": "Stream every archived and live Jetstream event in seq order, framed per the xrpc.v1.json subprotocol. Server-push only. The kinds, dids, and collections filters are independent predicates ANDed together; each is match-all when omitted. See the parameter and error descriptions for cursor, filtering, and compression semantics.",
      "subprotocol": "xrpc.v1.json",
      "parameters": {
        "type": "params",
        "properties": {
          "cursor": {
            "type": "integer",
            "description": "Resume position, inclusive: the server replays events with seq >= cursor and the client dedups the overlap. Values >= 1e15 are interpreted as a unix-microseconds timestamp instead of a seq (legacy jetstream v1 cursor compatibility) and always clamp to the retention floor. Omitted: start at the live tip."
          },
          "kinds": {
            "type": "array",
            "items": { "type": "string", "enum": ["commit", "identity", "account", "sync"] },
            "maxLength": 4,
            "description": "Event kinds to receive; values are the message $type fragment names. Omitted or empty: all kinds. A value outside the enum is rejected pre-upgrade with HTTP 400 (InvalidRequest) rather than silently never matching."
          },
          "dids": {
            "type": "array",
            "items": { "type": "string", "format": "did" },
            "maxLength": 10000,
            "description": "Repo DIDs to receive events for; applies to every event kind. Omitted or empty: all repos."
          },
          "collections": {
            "type": "array",
            "items": { "type": "string" },
            "maxLength": 100,
            "description": "Collection NSIDs or '<prefix>.*' patterns; constrains which commit events are delivered. Non-commit kinds are unaffected — combine with kinds=commit for a commits-only collection stream. Rejected pre-upgrade with HTTP 400 (InvalidRequest) when kinds is set and excludes commit, since the filter could never apply. Omitted or empty: all collections."
          },
          "maxMessageSizeBytes": {
            "type": "integer",
            "minimum": 0,
            "default": 0,
            "description": "Skip events whose uncompressed frame exceeds this many bytes. 0 (default) means no limit; malformed or negative values are treated as 0."
          },
          "zstdDictionary": {
            "type": "integer",
            "minimum": 1,
            "description": "Jetstream extension: opt into dict-zstd frame compression with the given zstd dictionary ID (obtained via network.bsky.jetstream.getZstdDictionary). An unknown or retired ID is rejected pre-upgrade with HTTP 400 carrying the current ID."
          }
        }
      },
      "message": {
        "schema": {
          "type": "union",
          "refs": ["#commit", "#identity", "#account", "#sync", "#info"]
        }
      },
      "errors": [
        { "name": "ConsumerTooSlow", "description": "The client is far behind the live tip AND reading below the server's floor rate for a sustained window; the server drops adversarially-slow readers. A merely-slow-but-progressing reader is never dropped." },
        { "name": "CursorTooOld", "description": "Rejected pre-upgrade with HTTP 400, never as a stream error frame: the requested seq cursor is below the server's retention floor. The message carries the floor seq; archive-backfilling clients re-enter backfill from their last durable seq." },
        { "name": "UnknownZstdDictionary", "description": "Rejected pre-upgrade with HTTP 400, never as a stream error frame: the zstdDictionary ID is unknown or retired. The message carries the current dictionary ID; re-fetch via network.bsky.jetstream.getZstdDictionary." }
      ]
    },
    "commit": {
      "type": "object",
      "description": "A single record mutation (create, update, or delete).",
      "required": ["seq", "did", "time", "rev", "operation", "collection", "rkey"],
      "properties": {
        "seq": { "type": "integer", "description": "Jetstream's monotonic per-event sequence number; the stream cursor." },
        "did": { "type": "string", "format": "did" },
        "time": { "type": "string", "format": "datetime", "description": "Timestamp of when Jetstream witnessed this event, microsecond precision. Its unix-microseconds value is what a timestamp cursor compares against." },
        "rev": { "type": "string", "format": "tid", "description": "The repo rev of the commit that produced this op." },
        "operation": { "type": "string", "knownValues": ["create", "update", "delete"] },
        "collection": { "type": "string", "format": "nsid", "description": "Collection NSID of the record." },
        "rkey": { "type": "string", "format": "record-key", "description": "Record key." },
        "record": { "type": "unknown", "description": "The record decoded to JSON. Absent for deletes." },
        "cid": { "type": "string", "format": "cid", "description": "CID of the record. Absent for deletes." },
        "recordCbor": { "type": "bytes", "description": "The record's raw DAG-CBOR, byte-exact as archived — suitable for CID verification, MST reconstruction, or typed decoding via lexicon codegen. Absent for deletes." }
      }
    },
    "identity": {
      "type": "object",
      "description": "An identity change (handle or DID document update), wrapping the upstream firehose event verbatim.",
      "required": ["seq", "did", "time", "identity"],
      "properties": {
        "seq": { "type": "integer" },
        "did": { "type": "string", "format": "did" },
        "time": { "type": "string", "format": "datetime", "description": "Timestamp of when Jetstream witnessed this event, microsecond precision." },
        "identity": { "type": "ref", "ref": "com.atproto.sync.subscribeRepos#identity", "description": "The upstream event; its seq is the upstream relay's, not jetstream's." }
      }
    },
    "account": {
      "type": "object",
      "description": "An account status change (active/deactivated/deleted/...), wrapping the upstream firehose event verbatim.",
      "required": ["seq", "did", "time", "account"],
      "properties": {
        "seq": { "type": "integer" },
        "did": { "type": "string", "format": "did" },
        "time": { "type": "string", "format": "datetime", "description": "Timestamp of when Jetstream witnessed this event, microsecond precision." },
        "account": { "type": "ref", "ref": "com.atproto.sync.subscribeRepos#account" }
      }
    },
    "sync": {
      "type": "object",
      "description": "An archived #sync event (broken commit chain; consumers should resync the repo), wrapping the upstream firehose event verbatim. Never emitted on the legacy v1 /subscribe wire.",
      "required": ["seq", "did", "time", "sync"],
      "properties": {
        "seq": { "type": "integer" },
        "did": { "type": "string", "format": "did" },
        "time": { "type": "string", "format": "datetime", "description": "Timestamp of when Jetstream witnessed this event, microsecond precision." },
        "sync": { "type": "ref", "ref": "com.atproto.sync.subscribeRepos#sync" }
      }
    },
    "info": {
      "type": "object",
      "description": "An advisory, non-fatal notice about the stream (mirrors com.atproto.sync.subscribeRepos#info). Carries no seq and does not advance the cursor. OutdatedCursor is sent as the first frame when a unix-microseconds timestamp cursor below the retention floor was clamped up to the floor; the message names the seq actually resumed from.",
      "required": ["name"],
      "properties": {
        "name": { "type": "string", "knownValues": ["OutdatedCursor"] },
        "message": { "type": "string" }
      }
    }
  }
}
```

Example wire frames:

```json
{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribe#commit","seq":42,"did":"did:plc:abc","time":"2024-08-10T15:06:40.000000Z","rev":"3kx2...","operation":"create","collection":"app.bsky.feed.post","rkey":"3kx2...","record":{...},"cid":"bafy...","recordCbor":{"$bytes":"omdw..."}}}
{"$type":"error","error":"ConsumerTooSlow","message":"reader below floor rate for 30s at 2.1M seqs behind tip"}
```

### Corpus/style audit (2026-08-10)

Audited the draft against the atproto lexicon spec
(https://atproto.com/specs/lexicon) and the full bluesky-social/atproto
lexicon corpus (~/go/src/github.com/bluesky-social/atproto/lexicons),
with special attention to the three existing `subscription` lexicons
(subscribeRepos, subscribeLabels, chat subscribeModEvents). Fixes
applied to the draft above; deviations that remain are deliberate and
listed with rationale.

Fixed to match corpus/spec:

- **`kinds` items: `enum`, not `knownValues`.** The spec is explicit:
  knownValues is "suggested or common values… not limited to this set";
  enum is "a closed set of allowed values". We 400 on unrecognized
  kinds — that IS a closed set, and declaring knownValues would promise
  openness the server doesn't honor. Corpus params use enum for exactly
  this (ozone sortDirection et al.). `#commit.operation` stays
  `knownValues` — matching `subscribeRepos#repoOp.action`, which uses
  knownValues for create/update/delete (a reader-side field where new
  operations may appear; consumers must tolerate unknowns).
- **String formats added** where the corpus always declares them:
  `rev` → `format: "tid"` (listRepos precedent), `collection` →
  `format: "nsid"`, `rkey` → `format: "record-key"` (applyWrites
  precedent). Our ingest gate (`internal/ingest/live/events.go`
  `validateOpPath`/`validateOp`) already drops ops whose
  collection/rkey/rev fail exactly these spec validators, so the wire
  can honestly declare them. Note the `collections` *filter param*
  items stay plain `string` deliberately — `<prefix>.*` patterns are
  not valid NSIDs (ozone's `collections` params use format nsid but
  don't support prefixes; ours does, and the description says so).
- **`#info` def added** to the message union. Every corpus subscription
  reserves the advisory info channel (`subscribeRepos#info`,
  `subscribeLabels#info`: `{name, message}`, name knownValues
  ["OutdatedCursor"]). Unions are open-by-default so we could bolt it on
  later without a breaking change, but declaring it now costs nothing,
  and it surfaces a real gap: **jetstream v2 currently clamps silently**
  in two cases (timestamp cursor below the floor → floor; future cursor →
  live tip). subscribeRepos emits `#info OutdatedCursor` for precisely
  the below-floor clamp. Recommendation (small server change, very
  house-style — replaces a silent fallback with an observable signal):
  emit `#info {name:"OutdatedCursor", message:"requested timestamp
  cursor below retention floor; starting at seq N"}` as the first frame
  on a clamped timestamp resume. Info frames carry no seq and do not
  advance the cursor; atmos's v1-frame decoder already models
  seq-less info messages. The future-cursor→tip clamp stays silent and
  documented (it is the defined semantics of "start at tip", not a
  degradation). Marked as decision 6 below (pending Jim's ack); the
  draft above includes `#info` with OutdatedCursor accordingly.
- **`CursorTooOld` / `UnknownZstdDictionary` declared in `errors`**,
  next to ConsumerTooSlow, with descriptions stating they are
  pre-upgrade HTTP 400s and never stream error frames. Deliberate
  deviation, flagged: in the corpus, a subscription's `errors` are
  stream-error-frame names (FutureCursor, ConsumerTooSlow) — HTTP-level
  errors are what query/procedure lexicons declare. But these two ARE
  this endpoint's contract, the names appear in the XRPC error envelope
  body, and leaving them undeclared would hide the endpoint's most
  load-bearing failure mode from the schema. The descriptions carry the
  distinction. (atmos lexgen note: subscription error constants aren't
  generated today — genErrorConstants runs only for query/procedure —
  so this is documentation-only until lexgen learns otherwise.)

Also fixed to match corpus/spec (Jim, 2026-08-10 follow-up):

- **`timeUs` → `time` (`string`, `format: "datetime"`).** The corpus has
  NO epoch-integer timestamp precedent — every subscription/record
  timestamp is a datetime string (subscribeRepos `time`); we adopt the
  same name and type. Precision is preserved: RFC 3339 fractional
  seconds carry the full microsecond value (the encoder emits exactly
  six fractional digits from `WitnessedAt`/`DisplayTimeUS()`, e.g.
  `2024-08-10T15:06:40.123456Z`), so `time` ↔ unix-µs round-trips
  losslessly and the timestamp-cursor mode (`cursor >= 1e15` ⇒ unix-µs)
  still has an exact wire-visible counterpart — the field descriptions
  say so. Cost: consumers doing cursor math parse a datetime once per
  event (the bundled client converts at the decode boundary and keeps
  exposing `TimeUS int64` in the public Go API; internal storage stays
  unix-µs throughout). A wrapped upstream event's own `time` remains the
  upstream relay's broadcast timestamp — distinct from jetstream's
  witnessed time on the envelope, same as today.
- **`main.description` trimmed to corpus length** (three sentences:
  what the stream is, server-push-only, filter composition). The
  wire-contract detail it carried (cursor semantics, floor rejection,
  compression scheme) lives on in the parameter and error descriptions
  — which the corpus also does (subscribeRepos puts its detail on
  fields, not the main def) — and in docs/README.md §5.

Deviations reviewed and kept, with rationale:

- **`required` on commit fields + `format: "nsid"` collection vs the
  empty-collection filter bypass.** The v1-parity filter rule "a commit
  with an empty collection bypasses the collections filter" stays in
  the *filter* (defensive, never drop unclassifiable data), but the
  lexicon declares `collection` required and NSID-valid. Not a
  contradiction: the ingest gate guarantees archived commits carry
  spec-valid collections, so the declared schema is truthful for
  everything the server can emit; the filter rule is defense-in-depth,
  unreachable on spec-clean data.
- **`maxLength` on params arrays** (kinds 4, collections 100, dids
  10000): corpus-consistent (getProfiles `actors` maxLength 25; ozone
  `collections` maxLength 20). Our caps are bigger but that's server
  policy, not style.
- **No `default` on filter params**: omitted-means-match-all is
  documented in each description rather than encoded as a default value
  (an empty-array default is meaningless in query params). Corpus
  filter params (listNotifications `reasons`, ozone `collections`) do
  the same.

Non-findings (checked, already conformant): def naming (`#commit` /
`#identity` / `#account` / `#sync` / `#info` — byte-identical to
subscribeRepos's def names); camelCase field names; `record` as
`type: "unknown"` (getRecord `value` precedent; the spec's
don't-use-unknown advice applies to `record` type objects, not
subscription messages); `bytes` for recordCbor (subscribeRepos
`blocks`); message schema is a union of local refs (spec requirement);
params use only boolean/integer/string/array (spec restriction —
worth remembering that params can never grow an object, so any future
structured filter needs a procedure or encoded string); errors array
shape; `did` params with `format: "did"` items (chat subscribeModEvents
`recipients` precedent); `dids` as a bare plural param name (ozone
getAssignments/findCorrelation precedent).

Re-verified after all fixes: the updated draft (enum kinds, formats,
#info, three errors) generates with atmos lexgen v0.3.2 and compiles
(/tmp/lexgen-experiment).

Notes on the draft:

- **camelCase** (`recordCbor`) and corpus field names (`time`) per
  lexicon convention. The old snake_case names (`time_us`,
  `record_cbor`) die with the old wire — v2 has no deployed users, and a
  new NSID should not carry v1's naming debt.
- **`bytes` fields serialize as `{"$bytes":"<base64>"}`** (atproto
  data-model JSON). That is what "lex parser compatible" means and what
  atmos-generated types emit/consume. It is one nesting level uglier for
  casual consumers than today's bare base64; the alternative (declare them
  `string`) would lie to the schema. Recommend `bytes`.
- **identity/account/sync wrap the upstream event as a cross-NSID `ref`**,
  which is exact information parity with today's wire (we already embed
  the decoded `comatproto` structs verbatim, upstream seq and all).
  `#commit` is flattened because it is jetstream-native with no upstream
  analog.

**Experimentally verified (2026-08-10, atmos v0.3.2, /tmp/lexgen-experiment):**
the draft lexicon above generates, compiles, and round-trips. Findings:

- Cross-NSID refs work when (a) `com/atproto/sync/subscribeRepos.json` is
  vendored into `lexicons/` and (b) `lexgen.json` maps prefix
  `com.atproto` → package `comatproto`, import
  `github.com/jcalabro/atmos/api/comatproto`. Generated fields then type
  as `comatproto.SyncSubscribeRepos_Account` etc. — exactly the structs
  the encoder already decodes CBOR into today.
- **Wrinkle**: lexgen has no import-only package mode — the `com.atproto`
  mapping also *regenerates* comatproto into its `outDir`. Options:
  point `outDir` at a scratch dir the `just lexgen` recipe deletes
  (works today, mildly gross), or add an `"importOnly": true` package
  flag to atmos lexgen (small, clean; recommended — second atmos PR).
- Generated: `JetstreamSubscribe_Message` union with
  `AppendJSON`/`UnmarshalJSON` (stamps/dispatches full
  `network.bsky.jetstream.subscribe#<kind>` `$type`s), per-kind structs
  with `AppendJSON`/`AppendCBOR`/`UnmarshalJSON`/`UnmarshalCBOR`, and
  `const JetstreamSubscribe_Subprotocol = "xrpc.v1.json"`.
- `bytes` fields emit atproto data-model JSON: `{"$bytes":"<base64-raw-
  std-encoding>"}` (no padding — note today's wire uses padded StdEncoding;
  client decode changes alongside).
- Unknown payload `$type` decodes into the union's `Unknown` variant
  (raw bytes + type preserved) — forward-compat skip policy implementable
  without error.
- Generated seq fields are `int64` (lexicon `integer`); jetstream uses
  `uint64` internally. Fine for centuries; cast at the encoder boundary.
- Actual frame produced by the generated encoder:

```json
{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribe#commit","cid":"bafyreib...","collection":"app.bsky.feed.post","did":"did:plc:abc123","operation":"create","record":{"$type":"app.bsky.feed.post","text":"hi"},"recordCbor":{"$bytes":"oWR0ZXh0"},"rev":"3kx2aaa","rkey":"3kx2bbb","seq":42,"time":"2024-08-10T15:06:40.123456Z"}}
```
- The `time` datetime round-trips µs-exactly through the generated
  types, and atmos `ParseDatetime` parses via `time.RFC3339Nano` — a
  six-fractional-digit string is fully supported on both sides. The
  server encoder formats `WitnessedAt` with
  `time.Format("2006-01-02T15:04:05.000000Z")` (always six digits,
  always UTC — one canonical rendering, byte-stable for goldens and the
  zstd dictionary).
- `errors` lists only true in-stream errors. `CursorTooOld` is pre-upgrade
  HTTP and `FutureCursor` never happens (we clamp to tip, documented in
  the cursor param). If the draft proposal later wants future-cursor to be
  an in-stream error we can revisit; clamping is the v1-compatible
  behavior and is load-bearing for `WithLiveCursor` semantics.
- `maxMessageSizeBytes` now measures the **whole uncompressed frame**
  (envelope included) rather than the bare event JSON — that's what the
  client actually has to buffer. ~35-byte behavioral delta, worth the
  honesty; document in the param description (done above).

### Shape 2 — single `#event` message (minimal delta, not recommended)

Wrap today's `v2Event` as-is: one `#event` def with `kind` discriminator
and `commit|account|identity|sync` sub-objects, snake_case field names
preserved. Wire delta shrinks to "add envelope + $type", client decode
barely changes. But it imports v1's naming debt into a brand-new NSID,
keeps the redundant `kind`/`cursor`/`seq` triplet, and squanders the
stated point of #318 — standard lexicon tooling dispatching on typed
messages. Only worth it if we valued wire stability for a wire with zero
users.

## 6. Compression × subprotocol (the issue's open question)

Decision: **keep dict-zstd as an orthogonal, connection-scoped transport
layer, negotiated exactly as today** (`?zstdDictionary=<id>`, pre-upgrade
400 on unknown ID), composing with 0015 as:

> a compressed connection carries binary websocket frames; each binary
> frame is one zstd frame whose decompressed bytes are exactly the
> `xrpc.v1.json` text frame (message OR error) that would otherwise have
> been sent.

Rationale:

- The proposal only *recommends* permessage-deflate; it mandates nothing.
  Our #294 measurements are decisive: per-connection deflate is 2.3x the
  server CPU of shared dict-zstd at c=200 and scales linearly with
  subscribers. We keep never-negotiating deflate on v2.
- Subprotocol tokens are the wrong negotiation channel for this: the
  dictionary **ID** must travel anyway (a bare `jetstream.zstd` token
  can't carry it), the token namespace is standardized by the proposal and
  polluting it with vendor tokens invites collision, and query-param
  opt-in already has exactly the right failure mode (pre-upgrade 400
  carrying the current ID → client refetches the dictionary — the
  never-send-undecodable-frames invariant).
- Spec-purity is preserved for anyone who doesn't ask for it: a client
  that negotiates `xrpc.v1.json` and does NOT pass `zstdDictionary` sees
  only conformant text frames. Compression is documented as a jetstream
  extension in the lexicon description (done above).
- Error frames on compressed connections are compressed like any frame —
  uniform "every frame on a zstd conn is one zstd frame" is the simplest
  client contract (the bundled client already switches on websocket
  message type).

Operational follow-up: **retrain the v2 dictionary after the wire change**
(`just train-subscribe-dict`) — the envelope + `$type` strings + `$bytes`
nesting shift the byte distribution. The old dictionary still round-trips
correctly (zstd dictionaries don't constrain content), so this is a ratio
optimization, not a correctness gate; the ID rotation flow (client
refetch-on-400 / refresh-on-unknown-frame) already exists and is tested.

## 7. Server changes (Option A shape)

`internal/subscribe`:

- `encoder.go`: `EncodeV2` renamed/reshaped to emit the full v1.json frame
  per the new lexicon (envelope + `$type`-stamped payload). Two
  sub-decisions: build JSON via the atmos-generated
  `Subscribe_Message`-union `AppendJSON` (zero drift between lexicon and
  wire, `$bytes` handled for free) vs. keep hand-rolled structs (must
  hand-maintain `$bytes` etc.). Recommend generated types — this is the
  coherence payoff of publishing a lexicon; benchmark against the current
  `json.Marshal` path before committing (encoder is hot: once per event,
  amortized across subscribers).
- `handler.go`: subprotocol offer intersection → `AcceptOptions.
  Subprotocols` (exact-match, atmos-style); metrics label for
  negotiated-vs-defaulted; error-frame emission at the slow-detector drop
  and internal-failure paths, before the existing closes. Pings and the
  Tail registry: unchanged. **The v2 read side becomes server-push only**:
  `runReader`, `options_update`, and `requireHello` are v1-only paths;
  on v2 the reader is replaced with `conn.CloseRead`-style handling — any
  client data frame closes the connection (`StatusPolicyViolation`),
  matching atmos xrpcserver's contract. `?requireHello` / large-filter
  installs die on v2 (v1 unchanged). If a >URL-length filter use case
  materializes later it needs a new design (e.g. a POST-a-filter-handle
  XRPC), not a revival of subscriber-sourced frames.
- `cursor.go` / pre-upgrade errors: replace the bare-text 400 bodies with
  the standard XRPC JSON error envelope (`{"error":"CursorTooOld",
  "message":"...floor=N"}`, `{"error":"UnknownZstdDictionary","message":
  "...current=M"}`). The client stops substring-matching markers and
  matches structured error names — strictly more robust; the
  cross-package contract tests move from pinning marker literals to
  pinning error names.
- `entry.go`: unchanged mechanically — the memoized `EncodedV2`/
  `CompressedV2` bodies simply become enveloped frames.
- Frame-size accounting: `maxMessageSizeBytes` measured on the enveloped
  frame.
- `filter.go`: the v1 `Filter` (wantedDids/wantedCollections,
  identity-account-always-bypass) is frozen for `/subscribe`. v2 gets its
  own filter type implementing the three-axis predicate from §5
  (`kinds` set → new; `dids`/`collections` semantics mostly shared with
  v1 mechanically, but the *composition* differs: no unconditional
  account/identity bypass — `kinds` owns kind selection). Parse paths
  split too: v2 parses `kinds`/`dids`/`collections` and 400s on unknown
  kind values and on the inert-collections combination; v1's `ParseQuery`
  keeps `wantedDids`/`wantedCollections` untouched. The shared
  machinery (NSID/prefix matching, DID set, dedupe+cap) stays common
  code; only the predicate and parser differ. The old params are NOT
  accepted as aliases on v2 — one spelling per endpoint. And explicitly
  NOT silently ignored either: `wantedDids`/`wantedCollections` on v2 is
  a 400 with a message naming the replacement (`use dids= / collections=`).
  A migrating v1 consumer whose filter param were ignored would receive
  the full firehose without noticing — the classic silent-fallback trap;
  a named 400 is the crash-loud boundary the house style wants. Other
  unknown params stay ignored per XRPC convention (only the two legacy
  names get the tombstone treatment).

`internal/jetstreamd/runtime.go`: the v2 registration moves from
`GET /subscribe-v2` to `GET /xrpc/network.bsky.jetstream.subscribe`
(more-specific pattern wins over the `/xrpc/` xrpcserver catch-all in the
Go 1.22 mux). `/subscribe-v2` is removed.

## 8. Client changes

`internal/client`:

- `live.go` `subscribeURL`: path changes to
  `/xrpc/network.bsky.jetstream.subscribe`; query params renamed
  `wantedCollections`→`collections`, `wantedDids`→`dids`. The client's
  local matcher (`engine.go` `wantsLive`, the correctness backstop that
  re-checks server-side pruning) picks up the same three-axis semantics
  so client- and server-side predicates can't drift. Public options: if
  we expose kind filtering (natural now that the wire has it), add
  `WithKinds(...)` beside `WithCollections`/`WithDIDs`; note the archive
  fold path must then apply the same kind predicate so backfill and live
  tail deliver the same stream.
- `live.go` `dialWebsocket`: offer `Subprotocols:
  []string{"xrpc.v1.json"}`; after dial, verify the echo is either empty
  (lexicon-default fallback — same framing) or exactly the offered token
  (RFC 6455 §4.1: a non-offered selection fails the connection;
  non-retryable). 400-body handling switches from substring markers to
  decoding the XRPC error envelope and matching `error` names.
- `livedecode.go`: decode the envelope first (`$type` message/error), then
  dispatch the payload on its `$type` — recommend the atmos-generated
  union's `UnmarshalJSON` (same types the server encodes with; the
  hand-rolled `liveFrame` structs retire). Unknown envelope `$type` and
  unknown payload `$type`: skip-and-continue (forward compat, same policy
  as today's unknown `kind`). Error frames map to the existing
  reconnect/terminal classification (`ConsumerTooSlow` → reconnect with
  backoff, as the close today does). `#info` frames: log at info level
  and continue — no seq, no cursor advance, never delivered as events
  (OutdatedCursor on a clamped timestamp resume is operator-relevant,
  not data).
- zstd path: unchanged except the decompressed bytes now parse as
  envelopes.
- Public `jetstream.Event` (module root): field mapping updates
  (`time` datetime parsed back to unix-µs at the decode boundary,
  `$bytes`-wrapped `recordCbor`, upstream-wrapped
  identity/account/sync). The public Go struct API can stay
  shape-compatible (Seq/DID/TimeUS/Kind + sub-structs) — only the wire
  and the decode path change; `typed.go`'s `RecordCBOR` fast path is
  unaffected once the bytes are unwrapped.

## 9. Docs and specs

- `docs/README.md` §5.1/§5.2: rewrite the v2 wire contract (framing,
  negotiation, structured pre-upgrade errors, error frames, compression
  composition). §5.2's "strict superset of v1" framing dies — v2 is now
  its own lexicon-defined contract.
- `specs/client.md`: wire params, cutover, error handling, compression
  sections.
- `internal/subscribe/doc.go` + `internal/client/doc.go`: new contract.
- Publish/upstream the lexicon: `lexicons/` is the in-repo home; whether
  it also lands in bluesky-social/atproto's lexicon tree is a launch-time
  question outside this change (the NSID is ours: network.bsky.jetstream).

## 10. Test plan

- **Encoder goldens**: byte-exact frames per kind (envelope, `$type`,
  `$bytes`), delete-omits-record, error-frame shape. Fuzz `EncodeV2`
  (existing target reshapes).
- **Cross-stack conformance**: decode server-encoded frames with atmos's
  generated union `UnmarshalJSON` (and, where practical, atmos
  `streaming`'s envelope parser semantics) — pins us to the ecosystem
  parser, not our own mirror.
- **Handler e2e** (existing `handler_test.go` patterns): subprotocol
  offer/echo matrix (offered → echoed; not-offered → no echo, same
  framing; junk tokens → no echo; case-variant token → no echo), error
  frame then close from the slow-detector path, structured 400 bodies.
- **Filter semantics**: table-driven truth-table over the three-axis
  predicate — every row of the §5 composition table plus the edge rows
  (kinds excludes commit ⇒ collections combination 400s; unknown kind
  400s; legacy `wanted*` params 400 with replacement message;
  empty-collection commit bypasses collections; dids applies to every
  kind; no-params = match-all). Property test: for any event and any
  filter, `deliver(evt)` equals the conjunction of the three
  independently-evaluated axis predicates — pins orthogonality itself,
  not just examples. v1 `Filter` tests unchanged (frozen contract).
- **Contract tests** (`live_subscribe_contract_test.go`): repoint from
  marker literals to error names + envelope decode.
- **Client**: decode fixtures (message/error/unknown-$type/missing
  payload), dial echo verification (bogus echo → terminal), zstd round
  trip over enveloped frames, cursor-too-old re-backfill loop unchanged.
- **Oracle**: `partb_harness_test.go` raw `dialSubscribeV2` probe learns
  the envelope; `client_observer_test.go` flows through the public client
  automatically. Run `just test-long ./internal/oracle` (cutover seams
  touched).
- **Fuzz**: `FuzzDecodeLiveFrame` reshapes to envelope input; keep the
  no-crash + no-misattribution properties.
- **Bench**: `just bench` the encoder path before/after (generated
  AppendJSON vs json.Marshal), and spot-check frame size delta
  (~+75 B/event uncompressed; expect dict retrain to claw back most of it
  on the wire).

## 11. Implementation milestones

Ordered so each lands green on its own; M1–M2 are prep, M3 is the wire
flip (server+client+tests in one series since there are no deployed
users to stage for).

- **M0 (atmos, optional but preferred)**: lexgen `importOnly` package
  flag so `com.atproto` refs resolve without regenerating comatproto
  into jetstream's tree. Fallback: scratch outDir deleted by `just
  lexgen`.
- **M1 — lexicon + codegen**: `lexicons/network/bsky/jetstream/
  subscribe.json` (per §5, minus optionsUpdate/requireHello), vendor
  `com/atproto/sync/subscribeRepos.json`, update `lexgen.json`, run
  `just lexgen`, commit generated `api/jetstream/jetstreamsubscribe.go`.
  No behavior change.
- **M2 — encoder**: reshape `EncodeV2` to emit v1.json frames via the
  generated union's `AppendJSON`; error-frame append helper; goldens +
  fuzz reshaped; bench vs old `json.Marshal` path (hot path — compare
  before committing to generated encoding); cross-stack decode test
  (atmos union parses our frames).
- **M3 — handler + routing + filters + client**: subprotocol
  negotiation, server-push-only read side (v2 reader →
  close-on-data-frame), error frames at slow-detector/internal-failure,
  structured 400 envelopes, route move to `/xrpc/<nsid>`; the v2
  three-axis filter (`kinds`/`dids`/`collections` parser + predicate,
  legacy-param tombstone 400s, truth-table + orthogonality property
  tests); client dial/decode rewrite (subprotocol offer + echo
  verification, envelope decode via generated union, error-name
  matching, renamed params, matcher updated to three-axis semantics);
  contract tests repointed; loadtest/dicttrain/oracle-harness dial
  sites; `maxMessageSizeBytes` measured on the enveloped frame.
- **M4 — docs + polish**: docs/README.md §5, specs/client.md, both
  doc.go files; metrics for negotiated-vs-defaulted; `just test-long
  ./internal/oracle`; retrain v2 dictionary (`just
  train-subscribe-dict`) and rotate before launch.

## 12. Rollout

No deployed v2 users (issue states this; the endpoint pre-dates launch).
Single-release cutover, no legacy unnegotiated mode, no dual-format
window. The bundled client and server change in the same commit series;
`cmd/client` (loadtest) and `testing/dicttrain` update alongside. Retrain
+ rotate the zstd dictionary as a follow-up before launch.

## 13. Decisions (Jim, 2026-08-10)

1. **Architecture**: Option A — bespoke handler conforms to the wire
   spec. atmos HandleSubscription not adopted (extension list in §3
   remains the atmos backlog if ever wanted).
2. **Payload shape**: per-kind union (Shape 1), camelCase, `$bytes`,
   upstream events wrapped as cross-NSID refs.
3. **Routing**: only `/xrpc/network.bsky.jetstream.subscribe`;
   `/subscribe-v2` removed. All dial sites (bundled client, cmd/client,
   dicttrain, oracle harness, docs) update in the same series.
4. **Subscriber-sourced messages**: dropped from v2 — server-push only,
   any client data frame closes the connection. `options_update` /
   `requireHello` stay v1-only. The `#optionsUpdate` def is gone from the
   lexicon; `requireHello` is gone from its params.
5. **Filtering model** (meeting notes, iterated 2026-08-10): three
   orthogonal AND-composed axes — `kinds` (event types), `dids` (all
   kinds), `collections` (commits only, never drops other kinds). No
   parameters = one big stream. `wanted` prefix dropped from all param
   names. v1's implicit "collections ⇒ plus everyone's account/identity
   events" coupling is replaced by explicit composition
   (`kinds=commit&collections=X` for commits-only). Legacy `wanted*`
   params on v2 are named 400s, not silently ignored. `/subscribe` (v1)
   filtering is frozen unchanged.
6. **#info channel** (proposed by the corpus audit, pending Jim's ack):
   reserve the standard subscription `#info` def in the union and emit
   `OutdatedCursor` as the first frame when a below-floor *timestamp*
   cursor is clamped to the floor — replaces today's silent clamp with
   the same signal subscribeRepos uses. Seq-cursor-below-floor stays a
   pre-upgrade 400 (CursorTooOld), and future-cursor→tip stays a silent
   documented clamp. Info frames carry no seq; kind/DID/collection
   filters do not apply to them. The `kinds` enum does NOT include
   "info" — it selects data kinds, and filtering away advisories would
   defeat their purpose.

Decisions taken in this doc unless objected: only `xrpc.v1.json`
supported (no v0.cbor offer); dict-zstd stays query-param-negotiated and
orthogonal; permessage-deflate stays never-negotiated; future cursors
keep clamping (no FutureCursor error); pre-upgrade 400s move to
structured XRPC error envelopes; `bytes` fields use `$bytes`; dictionary
retrain post-change; atmos follow-up PR for lexgen `importOnly` package
mode (or tolerate a scratch outDir in `just lexgen` until then).
