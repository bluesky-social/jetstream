# Client protocol

This describes archive negotiation, downloads, live streaming, and recovery. `docs/README.md` §5 defines wire formats; `specs/architecture.md` maps the system. The Go client lives at the module root, with its public API in `client.go`, `event.go`, `options.go`, and `typed.go`. Code comments define implementation details; fix this summary when it disagrees.

Design references: low-numbered "§N" (§2, §5) cite `docs/README.md`;
§11–§14 and §R-numbered rules cite
`specs/notes/2026-06-28-drop-client-tombstones-design.md` (the
backfill/cutover design); the original client design is
`specs/notes/2026-06-18-go-client-design.md`.

## Transports

Jetstream delivers filtered historical and live events through two transports:

- **Sealed history** is downloaded over HTTP/XRPC (`planSnapshot` →
  `getSegment`/`getBlock`), because bulk history wants parallelism, resume,
  and CDN-cacheable immutable artifacts.
- **The live tail** is a websocket (`/xrpc/network.bsky.jetstream.subscribeEvents`,
  proposal-0015 xrpc.v1.json framing), because the tip wants
  push latency.

The client joins these into an ordered, at-least-once stream without gaps. Server-assigned seqs are monotonic 64-bit cursors. Websocket `?cursor=N` replays inclusively; seqs start at 1. Archive cursor 0 requests all history, while websocket replay remains limited by lookback.

## Protocol invariants a client must honor

1. **At-least-once, never exactly-once.** Every boundary (page, reconnect,
   cutover, re-backfill) re-delivers; the client dedups by seq or the
   consumer folds idempotently. The bundled client dedups by seq on the live
   tail and keeps the backfill/live seam in seq order.
2. **Deliver markers, don't fold them.** The stream contains positive
   deletion markers (`#delete`, `#update` commits, `#account`
   `active=false`, `#sync`) rather than silent absences (§2 invariant 4).
   The client library delivers them; *consumers* fold. There is no
   client-side suppression of dead records — a backfill can deliver creates
   for records that are already deleted, followed by their markers.
3. **DID-level markers survive collection filters.** The v2 `collections`
   filter constrains only commit events, so `#account`/`#identity`/`#sync`
   are delivered regardless of it (still gated by `dids`) — they are the
   only purge signal a folding consumer gets (§5, "unconditional events").
   The bundled client's exact matcher (`filter.go`) honors
   this. A consumer that wants a commits-only stream must say so
   explicitly with `kinds=commit`.
4. **Cursors are instance-local.** Switching servers means rewinding a
   margin and re-deduping; seq values do not transfer.

## Phase 1: archive negotiation (planSnapshot)

`network.bsky.jetstream.planSnapshot` (lexicon:
`lexicons/network/bsky/jetstream/planSnapshot.json`; client: `planner.go`;
server: `internal/xrpcapi/plansnapshot.go`) is
a **transport planner only**: it names which immutable artifacts might
contain matching rows. Exact filtering, decoding, and folding stay
client-side.

Request: `kinds` (`commit`, `identity`, `account`, `sync`), `dids` (exact), `collections` (exact NSIDs or `ns.*` namespace
wildcards; a wildcard matching nothing yields an empty plan, not match-all),
`afterSeq` (exclusive lower bound), `beforeSeq` (inclusive upper bound).

Planning remains manifest-only. Real footer collection ids are coarse commit
candidates; `$account`/`$identity`/`$sync` sentinel ids are coarse marker-kind
candidates. Mixed blocks and whole segments may over-send, and the client exact
matcher is authoritative. Omitted kinds preserves marker-safe collection
semantics; explicit `kinds=commit` removes the marker-block baseline.

Response, per page:

- `segments[]` — work units. Each carries `name`, `index`, a 16-hex-char
  xxh3 `checksum` (cache key + integrity pin), `minSeq`/`maxSeq`, and a
  `mode`: `"segment"` (download the whole file via `getSegment`) or
  `"blocks"` (download the listed inclusive block ranges via `getBlock`).
  The server picks the mode by match density — dense matches fetch the whole
  segment, sparse matches fetch only the indexed blocks.
- `sealedTipSeq` — the pagination goal: the sealed-archive tip, capped by
  `beforeSeq`. **Pin it from the first page** and page until done; segments
  sealed mid-sweep carry seqs above the pin and are deliberately NOT chased
  (they're covered by the live tail's cold replay at cutover, §14.1).
- `plannedThroughSeq` — the continuation cursor: highest sealed seq this
  page accounts for (the MaxSeq of the last included unit when truncated,
  else `sealedTipSeq`). Next page: `afterSeq=plannedThroughSeq`. Done
  predicate: `plannedThroughSeq >= sealedTipSeq` — unambiguous even when a
  sparse filter matches zero segments in a sub-range (§12.2).
- `stats` — `segmentsExamined/segmentsMatched/blocksMatched/entries`
  (entries counts against the server's per-page plan limit).

The planner prunes by seq overlap conservatively: a segment/block whose range
*straddles* `afterSeq` is included whole, so the client must still run its
row selector below the floor (see Phase 4).

## Phase 2: download + decode (the sweep)

`replayEngine.sweepSealedArchive` (`client_core.go`) pages the plan and hands
work units to the downloader (`downloader.go`):

- **Parallelism**: `concurrency` decode workers (default `GOMAXPROCS`
  clamped to [4, 32]; override `WithDownloadConcurrency`) decompress and
  CBOR-decode block frames in parallel. Block fetches run on their own
  FIFO pool (`min(2*concurrency, 64)` workers) so RTTs overlap across
  plan entries; whole-segment prefetch is bounded (`prefetchDepth = 2`,
  ~280 MB resident compressed buffers each) so memory stays flat.
- **Ordering**: decode is parallel but **emission is in seq order** — the
  downloader sequences completed units back into plan order before emit.
- **Filtering**: the exact row selector (the client's matcher) runs per row
  before decode surfaces it; the plan's over-approximation (whole blocks)
  is trimmed here.
- **Fast path**: worker transforms construct block-aligned `Batch` or
  `TypedBatch[T]` values directly from the decoded public `Event` slice and
  deliver them in seq order, bypassing the serial per-event batcher (the #142
  throughput path). Typed CBOR decoding therefore stays parallel too.

**Error contract**: per-block download/decode failures stream as in-order
recoverable errors — the good prefix of an entry's blocks is emitted, then
the error, then the next entry continues. Malformed rows are surfaced
alongside the block's valid rows, never silently dropped.

`--backfill-only` (`WithSnapshotOnly`) stops here: a point-in-time snapshot of
the matched *sealed* range, no cutover — rows still in the active
(unsealed) segment are deliberately not included.

## Phase 3: cutover to live

After the sweep consumes the sealed archive (through pinned tip `S`), the
engine cuts over (`runBackfillThenLive`, client_core.go): connect the live
websocket once with `?cursor=max(S, lastProcessedSeq)`.

- The cursor is the **dedup floor**: the server replays inclusively
  (seq >= cursor), the consumer's seq dedup (`ev.Seq <= lastSeq` → drop)
  deduplicates cutover. The server’s cold-replay path serves events between
  the sealed tip and live tip on connect, without a client-side buffer.
- The `max()` matters: on a re-backfill cycle the freshly learned sealed tip
  can be *below* the cursor already delivered (live delivered from the
  unsealed active segment), and cutting at the lower value would regress the
  floor and re-deliver out of order. Cutover is monotonic non-decreasing.
- Seqs are monotonic but not contiguous. If the server restarted uncleanly
  between `S` and the live tip, its registered vacancy is crossed server-side
  and the first delivered live seq can jump forward by up to a block (or more
  after repeated crashes). The client accepts this as ordinary forward
  progress; it neither reconnects nor re-enters backfill solely because values
  are absent.

## Phase 4: the live tail and its failure modes

`liveConsumer` (`live.go`) runs dial → read → decode → dedup
→ emit, reconnecting on error with exponential backoff (250ms → 30s,
progress resets it).

**Wire and framing** (`subscribeURL`, `dialWebsocket`, `livedecode.go`):
the dial offers `xrpc.v1.json` via `Sec-WebSocket-Protocol` and verifies
the echo (RFC 6455 §4.1: a non-offered selection fails the connection; an
empty echo is the lexicon-default fallback — identical framing). Frames
decode through the lexgen-generated `JetstreamSubscribeEvents_Message` union —
the same types the server encodes with. `#info` advisories are logged and
skipped (no seq); unknown envelope/payload `$type`s skip for forward
compat; `{"$type":"error",...}` frames surface typed through the
reconnect path. Params: `cursor` (omitted on a from-tip start —
`WithLiveCursor(0)` means "tip", distinct from an explicit cursor 0
meaning "everything"), repeated `collections`/`dids` (server-side pruning;
the client matcher remains the correctness backstop),
`zstdDictionary=<id>` when compression is on. Read limit: 32 MiB
(`defaultLiveReadLimit`) — v2 frames embed record CBOR.

**Reconnect resume**: after any delivery, reconnects send
`cursor=lastSeq` (re-anchoring at the tip would gap); `seenAny`
disambiguates "from-tip, nothing yet" (keep omitting the cursor) from a
real resume. Under `CursorTime` the seq resume is verified by boot ID and
may be replaced by a witnessed-time resume; see "Cursor modes & failover".

**Too-old cursor (§14)**: a seq cursor below the server's lookback floor
is a pre-upgrade HTTP 400 whose XRPC error envelope names `CursorTooOld`
(declared in the subscribe lexicon). The client matches the structured
error name (`dialWebsocket`; the contract tests pin the names) into the
typed `errLiveCursorTooOld` — **terminal for the connection, not the
stream**:
the engine re-enters the backfill loop from the last durably-processed seq,
sweeps the now-sealed gap, and cuts over again. Cycles are bounded by
`maxRebackfillStalls = 5` *non-advancing* cycles (client_core.go); a resume
cursor that fails to advance is a pathological loop and surfaces as
`ErrFatal`. On a **pure-live** stream (no backfill configured) too-old is
immediately fatal — there is no archive loop to re-enter. Two subtleties
handled at the re-backfill seam: the batcher is flushed before the next
sweep (buffered live rows must not be overtaken by newer archive rows),
and the matcher's seq floor advances to the resume point so the one plan
unit that straddles it doesn't re-emit already-delivered rows.

**v1/v2 cursor namespace**: the server splits seq cursors from v1
unix-microsecond cursors at `CursorSeqMaxThreshold = 1e15`
(`internal/subscribe/cursor.go`). The bundled client's pure-live path mirrors
that split for one reason: a seq cursor is also the initial dedup floor, while a
timestamp is only a server-side seek position and starts with no seq dedup
floor. Once the first timestamp-resumed event arrives, reconnects use its real
seq. Clients must not fabricate cursors near the namespace boundary.

## Cursor modes & failover

A seq only means something on the instance that assigned it. Callers that
want to move between instances choose `WithCursorMode(CursorTime)`:
`Batch.LastCursor` then returns the max `Event.WitnessedAtUS` (the
`witnessedAt` frame field, or the segment's `witnessed_at` on backfill)
instead of the max seq, and `WithFailoverHosts` becomes legal (it is
rejected under `CursorSeq`). Backfill (`planSnapshot`/`getSegment`) stays
seq-only and primary-only; the mode only changes the live tail.

The consumer (`live.go`) tracks which seq namespace `lastSeq` belongs to
(`nsHost`, `nsBoot` from the `Jetstream-Boot-Id` header) and the latest
delivered `lastWitnessed` (seeded from the backfill's max witnessed_at at
cutover):

- **Same host, same boot**: resume by `cursor=lastSeq`, exact, seq-deduped.
- **Boot changed** (restart, or a different instance behind one name): the
  session is closed before reading any frame (`errBootMismatch`, an
  immediate redial with no backoff or error) and the next one resumes at
  `lastWitnessed - WithFailoverRewind` (default 5s). `lastSeq` resets to 0
  because the new namespace's seqs may be lower; the rewind overlap is
  re-delivered (at-least-once, no dedup is possible across namespaces).
- **Dial failure** (transport/5xx, not a classified 400): rotate round-robin
  to the next host, which is always a witnessed-time resume. A read error
  after a connected session retries the same host.
- **No witnessed time** (nothing delivered carried `witnessedAt`, e.g. an
  old server, and the start was a seq): leaving is impossible, so the
  consumer stays on its seq namespace and warns once.
- **CursorTooOld** re-enters backfill only while `lastSeq` is provably in the
  archive's namespace (`archiveNS`, and the 400's boot ID matches or is
  unknown); otherwise it resumes by witnessed time. A foreign seq must never
  drive an archive sweep.
- A time resume that the server clamps (`#info OutdatedCursor`) surfaces as
  the recoverable `ErrCursorClamped`: the caller may have a gap.

Failover by time is approximate: instances witness the same event at
slightly different times, so the rewind must cover the skew between them.

## Compression (dict-zstd)

v2's only compression scheme (permessage-deflate is never negotiated; the
dial deliberately doesn't offer it — see the measured rationale in
`specs/notes/2026-07-09-subscribe-compression-cpu-analysis.md`):

1. **Fetch**: GET `network.bsky.jetstream.getZstdDictionary` (immutable,
   CDN-cacheable; optional `?id=` for a pinned version — unknown ID 404s
   with the current ID in the message).
2. **Negotiate**: parse the dictionary ID from the blob header
   (`internal/zstddict`) and connect with `?zstdDictionary=<id>`. The
   server 400s an unknown/retired ID pre-upgrade rather than ever sending
   undecodable frames.
3. **Decode**: frames arrive as binary websocket messages, one zstd frame
   each whose decompressed bytes are exactly the xrpc.v1.json text frame
   (message, info, and error frames alike), decoded with a
   dictionary-seeded decoder whose max decoded
   size is capped at the connection read limit
   (`WithDecoderMaxMemory`) — `SetReadLimit` bounds only the *compressed*
   bytes, and the library default is 64 GiB, so the cap is the
   decompression-bomb guard. A malformed frame surfaces as a recoverable
   error; the tail continues.
4. **Rotation recovery**: a server retrain+redeploy changes the current
   dictionary ID; the connected client's next reconnect is refused with a
   400 whose envelope names `UnknownZstdDictionary` (drift pinned by
   `TestDialWebsocketMatchesServerDictRejected`). The consumer maps it to
   `errLiveDictRejected` and recovers in-place (`refreshDict`): refetch the
   current dictionary, swap the decoder, reconnect compressed. If the
   refetch fails — or returns the very ID just rejected (mixed-version
   fleet behind a load balancer) — it sheds the opt-in and continues
   uncompressed for the consumer's lifetime.
5. **Degradation is never fatal**: dictionary fetch failure at startup,
   parse failure, or rotation-refetch failure all degrade to an
   uncompressed tail with a logged warning. Streaming continues without compression.

Compression is enabled by default in the Go client and normal CLI subscribe
command. Opt out with `WithZstdCompression(false)` or `--zstd=false`;
`WithZstdCompression(true)` explicitly selects the default.
The raw `loadtest` command remains opt-in with `--zstd` because its default URL
is the legacy `/subscribe` endpoint, which uses a different dictionary.

## Public Go API (module root)

- `Subscribe(host, opts...)` → `*Client`; `Client.Events(ctx)` is an
  `iter.Seq2[*Batch, error]` range-over-func iterator. Recoverable errors
  are yielded with iteration continuing; terminal failures satisfy
  `errors.Is(err, ErrFatal)` and end the stream. A second concurrent
  `Events` call on the same client is rejected deterministically with an
  `ErrFatal` "already running" error.
- `Batch.Events()` + `Batch.LastCursor()` — batches amortize cursor
  persistence: process the batch, persist `LastCursor` once (default batch
  size 64, `WithBatchSize`; live partial batches flush on
  `MaxBatchDelay`, default 20ms).
- Replay window: `WithAfterSeq` (exclusive; `WithAfterSeq(0)` = the
  whole archive) / `WithBeforeSeq` (inclusive; requires
  `WithSnapshotOnly` — a live tail with an upper bound would silently
  drop every later live event). Pure live: `WithLiveCursor` (0 = from
  the current tip; values below `1e15` are saved seqs; values at or above it
  are legacy unix-microsecond timestamp seek positions). Cursor style:
  `WithCursorMode` (`CursorSeq` default, `CursorTime` for portable
  witnessed-time cursors), `WithFailoverHosts`, `WithFailoverRewind`.
- Filters: `WithKinds`, `WithCollections` (exact or `ns.*`), `WithDIDs`.
  Subscribe validates, deduplicates, and canonicalizes all three axes once,
  then forwards the same immutable predicate to every plan page and live
  reconnect. Permanent live `InvalidRequest` responses are fatal, not retried.
- Performance: `WithDownloadConcurrency`, `WithRawRecords` (+`Copied`,
  `+CIDs`) to skip the generic record-map build, `TypedEvents[T]` for the
  typed decode fast path, `WithZstdCompression` for the live tail.
- Archive authentication: `WithAPIKey(rawKey)` sends exactly one
  `Authorization: Bearer <rawKey>` header on `planSnapshot`, `getSegment`,
  and `getBlock`. The raw value is opaque and must not include client-side
  parsing or logging. `getZstdDictionary` and the live WebSocket remain public
  and do not receive this option-owned credential. The bundled CLI exposes the
  same value as `--api-key` or the preferred
  `JETSTREAM_CLIENT_API_KEY` environment source.
- Transports: `WithHTTPClient` (replaces negotiation, public dictionary, bulk
  download, and live WebSocket transports without changing the API-key
  scope), `WithMaxDownloadAttempts`.
- `Client.Stats()` — replay progress (pages, pinned sealed tip, planned
  through, residual gap) for sustained-ingest observability.

## Events

One `Event` struct regardless of origin (archive or live): `Seq`, `DID`,
`TimeUS` (display time; never a cursor), `WitnessedAtUS` (the portable
timestamp cursor; 0 from servers that predate `witnessedAt`), `Kind` (`commit`/`identity`/`account`/`sync`), with the matching
sub-struct populated. Commits carry `Record` (generic map; nil in raw
mode), `RecordCBOR` (segment bytes on backfill; canonical DRISL encoding
reconstructed from JSON on live), and `CID`. `#sync` events are delivered
on backfill and the v2 live tail (v1 never emits them). On the wire, live
events are proposal-0015 message frames dispatched by payload `$type`
(docs/README.md §5.2), with the atproto JSON data-model value in `record`
and `time` as a microsecond-precision datetime the client parses back to
`TimeUS`; the decode is transparent to API consumers.

## Writing a third-party client: the checklist

1. Page `planSnapshot` with pinned `sealedTipSeq`; download by `mode`;
   verify checksums; decode blocks; run your exact filter per row. When the
   archive boundary requires an API key, send one RFC 6750-style
   `Authorization: Bearer <api-key>` header on `planSnapshot`, `getSegment`, and
   `getBlock`; treat the raw key as opaque and require TLS at the public
   boundary. Do not send that archive credential on `getZstdDictionary` or
   `/xrpc/network.bsky.jetstream.subscribeEvents`.
2. Emit in seq order; treat every boundary as at-least-once and dedup by
   seq (or make your consumer idempotent).
3. Deliver DID-level markers to your fold even under collection filters.
4. Cut over at `max(sealedTip, lastProcessed)`; expect inclusive replay.
5. Handle the too-old 400 by re-backfilling from your last seq — bound
   non-advancing cycles.
6. For compression: fetch the dictionary, send its ID, cap decoded size at
   your read limit, and treat a dict-rejected 400 as "refetch and retry
   once, else go uncompressed". Never hard-fail the stream on a
   compression problem.
