# The client protocol (for agents)

How a Jetstream client — the bundled Go client or any third-party
implementation — negotiates the archive, downloads history, tails live, and
survives the failure modes in between. This is the client-side counterpart to
`docs/README.md` §5 (which owns the wire formats) and `specs/architecture.md`
(the system map). The bundled Go client lives in `internal/client` with its
public API at the module root; `internal/client`'s code comments are
authoritative for implementation details — when this file disagrees with
them, fix this file.

Design references: low-numbered "§N" (§2, §5) cite `docs/README.md`;
§11–§14 and §R-numbered rules cite
`specs/notes/2026-06-28-drop-client-tombstones-design.md` (the
backfill/cutover design); the original client design is
`specs/notes/2026-06-18-go-client-design.md`.

## The shape of the problem

A consumer wants some slice of the network — "all likes since 2024", "these
five DIDs", or everything — delivered once, in order, and then kept current
forever. Jetstream splits that into two transports:

- **Sealed history** is downloaded over HTTP/XRPC (`planBackfill` →
  `getSegment`/`getBlock`), because bulk history wants parallelism, resume,
  and CDN-cacheable immutable artifacts.
- **The live tail** is a websocket (`/subscribe-v2`), because the tip wants
  push latency.

The client's job is to make the seam between the two invisible: one stream,
in seq order at the seam, at-least-once, with no gap. The server-assigned
**seq** (a monotonic 64-bit cursor; `?cursor=N` replays inclusively, seqs
are 1-based, 0 means "everything") is the coordinate system for everything
below.

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
3. **DID-level markers bypass collection filters.** `#account`/`#identity`
   (and `#sync` on v2) are delivered regardless of `wantedCollections`
   (still gated by `wantedDids`) — they are the only purge signal a folding
   consumer gets (§5, "unconditional events"). The bundled client's exact
   matcher (`internal/client/filter.go`) honors this.
4. **Cursors are instance-local.** Switching servers means rewinding a
   margin and re-deduping; seq values do not transfer.

## Phase 1: archive negotiation (planBackfill)

`network.bsky.jetstream.planBackfill` (lexicon:
`lexicons/network/bsky/jetstream/planBackfill.json`; client:
`internal/client/planner.go`; server: `internal/xrpcapi/planbackfill.go`) is
a **transport planner only**: it names which immutable artifacts might
contain matching rows. Exact filtering, decoding, and folding stay
client-side.

Request: `dids` (exact), `collections` (exact NSIDs or `ns.*` namespace
wildcards; a wildcard matching nothing yields an empty plan, not match-all),
`afterSeq` (exclusive lower bound), `beforeSeq` (inclusive upper bound).

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

The planner prunes by seq overlap one-sidedly: a segment/block whose range
*straddles* `afterSeq` is included whole, so the client must still run its
row selector below the floor (see the re-backfill subtlety in Phase 4).

## Phase 2: download + decode (the sweep)

`Engine.sweepSealedArchive` (`internal/client/engine.go:444`) pages the plan
and hands work units to the `Downloader` (`internal/client/downloader.go`):

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
- **Fast path**: `BackfillSink.Transform` (engine.go:197) moves per-event
  conversion onto the decode workers and delivers block-sized payloads via
  `Emit` in seq order, bypassing the per-event batcher (the #142
  throughput path). `Run` without a sink uses the legacy batcher.

**Error contract**: per-block download/decode failures stream as in-order
recoverable errors — the good prefix of an entry's blocks is emitted, then
the error, then the next entry continues. Malformed rows are surfaced
alongside the block's valid rows, never silently dropped.

`--backfill-only` (`WithBackfillOnly`) stops here: a point-in-time dump of
the matched *sealed* range, no cutover — rows still in the active
(unsealed) segment are deliberately not included.

## Phase 3: cutover to live

After the sweep consumes the sealed archive (through pinned tip `S`), the
engine cuts over (`runBackfillThenLive`, engine.go): connect
`/subscribe-v2` once with `?cursor=max(S, lastProcessedSeq)`.

- The cursor is the **dedup floor**: the server replays inclusively
  (seq >= cursor), the consumer's seq dedup (`ev.Seq <= lastSeq` → drop)
  turns that into effectively-once at the seam. No client-side buffer —
  "jetstream is your buffer" (§ 2.1): events between the sealed tip and the
  live tip are served by the server's cold-replay path on connect.
- The `max()` matters: on a re-backfill cycle the freshly learned sealed tip
  can be *below* the cursor already delivered (live delivered from the
  unsealed active segment), and cutting at the lower value would regress the
  floor and re-deliver out of order. Cutover is monotonic non-decreasing.

## Phase 4: the live tail and its failure modes

`liveConsumer` (`internal/client/live.go`) runs dial → read → decode → dedup
→ emit, reconnecting on error with exponential backoff (250ms → 30s,
progress resets it).

**Wire params** (`subscribeURL`): `cursor` (omitted on a from-tip start —
`WithLiveCursor(0)` means "tip", distinct from an explicit cursor 0 meaning
"everything"), repeated `wantedCollections`/`wantedDids` (server-side
pruning; the client matcher remains the correctness backstop),
`zstdDictionary=<id>` when compression is on. Read limit: 32 MiB
(`defaultLiveReadLimit`) — v2 frames carry base64 record CBOR.

**Reconnect resume**: after any delivery, reconnects send
`cursor=lastSeq` (re-anchoring at the tip would gap); `seenAny`
disambiguates "from-tip, nothing yet" (keep omitting the cursor) from a
real resume.

**Too-old cursor (§14)**: a seq cursor below the server's lookback floor is
a pre-upgrade HTTP 400 whose body carries `CursorTooOldMarker`
(`internal/subscribe/cursor.go:114`). The client substring-matches it
(`cursorTooOldMarker`, duplicated because the client can't import the
server package; drift is pinned by a contract test) into the typed
`errLiveCursorTooOld` — **terminal for the connection, not the stream**:
the engine re-enters the backfill loop from the last durably-processed seq,
sweeps the now-sealed gap, and cuts over again. Cycles are bounded by
`maxRebackfillStalls = 5` *non-advancing* cycles (engine.go:24); a resume
cursor that fails to advance is a pathological loop and surfaces as
`ErrFatal`. On a **pure-live** stream (no backfill configured) too-old is
immediately fatal — there is no archive loop to re-enter. Two subtleties
handled at the re-backfill seam: the batcher is flushed before the next
sweep (buffered live rows must not be overtaken by newer archive rows),
and the matcher's seq floor advances to the resume point so the one plan
unit that straddles it doesn't re-emit already-delivered rows.

**v1/v2 cursor namespace**: the server splits seq cursors from v1
unix-microsecond cursors at `CursorSeqMaxThreshold = 1e15`
(`internal/subscribe/cursor.go`) — a client never needs to disambiguate,
but must not fabricate cursors near that boundary.

## Compression (dict-zstd on /subscribe-v2)

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
3. **Decode**: event frames arrive as BINARY websocket messages, one zstd
   frame each, decoded with a dictionary-seeded decoder whose max decoded
   size is capped at the connection read limit
   (`WithDecoderMaxMemory`) — `SetReadLimit` bounds only the *compressed*
   bytes, and the library default is 64 GiB, so the cap is the
   decompression-bomb guard. A malformed frame surfaces as a recoverable
   error; the tail continues.
4. **Rotation recovery**: a server retrain+redeploy changes the current
   dictionary ID; the connected client's next reconnect is refused with a
   400 carrying `subscribe.ZstdDictRejectedMarker` (client duplicate:
   `zstdDictRejectedMarker`, drift pinned by
   `TestDialWebsocketMatchesServerDictRejected`). The consumer maps it to
   `errLiveDictRejected` and recovers in-place (`refreshDict`): refetch the
   current dictionary, swap the decoder, reconnect compressed. If the
   refetch fails — or returns the very ID just rejected (mixed-version
   fleet behind a load balancer) — it sheds the opt-in and continues
   uncompressed for the consumer's lifetime.
5. **Degradation is never fatal**: dictionary fetch failure at startup,
   parse failure, or rotation-refetch failure all degrade to an
   uncompressed tail with a logged warning. Compression is an optimization;
   the tail must keep flowing.

Opt-in: `WithZstdCompression()` (Go client), `--zstd` (CLI subscribe and
loadtest; loadtest is v2-URL-only since the v1 endpoint uses the legacy
embedded dictionary).

## Public Go API (module root)

- `Subscribe(host, opts...)` → `*Client`; `Client.Events(ctx)` is an
  `iter.Seq2[*Batch, error]` range-over-func iterator. Recoverable errors
  are yielded with iteration continuing; terminal failures satisfy
  `errors.Is(err, ErrFatal)` and end the stream. Not safe for concurrent
  `Events` calls on one client.
- `Batch.Events()` + `Batch.LastCursor()` — batches amortize cursor
  persistence: process the batch, persist `LastCursor` once (default batch
  size 64, `WithBatchSize`; live partial batches flush on
  `MaxBatchDelay`, default 20ms).
- Backfill window: `WithAfterSeq` (exclusive; `WithAfterSeq(0)` = the
  whole archive) / `WithBeforeSeq` (inclusive; requires
  `WithBackfillOnly` — a live tail with an upper bound would silently
  drop every later live event). Pure live: `WithLiveCursor` (0 = from
  the current tip).
- Filters: `WithCollections` (exact or `ns.*`), `WithDIDs`.
- Performance: `WithDownloadConcurrency`, `WithRawRecords` (+`Copied`,
  `+CIDs`) to skip the generic record-map build, `TypedEvents[T]` for the
  typed decode fast path, `WithZstdCompression` for the live tail.
- Plumbing: `WithHTTPClient` (replaces both the negotiation and bulk
  download transports), `WithMaxDownloadAttempts`.
- `Client.Stats()` — backfill progress (pages, pinned sealed tip, planned
  through, residual gap) for sustained-ingest observability.

## Event shape

One `Event` struct regardless of origin (archive or live): `Seq`, `DID`,
`TimeUS`, `Kind` (`commit`/`identity`/`account`/`sync`), with the matching
sub-struct populated. Commits carry `Record` (generic map; nil in raw
mode), `RecordCBOR` (byte-exact DAG-CBOR), and `CID`. `#sync` events are
delivered on backfill and the v2 live tail (v1 never emits them). The v2
wire adds `seq` and `commit.record_cbor` over the v1 shape (§5.2); the
client decodes both transparently.

## Writing a third-party client: the checklist

1. Page `planBackfill` with pinned `sealedTipSeq`; download by `mode`;
   verify checksums; decode blocks; run your exact filter per row.
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
