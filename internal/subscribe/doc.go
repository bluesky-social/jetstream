// Package subscribe owns the websocket fan-out behind the public
// /subscribe endpoint, plus the v1-compatible filtering, cursor replay,
// and subscriber-sourced-message protocol that clients depend on.
//
// # Pull-based fan-out
//
// Every subscriber — live or cursor-resuming — runs the same pull loop:
// it calls Tail.ReadFrom(cursor) in a loop and is served wherever its
// cursor points. There is no per-subscriber outbound queue and no
// push broadcaster (the old model, which dropped clients once a bounded
// channel overflowed during a #sync-triggered burst). Backpressure is
// implicit: a client that reads slowly simply advances its cursor
// slowly. Nothing buffers on its behalf, so a slow reader cannot grow
// unbounded server memory.
//
// ReadFrom resolves a cursor against two tiers:
//
//   - The writer readable log: a byte-bounded, seq-indexed FIFO owned by the
//     ingest writer. It exposes recently appended events to subscribers
//     immediately and shares encode-once memoization via Entry (entry.go).
//     Caught-up live readers park here and wake on the next writer append.
//
//   - The cold reader (replay.go): when a cursor falls below the readable
//     log's resident base (evicted to disk) or replays older history, the
//     read falls through to a bounded disk walk over sealed segments +
//     the active segment's flushed region, routed through a shared byte-bounded decoded-
//     block LRU cache (blockcache.go) so concurrent cold readers don't
//     each re-decode the same block. The cache also carries one shared
//     Entry per cached event (#295), so cold subscribers reuse each
//     other's memoized JSON encodes and compressed frames the way hot
//     ones do; the entries charge their lazily-memoized bodies back to
//     the cache's byte budget as they materialize. zstd compression
//     itself runs on a bounded free list of encoders (encoderpool.go)
//     rather than one process-wide serialized encoder.
//
// Tail (tail.go) owns the readable-log adapter, the cold reader, and the graceful-close
// connection registry. The hot/cold boundary is transparent to ReadFrom
// callers. An adversarially-slow client — far behind the tip AND scanning
// the log below a floor rate for a sustained window — is dropped by the
// detector (slowdetect.go); a client that is merely behind but keeping
// pace, or behind a selective filter, is never dropped.
//
// Other concerns, each in its own file:
//
//   - encoder.go: a pure function family that turns a segment.Event into
//     the Jetstream v1-compatible JSON wire format.
//
//   - cursor.go: resolves the ?cursor= query parameter (seq or time_us)
//     against the manifest, clamped to the configured lookback floor.
//
//   - filter.go: the per-connection Filter — wantedCollections,
//     wantedDids, maxMessageSizeBytes — plus parsers for the query-string
//     and options_update wire formats.
//
//   - handler.go: an http.Handler that upgrades to a websocket, resolves
//     the cursor, and runs the pull loop, pumping filtered+encoded events
//     to the client. The reader goroutine accepts SubscriberSourcedMessage
//     frames and applies options_update by swapping a per-connection
//     atomic.Pointer[Filter].
//
// V1 wire compatibility is the explicit design point. Where v2's house
// style ("crash loud, no silent fallbacks" — CLAUDE.md) would diverge
// from the v1 README's stated contract, this package deliberately
// matches v1. The places we do that are:
//
//   - maxMessageSizeBytes silently coerces empty/malformed/negative
//     values to 0 ("no cap"). v1 README: "Zero means no limit, negative
//     values are treated as zero." Locked down by
//     TestParseMaxMsgSize_V1Compat.
//
//   - Identity and Account events bypass wantedCollections — they are
//     always delivered, regardless of the subscriber's collection
//     filter. v1 README: "Regardless of desired collections, all
//     subscribers receive Account and Identity events." Locked down by
//     TestWants_IdentityBypassesCollectionFilter.
//
//   - #sync events are deliberately not emitted on the v1 /subscribe wire —
//     v1 didn't emit them either (encoder.go Encode → errSkipEvent). The
//     /subscribe-v2 wire DOES emit #sync (EncodeV2), which is what the
//     bundled Go client consumes. #account and #identity are emitted on
//     both wires.
//
//   - Unknown SubscriberSourcedMessage.Type values are logged and
//     ignored, not fatal. v1 has the same policy. Locked down by
//     TestHandler_OptionsUpdate_UnknownTypeIgnored.
//
//   - wantedCollections accepts any "<prefix>.*" pattern with no
//     validation of the head — v1's docs claim the head must pass
//     NSID validation, but v1's actual code does not enforce it,
//     and patterns like "app.bsky.*" appear in v1 client examples.
//     Locked down by TestParseQuery_PrefixCollection_TwoSegment.
//
//   - Filter caps fire post-dedupe (parseWantedDIDs and
//     parseWantedCollections build the unique set first, then
//     compare to MaxWantedDIDs / MaxWantedCollections). v1 does
//     the same on DIDs; we extend the same forgiveness to
//     collections for symmetry.
//
//   - A commit with an empty collection field bypasses the
//     wantedCollections filter — matches v1's WantsCollection.
//
//   - ?requireHello=true blocks event delivery until the client sends a
//     valid options_update over the websocket. Matches v1 README:
//     "a client can connect with ?requireHello=true ... to pause
//     replay/live-tail until the first Options Update message is
//     sent by the client over the socket." Locked down by
//     TestHandler_RequireHello_BlocksUntilOptionsUpdate. Invalid
//     updates during the wait disconnect the client; locked down by
//     TestHandler_RequireHello_InvalidUpdateDisconnects. Implementation
//     note: the pull loop only starts after the hello arrives, so the
//     first event a hello-mode client sees is one read from its start
//     cursor after the hello — there is no pre-hello buffering.
//
// # Cursor replay and compression
//
// ?cursor= replay IS supported (cursor.go + the cold reader), resolving a
// seq or time_us cursor against the manifest, clamped to the configured
// --cursor-lookback floor. The too-old-cursor policy is endpoint-specific
// (Subscription.V2, set true only on /subscribe-v2):
//
//   - /subscribe (v1): a seq cursor below the floor is silently CLAMPED up
//     to the floor (legacy v1 wire parity), made observable via the
//     "clamped" cursorRequests metric label.
//   - /subscribe-v2: a seq cursor below the floor is REJECTED with a
//     pre-upgrade HTTP 400 carrying the floor seq (ErrCursorTooOld), so a
//     backfilling client detects a slow handoff and re-backfills from its
//     last seq instead of silently skipping (requestedSeq, floor].
//   - The time_us cursor path always clamps under BOTH endpoints: a legacy
//     timestamp cursor's documented contract is to start at the oldest
//     retained event, and the v2 reject policy governs only the seq path.
//
// Setting --cursor-lookback=0 disables replay: a cursor param is then
// accepted but resolves to the live tip rather than 400-ing, so v1 clients
// that always send a cursor still connect.
//
// Compression is endpoint-specific (#294; measured basis in
// specs/notes/2026-07-09-subscribe-compression-cpu-analysis.md):
//
// /subscribe (v1, wire-frozen) offers two schemes, at most one per client:
//
//   - RFC 7692 permessage-deflate, negotiated transparently when the
//     client offers it via Sec-WebSocket-Extensions (handler.go). Kept
//     solely for v1 wire parity: per-connection deflate is the dominant
//     server CPU cost at fanout scale (~2.3x the shared-zstd path at 200
//     subscribers, scaling linearly with subscriber count).
//
//   - The v1 custom-zstd-dictionary scheme (?compress=true or
//     Socket-Encoding: zstd). Opted-in connections receive binary
//     websocket frames, each a zstd frame compressed with the v1
//     custom dictionary (dict ID 1612007021, embedded in compress.go). A
//     client decodes with zstd.NewReader(nil, WithDecoderDicts(dict)).
//
//   - Offering BOTH at once is rejected with a 400: the two would
//     double-compress, so the client must pick one.
//
// /subscribe-v2 serves uncompressed text frames by default and offers
// exactly ONE compression scheme, chosen deliberately for server
// cheapness at high fanout (the compressed frame is memoized per event
// and shared by every subscriber — near-zero marginal compression cost):
//
//   - dict-zstd, opted into with ?zstdDictionary=<id>, where <id> is the
//     zstd dictionary ID of the v2 dictionary (zstd_dictionary_v2,
//     retrained via `just train-subscribe-dict`) the client downloaded
//     through the getZstdDictionary XRPC endpoint. An unknown or retired
//     ID is a pre-upgrade 400 carrying the current ID — the server never
//     sends frames a client can't decode. The legacy v1 opt-ins are 400s
//     on v2, and permessage-deflate is NEVER negotiated (a deflate offer
//     silently falls back to uncompressed per RFC 7692). This is a
//     deliberate "no zero-setup compression on v2" decision: v2's
//     audience is thick clients; browsers consume uncompressed frames or
//     bring a wasm zstd decoder.
//
// The maxMessageSizeBytes cap is enforced on the uncompressed JSON length
// for all clients on both endpoints.
package subscribe
