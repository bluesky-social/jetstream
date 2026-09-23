// Package subscribe serves /subscribe (legacy v1) and
// /xrpc/network.bsky.jetstream.subscribeEvents (v2, atproto proposal 0015).
// Both endpoints share cursor replay and a pull loop; framing, filters, and
// errors differ.
//
// # Pull-based fan-out
//
// Each subscriber calls Tail.ReadFrom(cursor). There is no outbound queue per
// subscriber: slow readers advance slowly without accumulating server-side
// buffers.
//
// The writer's byte-bounded readable log exposes appended events immediately.
// Caught-up readers wait for the next append. Older cursors use the cold
// reader (replay.go), which walks sealed segments and the active segment's
// flushed blocks through a shared, byte-bounded LRU block cache.
//
// Hot and cached cold events share read-only Entry values, memoizing JSON and
// compressed frames across subscribers. Lazily encoded bodies count against
// the cache budget. A bounded encoder pool allows concurrent compression
// (encoderpool.go).
//
// Tail owns both readers and the connection registry for graceful shutdown.
// The slow-client detector (slowdetect.go) disconnects only clients that
// remain far behind and scan below the minimum rate for a sustained window.
// Lag alone or a selective filter does not trigger it.
//
// encoder.go defines v1 Encode and v2 EncodeV2, EncodeV2Error, and
// EncodeV2Info. cursor.go resolves seq and timestamp cursors. filter.go and
// filterv2.go parse endpoint filters. handler.go upgrades connections and
// runs the pull loop.
//
// # V1 compatibility
//
// The legacy endpoint preserves these behaviors:
//
//   - Empty, malformed, or negative maxMessageSizeBytes becomes 0 (no cap).
//     V2 rejects malformed syntax and overflow.
//   - Account and identity events bypass wantedCollections but still obey
//     wantedDids. A commit with an empty collection also bypasses
//     wantedCollections.
//   - Sync events are omitted by Encode (errSkipEvent); EncodeV2 emits them.
//     The bundled Go client uses v2.
//   - Unknown SubscriberSourcedMessage.Type values are logged and ignored.
//   - wantedCollections accepts any <prefix>.* without validating the prefix,
//     matching v1 code and examples such as app.bsky.*.
//   - DID and collection caps apply after deduplication. V1 does this for
//     DIDs; this endpoint also does it for collections.
//   - requireHello=true delays the pull loop until a valid options_update.
//     Invalid updates disconnect. Reading starts at the requested cursor
//     after hello; no events are buffered before it.
//
// V1 accepts options_update on a reader goroutine and atomically replaces the
// connection's Filter.
//
// # V2 contract
//
// lexicons/network/bsky/jetstream/subscribeEvents.json defines the wire
// format. Each text frame is one xrpc.v1.json object: a message with a
// payload whose $type is <nsid>#<kind>, a terminal error, or an info
// advisory. xrpc.v1.json is the only supported subprotocol and the lexicon
// default, so negotiated and unnegotiated connections receive identical
// framing.
//
// V2 is server-push only. Any client data frame closes the connection with
// StatusPolicyViolation; options_update and requireHello are unsupported.
//
// The kinds, dids, and collections filters combine with AND and each match
// all when omitted. Collections constrain commits only; kinds controls other
// event types. Unknown kinds, ineffective combinations, and legacy wanted*
// parameters return HTTP 400.
//
// Pre-upgrade errors use XRPC envelopes with structured names:
// InvalidRequest, CursorTooOld, UnknownZstdDictionary, and
// ServiceUnavailable. Clients match names, not message text.
//
// # Cursor replay
//
// cursor accepts a seq or Unix-microsecond timestamp. Replay is bounded by
// --cursor-lookback:
//
//   - V1 clamps a below-floor seq to the floor and increments cursorRequests
//     with the clamped label.
//   - V2 rejects a below-floor seq with HTTP 400 CursorTooOld and the floor
//     seq, allowing clients to backfill the missing range.
//   - Both endpoints clamp timestamp cursors. V2 first sends an info
//     OutdatedCursor naming the resumed seq.
//   - Timestamp resolution uses sealed indexes or the active writer's block
//     bounds, then suppresses rows before the exact witnessed_at boundary in
//     the one candidate block. It does not replay from the active segment's
//     beginning or force a flush.
//   - A cursor inside a registered seq vacancy advances to its exclusive end
//     before lookback policy applies. Cold replay makes the same jump below
//     the readable-log floor. Unregistered coverage holes remain errors.
//
// With --cursor-lookback=0, cursor parameters resolve to the live tip rather
// than being rejected.
//
// # Compression
//
// V1 supports either RFC 7692 permessage-deflate or its custom zstd
// dictionary. Deflate is negotiated through Sec-WebSocket-Extensions. Zstd is
// selected by compress=true or Socket-Encoding: zstd and uses dictionary ID
// 1612007021 (compress.go). Each binary message contains one zstd frame;
// clients decode with zstd.NewReader(nil, WithDecoderDicts(dict)). Requesting
// both schemes returns HTTP 400 to prevent double compression.
//
// V2 defaults to uncompressed text. Its only compression option is dict-zstd:
// download the dictionary from getZstdDictionary and pass
// zstdDictionary=<id>. Unknown or retired IDs return HTTP 400 with the
// current ID. Each binary message decompresses to one complete xrpc.v1.json
// frame, including info and error frames. Retrain the dictionary with just
// train-subscribe-dict.
//
// V2 rejects v1 compression parameters and ignores deflate offers, using
// uncompressed frames as RFC 7692 permits. Shared compressed frames avoid
// per-connection compression cost; measurements are in
// specs/notes/2026-07-09-subscribe-compression-cpu-analysis.md. Browser
// clients need a zstd decoder or uncompressed frames.
//
// On both endpoints, maxMessageSizeBytes applies to the complete uncompressed
// frame, including the v2 envelope.
package subscribe
