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
//   - The hot ring (hotring.go): a byte-bounded, seq-indexed FIFO of the
//     most recent decoded events, shared encode-once across all caught-up
//     subscribers via Entry (entry.go). Caught-up live readers park here
//     and wake on the next Append.
//
//   - The cold reader (replay.go): when a cursor falls below the ring's
//     resident base (evicted to disk) or replays pre-ring history, the
//     read falls through to a bounded disk walk over sealed segments +
//     the active segment, routed through a shared byte-bounded decoded-
//     block LRU cache (blockcache.go) so concurrent cold readers don't
//     each re-decode the same block.
//
// Tail (tail.go) owns the ring, the cold reader, and the graceful-close
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
//   - #sync events are deliberately not emitted. v1 didn't emit them
//     either; the v2 archive path is authoritative for #sync.
//     Implemented in encoder.go via errSkipEvent.
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
// --cursor-lookback floor. A cursor older than the floor is clamped, not
// rejected. Setting --cursor-lookback=0 disables replay: a cursor param
// is then accepted but resolves to the live tip rather than 400-ing, so
// v1 clients that always send a cursor still connect.
//
// RFC 7692 permessage-deflate compression is negotiated when the client
// offers it (handler.go). The v1 zstd-with-custom-dictionary scheme
// (?compress=true / Socket-Encoding: zstd) is NOT supported and is
// rejected with a 400 so a v1 client fails loudly rather than receiving
// uncompressed frames it can't decode.
package subscribe
