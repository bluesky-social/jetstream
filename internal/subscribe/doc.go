// Package subscribe owns the live websocket fan-out behind the public
// /subscribe endpoint, plus the v1-compatible filtering and
// subscriber-sourced-message protocol that clients depend on.
//
// The package has four concerns, each in its own file:
//
//   - broadcaster.go: a single-publisher / many-subscriber pub/sub with
//     bounded per-subscriber channels. Slow subscribers are dropped, never
//     blocked; the firehose pipeline stays uncoupled from any one client.
//
//   - encoder.go: a pure function family that turns a segment.Event into
//     the Jetstream v1-compatible JSON wire format.
//
//   - filter.go: the per-connection Filter — wantedCollections,
//     wantedDids, maxMessageSizeBytes — plus parsers for the query-string
//     and options_update wire formats.
//
//   - handler.go: an http.Handler that upgrades to a websocket, parses
//     the initial filter, registers with the broadcaster, and pumps
//     filtered+encoded events to the client. The reader goroutine
//     accepts SubscriberSourcedMessage frames and applies options_update
//     by swapping a per-connection atomic.Pointer[Filter].
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
//   - ?requireHello=true blocks event delivery (the broadcaster
//     Subscribe call is delayed) until the client sends a valid
//     options_update over the websocket. Matches v1 README:
//     "a client can connect with ?requireHello=true ... to pause
//     replay/live-tail until the first Options Update message is
//     sent by the client over the socket." Locked down by
//     TestHandler_RequireHello_BlocksUntilOptionsUpdate. Invalid
//     updates during the wait disconnect the client; locked down by
//     TestHandler_RequireHello_InvalidUpdateDisconnects. Implementation
//     note: events published during the wait are dropped, not queued
//     — we delay Subscribe rather than registering and buffering. v1
//     registers the subscriber on connect and buffers into the outbox
//     during the wait; v2's behavior is observable in the visible
//     contract (the first event a hello-mode client sees is one
//     published after its hello), and TestHandler_RequireHello_-
//     BlocksUntilOptionsUpdate locks the drop semantics down.
//
// Out of scope for this v1-compat surface: cursor replay, zstd
// compression. We silently ignore the cursor query param so that
// v1 clients that send it aren't rejected. Future v2-native
// endpoints will live alongside this package or in a sibling.
package subscribe
