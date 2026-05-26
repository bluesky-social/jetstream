// Package subscribe owns the live websocket fan-out behind the public
// /subscribe endpoint.
//
// The package has three concerns, each in its own file:
//
//   - broadcaster.go: a single-publisher / many-subscriber pub/sub with
//     bounded per-subscriber channels. Slow subscribers are dropped, never
//     blocked; the firehose pipeline stays uncoupled from any one client.
//
//   - encoder.go: a pure function family that turns a segment.Event into
//     the Jetstream v1-compatible JSON wire format.
//
//   - handler.go: an http.Handler that upgrades to a websocket, registers
//     with the broadcaster, and pumps encoded events to the client until
//     either side hangs up.
//
// The first cut deliberately omits filtering, cursor replay,
// SubscriberOptionsUpdatePayload, and compression. See
// docs/superpowers/specs/2026-05-25-subscribe-design.md §10.
package subscribe
