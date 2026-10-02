// Package backfill drives Jetstream's direct-PDS initial network backfill
// (docs/README.md §4.1). Atmos owns listHosts enumeration, per-host listRepos
// scheduling, direct downloads, and retry policy. This package owns the
// Jetstream adapter: repo/<did> lifecycle/PDS attribution,
// pdshost/<hostname> roster and cursors, segment emission, durability staging,
// metrics, and the steady-state failed-repo retry pass.
//
// Restart safety is host-local and at-least-once. Completed DIDs are skipped,
// while each PDS resumes from its last durable cursor. A cursor is staged into
// the writer's synced Pebble batch only after all segment-backed completions it
// covers can become durable in that same batch, so a crash may repeat a page
// but can never skip undurable archive data.
//
// atmos's Store takes enumeration callbacks a page at a time, so each
// listRepos page and listHosts page costs a constant number of metadata
// round trips, and
// concurrent hosts' page writes share one group-committed transaction. In
// disaggregated mode every metadata read is a network round trip and every
// commit a fenced catalog transaction; per-entry calls would cap a large PDS
// at a few repos per second (specs/gotchas.md).
//
// SegmentHandler.HandleRepo walks the downloaded repo's MST and emits one
// segment.KindCreate event per record into the shared segment writer.
package backfill
