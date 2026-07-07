// Package backfill drives the initial atproto network backfill phase
// for jetstream (docs/README.md §4.1). It wraps the atmos backfill engine,
// persists per-DID lifecycle state into pebble at repo/<did> per
// docs/README.md §3.5, and is invoked once per process start from
// cmd/jetstream.
//
// The package is single-shot per Run: each call paginates listRepos
// and downloads any DID not already at StatusComplete. Restart-resume
// falls out of that model — completed rows are skipped on Lookup.
//
// SegmentHandler.HandleRepo walks the downloaded repo's MST and emits one
// segment.KindCreate event per record into the shared segment writer.
package backfill
