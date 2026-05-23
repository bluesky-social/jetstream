// Package lifecycle owns the persistent process-lifecycle markers
// that gate which subsystems jetstream starts on a given run.
//
// Today there is exactly one marker: "phase", a string-valued pebble
// key whose value tells cmd/jetstream whether we are still in the
// bootstrap phase (running the backfill engine + live_segments
// consumer) or in steady state (running only the live consumer
// against data/segments). Future PRs may add further markers
// (e.g. backfill_done) here so cmd/jetstream's phase decisions stay
// in one place.
//
// We deliberately keep this outside internal/store, whose package
// doc reserves that package for keyspace-agnostic database lifecycle.
package lifecycle
