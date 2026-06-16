// Package lifecycle owns the persistent process-lifecycle markers
// that gate which subsystems jetstream starts on a given run.
//
// The primary marker is "phase", a string-valued pebble key whose
// value tells cmd/jetstream whether we are still in the bootstrap
// phase (running the backfill engine + live_segments consumer) or in
// steady state (running only the live consumer against data/segments).
// Related lifecycle metadata, such as the completed bootstrap-backfill
// timing, lives here too so cmd/jetstream's phase decisions and
// operator diagnostics stay in one place.
//
// We deliberately keep this outside internal/store, whose package
// doc reserves that package for keyspace-agnostic database lifecycle.
package lifecycle
