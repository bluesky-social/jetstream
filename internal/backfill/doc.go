// Package backfill drives the bootstrap-phase work described in
// DESIGN.md §4.1: seeding the metadata store with the relay's full
// account list, downloading each repo's history, and tracking
// per-DID progress in pebble at data/meta.pebble/.
//
// The package is intentionally split from the live ingest path. The
// bootstrap phase is a one-shot operation that only runs on a fresh
// data directory (or to fill in DIDs that have appeared since the
// last run).
//
// In this slice the package exposes only the metadata store and the
// listRepos seed step. Repo download, live shadowing, and the merge
// phase will land in subsequent iterations.
package backfill
