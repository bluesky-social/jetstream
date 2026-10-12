// Package migrate moves a running local-mode archive to disaggregated
// storage without stopping it (specs/notes/2026-10-09-local-to-disagg-
// migration.md).
//
// A Migrator runs inside the local-mode process. It holds the
// disaggregated writer lease and writes the catalog through fenced
// transactions, so the catalog is always a consistent prefix of the local
// archive with the same seqs:
//
//   - it imports every sealed local segment byte for byte, then each new
//     durable block and seal as local ingest produces them;
//   - it copies the Pebble metadata, then each change the store's commit
//     observer reports;
//   - on an operator's request it stops local ingest, ships what is left,
//     and commits migration/state done, which hands the lease to the
//     disaggregated pods.
//
// The migrator never writes local segments, and writes Pebble only under
// migration/. Its failures stop it, never the process: local ingest and
// reads carry on, and a restart resumes from what the catalog holds.
package migrate
