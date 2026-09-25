// Package maintainer folds hot mode's closed blocks into active blocks and
// seals the active segment into a generation (design §10.7, §10.8).
//
// A Maintainer is the hot writer's ingest.BlockSink. It runs one goroutine
// per leader session that folds and seals in order, never concurrently, so
// the in-memory view of the active segment (its index, block list, and
// virtual size) is owned by that goroutine alone. Hot batches keep
// committing during a seal; folds queue behind it, and the writer's
// unfolded cap bounds how far behind they get.
//
// Every failure ends the session. Nothing is retried: the next session
// rebuilds from what committed (§10.9). Blocks still queued when the
// maintainer stops are left as hot batches for that rebuild.
//
// Rebuild is that session-start step. It checks the catalog, folds and seals
// every hot batch that can no longer grow into a block, and returns the rest
// as the open block the next hot writer resumes (ingest.HotConfig.Resume).
package maintainer
