// Package segment implements the jetstream segment file format: a
// columnar, zstd-compressed, length-prefixed binary log of atproto
// firehose events.
//
// Writer is not safe for concurrent use. Callers serialize access.
// The package contains no goroutines, timers, or context plumbing;
// lifecycle (time-based flushes, graceful shutdown, metadata
// coupling) is the responsibility of the ingestion orchestrator
// that composes Writer with the rest of the system.
package segment
