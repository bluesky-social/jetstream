// Package segment implements the jetstream segment file format: a
// columnar, zstd-compressed, length-prefixed binary log of atproto
// firehose events.
//
// This slice covers writing only. A future slice will add a public
// Reader, segment sealing (the 256-byte fixed header and footer
// described in DESIGN.md §3.1.2), and crash recovery.
//
// Concurrency: Writer is not safe for concurrent use. Callers
// serialize access. The package contains no goroutines, timers,
// or context plumbing; lifecycle (time-based flushes, graceful
// shutdown, pebble metadata coupling) is the responsibility of the
// ingestion orchestrator that composes Writer with the rest of the
// system.
package segment
