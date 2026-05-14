// Package segment implements the jetstream segment file format: a
// columnar, zstd-compressed, length-prefixed binary log of atproto
// firehose events.
//
// This slice covers writing only. A future slice will add a public
// Reader, segment sealing (the 256-byte fixed header and footer
// described in DESIGN.md §3.1.2), and full crash recovery.
//
// Crash safety in this slice is limited to the writer's own
// torn-tail truncation on reopen: New walks the framed-block region
// from offset 256 forward and truncates any partially-written
// trailing frame before allowing further appends. A future Reader
// type will share that walker.
//
// Concurrency: Writer is not safe for concurrent use. Callers
// serialize access. The package contains no goroutines, timers,
// or context plumbing; lifecycle (time-based flushes, graceful
// shutdown, pebble metadata coupling) is the responsibility of the
// ingestion orchestrator that composes Writer with the rest of the
// system.
package segment
