// Package segment implements the jetstream segment file format: a
// columnar, zstd-compressed, length-prefixed binary log of atproto
// firehose events. Every active segment can be sealed into an
// immutable, self-describing file with a footer (block index,
// segment-level DID bloom, per-block DID blooms, collection block
// index) and a finalized 256-byte header carrying an xxh3 checksum
// over the metadata.
//
// The package is split by responsibility: event.go, block.go, and
// zstd.go define the row layout and block wire format; builder.go
// accumulates events into a block and encodes it (BlockBuilder), with
// no file behind it; writer.go owns the active-segment file state
// machine (append, flush, fsync, seal) on top of a BlockBuilder;
// header.go, footer.go, bloom.go, and collection.go are pure
// encode/decode for footer sub-formats; seal.go computes the sealed
// header and footer from a sequence of block frames (BuildSealed) and
// writes them to the active file; rewrite.go computes a compacted
// segment in memory and swaps it in atomically; reader.go ships a
// goroutine-safe public Reader over a sealed file, any io.ReaderAt
// holding its bytes, or its header and footer plus a per-block fetcher.
//
// The pure pieces (BlockBuilder, BuildSealed, and the byte-source
// Reader constructors) use virtual file offsets: a sealed segment's
// offsets are the positions its sections would have in a segment file,
// whether or not that file ever exists.
//
// Writer is not safe for concurrent use; callers serialize access.
// Reader is safe for concurrent reads.
//
// The package contains no goroutines, timers, or context plumbing;
// lifecycle (time-based flushes, when to seal, graceful shutdown,
// metadata coupling) is the responsibility of the ingestion
// orchestrator that composes Writer with the rest of the system.
package segment
