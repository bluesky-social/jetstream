// Package overlay builds and caches the compaction overlay blob served
// by network.bsky.jetstream.getTombstones. The blob is a compact,
// zstd-compressed binary serialization of the in-memory tombstone set
// (internal/tombstone) covering the seq range (W, M], where W is the
// compaction watermark and M is the highest seq folded in.
//
// The blob is precomputed and immutable once published: every reader
// shares the same backing bytes with zero per-request CPU. A cached
// blob may lag the live tip by one rebuild interval, but it is never
// invalid data — it is an atomic point-in-time snapshot that reports
// the exact W and M it covers, and the query-plan contract resumes the
// live tail from that M so coverage stays gapless. See
// docs/superpowers/specs/2026-06-15-getTombstones-overlay-design.md §5.4.
package overlay
