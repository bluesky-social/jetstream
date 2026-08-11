# Compaction Cache Invalidation

This historical design addressed stale serving state after compaction rewrote or removed segment files. The compaction completion hook would refresh `internal/manifest.Manifest` and invalidate decoded blocks in `internal/subscribe`, including protection against an in-flight decode repopulating stale cache entries. Runtime wiring belonged in `internal/jetstreamd/runtime.go`, with unit and oracle coverage for post-compaction reads.
