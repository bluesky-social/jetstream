# Compaction Cache Invalidation Implementation Plan

This historical plan added per-segment block-cache generations, a cold-reader invalidation API, manifest-refresh regression tests, and runtime compaction-hook wiring. Its failure contract required refresh errors to stop lifecycle progress rather than serve against stale archive metadata. Key references were `internal/subscribe/blockcache.go`, `internal/subscribe/replay.go`, `internal/manifest/`, `internal/jetstreamd/runtime.go`, and `internal/oracle/`.
