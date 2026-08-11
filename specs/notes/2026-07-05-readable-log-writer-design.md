# Readable-Log Writer: Unifying Visibility Across the Ingest/Subscribe Seam

**Status: analysis with direction agreed; containment first, refactor separately.** The note traced visibility races and duplicated buffering across ingest and subscribe, measured the reasons async flush existed, and chose an immediate wait-before-sink containment fix. The longer-term direction was a writer-owned readable log with a single visibility watermark and no special-case hot ring. References were `internal/ingest/`, `internal/jetstreamd/runtime.go`, and `internal/ingest/writer_bench_test.go`.
