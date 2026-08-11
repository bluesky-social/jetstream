# Merge Phase — Implementation Plan

This historical plan replaced the merge stub with source-segment iteration, keep/drop filtering, atomic progress checkpoints, post-merge discovery, sealing, cleanup, and crash-resume coverage. It also extracted shared store and segment helpers and added integration, kill-point, and swarm/property tests. Principal references were `internal/ingest/orchestrator/merge*.go`, `internal/ingest/backfill/`, `internal/ingest/live/cursor.go`, and `internal/store/encoding.go`.
