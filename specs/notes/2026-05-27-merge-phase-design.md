# Merge Phase Design

**Status: historical draft.** This design specified draining bootstrap-live segments into the steady archive, filtering redundant commits against completed backfill revisions, re-stamping `IndexedAt`, and checkpointing each source segment atomically for crash recovery. It also proposed post-merge listRepos discovery and explicitly deferred lookaside coordination. References were `internal/ingest/orchestrator/merge.go`, `internal/ingest/backfill/`, `internal/ingest/live/`, and `internal/store/`.
