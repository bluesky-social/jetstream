# Backfill-to-Live Cutover Orchestration

This historical design defined a crash-safe `bootstrap → merging → steady_state` lifecycle owned by `internal/ingest/orchestrator`. Durable phase markers bracketed draining bootstrap live ingestion, sealing writers, merge, and steady-state startup; failed-repo retry and the concrete merge algorithm were deferred. Key references were `internal/ingest/orchestrator/`, `internal/lifecycle/`, `internal/ingest/`, and `cmd/jetstream/main.go`.
