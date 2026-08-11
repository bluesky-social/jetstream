# Backfill Bootstrap (PR 1): Wire the atmos Backfill Engine

This historical design proposed an `internal/backfill` wrapper around atmos, backed by Pebble repo-status rows and wired into `cmd/jetstream`. Completed repos would be skipped on restart, while handlers, metrics, and tests established the initial bootstrap lifecycle. Relevant references were `internal/backfill/`, `internal/store`, `cmd/jetstream/main.go`, and `cmd/jetstream/serve_test.go`.
