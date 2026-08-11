# Backfill Segment Writes — Active Segment Lifecycle and Real `HandleRepo`

This historical design moved backfill from a logging handler to durable segment writes through a new `internal/ingest` writer. It specified sequence allocation, flush/fsync ordering with Pebble state, rotation, startup recovery, metrics, tracing, and adversarial tests. Primary references were `internal/ingest/`, `internal/backfill/`, `segment/scan.go`, `internal/store`, and `cmd/jetstream/`.
