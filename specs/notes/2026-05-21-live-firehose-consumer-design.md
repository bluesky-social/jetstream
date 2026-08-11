# Live Firehose Consumer During Backfill

This historical design added a live firehose consumer during bootstrap so events arriving during backfill were durably captured in a separate segment tree. It established cursor-after-fsync ordering, lifecycle phase gating, event conversion, observability, and reuse of the consumer for steady state; merge itself remained deferred. References included `internal/ingest/live/`, `internal/ingest/`, `internal/lifecycle/`, `cmd/jetstream/main.go`, and the relay at https://bsky.network.
