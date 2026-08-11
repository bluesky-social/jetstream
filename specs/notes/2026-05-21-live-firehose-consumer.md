# Live Firehose Consumer Implementation Plan

This historical plan introduced lifecycle phases, ingest durability hooks, live cursor persistence, event conversion, the firehose consumer, and command-level phase gating. The intended result was durable bootstrap-time live ingestion while steady-state merge plumbing remained explicitly unimplemented in this slice. Key references were `internal/ingest/live/`, `internal/lifecycle/`, `internal/ingest/writer.go`, and `cmd/jetstream/main.go`.
