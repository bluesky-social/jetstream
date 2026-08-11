# Backfill-to-Live Cutover — Implementation Plan

This historical implementation plan created the orchestrator and lifecycle phases, added writer sealing, and wired bootstrap drain through merge into steady state. Two persisted phase commits made restart recovery retry the remaining suffix of the state machine; the merge body was initially a stub in this slice. References included `internal/ingest/orchestrator/`, `internal/lifecycle/phase.go`, `internal/ingest/writer.go`, and `cmd/jetstream/main.go`.
