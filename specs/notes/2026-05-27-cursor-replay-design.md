# Subscriber Cursor Replay — Design

This historical design added `/subscribe` replay from a caller-provided cursor, supporting legacy timestamp cursors and sequence cursors before cutting over to the live broadcaster. A manifest and cold replayer resolved the retained window, with out-of-window legacy timestamps clamped to the oldest available event. Key references were `docs/README.md`, `internal/subscribe/`, the then-`internal/replay` components, `internal/ingest/`, and `cmd/jetstream/main.go`.
