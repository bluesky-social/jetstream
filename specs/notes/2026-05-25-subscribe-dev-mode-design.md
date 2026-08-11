# `/subscribe` Dev-Mode Flag — Design

This historical design added a `--dev-mode` escape hatch allowing `/subscribe` before steady state for local development, while production remained gated on the persisted lifecycle phase. Bootstrap live events would always publish to the broadcaster, but only dev-mode subscribers could receive them early. References were `internal/subscribe/`, `internal/ingest/orchestrator/bootstrap.go`, `internal/lifecycle/`, and `cmd/jetstream/main.go`.
