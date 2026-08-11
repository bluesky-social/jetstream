# Steady-State Failed Repo Retry

This historical implementation note replaced a one-time backfill-complete marker with periodic retry of active failed repositories whose backoff had elapsed. A retry prepended `KindSync`, wrote replacement events, drained segment durability, and only then committed completion metadata, making crash retries idempotent. References were `docs/README.md`, `internal/ingest/backfill/`, `internal/ingest/orchestrator/`, and `cmd/jetstream/`.
