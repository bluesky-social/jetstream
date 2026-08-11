# Public Status Page

This historical design proposed a public `GET /status` HTML page showing lifecycle, backfill, segment, cursor, and Pebble aggregates. A typed `internal/status` collector with TTL caching and singleflight would feed an embedded `internal/web` template, with abuse hardening and partial-error reporting. References were `internal/status/`, `internal/web/`, `internal/server/`, `internal/lifecycle/`, and `cmd/jetstream/`.
