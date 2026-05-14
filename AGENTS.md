# AGENTS.md

## What this is

Jetstream v2 is a full-network archive and live-streaming service for atproto. It ingests every record from every known repo on a relay, transitions seamlessly to live, and serves clients either large HTTPS segment-file downloads (for backfill) or the same JSON websocket protocol as Jetstream v1 (for live tail).

`DESIGN.md` is the source of truth for the system. Read it before making non-trivial changes — especially before touching any on-disk format. `PRACTICES.md` is the team's coding conventions. Both override anything inferred from existing code.

## Common commands

The justfile is the single source of truth for build/test/lint. Prefer `just` recipes over invoking `go test` / `golangci-lint` directly so behaviour matches CI.

```sh
just install-tools              # one-time: golangci-lint + gotestsum

just                            # default: lint + test (-short). Sub-second.
just lint                       # golangci-lint, ~0.5s
just test                       # gotestsum -short. The everyday loop.
just test-long                  # full suite, no flags. Includes 1000-iter swarm.
just test-race                  # full suite under -race. ~30s, swarm-dominated.

just test ./segment             # one package
just test ./segment -run TestX  # one test (gotestsum forwards args after `--`)
just run serve                  # go run ./cmd/jetstream serve
just build                      # binary at ./bin/jetstream
```

## Observability

Use the package-level metrics/tracer rather than rolling your own. `obs.Tracer("foo")` returns a tracer namespaced under `jetstream/foo`. HTTP handlers should be wrapped with the `otelhttp` middleware in `obs.Middleware`. Logging is `slog` with a `JETSTREAM_LOG_LEVEL` / `JETSTREAM_LOG_FORMAT` env-var override (text/json).

## CI

`.github/workflows/ci.yml` is heavily security-hardened. Two jobs: `lint` and `test (race)`. They run on every push to any branch.
