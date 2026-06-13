# AGENTS.md

## Orientation

Jetstream v2 is a full-network archive and live-streaming service for atproto. Backfill is served as HTTPS segment-file downloads; live tail is the same JSON websocket protocol as Jetstream v1.

- `README.md` covers running the app, tests, and the simulator.
- `DESIGN.md` is the source of truth for the system. Read it before any non-trivial change, especially anything touching the on-disk segment format.
- This file is the team's coding conventions. It overrides anything inferred from existing code.

## Repo layout

```
cmd/
  jetstream/      main binary: `serve`, `inspect-segment`
  simulator/      local PLC + PDS + Relay on :7777
segment/          on-disk segment file format (header, blocks, footer, reader, writer, sealer)
internal/
  ingest/         backfill + live firehose + orchestrator that merges them
  subscribe/      websocket /subscribe endpoint (v1 protocol parity)
  server/         HTTP listeners (public :8080, debug :6060) and middleware
  store/          pebble-backed cursor + metadata store
  simulator/      simulator internals (world, traffic, http handlers)
  identity/       DID resolution
  status/         /status endpoint collector
  obs/            metrics, tracing, slog setup
  lifecycle/      graceful start/stop helpers
  web/            static UI assets
```

Atproto lexicon JSON (authoritative for XRPC and record schemas) lives at `~/go/src/github.com/bluesky-social/atproto/lexicons` on dev machines.

## Working in the codebase

The justfile is the single source of truth for build/test/lint. Prefer `just` recipes over invoking `go test` / `golangci-lint` directly so behaviour matches CI.

Frequently useful beyond what's in the README:

```sh
just test ./segment -run TestX  # one test (gotestsum forwards args after `--`)
just bench ./segment            # benchmarks
just fuzz 30s ./segment         # fuzz every Fuzz* target for 30s each
just modernize                  # apply gopls modernize rewrites
```

Oracle tests live in `internal/oracle` and compare Jetstream's durable output against a simulator model. Run them after changes to ingest, segment persistence, lifecycle/orchestrator phases, cursor handling, or restart recovery:

```sh
just test ./internal/oracle                                      # fast short-mode oracle checks
just test-long ./internal/oracle -run TestOracle_Restart -v      # non-short restart/recovery oracle
just oracle                                                      # heavier stress oracle mode
just oracle-sweep                                                # deterministic multi-seed stress sweep
```

The default `just` target intentionally runs short tests, so it does not execute non-short restart or stress oracle coverage. Use `just test-long` or the dedicated oracle recipes when the change could affect crash/restart correctness or end-to-end event fidelity.

The oracle's bug-detection power is measured by a mutation campaign. `testing/mutation/mutants/*.patch` are curated single-edit bugs ("mutants"), each documented with the production failure mode it models and a prediction of which oracle tier should catch it. `just mutation-campaign` applies them one at a time and verifies the oracle kills them; the scorecard lives in `testing/mutation/RESULTS.md`. Never apply these patches outside the driver, and never "fix" production code to match a mutant — they are deliberate bugs. Re-run the campaign after major changes to ingest, segment, or orchestrator logic; a STALE result means the underlying code moved and the mutant needs re-review and refresh.

Configuration is env-var driven (`JETSTREAM_*`). Defaults for local dev land in the committed `.env`; `just run-prod` overrides inline. Do not put secrets in `.env`.

## Observability

Use the package-level metrics/tracer rather than rolling your own. `obs.Tracer("foo")` returns a tracer namespaced under `jetstream/foo`. HTTP handlers should be wrapped with the `otelhttp` middleware in `obs.Middleware`. Logging is `slog`, with `JETSTREAM_LOG_LEVEL` and `JETSTREAM_LOG_FORMAT` (text/json) env-var overrides.

## CI

`.github/workflows/ci.yml` is heavily security-hardened. Two jobs: `lint` and `test (race)`. They run on every push to any branch.

## Practices

- **Testing.** Be liberal with the right kind of test for the job:
    - Unit tests sparingly — limited utility in this codebase, but useful for very small code paths.
    - Integration tests for happy paths.
    - Fuzz and property-based tests for untrusted input and edge cases that violate invariants.
    - Swarm tests for meaningful randomness (not white-noise flakes).
    - Smoke tests against real production occasionally.
    - Oracle tests that run a seeded simulator that validate data correctness at various parts of the server lifecycle
    - Tests must be fast. If a package's full test suite takes >1s, question it and try to bring it under a second.
- **Observability over logging.** Minimal stdout/stderr. Instrument with Prometheus metrics and OTEL traces liberally.
- **Local dev simplicity.** The justfile is the UX. CI mirrors it as closely as possible.
- **Few dependencies.** Only the whitelist below; question additions:
    - `github.com/jcalabro/atmos`, `gloom`, `gt`, `jttp`
    - `github.com/urfave/cli` v3
    - `github.com/zeebo/xxh3`
    - `github.com/coder/websocket`
    - `github.com/stretchr/testify`
    - `github.com/klauspost/compress`
    - `github.com/prometheus/client_golang`
    - `go.opentelemetry.io/otel` and related
    - `github.com/puzpuzpuz/xsync`
    - anything under `golang.org/x`
- **Follow existing conventions.** Don't introduce new patterns when the codebase already has one for code style, error handling, or logging.
- **Comments explain why, not what.** Exported symbols and packages get a high-level docstring; otherwise comment only when the reasoning isn't obvious from the code.
- **Never crash, and never corrupt data.** The process is a mission-critical, long-lived server daemon. Add observability in the case of incorrect/adversarial user input, but don't crash.
    - Treat all upstream relay/firehose/backfill data as user input. Invalid external records must not abort, stop, exit, or crash the server.
    - If upstream record data cannot be represented safely in Jetstream's internal format (for example a field exceeds a segment column width), drop that record or event, increment a warning/error metric, and log bounded diagnostic fields. Do not silently truncate or coerce it.
    - Preserve crash-loud behavior for invalid internal state, persistence corruption, fsync/store failures, and other conditions where continuing could corrupt Jetstream-owned data.
