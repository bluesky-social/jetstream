# AGENTS.md

## What this is

Jetstream v2 is a full-network archive and live-streaming service for atproto. It ingests every record from every known repo on a relay, transitions seamlessly to live, and serves clients either large HTTPS segment-file downloads (for backfill) or the same JSON websocket protocol as Jetstream v1 (for live tail).

`DESIGN.md` is the source of truth for the system. Read it before making non-trivial changes — especially before touching any on-disk format. The "Practices" section below is the team's coding conventions. Both override anything inferred from existing code.

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
just simulate --accounts=100    # run the simulator with 100 mock accounts
just run serve                  # go run ./cmd/jetstream serve against the simulator
just run-prod serve             # go run ./cmd/jetstream serve against the real production firehose
just build                      # build binaries to ./bin
```

## Observability

Use the package-level metrics/tracer rather than rolling your own. `obs.Tracer("foo")` returns a tracer namespaced under `jetstream/foo`. HTTP handlers should be wrapped with the `otelhttp` middleware in `obs.Middleware`. Logging is `slog` with a `JETSTREAM_LOG_LEVEL` / `JETSTREAM_LOG_FORMAT` env-var override (text/json).

## CI

`.github/workflows/ci.yml` is heavily security-hardened. Two jobs: `lint` and `test (race)`. They run on every push to any branch.

## Practices

Internalize and always carry forward the following:

- We test thoroughly, using all means available to us where helpful
    - Unit tests have limited utility, but are helpful for some small things. Use sparingly
    - Integration tests are very valuable for testing happy paths
    - Fuzz tests and property based tests are valuable for many things, notably handling untrusted user input, or finding edge cases you may not have thought of that violate your invariants
    - Swarm testing to generate some meaningful randomness, not just white noise randomness
    - Smoke tests against real, live, production data
    - We make our tests execute very quickly so we can maintain fast feedback loops
- We use minimal logging to stdout/stderr, but we do extensive metrics with prometheus and distributed tracing with OTEL
    - Observability is critical to the success of this project, so we instrument with metrics and traces liberally
- The local development environment must be very simple to use
    - We use a justfile to manage common tasks
    - CI should match our local setup as closely as possible
- Use as few external dependencies as possible. A few white listed ones are:
    - github.com/jcalabro/atmos
    - github.com/jcalabro/gloom
    - github.com/jcalabro/gt
    - github.com/jcalabro/jttp
    - github.com/urfave/cli v3
    - github.com/zeebo/xxh3
    - github.com/coder/websocket
    - github.com/stretchr/testify
    - github.com/klauspost/compress
    - github.com/prometheus/client_golang
    - go.opentelemetry.io/otel and all other otel-related packages
    - anything under golang.org/x
    - github.com/puzpuzpuz/xsync
- Follow existing conventions closely. Avoid introducing new patterns, make sure code style, error handling, and logging is consistent.
- Avoid doing overly verbose comments. Comments should provide context about decision making rather than explaining simple lines of code. We should ensure that all exported symbols and packages have high level overview docstring comments.

## Simulator

We have a local minimal simulator available to test against quickly. It implements the minimal API surface required by PLC, PDSes, and the Relay in order to run the local jetstream development environment.
