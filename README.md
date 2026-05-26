# Jetstream

[![ci](https://github.com/bluesky-social/jetstream-v2/actions/workflows/ci.yml/badge.svg)](https://github.com/bluesky-social/jetstream-v2/actions/workflows/ci.yml)

Full-network archive and streaming service for atproto.

## Getting started

You'll need a recent Go (see `go.mod` for the version) and [`just`](https://github.com/casey/just). Once you have those:

```sh
just install-tools   # one-time: installs golangci-lint, gotestsum, etc.
just                 # lint + test, the default recipe
```

## Running it

```sh
just run serve              # starts the HTTP server on :8080 (debug on :6060)
just run-race serve         # same thing with the race detector on
```

## Tests

```sh
just test                     # everything
just test ./internal/foo/...  # one package
just test-race                # with -race

just lint # runs the linter
```

## Building a binary

```sh
just build      # drops the binary at ./bin/jetstream
just clean      # nukes ./bin
```

## Local development with the simulator

`just run` defaults to a local atproto simulator (`./cmd/simulator`) that
emulates PLC, a single PDS, and a relay (firehose) under one HTTP listener
at `:7777`. The defaults are wired through a committed `.env` at the repo
root, so you don't have to set anything to point jetstream at it.

```sh
just simulator        # terminal 1: starts the simulator on :7777
just run serve        # terminal 2: jetstream points at the simulator
```

Default world: 10,000 deterministic accounts with realistic activity
distributions (Zipfian per-account, exponential inter-arrival, weighted
collection mix). Bootstrap takes a few seconds on first run; subsequent
runs resume from `./data/simulator/`.

Reset the simulator's world without touching jetstream's data:

```sh
just simulator-reset
```

If you change `--seed` between runs, the simulator refuses to start with
`ErrSeedMismatch` until you `--reset`. Likewise, after `simulator-reset`
the firehose seq counter goes back to 0; if jetstream is still running
with its old persisted cursor it will re-receive events. Either restart
both, or run `just clean` to also wipe jetstream's `./data`.

Smoke against real production occasionally:

```sh
just run-prod serve   # uses bsky.network + plc.directory
```

The simulator is a dev tool: not in the Dockerfile, not shipped to users.

### Known limitations (v1)

The simulator emits 100% valid data per the design doc, but in heavy
mixed-traffic scenarios it can produce commits that fail jetstream's
verifier — most notably:

- **Update ops with missing record blocks in the CAR diff.** When an
  account performs both a create and an update in the same commit
  cycle, the diff packaging can omit a block jetstream's verifier
  expects. Crashes the livestream consumer.
- **Duplicate op paths within a single commit.** When `--commits-per-sec`
  is high and the per-account record set is small, the random
  pick-existing-record draw can land on the same path twice in one
  commit.
- **MST inversion incomplete warnings.** The verifier accepts these
  under lenient mode, but they indicate the simulator's per-commit
  `prevData` doesn't always match the previous commit's MST root
  exactly.

The `cmd/simulator/e2e_test.go` smoke test reads ONE event after backfill
drains, so it doesn't hit these. They surface under sustained traffic.
Workarounds: keep `--commits-per-sec` low (≤5) and `--initial-records-per-account`
high (≥10) until v2 fixes commit construction.

See `docs/superpowers/specs/2026-05-26-local-simulator-design.md` for design
context and `docs/superpowers/plans/2026-05-26-local-simulator.md` for the
implementation plan.
