# Jetstream 🛩️

[![Go Reference](https://pkg.go.dev/badge/github.com/bluesky-social/jetstream.svg)](https://pkg.go.dev/github.com/bluesky-social/jetstream)
[![Go Version](https://img.shields.io/github/go-mod/go-version/bluesky-social/jetstream)](https://github.com/bluesky-social/jetstream/blob/main/go.mod)
[![Latest Release](https://img.shields.io/github/v/release/bluesky-social/jetstream)](https://github.com/bluesky-social/jetstream/releases/latest)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](https://github.com/bluesky-social/jetstream/blob/main/LICENSE-DUAL)
[![CI](https://github.com/bluesky-social/jetstream/actions/workflows/ci.yml/badge.svg)](https://github.com/bluesky-social/jetstream/actions/workflows/ci.yml)

Full-network archive, replay, and streaming service for atproto.

Docker images are available [here](https://github.com/bluesky-social/jetstream/pkgs/container/jetstream).

The original jetstream codebase is available [here](https://github.com/bluesky-social/jetstream-legacy).

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Documentation

Coming soon!

## Examples

See the [examples](https://github.com/bluesky-social/jetstream/tree/main/examples) directory of this repo for several minimal usage examples.

You can run them like so:

```
# start the nix environment (see below)
just dev

# run any of the examples by name
just run-example live-tail
```

## Developing Locally

Jetstream development uses Nix for a pinned Go and toolchain environment. Install Nix, then enter the dev shell with either:

```sh
./dev.sh
# or
just dev
```

For development purposes, to run against the real production network in a setup that doesn't require a whole-network backfill:

```sh
# backfill 20 random repos, then cut over to the live tail
just run-prod serve --max-backfill-repos=20

# backfill a small number of chosen DIDs (csv), then cut over to the live tail
just run-prod serve --backfill-repos=did:plc:4uz2445cjiw7w4nobfgnu35f
```

This repo also ships with an extremely minimal atproto simulator (PLC, PDS, and the Relay). To run the local environment against it, use two terminals like:

```sh
# terminal 1: starts the simulator on :7777 with 10,000 mock accounts
# (this takes a minute to start up)
just simulator serve

# terminal 2: jetstream points at the simulator
just run serve
```

Simulator and prod data are always isolated, so you can swap between them without worry (they each get a unique data directory).

To fully reset your local environment (warning: destructive action!):

```sh
just clean  # removes all built binaries and all data directories
```

To run the linter and tests, you can do things like:

```sh
just       # run the linter and all -short tests
just lint  # run the linter

just test                     # everything, -short mode
just test ./internal/foo/...  # one package
just test-race                # full suite with -race
just test-long                # full suite without -short

just oracle                   # heavier simulator oracle (stress mode)
```
