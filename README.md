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

## User Documentation

See the [jetstream documentation website](https://bsky.network/docs/jetstream/) for usage documentation.

See the original [RFD](https://github.com/bluesky-social/jetstream/blob/main/docs/README.md) for the project goals and design.

## Examples

See [examples](https://github.com/bluesky-social/jetstream/tree/main/examples) for a few minimal jetstream usage programs.

## Developing Locally

Jetstream development uses Nix for a pinned Go and toolchain environment. Install the [Nix package manager](https://nixos.org/download/) and [direnv](https://direnv.net/), then allow your system to automatically launch the dev env with:

```sh
direnv allow
```

To develop against the production network with a limited backfill:

```sh
# backfill 20 random repos, then cut over to the live tail
just run-prod serve --max-backfill-repos=20

# backfill a small number of chosen DIDs (csv), then cut over to the live tail
just run-prod serve --backfill-repos=did:plc:4uz2445cjiw7w4nobfgnu35f
```

To run against the included atproto simulator (PLC, PDS, and relay), use two terminals:

```sh
# terminal 1: starts the simulator on :7777 with 10,000 mock accounts
# (this takes a minute to start up)
just simulator serve

# terminal 2: jetstream points at the simulator
just run serve
```

The simulator and production recipes use separate data directories.

To fully reset your local environment (warning: destructive action!):

```sh
just clean  # removes all built binaries and all data directories
```

Run checks:

```sh
just       # run the linter and all -short tests
just lint  # run the linter

just test                     # everything, -short mode
just test ./internal/foo/...  # one package
just test-race                # full suite with -race
just test-long                # full suite without -short

just oracle                   # heavier simulator oracle (stress mode)
```

## Security disclosures

If you discover any security issues, please send an email to security@bsky.app. The email is automatically CCed to the entire team, and we'll respond promptly. See [SECURITY.md](https://github.com/bluesky-social/jetstream/blob/main/SECURITY.md) for more info.

## License

This project is dual-licensed under MIT and Apache 2.0 terms:

- MIT license ([LICENSE-MIT.txt](https://github.com/bluesky-social/jetstream/blob/main/LICENSE-MIT.txt) or http://opensource.org/licenses/MIT)
- Apache License, Version 2.0, ([LICENSE-APACHE.txt](https://github.com/bluesky-social/jetstream/blob/main/LICENSE-APACHE.txt) or http://www.apache.org/licenses/LICENSE-2.0)

Downstream projects and end users may choose either license individually, or both together, at their discretion. The motivation for this dual-licensing is the additional software patent assurance provided by Apache 2.0.

Bluesky Social PBC has committed to a software patent non-aggression pledge. For details see [the original announcement](https://bsky.social/about/blog/10-01-2025-patent-pledge).
