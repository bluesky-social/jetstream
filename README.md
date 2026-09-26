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

The disaggregated storage backend (in development) needs PostgreSQL and an S3-compatible object store. `just up` starts PostgreSQL, SeaweedFS, and MinIO in Docker (see `compose.yaml`), waits until they are healthy, and prints connection details:

```sh
just up       # start (idempotent); prints URLs and dev credentials
just psql     # psql on the dev database
just down     # stop and delete everything
just down up  # reset to empty
```

The environment keeps no state. All data lives in tmpfs, so `just down` discards every table and object. Ports bind to `127.0.0.1` only (Postgres 15432, SeaweedFS 18333, MinIO 19000, MinIO console 19001). To remap them, use a gitignored `compose.override.yaml`. The app credentials only have PutObject, GetObject, and DeleteObject on the `jetstream` bucket, which is all production grants. `just up` verifies this on every run.

`just test-storage` runs the storage contract and fault suites against this environment. It runs every package whose tests use PostgreSQL or S3, with SeaweedFS as the object store, then runs the object-store packages again against MinIO. Storage is required, so a missing backend fails rather than skips. The recipe needs `just up` running and fails fast when it is not. It never starts or stops the environment itself, so after a failure the data is still there to inspect (`just psql`). Plain `just` never needs PostgreSQL or S3: those tests skip.

```sh
just up && just test-storage
just test-storage -run TestReaderRole  # arguments pass through to go test
```

To point `serve` at `just up`, select disaggregated storage and pass the connection settings in the environment. The PostgreSQL URL and the S3 keys are secrets, so they never go in `.env`. Jetstream reads the S3 keys from the standard AWS SDK chain, not from a `JETSTREAM_` variable. Disaggregated mode keeps nothing on local disk, so `JETSTREAM_DATA_DIR` must be unset. `.env` sets it, so run the binary directly rather than through `just run`. The pod refuses to start without `GOMEMLIMIT`, and its memory budgets must fit in 75% of it. Compaction must also be off (`JETSTREAM_COMPACTION_INTERVAL=0`) until it supports disaggregated mode.

A new database needs `jetstream storage init` once first. It probes the bucket, applies the schema, and creates the archive. It refuses a database that already holds one, so after `just down && just up` run it again:

```sh
go build -o bin/jetstream ./cmd/jetstream
export JETSTREAM_PG_URL='postgres://jetstream:jetstream@127.0.0.1:15432/jetstream?sslmode=disable' \
  JETSTREAM_S3_ENDPOINT=http://127.0.0.1:18333 JETSTREAM_S3_PATH_STYLE=true \
  JETSTREAM_S3_REGION=us-east-1 JETSTREAM_S3_BUCKET=jetstream \
  AWS_ACCESS_KEY_ID=jetstream AWS_SECRET_ACCESS_KEY=jetstream-dev-secret
./bin/jetstream storage init
env -u JETSTREAM_DATA_DIR \
  JETSTREAM_STORAGE=disaggregated GOMEMLIMIT=8GiB \
  JETSTREAM_COMPACTION_INTERVAL=0 \
  ./bin/jetstream serve
```

For MinIO, use `JETSTREAM_S3_ENDPOINT=http://127.0.0.1:19000`. `jetstream serve --help` lists every storage setting under "Disaggregated storage".

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
