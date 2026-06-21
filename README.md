# Jetstream

[![ci](https://github.com/bluesky-social/jetstream-v2/actions/workflows/ci.yml/badge.svg)](https://github.com/bluesky-social/jetstream-v2/actions/workflows/ci.yml)

Full-network archive and streaming service for atproto.

**NOTE:** This project is not yet deployed to production, and there will be backwards-incompatible changes to the on-disk format in the next few days. Do not run this yourself and expect it to remain stable until we tag a 1.0 release (coming soon!).

## Getting started

You'll need a recent Go (see [go.mod](https://github.com/bluesky-social/jetstream-v2/blob/main/go.mod) for the version) and [`just`](https://github.com/casey/just).

Once you have those, run this for first-time repo setup:

```sh
just install-tools  # run once after cloning
```

## Running Locally

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

## Testing and Linting

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

## Inspecting segment files

```sh
# inspect all segment files
just run inspect-all

# inspect a single file
just run inspect-segment ./data-prod/segments/seg_0000000000.jss
```

Dumps the header, footer, per-block stats, and collection event counts for a sealed segment.

## Web Dashboard

The jetstream server also offers a web dashboard with some basic read-only features.

Start the server locally in your desired configuration, then open `http://localhost:8080/status` in your browser.
