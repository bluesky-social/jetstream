# Local atproto Simulator Implementation Plan

This historical plan implemented the local simulator’s world model, deterministic account and record generation, fanout ring, PLC/PDS/relay HTTP behavior, and subscribeRepos websocket stream. It also added simulator CLI wiring, local `.env`/justfile defaults, and Jetstream PLC configuration while keeping the tool out of production packaging. References were `cmd/simulator/`, `internal/simulator/`, `cmd/jetstream/main.go`, https://plc.directory, and https://bsky.network.
