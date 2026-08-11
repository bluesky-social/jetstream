# Local atproto Simulator

This historical design defined a deterministic, development-only PLC/PDS/relay simulator with seeded accounts, mutable repos, listRepos, getRepo, and websocket firehose behavior. It used Pebble for reproducible state and provided local defaults while keeping production endpoints available through explicit configuration. References included `cmd/simulator/`, `internal/simulator/`, `cmd/jetstream/main.go`, https://plc.directory, and https://bsky.network.
