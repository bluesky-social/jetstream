# Backfill Bootstrap Implementation Plan

This historical plan detailed the initial `internal/backfill` package: status types, metrics, Pebble store callbacks, handler, run entrypoint, and command wiring. Its intended outcome was restart-safe atmos backfill that treats completed rows as already processed. References included [Pebble](https://github.com/cockroachdb/pebble), [atmos](https://github.com/jcalabro/atmos), `internal/backfill/`, and `cmd/jetstream/main.go`.
