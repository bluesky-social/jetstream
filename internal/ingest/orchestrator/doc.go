// Package orchestrator manages bootstrap, merge, and steady-state ingestion
// (docs/README.md §4). Runtime wiring supplies the verifier, identity
// directory, store, and HTTP client. Run reads the persisted phase, builds
// its subsystems, and returns on cancellation or consumer exit.
//
// Two durable phase commits govern restart: phase=merging after backfill
// drains but before bootstrap teardown, and phase=steady_state after merge
// completes but before the steady consumer starts. Each intervening
// filesystem operation can be retried from the persisted phase.
package orchestrator
