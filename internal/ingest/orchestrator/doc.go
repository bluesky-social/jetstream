// Package orchestrator owns the ingestion-lifecycle state machine
// for jetstream. It drives the bootstrap → merging → steady-state
// transition described in docs/README.md §4.2 and §4.3.
//
// cmd/jetstream constructs cross-cutting primitives (verifier,
// identity directory, store, HTTP client) and calls Orchestrator.Run.
// The orchestrator reads the persisted lifecycle phase, builds the
// per-phase ingestion subsystems internally, and walks the cutover
// state machine when initial backfill drains. Phase dispatch is
// internal to this package; cmd/jetstream sees one Run that returns
// when ctx is cancelled or the steady-state consumer exits.
//
// Two durable commit points anchor the cutover:
//
//  1. WritePhase(merging): after backfill drains, before any
//     bootstrap teardown.
//  2. WritePhase(steady_state): after merge completes, before
//     starting the steady-state live consumer.
//
// A crash between either commit point and the next durable
// filesystem effect is recoverable on restart by re-entering the
// state machine at the appropriate point. See the spec for the
// exact restart matrix.
//
// Merge (state 5) is currently a no-op; merge.go documents the
// contract its eventual implementation must satisfy.
package orchestrator
