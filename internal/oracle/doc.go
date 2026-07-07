// Package oracle is jetstream's end-to-end correctness harness. It boots a
// real jetstream server against a simulated atproto network, drives it through
// its whole lifecycle, and compares what jetstream durably produced against an
// independently derived model of what it should have produced. It is a
// high-value bug detector, not a proof of correctness: a green run means one
// set of strong contracts held for one scenario. specs/oracle.md is the source
// of truth for the design; read it before changing anything here.
//
// The moving parts, in the vocabulary the tests use:
//
//   - driver: the code that starts jetstream (pointed at the simulator's relay,
//     PDS, and PLC), walks it through bootstrap → merge → steady-state →
//     compaction → shutdown/restart, and gates each step on durable acks rather
//     than sleeps.
//   - observers: the surfaces that collect what jetstream produced — segment
//     files read straight off disk, the /subscribe websocket replay, downloaded
//     XRPC archive segments, the real Go client's replay, and durable
//     store/metrics state. Observers never silently stand in for one another; a
//     serving bug is not allowed to pass by falling back to a disk read.
//   - checkers: the code that turns observations into contracts — physical
//     segment invariants, final-state comparison against simulator world state,
//     event-log equivalence, the compaction/tombstone drop rules, and
//     fail-loud fault assertions.
//
// "bubble" refers to a testing/synctest bubble: a Go test scope with a fake
// clock in which the runtime can tell when every goroutine is durably blocked.
// The default lifecycle tier runs inside one so the whole system quiesces
// deterministically without wall-clock waits. Only one bubble may exist per
// process: zstd and other package-global goroutines bind their channels to the
// first bubble that uses them, and a second bubble in the same process
// (go test -count=N) makes the runtime abort with "receive on synctest channel
// from outside bubble". Re-runs and the real-process tiers therefore each get
// their own process; TestOracle_DefaultLifecycle owns the one bubble.
//
// "tier" is a family of checks that share helpers but fail with a distinct
// explanation (storage/final-state, event-log, client-driven historical,
// live-tail replay, XRPC egress, crash/restart, store-fault, simulator
// fidelity, real-data corpus, soak, determinism experiments). Keeping them
// separate is deliberate: one giant oracle test would be unreadable and
// undebuggable. specs/oracle.md enumerates the tiers and what each can and
// cannot prove.
//
// The oracle follows the fail-loud-over-corrupt rule in the opposite direction
// from production: the daemon must never crash on bad upstream input, but the
// oracle crashes loudly on invalid internal state, persistence corruption, and
// anti-vacuity failures (a scenario that passed without actually exercising the
// path it claims to). Every injected fault must be proven to have fired.
package oracle
