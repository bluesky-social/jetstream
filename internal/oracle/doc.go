// Package oracle runs Jetstream against a simulated atproto network and
// compares durable output and public replay against independent expectations.
// A passing scenario checks only the paths and interleavings exercised. Read
// specs/oracle.md before changing the harness.
//
// Drivers advance bootstrap, merge, steady state, compaction, and restart
// using durable acknowledgements rather than sleeps. Observers collect
// segments, websocket replay, XRPC downloads, Go client output, and store or
// metrics state. An observer must not substitute a disk read for a failed
// serving path. Checkers validate segment structure, final state, event
// coverage, compaction rules, and failure handling.
//
// The default lifecycle uses one testing/synctest bubble with a fake clock
// and deterministic quiescence. Package-global channels can bind to a bubble,
// so repeated lifecycle runs need separate processes; go test -count=N can
// abort with a channel ownership error. TestOracle_DefaultLifecycle owns the
// bubble.
//
// A tier groups checks with a distinct failure explanation. specs/oracle.md
// lists their coverage and limits. Every fault test must verify that its
// fault fired; invalid internal state, persistence corruption, and
// unexercised scenarios fail the test.
package oracle
