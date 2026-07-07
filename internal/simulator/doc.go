// Package simulator is the parent of jetstream's local atproto network
// simulator. It has no code of its own; it exists to group and document the
// three subpackages that together stand in for the upstream network during
// local runs and oracle tests:
//
//   - world: the deterministic, seeded source of truth. It owns the pebble db,
//     the account roster, repo/MST state, and the traffic generator that
//     produces real atproto-shaped bytes — signed commits, CAR blocks, CBOR
//     firehose frames, account and sync events — rather than mocked structs.
//     It also serves as the independent expected-state model the oracle checks
//     jetstream against, and includes the adversarial-traffic modes that feed
//     bad-but-bounded input through the honest pipeline.
//   - http: the network surface in front of the world — a fake PLC, PDS, and
//     relay (listRepos, getRepo, the subscribeRepos firehose websocket) plus
//     the fault-injection knobs (HTTP status, CAR truncation, disconnects).
//   - fanout: the in-memory pub/sub that delivers the world's generated
//     firehose events to connected relay-subscribe websocket clients.
//
// cmd/simulator wires these together into the standalone dev simulator on
// :7777; the oracle wires them into an in-process harness. See specs/oracle.md
// for how the simulator and oracle fit together and why the world is a real
// byte generator rather than a set of fakes.
package simulator
