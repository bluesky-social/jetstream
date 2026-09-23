// Package simulator documents the local atproto network used by development
// runs and oracle tests.
//
// world generates seeded accounts, repos, signed commits, CARs, and firehose
// frames, and supplies independent expected state to the oracle. Adversarial
// modes produce bounded malformed input through the same encoding paths.
//
// http serves PLC, PDS, and relay endpoints with configurable HTTP, CAR, and
// websocket faults. fanout distributes generated firehose events to relay
// subscribers.
//
// cmd/simulator serves the standalone network on :7777. The oracle runs it
// in-process; see specs/oracle.md.
package simulator
