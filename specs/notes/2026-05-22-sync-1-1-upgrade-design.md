# Sync 1.1 Upgrade Design

**Date:** 2026-05-22
**Branch context:** `live-firehose-consumer` (extends the live consumer landed
in `docs/superpowers/specs/2026-05-21-live-firehose-consumer-design.md`).

## Goal

Upgrade `github.com/jcalabro/atmos` from v0.0.16 → v0.1.0 and turn on Sync 1.1
verification end-to-end in the livestream consumer. After this PR, every
`#commit` and `#sync` event reaching the segment writer has been:

1. Signature-verified against the account's atproto signing key,
2. Chain-checked against the locally-tracked per-DID `(rev, MST root)`,
3. Inversion-validated against the commit CAR, and
4. Hosting-tracked from `#account` events (so consumers downstream of the
   archive can filter by takedown status — we do not gate the archive itself).

All four checks share durable per-DID state in our existing pebble database.
A pebble-backed `identity.Cache` keeps DID-document resolutions across
restarts so the verifier doesn't replay the entire firehose's worth of
plc.directory lookups on every cold start.

The writer-side durability invariant from the live-firehose PR is preserved
unchanged: the persisted relay cursor never exceeds the latest event durable
in a sealed segment block.

## Non-goals

- No segment-format change. Resync ops map to `segment.KindCreate` (decided
  during brainstorming); no new `segment.Kind` is introduced. The archive
  records the post-resync state of each record exactly the way it would record
  a new commit creating that record.
- No backfill changes. The backfill engine continues to use `sync.Client`
  for `listRepos` / `getRepo`, but does not run a verifier — backfill snapshots
  are accepted as ground truth at boot, and the verifier picks up
  chain-tracking from the first `#commit` it sees per DID.
- No metrics overhaul. Existing livestream metrics continue to fire.
  Verifier-specific counters (resyncs triggered, signature failures,
  buffer overflows) will surface in a follow-up.
- No HostingGate path. We track `#account` state but do not drop events on
  it. The archive must record takedowns happening, not become invisible to
  them.
- No backwards compatibility. atmos v0.0.16 → v0.1.0 is a hard bump; the
  legacy "no verifier, no resync" code path comes out.

## Reference

`/home/jcalabro/go/src/github.com/jcalabro/atp` is a minimal firehose
consumer that exercises Sync 1.1 against a SQLite-backed StateStore. Its
`subscribe.go` is the closest published shape to what this PR produces, and
`state.go` is a useful field-by-field crib for the StateStore interface. We
diverge in three ways:

- We persist state in pebble, alongside our existing per-DID metadata, not
  in a separate SQLite database.
- We do NOT use `streaming.Options.CursorStore`. Our cursor advance is
  block-flush-anchored (see "internal/livestream/Config" below).
- We persist `*identity.Identity` resolutions in pebble too, so a process
  restart doesn't replay millions of plc.directory lookups.

## Architecture

```
                     cmd/jetstream
                          │
         ┌────────────────┼────────────────┐
         │                │                │
   *store.Store    *identity.Directory   relay URL
         │                │                │
         │      ┌─────────┴────────┐       │
         │      │                  │       │
         │  identitycache       *xrpc.Client
         │  (identity.Cache)        │
         │      │                  │
         ├──────┘                  ▼
         │                     *sync.Client
         │                         │
         ▼                         │
   syncstate                       │
   (sync.StateStore)               │
         │                         │
         └──────────┬──────────────┘
                    ▼
              *sync.Verifier
                    │
                    ▼
           streaming.Options.Verifier
                    │
                    ▼
            livestream.Consumer
                    │
                    ▼
            ingest.Writer → segments
```

`*store.Store` is constructed once in `runServe`, then handed to:

- `syncstate.New(store)` → `*PebbleStateStore` (implements `sync.StateStore`).
- `identitycache.New(store)` → `*PebbleCache` (implements `identity.Cache`).
- `livestream.Open(...)` (already takes a `*store.Store`) for cursor and
  segment-seq counters.

Both new packages own their own pebble key prefix (`sync/chain/`, `sync/host/`,
`sync/identity/`). They never touch keys outside their prefix. This is the
same pattern as `lifecycle/phase.go` and `livestream/cursor.go`.

`*sync.Verifier`, `*sync.Client`, and `*identity.Directory` are constructed
in `cmd/jetstream/main.go` from the inputs above and passed into
`livestream.Open` via new `Config` fields. Construction lives at the cmd
boundary so:

- The same primitives are reusable from a future steady-state consumer
  (post-merge) without rebuilding the dependency graph.
- Tests can construct fakes for any of these without standing up real PLC
  resolution.

## Components

### `internal/syncstate` (new package)

Implements `sync.StateStore` against a `*store.Store`. Three method pairs:

- `LoadChain(ctx, did)` / `SaveChain(ctx, did, state)` over key
  `sync/chain/<did>`, value = MarshalCBOR of `ChainState` (rev string + data
  cbor.CID).
- `LoadHosting(ctx, did)` / `SaveHosting(ctx, did, state)` over key
  `sync/host/<did>`, value = MarshalCBOR of `HostingState` (active bool +
  status string + seq int64 + time string).
- `Delete(ctx, did)` removes both keys atomically via `pebble.Batch`.

Encoding choice: hand-rolled compact binary (varints + length-prefixed
strings + 33-byte raw CID via `cbor.CID.Bytes()` / `cbor.ParseCIDBytes`),
wrapped in a tiny version-byte header for forward-compat. Justification:
`ChainState` and `HostingState` are both small fixed-shape records, and we
prefer a schema we control over chasing CBOR encoding helpers across atmos's
surface. Round-trip tests pin the format.

For `ChainState` specifically, `cbor.CID` may be the zero value
(`!Defined()`) — that signals "no chain state yet." We refuse to round-trip
zero CIDs through `Save`: the `StateStore` contract returns `(nil, nil)` for
absent state, and an explicit zero-CID save would be ambiguous on read.
A defensive check in `SaveChain` rejects the zero CID with an error.

`pebble.Sync` is used on every Save. Verifier failures often correlate with
crashes (a bad commit might wedge a worker), and we cannot afford to silently
revert per-DID chain state to a pre-crash value.

`Load` returns `(nil, nil)` for absent keys per the `StateStore` contract.
`pebble.ErrNotFound` is the only "absent" signal; any other error propagates.

A swarm test exercises the round-trip property under random
chain/hosting/delete sequences.

### `internal/identitycache` (new package)

Implements `identity.Cache` against a `*store.Store`. Stores
`*identity.Identity` as JSON under key `sync/identity/<did>` plus a TTL byte
prefix. Get returns false for absent keys, expired entries, or any decode
error (treats decode failure as cache miss — the verifier will re-resolve and
overwrite the bad row on Save).

TTL is encoded inline (8-byte big-endian unix-nano expiry) so we don't need
a parallel index. Default TTL: 6 hours, matching atmos's
`InMemoryDirectoryTTL`. Configurable via constructor.

Capacity bound: none. The atproto network has tens of millions of DIDs;
capping by count would force LRU eviction logic in pebble where the storage
layer's natural compaction already keeps the working set bounded. Operators
who care about disk usage can run `pebble compact` against the prefix or
add a sweep tool later.

JSON over CBOR because `identity.Identity` has no MarshalCBOR helper but
its fields are all JSON-friendly types from `encoding/json`'s perspective —
no time.Time, no interface{}, no unexported fields. We don't synthesize a
serialization shape that's not already on the type.

A unit test pins the round-trip across the four sub-types (DID, Handle, Keys,
Services). A second test verifies expired entries are treated as cache misses.

### `internal/livestream/Config` (modified)

New required fields:

- `Verifier *sync.Verifier`: the configured Sync 1.1 verifier. Required;
  validation rejects nil. The package's purpose is now Sync 1.1.

The pre-existing `SyncClient: gt.Some[*atmossync.Client](nil)` line in
`livestream.Open` — which disabled the streaming layer's auto-resync — is
removed. With our verifier supplied, `streaming.Client` does NOT auto-attach
its own verifier; auto-resync is driven entirely by ours. The streaming
layer's `DisableAutoResync` flag stays false (default), so `#sync` events
still flow into our `ConvertEvent` for archival.

`livestream.Open` passes `streaming.Options.Verifier = gt.Some(cfg.Verifier)`.
The pre-existing `SyncClient: gt.Some[*atmossync.Client](nil)` line — which
disabled auto-resync — is removed.

Crucially, we do NOT use `streaming.Options.CursorStore` even though atmos
v0.1.0 added it. The streaming layer's CursorStore writes the cursor every N
events INDEPENDENTLY of segment block flushes. That would violate our
durability invariant — `persisted_cursor ≤ latest_event_durable_in_a_sealed_block`
— because a cursor write could land between an Append and the next flush, so
a crash mid-write would persist a cursor pointing at events that never made
it to disk. Cursor advance stays on the existing `OnAfterFlush` path
through `livestream/cursor.go`, anchored to block flushes. atp uses the
streaming CursorStore because it has no segment writer — it just prints
JSON lines. Our constraint is different.

### `internal/livestream/events.go` (modified)

`actionKind` currently returns an error for `streaming.ActionResync`
("unexpected resync op (sync handling is disabled)"). Change to:

```go
case streaming.ActionResync:
    return segment.KindCreate, nil
```

This is the brainstorming-locked decision: a resync op carries the live
record bytes, and we surface it to the archive as a create. A code comment
notes that resync ops can re-create a record we already had a Create for —
the segment is an event log, not a state table, so duplicate Creates are
acceptable.

The `ConvertEvent` swarm test grows a resync-ops case asserting:
- `Kind == KindCreate`
- `Payload != nil` (resyncs always carry record bytes)
- `Rev == commit.Rev` (the post-resync rev)

### `cmd/jetstream/main.go` (modified)

In `runServe`, after the `metaStore` open and before `livestream.Open`:

1. Derive the relay HTTP base URL from the WebSocket URL (mirror atp's
   `deriveHTTPURL` — wss → https, drop path/query/fragment). The shared
   helper goes in `internal/livestream/url.go` next to the existing
   `deriveSubscribeReposURL`. The atp reference impl is at
   `/home/jcalabro/go/src/github.com/jcalabro/atp/subscribe.go`.
2. Build the xrpc client with bulk-download tuning:
   ```go
   xrpcClient := &xrpc.Client{
       Host:       httpURL,
       HTTPClient: gt.Some(jttp.New(xrpc.BulkDownloadOpts()...)),
   }
   ```
   Justification: getRepo against the largest accounts (~1 GiB CAR) can run
   for minutes. The BulkDownloadOpts triad (30s TTFB, 30s idle, 64 KiB/s
   floor over 60s) keeps slow but progressing streams alive while still
   killing wedged ones. atp uses the same pattern.
3. Build the identity directory:
   ```go
   directory := &identity.Directory{
       Resolver:               &identity.DefaultResolver{},
       Cache:                  identitycache.New(metaStore, identitycache.DefaultTTL),
       SkipHandleVerification: true,  // signing-key-only on the firehose hot path
   }
   ```
   We can't use `identity.NewInMemoryDirectory()` (atp's one-liner) because
   that hard-codes the LRU; we want our pebble cache. Everything else
   matches what NewInMemoryDirectory would have produced.
4. Build the sync client:
   ```go
   syncClient := sync.NewClient(sync.Options{Client: xrpcClient})
   ```
   `sync.Options.Directory` is intentionally omitted. The relay's getRepo
   responds with a 302 redirect to the account's PDS; xrpc.NewHTTPClient
   follows up to 5 redirects, so DID resolution at the sync.Client layer
   is unnecessary. atp does the same. The verifier still gets the Directory
   directly for its signature-verification path.
5. Build the state store: `stateStore := syncstate.New(metaStore)`.
6. Build the verifier:
   ```go
   verifier, err := sync.NewVerifier(sync.VerifierOptions{
       Directory:  directory,
       StateStore: stateStore,
       SyncClient: gt.Some(syncClient),
   })
   ```
   All other VerifierOptions left at default (PolicyResync, LegacyAccept,
   HostingTrack, 5-min future-rev tolerance, 5/min resync rate limit).
7. `defer verifier.Close()` — releases the resync worker pool.
8. Pass `verifier` into `livestream.Open` via the new Config field.

Constructed at the cmd boundary so a future `cmd/jetstream merge` step can
share the same primitives.

### `go.mod` (modified)

`github.com/jcalabro/atmos v0.0.16` → `v0.1.0`. Indirect deps refreshed via
`go mod tidy`.

## Failure modes

### Verifier `OnVerificationFailure`

Wired to a logger that records `(did, error type)` at WARN. Counted via
metric `livestream_verifier_failures_total{kind="..."}` (kind = chain_break /
inversion / signature / etc.). The verifier itself decides whether to
resync, drop, or pass through; this hook is for observability only.

### Verifier `AsyncErrors()`

Drained in a sibling goroutine of the consumer loop. Logs at WARN. Counted
via `livestream_verifier_async_errors_total{kind="..."}`.

Crash on persistent buffer overflow is OUT OF SCOPE for this PR — we'd
want a metric-driven alert before we let the verifier wedge the process.

### Resync worker pool shutdown ordering

Verifier.Close() must run before metaStore.Close(): workers in flight may
hold references to the StateStore. Defer order in `runServe` (LIFO) is:

1. `defer metaStore.Close()` — runs LAST.
2. `defer ingestWriter.Close()` — runs after live consumer.
3. `defer liveConsumer.Close()` — runs after verifier; consumer holds no
   references to the verifier across calls, but the verifier may yield ops
   into the consumer's batch iterator.
4. `defer verifier.Close()` — runs FIRST. Drains worker pool; subsequent
   liveConsumer.Close() then drains its own state cleanly.

We add a single test that races `runServe` with a context cancel and
verifies all four close paths complete without panic or deadlock.

### Signature verification calls plc.directory at runtime

First-sighting events for a DID will block on a plc.directory HTTP round-trip
(~30ms warm, longer cold). With Parallelism=32 (atmos default), this is fine
in steady state but can stall a worker on takeover. We accept this — the
identity cache absorbs the long tail. If it becomes a problem, we'd add a
warmup pass over `repo/<did>` keys in pebble during boot.

## Tests

### Unit / package-level

- `internal/syncstate`: round-trip test per sub-type
  (LoadChain/SaveChain absent + present, LoadHosting/SaveHosting same,
  Delete clears both fields atomically).
- `internal/syncstate`: swarm test (random sequence of save/load/delete
  across N DIDs, asserts the StateStore is observationally equivalent to a
  `map[atmos.DID]ChainState/HostingState` reference impl).
- `internal/identitycache`: TTL expiry, decode-failure-as-miss,
  Get/Set/Delete round-trip.
- `internal/livestream/events_test`: extend `TestConvertEvent_*` table to
  cover ActionResync → KindCreate. Extend the swarm test similarly.

### Integration

- `internal/livestream/consumer_test.go`: existing happy-path test rebuilt
  against the new Config (Verifier injected). Use `sync.NewMemStateStore` for
  state but our `identitycache` against an `httptest.Server` PLC fake. This
  exercises the full Sync 1.1 path including the directory cache layer
  without taking on plc.directory as a test dependency.
- New `consumer_test.go::TestConsumer_Run_VerifierTriggersResync`: scripts a
  fake firehose that emits one valid commit, then a chain-break (commit with
  bad prevData). Asserts that:
  1. The bad commit's ops do NOT reach the segment.
  2. The verifier triggers a resync.
  3. Resync ops DO reach the segment (as KindCreate with the post-resync
     record bytes).
  4. Persisted chain state advances to the post-resync rev.

### cmd/jetstream

- Existing `TestServe_BootstrapsAndShutsDownCleanly` extended to verify:
  1. The verifier reaches its post-resync state for one in-test resync,
  2. `verifier.Close()` runs in the right defer order.

## Implementation order (TDD)

1. `go.mod` bump + `go mod tidy`. CI green at this state requires removing
   the `streaming.Options.SyncClient: gt.Some[*atmossync.Client](nil)` knob
   from `livestream.Open` — under v0.1.0, leaving it nil while no Verifier
   is configured would still leave the streaming layer auto-attaching its
   own (in-memory) verifier, and our pebble-backed verifier wouldn't be
   wired in. The cleanest first commit either removes the line entirely
   (deferring full verifier wiring to step 6) or replaces it with a
   `Verifier: gt.Some[*sync.Verifier](nil)` opt-out so the legacy code path
   is preserved as we land subsequent commits. We pick option (b): zero
   behavior change at the bump commit, full Sync 1.1 behavior at step 6.
2. `internal/syncstate`: package + tests. Self-contained; no other code
   imports it yet.
3. `internal/identitycache`: package + tests. Self-contained.
4. `internal/livestream/events.go`: ActionResync mapping + tests.
5. `internal/livestream/Config`: add Verifier field, drop SyncClient knob,
   update validation, update consumer_test.
6. `cmd/jetstream/main.go`: wire it all together.

Each step is a green-bar commit. The plan document will sequence them as
~7 tasks following the same template as the live-firehose plan.

## Risks

- **plc.directory rate limits on cold start**: 1M+ accounts means the first
  hour of streaming after a fresh deploy will hammer plc.directory. The
  pebble identity cache makes warm restarts cheap. Cold starts are still a
  problem; future work warms the cache from the existing `repo/<did>` keys.
- **Resync storms after a relay desync**: if a relay chain breaks for many
  DIDs at once, the 32-worker pool serializes resyncs. With 5/min/DID limit,
  this is bounded but slow. Acceptable for now; a metric-driven dashboard
  is the right escalation tool.
- **State migration on restart**: the existing `live-firehose-consumer`
  branch deploys with an empty `sync/...` keyspace. The verifier accepts
  the first event for each DID as ground truth, so there is no migration
  needed; we get one free pass per DID.
