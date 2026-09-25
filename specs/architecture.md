# Architecture overview (for agents)

This file maps the subsystems and their documentation. `docs/README.md` is the authoritative system spec; package `doc.go` files define package contracts. Fix this summary when it disagrees with either.

Before changing code, read this file, `specs/invariants.md`, the relevant section of `docs/README.md`, and the package `doc.go`.

## What jetstream is

Jetstream archives retrievable records from every PDS in a relay’s host roster, then follows the live firehose. Clients can replay the archive and continue streaming. It runs as one static binary on one server; high availability is future work.

Two things clients can do:

- **Live tail** — connect to the `/subscribe` websocket and get the same filterable JSON payload as Jetstream v1. Existing v1 consumers work unchanged.
- **Backfill then cutover** — ask for historical data (e.g. "all likes since 2024"), page through the sealed archive over HTTP, and cut over to the live websocket when caught up. Client libraries handle the transition.

## The three big subsystems

The three subsystems are ingest, storage, and serving. The simulator and oracle test them together.

### Ingest — getting data onto disk

The orchestrator (`internal/ingest/orchestrator`) manages three phases and their durable checkpoints. After a crash, it resumes from the last checkpoint.

- **Bootstrap** (`internal/ingest/backfill` + `internal/ingest/live`): on first start, two things run in parallel — a live consumer captures the firehose into a temporary `backfill/live_segments/` tree, while the backfill engine reads the relay's listHosts roster, paginates listRepos directly on each PDS, and downloads every repo directly from its PDS. Per-host cursors and terminal state live in `pdshost/<hostname>`.
- **Merge** (`internal/ingest/orchestrator`): once backfill drains, the captured live segments are drained into the permanent `segments/` tree, dropping events already covered by each repo's backfilled head, then a tombstone compaction runs so the archive is delete/update-correct before cutover.
- **Steady state** (`internal/ingest/live` again): one live consumer pumps the firehose into `segments/`. On the side, a retry loop re-downloads repos that failed during bootstrap or post-merge fleet discovery, routing directly by the persisted roster PDS when available. Live first sighting is not a getRepo trigger; repo-wide repair comes from explicit `#sync`. Compaction runs periodically to clean up deleted and updated records.

The live consumer and backfill both write through a shared `ingest.Writer` (`internal/ingest`), which owns segment append/flush/fsync, seq assignment, and the in-memory readable log the live tail reads from. The client-visible steady writer also owns a one-block durable seq lease and the registered-vacancy set used after an unclean exit; bootstrap writers stay unleased because serving is lifecycle-gated until merge. All upstream data is untrusted: a validation gate at each conversion point drops bad revs, bad op paths, and unrepresentable fields with a labeled metric rather than crashing or corrupting (`docs/README.md` §4.4).

### Storage — the segment format and the metadata store

Two places hold state:

- **Segment files** (`segment/`): the columnar, zstd-compressed, append-only logs. An active segment is a file state machine (append → flush → fsync → seal); sealing finalizes it into an immutable file with a footer full of indexes. The `segment` package is pure format code — no goroutines, no timers, no lifecycle — and is intentionally public API. Read `segment/doc.go` and `docs/README.md` §3.1–§3.2.
- **The metadata store** (the `internal/metastore` interface; local mode is `internal/metastore/pebblestore`, pebble at `data/meta.pebble/`): everything that isn't cheaply re-derivable from segments — the upstream cursor, lifecycle phase, durable seq coverage frontier, write-ahead seq lease and registered vacancies, per-DID backfill/PDS status, durable PDS roster and host-local cursors, account/sync state, compaction watermark. The manifest is deliberately *not* here; it's just a directory scan plus self-describing file headers. Read `docs/README.md` §3.5.

The durability ordering between these two is the invariant that keeps a crash safe: segment fsync first, pebble commit second. See `specs/invariants.md`.

### Serve — getting data out

- **Subscribe websockets** (`internal/subscribe`): pull-based fan-out behind two endpoints — `/subscribe` (legacy v1 wire, frozen for backwards compatibility with the original https://github.com/bluesky-social/jetstream-legacy system; deliberate compatibility quirks are listed in `internal/subscribe/doc.go`) and `/xrpc/network.bsky.jetstream.subscribeEvents` (v2, atproto proposal-0015 xrpc.v1.json framing declared by `lexicons/network/bsky/jetstream/subscribeEvents.json`, server-push only, three orthogonal kinds/dids/collections filters). Every subscriber runs the same pull loop and is served from wherever its cursor points — the writer's readable log (the hot tail) for recent events, or the cold reader (a bounded disk walk over sealed segments through a shared block cache) for older cursors. There's no per-client outbound queue, so a slow reader can't blow up server memory. Compression is endpoint-specific: v1 keeps its frozen contract (legacy zstd dictionary + permessage-deflate), while v2's only scheme is dict-zstd negotiated by dictionary ID — deflate is never negotiated on v2. `internal/subscribe/doc.go` has the server-side contract; `specs/client.md` the client side; `specs/notes/2026-07-09-subscribe-compression-cpu-analysis.md` the measured rationale.
- **Compression dictionary endpoint** (`internal/xrpcapi/getzstddictionary.go`): serves the v2 subscribe dictionary as an immutable, CDN-cacheable blob keyed by its embedded zstd dictionary ID. Retrained against live traffic with `just train-subscribe-dict` (`testing/dicttrain`); each retrain embeds a fresh ID and clients recover from rotation in-place (see `specs/client.md`).
- **Archive download over HTTP/XRPC** (`internal/xrpcapi`): the paginated `planSnapshot` → `getSegment`/`getBlock` path clients use to pull sealed history before cutover.
- **HTTP plumbing** (`internal/server`): the public listener (default :8080) and opt-in debug listener (commonly :6060) and middleware. Status, health, and metrics live off these (`internal/status`, `internal/obs`).
- **Client library** (module-root `jetstream` package): the Go client that negotiates the archive, downloads and decodes it in parallel, dedupes by seq, cuts over to live, and recovers from too-old cursors and dictionary rotations. `specs/client.md` is the end-to-end protocol description; `docs/README.md` §5 owns the wire formats.

### Testing rig — the oracle and simulator

The test rig checks storage and delivery across the full lifecycle.

- **Simulator** (`internal/simulator`): a fake atproto network — PLC, a skewed fleet of virtual PDSes, and an incomplete-roster relay — that generates *real* atproto-shaped bytes (signed commits, CAR blocks, CBOR frames), not mocked structs. Includes per-PDS lifecycle faults and adversarial-traffic modes that feed bounded malformed input through the production pipeline.
- **Oracle** (`internal/oracle`): boots a real server against the simulator, drives it through its whole lifecycle, and compares durable output against an independently derived model. Its checks are organized into tiers (storage, event-log, replay, crash/restart, and more). A green run proves strong contracts held for one scenario, not universal correctness.
- **Mutation campaign** (`testing/mutation`): curated single-edit bugs that measure the oracle's bug-detection power. `specs/oracle.md` is the source of truth for this whole rig — read it before touching any of it.

## Where to look

| I want to understand… | Start here |
|---|---|
| The whole system, authoritatively | `docs/README.md` |
| The rules I must not break | `specs/invariants.md` |
| A term I don't recognize | `specs/glossary.md` |
| Accepted limitations / past mistakes | `specs/gotchas.md` |
| The on-disk segment format | `segment/doc.go`, `docs/README.md` §3.1–§3.2 |
| The metadata store keys | `internal/metastore`, `docs/README.md` §3.5 |
| The ingest lifecycle / cutover | `internal/ingest/orchestrator/doc.go`, `docs/README.md` §4 |
| Initial backfill | `internal/ingest/backfill/doc.go`, `docs/README.md` §4.1 |
| The live firehose consumer | `internal/ingest/live/doc.go`, `docs/README.md` §4.1, §4.3 |
| The segment writer (append/flush/seal) | `internal/ingest/doc.go` |
| The `/subscribe` websocket + v1 quirks | `internal/subscribe/doc.go`, `docs/README.md` §5 |
| Archive download (planSnapshot/getSegment) | `internal/xrpcapi`, `docs/README.md` §5 |
| The client protocol, end to end (negotiate → download → cutover → live, compression, failure modes) | `specs/client.md` |
| The Go client implementation | module root, `specs/client.md` |
| Wire compression (dict-zstd, dictionary rotation, retraining) | `specs/client.md`, `internal/subscribe/doc.go` |
| Compaction / tombstones | `internal/tombstone`, `docs/README.md` §3.3 |
| The oracle / simulator | `specs/oracle.md`, `internal/oracle/doc.go`, `internal/simulator/doc.go` |
| The mutation campaign (oracle scorecard) | `specs/mutation.md`, `testing/mutation/RESULTS.md` |
| Coding conventions, workflow, task tracking | `AGENTS.md` |
| Design history / why a thing is the way it is | `specs/notes/` (dated design + implementation notes) |

`specs/notes/` records past designs and implementation decisions. Use the living docs above for current behavior.
