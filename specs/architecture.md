# Architecture overview (for agents)

This is a fast orientation map for an agent dropping into the codebase: what the big pieces are, how they fit together, and where to read more. It is deliberately high-level and light on detail so it doesn't rot every time a function moves. `docs/README.md` is the authoritative spec — when this file and that one disagree, that one is right, and you should fix this one. Package `doc.go` files are authoritative for their own package's internals.

If you're here to make a change, the useful reading order is usually: this file (get oriented) → `specs/invariants.md` (the rules you can't break) → the relevant `docs/README.md` section → the package `doc.go`.

## What jetstream is

Jetstream is a full-network archive and live-streaming service for atproto. It ingests every record from every repo on a relay, stores it all in a custom columnar file format on a single machine, and lets clients replay that history and then seamlessly follow the live firehose. It runs as one static binary on one server. High availability is a future goal, not something the current design does.

Two things clients can do:

- **Live tail** — connect to the `/subscribe` websocket and get the same filterable JSON payload as Jetstream v1. Existing v1 consumers work unchanged.
- **Backfill then cutover** — ask for historical data (e.g. "all likes since 2024"), page through the sealed archive over HTTP, and cut over to the live websocket when caught up. The client libraries hide the seam.

## The three big subsystems

Everything falls into ingest (data in), storage (data on disk), or serve (data out), with a testing rig that wraps the whole thing.

### Ingest — getting data onto disk

Ingest is a lifecycle state machine that walks through three phases. The orchestrator (`internal/ingest/orchestrator`) owns that machine and the durable commit points between phases; a crash mid-cutover recovers by re-entering the machine at the right spot.

- **Bootstrap** (`internal/ingest/backfill` + `internal/ingest/live`): on first start, two things run in parallel — a live consumer captures the firehose into a temporary `backfill/live_segments/` tree so nothing is missed, while the backfill engine paginates listRepos and downloads every repo via getRepo straight into the archive.
- **Merge** (`internal/ingest/orchestrator`): once backfill drains, the captured live segments are drained into the permanent `segments/` tree, dropping events already covered by each repo's backfilled head, then a tombstone compaction runs so the archive is delete/update-correct before cutover.
- **Steady state** (`internal/ingest/live` again): one live consumer pumps the firehose into `segments/`. On the side, a retry loop re-downloads repos that failed during bootstrap or post-merge listRepos discovery. Live first sighting is not a getRepo trigger; repo-wide repair comes from explicit `#sync`. Compaction runs periodically to clean up deleted and updated records.

The live consumer and backfill both write through a shared `ingest.Writer` (`internal/ingest`), which owns segment append/flush/fsync, seq assignment, and the in-memory readable log the live tail reads from. All upstream data is untrusted: a validation gate at each conversion point drops bad revs, bad op paths, and unrepresentable fields with a labeled metric rather than crashing or corrupting (`docs/README.md` §4.4).

### Storage — the segment format and the metadata store

Two places hold state:

- **Segment files** (`segment/`): the columnar, zstd-compressed, append-only logs. An active segment is a file state machine (append → flush → fsync → seal); sealing finalizes it into an immutable file with a footer full of indexes. The `segment` package is pure format code — no goroutines, no timers, no lifecycle — and is intentionally public API. Read `segment/doc.go` and `docs/README.md` §3.1–§3.2.
- **The metadata store** (`internal/store`, pebble at `data/meta.pebble/`): everything that isn't cheaply re-derivable from segments — the upstream cursor, lifecycle phase, per-DID backfill status, account/sync state, compaction watermark. The manifest is deliberately *not* here; it's just a directory scan plus self-describing file headers. Read `docs/README.md` §3.5.

The durability ordering between these two is the invariant that keeps a crash safe: segment fsync first, pebble commit second. See `specs/invariants.md`.

### Serve — getting data out

- **`/subscribe` websocket** (`internal/subscribe`): pull-based fan-out. Every subscriber runs the same loop and is served from wherever its cursor points — the writer's readable log (the hot tail) for recent events, or the cold reader (a bounded disk walk over sealed segments through a shared block cache) for older cursors. There's no per-client outbound queue, so a slow reader can't blow up server memory. This package carries a lot of deliberate v1 wire-compatibility quirks; `internal/subscribe/doc.go` lists them. This maintains backwards compatibility with the original https://github.com/bluesky-social/jetstream-legacy system. Compression is endpoint-specific: v1 keeps its frozen contract (legacy zstd dictionary + permessage-deflate), while `/subscribe-v2`'s only scheme is dict-zstd negotiated by dictionary ID — deflate is never negotiated on v2. `internal/subscribe/doc.go` has the server-side contract; `specs/client.md` the client side; `specs/notes/2026-07-09-subscribe-compression-cpu-analysis.md` the measured rationale.
- **Compression dictionary endpoint** (`internal/xrpcapi/getzstddictionary.go`): serves the `/subscribe-v2` dictionary as an immutable, CDN-cacheable blob keyed by its embedded zstd dictionary ID. Retrained against live traffic with `just train-subscribe-dict` (`testing/dicttrain`); each retrain embeds a fresh ID and clients recover from rotation in-place (see `specs/client.md`).
- **Archive download over HTTP/XRPC** (`internal/xrpcapi`): the paginated `planBackfill` → `getSegment`/`getBlock` path clients use to pull sealed history before cutover.
- **HTTP plumbing** (`internal/server`): the public listener (default :8080) and opt-in debug listener (commonly :6060) and middleware. Status, health, and metrics live off these (`internal/status`, `internal/obs`).
- **Client library** (`internal/client` and the module root): the "thick" Go client that negotiates the archive, downloads and decodes it in parallel, dedupes by seq, cuts over to live, and recovers from too-old cursors and dictionary rotations. `specs/client.md` is the end-to-end protocol description; `docs/README.md` §5 owns the wire formats.

### Testing rig — the oracle and simulator

This is unusually central to the project, so it's worth knowing even if you're not touching tests.

- **Simulator** (`internal/simulator`): a fake atproto network — PLC, PDS, relay — that generates *real* atproto-shaped bytes (signed commits, CAR blocks, CBOR frames), not mocked structs. Includes adversarial-traffic modes that feed bad-but-bounded input through the honest pipeline.
- **Oracle** (`internal/oracle`): boots a real server against the simulator, drives it through its whole lifecycle, and compares durable output against an independently derived model. It's a high-value bug detector organized into tiers (storage, event-log, replay, crash/restart, and more). A green run proves strong contracts held for one scenario, not universal correctness.
- **Mutation campaign** (`testing/mutation`): curated single-edit bugs that measure the oracle's bug-detection power. `specs/oracle.md` is the source of truth for this whole rig — read it before touching any of it.

## Where to look

| I want to understand… | Start here |
|---|---|
| The whole system, authoritatively | `docs/README.md` |
| The rules I must not break | `specs/invariants.md` |
| A term I don't recognize | `specs/glossary.md` |
| Accepted limitations / past mistakes | `specs/gotchas.md` |
| The on-disk segment format | `segment/doc.go`, `docs/README.md` §3.1–§3.2 |
| The metadata store keys | `internal/store`, `docs/README.md` §3.5 |
| The ingest lifecycle / cutover | `internal/ingest/orchestrator/doc.go`, `docs/README.md` §4 |
| Initial backfill | `internal/ingest/backfill/doc.go`, `docs/README.md` §4.1 |
| The live firehose consumer | `internal/ingest/live/doc.go`, `docs/README.md` §4.1, §4.3 |
| The segment writer (append/flush/seal) | `internal/ingest/doc.go` |
| The `/subscribe` websocket + v1 quirks | `internal/subscribe/doc.go`, `docs/README.md` §5 |
| Archive download (planBackfill/getSegment) | `internal/xrpcapi`, `docs/README.md` §5 |
| The client protocol, end to end (negotiate → download → cutover → live, compression, failure modes) | `specs/client.md` |
| The Go client implementation | `internal/client`, module root, `specs/client.md` |
| Wire compression (dict-zstd, dictionary rotation, retraining) | `specs/client.md`, `internal/subscribe/doc.go` |
| Compaction / tombstones | `internal/tombstone`, `docs/README.md` §3.3 |
| Timestamp import | `internal/timestamp`, `docs/README.md` §8 |
| The oracle / simulator | `specs/oracle.md`, `internal/oracle/doc.go`, `internal/simulator/doc.go` |
| The mutation campaign (oracle scorecard) | `specs/mutation.md`, `testing/mutation/RESULTS.md` |
| Coding conventions, workflow, task tracking | `AGENTS.md` |
| Design history / why a thing is the way it is | `specs/notes/` (dated design + implementation notes) |

`specs/notes/` is a write-once archive of design and implementation notes, one topic per file, dated. It's the record of how we got here — useful for "why did we do it this way," less so for "what does it do now" (some notes predate later rewrites). The living docs above are the current truth.
