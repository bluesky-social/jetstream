# Local atproto simulator

## Why

Running jetstream against the real production network is slow: backfill takes
~24 hours, and during that window `/subscribe` returns 503. There's also no
control over the data flowing through the system, so debugging specific
record shapes or load patterns means waiting for them to occur in the wild.

We want a single-process, dead-simple simulator that jetstream can point at
in place of `bsky.network` + `plc.directory`, so the local iteration loop
collapses from "hours" to "seconds" and we have full control over what
events the system sees.

## Scope (v1)

In:

- A new `cmd/simulator` binary that simulates PLC, a single PDS, and a relay
  (firehose) under one HTTP listener, using the real production wire formats
  and HTTP paths.
- 10,000 simulated accounts pre-populated with a small initial record set,
  so backfill exercises real CARs and lists across the listRepos pagination
  boundary.
- A live commit generator with realistic distributions (Zipfian per-account
  activity, exponential inter-arrival, weighted action mix and collection
  mix, log-normal post body length).
- Cryptographically valid signed commits — the verifier runs as in
  production.
- Persistent state in pebble at `./data/simulator/`, with a `--reset` flag
  and `just simulator-reset` recipe.
- `.env`-driven default config that makes `just run` point at the simulator
  out of the box; `just run-prod` for occasional smoke tests against the
  real network.

Out of scope (explicit non-goals for v1):

- No fault injection. All emitted events are valid; `#sync` events,
  signature mismatches, MST inversion failures, takedowns, and chain breaks
  are deferred.
- No genesis PLC operations — accounts exist from `t=0`; we don't simulate
  account creation flows.
- No multi-PDS topology. One PDS, advertised in every DID document.
- No `since` diff support on `getRepo`; we always serve the full repo.
- No production-facing surface. Not in the Dockerfile; not shipped to
  users.

## Architecture

A new `cmd/simulator` Go binary lives alongside `cmd/jetstream`. It binds
one HTTP listener (default `:7777`) that serves PLC, PDS, and relay
endpoints at their real production paths — there is no namespacing prefix.
The three URL shapes used by jetstream are:

| Path                                          | Method      | Service | Notes                                                                                |
| --------------------------------------------- | ----------- | ------- | ------------------------------------------------------------------------------------ |
| `/{did}` (path starts with `did:plc:`)        | GET         | PLC     | Returns the DID document. The `atproto_pds` service endpoint is the simulator URL.   |
| `/xrpc/com.atproto.sync.getRepo`              | GET         | PDS     | Streams a CAR via `repo.ExportCAR` (re-signs commit on the way out). Ignores `since`. |
| `/xrpc/com.atproto.sync.listRepos`            | GET         | relay   | Cursor = stringified account index. 1000 entries/page → 10 pages over 10k accounts.  |
| `/xrpc/com.atproto.sync.subscribeRepos`       | GET (WS)    | relay   | Replays events with `seq > cursor` from a persisted ring buffer, then joins live.    |

These paths don't collide: PDS and relay share `/xrpc/` but on different
methods, and PLC's `/did:plc:*` namespace is disjoint from `/xrpc/`.

Internally there's one `*World` object that owns:

- a pebble db at `--data-dir` (default `./data/simulator/`),
- a global `*math/rand/v2.Rand` seeded from `--seed`,
- 10,000 simulated accounts (DID, signing key, current MST root + rev),
- a monotonic firehose seq counter,
- an in-memory pub/sub fan-out from the world's commit-generator goroutine
  to every connected websocket subscriber,
- a bounded historical-events ring buffer for cursor-replay.

Code layout:

```
cmd/simulator/
  main.go              — cli wiring + serve
internal/simulator/
  world/               — pebble-backed world: accounts, repos, RNG, commit generator
    distributions.go   — Zipfian, exponential, weighted-choice, log-normal helpers
    bootstrap.go       — first-run world generation
  http/
    handler.go         — top-level mux dispatching by path
    plc.go             — DID document GETs
    pds.go             — getRepo
    relay.go           — listRepos + subscribeRepos
  fanout/              — connection registry + per-subscriber outbound queue
```

The simulator does not import any production jetstream packages. Its only
significant dependencies are `atmos` (for crypto, repo, MST, CAR, CBOR, and
the wire-format types) and pebble. This one-way dependency keeps simulator
drift from poisoning the production binary, and the `internal/simulator/`
location keeps the code physically separated from production code paths.

## Data model

### Accounts

10,000 accounts (`--accounts`, default 10000). Each is generated
deterministically from the global seed — for account index `i`, a
sub-seed `hash(globalSeed, i)` drives secp256k1 key generation. Same
`--seed` produces the same DIDs and keys across runs.

Per-account fields:

- `did` — `did:plc:<24 base32 chars derived from pubkey hash>`. Format
  matches real PLC DIDs closely enough that `atmos.ParseDID` accepts it.
- `signingKey` — secp256k1 private key.
- `handle` — `user-<i>.test`. Cosmetic; jetstream sets
  `SkipHandleVerification: true`.
- `repoState` — current MST root CID + rev (TID).

### Pebble layout

```
sim/meta/seed                   → uint64       (refuses to start if --seed mismatches stored)
sim/meta/seq                    → int64        (firehose seq counter)
sim/account/<idx>/key           → 32-byte k256 private key
sim/account/<idx>/did           → string
sim/account/<idx>/state         → CBOR { rev, dataCID, recordCount }
sim/account/<idx>/blocks/<cid>  → block bytes  (commit + record + MST node blocks)
sim/account/<idx>/mst/<key>     → block CID    (key→cid index for MST rebuild)
sim/firehose/<seq>              → CBOR-encoded event frame  (ring-buffered)
```

The flat key→cid MST index lets us rebuild a `*mst.Tree` for any account
on demand (O(log n) reads), without holding all 10k trees in memory. An
LRU cache (`--repo-cache=512`) keeps hot accounts resident.

### Initial seed of the world

On first start (or after `--reset`), the bootstrap pass:

1. Generates 10k account keys + DIDs.
2. For each account, creates `--initial-records-per-account` (default 5)
   records with collection chosen via the live-traffic distribution
   (mostly posts and likes), commits + signs them.
3. Persists keys, blocks, MST index, and state.
4. Writes `sim/meta/seq = 0`.

No firehose events are emitted for the initial population — it's the
world's "before time began" state that backfill discovers.

The bootstrap walks ~75k pebble writes. Batched into a single pebble
commit per account, the whole pass finishes in a few seconds. Progress is
logged every 1k accounts.

### Realistic traffic distributions

A `world/distributions.go` file owns the random model. All draws go
through the world's single `*rand.Rand`.

| Property                       | Distribution                                                                          |
| ------------------------------ | ------------------------------------------------------------------------------------- |
| Which account commits next     | Zipfian over N accounts, `s=1.07`                                                     |
| Inter-arrival between commits  | Exponential, mean = `1/--commits-per-sec` (default `--commits-per-sec=10`)            |
| Action choice per commit       | Weighted: 75% create, 15% update, 10% delete                                          |
| Collection choice for creates  | 60% `app.bsky.feed.post`, 20% `feed.like`, 10% `graph.follow`, 5% `feed.repost`, 5% `actor.profile` |
| Post body length               | Log-normal μ=4.0 σ=1.0 (median ~55 chars), clamped to [1, 3000]                       |
| Records per commit             | Geometric p=0.7 (mostly 1, occasionally 2–3)                                          |

`--traffic-rate-multiplier` scales `--commits-per-sec` without touching
the model shape — useful for cheap load tests at e.g. 10× rate.

### Sequence numbers

The firehose `seq` is a single monotonic int64 in pebble, bumped before
every emitted event and persisted in the same write batch as the new
state and ring-buffer entry. At 10 events/sec the per-event pebble write
is irrelevant; if we ever push the rate hard we'll batch.

Resets only on `--reset`.

## Per-service wire behavior

### PLC: `GET /{did}`

Looks up the account by DID, returns:

```json
{
  "id": "did:plc:abc…",
  "alsoKnownAs": ["at://user-42.test"],
  "verificationMethod": [{
    "id": "did:plc:abc…#atproto",
    "type": "Multikey",
    "controller": "did:plc:abc…",
    "publicKeyMultibase": "zQ3sh…"
  }],
  "service": [{
    "id": "#atproto_pds",
    "type": "AtprotoPersonalDataServer",
    "serviceEndpoint": "http://localhost:7777"
  }]
}
```

The PDS endpoint advertised here is the simulator's own listener URL (no
path — atmos's xrpc client appends `/xrpc/...`), so `getRepo` calls round
back to us.

`atmos.identity.DefaultResolver.resolvePLC` builds the request URL as
`plcURL + "/" + did`. Jetstream's new `--plc-url` flag will be set to
`http://localhost:7777`, so the resolver hits `/did:plc:...` on our
listener. 404 for unknown DIDs.

### PDS: `GET /xrpc/com.atproto.sync.getRepo`

Reads `?did=…&since=…` (we ignore `since` in v1 — full repo always).
Loads the account's state + blocks from pebble, rebuilds its `*repo.Repo`
with an LRU-cached `*mst.Tree`, calls `repo.ExportCAR(w, signingKey)`
streamed straight into the response with
`Content-Type: application/vnd.ipld.car`.

`ExportCAR` re-signs the commit on the way out, so the CAR's commit
signature always validates against the pubkey we just published in PLC.
That's the entire crypto story for backfill.

404 for unknown DID.

### Relay: `GET /xrpc/com.atproto.sync.listRepos`

Reads `cursor`, `limit` (capped at 1000 per protocol). Cursor is the
integer index of the last account already returned; `""` = start at 0.
Returns:

```json
{
  "repos": [
    { "did": "did:plc:abc…", "head": "<commitCID>", "rev": "<tid>", "active": true },
    …
  ],
  "cursor": "1000"
}
```

`cursor` is omitted on the last page. With 10k accounts and the 1000-cap
page size, jetstream's backfill engine pages exactly 10 times.

### Relay: `GET /xrpc/com.atproto.sync.subscribeRepos` (websocket)

Upgrades via `coder/websocket`. Reads `?cursor=…` from the query.

1. **Replay phase.** Stream every persisted historical event with
   `seq > cursor` from `sim/firehose/<seq>` in seq order to the consumer.
   The ring buffer is bounded (`--firehose-history`, default 10000
   events); when the consumer's cursor is older than the oldest retained
   event, we send a `#info` frame with name `OutdatedCursor` and start
   from the current seq. (atmos's verifier handles `#info` gracefully —
   we already see the type in `streaming/event.go`.)
2. **Live phase.** Join the fanout: every newly generated event is
   broadcast to all connected subscribers in seq order.

Each subscriber has a bounded outbound channel (default 1024 events).
Overflow closes the connection with `1011 Internal Error` — atmos
reconnects from cursor, and the simulator's behavior matches how real
relays handle slow consumers.

### Firehose frame format

Every event on the wire is two concatenated CBOR values, exactly as
`atmos.streaming.decodeFrame` expects:

```
header: { "op": 1, "t": "#commit" }   (or "#identity" / "#account" / "#sync" / "#info")
body:   <CBOR per the lexicon shape>
```

For `#commit`, the body is a `comatproto.SyncSubscribeRepos_Commit`
whose `Blocks` field is a freshly built CAR diff containing only:

- the new commit block (signed),
- new MST node blocks reachable from the new root,
- the new record blocks created/updated by this commit.

We use a "diff store" (a fresh `mst.MemBlockStore` populated only with
new blocks) so `repo.ExportCAR` produces a diff CAR rather than the full
repo. `Since` is set to the previous rev and `PrevData` to the previous
MST root CID, both tracked in `account/<idx>/state`.

`#identity` and `#account` are emitted occasionally (default: every
~5000 commits, pick a random account) so jetstream's identity and
account paths stay exercised. `#sync` is not emitted in v1 — it's a
recovery mechanism that fault injection would drive.

## Process lifecycle

`cmd/simulator/main.go` runs:

1. Parse flags + env via `urfave/cli` v3, building a slog logger using
   the same `obs.BuildLoggerFromStrings` helper jetstream uses, so log
   format/level matches.
2. Validate `--data-dir` is not exactly `./data` (after `filepath.Clean`).
   Refuse to start otherwise — guards against accidentally pointing the
   simulator at jetstream's pebble dir.
3. If `--reset`, `os.RemoveAll(cfg.DataDir)`. Then `os.MkdirAll(cfg.DataDir, 0o755)`.
4. Open pebble at `cfg.DataDir`.
5. Read `sim/meta/seed`. If absent, this is a first run: store the seed
   and run **world bootstrap**. If present and matches `--seed`, skip
   bootstrap and resume. If present and does not match, hard-fail with
   a message pointing at `--reset` (silent fallbacks are mistakes).
6. Construct the in-memory `*World` (LRU repo cache, fanout registry,
   traffic generator).
7. Wire the HTTP mux.
8. Start three goroutines under an `errgroup.WithContext` rooted at
   `signal.NotifyContext(SIGINT, SIGTERM)`:
   - **HTTP server** on `--addr` (default `:7777`).
   - **Traffic generator**: in a loop, draw next-event delay from the
     exponential, sleep, pick an account (Zipfian), generate a commit,
     persist new state + blocks + ring-buffer entry in one pebble batch,
     broadcast to fanout.
   - **Metrics server** on `--metrics-addr` (default `:7778`) exposing
     `/metrics` from a simulator-owned `prometheus.Registry`. Trace
     setup is skipped in v1.
9. On shutdown: close fanout (closes subscriber connections with
   `1001 Going Away`), close pebble, return.

### Single-writer invariant

Only the traffic-generator goroutine writes to pebble after bootstrap.
HTTP handlers are read-only against pebble. No concurrent-write
coordination on per-account state — every commit is generated, persisted,
and broadcast in one synchronous step before the generator picks the next
event.

### `--reset` scope

`--reset` operates exclusively on the configured `--data-dir`. It never
reaches into `./data` directly. Combined with the startup guard against
`--data-dir == ./data`, the simulator binary cannot touch jetstream's
storage, even if invoked with bad flags.

`just simulator-reset` runs `rm -rf ./data/simulator` literally.

## Configuration surface

All flags follow jetstream's existing convention: `--flag` long form,
`JETSTREAM_SIM_*` env var via `cli.EnvVars`, and a documented default.

| Flag                                | Env var                                  | Default              | Notes                                                          |
| ----------------------------------- | ---------------------------------------- | -------------------- | -------------------------------------------------------------- |
| `--addr`                            | `JETSTREAM_SIM_ADDR`                     | `:7777`              | Public HTTP listener (PLC + PDS + relay).                      |
| `--metrics-addr`                    | `JETSTREAM_SIM_METRICS_ADDR`             | `:7778`              | Prometheus `/metrics` listener.                                |
| `--data-dir`                        | `JETSTREAM_SIM_DATA_DIR`                 | `./data/simulator`   | Pebble db location. Must not equal `./data`.                   |
| `--reset`                           | `JETSTREAM_SIM_RESET`                    | `false`              | Wipe `--data-dir` before opening. Re-bootstraps the world.     |
| `--seed`                            | `JETSTREAM_SIM_SEED`                     | `42`                 | Global RNG seed. World refuses to start if it changes.         |
| `--accounts`                        | `JETSTREAM_SIM_ACCOUNTS`                 | `10000`              | Number of simulated DIDs.                                      |
| `--initial-records-per-account`     | `JETSTREAM_SIM_INITIAL_RECORDS`          | `5`                  | Pre-populated record count per account at bootstrap.           |
| `--commits-per-sec`                 | `JETSTREAM_SIM_COMMITS_PER_SEC`          | `10`                 | Mean event rate from the live generator.                       |
| `--traffic-rate-multiplier`         | `JETSTREAM_SIM_TRAFFIC_RATE_MULTIPLIER`  | `1.0`                | Scales `commits-per-sec` without touching distribution shape.  |
| `--firehose-history`                | `JETSTREAM_SIM_FIREHOSE_HISTORY`         | `10000`              | Ring-buffered events available for cursor replay.              |
| `--repo-cache`                      | `JETSTREAM_SIM_REPO_CACHE`               | `512`                | LRU size for in-memory `*mst.Tree` reconstruction.             |
| `--log-level`, `--log-format`       | `JETSTREAM_LOG_LEVEL`, `JETSTREAM_LOG_FORMAT` | inherits jetstream's | Reuse the existing logging convention.                |

## Jetstream-side wiring

Two changes:

1. **New `--plc-url` flag** on the `serve` command. `Sources:
   cli.EnvVars("JETSTREAM_PLC_URL")`, `Value: ""`. When non-empty,
   `cmd/jetstream/main.go` plumbs it into the resolver:

   ```go
   resolver := &identity.DefaultResolver{}
   if u := cmd.String("plc-url"); u != "" {
       resolver.PLCURL = gt.Some(u)
   }
   directory := &identity.Directory{
       Resolver:               resolver,
       Cache:                  identcache.New(metaStore, identcache.DefaultTTL),
       SkipHandleVerification: true,
   }
   ```

   When empty, falls back to atmos's default of `https://plc.directory`.

2. **`--relay-url`** is unchanged — already a flag with env source.

## Justfile + .env wiring

The repo gains a committed `.env` with simulator-pointing defaults:

```
JETSTREAM_RELAY_URL=http://localhost:7777
JETSTREAM_PLC_URL=http://localhost:7777
```

`.gitignore` stops ignoring `.env` (the file is now intentionally
checked in). A short comment near the top of `.env` notes that it's a
dev-defaults file; no secrets.

The justfile gains `set dotenv-load` and four recipes:

```just
set dotenv-load

# Run jetstream against the local simulator (default).
# Picks up JETSTREAM_RELAY_URL and JETSTREAM_PLC_URL from .env.
run *ARGS:
    go run ./cmd/jetstream {{ARGS}}

# Run jetstream against real production (bsky.network + plc.directory).
run-prod *ARGS:
    JETSTREAM_RELAY_URL=https://bsky.network \
    JETSTREAM_PLC_URL=https://plc.directory \
    go run ./cmd/jetstream {{ARGS}}

# Run the local simulator (PLC + PDS + relay + firehose).
simulator *ARGS:
    go run ./cmd/simulator {{ARGS}}

# Wipe the simulator's pebble db so the next `just simulator` re-bootstraps.
simulator-reset:
    rm -rf ./data/simulator
```

`just`'s precedence is recipe-local env > shell env > `.env`, so the
inline values in `run-prod` cleanly override the `.env` defaults.

The existing `--relay-url` flag default of `https://bsky.network` stays
in the source — it's the safe default if someone runs the binary
directly without `.env`.

## Two-terminal workflow

```sh
# Terminal 1: start the simulator
just simulator
# (first run takes a few seconds bootstrapping 10k accounts; subsequent runs are instant)

# Terminal 2: jetstream points at the simulator by default
just run serve
```

Then:

```sh
websocat ws://localhost:8080/subscribe   # streams events from the local sim
```

Smoke against production:

```sh
just run-prod serve
```

## Cross-binary cursor coherence

Jetstream and the simulator each have their own pebble db (`./data` and
`./data/simulator`). They never share state. The only coordination
concern: if you `just simulator-reset` (re-bootstrapping the world from
seed → seq counter back to 0), you should also `just clean` jetstream,
otherwise jetstream resumes at a cursor that's now in the future. The
simulator handles this gracefully by sending `#info OutdatedCursor` and
starting from current seq, but the user-visible behavior is "events
appear to start from a recent point" rather than from where jetstream
expected.

This asymmetry is documented in the simulator's package doc.

## Observability

The simulator exposes a small set of Prometheus metrics on its own
registry (no shared series with jetstream):

- `simulator_events_emitted_total{kind}` — counter, labels: commit / identity / account.
- `simulator_subscribers` — gauge, current websocket subscriber count.
- `simulator_subscriber_drops_total{reason}` — counter, labels: backpressure / shutdown.
- `simulator_listrepos_calls_total` — counter.
- `simulator_getrepo_bytes_total` — counter, total CAR bytes served.
- `simulator_repo_cache_hits_total`, `simulator_repo_cache_misses_total` — counters.
- `simulator_seq` — gauge, current firehose seq.

slog logging is structured-JSON to stderr by default, with the same
`JETSTREAM_LOG_LEVEL` / `JETSTREAM_LOG_FORMAT` env-var overrides that
jetstream uses.

## Testing strategy

- **Unit tests** for the distribution helpers (Zipfian, exponential,
  weighted-choice), the bootstrap state-shape, the listRepos cursor
  paging math, and the firehose ring buffer.
- **Integration tests** that boot the simulator into an `httptest.Server`,
  point jetstream's existing client code (`atmos.identity.Directory`,
  `atmos.sync.Client`, `atmos.streaming.Client`) at it, and verify:
  - `LookupDID` round-trips for a known account,
  - `getRepo` returns a CAR that decodes via `repo.LoadFromCAR` and
    whose commit signature verifies against the PLC-published key,
  - `listRepos` paginates through all 10k accounts (smaller account
    count in tests, e.g. 25, with the same paging logic),
  - the firehose websocket emits valid CBOR frames whose commit
    signatures verify and whose `Blocks` CAR decodes cleanly.
- **End-to-end smoke test** (extending `cmd/jetstream/serve_test.go`):
  start the simulator, start jetstream pointed at it, wait for backfill
  to drain, connect a websocket client to `/subscribe`, observe events
  flow within a bounded timeout. This is the real test we cared about
  when we set out to build this thing.
- No fuzz tests in v1 — the simulator emits valid data only, and there's
  no untrusted-input boundary on its inside.

## Risks and follow-up work

- **Drift from real network behavior.** The simulator is a synthetic
  approximation of three large external systems. We'll discover gaps
  every time jetstream hits a real-network case the simulator doesn't
  replicate. Mitigation: keep `just run-prod` ergonomic, and add
  simulator coverage for each gap as it's found.
- **Verifier auto-attaches an in-memory directory in atmos's streaming
  layer when no Verifier is supplied.** Jetstream supplies its own, so
  this is a non-issue today. If we ever simplify the live consumer to
  rely on the auto-attached verifier, the simulator's PLC endpoint
  would need to be reachable from that auto-verifier's resolver too.
  Already true today (single-port simulator), but flagged for the
  future.
- **No `since` diff support on `getRepo`.** atmos's verifier currently
  does full-repo resync on chain breaks; if we ever start emitting
  `#sync` events for fault-injection tests, we'll need partial-CAR
  support so the verifier's `since`-based resync round-trips.
- **Single global seq across all accounts** is correct — that's how
  the real relay works — but a future "shard the relay across
  partitions" mode would diverge.
- **Fault injection** (`#sync` events, signature mismatches, MST chain
  breaks, takedowns, slow PDS, listRepos errors) is the obvious v2
  surface. Not here because the goal is "unblock local debugging,"
  not "build a full atproto chaos lab."
- **Trace export.** The simulator skips OTEL setup in v1. If we end up
  debugging cross-process trace propagation, that's the first thing to
  add.
- **Replay from captured production trace.** Higher fidelity than
  procedural generation; requires both a capture tool and a replayer.
  Deferred.
