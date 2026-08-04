# PDS-Direct Backfill Design (listHosts → per-PDS listRepos)

**Author:** jcalabro + claude
**Date:** 2026-08-03
**Status:** Implemented 2026-08-03 (production-scale tuning remains operational validation)
**Tracks:** docs/README.md §4.1, §4.2 (merge discovery), §4.3 (steady-state retry)
**Repos touched:** atmos (`~/go/src/github.com/jcalabro/atmos`, branch `jc/backfill`) — the strategy lives here; jetstream (this repo, branch `jc/backfill`) — persistence + handler + lifecycle integration.

## 1. Overview

Today the bootstrap backfill enumerates the network by paginating
`com.atproto.sync.listRepos` on the upstream relay and downloading every repo
via `com.atproto.sync.getRepo` through the relay's 302 redirect. This has two
structural problems:

1. **Incomplete discovery.** The bsky.network relay was recreated ~a year ago
   and only knows about accounts that have emitted an event since. Its
   listRepos returns ~18.9M repos; the network holds 45M+ accounts. The
   missing mass sits on the older mushroom PDSes.
2. **Blind scheduling.** Because a DID alone doesn't tell us its PDS, the
   engine accumulates 100k-entry batches and random-shuffles them to spread
   load across hosts statistically. It works, but it's a heuristic standing in
   for information we could simply have.

The redesign: call `com.atproto.sync.listHosts` on the relay to enumerate
every PDS the relay has ever seen, then call `listRepos` **directly on each
PDS**. That yields (a) the complete account list, and (b) the DID→PDS mapping
*at discovery time*, which lets us replace the shuffle with real per-host
scheduling: keep every host's rate-limit bucket saturated for the whole run,
never let one parked host idle global capacity, and download via the PDS
directly (halving request count vs. the relay-redirect path and removing the
relay as a download-path dependency).

**Placement decision (per Jim, 2026-08-03): the whole strategy lives in
atmos.** PDS-direct enumeration and per-host scheduling become *the* backfill
strategy for every atmos consumer, not a jetstream specialization. atmos owns
host enumeration, the host roster lifecycle, per-host scheduling, hostname
validation, and download routing. Consumers own persistence (a `Store` that
now also persists host state) and per-repo handling, exactly as they own
per-DID persistence today. The existing single-relay `atmos/backfill` API is
replaced wholesale — consistent with the package's original redesign doc
("backwards compatibility is not a goal"), and verified: jetstream is the
only extant consumer (witness is not on this machine and nothing else
imports `atmos/backfill`).

## 2. Measured network facts (2026-08-03)

All measured live against bsky.network and individual PDSes; treat as
point-in-time but structurally stable.

- `listHosts` (limit=1000) returns **5,772 hosts** in 6 pages. By status:
  1,820 `active` (22.74M relay-known accounts), 3,669 `offline` (178k), 251
  `idle` (6.7k), 32 `banned` (4.8k). 88 hosts are `*.host.bsky.network`
  mushrooms holding 22.4M relay-known accounts; ~5,684 third-party hosts hold
  ~500k.
- **The relay's `accountCount` is a floor, not a truth.** lepista:
  relay says 125,387; a full direct `listRepos` crawl returns **495,398**
  repos (~3.9x). The lexicon documents this: "the upstream may actually have
  more accounts." Per-PDS enumeration is the only way to get the real roster.
  (It can also overcount post-migration: zio.blue relay=166, actual=47.)
- **Rate limits are per-host and per-endpoint-group.** Mushroom PDSes:
  `listRepos` bucket `3000;w=300` (10 rps), `getRepo` bucket `6000;w=300`
  (20 rps). Standard `ratelimit-*` headers on every response — atmos already
  parses these and does per-host proactive parking.
- **Enumeration is cheap and latency-bound, not rate-bound.** lepista's 497
  pages (limit=1000) took 276s sequential (~1.8 pages/s). Enumerating all
  hosts in parallel is a ~15-minute full-network DID census; it does not
  compete with getRepo for rate budget.
- **PDS listRepos pagination**: cursor is opaque
  (`<createdAtMillis>::<did>`), creation-time ordered, per-host. Pages carry
  `did/head/rev/active[/status]`; observed ~1.1% inactive
  (deactivated/etc.) on lepista page 1.
- **Long-pole math.** The current ~16h bootstrap is the largest mushroom
  (jellybaby, 1.14M relay-known) pinned at exactly its 20 rps getRepo cap
  (1.14M / 16h ≈ 19.8 rps). Wall-clock is bounded below by
  `max over hosts (true accountCount / 20 rps)` — scheduling cannot beat
  per-host limits, it can only avoid wasting them. If the sampled 2-4x
  relay-to-true ratio holds for jellybaby itself, the same arithmetic gives
  **2-4x the current wall clock** (32-64h) while downloading ~2.4x the
  repos; the long pole could also move to a different host whose true count
  is unknown until the census runs. No tighter estimate is defensible before
  step 15 measures the largest true host. (The win is completeness either
  way, and efficiency-per-hour improves because small hosts no longer finish
  early and strand capacity behind the shuffle.)
- Some third-party hosts redirect (`atproto.brid.gy` → 301 → `bsky.brid.gy`);
  the client must follow redirects and attribute rate-limit state to the
  post-redirect host (atmos xrpc already does both).

## 3. Goals and non-goals

### Goals

1. Discover **every account on every host the relay knows about**, not just
   relay-known accounts — as the default behavior of `atmos/backfill` for
   every consumer.
2. Surface the DID→PDS mapping at discovery time (host is a first-class
   argument to the Store callbacks) and use it for download scheduling.
3. Replace the 100k-batch random shuffle with per-host scheduling: per-host
   worker caps sized to the host's rate budget, a global in-flight cap for
   bandwidth, and host-level parking that never blocks other hosts.
4. Per-host `listRepos` cursors with the same durability contract as today's
   single relay cursor: atmos hands a host's cursor to the Store only after
   every repo it covers reached a terminal state; the Store decides how to
   make it durable.
5. Crash/restart resumability at host granularity: a restart re-lists
   `listHosts`, resumes each host from its stored cursor, and skips
   completed repos exactly as today.
6. Keep jetstream's merge-phase new-DID discovery and steady-state
   failed-repo retry correct under the new model, built from the same atmos
   primitives (a discover-only mode; per-host clients).
7. Treat all listHosts/listRepos data as untrusted input **inside atmos**:
   hostname validation, roster caps, drop-don't-crash on malformed entries.
   Every consumer inherits the hardening.

### Non-goals

1. **Discovering hosts the relay has never seen.** If a PDS is not in
   `listHosts`, we can't find it (and the relay isn't federating its events
   anyway). Accepted limitation.
2. **Beating per-host rate limits.** The largest single host bounds
   wall-clock time.
3. **Correct copy selection for mid-migration accounts.** If a stale host
   still lists a migrated DID as active, first-successful-download wins.
   A DID listed by multiple hosts is downloaded once (first host to reach
   it; the Store's Complete state short-circuits the second). Live
   #sync/#identity events correct staleness in steady state. Accepted,
   unchanged in spirit from today's relay-redirect behavior.
4. **Backwards compatibility of the atmos backfill API.** Replaced
   wholesale; jetstream is the only consumer and migrates in lockstep.
5. **Migration of existing data dirs.** Decided 2026-08-03: there are no
   production jetstream deployments; existing bootstrapped dirs are
   discarded and a full backfill runs from scratch on the new code. No
   reconciliation sweep, no in-place upgrade. The only concession is a
   one-line loud-fail guard on the retired cursor key (§6.1) so a stale dev
   dir produces a clear error instead of confusing behavior.

## 4. Architecture: the atmos multi-host engine

`atmos/backfill` is rebuilt around a **fleet of per-host runs behind one
public Engine**. Internally, the proven single-host machinery (producer →
reconcile → batch barrier → worker pool → two-budget retry loop → download
timeout → CAR completeness check) survives nearly unchanged as the per-host
run loop; what's new is the coordinator wrapped around it:

```
Engine.Run(ctx)
 ├─ enumerate: relay listHosts → validate hostnames → Store.OnHost upserts
 ├─ schedule: eligible hosts, big-first, ≤ MaxActiveHosts concurrent
 │    └─ per-host run (existing engine loop, per-host sync.Client)
 │         ├─ listRepos pages from Store-provided cursor
 │         ├─ reconcile → OnDiscover(host, entry) / OnUpdate
 │         ├─ dispatch to per-host workers (≤ HostWorkers)
 │         │    └─ download: acquire global slot → getRepo (direct) →
 │         │       CAR load+CheckComplete → release slot → Handler → OnComplete
 │         └─ batch barrier → Store.SaveHostCursor(host, cursor)
 ├─ host lifecycle: drained | backoff+requeue | exhausted
 └─ terminal: all hosts drained/exhausted AND a final re-list finds no new host
```

Why one Engine wrapping per-host runs, rather than per-host engines managed
by each consumer:

- The strategy — enumeration, eligibility, ordering, backoff, exhaustion,
  the terminal condition — is policy every consumer should share. Leaving it
  to consumers reproduces the divergence this package exists to prevent.
- The retry budgets, batch/cursor barrier, and download-timeout semantics
  compose per-host with zero change; they're reused, not re-derived.
- A worker sleeping on a 429 is nearly free (a goroutine and a timer) as
  long as it doesn't hold a global download slot. Slots are acquired only
  around the download attempt and released before any retry sleep, so a
  rate-parked host never strands fleet capacity.
- Goroutine count is a non-issue: ~1,820 active hosts × small pools, sockets
  bounded by one shared `http.Transport` and the global slots.

### 4.1 Public API sketch (atmos/backfill)

```go
type Options struct {
    // Relay is the sync client for the listHosts source (a relay).
    // Per-host listRepos/getRepo clients are constructed internally
    // via NewHostClient. Required.
    Relay *sync.Client

    // NewHostClient builds the per-host sync client for a validated
    // hostname. None = default: https://<hostname>, shared Transport,
    // xrpc retries disabled (the engine owns retries). Consumers inject
    // this for tests/simulators (http scheme, pipe transports).
    NewHostClient gt.Option[func(hostname string) (*sync.Client, error)]

    Store   Store    // per-DID and per-host persistence. Required.
    Handler Handler  // per-repo callback, unchanged. Required.

    // GlobalDownloads bounds aggregate in-flight download attempts
    // across all hosts (≈ bandwidth). None = 256.
    GlobalDownloads gt.Option[int]
    // HostWorkers caps per-host download workers. The engine sizes each
    // host's pool as clamp(accountCount/10_000, 1, HostWorkers).
    // None = 32.
    HostWorkers gt.Option[int]
    // MaxActiveHosts bounds concurrently-running host loops (mostly the
    // producer-side listRepos sockets). None = 512.
    MaxActiveHosts gt.Option[int]
    // MaxHosts caps the roster (untrusted input bound). Hitting the cap
    // stops listHosts pagination entirely (bounding work, not just
    // memory) and fires OnRosterCapped. The cap is ~9x the observed
    // network (5,772 hosts); consumers must treat OnRosterCapped /
    // roster_cap_hits_total as an alert — a capped crawl is not a
    // complete crawl. None = 50_000.
    MaxHosts gt.Option[int]

    // HostBackoffBase/Max and HostMaxAttempts govern host-level
    // (producer) failures: backoff+requeue, then exhausted.
    // Defaults 1m / 1h / 8.

    // DiscoverOnly skips downloads entirely: enumerate hosts and repos,
    // fire OnHost/OnDiscover/OnUpdate, save cursors. Used for
    // post-bootstrap discovery sweeps. None = false.
    DiscoverOnly gt.Option[bool]

    // IncludeBannedHosts opts banned-status hosts into the crawl.
    // None = false (skip; relay banned them for a reason).
    IncludeBannedHosts gt.Option[bool]

    // Existing per-repo knobs carry over unchanged: MaxRetries,
    // RetryBaseDelay, RetryMaxDelay, RetryRateLimitMaxAttempts,
    // DownloadTimeout, VerifyCommits, Directory, BatchSize (now
    // per-host checkpoint granularity, default 5_000), OnError,
    // OnProgress.
}
```

`sync.Client` gains a `ListHosts(ctx, limit, startCursor)
iter.Seq2[ListHostsPage, error]` iterator mirroring `ListRepos` (the
generated `comatproto.SyncListHosts` and its types already exist).

### 4.2 Store interface (grows host awareness)

```go
type Store interface {
    // Per-DID, as today — but host is now explicit so consumers can
    // record DID→PDS at discovery time.
    Lookup(ctx, did) (StoreEntry, error)
    OnDiscover(ctx, host string, entry sync.ListReposEntry) error
    OnUpdate(ctx, host string, entry sync.ListReposEntry) error
    OnComplete(ctx, did, host string, commit *repo.Commit) error
    OnFail(ctx, did, host string, err error, attempts int) error

    // Host roster. The engine is the source of truth for *strategy*;
    // the Store is the source of truth for *state*.
    OnHost(ctx, host HostInfo) error            // upsert on every (re-)listHosts sighting
    HostCursor(ctx, hostname string) (cursor string, drained bool, err error)
    SaveHostCursor(ctx, hostname, cursor string) error  // only after batch-terminal barrier
    OnHostDrained(ctx, hostname, lastNonEmptyCursor string) error
    OnHostExhausted(ctx, hostname string, err error, attempts int) error
}
```

Deliberate scope note: the engine restores only `(cursor, drained)` across
restarts. Attempt counts and backoff timers are *per-Run* state — a process
restart intentionally resets a host's retry budget (a restart is an
operator action; re-probing an exhausted host is desired, and §6.3 gives
exhausted hosts another attempt anyway). The richer `PDSHost` fields
(§6.1: Attempts, LastError, NextAttemptAt, State) are jetstream
*diagnostics* fed by OnHostState/OnHostExhausted callbacks, not durable
engine inputs. If cross-restart backoff continuity ever matters, that is a
deliberate interface extension, not an oversight.

Contract, mirroring today's cursor invariant: the engine calls
`SaveHostCursor` only after every repo covered by that cursor reached a
terminal state via `OnComplete`/`OnFail`. Durability semantics belong to the
Store (jetstream stages cursors into its writer's durable batch, §6.1). On
crash, a lagging cursor re-lists pages; `Lookup` short-circuits completed
DIDs; at-least-once holds. `HostInfo` carries hostname, relay status,
relay accountCount, and seq.

### 4.3 Scheduling policy (in atmos)

- **Eligibility**: every non-`banned` roster host (offline/idle included —
  the relay's view is stale; probing is cheap and host backoff parks the
  truly dead). `IncludeBannedHosts` opts the rest in.
- **Ordering**: descending relay `accountCount`, so the long-pole mushrooms
  start immediately and small hosts fill spare slots.
- **Host lifecycle**: producer failure (listRepos unreachable/persistent
  5xx) ⇒ backoff+requeue up to HostMaxAttempts ⇒ `exhausted` (reported via
  OnHostExhausted; the run does not block on it). Per-repo failures inside a
  healthy host remain the per-repo loop's business and land `OnFail`, as
  today.
- **Terminal condition**: all hosts drained or exhausted, and one final
  listHosts re-list discovers no new eligible host. `Run` then returns nil:
  "the network as discoverable right now has been enumerated, and every
  discovered repo reached a terminal state." **Caveat consumers must own:**
  an exhausted host's repos were possibly never enumerated at all, so a nil
  return with `Stats.HostsExhausted > 0` is a *degraded* completion. The
  engine deliberately does not block or error on it (a permanently dead
  host must not wedge bootstrap forever — Appendix B.6); the consumer
  decides the policy. jetstream: alert on `hosts_total{state="exhausted"}`,
  surface exhausted hosts on the status page, and re-attempt them in
  merge-phase discovery (§6.3) and subsequent sweeps.
- **Dedup across hosts**: a DID appearing on two hosts (migration windows)
  downloads at most once per Run. Store-level `StateComplete` alone is
  check-then-act and cannot carry this under concurrency, so the engine
  keeps an in-process claims map: the first host's reconcile claims the
  DID and dispatches; a concurrent second host gets a wait-only barrier
  job whose batch (and therefore cursor) cannot complete until the owning
  download reaches a terminal Store transition. A claim that resolves
  non-terminally (owner aborted) fails the waiter's host attempt
  retryably rather than letting its cursor skip the DID. Across Runs,
  `Lookup`/`StateComplete` dedups as before. First-complete-wins is the
  accepted copy policy (§3 non-goal 3).
- The old 100k shuffle disappears: within a single host it has no
  load-spreading value, and cross-host spreading is now explicit.

### 4.4 Hostname validation & SSRF hardening (in atmos)

`listHosts` hostnames originate from arbitrary `requestCrawl` calls — treat
each as adversarial, in the library so every consumer inherits it:

1. Syntax: RFC 1123 DNS hostname, no scheme/path/userinfo/port. (The
   default NewHostClient builds `https://<hostname>`; injected builders may
   relax rules — the simulator uses ports — but injection is an explicit
   consumer act.)
2. Reject literal IPs, `localhost`, single-label names, and
   `.local`/`.internal`/`.lan` suffixes in the default builder.
3. Rejected hostnames are dropped with a callback/metric hook, never fatal.
4. Roster cap (`MaxHosts`) bounds consumer-side state growth from a hostile
   relay.
5. Response bounds: existing xrpc response caps + per-entry drop-don't-abort
   on malformed listRepos/listHosts entries, mirroring `ListRepos` today.
   Honest consequence: a dropped listRepos entry is skipped and the page
   cursor advances past it — if the malformation was transient corruption
   rather than genuinely bad data, that repo is lost until a future
   re-enumeration (fresh bootstrap, or a sweep from cursor ""). This is the
   deliberate crash-loud vs drop-quiet split (Appendix B.2): aborting a
   750-host crawl on one bad entry is worse. The engine surfaces every drop
   via OnEntryError; jetstream must meter it (`list_entry_errors_total`)
   and alert on non-trivial rates rather than treating drops as free.

Resolved during implementation: the default builder enables jttp's strict
SSRF protection (`WithStrictSSRFProtection`), which resolves each initial
hostname at dispatch and refuses loopback/private/link-local/metadata
targets; jttp applies the same IP policy to every redirect hop by default.
Syntax validation is the first gate, not the only one.

## 5. atmos work items (branch `jc/backfill` there)

1. **`sync.ListHosts`** iterator + `ListHostsEntry/Page` types; httptest
   pagination + malformed-entry tests mirroring `TestListRepos_Pagination`.
2. **Hostname validator** + default `NewHostClient` (shared Transport,
   retries off, https) + fuzz tests on the validator (untrusted input).
3. **Engine rebuild**: per-host run loop extracted from the current engine
   (mostly moves), coordinator (enumeration, roster reconcile against
   Store, scheduling, host lifecycle, terminal condition), global download
   slots acquired around the download attempt only, `DiscoverOnly` mode.
   New Store interface. The single-relay code path is deleted.
4. **Tests**: multi-host httptest fleets — relay-gap (host lists repos the
   relay's own listRepos never returned — the headline property), per-host
   cursor resume after kill, host 429 parking not starving other hosts,
   host death mid-enumeration → backoff → recovery, permanent death →
   exhausted → Run still terminates, slots cap honored fleet-wide, slot
   released during retry sleeps, DiscoverOnly fires no downloads,
   cross-host DID dedup. Race-mode (`just test-race`) on all of it.
5. **Docs**: package doc.go rewrite; `docs/superpowers/` design doc for the
   engine rebuild.
6. **Tag** (breaking: new major-ish minor, v0.3.0 given v0.x semantics) and
   bump jetstream's go.mod; temporary `replace` during development, removed
   before merge.

## 6. jetstream work items

jetstream keeps: pebble `Store` implementation, `SegmentHandler`, cursor
durability staging, orchestrator lifecycle, retry pass, observability. It
deletes: its own copy of scheduling/shuffle concerns (there were none — it
already delegated to atmos) and the singleton relay cursor.

### 6.1 Store implementation (`internal/ingest/backfill/store.go`)

- Implement the new host methods over pebble:
  `pdshost/<hostname>` → JSON `PDSHost{Hostname, RelayStatus, RelayAccounts,
  ListReposCursor, LastNonEmptyCursor, Enumerated, ActualAccounts, Attempts,
  LastError, NextAttemptAt, State, FirstSeenAt, UpdatedAt}`. Control-plane
  state, separate from the diagnostic `host/<bucket>` aggregates (unchanged,
  status page).
- `OnDiscover(host, entry)` stamps `RepoStatus.PDS = host` at discovery
  (today PDS/Host are post-download best-effort).
- `SaveHostCursor` **stages** `(host, cursor)` into the existing
  `completionBatcher`; the `StageDurable` hook folds staged cursors into
  the writer's synced pebble batch after the completions they cover. This
  keeps the invariant (cursor never ahead of segment durability) without a
  forced writer flush per host-batch — with hundreds of hosts checkpointing
  every 5k entries, per-batch forced flushes would thrash
  (`forced_checkpoint_flushes_total`). In `DiscoverOnly` runs (no writer),
  cursors write directly with `store.SyncWrites`.
- Migration: `relay/list_repos_cursor` and `bootstrap/last_listrepos_cursor`
  are retired. If the old cursor key exists non-empty at startup, **fail
  loudly** ("old-scheme bootstrap in progress; finish on the old binary or
  restart bootstrap from a fresh data dir"). Steady-state dirs are
  unaffected.

### 6.2 Bootstrap wiring (`run.go`, orchestrator)

`backfill.Run` builds the new atmos `Options` (Relay client, Store, Handler,
knobs from env) and runs the engine; nil return still commits
`phase=merging`. The `MaxRepos`/`BackfillRepos` debug paths stay
jetstream-side as today (`selected.go` untouched; `MaxRepos` becomes a
fleet-level "stop after N terminal repos" cancel — dev knob, precision not
required, documented).

### 6.3 Merge-phase discovery (`merge_discovery.go`)

Becomes an atmos `DiscoverOnly` run against the same Store, with a wrapper
Store whose `OnDiscover` writes the
`StatusFailed("discovered post-bootstrap; queued for retry")` marker rows
for unknown DIDs (downloaded later by steady-state retry — preserving the
"live first sighting is not a getRepo trigger" gotcha). Per-host cursors
resume from `LastNonEmptyCursor`, so the sweep is a few tail pages per host;
new hosts enumerate from "". Exhausted hosts get one more attempt.

Cursor-stability caveat: tail-resume assumes the PDS's creation-time
ordering means new accounts appear after the stored cursor. That matches
observed reference-PDS behavior but is not a lexicon guarantee (the cursor
is formally opaque). Two mitigations: (a) accounts that migrated *onto* a
host (created earlier elsewhere, so potentially inserted before the
watermark) are also announced via live #sync/#identity events, which
jetstream consumes independently; (b) if a host rejects a stored cursor
(4xx), the sweep falls back to a full re-list from "" for that host —
`Lookup` dedup makes that cheap in writes, just not in reads. A periodic
from-"" deep sweep remains available as an operator action if drift is
ever suspected.

### 6.4 Steady-state failed-repo retry (`retry.go`)

Keeps its structure (global pool + per-host semaphores + parking). Changes:
route each download via the DID's recorded `RepoStatus.PDS` using the atmos
host-client builder (shared client cache), falling back to the relay
redirect path when `PDS` is empty (rows from older dirs) **or when the
recorded PDS authoritatively lacks the repo** (RepoNotFound / connection
refused after host-level retries) — the relay 302 tracks migrations the
stamp predates, and a success via the fallback re-stamps `PDS`. `#identity`
events that carry a new PDS endpoint also update the stamp in steady state.
Park/bucket on the roster hostname when present.

### 6.5 Configuration

| Env var | Default | Meaning |
|---|---|---|
| `JETSTREAM_RELAY_URL` | unchanged | listHosts + live firehose + fallback routing |
| `JETSTREAM_BACKFILL_GLOBAL_DOWNLOADS` | 256 | atmos GlobalDownloads (the throughput knob; bandwidth-bound, tune on the box) |
| `JETSTREAM_BACKFILL_HOST_WORKERS_MAX` | 32 | atmos HostWorkers |
| `JETSTREAM_BACKFILL_MAX_ACTIVE_HOSTS` | 512 | atmos MaxActiveHosts |
| `JETSTREAM_BACKFILL_MAX_HOSTS` | 50000 | atmos MaxHosts (roster cap) |
| `JETSTREAM_BACKFILL_BATCH_SIZE` | 5000 | per-host checkpoint granularity (was 100k shuffle batch) |
| `JETSTREAM_BACKFILL_WORKERS` | removed | superseded; startup warns if set |

`.env` gets dev defaults; the oracle/simulator injects `NewHostClient`
rather than a scheme env var, so no production knob exists for http.

## 7. Store schema summary (new/changed keys, jetstream)

| Key | Value | Notes |
|---|---|---|
| `pdshost/<hostname>` | JSON `PDSHost` (§6.1) | new; control-plane roster |
| `repo/<did>` | `RepoStatus` | `PDS` stamped at discovery |
| `relay/list_repos_cursor` | — | retired; non-empty ⇒ loud startup error |
| `bootstrap/last_listrepos_cursor` | — | retired; folded into roster rows |
| `host/<bucket>` | `HostStatus` | unchanged (diagnostics/status page) |
| `backfill/counts` | `Counts` | + `HostsDrained/HostsExhausted` |

## 8. Simulator and oracle (jetstream)

The simulator currently models exactly one host: one listener, relay
listRepos + PDS getRepo from the same origin, PLC docs pointing at
`publicURL`. Required changes:

1. **World**: assign each account a virtual PDS hostname at generation time
   (deterministic from seed; N hosts, default 4, skewed so one host is the
   "big mushroom"). Ground truth exposes DID→host.
2. **simhttp**: add `com.atproto.sync.listHosts` (relay surface). Dispatch
   `listRepos`/`getRepo` on the request `Host` header: the oracle's pipe
   client routes by connection, not name, so `pds0.sim.invalid`,
   `pds1.sim.invalid`, … all reach the same in-process server and the
   handler switches on `r.Host`. Per-host listRepos serves only that host's
   accounts with per-host cursor index spaces.
3. **Relay-gap modeling — the headline correctness property**: the simulator
   relay's own `listRepos` returns only a *subset* of accounts (relay-known,
   with understated per-host accountCounts in listHosts), while per-host
   listRepos returns all. The oracle asserts the archive contains repos the
   relay never listed. This fails on the old code path and passes on the
   new one. (atmos's own httptest suite covers the same property at the
   library level; the oracle proves it end-to-end through segments.)
4. **Faults**: per-host knobs — listHosts flake, per-host listRepos 5xx/429
   (host backoff + parking without starving others), status lies (offline
   but alive, active but dead), death mid-enumeration with recovery (cursor
   resume), permanent death (`exhausted` while bootstrap still terminates).
5. **Restart tiers**: crash mid-bootstrap with hosts in mixed states
   (drained / mid-cursor / exhausted) → per-host resume; crash between
   host-batch terminal states and cursor staging (at-least-once re-list
   direction).
6. **Oracle wiring**: `RelayURL` stays `http://sim.invalid`; jetstream
   passes a `NewHostClient` that builds pipe-transport clients for
   `http://<virtual-host>`. The standalone simulator binary (`:7777`)
   advertises `pds<N>.localhost:7777`-style hostnames (documented caveat)
   or a single-virtual-host mode if that's flaky in CI.
7. **Mutation campaign**: new mutants for the seams — "cursor staged before
   completions durable", "exhausted host treated as drained", "relay-only
   enumeration silently restored", "OnDiscover host mis-stamped" — with
   predicted killing tiers; re-run `just mutation-campaign` on a clean tree.

## 9. Observability

- atmos exposes engine state via callbacks/Stats (hosts by state, repos
  enumerated/completed, slot-wait); it stays metrics-free per its
  conventions. jetstream maps these to Prometheus (namespace `jetstream`,
  subsystem `backfill`): `hosts_total{state}`,
  `host_enumerated_repos_total`, `host_attempts_total`,
  `hostname_rejected_total`, `roster_cap_hits_total`,
  `download_slot_wait_seconds` histogram (the "bandwidth- or rate-bound?"
  signal), `engine_active_hosts` gauge, existing counters unchanged.
  **No per-hostname labels** (5,772-way cardinality); a
  `host_class` label (mushroom/third-party) is cheap and useful. Per-host
  detail lives in roster rows + status page.
- Status page: hosts drained/exhausted/in-flight, top-N by remaining
  estimate, relay-floor vs. enumerated counts as they become known.
- Tracing: `obs.Span` around host lifecycle transitions with `hostname`
  attributes; per-repo spans unchanged.

## 10. Ordered implementation plan

Each step lands green before the next (`just` in the respective repo, plus
listed extras).

**atmos:**
1. `sync.ListHosts` (+tests).
2. Hostname validator + default host-client builder (+fuzz).
3. Engine rebuild: per-host run extraction, coordinator, new Store
   interface, global slots, DiscoverOnly. (`just test-race`.)
4. Multi-host fleet test suite (§5.4 list, incl. relay-gap).
5. Docs + tag v0.3.0.

**jetstream:**
6. go.mod bump (temporary `replace` during dev); Store host methods +
   roster schema + old-cursor loud-fail; discovery-time PDS stamping.
7. Cursor staging via `completionBatcher.StageDurable`; crash-ordering
   tests extended.
8. Bootstrap wiring (`run.go`, orchestrator config plumbing, env vars,
   `.env`).
9. Simulator multi-host world + listHosts + Host-header dispatch +
   relay-gap + fault knobs.
10. Oracle: relay-gap tier, per-host fault/restart tiers.
    (`just test ./internal/oracle`, `just test-long ./internal/oracle`,
    `just oracle-sweep`.)
11. Merge-discovery rework on DiscoverOnly (+oracle merge coverage).
12. Steady-state retry PDS-direct routing (+`just test-long` retry tiers).
13. Status page, metrics mapping; docs pass: docs/README.md §4.1/§4.2/§4.3,
    specs/architecture.md, glossary ("host roster", "mushroom"), gotchas
    (relay accountCount is a floor; old-cursor migration rule); update this
    note to Implemented.
14. Mutation campaign: new mutants (§8.7), `just mutation-campaign` clean
    tree, RESULTS.md.
15. Production validation: fresh bootstrap on real hardware; compare
    enumerated totals vs. https://jetstream.us-east.bsky.network/status;
    record true long-pole host + wall clock here.

## 11. Risks / review flags

- **Engine rebuild blast radius**: the per-host loop is a move, not a
  rewrite, but the coordinator is new concurrent code (host lifecycle ×
  slots × cancellation). Mitigation: race-mode fleet tests in atmos before
  jetstream touches it; jetstream's oracle then re-proves the whole path.
- **Store interface breadth**: five new methods is real consumer burden.
  Accepted: the roster *is* consumer state (must survive restarts in the
  consumer's store); a split `HostStore` interface adds ceremony for one
  implementor.
- **Writer-durability coupling stays in jetstream**: atmos deliberately
  knows nothing about segment durability; the contract is only "cursor
  handed over after terminal states". The staging trick is jetstream's
  Store implementation detail, where it belongs.
- **~1,800 concurrent hosts on day one**: MaxActiveHosts=512 + slots keep
  sockets/memory bounded; the shared Transport needs
  `MaxConnsPerHost`/idle tuning verified under load.
- **Slots bound downloads, not end-to-end repo memory.** A worker releases
  its global slot after download+parse but *before* Handler/OnComplete, so
  the number of decoded repos held in memory is bounded by total workers,
  not GlobalDownloads. With the realistic fleet (88 mushrooms at the
  32-worker cap, ~1,700 small hosts at 1 worker) that's ~4,500 workers —
  but a worker only holds a decoded repo while the Handler/Store call is
  in flight, so sustained memory tracks Handler throughput, not worker
  count. This is deliberate: scoping the slot around Handler would let a
  stalled writer idle all download bandwidth. Step 15 must measure RSS and
  goroutine count at defaults; if decoded-repo retention is a problem in
  practice, the fix is a second (cheap) semaphore around parse→OnComplete,
  not re-scoping the download slot.

## 12. Decisions log & remaining open questions

Decided (Jim, 2026-08-03):

1. **No reconciliation sweep / no migration.** No production deployments
   exist; re-bootstrap from scratch on the new code. `DiscoverOnly` still
   ships (merge-phase discovery needs it), but no steady-state sweep is
   built in this change.
2. **The strategy lives in atmos.** PDS-direct enumeration + per-host
   scheduling is the backfill strategy for all atmos consumers; the
   single-relay path is deleted, not kept as a mode.

Remaining (implementing agent: use the stated default unless Jim says
otherwise in review; none of these block starting):

1. **Dial-time SSRF guard** (§4.4): resolved — shipped in this change via
   jttp `WithStrictSSRFProtection` on the default host-client builder
   (resolve-then-check on initial requests; redirect IP policy was already
   on by default). No fast-follow needed.
2. **GlobalDownloads default** (256): ship 256, tune from
   `download_slot_wait_seconds` on real hardware during step 15.
3. **Banned hosts**: default-skip with `IncludeBannedHosts` opt-in.
4. **witness**: not on this machine; if it consumes `atmos/backfill`
   elsewhere it takes the new API on its next atmos bump. Flag in the atmos
   release notes.

## Appendix A: code anchors (for the implementing agent)

Everything below was verified on 2026-08-03 on branch `jc/backfill` in both
repos. Line numbers drift; symbol names are the stable handle.

### atmos (`~/go/src/github.com/jcalabro/atmos`)

- `sync/list.go` — `Client.ListRepos(ctx, limit, startCursor)
  iter.Seq2[ListReposPage, error]`: the exact pattern `ListHosts` mirrors.
  Types to mirror live in `sync/sync.go` (`ListReposEntry`,
  `ListReposPage`).
- `api/comatproto/synclisthosts.go` — generated `SyncListHosts(ctx, c,
  cursor, limit)` and `SyncListHosts_Output/Host` **already exist**; do not
  regenerate or hand-edit. Host status constants in
  `api/comatproto/syncdefs.go` (`active|idle|offline|throttled|banned`).
- `backfill/engine.go` — the per-host run loop to extract: `producerLoop`
  (listRepos walk + reconcile + page-aligned batching), `reconcile`
  (Lookup → OnDiscover/OnUpdate → dispatch rules), `finishBatch`/
  `dispatchBatch` (batch barrier + the `rand.Shuffle` to delete),
  `workerLoop`, `processRepo` (two-budget retry: `transientAttempt` vs
  `rlAttempt`), `tryRepo`, `download` (DownloadTimeout, `LoadFromCAR` +
  `CheckComplete`, `errDownloadTimeout` translation). Global slots wrap
  the `e.download` call inside `tryRepo` only — acquire before, release
  immediately after it returns (before HandleRepo/OnComplete and before
  any retry sleep in `processRepo`).
- `backfill/engine.go` sentinels to preserve: `errOnCompleteRecorded`
  (handler ran, OnComplete failed — never OnFail after it),
  `errDownloadTimeout` (terminal for this run, no in-loop retry),
  `ErrEngineAlreadyRan` (engines single-shot).
- `backfill/options.go` — current Options doc comments carry the semantics
  to preserve per-host (retry budgets, DownloadTimeout rationale,
  OnBatchComplete contract). `BatchSize`'s "spread load across PDSes"
  rationale becomes obsolete — rewrite as checkpoint granularity.
- `backfill/store.go` — current `Store` interface + `StoreEntry`/`State`;
  grows the host methods (§4.2). `backfill/handler.go` — unchanged.
- `xrpc/ratelimit.go` — per-host proactive parking keyed by post-redirect
  host; already correct for the fleet. `xrpc/error.go` —
  `parseRateLimit` (RateLimit-* + Retry-After), `Error.Host`.
  `xrpc/transient.go` — `IsTransient`/`IsRateLimited`/`RetryAfter`.
- `xrpc/client.go` — one `Client` per base `Host`; retries disabled via
  `Retry: gt.Some(RetryPolicy{MaxAttempts: gt.Some(1)})` (the engine owns
  retries — same rule per host). `QueryStreamHost` returns the
  post-redirect host.
- Test conventions: `sync/sync_test.go` `TestListRepos_Pagination`
  (httptest stub, multi-page cursors) is the model for ListHosts tests;
  `backfill/backfill_test.go` + `engine_internal_test.go` for engine
  coverage; `retrySleeper` seam for testing backoff without wall-clock.
- Build/verify: `just` (lint+test), `just test-race`, `just lexgen` only if
  lexicons change (they don't for this work).

### jetstream (this repo)

- `internal/ingest/backfill/run.go` — `Config`, `Run`; builds the
  relay `xrpc.Client` (retries off) + `atmossync.NewClient` + engine
  `Options`; `rememberPageCursor`/`saveBatchCursor`/
  `finishCleanEngineDrain` = the cursor plumbing being replaced;
  `recordFatal` = the local-writer crash-loud path to keep. Debug paths:
  `runSelectedRepos` (`selected.go`), `collectLimitedListRepos`.
- `internal/ingest/backfill/store.go` — pebble `Store` impl; `Lookup`'s
  crash-recovery projection of pre-existing `not_started` → treated
  complete (issue #262) must survive; `OnDiscover/OnUpdate/OnComplete/
  OnFail` gain the host param; `hostBucketFromAuthority` stays for the
  diagnostic aggregates.
- `internal/ingest/backfill/completion_batcher.go` — `StageDurable` is the
  hook where staged host cursors join the writer's synced pebble batch;
  the crash-if-checkpoint-excludes-completion guard is the invariant to
  extend to cursors.
- `internal/ingest/backfill/cursor.go` — retire `listReposCursorKey`
  ("relay/list_repos_cursor") and `bootstrapLastListReposCursorKey`; the
  loud-fail guard on the old key lands here.
- `internal/ingest/backfill/status.go` — `RepoStatus` (add discovery-time
  `PDS` stamping), new `PDSHost` schema + `RosterKey` helpers.
- `internal/ingest/backfill/retry.go` — `retryRunner` per-host semaphores
  (`acquireHost`) + parking (`parkHost`) already exist; add PDS-direct
  routing via the host-client cache.
- `internal/ingest/orchestrator/bootstrap.go` — `runBootstrap` wires
  `backfill.Run` + bootstrap-live consumer as errgroup siblings; nil
  return → `writeMergingPhase`. Shape unchanged.
- `internal/ingest/orchestrator/merge_discovery.go` — `runDiscovery`
  becomes the `DiscoverOnly` wrapper (marker rows:
  `StatusFailed("discovered post-bootstrap; queued for retry")`).
- `internal/simulator/http/handler.go` — mounts
  getRepo/listRepos/subscribeRepos/PLC; add listHosts + `r.Host` dispatch.
  `internal/simulator/http/relay_listrepos.go` — integer-index cursor
  model to generalize per-host. `internal/simulator/world/…` — account
  roster gains a host assignment. `faults.go` — per-host fault knobs.
- `internal/oracle/harness_test.go` — pipe-listener transport
  (`newPipeListener`, `simURL = "http://sim.invalid"`, routes by
  connection not host — which is exactly why Host-header dispatch works);
  `RelayURL`/`HTTPTransport` wiring where `NewHostClient` injection lands.
- `cmd/jetstream/main.go` — env flags (§6.5);
  `internal/jetstreamd/options.go` — defaults
  (`DefaultBackfillBatchSize = 100_000` becomes 5_000, workers knobs
  replaced).
- Verify matrix (AGENTS.md): `just` always; `just test-long
  ./internal/oracle` + `just oracle-sweep` + `just fuzz 30s ./segment` for
  ingest/orchestrator changes; `just mutation-campaign` on a clean tree
  after; `just bench ./segment` untouched (no hot-path changes expected).

## Appendix B: behavioral contracts that must not change

A checklist for review of the finished work; each is load-bearing today.

1. **Cursor-durability ordering** (specs/invariants.md): a persisted
   listRepos cursor (now per-host) never covers a repo whose completion is
   not durable in both pebble and the segment files. Direction of failure:
   cursor lags, work re-lists, `Lookup` dedups. Never leads.
2. **Crash-loud vs. drop-quiet split**: writer/pebble/fsync errors abort
   the run (jetstream `recordFatal`); malformed upstream data (hostnames,
   listRepos entries, CARs, over-wide fields) drops with a metric and
   never crashes or aborts a host, let alone the process.
3. **`errOnCompleteRecorded`**: after Handler success + OnComplete failure,
   no OnFail — the DID is partially-complete, not failed.
4. **DownloadTimeout is terminal-for-this-run**, not in-loop retried; 429s
   honor server reset (clamped to the 330s ceiling) on a budget separate
   from transient retries.
5. **"Live first sighting is not a getRepo trigger"** (specs/gotchas.md):
   only listRepos-derived rows (bootstrap or discovery marker rows) are
   download-eligible; live-firehose first sightings never create eligible
   `repo/<did>` rows.
6. **Engine nil return still means** "everything discoverable reached a
   terminal state" — the orchestrator commits `phase=merging` on it, so
   exhausted hosts must be excluded from the definition explicitly (they
   are: reported, metered, not blocking).
7. **At-least-once everywhere**: re-listing, re-downloading, and duplicate
   events across crash boundaries are all legal; ordering per DID within
   the archive is what matters.
8. **`Lookup` state projection quirks** (jetstream store.go, issue #262):
   pre-existing `not_started` rows from a crashed run project as complete
   to the engine and are picked up by pending-retry, not re-downloaded in
   the main pass.
