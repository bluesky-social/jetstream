# Go Client Design

Date: 2026-06-18
Status: design / planning
Labels: `client`, `testing`
Related: #77 (oracle: drive historical reads through the real Go client)

## 1. Goal

Ship the public Go client that real-world users import to (a) live-tail the
firehose and (b) complete a full or filtered network backfill that seamlessly
cuts over to live. It must be importable as `github.com/bluesky-social/jetstream`
(root package), be flexible and idiomatic, drag in **only** decode-path code
(no OTEL/Prometheus/pebble/server trees), and be rigorously tested.

A second, equally-important goal: the client becomes a first-class **oracle
observation surface**, replacing the bespoke `/subscribe?cursor=0` whole-archive
replay that issue #77 identifies as both wrong-per-contract and the leading
explanation for scheduled-sweep flakes. Major parts of the oracle's historical
read path get rewritten to drive the real client end-to-end, and the
now-redundant bespoke replay path is deleted.

## 2. Public API surface (high bar; minimal)

Third parties may import **only** the root package and `./segment` (already
public). Everything else lives under `./internal`. The root package re-exports
nothing it doesn't intend to support forever.

### 2.1 Root package `jetstream` (file `client.go`)

```go
// Subscribe opens a client. With no backfill options it live-tails only.
func Subscribe(host string, opts ...Option) (*Client, error)

type Client struct { /* opaque */ }

// Events yields batches in delivery order until ctx is cancelled or a
// terminal error occurs. Range-over-func (Go 1.26 iter.Seq2).
func (c *Client) Events(ctx context.Context) iter.Seq2[*Batch, error]

func (c *Client) Close() error

type Batch struct { /* opaque */ }
func (b *Batch) Events() []Event
func (b *Batch) LastCursor() uint64   // highest seq in the batch; persist this

// Event is the decoded-record contract (v1-JSON-shaped, see §4).
type Event struct {
    DID     string
    Seq     uint64    // jetstream cursor
    TimeUS  int64     // indexed-at, unix micros
    Kind    Kind      // commit | identity | account | sync
    Commit  *Commit   // non-nil iff Kind==commit
    Identity *Identity
    Account  *Account
    Sync     *Sync
}
type Commit struct {
    Operation  string // create | update | delete
    Collection string
    Rkey       string
    Rev        string
    CID        string          // empty for delete
    Record     map[string]any  // decoded; nil for delete
    RecordCBOR []byte          // raw DAG-CBOR, cheap on backfill path; may be nil
}
```

Options (functional, mirror DESIGN.md §2.1):

```go
func WithCollections(c []string) Option // exact NSID or "ns.*" wildcard
func WithDIDs(d []string) Option
func WithAfterSeq(seq uint64) Option     // backfill lower bound (exclusive)
func WithBeforeSeq(seq uint64) Option    // backfill upper bound (inclusive)
func WithLiveCursor(seq uint64) Option   // resume live-only from a saved cursor
func WithBatchSize(n int) Option
func WithDownloadConcurrency(n int) Option
func WithLiveBuffer(b LiveBuffer) Option // optional; default in-memory
func WithHTTPClient(h *http.Client) Option
func WithLogger(l *slog.Logger) Option   // default: discard
```

Live resume is caller-owned: persist `Batch.LastCursor()` and pass it back via
`WithLiveCursor` on the next run (the README example pattern). v1 does NOT
persist backfill progress — a crashed backfill restarts from the plan (segments
are cheap/CDN-cached). Resumable backfill is deferred follow-up work (see §9).

Backfill is **opt-in via the seq/filter options**: a bare `Subscribe(host)` is a
pure live tail from the current tip (or from `WithLiveCursor`). Supplying any of
`WithAfterSeq`/`WithBeforeSeq` (or just wanting "everything since the start",
`WithAfterSeq(0)`) triggers the full archive-negotiation path.

### 2.2 What we do NOT export

- No XRPC types, planner internals, downloader, or cutover machinery.
- No `tombstone.Set` / `overlay.Cache` / `overlay.Source` / `overlay.Blob`
  (server-side mutable machinery; audited out — see §6).
- The decoded record is `map[string]any` (open atproto record). We do not
  expose typed lexicon records from the client; callers who want types decode
  `RecordCBOR` themselves with whatever codegen they use.

## 3. The correctness backbone (read this before coding)

The seq space decomposes into three coverage regions the client must stitch
with **no record gap and no missing suppression**:

| region | covered by | bound |
|---|---|---|
| historical | sealed segments (`getSegment`/`getBlock`) | `seq <= plannedThroughSeq` |
| overlay window | `getTombstones` blob | `(W, M]` |
| live | `/subscribe-v2` | `(cutover, inf)` |

Two numbers that are easy to conflate and **must not be**:

- `plannedThroughSeq` (from `planBackfill`) = the **sealed-archive tip**. This
  is the highest seq whose *record bytes* are downloadable as immutable files.
  (`internal/manifest/plan.go:103` — sealed tip capped by `beforeSeq`.)
- `M` (from `getTombstones`) = the **highest seq folded into the tombstone
  set**, computed over the *entire* live tombstone set including the **active,
  unsealed** segment (`internal/overlay/cache.go:94`, `SnapshotRange(w, ^0)`).
  So `M >= plannedThroughSeq`, and the gap `(plannedThroughSeq, M]` is records
  that live in the active segment and are **not** downloadable via the archive
  endpoints.

### 3.1 Cutover rule (the centerpiece)

The **record-stream** handoff must be at `plannedThroughSeq`, NOT at `M`:

> Start the live `/subscribe-v2` tail from `cursor = plannedThroughSeq`
> (the server delivers `seq > cursor`). The live tail then re-covers
> `(plannedThroughSeq, M]` — the active-segment records the archive could not
> serve — plus everything after. At-least-once + idempotent dedup absorb the
> overlap.

Starting live at `M` (the loose phrasing in DESIGN.md §5) would **silently drop
active-segment creates in `(plannedThroughSeq, M]`**. `M` governs only the
*tombstone-coverage* floor, a separate concern from the record handoff.

To be conservative and lean on at-least-once, the client rewinds the live start
slightly below `plannedThroughSeq` (a small, bounded margin) rather than exactly
at it; duplicates are deduped by `(did, seq)`.

### 3.2 Suppression rule

The client holds the **union** of overlay tombstones `(W, M]` and live-tail
tombstones `(M, inf)` simultaneously, and applies the combined set (max seq per
key) to **every materialization row** it emits from any region. This is exactly
`CheckOverlayReconstruction` in `internal/oracle/overlay.go:52`. A create/update
row is suppressed iff a tombstone (record-key or DID-level) exists at a strictly
higher seq.

Concretely:
1. record tombstone `(did, collection, rkey) -> seq`: suppress materialization
   rows with `row.seq < tombstone.seq`.
2. DID tombstone `did -> seq` (account delete / sync divergence): suppress
   materialization rows for that DID with `row.seq < tombstone.seq`.

Because suppression depends on live-tail tombstones that arrive *after* the
historical rows are downloaded, the client cannot know at decode time about a
delete that has not yet streamed in. **Decision (resolved, was O1): eager
emission + at-least-once.** The client applies the suppression it *already
holds* at emit time (overlay tombstones, plus any live tombstones already
tailed), but does NOT hold a historical row back waiting for a tombstone it has
not seen. A delete/update that arrives later on the live tail is emitted as its
own row, and the consumer applies it idempotently — exactly the documented
firehose contract (DESIGN.md §2.1) and how every real AppView already consumes
the stream. Holding emission until "through M" was rejected: it requires
buffering an entire full-network backfill's emit decisions, and still draws an
arbitrary snapshot line because the live tail produces deletes forever.

### 3.3 Filtering

`planBackfill` is a transport hint with a one-sided contract (no false
negatives, possible false positives: DID blooms and block collection summaries
over-select). The client MUST apply exact DID + collection filtering after
decode, and exact `(afterSeq, beforeSeq]` seq bounds, before emitting. Wildcard
collections (`ns.*`) are matched server-side in the plan and re-checked client
side by prefix.

## 4. Event model (decided: decoded-record struct)

- Backfill rows arrive as `segment.Event` (raw CBOR payload). The client decodes
  CBOR -> `map[string]any` for `Record`, computes `CID` from the payload, and
  keeps `RecordCBOR` (free on this path).
- Live rows arrive over `/subscribe-v2` in **extended mode** (`?extended=true`)
  so both sides carry `seq`, `upstream_relay_cursor`, and `record_cbor`. The
  client decodes the same way, producing identical `Event` values regardless of
  region. One code path, byte-faithful, and the decoded record is what most
  callers want.
- `#sync` events are visible on `/subscribe-v2` (extended) and in the archive;
  they flow through as `Kind==sync`.

## 5. Package layout

```
client.go                         root pkg `jetstream`: Subscribe, Client,
                                  Options, Event/Commit/..., Batch, Kind,
                                  LiveBuffer interface
client_test.go                    public-contract tests (fixture-driven)
internal/client/                  orchestration (unimportable by 3rd parties)
  planner.go                      planBackfill negotiation + plan model
  downloader.go                   bounded-concurrency getSegment/getBlock fetch
  decode.go                       segment.Event/live frame -> jetstream.Event
  suppress.go                     combined overlay+live tombstone application
  cutover.go                      plannedThroughSeq handoff + overlap dedup
  live.go                         /subscribe-v2 extended consumer + buffering
  buffer_mem.go                   default in-memory live buffer
  buffer_file.go                  JSONL file-backed live buffer (fsync cadence)
segment/                          decode core, refactored to drop internal deps
cmd/client                        rewritten to drive the real client; keeps a
                                  raw-websocket --direct mode
```

The root package depends on: `internal/client` -> {`api/jetstream` (gen XRPC),
`segment` decode core, `internal/overlay` + `internal/tombstone` (decode +
suppress; stay internal), `atmos/xrpc`, `atmos/cbor`, `coder/websocket`}. None
of these pull OTEL, Prometheus, pebble, or any ingest/server package. Because
`internal/client` can freely import other `internal/*` packages, overlay and
tombstone decode are reused directly — nothing is promoted to public.

## 6. Dependency + API-surface audit (enabler refactors)

### 6.1 `segment` decode core must shed `internal/*`

`segment` transitively imports `internal/obs` (OTEL+Prometheus) via `metrics.go`
and `internal/crashpoint` via `rewrite.go`. **Neither is on the decode path**
(reader/block/event/blockframe/zstd/scan/select/footer/header/bloom/collection).
Refactor so the decode core has zero `internal/*` imports:
- Move `rewrite.go` (server-only compaction rewrite) and `metrics.go` to a
  sibling server package (e.g. `internal/segmentserver`), OR invert the metrics
  dependency by injecting a tiny metrics interface defined in `segment` and
  implemented in `internal/obs`.
- Decision: prefer **moving the server-only files out** over interface
  injection, since rewrite is genuinely a server concern and the decode core
  should be pure. Confirm no decode-path code references them.

### 6.2 Overlay/tombstone stay internal (decided, was O2)

`internal/tombstone` mixes the server-side mutable `Set` (`Observe`, `Evict`,
`Replace`, `Dirty`, `SnapshotRange`) with the client-relevant immutable
`Snapshot` (`ShouldDrop`, `Merge`, `Empty`) and the keys (`RecordKey`,
`DIDTombstone`). `internal/overlay` mixes the server-side `Cache`/`Source`/`Blob`
with the pure `Decode`/`Encode`.

**Decision: expose nothing. Both packages stay under `internal/`.** Because
`internal/client` is itself internal, it imports `overlay.Decode` and
`tombstone.Snapshot`/`ShouldDrop`/`Merge` directly — no promotion needed. Third
parties never see `tombstone` or `overlay`; the public surface is exactly the
`Client`/`Batch`/`Event` contract. Rationale: keep the public API as small as
possible. Making an API public is a one-way door — easy to widen later, very
hard to retract. If a concrete need for client-side suppression-as-a-library
emerges, we revisit then, exposing only a minimal `Snapshot`/`Decode` surface.

## 7. Live buffer (optional, opt-in)

One small interface; the default is in-memory so casual callers persist nothing.
Serious callers opt into a durable file buffer. This buffer exists regardless of
the (deferred) resume feature: a full-network backfill takes ~16h, during which
tens of millions of live events accumulate — far too many to hold in RAM — so
spilling the live tail to disk during cutover is an operational requirement on
its own.

```go
// LiveBuffer stores live frames received during cutover until they are drained
// and emitted in order. Default: in-memory ring. Provided: JSONL file buffer.
type LiveBuffer interface {
    Append(frames [][]byte) error // batched; impl decides fsync cadence
    Replay(ctx context.Context, from uint64) iter.Seq2[[]byte, error]
    Truncate(throughSeq uint64) error
    Close() error
}
```

### 7.1 File buffer format + fsync cadence (decided, was O3)

- **On-disk format: JSONL** (newline-delimited JSON). Live frames already arrive
  as single-line JSON over `/subscribe-v2` (`json.Marshal` emits no embedded
  newlines), so they are appended verbatim with a `\n` separator — zero
  re-encoding. Crash recovery is trivial: scan complete lines, discard a
  trailing partial line. We deliberately do NOT reuse the segment WAL/CBOR
  format here; the data is already JSON and converting it would be wasted work.
- **Fsync cadence: every 5,000 frames or 5 seconds, whichever comes first.**
  Bounded loss window leans on at-least-once: a crash loses at most the last
  sub-window of buffered frames, which are re-delivered when the live tail
  resumes from the last persisted cursor.

## 7.2 No backfill resume in v1 (deferred)

v1 does **not** persist backfill download progress. A crashed/restarted backfill
re-negotiates the plan and re-downloads (segments are immutable, cheap, and
CDN-cacheable). Live resume is caller-owned via `Batch.LastCursor()` +
`WithLiveCursor`. Resumable backfill (a `CheckpointStore` that tracks per-entry
download progress keyed by `{segment-checksum, block-range}`, with checksum-drift
re-planning) is valuable for very long real-world backfills but is explicitly
out of scope here to keep the initial client small. Tracked as follow-up
(see §10). This also removes the former O4 (re-plan-on-drift / `PlanTooLarge`
on resume) from v1 scope; `PlanTooLarge` is still handled at initial plan time
(surface or narrow the query — never silently truncate).

## 8. Oracle rewrite (subsumes #77)

### 8.1 What changes

- Add a **client-driven historical observation tier**: the oracle constructs a
  real `jetstream.Client` pointed at the simulator-backed server and drives the
  full negotiation (`getTombstones` -> `planBackfill` ->
  `getSegment`/`getBlock` -> cutover at `plannedThroughSeq` -> overlay+filter+
  dedup). The client is used as an **observation surface only**; expected state
  is still derived independently from simulator world + firehose history
  (never client-vs-client).
- **Delete** the bespoke `/subscribe?cursor=0` whole-archive replay used as the
  historical surface (`internal/oracle/subscribe_replay_test.go`'s
  full-archive usage). `/subscribe` remains exercised ONLY for its real role:
  the recent live tail (post-cutover) and dedicated mid-stream / boundary-cursor
  cases (#25). Do not delete the live-tail replay coverage; delete the
  full-archive-as-history misuse.
- **Retain** the direct filesystem/segment observer as a separate storage tier
  so an oracle failure can still bisect server/storage bug vs. client bug.
- Exercise filtered queries (exact DID + collection) through the client and
  reconcile bloom/block-hint over-fetch via the client's exact post-filter.
- Route `CheckOverlayReconstruction`-style coverage through the client path,
  not just direct segment scans.

### 8.2 Sequencing / anti-vacuity

- The client must have its own fixture-based unit/integration tests and be
  reasonably trusted **before** being wired into the 5-minute lifecycle, so a
  lifecycle failure isn't ambiguous between a new client bug and a server bug.
- Anti-vacuity: the client tier must prove it actually downloaded sealed
  segments/blocks (assert `stats`/entry counts > 0 where the scenario has
  sealed data) so a misconfigured plan can't pass by observing nothing.
- Update `testing/mutation/RESULTS.md` after wiring: the client tier should kill
  (or explain) mutants in plan/segment-egress/compaction-refresh paths.

### 8.3 Mutants to (re)consider

Compaction-underneath-the-walk and stale-checksum mutants now have a real
client to catch them. Add/verify mutants that corrupt block-range planning,
overlay `(W,M]` bounds, and cutover cursor selection.

## 9. Open questions

All initial design questions are resolved:

- **O1 (suppression timing): RESOLVED — eager emission + at-least-once** (§3.2).
- **O2 (overlay/tombstone exposure): RESOLVED — keep both internal, expose
  nothing** (§6.2).
- **O3 (live buffer durability): RESOLVED — JSONL, fsync every 5,000 frames or
  5s** (§7.1).
- **O4 (plan staleness on resume): DROPPED from v1** — backfill resume is
  deferred (§7.2), so there is no saved plan to invalidate. `PlanTooLarge` is
  handled at initial plan time only.

## 10. Issue breakdown (label `client`, oracle work also `testing`)

Filed under epic #80:

1. (#81) `segment: drop internal/obs+crashpoint from decode core` (enabler)
2. (#82) `client: root package skeleton — Subscribe, Options, Event, Batch, iter.Seq2`
3. (#83) `client: backfill planner negotiation (planBackfill) + plan model`
4. (#84) `client: bounded-concurrency segment/block downloader + decode`
5. (#85) `client: combined overlay+live tombstone suppression + exact filtering`
6. (#86) `client: /subscribe-v2 extended live tail consumer + buffering`
7. (#87) `client: backfill->live cutover at plannedThroughSeq + at-least-once overlap`
8. (#88) `client: LiveBuffer (in-mem default + JSONL file impl)` — rescoped: no CheckpointStore
9. (#89) `cmd/client: drive the real client; retain raw-websocket --direct mode`
10. (#90) `client: fixture unit/integration + swarm/property tests (ordering, suppression, cutover gap)`
11. (#91) `oracle: client-driven historical tier; delete bespoke /subscribe?cursor=0 archive replay (Closes #77)`

Deferred follow-up (filed separately, not part of the initial client):

- (#93) `client: resumable backfill via optional CheckpointStore (per-entry
  progress, checksum-drift re-plan)` — out of scope for v1 per §7.2.
```
