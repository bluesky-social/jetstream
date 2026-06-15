# getTombstones — Compaction Overlay API — Design

**Date:** 2026-06-15
**Status:** Approved, pending implementation
**Author:** jcalabro (with Claude)

## 1. Summary

Ship the final piece of DESIGN.md §3.3 (compaction): a new XRPC query
`network.bsky.jetstream.getTombstones` that serves the in-memory delete/update/
account-deletion tombstone overlay to clients, so a future client can suppress
superseded record rows during backfill replay.

**This task is server-side only**: the lexicon, an XRPC handler, a precomputed
in-memory binary blob, the DESIGN.md update, and tests. There is no Jetstream
v2 client library yet. Everything in this document about *how a client consumes
the overlay* is **documented contract for future work**, not code we write now.
We encode the data so the client suppression rule and the query-plan cursor
selection *can* be implemented later; we do not build those consumers here.

The three data types that compaction must convey to clients collapse to a
single seq-bounded suppression model that already exists in
`internal/tombstone.Snapshot`:

| Source                                            | Tombstone                          | Key    |
| ------------------------------------------------- | ---------------------------------- | ------ |
| Record delete (`KindDelete`)                      | `(did, collection, rkey) → seq`    | AT-URI |
| Record update (`KindUpdate`)                      | `(did, collection, rkey) → seq`    | AT-URI |
| Account delete (`active=false,status=deleted`) / sync (`KindSync`) | `did → (seq, reason)` | DID    |

## 2. Goals & non-goals

### Goals

- Expose the live tombstone overlay covering `(W, M]` (above the compaction
  watermark, up to the highest folded seq) as one atomically-consistent,
  extremely compact binary payload.
- Correctly convey all three compaction data types: record deletions, account
  deletions (delete all records for a DID), and record updates.
- Avoid the **permanent-tombstone problem**: a DID deleted then reactivated must
  not be permanently masked.
- Precompute and hold the payload in memory; serve concurrent readers the same
  immutable bytes with ~zero per-request CPU.
- Update DESIGN.md §3.3 with the wire format and coverage contract so spec and
  code land together.
- Full test pyramid: encoder unit/property/fuzz, cache concurrency, handler
  integration, a compressibility benchmark, an oracle end-to-end determinism
  test, and mutation-campaign mutants.

### Non-goals (future work, documented here, tracked as follow-up issues)

- The Jetstream v2 **client library / decoder**. We pin the format with a test
  decoder that is the reference implementation the future client will mirror.
- The **query plan** negotiated between client and server (which segments to
  download, the cache directives per file, and the `/subscribe` cursor). The
  endpoint reports the numbers the plan needs (`W`, `M`); it does not negotiate.
- CDN caching of this endpoint. Deletes/updates arrive over the firehose at a
  few per second, so `M` advances continuously and the blob changes ~every
  second. This endpoint is **not** expected to be CDN-cacheable in practice; the
  optimization is in-memory precompute + sharing one blob across readers, not
  edge caching. We still emit a strong ETag (nearly free) so the occasional
  coincidental revalidation works.

## 3. Background & the consistency model

### 3.1 The unified suppression rule (FUTURE client behavior, documented)

A client will **suppress any `Create`/`Update` segment row whose `seq` is
strictly less than the matching tombstone's `seq`.** This is exactly
`tombstone.Snapshot.ShouldDrop` today.

Updates carry **no payload** in the overlay. An update writes both the original
`KindCreate` row (seq `S_old`) and a new `KindUpdate` row carrying the new CBOR
(seq `S_new`). The tombstone is `URI → S_new`. The client suppresses the stale
create (`S_old < S_new`) and emits the `KindUpdate` row from its own segment/live
delivery as-is (`S_new` is not `< S_new`). So the overlay stays a pure
key→seq suppression set; DESIGN §3.3 step 7's "emit with replacement payload" is
achieved by suppress-old + deliver-new, not by carrying the payload in the
overlay.

### 3.2 No permanent tombstones

A tombstone masks only rows with `seq < tombstone.seq`. A DID deleted at seq 100
then reactivated and posting again at seq 200 is **not** masked (200 ≥ 100). The
mask is a half-open seq window, never an absolute "this DID is dead" flag. This
is already the semantics in `ShouldDrop` (`ts.Seq > ev.Seq`); we preserve `seq`
faithfully on the wire so the client can honor it.

### 3.3 Coverage contract

The system is internally consistent at all times: compaction rewrites sealed
segments to physically remove superseded rows `≤ compaction/seq`, **then**
advances the watermark, **then** evicts those tombstones from the in-memory set.
So at any instant segments are clean up to `W` and the set holds tombstones in
`(W, tip]`.

The three client data sources stitch together as:

```
segments      cover  ≤ W    (physically compacted; superseded rows removed)
overlay blob  covers (W, M]  (this endpoint — IN SCOPE)
/subscribe    covers (M, ∞)  (query plan sets cursor = M — FUTURE)
```

**In scope now:** the endpoint reports `W` (the compaction watermark at build
time) and `M` (the highest seq folded into the blob) honestly, and serves the
tombstones in `(W, M]`.

**Future (documented contract the query plan must uphold):** the query plan sets
the client's `/subscribe` cursor to the blob's `M`. `/subscribe` replays
`(M, ∞)` from its 36h lookback buffer; `M` is always seconds old, well within the
window. This makes coverage gapless with only idempotent overlap (tombstones take
the max seq, so a tombstone appearing in both overlay and live is a no-op).

### 3.4 The stale-cached-segment hazard (Seam B) — resolved by the query plan

A leak is possible only if a client reads a **segment body older than the
overlay's watermark**: rows `≤ W` are physically removed from current-generation
segments and their tombstones are evicted from the set, so the overlay
deliberately cannot cover anything `≤ W`. If a CDN edge or client cache serves a
pre-compaction generation of a segment within its `Cache-Control: max-age`, a
create whose delete was already compacted+evicted would appear with no
suppressing tombstone anywhere → a deleted record leaks. This is exactly the
bounded window DESIGN §3.3's final paragraph describes.

The origin server never serves a stale generation; the hazard is purely a cache
between origin and client. With segment caching disabled
(`SegmentCacheMaxAge=0`, the current default) the hazard does not exist.

**Resolution (FUTURE, query plan):** the consistency contract lives in the query
plan, not in the segment format. At negotiation the server hands the client a
coherent triple — the overlay (with `W`, `M`), the segment list, and the
`/subscribe` cursor `= M` — and per-segment cache directives: any segment that
could still be rewritten under the chosen `W` is marked must-revalidate so the
client is forced to the fresh generation at origin. No new segment-header field
is required (the existing checksum/ETag already changes on rewrite). We document
this invariant and file a follow-up issue for the query plan.

## 4. Wire format (`application/octet-stream`)

Spec-compliant: across the 389 official atproto lexicons, query outputs are 379
`application/json` and a handful of binary, and **every binary-output query is a
`com.atproto.sync.*` bulk-transfer endpoint** (`getRepo`/`getBlocks`/… return
`application/vnd.ipld.car`). Our own `getSegment` returns `octet-stream`. A
custom binary body is therefore both idiomatic for this domain and consistent
with the repo.

Layout. All multi-byte integers little-endian; "varint" = LEB128 unsigned.

```
┌─ Uncompressed framing ────────────────────────────────────┐
│ magic:        [4]byte = "jsto"                            │
│ version:      uint16  = 1                                 │
│ flags:        uint16  (reserved, 0)                       │
│ watermark W:  uint64  (compaction/seq at build time)      │
│ maxSeq    M:  uint64  (highest seq folded into this blob) │
│ body_len:     uint64  (compressed byte length)            │
│ body:         [body_len]byte  (single ZSTD frame)         │
└────────────────────────────────────────────────────────────┘

ZSTD frame decompresses to:
┌─ DID string table ────────────────────────────────────────┐
│ did_count:    varint                                      │
│ repeated:     (len varint, [len]byte utf8)   -> didID = i │
├─ Collection NSID string table ────────────────────────────┤
│ coll_count:   varint                                      │
│ repeated:     (len varint, [len]byte utf8)   -> collID=i  │
├─ Record tombstones (grouped by didID, ascending) ─────────┤
│ group_count:  varint                                      │
│ per group:                                                │
│   didID:        varint                                    │
│   entry_count:  varint                                    │
│   entries sorted by seq; seq delta-varint within group:   │
│     collID:     varint                                    │
│     rkey_len:   varint, rkey [rkey_len]byte               │
│     seq_delta:  varint  (first = seq - W; rest = Δ prev)  │
├─ DID tombstones (account/sync; ascending by didID) ───────┤
│ did_tomb_count: varint                                    │
│ per entry:                                                │
│   didID:      varint   (same table; appended if needed)   │
│   seq_delta:  varint   (delta vs W, then vs prev)         │
│   reason:     uint8    (1=account, 2=sync)                │
└────────────────────────────────────────────────────────────┘
```

### Rationale

- **`W`/`M` in the uncompressed framing** so the query plan reads them without
  decompressing the body.
- **String tables dedup DIDs and NSIDs globally** by index. This beats relying
  on zstd's bounded match window: at the 32M-tombstone cap the uncompressed body
  is GB-scale, so far-apart repeats fall outside zstd's window and would be
  re-emitted; an explicit table stores each string once regardless of distance.
- **Delta-varint seqs.** Seqs are dense within `(W, M]`; deltas turn 8-byte
  values into mostly 1–2-byte runs.
- **Columnar grouping** keeps homogeneous data adjacent so zstd compresses each
  column well.
- **`reason` enum** preserves `DIDTombstone.Reason` (account vs sync) for future
  audit tooling.

### Properties

- **Empty overlay is valid and common** (nothing in `(W, M]`): zero-count tables
  and groups, a tiny body. Not an error.
- **Versioning.** `version=1`; an unknown version must be a hard client-side
  error later (no silent misparse). `flags` reserved for a future dictionary-id
  or alternate-layout bit.
- **Determinism.** Same snapshot → byte-identical blob (stable table order,
  stable sort). Required for a stable ETag and for the oracle.
- **Decode safety.** The decoder (FUTURE, hostile input) must bound every length
  against remaining buffer and reject trailing garbage, never panic. We build the
  *encoder* now plus a test decoder + round-trip harness.

### Columnar-vs-flat benchmark gate

The framing/`W`/`M` header is identical regardless of body layout, so
columnar-vs-flat is a body-internal decision we settle with a number, not an
argument. We implement the columnar layout above and add a benchmark (§6.4)
comparing it against a simpler sorted-flat-rows body on a realistic distribution;
we keep columnar only if it actually wins on bytes-on-wire. Decision recorded in
the benchmark note.

## 5. Server architecture

### 5.1 Packages

- New `internal/overlay` package owns the precomputed blob and the build
  function. Depends only on `tombstone` and `segment` (transport-agnostic,
  mirroring how `manifest` stays clean of XRPC).
- The handler lives in the existing `internal/xrpcapi` package next to
  `getSegment`/`listSegments`.

### 5.2 The cache

```go
type Blob struct {
    Bytes      []byte    // full octet-stream body (framing + zstd), ready to write
    ETag       string    // strong validator = hex(xxh3(Bytes))
    Watermark  uint64    // W
    MaxSeq     uint64    // M
    BuiltAt    time.Time
    NumRecords int
    NumDIDs    int
}

type Cache struct {
    mu  sync.RWMutex   // RLock on serve; Lock only to swap the pointer
    cur *Blob          // immutable once published
    src *tombstone.Set
    // debounce state, metrics, logger, zstd encoder
}
```

Serving is `RLock → copy *Blob pointer → RUnlock → w.Write(blob.Bytes)`. The
blob is **immutable after publish**, so concurrent readers share one backing
array with zero copying and zero per-request CPU. A rebuild constructs a brand-new
`*Blob` off-lock and swaps the pointer under a brief `Lock`.

### 5.3 Build

`Build(snapshot tombstone.Snapshot, W uint64) *Blob` is a **pure function**. `M`
is the max seq across the snapshot, or `W` if empty. The snapshot comes from
`Set.SnapshotRange(W, ^uint64(0))` (already implemented) taken under the set's
`RLock`, released *before* encode+compress so a slow build never blocks the live
consumer's `Observe`.

zstd level: a mid level (e.g. `SpeedDefault`), defined as a tunable constant;
builds are amortized but can occur during heavy delete bursts.

### 5.4 A stale blob is stale-but-coherent, not incorrect

This is the linchpin of the design and the reason we precompute instead of
rebuilding per request. A cached blob may lag the live tip by up to one
coalescing interval, but it is **never invalid data** — and a fresher blob would
not be *more correct*, only more expensive.

Three facts make this true:

1. **Every build is an atomic point-in-time snapshot.** `Build` takes
   `Set.SnapshotRange` under the set's `RLock`, so the blob contains *exactly*
   the tombstones in `(W, M]` and reports those exact `W` and `M`. It can never
   report an `M` it does not actually cover. "Stale" means only that `M` trails
   the live tip; it does not mean the blob is internally inconsistent.

2. **`M` is the handoff point between overlay and live tail.** The contract is
   that the consumer resumes `/subscribe` from *the blob's actual `M`* (not from
   "now"). So every tombstone is delivered by exactly one side of the seam,
   regardless of how stale the blob is:

   ```
   record created@50, deleted@1000;  blob built stale with maxSeq M0
     M0 ≥ 1000:  delete@1000 IS in the overlay        -> suppress create@50      ✓
     M0 < 1000:  delete@1000 NOT in the overlay,
                 but cursor = M0 < 1000, so live covers (M0, ∞) ∋ 1000
                 -> delete@1000 arrives on the live tail -> suppress create@50   ✓
   ```

   Whatever a stale overlay omits, the live tail covers, because the cursor is
   pinned to the same `M`. The only cost of staleness is a few extra seconds of
   live replay on connect — negligible against the 36h lookback. No gap, no leak,
   no corruption. This is a correct self-describing snapshot, **not** a silent
   fallback.

3. **Per-request rebuild would not improve correctness.** Correctness depends on
   an *honest* `M` plus `cursor = M`, not on `M` being maximally fresh. The
   tombstone *set* is already always-current (updated on every `Observe`);
   reserializing it per request only re-pays the encode+zstd cost (GB-scale at
   the 32M cap) N times for N concurrent clients, defeating the shared-in-memory
   goal — without making any client's output more correct.

**The invariant that must be protected** is therefore not blob freshness but the
consumer using the blob's *actual* `M` as the subscribe cursor. A consumer that
instead subscribed "from now()" would leak. That invariant is enforced by the
future query plan and asserted by the oracle seam test (§7.5); the endpoint's
sole obligation is to report `W`/`M` honestly, which it does by construction.

### 5.5 Rebuild triggers (coalesced; dirty-driven)

- **On every compaction pass** (via the existing `OnCompactionPass` callback in
  runtime.go): the set was just evicted and `W` advanced, so the blob is stale
  by definition — rebuild.
- **Background ticker** for the growing tail: rebuild when the set is **dirty**
  (changed via any `Set.Observe` or compaction since the last build), coalesced
  to at most once per `rebuildMinInterval` (default ~2s). A clean set is skipped
  cheaply (a dirty flag / monotonic observe counter compared to the last build).
  This bounds staleness to ≤ `rebuildMinInterval` while a few-events/sec firehose
  rebuilds at most once per interval rather than per event. We deliberately drop
  any new-tombstone *count* threshold: it is not a correctness knob (see §5.4) and
  only complicates the trigger.
- **Lazily on first request** if no blob exists yet (cold start before the first
  tick).

**Serve never triggers a synchronous rebuild except cold-start.** Under load we
serve stale-but-coherent bytes (§5.4) rather than risk a request-time compress
stall; staleness is bounded by `rebuildMinInterval` and harmless because the
query plan derives the cursor from the blob's actual `M`.

### 5.6 Wiring (runtime.go)

Construct the `overlay.Cache` with the existing `tombstones *tombstone.Set` and
the compaction watermark loader; pass it to the `xrpcapi` constructor so the
handler can serve it; hook its rebuild into the existing `OnCompactionPass`
callback and start its ticker under the lifecycle manager. Gate behind the same
readiness check as the other XRPC routes (503 until steady-state + manifest warm).

### 5.7 Compaction-disabled mode

When `CompactionInterval == 0`, the steady path detaches the tombstone set (see
`steady.go`), so the set is empty/frozen. The overlay serves an empty blob with
`W = M = current seq` — correct (nothing to suppress) and not an error.

### 5.8 Observability

Metrics: `overlay_blob_bytes`, `overlay_rebuild_total`,
`overlay_rebuild_duration_seconds`, `overlay_build_records` / `_dids`,
`overlay_requests_total`, `overlay_serve_bytes_total`. A trace span around build.

## 6. Lexicon

`lexicons/network/bsky/jetstream/getTombstones.json`: a parameterless `query`
with `output.encoding = application/octet-stream`. Regenerate Go types via the
existing lexgen pipeline (`api/jetstream`). Handler registered in
`xrpcapi.Server` alongside the existing two, behind the readiness gate.

## 7. Testing strategy

Layered; each layer tests what only it can; all fast (<1s/package per AGENTS.md).

### 7.1 Encoder unit + property/fuzz (`internal/overlay`)

- **Round-trip**: `Build → (test) decode → assert equal snapshot`. The test
  decoder pins the format and is the reference the future client mirrors.
- **Property/fuzz** (`testing/quick` + a `Fuzz` target): random snapshots —
  empty, single entry, many DIDs × many collections, adversarial rkeys (empty,
  max-len, non-UTF8, embedded NUL), seqs clustered just above `W` and at `M`,
  a DID present in both record and DID tombstones. Assert round-trip fidelity and
  `W ≤ every seq ≤ M`.
- **Decode-safety fuzz**: truncated/garbage bodies → bounded error, never panic.
- **Determinism**: same snapshot → byte-identical blob.

### 7.2 Cache concurrency

Hammer `Serve` from N goroutines while `Rebuild` swaps blobs under `-race`;
assert every served body is internally coherent. Assert debounce: sub-threshold
observes → no rebuild; crossing time+count → exactly one rebuild. Assert serve
allocates nothing beyond the response write (`-benchmem`).

### 7.3 Handler integration (`internal/xrpcapi`)

Real `xrpcserver`, fake/seeded `tombstone.Set`. Assert: `Content-Type`, stable
`ETag` across identical requests, 503 before steady-state, empty-overlay 200 with
a valid tiny body, body decodes to the seeded tombstones. Mirrors
`getsegment_test.go` / `listsegments_test.go`.

### 7.4 Compressibility benchmark (the §4 gate)

`Benchmark` over a realistic distribution (Zipfian DIDs, a few hot collections,
TID-shaped rkeys, dense seqs) at 1K / 100K / several-M tombstones. Report
bytes-on-wire and build-time for columnar vs flat. Keep columnar only if it wins;
record the decision.

### 7.5 Oracle / end-to-end determinism (`internal/oracle`)

The correctness keystone; the existing harness (`compacted.go`,
`compacted_test.go`) already models the durable suppression contract.

- Drive the seeded simulator to produce creates, then
  deletes/updates/account-deletes/sync-divergences for a known subset, advancing
  through a compaction pass so the set spans `(W, M]`.
- Fetch the overlay blob, decode it, and **reconstruct the client's would-be
  output**: scan segments, apply `Snapshot.ShouldDrop` using the decoded overlay
  snapshot, union with live-tail events from `(M, ∞)`.
- Assert the reconstructed output **exactly equals** the simulator's ground-truth
  "currently-live records" model: every superseded/deleted record suppressed,
  nothing live dropped.
- **No-permanent-tombstone case**: delete-account@100 → reactivate → post@200;
  assert the seq-200 record survives.
- **Seam case**: take the live portion from `cursor = M`; assert no gap and no
  double-suppression at the `W` and `M` boundaries.

The reconstruction decoder/applier in this layer is the reference implementation
the future Go client will mirror — not throwaway.

### 7.6 Mutation campaign

Add curated single-edit mutants (per AGENTS.md, `testing/mutation/mutants/`) that
this path's tests must kill, each documented with predicted oracle tier:

- `ShouldDrop` `>` → `>=` (off-by-one masks a live record).
- Encoder omits the DID-tombstone section.
- Encoder writes raw `seq` instead of `seq - W` / wrong delta base.
- Cache serves a stale `M` (reports a smaller `M` than folded).

Re-run `just mutation-campaign`; update `testing/mutation/RESULTS.md`.

## 8. Deliverables

1. `lexicons/network/bsky/jetstream/getTombstones.json` + regenerated
   `api/jetstream` types.
2. `internal/overlay` package: blob encoder (pure), `Cache`, rebuild triggers,
   metrics.
3. `xrpcapi` handler + registration behind the readiness gate.
4. runtime.go wiring (construct cache, hook `OnCompactionPass`, start ticker).
5. Tests for layers §7.1–§7.6, including the benchmark and mutation mutants.
6. **DESIGN.md §3.3 update**: the binary wire-format diagram, the `W`/`M`
   coverage contract, and a note that the client consumer + query-plan cursor
   selection are future work.
7. Follow-up GitHub issues: (a) Jetstream v2 client decoder/overlay applier;
   (b) the query-plan negotiation (segment list + cache directives +
   `/subscribe` cursor = `M`).

## 9. Open questions / risks

- **zstd level** is a guess pending the §7.4 benchmark; tune with a number.
- **Columnar vs flat** is benchmark-gated; if flat wins we keep the simpler body.
- **`rebuildMinInterval`** (~2s) is a starting point to validate under a
  realistic firehose rate; it bounds staleness, not correctness (§5.4), and can
  be exposed as a tunable if needed.
