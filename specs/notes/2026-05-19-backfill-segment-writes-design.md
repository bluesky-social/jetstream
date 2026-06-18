# Backfill Segment Writes — Active Segment Lifecycle and Real `HandleRepo`

**Date:** 2026-05-19
**Scope:** First production write path that produces real `data/shards/seg_*.jss` files. Introduces a new `internal/ingest` package owning the active-segment writer, its rotation, and monotonic seq allocation. Replaces the placeholder `backfill.LogHandler` with a real `backfill.SegmentHandler` that walks each downloaded repo and emits one `KindCreate` event per record. Wires the writer through `cmd/jetstream serve` so the bootstrap phase materializes segment files on disk.

## 1. Goals

1. Bootstrap-phase writes produce well-formed segment files at `<data-dir>/shards/seg_<base36>.jss` exactly as specified by DESIGN.md §3.4.
2. Allocate every event a monotonic 64-bit `seq` from a single global pebble counter (`seq/next`) per DESIGN.md §2.
3. Honor DESIGN.md §3.1.1's per-block durability ordering: block fsync first, then a `pebble.Sync` batch advances the persisted state. A crash between the two is recoverable.
4. Rotate active segments at a configurable byte threshold (default 256MB) by sealing the current file and opening the next index.
5. Resume cleanly across process restarts: scan `shards/`, resume the highest active segment if any, otherwise open the next index. Reconcile in-memory `nextSeq` against the actual events on disk so a crash between block fsync and pebble batch never produces duplicate seq numbers.
6. Keep the `segment` package's purity invariants intact (no goroutines, no contexts, no pebble imports). All lifecycle concerns live in `internal/ingest`.
7. Preserve the existing fast feedback loop: tests must keep `just test` under a second; rotation must be exercised against tiny `MaxSegmentBytes` values rather than multi-MB fixtures.

## 2. Non-Goals

- 30-second time-based block flush (DESIGN.md §3.1.1's "or 30 seconds, whichever first"). Not needed for backfill, where every block fills within seconds. Lands with the live-tail consumer.
- Live-tail writer to `backfill/live_shards/` (DESIGN.md §4.1 step 1). Separate PR; this PR is backfill-only.
- `backfill_complete.log` append on `OnComplete`. DESIGN.md §3.5 calls it out as replicas-only state; lands with the replication PR.
- `relay/cursor` advancement. That's the upstream firehose cursor, only meaningful for the live-tail consumer.
- Per-block per-DID `repo/<did>.Rev` updates inside the durability batch. The atmos-driven `OnComplete` already records the per-repo `Rev` once at end-of-repo; the per-block fan-out is a steady-state concern (DESIGN.md §4.3).
- Merge phase that compacts `live_shards/` into `shards/` (DESIGN.md §4.2). Has no live-tail data to merge yet.
- Lookaside file writes, identity / account / sync events, signature-on-replay verification, segment HTTP serving, replica subscription. All land in their own slices.
- Segment-rotation seal failures graceful-recovery beyond what `segment.Writer.Seal` already provides. We surface the error and let the errgroup tear the process down — DESIGN.md and PRACTICES.md prefer crashing over data corruption.

## 3. Architecture

### 3.1 Package Layout

```
internal/ingest/                  (NEW)
  doc.go                          package overview
  config.go                       Config + validate()
  writer.go                       Writer (Open/Append/Close), seq allocation, rotation
  filename.go                     segmentFilename helper, base-36 index parser
  metrics.go                      Prometheus counters/gauges, nil-safe
  errors.go                       sentinel errors

  writer_test.go                  open/append/close, rotation, restart-recovery
  filename_test.go                base-36 round-trip
  metrics_test.go                 registration round-trip

internal/backfill/
  handler.go                      MODIFIED: drop LogHandler, add SegmentHandler
  handler_test.go                 MODIFIED: replace log assertions with segment
                                  round-trip assertions via a real ingest.Writer
                                  in a t.TempDir
  run.go                          MODIFIED: Config gains *ingest.Writer; constructed
                                  in cmd/jetstream
  run_test.go                     MODIFIED: stubServer flow exercises segment writes

cmd/jetstream/
  main.go                         MODIFIED: serve wires ingest.Writer into both
                                  backfill.Run and the metrics registry
```

Boundary discipline:

- `internal/ingest` imports `internal/store` and `segment`. Nothing else.
- `internal/backfill` imports `internal/ingest` and `internal/store`. The `LogHandler` placeholder is deleted in this PR.
- `segment` gains exactly one new exported helper (see §3.4): `ScanMaxSeq(path string) (uint64, error)`. No other changes to `segment`.

### 3.2 Concurrency Model

- `ingest.Writer` is safe for concurrent use. A single `sync.Mutex` serializes `Append`, `Close`, and the rotation it triggers.
- atmos calls `SegmentHandler.HandleRepo` from many workers concurrently; each call appends one event at a time under the writer lock per the user's preference. The writer lock is held only for the duration of one `Append` (which fans out into at most one block flush + one rotation seal+open as needed). Workers do not hold the lock across record reads from `r.Tree` or `r.Store`.
- `segment.Writer` itself remains single-threaded as documented; `ingest.Writer`'s mutex is the discipline that upholds that contract.
- No goroutines internal to `ingest`. Every action runs on the caller's goroutine.

### 3.3 Data Flow

```
atmos worker (one of N)
  └─ HandleRepo(ctx, did, r, commit)
       └─ r.Tree.Walk(func(key, cid) { … })           //  per-record callback
            ├─ payload, _ = r.Store.GetBlock(cid)
            ├─ collection, rkey = splitMSTKey(key)
            └─ ingest.Writer.Append(&segment.Event{
                 IndexedAt: handlerStartTime.UnixMicro(),
                 Kind:      KindCreate,
                 DID:       string(did),
                 Collection, Rkey, Rev: commit.Rev,
                 Payload:   payload,
               })
                 ├─ mu.Lock()
                 ├─ ev.Seq = w.nextSeq;     w.nextSeq++
                 ├─ full, err := segWriter.Append(ev)
                 ├─ if full:
                 │    segWriter.Flush()                // fsyncs the block
                 │    w.activeBytes = stat(path).Size() - reservedHeaderBytes
                 │    pebble.Set("seq/next", w.nextSeq, Sync)
                 │    metrics.BlocksFlushed.Inc()
                 │    if w.activeBytes >= MaxSegmentBytes:
                 │       segWriter.Seal()             // walk + footer + header
                 │       w.activeIdx++
                 │       segWriter = segment.New(filenameFor(activeIdx))
                 │       w.activeBytes = 0
                 │       metrics.SegmentsRotated.Inc()
                 └─ mu.Unlock()
```

The handler captures `handlerStartTime` once at the top of `HandleRepo`. Every event for that repo gets the same `IndexedAt` value. This is the closest thing to the wall-clock time at which jetstream "saw" the repo — finer-grained per-record timestamps would imply false ordering.

### 3.4 New Helper in `segment`

The seq-recovery scan introduced in §3.6 needs a tiny one-shot pass over an active segment file. `internal/ingest` doesn't have block-decoder access, so we add one exported helper next to `lastGoodOffset` semantics in `writer.go`:

```go
// ScanMaxSeq returns the maximum Seq value across all fully-durable
// blocks of an active segment file. The bool reports whether any
// events were observed; on an empty active segment (zero blocks) it
// returns (0, false, nil). The bool is necessary because seq=0 is a
// valid first-event value: callers can't distinguish "max is 0" from
// "no events" without it.
//
// Intended for crash recovery in callers that own the active-segment
// lifecycle (e.g. internal/ingest); it does not open or modify the
// file beyond a read-only ReadAt walk.
//
// The walk is bounded by lastGoodOffset semantics: torn tails are
// ignored and not returned as max. Returns ErrSegmentSealed if the
// file is sealed (no recovery scan needed; sealed-file readers should
// use Reader instead).
func ScanMaxSeq(path string) (maxSeq uint64, found bool, err error)
```

This is the only `segment` API surface added in this PR. Implementation is straightforward: open `O_RDONLY`, validate magic + check the offset-4 checksum is zero, walk the framed-block region using the existing `lastGoodOffset` traversal, decompress each block, take the per-frame max from the seq column, return the running max.

`ScanMaxSeq` lives in a new file `segment/scan.go` rather than in `writer.go` so it stays adjacent to its callers conceptually (a recovery helper, not a writer method).

### 3.5 `ingest.Writer` Lifecycle and Public API

```go
// Package ingest owns the active-segment writer for jetstream. It
// allocates monotonic seq numbers, rotates segment files, and commits
// the per-block durability batch to pebble.
package ingest

type Config struct {
    ShardsDir         string         // <data-dir>/shards
    Store             *store.Store
    MaxSegmentBytes   int64          // default 256 << 20
    MaxEventsPerBlock int            // default segment.DefaultMaxEventsPerBlock
    Logger            *slog.Logger
    Metrics           *Metrics       // nil-safe
}

type Writer struct {
    cfg         Config
    mu          sync.Mutex
    active      *segment.Writer
    activeBytes int64
    activeIdx   uint64
    nextSeq     uint64
}

// Open scans cfg.ShardsDir, resumes the highest active segment if one
// exists, else opens seg_0000000000.jss. Reads seq/next from pebble,
// reconciles it against the resumed segment's max seq+1, and persists
// the reconciled value back if it advanced.
func Open(cfg Config) (*Writer, error)

// Append writes one event into the active segment. Mutates ev.Seq in
// place to the allocated value. Goroutine-safe. Latches and returns
// any underlying segment.Writer sticky error; subsequent Appends
// return the same error.
func (w *Writer) Append(ev *segment.Event) error

// Close flushes any pending events and closes the active writer file.
// Idempotent. Does NOT seal — that's a rotation-time / shutdown-flow
// concern outside the scope of this PR.
func (w *Writer) Close() error
```

#### Why `Append(*Event)` rather than `Append(Event) (seq uint64, err error)`

Two reasons: (1) `segment.Writer.Append` already takes `Event` by value and never mutates fields; we want the same shape in `segment` itself unchanged. (2) The caller (`SegmentHandler`) constructs `Event` once per record and discards it; passing by pointer lets us write `ev.Seq = w.nextSeq` without an extra struct copy. The function signature documents the mutation explicitly.

#### Pebble keyspace

```
seq/next   ->   uint64 LE (next seq number to allocate)
```

- Initial value 0. A missing key reads as 0 — fresh data dir works without seeding.
- Read once at `Open`. Mirrored into `Writer.nextSeq`.
- Rewritten via a `pebble.Sync` batch only inside the per-block durability commit.

### 3.6 Startup Discovery and Crash Recovery

```
Open(cfg):
  1. os.MkdirAll(cfg.ShardsDir, 0o755)
  2. entries = readdir, filter to seg_<10 base36>.jss, parse indices
  3. if entries empty:
        idx = 0
        seg = segment.New(filenameFor(0))
        activeBytes = 0
     else:
        idx = max(entries)
        seg, err = segment.New(filenameFor(idx))
        switch:
          case err is nil:                 // resumed an active file
            // segment.New has already truncated any torn tail, so the
            // post-resume size reflects only fully-durable frames.
            maxSeq, found = segment.ScanMaxSeq(path)
            activeBytes = stat(path).Size() - reservedHeaderBytes
          case errors.Is(err, ErrSegmentSealed):
            idx = idx + 1
            seg = segment.New(filenameFor(idx))
            activeBytes = 0
            found = false
          default:
            return err
  4. pebbleValue = pebble.Get("seq/next") (default 0 if missing)
     reconciled = pebbleValue
     if found and maxSeq+1 > reconciled:
         reconciled = maxSeq + 1
     // Persist a forward correction so a crash here doesn't reintroduce
     // the duplicate-seq risk on the next restart.
     if reconciled > pebbleValue:
         pebble.Set("seq/next", reconciled, Sync)
     nextSeq = reconciled
  5. return &Writer{...}
```

Crash matrix:

| Crash point                                                 | On-disk state                          | Recovery                                                                                                  |
| ----------------------------------------------------------- | -------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| Mid-Append, before block flush                              | block bytes only in memory             | Lost. Repos that didn't reach `OnComplete` get retried on next Run.                                       |
| After block flush, before pebble batch commit               | block fsynced, `seq/next` lags         | `Open` runs `ScanMaxSeq` on resume, reconciles `nextSeq = max(pebble, scan+1)`, writes back. No dupes.    |
| After pebble batch, before rotation seal                    | block durable, segment unsealed        | Resume reopens it normally; `activeBytes` re-derived from `stat`.                                         |
| Mid-seal (footer durable, header zero)                      | already handled by `segment.Writer`    | `segment.Writer.Seal`'s own truncate-back-off path restores active-state invariants. Re-call seal on resume rotation.                                |
| Mid-seal (header durable but unfsynced)                     | already handled by `segment`           | `segment.Reader.Open` checksum check surfaces it. We don't try to handle this in ingest; surface the error and crash.                            |
| After seal but before opening seg+1                         | sealed file exists, no successor       | Resume sees highest as sealed; opens `idx+1` per discovery rule 3.                                        |

The reconcile pass is read-only and bounded: at most one full segment's worth of decompression per process start. With a 256MB segment that's seconds at worst.

### 3.7 `backfill.SegmentHandler`

```go
type SegmentHandler struct {
    writer *ingest.Writer
    logger *slog.Logger
    now    func() time.Time
}

func NewSegmentHandler(writer *ingest.Writer, logger *slog.Logger) *SegmentHandler

func (h *SegmentHandler) HandleRepo(ctx context.Context, did atmos.DID,
    r *repo.Repo, commit *repo.Commit) error
```

Behavior:

1. `t := h.now().UnixMicro()` once at the top.
2. `r.Tree.Walk(func(key string, cid cbor.CID) error { ... })`:
   - Split `key` on `/` into `(collection, rkey)`. A key without exactly one slash is an integrity error → return wrapped error.
   - `payload, err := r.Store.GetBlock(cid)`. Missing block → return wrapped error.
   - Construct `segment.Event{IndexedAt: t, Kind: KindCreate, DID: string(did), Collection, Rkey, Rev: commit.Rev, Payload: payload}`.
   - `if err := h.writer.Append(&ev); err != nil { return err }`.
3. A non-nil return from `Tree.Walk` propagates verbatim. atmos retries per its policy; on retry-exhaustion the DID hits `StateFailed`. Some duplicate Append rows may already be on disk; that's tolerated under at-least-once semantics. No partial-cleanup is attempted.

Concurrency: atmos guarantees no two `HandleRepo` calls overlap for the same DID, and `ingest.Writer` is goroutine-safe across DIDs.

### 3.8 Wiring in `cmd/jetstream`

`runServe` constructs the writer between the metadata store and the errgroup, and wires it into both backfill and the metrics registry:

```go
metaStore, _ := store.Open(dataDir)

ingestMetrics := ingest.NewMetrics(metrics.Registry)
ingestWriter, err := ingest.Open(ingest.Config{
    ShardsDir: filepath.Join(dataDir, "shards"),
    Store:     metaStore,
    Logger:    logger,
    Metrics:   ingestMetrics,
    // MaxSegmentBytes / MaxEventsPerBlock left zero → defaults.
})
if err != nil { return err }
defer func() { _ = ingestWriter.Close() }()

backfill.Run(gctx, backfill.Config{
    Store:    metaStore,
    Writer:   ingestWriter,           // NEW field
    RelayURL: cmd.String("relay-url"),
    Logger:   logger,
    Metrics:  backfill.NewMetrics(metrics.Registry),
})
```

The writer is constructed before the errgroup starts so its `Open` errors are reported pre-goroutine, simplifying the error reporting path. The defer-Close ensures any pending events are flushed when serve exits — still without sealing, which is intentional for this PR (sealing-on-shutdown is its own concern).

`backfill.Config` gains one new required field: `Writer *ingest.Writer`. `validate()` rejects nil with `Config.Writer is required`.

### 3.9 Metrics

```go
// Namespace=jetstream, Subsystem=ingest
type Metrics struct {
    EventsAppended  prometheus.Counter   // every successful Append
    BlocksFlushed   prometheus.Counter   // every block fsync
    SegmentsRotated prometheus.Counter   // every successful seal+open
    AppendErrors    prometheus.Counter   // every Append that returned non-nil
    ActiveSegBytes  prometheus.Gauge     // current activeBytes
    NextSeq         prometheus.Gauge     // current nextSeq (debugging aid)
}

func NewMetrics(reg prometheus.Registerer) *Metrics
```

Constructor mirrors `backfill.NewMetrics` exactly: `MustRegister`, nil-safe `inc*`/`set*` helpers, registered against the shared `obs.Metrics` registry from `cmd/jetstream`. Gauges are set at the end of every `Append` under the writer lock. Block-flush latency histograms are deliberately not introduced here — we lack realistic load-test data to set bucket boundaries. Follow-up PR.

### 3.10 Tracing

- `obs.Tracer("ingest").Start(ctx, "ingest.flush_block")` wraps the block-flush + pebble-batch path. One span per ~4096 events.
- `obs.Tracer("ingest").Start(ctx, "ingest.rotate_segment")` wraps the seal + open path. One span per ~256MB.
- `obs.Tracer("backfill").Start(ctx, "backfill.handle_repo")` wraps `SegmentHandler.HandleRepo`. One span per backfilled DID; the right granularity for performance debugging without span explosion.
- No per-`Append` spans. At full network scale that would be ~1B spans/day.

## 4. Error Handling and Edge Cases

- `ingest.Open` returns wrapped errors for bad ShardsDir, corrupt highest segment, or pebble read failure. `runServe` reports them and exits.
- `Append` failures (segment.Writer's sticky error from a flush/fsync failure) latch on `ingest.Writer` too: every subsequent call returns the same error. The errgroup tears the process down. Restart triggers the recovery path.
- `SegmentHandler.HandleRepo` propagates `Append` errors verbatim. atmos's retry/failure path turns them into `StateFailed` after attempt budget exhaustion.
- `r.Tree.Walk` errors (missing CID block, walk-callback panics caught by atmos) are wrapped and returned. We do not silently skip records — DESIGN.md's "no data loss" invariant requires we surface the integrity violation rather than tolerate it.
- Empty repos: `Tree.Walk` calls the callback zero times. `OnComplete` records `StatusComplete` with `commit.Rev`. No segment-file rows produced. Correct.
- Segment files already exist but parent dir is unwritable / disk full: surfaces from `segment.New` directly. We don't wrap.
- `MaxSegmentBytes` set to a tiny value (test-only): rotation fires after every block. Same code path as production. Seal must work on segments containing exactly 1 block.
- Length-violations (DID > 65535 bytes, Collection/Rkey/Rev > 255, Payload > MaxUint32): atproto in the wild stays well within these. `segment.validate` already enforces them and we let the error propagate.
- MST keys that don't split cleanly into `collection/rkey`: returned as wrapped error. Should never happen for a well-formed repo (atmos's MST already validates).

## 5. Testing Strategy

PRACTICES.md: integration tests for happy paths, fuzz tests for untrusted-input edges, swarm tests for invariant exploration, smoke tests against real production data where possible. Unit tests sparingly.

### 5.1 Integration tests in `internal/ingest`

- `TestWriter_OpenFreshDir` — empty `ShardsDir`, opens `seg_0000000000.jss`, `nextSeq=0`.
- `TestWriter_AppendAllocatesMonotonicSeq` — N appends produce seqs `[0..N)` in `ev.Seq`.
- `TestWriter_BlockFlushOn4096` — append exactly `MaxEventsPerBlock` events, observe one block on disk via `segment.Reader` after Close.
- `TestWriter_PebbleAdvancesOnBlockFlush` — after first block flush, `seq/next` in pebble equals `MaxEventsPerBlock`.
- `TestWriter_RotationOnByteThreshold` — set `MaxSegmentBytes=4096`, append enough events to rotate, observe two files: `seg_0000000000.jss` sealed (via `segment.Reader.Open` succeeding) and `seg_0000000001.jss` active.
- `TestWriter_ResumeActive` — open, append, close (no seal). Re-Open, append more. Both ranges land in the same active file with no seq dupes; `ScanMaxSeq` finds the right max.
- `TestWriter_ResumeSealed_OpensNextIndex` — open, fill, force seal via small `MaxSegmentBytes`, close. Re-Open should pick `seg_*0001.jss` not `*0000`.
- `TestWriter_RecoversFromTornTail` — write a block, manually corrupt the file by truncating one byte off the end, Re-Open. Recovery truncates the torn frame; `ScanMaxSeq` returns the previous block's max; `nextSeq` reconciles correctly.
- `TestWriter_NextSeqReconcileOnDriftedPebble` — write a block, fsync, simulate crash by manually rewriting `seq/next` in pebble to a smaller value (mimicking the "block fsync, pebble batch lost" case). Re-Open. `nextSeq` reconciles up to `scan+1`; pebble is rewritten.
- `TestWriter_ConcurrentAppend` — N goroutines Append concurrently, expect `nextSeq == N` and unique seq per event in the resulting segment file. Run under `-race`.

### 5.2 Tests in `internal/backfill`

- `TestSegmentHandler_EmitsKindCreatePerRecord` — fixture repo with K records, single-record-collection mix, expect K events in segment with the right `(DID, Collection, Rkey, Rev)` and matching CBOR payloads.
- `TestSegmentHandler_EmptyRepo_NoEvents` — repo with zero records produces zero events.
- `TestRun_HappyPath_WritesSegments` — adapt the existing happy-path test: after `Run`, verify `shards/seg_0000000000.jss` exists, opens via `segment.Reader`, contains the expected event count for all DID fixtures.
- `TestRun_RejectsMissingWriter` — extend `TestRun_RejectsInvalidConfig` with a `nil Writer` case.
- The existing `TestRun_HappyPath_DownloadsAllRepos`, `TestRun_Resume_NoOpAfterCompletion`, `TestRun_PersistsCursorAfterDrain`, `TestRun_PassesSavedCursorToRelay` continue to pass; they're updated to construct a real `ingest.Writer` rooted at `t.TempDir()`.

### 5.3 Swarm test in `internal/ingest`

`TestWriter_Swarm` (gated like the existing segment swarm tests):

- Random `MaxSegmentBytes` between 1KB and 64KB.
- Random `MaxEventsPerBlock` between 1 and 256.
- Random number of appends (10–10k) of randomized events.
- Optional random crash-injection mid-run via writer abort + reopen.
- Invariants checked at the end:
  - The set of seqs across all sealed and active segments has no duplicates.
  - `max(seq) + 1 == nextSeq`.
  - Every sealed file passes `segment.Reader.Open` (checksum valid).
  - Active file truncates cleanly; re-Open succeeds without error.

Swarm runs only on `just test-long`, matching the existing segment swarm convention.

### 5.4 Fuzz

No new fuzz target this PR. The fuzzable-surface code (block decode, footer decode) already has fuzz coverage in `segment`. `ingest` is just composition.

### 5.5 Performance

A `BenchmarkWriter_Append` in `writer_bench_test.go` measures appends-per-second on a tiny in-memory event with `MaxSegmentBytes` set high enough to skip rotation. Developer-invoked only.

## 6. Open Questions

None blocking. One follow-up worth flagging:

- **Block-flush latency histograms.** Deliberately omitted until we have realistic production data. Adding a histogram with the wrong bucket boundaries forces a Prometheus state-rebuild later. Resolve when the live-tail PR has a realistic load test in place.

## 7. References

- DESIGN.md §2 (seq invariants)
- DESIGN.md §3.1.1 (durability ordering, rotation threshold)
- DESIGN.md §3.4 (segment file naming, directory layout)
- DESIGN.md §3.5 (pebble keyspace conventions)
- DESIGN.md §4.1 (bootstrap phase)
- PRACTICES.md (testing levels, dependency whitelist, observability)
- 2026-05-19-segment-sealing-design.md (the sealing semantics this PR drives)
- 2026-05-18-backfill-bootstrap-design.md (the atmos engine wiring this PR upgrades)
