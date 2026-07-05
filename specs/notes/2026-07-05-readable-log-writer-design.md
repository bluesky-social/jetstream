# Readable-log writer: unifying visibility across the ingest/subscribe seam

Date: 2026-07-05
Issues: #248 (design C refactor), #249 (interim containment), grew out of #244 (589f632)
Status: analysis + direction agreed; containment first, refactor as its own designed effort

## 1. Trigger

Adversarial review (roast, cross-model verified) of 589f632 — "feed /subscribe
tail from writer ordered sink; harden hot ring" — found that in async-flush
mode, `ingest.Writer.Append`/`AppendBatch` deliver events to the ordered sink
immediately after submitting the detached block to the flush pipeline, before
it commits:

- Not in `SnapshotPending()` — `segment.PrepareFlush` detached the block from
  the pending buffer.
- Not in the active file — the background `CommitPreparedFlush` hasn't run.

So a sink-delivered event can be readable **nowhere** on the cold path. If the
subscribe hot ring evicts it inside that window, a subscriber's cold fallback
finds a hole: transient gap at best, a spurious loud
`subscribe: walk did not converge` at worst.

Unreachable in production today, by wiring accident only: the sink is
installed solely on the sync steady writer (`internal/jetstreamd/runtime.go`
`OnSteadyStateWriter`), and async flush is bootstrap-backfill-only
(`orchestrator/bootstrap.go:58`). `TestOrderedSink_AsyncWriterCovered`
green-stamps the broken combination.

## 2. Contributing factors, not a point bug

Working backwards: #244 itself, this finding, and the cold walker's
rotation-seam convergence machinery (two-retry heuristic, #190) are the same
disease. The writer conflates three separable concerns:

1. **Ordering** — allocate dense seqs.
2. **Visibility** — make appended events readable (tail sink,
   `SnapshotPending`, cold walker).
3. **Durability** — compress, write, fsync, commit pebble metadata.

Visibility today is an accident of where an event sits in the durability
pipeline. Three readers grope into three pipeline stages: the sink fires at
append time, `SnapshotPending` reads the pending block, the cold walker reads
flushed blocks + pending. When async flush added a fourth stage — the detached
`PreparedBlock` — no reader covered it. That is the architecture guaranteeing
that **every new pipeline stage creates a new visibility gap by default**.
Likewise #244: the tail was fed by wiring convention (remember to install the
hook on the right producer) rather than by construction.

### Special-casing inventory

Mode-dispatch points in `internal/ingest` (`w.async != nil`):

| Site | Divergence |
|---|---|
| `writer.go` Open | constructs pipeline when `AsyncFlushWorkers > 0` |
| `writer.go` Close | delegates `closeAsync` |
| `writer.go` SealActiveAndClose | delegates `sealActiveAndCloseAsync` |
| `writer.go` Append | submit/wait/rotate-outside-lock vs inline flush |
| `writer.go` appendLocked | prepare job vs `flushAndRotateLocked` |
| `writer.go` Flush | prepare+submit+wait vs `flushSync` |
| `writer.go` DrainDurability | `drainAsync` vs `drainSync` |
| `async_flush.go` rotateIfFull | no-op for sync; drain-to-quiescence for async |

Mutual-exclusion rules: `OnAfterFlush`×async (config.go validation),
`ForceRotate`×async (runtime error), and now sink×async (undocumented until
#249). Each individually defensible; the set is a matrix nobody holds in their
head — exactly the "error prone or at least confusing" smell that prompted
this analysis.

## 3. Why async flush exists (history)

- #51: production profiling, 188/200 backfill workers blocked on the writer
  mutex; hold time = 73% block compression, 18% seal, 7% appends.
- #55 (commit d3d6290, 2026-06-16): move zstd compression off the lock via
  PrepareFlush/CompressPreparedBlock/CommitPreparedFlush with ordered commits.
  Measured on cpu2-pop3: mutex wait ~115 → ~6 goroutine-s/s; throughput
  ~682k → ~1.0–1.16M events/s (~1.5x, "indicative rather than a controlled
  benchmark"). Production default: 100 backfill workers / 4 flush workers.
- 9a54911: async rotation starvation fix (`rotateIfFull` drain-to-quiescence)
  after a 126GB runaway active segment — an example of the async path growing
  its own parallel lifecycle machinery.
- Scoped to bootstrap only because the live consumer's `OnAfterFlush` persists
  a mutable cursor (not block-specific); `OnDurableBatch` (#62) is the
  async-compatible replacement and the live cursor could migrate to it.

## 4. Benchmarks (new, `internal/ingest/writer_bench_test.go`)

The repo had **no** sync-vs-async writer benchmark; the claim lived only in
issue comments. Now measured (Ryzen 9 9950X, 2026-07-05; backfill shape =
8 producers × 1024-event `AppendBatch`, ~200B payloads; `BENCH_DIR` selects
storage):

| Mode | tmpfs (compression-bound) | ext4/dm-crypt (fsync-bound) |
|---|---|---|
| sync, 8 producers | 7.2M ev/s | 488k ev/s |
| async×1 | 9.2M ev/s | 501k ev/s |
| async×4 | 12.5M ev/s | 510k ev/s |
| async×8 | 13.3M ev/s | 507k ev/s |

Live shape (1 producer, per-event `Append`, sink installed): sync ≈ 480k ev/s
on real disk — two orders of magnitude above firehose rate (~1–3k ev/s); the
sink costs nothing measurable.

Findings:

1. The async win is real and is exactly the compression-serialization effect
   (~1.7x on tmpfs, matching #55's production numbers), **but ordered commits
   serialize fsync regardless**, so on fsync-bound storage the win collapses
   to ~4%. The pipeline's value is a function of the box's compression:fsync
   cost ratio. Re-measure on production storage before tuning worker counts.
2. Sync is nowhere near a steady-state bottleneck; async is a bootstrap-only
   optimization and must be preserved (bootstrap wall-clock matters), but it
   earns none of its current architectural footprint.

## 5. Greenfield candidates

**A. Unify the pipeline — sync = async with an inline executor.** One code
path: append → prepare → compress → commit; `workers=0` executes stages
inline. Kills the dispatch matrix and the `ForceRotate`/`OnAfterFlush`
carve-outs (live cursor → `OnDurableBatch`). Does NOT fix the visibility
disease — prepared blocks still gap; readers would need to scan in-flight
prepared blocks as a patch.

**B. Two writer types (BackfillWriter / LiveWriter) over a shared core.**
Encodes the exclusions in the type system; honest about today's usage. But it
freezes the current accident into the architecture, duplicates lifecycle
logic, and the bootstrap→steady cutover (same seq space) straddles two types.
Local maximum; rejected.

**C. The readable log — visibility owned by the writer, durability trails a
watermark.** The writer owns an ordered in-memory ring of appended events as
the authoritative source for everything not yet durable, plus a byte-budgeted
retention window for hot serving:

- `Append`: allocate seq, place in ring — **readable by construction** from
  that instant.
- Durability pipeline (inline or worker-fanned compression, ordered commits)
  trails behind, advancing a `durableSeq` watermark. Entries above the
  watermark are pinned (exist nowhere else — cannot evict); entries below are
  evictable under the byte budget.
- /subscribe reads the ring directly. `SetOrderedEventSink`, the separate
  subscribe `hotRing`, its non-dense-append reset heuristic, and
  `SnapshotPending`-for-replay all cease to exist. #244-class feed-bypass and
  advertised-but-unreadable windows become structurally impossible.
- The cold reader serves only seqs below the ring floor — durable by
  definition. One well-defined hot/cold boundary replaces the four-source walk
  (sealed → flushed → prepared → pending) and the seam-convergence retry
  machinery.

Memory bound: pinned region ≈ blocks-in-flight × 4096 events × ~300B ≈ 5MB at
4 workers, plus the retention budget already spent on the subscribe hot ring —
net it removes a copy (today every event is duplicated into the subscribe
ring).

## 6. Decision

- **Now (#249):** containment — `SetOrderedEventSink` rejects async writers,
  loud. Flip `TestOrderedSink_AsyncWriterCovered` to pin the rejection. Do not
  adopt roast's proposed wait-before-sink fix: correct in all futures, but it
  serializes the pipeline for a configuration nothing uses, and C deletes the
  dichotomy anyway.
- **Next (#248):** C, staged as A→C (mechanical pipeline unification first,
  then lift visibility into the ring and repoint subscribe + cold walker).
  This is the most load-bearing seam in the codebase — writer, tail, cold
  walker, rotation seam all move — so it gets its own spec, issue-per-stage,
  oracle tiers green at every stage, and a mutation-campaign re-run at the
  end. Not to be done as a side effect of another branch.
- Bench file `writer_bench_test.go` is committed as the sync-vs-async
  benchmark the repo lacked; `BenchmarkWriterBackfillShape` doubles as the
  no-regression gate for #248.

## 7. Kaizen / open questions for the #248 spec

- Eviction policy details: byte budget vs entry count; interaction with very
  large events; whether the retention window should adapt to subscriber lag.
- Watermark semantics at rotation and seal; what "ring floor" means across a
  segment boundary.
- What replaces the walk seam-convergence tests — the invariant shifts from
  "walk converges across the seam" to "ring floor never exceeds durableSeq+1".
- Live cursor migration `OnAfterFlush` → `OnDurableBatch` is a prerequisite
  and independently shippable.
- Oracle/mutation coverage: several mutants target the current seam machinery;
  expect STALE results and plan the refresh.
