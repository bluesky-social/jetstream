# Readable log (#248): design and implementation plan

Date: 2026-07-06
Issue: #248 (design C — unify writer visibility into a readable log)
Prior analysis: `specs/notes/2026-07-05-readable-log-writer-design.md` (contributing
factors, benchmarks, candidate comparison). This doc is the implementation spec:
it resolves the open design questions from the issue and stages the work.
Status: DRAFT for Jim's review — do not start implementation until reviewed.

---

## 1. Problem restatement (one paragraph)

`ingest.Writer` conflates ordering (seq allocation), visibility (what readers
can see), and durability (compress/fsync/pebble). Visibility today is an
accident of pipeline position: three readers grope into three pipeline stages
(ordered sink at append, `SnapshotPending` for the pending block, cold walker
for flushed blocks + manifest), and the async pipeline's detached
`PreparedBlock` is a fourth stage no reader covers — so every new stage creates
a visibility gap by default (#244, #249, #190 are all this disease). The fix:
the writer owns an ordered in-memory ring of appended events as the
*authoritative* source for everything not yet durable; the durability pipeline
trails behind a `durableSeq` watermark; /subscribe reads the ring; the cold
reader serves only seqs below the ring floor, which are durable by definition.

## 2. What the research confirmed (data)

### 2.1 Benchmarks re-run 2026-07-06 (Ryzen 9 9950X, this machine)

`BenchmarkWriterBackfillShape` (8 producers × 1024-event AppendBatch, ~200B payloads):

| Mode      | tmpfs (compression-bound) | ext4/dm-crypt (fsync-bound) |
|-----------|---------------------------|------------------------------|
| sync/p8   | 6.8M ev/s                 | 488k ev/s                    |
| async×1   | 8.5M ev/s                 | 512k ev/s                    |
| async×4   | 11.5M ev/s                | 498k ev/s                    |
| async×8   | 11.6M ev/s                | 506k ev/s                    |

Matches the 2026-07-05 numbers: async is ~1.7x where compression dominates,
~2-5% noise where fsync dominates (ordered commits serialize fsync regardless).
Live shape: sync ≈ 5.5M ev/s on tmpfs, ~480k ev/s on disk — two-plus orders of
magnitude above firehose rate (~1–3k ev/s). Conclusions unchanged:

- The async pipeline must be preserved for bootstrap wall-clock, but it earns
  none of its architectural footprint (8 dispatch points, 3 exclusion rules).
- The steady-state path has enormous headroom; nothing in this refactor is
  latency-critical on the live path as long as we don't add per-event fsyncs.

### 2.2 The current machinery to be replaced (inventory, verified against HEAD)

**Writer dispatch matrix** (`internal/ingest/writer.go`, `async_flush.go`) —
8 `w.async != nil` branches: Open, Close, SealActiveAndClose, Append,
appendLocked, Flush, DrainDurability, rotateIfFull. Three exclusion rules:

- `OnAfterFlush`×async — config validation error (config.go:165). Exists because
  the live consumer persists a *mutable* cursor (atmos watermark) rather than
  block-specific state (`live/consumer.go:357 onAfterFlush`).
- `ForceRotate`×async — runtime error (writer.go:692). Sole production caller is
  the steady delete compactor (`compact_deletes.go:99`), which only ever sees the
  sync steady writer.
- sink×async — panic (#249, writer.go:366). The prepared-block visibility hole.

**Subscribe-side machinery** (`internal/subscribe/`):

- `hotRing` (hotring.go): byte-budgeted (256MiB default) dense-seq FIFO,
  `idx = cursor - baseSeq` index math, non-dense-append reset heuristic.
- `Tail.regressFloor` (tail.go:49): two commits of patch-on-patch (1d02fa5,
  0a0fcb6) papering over what resets do to cursor classification. This is the
  freshest evidence the reset heuristic is a bug generator: #244's hardening
  itself needed two follow-up fixes within days.
- Cold walker seam-convergence machinery (replay.go:59-193): strict contiguity,
  hole detection, sub-tip convergence check against `Writer.NextSeq()`,
  two-retry `noAdvanceHoles` heuristic, loud `walk did not converge` error.
  Exists solely because the sealed/active/pending sources are read non-atomically
  across the rotation seam (#190).
- `SnapshotPending`-for-replay (replay.go:299): the walk's third source.
- Every event is **copied twice**: once into the writer's columnar pending
  block, once into the subscribe ring (`Tail.Append` copies the struct;
  entry memoizes up to 4 wire encodings on top).

**Wiring** (`internal/jetstreamd/runtime.go:375`): `OnSteadyStateWriter` installs
`w.SetOrderedEventSink(tail.Append)` and publishes `writerPtr`. The tail's
`nextSeq` closure reads `writerPtr.Load().NextSeq()`.

**Other readers of the writer's in-memory state** (must keep working):

- `pendingEventsForDID` (`internal/jetstreamd/pending.go`) — /status MST
  verification reads `SnapshotPending()` filtered by DID. Trivially re-served
  by the ring (strictly better: it also sees prepared/uncommitted events).
- Oracle/compaction/repoexport read `segment.WalkActive` on *files*, not writer
  memory — unaffected.

### 2.3 Durability semantics that must not move

- **DESIGN.md §3.1.1 ordering**: block fsync → pebble.Sync(seq/next + hook
  metadata) → maybe rotate. `commitDurableBatchLocked` (writer.go:921) is the
  single choke point and already maintains `w.durableNextSeq` — the watermark
  exists today; nothing reads it for visibility yet.
- **AppendBatch return contract** (#55): when Append/AppendBatch returns
  successfully, the just-filled block is durable (async callers wait on
  `job.done`). Backfill's completion batcher (`OnDurableBatch`) and its
  `DrainDurability` checkpoints depend on this.
- **Cursor ≤ durable data across crash**: live consumer persists the atmos
  watermark after block fsync + seq commit; restart replays the ≤1-block delta
  under at-least-once with the replay/dedupe guards (verifier rev-replay,
  #231 account ratchet, #234 identity ratchet).
- **Seqs are dense at append time**, allocated at exactly one point
  (`appendLocked`), seq 0 reserved. Post-compaction holes exist in history and
  are handled by manifest envelope bumps — the ring never sees those (they're
  below the floor by construction).

## 3. Design

### 3.1 The ring

A new type owned by `ingest.Writer` (new file `internal/ingest/readlog.go`):

```go
// readableLog is the writer-owned ordered ring of appended events. It is the
// authoritative read source for every seq in (floor, nextSeq): an event is
// present in the ring from the instant Append allocates its seq until eviction,
// and eviction is only legal below the durable watermark.
type readableLog struct {
    mu       sync.RWMutex
    entries  []logEntry // ring buffer, power-of-two capacity, index by seq&mask
    baseSeq  uint64     // seq of the oldest resident entry (floor+1)
    tipSeq   uint64     // one past the newest resident entry
    durable  uint64     // one past the newest durable seq (durableNextSeq mirror)
    curBytes int64
    maxBytes int64      // retention budget for the evictable (durable) region
    notify   chan struct{} // closed+replaced on append; readers park here
}
```

Key decisions:

1. **The ring stores `*subscribe`-agnostic entries.** Each entry is one deep-
   copied `segment.Event` plus a lazily-memoized wire-encoding slot. The
   encode-once machinery currently in `subscribe.Entry` moves down (or the ring
   exposes `*Entry`-compatible handles — see §3.6 on package layout). The event
   must be deep-copied at append: the caller's payload aliasing rules
   (`segment.Event.Payload` read-only, valid only for the call) are unchanged,
   and ring entries outlive the call by design.

2. **Two regions, one invariant.**
   - *Pinned region* `[durable, tipSeq)`: events not yet durable. They exist
     nowhere else readable (the pending block and prepared blocks are
     implementation details of the durability pipeline, no longer read
     sources). **Never evicted.** Bounded by pipeline depth: ≤ (workers+1)
     blocks × MaxEventsPerBlock events ≈ 5×4096×~630B ≈ 13MB worst-case at 4
     workers (using the pending block's own capacity estimate; the ~300B
     average from the prior note gives ~6MB typical).
   - *Retention region* `[baseSeq, durable)`: durable events kept for hot
     serving, evicted FIFO under `maxBytes` (default: keep today's 256MiB
     `DefaultHotTailBytes`, budget accounted like `Entry.approxBytes`).
   - **The invariant** (replaces all seam machinery): `baseSeq <= durable` at
     all times, i.e. *the ring floor never exceeds the durable watermark* —
     equivalently, every seq below `baseSeq` is durable and cold-readable.
     Enforced structurally: `evict()` refuses to advance `baseSeq` past
     `durable`. If the byte budget is exhausted purely by pinned entries
     (pathological giant events), the ring exceeds its budget rather than
     evicting unpinned-able entries — memory is bounded by pipeline depth
     regardless, and a metric (`readable_log_pinned_overrun_bytes`) makes it
     observable. Crash-loud alternative rejected: overrun is bounded and safe.

3. **Byte budget, not entry count.** Entry count is meaningless with 200B–1MB
   payload variance. Very large events are handled by the pinned/evictable
   split: a 10MB payload transiently blows the budget while pinned, then evicts
   promptly once durable if the budget is tight. No per-event cap needed beyond
   segment's existing column limits.

4. **No lag-adaptive retention (rejected for now).** Subscriber-lag-adaptive
   sizing couples the writer to subscribe-side state and turns a memory bound
   into a subscriber-controlled value — an adversarial slow client could pin
   memory. The slow detector + fixed budget already handle this; a lagging
   subscriber below the floor reads cold, which is the design working as
   intended. Revisit only with production evidence (kaizen note).

5. **Reads**: `ReadFrom(cursor, max)`-shaped lookup: RLock, if
   `cursor >= baseSeq && cursor < tipSeq` return the resident suffix (copied
   pointer slice, same discipline as today's tail); if `cursor >= tipSeq`
   caller parks on `notify`; if `cursor < baseSeq` → cold. The ring absorbs
   today's `Tail` classification logic *minus* regressFloor (impossible by
   construction) and *minus* the empty-ring `nextSeq()` special cases (the ring
   is never conceptually empty once the writer is open: `baseSeq == tipSeq ==`
   next-seq-at-open; a fresh subscriber at the tip parks — same behavior,
   no special case).

6. **Locking**: the ring has its own lock, subordinate to the writer's locks.
   Append path: writer holds `drainMu`(+`mu` for seq allocation) exactly as
   today, then inserts into the ring — under `drainMu`, after `mu` release, at
   the point the ordered sink fires today (this preserves the proven global
   seq-order delivery across producers). Watermark advance: after
   `commitDurableBatchLocked` succeeds, writer calls `ring.advanceDurable(nextSeq)`
   (cheap, ring lock only). Reader path: ring RLock only — **subscriber reads
   no longer contend with the writer mutexes at all** (today `SnapshotPending`
   takes `w.mu` on every cold walk; that contention disappears).

### 3.2 Watermark semantics at rotation, seal, and restart

- **Rotation/seal do not touch the ring.** Seqs are dense across segment
  boundaries; the ring is indexed by seq alone. `rotateLocked`'s
  publish-manifest-before-bump ordering stays (the cold path still needs the
  manifest), but the *subscriber-visible* seam disappears: a reader crossing a
  rotation is either below the floor (manifest has the sealed segment — it was
  published before any post-rotation commit could advance the watermark past
  it, see below) or in the ring. The four-source walk collapses to two sources
  with one boundary.
- **Ordering obligation at rotation** (the new invariant's one subtle case):
  the watermark must not advance past seqs in a just-sealed segment before that
  segment is manifest-visible, or a cold read at `floor-ε` could miss it.
  Today's code already satisfies this: `rotateLocked` runs under `mu` and fires
  `OnAfterSeal` (manifest publish) before returning; the durable-batch commit
  for the sealed segment's final block happened *before* the seal, and eviction
  below the floor only matters for seqs ≤ watermark, all of which are in
  flushed blocks of the sealed file or earlier segments, both reachable via
  manifest + `WalkActive`. Stage C adds a red-first test pinning exactly this:
  *cold read at ring floor − 1 always succeeds during concurrent rotation*.
- **Restart**: the ring is memory-only. On `Open`, `baseSeq = tipSeq = durable =`
  reconciled next-seq. Visibility floor = durable tip, i.e. everything is cold
  until new appends arrive — exactly today's post-restart behavior (empty hot
  ring, cold reads from disk). Subscriber cursor semantics across restart are
  unchanged: cursors are client-held seqs resolved against manifest + writer
  tip; no server-side cursor state exists. Confirmed no change needed.
- **Close/terminal**: `Close`/`SealActiveAndClose` drain the pipeline, so the
  watermark reaches `nextSeq` and the pinned region empties before the writer
  goes away. Late reads against a closed writer read cold (writerPtr semantics
  unchanged; only relevant in tests — production never closes the steady
  writer while serving).

### 3.3 One pipeline (Stage A: sync = async with inline executor)

The writer keeps exactly one append→prepare→compress→commit path:

- `Append`/`AppendBatch`: allocate seqs into the pending block under `mu`;
  when a block fills, `PrepareFlush` detaches it and produces a job
  (today's `prepareAsyncFlushLocked`, now unconditional).
- `workers == 0` (sync mode): the job executes **inline before Append returns**
  — compress + `CommitPreparedFlush` + `commitDurableBatchLocked` on the
  caller's goroutine, preserving today's sync semantics exactly (durable on
  return, same fsync cadence, same hook timing). No pipeline goroutines exist.
- `workers > 0`: today's pipeline (parallel compress, single ordered committer),
  unchanged in structure. Callers still wait on `job.done` before returning
  (the #55 durable-on-return contract is not weakened by this refactor —
  loosening it is a separate future decision).
- Rotation: one code path keyed on `activeBytes >= MaxSegmentBytes` that drains
  the pipeline to quiescence and rotates — `rotateIfFull` generalized; in
  inline mode the drain is a no-op, so it degenerates to today's
  `flushAndRotateLocked` behavior. `ForceRotate` uses the same drain — the
  async carve-out is deleted, and `ForceRotate` becomes legal on any writer.
- `Flush`, `DrainDurability`, `Close`, `SealActiveAndClose`: single
  implementations over "prepare current pending + drain to quiescence".

This deletes the 8-branch matrix. Config: `AsyncFlushWorkers` is renamed
`FlushWorkers` (0 = inline; project is pre-production, no compat shim), and the
`OnAfterFlush`×async validation rule is deleted along with `OnAfterFlush`
itself (see Stage 0).

### 3.4 Stage 0: live cursor migrates OnAfterFlush → OnDurableBatch

The only `OnAfterFlush` consumer is `live.Consumer.onAfterFlush`. Migration:

- `live.Open` installs an `OnDurableBatch` hook instead. The hook stages
  `relay/cursor` (versioned uint64, same encoding) **and**
  `SyncStateStore.StageFlush` into the writer's durable batch;
  `afterCommit` runs `SyncStateStore.CommitStaged()` + cursor metric.
  This is strictly better than today: cursor + syncstate + seq/next become
  **one atomic synced batch** instead of two sequential commits (removes the
  crash window between seq-commit and cursor-commit — currently benign in
  direction, but one less state to reason about).
- Semantics check (the mutable-cursor concern that created the exclusion): the
  hook samples `cursorValue()` (atmos watermark) at commit time. The watermark
  is monotonic and only covers upstream seqs whose ops were already appended
  (buffered) before yield. Under inline execution, commit time == flush time —
  identical to today. Under worker execution (not used by live today, but now
  legal), commits are ordered, and a watermark sampled at commit time covers at
  most ops that were appended before that commit's block was *prepared* — the
  cursor can only be *ahead* of a specific block's content if those ops are in
  a *later* prepared block, which commits before any later watermark sample.
  Wait — the safety condition is cursor ≤ durable data. The watermark at
  commit(block N) may cover ops living in block N+1 (prepared, not yet
  committed). A crash then loses block N+1 but persists a cursor past its
  events → **data loss**. Two mitigations considered:
    1. Sample the watermark at *prepare* time and carry it on the job
       (block-specific cursor, the "make the hook block-specific" fix
       config.go always promised). Correct in all modes.
    2. Keep live on inline mode (workers=0) where commit==prepare, and assert.
  **Decision: (1).** `PrepareFlush` job carries `cursorAtPrepare`; the durable
  hook receives it via the job context (new optional field on the hook
  signature or a per-writer "watermark provider" sampled in `appendLocked` —
  resolved at implementation to whichever is cleaner; the contract is
  *cursor value staged with block B must have been read before B was
  detached*). This makes the cursor block-specific for real, closes the
  exclusion permanently, and is still correct inline. The `cur == 0`
  syncstate-only branch and `Close()`'s explicit final save carry over as-is
  (force-commit path handles the terminal case).
- Backfill's completion batcher already owns `SetDurableBatchHook` on the
  *backfill* writer; live installs its hook at `live.Open` config time on its
  *own* writer — no collision (different writer instances). But note the
  bootstrap-live writer and steady-live writer both get the cursor hook —
  same as today's OnAfterFlush wiring.
- Oracle gate: `just test-long ./internal/oracle -run TestOracle_Restart` plus
  the storefault tests (`TestConsumer_SaveCursorFailsLoudOnStoreFault` must be
  re-targeted at the batch-commit fault point;
  `TestWriter_DurableBatchFailsLoudOnStoreFault` already covers the shared
  path).

### 3.5 Stage C: repoint readers; delete the machinery

With the ring authoritative:

- **`subscribe.Tail`** keeps its public shape (`ReadFrom`, `Tip`, conn
  registry, slow detector, metrics) but its hot tier becomes a thin adapter
  over `writer.ReadLog()` instead of an owned `hotRing` fed by a sink.
  Deleted: `hotRing`, `Tail.Append`, `regressFloor`, the gap-reset heuristic,
  `SetOrderedEventSink` (+ #249 panic), `hot_ring_resets_total` (replaced by
  ring metrics: `readable_log_bytes`, `readable_log_pinned_bytes`,
  `readable_log_floor_seq`, `readable_log_durable_seq`).
- **Cold reader**: `WalkFromCursor` serves only `[cursor, ringFloor)` from
  manifest + `WalkActive` on the active file's *flushed* region. Deleted:
  `SnapshotPending`-for-replay, strict-contiguity hole detection, seam retry
  loop, `noAdvanceHoles`, the `walk did not converge` error, the sub-tip
  `NextSeq()` convergence check. The walk becomes: sealed sweep → active
  flushed sweep → if cursor still below the floor it handed us, *the floor
  moved up while we walked* — re-read the floor and continue (bounded, floor
  is monotonic); events between old-floor and new-floor are durable and
  file-visible by the invariant. One boundary, one happens-before
  (watermark advances only after fsync+manifest visibility), zero heuristics.
- **Handoff**: `Tail.ReadFrom` classifies once against the ring floor
  (ring read / cold read / park), the same transparent loop as today.
- **`SnapshotPending` residual consumers**: `pendingEventsForDID` (status MST
  check) re-reads from the ring (`ReadLog().Range(durableRegion+pinned)` or a
  purpose-built `PendingForDID`); `segment.Writer.SnapshotPending` itself can
  then go package-private or die (repoexport comment reference only). The
  oracle's uses are on files, unaffected.
- **`ingest.Writer` API deltas**: `+ReadLog() *ReadableLog` (or methods
  directly on Writer), `-SetOrderedEventSink`, `-SnapshotPending`,
  `AsyncFlushWorkers→FlushWorkers`. `NextSeq()` stays (cursor resolution,
  status).
- **Config**: new `ReadLogRetentionBytes` on `ingest.Config` (default 256<<20,
  0 legal = pinned-only ring; subscribe's `HotTailBytes` knob is deleted /
  forwarded). **Bootstrap backfill runs with retention 0**: no subscribers
  exist pre-steady (503-gated), so the ring holds only the pinned region —
  this answers the issue's "zero-retention during bootstrap" question with
  yes, via configuration rather than a special mode. The append-time deep copy
  still costs something at 1M+ ev/s; measured by the Stage C bench gate, and
  if it shows up, a `retention==0 && no-reader` fast path that skips the copy
  for already-durable-on-return batches is the escape hatch (decide on data,
  not speculation).

### 3.6 Package layout

The ring lives in `internal/ingest` (it is writer state). The encode-once
`Entry` memoization is subscribe's concern (wire formats live there). To avoid
an ingest→subscribe dependency, the ring stores events plus an opaque
`atomic.Pointer[any]`-style memo slot that subscribe's adapter populates —
or more simply: the ring returns `*segment.Event` handles with stable identity,
and subscribe keeps a small seq-keyed memo cache for encodings of *resident*
entries. Resolved at implementation; the constraint is: **no second copy of
event bytes** (the design's memory win) and **no ingest import of subscribe**.

### 3.7 What becomes structurally impossible

- #244-class feed bypass: any append through the writer is in the ring by
  construction; there is no second feed to forget to wire.
- #249-class advertised-but-unreadable: an event is readable from the instant
  its seq exists; durability stage is irrelevant to visibility.
- #190-class seam holes: cold reads only touch seqs below a floor that is ≤
  the durable watermark; the walk has no in-flight sources to tear across.
- Ring index-math corruption from non-dense feeds: the ring is fed at the seq
  allocator itself; a non-dense insert is `panic`-level internal-invariant
  violation (crash-loud is correct there — it means seq allocation itself
  broke, which is persistence-corruption territory, not user input).

## 4. Staging plan (issues, branches, gates)

Each stage: own issue + branch + roast; full oracle tiers green before merge.
Stage boundaries are chosen so the system is shippable after every stage.

### Stage 0 — `live: persist relay cursor via OnDurableBatch` (issue at start)
Migrate cursor+syncstate into the writer's durable batch with prepare-time
watermark sampling (§3.4). Delete `OnAfterFlush` (hook + config rule + docs).
- Red-first: crash-window test — cursor staged with block B never exceeds the
  watermark at B's prepare; storefault re-target; restart oracle.
- Gate: `just` + `just test-long ./internal/oracle -run TestOracle_Restart`
  + `just oracle` + storefault tier.
- Risk: low; independently shippable and valuable (one atomic batch).

### Stage A — `ingest: unify sync/async writer into one flush pipeline`
Inline-executor unification (§3.3). No visibility changes; readers untouched.
Mechanical but load-bearing: this is where the 8-branch matrix dies.
- Red-first: sync-semantics parity tests (durable-on-return with workers=0;
  hook cadence identical), `ForceRotate` on a workered writer, rotation under
  concurrent producers at quiescence.
- Gate: full oracle tiers; `BenchmarkWriterBackfillShape` no-regression on
  tmpfs AND ext4 (both shapes recorded in the PR); mutation campaign re-run
  (expect m011 and friends unaffected; refresh anything STALE).

### Stage C1 — `ingest: writer-owned readable log ring + durable watermark`
Add the ring, fed from the (now single) append path; watermark advance from
`commitDurableBatchLocked`; pinned/evictable regions; metrics. The ordered
sink still exists and subscribe still works exactly as today — the ring runs
dark (asserted-but-unread) for one stage.
- Red-first: invariant tests — floor ≤ watermark under concurrent
  append/flush/rotate/close (swarm test); pinned region never evicts; byte
  budget honored; notify wakeups; deep-copy aliasing.
- Gate: full oracle + bench (ring insert cost visible in backfill shape;
  budget: <5% on tmpfs shape, else the retention-0 fast path from §3.5).

### Stage C2 — `subscribe: read the writer ring; delete hot ring, sink, and seam machinery`
Repoint `Tail` hot tier; simplify `WalkFromCursor` to floor-bounded; delete
`hotRing`/sink/`SnapshotPending`-for-replay/regressFloor/seam retries; move
`pendingEventsForDID`; docs §4.3/§5 rewrite.
- Red-first: cold-read-at-floor−1 under concurrent rotation (the §3.2 test);
  live-edge park/wake parity; replay-from-0 full-fidelity against a written
  history (existing partb oracle scenarios are the backstop); restart floor.
- Test migration: `walk_seam_test.go` and `tail_gap_test.go` largely pin
  machinery that no longer exists. Port the *contracts* (no silent holes, no
  wedge, no wrong-seq) onto the new invariant; delete heuristic-specific
  cases; keep `TestWalkFromCursor_ConcurrentRotationSeam` reshaped as the
  floor-invariant stress test.
- Gate: full oracle tiers incl. live-tail replay tier + `just oracle-sweep`;
  mutation campaign full re-run — new mutants required (see below); docs
  updated; `git grep -i 'hot ring\|ordered sink\|SnapshotPending'` clean of
  stale references.

### Mutation-campaign work (lands with C2)
The current campaign has **no mutants** on the sink/hot-ring/seam machinery
(verified). The refactor's new load-bearing seams need coverage:
- mutant: watermark advances before pebble commit success (visibility ahead of
  durability) — killed by cold-read-at-floor test / live-tail tier.
- mutant: evict ignores the pinned boundary (`baseSeq` past `durable`) —
  killed by replay-cursor oracle (hole below floor).
- mutant: ring insert skipped on the batch partial-failure prefix — killed by
  the #244-descendant oracle scenario (retry rows visible to subscriber).
- mutant: prepare-time cursor sampling replaced by commit-time — killed by the
  Stage 0 crash-window test promoted into the restart tier.
Predictions recorded per `testing/mutation` convention; STALE review for any
existing mutant whose context moved (m032 and partb cursor mutants should
survive untouched — they target cursor *policy*, not the walk).

### Explicitly out of scope (own issues, filed when we get there)
- Loosening the durable-on-return AppendBatch contract (pipelined ack) — the
  ring makes it *possible* (visibility no longer needs the wait); it is a
  separate performance decision with its own oracle implications.
- Lag-adaptive retention (§3.1.4) — needs production data.
- Worker-count tuning on production storage (issue note: re-measure; the value
  is compression:fsync-ratio-dependent).

## 5. Definition of done (mirrors issue #248, made concrete)

- One append/flush/rotate/close path; zero `w.async != nil`-style behavioral
  branches; all three exclusion rules deleted (not documented — deleted).
- /subscribe hot path reads the writer ring; `SetOrderedEventSink`,
  `subscribe.hotRing`, `regressFloor`, seam-convergence retries, and
  `SnapshotPending`-for-replay are gone from the tree.
- Invariant `ring floor ≤ durable watermark` pinned by red-first unit + swarm
  tests and an oracle scenario; cold reads below the floor always succeed.
- Oracle tiers green at every stage boundary; mutation campaign re-run with
  the four new mutants KILLED and no new SURVIVED.
- `BenchmarkWriterBackfillShape` within noise of today's async×4 on tmpfs and
  ext4; live-shape unregressed.
- `docs/README.md` §4.3/§5 describe the ring/watermark model; the sink-era
  paragraph (PR #240) removed.

## 6. Open questions for Jim before Stage 0 starts

1. **Hook plumbing for prepare-time cursor sampling** (§3.4): a generic
   "sample-at-prepare" value threaded through the job, or a live-specific
   solution? I lean generic (`OnDurableBatch` gains a per-block opaque value
   captured at prepare; backfill's batcher ignores it) — one contract, no
   special cases. Costs a small API change to the hook signature.
2. **Package placement of encode-once memoization** (§3.6): memo slot on ring
   entries (ingest stays wire-format-agnostic via `any`) vs. subscribe-side
   seq-keyed cache. I lean memo-slot-on-entry: it preserves today's
   encode-exactly-once-per-event property across all subscribers with zero
   coordination; the `any` is ugly but contained.
3. **Stage granularity**: C1/C2 as separate PRs (ring runs dark for one merge)
   vs. one C branch. I lean separate — the dark stage de-risks the invariant
   under real oracle load before any reader depends on it.
