# Seq reuse after crash: delivered seqs can be reassigned to different events

Date: 2026-08-25. Status: implemented on `jc/issue-345`.
Scope: this one defect only. (A future backup/restore feature will reuse the
same mechanism with a larger window, but nothing here depends on it.)

## Problem statement

Before this fix, live delivery preceded durability, but crash recovery resumed seq allocation from durable state. Reused seqs could identify different events, causing clients with saved cursors to miss or misidentify records.

The current invariant (`docs/README.md` §3.1.1, `specs/invariants.md`) says
"sequence numbers will never go backwards or be duplicated; they only go
forward (even on crash and restart)." That is true of the *durable archive*,
but not of the *delivered stream*: a seq that a client has observed can be
reassigned to a different event after a crash. This spec proposes closing
that gap:

> **A seq observable by any client is never reused for a different event,
> across crash and restart.**

## Mechanics of the defect (verified against code)

1. **Delivery leads durability.** `appendLocked` copies the event into the
   in-memory readable log at seq assignment (`internal/ingest/writer.go`,
   `w.readLog.append` immediately after `candidate.Seq = w.nextSeq`), before
   the block is fsynced and before the pebble batch commits. Live subscribers
   are served from the readable log, so they see seqs the moment they are
   allocated. The delivered-but-not-durable window is the pending block plus
   whatever the async flush pipeline has in flight (`AsyncFlushWorkers`,
   `writer.go:233-235`) — order of one to a few blocks of ≤4096 events each.

2. **Crash recovery resumes at the durable tip.** `ingest.Open` reconciles
   `nextSeq = max(pebble seq/next, scan-of-segments max + 1)`
   (`internal/ingest/writer.go:197-228`). Both inputs are durable state; both
   lag the delivered tip after a crash. The next appends therefore reuse seqs
   that were already delivered.

3. **The reused seqs can name different events.** After the crash, the writer
   re-ingests from the persisted `relay/cursor` (committed in the same durable
   batch, so it also lags). Relay replay re-delivers the same firehose events
   in the same order — but the writer is not fed by the firehose alone. The
   failed-repo retry runner, `#sync` repair downloads, and the net-new-DID
   enqueuer all append through the same writer from separate goroutines, so
   any non-firehose append that landed in the lost window (or lands during the
   replay) shifts the alignment. Upstream relay gaps (`GapError`) and
   guard-suppressed replays shift it further. Once shifted, seq N durably
   names a different event than the one a client observed under N.

4. **The client impact is silent.** A client resuming at cursor c from the
   lost window either (a) reconnects before the counter catches up — the
   future-cursor path silently drops it to live mode
   (`internal/subscribe/cursor.go:155-157`) — or (b) reconnects after, and
   replays from c *as if the seqs beneath c were the events it saw*. The
   events it actually observed under (durable, c] are gone or renumbered; it
   never re-receives them and gets no signal. This is a silent-gap violation
   of at-least-once, which is exactly the guarantee `docs/README.md` §2 tells
   clients they can build on. Note the failure needs no cooperation from the
   client: saving a cursor from a delivered event is the documented usage.

Graceful shutdown does **not** have this window: `Writer.Close` fsyncs the
buffered block and then commits the terminal durable batch
(`writer.go:247-277`), so delivered == durable at a clean exit. Only unclean
exits create the window.

## Why not "clients should tolerate rewinds"?

At-least-once consumers can deduplicate replay, but cannot detect a seq reassigned to a different event without comparing payloads. The server must prevent reuse.

## Implemented design

The implementation uses a one-block **write-ahead seq lease**, not a fixed
epoch bump or clean-shutdown marker. This tightens the bound to the actual
configured block size and proves safety directly at the publication point.

### 1. One-block write-ahead lease

- `seq/next` is the exact durable coverage frontier: every lower value is
  represented by a durable event or a registered vacancy.
- `seq/max_reserved` is the exclusive end of the client-visible allocation
  lease. Before `Open` returns, it synchronously reserves
  `[nextSeq, nextSeq + MaxEventsPerBlock)`. Before readable-log publication,
  `Append` requires `candidate.Seq < maxReserved`.
- A block flush fsyncs segment bytes first, then advances exact `seq/next` and
  renews the lease to `nextSeq + MaxEventsPerBlock` in the existing synced
  pebble metadata batch. The relay cursor and other event-backed metadata stay
  atomic with that commit. A renewal failure is fatal to the append and no
  allocation can pass the old lease end.
- `DrainDurability` is non-terminal and renews the full lease. Terminal
  `Close` flushes pending data and collapses `seq/max_reserved` to exact
  `nextSeq`, so a graceful restart creates no vacancy.
- Lease-enabled writers reject async block flushing. The configured block is
  therefore the exact maximum visible-minus-durable window. The steady server
  currently uses the internal 4096-event default; `MaxEventsPerBlock` is known
  by `ingest.Config` at startup but is not currently a server-admin CLI/env
  option.
- Only the canonical steady namespace (`seq/next`) may enable leasing.
  Bootstrap writers remain unleased because serving is lifecycle-gated. The
  pre-serving merge transition explicitly attests that legacy bootstrap seqs
  were unobservable when it first adopts the canonical writer.

### 2. Recovery and durable vacancy registry

On startup, the writer reconciles exact durable state from `seq/next` and
segment blocks. If the prior reservation leads that tip, it atomically:

1. registers `[durableNext, priorReserved)`;
2. resumes allocation at `priorReserved`; and
3. reserves one new configured block.

Registry records use `seq/gap/<big-endian start>` with a versioned value
containing reason (`crash`) and exclusive end. Startup loads, validates, sorts,
coalesces, and canonicalizes these half-open intervals. Every interval is
checked against each non-empty durable block range; overlap is internal
corruption and fails `Open`. Segment envelopes are deliberately not used for
this validation because a legitimate vacancy can lie between two blocks in
the same segment.

A direct upgrade from a legacy data dir with no reservation key conservatively
burns one configured block. Absence of the key cannot prove the old process
exited cleanly: its final pending block could have been client-visible. This is
a one-time compatibility cost. Repeated crashes without progress consume one
lease each and adjacent vacancies coalesce.

The cursor namespace ceiling is still 1e15. At the default 4096 values per
abandoned lease, exhausting it without event traffic would require more than
244 billion unclean startups.

### 3. Lossless cursor resolution and replay

`ResolveCursor` losslessly advances a seq cursor inside a registered vacancy
to its exclusive end before applying v2's too-old decision. A vacancy ending
at the lookback floor is therefore not stale; one ending below the floor still
is. Timestamp translation applies the same normalization, including after a
lookback-floor clamp. Gap clamps have a dedicated metric label.

The cold walker uses the same immutable registry. It jumps only when `current`
is contained by a registered interval and records jump count/width metrics.
The jump is tracked separately from rotation-seam progress, so an adjacent
unpublished segment still retries normally and an unexplained hole still
trips the existing no-progress invariant. The manifest remains gap-agnostic.

The readable log starts at recovered `nextSeq`, so hot reads begin at or after
the vacancy end and never need special handling.

### Client-visible contract

- No wire change. Stale cursors resolve to duplicates-then-continue, which is
  the existing at-least-once contract. The vacancy itself is invisible except
  as a seq jump, which clients must already tolerate (compaction thins seqs
  today).
- The module-root client's archive→live test covers an archive tip followed by
  a registered server vacancy and confirms the forward jump causes neither a
  reconnect nor a re-backfill loop.
- `docs/README.md` §2/§3.1.1 and `specs/invariants.md` carry the strengthened
  client-observable seq invariant and registered-vacancy contract.

## Alternatives considered

- **Fixed epoch bump plus clean-shutdown marker:** the original proposal used
  `1<<20`. Rejected in favor of the write-ahead lease: the configured block
  size is the exact synchronous-window bound, the lease itself distinguishes
  clean from unclean shutdown, and there is no second marker protocol.
- **Async lease sized by `(workers+1)*blockSize`:** possible, but broadens the
  exposed window and complicates proof across ordered in-flight commits.
  Client-visible writers instead reject async flush; bootstrap-only writers
  may continue using it because their seqs are not served.
- **Generic clamp-forward on any coverage gap** (no registry): simplest for
  the walker, but destroys the crash-loud property — a missing/corrupt
  segment file would silently skip events. Rejected outright; the loud
  no-progress guard exists because issue #190 was a real silent-loss bug.
- **Flush-before-deliver** (make delivery trail durability): closes the
  window at the source but adds fsync latency to the hot delivery path and
  changes the readable-log design substantially. Out of proportion to the
  defect; not pursued.
- **Persist a delivered high-water mark**: a synchronous pebble write per
  block-worth of delivered events on the hot path, to shave slack we can get
  with a constant. Rejected because it adds writes to the delivery path.

## Implemented verification

- **Ingest:** fresh/open/crash/clean-close, repeated crash coalescing, partial
  flush, non-terminal drain, legacy migration, pre-serving merge attestation,
  configured block-size change, startup/renewal commit faults, malformed and
  overlapping records, lease ceiling, and a deterministic 64-crash model.
- **Unit, subscribe:** `ResolveCursor` matrix — cursor inside gap (both
  endpoints, seq + timestamp modes), at gap boundaries, gap + lookback-floor
  interaction (floor above/below/inside the gap), v2 `RejectBelowFloor`
  not tripped by gap clamps.
- **Unit, walker:** replay starting below, inside, and above a registered
  gap; multiple gaps in one walk; unregistered holes across segments, between
  blocks in one segment, and in the active-only path still fail loud. Sparse
  rows inside a compaction-preserved block envelope remain valid without a
  crash-gap record.
- **Oracle:** a real websocket subscriber records `(seq,event identity)` from
  a pending sub-block event in a child process. The parent SIGKILLs the child,
  reopens the same data dir, and proves a different event starts at the lease
  end rather than reusing the observed seq. The old implementation reuses 1.
- **Mutation campaign:** the dedicated `seqlease` tier kills six mutants:
  resuming at the durable tip, omitting renewal, skipping registry persistence,
  silently accepting cross-segment or same-segment unregistered replay holes,
  and terminally collapsing a non-terminal drain.
  `testing/mutation/RESULTS.md` records the targeted run.
- **Determinism:** `TestOracle_SameSeedTraceDeterminism` must stay green (the
  lease transition is deterministic given durable state and block size).

Per `AGENTS.md`, this touches ingest/cursor/restart-recovery: run `just`,
`just test-long ./internal/oracle`, `just oracle-sweep`, and
`just fuzz 30s ./segment` at minimum.

## Decisions recorded

1. One versioned pebble key per normalized gap, keyed by big-endian start.
2. `/status` surfaces lease end/headroom, gap count/width, and latest range;
   Prometheus exposes reservation, registration, clamp, and replay-jump data.
3. The lease width is exactly the configured `MaxEventsPerBlock`; no slack
   constant remains.
4. Resolver and walker share the immutable `seqspace.Gaps.EndContaining`
   primitive while retaining their own control flow and observability.

## Related, explicitly out of scope

- Backup/restore to object storage will later create much larger vacancies
  via the same bump + registry mechanism (see
  `specs/notes/2026-08-25-s3-backup-research.md` §"The cursor problem").
  Design the registry so a gap's `reason` can distinguish crash from restore,
  and nothing else here needs to know about backups.
- The docs/code divergence on the steady-state 30s block flush timer
  (`docs/README.md` §3.1.1 describes it; only the backfill drain has one).
  Separate kaizen item; fixing it narrows the wall-clock exposure of the
  window but does not change this design.
