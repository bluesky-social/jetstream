# Seq reuse after crash: delivered seqs can be reassigned to different events

Date: 2026-08-25. Status: problem spec + proposed design, ready for handoff.
Scope: this one defect only. (A future backup/restore feature will reuse the
same mechanism with a larger window, but nothing here depends on it.)

## Problem statement

Jetstream delivers events to live subscribers *before* they are durable, and
after a crash it resumes the seq counter from durable state. The seqs in
between get handed out a second time — usually to the same events, but not
provably, and sometimes not actually. A client that saved a cursor from that
window can silently miss or misattribute events forever after.

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

Cursor *rewinds* (re-delivery) are already the contract — at-least-once,
clients dedupe. No client-side rule can fix *reuse*: a client cannot
distinguish "seq 500 is the event I saw" from "seq 500 is now a different
event" without comparing payloads, which defeats the point of a cursor. The
server is the only place the invariant can live.

## Proposed design

Three cooperating pieces. The theme: make post-crash seq vacancies **explicit
and durable**, so cursors resolve across them losslessly and genuine holes
stay crash-loud.

### 1. Epoch bump on unclean boot

- On graceful shutdown, the writer's terminal durable batch additionally
  writes a clean-shutdown marker (e.g. `seq/clean_shutdown`) recording the
  final `nextSeq`. `ingest.Open` deletes the marker as soon as it reads it
  (before any append), so the marker's presence proves the previous exit was
  clean *and* nothing has run since.
- On boot, after the existing reconcile: if the marker is present and matches
  the reconciled seq, no bump — the common deploy/restart path stays gap-free.
  If the marker is absent or mismatched (crash, kill -9, torn state), bump:
  `nextSeq += gapSlack`, and durably record the vacancy (see §2) in the same
  pebble write as the bumped `seq/next`, before the first append.
- `gapSlack` must exceed the maximum possible delivered-minus-durable window.
  Derive the bound from code (pending block + async pipeline depth, i.e.
  roughly `(AsyncFlushWorkers + 1) × MaxEventsPerBlock`), then use a flat
  constant with a wide margin — 1<<20 is suggested — plus a startup assertion
  that the constant exceeds the derived bound for the configured writer. Do
  not make it clever; make it obviously sufficient.
- Seq-space budget: v1 cursor disambiguation caps usable seqs at 1e15
  (`CursorSeqMaxThreshold`, `internal/subscribe/cursor.go:15-31`, with the
  non-overlap argument in `specs/notes/2026-05-27-cursor-replay-design.md`).
  At 1<<20 per crash that allows ~10^9 crashes; update the comment's
  arithmetic to account for bumps.
- Scope note: only the steady writer's counter (`seq/next`) needs this.
  Bootstrap-phase seqs (`live_segments/seq/next`) are never client-visible —
  serving ungates at steady state — but the implementer should confirm that
  and document it where the decision lands.

### 2. Durable gap registry

- New pebble keyspace, e.g. `seq/gap/<start>` → `{end, reason, boot_time}`
  (or one `seq/gaps` record; implementer's choice — entries are tiny and
  count one-per-crash). Loaded once at startup into an immutable in-memory
  set; gaps are only ever created at boot, so no synchronization story.
- The registry is the source of truth for "this vacancy is legitimate."
  Anything that encounters a seq hole NOT covered by the registry keeps
  today's crash-loud behavior.

### 3. Lossless cursor resolution and replay across registered gaps

Two consumers need the registry:

- **`ResolveCursor`** (`internal/subscribe/cursor.go`): a requested seq cursor
  that lands inside a registered gap is clamped forward to the gap end, with
  `Clamped = true` and a dedicated metric label. This clamp is semantically
  lossless — vacant seqs name no events — so it applies silently on **both**
  endpoints; it is not a too-old condition and must not trip v2's
  `RejectBelowFloor`. The timestamp path needs the same post-translation
  check: `translateTimeUSToSeq` can return `last.MaxSeq + 1`
  (`cursor.go:256-263`), which can sit inside a gap after a crash.
- **The cold walker** (`internal/subscribe/replay.go`): resolving is not
  enough — a deep replay that *starts below* a gap must cross it.
  `walkSealedRegion` currently returns at a coverage miss and
  `WalkFromCursor`'s no-progress guard then fails loud ("rotation seam
  invariant violated", `replay.go:164-170`). Thread the gap set through
  `WalkInput`; when the walk's `current` enters a registered gap, jump it to
  the gap end (metric), and leave the no-progress guard exactly as it is for
  unregistered holes. Take care at the boundaries: a gap ending at the
  readable-log floor, a gap covering `StartSeq`, and the interaction with the
  rotation-seam retry (a seam retry that lands in a registered gap must not
  count as progress-by-gap-jump masking a real seam violation — keep the two
  signals separable).

Also confirm the hot path: after a bump, the readable log is constructed at
the bumped seq (`writer.go:226-228`), so `FloorSeq` sits at/above the gap end
and hot reads never see the vacancy. The manifest already tolerates
non-contiguous seq ranges (`validateSegmentSeqMonotonicity` rejects overlap,
not gaps), and `SegmentForSeq` deliberately reports gaps as not-found
(`internal/manifest/manifest.go:583-605`) — that contract can stand; the gap
knowledge belongs to the callers above, not the manifest.

### Client-visible contract

- No wire change. Stale cursors resolve to duplicates-then-continue, which is
  the existing at-least-once contract. The vacancy itself is invisible except
  as a seq jump, which clients must already tolerate (compaction thins seqs
  today).
- The module-root client dedupes by seq and cuts over archive→live; verify
  its cutover handles an archive tip followed by a gap to the live tip
  (it should — the server replays from the requested cursor and the gap jump
  is server-side — but test it).
- Document in `docs/README.md` §2 and `specs/invariants.md`: strengthen the
  seq invariant to "a seq observable by a client is never reused for a
  different event, across crash and restart," and describe registered
  vacancies.

## Alternatives considered

- **Unconditional bump every boot** (no clean marker): simpler, but litters
  gaps on every deploy and makes the gap path the common path. Rejected —
  the marker is one key in a batch that already exists.
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
  for free with a constant. Rejected (mechanical sympathy).

## Test plan (red-first where possible)

- **Unit, ingest:** Open-after-crash bumps and records a gap; Open-after-clean
  does not; marker is deleted on read (a crash immediately after boot still
  bumps on the next boot); slack assertion trips when misconfigured.
- **Unit, subscribe:** `ResolveCursor` matrix — cursor inside gap (both
  endpoints, seq + timestamp modes), at gap boundaries, gap + lookback-floor
  interaction (floor above/below/inside the gap), v2 `RejectBelowFloor`
  not tripped by gap clamps.
- **Unit, walker:** replay starting below, inside, and above a registered
  gap; multiple gaps in one walk; unregistered hole still fails loud (this
  test guards the guard).
- **Oracle:** the crash/restart tier is the natural home and this is the
  red-first vehicle: have a subscriber record (seq → event identity) for
  delivered events pre-crash, crash inside the delivered-not-durable window
  (crashpoint seams exist), restart, re-subscribe with the pre-crash cursor,
  and assert no seq ever maps to a different event identity than first
  observed. This test MUST fail against today's code (that failure is the
  proof the defect is real) and pass with the fix. Note `specs/oracle.md`
  discipline and add a diary entry if the work uncovers adjacent flakes.
- **Mutation campaign:** after landing, add mutants for (a) dropping the bump
  on unclean boot, (b) skipping the gap-registry write, (c) clamping
  unregistered holes — predict which tier kills each, run
  `just mutation-campaign` on a clean tree, update `testing/mutation/RESULTS.md`.
- **Determinism:** `TestOracle_SameSeedTraceDeterminism` must stay green (the
  bump is deterministic given a deterministic crash point).

Per `AGENTS.md`, this touches ingest/cursor/restart-recovery: run `just`,
`just test-long ./internal/oracle`, `just oracle-sweep`, and
`just fuzz 30s ./segment` at minimum.

## Open questions for the implementer (decide and document, don't stall)

1. Registry shape: one key per gap vs one record; pick the one that keeps the
   boot path simplest.
2. Whether `getConfig`/status should surface gap count/last-gap for operator
   visibility (`/status` already aggregates similar counters) — recommended.
3. Exact slack constant and where the derived-bound assertion lives.
4. Whether the resolver clamp and walker jump share one helper (they should
   if it doesn't contort the signatures).

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
