# Oracle failure diary — retry appends bypass the /subscribe tail (hot-ring seq hole)

- **Date:** 2026-07-05
- **Commit (failure observed on):** `376d718` (branch `issue-207-backfill-adversity`) + one harness edit (hidden net-new account seeded with 1 backfill-only record instead of 0)
- **Test:** `TestOracle_DefaultLifecycle` (fast mode, seed 42)
- **Symptom:**
  - `steady-state-client-backfill mode=fast seed=42: client stream final state does not match simulator ground truth` — `oracle: missing did:plc:qdkuu6zicd2z4zoamuxrvlpw app.bsky.feed.post/22bg6if53ce32 rev=`
  - The record IS durably archived: a direct segment scan shows the hidden DID's retry-resync rows at seqs 85–87 (sync tombstone + create_resync ×2). The REAL public client, replaying the full archive through `/subscribe`, receives every seq except exactly `[85 86 87]`.
- **Classification:** seed-deterministic production bug, pre-existing on main since #188 wired the failed-repo retry runner into steady state. Surfaced only when #207's net-new scenario gave the hidden repo backfill-only content (a roast/verify finding: the zero-record variant was vacuous — the final compare converged from the live commit alone).
- **Status:** FIXED
- **Tracking issue:** https://github.com/bluesky-social/jetstream/issues/244

## Repro

```
# On the pre-fix branch, with harness_test.go seeding
# AddHiddenAccountForTest(t.Context(), 1):
go test ./internal/oracle -run TestOracle_DefaultLifecycle -count=1 -short
```

Fails 100%: fast mode enables the net-new oracle path (ms retry cadence), the
hidden DID's pending row is completed by the retry runner mid-steady-state,
and the client-stream compare reports the seeded record missing.

## Analysis

Instrumented `collectClientBackfill` to log missing seqs → exactly the retry
rows. Segment scan (`ObserveSegments`) confirmed the rows durable on disk, so
the defect is in serving, not archiving. Traced the fanout:

- The subscribe hot ring is fed exclusively via the steady live consumer's
  `OnEvent` hook (`runtime.go` → `tail.Append`).
- The retry runner (`backfill/retry.go` `tryRepo` → `HandleRepoResync` →
  `AppendBatch`) writes through the SAME shared `ingest.Writer`
  (`orchestrator/steady.go` wires `c.Writer()` into `RetryConfig`), consuming
  seqs from the shared allocator — but never fires `OnEvent`.
- `hotRing` assumes dense seqs (`idx = cursor - baseSeq`). The bypassed rows
  punch a hole inside the resident window: `len(buf)` no longer equals
  `tipSeq-baseSeq`, so every lookup past the hole returns entries shifted by
  the hole width — the client walking cold→hot silently skips the retry rows.
- Worse: a cursor in the tip-side of the hole makes `lookup` return an empty
  slice with `ok=true`, and `ReadFrom`'s `out[len(out)-1]` panics **while
  holding `Tail.mu` with no deferred unlock**. `tail.Append` runs
  synchronously on the live consumer's per-event path, so the poisoned mutex
  wedges ingestion process-wide (confirmed with a gap-feed probe test that
  deadlocked exactly as predicted).

## Contributing factors

1. The tail feed was attached to a *consumer-scoped* hook (`live.Config.OnEvent`)
   while the seq allocator is *writer-scoped* and shared by two producers.
   Nothing tied "allocates a seq on the steady writer" to "reaches the tail".
2. `hotRing`'s dense-feed assumption was enforced only by convention; its
   violation mode was silent wrong-serving rather than a loud failure.
3. `ReadFrom` held `t.mu` across index math with no bounds guard and no
   deferred unlock, converting an index bug into a process-wide wedge.
4. The pre-existing net-new oracle scenario used a zero-record hidden repo,
   so the only content the retry archived was already on the firehose — the
   compare could never distinguish "retry rows served" from "live rows served".

## Fix (issue-207-backfill-adversity, with #244)

Two layers:

1. **Ordered writer sink** (`ingest.Writer.SetOrderedEventSink`): the tail is
   now fed from the shared steady writer's append path — under `drainMu`,
   after `w.mu` is released — so ALL producers (live consumer, retry runner)
   reach the hot ring in global seq order. Wired in `runtime.go`'s
   `OnSteadyStateWriter` (fires before any producer starts). Lock order
   `drainMu → t.mu → w.mu(NextSeq)` verified acyclic. `OnEvent` remains a
   consumer-only observation hook (oracle recorders unchanged).
2. **Ring/tail hardening** (defense-in-depth): `hotRing.append` resets on any
   non-dense append (returns `reset`; `Tail.Append` warns +
   `jetstream_subscribe_hot_ring_resets_total`); `lookup` gets an explicit
   bounds guard; `ReadFrom` treats an empty-but-ok batch as a miss instead of
   panicking. A future bypassing producer degrades to cold reads + a nonzero
   counter instead of wrong data + a wedged mutex.

Red-first evidence:

- `TestTail_GapFeedNeverServesWrongSeq` / `TestTail_GapFeedDoesNotWedgeAppend`
  (subscribe): wedged/served-wrong pre-fix, green post-hardening.
- `TestOrderedSink_*` (ingest): concurrent live-shaped + retry-shaped
  producers must deliver dense, in-order, exactly-once to the sink (-race).
- The oracle scenario above: red pre-fix, green post-fix; stress mode and the
  restart tier green; full suite + -race on subscribe/ingest/live/jetstreamd/
  oracle green.

## Lesson

When a fanout cache is keyed to a shared allocator's output, feed it from the
allocator, not from one of the producers. Assumption-carrying data structures
(dense-index rings) need their invariant either enforced at the single choke
point or checked at use — this one had neither.

## Follow-up

Post-commit adversarial review of the fix (589f632) found a latent defect in
the sink×async-flush combination: the async writer delivers to the sink after
`PrepareFlush` detaches a filled block but before the background commit lands
it, so a delivered event can transiently be cold-readable nowhere. Unreachable
in production wiring (sink is steady-writer-only; async is bootstrap-only);
contained by construction in #249 (`SetOrderedEventSink` panics on an async
writer, 90769e1). The structural fix — visibility owned by the writer instead
of being an accident of durability-pipeline position — is the #248
readable-log refactor; analysis and benchmarks in
`specs/notes/2026-07-05-readable-log-writer-design.md`.
