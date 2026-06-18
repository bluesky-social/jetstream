# Merge Phase Design

**Author:** jcalabro
**Date:** 2026-05-27
**Status:** Draft
**Tracks:** DESIGN.md §4.2

## 1. Overview

The merge phase is State 5 of the bootstrap-to-live cutover state machine. By the time `runMerge` is invoked, initial backfill has drained, the bootstrap-time live consumer has been stopped and its trailing active segment sealed, and `phase=merging` is durably written to pebble.

The merge phase compacts the throwaway segment files in `data/backfill/live_segments/` into the long-term archive at `data/segments/`, dropping events whose data was already authoritatively written by the backfill engine. Once the merge returns, the orchestrator's `Run` writes `phase=steady_state` and the steady-state live consumer takes over.

This is a low-frequency code path — every jetstream instance hits it exactly once per data-dir lifetime — but the consequences of getting it wrong are severe: invalid data in the archive forces a full network re-backfill (~16 hours). The design prioritizes correctness, simplicity, and crash-recoverability over throughput.

## 2. Goals and Non-Goals

### Goals

1. Drain every sealed segment file under `data/backfill/live_segments/` into `data/segments/` in seq-ascending file order.
2. Drop commit events whose data was already authoritatively written by the backfill engine, identified by `event.Rev <= repo/<did>.Backfill.Rev`.
3. Allocate destination seq numbers monotonically continuing from where the backfill writer left off, so client cursors stream cleanly across the boundary.
4. Be idempotent under partial completion: a crash mid-merge restarts in `PhaseMerging` and re-runs `runMerge` to a consistent terminal state.
5. After successful merge: seal the active destination segment, run the post-merge new-DID discovery step (§4.7), remove `data/backfill/`, delete the merge cursor and bootstrap-last-listrepos-cursor keys, and return so `Run` can advance to `PhaseSteadyState`.
6. Refresh `repo/<did>.Rev` for every DID that emitted a surviving commit event during the merge, atomically batched with the merge cursor advance, so steady-state ingest does not see a stale watermark.

### Non-Goals

1. **Exactly-once delivery.** A crash between destination flush and merge-cursor advance produces logical duplicates with new seq numbers in the destination. This is consistent with the existing at-least-once contract (DESIGN.md §2). Clients are required to be idempotent.
2. **Concurrency.** The merge runs serially over a small data volume (the live tail accumulated during ~16h of backfill plus a small overlap window). Goroutine fan-out adds testing complexity for no meaningful win.
3. **Correctness in the face of disk corruption.** A corrupt source segment surfaces as an error and aborts the merge. Operator intervention (e.g., restart from a fresh data dir) is the recovery path. Silent skip would lose live tail data permanently.
4. **Preserving bootstrap-live observation timestamps.** The `IndexedAt` of every surviving event is overwritten with the merge-time timestamp at append (see §3.4). The bootstrap-live moment-of-observation is discarded. If we ever need witness-timestamp attribution, that semantic belongs in `RenderedAt` (DESIGN.md §8), not `IndexedAt`. The `Rev` field — which IS a meaningful network-creation timestamp for commit kinds — is preserved unchanged.
5. **Lookaside file (`lookaside.upd`) coordination.** The lookaside writer is not implemented anywhere in the codebase yet. The merge writes nothing to it and does not migrate any pre-existing entries. When the lookaside writer lands, its interaction with bootstrap-live and merge must be re-examined; flagged in §9.

## 3. Architecture

### 3.1 Inputs

- **Sealed source segments** at `data/backfill/live_segments/seg_*.jss`. The bootstrap-time live consumer wrote these; `finishBootstrap` sealed the trailing active segment before phase=merging was written. The seq numbers in these files come from the throwaway `live_segments/seq/next` namespace and are not preserved.
- **Per-DID backfill records** at `repo/<did>` in pebble, encoded as JSON `RepoStatus`. We read `Backfill.Status` and `Backfill.Rev` to decide whether commit events are redundant.

### 3.2 Outputs

- **New blocks appended** to `data/segments/` via a fresh `ingest.Writer` keyed by `seq/next`. Surviving events get fresh seq numbers continuing monotonically from the backfill writer's last allocation.
- **`data/backfill/` directory removed** wholesale via `os.RemoveAll`.
- **`merge/next_source_idx` and `bootstrap/last_listrepos_cursor` pebble keys deleted** so the keyspace is clean once we reach steady state.
- **`repo/<did>.Rev` and `UpdatedAt` advanced** for every DID that emitted a surviving commit event (see §4.6).
- **`repo/<did>` rows with `StatusFailed = "discovered post-bootstrap; queued for retry"`** for every DID `listRepos` reports as new since the last bootstrap-recorded cursor (see §4.7).

### 3.3 Invariants Held

- The destination writer's `seq/next` only ever advances. Crash-induced reprocessing produces new seq numbers; old seq numbers from the pre-crash run remain on disk in the active segment (per the writer's torn-tail recovery).
- Surviving events appear in destination seq order, which equals their original ingestion order from the live tail (the source segments are read in ascending file order, blocks within a source are read in ascending block order).
- `merge/next_source_idx` advances monotonically and is durably written via `pebble.Sync` after each fully-drained source segment.
- The DESIGN.md §3.4 "all events in segment file N have indexed_at before all events in segment file N+1" invariant is preserved across the bootstrap → merge → steady-state boundary by re-stamping `IndexedAt` (see §3.4 below). Specifically: `max(IndexedAt) of all events in any backfill segment` ≤ `min(IndexedAt) of any event written by the merge writer`. Holds by construction because the merge runs strictly after the backfill writer is closed and stamps merge-time timestamps at append.

### 3.4 Re-stamping `IndexedAt`

Every surviving event has its `IndexedAt` rewritten to `time.Now().UnixMicro()` at the moment of `dst.Append(ev)`. The bootstrap-live observation timestamp on the source event is dropped.

**Why.** The bootstrap-live consumer ran concurrently with the ~16h backfill, observing the upstream firehose across the entire bootstrap window. Its events carry `IndexedAt` values that are *earlier* than many of the timestamps written by the backfill engine. If we preserved those values, the merge segment in `data/segments/` would contain `IndexedAt` values that overlap with — and are sometimes earlier than — the values in the prior backfill segments, breaking the §3.4 file-order-equals-time-order invariant. That invariant is load-bearing for client time-range scans (clients stop walking older segments once `min_indexed_at > range_end`).

The alternative — preserving timestamps and asking clients to widen by one segment at the bootstrap boundary — pushes a non-uniform special case into every client time-scan code path forever. Re-stamping is local and contained.

**Why per-Append rather than one-timestamp-per-source.** Costs nothing extra, gives clients meaningful intra-segment temporal resolution, matches the live consumer's per-event stamping pattern. The backfill handler stamps one timestamp per repo, but that's because each repo is logically a single observation event; merge events are heterogeneous and benefit from per-event timestamps.

**What we do NOT touch.** `Rev`, `RenderedAt`, payload, DID, collection, rkey, kind. Only `IndexedAt` and the new `Seq` (which `ingest.Writer.Append` allocates).

## 4. Components

All code lives in `internal/ingest/orchestrator/` alongside `merge.go`. We deliberately do NOT promote merge to its own package: it has intimate knowledge of the orchestrator's resource lifecycle, the `repo/<did>` shape (owned by `internal/ingest/backfill`), and the segment file layout. A separate package would force re-exports of these internals with no reuse benefit.

### 4.1 `runMerge(ctx)` — orchestrator entry point

Replaces the no-op stub in `merge.go`. Owns the lifecycle:

1. If `data/backfill/live_segments/` doesn't exist, treat the merge as already-completed: delete the `merge/next_source_idx` key (idempotent — absence is fine), return nil. This is the restart-after-cleanup guard.
2. Open destination `ingest.Writer` on `data/segments/` with `SeqKey = live.SteadySeqKey`.
3. Construct a `mergeRunner` and call `runner.run(ctx)`.
4. On success of the source-drain loop: `writer.SealActiveAndClose()`.
5. Run the post-merge new-DID discovery step (see §4.7). On failure, return the error; merge will be retried on next start. The drained data is durable; the cursor advance is durable; only the discovery step is repeated.
6. `os.RemoveAll(data/backfill/)`, delete merge cursor key, delete bootstrap last-listrepos cursor key.
7. On error before step 4: best-effort `writer.Close()` (NOT seal — we don't want to mark a partial-merge active as terminally sealed), return the error.

The split between `runMerge` (lifecycle) and `mergeRunner.run` (iteration logic) keeps both small and independently testable.

### 4.2 `mergeRunner` — iteration loop

Unexported struct in `merge_runner.go`. Holds:

```go
type mergeRunner struct {
    dst        *ingest.Writer
    store      *store.Store
    sourceDir  string
    logger     *slog.Logger
    metrics    *Metrics

    // repoStatusCache memoizes pebble repo/<did> lookups within a
    // single run. Bounded by the number of unique DIDs in the live
    // tail (small relative to the network — only DIDs that emitted
    // events during the ~16h backfill window). No eviction needed
    // because the merge run is short-lived.
    repoStatusCache map[string]*backfill.RepoStatus
}
```

One method, `run(ctx)`. Pseudocode:

```
fromIdx := loadMergeCursor(store) // 0 if absent
sources := iterSourceSegments(sourceDir, fromIdx)
for idx, path := range sources {
    perDIDLastRev, err := runner.processSourceSegment(ctx, idx, path)
    if err != nil { return err }
    err = runner.commitSourceComplete(idx + 1, perDIDLastRev)
    if err != nil { return err }
}
```

`processSourceSegment` opens the source via `segment.Open`, iterates blocks via `Reader.DecodeBlock(i)` for `i = 0..BlockCount-1`, and for each event:

```
ev.IndexedAt = time.Now().UnixMicro()         // §3.4
st := runner.lookupRepoStatus(ev.DID)
if shouldKeep(&ev, st) {
    err := runner.dst.Append(ctx, &ev)
    if err != nil { return err }
    if isCommitKind(ev.Kind) && ev.Rev != "" {
        perDIDLastRev[ev.DID] = ev.Rev          // §4.6
    }
}
```

After all blocks of a source are consumed, `processSourceSegment` calls `runner.dst.Flush()` (explicit fsync of any pending block) and returns the per-DID last-rev map. The atomic cursor + per-DID Rev write happens in `commitSourceComplete` (see §4.6).

### 4.3 `shouldKeep(ev *segment.Event, st *backfill.RepoStatus) bool`

Pure predicate in `merge_filter.go`. The only piece of business logic in the merge.

```
shouldKeep returns true unless ALL of:
  - ev.Kind is a commit kind (KindCreate, KindUpdate, KindDelete)
  - st != nil
  - st.Backfill.Status == backfill.StatusComplete
  - st.Backfill.Rev != ""
  - ev.Rev != ""
  - ev.Rev <= st.Backfill.Rev   (lexicographic)
```

In all other cases, return true (keep). Rationale:
- **Non-commit kinds (Identity/Account/Sync)** are not in the repo MST and have no rev to compare against. Keep unconditionally.
- **DIDs with no `repo/` row, or `StatusNotStarted`, or `StatusFailed`** have no backfilled history. Dropping live-tail events here would create permanent data gaps. Keep.
- **`StatusComplete` with empty `BackfillRev`** is defensive: the row was written by `OnComplete` with the commit's rev, but if somehow the rev is empty we keep events rather than risk losing them.
- **Empty `ev.Rev`** is similarly defensive — only commit kinds have revs and they're always set in practice.
- **Lexicographic comparison** is correct for atproto TID-shaped revs (TIDs are designed to sort lexicographically).

**Cross-component dependency.** This predicate's correctness leans on the backfill engine stamping `commit.Rev` (the head rev of the repo at download time) onto every synthetic Create event, which is exactly what `internal/ingest/backfill/handler.go:HandleRepo` does today. If a future refactor of that handler ever switches to per-record commit revs (the rev of the commit that last touched each record, which the PDS exposes in the CAR), this predicate's `BackfillRev` comparison stops being a coherent watermark for the whole repo and would need to be reworked. Flag with a comment in `merge_filter.go` pointing at `backfill/handler.go`.

### 4.4 `repoStatusLookup` cache

Method on `mergeRunner` in `merge_filter.go`:

```go
func (r *mergeRunner) lookupRepoStatus(did string) *backfill.RepoStatus
```

Lazy-populates the cache on first lookup per DID. Pebble `repo/<did>` Get failures (other than `ErrNotFound`) are surfaced via a sticky error stored on the runner — pebble I/O errors during merge are infrastructure-level and we abort. `ErrNotFound` caches a `nil` to avoid repeated misses.

### 4.5 `merge/next_source_idx` cursor helpers

In `merge_cursor.go`. The single-key cursor read and delete:

```go
func loadMergeCursor(s *store.Store) (uint64, error)
func deleteMergeCursor(s *store.Store) error
```

Cursor key: `merge/next_source_idx`. Encoding: `[1B version][8B LE uint64]`, matching the live-cursor convention for forward-compat. Absent key reads as 0 ("haven't started"). Uses the new `store.GetVersionedUint64LE` helper (§4.8) with a merge-specific version byte (`mergeCursorV1 = 0x01`).

There is no standalone `saveMergeCursor` because the cursor advance is always coupled with the per-DID Rev refresh in a single atomic batch; see §4.6.

### 4.6 `commitSourceComplete(idx uint64, perDIDLastRev map[string]string)`

Method on `mergeRunner` in `merge_cursor.go`. Atomically commits four things in one `pebble.Sync` batch:

1. `merge/next_source_idx` ← `idx` (advances the source-segment cursor)
2. For each `(did, rev)` in `perDIDLastRev`:
   - Read the existing `repo/<did>` row (cached via `lookupRepoStatus` from the just-completed scan, so this is in-memory).
   - Update `RepoStatus.Rev` ← `rev` and `RepoStatus.UpdatedAt` ← `time.Now().UTC()`.
   - **Do NOT touch `RepoStatus.Backfill.*`.** `Backfill.Rev` is the immutable signal of where backfill stopped (used by replicas reading `backfill_complete.log`); it must not change after merge.
   - Re-encode and stage in the batch.
3. (No-op if no DIDs had surviving commit events: the batch carries just the cursor advance.)

Then commit with `store.SyncWrites`. A successful commit means: drained source N is durable, cursor has advanced, and every DID's top-level `Rev` reflects the highest rev surviving from this source. A crash anywhere in this batch's commit either fully succeeds or fully fails; we never persist a half-state where the cursor advanced but Revs didn't.

Cache update: on successful commit, also update each DID's cached `*RepoStatus` entry in `mergeRunner.repoStatusCache` so subsequent sources see the updated Rev without a fresh pebble read.

**Why update top-level `Rev` and not `Backfill.Rev`.** DESIGN.md §3.5 defines:
- `Rev` — "latest rev, updated on every commit" (steady-state mutation point)
- `Backfill.Rev` — "rev at end of initial download" (immutable post-backfill)

Steady-state ingest (DESIGN.md §4.3) advances `Rev` on every commit. The merge is logically writing real commit events into segments, so it must advance `Rev` in the same way. Leaving it stale across the merge window would mis-classify sync-divergence events (DESIGN.md §4.4) and confuse the StatusFailed retry path (§4.3) — both compare against `Rev`, not `Backfill.Rev`.

### 4.7 Post-merge new-DID discovery

In `merge_discovery.go`. Runs after the source-drain loop has fully completed and the destination active segment has been sealed (per §4.1 step 5).

**Why this exists.** During the ~16h bootstrap, `listRepos` was called once at the start. New accounts created during that window are returned by listRepos (the relay's listRepos cursor advances over the whole stream of DIDs ever seen), but only for *pages we hadn't visited yet*. For the bsky relay, listRepos cursors are monotonically increasing IDs, so any DID created since our last paged cursor is at index > `bootstrap_last_listrepos_cursor` and we can resume from there.

**Bootstrap-side change required for this to work.** `internal/ingest/backfill/cursor.go` currently saves whatever the relay returns as `NextCursor`, which means the cursor drains to `""` once listRepos completes. We add a sibling key `bootstrap/last_listrepos_cursor` written by a wrapper around the existing `OnPageComplete` callback that **only saves when the cursor is non-empty**. That gives merge a usable resume point even though the existing `relay/list_repos_cursor` properly drained.

**Discovery flow.**

```go
func (r *mergeRunner) discoverPostBootstrapDIDs(ctx context.Context) error {
    cursor, err := loadBootstrapLastListReposCursor(store) // "" if absent
    if err != nil { return err }
    if cursor == "" {
        return nil // bootstrap never paged past page 1; nothing to discover
    }

    sc := atmossync.NewClient(...)              // same client construction as backfill/run.go
    bs := backfill.NewStore(store, nil)         // reuse the existing store wrapper
    for page, err := range sc.ListRepos(ctx, listReposPageLimit, cursor) {
        if err != nil { return fmt.Errorf("...: %w", err) }
        for _, entry := range page.Repos {
            ent, err := bs.Lookup(ctx, entry.DID)
            if err != nil { return err }
            if ent.State == atmosbackfill.StateUnknown {
                // DID born during bootstrap; queue for steady-state retry.
                if err := writeStatusFailedDiscoveredRow(store, entry); err != nil {
                    return err
                }
            }
            // Existing rows: ignore. Race-safe with bootstrap's final pages
            // (a DID near the boundary may already exist with any status).
        }
    }
    return nil
}
```

`writeStatusFailedDiscoveredRow` writes a `RepoStatus` with:
- `Backfill.Status = StatusFailed`
- `Backfill.LastError = "discovered post-bootstrap; queued for retry"`
- `Backfill.Attempts = 0`
- `Active = entry.Active` (from the listRepos entry)

The steady-state retry path (DESIGN.md §4.3) retries `StatusFailed` rows with exponential backoff. Using `StatusFailed` rather than `StatusNotStarted` avoids needing to teach a new code path how to drive these DIDs to completion — the existing retry machinery handles them with no other changes.

**Idempotency.** Re-running the listRepos walk just re-Lookups DIDs that already have rows (they're not StateUnknown, so we skip them). The bootstrap-last-listrepos-cursor key is deleted by step 6 of `runMerge` after the discovery completes; if the key is still present on restart, redo the walk.

**Failure mode.** This step couples merge completion to relay availability. If the relay is unreachable, merge cannot complete and we stay in PhaseMerging until restart succeeds. This is the same coupling DESIGN.md §6.3 describes for replica promotion ("warmup window measured in minutes dominated by listRepos pagination"). If relay flakiness during cutover becomes a real operational issue, the discovery step can be moved out of `runMerge` into a separate post-steady-state-startup task with no protocol changes.

**Spec compliance caveat.** atproto says listRepos cursors are "opaque." Bluesky's relay implementation uses monotonically-increasing IDs, which is what makes "resume from saved cursor → see only new DIDs" work. A spec-compliant relay implementation that issued cursors with different semantics could in principle return a cursor that no longer resolves to "everything since X." If we retarget at a non-bsky relay in the future, this step needs revisiting. Flag in §9.

### 4.8 Adjacent refactors

Two small cleanups folded in:

**`ingest.SegmentFiles(dir) ([]SegmentFile, error)`** — exported helper in `internal/ingest/writer.go` returning `[]struct{ Idx uint64; Path string }` sorted ascending. The existing private `scanSegmentsDir` becomes a thin wrapper that returns `result[len-1]`. The merge needs the full list; the future compactor and inspect tooling will too.

**`store.GetVersionedUint64LE` / `store.SetVersionedUint64LE`** — added to `internal/store/encoding.go`. Pure functions over the `[1B version][8B LE uint64]` shape. `internal/ingest/live/cursor.go` is refactored to delegate to these helpers (its negative-cursor and version-mismatch checks remain its own concern). The merge uses the same helpers with its own version byte. Note: the merge cursor is staged into the same batch as the per-DID Rev updates (§4.6), so the merge does not call the single-key `Set` helper directly — it builds a batch and uses `batch.Set` with the encoded payload.

### 4.9 Metrics

Add to `orchestrator.Metrics`:
- `merge_events_kept` (counter)
- `merge_events_dropped` (counter)
- `merge_segments_consumed` (counter)
- `merge_did_lookups` (counter; instrumented inside `lookupRepoStatus` so cache hits don't count)
- `merge_repo_revs_updated` (counter; sums per-source `len(perDIDLastRev)` after each successful `commitSourceComplete`)
- `merge_dids_discovered_post_bootstrap` (counter; new-DID-discovery-step writes)

The existing `observeState("...", duration)` is called for `merge` overall via `obs.Span` wrapping `runMerge`. Per-source iteration and the discovery step are wrapped in their own `obs.Span`s so traces show progress.

## 5. Data Flow

### 5.1 Steady-state (no crashes)

```
data/backfill/live_segments/seg_NNNN.jss   data/segments/seg_MMMM.jss
        │                                          ▲
        ▼                                          │
   Reader.DecodeBlock(0..N-1)                ingest.Writer.Append
        │                                          │
        ▼                                          │
   for each ev:                                    │
      ev.IndexedAt = time.Now()         (§3.4 re-stamp)
      st = lookupRepoStatus(ev.DID) ──── pebble repo/<did> ──┘
      if shouldKeep(ev, st):
         dst.Append(ev)
         if commit-kind: perDIDLastRev[ev.DID] = ev.Rev
   ─────────────────────────────────
   After source seg fully drained:
      dst.Flush()                       (segment fsync)
      commitSourceComplete(srcIdx + 1, perDIDLastRev)
        (one pebble batch w/ Sync: merge/next_source_idx + N × repo/<did>)
```

After the loop, in `runMerge`:
```
dst.SealActiveAndClose()              (active dst seg fsynced + sealed; persists seq/next)
discoverPostBootstrapDIDs()           (listRepos resume from bootstrap_last_listrepos_cursor)
os.RemoveAll(data/backfill)
deleteMergeCursor()                   (pebble.Sync)
deleteBootstrapLastListReposCursor()  (pebble.Sync)
```

### 5.2 Durability ordering

Mirrors DESIGN.md §3.1.1:
1. `segment.Writer.Flush` fsyncs the destination block.
2. `pebble.Sync` commits the atomic batch advancing `merge/next_source_idx` and updating `repo/<did>.Rev` / `UpdatedAt` for every DID with a surviving commit event in the source.

A crash between (1) and (2) leaves the merge cursor pointing at the source we just drained, so on restart we redo it — the surviving events get re-appended with new seqs (logical duplicates, distinct seqs). This is the at-least-once contract. The cursor advance and Rev refresh are coupled in one batch so we never observe "cursor advanced but pebble Rev stale" or vice versa.

The destination writer's `OnAfterFlush` is left nil. We do NOT want it advancing `relay/cursor` — that's the live consumer's job, and its value was already finalized by the bootstrap-time consumer at cutover.

### 5.3 Crash recovery matrix

| Crash point | On-disk state | Restart behavior |
|---|---|---|
| Mid-event-decode | Source unchanged; dst pending block uncommitted | Resume at same merge cursor; redo whole source. Pre-crash flushed dst blocks remain → logical duplicates with new seqs. |
| After dst.Flush, before commitSourceComplete | Dst block durable; cursor + Revs unchanged | Same: redo from same source idx. Duplicate window: one source segment. Pebble Revs reflect previous state — correct, since the merge writer hasn't yet "claimed" advancement of those Revs. |
| After commitSourceComplete | Cursor advanced; Revs updated; dst block durable | Resume at cursor + 1. No duplicates. |
| After all sources processed, before SealActiveAndClose | Active dst unsealed; cursor at len(sources) | Resume: source loop empty; proceed to seal+discovery+cleanup. Idempotent. |
| After SealActiveAndClose, before discoverPostBootstrapDIDs | Dst sealed; live_segments still on disk; cursor key set; bootstrap_last_listrepos_cursor still set | Resume: source loop empty (cursor=len), seal is idempotent (already sealed), discovery re-runs. Discovery is idempotent: existing rows are skipped, only StateUnknown DIDs get StatusFailed-discovered rows. |
| After discoverPostBootstrapDIDs, before RemoveAll | Dst sealed; discovery rows written; live_segments still on disk | RemoveAll runs again. Idempotent. |
| After RemoveAll, before deleteMergeCursor | live_segments gone; cursor key still set | The "live_segments missing" guard at the top of runMerge runs cleanup again: deletes both the merge cursor and the bootstrap-last-listrepos cursor; returns nil. |
| After deleteMergeCursor + delete bootstrap cursor, before WritePhase(steady_state) | Clean. Merge looks like it never ran. | Run dispatches into PhaseMerging, hits the missing-dir guard, returns nil, advances phase. |

Note on `seq/next` durability: the destination writer's own counter is persisted by `ingest.Writer.Close` and during normal block-fill rotations via `flushAndRotateLocked`. On crash mid-merge, restart's `ingest.Open` calls `ScanMaxSeq` against the active dst seg and reconciles `seq/next` upward if needed. Crash-induced duplicates produce *new* seqs (still strictly monotonic) — clients handle them under the at-least-once contract.

### 5.4 Edge cases

- **Empty source directory.** Treated as "already cleaned up": delete both cursor keys, return nil. Covers the post-RemoveAll restart window.
- **No surviving events in a non-empty source set.** We still seal the active destination segment; an empty active is consistent with how `finishBootstrap` handles edge cases. The future compactor reads zero events and ignores the file.
- **Source file with index < cursor.** Skipped silently; we already drained it.
- **Source file with index gap (e.g., cursor=2, files are 0,1,3,4).** Treated as corruption; surface error. The bootstrap-live consumer rotates contiguously, so this should never happen.
- **Decoded event with Kind outside [1..6].** The segment package's decoder validates this at decompress time. Defense-in-depth: we'd surface as error, but the path shouldn't trigger.
- **Survivor `KindDelete` and `KindUpdate` events.** Ride the segment stream like any other commit event during merge. `lookaside.upd` is only populated by the steady-state path (DESIGN.md §3.3); the merge does not write to it. Cross-segment suppression of records present in earlier backfill segments is handled by the client per §3.3's reader algorithm — a delete in the merge segment for a record in an earlier backfill segment is delivered to clients, who apply it.
- **MaxBackfillRepos debug short-circuit during bootstrap.** When `Config.MaxBackfillRepos > 0` is set for local-dev iteration, bootstrap may finish before listRepos has paged past page 1. In that case `bootstrap/last_listrepos_cursor` is absent at merge time, and `discoverPostBootstrapDIDs` short-circuits with no work. Correct: there are no "post-bootstrap-born" DIDs in a debug run by definition.

## 6. Error Handling

| Failure | Action |
|---|---|
| Open destination writer fails | Return wrapped error. Restart re-enters PhaseMerging cleanly. |
| `segment.Open(source)` fails (`ErrCorruptSegment` / `ErrChecksumMismatch`) | Return wrapped error. Operator intervention required (DESIGN.md §4.2: "first-time right or full re-backfill"). Silent skip would lose live tail data. |
| Pebble `repo/<did>` Get fails (non-NotFound) | Surface via sticky error on `mergeRunner`; abort. Pebble I/O errors are infrastructure-level. |
| Destination `Append` fails | Return error; segment.Writer's sticky-err latch already prevents partial-frame retries. Restart redoes the source. |
| Source index gap | Return error (suspected corruption). |
| `dst.Flush` fails | Return error. |
| `commitSourceComplete` batch commit fails | Return error. The flushed dst block is already durable — restart will redo the source and produce duplicates. |
| `SealActiveAndClose` fails | Return error. We don't advance to PhaseSteadyState with an unsealed terminal active. |
| `discoverPostBootstrapDIDs` listRepos call fails | Return error. The drained source data is already durable; the cursor batch is already durable; only discovery is repeated on restart. |
| `RemoveAll(data/backfill)` fails | Return error. Orphan tree on disk would re-trigger merge on next start (harmless under idempotency, but the operator deserves to know about the disk health issue). |
| `deleteMergeCursor` / `deleteBootstrapLastListReposCursor` fails | Return error. Stale keys would re-trigger source loop / discovery on next start; correctness preserved by the missing-dir guard, but again operator should know. |

All errors are wrapped with `fmt.Errorf("orchestrator: merge: ...: %w", err)` so the orchestrator-level error message is consistent.

## 7. Testing Strategy

Tests live in `internal/ingest/orchestrator/` so they have access to unexported helpers. Test design prioritizes fast feedback: the everyday `just test` (`-short`) loop runs all merge tests in well under a second; the swarm test gates expensive iterations behind `just test-long` / `just test-race`.

### Tier 1 — Unit tests of `shouldKeep`

`merge_filter_test.go`. Pure-function table tests covering:

- nil RepoStatus → keep all kinds
- `StatusNotStarted`, `StatusFailed` → keep all kinds
- `StatusComplete` with empty `BackfillRev` → keep all (defensive)
- `StatusComplete` + `BackfillRev` set:
  - commit kinds with `ev.Rev <= BackfillRev` → drop
  - commit kinds with `ev.Rev > BackfillRev` → keep
  - commit kinds with empty `ev.Rev` → keep (defensive)
  - non-commit kinds (Identity/Account/Sync) → keep regardless of rev

Sub-millisecond per case.

### Tier 2 — Integration tests

`merge_test.go`. Build a real `data/` tree on `t.TempDir()`, populate `repo/<did>` rows via the real backfill store, populate `live_segments/` via real `ingest.Writer`s, call `runMerge`, assert outcomes. Cases:

- Mixed-rev case: some events dropped, some kept, interleaved within a single block; assert destination contents match expected survivors in seq-ascending order.
- Multi-source-segment case: events spanning 2+ source segments with rotation.
- Empty `live_segments/` directory: no-op return.
- Destination-seal verification via `segment.Inspect(...).Sealed == true`.
- `data/backfill/` removed at end.
- `merge/next_source_idx` and `bootstrap/last_listrepos_cursor` keys absent at end.
- Top-level `Run` from `PhaseMerging` advances to `PhaseSteadyState` (exercises the merge → phase-write fallthrough in `orchestrator.Run`).
- **`IndexedAt` re-stamping invariant (§3.4):** every surviving event in the destination has `IndexedAt` strictly greater than `max(IndexedAt)` of all events in the source segments. Catches a future regression where someone "preserves" the source timestamp.
- **`repo/<did>.Rev` advancement (§4.6):** for each DID with surviving commit events, `repo/<did>.Rev` after the merge equals the rev of the last surviving commit event for that DID, and `Backfill.Rev` is unchanged from before the merge.
- **Post-merge new-DID discovery (§4.7):** integration test with a fake relay that returns one page on the bootstrap walk, then on the merge-time resume walk returns a second page with two new DIDs (one already present from a hypothetical race, one truly new). Assert: the new DID gets a `StatusFailed` row with the synthetic LastError, the already-present row is unchanged.

### Tier 3 — Crash-and-resume tests (focused)

`merge_test.go`. We expose a test-only kill-point hook (unexported package var, set via test helpers, no-op in production builds). Two cases — chosen because they exercise the two materially different failure windows:

1. **Crash after dst.Flush, before commitSourceComplete.** Restart re-runs the same source. Assert: all surviving events appear in destination at least once; pre-crash and post-crash copies have *distinct* seq numbers; seq numbers are strictly monotonically increasing in the destination; pebble `Rev`s reflect either pre-crash state or post-recovery state, never a half-state.
2. **Crash after final SealActiveAndClose, before RemoveAll.** Restart cleans up. Assert: terminal state matches successful no-crash run.

Other crash points are subsumed by these (and by the swarm test below) and don't justify additional test surface.

### Tier 4 — Swarm/property test

`merge_swarm_test.go`. Seeded-PRNG-driven scenario generator. Each iteration:

- Random ~50–500 events across a small DID pool (~10 DIDs)
- Random kinds; per-DID monotonically-increasing TID-shaped revs
- Random per-DID `Backfill.Rev` cutoffs in pebble
- Random source-segment block boundaries
- Random kill-point injection: 0, 1, or 2+ crashes per iteration

Invariants asserted at terminal state:
1. Every surviving event whose `rev > BackfillRev` appears at least once in the destination.
2. No commit event whose `rev <= BackfillRev` appears in the destination.
3. Destination seq numbers are strictly monotonically increasing.
4. After successful terminal completion, `data/backfill/` is gone and both `merge/next_source_idx` and `bootstrap/last_listrepos_cursor` are absent.
5. For every DID with at least one surviving commit event, `repo/<did>.Rev` post-merge equals the highest rev among that DID's surviving commit events.
6. `repo/<did>.Backfill.Rev` for every pre-existing row is byte-for-byte unchanged from its pre-merge value.
7. Every surviving event in the destination has `IndexedAt > max(IndexedAt)` of all source events.

Run a small smoke count (10 iterations) under `-short` to keep `just test` fast; full count (>=1000) under `just test-long` and `just test-race`. Generation is cheap; bottleneck is segment encode/decode.

## 8. Code Layout

```
internal/ingest/orchestrator/
  merge.go                  ← runMerge entry (replaces stub)
  merge_runner.go           ← mergeRunner struct + run loop
  merge_filter.go           ← shouldKeep + repoStatusLookup cache
  merge_cursor.go           ← cursor helpers + commitSourceComplete batch
  merge_discovery.go        ← post-merge listRepos resume + new-DID rows
  merge_filter_test.go      ← Tier 1
  merge_test.go             ← Tier 2 + Tier 3
  merge_swarm_test.go       ← Tier 4
  metrics.go                ← (modified) +6 counters

internal/ingest/
  writer.go                 ← (modified) +SegmentFiles helper

internal/ingest/backfill/
  cursor.go                 ← (modified) +bootstrap_last_listrepos_cursor sibling key
                              + wrapper that only persists when cursor != ""

internal/store/
  encoding.go               ← (modified) +GetVersionedUint64LE / SetVersionedUint64LE

internal/ingest/live/
  cursor.go                 ← (modified) refactored to delegate to store helpers
```

## 9. Open Questions and Future Work

- **Compaction-time interaction with merge.** Steady-state lookaside compaction (DESIGN.md §3.3.1) operates on sealed segments. The merge's terminal `SealActiveAndClose` produces a sealed file that lookaside compaction can later target if needed. No special coordination required.
- **Replication.** Replicas don't run the merge phase — they receive events through the extended websocket and apply them directly. The merge is a leader-only operation (DESIGN.md §6).
- **`backfill_complete.log` interaction.** The merge does not read or write this log. The signal it carries (per-DID backfill completion) flows via `repo/<did>.Backfill.Status` for the merge's purposes; the log is purely for replica bootstrap.
- **Lookaside file (`lookaside.upd`) coordination.** The lookaside writer is not yet implemented anywhere. When it lands, three things need re-examination: (1) does bootstrap-live write any entries (e.g., type=3 sync-divergence suppressions observed during the 16h window)? (2) if so, where do they live during bootstrap and how does the merge coordinate? (3) does the merge need to write any entries itself? Resolve before steady-state ships. Until then, the merge writes nothing to the lookaside and assumes the file does not exist.
- **Spec compliance of post-merge listRepos resume.** §4.7's "resume from saved cursor → see only DIDs born since" relies on Bluesky's relay using monotonically-increasing cursor values. atproto says the cursor is opaque. If we ever target a non-bsky relay, the discovery step needs to fall back to a full re-walk and full diff against `repo/<did>` (Option B in the brainstorming notes). Acceptable risk today; flag in code with a comment.
- **Coupling merge completion to relay availability.** §4.7 makes merge completion depend on a successful listRepos call. If relay flakiness during cutover becomes a real operational issue, lift discovery out of `runMerge` into a separate post-steady-state-startup task. No protocol changes required; this is purely a where-the-code-lives decision.

## 10. References

- DESIGN.md §3.1 (segment file format)
- DESIGN.md §3.5 (metadata store, RepoStatus shape)
- DESIGN.md §4.1 (bootstrap phase)
- DESIGN.md §4.2 (merge phase — the spec for this design)
- `docs/superpowers/specs/2026-05-23-backfill-to-live-cutover-design.md` (the cutover state machine that calls `runMerge`)
- `internal/ingest/orchestrator/merge.go` (current stub with required-behavior comments)
