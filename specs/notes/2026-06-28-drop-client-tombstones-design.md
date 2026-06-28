# Simplify backfill: drop client tombstones + paginated, bufferless cutover

Date: 2026-06-28
Branch: `less-tombstones`
Status: design / plan (not yet implemented)
Author: jcalabro (with Claude)

> This spec covers **two linked changes** that are hard to separate cleanly:
>
> - **Part A — Drop client-side tombstones** (§1–§9): backfill becomes
>   eventually-consistent; the client emits every row and converges under fold;
>   the `getTombstones` overlay endpoint is removed.
> - **Part B — Paginated, bufferless cutover** (§10–§16): a client completes a
>   backfill via *repeated* `planBackfill` calls (pagination), then connects
>   `/subscribe` only once it has caught up to the sealed tip. This removes the
>   client-side cutover buffer entirely ("jetstream is your buffer") and makes the
>   websocket lookback window no longer load-bearing for correctness.
>
> They are sequenced **A then B** in one effort because the cutover buffer
> (`liveSink`) currently does double duty — event ordering *and* live-tombstone
> folding. Part A strips the folding role, leaving a pure ordering buffer; Part B
> then deletes the buffer outright. Doing them together avoids reworking
> `engine.go`'s cutover path twice. See §15 for why they cannot be cleanly split.

## 1. Summary

Today the Go client consumes a server-built **compaction overlay** (the
`network.bsky.jetstream.getTombstones` endpoint) so that, during backfill, it can
**suppress** every create/update row that has since been deleted or superseded. The
client holds a combined tombstone set (overlay base covering `(W, M]` plus live-tail
tombstones covering `(M, ∞)`) and drops masked rows before emitting them. The result
is that a backfilling client presents a **point-in-time-correct** view: it never emits
a record that was already dead at the moment it started.

This change removes that machinery entirely. The client will:

1. Note the current live cursor (the planner's `plannedThroughSeq`, exactly as today).
2. Download the planned segment files / blocks and emit **every** matching row to the
   consumer — creates, updates, deletes, accounts, syncs — applying only the caller's
   exact DID/collection/seq filter. No tombstone suppression.
3. Subscribe to the live tail from the noted cursor and churn through it to catch up.

Because the archive **retains every delete/update/account/sync row as its own event**
(`docs/README.md:348`), a consumer that folds the emitted stream in seq order converges
to the same live set the overlay used to hand it directly. The view transitions from
*immediately correct* to *eventually correct*, and from *exactly-once-ish* to
*at-least-once* (rows may be re-emitted across the backfill→live overlap and on live
reconnect). Both are explicitly acceptable and, for at-least-once, already true today.

Server-side compaction is **unchanged**: tombstones, the in-memory `tombstone.Set`, the
`compaction/seq` watermark, and `segment.Rewrite` all stay. Tombstones remain essential
for physically reclaiming superseded rows in sealed segments. What goes away is the
*read-time exposure* of tombstones to clients and the client's obligation to apply them.

## 2. Motivation

- **Massive surface-area reduction.** The suppressor, the overlay wire format, the
  overlay cache + ticker, the `getTombstones` endpoint and lexicon, the overlay metrics,
  and the client's two-layer copy-on-write tombstone folding are all difficult-to-get-
  right concurrency- and correctness-sensitive code. None of it is needed for a correct
  (eventually-consistent) client.
- **Every future client library gets simpler.** A third-party client in any language
  now only needs: `planBackfill` → download segments/blocks → emit rows → tail
  `/subscribe`. No overlay decode, no half-open seq-window suppression logic, no
  combined-set reasoning. This is the single biggest lever on "how hard is it to write a
  correct Jetstream client."
- **Removes a sharp correctness coupling.** The current `#account` wire policy is only
  safe "because tombstone folding happens before the delivery filter" (`docs/README.md:566`,
  `internal/subscribe/filter.go:50-56`, `internal/client/filter.go:29-33`). That coupling
  is subtle and fragile; dropping suppression lets us replace it with a simpler, more
  honest delivery contract.

### 2.1 Why this is correct

Two properties of the system make the eventually-consistent model sound:

1. **Tombstone rows are durable events, not just compaction metadata.** Delete, update,
   account-delete, and sync rows are stored in segment files forever and streamed on the
   live tail (`docs/README.md:348`, §4.4). A client that processes the full stream in seq
   order therefore *sees the delete that kills a stale create* — it just sees it later
   than the create rather than never seeing the create at all.

2. **Compaction bounds the inconsistency window from below.** Below the compaction
   watermark `W`, superseded create/update rows are already physically removed, so the
   archive only carries them in `(W, active-tip]`. The maximum staleness a consumer
   observes is the time between emitting a create and later emitting its tombstone —
   bounded by how long the backfill takes to drain plus the live catch-up. After that the
   consumer's folded view equals ground truth.

Per-record-key, the invariant the client now guarantees is **convergence under fold**,
not **point-in-time correctness**: folding the entire emitted stream (creates/updates
applied, deletes/account-deletes/syncs removing) yields the ground-truth live set for the
caller's query. This is exactly what the oracle's `groundTruthLive` already computes
(`internal/oracle/overlay.go:108-158`); §6 rewrites the oracle check around it.

### 2.2 Trade-offs we are explicitly accepting

- **Transient stale records.** A consumer that materializes records as they arrive will
  briefly hold a record that has already been deleted/updated, until the corresponding
  tombstone row is processed. Consumers must apply deletes/updates idempotently (they
  already must, because of at-least-once redelivery on reconnect).
- **More bytes on the wire / more rows emitted.** A backfill now emits superseded rows in
  `(W, tip]` that the overlay used to suppress. Bounded by the compaction interval
  (default 4h); below `W` nothing extra is emitted because compaction already removed it.
- **No `getTombstones` for third parties.** We are removing the endpoint outright (see
  §4 decision), not deprecating it. There is no TypeScript client yet and the only
  first-party consumer is the Go client, so there is no external contract to preserve.

## 3. Decisions (resolved with Jim, 2026-06-28)

- **Scope:** Go client + server endpoint removal + docs + oracle/simulator. There is no
  TypeScript client to migrate. Replication (Section 6 of `docs/README.md`) is reviewed
  for overlay coupling but is early-days and not a focus.
- **Overlay fate:** **Remove entirely.** Delete the `getTombstones` handler, the
  `internal/overlay` package (format + cache + metrics), the `overlay_source` adapter, the
  overlay cache ticker wiring, the lexicon, and the generated API stub. Keep only the
  in-memory `tombstone.Set` that compaction needs.
- **Account/identity delivery:** **Deliver both `#account` and `#identity`** to
  collection-filtered subscribers (and the Go client). This reverts the v2 collection-
  scoping of account/identity to the v1 always-deliver contract. With suppression gone,
  delivering `#account` is what lets a consumer reconcile a deleted account's records, and
  delivering `#identity` removes a special case.

## 4. Current architecture (as-is) — what we are removing

Verified file references as of this branch.

### 4.1 Client suppression path (REMOVE)

- `internal/client/suppress.go` — the `Suppressor`: `base`/`live` `atomic.Pointer`
  layers, `SeedFromOverlay`, `Merge`, `ObserveLive`, `ShouldDrop`. **Delete the file.**
- `internal/client/selector.go` — `rowSelector` combines `Matcher` + `Suppressor`.
  Collapses to just the matcher (see §5.1). **Delete or reduce to a thin matcher call.**
- `internal/client/livesink.go` — `observeTombstone` / `ObserveLive` calls and the
  `suppressor.ShouldDrop` check inside `wantLive` (`:62`, `:165`, `:171-175`). The sink
  keeps its buffer/flip/drain role; only the suppressor coupling is removed.
- `internal/client/engine.go` — `runBackfillThenLive` step 1 `SeedFromOverlay`
  (`:419-422`), `runBackfillOnly` step 1 (`:371-374`), the `suppressor` field on `Engine`
  (`:123`, `:137`), and all `newRowSelector(e.matcher, e.suppressor)` call sites
  (`:388`, `:493`). Note the discarded `(w, m)` return at `:419` — the cutover boundary
  is `plan.PlannedThroughSeq` (`:434`), independent of the overlay, so the live tail
  start logic is untouched.
- `internal/client/filter.go` — `Matcher.Wants` collection-filter branch that drops
  account/identity (`:97-103`) plus the type doc (`:14-33`). Changed per §5.2.
- `client.go` / `options.go` / `doc.go` — any public surface or docs that reference
  overlay seeding or suppression. (Backfill remains opt-in; only the suppression
  narrative changes.)

### 4.2 Server overlay endpoint (REMOVE)

- `internal/overlay/` — entire package: `format.go` (`.jsto` encode/decode), `cache.go`
  (precomputed-blob cache + `RunTicker`), `doc.go`, `metrics.go`, and their tests
  (`format_test.go`, `cache_test.go`, `bench_test.go`). **Delete the package.**
- `internal/xrpcapi/gettombstones.go` + `gettombstones_test.go` — the handler. **Delete.**
- `internal/xrpcapi/server.go` — route registration for `getTombstones`. **Unregister.**
- `internal/jetstreamd/overlay_source.go` — `overlaySource` adapter (`Watermark`/`Dirty`/
  `SnapshotRange`). **Delete.**
- `internal/jetstreamd/runtime.go` — `overlay.NewCache` construction (~`:275`) and the
  overlay ticker goroutine. **Remove.**
- `internal/obs/overlay.go` — overlay Prometheus metrics. **Delete** (and drop any
  registration site).
- `lexicons/network/bsky/jetstream/getTombstones.json` — **Delete.**
- `api/jetstream/jetstreamgettombstones.go` — generated client stub. **Delete** (and
  regenerate API bindings so nothing references it).

### 4.3 Tombstone package (MOSTLY KEEP — prune client/overlay-only API)

`internal/tombstone/tombstone.go` stays: compaction depends on `Set.Observe`,
`Set.Evict`, `FoldRange`, and `Snapshot.ShouldDrop` (used by the compaction `decide`
callback in `internal/ingest/orchestrator/compact_deletes.go`). During implementation,
audit for members that become **overlay/client-only** and delete them, e.g.:

- `Set.SnapshotRange` — used only by `overlaySource`. Likely removable.
- `Set.ApproxBytes` / `Set.Dirty` — used by the overlay cache + metrics; verify whether
  any compaction metric still needs them before removing.
- single-event `Fold` (vs `FoldRange`) — `ObserveLive` is the only caller; check.

Do not pre-decide these in the plan beyond "audit and remove what only the overlay/client
used." Keep everything compaction still needs.

### 4.4 Server live filter (CHANGE)

- `internal/subscribe/filter.go` — `filterIdentityByCollection` field and its branch
  (`:48-56`, `:337-344`), the `withIdentityCollectionPolicy` helper (`:71-77`), and the
  account-bypass justification comments (`:50-56`, `:311-319`, `:335-336`). With the new
  policy, account **and** identity both bypass the collection filter unconditionally, so
  `/subscribe-v2` and `/subscribe` (v1) behave identically on this axis. Remove the flag.
- `internal/subscribe/handler.go` — wherever `FilterIdentityByCollection` is plumbed per
  endpoint. Remove the per-endpoint distinction.

### 4.5 Compaction & ingest (UNCHANGED — verify only)

`internal/ingest/orchestrator/compact_deletes.go`, `compaction_watermark.go`,
`internal/ingest/live/consumer.go` (the `onAppend` → `ts.Observe` hook at `:81-93`),
`segment/rewrite.go`, and the `compaction/*` metrics all stay. The `tombstone.Set` is
still built and evicted exactly as today. Only confirm nothing in this path reached into
the overlay package.

## 5. Target design (to-be)

### 5.1 Client: emit everything matching the filter

The backfill download path keeps the `Matcher` (exact DID/collection/seq filtering — the
planner is a one-sided transport hint, so post-decode filtering is still mandatory) and
drops the `Suppressor`:

- `rowSelector.Keep` becomes "matcher only." Simplest landing: delete `rowSelector` and
  have the downloader call `matcher.Wants` directly; or keep a one-field `rowSelector` for
  call-site stability. Implementer's choice — prefer deleting the indirection.
- `liveSink` keeps buffer → flipAndDrain → forward. `wantLive` drops the
  `suppressor.ShouldDrop` check and keeps only `matcher.Wants`. `observeTombstone` and the
  always-fold-during-buffering logic are deleted.
- `engine.runBackfillThenLive` / `runBackfillOnly` drop the `SeedFromOverlay` step. The
  live-tail start (`plannedThroughSeq - liveRewindMargin`), the empty-archive
  `backfillCoveredNothing` handling, and the dedup floor logic are all **unchanged** — they
  never depended on the overlay.
- `Engine` loses its `suppressor` field and `NewSuppressor()` call.

At-least-once and dedup are preserved exactly: the `liveRewindMargin` overlap and the
`liveConsumer.lastSeq` / `dedupFloor` seq-dedup (`internal/client/engine.go:33-37`,
`livesink.go:140-152`) stay. We are not weakening dedup; we are only removing suppression.

### 5.2 Delivery contract: account + identity always delivered

`Matcher.Wants` (`internal/client/filter.go`) and `subscribe.Filter.Wants`
(`internal/subscribe/filter.go`) both change so that, when a collection filter is set:

- Commit events are gated by the collection filter (unchanged).
- `#account` and `#identity` bypass the collection filter and are delivered (subject to
  the DID filter). `#sync` continues to bypass.

This is the v1 contract ("regardless of desired collections, all subscribers receive
Account and Identity events"), now applied uniformly to v1 `/subscribe`, v2
`/subscribe-v2`, and the Go client. The `filterIdentityByCollection` toggle is deleted.

Rationale: with no client-side suppression, `#account` deletes are the consumer's only
signal to purge a dead account's records, and `#identity` is cheap and expected. Hiding
either would create a *permanently* stale view for collection-scoped consumers — the one
outcome the eventually-consistent model must avoid.

### 5.3 Server: no read-time tombstone exposure

After removal, the only tombstone consumer is compaction. The server still:

- Folds live events into `tombstone.Set` via the `onAppend` hook.
- Runs periodic + cap-triggered compaction, advancing `compaction/seq`, rewriting sealed
  segments, and evicting the in-memory set below the new watermark.

It no longer builds overlay blobs, runs the overlay ticker, or serves `getTombstones`.

## 6. Oracle & simulator changes

The oracle currently asserts **point-in-time correctness**: the set of record rows a
client would emit, after applying the combined overlay+live suppression set, must exactly
equal `groundTruthLive` (`internal/oracle/overlay.go:CheckOverlayReconstruction`). That
check encodes the very behavior we are deleting, so it must be rewritten, not merely
unplugged.

New invariants to assert (the heart of the testing change — get these right):

1. **Convergence under fold (correctness).** Collect the full stream the client emits for
   a query (creates, updates, deletes, accounts, syncs). Fold it in seq order with the same
   rules as `groundTruthLive`. The result must equal `groundTruthLive(allObservedEvents)`
   restricted to the query's DID/collection filter. This replaces
   `CheckOverlayReconstruction` and reuses `groundTruthLive` (`overlay.go:108-158`).
2. **At-least-once coverage (liveness).** Every record live in ground truth must appear
   **at least once** as a create/update in the emitted stream. (We no longer assert
   "emitted ≤ ground truth"; transient stale rows are allowed, so the old "emitted a record
   that ground truth deleted" failure must be removed — it is now expected behavior.)
3. **Tombstone delivery.** For every record that ground truth considers dead, the emitted
   stream must contain the delete/update/account-delete/sync row that kills it (so a folding
   consumer can converge). This is what makes the §5.2 account/identity delivery change a
   *tested* guarantee, not just a wire-policy assertion.

Files in scope (audit each; many only reference the overlay incidentally):

- `internal/oracle/overlay.go`, `overlay_test.go`, `overlay_integration_test.go` — rewrite
  around fold-convergence; delete overlay-blob-specific assertions. The
  `overlay_integration_test` likely loses its reason to exist; fold its useful coverage
  into the client-observer path.
- `internal/oracle/client_observer_test.go` — the client-side end-to-end observer; update
  expectations from point-in-time to eventually-consistent.
- `internal/oracle/compacted.go` / `compacted_test.go` — server compaction correctness
  stays; verify it does not assert anything about client suppression.
- `internal/oracle/harness_test.go`, `main_test.go`, `eventlog_test.go`,
  `restart_*_test.go`, `trace_determinism_test.go`, `synctest_test.go` — these reference
  overlay/tombstone broadly; most need only import/wiring cleanup once the overlay package
  and the suppressor are gone. Triage each: behavioral change vs. mechanical removal.
- `internal/simulator/world/*_test.go` — account/targeted-op tests; verify the new
  account/identity delivery policy does not regress simulator expectations.

### 6.1 Mutation campaign

`testing/mutation/mutants/*.patch` and `testing/mutation/RESULTS.md`: any mutant that
modeled an overlay/suppression bug is now testing dead code and must be retired or
repointed at the new fold-convergence invariant. `testing/mutation/baseline.json`
references tombstones — refresh. Re-run `just mutation-campaign` after the refactor; a
STALE scorecard is expected until the mutants are re-reviewed.

## 7. Documentation changes

- `docs/README.md` — the big one. Rewrite §3.3 to drop the `getTombstones` overlay
  endpoint subsection (`:354-394`) while keeping the compaction narrative; the in-memory
  tombstone set is now purely a compaction-internal structure with no read-time exposure.
  Rewrite the "putting it all together" client flow (`:406-417`) to the 3-step
  note-cursor → download → tail model with no overlay download and no suppression steps.
  Update §4.4 (`:558-568`) to the new "account + identity always delivered" contract and
  delete the "tombstone folding happens before the delivery filter" justification.
  Add an explicit **eventual consistency** statement to §1.1 / §5 so the contract is
  documented, not implicit: backfill is at-least-once and converges under fold; consumers
  must apply deletes/updates idempotently.
- `README.md` — check for any overlay/`getTombstones`/suppression mention.
- `specs/oracle.md` — update the description of what the oracle proves (point-in-time →
  fold-convergence + at-least-once).
- Lexicon docs / any generated lexicon index — remove `getTombstones`.

## 8. Execution plan (suggested ordering)

File one GitHub issue per discrete unit (per AGENTS.md). **Part A** lands first because it
strips `liveSink`'s tombstone-folding role, leaving a pure ordering buffer that **Part B**
then deletes (§15).

### Part A — drop client tombstones

1. **`subscribe: deliver #account and #identity to collection-filtered subscribers`** —
   server-side delivery policy (§5.2 server half). Independent, shippable first; includes
   `internal/subscribe/filter.go` + handler + filter tests. Lands the new wire contract
   before the client stops suppressing, so the order is safe.
2. **`client: deliver account/identity under collection filter`** — `Matcher.Wants`
   change to match (§5.2 client half) + matcher tests.
3. **`client: remove tombstone suppression from backfill`** — delete `suppress.go`,
   collapse `selector.go`, strip the suppressor from `engine.go` / `livesink.go`, drop the
   `SeedFromOverlay` steps. Update client tests (`suppress_test.go` deleted;
   `selector_test.go`, `engine_test.go`, `live_test.go`, `reconstruct_test.go` updated).
4. **`server: remove getTombstones overlay endpoint`** — delete `internal/overlay`,
   `gettombstones.go`, `overlay_source.go`, overlay metrics, runtime ticker wiring,
   lexicon, generated stub; unregister the route.
5. **`tombstone: prune overlay/client-only API`** — remove `SnapshotRange` et al. that no
   longer have callers (§4.3 audit).
6. **`oracle: assert fold-convergence instead of point-in-time suppression`** — rewrite
   the oracle checks (§6) and refresh the mutation campaign (§6.1).

### Part B — paginated, bufferless cutover

7. **`manifest: paginate planBackfill (truncate-and-continue + sealedTipSeq)`** — replace
   `ErrPlanTooLarge` with seq-boundary truncation; add `sealedTipSeq` to the result, lexicon,
   and generated bindings; guarantee forward progress on a sub-unit cap (§12).
8. **`subscribe: explicit too-old signal for v2 seq cursors below the lookback floor`** —
   replace the silent clamp with a documented signal; keep v1 timestamp clamping (§14). This
   is a standalone correctness fix and can land early/independently.
9. **`client: paginate backfill and remove the cutover buffer`** — rewrite
   `runBackfillThenLive` as the §11 loop; delete `livesink.go`, the `Buffer` interface, the
   rewind margin and dedup-floor special cases; add the §14.1 handoff threshold; make backfill
   resumable (§13).
10. **`oracle: multi-page backfill, mid-download seal, stale-cursor, livelock`** — the §16
    additions + new mutants.
11. **`docs: paginated bufferless backfill; eventual consistency; drop overlay`** — the §7
    docs work plus the Part B model (rewrite the client-flow narrative to the paginated loop,
    document the two-seq done predicate, the too-old signal, and the handoff threshold). Also
    correct the stale `2026-05-27-cursor-replay-design.md` claim that v2 seq cursors have no
    window cap.

Steps 1–2 land together. 3 and 4 are independent once 1–2 are in; 5 depends on 4; 6 depends on
3+4. Step 8 is independent and can land anytime. Step 9 depends on 3 (buffer must lose its
folding role first) and 7 (needs `sealedTipSeq`); 10 depends on 7+9; 11 is last. Run
`just test ./internal/oracle`, `just test-long`, and the oracle/mutation recipes after 3, 4,
7, 8, 9, and 10 — every one touches ingest-adjacent or end-to-end correctness.

## 9. Open questions / risks to validate during implementation

- **`(w, m)` truly unused at the cutover boundary?** Verified the engine discards the
  `SeedFromOverlay` return and keys the handoff on `plannedThroughSeq`. Re-confirm there is
  no other consumer of overlay `M` (e.g. a metric or a status-page field) before deleting.
- **Status page / `/status`** — does `internal/status` surface overlay or `getTombstones`
  health? Audit and remove if so.
- **Cold cursor replay depth.** A client that is offline longer than `--cursor-lookback`
  (default 36h, `docs/README.md:614`) cannot tail from its saved cursor and must re-backfill.
  Unchanged by this work, but worth a note in the eventual-consistency docs: convergence
  assumes the consumer either stays within the live lookback window or re-runs backfill.
- **Replication.** Section 6 of `docs/README.md` is early-days; confirm the replication
  protocol (extended-mode subscriber writing its own segments) does not consume the overlay.
  A replica tails extended `/subscribe` and writes raw rows, so it should be unaffected —
  verify.
- **Consumer-facing guidance.** Because consumers now must apply deletes/updates
  idempotently and tolerate transient stale rows, the Go client docs/examples should show
  the fold pattern explicitly so users don't assume point-in-time correctness.

---

# Part B — Paginated, bufferless cutover

## 10. Problem: the websocket lookback window is load-bearing for correctness

Today's cutover (`internal/client/engine.go:runBackfillThenLive`) is a single-shot dance:

1. Plan **once** → `plannedThroughSeq = S` (the sealed tip at that instant).
2. Start `/subscribe` at `S - liveRewindMargin` into a client-side buffer (`liveSink` +
   the `Buffer` interface) **before** downloading, so no live event is lost.
3. Download the sealed archive (seq ≤ `S`).
4. `flipAndDrain`: replay buffered live frames > `S`, then forward live directly.

Two structural problems:

### 10.1 A confirmed silent-data-loss bug (CLAUDE.md violation)

`/subscribe` **silently clamps a v2 seq cursor up to the lookback floor** and skips the
gap. `internal/subscribe/cursor.go:124-130`:

```go
if env.Manifest != nil && env.Lookback > 0 {
    floorSeq, _ := env.Manifest.LookbackFloor(env.Lookback)
    if startSeq < floorSeq {
        startSeq = floorSeq   // events in (startSeq, floorSeq] are silently dropped
        plan.Clamped = true   // only a metric flag; no error, no client signal
    }
}
```

The cursor-replay design note claims "v2 seq cursors have no window cap … can replay from
the beginning of the archive" (`specs/notes/2026-05-27-cursor-replay-design.md:94-95`), but
the shipped code clamps them exactly like legacy timestamp cursors. **The doc is stale and
the code silently loses data.** This directly violates CLAUDE.md ("Silent fallbacks are
often a mistake. Crashing is preferred over data corruption").

Concretely: if the backfill download + buffered tail takes longer than `--cursor-lookback`
(default 36h), the live cursor `S` falls below `now - 36h`, the clamp fires, and the client
silently loses every event in `(S, floor]`. **Expanding the window only lowers the
probability; it does not remove the failure mode.** The window must stop being load-bearing
for correctness.

### 10.2 The client-side cutover buffer is fragile and unnecessary

The buffer (`liveSink`, `Buffer`, `flipAndDrain`, the concurrent live-tail-during-download
goroutine, the dedup-floor / `backfillCoveredNothing` edge cases) exists only to bridge a
**single** plan's `S` to live without losing the `(S, M]` band. It is the most
intricate, most edge-case-laden code in the client. The insight (your coworker's):
**jetstream's durable archive already IS the buffer.** If the client keeps re-planning, every
event sealed during the previous download is picked up by the next plan — no client buffer
needed.

## 11. Target model: pagination over planBackfill

Treat backfill as **pagination** (the mental model: `afterSeq` ≈ offset, `MaxEntries` ≈
limit, the returned continuation seq ≈ the "next page" token):

```
cursor := request.AfterSeq            // 0 for a full backfill
for {
    p := planBackfill(afterSeq=cursor, beforeSeq=request.BeforeSeq, filters…)
    download + emit p.Segments         // every matching row, in seq order, no suppression
    cursor = p.PlannedThroughSeq       // continuation cursor (see §12)
    if cursor >= p.SealedTipSeq {      // ALL sealed segments consumed (see §14.1)
        break
    }
}
subscribe(cursor=cursor)               // connect ONCE, at the sealed tip — never clamped
```

**The governing principle (resolved with Jim): prefer the sealed archive; use the websocket
as little as possible.** The client downloads *every* sealed segment via HTTP and connects
`/subscribe` only for the remainder that HTTP physically cannot serve — the active, unsealed
segment. This replaces the earlier "hand off when the residual gap is small enough" idea with
a principled, knob-free rule: the cutover point is the **sealed/unsealed boundary**, not a
tunable threshold. See §14.1 for why this is both correct and the right efficiency choice.

Why each step is correct (grounded in code read this session):

- **Sealed-segment downloads never expire.** The lookback clamp lives *only* in `/subscribe`
  cursor resolution. `planBackfill` (`internal/manifest/plan.go`) and `getSegment`/`getBlock`
  read durable files with no time bound. A 100-hour backfill is fine; segments don't age out
  (compaction only rewrites in place, preserving seq ranges).
- **Re-planning absorbs mid-download seals.** Segments sealed while page *k* downloaded are
  returned by page *k+1*'s `planBackfill(afterSeq=cursor)`. This is precisely why the client
  buffer becomes unnecessary.
- **The terminal `/subscribe` hop is always serviceable, never clamped.** `LookbackFloor`
  (`internal/manifest/manifest.go:617-637`) is the `MinSeq` of the freshest sealed segment
  still inside the window, computed over **sealed segments only** (the active segment is not
  in the manifest). That floor is ≤ that segment's `MaxSeq` = the sealed tip. So **the sealed
  tip is never below the floor**, and a `/subscribe` connect at the tip never triggers the
  §10.1 clamp. `/subscribe`'s own replay (`internal/subscribe/replay.go:WalkFromCursor`) then
  reads any segments sealed during the handoff, the active segment's flushed blocks + in-memory
  pending, and cuts over to live atomically (`SnapshotPendingAndRegister`) — no gap, no client
  buffer.

**One mechanism, three cases.** Cold backfill, resume-from-stale-cursor, and fell-off-live
all become the same loop: re-enter pagination from the last seq the consumer durably
processed. This is the deep simplification — we delete `liveSink`, `Buffer`, `flipAndDrain`,
and the concurrent-download-tail entirely (§13).

## 12. Required server change: planBackfill paginates instead of refusing

Today the planner **refuses** an oversized plan — the opposite of pagination
(`internal/manifest/plan.go:159-160`):

```go
if req.MaxEntries > 0 && result.Stats.Entries > req.MaxEntries {
    return PlanBackfillResult{}, ErrPlanTooLarge
}
```

### 12.1 Truncate-and-continue (decision: paginate)

The planner walks segments in seq order (`plan.go:122`), so when it reaches the `MaxEntries`
cap it stops at a **clean seq boundary** — the `MaxSeq` of the last fully-included segment
(or last included block range) — and reports that as the continuation cursor instead of
erroring. "Give me all `app.bsky.feed.like` since 2023" then pages into bounded chunks rather
than failing with `PlanTooLarge`. This also bounds per-call response size and server planning
memory, which is an operational win independent of correctness.

`ErrPlanTooLarge` is retired from the normal path. (`ErrInvalidPlanRequest` for malformed
input — negative `MaxEntries`, inverted seq window, bad threshold — stays.) Edge case to get
right: if even a *single* segment/block entry exceeds `MaxEntries` (cap smaller than one unit
of work), the planner must still return that one entry and advance — never make zero forward
progress, or the client livelocks. Document a sane minimum and clamp.

### 12.2 Two seq fields disambiguate continuation vs. goal (decision: two fields)

A single `plannedThroughSeq` is overloaded: it cannot mean both "where this page stopped"
and "the archive tip" once truncation exists. Three situations collide — a filtered page that
matched zero segments in its sub-range (but data exists above), a page truncated early at a
boundary `< tip`, and a genuinely-caught-up client. So the response carries **two** values:

- `plannedThroughSeq` — the **continuation cursor**: resume the next page at
  `afterSeq = plannedThroughSeq`. Equals the truncation boundary when truncated, else the
  tip (capped by `beforeSeq`). This is the existing field's seq-monotone meaning, now also
  valid mid-pagination.
- `sealedTipSeq` *(new)* — the current sealed-archive tip (capped by `beforeSeq`). The
  **goal**.

**Done predicate (unambiguous, no boolean):** the client has consumed the whole sealed
archive when `plannedThroughSeq >= sealedTipSeq`. It then connects `/subscribe` at
`plannedThroughSeq`. This is strictly more robust than inferring "done" from an empty segment
list, which is ambiguous for sparse filters (a page can legitimately match zero segments yet
have matching data — and more sealed data — above its truncation point).

Note both fields move under concurrent ingest: `sealedTipSeq` advances as new segments seal.
The loop terminates when the client has consumed every sealed segment and only the active
(undownloadable) segment remains; see §14.1 for why "exhaust the sealed archive" is the right
termination rule and why it converges.

Lexicon + generated bindings: add `sealedTipSeq` to
`lexicons/network/bsky/jetstream/planBackfill.json` output and regenerate
`api/jetstream/*`. Update `internal/xrpcapi/planbackfill.go` to populate it and to stop
mapping `ErrPlanTooLarge` to an error response.

## 13. Client changes: delete the buffer, loop the plan

`internal/client/engine.go` is the focus. `runBackfillThenLive` is rewritten as the §11 loop:

- **Delete** `internal/client/livesink.go` (`liveSink`, `flipAndDrain`, `onLive`,
  `observeTombstone`), the `Buffer` interface and `LiveFrame` type (`engine.go:14-31`), the
  root-package `LiveBuffer` adapter, the `liveRewindMargin` cutover overlap, the
  `backfillCoveredNothing` / dedup-floor special-casing, and the concurrent live-tail
  goroutine started before download (`engine.go:477-480`).
- **Rewrite** `runBackfillThenLive`: page through `planBackfill` (download + emit each page
  in seq order via the existing `Downloader`), advancing `cursor = plannedThroughSeq` until
  `plannedThroughSeq >= sealedTipSeq`, then start the steady-state live consumer at `cursor`.
  No buffering phase, no flip.
- **`runBackfillOnly`** becomes: page until done, then return (no `/subscribe`) — it already
  has no live tail, so it loses only the single-plan assumption.
- **`Planner.Plan`** (`internal/client/planner.go`) returns `SealedTipSeq` alongside
  `PlannedThroughSeq`; the loop lives in the engine, not the planner (the planner stays a
  single-call wrapper).
- **Resumability bonus:** because the loop is just "plan from `cursor`," a crashed backfill
  resumes from the last durably-emitted seq instead of restarting — fixing the "backfill NOT
  resumable in v1" limitation noted in the go-client design. Wire this to `Batch.LastCursor()`.

Dedup is preserved: the steady-state `liveConsumer` already dedups by seq
(`internal/client/live.go` `lastSeq` / `dedupFloor`). Pages overlap only at the `afterSeq`
boundary (exclusive lower bound), so at-least-once across the final `/subscribe` handoff is
covered by the consumer's existing seq dedup — no rewind margin needed.

## 14. Server change: /subscribe must not silently drop a stale v2 cursor

Decision: **explicit "cursor too old, re-backfill" signal.** When a v2 *seq* cursor resolves
below the lookback floor, `/subscribe` must NOT silently clamp (§10.1). Instead it returns a
distinct, documented signal (an error close-frame / 4xx with a machine-readable reason and the
floor seq) telling the client to re-enter the backfill loop from its cursor.

- `internal/subscribe/cursor.go:124-130` — for `ModeReplaySeq`, replace the silent clamp with
  a "below floor" outcome the handler turns into the explicit signal. Add a `CursorPlan`
  state (e.g. `Mode = ModeReplayTooOld` or a typed error) carrying the floor seq.
- `internal/subscribe/handler.go` — translate that outcome into the wire signal (close code +
  reason, or HTTP 4xx pre-upgrade). Define the exact wire shape in the lexicon/docs.
- **v1 timestamp cursors keep silent clamping** (`cursor.go:149-155`): that is the documented
  v1 wire contract (`2026-05-27-cursor-replay-design.md:85-95`) and must not change. Only the
  v2 *seq* path gets the explicit signal. This asymmetry is intentional and must be commented.
- The Go client treats the signal as "re-backfill from `cursor`" — closing the §11 loop's
  third case (fell-off-live) with no special handling.

### 14.1 Termination rule: exhaust the sealed archive, then hand off (no tunable threshold)

The handoff point is the **sealed/unsealed boundary**, not a configurable gap size. The loop
pages over HTTP until `plannedThroughSeq >= sealedTipSeq` — i.e. every sealed segment has been
downloaded — and only then connects `/subscribe`, which covers the active (undownloadable)
segment and the live tail. "Prefer the sealed archive; use the websocket as little as
possible."

This is provably correct and is also the right operational choice; an earlier draft proposed a
tunable "hand off when the residual gap is small enough" threshold, which is **rejected**
because it protects against nothing the boundary rule doesn't already cover:

1. **Keeping the connect cursor in-window is already guaranteed.** `LookbackFloor` is computed
   over sealed segments only and is ≤ the sealed tip (§11, `manifest.go:617-637`). Connecting
   at the sealed tip therefore never clamps, regardless of how small or large the remaining gap
   is. A threshold buys no additional safety here.
2. **An early threshold does not cure livelock; it relocates it to a worse transport.** If
   bulk HTTP segment download cannot keep pace with the firehose, the per-event JSON websocket
   — the *same* firehose, more bytes per event, no bulk compression — certainly cannot. Handing
   off "early" to `/subscribe` under sustained overload just moves a losing race onto the
   slower path. The realistic ordering is the opposite: bulk HTTP throughput ≫ live ingest
   rate, so the gap closes and the loop converges.

**Why the active segment is the natural, irreducible websocket region.** `getSegment` /
`getBlock` serve sealed files only; the active segment is physically not downloadable. It is
bounded by `MaxSegmentBytes` (default 256 MB) — the moment it fills it seals and becomes
downloadable on the next page. So the websocket's job shrinks to "the current ≤256 MB active
segment plus whatever arrives live," which is exactly what `/subscribe`'s cold-then-live replay
is built for.

**Convergence is backstopped even if segments seal during the handoff.** Between the final page
and the `/subscribe` connect, one or more segments may seal. That is harmless: `/subscribe` is
**not** websocket-only — its replay path (`internal/subscribe/replay.go:WalkFromCursor`) reads
sealed segments from disk first, then the active segment, then live. So any just-sealed segment
is still delivered losslessly, merely per-event instead of bulk. The "prefer HTTP" rule is thus
about *efficiency* (parallel, compressed, bulk downloads for the overwhelming majority of
data); correctness at the seam is guaranteed by `/subscribe`'s cold replay regardless.

**Pathological note (low traffic, not a livelock):** under very low ingest the active segment
may stay unsealed for a long wall-clock span (segments seal by size, not time — confirmed:
there is no age-based seal). This is benign: the unsealed band is small by definition (it never
reached 256 MB), and `/subscribe` replays it from the active segment's flushed blocks. No data
is at risk; the client simply tails a quiet stream.

Observability: emit `sealedTipSeq - plannedThroughSeq` (the residual gap) and a page counter
per backfill so operators can watch convergence and spot a client that genuinely cannot keep
up (a capacity problem, surfaced — not silently absorbed).

## 15. Why A and B cannot be cleanly separated

`liveSink.onLive` does two jobs at once: it **orders** events across the cutover (buffer →
drain → forward) and it **folds live tombstones** into the suppressor
(`internal/client/livesink.go:62,172-175`). 

- If we did **B before A**, the new bufferless loop would still need somewhere to fold live
  tombstones during catch-up — re-introducing a parallel structure we'd immediately delete.
- Doing **A first** removes the folding job, leaving `liveSink` as a pure ordering buffer; **B
  then deletes that buffer wholesale.** `engine.go`'s cutover path is rewritten once, not
  twice.

Hence one spec, sequenced A → B.

## 16. Oracle / testing additions for Part B

The oracle must now exercise pagination and the bufferless handoff, not just a single plan:

- **Multi-page backfill correctness.** Drive a backfill with a small `MaxEntries` so the plan
  truncates repeatedly; assert the union of all pages' emitted rows, folded in seq order,
  equals ground truth (reuses the §6 fold-convergence check). No row may be skipped at a page
  boundary and the continuation cursor must be exactly the prior page's `plannedThroughSeq`.
- **Mid-download seal.** Seal new segments *between* pages (the simulator can drive ingest
  during the paged download) and assert page *k+1* picks them up — i.e. nothing sealed during
  page *k* is lost without a client buffer.
- **Caught-up handoff.** Assert the client connects `/subscribe` exactly when
  `plannedThroughSeq >= sealedTipSeq`, and that the seq at handoff is ≥ the lookback floor (so
  it never clamps).
- **Stale-cursor signal.** With a tiny `--cursor-lookback`, connect `/subscribe` at a seq
  below the floor and assert the explicit "too old" signal is returned (NOT a silently
  truncated stream). This is the regression test for the §10.1 bug.
- **Fell-off-live recovery.** Force the consumer below the floor mid-stream and assert it
  re-enters the backfill loop and re-converges.
- **Exhaust-sealed termination.** With ingest paused, assert the loop pages until
  `plannedThroughSeq == sealedTipSeq` (every sealed segment consumed) and then — and only then
  — connects `/subscribe`. Then resume ingest and assert the just-sealed segments arrive via
  `/subscribe`'s cold replay (the §14.1 handoff backstop), losslessly.
- **Sustained-ingest convergence.** Drive moderate continuous ingest (below bulk-download
  throughput) and assert the loop still reaches the sealed tip and hands off — i.e. the gap
  trends to zero rather than diverging. Assert the residual-gap metric is observable.

Add a mutant modeling each new failure mode (off-by-one continuation cursor that skips a page
boundary; silent clamp re-introduced; handoff below floor).
