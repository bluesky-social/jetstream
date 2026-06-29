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

---

# Revision 2026-06-28b — core-assurance change + DID-tombstone catch-up (READ FIRST)

> **Status:** decided with Jim, verified against code + an adversarial review.
> **This section supersedes the conflicting parts of §1, §2.1, §2.2, §5.2, and §6
> below.** Where the older sections still describe "point-in-time correctness,"
> "fold-convergence to ground truth," or "always-deliver #account as the
> deletion signal," read them through this revision. The rest of the mechanics
> (overlay removal, pagination, bufferless cutover) stand.

## R1. We are changing Jetstream's core data assurance — on purpose

**Old (incorrect) framing.** Earlier drafts implied Jetstream hands each consumer a
*self-correcting, point-in-time-accurate* view: the server/client conspired (via the
`getTombstones` overlay + the client `Suppressor`) to **hide** create rows that a
later delete/account-delete had already killed, so a backfilling consumer never saw a
record that was already dead. That hiding is the machinery we are deleting.

**New framing.** Jetstream is an **at-least-once, filter-honoring event log**, not a
live mirror of current network truth. It will deliver create rows for records that are
already dead on the network, and it will **not** silently fold them away for you.
Deletions are **positive marker events** (`#delete`, `#update`, `#account` with
`active=false,status=deleted`, `#sync`), retained durably forever, never silent
absences.

**The assurance we still guarantee — and must not weaken:**

> **No silent loss of in-scope, retrievable data.** Every event that matches a
> subscription's filter and that the server can still serve is delivered **at least
> once, in sequence order.** If the server holds a matching event and walks past it
> without delivering it, that is a bug (CLAUDE.md: crashing is preferred over data
> corruption).

## R2. Completeness is still achievable — it just requires cooperation

This is the crucial point and must be stated plainly in the docs: **a correctly-behaving
consumer can still build a complete, correct copy of the atproto network from
Jetstream.** Completeness is now a *joint* property of three cooperating parties:

- **The server** preserves every deletion marker forever and delivers every in-scope,
  retrievable event at least once in seq order.
- **The client** *folds* the stream it receives: creates/updates apply; deletes,
  account-deletes, and syncs remove. (The bundled Go client does this; third-party
  clients must too.)
- **The end user** subscribes to the markers their data model needs. A consumer that
  wants account-deletion completeness must consume `#account`/`#sync` (which it does —
  see R3/R4), not filter them away and then complain they never arrived.

Under that cooperation the folded result equals network truth. What we are dropping is
the promise that Jetstream pre-folds it *for* you at delivery time. We are **not**
dropping the ability to reach a complete, correct mirror.

**Bounded incompleteness, stated precisely (this is reassuring, not scary).** Below the
compaction watermark `W`, superseded create/update rows are already *physically removed*
from segments, so a backfill never even emits them — nothing to reconcile. The only
records a consumer may transiently hold that are already dead live in the uncompacted
tail `(W, tip]` (≈ one compaction interval). Those converge as their markers arrive.

> # ⚠ REVISION 2026-06-29 — §R4 mechanism REPLACED (READ FIRST)
> #
> # §R3 (the gap) stands exactly as written. §R4's **mechanism** does not: the
> # `wantDidTombstones` / `didTombstones` planBackfill start-snapshot (shipped as
> # step 3, commit 154eee3) has been **reverted and replaced** by an in-archive
> # **reserved DID-marker sentinel collection** index (issue #175, decided with
> # Jim). The gap is now closed where it originates — the segment index — instead
> # of with a cross-process side-channel snapshot.
> #
> # **The replacement, in one paragraph.** DID-level markers (#account, #identity,
> # #sync) carry an empty collection, so the seal/rewrite index now tags each
> # marker-bearing block with a reserved sentinel collection name (`$account`,
> # `$identity`, `$sync`; see `segment/sentinel.go`). These names are invalid
> # NSIDs (`atmos.ParseNSID` rejects a `$`-leading, <3-segment string) and the
> # planBackfill request validator only admits real NSIDs / NSID-authority
> # wildcard prefixes, so a client can never name or prefix-match a sentinel — it
> # cannot collide with real traffic. The planner
> # (`manifest.collectionIDsForSegment`) unconditionally admits a segment's
> # sentinel ids under any collection filter, so marker blocks are always
> # selected; the per-block DID bloom still narrows by DID. The markers then ride
> # **inline** through the normal getBlock download, exactly as record-level
> # deletes already do, and a folding consumer converges with **zero** client-side
> # special-casing.
> #
> # **Why this is better.** It deletes the entire snapshot surface — the
> # `wantDidTombstones` input, the `didTombstones`/`didTombstonesIncluded` output,
> # the client `snapshotSelector`/suppression fold, `planBackfillStart`, the
> # fail-closed `errSnapshotMissing` gate, the server `attachDIDTombstones` +
> # `Tombstones` wiring, and the whole snapshot-before-first-fetch ordering
> # invariant and its race-freedom proof. The DID-level case collapses into the
> # already-solved record-level case (getBlock reads on-disk truth at fetch time;
> # the killer is downloaded in seq order alongside its victim). It is also
> # simpler for third-party clients (a future TS client needs no snapshot logic),
> # more precise under a DID filter (the block DID bloom narrows server-side, no
> # hand-rolled `didTombstones`-vs-`dids` intersection), and requires no new wire
> # field. Cost: a localized change to the seal/rewrite index path (metadata only;
> # seq envelopes preserved) and, in a deployed world, a reseal/reindex of
> # pre-existing segments — which is free today because nothing is deployed.
> #
> # **What this does NOT change.** §R3 (the gap), §R5 (pagination), §R6 invariants
> # 1/4/5/7/8 (pin `beforeSeq=S`, deliver markers on both live wires, connect
> # `/subscribe` at S, getBlock reads on-disk truth, the §14 too-old signal), §R7's
> # by-DID fold-convergence oracle check, and §R8 (1-based seqs) all stand. §R6
> # invariants **2, 3, and 6** (snapshot-once, snapshot-as-suppression, fail-closed
> # on snapshot fetch) are **deleted** — there is no snapshot. Read the §R4 body
> # below through the rewritten "§R4 (REVISED)" subsection that immediately follows
> # the original; the original §R4 text is retained only as the reasoning trail for
> # why the snapshot was *considered*, and must not be implemented.

## R3. The one real gap this revision closes: DID-level tombstones under a filter

Record-level deletes/updates carry a collection, so a collection-filtered backfill
already downloads them inline and in order — **no problem.** The gap is narrow and
specific:

- **DID-level** tombstones — account-delete (`KindAccount`) and `KindSync` — carry an
  **empty collection** (`segment/event.go:62-65`). They are never indexed into a
  block's collection summary (`segment/seal.go:327`, `segment/rewrite.go:260` only index
  non-empty collections).
- A **collection-filtered** backfill selects blocks **by collection** (`internal/
  manifest/plan.go:215,276-286`), so it never downloads a DID-tombstone-only block. The
  live tail starts at the sealed tip, *above* those markers, so they are never
  re-delivered either.
- Today the (being-deleted) `getTombstones` overlay is the only thing covering this.
  Remove it with no replacement and a collection-filtered consumer keeps a deleted
  account's records **forever** — a silent violation of R1.

(Unfiltered and DID-only backfills are already safe: they bypass the collection gate and
the DID bloom pulls the account-delete blocks. The gap is collection-filtered queries
only.)

## R4 (REVISED 2026-06-29). The fix — reserved DID-marker sentinel collections (inline, no snapshot)

> This is the **implemented** mechanism (issue #175). It replaces the original §R4
> snapshot below, which was shipped then reverted (commit 154eee3). See the
> REVISION banner above §R3 for the rationale.

The gap is closed in the **segment index**, so DID-level markers become selectable
by a collection-filtered plan and ride **inline** through the existing download —
the same path record-level deletes already take. No side channel, no client
suppression, no ordering invariant, no race proof.

1. **Reserve a sentinel collection name per DID-level marker kind**
   (`segment/sentinel.go`): `$account`, `$identity`, `$sync`. They begin with `$`,
   which makes them invalid NSIDs (`atmos.ParseNSID` requires ≥3 dot-separated
   segments). Requested collections are validated as exact NSIDs or NSID-authority
   wildcard prefixes, so **no client request can name or prefix-match a sentinel** —
   it can never collide with real collection traffic. Locked by a test asserting
   `ParseNSID` rejects each sentinel. The names are written into sealed footers'
   collection string tables and are therefore part of the on-disk format (load-bearing
   once sealed; rename ⇒ reseal).

2. **Index the sentinel at seal AND rewrite time** (one shared helper,
   `blockWalkResult.indexEventCollection`, used by both `segment/seal.go` and
   `segment/rewrite.go` so the two index paths cannot drift): for a block containing a
   marker of kind K, add `didMarkerSentinel(K)` to that block's collection set. The
   marker's own (empty) collection is still not interned, and the sentinel does **not**
   increment `collectionEventCounts` (it is a selection hint, not per-collection
   traffic). Compaction's rewrite re-derives the index from scratch, so a marker that
   survives compaction keeps its sentinel.

3. **Admit the sentinel in the planner unconditionally under a collection filter**
   (`manifest.collectionIDsForSegment`): when building the matched collection-id set
   for a segment, always include that segment's DID-marker sentinel ids
   (`segment.IsDIDMarkerSentinelCollection`). This only widens the set, preserving the
   one-sided no-false-negatives contract. The per-block **DID bloom still applies**, so
   a collection+DID-filtered request pulls only marker blocks that may contain the
   requested DID — strictly more precise than the reverted server-side
   `didTombstones`-vs-`dids` intersection.

4. **The client needs nothing new.** The exact `Matcher` already delivers
   `#account`/`#identity`/`#sync` under a collection filter (issue #171, the
   `!Kind.IsCommit()` bypass at `internal/client/filter.go`). The marker arrives via
   `getBlock` in seq order, the consumer folds it, and a record the account re-created
   after the delete (seq > marker) is naturally retained (reactivation, §R4 original
   step’s concern — now handled for free because there is no synthesized event and no
   suppression to mis-scope).

**Why race-free (trivially).** `getBlock` reads on-disk truth at fetch time. The
killer marker D is selected and downloaded by the same plan, in seq order, as its
victim create C — there is no separate snapshot that could be stale relative to the
bytes. The original §R4's entire eviction-interleaving race (and its proof) does not
arise because there is no second source of truth. The §R7 eviction-interleaving and
reactivation oracle scenarios still apply as **convergence** checks (fold the full
stream, match killers by DID); they no longer gate a snapshot-vs-seam distinction.

**Coverage note (§R3 boundary).** Unfiltered and DID-only backfills were already
safe and are unchanged. The sentinel makes **collection-filtered** backfills safe by
selection, closing exactly the §R3 gap. `#identity` is included for symmetry and
future coverage; it kills no records, so it never affected fold-convergence, but it
now reaches a collection-filtered backfill inline at no extra cost.

---

> ⚠ SUPERSEDED BY §R4 (REVISED) above. The snapshot mechanism below was implemented
> (step 3 / commit 154eee3) and then **reverted** in favor of the sentinel index.
> Kept only as the reasoning trail for why a snapshot was considered. **Do not
> implement.** Its §R6 invariants 2/3/6 and §R4.1 wire surface are void.

## R4 (ORIGINAL, SUPERSEDED). The fix — a backfill-start DID-tombstone snapshot (verified race-free)

No segment-format change. No pebble persistence. We **reuse the in-memory
`tombstone.Set`** that the server already maintains for compaction (it already holds
exactly the DID-level tombstones in `(W, tip]`, keyed by DID, and already has a
seq-ranged extractor, `SnapshotRange`). The mechanism:

1. **Pin the backfill upper bound to `S`, acquired on page 1.** The first `planBackfill`
   call returns `sealedTipSeq` (§12.2); the client sets `S = that value` and then uses
   `beforeSeq = S` for **every** page **including page 1** — i.e. page 1's emitted rows must
   be re-clamped to `seq <= S` even though `S` was learned *from* page 1 (page 1 is planned
   with no `beforeSeq`, returns `sealedTipSeq`, and the client discards/ignores any planned
   work above `S`). This guarantees the backfill covers exactly the fixed range
   `(afterSeq, S]`; everything above `S` is the live tail's job. Pinning is mandatory — a
   floating upper bound reopens the moving-tip leak (R4 proof) and lets rows above the
   snapshot's coverage enter the download.
2. **Snapshot the DID-level tombstones once, co-atomically with `S`, on page 1.** The same
   page-1 `planBackfill` response that carries `sealedTipSeq = S` **also carries the
   DID-level tombstone snapshot over `(afterSeq, S]`** (the wire surface is R4.1 below). One
   server-side read produces both `S` and the snapshot under the planner's view, so they are
   co-atomic by construction and **strictly precede the client's first `getBlock`.** The
   client **holds this snapshot for the whole backfill**; it is NOT re-requested per page or
   at the seam. (Only page 1 carries it; later pages omit it.)
3. **Fold the snapshot as seq-scoped, DID-only suppression — not as delivered events.** For
   each materialization row the backfill emits in `(afterSeq, S]`, drop it iff
   `snap.DIDs[row.DID].Seq > row.Seq`. Consult **only the DID-level entries** (`snap.DIDs`);
   do **not** apply record-level entries — record deletes/updates carry collections and ride
   inline through the normal download, so the snapshot's job is purely the collection-less
   DID-level markers. (If reusing `tombstone.Snapshot.ShouldDrop`, note it also checks
   record-level entries `tombstone.go:172-184`; use a DID-only variant or pass a snapshot
   whose `Records` map is empty.) **Never synthesize an `#account`/`#delete` event from a
   snapshot entry** — the set is a compaction structure, not a liveness mirror (reactivation
   note below).
4. **Authoritative account/identity liveness comes from the live tail.** Both
   `/subscribe` and `/subscribe-v2` deliver `#account`, `#identity`, and `#sync`
   **unconditionally** (bypassing the collection filter) on the live wire. Any state
   change above `S` — including an account *reactivation* after a snapshotted delete —
   arrives there for the consumer to fold.
5. **Connect `/subscribe` at `cursor = S`** once the paginated loop has consumed the
   sealed range. Replay is inclusive (`seq >= cursor`) and the client dedups by seq, so
   the seam is at-least-once with no gap; segments sealed *during* backfill are picked up
   by `/subscribe`'s cold replay (`WalkFromCursor` re-reads the manifest at connect).

**Why this is race-free (the proof, in one line).** Within a compaction chunk, the
segment rewrite that physically drops a victim-create C **returns before** the watermark
save and before `Tombstones.Evict` (`compact_deletes.go:163 → :182 → :189`); `getBlock`
reads on-disk truth at fetch time (`getblock.go:85-115`); the snapshot precedes every
fetch. So "the client downloaded C" and "the snapshot is missing C's killer D" **cannot
both happen** — if D was evicted before the snapshot, the rewrite that removed C also
ran before the snapshot, so `getBlock` reads the post-drop generation and C is already
gone. The **rejected** alternative — chase a moving tip and snapshot at the cutover seam
— leaks here: a slow client downloads C (old generation) and `Evict` removes D before
the seam snapshot, so C survives with no killer. Both the `beforeSeq` pin **and**
snapshot-at-start are required; either alone has a hole.

**Reactivation note (don't skip this).** `Observe` records only the max account-*delete*
seq and does **not** clear a tombstone on reactivation (`tombstone.go:241-243`). That is
correct for our use: the snapshot suppresses only the consumer's own emitted creates
with `seq < D` inside `(afterSeq, S]`; a record the account re-created after the delete
has `seq > D` and is correctly retained, and the reactivation `#account` row arrives on
the live tail above `S`. This matches the oracle's `groundTruthLive` (a record survives
iff `kill_seq <= record_seq`). The danger exists **only** if a client wrongly treats a
snapshot DID entry as "this account is dead now, purge it" — it must not.

### R4.1 Wire surface: piggyback the DID-tombstone snapshot on planBackfill page 1

> ⚠ VOID — SUPERSEDED BY §R4 (REVISED). There is no wire surface: `wantDidTombstones`,
> `didTombstones`, and `didTombstonesIncluded` were added then reverted. The sentinel
> index needs **no new lexicon field, no generated-binding change, and no
> `tombstone.Set` on the read path.** Post-revert, `SnapshotRange` is overlay-only again
> (planBackfill no longer calls it), so §8 step 5 can remove it after the overlay is
> deleted. Text below is void.

The snapshot must cross a process boundary — the client reaches the server only over XRPC,
`SnapshotRange` is an in-memory server method (`tombstone.go:73`) with no caller once the
overlay/`getTombstones` bridge is deleted (§8 step 4). **Decision: piggyback it on the
`planBackfill` response, populated on the first page only.** No new endpoint; reuses the call
the client already makes to learn `S`, which is what makes `S` and the snapshot co-atomic.

- **Lexicon:** add an optional output field `didTombstones` to
  `lexicons/network/bsky/jetstream/planBackfill.json` — an array of `{ did, seq }` (the
  account/sync DID-level tombstones with `seq` in `(afterSeq, sealedTipSeq]`). Regenerate
  `api/jetstream/*`. DID-level only; record-level tombstones are never sent (they ride inline).
- **Server:** wire the `*tombstone.Set` (constructed at `runtime.go:273`) into
  `newPlanBackfillHandler` (`planbackfill.go:62`). Populate `didTombstones` from
  `Set.SnapshotRange(afterSeq, sealedTipSeq).DIDs` **only when the request is page 1** — i.e.
  when the client signals it is starting a fresh backfill. Page 1 is "the client has no prior
  cursor for this backfill"; encode it explicitly (e.g. a request flag `wantDidTombstones`, set
  by the client only on its first call) rather than inferring from `afterSeq == 0`, since a
  resume-from-cursor backfill also needs the snapshot over its own `(afterSeq, S]`. Capturing
  `sealedTipSeq` and the snapshot in the same handler invocation, under the planner's manifest
  view, is what gives the co-atomic `S`+snapshot guarantee R4 step 2 relies on.
- **Client:** on the first `planBackfill` call set `wantDidTombstones=true`, read both
  `sealedTipSeq` (→ `S`, pinned as `beforeSeq` thereafter) and `didTombstones` (→ the held
  suppression snapshot) from that one response. Subsequent pages omit the flag and ignore the
  field.
- **`SnapshotRange` survives §4.3 pruning** — it now has this production caller, so it is NOT
  removed (update §4.3, which currently lists it as overlay-only/removable).
- **Bound:** the snapshot is DID-level tombstones in the uncompacted tail `(W, S]`, intersected
  with the request's DID filter when one is set — small (deletes are sparse). For a DID-filtered
  request, filter `didTombstones` to the requested DIDs server-side.

## R5. How this rides the paginated `planBackfill` (Part B) — it makes it easier

Part B already turns backfill into a paginated limit/offset loop: repeated
`planBackfill(afterSeq=cursor, beforeSeq=…)` calls advancing `cursor` until the sealed
range is consumed, then one `/subscribe` connect. This revision slots in cleanly:

- `beforeSeq = S` (R4 step 1) is **exactly the "end sequence number"** of the paginated
  range. Pinning it for every page is not extra work — it is the natural upper bound of
  a bounded paginated scan, and it is what makes the start-snapshot's range
  `(afterSeq, S]` line up *exactly* with the bytes the loop will download. Floating the
  upper bound (chasing a live tip) is what reintroduces the leak, so the pinned-range
  pagination model is also the *correct* one.
- **(REVISED 2026-06-29.)** There is no per-backfill snapshot to carry: DID-level
  markers are selected inline by every page whose plan touches their blocks (via the
  sentinel index), so they need no client state and no coupling to page boundaries. The
  pinned `beforeSeq = S` range still matters — it bounds the paginated scan — but it no
  longer has to "line up with a snapshot," only with the bytes the loop downloads.
- The done predicate (`plannedThroughSeq >= sealedTipSeq`, where here `sealedTipSeq` is
  pinned to `S`) and the `/subscribe` connect at `cursor = S` are unchanged from Part B.

## R6. Required invariants (implementers MUST encode all of these)

> ⚠ REVISED 2026-06-29: invariants **2, 3, and 6 are VOID** — they governed the
> reverted snapshot and there is no snapshot. Their replacement is a single
> invariant **2′** (the sentinel index). Invariants 1, 4, 5, 7, 8 stand unchanged.

1. **Pin `beforeSeq = S`** for the entire backfill; never let the upper bound float per
   page.
2. **(REPLACES old 2/3/6.) Index DID-level markers under their reserved sentinel
   collection at seal AND rewrite, and admit those sentinels in every
   collection-filtered plan.** The markers then ride inline through `getBlock` in seq
   order; the consumer folds them with no suppression and no synthesized events. Tests
   must pin: (a) `ParseNSID` rejects every sentinel (collision-proof); (b) seal and
   rewrite both index the sentinel for a marker-bearing block without inflating
   collection event counts; (c) a collection-filtered plan selects marker blocks while
   the DID bloom still narrows; (d) end-to-end fold-convergence for a collection-filtered
   backfill whose victim's killer is a DID-level marker (the §R7 gate test).
4. **Deliver `#account`/`#sync`/`#identity` unconditionally on both live wires**, so
   post-`S` state (incl. reactivation) reaches the consumer.
5. **Connect `/subscribe` at `cursor = S`** after the loop; rely on inclusive replay +
   client seq dedup; cold replay covers segments sealed during backfill.
6. **(VOID — was: fail closed on snapshot fetch.)** No snapshot is fetched, so there is
   nothing to fail closed on. A planner that fails to admit the sentinel, or a seal that
   fails to index it, is a correctness bug caught by the invariant-2′ tests, not a
   runtime fail-closed gate.
7. **Keep `getBlock` reading on-disk truth at fetch time** and segment seq-envelope
   preservation as **load-bearing invariants.** The race-freedom proof depends on them;
   gate any future block-repacking or "trust the planned checksum" optimization on
   re-proving the chain.
8. **The §14 explicit "cursor too old" signal is still required and separate.** Pinning
   closes the snapshot race, but a very slow backfill can still push `S` below the live
   lookback floor by connect time; without the explicit signal the terminal `/subscribe`
   hop silently drops `(S, floor]`. (See §10.1/§14.)

## R7. Oracle / test additions for this revision

> ⚠ REVISED 2026-06-29 for the sentinel-index mechanism. The **fold-convergence**
> check (match killers by DID, output-restricted) is unchanged and is the load-bearing
> invariant. The snapshot-specific scaffolding (snapshot-at-start vs at-seam,
> snapshot-before-first-fetch ordering) is **dropped** — there is no snapshot. The
> eviction-interleaving and reactivation scenarios remain valuable as convergence
> checks; they just no longer distinguish a snapshot timing.

- **Collection-filtered fold-convergence (THE gate, replaces old §6 invariant 3):** fold
  the *full* received stream, then restrict the *output* record set by collection; match
  a dead record's killer to a DID-level marker **by DID** (not collection). Do **not**
  cross-check filtered-vs-filtered on the same server (that self-comparison is blind to
  the gap). Implemented as `TestFoldConvergence_CollectionFilteredDIDTombstoneGap`
  (`internal/oracle/foldconvergence_gate_test.go`) — now PASSES via the inline sentinel
  path. A mutant that reverts the sentinel index (seal or planner) must FAIL it.
- **Sentinel index unit coverage** (`segment/sentinel_test.go`,
  `internal/manifest/plan_test.go`): `ParseNSID` rejects each sentinel; seal AND rewrite
  index the sentinel for a marker block without inflating event counts; a
  collection-filtered plan selects marker blocks; a collection+DID-filtered plan still
  narrows by the block DID bloom.
- **Eviction-interleaving (now a convergence check, not a race check):** drive a
  collection-filtered backfill; advance compaction across an account-delete `D` whose
  victim create `C < D` was already downloaded. Assert the client converges to `C`-dead.
  Race-freedom is structural now (getBlock reads on-disk truth; `D` is selected inline by
  the same plan), so there is no snapshot-timing mutant to kill — but the convergence
  assertion still guards against a regression that drops `D` from selection.
- **Reactivation-after-delete:** account deleted at `D`, reactivated and re-creates a
  record at `R > D`. Assert the folded result retains the `R` record and drops the `< D`
  creates — naturally, since markers fold inline and there is no suppression to mis-scope.
- **`afterSeq < W` boundary:** a backfill whose `afterSeq` is below the watermark; assert
  no create in `(afterSeq, W]` survives un-dropped on disk (rewrite-before-evict). Sharp
  invariant worth locking; unaffected by the mechanism change.

## R8. Sequence numbers start at 1, not 0 (decided 2026-06-28b)

**Decision: the seq counter starts at 1. Seq 0 becomes a pure "nothing yet" sentinel and is
never a real event.** This is a wire/format change, taken freely because nothing is deployed and
there are no consumers (see the deployment-context note above). It supersedes the targeted
`#7-internal` "seq-0 swallow" patch — it removes the bug's *cause* rather than re-pinning a rule.

**Why.** The whole 0-based seq space forces presence to be tracked separately from value, because
seq 0 is simultaneously a valid first event *and* the universal "nothing" sentinel. That
collision is the sole reason for a pile of machinery: the `gt.Option[uint64]` seq presence-tracking
(the `dedupFloor` vs. wire-cursor split, `live.go:64-79,136-145`), the `backfillCoveredNothing`
flag and `coveredThrough` optional (`engine.go:448-452`, `livesink.go:93-116`), and the `#112`
"don't swallow seq 0" comments throughout. (Audit the `gt.Option[uint64]` seq sites and collapse
those that only existed to distinguish "seq 0" from "nothing"; the symbol-named targets above are
the concrete anchors — don't rely on an exact site count.) With seqs starting at 1:

- `0` is an unambiguous "nothing delivered / empty archive" sentinel. The dedup check
  `ev.Seq <= lastSeq` with `lastSeq = 0` passes the first real event (seq 1 > 0) automatically —
  **the seq-0 swallow becomes structurally impossible, not patched.**
- `plannedThroughSeq == 0` unambiguously means "empty archive"; the `backfillCoveredNothing` flag
  is deleted, which also dissolves the §5.1↔§13 contradiction (there is no special-case left to
  disagree about).
- Much of the `gt.Option[uint64]` seq machinery collapses back to plain `uint64`, simplifying
  both the client cutover and the subscribe cursor/dedup paths.

**Semantics that stay correct (verified by inspection):**

- `cursor=0` now means "replay from before the first real event" = replay everything — the same
  effective behavior as today's "replay from seq 0," and more intuitive (0 = "from the
  beginning," not "the event numbered 0").
- `afterSeq` is an EXCLUSIVE lower bound, so `afterSeq=0` still means "from the start" and the
  matcher's `afterSeq>0` guard (`internal/client/filter.go:127`) stays correct with the first real
  seq at 1.
- The v1/v2 cursor split (`CursorSeqMaxThreshold = 1e15`, `cursor.go:29`) is unaffected — seqs
  still sit far below the timestamp namespace.
- Segment *index* stays 0-based (`planner.go:52`) — that is a different counter from event seq;
  do not conflate them.

**Implementation scope.** Seed `nextSeq = 1` on a fresh archive. The fresh-dir default reads as
`0` today (`loadNextSeq`, `writer.go:698-702` — missing key → 0). The seed/reconcile point is the
**event-seq** counter (`seq/next`) in the writer's open path: `reconciled := pebbleSeq` then
`w.nextSeq = reconciled` (`internal/ingest/writer.go:126-142`) — floor `reconciled`/`nextSeq` to 1
when the pebble key is absent (fresh dir), leaving the crash-recovery reconcile (`maxSeq+1`)
untouched. NOTE: this is a DIFFERENT counter from the compaction watermark `compaction/seq`
(`compaction_watermark.go`); do **not** edit `initCompactionWatermarkFloor` — with `nextSeq = 1`
its existing `else` branch already yields watermark `nextSeq-1 = 0` ("nothing compacted"), which is
correct. **Do NOT touch the running increment** `nextSeq: prepared.MaxSeq()+1`
(`async_flush.go:114`) — that is the steady-state advance and is already correct; patching it would
corrupt the counter. Confirm the `+1` chain yields `1` for the first-ever event given the new seed.
No migration (nothing deployed). Then *delete* the now-unnecessary presence machinery — most of
this change is removal, which lowers long-term risk. Update the in-code and doc statements that
assert 0-based seqs, at least: `docs/README.md:58`, and the load-bearing comment at
`internal/client/filter.go:121-123` ("jetstream's seq space is 0-based — the first-ever event is
seq 0", ref #111) plus any parallel server-side comment — these become false under 1-based seqs.

**Test:** start from a genuinely empty archive, ingest the first-ever event (now seq 1), and
assert it is delivered exactly once across a from-empty backfill→live handoff (the old seq-0
swallow regression test, re-pointed at seq 1).

---

> # ⚠ READING ORDER FOR IMPLEMENTERS — the Revision block (§R1–R8) is AUTHORITATIVE
> #
> # Everything below this line (§1–§16) is the **original draft**. The adversarial review
> # found several holes and self-contradictions in it; the decisions that fix them live in
> # the **Revision 2026-06-28b** block ABOVE (§R1–R8). Where the two disagree, **§R wins.**
> #
> # Each original section that has been superseded carries an inline `⚠ SUPERSEDED BY §Rn`
> # banner pointing to the governing decision. The original text is kept (not deleted) only
> # as the *reasoning trail* — do **not** implement from it directly. The decision map:
> #
> #   §2.1 "convergence to ground truth"        → §R1/R2  (relaxed: cooperative completeness)
> #   §2.2 "no external contract" / endpoint     → Deployment-context note + §R1
> #   §3   "account/identity = revert to v1"     → §R3 (v1 AND v2 always deliver acct/id/sync)
> #   §5.1 "backfillCoveredNothing unchanged"    → §R8 (1-based seqs delete that machinery)
> #   §5.2 delivery contract (no backfill catch-up) → §R3 + §R4 (the DID-tombstone snapshot)
> #   §6   oracle invariants 1 & 3               → §R7 (output-restricted fold; by-DID killer)
> #   §10.1/§11/§14 cursor clamp & "never clamps" → §R5 / §14-rewritten / §14.1-corrected
> #   §12.1 truncation "clean boundary"          → §12.1-rewritten (per-unit cursor rule)
> #   §13  buffer deletion / dedup-floor          → §R8 + §R4 (handoff cursor = plannedThroughSeq)
> #   §16  "Caught-up handoff" test (≥ floor)      → §R5/§14/§14.1 (below-floor is handled, not barred)
> #
> # If a section has no banner, it stands as written.

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

> ⚠ SUPERSEDED BY §R1/§R2. The claim below — "folding the entire emitted stream yields the
> ground-truth live set for the caller's query" — is **only true for unfiltered / DID-only
> queries.** For a collection-filtered backfill it is FALSE without the §R4 DID-tombstone
> snapshot (DID-level account/sync markers carry no collection, so the planner never downloads
> them — see finding #1 / report). The governing model is §R1 (at-least-once, no silent loss of
> in-scope retrievable data) + §R2 (completeness is cooperative, bounded to the uncompacted tail).
> Read "convergence under fold" below as the *goal the cooperation achieves*, not an automatic
> property of the raw emitted stream.

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
  §4 decision), not deprecating it.
  > ⚠ SUPERSEDED BY the Deployment-context note (§R block). The rationale "only first-party
  > consumer, no external contract" was imprecise (the Go module is importable). The *correct*
  > and stronger justification: **nothing is deployed and there are no consumers yet**, so there
  > is no compat window to manage at all — remove it cleanly.

## 3. Decisions (resolved with Jim, 2026-06-28)

- **Scope:** Go client + server endpoint removal + docs + oracle/simulator. There is no
  TypeScript client to migrate. Replication (Section 6 of `docs/README.md`) is reviewed
  for overlay coupling but is early-days and not a focus.
- **Overlay fate:** **Remove entirely.** Delete the `getTombstones` handler, the
  `internal/overlay` package (format + cache + metrics), the `overlay_source` adapter, the
  overlay cache ticker wiring, the lexicon, and the generated API stub. Keep only the
  in-memory `tombstone.Set` that compaction needs.
- **Account/identity delivery:** **Deliver both `#account` and `#identity`** to
  collection-filtered subscribers (and the Go client).
  > ⚠ REFINED BY §R3. The decision stands and is now broader: **`#account`, `#identity`, AND
  > `#sync` are delivered unconditionally on BOTH `/subscribe` (v1) and `/subscribe-v2`**,
  > bypassing the collection filter. So v1 and v2 behave identically on this axis (the
  > `filterIdentityByCollection` toggle is removed — both always deliver). Note the precise
  > current state (report #11): `#account`/`#sync` *already* bypass on the server; only
  > `#identity` on v2 actually changes server-side, plus the client `Matcher` (`filter.go:101`)
  > stops dropping account under a collection filter. CRUCIAL: live delivery is necessary but
  > **not sufficient** for a collection-filtered *backfill* — that needs the §R4 DID-tombstone
  > start-snapshot, because the planner can't fetch collection-less markers from sealed blocks.

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

> ⚠ PARTIALLY SUPERSEDED. Two corrections: (1) the `liveSink` buffer/flip/drain described
> here is **deleted entirely** by Part B / §13 — do not preserve it. (2) The
> `backfillCoveredNothing` / empty-archive dedup-floor handling called "unchanged" below is
> **deleted** by §R8 (1-based seqs make seq 0 a pure sentinel, so the special case vanishes).
> The Matcher-only filtering and Suppressor removal described here are correct and stand.

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

> ⚠ REFINED BY §R3 + §R4. The wire policy below is correct (now also covering `#sync` and v2),
> but it governs only **live delivery**. It does NOT make a collection-filtered *backfill*
> complete: the planner cannot fetch collection-less DID-level markers from sealed blocks, so
> the client must also take the §R4 start-snapshot of the in-memory `tombstone.Set` over
> `(afterSeq, S]` and fold it as seq-scoped suppression. The "permanently stale view" this
> section warns against is exactly finding #1; §R3+§R4 together are what actually prevent it.

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

> ⚠ SUPERSEDED BY §R7. Invariant 1 below is **ambiguous in a way that hides finding #1** and
> invariant 3 must match killers **by DID, not collection** — see §R7 for the precise, correct
> formulation (fold the full stream, then restrict the OUTPUT record set by collection; retain
> DID-level tombstones by DID). §R7 also adds the eviction-interleaving, reactivation,
> `afterSeq<W`, and snapshot-ordering tests that this original list lacks. Implement §R7's
> versions, not these.

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

> ⚠ REWRITTEN 2026-06-28b to fold in decisions D3–D7. The list below is the authoritative
> step plan; it replaces the earlier 11-step list. File one GitHub issue per step (per
> AGENTS.md). Pre-release context: no compat windows to manage; "land X before Y" below is
> about *internal* correctness/dependency, not deployment ordering.

**Part A — drop client tombstones, relax the contract.**

1. **`subscribe+client: always deliver #account/#identity/#sync on v1 and v2`** (§R3). Remove
   the `filterIdentityByCollection` toggle so both endpoints always deliver these (server:
   `subscribe/filter.go` — only `#identity` on v2 actually changes; `#account`/`#sync` already
   bypass). Client `Matcher.Wants` (`filter.go:101`) stops dropping account under a collection
   filter. Tests for both.
2. **`client: remove tombstone suppression from backfill`** (§5.1). Delete `suppress.go`,
   collapse `selector.go` to matcher-only, strip the suppressor from `engine.go`. (Do NOT yet
   touch the cutover buffer — that's step 7.) Update/delete the suppressor tests.
3. **`segment+manifest: DID-marker sentinel collections close the §R3 gap`** (§R4 REVISED,
   issue #175). **REPLACES the reverted `client: backfill DID-tombstone start-snapshot`
   (commit 154eee3).** Reserve `$account`/`$identity`/`$sync` sentinel collection names
   (`segment/sentinel.go`); index them per marker-bearing block at seal AND rewrite (shared
   `indexEventCollection` helper); admit them unconditionally under a collection filter in
   `manifest.collectionIDsForSegment`. The markers then ride inline through getBlock — no wire
   field, no client snapshot, no `tombstone.Set` on the read path, no fail-closed gate, no
   ordering invariant. Depends on step 1 (the client `Matcher` already delivers the markers).
   Gated by the §R7 fold-convergence gate test (`TestFoldConvergence_CollectionFilteredDIDTombstoneGap`),
   which fails without the index and passes with it.
4. **`server: remove getTombstones overlay endpoint`** (§4.2). Delete `internal/overlay`,
   `gettombstones.go`, `overlay_source.go`, overlay metrics, runtime ticker, lexicon, stub;
   unregister the route. **Gated on step 3** (the sentinel index must replace the overlay's
   DID-tombstone coverage first — do not delete the overlay before its replacement lands).
5. **`tombstone: prune overlay-only API`** (§4.3). Remove members only the overlay used.
   **Note (2026-06-29):** with the snapshot reverted, `SnapshotRange` is **overlay-only again**
   — compaction folds on-disk (`FoldRange`/`collectCompactionTombstones`), it does not call
   `SnapshotRange` — so once step 4 deletes the overlay, `SnapshotRange` (and `Snapshot`, which
   wraps it) become removable. Keep `Observe`/`Evict`/`FoldRange`/`Snapshot.ShouldDrop` and
   everything compaction needs. Depends on step 4.
6. **`oracle: fold-convergence + DID-tombstone delivery`** (§R7). Replace
   `CheckOverlayReconstruction` with the §R7 invariants (output-restricted fold; by-DID
   killers; eviction-interleaving; reactivation; `afterSeq<W`; snapshot-ordering). Refresh the
   mutation campaign (§6.1) with the §R7 replacement mutants.

**Part B — 1-based seqs, paginated bufferless cutover.**

7. **`seqs: start at 1`** (§R8). Initialize `nextSeq=1` on a fresh archive; delete the
   `backfillCoveredNothing` flag and collapse the `gt.Option[uint64]` seq machinery. Best done
   early — it simplifies steps 8–9. Update docs' 0-based statements.
8. **`manifest: paginate planBackfill`** (§12, §12.1-rewritten). Replace `ErrPlanTooLarge`
   with the per-unit truncation rule (continuation cursor = last-included-unit `MaxSeq`; always
   admit ≥1 unit); add `sealedTipSeq` (required field) to result+lexicon+bindings.
9. **`subscribe: v2 too-old cursor → HTTP 400`** (§14-rewritten, D5). Add
   `CursorEnv.RejectBelowFloor` (v2 true, v1 false); return pre-upgrade 400 with the floor seq;
   v1 unchanged. Standalone server change.
10. **`client: paginate backfill, delete the cutover buffer`** (§13, §R8). Rewrite
    `runBackfillThenLive` as the §11 loop (pin `beforeSeq=S`; done when
    `plannedThroughSeq >= sealedTipSeq`; connect `/subscribe` at `plannedThroughSeq`); delete
    `livesink.go` + the `Buffer` interface; handle the step-9 400 by re-backfilling from
    `Batch.LastCursor` (the folded-in #4 client work). Depends on steps 2, 7, 8, 9.
11. **`oracle: Part B scenarios`** (§16) — multi-page, mid-segment truncation, mid-download
    seal, terminal-hop too-old (clock-advance), fell-off-live recovery, exhaust-sealed,
    sustained-ingest. New mutants per §16.
12. **`docs: rewrite for the relaxed cooperative contract`** (§7 + §R1/§R2). State at-least-once
    + no-silent-loss + cooperative completeness + bounded incompleteness; the §R5 "bounded
    suppression, not zero suppression" wording; the paginated loop; 1-based seqs; drop overlay.
    Update `doc.go` (the #15 public-contract obligation). Correct the stale
    `2026-05-27-cursor-replay-design.md` "no window cap" claim.

**Dependency summary:** 6 before 3 (tests gate the fix); 3 before 4 before 5; 7 early; 8+9 before
10. Run `just test ./internal/oracle`, `just test-long`, and the oracle/mutation recipes after
each of 3, 6, 8, 10, 11 — every one touches end-to-end correctness.

**Still-open design item (does not block the above):** #8 Part B observability — the client
library's telemetry surface for the residual gap / page count (it has no Prometheus registry; the
§14 400 already carries the floor seq). Decide the mechanism (Stats accessor vs. progress
callback) during step 10.

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

> ℹ This is an accurate as-is problem statement. The **fix** is §14-rewritten (D5): v2 returns a
> pre-upgrade HTTP 400 "too old" and the client re-backfills; v1 keeps the documented clamp. Read
> this section as motivation, not as the resolution.

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

> ⚠ TWO CORRECTIONS to the pseudocode below (read with §R4 + §14-rewritten):
> (1) `beforeSeq` must be **pinned to `S`** (the sealed tip read on the first page), NOT
> `request.BeforeSeq` floating per page — pinning is required for the §R4 snapshot range to line
> up and to avoid the moving-tip leak. (2) The `subscribe(cursor=cursor)` connect is **NOT
> "never clamped"** (that claim is the unsound proof, finding #2). It CAN resolve below the floor
> under a slow handoff; the server returns the §14 HTTP 400 "too old" and the client re-backfills
> from its last seq. The loop structure (re-plan, advance cursor, done at `>= sealedTipSeq`) is
> correct; only the two annotations above change.

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
- **The terminal `/subscribe` hop is serviceable; if its cursor has aged out, the §14 400
  catches it.** CORRECTION (2026-06-28b, findings #2 + #16): the original claim here — "never
  clamped, because the floor ≤ the sealed tip" — is unsound. `LookbackFloor`
  (`internal/manifest/manifest.go:617-637`) returns the `MinSeq` of the **oldest** sealed segment
  still in the window (or the freshest if all aged out), computed over sealed segments only. The
  conclusion `floor ≤ current sealed tip` is true (every segment `MinSeq ≤ MaxSeq ≤ tip`), but the
  connect cursor `S` is the tip from the *last plan*, not the current tip — under a slow handoff
  `S` can age below `floor(now)`. So the connect CAN clamp; the §14 HTTP 400 detects it and the
  client re-backfills (§R5). `/subscribe`'s replay (`internal/subscribe/replay.go:WalkFromCursor`)
  reads any segments sealed during the handoff, the active segment's flushed blocks, and in-memory
  pending, then transitions to live via the `Tail.ReadFrom` cold→hot poll loop
  (`internal/subscribe/tail.go:164-216`) — **not** an atomic `SnapshotPendingAndRegister` (that
  symbol does not exist; finding #9). The cold→live seam is gap-free via the monotone cross-phase
  cursor, not a single atomic call. No client buffer needed either way.

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

### 12.1 Truncate-and-continue — exact rule (decided 2026-06-28b, finding #6)

`ErrPlanTooLarge` is retired from the normal path; the planner truncates at a clean seq boundary
and reports a continuation cursor. (`ErrInvalidPlanRequest` for malformed input — negative
`MaxEntries`, inverted seq window, bad threshold — stays.) The exact arithmetic, which an
implementer MUST follow precisely (getting it wrong is either silent data loss or a livelock):

**The unit of truncation is the included work unit, not the enclosing segment.** A work unit is
*one whole-segment entry* (whole-segment mode) or *one coalesced block range* (block mode). The
current loop adds **all** of a segment's units then checks the cap (`plan.go:151-161`) — so the
cap can be exceeded *mid-segment*. The rule:

1. **Count and cap per included unit.** Accumulate units in seq order; check `Stats.Entries`
   after each unit, not once per segment.
2. **Continuation cursor = `MaxSeq` of the LAST included unit.** Block mode: the last included
   coalesced block range's `MaxSeq` (from `segment.BlockInfo.MaxSeq`). Whole-segment mode: that
   segment's `MaxSeq`. **NOT** the enclosing segment's `MaxSeq` after a mid-segment cut — that
   would silently skip the segment's un-included tail blocks (`blockOverlapsSeq` drops
   `MaxSeq <= afterSeq` on the next page, `plan.go:185`, so the skipped band is lost forever).
3. **Always admit at least one unit per page.** If the first matched unit alone exceeds
   `MaxEntries` (cap smaller than one unit of work), include that one unit anyway and set the
   cursor to its `MaxSeq`. Never return zero units with the cursor unadvanced — that livelocks
   the client.

**Why this is gap-free and progressing (the invariant it rests on, verified):** blocks within a
segment are **seq-disjoint and monotonic by block index** — events get their seq from a single
`nextSeq++` counter under the writer lock (`writer.go:324-331`) and flush into blocks in order,
so block *i*'s seqs are all below block *i+1*'s. Therefore the last included unit's `MaxSeq = X`
cleanly separates "included" from "not yet": the next page's `afterSeq = X` (an **exclusive**
lower bound) drops every already-included block (`MaxSeq <= X`) and the very next block
(`MinSeq > X`) is the first one re-planned. No gap, no overlap, no skip; and because `X` is
strictly greater than the previous page's cursor whenever ≥1 unit was admitted, the loop always
advances. The exclusive-`afterSeq` semantics line up exactly with "last included `MaxSeq`" — no
off-by-one.

This also bounds per-call response *entry count* (not bytes — a whole-segment unit can still be
large; see §16 sizing note) and server planning memory, an operational win independent of
correctness.

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

> ⚠ AUGMENTED by §R4 + §R8. This section is correct but incomplete as written; the rewritten
> `runBackfillThenLive` must ALSO: (a) pin `beforeSeq=S` and take the §R4 DID-tombstone
> start-snapshot before the first download, folding it as suppression; (b) handle the §14 HTTP
> 400 "too old" by re-entering the loop from `Batch.LastCursor` (the #4 client work); (c) the
> "`backfillCoveredNothing` / dedup-floor special-casing" listed for deletion is deleted by §R8
> (1-based seqs), and the `liveRewindMargin` overlap likewise goes — no rewind margin needed
> (dedup floor seeded from `plannedThroughSeq`, first live event passes because seq ≥ 1 > 0).

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

## 14. Server change: /subscribe-v2 rejects a too-old cursor with HTTP 400 (decided 2026-06-28b)

Decision (revised — keep it simple): **`/subscribe-v2` returns a pre-upgrade HTTP 400 "cursor
too old" when a v2 seq cursor resolves below the lookback floor.** No close-frame, no new wire
machinery, no lexicon addition. `/subscribe` (v1) is **unchanged** — it keeps silent clamping
for legacy jetstream-v1 compatibility.

This reuses an existing code path. Cursor resolution already happens **before** the websocket
upgrade specifically so a bad cursor can return HTTP 400 (`internal/subscribe/handler.go:131`
"Resolve cursor BEFORE upgrade …"; the upgrade is `websocket.Accept` at `:223`). Invalid
cursors and bad options already return 400 there (`:183-186`). The too-old case routes into the
same path, so the client gets a clean synchronous 400 at connect and never holds a half-open
websocket.

- `internal/subscribe/cursor.go:124-130` — for `ModeReplaySeq`, when `startSeq < floorSeq`,
  return a typed error (e.g. `ErrCursorTooOld` wrapping the requested seq and the floor seq)
  **instead of** clamping — but only when the new per-endpoint reject flag is set (v2). When it
  is unset (v1), keep the existing clamp behavior verbatim.
- `CursorEnv` gains a `RejectBelowFloor bool` (set true by the v2 handler, false by v1),
  mirroring how `EmitResyncReplacementRows` / `FilterIdentityByCollection` already vary v1-vs-v2
  behavior per-endpoint (`internal/jetstreamd/runtime.go:411-419` v1 vs `:420-430` v2; the v2
  flags are set at `:428-429`). This keeps the
  v1/v2 split in the one place all the other per-endpoint policy already lives. `ResolveCursor`
  has exactly one production caller (`handler.go:177` — verified), so the new path affects
  nothing else.
- `internal/subscribe/handler.go` — the existing `if err != nil { http.Error(w, ..., 400) }` at
  `:183-186` handles it. The 400 **body must include the floor seq** (machine- and
  human-readable, e.g. `"cursor 1000 below lookback floor 1500; re-backfill from your last
  seq"`) so the client can log/observe how far behind it was — this is also the observability
  hook for §14.1 / finding #8.
- **v1 keeps silent clamping** (both the seq path `cursor.go:124-130` with the flag unset, and
  the timestamp path `cursor.go:149-155`): the documented v1 wire contract
  (`2026-05-27-cursor-replay-design.md:85-95`), unchanged for backward compatibility with the
  legacy jetstream system. This v1/v2 asymmetry is intentional and must be commented at the
  clamp site. (Finding #14 — v1's silent clamp is bounded, deliberately-retained legacy debt;
  give it a distinct metric label so it stays visible.)

**Client side (the half folded in from the old finding #4).** The Go client's terminal
`/subscribe-v2` connect must treat an HTTP 400 too-old response as **"re-enter the §11
pagination loop from my last durably-processed seq (`Batch.LastCursor`)"**, NOT as a fatal abort
and NOT as generic reconnect churn. A pre-upgrade 400 is *easier* to handle than a mid-stream
close-frame: it arrives synchronously at connect, before any events flow, so it is trivially
distinguishable from live-tail disconnects (`internal/client/live.go:154-182` reconnect path).
This closes the §11 loop's third case (fell-off-live) and the terminal-hop case with the **same**
recovery path — re-backfill from the last seq — so it adds no new client state machine beyond
"recognize the 400, loop back to planBackfill." Bound the re-backfill cycles and assert the
cursor advances monotonically across them (defense against a pathological ping-pong).

### 14.1 Termination rule: exhaust the sealed archive, then hand off (no tunable threshold)

The handoff point is the **sealed/unsealed boundary**, not a configurable gap size. The loop
pages over HTTP until `plannedThroughSeq >= sealedTipSeq` — i.e. every sealed segment has been
downloaded — and only then connects `/subscribe`, which covers the active (undownloadable)
segment and the live tail. "Prefer the sealed archive; use the websocket as little as
possible."

This is provably correct and is also the right operational choice; an earlier draft proposed a
tunable "hand off when the residual gap is small enough" threshold, which is **rejected**
because it protects against nothing the boundary rule doesn't already cover:

1. **Keeping the connect cursor in-window is handled by the §14 too-old 400, not by the boundary
   rule.** CORRECTION (2026-06-28b, finding #2): the earlier claim that "connecting at the sealed
   tip never clamps" was **unsound**. `LookbackFloor(now)` is evaluated against wall-clock time at
   *connect* (`manifest.go:626`), while the connect cursor `S` is the sealed tip read at the *last
   plan* — an earlier moment. Under a slow backfill or a short `--cursor-lookback`, `S` can age out
   of the window before connect (`floor(now) > S`), so the terminal hop CAN resolve below the
   floor. The invariant "floor ≤ *current* sealed tip" is true but compares the wrong tip. We do
   not rely on a boundary or threshold to prevent this; we **detect** it: `/subscribe-v2` returns
   the §14 too-old 400 and the client re-backfills from its last seq. A tunable handoff threshold
   would not help — it cannot prevent `S` from aging out during the handoff window.
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
may stay unsealed for a long wall-clock span. CORRECTION (2026-06-28b, finding #10): the earlier
"segments seal by size, not time — there is no age-based seal" is **factually wrong**. Segments
seal on size (`MaxSegmentBytes`, default 256 MB, `async_flush.go:211`) **OR** are force-rotated
at the start of every compaction pass (`ForceRotate`, `compact_deletes.go:92-96`), which fires
every `--compaction-interval` (default 4h, `main.go:343-346`). So under any non-zero ingest the
active segment seals at least every ~4h regardless of size. This is still benign for the handoff
(the unsealed band is bounded by `min(MaxSegmentBytes, one compaction-interval of ingest)` and
`/subscribe` replays it from the active segment's flushed blocks). But note the interaction with
finding #2: the ~4h force-rotate accrues sealed segments with stale `indexed_at`, which is part
of what can push `S` out of the lookback window — so `--cursor-lookback` should comfortably
exceed `--compaction-interval`, and the §14 too-old 400 is the backstop when it doesn't.

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

> ⚠ CORRECTED BY §R5 / §14 / §14.1. The original "Caught-up handoff" bullet asserted the handoff
> seq is "≥ the lookback floor (so it never clamps)" — that encodes the **reversed**, unsound
> finding-#2 claim and is corrected below. The terminal connect cursor MAY be below the floor;
> that is a *handled* case (§14 HTTP 400 + re-backfill), not an invariant. The Part-A oracle tests
> (eviction-interleaving, reactivation, output-restricted fold) live in §R7.

The oracle must now exercise pagination and the bufferless handoff, not just a single plan:

- **Multi-page backfill correctness.** Drive a backfill with a small `MaxEntries` so the plan
  truncates repeatedly; assert the union of all pages' emitted rows, folded in seq order,
  equals ground truth (reuses the §6 fold-convergence check). No row may be skipped at a page
  boundary and the continuation cursor must be exactly the prior page's `plannedThroughSeq`.
- **Mid-segment truncation (§12.1).** Construct a single block-mode segment whose matched
  coalesced block ranges exceed `MaxEntries`, forcing a cut *inside* the segment. Assert: (1) the
  continuation cursor equals the **last included block range's** `MaxSeq` (a value strictly
  *inside* the segment, NOT the segment's `MaxSeq`); (2) the next page resumes within the same
  segment and emits the un-included tail blocks with **no skipped block**; (3) the cursor strictly
  advances. Then a **one-unit-over-cap** case: set `MaxEntries` below a single block range's entry
  count and assert the planner still returns that one unit and advances (no zero-progress
  livelock).
- **Mid-download seal.** Seal new segments *between* pages (the simulator can drive ingest
  during the paged download) and assert page *k+1* picks them up — i.e. nothing sealed during
  page *k* is lost without a client buffer.
- **Caught-up handoff.** Assert the client connects `/subscribe` exactly when
  `plannedThroughSeq >= sealedTipSeq`. The connect cursor MAY be below the lookback floor (a
  slow handoff); assert that when it is, the §14 HTTP 400 fires and the client re-enters the
  pagination loop — do NOT assert "≥ floor / never clamps" (that is the corrected finding-#2
  claim). When the cursor is in-window, assert a clean connect with no re-backfill.
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

Add a mutant modeling each new failure mode: off-by-one continuation cursor that skips a page
boundary; **a below-floor handoff that is NOT surfaced as a §14 400** (the silent clamp
re-introduced) — killed by the Stale-cursor / Caught-up-handoff tests; **the client treating the
§14 400 as a fatal abort instead of re-entering pagination** — killed by Fell-off-live recovery;
**§12.1 mid-segment cut that reports the enclosing segment's `MaxSeq` instead of the last included
block range's `MaxSeq`** — killed by the mid-segment truncation test; **truncation that returns
zero units with the cursor unadvanced** — killed by the one-unit-over-cap test. (Note: a below-floor
handoff is itself an *expected, handled* condition per §14/§14.1 — the mutant must target the
failure to *signal/handle* it, not the handoff occurring.)
