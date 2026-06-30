# Review + remediation tracker: drop-client-tombstones + paginated bufferless cutover

Date: 2026-06-30
Branch: `tombstone-query-plan-refactor`
Reviewer: jcalabro (with Claude)
Status: **remediation in progress**

> Pre-ship review of the two linked refactors landed on this branch (see
> `2026-06-28-drop-client-tombstones-design.md` and its implementation plan).
> The review ran an adversarial multi-agent sweep across 11 risk dimensions
> (sentinel index, pagination planner, client cutover loop, a dedicated
> silent-data-loss hunt, cursor/subscribe, seq seeding, tombstone/compaction,
> live decode/filter, test quality, docs/comments, lexicon/wire/overflow). Every
> finding below was **adversarially re-verified** (a second agent tried to refute
> it against the shipped code) and cross-checked by hand before landing here.

## Clean dimensions (verified, no action)

These came back empty after a thorough read + an independent refute pass — recorded
so we know they were actually examined, not skipped:

- **Sentinel index (§R4-revised)** — seal/rewrite both index marker blocks via the
  shared `indexEventCollection`; `collectionIDsForSegment` always admits sentinels;
  the per-block DID bloom still narrows; sentinels are invalid NSIDs. No drift, no
  exclusion path, reactivation folds correctly. *The load-bearing prime-directive
  mechanism is sound.*
- **Cutover data-loss hunt** — the dedicated "construct a lost event" pass found no
  silent-loss path in the sealed→live seam: cold replay (`WalkFromCursor`) covers
  `(S, activeTip]`, the cold→live cursor is monotone (no atomic-snapshot dependency),
  the dedup floor `S` is correct, pinned `beforeSeq=S` leaves mid-backfill seals to
  cold replay. (One real loss path was found by the *live-decode* lens instead — F1.)
- **Tombstone / compaction** — `bytes` accounting balances across Observe/Evict/
  Replace; rewrite→save-watermark→evict ordering preserved; on-disk windowed fold is
  correct vs the in-memory global-max set; reactivation handling correct for
  compaction-only use.
- **Lexicon / wire / overflow** — `sealedTipSeq` required+populated, int64↔uint64
  guards present on both ends, no dangling `getTombstones`/`PlanTooLarge`/
  `didTombstones` references.

## Severity legend

- **HIGH** — correctness/prime-directive impact reachable through a documented API.
- **MEDIUM** — operational/observability defect or a latent correctness trap.
- **LOW** — stale/misleading docs or comments that could lead a maintainer to
  reintroduce a bug (zero-tech-debt policy still wants these fixed).

---

## Findings

### ☑ F1 — [HIGH] `WithBeforeSeq` without `WithBackfillOnly` silently drops the entire live tail — **FIXED**

- **File:** `internal/client/filter.go:143` (`Matcher.wantsSeq`), wiring in
  `internal/client/engine.go` (`sweepSealedArchive` pins `beforeSeq` only on the
  per-page request, not on `e.matcher`) + `client.go:260` (`validateConfig`).
- **Defect:** `Subscribe(host, WithBeforeSeq(N))` with no `WithBackfillOnly` is
  accepted and dispatches to `runBackfillThenLive`. The engine matcher carries
  `hasBeforeSeq/beforeSeq=N`, and the live cutover tail runs every live event through
  `e.matcher.Wants → wantsSeq`, which drops every event with `seq > N`. The server
  keeps delivering them; the client receives and discards them. After the brief
  `(S, N]` window the live tail runs forever delivering nothing — **silent loss of
  in-scope, server-serveable data** (prime-directive violation). The documented
  contract (`client.go:104-106`) promises this backfill-then-live path works.
- **Why untested:** every in-repo `WithBeforeSeq` use pairs it with
  `WithBackfillOnly`, so the broken then-live path has no coverage.
- **Decision:** `beforeSeq` is by design an *archive/backfill* upper bound; it must
  not gate the live tail. Two viable fixes: (a) reject the combination loudly in
  `validateConfig` (crash-over-silent-loss), or (b) clear the matcher's `beforeSeq`
  bound when entering the live phase (mirroring `setAfterSeq` at cutover).
  **Chosen: (a) reject in `validateConfig`** — it is the least surprising (a
  `beforeSeq` consumer that *also* wanted to keep live-tailing past `beforeSeq` is a
  contradiction in terms; the honest intent is `WithBackfillOnly`), preserves the
  meaning of `beforeSeq` everywhere, and follows CLAUDE.md (crash > silent loss).
  Add a regression test asserting the combination errors.
- **DONE:** `validateConfig` (`client.go`) now rejects `hasBeforeSeq && !backfillOnly`;
  `WithBeforeSeq` doc + the CLI `--before-seq` usage/early-check updated; `cmd/client`
  rejects `--before-seq` without `--backfill-only`. `TestSubscribeValidation` flipped to
  assert the combination errors and that `+WithBackfillOnly()` is accepted.

### ☑ F2 — [MEDIUM] too-old detection rides an untested cross-package error-string substring — **FIXED**

- **File:** `internal/client/live.go:314` (`dialWebsocket`) vs
  `internal/subscribe/cursor.go:104` (`ErrCursorTooOld`).
- **Defect:** the client recognizes the server's pre-upgrade 400 *only* via
  `strings.Contains(body, "cursor too old")`, a literal duplicated across the
  package boundary with no shared constant and no end-to-end test crossing the seam.
  If a maintainer edits `ErrCursorTooOld`'s message, all server-side and engine
  (fake-dialer) tests still pass, but in production the client no longer maps the 400
  to `errLiveCursorTooOld`: `liveConsumer.Run` treats it as a transient dial error and
  **reconnect-loops forever** against a cursor the server keeps rejecting — a wedged
  consumer that silently stops making progress.
- **Decision:** export a single shared marker the server message is built from and the
  client matches on, so drift is impossible by construction; add an httptest-backed
  client test that returns the real 400 body and asserts the mapping (so future drift
  fails CI). `internal/subscribe` does not import `internal/client`, so a client test
  may import `internal/subscribe` for the shared constant.
- **DONE:** added `subscribe.CursorTooOldMarker` (the server `ErrCursorTooOld` message is
  now built from it) and a mirrored client `cursorTooOldMarker` const that `dialWebsocket`
  matches on. New `TestDialWebsocketMatchesServerTooOld` (live_subscribe_contract_test.go)
  asserts the two literals are equal, that the real server message contains the client
  marker, and drives a real httptest 400 body through the production `dialWebsocket` →
  `errLiveCursorTooOld` (plus a negative case: an unrelated 400 is NOT misread). Verified it
  fails CI when the server marker is drifted. Production `internal/client` still imports only
  `api/jetstream`+`segment` (the contract test is the only place that imports subscribe).

### ☑ F3 — [MEDIUM] server I/O fault during timestamp-cursor translation is mis-mapped to HTTP 400 — **FIXED**

- **File:** `internal/subscribe/handler.go:185` + `internal/subscribe/cursor.go:167`.
- **Defect:** any non-`ErrCursorTooOld` error from `ResolveCursor` is returned as
  HTTP 400 with `err.Error()` in the body. But `translateTimeUSToSeq` can fail on
  server-side faults (`segment.Open` / `DecodeBlock` / `BlockIndex`) — 5xx-class. A
  valid in-window timestamp cursor hitting a corrupt/transiently-unreadable sealed
  segment yields `"subscribe: translate cursor: open seg 7: ..."` returned as a 400.
  Effects: (1) client treats a transient server fault as a permanent bad request and
  won't retry; (2) operators watching 5xx never see the disk fault (broken alerting);
  (3) internal segment path/index leaked to the client; (4) no `cursorRequests` metric
  label increments for this class (handler returns before the metric block).
- **Decision:** distinguish input faults from translation/IO faults. Wrap the
  translation IO errors in a typed sentinel (e.g. `ErrCursorResolveFailed`) → map to
  HTTP 500 (or 503) with a generic body, and count under a distinct metric label;
  keep `ErrInvalidCursor`/`ErrCursorTooOld` → 400. Don't echo internal paths.
- **DONE:** added `subscribe.ErrCursorResolveFailed`; `ResolveCursor` wraps the
  `translateTimeUSToSeq` IO error with it. The handler maps it to HTTP 503 with a generic
  body (no segment-path leak), logs the wrapped detail server-side, and counts it under a
  new `resolve_failed` metric label (Help updated). New
  `TestResolveCursor_TranslateIOFaultIsResolveFailed` asserts the classification.

### ☑ F4 — [LOW] cross-block seq-monotonicity invariant is relied on by the planner but never validated at load — **FIXED**

- **File:** `internal/manifest/plan.go:142` (the gap-free reasoning) /
  `segment/reader.go` `validateBlockOffsets`.
- **Defect:** the planner's continuation cursor (`lastUnitMaxSeq = seg.Blocks[...].MaxSeq`,
  next page's exclusive `afterSeq`) is gap-free *only* if blocks within a segment are
  seq-disjoint and index-monotonic (`block[i].MaxSeq < block[i+1].MinSeq`). Nothing
  validates this on load — `validateBlockOffsets` checks offset ordering and
  per-block `MaxSeq>=MinSeq` only. On the trusted single-writer path the invariant
  holds today, so this is **latent, not actively triggered**; but a future ingest
  change, a hostile/corrupted imported segment, or a compaction bug would pass
  validation and then drive the planner to silently skip a block — silent loss.
- **Decision:** add a defensive check in `validateBlockOffsets` that consecutive
  *non-empty* blocks satisfy `blocks[i].MaxSeq < blocks[i+1].MinSeq`, failing loudly
  with `ErrInvalidBlockIndex` (crash > corruption). Must exclude `EventCount==0`
  blocks (compaction-to-empty rewrites retain stale seq bounds). This converts the
  load-bearing-but-unchecked planner assumption into an enforced on-load invariant.
- **DONE:** `validateBlockOffsets` (`segment/reader.go`) now rejects a later non-empty block
  with `MinSeq <= prior non-empty block MaxSeq` via `ErrInvalidBlockIndex`, excluding
  `EventCount==0` (compacted-to-empty) blocks. New `TestValidateBlockOffsetsCrossBlockSeqMonotonicity`
  covers ascending-ok, out-of-order-rejected, later-fully-below-rejected, and empty-stale-skipped.
  Full `./segment ./internal/ingest/... ./internal/manifest` suites still pass (no legitimate
  segment violates it).

### ☑ F5 — [LOW] stale comment + dead `[]byte` plumbing: live `emit` still claims to feed the deleted cutover buffer — **FIXED**

- **File:** `internal/client/live.go:233` (+ the `data []byte` param threaded through
  `emit` and bound to `_` at both call sites: `engine.go:299`, `engine.go:694`).
- **Defect:** the comment says the raw frame is passed "so the cutover buffer can
  persist verbatim bytes (re-decoded on replay)", but refactor (B) deleted the
  client-side cutover buffer; no consumer uses the bytes. The comment invites
  reintroducing the buffered-replay path the bufferless redesign deliberately removed.
- **Decision:** drop the dead `[]byte` from the `emit` signature (and the
  `evCopy := ev` raw-frame plumbing) since nothing consumes it, and remove/rewrite the
  comment. Update the two production call sites + the two test call sites.
- **DONE:** `emit` is now `func(*Event, error) bool`; the raw-frame `[]byte` and the
  cutover-buffer comment are gone. Updated both production call sites (engine.go) and the
  three test call sites (live_test.go). (`session` still reads the frame bytes locally for
  decode; only the dead emit arg was removed.)

### ☑ F6 — [LOW] stale test doc: `TestLiveConsumerSubscribeURLCursorZero` describes the deleted "rewind margin" — **FIXED**

- **File:** `internal/client/live_test.go:286`.
- **Defect:** the comment frames `cursor=0` as a "rewind start" landing "below the
  rewind margin" that would otherwise drop "(plannedThroughSeq, tip]" — all
  buffered-cutover concepts removed by refactor (B). The assertion is still correct;
  only the explanatory model is obsolete and contradicts `engine.go:679` ("No rewind
  margin is needed").
- **Decision:** rewrite the comment to the surviving `cursor=0` path: an empty-archive
  (`cutover==0`) connect must send `cursor=0` to replay from the first event, distinct
  from the `WithLiveCursor(0)` from-tip contract. Drop the rewind-margin language.
- **DONE:** comment rewritten to the empty-archive (`cutover==0`) framing; rewind-margin /
  `(plannedThroughSeq, tip]` language removed. Assertion unchanged (still passes).

### ☑ F7 — [LOW] `ScanMaxSeq` docstring still claims seq=0 is a valid first-event value — **FIXED**

- **File:** `segment/scan.go:14`.
- **Defect:** the docstring asserts "seq=0 is a valid first-event value" and that the
  `found` bool disambiguates "max is 0" from "no events" — a 0-based assumption
  invalidated by §R8 (seqs start at 1; seq 0 is the reserved nothing-yet sentinel).
  Code is correct; the comment is load-bearing guidance for the crash-recovery floor
  and could lead a maintainer to relax the `nextSeq>=1` floor and reintroduce the
  seq-0 swallow.
- **Decision:** update the docstring to the 1-based invariant: first real event is
  seq 1, seq 0 is the nothing-yet sentinel; `found` now disambiguates an empty active
  segment from a real envelope and forward-correction still gates on `found=true`.
- **DONE:** docstring rewritten to the 1-based invariant (`segment/scan.go`).

### ☑ F8 — [MEDIUM] m022 mutation regressed KILLED→SURVIVED: live compaction data-loss path lost its enforced gate — **FIXED (code+test; baseline re-bank is a 1-command human follow-up)**

- **File:** `testing/mutation/mutants/m022_shoulddrop_did_seq_inverted.patch` (+
  `internal/tombstone/tombstone.go:157` `ShouldDrop`).
- **Defect:** m022 inverts the DID-tombstone seq comparison in `Snapshot.ShouldDrop`
  (a live steady-state compaction path: superseded rows survive, live rows get
  dropped — data loss). On `main` it was KILLED by the overlay-reconstruction oracle;
  this branch deleted `internal/overlay` (#177), and `CheckFoldConvergence` does not
  reproduce it, so the most data-loss-sensitive suppression path now ships green
  through the enforced mutation gate. RESULTS.md discloses this honestly and #184
  tracks it, but a real `ShouldDrop` regression currently passes CI.
- **Decision:** close #184 *now* rather than defer — add a direct unit test on
  `Snapshot.ShouldDrop` that asserts **both directions** across the DID-tombstone seq
  boundary (a row at `seq < tombstone.Seq` is dropped AND a row at `seq > tombstone.Seq`
  survives), which deterministically kills m022 without needing an oracle scenario.
  Then re-bank m022 KILLED in `baseline.json` and update RESULTS.md. (A unit test is
  the right granularity: ShouldDrop is a pure function; the killing assertion should
  not depend on a full lifecycle run.)

  > NOTE: re-running the full mutation campaign / regenerating `baseline.json` is a
  > heavy, environment-sensitive step. F8's code+test fix lands here; the
  > baseline-regen + RESULTS dated-section is called out explicitly as a follow-up
  > action the human runs (`just mutation-baseline`) so the gate flips to PASS.
- **DONE (revised approach after verification):** root cause was *harness wiring*, not a
  missing assertion — the existing `TestSnapshotShouldDropDIDChainsWithSpecificReason`
  already fails under m022, but NO campaign tier runs `./internal/tombstone` (every tier
  runs only the oracle + a couple of packages). Fix: (1) added a `tombstone` tier in
  `run.sh` that runs `./internal/tombstone`; (2) re-pointed m022's patch header to
  `tiers: tombstone` with an updated `expected-detection`; (3) strengthened the unit test
  with the explicit data-loss direction (a `seq > tombstone.Seq` reactivation row MUST
  survive) so both directions are asserted. Verified the tier kills m022 (exit 1) and
  passes clean. The gate treats the resulting SURVIVED→KILLED as a non-failing
  IMPROVEMENT; **human follow-up: `just mutation-baseline` to re-bank m022 KILLED.**
  RESULTS.md catalog line updated.

### ☑ F9 — [LOW] dangling design-doc reference in the load-bearing cursor-split comment — **FIXED**

- **File:** `internal/subscribe/cursor.go:28`.
- **Defect:** the comment justifying `CursorSeqMaxThreshold` (the forever-fixed wire
  constant) points to `docs/superpowers/specs/2026-05-28-jetstream-v1-cursor-design.md`,
  which does not exist. A maintainer can't re-check the §4.1 non-overlap argument
  before touching the constant.
- **Decision:** repoint to the surviving note
  (`specs/notes/2026-05-27-cursor-replay-design.md`) and/or inline the bounding
  argument so the rationale is reachable.
- **DONE:** comment in `cursor.go` now points at the surviving note + `docs/README.md` §5.1
  (the authoritative 1e15 sources) instead of the non-existent path.

### ☑ F10 — [LOW] cursor-replay note states the disambiguation floor as 1.5e15; shipped code/README use 1e15 — **FIXED**

- **File:** `specs/notes/2026-05-27-cursor-replay-design.md:53` (and the other 1.5e15
  occurrences).
- **Defect:** the note's table + safety argument use a `1.5e15` floor with a
  "predates atproto (~Jan 2017)" justification. The shipped constant
  (`cursor.go:29`) and README (§5.1) use `1e15` (= 2001-09-09 in unix-micros), so the
  *specific numeric justification* is false for the value that ships. Disambiguation
  is still correct in practice (today ~1.75e15 ≫ 1e15; v2 seq stays far below 1e15 for
  centuries), so this is doc drift, not a runtime bug — but a maintainer could "correct"
  the code up to 1.5e15 and silently break the wire contract.
- **Decision:** reconcile the note to the shipped 1e15 (restate the bound in terms of
  1e15) or add a supersession banner pointing at the README/code as authoritative.
- **DONE:** added a "SUPERSEDED VALUES (2026-06-30)" banner at the top of the note
  redirecting every `1.5e15` to the shipped `1e15`, restating the non-overlap bound at
  1e15 (order-of-magnitude timestamp/seq gap, not the 2017 anchor), and noting v2 now
  rejects a below-floor cursor with 400 (not the "no window cap" the note describes).
  Chose a banner over a line-by-line rewrite to avoid introducing a new inconsistency in a
  superseded reasoning-trail doc.

---

## Remediation order

1. F1 (HIGH, prime-directive) — first.
2. F2, F3, F4, F8 (correctness/operational/coverage traps).
3. F5, F6, F7, F9, F10 (stale docs/comments — zero-tech-debt sweep).

After each fix: targeted package tests. Final gate: `just lint`, full `go test ./...`,
`just test-long ./internal/oracle`, `just oracle`. F8's baseline regen
(`just mutation-baseline`) is the human-run follow-up.

## Outstanding human follow-up

- **F8 baseline re-bank — DONE (2026-06-30).** Ran the full campaign at `dba121e`
  (`just mutation-baseline` equivalent): **21 KILLED, 6 SURVIVED, zero
  STALE/BUILD-BROKEN**. Diffed the fresh result against the prior baseline — the ONLY
  disposition change is m022 SURVIVED→KILLED@tombstone (no catalog drift, no other
  regression; m029–m033 the F1–F4 changes touch all stayed KILLED). `baseline.json`
  regenerated and `go run ./testing/mutation/gate` reports `PASS — 27 mutants match
  baseline`. RESULTS.md catalog line + a dated section updated.
- **#183** (pre-existing, unrelated to this review): re-derive a dedicated mutant for the
  #100 above-watermark over-drop recorder (m025 was retired in #178). Left as-is.

## Progress log

- 2026-06-30: report created; all 10 findings verified.
- 2026-06-30: all 10 findings remediated (F1–F10). `go build ./...`, full `go test ./...`,
  `golangci-lint` on touched packages, `TestOracle_DefaultLifecycle`, and `-race` on
  `internal/client`+`internal/subscribe` all green. Only remaining item is the F8
  baseline re-bank (1-command human follow-up above); m022 is already killed by the new
  `tombstone` tier.
