# #204 — adversarial revs/rkeys/collections through the oracle (plan of record)

Status: active working note for issue #204. Companion to
`2026-07-03-oracle-testing-improvements.md` (campaign plan) and the #197 gate
(docs/README.md §4.4). Research evidence is in the 2026-07-04 issue comment;
this note pins the layer-ownership matrix, the simulator lie seams, and the
oracle reconciliation design so the implementation is driven by verified data.

## Verified layer-ownership matrix

Jetstream requires atmos's Sync-1.1 verifier (`live.Config.Verifier` is
required; `runtime.go` constructs it with default `PolicyResync`). The
verifier runs BEFORE `ConvertEvent`, so several DoD cases never reach the
#197 gate. Every claim below was verified against atmos v0.2.10 source
and/or the spike (see §Spike).

| Adversarial input | Live path owner | Observable | Backfill path owner | Observable |
|---|---|---|---|---|
| non-TID rev (#commit) | verifier `VerifyCommit` (`InvalidRevError`, pre-lock, policy-bypassing) | consumer "stream error" branch → `incDecodeErrors` + `verifier_failures` | atmos `repo/load.go` rejects non-empty invalid rev at CAR load → repo FAILS | failed-repo diagnostics |
| empty rev | verifier for #commit envelope (as above); the #sync-envelope case is gate-owned (see the garbage-#sync row) | — | GATE-OWNED: `repo/load.go` skips validation when rev is empty; jetstream `handler.go:105` ParseTID fails → repo FAILS + `{backfill,invalid_rev}` | shared counter |
| regressing rev (#commit) | verifier chain check → `RevRegressionError` → PolicyResync silently repairs (resync) | no gate counter; archive converges via `KindCreateResync` rows | n/a (backfill loads one head) | — |
| future rev >5m (#commit, #sync) | verifier `checkFutureRev` → `FutureRevError`, event rejected pre-gate | `verifier_failures` + decode-error yield | n/a — backfill has no future-rev check; a future-but-valid TID archives normally | none (documented) |
| garbage rev on #sync envelope | GATE-OWNED: reaches `convertSync`'s `validateRev` → `{live,invalid_rev}` whole-event drop (see note 3) | shared counter | n/a | — |
| invalid rkey/collection in op path (spec-invalid, MST-legal charset) | GATE-OWNED: verifier passes through unvalidated → `validateOpPath` per-op drop, `invalid_collection` / `invalid_rkey` reason, siblings survive (see note 4) | shared counter | `splitRecordPath` on the MST walk → per-record drop, siblings survive | shared counter |
| spec-valid rkey 256–512B | passes gate validation; `segment.ValidateEvent` → `{live, field_too_long}` | shared counter | same → `{backfill, field_too_long}` | shared counter |
| invalid UTF-8 in rkey | UNREACHABLE on live op.Path (wire CBOR text-string decode rejects invalid UTF-8 — spike-verified) | n/a | REACHABLE: MST node `KeySuffix` is a CBOR byte string; hostile getRepo CAR can carry it → `splitRecordPath` → `{backfill, invalid_rkey}` | shared counter |
| null byte / emoji / `.` / `..` / >512B rkey; `$`-prefix / empty / Unicode / 2-segment collection; no-slash MST key | wire + MST + inversion all pass (spike-verified) → gate-owned as above | shared counter | same | shared counter |

Notes:

1. The `.`/`..`/emoji/null classes are valid UTF-8 (or pass MST charset
   non-checking) and ride op.Path fine; only invalid-UTF-8 is wire-blocked.
2. `checkCommitFields` requires envelope.Rev == signed inner commit.Rev, so
   rev lies must be signed-in (the world signs adversarial revs; the
   simulator's key is authoritative for its DIDs).
3. Why the garbage #sync rev reaches the gate: `checkFutureRev` returns nil
   for unparseable revs, and `VerifySync` has no ParseTID gate. With no
   prior chain state the `syncEvt.Rev <= state.Rev` replay check is
   skipped and the resync runs; the resync ops carry `Rev: commit.Rev`
   from the FETCHED getRepo head (valid TID), but the KindSync tombstone
   row is built by `convertSync` from the envelope rev, where
   `validateRev(evt.Sync.Rev)` drops the whole event.
4. Why invalid op paths reach the gate: the verifier casts wire strings to
   typed fields without calling `.Validate()` (`buildOpsFromCommit`), and
   `checkOpCIDs` only requires MST consistency — an adversarial key
   present in the signed MST passes. `convertCommit`'s `validateOpPath`
   is the first spec check on the path.
5. Verifier-owned rev cases still MUST be exercised e2e: assert repair (rev
   regression → resync rows converge) or rejection (nothing archived, cursor
   advances, `verifier_failures` classified counter increments, server keeps
   running). They are part of the DoD under the amended "layered ownership"
   contract (Jim approved 2026-07-04).

## m013/m014 disposition (verified dead path)

`convertVerifiedOps` (`events.go:143`) handles the ConvertEvent default arm:
no public envelope. Under atmos v0.2.10 every verified-ops event carries an
envelope: `verify_worker.go` mutates the original event in place (Commit
stays set), and async resyncs are wrapped by `eventFromAsyncResync` with a
synthetic `Sync` envelope. The comment in events.go:53-64 documents the arm
as a bisect-tolerance fallback. Therefore NO simulator traffic can reach
m013/m014's mutated lines — they model bugs in dead code.

Disposition (Jim, 2026-07-04): retire m013/m014 (Adding Mutants convention:
"retire when code movement makes it stale or dead" — dead applies), document
in RESULTS.md, and add analogues against `convertSync`'s resync-op loop
(`events.go:376-383`, `Collection: string(op.Collection)` swap and
`Rev: string(op.Rev)` drop) which IS live under sync-divergence traffic
(existing `GenerateSilentMutationThenSyncForTest` flow). Also consider
removing `convertVerifiedOps`' unreachable op-loop in a separate issue if
the fallback is truly vestigial — NOT in this change (it is deliberate
bisect tolerance; keep).

## Simulator lie seams (spike-verified 2026-07-04)

Spike: `internal/simulator/world/spike204_test.go` (TEMPORARY, deleted after
plan approval; results recorded here). Proved with real MST + CAR + signed
commit structure + `sync.InvertCommit`:

1. `mst.Tree.Insert` performs NO key validation (`IsValidMstKey` exists,
   never called by Insert). A lie inserted here lives in a REAL signed MST:
   `checkOpCIDs`' `tree.Get(op.Path)` and inversion both succeed.
2. Wire `RepoOp.Path` is CBOR text — accepts any valid-UTF-8 string
   including null bytes, emoji, `.`, `..`, 600B keys, `$`-collections,
   empty collection, no-slash. Rejects invalid UTF-8 (backfill-only class).
3. `commitAndBroadcast` re-parses `newState.Rev` for the envelope `Time` —
   the adversarial generator must compute Time from the logical clock
   instead (new `commitAndBroadcastWithRev` variant or a rev-override).
4. getRepo CARs are built by `ExportRepoCAR` from persisted blocks — a lie
   in the MST automatically flows to backfill; invalid-UTF-8 keys can ONLY
   go this route.

Generator design (follows `GenerateRecordOpForTest` targeted-op precedent,
`specs/oracle.md` "Adding Simulator Behavior" — explicit modes, traced):

- `GenerateAdversarialOpForTest(ctx, idx, lie)` — commits a benign sibling
  op via `repo.Create` AND an adversarial key via raw `Tree.Insert` in the
  SAME commit, wire ops carrying both paths. Returns a ledger entry.
- `GenerateAdversarialRevForTest(ctx, idx, rev)` — targeted op committed
  via a rev-override variant of commitAndPersist (empty/garbage/regressing/
  future), Time from logical clock.
- `GenerateAdversarialSyncForTest(ctx, idx, rev)` — #sync frame with a
  garbage/empty envelope rev (gate-owned live invalid_rev case).
- Backfill lies ride bootstrap: adversarial keys inserted into an account's
  MST pre-bootstrap (before jetstream's listRepos/getRepo pass) so the
  normal backfill walk hits `splitRecordPath`.
- All adversarial generators record into the world's adversarial ledger and
  a trace kind (`trace.go`), and are used ONLY by oracle tests — default
  RunTraffic stays polite. No new world.Config knob needed (test-targeted
  methods, not a mix mode).

## Oracle reconciliation — adversarial ledger

New: world-side ledger (guarded by mutationMu), entries
`{Source live|backfill, Seq, DID, Collection, Rkey, Reason, Layer gate|verifier, WholeEvent bool}`.

Consumers:
1. `ExpectedEventLogFromFirehose` — drop ledger-matched rows (whole-event
   entries drop every row of that seq; per-op entries drop the one row).
2. `GroundTruthFromWorld` — skip ledger-matched (did, coll, rkey) records
   (the world MST contains the lie; jetstream rightly never archives it).
   One-directional-safe: if the gate FAILS to drop, the archived record
   fails Compare as an extra, and the counter assertion fails too.
3. `assertNoPermanentCursorGap` — whole-event-dropped seqs are exempted
   from the observed-cursor requirement but MUST still be ≤ the persisted
   relay cursor (cursor advanced past them — that's the contract).
4. Drop-counter assertion: scrape `/metrics` via `runtimeDebugLn.httpClient()`
   (promhttp already wired, never queried before) or expose DropMetrics via
   a Build hook; assert per-(source,reason) counts ≥ ledger counts per mode
   (≥ not ==: swarm faults may add missing_block drops independently).
   Anti-vacuity: every mode's ledger non-empty, every reason label hit.
5. Survivors contract: each adversarial commit carries a benign sibling —
   assert the sibling row IS archived (exact eventlog match already does
   this once expected-side filtering is ledger-aware).

## Work plan (red-first per gate)

1. World: adversarial ledger + generators + trace kinds; unit tests pinning
   frame bytes decode + verifier acceptance (verifier-consistency is the
   fragile part).
2. Oracle: ledger-aware expected-eventlog/groundtruth/cursor-gap filtering;
   metrics scrape helper; adversarial phase in TestOracle_DefaultLifecycle
   injecting ≥1 of each mode (deterministic, not seed-luck); layered
   assertions per the matrix.
3. Red-first proof: neuter each gate arm (validateOpPath → nil, etc.) and
   confirm the new oracle phase goes red for the right reason.
4. Mutants: retire m013/m014 (+RESULTS.md); add convertSync analogues +
   validation-skipped + wrong-severity + backfill validRecordPath-skip;
   full campaign; re-bank baseline (`just mutation-baseline`).
5. Docs: specs/oracle.md simulator-fidelity tier entry; this note updated;
   issue comment + PR.

## Coordination

#205 (relay seq dup/regress) is in flight in a parallel session and touches
harness_test.go + the relay handler — rebase expected; keep this branch's
harness edits additive (new phase + helpers, no reflow of existing phases).
