# Oracle failure diary — boundary-truncated getRepo CAR misclassified as permanent

- **Date:** 2026-06-28
- **Commit (failure observed on):** `c56082e` (main)
- **Test:** `TestOracle_DefaultLifecycle` (stress mode, swarm faults)
- **Symptom:**
  - Backfill WARN: `repo failed component=backfill/run did=did:plc:6zlhhxbddahufaoaetav6tsv err="mst: loading node bafyreibp2nsyyjp5fi3jjee7kemwqxqglvhy2vhduamu3xdlrsak4chxky: block not found: bafyreibp2nsyyjp5fi3jjee7kemwqxqglvhy2vhduamu3xdlrsak4chxky"`
  - Then after-bootstrap `Compare` fails: `oracle: missing did:plc:6zlhhxbddahufaoaetav6tsv app.bsky.actor.profile/22bb3uhadb2kf rev=`
- **Classification:** seed-deterministic (NOT wall-clock flaky). The
  `-count` bubble guard (`harness_test.go:40`) runs the test body exactly once
  per process, so each separate `go test` invocation reproduces the identical
  DID, missing MST node CID, and missing record. (The trace-artifact sha256
  differs between runs only because concurrent trace-row ordering is
  nondeterministic; the failure outcome is fixed by the seed.)
- **Status:** FIXED
- **Tracking issue:** https://github.com/bluesky-social/jetstream/issues/168
- **Original CI run:** https://github.com/bluesky-social/jetstream/actions/runs/28302460522/job/83853056251

## Repro

```
JETSTREAM_ORACLE_MODE=stress \
  JETSTREAM_ORACLE_SEED=9699712896376246338 \
  GOMAXPROCS=2 go test ./internal/oracle -run TestOracle_DefaultLifecycle \
    -count=1 -timeout 30m -v
```

`-count=N>1` does not help — the bubble guard skips every iteration after the
first in a process. Re-running as separate processes reproduces 100% pre-fix.

## Analysis

The decisive observations, gathered by instrumenting the simulator getRepo
handler and sweeping every truncation offset of the failing repo's CAR:

1. At this seed the fault planner (`internal/oracle/faults.go`) selects
   **account 8** (`did:plc:6zlhhxbddahufaoaetav6tsv`) as the *hot* DID:
   **2× getRepo HTTP 503 + 1× getRepo CAR truncation**. The truncation cuts
   the served CAR at `len/2` (`internal/simulator/http/pds.go`).

2. The getRepo attempt order for that DID was: `503`, `503`, **truncation
   (full=2974 bytes, served=1487)**. By the swarm's design
   (`CheckWithinRetryBudget`), 3 retry-consuming faults leave exactly one clean
   4th attempt — so the repo is *supposed* to recover in-loop during bootstrap.

3. **The truncation landed exactly on a CAR block boundary.** A CAR v1 stream
   is self-framing with no total length/block count; the reader detects the end
   of blocks by a *clean* `io.EOF` at a block-length varint
   (`atmos/car/car.go` `Reader.Next`). A cut on a block boundary is therefore
   indistinguishable, at the framing layer, from a complete (smaller) CAR:
   `repo.LoadFromCAR` reads every delivered block, sees a clean EOF, and
   **returns success with a partial block set**. A boundary sweep over the
   1968 prefixes of the clean CAR found **8** such offsets; `len/2` for the
   live-traffic-grown repo was one of them.

4. The missing interior MST node only surfaces later, in
   `SegmentHandler.handleRepo` at `r.Tree.Walk` (`handler.go`), as
   `mst: loading node ...: block not found`.

5. **The bug — an asymmetry in error classification.** The handler already
   *deliberately* wrapped a missing **leaf record** block in
   `io.ErrUnexpectedEOF` (so `xrpc.IsTransient` retries it). But a missing
   **interior MST node** bubbles up raw from `Tree.Walk`, is not transient, so
   `processRepo` (`atmos/backfill/engine.go`) fails the DID immediately —
   *before* the reserved clean 4th attempt runs. The DID is parked
   `StateFailed`; the after-bootstrap barrier releases; the profile record is
   absent on disk while present in ground truth → `Compare` fails.

### Contributing factors

- **Root gap (atmos):** `LoadFromCAR` enforced no completeness invariant on a
  full-repo download, even though the producer side (`repo/export.go`) already
  refuses to *emit* a CAR omitting a referenced record. The read side lacked
  the reciprocal check, so a boundary-aligned truncation passed silently.
- **Bandaid (jetstream):** `SegmentHandler` compensated by re-tagging a missing
  block as `io.ErrUnexpectedEOF`, but only for **leaf record** blocks — the
  interior-MST-node path was never covered.
- **Same gap at the root block:** a truncation severing the body before the
  declared root (commit) block arrived also reached `LoadFromCAR`'s
  `commit block not found` as a non-transient error.

### Production impact (not just a test artifact)

A real boundary-aligned truncated `getRepo` download (connection reset / proxy
timeout / PDS crash mid-stream landing on a block boundary) was classified
**permanent**. The repo was parked `StateFailed` and only retried by
`RunFailedRepoRetry`, whose default interval is **4 hours**
(`DefaultFailedRepoRetryInterval`, `internal/ingest/backfill/retry.go`) and
which only runs in steady-state. During that window Jetstream serves an
**incomplete archive** for that repo. Recoverable, not corruption — but an
availability/latency defect at scale (≈0.4% of arbitrary truncation offsets are
block-boundary, per the sweep).

## Root cause

A CAR truncated exactly on a block boundary is a *complete-looking but partial*
stream. atmos's full-repo loader did not verify completeness, so the missing
block surfaced downstream as a non-transient `block not found` and permanently
failed an otherwise-recoverable repo, instead of being classified as the
transient download truncation it is and retried.

## Fix (root in atmos; jetstream bandaid removed)

**atmos** (`github.com/jcalabro/atmos`):

- `mst.ErrBlockNotFound` — a matchable sentinel; `MemBlockStore.GetBlock` wraps
  it (was a bare `fmt.Errorf` string).
- `repo.(*Repo).CheckComplete()` — walks the MST from the data root, confirming
  every interior node (via `Tree.Walk` → `ensureLoaded`) and every leaf record
  block (explicit `GetBlock`) is present. A missing block is wrapped in BOTH
  `mst.ErrBlockNotFound` (matchable cause) and `io.ErrUnexpectedEOF` (so
  `xrpc.IsTransient` treats it as the truncation it is, symmetric with a
  mid-block CAR truncation `errVarintTruncated`).
- `repo.LoadCompleteFromCAR()` — `LoadFromCAR` + `CheckComplete`, for full-repo
  callers. `LoadFromCAR` stays **permissive** (diff-safe: a `since` diff CAR
  legitimately omits blocks; no non-test caller passes `since` today, but the
  contract is preserved).
- `repo.LoadFromCAR` now also wraps the `commit block not found` (missing
  declared root) case in `io.ErrUnexpectedEOF`.
- `backfill/engine.go` `download()` calls `rp.CheckComplete()` right after
  `LoadFromCAR`, through the existing `translate()` so the retry loop re-fetches
  and uses the reserved clean attempt.

**jetstream:**

- `internal/ingest/backfill/retry.go` and `selected.go` use
  `repo.LoadCompleteFromCAR` instead of `LoadFromCAR` (both are full-repo
  downloads).
- **Deleted the bandaid** in `internal/ingest/backfill/handler.go`: both
  `handleRepo` and `validateRepoMaterializations` no longer re-tag a missing
  block as `io.ErrUnexpectedEOF` — completeness is now guaranteed upstream
  before the handler runs.
- `internal/repoexport/verify.go` intentionally still uses `LoadFromCAR`: it
  builds a single-commit-block CAR and reads only the commit, so a completeness
  walk would wrongly fail (no MST data).

### Regression tests (atmos)

`repo/complete_test.go`:
- `TestCheckComplete_FullRepoPasses` / `_EmptyRepo` — complete + trivial cases.
- `TestCheckComplete_MissingLeafRecordBlock` — missing leaf → transient +
  `mst.ErrBlockNotFound`.
- `TestCheckComplete_MissingInteriorMSTNode` — missing interior node → transient
  (the exact oracle case).
- `TestCheckComplete_BoundaryTruncationSweep` — exports a real repo, truncates
  at every prefix past the header, asserts every offset is either a clean
  complete prefix or transient-classifiable, and that ≥1 offset is a boundary
  cut that `LoadFromCAR` accepts but `CheckComplete` rejects.
- `TestLoadFromCAR_StaysPermissive` — pins the diff-safe contract.
- `TestMemBlockStore_ErrBlockNotFound` — sentinel is `errors.Is`-matchable.

`backfill/truncation_test.go`:
- `TestEngine_BoundaryTruncatedCAR_RetriesAndCompletes` — serve a
  boundary-truncated CAR twice then the full CAR; repo must reach
  `StateComplete` and the handler must only ever see a complete repo.
- `TestEngine_BoundaryTruncatedCAR_ExhaustsBudget` — always-truncated →
  `StateFailed` after the retry budget (loud failure, no infinite loop, handler
  never runs on a partial repo).

**Red-first verified:** with the engine's `CheckComplete()` call disabled,
`TestEngine_BoundaryTruncatedCAR_RetriesAndCompletes` fails with the repo in
`StateFailed` (the exact pre-fix bug); restoring it passes.

### Regression test (jetstream)

`internal/ingest/backfill/handler_test.go`:
`TestSegmentHandler_MissingDownloadedRecordBlockSurfacesError` (renamed from
`...IsTransient`) now pins the post-fix handler contract: a missing block
surfaces plainly and is matchable as `mst.ErrBlockNotFound` (fails loud), with
transient-classification owned upstream.

## Verification

- Failing seed `9699712896376246338` (stress): **PASS**, 5/5 separate
  processes.
- Stress sweep seeds `{1, 2, 42, 777, 123456789, 11212589348287832646,
  9699712896376246338}`: all PASS.
- atmos: full `go test ./...` green; `go vet`, `gofmt` clean.
- jetstream: `internal/ingest/...`, `internal/simulator/...`,
  `internal/oracle/...` green; `go build ./...`, `go vet`, `gofmt` clean.

## Files touched

atmos:
- `mst/mst.go` (`ErrBlockNotFound` sentinel + wrap)
- `repo/repo.go` (`CheckComplete`)
- `repo/load.go` (`LoadCompleteFromCAR`; root-block truncation classification)
- `backfill/engine.go` (`download()` completeness check)
- `repo/complete_test.go` (new), `backfill/truncation_test.go` (new)

jetstream:
- `internal/ingest/backfill/handler.go` (removed bandaid)
- `internal/ingest/backfill/retry.go`, `selected.go` (`LoadCompleteFromCAR`)
- `internal/ingest/backfill/handler_test.go` (test rewrite)
- `go.mod` / `go.sum` (bump `github.com/jcalabro/atmos` to `v0.2.10`, the
  release carrying the completeness fix; developed against a temporary local
  `replace` directive that was dropped once `v0.2.10` was published)
