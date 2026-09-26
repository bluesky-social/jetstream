# Oracle failure diary — a pipelined save hides an earlier event's chain state

- **Date:** 2026-09-26
- **Commit (failure observed on):** `ae83abb` plus the uncommitted S3.5 lifecycle harness (branch `jc/new-storage`)
- **Test:** `TestDisagg_OracleChild` (layer 3 disaggregated oracle, lifecycle harness), about 4% of full and short runs over seeds 100-199 under parallel load
- **Symptom:** a steady-state wave with `crash:after-hot-batch-commit-before-ack` or `commit_lost` failed in one of two ways. In the first, a DID's stream held a `KindSync` row and a full set of resync creates that the model did not expect (`event 28 … is not in the model; want seq=64 …`). In the second, one profile update was archived twice, at seqs 40 and 45, after `commit_lost` of hot batch [37,44].
- **Classification:** production bug in live ingest (`internal/ingest/syncstate`). It affects local mode too. It needs a crash between durable batches while the verifier has two events of one DID in flight.
- **Status:** FIXED

## Repro

```
cd internal/oracle
for s in $(seq 100 199); do for m in full short; do echo "$s $m"; done; done |
  xargs -P 16 -n 2 sh -c 'JETSTREAM_ORACLE_DISAGG_CHILD=1 JETSTREAM_ORACLE_DISAGG_SEED=$0 \
    JETSTREAM_ORACLE_DISAGG_MODE=$1 go test . -run "^TestDisagg_OracleChild$" -count=1 >/dev/null || echo "FAIL $0 $1"'
```

The failure depends on the interleaving, so no single seed reproduces it.
The deterministic unit repro is `TestStateStore_PipelinedSavesPromoteEach`
(`internal/ingest/syncstate`).

## Analysis

- Seed 135 short: the first steady-state session opened at relay cursor 23,
  archived upstream 25 for `did:plc:3b2z…` (rev `…2z22`) and part of
  upstream 27 (rev `…3322`), and was killed at
  `after-hot-batch-commit-before-ack` after a hot batch committed relay
  cursor 26. The successor opened at cursor 26. Its
  verifier logged `chain break … seen (rev=3ke6kg3wk2x22 …), got
  (rev=3ke6kg3wk3322 …)`, so the durable chain state was still the bootstrap's
  rev `…2x22`, although the rows and the cursor for `…2z22` were durable.
- Promotion itself ran at the right point (in `OnAppend` on the event's last
  row, under the writer mutex; see the 2026-09-25 batch-boundary entry). But
  `StateStore` kept one pending entry per DID. atmos verifies ahead of the
  appends, so it had already called `SaveChain` for upstream 27 (`…3322`)
  when upstream 25's last row landed. `PromoteChain(did, "…2z22")` found a
  pending rev newer than its own, left it for its own event, and promoted
  nothing. `…2z22`'s state was gone. It was overwritten, not queued.
- The batch therefore committed upstream 25's rows and a cursor past it with
  the DID's state two revs back. There are two outcomes after a crash. The
  successor either chain-breaks on the next commit into a needless resync
  (seed 135), or it is redelivered the event itself because a pending resync
  on another DID held the relay watermark back (seed 105 resumed at cursor 30,
  behind rows already committed in batch [37,44]). The redelivered commit then
  verifies against the older state and is archived a second time.

## Root cause

A DID with several verified events in flight had only one pending slot. A
later event's save hid an earlier event's state, so the earlier event's
promotion could not make its own state durable.

## Fix

Each DID now has a queue of pending entries, oldest first
(`pending[K]`, `savePending`/`takePending` in `internal/ingest/syncstate/store.go`).
Promotion takes the newest entry at or below the appended event's rev (for
hosting, its account seq) and drops the older entries, which it supersedes.
Loads still read the newest entry. A save at or below a queued key replaces
the entries from that key up. The queue is capped at 64 entries per DID. Only
events that verify but never append can reach the cap, and dropping the
oldest entry then costs a resync, not a lost or duplicated event.

## Verification

- `TestStateStore_PipelinedSavesPromoteEach` fails on the one-slot store and
  passes with the queue.
- Before the fix, 19 of the 200 seed/mode pairs over seeds 100-199 failed at
  least once across the sweeps (about 4% of runs). With the fix: 400 of 400
  non-race runs (two passes), 200 of 200 race
  runs, `just oracle-disagg`, `just oracle-disagg-sweep 20 race` and
  `just test-long ./internal/oracle` pass.

## Lesson

The earlier fix (2026-09-25 batch-boundary entry) moved promotion to the right
moment but kept one pending slot per DID, which is only correct if the
verifier never runs ahead of the appends. It does. The S2 harness never hit the
overwrite. The S3.5 lifecycle harness does, because it kills the leader during
steady-state traffic right after the bootstrap handoff. Replay guards need one
pending entry per verified event, not one per DID.
