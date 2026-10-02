# Oracle failure diary — seeded scheduler wedged by a mutex held across a meta read

- **Date:** 2026-09-25
- **Commit (failure observed on):** `92ed03d` plus the uncommitted S2.18 work (branch `jc/new-storage`)
- **Test:** `TestDisagg_OracleChild` (layer 3, full mode, seed 8548589797312567258, found by a 400-seed random hunt)
- **Symptom:** the child ran until its timeout with no output after `wave 6 (crash:after-seal-footer-upload-before-commit) fired`. About 1 in 400 random seeds.
- **Classification:** harness limitation (the D4 scheduler under synctest), not a production bug.
- **Status:** FIXED

## Repro

```
JETSTREAM_ORACLE_DISAGG_CHILD=1 JETSTREAM_ORACLE_DISAGG_SEED=8548589797312567258 \
JETSTREAM_ORACLE_DISAGG_MODE=full timeout -s QUIT 60 go test ./internal/oracle \
  -run '^TestDisagg_OracleChild$' -count=1 -v
```

`timeout -s QUIT` gets a goroutine dump.

## Analysis

- In the dump, every goroutine in the bubble is durably blocked except one,
  an atmos verifier resync worker in `Verifier.lockDID`, waiting on a
  `sync.Mutex`.
- The holder was `Verifier.verifyCommitLocked`, parked in `Seeded.Yield` on
  `meta/get`: a `syncstate.LoadChain` cache miss reads through the session
  metastore into `storagefake`.
- `Seeded.Run` admits the next call only after `synctest.Wait` returns.
  synctest does not count a `sync.Mutex` wait as durably blocked, so Wait
  never returned. Fake time could not advance either, so the harness's
  fake-time converge deadline never fired.

## Root cause

The seeded scheduler requires that no goroutine hold a mutex across a
storage call. The harness follows that rule, and so does Jetstream's own
code. Third-party code does not: the atmos verifier holds a per-DID lock
across its StateStore loads.

## Fix

`storagefake`'s meta reads (`metaStore.Get`, `NewIter`) still check that the
client is alive, but they no longer take a scheduler turn. Such a read holds
no catalog lock and reads one immutable published state, so its place in the
interleaving does not matter to it. Transactions, read transactions, lease
statements, and commits still yield.

## Verification

- The seed passed 5 of 5 fresh runs after the fix. A 1200-seed random hunt
  found no further hangs.

## Lesson

A yield point inside code that may run under someone else's mutex wedges the
whole bubble, and it looks like a hang rather than a failure. Hunt for these
with many random seeds, a short per-child timeout, and `-s QUIT`.
