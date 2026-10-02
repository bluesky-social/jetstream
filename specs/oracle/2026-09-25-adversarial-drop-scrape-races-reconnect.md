# Oracle failure diary — adversarial drop-counter scrape races a reconnect

- **Date:** 2026-09-25
- **Commit (failure observed on):** `2ea624a` (branch `jc/new-storage`). Also reproduces on `main` at `3fa54fd`.
- **Test:** `TestOracle_DefaultLifecycle` (stress mode, seed 17946718280370232974, found by a local `just oracle-sweep`)
- **Symptom:** `steady-state mode=stress seed=17946718280370232974: dropped_events_total{source=live,reason=invalid_rev} is 0, ledger requires ≥1 — a scheduled lie was not dropped by the gate`
- **Classification:** harness race, not a production bug. It is seed-dependent and interleaving-dependent.
- **Status:** FIXED

## Repro

The lifecycle tier runs one synctest bubble per process, so `-count=N`
repeats nothing. Only the first iteration runs; the rest skip. Loop over
fresh processes instead:

```
for i in $(seq 16); do
  JETSTREAM_ORACLE_MODE=stress \
  JETSTREAM_ORACLE_SEED=17946718280370232974 \
  GOMAXPROCS=2 go test ./internal/oracle -run 'TestOracle_DefaultLifecycle$' -count=1
done
```

Before the fix, 5 of 24 fresh runs failed (4 of 16 on `main`, 1 of 8 on
`2ea624a`).

## Analysis

- A first bisect with `-count=2` blamed a docs-only commit. `-count` does not
  repeat this tier, so every bisect step was a single run of a ~20% flake,
  and the result was noise. Failure rates on fresh processes showed that
  `main` fails too.
- Every failing log ends the same way. Seq 10016 is dropped twice (it was
  redelivered after a reconnect), then `reconnecting attempt=0 delay=1ms`, and
  nothing after that. The adversarial `#sync` lie is the window's last frame
  and was never processed.
- The stress plan's subscribeRepos disconnect schedule
  (`FaultPlan.SetSubscribeReposDisconnectSchedule`) can drop the connection
  just before the lie is delivered. The consumer then sleeps for its 1ms
  reconnect backoff.
- The lie is a whole-event drop, so the ack is exempted and `steadyAck.Wait`
  does not wait for it. The harness relied on `drain()` (`synctest.Wait`) to
  make sure the consumer had processed it. `synctest.Wait` counts a goroutine
  asleep on a timer as durably blocked, so it returned with the consumer in
  backoff. The scrape then ran before fake time advanced 1ms.
- Recording the scrape count confirmed it: after the fix, 3 of 12 runs
  needed a second scrape, and one 10ms fake-time poll was always enough.

## Root cause

A whole-event drop has no ack-visible row. `drain()` does not mean "the
consumer has processed every frame" when the consumer can be asleep in a
timer, such as reconnect backoff.

## Fix

`assertAdversarialDropCounters` polls: drain, scrape, and if any counter is
below its floor, sleep 10ms of fake time and try again, up to a 5s fake-time
deadline. A gate that never drops the lie still fails at the deadline with
the same message, and in fake time that costs almost nothing. The scrape
count is recorded in the `adversarial_drop_counters` trace event.

## Verification

- The seed passed 16 of 16 fresh-process runs with the fix. Without it, 5 of
  24 failed.
- `just test ./internal/oracle`, and `just mutation-gate` on a clean tree.

## Lesson

Any oracle wait that uses `drain()` as a stand-in for "the consumer has
processed frame X" can race a timer-sleeping consumer. Prefer a positive
signal, or poll the observable under fake time until it holds.
