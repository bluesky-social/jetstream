# Oracle failure diary — a live subscriber starts past an event appended while it connected

- **Date:** 2026-10-04
- **Commit (failure observed on):** `de648c7` (branch `jc/direct-wait-herd`, PR #359), reproduced on `eac87dd` (`main`)
- **Test:** `TestOracle_ObservedSeqIsNotReusedAfterSIGKILL`, in CI's race-detector job
- **Symptom:** `timed out waiting for child subscriber observation`; the child
  printed `failed to get reader: context deadline exceeded`, and the race
  detector flagged the parent reading the child's output buffer.
- **Classification:** a test race, and next to it a production bug in
  `internal/subscribe`. A client resuming with a cursor equal to the writer's
  next seq skipped every event committed while it connected.
- **Status:** FIXED

## Repro

```
go test -race -c -o /tmp/oracle.test ./internal/oracle
for i in $(seq 1 8); do
  /tmp/oracle.test -test.run '^TestOracle_ObservedSeqIsNotReusedAfterSIGKILL$' -test.count=25 &
done; wait
```

It failed 3 times in 200 on both `main` and the branch. Load is what makes
it fail, so the parallel runs matter.

## Analysis

- A goroutine dump taken in the child when its read timed out showed the
  subscriber in `Tail.ReadFrom(…, 2, …)`, with `NextSeq` 2. The child had
  appended seq 1, and the subscriber had started after it.
- `websocket.Dial` returns when the upgrade response arrives. The handler
  then registers the connection and only then reads the live tip as its
  start. The child appended right after `Dial`, so under load the append
  landed first.
- For a subscriber with no cursor that is correct: live means from when the
  stream starts. The same code ran for a seq cursor equal to `NextSeq`,
  though. `ResolveCursor` resolves the cursor against `NextSeq` before the
  upgrade and turns it into a live plan, and the handler then started at the
  tip. Events committed between the two were never sent, although
  `docs/README.md` promises every event with seq >= the cursor.
- The data race was the parent formatting the child's output for the
  failure message while the child was still writing to it.

## Fix

- `ResolveCursor` gives a cursor equal to `NextSeq` a `StartSeq`, and the
  handler starts a live plan at the tip only when it has none.
  `TestHandler_NextSeqCursorKeepsEventsCommittedDuringConnect` appends an
  event inside that window, through a SeqSource that appends when the
  handler reads `NextSeq`. It fails on the old code for v1 and v2: the event
  never arrives.
- The oracle child waits for the subscriber gauge to show its stream started
  before it appends. Its server runs without lookback, so it cannot use a
  cursor. The parent kills and waits for the child before it reads the
  child's output.
- Verification: 600 runs of the repro loop passed after the fix.
