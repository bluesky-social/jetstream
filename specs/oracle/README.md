# Oracle test failure diary

A running log of oracle simulator test failures, one markdown file per
incident. Each entry records, for a single failure:

- the commit hash the failure was observed on,
- the exact repro command,
- the analysis (how the diagnosis was reached),
- the root cause (contributing factors), and
- the fix and its verification.

The goal is institutional memory: a future flake at a similar seam should be
diagnosable by reading past entries, and each fix's red-first evidence is on
record.

## Naming

`YYYY-MM-DD-short-slug.md` — date the failure was investigated, plus a slug
naming the failure mode (not the test).

## Entries

- [2026-06-27 — restart-chain cutover delivery race](2026-06-27-restart-chain-cutover-delivery-race.md):
  durable-intermediate chain frames lost when cutover cancelled the
  bootstrap-live consumer before the tail was archived; fixed with a
  cross-process cutover delivery gate.
- [2026-06-28 — boundary-truncated getRepo CAR misclassified as permanent](2026-06-28-boundary-truncated-car-misclassified-permanent.md):
  a getRepo CAR truncated exactly on a block boundary loaded cleanly but
  incomplete, so a missing interior MST node failed backfill non-transiently
  (no retry) and a record went missing; fixed in atmos with a repo
  completeness check (`CheckComplete` / `LoadCompleteFromCAR`) that classifies
  the truncation as transient, and removed the jetstream handler bandaid.
- [2026-07-05 — retry appends bypass the /subscribe tail](2026-07-05-retry-appends-bypass-subscribe-tail.md):
  failed-repo/net-new retry rows consumed shared-writer seqs but never fed
  the hot ring (consumer-scoped OnEvent vs writer-scoped allocator), punching
  a seq hole that served wrong events to replaying clients and could panic
  while holding the tail mutex, wedging ingestion; fixed with a writer-level
  ordered event sink feeding the tail from the shared append path, plus
  ring reset-on-gap hardening (#244).
- [2026-09-25 — adversarial drop-counter scrape races a reconnect](2026-09-25-adversarial-drop-scrape-races-reconnect.md):
  a scheduled subscribeRepos disconnect put the consumer in a 1ms reconnect
  backoff just before the whole-event `#sync` lie. `synctest.Wait` counts a
  timer-sleeping goroutine as quiescent, so the counters were scraped before
  the lie was dropped. Fixed by polling the counters under fake time up to a
  deadline.
- [2026-09-25 — verifier sync state crosses a durable batch boundary](2026-09-25-disagg-syncstate-batch-boundary.md):
  the layer 3 oracle's first production bug. Chain state was staged when a
  batch committed, not when it was cut, so a replay after a crash was
  dropped and a row was lost. Promotion also ran after `Append`, outside the
  writer mutex, so a whole event could be archived twice. Fixed with a
  snapshot taken at cut time, and promotion in `OnAppend` on the event's last
  row.
- [2026-09-25 — seeded scheduler wedged by a mutex held across a meta read](2026-09-25-disagg-scheduler-wedged-by-verifier-mutex.md):
  the atmos verifier holds a per-DID `sync.Mutex` across syncstate loads. A
  load parked in `storagefake.Seeded` kept `synctest.Wait` from returning,
  and the child hung. Fixed by making the fake's meta reads skip the
  scheduler.
- [2026-09-26 — a pipelined save hides an earlier event's chain state](2026-09-26-disagg-pipelined-chain-state-hidden.md):
  found by the S3.5 lifecycle harness. The atmos verifier saves a DID's
  state for a later event before the earlier event's rows are appended, and
  the syncstate store kept one pending entry per DID, so the earlier
  event's promotion made nothing durable. After a crash the successor
  resynced needlessly or archived an event twice. Fixed with an ordered
  pending queue per DID.
