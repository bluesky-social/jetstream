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
