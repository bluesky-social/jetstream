# Steady-State Failed Repo Retry

## Summary

Implement issue #127 as a steady-state-only background retry loop. Do not retry
during bootstrap/backfill, and do not run retry work during merge.

Treat `backfill_complete.log` as a design mistake. Failed and completed repo
state lives in Pebble under `repo/<did>`, and this work removes design and code
references to the nonexistent completion log.

Before implementation, update GitHub issue #127 with the corrected scope so the
public worklog matches the code we are actually building.

## Key Changes

- Add configurable steady-state retry options:
  - `--failed-repo-retry-interval` / `JETSTREAM_FAILED_REPO_RETRY_INTERVAL`, default `4h`, `0` disables.
  - `--failed-repo-retry-workers` / `JETSTREAM_FAILED_REPO_RETRY_WORKERS`, default `16`.
  - `--failed-repo-retry-host-workers` / `JETSTREAM_FAILED_REPO_RETRY_HOST_WORKERS`, default `4`.
  - `--failed-repo-retry-max-delay` / `JETSTREAM_FAILED_REPO_RETRY_MAX_DELAY`, default `7d`.
- Extend `RepoBackfillStatus` with persistent retry scheduling metadata:
  - `RetryCount int json:"retry_count,omitempty"`
  - `NextAttemptAt time.Time json:"next_attempt_at,omitzero"`
- Add a steady-state retry runner that:
  - Periodically scans `repo/` for `Backfill.Status == failed`, `Active == true`, and `NextAttemptAt <= now` or unset.
  - Uses bounded global workers plus per-host semaphores keyed by `RepoStatus.Host`; missing-host rows share an `unknown` bucket.
  - Downloads via the relay `com.atproto.sync.getRepo` path, preserving 302-to-PDS behavior and existing host attribution.
  - Honors 429 rate-limit reset metadata (the XRPC `RateLimit-Reset` value surfaced through `xrpc.RetryAfter`) by parking that host until reset, updating affected repo `NextAttemptAt`, and skipping same-host work while parked.
  - On ordinary failure, keeps the repo `failed`, increments `RetryCount`, records bounded `LastError`, and schedules exponential backoff from the 4h base up to 7d with jitter.
  - On terminal unavailable/not-found behavior, preserves existing `StatusUnavailable` / terminal handling.
- On successful retry:
  - Append a synthetic `KindSync` DID tombstone row for the repo, then append downloaded records as `KindCreateResync`.
  - Clear retry metadata, set `Backfill.Status = complete`, update `Backfill.Rev`, top-level `Rev`, timestamps, counts, and host diagnostics through the existing store transition machinery.
  - Let the steady-state writer's normal `OnAppend` tombstone hook and compaction path handle suppression of older rows.
- Wire the retry runner into `runSteadyState` after the live consumer opens, using the same `data/segments` writer as live ingest. Add it to the steady-state errgroup as best-effort for remote failures: local infrastructure/write errors return and stop the daemon; remote repo failures are recorded and retried later.

## Documentation And Issue

- Update `docs/README.md` to remove all `backfill_complete.log` claims and describe Pebble `repo/<did>` rows as the durable retry source of truth.
- Update the stale code comment in `merge_cursor.go` that references `backfill_complete.log`.
- Replace issue #127's body with the corrected design summary: steady-state only, Pebble-backed failed rows, no completion log, configurable gentle retry pacing.

## Test Plan

- Unit tests for retry candidate scanning: failed-only, active-only, due-time filtering, missing `NextAttemptAt`, corrupt rows surfaced as scan errors.
- Store tests for retry metadata: failure schedules next attempt, success clears retry fields, unavailable rows are not retried, counts/host aggregates remain correct.
- Runner tests with `httptest`: successful retry writes `KindSync` before `KindCreateResync`, marks repo complete, and old rows are tombstoned for compaction.
- Rate-limit tests: 429 with rate-limit reset metadata parks the host, skips same-host candidates, and persists next attempt without busy-looping.
- Orchestrator tests: retry runner starts only in `PhaseSteadyState`, is not wired in merge, and `0` interval disables it.
- Run targeted tests with `just test ./internal/ingest/backfill ./internal/ingest/orchestrator ./cmd/jetstream`; run broader `just test` after implementation.
