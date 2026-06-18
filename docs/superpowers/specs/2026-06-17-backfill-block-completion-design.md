# Backfill Block Completion Design

## Context

Staging backfill profiling on `cpu2-pop3` showed low network utilization while
most backfill workers waited on `internal/ingest.Writer.AppendBatch`. The
current `SegmentHandler.HandleRepo` flushes the writer before returning so
`Store.OnComplete` can mark the repo complete only after its events are durable.
That preserves correctness, but it turns ordinary repo completion into a
per-repo segment fsync plus a separate synced Pebble metadata write.

`DESIGN.md` already describes the desired durable unit as a segment block:
fsync the block first, then commit a synced Pebble batch containing `seq/next`
and metadata for DIDs represented by that block. This change aligns initial
backfill completion with that model.

Issue: #62.

## Goals

- Remove the per-repo `writer.Flush()` from the hot backfill path.
- Allow atmos progress to advance after a repo's completion is queued in memory.
- Make durable repo completion happen only after the segment block containing
  the repo's final appended event is fsynced.
- Batch repo completion metadata into the block durability Pebble commit.
- Ensure durable `listRepos` cursor checkpoints never get ahead of queued but
  not-yet-durable repo completions.

## Non-Goals

- Do not change the segment file format.
- Do not weaken the rule that a persisted `StatusComplete` row implies the
  repo's initial backfill events are durable.
- Do not make `OnComplete` write a durable pending-completion log. In-process
  queued completions may be lost on crash and recovered by re-downloading.
- Do not redesign the backfill worker scheduler or batch barrier in this change.

## Architecture

Backfill completion becomes a writer-coordinated durability side effect.

`SegmentHandler.HandleRepo` will append events as it does today, but instead of
forcing a flush it will register a completion record with the ingest writer.
That completion record includes the DID, commit rev, completion timestamp, and
the store callback data needed to update repo, count, host, and completion-log
state later.

The ingest writer will associate each completion record with the current pending
segment block after the repo's final event has been appended. When a block
flushes naturally, the writer will fsync the block, then commit one synced
Pebble batch containing:

- `seq/next` for the flushed block.
- All repo completion updates attached to that flushed block.
- Related aggregate updates such as counts and host status.
- Any completion hook/log side effects that must happen after the complete row is
  durable.

If a repo has no appended events, the completion is metadata-only. Metadata-only
completions can be batched and durably committed at explicit checkpoint barriers
without waiting for a segment block.

## Data Flow

1. A worker downloads a repo and calls `HandleRepo`.
2. `HandleRepo` walks the MST and appends event batches.
3. After the final append for that repo, `HandleRepo` queues completion metadata
   against the writer's current block instead of calling `Flush`.
4. `Store.OnComplete` returns after the completion is queued, allowing atmos to
   count the repo complete in memory.
5. When the attached block flushes, the writer fsyncs the segment data first.
6. The writer commits the synced Pebble batch for `seq/next` and all completion
   metadata attached to that block.
7. After the Pebble batch commits, completion hooks run or are considered
   successful according to their existing contract.

## Cursor Checkpointing

The current `OnPageComplete` cursor write is not safe enough for async
completion because atmos calls it before page jobs are necessarily durable.
Cursor persistence must move to a durability barrier.

`OnBatchComplete` should become the durable cursor checkpoint. Before saving the
batch cursor, the backfill runner will force the writer to drain queued
completions for the batch:

- Flush any pending block containing queued event-backed completions.
- Commit any metadata-only completions.
- Wait for async flush jobs containing completions to finish.
- Then save `relay/list_repos_cursor` and
  `bootstrap/last_listrepos_cursor`.

This preserves the restart invariant: if a cursor checkpoint is durable, all
repos in the covered batch that atmos counted complete have durable completion
metadata or will be rediscovered from an earlier cursor after crash.

## Crash Behavior

- Crash after `OnComplete` queues completion but before block fsync:
  repo remains not complete in Pebble; segment recovery truncates to the last
  complete block; a future run re-downloads the repo.
- Crash after block fsync but before metadata batch commit:
  repo remains not complete in Pebble; duplicate durable rows may exist after
  re-download, which downstream consumers must already tolerate.
- Crash after metadata batch commit:
  repo is complete and its events are durable.
- Crash before batch cursor checkpoint:
  the next run resumes from the previous cursor and reconciles per-DID state.
- Crash after batch cursor checkpoint:
  every queued completion covered by the checkpoint has reached its durability
  barrier.

## Error Handling

Writer or metadata commit errors remain fatal for the run. A failed durability
barrier must abort before saving the cursor. `OnComplete` can return success
after queueing, but queue insertion failure must still surface as an atmos
processing error.

Completion hook errors should preserve the existing "completion recorded but
hook failed" behavior: surface the error to the backfill runner and abort rather
than silently losing the replicated completion signal.

## Observability

Add metrics or spans for:

- queued repo completions.
- completions committed per block flush.
- completion queue wait duration.
- forced checkpoint flushes.
- block fullness during backfill.
- writer lock wait or append contention if practical within the existing metrics
  style.

These metrics make it possible to verify that network utilization improves
without hiding a new durability backlog.

## Testing

Tests should cover:

- `HandleRepo` no longer flushes per repo.
- repo completion is invisible in Pebble until the attached block flushes.
- multiple repos completed in one block are committed in one durability batch.
- a repo spanning multiple blocks completes only with the final block.
- metadata-only completions are durably committed at a checkpoint barrier.
- cursor checkpoint waits for queued completions before saving.
- crash/restart cases around queued completion, block fsync, metadata commit,
  and cursor checkpoint.
- existing restart oracle coverage still passes for bootstrap and merge.

## Rollout

Keep the existing per-repo flush behavior easy to restore during development
until oracle and restart tests pass. Once merged, staging validation should
compare network receive rate, block fullness, worker goroutine states, and
completion lag against the baseline recorded in #60.
