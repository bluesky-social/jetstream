# Active cold replay: offset-bounded range scanning

Issue: https://github.com/bluesky-social/jetstream/issues/300

## Problem

A client that finishes sealed archive backfill cuts over to the v2 websocket at
`sealedTipSeq`. If that cursor is below the writer readable-log floor, subscribe
uses `ColdReader` to replay durable disk data. For rows in the active, unsealed
segment, every bounded `ColdReader.Read` currently calls `segment.WalkActiveFS`,
which starts at byte 256 and reconstructs/decodes the whole active frame prefix
before returning at most `DefaultReadBatch` events. The next batch starts from
byte 256 again.

Across N batches this repeats progressively longer prefixes and produces
quadratic work. Sparse DID/collection filters make the failure look like an idle
client because filtering occurs after the global batch is read.

### Production evidence

On pop2 on 2026-08-10:

- planner cutover: `24,588,338,585`
- writer tip: `24,589,902,470`
- readable-log floor: `24,589,351,106`
- cutover was 1,012,521 seqs below the hot floor
- active segment was about 192 MiB; readable log about 256 MiB / 551k events
- at 21:33 UTC Grafana showed 38,980 filtered events/s and 38.06 cold reads/s
- at 21:46 UTC those decayed to 572 filtered events/s and 0.56 cold reads/s
- `filtered events/s ~= cold reads/s * 1024`, exactly the current read batch
- hot reads, adversarial drops, and encode errors stayed at zero

The full evidence is recorded on issue #300.

## Design

### Bounded segment scanner

Add `segment.WalkActiveRangeFS(path, startOffset, endOffset, fn)` plus the host-FS
wrapper. It opens an active file, rejects a file already sealed at open, validates
an exact frame-aligned `[start,end)` range, and decodes only frames in that range.
It must not stat or scan to current EOF. The snapshotted end excludes concurrent
appends and a footer appended by a concurrent seal.

Keep `WalkActiveFS` unchanged for recovery/inspection callers.

### Existing active block index

`segment.Writer` already owns `flushedBlocks []BlockInfo` and
`nextBlockOffset`. It updates them after each successful block write and rebuilds
them when reopening an active file. Add `FlushedRangeFromSeq(seq)` that binary
searches for the first block whose `MaxSeq >= seq` and returns that block offset
plus `nextBlockOffset`. Do not copy the full block slice or duplicate this index
in `ingest.Writer`.

### Coherent ingest snapshot

Add `ingest.Writer.ActiveFlushedRange(startSeq)`. Under `Writer.mu`, capture the
active segment index and its `[start,end)` offsets in one value. Hold the lock
only for the query/value copy, never for disk I/O. This prevents mixing an old
segment index with a new segment's offsets during rotation.

### Subscribe integration

Replace `ActiveIndex` + `WalkActiveFS` in `walkActiveRegion` with the coherent
snapshot + `WalkActiveRangeFS`. Preserve all existing cursor, stop-floor, hole,
batch-full, and pending-memory behavior. Treat `ErrSegmentSealed` and a missing
active path as a no-progress rotation seam when a manifest is present; the
existing sealed/active convergence loop re-sweeps the freshly published sealed
segment and fails loud after two genuinely non-advancing passes.

## Invariants

- No segment format change.
- Pending/unflushed rows are never in the searchable index or cold replay.
- Readable-log floor movement still depends on completed durability ordering;
  the range scanner cannot make non-durable rows required by a cold read.
- A range snapshot contains one active generation: index, start, and end are
  captured under the ingest writer lock.
- Concurrent append is excluded by the snapshotted end and picked up later.
- Concurrent seal never exposes footer bytes. Open-after-seal returns
  `ErrSegmentSealed`; open-before-seal reads the unchanged frame region only.
- Restart requires no sidecar: active block metadata is reconstructed from the
  durable complete-frame prefix.
- Cursor delivery remains contiguous and at-least-once across rotation seams.

## Non-goals

- No DID/collection filter pushdown in this change. V1 filters are mutable,
  filtered reads need a scan-cursor contract even for empty output, and active
  blocks do not currently carry DID/collection indexes.
- No active decoded-block cache.
- No reader-triggered flush or seal.
- No per-subscriber byte-offset state.

## Work checklist

- [x] Record design, evidence, invariants, and verification plan.
- [x] Add exact offset-bounded active range scanner in `segment`.
- [x] Add binary-search flushed range lookup on `segment.Writer`.
- [x] Add coherent active flushed range snapshot on `ingest.Writer`.
- [x] Switch subscribe active cold replay to range snapshots.
- [x] Add segment range/rotation/error tests.
- [x] Add segment and ingest flushed-range lookup tests.
- [x] Add subscribe multi-batch active replay linear-work regression test.
- [x] Add active cold replay benchmark.
- [x] Run targeted segment/ingest/subscribe tests and affected race tests.
- [x] Run `just`.
- [x] Run long oracle and oracle sweep.
- [x] Run segment fuzz targets.
- [x] Record verification results and remaining risks below.

## Verification results

All checks passed on 2026-08-10:

- `just test ./segment ./internal/ingest ./internal/subscribe`: 753 tests.
- `go test -race ./internal/ingest ./internal/subscribe`: passed.
- `BenchmarkColdReadActiveRange` at 16/64/256 blocks: about 60 us for 256
  events, 209 us for 1,024 events, and 781 us for 4,096 events. Cost per event
  remains approximately constant as active history grows.
- `just`: lint reported 0 issues; 2,328 short tests passed (24 skipped).
- `just test-long ./internal/oracle`: 211 passed (1 skipped).
- `just oracle-sweep`: all 10 deterministic seeds and restart sub-runs passed.
- `just fuzz 30s ./segment`: every segment fuzz target passed.

Remaining risk: filter pushdown remains intentionally out of scope, so sparse
subscribers must still decode/filter every global active row. This change makes
that scan linear and resumable by block offset rather than quadratic. Production
validation should compare pop2 cold reads/s over a backfill-to-live cutover and
confirm the rate no longer decays with cursor depth.
