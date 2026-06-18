# Compaction cache invalidation

## Summary

Compaction rewrites sealed segment files in place by writing a sealed
temporary file and atomically renaming it over the old path. Serving must not
continue to use stale in-memory state for that segment after the rewrite is
published.

Use targeted per-segment invalidation:

- Refresh the manifest entry for the compacted segment by reopening the
  rewritten file and verifying its sealed header/footer checksum.
- Invalidate the subscribe cold-path decoded block cache for that segment.
- Keep the current checksum-based decoded block cache key so stale block
  generations cannot be reached by new reads.

Do not rebuild the whole manifest or clear the whole subscribe block cache
after each rewritten segment. The mutation boundary is one segment file, so
the cache invalidation boundary should also be one segment.

## Motivation

Compaction physically removes superseded updates and deletes from sealed
segment files. The durable file content changes immediately after rename, but
jetstream also keeps serving metadata and decoded block bodies in memory:

- `internal/manifest.Manifest` keeps segment headers, block indexes, blooms,
  collection indexes, size, and mtime resident.
- `internal/subscribe` keeps a shared decoded block LRU for cold websocket
  replay.

If either cache can serve pre-compaction data after the file is rewritten,
clients may observe rows that the archive has already compacted away. That is
a data correctness bug. The fix should be simple, bounded, and safe under
concurrent replay.

## Non-goals

- **No full manifest reload on every compaction.** It is unnecessary work,
  increases lock hold pressure, and creates a larger failure surface than
  replacing one segment entry.
- **No global subscribe cache clear.** A compacted segment should not evict
  decoded blocks for unrelated segments.
- **No cancellation of already-running subscriber reads.** A subscriber that
  decoded an old block before invalidation may still emit it. Preventing that
  requires reader/compactor coordination across active websocket reads and is
  outside this change. The required contract is that new cold reads after the
  compaction refresh hook completes cannot hit stale decoded blocks for the
  compacted segment.
- **No block repacking.** The existing rewrite path preserves block topology
  and historical sequence envelopes. This work does not change that segment
  format contract.

## Design

### Manifest refresh

Keep the existing `Manifest.OnSegmentCompacted(idx, path)` shape. It should:

1. Wait for initial manifest load.
2. Reopen `path` as a sealed segment.
3. Verify the header/footer checksum.
4. Rebuild all resident metadata for that segment.
5. Replace the resident entry for `idx` under the manifest lock.

If the refresh fails, compaction must return an error and must not advance the
compaction watermark. Serving a rewritten file without a validated manifest
entry is not acceptable.

### Subscribe block cache invalidation

Add a narrow invalidation surface to the cold reader:

```go
type ColdReader struct {
    // unexported fields
}

func NewColdReader(cfg ColdReaderConfig) *ColdReader
func (r *ColdReader) Read(ctx context.Context, cursor uint64, max int) ([]*Entry, uint64, error)
func (r *ColdReader) InvalidateSegment(idx uint64)
```

The existing `coldReader` function type can stay as the `Tail` dependency.
Runtime wiring passes `coldRd.Read` to `subscribe.New`, while retaining the
`*ColdReader` so compaction can invalidate it.

The decoded block cache should use generation-aware invalidation:

- Track `generationBySegment map[uint64]uint64` inside `blockCache`.
- Include the segment generation in `blockKey`.
- `InvalidateSegment(idx)` increments that segment's generation and removes
  resident cached entries for `idx`.
- If a decode is in flight while invalidation happens, the in-flight decode
  may return to its caller, but it must not insert into the resident cache if
  its key generation is no longer current.

The checksum remains part of `blockKey`. The generation closes the race where
a plain purge can be undone by an old in-flight decode that finishes after the
purge. The checksum keeps distinct file generations separate even if an
invalidation call is missed during a transient crash window.

### Runtime wiring

Compose the compaction hook in `internal/jetstreamd/runtime.go`:

```go
onSegmentCompacted := func(idx uint64, path string) error {
    if err := mft.OnSegmentCompacted(idx, path); err != nil {
        return err
    }
    coldRd.InvalidateSegment(idx)
    return nil
}
```

Pass `onSegmentCompacted` to `orchestrator.Config.OnSegmentCompacted`.

The ordering is deliberate:

1. Refresh and verify manifest metadata first.
2. Invalidate decoded blocks only after the manifest refresh succeeds.

If manifest refresh fails, the compaction pass fails and retries later with
the same watermark. The decoded block cache remains conservative: it may still
hold old data, but the process has not acknowledged the compaction watermark
or advertised a fresh manifest entry.

## Failure behavior

- **Rewrite succeeds, manifest refresh succeeds, cache invalidation succeeds:**
  new cold reads use fresh manifest metadata and cannot hit old decoded blocks.
- **Rewrite succeeds, manifest refresh fails:** compaction returns an error,
  the watermark does not advance, and the next pass reconciles the manifest
  by comparing resident and on-disk checksums.
- **Rewrite succeeds, process crashes before refresh:** startup reads the
  rewritten segment from disk into the manifest. The subscribe block cache is
  process-local and empty after restart.
- **Rewrite succeeds, process crashes after refresh but before watermark:**
  startup reads the rewritten segment from disk. The next compaction pass
  observes the watermark lag and continues.
- **Decode is in flight during invalidation:** the old decode may satisfy the
  read that started before invalidation, but it cannot repopulate the cache
  for reads that start after invalidation.

## Testing

### Unit tests

Add manifest tests that:

- Open a manifest over one sealed segment.
- Rewrite that segment to drop rows.
- Call `OnSegmentCompacted`.
- Assert `ListFrom`, `SegmentByIdx`, `SegmentStats`, and `BlockIndex` reflect
  the rewritten checksum, size, event count, block event counts, and
  collection counts.

Add subscribe cache tests that:

- Warm the decoded block cache for a segment.
- Invalidate that segment.
- Read the same block again.
- Assert the second read re-decodes instead of returning the old cached
  events.

Add an in-flight race test that:

- Starts a decode and blocks it before insertion.
- Calls `InvalidateSegment(idx)`.
- Lets the old decode finish.
- Asserts the old generation is not resident in the cache and a later read
  runs a fresh decode.

Add runtime/orchestrator hook tests that:

- Verify rewritten segments call manifest refresh and subscribe invalidation.
- Verify clean segments do not call invalidation.
- Verify manifest refresh failure propagates and prevents watermark
  advancement.

### Oracle tests

Extend oracle coverage so the simulator enforces the serving contract, not
only the on-disk compaction contract:

- Build a scenario with create/update/delete traffic that produces tombstones
  for rows already sealed into a segment.
- Start a cold replay before compaction to warm the subscribe decoded block
  cache.
- Run steady-state compaction and wait for a successful compaction pass.
- Start a new cold replay from before the compacted segment.
- Assert replayed events satisfy the oracle's compacted archive model: no
  superseded record rows or account-deleted rows at or below the compaction
  watermark are emitted.

This test should fail if `OnSegmentCompacted` is not wired, if manifest
metadata remains stale, or if the subscribe decoded block cache can serve a
stale post-invalidation block.

## Operational notes

The manifest and block cache are process-local. This design does not change
external HTTP cache behavior. Whole-file segment downloads already derive
ETags from the file descriptor being served, which protects range and
conditional requests during a compaction rename and manifest refresh window.

The implementation should avoid claims about latency or memory wins beyond the
bounded behavior visible in code: invalidation scans only resident decoded
cache entries, and manifest refresh reads metadata for one segment.
