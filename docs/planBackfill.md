# planBackfill Implementation Plan

## Goal

Add `network.bsky.jetstream.planBackfill`, an XRPC procedure that plans sealed
archive downloads for historical backfill. The planner returns segment files
and/or block ranges that may contain rows matching exact DID and collection
filters. It is a transport planner over the sealed archive, not a query engine.

Issue: https://github.com/bluesky-social/jetstream-v2/issues/71

## Non-Goals

- Do not apply compaction overlay semantics.
- Do not orchestrate `/subscribe` live-tail startup or cutover.
- Do not include active/unsealed segment data in v1.
- Do not paginate v1 responses.
- Do not support wildcard collection filters in v1.
- Do not decode segment blocks on the normal planning path.

Clients remain responsible for fetching tombstones, subscribing to live,
downloading planned archive data, decoding blocks, and applying exact row
filtering.

## Endpoint Contract

`network.bsky.jetstream.planBackfill` is an XRPC procedure (`POST`) with JSON
input/output. It uses a procedure because DID lists can be large enough that
URL query parameters are a poor fit.

Input fields:

- `dids`: optional array of DID strings. Missing or empty means all DIDs.
- `collections`: optional array of exact collection NSIDs. Missing or empty
  means all collections.
- `afterSeq`: optional integer. Rows with `seq <= afterSeq` are outside the
  requested window. This matches cursor resume semantics: resume after the
  last seen seq.
- `beforeSeq`: optional integer. Rows with `seq > beforeSeq` are outside the
  requested window.

Seq window semantics are `(afterSeq, beforeSeq]`. If `beforeSeq` is absent, the
upper bound is the sealed archive tip. If both bounds are present and
`beforeSeq <= afterSeq`, the request is invalid.

Output fields:

- `plannedThroughSeq`: highest sealed seq covered by the plan, capped by
  `beforeSeq` when present. Clients that want a complete backfill+live flow can
  separately subscribe from this cursor, but the planner itself does not manage
  that flow.
- `segments`: array of planned sealed segment work in ascending segment index.
- `stats`: small diagnostic counts useful to clients and operators.

Each segment entry has:

- `name`: segment filename accepted by `getSegment` and `getBlock`.
- `index`: segment index.
- `checksum`: resident segment checksum as hex. This is advisory metadata and
  equals the segment ETag when the file generation has not changed.
- `minSeq`, `maxSeq`: segment seq envelope.
- `mode`: either `segment` or `blocks`.
- `blocks`: only present for `mode=blocks`, as inclusive `{first,last}` ranges.

Dense matches use `mode=segment`; sparse matches use `mode=blocks`. The
density threshold is configurable.

Errors:

- `InvalidRequest`: malformed input, invalid DID/NSID, negative seq, invalid
  seq window, or limit violation.
- `PlanTooLarge`: the planner would exceed the configured response/work entry
  limit. It must not silently truncate.

## Configuration

Defaults:

- `--plan-max-dids` / `JETSTREAM_PLAN_MAX_DIDS`: `1000`
- `--plan-max-collections` / `JETSTREAM_PLAN_MAX_COLLECTIONS`: `25`
- `--plan-max-entries` / `JETSTREAM_PLAN_MAX_ENTRIES`: `100000`
- `--plan-whole-segment-threshold` / `JETSTREAM_PLAN_WHOLE_SEGMENT_THRESHOLD`:
  `0.75`

Validation:

- Max values must be non-negative.
- `plan-whole-segment-threshold` must be in `(0, 1]`.
- Zero max DID/collection values mean the corresponding non-empty filter is
  disabled by configuration. Empty filters still mean match-all.
- `plan-max-entries` must be positive. Returning an unbounded response is not
  acceptable on the public surface.

## Planner Algorithm

The planner runs entirely over manifest-resident sealed-segment metadata:
segment bounds, block bounds, segment DID blooms, per-block DID blooms, and
per-block collection IDs.

For each sealed segment in ascending index order:

1. Skip the segment when its seq envelope does not overlap `(afterSeq,
   beforeSeq]`.
2. If both DID and collection filters are match-all, all seq-overlapping
   blocks are candidates.
3. If only collection filters are present, select blocks whose resident
   collection ID set intersects the requested collection IDs.
4. If only DID filters are present, select blocks whose per-block DID bloom may
   contain at least one requested DID. A segment-level bloom miss skips the
   segment.
5. If both filters are present, intersect the DID-selected and
   collection-selected block sets.
6. Apply block seq envelopes so blocks fully outside `(afterSeq, beforeSeq]`
   are skipped.
7. If selected block density is at least the configured threshold, emit a
   whole-segment entry. Otherwise coalesce selected block indices into
   inclusive ranges.

The selection contract is one-sided:

- No false negatives: every block that can contain a matching row in the seq
  window must be returned.
- False positives are allowed: clients filter decoded rows exactly.

For DID filters, bloom filters can over-include. If future work adds planner
budget fallbacks for very large DID filters, the fallback must over-include
rather than risk missing data.

## Data Structures

Add a manifest-facing planner API instead of pushing planning logic into the
XRPC handler. The handler should validate transport input, call the planner,
and translate the result into generated API types.

Proposed internal types:

```go
type PlanBackfillRequest struct {
    DIDs        []string
    Collections []string
    AfterSeq    uint64
    HasAfterSeq bool
    BeforeSeq   uint64
    HasBeforeSeq bool
    MaxEntries  int
    WholeSegmentThreshold float64
}

type PlannedSegment struct {
    Idx       uint64
    Checksum  uint64
    MinSeq    uint64
    MaxSeq    uint64
    Mode      PlanMode
    Blocks    []BlockRange
}
```

The API should return stats alongside segments. Tests should assert stats only
where they prove behavior; they must not make brittle assumptions about bloom
false positives.

## Testing Plan

Manifest/planner tests:

- Empty archive returns an empty plan and `plannedThroughSeq=0`.
- Empty filters match all sealed blocks.
- DID-only filter selects the block holding the DID and prunes obvious misses.
- Collection-only filter uses the resident collection index.
- DID+collection filters intersect block candidates.
- `afterSeq` and `beforeSeq` prune segment and block envelopes correctly.
- Dense selected blocks produce whole-segment mode.
- Sparse selected blocks produce coalesced block ranges.
- Invalid windows and limit breaches return errors.
- `PlanTooLarge` is returned before emitting a truncated result.

XRPC handler tests:

- Route is registered as a POST procedure.
- Readiness gate returns the existing 503 envelope.
- Invalid JSON/input produces `InvalidRequest`.
- Invalid DID/NSID produces `InvalidRequest`.
- Limit violations produce `InvalidRequest`.
- Oversized plans produce `PlanTooLarge`.
- Response JSON contains planned segment/blocks and stats.

Integration-style test:

- Build real sealed segments with multiple DIDs, collections, and blocks.
- Call the XRPC endpoint.
- Fetch planned blocks/segments through existing `getBlock`/`getSegment`
  surfaces.
- Decode locally and prove all expected matching rows are present after exact
  row filtering.

DESIGN.md updates:

- Document the narrowed `planBackfill` contract in Section 3.3 or 5.
- Fix the segment block layout diagram to include the persisted `rendered_at`
  column, matching `segment/block.go`.

## Follow-Ups

- Basic collection wildcard support.
- Pagination if production response sizes require it.
- Active-segment planning if a client use case needs it.
- Planner observability once request volume and latency targets are known.
