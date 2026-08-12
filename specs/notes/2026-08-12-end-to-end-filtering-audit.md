# End-to-end kind, DID, and collection filtering audit

**Status: implementation and verification complete on issue [#335](https://github.com/bluesky-social/jetstream/issues/335).** This note records the
filtering contract implemented after [PR #324](https://github.com/bluesky-social/jetstream/pull/324),
the gaps found while testing the refactored module-root Go client, and the
recommended implementation direction. The governing performance choice is that
the server read path stays cheap and metadata-driven while the bundled client
remains relatively thick. Coarse server-side planning and modest over-send are
acceptable; false negatives and phase-dependent filter semantics are not.

Audit revision: `748a30d` (`main`) on 2026-08-12.

## Implemented resolution

The `jc/end-to-end-filtering` branch implements the three-axis predicate end to
end while preserving the cheap server/thick-client split:

- `planSnapshot` now accepts `kinds`; the manifest planner derives coarse kind
  candidates entirely from real collection IDs and the existing three marker
  sentinels. It does not open segments or decode rows.
- Omitted kinds keeps the marker-safe collection behavior. Explicit
  `kinds=commit` excludes marker-only blocks, removing the global marker
  baseline from rare collection backfills. Mixed blocks may still over-send.
- The Go client exposes `WithKinds`, validates/deduplicates all filter axes once
  in `Subscribe`, forwards the canonical predicate to every plan page and live
  reconnect, and exact-filters both segment and live event forms.
- Archive and live limits are aligned at 4 kinds, 10,000 DIDs, and 100
  collections. Direct endpoint limits apply to raw arrays; the bundled client
  deduplicates before emitting either transport request.
- Live `InvalidRequest` responses are terminal and become `ErrFatal`, including
  during cutover; `CursorTooOld` remains the sole signal that re-enters archive
  backfill. V2 `maxMessageSizeBytes` parsing is strict while v1 compatibility
  coercion remains unchanged.
- `cmd/client` accepts repeated `--kind`; the raw load tester supports it only
  for the v2 endpoint and rejects it explicitly for v1.

The regression suite covers planner kind shapes, no-false-negative cross-axis
selection, rare-collection cost shape, manifest-only planning, XRPC validation,
client canonicalization and matching, archive-to-live predicate continuity,
filter preservation through re-backfill, terminal one-dial rejection, and
strict v2 size parsing.

Verification completed:

- `just` — lint clean; 2,383 short tests passed;
- `just test-race . ./internal/manifest ./internal/xrpcapi ./internal/subscribe ./cmd/client` — 829 tests passed under the race detector;
- `just test-long ./internal/oracle` — 211 tests passed;
- `just oracle-sweep` — all ten deterministic lifecycle and restart seeds passed;
- `just fuzz 30s ./segment` — all seven segment fuzz targets passed; and
- `just mutation-gate` — all 47 curated mutants were killed at their expected
  tiers and matched the mutation baseline.

## Executive finding

The three-axis v2 predicate is fully implemented only on the direct
`network.bsky.jetstream.subscribeEvents` websocket. It is not implemented across
the bundled client's sealed-archive backfill and live-tail abstraction:

- `kinds` exists in the subscribe lexicon and live server filter, but not in
  `planSnapshot`, the archive planner, the client matcher, the public Go client
  options, or `cmd/client`.
- DID and collection filtering are implemented across both transports, but
  their validation limits and duplicate handling differ between planning and
  subscribe.
- The public live client treats a permanent pre-upgrade `InvalidRequest` as a
  transient dial failure and reconnects forever.
- Documentation says a bundled-client consumer can request `kinds=commit`, but
  the bundled client has no way to express it.

The production symptom that prompted this audit was a backfill for the rare
collection `im.flushing.right.now`. The status page reported 3,023 collection
events, but the collection-only client delivered hundreds of thousands of
events and ran for a long time. This was not unrelated commit leakage. A
collection filter intentionally means:

> matching commits, plus every account, identity, and sync marker that is not
> excluded by a DID filter.

That default is necessary for folding correctness. The missing feature is the
explicit commits-only form already supported by the live protocol:
`kinds=commit&collections=im.flushing.right.now`.

## Intended predicate

For an event `e`, the semantic filter is:

```text
(kinds is unset OR kind(e) is in kinds)
AND
(dids is unset OR did(e) is in dids)
AND
(kind(e) is not commit
 OR collections is unset
 OR collection(e) matches collections)
AND
(e.seq is inside the requested seq window)
```

The collection clause deliberately does not reject non-commit events. A caller
that does not want account, identity, or sync markers must say so through the
kind axis. The same predicate must govern:

1. exact filtering after sealed block/segment download;
2. active/cold replay during websocket cutover;
3. the steady live tail; and
4. reconnect and re-backfill cycles.

The transport planner does not need to evaluate this predicate exactly. It must
return a superset of the rows it accepts, and the client must evaluate the exact
predicate before delivery.

## Current implementation matrix

| Axis | `planSnapshot` / manifest | Client exact matcher | V2 subscribe server | Public Go API / CLI |
|---|---|---|---|---|
| kinds | missing | missing | implemented | missing |
| DIDs | implemented | implemented | implemented | implemented |
| collections | implemented | implemented | implemented | implemented |
| seq bounds/cursor | implemented | implemented | implemented | implemented |
| max message size | not an archive predicate | not exposed | implemented with v1-style coercion | not exposed |

The direct websocket's three-axis implementation is sound in isolation. It has
truth-table and property coverage for the AND-composed predicate, validates the
four known kind names, applies DIDs to every kind, and applies collections only
to commits.

The archive path has a strong one-sided DID/collection planner and an exact
client-side matcher. DID blooms and collection block summaries may over-select,
and whole-segment mode may download unrelated rows; the client drops those rows
before delivery. This is the correct division of labor.

## Production evidence

On `jetstream.internal.pop2.bsky.network` on 2026-08-12:

- the status collection table reported 3,023 events and 416 blocks for
  `im.flushing.right.now`;
- its plan selected 72,657 blocks across 308 segments, including 98
  whole-segment downloads;
- a nonexistent collection still selected 72,430 blocks because marker
  sentinels are admitted under every collection filter;
- therefore the real collection added only 227 selected blocks over the global
  marker baseline;
- a ten-second client sample delivered 2,842 matching commits, 15,055 account
  events, 11,981 identity events, and 365 sync events; and
- no commits from an unrelated collection passed the exact client matcher.

The status page counts real collection events and intentionally excludes the
synthetic `$account`, `$identity`, and `$sync` planner sentinels. Comparing its
collection count directly with a collection-only client's total delivered event
count is therefore misleading.

## Findings

### F1: kind filtering is not end to end

`network.bsky.jetstream.subscribeEvents` declares `kinds`, `dids`, and
`collections`. The live handler implements all three. The bundled client's live
URL forwards only DIDs and collections, and its exact matcher has no kind set.
The archive protocol predates the kind axis and carries only DIDs, collections,
and seq bounds.

Consequences:

- the bundled client cannot request the documented commits-only stream;
- a rare collection can inherit the cost and output volume of the full-network
  DID-marker history;
- adding a client-only kind predicate would reduce delivered events but would
  not fix backfill I/O, because `planSnapshot` would still select every marker
  block; and
- adding only a live `WithKinds` option would make archive and live phases
  deliver different streams at cutover.

PR #324 was primarily the proposal-0015 websocket framing change. Its design
note explicitly made public `WithKinds` conditional and warned that exposing it
would also require the archive fold path to apply the same predicate. The
server-side live feature shipped, while that client/archive follow-through did
not. Later client documentation described `kinds=commit` as available without
distinguishing direct websocket consumers from the bundled client.

### F2: permanent live `InvalidRequest` responses reconnect forever

`Subscribe` validates incompatible seq option combinations, but does not parse
or normalize DIDs and collection patterns. The server therefore discovers bad
filters at websocket upgrade time.

The live dialer gives special meaning only to `CursorTooOld` and
`UnknownZstdDictionary`. An `InvalidRequest` envelope is returned as a generic
dial error, and the live consumer classifies generic dial errors as transient.
A malformed collection was observed reconnecting repeatedly against HTTP 400,
emitting recoverable errors and delivering zero events until context timeout.

This is a permanent configuration failure, creates useless server load, and
violates the crash-loud boundary expected for caller mistakes.

### F3: archive and live filter limits are inconsistent

Default limits differ:

- `planSnapshot`: 1,000 distinct DIDs and 25 distinct collection patterns;
- subscribe v2: 10,000 raw DID values and 100 raw collection values.

Planning deduplicates before enforcing its configurable limit. Subscribe v2
enforces its lexicon `maxLength` against the raw repeated query values and then
deduplicates for matching.

Production probes demonstrated both directions of drift:

- 26 distinct collections are rejected by planning even though subscribe
  accepts up to 100; and
- 101 copies of one collection are accepted by planning after deduplication but
  rejected by subscribe on raw count.

The second shape can complete archive backfill and then enter the permanent-400
reconnect loop from F2 at cutover. More generally, a filter should not change
validity because a caller enabled backfill or crossed a transport seam.

### F4: v2 max-message-size parsing silently falls back to unlimited

The v2 lexicon declares `maxMessageSizeBytes` as an integer with minimum zero.
The v2 parser reuses the v1 compatibility parser, under which malformed,
negative, and overflowing values silently become zero, meaning no limit. V2 has
no deployed legacy contract requiring this coercion. A typo can therefore
silently disable the caller's requested safety bound.

This option is intentionally lossy and is not an archive filter axis, so it
needs a separate API decision if the thick client is to expose it uniformly.
The direct v2 endpoint should still reject invalid syntax rather than silently
changing meaning.

### F5: tests prove layers, not the complete client contract

Existing coverage is strong but partitioned:

- subscribe tests prove the live three-axis truth table;
- manifest tests prove no false negatives for DID/collection/seq planning over
  commit rows;
- client matcher tests prove DID/collection/seq exact filtering;
- engine tests prove selected live-only and cutover behaviors; and
- the fold-convergence gate proves marker sentinels survive a collection-filtered
  backfill.

There is no public-client conformance test that runs a matrix of kinds, DIDs,
and collections across sealed rows, cutover cold replay, and newly arriving live
rows. There is also no contract test requiring one filter request to be valid on
both archive and live transports.

### F6: sparse live streams have no externally durable scan progress

The server advances its internal subscriber scan cursor over filtered-out rows,
but the client can persist only cursors attached to delivered events. If a
sparse filter produces no events for a long interval and the connection is
lost, reconnect starts from the last matching event. It may repeat a large
global cold scan or fall below the lookback floor and re-enter archive planning.

Archive pagination avoids this ambiguity with `plannedThroughSeq`, which
advances even when no rows match. The websocket protocol has no equivalent
periodic progress coordinate. This is not required to add kind filtering, and
server-side read filter pushdown is not the right accidental solution. It is a
separate performance/operability question that must be kept visible when
testing very sparse filters and long-lived reconnects.

## Recommended architecture

### Keep planning metadata-only and one-sided

`planSnapshot` should continue to run entirely from manifest-resident metadata.
It should not open segment files, decode blocks, inspect individual rows, fold
tombstones, or construct per-request exact result sets. Its contract remains:

> cheaply return ordered immutable transport units that are guaranteed to
> contain every potentially matching row; false positives are allowed.

Mixed blocks and whole-segment downloads remain valid. The density threshold is
a transport decision, not a semantic decision. The exact client matcher is the
authority for delivery.

### Extend the existing collection/sentinel index to coarse kind planning

The current footer already contains enough metadata to make the useful kind
distinctions without adding another per-block index:

- commit rows with valid collections contribute real collection IDs;
- account rows contribute `$account`;
- identity rows contribute `$identity`; and
- sync rows contribute `$sync`.

The planner can construct a segment-local set of admissible IDs from the
requested kinds and collections:

| Request shape | Candidate collection IDs |
|---|---|
| kinds omitted, collections omitted | all blocks, current behavior |
| kinds omitted, collections set | matching real IDs plus all marker sentinels |
| `kinds=commit`, collections set | matching real IDs only |
| `kinds=commit`, collections omitted | every real, non-sentinel ID |
| marker kinds only | only the requested marker sentinels |
| commit plus selected marker kinds | applicable real IDs plus selected sentinels |

Selection remains block-coarse. A block containing one selected marker and many
unselected commits is downloaded whole, then exact-filtered by the client. A
high selected-block density may still choose whole-segment mode. This is
intentional mechanical sympathy: use the compact metadata already resident in
memory and avoid server-side row work during large backfills.

The ingest gate requires persisted commits to have representable collection
NSIDs, so using real collection IDs as the coarse `commit` class is safe for
valid archives. Corrupt internal state must fail loudly rather than requiring a
match-all planner fallback. If a future persisted non-commit kind is added, its
selection sentinel/index representation must land with it before it becomes a
valid segment kind.

### Make the thick client own exactness

Add a public kind option, expected to be shaped like
`WithKinds([]Kind{KindCommit})`, and carry the normalized kind set through:

- public option resolution and validation;
- `planSnapshot` input;
- the archive row matcher;
- live subscribe query construction;
- the live exact matcher; and
- every reconnect/re-backfill cycle.

The matcher should remain one small, allocation-free-per-event predicate after
construction. It should evaluate the same expression for decoded segment rows
and public live events. Planner false positives must never escape it.

The client should parse, canonicalize, and deduplicate DIDs, kinds, and
collection patterns once during `Subscribe`. Invalid values and impossible
combinations should fail before network I/O. The canonical filter should then
be reused by both transports so duplicate representation and phase transitions
cannot change validity.

Server responses remain authoritative because operators can configure limits
and versions can differ. A pre-upgrade `InvalidRequest` is nevertheless
terminal for a fixed client configuration; it must surface as `ErrFatal`, not
enter reconnect backoff. Transient service/network errors continue to retry.

### Preserve marker-safe defaults

Do not change `WithCollections(X)` to imply commits-only. Existing folding
consumers rely on account deletes and sync markers to purge state. The backward
compatible meanings should be:

- `WithCollections(X)`: X commits plus account/identity/sync markers;
- `WithKinds(commit), WithCollections(X)`: X commits only; and
- `WithDIDs(D), WithCollections(X)`: X commits and markers only for D.

The commits-only option is an explicit correctness trade: a consumer selecting
it cannot build a generally convergent materialized view unless it obtains
account/sync invalidation through another channel.

## Required protocol and code changes

1. Add `kinds` to `network.bsky.jetstream.planSnapshot`, using the same four
   values and impossible-combination validation as subscribe v2.
2. Regenerate the API bindings with `just lexgen`.
3. Add kind selection to `manifest.PlanSnapshotRequest` and its cheap block-ID
   selection logic; retain the one-sided planner contract.
4. Add kinds to the root client's resolved request and exact matcher.
5. Add `WithKinds` and `cmd/client --kind`; add the corresponding load-test
   option where appropriate.
6. Forward canonical kinds, DIDs, and collections on every initial live dial and
   reconnect.
7. Unify or explicitly reconcile archive/live limits and duplicate semantics.
8. Validate client filters before network I/O and classify server
   `InvalidRequest` as terminal.
9. Make v2 max-message-size parsing strict, separately from the v1-frozen
   parser.
10. Correct the client specification and status/operator documentation.

## Verification requirements

### Predicate conformance

- Build a table covering every kind, multiple DIDs, exact collections,
  wildcard collections, omitted axes, and meaningful axis combinations.
- Feed the same table through the server `FilterV2`, the archive client matcher,
  and the live client matcher; require identical semantic results.
- Reject unknown kinds and `collections` combined with an explicit kind set
  that excludes commit.

### Planner safety and cost shape

- Extend the manifest no-false-negatives integration/property test to all event
  kinds and filter-axis combinations.
- Assert that `kinds=commit&collections=X` does not admit marker-only blocks.
- Assert that selected marker kinds admit their sentinel blocks.
- Permit unrelated rows inside admitted blocks; test exact client filtering
  rather than demanding a minimal plan.
- Keep a regression fixture shaped like the production rare-collection case:
  the commits-only plan should track the rare collection's block footprint, not
  the global marker footprint.
- Retain a test proving planning performs no segment-file reads on the normal
  path.

### Full client lifecycle

- Run the public client over sealed archive rows, the active/cold cutover gap,
  and newly appended live rows for representative filter combinations.
- Require the same predicate before and after cutover, including a too-old
  re-backfill cycle.
- Preserve marker fold convergence when kinds are omitted.
- Prove commits-only mode excludes markers in both archive and live phases.
- Prove an invalid filter terminates once with `ErrFatal` and does not redial.
- Prove filters at the supported cap survive archive-to-live cutover, including
  duplicate inputs after client canonicalization.

### Operational verification

- Compare plan `blocksMatched`, whole-segment count, downloaded bytes, decoded
  rows, and delivered rows for collections-only versus commits-only queries.
- Add per-kind counts to the diagnostic client output or equivalent temporary
  tooling so marker volume is visible during validation.
- Exercise a sparse live filter across reconnect and measure repeated cold-scan
  work; record whether a separate progress-coordinate design is warranted.
- Run `just`, focused race tests, the long oracle, oracle sweep, relevant segment
  fuzz targets, and the mutation gate/campaign if planner or oracle target code
  moves.

## Documentation corrections

- `specs/client.md` must distinguish direct websocket capabilities from options
  actually exposed by the bundled client until `WithKinds` lands.
- Its statement that a wildcard matching no collection yields an empty plan
  needs the marker qualification: with kinds omitted, marker-bearing blocks can
  still be selected and marker events can still be delivered.
- The status collections table should explain that its event count excludes
  DID-level markers and therefore is not the expected total for a
  collections-only subscription.
- Client and engine comments should say that server-side filters are pruning
  and the local matcher is the exactness backstop; stale claims that the v2
  server sends every collection should be removed.

## Non-goals

- Exact row filtering inside `planSnapshot` or `getBlock`/`getSegment`.
- Opening or decoding segment files during normal planning.
- Eliminating all transport over-send from mixed blocks or whole-segment mode.
- Folding records or suppressing tombstones in the client library.
- Changing the frozen v1 `/subscribe` filter contract.
- Solving sparse websocket progress as an accidental side effect of kind
  filtering; that requires an explicit protocol decision if measurements show
  it is necessary.
