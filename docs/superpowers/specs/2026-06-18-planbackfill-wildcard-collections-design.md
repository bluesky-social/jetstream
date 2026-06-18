# planBackfill: wildcard collection filters

**Date:** 2026-06-18
**Status:** Approved design, pre-implementation
**Subsystem:** `xrpcapi`, `manifest` (lexicon doc touch)

## Context

`network.bsky.jetstream.planBackfill` (`internal/xrpcapi/planbackfill.go`) plans
sealed-archive downloads. Its `collections` input currently accepts only exact
NSIDs. The `/subscribe` websocket endpoint already lets clients pass collection
*wildcards* of the shape `app.bsky.feed.*` (see `internal/subscribe/filter.go`),
and we want the same ergonomics on the query planner: a client backfilling "all
feed records" should write `app.bsky.feed.*` instead of enumerating every NSID
under that namespace by hand.

We do **not** support arbitrary glob patterns — only a trailing `.*` on a
namespace prefix, matching the one shape `/subscribe` allows.

### How the relevant pieces work today

- **Wire decode** — `JetstreamPlanBackfill_Input.UnmarshalJSON`
  (`api/jetstream/jetstreamplanbackfill.go`) reads `collections` as plain
  strings. Despite the lexicon declaring `items: {format: nsid}`, the generated
  binding performs **no** NSID-format check on input. A value like
  `app.bsky.feed.*` therefore flows untouched to the handler. This is the
  single interception point — no decoder change is needed.
- **Validation** — `validatePlanCollections` (`planbackfill.go:177`) dedupes and
  `atmos.ParseNSID`-validates each entry, capped at `cfg.MaxCollections`
  distinct entries.
- **Matching** — `Manifest.PlanBackfill` (`internal/manifest/plan.go:65`) builds
  a `map[string]struct{}` of wanted NSIDs once, then per segment calls
  `collectionIDsForSegment` (`plan.go:242`), which walks that segment's resident
  `Collections []string` table and keeps the indices whose NSID is in the want
  set. Block-level filtering uses those indices against `BlockCollections`.
- **Manifest residence** — every sealed segment's `Collections`,
  `CollectionEventCounts`, `BlockCollections`, `Blocks`, and blooms are loaded
  into memory at startup / on seal (`SegmentMetadata`, `manifest.go:37-50`). The
  whole plan path runs under `m.mu.RLock()` and touches only these in-memory
  fields. **No segment file is opened during planning.**

### "Omniscient" collection knowledge

The union of every sealed segment's `Collections` table *is* the set of all
collections the archive has ever seen. We use this knowledge *implicitly*: a
segment can only contain collections that are in that union, so prefix-matching
each segment's own table is provably identical to expanding a wildcard against
the global union and then exact-matching. We never need to materialize or cache
the union.

## Decision

### Approach: prefix-match inside the planner (not request-time expansion)

Carry wildcard prefixes alongside exact NSIDs through to the planner and match
them against each segment's resident collection table.

**Rejected alternative — request-time expansion:** resolve a cached "all known
collections" set, expand each wildcard to concrete NSIDs, feed those to the
existing exact-match planner. Rejected because it requires a new global-collection
cache with invalidation on every seal/compaction, opens a staleness window
(a wildcard could miss a collection that was sealed after the cache snapshot),
and complicates the `MaxCollections` cap (one wildcard could expand past it).
The chosen approach has none of these problems: no cache, no invalidation, no
staleness (all matching is under the manifest `RLock` against current state),
and the cap counts patterns, not expansions.

**Equivalence claim (the core correctness property):** for any archive and any
prefix `P`, planning with `{prefixes: [P]}` selects exactly the same segments
and blocks as planning with `{collections: <every NSID in the archive under P>}`.
This is what the equivalence property test (below) proves.

### Wire shape: no new field

Wildcards ride in the existing `collections` array, e.g.
`["app.bsky.feed.post", "app.bsky.graph.*"]`. Exact NSIDs and wildcards may be
mixed freely. This mirrors `/subscribe`'s `wantedCollections`.

### Validation rules (stricter than `/subscribe`)

`/subscribe` deliberately mirrors v1's lax behavior (no validation of the prefix
head). `planBackfill` is a new endpoint with no v1 wire contract, so we validate
strictly, reusing `atmos` as the single source of truth for NSID grammar rather
than re-deriving label rules.

A `collections` entry is processed as:

1. If it ends in `.*`: it is a **wildcard**. Let `head = strings.TrimSuffix(raw, ".*")`.
   Accept iff `atmos.ParseNSID(head + ".wildcard")` succeeds, where `wildcard`
   is a synthetic, known-valid name label. (We append a name label because
   `atmos.ParseNSID` requires the final segment to start with a letter and be
   alphanumeric — `head` alone, e.g. `app.bsky`, is only an authority and would
   wrongly fail as a 2-segment NSID. The probe string is never stored.)
   The stored prefix is `head + "."` (e.g. `app.bsky.feed.`).
2. Otherwise: parse as an exact NSID via `atmos.ParseNSID`. Invalid → reject.

Because `atmos.ParseNSID` (v0.1.10) requires the name segment to start with a
letter and contain only alphanumerics, `_` is invalid; the probe name `wildcard`
is plain ASCII letters and always valid.

Boundary table (all rows become explicit unit tests):

| input | classified as | head | probe | outcome |
|---|---|---|---|---|
| `app.bsky.feed.post` | exact | — | — | accept (exact) |
| `app.bsky.feed.*` | wildcard | `app.bsky.feed` | `app.bsky.feed.wildcard` | accept, prefix `app.bsky.feed.` |
| `app.bsky.*` | wildcard | `app.bsky` | `app.bsky.wildcard` | accept, prefix `app.bsky.` |
| `com.example.*` | wildcard | `com.example` | `com.example.wildcard` | accept, prefix `com.example.` |
| `app.*` | wildcard | `app` | `app.wildcard` (2 segs) | reject |
| `*` | wildcard? no `.` before `*` | — | — | not `.*`-suffixed → exact branch → reject |
| `.*` | wildcard | `` (empty) | `.wildcard` (empty label) | reject |
| `app.bsky..*` | wildcard | `app.bsky.` | `app.bsky..wildcard` (empty label) | reject |
| `app.bsky.feed.*.*` | wildcard | `app.bsky.feed.*` | bad label `*` | reject |
| `app.bsky.fo*` | exact (no `.` before `*`) | — | — | reject (invalid NSID) |
| `app.bsky.feed.*x` | exact (ends `*x`, not `.*`) | — | — | reject (invalid NSID) |
| `1pp.bsky.*` | wildcard | `1pp.bsky` | `1pp.bsky.wildcard` | reject (TLD not letter-initial) |
| `APP.BSKY.*` | wildcard | `APP.BSKY` | `APP.BSKY.wildcard` | accept (atmos permits uppercase labels) |

(The `APP.BSKY.*` row documents atmos's actual behavior; we assert whatever
`atmos.ParseNSID` decides rather than inventing a stricter rule. If atmos
rejects it, the test asserts rejection — the test mirrors the parser, it does
not override it.)

### Cap semantics

`MaxCollections` counts **distinct patterns** (exact NSIDs + distinct prefixes),
not expanded collections. `app.bsky.*` counts as 1 regardless of how many
collections it covers. This matches `/subscribe` and keeps the cap meaningful
and predictable. Exact NSIDs and prefixes are deduped independently (an entry
repeated verbatim counts once). `MaxCollections == 0` with any pattern present
yields the existing "collection filters are disabled" `InvalidRequest`.

### Lexicon

Relax `collections.items` in `lexicons/network/bsky/jetstream/planBackfill.json`
from `{type: string, format: nsid}` to `{type: string}` and update the field
description to document the two accepted shapes (exact NSID, or
`<namespace>.*` wildcard). The generated binding already reads plain strings, so
this is behaviorally a doc change — but we regenerate the binding so the lexicon
and code do not contradict each other (a `format: nsid` annotation alongside
code that accepts non-NSID wildcards is latent tech debt).

## Components & changes

### `internal/manifest/plan.go`

- `PlanBackfillRequest` gains `CollectionPrefixes []string` (each entry ends in
  `.`). Existing `Collections []string` (exact) is unchanged.
- `collectionMatchAll` becomes
  `len(req.Collections) == 0 && len(req.CollectionPrefixes) == 0`.
- `collectionIDsForSegment(seg, want, prefixes)` keeps a segment collection
  index if its NSID is in the exact `want` set **or** has any prefix in
  `prefixes` as a `strings.HasPrefix` match.
- One-sided contract (no false negatives, possible false positives) is preserved
  — prefixes only ever widen the matched set, never narrow it.

### `internal/xrpcapi/planbackfill.go`

- `validatePlanCollections` returns `(exact []string, prefixes []string, err error)`.
  It splits entries per the rules above, dedupes each kind, and enforces
  `MaxCollections` against `len(exactDistinct) + len(prefixDistinct)`.
- A small pure helper `classifyCollectionPattern(raw string) (exact string, prefix string, err error)`
  (or two predicates) isolates the wildcard-vs-exact decision so it can be unit
  tested directly without HTTP plumbing. This helper is the "wildcard parser"
  that gets exhaustive unit coverage.
- `planRequestFromInput` threads the prefixes into
  `manifest.PlanBackfillRequest.CollectionPrefixes`.

### `lexicons/.../planBackfill.json` + regenerated `api/jetstream/...`

Doc/format relaxation as above; regenerate the binding via the repo's codegen.

## Performance

Per segment, matching cost is `O(collections × prefixes)`. Collections per
segment are <1000 in practice; prefixes are bounded by `MaxCollections`
(default 25). Worst case is a few tens of thousands of `strings.HasPrefix` calls
per segment, each cheap, and the exact-match map lookup still runs first. No new
allocation on the match hot path beyond the existing per-segment index set.
**The path is in-memory only — no segment file is read during planning** (verified
against `plan.go` and `SegmentMetadata`).

## Error handling

Surface is unchanged: invalid wildcard or invalid NSID → `InvalidRequest` (400);
over-cap → `InvalidRequest`; `MaxCollections == 0` with any pattern → "collection
filters are disabled" `InvalidRequest`. No new error codes; the existing
`PlanTooLarge` semantics are untouched.

## Testing

Per AGENTS.md: unit tests sparingly but they earn their place for small pure
paths like the parser; integration for happy paths; fuzz/property for untrusted
input and invariants. Every test must keep its package suite under ~1s.

### 1. Wildcard-parser unit tests (exhaustive — primary assurance) — `xrpcapi`

Table-driven tests over the pure classify/validate helper covering **every row**
of the boundary table above, plus:

- exact-only, wildcard-only, and mixed inputs;
- dedup: repeated exact NSID counts once; repeated identical wildcard counts
  once; exact + its own wildcard (`app.bsky.feed.post` + `app.bsky.feed.*`) kept
  as distinct patterns;
- ordering independence (prefix before/after exact yields same parsed sets);
- cap: `len(exact)+len(prefixes)` exactly at limit accepts, one over rejects;
  a wildcard counts as exactly 1 toward the cap;
- `MaxCollections == 0` with a wildcard → disabled error;
- stored prefix always ends in exactly one `.` and never retains the `*`;
- empty input → no patterns, match-all preserved.

Assertions check the *parsed output* (exact set, prefix set), not just
accept/reject, so a regression that mis-buckets an entry is caught.

### 2. Manifest equivalence property test (core correctness) — `manifest`

Seeded/randomized: build an archive of several segments over a known NSID
universe with overlapping namespaces (e.g. `app.bsky.feed.*`, `app.bsky.graph.*`,
`com.example.*`). For each namespace prefix `P` present (and some absent), assert
`PlanBackfill({CollectionPrefixes: [P]})` produces byte-identical segments,
block ranges, modes, and stats to `PlanBackfill({Collections: <all archived
NSIDs under P>})`. Run as a small swarm over multiple seeds. This directly
proves the prefix-match ≡ expansion claim.

### 3. xrpcapi integration tests (happy paths + edges) — `xrpcapi`

End-to-end HTTP POST against a real manifest built from written segments:

- wildcard-only request matches the expected blocks;
- mixed exact + wildcard;
- wildcard matching nothing in a non-empty archive still returns the correct
  `plannedThroughSeq` (coverage horizon) with zero segments;
- wildcard combined with a `dids` filter (intersection still correct);
- wildcard with a seq window (`afterSeq`/`beforeSeq`);
- invalid wildcard shapes return 400 `InvalidRequest` (a couple of representative
  reject rows, end-to-end, to prove the wire path surfaces the handler error).

### 4. Fuzz test — `xrpcapi`

Mirror `internal/subscribe/filter_fuzz_test.go`: feed random pattern lists to the
parser; assert it never panics and that for every accepted entry the invariant
holds — exact entries parse as NSIDs, and every stored prefix ends in `.` with a
head that, re-probed, parses as a namespace. Bounded input size.

## Out of scope (kaizen — file as separate issues if warranted)

- Backporting strict validation to `/subscribe` (it is intentionally v1-lax; do
  not touch).
- Wildcards for the `dids` filter (DIDs have no namespace hierarchy; not requested).
- Any global-collection enumeration API (the chosen approach does not need one).
