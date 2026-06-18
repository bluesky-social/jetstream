# Implementation plan: planBackfill wildcard collection filters

Tracks issue #73. Design: `2026-06-18-planbackfill-wildcard-collections-design.md`.

Work proceeds test-first. Each task lands its tests (red) then code (green) and
keeps the touched package's suite < ~1s. Branch: `73-planbackfill-wildcards`.

## Task 1 — manifest: prefix matching in the planner

**Files:** `internal/manifest/plan.go`, `internal/manifest/plan_test.go`

1. Add `CollectionPrefixes []string` to `PlanBackfillRequest` (each entry ends in `.`).
2. `collectionMatchAll` becomes `len(Collections)==0 && len(CollectionPrefixes)==0`.
3. `collectionIDsForSegment(seg, want, prefixes)` keeps an index if its NSID is in
   `want` OR `strings.HasPrefix(nsid, p)` for some `p` in `prefixes`.
4. Tests first: extend `plan_test.go` with prefix-only, mixed exact+prefix,
   prefix-matches-nothing, and a small assertion that prefixes never narrow the
   set (one-sided contract). Keep existing exact-match tests green.

**Done when:** planner matches prefixes; existing plan tests still pass.

## Task 2 — manifest: equivalence property test

**Files:** `internal/manifest/plan_prefix_equiv_test.go` (new)

Seeded/randomized archive over a known NSID universe with overlapping namespaces.
For each present (and some absent) prefix `P`, assert
`PlanBackfill({CollectionPrefixes:[P]})` == `PlanBackfill({Collections: <all
archived NSIDs under P>})` for segments, block ranges, modes, and stats. Swarm
over several fixed seeds (no `Math.random`; iterate seed values).

**Done when:** equivalence holds across all seeds; runs fast (< ~1s).

## Task 3 — xrpcapi: wildcard parser + validation

**Files:** `internal/xrpcapi/planbackfill.go`, `internal/xrpcapi/planbackfill_test.go`

1. Add pure helper `classifyCollectionPattern(raw) (exact, prefix string, err error)`:
   - ends in `.*` → wildcard; `head = TrimSuffix(raw, ".*")`; accept iff
     `atmos.ParseNSID(head + ".wildcard")` succeeds; return `prefix = head + "."`.
   - else → exact via `atmos.ParseNSID`.
2. `validatePlanCollections` returns `(exact, prefixes []string, err error)`:
   dedup each kind; cap on `len(exactDistinct)+len(prefixDistinct)` vs
   `MaxCollections`; `MaxCollections==0` with any pattern → disabled error.
3. `planRequestFromInput` threads `prefixes` into `CollectionPrefixes`.
4. Tests first — exhaustive table over the design's boundary table, asserting
   parsed exact/prefix sets; dedup; ordering independence; cap-at-limit vs
   over-by-one; disabled; stored prefix ends in exactly one `.`.

**Done when:** parser unit tests pass; existing planbackfill tests green.

## Task 4 — xrpcapi: integration + fuzz

**Files:** `internal/xrpcapi/planbackfill_test.go`, `internal/xrpcapi/planbackfill_fuzz_test.go` (new)

- Integration POSTs: wildcard-only, mixed, matches-nothing (still returns
  `plannedThroughSeq`), wildcard+DID, wildcard+seq window, invalid wildcard → 400.
- Fuzz: random pattern lists → parser; assert no panic and the accepted-entry
  invariant (exact parses as NSID; every prefix ends in `.` and its head re-probes
  as a namespace). Mirror `internal/subscribe/filter_fuzz_test.go`.

**Done when:** integration + fuzz pass; suite fast.

## Task 5 — lexicon + regenerate binding

**Files:** `lexicons/network/bsky/jetstream/planBackfill.json`, regenerated `api/jetstream/jetstreamplanbackfill.go`

Relax `collections.items` to `{type: string}`; update description to document the
two shapes. Regenerate via the repo's codegen recipe; verify only the intended
diff (binding already reads plain strings).

**Done when:** lexicon + binding consistent; build green.

## Task 6 — verify, commit, PR

- `just test ./internal/manifest ./internal/xrpcapi` (+ race), `just lint`.
- Run a brief fuzz pass (`just fuzz 30s ./internal/xrpcapi`).
- Self-review (requesting-code-review skill or /code-review).
- Commit with `Closes #73`; open PR.
