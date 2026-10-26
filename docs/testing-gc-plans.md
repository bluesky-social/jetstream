# Testing PostgreSQL GC plans

The GC tests use the live Jetstream migration, `pgstore.Store`, and catalog
session methods against a fresh database created by `pgtest`. They are intended
for the disposable development database or a dedicated test PostgreSQL server
whose role can create and drop scratch databases.

## Included query change

`MarkUnreferenced` and `ClaimObjects` explicitly restrict each UPDATE target to
IDs in its selected CTE batch, using `o.object_id = ANY (ARRAY(SELECT object_id
FROM page))` for marking and the equivalent expression using `c` for claiming.
This prevents the observed generic-plan full-target scan while preserving the
joins, reference checks, explicit claim locks and pre-claim return values.
The restriction is application SQL in `internal/pgstore/tx.go`; it requires no
new index, migration or database-admin setting. POP1 already has the separate
`objects_gc_claim_candidates_id` candidate-selection index.

The query fix and the verification tests are included together. Plan diagnostics
exercise the checked-out queries directly; no overlay is needed to test the fix.

## Correctness and plan capture

Start the repository's development services with `just up`, then select its
PostgreSQL endpoint:

```sh
export JETSTREAM_TEST_PG_URL='postgres://jetstream:jetstream@127.0.0.1:15432/jetstream?sslmode=disable'
JETSTREAM_TEST_GC_PLAN_ARTIFACT_DIR=/tmp/jetstream-gc-plans just test-gc-plans
```

`test-gc-plans` requires PostgreSQL instead of silently skipping it. Its default
20,000-object fixture is a functional smoke test. A larger diagnostic run is:

```sh
JETSTREAM_TEST_GC_PLAN_ARTIFACT_DIR=/tmp/jetstream-gc-plans just test-gc-plans 1000000
```

The runner uses the existing Go toolchain and test dependencies. It is not tied
to a laptop filesystem layout or a separately maintained SQL/schema copy.
`TestGCSessionPlanModes` also runs in the existing storage CI job. The plan diagnostics
`TestGCQueryPlans` and `TestGCQueryPlansMixedMark` are opt-in through
`JETSTREAM_TEST_GC_PLAN_ROWS`; ordinary tests do not create a large fixture.

`TestGCSessionPlanModes` exercises committed mark pagination, all four reference
kinds, age eligibility, bounded claims with pre-claim rows, deleting-row resume,
forget, the catalog reference guard, and stale-epoch rejection under custom,
generic, and automatic planning. These are actual catalog script calls, so a
correct isolated SQL result cannot hide a missing script-level guard.
`TestGCClaimPreservesRowLock` checks from a second connection that a claimed
object retains its explicit `FOR UPDATE` lock until rollback, which must restore
the object and fence revision. A plain non-key UPDATE has weaker locking.

`TestGCQueryPlans` calls the real storage transaction methods and captures the
statement that actually executed through pgx. It keeps one backend per case and
records prepared-statement counter changes, actual SQL, passed arguments, server
settings, fixture size, and JSON execution plans. It compares the production
migration schema with and without the separately deployed candidate-selection
index. It does not substitute a separately maintained query. Reports remain in the
unique directory logged by the test after its scratch database is dropped.

The synthetic graph keeps the first 90% of objects referenced and mixes old and
young eligible/ineligible objects and all four reference kinds in the final
10%. The mark diagnostic starts at object zero; with larger fixtures its first
10,000 candidates are all referenced. `TestGCQueryPlansMixedMark` removes the
generation references from every tenth object in that first page, producing
1,000 actual marks at the default and larger fixture sizes. Both variants run
through the same methods, capture, and rollback checks; reports identify the
variant with `MixedMark`. The small session tests separately cover committed
marking and pagination. Fixture headers are placeholders: these tests
validate GC catalog behavior, not segment decoding or complete archive integrity.

## Interpreting the result

- Forced generic plans establish susceptibility. Automatic planning must be
  checked through the same backend's custom/generic counters; it need not choose
  a generic plan after a particular number of executions.
- Inspect the UPDATE target's actual rows, rows removed by filters, loops, and
  buffers separately from the candidate page. A bounded candidate page can still join a large target
  scan. Small-table sequential scans may be appropriate.
- EXPLAIN ANALYZE executes the captured statement again and is rolled back.
  Its plan-counter increment is separate from the preceding storage calls.
  Rollback restores logical data, but does not reset cache state or physical
  tuple churn. These diagnostics are not an unbiased cold-cache A/B benchmark.
- Client timing fields include result validation; use the plan
  `Execution Time` for PostgreSQL execution time. The transaction timing includes
  begin, fence, the GC storage call, validation, and rollback, but not the full
  catalog GC script.
- The diagnostics isolate one transaction on one connection. The session tests
  validate Jetstream's committed GC scripts. Neither simulates production WAN
  latency, the full pool's workload mix, or external object deletion.
- A successful test means capture and correctness checks passed. It does not
  mean a bad plan was selected, nor identify the historical archive-lock holder.

Use representative cardinalities and distributions before drawing performance
conclusions. Query changes should preserve the session tests, avoid
oversized target work where reproduced, and be checked under both custom and
automatic planning. Timing thresholds and a required optimizer choice are
deliberately not correctness assertions.

When evaluating a query rewrite, compare at least two catalog sizes with the
same page limits, under all three planning modes, both with and without the
candidate index. Check candidate selection, UPDATE-target work, and all four
reference-table scans separately: a rewrite must not merely move a full-table
scan elsewhere. The mixed-mark variant prevents a no-op mark from appearing
successful. Keep the session and row-lock tests green before interpreting
performance results.

## Scan-work regression checks

`just test-gc-scans` extends the diagnostics with assertions on work reported
by real PostgreSQL execution plans. It uses the production migration and
captures SQL executed by the storage methods, rather than maintaining a copy
of the production queries. With the same disposable database URL as above:

```sh
JETSTREAM_TEST_GC_SCAN_ARTIFACT_DIR=/tmp/jetstream-gc-scans \
  just test-gc-scans 100000,1000000
```

The default sizes use the same 10,000-object mark page and 1,000-object claim
limit. Each size seeds one database, reused across both index configurations
and custom, generic, and automatic planning. The fixture grows all four
reference relations with the catalog, leaves some objects unreferenced, and
places sparse old claim candidates behind many young objects. Mark probes
cover first, middle, partial final, and empty pages. Claim probes continue
through exhaustion, committing each real storage call in its own transaction
after rolling back its EXPLAIN replay. Between planning modes, only the fixed
set of claimed objects is restored. Separate probes check deleting-row resume,
empty resume, and all four reference kinds. Bounded state checks verify that
marks actually persist and claims actually enter `deleting`. These synthetic
catalog rows do not model segment decoding or blob deletion.

Plans and a combined `summary.json` record actual arguments, prepared-plan
counters, results, scan work, and budget violations. The analyzer counts
rows returned plus rows removed by filters/rechecks, multiplied by loops,
for candidate, UPDATE-target, and reference scans. Bitmap index entries are
reported too, so a selective heap lookup cannot hide broad index work.
Buffers are counted without duplicating ancestor totals. PostgreSQL rounds
per-loop averages and does not expose every dead index entry or unsuccessful
probe: these are work measurements, not exact counts of physical rows read.
Unsupported executed plan shapes fail rather than silently producing zero
work. A blanket ban on sequential scans would miss broad index scans and
reject reasonable scans of small relations.

The default gate checks UPDATE-target row work against `2 * limit + 64`,
allowing both bitmap-index and heap observations. Total relation work and
shared buffers are also reported against `8 * limit + 64` and
`64 * limit + 1024`, respectively. These fixed, batch-relative budgets are
regression thresholds for these fixtures, not runtime guarantees. To enforce
the broader budgets as well:

```sh
JETSTREAM_TEST_GC_SCAN_STRICT_TOTAL=1 just test-gc-scans
```

**The strict total-work gate currently fails.** Candidate selection can still
examine a large population of ineligible objects, including an empty final
call. Passing the default gate establishes only the UPDATE-target regression
check. No production SQL or index change is included in these expanded tests.

An optional real-database negative control removes exactly the PR's redundant
target predicate from the captured SQL, checks equivalent results, and records
its execution plan. This mode additionally requires at least one control to
exceed the target budget for each of marking and claiming:

```sh
JETSTREAM_TEST_GC_SCAN_REQUIRE_RED_CONTROL=1 just test-gc-scans
```

Large cases use a dedicated recipe so ordinary unit/storage runs remain fast.
Storage CI runs the two default sizes separately and retains the JSON plans
as `gc-scan-plans`. The existing session and lock tests remain necessary for
committed catalog semantics and
concurrent locking. These checks do not cover every statement in a complete GC
run: in particular, the final object-count gauge intentionally reads all
objects, and object deletion and foreign-key trigger work are outside this
plan matrix.

## Full-sweep contention experiment

`just test-gc-contention` compares no GC and one complete Collector run in fresh
million-object databases, while the real inline hot writer receives 1,000 events
per second for 15 seconds and the real lease renews every second. Every case has
the already deployed POP1 `objects_gc_claim_candidates_id` index; this is not an
index-rollout experiment. It uses a 32-connection pool, 30-second lease, native
10s/5s/30s statement/lock/idle timeouts, and default GC pages, ages and deletion
concurrency. PostgreSQL must be the disposable development endpoint above.

The arguments are rows, duration, events per second, tail percentage, and case
(`both`, `no_gc` or `gc`). For a sparser backlog:

```sh
JETSTREAM_TEST_GC_CONTENTION_ARTIFACT_DIR=/tmp/jetstream-gc-contention \
  just test-gc-contention 1000000 15s 1000 1
```

The first 90% of the default fixture is referenced; the final 10% mixes old
eligible objects, young objects, referenced objects and objects that must be
marked but survive the six-hour delay. A 1% tail keeps the same mix at smaller
cardinality. To compare the included fix with the original queries, run each
revision with identical settings and fresh databases. `JETSTREAM_TEST_GC_CONTENTION_LABEL`
labels artifacts; `JETSTREAM_TEST_GC_CONTENTION_PLAN_MODE` defaults to `auto`.

The absolute arrival schedule is independent of Append completion. Reports
retain raw scheduled, append and durable-commit timestamps, due-cohort latency
percentiles, dropped arrivals, end-window backlog, real renewal outcomes,
transaction call timings, hot-batch fast-path fallbacks, GC counts and
server-global WAL. Slow events remain in latency cohorts even if they commit
after the cohort window ends. Per-second summaries are arrival cohorts, not
completion-throughput buckets; derive completion throughput from raw commit
timestamps. Transaction/fence-response timings are client measurements, not
exact server lock-hold durations. Automatic planning settings alone do not
identify which prepared plans executed across the pool.

GC wall time includes the final object-count gauge refresh; the native GC
histogram excludes that refresh. The test verifies full sweep completion during
active ingress, zero dropped arrivals, complete persisted frame/sequence
coverage, expected deletion/mark counts, protected rows and gauge totals. Small
smoke sweeps may overlap no renewal; use the reported overlap count before
claiming renewal-under-GC coverage. Failures retain the report before assertions
where possible.

This isolates PostgreSQL contention: blob storage is an empty in-memory store,
so deletion calls exercise successful missing-key semantics. The synthetic
sealed history has no decodable segment payload. The real hot writer commits
inline, with a 65,536-event open-block cap and five-minute block age to avoid
folding; offered load is capped at 60,000 events and closed blocks are rejected
by the test. There is no maintainer, follower, SeaweedFS latency or transport
simulation. These results cannot establish production p99 or the historical
lock holder. Ordinary test runs skip the experiment unless its row-count
variable is set.
