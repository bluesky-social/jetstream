package pgstore_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/pgstore/pgtest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

type gcQueryVariant struct {
	name           string
	indexes        []string
	split          bool
	omitOuterLimit bool
}

type gcVariantScenario struct {
	name, kind string
	after      int
	gcAge      int64
	orphanAge  int64
}

type gcVariantResult struct {
	Mark        *catalog.MarkPage `json:",omitempty"`
	IDs         []uint64          `json:",omitempty"`
	Fingerprint string
}

type gcVariantObservation struct {
	Rows          int
	Mode          string
	Scenario      string
	Operation     string
	Variant       string
	Repetition    int
	ExecutionMS   float64
	PlanningMS    float64
	Source        gcPlanStatement
	ExplainBefore gcPlanStatement
	ExplainAfter  gcPlanStatement
	Arguments     []any
	Result        gcVariantResult
	LockVerified  bool
	Indexes       []string
	Work          []gcPlanRelationWork
	Plan          json.RawMessage
}

type gcVariantSummary struct {
	Rows, Samples           int
	Mode, Scenario, Variant string
	MedianMS, MinMS, MaxMS  float64
}

var gcVariantScratchIndexes = map[string]string{
	"gc_variant_mark":          `CREATE INDEX gc_variant_mark ON objects(object_id) WHERE state='available' AND unreferenced_at IS NULL`,
	"gc_variant_available_age": `CREATE INDEX gc_variant_available_age ON objects(unreferenced_at,object_id) WHERE state='available'`,
	"gc_variant_uploading_age": `CREATE INDEX gc_variant_uploading_age ON objects(created_at,object_id) WHERE state='uploading'`,
}

const gcVariantClaimLockSQL = `SELECT object_id FROM objects WHERE object_id=$1 FOR KEY SHARE NOWAIT`

// TestGCQueryVariants is a warm-cache experiment, not a latency gate. DDL,
// semantic checks and rollback are outside the server execution measurement.
// All indexes and query rewrites exist only in disposable test databases.
//
//nolint:paralleltest // Alternating variants share one seeded database per size.
func TestGCQueryVariants(t *testing.T) {
	raw := os.Getenv("JETSTREAM_TEST_GC_VARIANT_ROWS")
	if raw == "" {
		t.Skip("set JETSTREAM_TEST_GC_VARIANT_ROWS to comma-separated catalog sizes")
	}
	repetitions := 3
	if rawReps := os.Getenv("JETSTREAM_TEST_GC_VARIANT_REPS"); rawReps != "" {
		var err error
		repetitions, err = strconv.Atoi(rawReps)
		require.NoError(t, err)
	}
	require.GreaterOrEqual(t, repetitions, 3)
	require.LessOrEqual(t, repetitions, 5)
	directory := os.Getenv("JETSTREAM_TEST_GC_VARIANT_ARTIFACT_DIR")
	if directory != "" {
		require.NoError(t, os.MkdirAll(directory, 0700))
	}
	directory, err := os.MkdirTemp(directory, "jetstream-gc-variants-")
	require.NoError(t, err)
	t.Logf("warm-cache GC query variant artifacts: %s", directory)
	var observations []gcVariantObservation
	for _, value := range strings.Split(raw, ",") {
		rows, err := strconv.Atoi(strings.TrimSpace(value))
		require.NoError(t, err)
		require.GreaterOrEqual(t, rows, 2_000)
		require.LessOrEqual(t, rows, 20_000_000)
		t.Run(strconv.Itoa(rows), func(t *testing.T) {
			seed, url := pgtest.Open(t, nil)
			fixture := seedGCScanFixture(t, seed, rows)
			_, err := seed.Pool().Exec(t.Context(), `CREATE INDEX gc_variant_deployed_candidates ON objects(object_id) WHERE state='uploading' OR (state='available' AND unreferenced_at IS NOT NULL)`)
			require.NoError(t, err)
			// Build once, then transactional DROP selects each configuration;
			// rollback restores the indexes without rebuilding large relations.
			for _, name := range gcVariantIndexNames() {
				_, err := seed.Pool().Exec(t.Context(), gcVariantScratchIndexes[name])
				require.NoError(t, err)
			}
			lease := seed.NewLease()
			require.NoError(t, lease.Acquire(t.Context(), gcPlanGCDelay))
			for _, mode := range []string{"force_custom_plan", "force_generic_plan", "auto"} {
				t.Run(mode, func(t *testing.T) {
					store, conn := gcScanStore(t, url, mode)
					probe, err := seed.Pool().Acquire(t.Context())
					require.NoError(t, err)
					defer probe.Release()
					sources := map[string]gcPlanStatement{}
					for _, kind := range []string{"mark", "claim"} {
						tx := gcScanBegin(t, store, lease.Epoch())
						func() {
							defer func() { require.NoError(t, tx.Rollback(context.WithoutCancel(t.Context()))) }()
							call := gcScanCall{kind: kind, limit: fixture.claimLimit}
							if kind == "mark" {
								call.limit, call.args = fixture.markLimit, []any{int64(0), fixture.markLimit}
							}
							before := readGCPlanStatements(t, conn)
							result := gcScanExecute(t, tx, fixture, call)
							gcScanCheckState(t, conn, call, result)
							sources[kind] = gcScanCapture(t, before, readGCPlanStatements(t, conn), kind)
						}()
					}
					held, err := store.Pool().Acquire(t.Context())
					require.NoError(t, err)
					defer held.Release()
					require.Same(t, conn, held.Conn(), "isolated replay must retain the capture backend")
					server, settings := gcPlanConnectionInfo(t, conn)
					require.Equal(t, mode, settings["plan_cache_mode"])
					gcScanWriteJSON(t, filepath.Join(directory, fmt.Sprintf("%d-%s-settings.json", rows, mode)), map[string]any{"ServerVersion": server, "Settings": settings, "Indexes": gcVariantIndexInventory(t, conn), "Scope": "Local PostgreSQL, warm fixture; no cold-cache claim. Index DDL, correctness checks and rollback excluded from server execution time. Rollbacks retain physical tuple churn; alternating order reduces order bias, without making this a production benchmark."})
					verifyGCVariantClaimLocks(t, conn, probe.Conn(), fixture, sources["claim"])
					warmed := map[string]bool{}
					for _, scenario := range []gcVariantScenario{
						{name: "first", kind: "mark"},
						{name: "empty", kind: "mark", after: fixture.base},
						{name: "sparse-old", kind: "claim", gcAge: gcPlanGCDelay.Microseconds(), orphanAge: gcPlanOrphanAge.Microseconds()},
						{name: "dense-old", kind: "claim", gcAge: 60_000_000, orphanAge: 60_000_000},
						{name: "empty", kind: "claim", gcAge: 10 * 24 * 60 * 60_000_000, orphanAge: 10 * 24 * 60 * 60_000_000},
					} {
						variants := []gcQueryVariant{{name: "baseline"}, {name: "mark-index", indexes: []string{"gc_variant_mark"}}}
						if scenario.kind == "claim" {
							ages := []string{"gc_variant_available_age", "gc_variant_uploading_age"}
							variants = []gcQueryVariant{{name: "baseline"}, {name: "age-indexes", indexes: ages}, {name: "split-age-indexes", indexes: ages, split: true}, {name: "split-age-no-outer-limit", indexes: ages, split: true, omitOuterLimit: true}}
						}
						var reference gcVariantResult
						for repetition := range repetitions {
							order := slices.Clone(variants)
							if repetition%2 == 1 {
								slices.Reverse(order)
							}
							for _, variant := range order {
								observation := measureGCQueryVariant(t, conn, fixture, scenario, variant, sources[scenario.kind], mode, repetition, warmed)
								observations = append(observations, observation)
								name := fmt.Sprintf("%d-%s-%s-%s-%s-%d.json", rows, mode, scenario.kind, scenario.name, variant.name, repetition)
								gcScanWriteJSON(t, filepath.Join(directory, name), observation)
								if repetition == 0 && variant.name == "baseline" {
									reference = observation.Result
								}
								require.Equal(t, reference, observation.Result, "query/index variants must preserve full returned objects and selected membership; artifacts: %s", directory)
							}
						}
					}
				})
			}
		})
	}
	require.NotEmpty(t, observations)
	summaries := summarizeGCQueryVariants(observations)
	gcScanWriteJSON(t, filepath.Join(directory, "summary.json"), summaries)
	for _, summary := range summaries {
		t.Logf("rows=%d %s/%s/%s samples=%d median=%.3fms range=[%.3f,%.3f]ms", summary.Rows, summary.Mode, summary.Scenario, summary.Variant, summary.Samples, summary.MedianMS, summary.MinMS, summary.MaxMS)
	}
}

func gcVariantIndexNames() []string {
	names := make([]string, 0, len(gcVariantScratchIndexes))
	for name := range gcVariantScratchIndexes {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func gcVariantIndexInventory(t *testing.T, conn *pgx.Conn) []string {
	t.Helper()
	rows, err := conn.Query(t.Context(), `SELECT indexdef FROM pg_indexes WHERE schemaname='public'
 AND tablename IN ('objects','hot_batches','active_segment_blocks','generation_blocks','segment_generations') ORDER BY tablename,indexname`)
	require.NoError(t, err)
	indexes, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	return indexes
}

func measureGCQueryVariant(t *testing.T, conn *pgx.Conn, fixture gcScanFixture, scenario gcVariantScenario, variant gcQueryVariant, source gcPlanStatement, mode string, repetition int, warmed map[string]bool) gcVariantObservation {
	t.Helper()
	tx, err := conn.Begin(t.Context())
	require.NoError(t, err)
	defer func() { require.NoError(t, tx.Rollback(context.WithoutCancel(t.Context()))) }()
	for _, name := range gcVariantIndexNames() {
		if !slices.Contains(variant.indexes, name) {
			_, err := tx.Exec(t.Context(), "DROP INDEX "+pgx.Identifier{name}.Sanitize())
			require.NoError(t, err)
		}
	}
	sql := source.SQL
	if variant.split {
		sql = splitGCClaimSQL(t, sql, variant.omitOuterLimit)
	}
	name := "gc_variant_" + scenario.kind + "_" + strings.ReplaceAll(variant.name, "-", "_")
	_, err = conn.Prepare(t.Context(), name, sql)
	require.NoError(t, err)
	limit := fixture.markLimit
	args := []any{int64(scenario.after), limit}
	if scenario.kind == "claim" {
		limit = fixture.claimLimit
		args = []any{scenario.gcAge, scenario.orphanAge, limit}
	}
	execute := "EXECUTE " + pgx.Identifier{name}.Sanitize() + "(" + strings.Join(gcScanSQLArguments(args), ",") + ")"
	iterations := 1
	if mode == "auto" && !warmed[name] {
		iterations = 6
	}
	var result gcVariantResult
	for range iterations {
		_, err = tx.Exec(t.Context(), "SAVEPOINT gc_variant_measure")
		require.NoError(t, err)
		result = executeGCQueryVariant(t, conn, fixture, scenario, execute, args, limit)
		_, err = tx.Exec(t.Context(), "ROLLBACK TO SAVEPOINT gc_variant_measure")
		require.NoError(t, err)
		_, err = tx.Exec(t.Context(), "RELEASE SAVEPOINT gc_variant_measure")
		require.NoError(t, err)
	}
	warmed[name] = true
	before := gcPlanStatementNamed(t, readGCPlanStatements(t, conn), name)
	var plan json.RawMessage
	err = tx.QueryRow(t.Context(), "EXPLAIN (ANALYZE,BUFFERS,WAL,TIMING OFF,FORMAT JSON) "+execute).Scan(&plan)
	require.NoError(t, err)
	after := gcPlanStatementNamed(t, readGCPlanStatements(t, conn), name)
	checkGCPlanCounters(t, []gcPlanStatement{before}, after, mode)
	call := gcScanCall{kind: scenario.kind, args: args, limit: limit}
	if result.Mark != nil {
		gcScanCheckState(t, conn, call, *result.Mark)
	} else {
		gcScanCheckState(t, conn, call, result.IDs)
	}
	var timing []struct {
		Execution float64 `json:"Execution Time"`
		Planning  float64 `json:"Planning Time"`
	}
	require.NoError(t, json.Unmarshal(plan, &timing))
	require.Len(t, timing, 1)
	work, err := analyzeGCPlanWork(plan)
	require.NoError(t, err)
	indexes := gcVariantIndexInventory(t, conn)
	return gcVariantObservation{Rows: fixture.rows, Mode: mode, Scenario: scenario.name, Operation: scenario.kind, Variant: variant.name, Repetition: repetition, ExecutionMS: timing[0].Execution, PlanningMS: timing[0].Planning, Source: source, ExplainBefore: before, ExplainAfter: after, Arguments: args, Result: result, LockVerified: scenario.kind == "claim", Indexes: indexes, Work: work, Plan: plan}
}

func verifyGCVariantClaimLocks(t *testing.T, conn, probe *pgx.Conn, fixture gcScanFixture, source gcPlanStatement) {
	t.Helper()
	// DROP INDEX holds a table lock, so verify row-lock strength separately,
	// with the complete scratch-index set present and no transactional DDL.
	for _, variant := range []gcQueryVariant{
		{name: "production"},
		{name: "split", split: true},
		{name: "split_no_outer_limit", split: true, omitOuterLimit: true},
	} {
		tx, err := conn.Begin(t.Context())
		require.NoError(t, err)
		func() {
			defer func() { require.NoError(t, tx.Rollback(context.WithoutCancel(t.Context()))) }()
			sql := source.SQL
			if variant.split {
				sql = splitGCClaimSQL(t, sql, variant.omitOuterLimit)
			}
			name := "gc_variant_lock_" + variant.name
			_, err := conn.Prepare(t.Context(), name, sql)
			require.NoError(t, err)
			args := []any{gcPlanGCDelay.Microseconds(), gcPlanOrphanAge.Microseconds(), fixture.claimLimit}
			scenario := gcVariantScenario{kind: "claim", name: "sparse-old"}
			execute := "EXECUTE " + pgx.Identifier{name}.Sanitize() + "(" + strings.Join(gcScanSQLArguments(args), ",") + ")"
			result := executeGCQueryVariant(t, conn, fixture, scenario, execute, args, fixture.claimLimit)
			require.NotEmpty(t, result.IDs)
			var id uint64
			err = probe.QueryRow(t.Context(), gcVariantClaimLockSQL, int64(result.IDs[0])).Scan(&id)
			var pgError *pgconn.PgError
			require.ErrorAs(t, err, &pgError, "claim variant must retain explicit FOR UPDATE strength")
			require.Equal(t, "55P03", pgError.Code)
		}()
	}
}

func executeGCQueryVariant(t *testing.T, conn *pgx.Conn, fixture gcScanFixture, scenario gcVariantScenario, execute string, args []any, limit int) gcVariantResult {
	t.Helper()
	if scenario.kind == "mark" {
		var page catalog.MarkPage
		var last pgtype.Int8
		require.NoError(t, conn.QueryRow(t.Context(), execute).Scan(&page.Scanned, &last, &page.Marked))
		page.Last = uint64(last.Int64)
		want := catalog.MarkPage{}
		for id := scenario.after + 1; id <= fixture.base && want.Scanned < limit; id++ {
			want.Scanned++
			want.Last = uint64(id)
			if id%97 == 0 {
				want.Marked++
			}
		}
		require.Equal(t, want, page)
		gcScanCheckState(t, conn, gcScanCall{kind: "mark", args: args, limit: limit}, page)
		data, err := json.Marshal(gcPlanMarkPage{Scanned: page.Scanned, Last: page.Last, Marked: page.Marked})
		require.NoError(t, err)
		return gcVariantResult{Mark: &page, Fingerprint: fmt.Sprintf("%x", sha256.Sum256(data))}
	}
	rows, err := conn.Query(t.Context(), execute)
	require.NoError(t, err)
	defer rows.Close()
	type object struct {
		ID     uint64 `json:"ID"`
		Values []any  `json:"Values"`
	}
	objects := []object{}
	for rows.Next() {
		values, err := rows.Values()
		require.NoError(t, err)
		require.Len(t, values, 7)
		id, ok := values[0].(int64)
		require.True(t, ok)
		state := string(catalog.ObjectAvailable)
		if id%2 == 1 {
			state = string(catalog.ObjectUploading)
		}
		require.Equal(t, state, values[4], "RETURNING must preserve pre-claim state")
		objects = append(objects, object{ID: uint64(id), Values: values})
	}
	require.NoError(t, rows.Err())
	rows.Close()
	slices.SortFunc(objects, func(a, b object) int {
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		return 0
	})
	ids := make([]uint64, 0, len(objects))
	for _, object := range objects {
		ids = append(ids, object.ID)
	}
	want := []uint64{}
	switch scenario.name {
	case "sparse-old":
		want = fixture.claims[:min(limit, len(fixture.claims))]
	case "dense-old":
		for id := fixture.base + 1; id <= fixture.rows && len(want) < limit; id++ {
			want = append(want, uint64(id))
		}
	case "empty":
	default:
		t.Fatalf("unknown claim scenario %q", scenario.name)
	}
	require.Equal(t, want, ids, "age eligibility and global object-ID ordering must match the fixture")
	gcScanCheckState(t, conn, gcScanCall{kind: "claim", args: args, limit: limit}, ids)
	data, err := json.Marshal(objects)
	require.NoError(t, err)
	return gcVariantResult{IDs: ids, Fingerprint: fmt.Sprintf("%x", sha256.Sum256(data))}
}

func splitGCClaimSQL(t *testing.T, source string, omitOuterLimit bool) string {
	t.Helper()
	sql := strings.Join(strings.Fields(source), " ")
	const predicate = `WHERE (state = 'available' AND unreferenced_at < now() - $1::bigint * interval '1 microsecond') OR (state = 'uploading' AND created_at < now() - $2::bigint * interval '1 microsecond') ORDER BY object_id LIMIT $3 FOR UPDATE`
	require.Equal(t, 1, strings.Count(sql, predicate), "split variant must derive from the captured production eligibility predicate")
	selection := `WHERE object_id = ANY (ARRAY(SELECT object_id FROM page ORDER BY object_id LIMIT $3)) ORDER BY object_id`
	if !omitOuterLimit {
		selection += ` LIMIT $3`
	}
	// The merged page already bounds unique IDs; a second unknown LIMIT can
	// reduce the generic estimate from ten locking candidates to one.
	sql = strings.Replace(sql, predicate, selection+` FOR UPDATE`, 1)
	require.True(t, strings.HasPrefix(sql, "WITH c AS ("))
	return strings.Replace(sql, "WITH c AS (", `WITH page AS MATERIALIZED (
 (SELECT object_id FROM objects WHERE state='available' AND unreferenced_at < now() - $1::bigint * interval '1 microsecond' ORDER BY object_id LIMIT $3)
 UNION ALL
 (SELECT object_id FROM objects WHERE state='uploading' AND created_at < now() - $2::bigint * interval '1 microsecond' ORDER BY object_id LIMIT $3)
 ), c AS (`, 1)
}

func summarizeGCQueryVariants(observations []gcVariantObservation) []gcVariantSummary {
	groups := map[string][]gcVariantObservation{}
	for _, observation := range observations {
		key := fmt.Sprintf("%09d/%s/%s/%s/%s", observation.Rows, observation.Mode, observation.Operation, observation.Scenario, observation.Variant)
		groups[key] = append(groups[key], observation)
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	summaries := make([]gcVariantSummary, 0, len(groups))
	for _, key := range keys {
		group := groups[key]
		times := make([]float64, 0, len(group))
		for _, observation := range group {
			times = append(times, observation.ExecutionMS)
		}
		slices.Sort(times)
		median := times[len(times)/2]
		if len(times)%2 == 0 {
			median = (times[len(times)/2-1] + median) / 2
		}
		first := group[0]
		summaries = append(summaries, gcVariantSummary{Rows: first.Rows, Samples: len(times), Mode: first.Mode, Scenario: first.Operation + "/" + first.Scenario, Variant: first.Variant, MedianMS: median, MinMS: times[0], MaxMS: times[len(times)-1]})
	}
	return summaries
}
