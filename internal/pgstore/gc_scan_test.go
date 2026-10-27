package pgstore_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/pgstore"
	"github.com/bluesky-social/jetstream/internal/pgstore/pgtest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

type gcScanReport struct {
	Rows            int
	Index           string
	Mode            string
	Scenario        string
	Operation       string
	Control         bool
	ServerVersion   string
	Settings        map[string]string
	Statement       gcPlanStatement
	ExplainBefore   gcPlanStatement
	ExplainAfter    gcPlanStatement
	CommitReplay    *gcPlanStatement
	Committed       bool
	Arguments       []any
	Result          any
	Plan            json.RawMessage
	Work            []gcPlanRelationWork
	ExpectedUpdated int
	TargetRows      float64
	TotalRows       float64
	SharedBlocks    float64
	TargetBudget    float64
	TotalBudget     float64
	BlockBudget     float64
	Violations      []string
}

type gcScanFixture struct {
	rows, base, markLimit, claimLimit int
	claims                            []uint64
}

// TestGCScanWork asserts UPDATE-target work, and records candidate/reference
// work separately. STRICT_TOTAL deliberately exposes search-work failures in
// the current queries instead of treating the target fix as a complete bound.
//
//nolint:paralleltest // One seeded database per size is reused across rolled-back scenarios.
func TestGCScanWork(t *testing.T) {
	raw := os.Getenv("JETSTREAM_TEST_GC_SCAN_ROWS")
	if raw == "" {
		t.Skip("set JETSTREAM_TEST_GC_SCAN_ROWS to comma-separated catalog sizes")
	}
	baseDirectory := os.Getenv("JETSTREAM_TEST_GC_SCAN_ARTIFACT_DIR")
	if baseDirectory != "" {
		require.NoError(t, os.MkdirAll(baseDirectory, 0700))
	}
	directory, err := os.MkdirTemp(baseDirectory, "jetstream-gc-scans-")
	require.NoError(t, err)
	t.Logf("GC scan artifacts: %s", directory)
	var reports []gcScanReport
	for _, value := range strings.Split(raw, ",") {
		rows, err := strconv.Atoi(strings.TrimSpace(value))
		require.NoError(t, err)
		require.GreaterOrEqual(t, rows, 2_000)
		require.LessOrEqual(t, rows, 20_000_000)
		t.Run(strconv.Itoa(rows), func(t *testing.T) {
			seed, url := pgtest.Open(t, nil)
			fixture := seedGCScanFixture(t, seed, rows)
			lease := seed.NewLease()
			require.NoError(t, lease.Acquire(t.Context(), time.Hour))
			for _, index := range []string{"baseline", "candidate"} {
				if index == "candidate" {
					_, err := seed.Pool().Exec(t.Context(), `CREATE INDEX gc_scan_claim_candidates ON objects(object_id) WHERE state='uploading' OR (state='available' AND unreferenced_at IS NOT NULL)`)
					require.NoError(t, err)
				}
				for _, mode := range []string{"force_custom_plan", "force_generic_plan", "auto"} {
					t.Run(index+"/"+mode, func(t *testing.T) {
						store, conn := gcScanStore(t, url, mode)
						server, settings := gcPlanConnectionInfo(t, conn)
						require.Equal(t, mode, settings["plan_cache_mode"])
						record := func(report gcScanReport) {
							report.Rows, report.Index, report.Mode = rows, index, mode
							report.ServerVersion, report.Settings = server, settings
							reports = append(reports, report)
							gcScanWriteReport(t, directory, report)
						}
						// Warm the real methods on this backend. In auto mode this also
						// crosses PostgreSQL's initial custom-plan sampling window.
						gcScanWarm(t, store, conn, lease.Epoch(), fixture, mode)
						for _, page := range []struct {
							name  string
							after int
						}{
							{"first", 0}, {"middle", fixture.base / 2},
							{"last", fixture.base - fixture.markLimit/2}, {"empty", fixture.base},
						} {
							tx := gcScanBegin(t, store, lease.Epoch())
							func() {
								defer func() { require.NoError(t, tx.Rollback(context.WithoutCancel(t.Context()))) }()
								call := gcScanCall{kind: "mark", name: page.name, args: []any{int64(page.after), fixture.markLimit}, limit: fixture.markLimit}
								if page.name == "first" && mode == "force_generic_plan" && index == "baseline" && os.Getenv("JETSTREAM_TEST_GC_SCAN_REQUIRE_RED_CONTROL") == "1" {
									record(gcScanMeasure(t, conn, tx, fixture, call, true))
								}
								record(gcScanMeasure(t, conn, tx, fixture, call, false))
							}()
						}
						// Each batch commits an actual storage call after rolling back
						// its EXPLAIN. Later transactions observe durable claims, through
						// the final empty probe; the fixture is restored once per mode.
						for step := 0; step <= len(fixture.claims)/fixture.claimLimit+1; step++ {
							tx := gcScanBegin(t, store, lease.Epoch())
							func() {
								defer func() { require.NoError(t, tx.Rollback(context.WithoutCancel(t.Context()))) }()
								call := gcScanCall{kind: "claim", name: fmt.Sprintf("sparse-young-%02d", step), args: []any{gcPlanGCDelay.Microseconds(), gcPlanOrphanAge.Microseconds(), fixture.claimLimit}, limit: fixture.claimLimit, claimOffset: step * fixture.claimLimit, commit: true}
								if step == 0 && mode == "force_generic_plan" && index == "baseline" && os.Getenv("JETSTREAM_TEST_GC_SCAN_REQUIRE_RED_CONTROL") == "1" {
									record(gcScanMeasure(t, conn, tx, fixture, call, true))
								}
								report := gcScanMeasure(t, conn, tx, fixture, call, false)
								require.NoError(t, tx.Commit(t.Context()))
								report.Committed = true
								record(report)
							}()
							if step*fixture.claimLimit >= len(fixture.claims) {
								break
							}
						}
						tx := gcScanBegin(t, store, lease.Epoch())
						func() {
							defer func() { require.NoError(t, tx.Rollback(context.WithoutCancel(t.Context()))) }()
							for _, call := range []gcScanCall{
								{kind: "deleting", name: "resume-deleting", args: []any{fixture.claimLimit}, limit: fixture.claimLimit},
								{kind: "references", name: "four-reference-kinds", args: []any{[]int64{1, 2, 3, 4, 97, int64(rows)}}, limit: 6},
							} {
								record(gcScanMeasure(t, conn, tx, fixture, call, false))
							}
						}()
						// Claiming changes only state. Restore the small fixed ID set;
						// age timestamps, references and the large history stay intact.
						claimIDs := make([]int64, 0, len(fixture.claims))
						for _, id := range fixture.claims {
							claimIDs = append(claimIDs, int64(id))
						}
						tag, err := store.Pool().Exec(t.Context(), `UPDATE objects SET state=CASE WHEN object_id%2=1 THEN 'uploading' ELSE 'available' END WHERE object_id=ANY($1::bigint[]) AND state='deleting'`, claimIDs)
						require.NoError(t, err)
						require.EqualValues(t, len(fixture.claims), tag.RowsAffected())
						// Empty resume checks happen before every fresh claim.
						tx = gcScanBegin(t, store, lease.Epoch())
						func() {
							defer func() { require.NoError(t, tx.Rollback(context.WithoutCancel(t.Context()))) }()
							record(gcScanMeasure(t, conn, tx, fixture, gcScanCall{kind: "deleting", name: "empty-deleting", args: []any{fixture.claimLimit}, limit: fixture.claimLimit}, false))
						}()
					})
				}
			}
		})
	}
	gcScanWriteJSON(t, filepath.Join(directory, "summary.json"), reports)
	require.NotEmpty(t, reports, "fixture/setup failed before any plan was measured; artifacts: %s", directory)
	var redControls, totalViolations int
	redOperations := map[string]int{}
	maxRows := map[string]float64{}
	for _, report := range reports {
		if report.Control {
			if report.TargetRows > report.TargetBudget {
				redControls++
				redOperations[report.Operation]++
			}
			continue
		}
		if len(report.Violations) > 0 {
			totalViolations++
		}
		for _, work := range report.Work {
			maxRows[work.Scope] = max(maxRows[work.Scope], work.Rows)
		}
	}
	t.Logf("%d production plans exceed total-work budgets; maximum scan rows per node: candidate=%.0f target=%.0f reference=%.0f; artifacts: %s", totalViolations, maxRows["candidate"], maxRows["target"], maxRows["reference"], directory)
	for _, report := range reports {
		if report.Control {
			continue
		}
		label := fmt.Sprintf("rows=%d/%s/%s/%s/%s", report.Rows, report.Index, report.Mode, report.Operation, report.Scenario)
		if report.ExpectedUpdated > 0 {
			require.Positive(t, report.TargetRows, "%s updated rows require classified UPDATE-target scan work; artifacts: %s", label, directory)
		}
		require.LessOrEqual(t, report.TargetRows, report.TargetBudget, "%s UPDATE-target work; artifacts: %s", label, directory)
		if os.Getenv("JETSTREAM_TEST_GC_SCAN_STRICT_TOTAL") == "1" {
			require.Empty(t, report.Violations, "%s total-work gate; artifacts: %s", label, directory)
		}
	}
	if os.Getenv("JETSTREAM_TEST_GC_SCAN_REQUIRE_RED_CONTROL") == "1" {
		for _, operation := range []string{"mark", "claim"} {
			require.Positive(t, redOperations[operation], "pre-PR %s SQL must exceed target budget in at least one measured plan; artifacts: %s", operation, directory)
		}
	}
	t.Logf("recorded %d plans; %d pre-PR controls exceeded target budget", len(reports), redControls)
}

func gcScanStore(t *testing.T, url, mode string) (*pgstore.Store, *pgx.Conn) {
	t.Helper()
	store, err := pgstore.Open(t.Context(), pgstore.Config{URL: url, MaxConns: 1})
	require.NoError(t, err)
	t.Cleanup(store.Close)
	pooled, err := store.Pool().Acquire(t.Context())
	require.NoError(t, err)
	conn := pooled.Conn()
	_, err = conn.Exec(t.Context(), "SET plan_cache_mode="+mode)
	require.NoError(t, err)
	pooled.Release()
	// Tests run sequentially and MaxConns=1. Once Begin owns this backend,
	// using conn only between completed storage calls lets savepoints and
	// prepared-statement inspection share its exact transaction and cache.
	return store, conn
}

func gcScanBegin(t *testing.T, store *pgstore.Store, epoch uint64) catalog.Tx {
	t.Helper()
	tx, err := store.Begin(t.Context(), catalog.TxGC)
	require.NoError(t, err)
	_, ok, err := tx.FenceBump(t.Context(), epoch)
	require.NoError(t, err)
	require.True(t, ok)
	return tx
}

type gcScanCall struct {
	kind, name  string
	args        []any
	limit       int
	claimOffset int
	commit      bool
}

func gcScanWarm(t *testing.T, store *pgstore.Store, conn *pgx.Conn, epoch uint64, fixture gcScanFixture, mode string) {
	t.Helper()
	count := 1
	if mode == "auto" {
		count = 6
	}
	for range count {
		tx := gcScanBegin(t, store, epoch)
		func() {
			defer func() { require.NoError(t, tx.Rollback(context.WithoutCancel(t.Context()))) }()
			_, err := tx.MarkUnreferenced(t.Context(), 0, fixture.markLimit)
			require.NoError(t, err)
			_, err = tx.ClaimObjects(t.Context(), gcPlanGCDelay, gcPlanOrphanAge, fixture.claimLimit)
			require.NoError(t, err)
			_, err = tx.DeletingObjects(t.Context(), fixture.claimLimit)
			require.NoError(t, err)
			_, err = tx.ReferencedObjects(t.Context(), []uint64{1, 2, 3, 4, 97, uint64(fixture.rows)})
			require.NoError(t, err)
		}()
	}
	require.Equal(t, byte('I'), conn.PgConn().TxStatus())
}

func gcScanMeasure(t *testing.T, conn *pgx.Conn, tx catalog.Tx, fixture gcScanFixture, call gcScanCall, control bool) gcScanReport {
	t.Helper()
	_, err := conn.Exec(t.Context(), "SAVEPOINT gc_scan_before_call")
	require.NoError(t, err)
	before := readGCPlanStatements(t, conn)
	result := gcScanExecute(t, tx, fixture, call)
	gcScanCheckState(t, conn, call, result)
	after := readGCPlanStatements(t, conn)
	statement := gcScanCapture(t, before, after, call.kind)
	var mode string
	require.NoError(t, conn.QueryRow(t.Context(), "SELECT current_setting('plan_cache_mode')", pgx.QueryExecModeExec).Scan(&mode))
	checkGCPlanCounters(t, before, statement, mode)
	_, err = conn.Exec(t.Context(), "ROLLBACK TO SAVEPOINT gc_scan_before_call")
	require.NoError(t, err)
	queryName := statement.Name
	if control {
		cte := "page"
		if call.kind == "claim" {
			cte = "c"
		}
		predicate := "AND o.object_id = ANY (ARRAY(SELECT object_id FROM " + cte + "))"
		require.Equal(t, 1, strings.Count(statement.SQL, predicate), "red control must remove exactly the PR restriction from actual storage SQL")
		sql := strings.Replace(statement.SQL, predicate, "", 1)
		queryName = "gc_scan_pre_pr_" + call.kind
		_, err = conn.Prepare(t.Context(), queryName, sql)
		require.NoError(t, err)
	}
	arguments := gcScanSQLArguments(call.args)
	execute := "EXECUTE " + pgx.Identifier{queryName}.Sanitize() + "(" + strings.Join(arguments, ",") + ")"
	if control {
		gcScanCheckControl(t, conn, execute, call.kind, result)
		gcScanCheckState(t, conn, call, result)
		_, err = conn.Exec(t.Context(), "ROLLBACK TO SAVEPOINT gc_scan_before_call")
		require.NoError(t, err)
	}
	explainBefore := gcPlanStatementNamed(t, readGCPlanStatements(t, conn), queryName)
	var plan json.RawMessage
	err = conn.QueryRow(t.Context(), "EXPLAIN (ANALYZE,BUFFERS,WAL,TIMING OFF,FORMAT JSON) "+execute).Scan(&plan)
	require.NoError(t, err)
	gcScanCheckState(t, conn, call, result)
	explainAfter := gcPlanStatementNamed(t, readGCPlanStatements(t, conn), queryName)
	checkGCPlanCounters(t, []gcPlanStatement{explainBefore}, explainAfter, mode)
	work, err := analyzeGCPlanWork(plan)
	require.NoError(t, err)
	report := gcScanReport{Scenario: call.name, Operation: call.kind, Control: control, Statement: statement, ExplainBefore: explainBefore, ExplainAfter: explainAfter, Arguments: call.args, Result: result, Plan: plan, Work: work, TargetBudget: float64(2*call.limit + 64), TotalBudget: float64(8*call.limit + 64), BlockBudget: float64(64*call.limit + 1024)}
	switch value := result.(type) {
	case catalog.MarkPage:
		report.ExpectedUpdated = value.Marked
	case []uint64:
		if call.kind == "claim" {
			report.ExpectedUpdated = len(value)
		}
	}
	for _, item := range work {
		report.TotalRows += item.Rows
		report.SharedBlocks += item.SharedHitBlocks + item.SharedReadBlocks
		if item.Scope == "target" {
			report.TargetRows += item.Rows
		}
	}
	if report.TargetRows > report.TargetBudget {
		report.Violations = append(report.Violations, "UPDATE target exceeded row-work budget")
	}
	if report.TotalRows > report.TotalBudget {
		report.Violations = append(report.Violations, "candidate/target/reference work exceeded total row-work budget")
	}
	if report.SharedBlocks > report.BlockBudget {
		report.Violations = append(report.Violations, "relation work exceeded shared-buffer budget")
	}
	if control || call.commit {
		_, err = conn.Exec(t.Context(), "ROLLBACK TO SAVEPOINT gc_scan_before_call")
		require.NoError(t, err)
	}
	if call.commit && !control {
		beforeReplay := readGCPlanStatements(t, conn)
		require.Equal(t, result, gcScanExecute(t, tx, fixture, call), "committed storage call must match the measured pre-call result")
		gcScanCheckState(t, conn, call, result)
		afterReplay := gcScanCapture(t, beforeReplay, readGCPlanStatements(t, conn), call.kind)
		checkGCPlanCounters(t, beforeReplay, afterReplay, mode)
		report.CommitReplay = &afterReplay
	}
	_, err = conn.Exec(t.Context(), "RELEASE SAVEPOINT gc_scan_before_call")
	require.NoError(t, err)
	return report
}

func gcScanCheckState(t *testing.T, conn *pgx.Conn, call gcScanCall, result any) {
	t.Helper()
	switch call.kind {
	case "mark":
		page, ok := result.(catalog.MarkPage)
		require.True(t, ok, "mark result must be a MarkPage")
		var incorrect int
		err := conn.QueryRow(t.Context(), `SELECT count(*) FROM objects WHERE object_id>$1 AND object_id<=$2
 AND ((object_id%97=0 AND unreferenced_at IS NULL) OR (object_id%97<>0 AND unreferenced_at IS NOT NULL))`, call.args[0], int64(page.Last)).Scan(&incorrect)
		require.NoError(t, err)
		require.Zero(t, incorrect, "selected mark window must mark orphans and preserve all four reference kinds")
	case "claim":
		ids, ok := result.([]uint64)
		require.True(t, ok, "claim result must contain object IDs")
		values := make([]int64, 0, len(ids))
		for _, id := range ids {
			values = append(values, int64(id))
		}
		var deleting int
		err := conn.QueryRow(t.Context(), `SELECT count(*) FROM objects WHERE object_id=ANY($1::bigint[]) AND state='deleting'`, values).Scan(&deleting)
		require.NoError(t, err)
		require.Len(t, ids, deleting, "claim must persist deleting state for every returned object")
	}
}

func gcScanCheckControl(t *testing.T, conn *pgx.Conn, execute, kind string, want any) {
	t.Helper()
	if kind == "mark" {
		var got catalog.MarkPage
		var last pgtype.Int8
		err := conn.QueryRow(t.Context(), execute).Scan(&got.Scanned, &last, &got.Marked)
		require.NoError(t, err)
		got.Last = uint64(last.Int64)
		require.Equal(t, want, got, "pre-PR control must preserve actual storage result")
		return
	}
	rows, err := conn.Query(t.Context(), execute)
	require.NoError(t, err)
	defer rows.Close()
	ids := []uint64{}
	for rows.Next() {
		values, err := rows.Values()
		require.NoError(t, err)
		require.Len(t, values, 7, "control must preserve the complete pre-claim RETURNING row")
		value, ok := values[0].(int64)
		require.True(t, ok, "claim object_id must decode as int64")
		id := uint64(value)
		ids = append(ids, id)
		state := string(catalog.ObjectAvailable)
		if id%2 == 1 {
			state = string(catalog.ObjectUploading)
		}
		require.Equal(t, state, values[4])
	}
	require.NoError(t, rows.Err())
	// UPDATE RETURNING order is unspecified; storage sorts before returning.
	slices.Sort(ids)
	require.Equal(t, want, ids, "pre-PR control must preserve actual storage result")
}

func gcScanCapture(t *testing.T, before, after []gcPlanStatement, kind string) gcPlanStatement {
	t.Helper()
	if kind == "mark" || kind == "claim" {
		statement, err := captureGCPlanStatement(before, after, kind)
		require.NoError(t, err)
		checkGCPlanCounters(t, before, statement, "auto")
		return statement
	}
	var matches []gcPlanStatement
	for _, statement := range after {
		sql := strings.Join(strings.Fields(statement.SQL), " ")
		match := kind == "deleting" && strings.Contains(sql, "FROM objects WHERE state = 'deleting'") || kind == "references" && strings.HasPrefix(sql, "SELECT DISTINCT o.object_id FROM unnest(")
		if !match {
			continue
		}
		var previous gcPlanStatement
		for _, old := range before {
			if old.Name == statement.Name {
				previous = old
			}
		}
		if statement.Generic+statement.Custom-previous.Generic-previous.Custom == 1 {
			matches = append(matches, statement)
		}
	}
	require.Len(t, matches, 1, "capture exactly the %s statement executed by storage", kind)
	checkGCPlanCounters(t, before, matches[0], "auto")
	return matches[0]
}

func gcScanExecute(t *testing.T, tx catalog.Tx, fixture gcScanFixture, call gcScanCall) any {
	t.Helper()
	switch call.kind {
	case "mark":
		value, ok := call.args[0].(int64)
		require.True(t, ok, "mark cursor must be int64")
		after := int(value)
		want := catalog.MarkPage{}
		for id := after + 1; id <= fixture.base && want.Scanned < call.limit; id++ {
			want.Scanned++
			want.Last = uint64(id)
			if id%97 == 0 {
				want.Marked++
			}
		}
		got, err := tx.MarkUnreferenced(t.Context(), uint64(after), call.limit)
		require.NoError(t, err)
		require.Equal(t, want, got)
		return got
	case "claim":
		rows, err := tx.ClaimObjects(t.Context(), gcPlanGCDelay, gcPlanOrphanAge, call.limit)
		require.NoError(t, err)
		ids := make([]uint64, 0, len(rows))
		for _, row := range rows {
			ids = append(ids, row.ID)
			state := catalog.ObjectAvailable
			if row.ID%2 == 1 {
				state = catalog.ObjectUploading
			}
			require.Equal(t, state, row.State)
		}
		start := min(call.claimOffset, len(fixture.claims))
		end := min(start+call.limit, len(fixture.claims))
		require.Equal(t, fixture.claims[start:end], ids)
		return ids
	case "deleting":
		rows, err := tx.DeletingObjects(t.Context(), call.limit)
		require.NoError(t, err)
		ids := make([]uint64, 0, len(rows))
		for _, row := range rows {
			require.Equal(t, catalog.ObjectDeleting, row.State)
			ids = append(ids, row.ID)
		}
		want := []uint64{}
		if call.name == "resume-deleting" {
			want = fixture.claims[:min(call.limit, len(fixture.claims))]
		}
		require.Equal(t, want, ids)
		return ids
	case "references":
		ids, err := tx.ReferencedObjects(t.Context(), []uint64{1, 2, 3, 4, 97, uint64(fixture.rows)})
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 3, 4}, ids)
		return ids
	default:
		t.Fatalf("unknown operation %q", call.kind)
		return nil
	}
}

func gcScanSQLArguments(args []any) []string {
	out := make([]string, 0, len(args))
	for _, arg := range args {
		switch value := arg.(type) {
		case []int64:
			items := make([]string, 0, len(value))
			for _, id := range value {
				items = append(items, strconv.FormatInt(id, 10))
			}
			out = append(out, "ARRAY["+strings.Join(items, ",")+"]::bigint[]")
		default:
			out = append(out, fmt.Sprint(value))
		}
	}
	return out
}

func gcScanWriteReport(t *testing.T, directory string, report gcScanReport) {
	t.Helper()
	suffix := "production"
	if report.Control {
		suffix = "pre-pr-control"
	}
	name := fmt.Sprintf("%d-%s-%s-%s-%s-%s.json", report.Rows, report.Index, report.Mode, report.Operation, report.Scenario, suffix)
	gcScanWriteJSON(t, filepath.Join(directory, name), report)
}

func gcScanWriteJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0600))
}

func seedGCScanFixture(t *testing.T, store *pgstore.Store, rows int) gcScanFixture {
	t.Helper()
	f := gcScanFixture{rows: rows, base: rows - rows/10, markLimit: min(gcPlanMarkLimit, rows/2), claimLimit: min(gcPlanClaimLimit, rows/50)}
	// The fixed eligible count makes sweep length independent of catalog
	// size. Most candidate-index entries are young, including entries before
	// and after the sparse old objects; the terminal probe cannot stop early.
	oldStart := rows - 4*f.claimLimit - 2
	for id := oldStart + 1; id <= rows; id++ {
		if id%4 <= 1 {
			f.claims = append(f.claims, uint64(id))
		}
	}
	// Bound each statement for slower CI servers while sharing one now()
	// across the complete fixture.
	const batchSize = 20_000
	err := pgx.BeginFunc(t.Context(), store.Pool(), func(tx pgx.Tx) error {
		for first := 1; first <= rows; first += batchSize {
			last := min(first+batchSize-1, rows)
			tag, err := tx.Exec(t.Context(), `INSERT INTO objects(object_id,key,sha256,byte_length,state,created_at,unreferenced_at)
 SELECT id,md5('scan-key-'||id)::uuid,decode(md5('scan-sha-'||id)||md5('scan-sha2-'||id),'hex'),1000,
 CASE WHEN id>$3 AND id%2=1 THEN 'uploading' ELSE 'available' END,
 now()-CASE WHEN id>$4 AND id%4<=1 THEN interval '2 days' ELSE interval '10 minutes' END,
 CASE WHEN id>$3 AND id%2=0 THEN now()-CASE WHEN id>$4 AND id%4<=1 THEN interval '2 days' ELSE interval '10 minutes' END ELSE NULL END
 FROM generate_series($1::bigint,$2::bigint)id`, first, last, f.base, oldStart)
			if err != nil {
				return err
			}
			if want := int64(last - first + 1); tag.RowsAffected() != want {
				return fmt.Errorf("seed objects [%d,%d]: inserted %d rows, want %d", first, last, tag.RowsAffected(), want)
			}
		}
		_, err := tx.Exec(t.Context(), `INSERT INTO segments(namespace,segment_index,state,current_generation_id,revision) VALUES('main',0,'sealed',1,1),('main',1,'active',NULL,1)`)
		if err != nil {
			return err
		}
		// Each reference relation grows with the catalog, rather than placing
		// almost all history in generation_blocks and leaving the others tiny.
		for first := 1; first <= f.base; first += batchSize {
			last := min(first+batchSize-1, f.base)
			_, err := tx.Exec(t.Context(), `
 INSERT INTO segment_generations(generation_id,namespace,segment_index,header,footer_object_id,revision) SELECT object_id,'main',0,decode(repeat('00',256),'hex'),object_id,1 FROM objects WHERE object_id>=$1 AND object_id<=$2 AND object_id%4=1 AND object_id%97<>0;
 INSERT INTO generation_blocks(generation_id,ordinal,object_id,compressed_length) SELECT 1,object_id::integer,object_id,1000 FROM objects WHERE object_id>=$1 AND object_id<=$2 AND object_id%4=2 AND object_id%97<>0;
 INSERT INTO active_segment_blocks(namespace,segment_index,ordinal,object_id,event_count,min_seq,max_seq,min_witnessed_us,max_witnessed_us,compressed_length,uncompressed_length,revision) SELECT 'main',1,object_id::integer,object_id,1,object_id,object_id,1,1,1000,1000,1 FROM objects WHERE object_id>=$1 AND object_id<=$2 AND object_id%4=3 AND object_id%97<>0;
 INSERT INTO hot_batches(first_seq,last_seq,event_count,min_witnessed_us,max_witnessed_us,epoch,revision,object_id) SELECT object_id,object_id,1,1,1,1,1,object_id FROM objects WHERE object_id>=$1 AND object_id<=$2 AND object_id%4=0 AND object_id%97<>0;`, pgx.QueryExecModeSimpleProtocol, first, last)
			if err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	_, err = store.Pool().Exec(t.Context(), "ANALYZE")
	require.NoError(t, err)
	return f
}
