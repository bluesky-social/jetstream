package pgstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/pgstore"
	"github.com/bluesky-social/jetstream/internal/pgstore/pgtest"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

const (
	gcPlanMarkLimit  = 10_000
	gcPlanClaimLimit = 1_000
	gcPlanGCDelay    = 6 * time.Hour
	gcPlanOrphanAge  = time.Hour
)

type gcPlanStatement struct {
	Name    string `json:"Name"`
	SQL     string `json:"SQL"`
	PID     uint32 `json:"PID"`
	Generic int64  `json:"Generic"`
	Custom  int64  `json:"Custom"`
}

func TestGCPlanStatementCapture(t *testing.T) {
	t.Parallel()
	mark := gcPlanStatement{Name: "mark", SQL: "WITH page AS (SELECT 1), marked AS (UPDATE objects SET unreferenced_at = now()) SELECT 1", PID: 7, Custom: 1}
	claim := gcPlanStatement{Name: "claim", SQL: "WITH c AS (SELECT 1) UPDATE objects SET state = 'deleting'", PID: 7, Custom: 1}
	for _, tc := range []struct {
		name          string
		before, after []gcPlanStatement
		kind          string
		wantName      string
	}{
		{name: "missing", kind: "mark"},
		{name: "unrelated query", after: []gcPlanStatement{{Name: "other", SQL: "SELECT 1", PID: 7, Custom: 1}}, kind: "mark"},
		{name: "unchanged counter", before: []gcPlanStatement{mark}, after: []gcPlanStatement{mark}, kind: "mark"},
		{name: "changed backend", before: []gcPlanStatement{mark}, after: []gcPlanStatement{{Name: mark.Name, SQL: mark.SQL, PID: 8, Custom: 2}}, kind: "mark"},
		{name: "ambiguous", after: []gcPlanStatement{mark, {Name: "second", SQL: mark.SQL, PID: 7, Custom: 1}}, kind: "mark"},
		{name: "counter reset", before: []gcPlanStatement{{Name: mark.Name, SQL: mark.SQL, PID: 7, Generic: 2}}, after: []gcPlanStatement{{Name: mark.Name, SQL: mark.SQL, PID: 7, Custom: 3}}, kind: "mark"},
		{name: "multiple executions", after: []gcPlanStatement{{Name: mark.Name, SQL: mark.SQL, PID: 7, Custom: 2}}, kind: "mark"},
		{name: "statement SQL changed", before: []gcPlanStatement{mark}, after: []gcPlanStatement{{Name: mark.Name, SQL: mark.SQL + " ", PID: 7, Custom: 2}}, kind: "mark"},
		{name: "mixed backend snapshot", before: []gcPlanStatement{mark, {Name: "other", SQL: "SELECT 1", PID: 8}}, after: []gcPlanStatement{{Name: mark.Name, SQL: mark.SQL, PID: 8, Custom: 2}}, kind: "mark"},
		{name: "mark", after: []gcPlanStatement{mark, claim}, kind: "mark", wantName: "mark"},
		{name: "claim", after: []gcPlanStatement{mark, claim}, kind: "claim", wantName: "claim"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := captureGCPlanStatement(tc.before, tc.after, tc.kind)
			if tc.wantName == "" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantName, got.Name)
		})
	}
}

func captureGCPlanStatement(before, after []gcPlanStatement, kind string) (gcPlanStatement, error) {
	previous := make(map[string]gcPlanStatement, len(before))
	var pid uint32
	for _, statement := range before {
		if pid != 0 && statement.PID != pid {
			return gcPlanStatement{}, fmt.Errorf("capture %s: mixed backends in snapshot", kind)
		}
		previous[statement.Name] = statement
		pid = statement.PID
	}
	var matches []gcPlanStatement
	for _, statement := range after {
		if pid == 0 {
			pid = statement.PID
		}
		if pid != 0 && statement.PID != pid {
			return gcPlanStatement{}, fmt.Errorf("capture %s: backend changed from %d to %d", kind, pid, statement.PID)
		}
		sql := strings.Join(strings.Fields(statement.SQL), " ")
		var match bool
		switch kind {
		case "mark":
			match = strings.HasPrefix(sql, "WITH page AS (") && strings.Contains(sql, "marked AS (") && strings.Contains(sql, "UPDATE objects ") && strings.Contains(sql, "SET unreferenced_at = now()")
		case "claim":
			match = strings.HasPrefix(sql, "WITH c AS (") && strings.Contains(sql, "UPDATE objects ") && strings.Contains(sql, "SET state = 'deleting'")
		default:
			return gcPlanStatement{}, fmt.Errorf("unknown GC operation %q", kind)
		}
		if !match {
			continue
		}
		old := previous[statement.Name]
		if old.SQL != "" && old.SQL != statement.SQL {
			return gcPlanStatement{}, fmt.Errorf("capture %s: prepared statement SQL changed", kind)
		}
		if statement.Generic < old.Generic || statement.Custom < old.Custom {
			return gcPlanStatement{}, fmt.Errorf("capture %s: prepared plan counters decreased", kind)
		}
		delta := statement.Generic + statement.Custom - old.Generic - old.Custom
		if delta != 1 {
			continue
		}
		matches = append(matches, statement)
	}
	if len(matches) != 1 {
		return gcPlanStatement{}, fmt.Errorf("capture %s: found %d statements executed exactly once", kind, len(matches))
	}
	return matches[0], nil
}

// TestGCQueryPlans is opt-in because realistic generic-plan choices need a
// much larger catalog than the normal storage suite. It records evidence,
// rather than requiring PostgreSQL to choose a particular plan.
//
//nolint:paralleltest // Serial execution avoids competing for PostgreSQL resources during plan measurements.
func TestGCQueryPlans(t *testing.T) {
	testGCQueryPlans(t, false)
}

//nolint:paralleltest // Plan measurements should not compete for PostgreSQL resources.
func TestGCQueryPlansMixedMark(t *testing.T) {
	testGCQueryPlans(t, true)
}

func testGCQueryPlans(t *testing.T, mixedMark bool) {
	t.Helper()
	raw := os.Getenv("JETSTREAM_TEST_GC_PLAN_ROWS")
	if raw == "" {
		t.Skip("set JETSTREAM_TEST_GC_PLAN_ROWS to run GC plan diagnostics")
	}
	rows, err := strconv.Atoi(raw)
	require.NoError(t, err)
	require.GreaterOrEqual(t, rows, 1000)
	require.LessOrEqual(t, rows, 20_000_000)
	seed, url := pgtest.Open(t, nil)
	fixture := seedGCPlanFixture(t, seed, rows, mixedMark)
	lease := seed.NewLease()
	require.NoError(t, lease.Acquire(t.Context(), time.Hour))
	baseDirectory := os.Getenv("JETSTREAM_TEST_GC_PLAN_ARTIFACT_DIR")
	if baseDirectory != "" {
		require.NoError(t, os.MkdirAll(baseDirectory, 0700))
	}
	directory, err := os.MkdirTemp(baseDirectory, "jetstream-gc-plans-")
	require.NoError(t, err)
	t.Logf("GC plan artifacts: %s", directory)
	for _, index := range []string{"baseline", "candidate"} {
		if index == "candidate" {
			_, err = seed.Pool().Exec(t.Context(), `CREATE INDEX gc_plan_claim_candidates ON objects(object_id) WHERE state='uploading' OR (state='available' AND unreferenced_at IS NOT NULL)`)
			require.NoError(t, err)
		}
		for _, kind := range []string{"mark", "claim"} {
			for _, mode := range []string{"force_custom_plan", "force_generic_plan", "auto"} {
				t.Run(index+"/"+kind+"/"+mode, func(t *testing.T) {
					store, err := pgstore.Open(t.Context(), pgstore.Config{URL: url, MaxConns: 1})
					require.NoError(t, err)
					t.Cleanup(store.Close)
					conn, err := store.Pool().Acquire(t.Context())
					require.NoError(t, err)
					_, err = conn.Exec(t.Context(), "SET plan_cache_mode="+mode)
					require.NoError(t, err)
					pid := conn.Conn().PgConn().PID()
					server, settings := gcPlanConnectionInfo(t, conn.Conn())
					require.Equal(t, mode, settings["plan_cache_mode"])
					conn.Release()
					report := gcPlanReport{Operation: kind, CacheMode: mode, FixtureRows: rows, MixedMark: mixedMark, CandidateIndex: index == "candidate", ServerVersion: server, Settings: settings, PassedArguments: gcPlanArguments(kind)}
					stateBefore := gcPlanFixtureState(t, store)
					executions := 3
					if mode == "auto" {
						executions = 8
					}
					var statement gcPlanStatement
					for range executions {
						before := gcPlanSnapshot(t, store)
						start := time.Now()
						tx, err := store.Begin(t.Context(), catalog.TxGC)
						require.NoError(t, err)
						func() {
							defer func() { require.NoError(t, tx.Rollback(context.WithoutCancel(t.Context()))) }()
							_, ok, err := tx.FenceBump(t.Context(), lease.Epoch())
							require.NoError(t, err)
							require.True(t, ok)
							queryStart := time.Now()
							count := fixture.check(t, tx, kind)
							elapsed := time.Since(queryStart)
							require.NoError(t, tx.Rollback(t.Context()))
							transactionElapsed := time.Since(start)
							after := gcPlanSnapshot(t, store)
							statement, err = captureGCPlanStatement(before, after, kind)
							require.NoError(t, err)
							require.Equal(t, pid, statement.PID)
							checkGCPlanCounters(t, before, statement, mode)
							report.Executions = append(report.Executions, gcPlanExecution{Statement: statement, Result: count, ClientCallAndCheckMS: float64(elapsed) / float64(time.Millisecond), TransactionAndCheckMS: float64(transactionElapsed) / float64(time.Millisecond)})
						}()
					}
					report.Statement = statement
					report.Plan, report.ExplainBefore, report.ExplainAfter = explainGCPlan(t, store, statement, kind)
					checkGCPlanCounters(t, []gcPlanStatement{report.ExplainBefore}, report.ExplainAfter, mode)
					require.Equal(t, stateBefore, gcPlanFixtureState(t, store), "rollback must restore archive revision, NULL unreferenced timestamps and deleting state")
					b, err := json.MarshalIndent(report, "", "  ")
					require.NoError(t, err)
					name := index + "-" + kind + "-" + mode + ".json"
					require.NoError(t, os.WriteFile(filepath.Join(directory, name), b, 0600))
					t.Logf("%s: pid=%d executions=%d custom=%d generic=%d artifact=%s", kind, pid, executions, statement.Custom, statement.Generic, filepath.Join(directory, name))
				})
			}
		}
	}
}

type gcPlanExecution struct {
	Statement             gcPlanStatement `json:"Statement"`
	Result                gcPlanResult    `json:"Result"`
	ClientCallAndCheckMS  float64         `json:"ClientCallAndCheckMS"`
	TransactionAndCheckMS float64         `json:"TransactionAndCheckMS"`
}

func checkGCPlanCounters(t *testing.T, before []gcPlanStatement, after gcPlanStatement, mode string) {
	t.Helper()
	var previous gcPlanStatement
	for _, statement := range before {
		if statement.Name == after.Name {
			previous = statement
			break
		}
	}
	generic := after.Generic - previous.Generic
	custom := after.Custom - previous.Custom
	require.GreaterOrEqual(t, generic, int64(0))
	require.GreaterOrEqual(t, custom, int64(0))
	require.Equal(t, int64(1), generic+custom, "each call must execute the existing statement once")
	switch mode {
	case "force_custom_plan":
		require.Equal(t, int64(1), custom)
	case "force_generic_plan":
		require.Equal(t, int64(1), generic)
	}
}

type gcPlanReport struct {
	Operation       string            `json:"Operation"`
	CacheMode       string            `json:"CacheMode"`
	FixtureRows     int               `json:"FixtureRows"`
	MixedMark       bool              `json:"MixedMark"`
	CandidateIndex  bool              `json:"CandidateIndex"`
	ServerVersion   string            `json:"ServerVersion"`
	Settings        map[string]string `json:"Settings"`
	PassedArguments []int64           `json:"PassedArguments"`
	Statement       gcPlanStatement   `json:"Statement"`
	Executions      []gcPlanExecution `json:"Executions"`
	ExplainBefore   gcPlanStatement   `json:"ExplainBefore"`
	ExplainAfter    gcPlanStatement   `json:"ExplainAfter"`
	Plan            json.RawMessage   `json:"Plan"`
}

func gcPlanSnapshot(t *testing.T, store *pgstore.Store) []gcPlanStatement {
	t.Helper()
	conn, err := store.Pool().Acquire(t.Context())
	require.NoError(t, err)
	defer conn.Release()
	return readGCPlanStatements(t, conn.Conn())
}

func readGCPlanStatements(t *testing.T, conn *pgx.Conn) []gcPlanStatement {
	t.Helper()
	pid := conn.PgConn().PID()
	// Retain backend identity even before the first statement is prepared.
	statements := []gcPlanStatement{{PID: pid}}
	rows, err := conn.Query(t.Context(), `SELECT name,statement,generic_plans,custom_plans FROM pg_prepared_statements`, pgx.QueryExecModeExec)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		statement := gcPlanStatement{PID: pid}
		require.NoError(t, rows.Scan(&statement.Name, &statement.SQL, &statement.Generic, &statement.Custom))
		statements = append(statements, statement)
	}
	require.NoError(t, rows.Err())
	return statements
}

func gcPlanStatementNamed(t *testing.T, statements []gcPlanStatement, name string) gcPlanStatement {
	t.Helper()
	for _, statement := range statements {
		if statement.Name == name {
			return statement
		}
	}
	t.Fatalf("prepared statement %q disappeared", name)
	return gcPlanStatement{}
}

func explainGCPlan(t *testing.T, store *pgstore.Store, statement gcPlanStatement, kind string) (json.RawMessage, gcPlanStatement, gcPlanStatement) {
	t.Helper()
	conn, err := store.Pool().Acquire(t.Context())
	require.NoError(t, err)
	defer conn.Release()
	require.Equal(t, statement.PID, conn.Conn().PgConn().PID())
	before := gcPlanStatementNamed(t, readGCPlanStatements(t, conn.Conn()), statement.Name)
	tx, err := conn.Begin(t.Context())
	require.NoError(t, err)
	defer func() {
		err := tx.Rollback(context.WithoutCancel(t.Context()))
		if !errors.Is(err, pgx.ErrTxClosed) {
			require.NoError(t, err)
		}
	}()
	var arguments []string
	for _, argument := range gcPlanArguments(kind) {
		arguments = append(arguments, strconv.FormatInt(argument, 10))
	}
	query := `EXPLAIN (ANALYZE,BUFFERS,WAL,TIMING OFF,FORMAT JSON) EXECUTE ` + pgx.Identifier{statement.Name}.Sanitize() + `(` + strings.Join(arguments, ",") + `)`
	var plan json.RawMessage
	require.NoError(t, tx.QueryRow(t.Context(), query).Scan(&plan))
	require.NoError(t, tx.Rollback(t.Context()))
	after := gcPlanStatementNamed(t, readGCPlanStatements(t, conn.Conn()), statement.Name)
	require.Equal(t, statement.SQL, after.SQL)
	return plan, before, after
}

type gcPlanFixture struct {
	mark   catalog.MarkPage
	claims []uint64
}

type gcPlanResult struct {
	Rows     int             `json:"Rows"`
	MarkPage *gcPlanMarkPage `json:"MarkPage,omitempty"`
}

type gcPlanMarkPage struct {
	Scanned int    `json:"Scanned"`
	Last    uint64 `json:"Last"`
	Marked  int    `json:"Marked"`
}

func (f gcPlanFixture) check(t *testing.T, tx catalog.Tx, kind string) gcPlanResult {
	t.Helper()
	switch kind {
	case "mark":
		page, err := tx.MarkUnreferenced(t.Context(), 0, gcPlanMarkLimit)
		require.NoError(t, err)
		require.Equal(t, f.mark, page)
		return gcPlanResult{Rows: 1, MarkPage: &gcPlanMarkPage{Scanned: page.Scanned, Last: page.Last, Marked: page.Marked}}
	case "claim":
		rows, err := tx.ClaimObjects(t.Context(), gcPlanGCDelay, gcPlanOrphanAge, gcPlanClaimLimit)
		require.NoError(t, err)
		ids := make([]uint64, len(rows))
		for i, row := range rows {
			ids[i] = row.ID
			state := catalog.ObjectAvailable
			if row.ID%8 == 1 {
				state = catalog.ObjectUploading
			}
			require.Equal(t, state, row.State, "RETURNING must report the preclaim state")
		}
		require.Equal(t, f.claims, ids)
		return gcPlanResult{Rows: len(rows)}
	default:
		t.Fatalf("unknown GC operation %q", kind)
		return gcPlanResult{}
	}
}

func seedGCPlanFixture(t *testing.T, store *pgstore.Store, rows int, mixedMark bool) gcPlanFixture {
	t.Helper()
	var unreferencedPrefix int
	if mixedMark {
		// Exercise actual marks in the first page, not only reference rejection.
		unreferencedPrefix = gcPlanMarkLimit
	}
	base := rows - rows/10
	_, err := store.Pool().Exec(t.Context(), `INSERT INTO objects(object_id,key,sha256,byte_length,state,created_at,unreferenced_at)
 SELECT id,md5('key-'||id)::uuid,decode(md5('sha-'||id)||md5('sha2-'||id),'hex'),1000,
 CASE WHEN id>$2 AND id%8 IN (1,3) THEN 'uploading' ELSE 'available' END,
 now()-CASE WHEN id>$2 AND id%8=3 THEN interval '10 minutes' ELSE interval '2 days' END,
 CASE WHEN id>$2 AND id%8=0 THEN now()-interval '2 days' WHEN id>$2 AND id%8=2 THEN now()-interval '10 minutes' ELSE NULL END
 FROM generate_series(1,$1::bigint)id`, rows, base)
	require.NoError(t, err)
	_, err = store.Pool().Exec(t.Context(), fmt.Sprintf(`
 INSERT INTO segments(namespace,segment_index,state,current_generation_id,revision) VALUES('main',0,'sealed',1,1),('main',1,'active',NULL,1);
 INSERT INTO segment_generations(generation_id,namespace,segment_index,header,footer_object_id,revision) VALUES(1,'main',0,decode(repeat('00',256),'hex'),1,1);
 INSERT INTO generation_blocks(generation_id,ordinal,object_id,compressed_length) SELECT 1,(row_number() OVER(ORDER BY object_id)-1)::integer,object_id,1000 FROM objects WHERE object_id<>1 AND NOT (object_id<=%d AND object_id%%10=0) AND (object_id<=%d OR object_id%%8=7);
 INSERT INTO active_segment_blocks(namespace,segment_index,ordinal,object_id,event_count,min_seq,max_seq,min_witnessed_us,max_witnessed_us,compressed_length,uncompressed_length,revision) SELECT 'main',1,(row_number() OVER(ORDER BY object_id)-1)::integer,object_id,1,object_id,object_id,1,1,1000,1000,1 FROM objects WHERE object_id>%d AND object_id%%8=5;
 INSERT INTO hot_batches(first_seq,last_seq,event_count,min_witnessed_us,max_witnessed_us,epoch,revision,object_id) SELECT object_id,object_id,1,1,1,1,1,object_id FROM objects WHERE object_id>%d AND object_id%%8=6;
 ANALYZE;`, unreferencedPrefix, base, base, base))
	require.NoError(t, err)
	var fixture gcPlanFixture
	for id := 1; id <= rows; id++ {
		if (id <= base || id%8 >= 4) && fixture.mark.Scanned < gcPlanMarkLimit {
			fixture.mark.Scanned++
			fixture.mark.Last = uint64(id)
			if (id <= base && id <= unreferencedPrefix && id%10 == 0) || (id > base && id%8 == 4) {
				fixture.mark.Marked++
			}
		}
		if id > base && (id%8 == 0 || id%8 == 1) && len(fixture.claims) < gcPlanClaimLimit {
			fixture.claims = append(fixture.claims, uint64(id))
		}
	}
	return fixture
}

func gcPlanArguments(kind string) []int64 {
	if kind == "mark" {
		return []int64{0, gcPlanMarkLimit}
	}
	return []int64{gcPlanGCDelay.Microseconds(), gcPlanOrphanAge.Microseconds(), gcPlanClaimLimit}
}

func gcPlanConnectionInfo(t *testing.T, conn *pgx.Conn) (string, map[string]string) {
	t.Helper()
	var version string
	require.NoError(t, conn.QueryRow(t.Context(), "SELECT version()", pgx.QueryExecModeExec).Scan(&version))
	rows, err := conn.Query(t.Context(), `SELECT name,setting FROM pg_settings WHERE name IN ('plan_cache_mode','work_mem','shared_buffers','random_page_cost','seq_page_cost','jit','statement_timeout','lock_timeout','default_statistics_target')`, pgx.QueryExecModeExec)
	require.NoError(t, err)
	defer rows.Close()
	settings := map[string]string{}
	for rows.Next() {
		var name, value string
		require.NoError(t, rows.Scan(&name, &value))
		settings[name] = value
	}
	require.NoError(t, rows.Err())
	return version, settings
}

func gcPlanFixtureState(t *testing.T, store *pgstore.Store) [3]int64 {
	t.Helper()
	var state [3]int64
	require.NoError(t, store.Pool().QueryRow(t.Context(), `SELECT (SELECT catalog_revision FROM archive WHERE id=1),count(*) FILTER (WHERE unreferenced_at IS NULL),count(*) FILTER (WHERE state='deleting') FROM objects`).Scan(&state[0], &state[1], &state[2]))
	return state
}
