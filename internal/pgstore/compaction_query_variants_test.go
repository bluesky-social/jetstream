package pgstore_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

type compactionQuerySample struct {
	Rows        int
	Blocks      int
	Shape       string
	Mode        string
	Operation   string
	Variant     string
	Repetition  int
	Arguments   []string
	SQL         string
	Statement   gcPlanStatement
	Statements  []gcPlanStatement
	Before      gcPlanStatement
	After       gcPlanStatement
	ClientMS    float64
	ExecutionMS float64
	PlanningMS  float64
	SharedHits  int64
	SharedReads int64
	WALBytes    int64
	Triggers    json.RawMessage
	Plan        json.RawMessage
}

type compactionQueryFixture struct {
	publish      catalog.Publish
	sourceHeader []byte
	sourceFooter uint64
	ids          []int64
	pending      []int64
	shas         [][]byte
}

// TestCompactionQueryVariants measures committed production publications, then
// replays their captured read SQL. Variant times are isolated query times, not
// publication improvements. Synthetic unrelated catalog rows carry no blobs;
// the source and rewrite use actual encoded blocks and SparseRewrite headers.
//
//nolint:paralleltest // Plan/cache measurements share a PostgreSQL server and seeded catalog.
func TestCompactionQueryVariants(t *testing.T) {
	raw := os.Getenv("JETSTREAM_TEST_COMPACTION_QUERY_ROWS")
	if raw == "" {
		t.Skip("set JETSTREAM_TEST_COMPACTION_QUERY_ROWS to run compaction query comparisons")
	}
	batches := "680,4000"
	if value := os.Getenv("JETSTREAM_TEST_COMPACTION_QUERY_BATCHES"); value != "" {
		batches = value
	}
	reps := 3
	if value := os.Getenv("JETSTREAM_TEST_COMPACTION_QUERY_REPS"); value != "" {
		var err error
		reps, err = strconv.Atoi(value)
		require.NoError(t, err)
	}
	require.GreaterOrEqual(t, reps, 3)
	require.LessOrEqual(t, reps, 5)
	base := os.Getenv("JETSTREAM_TEST_COMPACTION_QUERY_ARTIFACT_DIR")
	if base != "" {
		require.NoError(t, os.MkdirAll(base, 0700))
	}
	directory, err := os.MkdirTemp(base, "jetstream-compaction-queries-")
	require.NoError(t, err)
	t.Logf("compaction query artifacts: %s", directory)
	var reports []compactionQuerySample
	record := func(sample compactionQuerySample) {
		reports = append(reports, sample)
		gcScanWriteJSON(t, filepath.Join(directory, fmt.Sprintf("sample-%04d.json", len(reports))), sample)
	}
	for _, size := range strings.Split(raw, ",") {
		rows, err := strconv.Atoi(strings.TrimSpace(size))
		require.NoError(t, err)
		require.GreaterOrEqual(t, rows, 10000)
		require.LessOrEqual(t, rows, 2000000)
		t.Run(size, func(t *testing.T) {
			seed, url := pgtest.Open(t, nil)
			_, err := seed.Pool().Exec(t.Context(), `INSERT INTO objects(object_id,key,sha256,byte_length,state)
    SELECT id,md5('cq-key-'||id)::uuid,decode(md5('cq-a-'||id)||md5('cq-b-'||id),'hex'),1000,'available' FROM generate_series(1,$1::bigint) id`, rows)
			require.NoError(t, err)
			_, err = seed.Pool().Exec(t.Context(), `CREATE INDEX objects_gc_claim_candidates_id ON objects(object_id) WHERE state='uploading' OR (state='available' AND unreferenced_at IS NOT NULL)`)
			require.NoError(t, err)
			lease := seed.NewLease()
			require.NoError(t, lease.Acquire(t.Context(), time.Hour))
			for _, batch := range strings.Split(batches, ",") {
				blocks, err := strconv.Atoi(strings.TrimSpace(batch))
				require.NoError(t, err)
				require.Positive(t, blocks)
				require.Less(t, 4*blocks+10, rows)
				for _, shape := range []string{"one-changed", "all-changed"} {
					fixture := seedCompactionQueryFixture(t, seed, rows, blocks, shape)
					for _, mode := range []string{"force_custom_plan", "force_generic_plan", "auto"} {
						t.Run(fmt.Sprintf("%d/%s/%s", blocks, shape, mode), func(t *testing.T) {
							store, conn := gcScanStore(t, url, mode)
							other, err := pgx.Connect(t.Context(), url)
							require.NoError(t, err)
							defer func() { require.NoError(t, other.Close(context.WithoutCancel(t.Context()))) }()
							var captured []gcPlanStatement
							warmups := 1
							if mode == "auto" {
								warmups = 6
							}
							// Warm the same publication statements/backend before timing. Each
							// committed publish is restored outside its measured client interval.
							for rep := -warmups; rep < reps; rep++ {
								before := gcPlanSnapshot(t, store)
								session := catalog.NewSession(catalog.SessionConfig{DB: store, Epoch: lease.Epoch()})
								start := time.Now()
								result, err := session.PublishGeneration(t.Context(), fixture.publish)
								elapsed := time.Since(start)
								require.NoError(t, err)
								after := gcPlanSnapshot(t, store)
								captured = compactionExecutedStatements(before, after)
								if rep >= 0 {
									record(compactionQuerySample{Rows: rows, Blocks: blocks, Shape: shape, Mode: mode, Operation: "PublishGeneration", Variant: "production-committed", Repetition: rep, ClientMS: float64(elapsed) / float64(time.Millisecond), Statements: captured})
								}
								func() {
									pooled, err := store.Pool().Acquire(t.Context())
									require.NoError(t, err)
									defer pooled.Release()
									require.Equal(t, conn.PgConn().PID(), pooled.Conn().PgConn().PID())
									checkCompactionPublication(t, pooled.Conn(), fixture, result)
									restoreCompactionQueryFixture(t, pooled.Conn(), fixture, result.GenerationID)
								}()
							}
							// Raw replay owns its backend exclusively; the pool cannot run
							// health checks or lend it out during these transactions.
							pooled, err := store.Pool().Acquire(t.Context())
							require.NoError(t, err)
							defer pooled.Release()
							require.Equal(t, conn.PgConn().PID(), pooled.Conn().PgConn().PID())
							conn = pooled.Conn()
							server, settings := gcPlanConnectionInfo(t, conn)
							indexRows, err := conn.Query(t.Context(), `SELECT indexdef FROM pg_indexes WHERE schemaname='public' ORDER BY tablename,indexname`, pgx.QueryExecModeExec)
							require.NoError(t, err)
							indexes, err := pgx.CollectRows(indexRows, pgx.RowTo[string])
							require.NoError(t, err)
							gcScanWriteJSON(t, filepath.Join(directory, fmt.Sprintf("metadata-%d-%d-%s-%s.json", rows, blocks, shape, mode)), map[string]any{"ServerVersion": server, "Settings": settings, "Indexes": indexes})
							for _, operation := range []string{"objects", "available", "source-blocks", "delete-generation"} {
								statement := compactionQueryStatement(t, captured, operation)
								args := compactionQueryArguments(fixture, operation)
								variants := []struct{ name, sql string }{{"production", statement.SQL}}
								if operation == "objects" || operation == "available" {
									variants = append(variants, struct{ name, sql string }{"unnest-distinct", compactionUnnestSQL(t, statement.SQL, operation)})
								}
								// Duplicate lookup inputs must behave as ANY membership, rather
								// than multiplying rows through the join variant.
								if operation == "objects" {
									args[0] = append(append([]int64(nil), fixture.ids...), fixture.ids[0])
								}
								if operation == "available" {
									args[0] = append(append([][]byte(nil), fixture.shas...), fixture.shas[0])
								}
								for _, lookup := range []string{"miss", "winner"} {
									if operation != "available" && lookup == "winner" {
										continue
									}
									if lookup == "winner" {
										seedCompactionQueryWinners(t, conn, fixture, rows)
									}
									for _, age := range []int64{0, int64((3 * time.Hour) / time.Microsecond)} {
										if operation != "available" && age != 0 {
											continue
										}
										if operation == "available" {
											args[1] = age
										}
										var want [][]any
										for rep := -warmups; rep < reps; rep++ {
											order := []int{0}
											if len(variants) > 1 {
												order = append(order, 1)
												if rep >= 0 && rep%2 == 1 {
													order = []int{1, 0}
												}
											}
											for _, index := range order {
												variant := variants[index]
												name := fmt.Sprintf("cq_%s_%s", strings.ReplaceAll(operation, "-", "_"), strings.ReplaceAll(variant.name, "-", "_"))
												_, err := conn.Prepare(t.Context(), name, variant.sql)
												require.NoError(t, err)
												got, sample := measureCompactionQuery(t, conn, other, name, args, operation, blocks, rep >= 0)
												checkCompactionQueryLookup(t, got, fixture, operation, lookup, rows, age)
												if want == nil {
													want = got
												} else {
													require.Equal(t, want, got, "%s/%s results", operation, variant.name)
												}
												if rep >= 0 {
													sample.Rows, sample.Blocks, sample.Shape, sample.Mode = rows, blocks, shape, mode
													sample.Operation = fmt.Sprintf("%s/%s/age-%d", operation, lookup, age)
													sample.Variant, sample.Repetition, sample.SQL = variant.name, rep, variant.sql
													sample.Statement = statement
													record(sample)
												}
											}
										}
									}
									if lookup == "winner" {
										clearCompactionQueryWinners(t, conn, fixture, rows)
									}
								}
							}
						})
					}
				}
			}
		})
	}
	require.NotEmpty(t, reports)
	gcScanWriteJSON(t, filepath.Join(directory, "summary.json"), reports)
}

func compactionExecutedStatements(before, after []gcPlanStatement) []gcPlanStatement {
	old := map[string]int64{}
	for _, s := range before {
		old[s.Name] = s.Generic + s.Custom
	}
	var out []gcPlanStatement
	for _, s := range after {
		if s.Generic+s.Custom > old[s.Name] {
			out = append(out, s)
		}
	}
	return out
}

func compactionQueryStatement(t *testing.T, statements []gcPlanStatement, operation string) gcPlanStatement {
	t.Helper()
	var matches []gcPlanStatement
	for _, s := range statements {
		sql := strings.Join(strings.Fields(s.SQL), " ")
		match := false
		switch operation {
		case "objects":
			match = strings.Contains(sql, "FROM objects WHERE object_id = ANY($1::bigint[]) ORDER BY object_id FOR UPDATE")
		case "available":
			match = strings.Contains(sql, "FROM objects WHERE sha256 = ANY($1::bytea[])")
		case "source-blocks":
			match = strings.Contains(sql, "FROM generation_blocks WHERE generation_id = $1 ORDER BY ordinal")
		case "delete-generation":
			match = strings.Contains(sql, "DELETE FROM segment_generations WHERE generation_id = $1")
		}
		if match {
			matches = append(matches, s)
		}
	}
	require.Len(t, matches, 1, "capture production %s SQL from PublishGeneration", operation)
	return matches[0]
}

func compactionUnnestSQL(t *testing.T, sql, operation string) string {
	t.Helper()
	column, typ := "object_id", "bigint"
	if operation == "available" {
		column, typ = "sha256", "bytea"
	}
	before := "FROM objects\n\t\t\t WHERE " + column + " = ANY($1::" + typ + "[])"
	if operation == "objects" {
		before = "FROM objects WHERE object_id = ANY($1::bigint[])"
	}
	require.Equal(t, 1, strings.Count(sql, before))
	replacement := "FROM objects o JOIN (SELECT DISTINCT unnest($1::" + typ + "[]) AS " + column + ") wanted USING (" + column + ") WHERE true"
	sql = strings.Replace(sql, before, replacement, 1)
	require.Equal(t, 1, strings.Count(sql, "FOR UPDATE"))
	return strings.Replace(sql, "FOR UPDATE", "FOR UPDATE OF o", 1)
}

func compactionQueryArguments(f compactionQueryFixture, operation string) []any {
	switch operation {
	case "objects":
		return []any{f.ids}
	case "available":
		return []any{f.shas, int64(0)}
	default:
		return []any{int64(1)}
	}
}

func compactionExecuteArguments(args []any) []string {
	out := make([]string, len(args))
	for i, arg := range args {
		switch v := arg.(type) {
		case []int64:
			items := make([]string, len(v))
			for j, id := range v {
				items[j] = strconv.FormatInt(id, 10)
			}
			out[i] = "ARRAY[" + strings.Join(items, ",") + "]::bigint[]"
		case [][]byte:
			items := make([]string, len(v))
			for j, sha := range v {
				items[j] = "decode('" + hex.EncodeToString(sha) + "','hex')"
			}
			out[i] = "ARRAY[" + strings.Join(items, ",") + "]::bytea[]"
		default:
			out[i] = fmt.Sprint(v)
		}
	}
	return out
}

func checkCompactionQueryLookup(t *testing.T, got [][]any, f compactionQueryFixture, operation, lookup string, rows int, age int64) {
	t.Helper()
	if operation == "source-blocks" {
		require.Len(t, got, len(f.publish.Blocks))
		for i, row := range got {
			require.Len(t, row, 4)
			require.Equal(t, int64(i+1), row[2], "source block object at ordinal %d", i)
		}
		return
	}
	if operation != "objects" && operation != "available" {
		return
	}
	want := []int64{}
	if operation == "objects" {
		want = append(want, f.ids...)
	} else if lookup == "winner" {
		for i := range f.pending {
			ordinal := i + 1
			if age == 0 || ordinal%3 != 0 {
				want = append(want, int64(rows-ordinal))
			}
		}
	}
	slices.Sort(want)
	ids := make([]int64, 0, len(got))
	for _, row := range got {
		require.Len(t, row, 7)
		id, ok := row[0].(int64)
		require.True(t, ok, "object ID has bigint representation")
		ids = append(ids, id)
	}
	require.Equal(t, want, ids, "%s/%s must return the expected distinct object IDs in lock order", operation, lookup)
}

func checkCompactionQuerySource(t *testing.T, conn *pgx.Conn, generations, blocks int) {
	t.Helper()
	var gotGenerations, gotBlocks int
	require.NoError(t, conn.QueryRow(t.Context(), `SELECT
		(SELECT count(*) FROM segment_generations WHERE generation_id=1),
		(SELECT count(*) FROM generation_blocks WHERE generation_id=1)`, pgx.QueryExecModeExec).Scan(&gotGenerations, &gotBlocks))
	require.Equal(t, generations, gotGenerations)
	require.Equal(t, blocks, gotBlocks)
}

func measureCompactionQuery(t *testing.T, conn, other *pgx.Conn, name string, args []any, operation string, blocks int, measured bool) ([][]any, compactionQuerySample) {
	t.Helper()
	_, err := conn.Exec(t.Context(), "BEGIN")
	require.NoError(t, err)
	defer func() { _, err := conn.Exec(context.WithoutCancel(t.Context()), "ROLLBACK"); require.NoError(t, err) }()
	rows, err := conn.Query(t.Context(), name, args...)
	require.NoError(t, err)
	got := [][]any{}
	for rows.Next() {
		values, err := rows.Values()
		require.NoError(t, err)
		got = append(got, values)
	}
	require.NoError(t, rows.Err())
	rows.Close()
	if operation == "delete-generation" {
		checkCompactionQuerySource(t, conn, 0, 0)
	}
	if (operation == "objects" || operation == "available") && len(got) > 0 {
		var id int64
		err = other.QueryRow(t.Context(), `SELECT object_id FROM objects WHERE object_id=$1 FOR KEY SHARE NOWAIT`, got[0][0]).Scan(&id)
		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, "55P03", pgErr.Code)
	}
	// Restore even deletes before timing the same captured statement.
	_, err = conn.Exec(t.Context(), "ROLLBACK")
	require.NoError(t, err)
	if operation == "delete-generation" {
		checkCompactionQuerySource(t, conn, 1, blocks)
	}
	sample := compactionQuerySample{}
	if !measured {
		return got, sample
	}
	_, err = conn.Exec(t.Context(), "BEGIN")
	require.NoError(t, err)
	arguments := compactionExecuteArguments(args)
	before := gcPlanStatementNamed(t, readGCPlanStatements(t, conn), name)
	var plan json.RawMessage
	require.NoError(t, conn.QueryRow(t.Context(), "EXPLAIN (ANALYZE,BUFFERS,WAL,TIMING OFF,FORMAT JSON) EXECUTE "+pgx.Identifier{name}.Sanitize()+"("+strings.Join(arguments, ",")+")").Scan(&plan))
	if operation == "delete-generation" {
		checkCompactionQuerySource(t, conn, 0, 0)
	}
	after := gcPlanStatementNamed(t, readGCPlanStatements(t, conn), name)
	var mode string
	require.NoError(t, conn.QueryRow(t.Context(), "SHOW plan_cache_mode", pgx.QueryExecModeExec).Scan(&mode))
	checkGCPlanCounters(t, []gcPlanStatement{before}, after, mode)
	var explain []struct {
		ExecutionMS float64         `json:"Execution Time"`
		PlanningMS  float64         `json:"Planning Time"`
		Triggers    json.RawMessage `json:"Triggers"`
		Plan        struct {
			Hits     int64 `json:"Shared Hit Blocks"`
			Reads    int64 `json:"Shared Read Blocks"`
			WALBytes int64 `json:"WAL Bytes"`
		} `json:"Plan"`
	}
	require.NoError(t, json.Unmarshal(plan, &explain))
	require.Len(t, explain, 1)
	sample = compactionQuerySample{Arguments: arguments, Before: before, After: after, ExecutionMS: explain[0].ExecutionMS, PlanningMS: explain[0].PlanningMS, SharedHits: explain[0].Plan.Hits, SharedReads: explain[0].Plan.Reads, WALBytes: explain[0].Plan.WALBytes, Triggers: explain[0].Triggers, Plan: plan}
	return got, sample
}

func seedCompactionQueryFixture(t *testing.T, store *pgstore.Store, rows, blocks int, shape string) compactionQueryFixture {
	t.Helper()
	frames := make([][]byte, blocks)
	builder, err := segment.NewBlockBuilder(2)
	require.NoError(t, err)
	tombstones := map[segment.RecordKey]uint64{}
	for i := range blocks {
		for j := range 2 {
			seq := uint64(2*i + j + 1)
			_, err := builder.Append(segment.Event{Seq: seq, WitnessedAt: int64(seq), Kind: segment.KindCreate, DID: "did:plc:compaction-query", Collection: "app.bsky.feed.post", Rkey: strconv.FormatUint(seq, 10), Rev: "r", Payload: []byte{0xa0}})
			require.NoError(t, err)
		}
		frames[i], _ = builder.Encode()
		if shape == "all-changed" || i == 0 {
			tombstones[segment.RecordKey{DID: "did:plc:compaction-query", Collection: "app.bsky.feed.post", Rkey: strconv.Itoa(2*i + 1)}] = uint64(2*blocks + 1)
		}
	}
	header, footer, _, err := segment.BuildSealed(segment.SliceFrameSource(frames))
	require.NoError(t, err)
	rewritten, err := segment.SparseRewrite(header, footer, func(i int) ([]byte, error) { return frames[i], nil }, segment.NewTombstones(nil, tombstones, 0), segment.SparseOptions{})
	require.NoError(t, err)
	require.True(t, rewritten.Rewritten)
	f := compactionQueryFixture{publish: catalog.Publish{Segment: 0, Source: 1, Header: rewritten.HeaderBytes, Blocks: make([]catalog.PublishBlock, blocks)}, sourceHeader: header, sourceFooter: uint64(2*blocks + 1)}
	ids := make([]int64, 0, 2*blocks+2)
	shas := make([][]byte, 0, 2*blocks+2)
	lengths := make([]int64, 0, 2*blocks+2)
	add := func(id int64, data []byte) {
		sha := sha256.Sum256(data)
		ids = append(ids, id)
		shas = append(shas, sha[:])
		lengths = append(lengths, int64(len(data)))
	}
	for i, frame := range frames {
		add(int64(i+1), frame)
		f.publish.Blocks[i] = catalog.PublishBlock{Object: catalog.ObjectRef{ID: uint64(i + 1)}, Reused: true, CompressedLength: int64(len(frame))}
	}
	add(int64(f.sourceFooter), footer)
	for _, frame := range rewritten.Frames {
		id := int64(blocks + frame.Block + 1)
		add(id, frame.Frame)
		sha := sha256.Sum256(frame.Frame)
		f.publish.Blocks[frame.Block] = catalog.PublishBlock{Object: catalog.ObjectRef{ID: uint64(id), SHA256: sha, Pending: true}, CompressedLength: int64(len(frame.Frame))}
		f.pending = append(f.pending, id)
		f.shas = append(f.shas, sha[:])
	}
	footerID := int64(2*blocks + 2)
	add(footerID, rewritten.Footer)
	footerSHA := sha256.Sum256(rewritten.Footer)
	f.publish.Footer = catalog.ObjectRef{ID: uint64(footerID), SHA256: footerSHA, Pending: true}
	f.pending = append(f.pending, footerID)
	f.shas = append(f.shas, footerSHA[:])
	for _, b := range f.publish.Blocks {
		f.ids = append(f.ids, int64(b.Object.ID))
	}
	f.ids = append(f.ids, footerID)
	_, err = store.Pool().Exec(t.Context(), `DELETE FROM generation_blocks;DELETE FROM segment_generations;DELETE FROM segments;`, pgx.QueryExecModeSimpleProtocol)
	require.NoError(t, err)
	// Reset fixture rows in bounded statements, including after prior shapes' tuple churn.
	for first := 1; first <= rows; first += 20_000 {
		last := min(first+19_999, rows)
		tag, err := store.Pool().Exec(t.Context(), `UPDATE objects SET state='available',unreferenced_at=NULL,
 sha256=decode(md5('cq-a-'||object_id)||md5('cq-b-'||object_id),'hex') WHERE object_id BETWEEN $1 AND $2`, first, last)
		require.NoError(t, err)
		require.Equal(t, int64(last-first+1), tag.RowsAffected())
	}
	_, err = store.Pool().Exec(t.Context(), `UPDATE objects o SET sha256=v.sha,byte_length=v.n FROM unnest($1::bigint[],$2::bytea[],$3::bigint[]) v(id,sha,n) WHERE o.object_id=v.id`, ids, shas, lengths)
	require.NoError(t, err)
	_, err = store.Pool().Exec(t.Context(), `UPDATE objects SET state='uploading' WHERE object_id=ANY($1::bigint[])`, f.pending)
	require.NoError(t, err)
	_, err = store.Pool().Exec(t.Context(), `INSERT INTO segments(namespace,segment_index,state,current_generation_id,revision) VALUES('main',0,'sealed',1,0),('main',1,'sealed',2,0);
 INSERT INTO segment_generations(generation_id,namespace,segment_index,header,footer_object_id,revision) VALUES(1,'main',0,$1,$2,0),(2,'main',1,$1,$3,0);
 INSERT INTO generation_blocks(generation_id,ordinal,object_id,compressed_length) SELECT 1,(object_id-1)::integer,object_id,byte_length FROM objects WHERE object_id<= $4;
 SELECT setval(pg_get_serial_sequence('segment_generations','generation_id'),$5);`, pgx.QueryExecModeSimpleProtocol, f.sourceHeader, int64(f.sourceFooter), int64(rows), blocks, int64(rows))
	require.NoError(t, err)
	// Bound fixture-only FK/index work without relaxing the measured queries' timeouts.
	base := 2*blocks + 2
	for first := base + 1; first <= rows; first += 20_000 {
		last := min(first+19_999, rows)
		tag, err := store.Pool().Exec(t.Context(), `INSERT INTO generation_blocks(generation_id,ordinal,object_id,compressed_length)
 SELECT 2,(object_id-$3)::integer,object_id,byte_length FROM objects WHERE object_id BETWEEN $1 AND $2`, first, last, base)
		require.NoError(t, err)
		require.Equal(t, int64(last-first+1), tag.RowsAffected())
	}
	_, err = store.Pool().Exec(t.Context(), "ANALYZE")
	require.NoError(t, err)
	return f
}

func checkCompactionPublication(t *testing.T, conn *pgx.Conn, f compactionQueryFixture, result catalog.PublishCommit) {
	t.Helper()
	want := make([]uint64, len(f.publish.Blocks))
	for i, b := range f.publish.Blocks {
		want[i] = b.Object.ID
	}
	require.Equal(t, want, result.ObjectIDs)
	require.Equal(t, f.publish.Footer.ID, result.FooterObjectID)
	var gen uint64
	var count, old int
	require.NoError(t, conn.QueryRow(t.Context(), `SELECT current_generation_id FROM segments WHERE namespace='main' AND segment_index=0`, pgx.QueryExecModeExec).Scan(&gen))
	require.Equal(t, result.GenerationID, gen)
	require.NoError(t, conn.QueryRow(t.Context(), `SELECT count(*) FROM generation_blocks WHERE generation_id=$1`, pgx.QueryExecModeExec, int64(gen)).Scan(&count))
	require.Equal(t, len(want), count)
	require.NoError(t, conn.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM segment_generations WHERE generation_id=1)+(SELECT count(*) FROM generation_blocks WHERE generation_id=1)`, pgx.QueryExecModeExec).Scan(&old))
	require.Zero(t, old)
	var available int
	require.NoError(t, conn.QueryRow(t.Context(), `SELECT count(*) FROM objects WHERE object_id=ANY($1::bigint[]) AND state='available' AND unreferenced_at IS NULL`, pgx.QueryExecModeExec, f.ids).Scan(&available))
	require.Equal(t, len(f.ids), available)
	rows, err := conn.Query(t.Context(), `SELECT ordinal,object_id FROM generation_blocks WHERE generation_id=$1 ORDER BY ordinal`, pgx.QueryExecModeExec, int64(gen))
	require.NoError(t, err)
	ordinal := 0
	for rows.Next() {
		var gotOrdinal int
		var id uint64
		require.NoError(t, rows.Scan(&gotOrdinal, &id))
		require.Equal(t, ordinal, gotOrdinal)
		require.Equal(t, want[ordinal], id)
		ordinal++
	}
	require.NoError(t, rows.Err())
	rows.Close()
	require.Equal(t, len(want), ordinal)
}

func restoreCompactionQueryFixture(t *testing.T, conn *pgx.Conn, f compactionQueryFixture, newGeneration uint64) {
	t.Helper()
	_, err := conn.Exec(t.Context(), `BEGIN;
 DELETE FROM segment_generations WHERE generation_id=$1;
 INSERT INTO segment_generations(generation_id,namespace,segment_index,header,footer_object_id,revision) VALUES(1,'main',0,$2,$3,0);
 INSERT INTO generation_blocks(generation_id,ordinal,object_id,compressed_length) SELECT 1,(object_id-1)::integer,object_id,byte_length FROM objects WHERE object_id<=$4;
 UPDATE segments SET current_generation_id=1,revision=0 WHERE namespace='main' AND segment_index=0;
 UPDATE objects SET state='uploading',unreferenced_at=NULL WHERE object_id=ANY($5::bigint[]);
 COMMIT;`, pgx.QueryExecModeSimpleProtocol, int64(newGeneration), f.sourceHeader, int64(f.sourceFooter), len(f.publish.Blocks), f.pending)
	require.NoError(t, err)
}

func seedCompactionQueryWinners(t *testing.T, conn *pgx.Conn, f compactionQueryFixture, rows int) {
	t.Helper()
	_, err := conn.Exec(t.Context(), `UPDATE objects o SET sha256=p.sha256,unreferenced_at=CASE WHEN v.ordinality%3=0 THEN now()-interval '12 hours' WHEN v.ordinality%3=1 THEN now()-interval '1 hour' ELSE NULL END
 FROM unnest($1::bigint[]) WITH ORDINALITY v(id,ordinality) JOIN objects p ON p.object_id=v.id WHERE o.object_id=$2-v.ordinality`, f.pending, int64(rows))
	require.NoError(t, err)
}

func clearCompactionQueryWinners(t *testing.T, conn *pgx.Conn, f compactionQueryFixture, rows int) {
	t.Helper()
	_, err := conn.Exec(t.Context(), `UPDATE objects SET sha256=decode(md5('cq-a-'||object_id)||md5('cq-b-'||object_id),'hex'),unreferenced_at=NULL WHERE object_id >= $1 AND object_id < $2`, int64(rows-len(f.pending)), int64(rows))
	require.NoError(t, err)
}
