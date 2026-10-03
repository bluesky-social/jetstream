package pgstore

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// tx is a READ COMMITTED leader transaction (design §9.1). Over a WAN link
// each round trip to PostgreSQL costs more than the statement it carries,
// and the fence's row lock is held from the fence to COMMIT, so the
// transaction pipelines what it can without changing what it does:
//
//   - BEGIN is not sent on its own. It goes in the same round trip as the
//     first statement, normally the fence.
//   - A statement whose only result is its error (ApplyMeta, the row
//     inserts, Notify) is queued, and goes in the same round trip as the
//     next statement that returns a result, or as COMMIT.
//
// PostgreSQL runs a pipeline's statements in order and aborts the
// transaction at the first failure, skipping the rest, so this commits
// exactly what sending each statement alone would. Only the call that
// reports a queued statement's error changes (catalog.Tx).
type tx struct {
	conn    *pgxpool.Conn
	kind    catalog.TxKind
	metrics *Metrics
	span    trace.Span
	start   time.Time
	failed  bool
	done    bool

	// queue holds the statements not yet sent: BEGIN until the first round
	// trip, then queued writes. names labels each for errors.
	queue *pgx.Batch
	names []string
}

var _ catalog.Tx = (*tx)(nil)

// Begin implements catalog.DB.
func (s *Store) Begin(ctx context.Context, kind catalog.TxKind) (catalog.Tx, error) {
	start := time.Now()
	_, span := tracer.Start(ctx, "pg.txn", trace.WithAttributes(attribute.String("kind", string(kind))))
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		s.metrics.observe(string(kind), start, true)
		span.RecordError(err)
		span.SetStatus(codes.Error, "begin")
		span.End()
		return nil, fmt.Errorf("%w: pgstore %s: begin: %w", catalog.ErrSessionEnded, kind, err)
	}
	t := &tx{conn: conn, kind: kind, metrics: s.metrics, span: span, start: start, queue: &pgx.Batch{}}
	t.enqueue("begin", `BEGIN ISOLATION LEVEL READ COMMITTED`)
	return t, nil
}

// queuedError is the failure of a queued statement, reported by the call
// whose round trip carried it.
type queuedError struct {
	stmt string
	err  error
}

func (e *queuedError) Error() string { return e.stmt + ": " + e.err.Error() }
func (e *queuedError) Unwrap() error { return e.err }

// fail marks the transaction failed and wraps err as session-ending. A
// queued statement's failure is named for that statement.
func (t *tx) fail(stmt string, err error) error {
	t.failed = true
	t.span.RecordError(err)
	if q, ok := errors.AsType[*queuedError](err); ok {
		stmt = q.stmt + " (sent with " + stmt + ")"
		err = q.err
	}
	return fmt.Errorf("%w: pgstore %s/%s: %w", catalog.ErrSessionEnded, t.kind, stmt, err)
}

func (t *tx) finish() {
	if t.done {
		return
	}
	t.done = true
	t.metrics.observe(string(t.kind), t.start, t.failed)
	if t.failed {
		t.span.SetStatus(codes.Error, "failed")
	}
	t.span.End()
	// The pool destroys a connection that is closed or still inside a
	// transaction, so a failed rollback cannot leak one.
	t.conn.Release()
}

// enqueue adds a statement to the next round trip.
func (t *tx) enqueue(name, sql string, args ...any) {
	t.queue.Queue(sql, args...)
	t.names = append(t.names, name)
}

// send starts a round trip carrying every queued statement, then extra.
// It checks the queued statements' results and returns the batch
// positioned at extra's. The caller must Close it.
func (t *tx) send(ctx context.Context, extra *pgx.Batch) (pgx.BatchResults, error) {
	b, names := t.queue, t.names
	t.queue, t.names = &pgx.Batch{}, nil
	b.QueuedQueries = append(b.QueuedQueries, extra.QueuedQueries...)
	br := t.conn.SendBatch(ctx, b)
	for _, name := range names {
		if _, err := br.Exec(); err != nil {
			_ = br.Close()
			return nil, &queuedError{stmt: name, err: err}
		}
	}
	return br, nil
}

// queryRow runs a single-row statement. The row's Scan makes the round
// trip, carrying the queued statements.
func (t *tx) queryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if len(t.names) == 0 {
		return t.conn.QueryRow(ctx, sql, args...)
	}
	return &pipelinedRow{t: t, ctx: ctx, sql: sql, args: args}
}

type pipelinedRow struct {
	t    *tx
	ctx  context.Context
	sql  string
	args []any
}

func (r *pipelinedRow) Scan(dest ...any) error {
	b := &pgx.Batch{}
	b.Queue(r.sql, r.args...)
	br, err := r.t.send(r.ctx, b)
	if err != nil {
		return err
	}
	err = br.QueryRow().Scan(dest...)
	if cerr := br.Close(); err == nil {
		err = cerr
	}
	return err
}

// exec runs a statement whose command tag the caller needs, carrying the
// queued statements.
func (t *tx) exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if len(t.names) == 0 {
		return t.conn.Exec(ctx, sql, args...)
	}
	b := &pgx.Batch{}
	b.Queue(sql, args...)
	br, err := t.send(ctx, b)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	tag, err := br.Exec()
	if cerr := br.Close(); err == nil {
		err = cerr
	}
	return tag, err
}

// query runs a multi-row statement. Rows stream back one at a time, so the
// queued statements go first in a round trip of their own; no script reads
// rows after a queued write on its hot path.
func (t *tx) query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if len(t.names) > 0 {
		br, err := t.send(ctx, &pgx.Batch{})
		if err != nil {
			return nil, err
		}
		if err := br.Close(); err != nil {
			return nil, err
		}
	}
	return t.conn.Query(ctx, sql, args...)
}

func (t *tx) FenceBump(ctx context.Context, epoch uint64) (uint64, bool, error) {
	var rev uint64
	err := t.queryRow(ctx,
		`UPDATE archive SET catalog_revision = catalog_revision + 1
		 WHERE id = 1 AND writer_epoch = $1
		 RETURNING catalog_revision`, epoch).Scan(&rev)
	if errors.Is(err, pgx.ErrNoRows) {
		t.span.SetAttributes(attribute.Bool("fenced", true))
		return 0, false, nil
	}
	if err != nil {
		return 0, false, t.fail("fence", err)
	}
	t.span.SetAttributes(attribute.Int64("revision", int64(rev)), attribute.Int64("epoch", int64(epoch)))
	return rev, true, nil
}

func (t *tx) MetaGetForUpdate(ctx context.Context, key []byte) ([]byte, bool, error) {
	var v []byte
	err := t.queryRow(ctx, `SELECT value FROM metadata_kv WHERE key = $1 FOR UPDATE`, nonNil(key)).Scan(&v)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, t.fail("meta_get_for_update", err)
	}
	return nonNil(v), true, nil
}

func (t *tx) ApplyMeta(_ context.Context, ops []metastore.Op) error {
	for _, q := range MetaBatch(ops).QueuedQueries {
		t.enqueue("apply_meta", q.SQL, q.Arguments...)
	}
	return nil
}

// MetaBatch turns ordered metastore ops into the design §14.2 statements:
// a run of Sets is one unnest upsert (last write per key wins, since one
// INSERT ... ON CONFLICT cannot touch a row twice), a run of Deletes is one
// `= ANY` delete, and a DeleteRange is its own statement that ends a run.
// Order across runs is kept, which is Pebble's batch semantics.
func MetaBatch(ops []metastore.Op) *pgx.Batch {
	b := &pgx.Batch{}
	var keys, vals [][]byte
	setAt := map[string]int{} // key -> index in the current Set run
	flush := func() {
		switch {
		case vals != nil:
			b.Queue(`INSERT INTO metadata_kv (key, value)
				SELECT * FROM unnest($1::bytea[], $2::bytea[])
				ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`, keys, vals)
		case keys != nil:
			b.Queue(`DELETE FROM metadata_kv WHERE key = ANY($1::bytea[])`, keys)
		}
		keys, vals = nil, nil
		clear(setAt)
	}
	for _, op := range ops {
		switch op.Kind {
		case metastore.OpSet:
			if keys != nil && vals == nil {
				flush()
			}
			// A nil element encodes as NULL; the key and value are NOT NULL
			// and an empty value is a present one.
			k, v := nonNil(op.Key), nonNil(op.Value)
			if i, ok := setAt[string(k)]; ok {
				vals[i] = v
				continue
			}
			setAt[string(k)] = len(keys)
			keys, vals = append(keys, k), append(vals, v)
		case metastore.OpDelete:
			if vals != nil {
				flush()
			}
			keys = append(keys, nonNil(op.Key))
		case metastore.OpDeleteRange:
			flush()
			if op.End == nil {
				b.Queue(`DELETE FROM metadata_kv WHERE key >= $1`, nonNil(op.Key))
			} else {
				b.Queue(`DELETE FROM metadata_kv WHERE key >= $1 AND key < $2`, nonNil(op.Key), op.End)
			}
		}
	}
	flush()
	return b
}

func nonNil(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}

const objectCols = `object_id, key, sha256, byte_length, state, created_at, unreferenced_at`

func scanObject(row pgx.Row) (catalog.ObjectRow, error) {
	var (
		o     catalog.ObjectRow
		sha   []byte
		state string
		unref pgtype.Timestamptz
	)
	if err := row.Scan(&o.ID, &o.Key, &sha, &o.Length, &state, &o.CreatedAt, &unref); err != nil {
		return o, err
	}
	copy(o.SHA256[:], sha)
	o.State = catalog.ObjectState(state)
	if unref.Valid {
		o.UnreferencedAt = unref.Time
	}
	return o, nil
}

func (t *tx) FindAvailableObject(ctx context.Context, sha [32]byte, maxUnrefAge time.Duration) (catalog.ObjectRow, bool, error) {
	o, err := scanObject(t.queryRow(ctx,
		`SELECT `+objectCols+` FROM objects
		 WHERE sha256 = $1 AND state = 'available'
		   AND ($2::bigint <= 0 OR unreferenced_at IS NULL
		        OR unreferenced_at > now() - $2::bigint * interval '1 microsecond')`,
		sha[:], maxUnrefAge.Microseconds()))
	if errors.Is(err, pgx.ErrNoRows) {
		return catalog.ObjectRow{}, false, nil
	}
	if err != nil {
		return catalog.ObjectRow{}, false, t.fail("find_available_object", err)
	}
	return o, true, nil
}

func (t *tx) InsertObjects(ctx context.Context, objs []catalog.NewObject) ([]uint64, error) {
	keys := make([][16]byte, len(objs))
	shas := make([][]byte, len(objs))
	lens := make([]int64, len(objs))
	for i, o := range objs {
		keys[i], shas[i], lens[i] = o.Key, o.SHA256[:], o.Length
	}
	// RETURNING order is not guaranteed, so match IDs back by key, which is
	// unique.
	rows, err := t.query(ctx,
		`INSERT INTO objects (key, sha256, byte_length, state)
		 SELECT k, s, l, 'uploading' FROM unnest($1::uuid[], $2::bytea[], $3::bigint[]) AS u(k, s, l)
		 RETURNING key, object_id`, keys, shas, lens)
	if err != nil {
		return nil, t.fail("insert_objects", err)
	}
	byKey := make(map[[16]byte]uint64, len(objs))
	var (
		k  [16]byte
		id uint64
	)
	_, err = pgx.ForEachRow(rows, []any{&k, &id}, func() error {
		byKey[k] = id
		return nil
	})
	if err != nil {
		return nil, t.fail("insert_objects", err)
	}
	ids := make([]uint64, len(objs))
	for i, o := range objs {
		id, ok := byKey[o.Key]
		if !ok {
			return nil, t.fail("insert_objects", fmt.Errorf("no id returned for key %x", o.Key))
		}
		ids[i] = id
	}
	return ids, nil
}

func (t *tx) SetObjectAvailable(ctx context.Context, id uint64) (bool, error) {
	tag, err := t.exec(ctx,
		`UPDATE objects SET state = 'available' WHERE object_id = $1 AND state = 'uploading'`, id)
	if err != nil {
		return false, t.fail("set_object_available", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (t *tx) RefCheck(ctx context.Context, ids []uint64) ([]uint64, error) {
	rows, err := t.query(ctx,
		`UPDATE objects SET unreferenced_at = NULL
		 WHERE object_id = ANY($1::bigint[]) AND state = 'available'
		 RETURNING object_id`, int64s(ids))
	if err != nil {
		return nil, t.fail("ref_check", err)
	}
	ok := map[uint64]bool{}
	var id uint64
	if _, err := pgx.ForEachRow(rows, []any{&id}, func() error { ok[id] = true; return nil }); err != nil {
		return nil, t.fail("ref_check", err)
	}
	var missing []uint64
	for _, id := range ids {
		if !ok[id] {
			missing = append(missing, id)
		}
	}
	return missing, nil
}

// int64s converts IDs for a bigint[] parameter. An ID beyond bigint range
// matches no row; -1 matches none either.
func int64s(ids []uint64) []int64 {
	out := make([]int64, len(ids))
	for i, id := range ids {
		if id > math.MaxInt64 {
			out[i] = -1
			continue
		}
		out[i] = int64(id)
	}
	return out
}

// nullID maps the zero ID, which stands for NULL, to a NULL parameter.
func nullID(id uint64) *uint64 {
	if id == 0 {
		return nil
	}
	return &id
}

func (t *tx) InsertHotBatch(_ context.Context, row catalog.HotBatchRow) error {
	t.enqueue("insert_hot_batch",
		`INSERT INTO hot_batches (first_seq, last_seq, event_count, min_witnessed_us, max_witnessed_us,
		                          epoch, revision, frame, object_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		row.FirstSeq, row.LastSeq, int64(row.EventCount), row.MinWitnessedUS, row.MaxWitnessedUS,
		row.Epoch, row.Revision, row.Frame, nullID(row.ObjectID))
	return nil
}

// clampSeq maps a seq bound into bigint range. Seqs are bigint columns, so
// no stored seq exceeds math.MaxInt64.
func clampSeq(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}

func (t *tx) DeleteHotBatches(ctx context.Context, lo, hi uint64) ([]catalog.HotBatchSpan, error) {
	if lo > math.MaxInt64 || lo > hi {
		// Run the statement anyway so the call shape matches storagefake.
		lo, hi = 1, 0
	}
	rows, err := t.query(ctx,
		`DELETE FROM hot_batches WHERE first_seq >= $1 AND first_seq <= $2
		 RETURNING first_seq, last_seq, event_count, object_id`, clampSeq(lo), clampSeq(hi))
	if err != nil {
		return nil, t.fail("delete_hot_batches", err)
	}
	var (
		out []catalog.HotBatchSpan
		sp  catalog.HotBatchSpan
		obj pgtype.Int8
	)
	_, err = pgx.ForEachRow(rows, []any{&sp.FirstSeq, &sp.LastSeq, &sp.EventCount, &obj}, func() error {
		sp.ObjectID = uint64(obj.Int64)
		out = append(out, sp)
		return nil
	})
	if err != nil {
		return nil, t.fail("delete_hot_batches", err)
	}
	slices.SortFunc(out, func(a, b catalog.HotBatchSpan) int {
		switch {
		case a.FirstSeq < b.FirstSeq:
			return -1
		case a.FirstSeq > b.FirstSeq:
			return 1
		}
		return 0
	})
	return out, nil
}

const segmentCols = `namespace, segment_index, state, current_generation_id, revision`

func scanSegment(row pgx.Row) (catalog.SegmentRow, error) {
	var (
		r         catalog.SegmentRow
		ns, state string
		gen       pgtype.Int8
	)
	if err := row.Scan(&ns, &r.Index, &state, &gen, &r.Revision); err != nil {
		return r, err
	}
	r.Namespace = catalog.Namespace(ns)
	r.State = segmentState(state)
	r.GenerationID = uint64(gen.Int64)
	return r, nil
}

func segmentState(s string) catalog.SegmentState {
	switch s {
	case "active":
		return catalog.Active
	case "sealed":
		return catalog.Sealed
	}
	return 0
}

func (t *tx) ActiveSegment(ctx context.Context, ns catalog.Namespace) (catalog.SegmentRow, bool, error) {
	r, err := scanSegment(t.queryRow(ctx,
		`SELECT `+segmentCols+` FROM segments WHERE namespace = $1 AND state = 'active' FOR UPDATE`, string(ns)))
	if errors.Is(err, pgx.ErrNoRows) {
		return catalog.SegmentRow{}, false, nil
	}
	if err != nil {
		return catalog.SegmentRow{}, false, t.fail("active_segment", err)
	}
	return r, true, nil
}

const activeBlockCols = `namespace, segment_index, ordinal, object_id, event_count, min_seq, max_seq,
	min_witnessed_us, max_witnessed_us, compressed_length, uncompressed_length, revision`

func activeBlockDest(r *catalog.ActiveBlockRow, ns *string) []any {
	return []any{ns, &r.Segment, &r.Ordinal, &r.ObjectID, &r.EventCount, &r.MinSeq, &r.MaxSeq,
		&r.MinWitnessedUS, &r.MaxWitnessedUS, &r.CompressedLength, &r.UncompressedLength, &r.Revision}
}

func collectActiveBlocks(rows pgx.Rows) ([]catalog.ActiveBlockRow, error) {
	var (
		out []catalog.ActiveBlockRow
		r   catalog.ActiveBlockRow
		ns  string
	)
	_, err := pgx.ForEachRow(rows, activeBlockDest(&r, &ns), func() error {
		r.Namespace = catalog.Namespace(ns)
		out = append(out, r)
		return nil
	})
	return out, err
}

func (t *tx) LastActiveBlock(ctx context.Context, ns catalog.Namespace, idx uint64) (catalog.ActiveBlockRow, bool, error) {
	rows, err := t.query(ctx,
		`SELECT `+activeBlockCols+` FROM active_segment_blocks
		 WHERE namespace = $1 AND segment_index = $2 ORDER BY ordinal DESC LIMIT 1`, string(ns), clampSeq(idx))
	if err != nil {
		return catalog.ActiveBlockRow{}, false, t.fail("last_active_block", err)
	}
	got, err := collectActiveBlocks(rows)
	if err != nil {
		return catalog.ActiveBlockRow{}, false, t.fail("last_active_block", err)
	}
	if len(got) == 0 {
		return catalog.ActiveBlockRow{}, false, nil
	}
	return got[0], true, nil
}

func (t *tx) ActiveBlocksForUpdate(ctx context.Context, ns catalog.Namespace, idx uint64) ([]catalog.ActiveBlockRow, error) {
	rows, err := t.query(ctx,
		`SELECT `+activeBlockCols+` FROM active_segment_blocks
		 WHERE namespace = $1 AND segment_index = $2 ORDER BY ordinal FOR UPDATE`, string(ns), clampSeq(idx))
	if err != nil {
		return nil, t.fail("active_blocks_for_update", err)
	}
	out, err := collectActiveBlocks(rows)
	if err != nil {
		return nil, t.fail("active_blocks_for_update", err)
	}
	return out, nil
}

func (t *tx) InsertActiveBlock(_ context.Context, r catalog.ActiveBlockRow) error {
	t.enqueue("insert_active_block",
		`INSERT INTO active_segment_blocks (`+activeBlockCols+`)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		string(r.Namespace), r.Segment, r.Ordinal, r.ObjectID, int64(r.EventCount), r.MinSeq, r.MaxSeq,
		r.MinWitnessedUS, r.MaxWitnessedUS, r.CompressedLength, r.UncompressedLength, r.Revision)
	return nil
}

func (t *tx) DeleteActiveBlocks(ctx context.Context, ns catalog.Namespace, idx uint64) (int, error) {
	tag, err := t.exec(ctx,
		`DELETE FROM active_segment_blocks WHERE namespace = $1 AND segment_index = $2`, string(ns), clampSeq(idx))
	if err != nil {
		return 0, t.fail("delete_active_blocks", err)
	}
	return int(tag.RowsAffected()), nil
}

func (t *tx) InsertGeneration(ctx context.Context, r catalog.GenerationRow) (uint64, error) {
	var id uint64
	err := t.queryRow(ctx,
		`INSERT INTO segment_generations (namespace, segment_index, header, footer_object_id, revision)
		 VALUES ($1, $2, $3, $4, $5) RETURNING generation_id`,
		string(r.Namespace), r.Segment, nonNil(r.Header), r.FooterObjectID, r.Revision).Scan(&id)
	if err != nil {
		return 0, t.fail("insert_generation", err)
	}
	return id, nil
}

func (t *tx) InsertGenerationBlocks(_ context.Context, rows []catalog.GenerationBlockRow) error {
	gens := make([]uint64, len(rows))
	ords := make([]int64, len(rows))
	objs := make([]uint64, len(rows))
	lens := make([]int64, len(rows))
	for i, r := range rows {
		gens[i], ords[i], objs[i], lens[i] = r.GenerationID, int64(r.Ordinal), r.ObjectID, r.CompressedLength
	}
	t.enqueue("insert_generation_blocks",
		`INSERT INTO generation_blocks (generation_id, ordinal, object_id, compressed_length)
		 SELECT * FROM unnest($1::bigint[], $2::integer[], $3::bigint[], $4::bigint[])`,
		gens, ords, objs, lens)
	return nil
}

func (t *tx) SealSegment(ctx context.Context, ns catalog.Namespace, idx, gen, revision uint64) (bool, error) {
	tag, err := t.exec(ctx,
		`UPDATE segments SET state = 'sealed', current_generation_id = $3, revision = $4
		 WHERE namespace = $1 AND segment_index = $2 AND state = 'active'`,
		string(ns), clampSeq(idx), nullID(gen), revision)
	if err != nil {
		return false, t.fail("seal_segment", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (t *tx) InsertSegment(_ context.Context, r catalog.SegmentRow) error {
	t.enqueue("insert_segment",
		`INSERT INTO segments (`+segmentCols+`) VALUES ($1, $2, $3, $4, $5)`,
		string(r.Namespace), r.Index, r.State.String(), nullID(r.GenerationID), r.Revision)
	return nil
}

func (t *tx) DeleteNamespace(_ context.Context, ns catalog.Namespace) error {
	// generation_blocks go with their generations (ON DELETE CASCADE).
	t.enqueue("delete_namespace",
		`WITH ab AS (DELETE FROM active_segment_blocks WHERE namespace = $1),
		      g AS (DELETE FROM segment_generations WHERE namespace = $1)
		 DELETE FROM segments WHERE namespace = $1`, string(ns))
	return nil
}

func (t *tx) SegmentForUpdate(ctx context.Context, ns catalog.Namespace, idx uint64) (catalog.SegmentRow, bool, error) {
	r, err := scanSegment(t.queryRow(ctx,
		`SELECT `+segmentCols+` FROM segments WHERE namespace = $1 AND segment_index = $2 FOR UPDATE`,
		string(ns), clampSeq(idx)))
	if errors.Is(err, pgx.ErrNoRows) {
		return catalog.SegmentRow{}, false, nil
	}
	if err != nil {
		return catalog.SegmentRow{}, false, t.fail("segment_for_update", err)
	}
	return r, true, nil
}

const generationCols = `generation_id, namespace, segment_index, header, footer_object_id, created_at, revision`

func scanGeneration(row pgx.Row) (catalog.GenerationRow, error) {
	var (
		g  catalog.GenerationRow
		ns string
	)
	err := row.Scan(&g.ID, &ns, &g.Segment, &g.Header, &g.FooterObjectID, &g.CreatedAt, &g.Revision)
	g.Namespace = catalog.Namespace(ns)
	return g, err
}

func (t *tx) Generation(ctx context.Context, id uint64) (catalog.GenerationRow, bool, error) {
	g, err := scanGeneration(t.queryRow(ctx,
		`SELECT `+generationCols+` FROM segment_generations WHERE generation_id = $1`, int64s([]uint64{id})[0]))
	if errors.Is(err, pgx.ErrNoRows) {
		return catalog.GenerationRow{}, false, nil
	}
	if err != nil {
		return catalog.GenerationRow{}, false, t.fail("generation", err)
	}
	return g, true, nil
}

func scanGenerationBlock(row pgx.CollectableRow) (catalog.GenerationBlockRow, error) {
	var b catalog.GenerationBlockRow
	err := row.Scan(&b.GenerationID, &b.Ordinal, &b.ObjectID, &b.CompressedLength)
	return b, err
}

func (t *tx) BlocksOfGeneration(ctx context.Context, id uint64) ([]catalog.GenerationBlockRow, error) {
	rows, err := t.query(ctx,
		`SELECT generation_id, ordinal, object_id, compressed_length FROM generation_blocks
		 WHERE generation_id = $1 ORDER BY ordinal`, int64s([]uint64{id})[0])
	if err != nil {
		return nil, t.fail("blocks_of_generation", err)
	}
	out, err := pgx.CollectRows(rows, scanGenerationBlock)
	if err != nil {
		return nil, t.fail("blocks_of_generation", err)
	}
	return out, nil
}

func (t *tx) SetSegmentGeneration(ctx context.Context, ns catalog.Namespace, idx, gen, revision uint64) (bool, error) {
	tag, err := t.exec(ctx,
		`UPDATE segments SET current_generation_id = $3, revision = $4
		 WHERE namespace = $1 AND segment_index = $2 AND state = 'sealed'`,
		string(ns), clampSeq(idx), nullID(gen), revision)
	if err != nil {
		return false, t.fail("set_segment_generation", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (t *tx) DeleteGeneration(ctx context.Context, id uint64) (bool, error) {
	// generation_blocks go with it (ON DELETE CASCADE).
	tag, err := t.exec(ctx, `DELETE FROM segment_generations WHERE generation_id = $1`, int64s([]uint64{id})[0])
	if err != nil {
		return false, t.fail("delete_generation", err)
	}
	return tag.RowsAffected() == 1, nil
}

// unreferencedSQL is the four §13 NOT EXISTS checks for the object o.
const unreferencedSQL = `NOT EXISTS (SELECT 1 FROM hot_batches h WHERE h.object_id = o.object_id)
	AND NOT EXISTS (SELECT 1 FROM active_segment_blocks a WHERE a.object_id = o.object_id)
	AND NOT EXISTS (SELECT 1 FROM generation_blocks g WHERE g.object_id = o.object_id)
	AND NOT EXISTS (SELECT 1 FROM segment_generations s WHERE s.footer_object_id = o.object_id)`

func (t *tx) MarkUnreferenced(ctx context.Context, after uint64, limit int) (catalog.MarkPage, error) {
	var (
		page catalog.MarkPage
		last pgtype.Int8
	)
	err := t.queryRow(ctx,
		`WITH page AS (
		     SELECT object_id FROM objects
		     WHERE state = 'available' AND unreferenced_at IS NULL AND object_id > $1
		     ORDER BY object_id LIMIT $2),
		 marked AS (
		     UPDATE objects o SET unreferenced_at = now()
		     FROM page p WHERE o.object_id = p.object_id AND `+unreferencedSQL+`
		     RETURNING 1)
		 SELECT (SELECT count(*) FROM page), (SELECT max(object_id) FROM page), (SELECT count(*) FROM marked)`,
		clampSeq(after), limit).Scan(&page.Scanned, &last, &page.Marked)
	if err != nil {
		return catalog.MarkPage{}, t.fail("mark_unreferenced", err)
	}
	page.Last = uint64(last.Int64)
	return page, nil
}

func collectObjects(rows pgx.Rows) ([]catalog.ObjectRow, error) {
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (catalog.ObjectRow, error) {
		return scanObject(row)
	})
}

func (t *tx) ClaimObjects(ctx context.Context, gcDelay, orphanAge time.Duration, limit int) ([]catalog.ObjectRow, error) {
	// The CTE's snapshot of each row is its pre-claim state; RETURNING
	// reports it, so the caller knows which claims to re-check.
	rows, err := t.query(ctx,
		`WITH c AS (
		     SELECT `+objectCols+` FROM objects
		     WHERE (state = 'available' AND unreferenced_at < now() - $1::bigint * interval '1 microsecond')
		        OR (state = 'uploading' AND created_at < now() - $2::bigint * interval '1 microsecond')
		     ORDER BY object_id LIMIT $3 FOR UPDATE)
		 UPDATE objects o SET state = 'deleting' FROM c WHERE o.object_id = c.object_id
		 RETURNING c.object_id, c.key, c.sha256, c.byte_length, c.state, c.created_at, c.unreferenced_at`,
		gcDelay.Microseconds(), orphanAge.Microseconds(), limit)
	if err != nil {
		return nil, t.fail("claim_objects", err)
	}
	out, err := collectObjects(rows)
	if err != nil {
		return nil, t.fail("claim_objects", err)
	}
	slices.SortFunc(out, func(a, b catalog.ObjectRow) int { return cmp.Compare(a.ID, b.ID) })
	return out, nil
}

func (t *tx) DeletingObjects(ctx context.Context, limit int) ([]catalog.ObjectRow, error) {
	rows, err := t.query(ctx,
		`SELECT `+objectCols+` FROM objects WHERE state = 'deleting' ORDER BY object_id LIMIT $1`, limit)
	if err != nil {
		return nil, t.fail("deleting_objects", err)
	}
	out, err := collectObjects(rows)
	if err != nil {
		return nil, t.fail("deleting_objects", err)
	}
	return out, nil
}

func (t *tx) ReferencedObjects(ctx context.Context, ids []uint64) ([]uint64, error) {
	rows, err := t.query(ctx,
		`SELECT DISTINCT o.object_id FROM unnest($1::bigint[]) AS o(object_id)
		 WHERE NOT (`+unreferencedSQL+`) ORDER BY o.object_id`, int64s(ids))
	if err != nil {
		return nil, t.fail("referenced_objects", err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[uint64])
	if err != nil {
		return nil, t.fail("referenced_objects", err)
	}
	return out, nil
}

func (t *tx) ForgetObjects(ctx context.Context, ids []uint64) (int, error) {
	tag, err := t.exec(ctx,
		`DELETE FROM objects WHERE object_id = ANY($1::bigint[]) AND state = 'deleting'`, int64s(ids))
	if err != nil {
		return 0, t.fail("forget_objects", err)
	}
	return int(tag.RowsAffected()), nil
}

func (t *tx) Notify(_ context.Context, revision uint64) error {
	t.enqueue("notify", `SELECT pg_notify('jetstream_catalog', $1)`, strconv.FormatUint(revision, 10))
	return nil
}

// Commit sends the queued statements and COMMIT in one round trip.
func (t *tx) Commit(ctx context.Context) error {
	if t.done {
		return fmt.Errorf("%w: pgstore %s: commit: %w", catalog.ErrSessionEnded, t.kind, pgx.ErrTxClosed)
	}
	err := t.commit(ctx)
	if err != nil {
		err = t.fail("commit", err)
	}
	t.finish()
	return err
}

func (t *tx) commit(ctx context.Context) error {
	if t.failed {
		return errTxAborted
	}
	b := &pgx.Batch{}
	b.Queue(`COMMIT`)
	br, err := t.send(ctx, b)
	if err != nil {
		return err
	}
	tag, err := br.Exec()
	if cerr := br.Close(); err == nil {
		err = cerr
	}
	if err == nil && tag.String() != "COMMIT" {
		// COMMIT of an aborted transaction rolls back and reports it in
		// its tag: nothing applied, but it is still a failure.
		err = pgx.ErrTxCommitRollback
	}
	return err
}

// errTxAborted rejects a commit after a failed statement, as PostgreSQL
// would, without a round trip.
var errTxAborted = errors.New("transaction is aborted")

func (t *tx) Rollback(ctx context.Context) error {
	if t.done {
		return nil
	}
	defer t.finish()
	if t.conn.Conn().PgConn().TxStatus() == 'I' || t.conn.Conn().IsClosed() {
		// BEGIN never reached the server, or a dead connection already
		// ended the transaction there.
		return nil
	}
	if _, err := t.conn.Exec(ctx, `ROLLBACK`); err != nil && !t.conn.Conn().IsClosed() {
		return fmt.Errorf("pgstore %s: rollback: %w", t.kind, err)
	}
	return nil
}
