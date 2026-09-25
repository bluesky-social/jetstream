package pgstore

import (
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
	"github.com/jackc/pgx/v5/pgtype"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// tx is a READ COMMITTED leader transaction (design §9.1).
type tx struct {
	tx      pgx.Tx
	kind    catalog.TxKind
	metrics *Metrics
	span    trace.Span
	start   time.Time
	failed  bool
	done    bool
}

var _ catalog.Tx = (*tx)(nil)

// Begin implements catalog.DB.
func (s *Store) Begin(ctx context.Context, kind catalog.TxKind) (catalog.Tx, error) {
	start := time.Now()
	_, span := tracer.Start(ctx, "pg.txn", trace.WithAttributes(attribute.String("kind", string(kind))))
	ptx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		s.metrics.observe(string(kind), start, true)
		span.RecordError(err)
		span.SetStatus(codes.Error, "begin")
		span.End()
		return nil, fmt.Errorf("%w: pgstore %s: begin: %w", catalog.ErrSessionEnded, kind, err)
	}
	return &tx{tx: ptx, kind: kind, metrics: s.metrics, span: span, start: start}, nil
}

// fail marks the transaction failed and wraps err as session-ending.
func (t *tx) fail(stmt string, err error) error {
	t.failed = true
	t.span.RecordError(err)
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
}

func (t *tx) FenceBump(ctx context.Context, epoch uint64) (uint64, bool, error) {
	var rev uint64
	err := t.tx.QueryRow(ctx,
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
	err := t.tx.QueryRow(ctx, `SELECT value FROM metadata_kv WHERE key = $1 FOR UPDATE`, nonNil(key)).Scan(&v)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, t.fail("meta_get_for_update", err)
	}
	return nonNil(v), true, nil
}

func (t *tx) ApplyMeta(ctx context.Context, ops []metastore.Op) error {
	b := MetaBatch(ops)
	if b.Len() == 0 {
		return nil
	}
	if err := t.tx.SendBatch(ctx, b).Close(); err != nil {
		return t.fail("apply_meta", err)
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
	o, err := scanObject(t.tx.QueryRow(ctx,
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
	rows, err := t.tx.Query(ctx,
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
	tag, err := t.tx.Exec(ctx,
		`UPDATE objects SET state = 'available' WHERE object_id = $1 AND state = 'uploading'`, id)
	if err != nil {
		return false, t.fail("set_object_available", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (t *tx) RefCheck(ctx context.Context, ids []uint64) ([]uint64, error) {
	rows, err := t.tx.Query(ctx,
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

func (t *tx) InsertHotBatch(ctx context.Context, row catalog.HotBatchRow) error {
	_, err := t.tx.Exec(ctx,
		`INSERT INTO hot_batches (first_seq, last_seq, event_count, min_witnessed_us, max_witnessed_us,
		                          epoch, revision, frame, object_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		row.FirstSeq, row.LastSeq, int64(row.EventCount), row.MinWitnessedUS, row.MaxWitnessedUS,
		row.Epoch, row.Revision, row.Frame, nullID(row.ObjectID))
	if err != nil {
		return t.fail("insert_hot_batch", err)
	}
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
	rows, err := t.tx.Query(ctx,
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
	r, err := scanSegment(t.tx.QueryRow(ctx,
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
	rows, err := t.tx.Query(ctx,
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
	rows, err := t.tx.Query(ctx,
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

func (t *tx) InsertActiveBlock(ctx context.Context, r catalog.ActiveBlockRow) error {
	_, err := t.tx.Exec(ctx,
		`INSERT INTO active_segment_blocks (`+activeBlockCols+`)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		string(r.Namespace), r.Segment, r.Ordinal, r.ObjectID, int64(r.EventCount), r.MinSeq, r.MaxSeq,
		r.MinWitnessedUS, r.MaxWitnessedUS, r.CompressedLength, r.UncompressedLength, r.Revision)
	if err != nil {
		return t.fail("insert_active_block", err)
	}
	return nil
}

func (t *tx) DeleteActiveBlocks(ctx context.Context, ns catalog.Namespace, idx uint64) (int, error) {
	tag, err := t.tx.Exec(ctx,
		`DELETE FROM active_segment_blocks WHERE namespace = $1 AND segment_index = $2`, string(ns), clampSeq(idx))
	if err != nil {
		return 0, t.fail("delete_active_blocks", err)
	}
	return int(tag.RowsAffected()), nil
}

func (t *tx) InsertGeneration(ctx context.Context, r catalog.GenerationRow) (uint64, error) {
	var id uint64
	err := t.tx.QueryRow(ctx,
		`INSERT INTO segment_generations (namespace, segment_index, header, footer_object_id, revision)
		 VALUES ($1, $2, $3, $4, $5) RETURNING generation_id`,
		string(r.Namespace), r.Segment, nonNil(r.Header), r.FooterObjectID, r.Revision).Scan(&id)
	if err != nil {
		return 0, t.fail("insert_generation", err)
	}
	return id, nil
}

func (t *tx) InsertGenerationBlocks(ctx context.Context, rows []catalog.GenerationBlockRow) error {
	gens := make([]uint64, len(rows))
	ords := make([]int64, len(rows))
	objs := make([]uint64, len(rows))
	lens := make([]int64, len(rows))
	for i, r := range rows {
		gens[i], ords[i], objs[i], lens[i] = r.GenerationID, int64(r.Ordinal), r.ObjectID, r.CompressedLength
	}
	_, err := t.tx.Exec(ctx,
		`INSERT INTO generation_blocks (generation_id, ordinal, object_id, compressed_length)
		 SELECT * FROM unnest($1::bigint[], $2::integer[], $3::bigint[], $4::bigint[])`,
		gens, ords, objs, lens)
	if err != nil {
		return t.fail("insert_generation_blocks", err)
	}
	return nil
}

func (t *tx) SealSegment(ctx context.Context, ns catalog.Namespace, idx, gen, revision uint64) (bool, error) {
	tag, err := t.tx.Exec(ctx,
		`UPDATE segments SET state = 'sealed', current_generation_id = $3, revision = $4
		 WHERE namespace = $1 AND segment_index = $2 AND state = 'active'`,
		string(ns), clampSeq(idx), nullID(gen), revision)
	if err != nil {
		return false, t.fail("seal_segment", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (t *tx) InsertSegment(ctx context.Context, r catalog.SegmentRow) error {
	_, err := t.tx.Exec(ctx,
		`INSERT INTO segments (`+segmentCols+`) VALUES ($1, $2, $3, $4, $5)`,
		string(r.Namespace), r.Index, r.State.String(), nullID(r.GenerationID), r.Revision)
	if err != nil {
		return t.fail("insert_segment", err)
	}
	return nil
}

func (t *tx) DeleteNamespace(ctx context.Context, ns catalog.Namespace) error {
	// generation_blocks go with their generations (ON DELETE CASCADE).
	_, err := t.tx.Exec(ctx,
		`WITH ab AS (DELETE FROM active_segment_blocks WHERE namespace = $1),
		      g AS (DELETE FROM segment_generations WHERE namespace = $1)
		 DELETE FROM segments WHERE namespace = $1`, string(ns))
	if err != nil {
		return t.fail("delete_namespace", err)
	}
	return nil
}

func (t *tx) Notify(ctx context.Context, revision uint64) error {
	if _, err := t.tx.Exec(ctx, `SELECT pg_notify('jetstream_catalog', $1)`, strconv.FormatUint(revision, 10)); err != nil {
		return t.fail("notify", err)
	}
	return nil
}

func (t *tx) Commit(ctx context.Context) error {
	if t.done {
		return fmt.Errorf("%w: pgstore %s: commit: %w", catalog.ErrSessionEnded, t.kind, pgx.ErrTxClosed)
	}
	err := t.tx.Commit(ctx)
	if err != nil {
		// pgx reports committing an aborted transaction as
		// ErrTxCommitRollback: nothing applied, but it is still a failure.
		err = t.fail("commit", err)
	}
	t.finish()
	return err
}

func (t *tx) Rollback(ctx context.Context) error {
	if t.done {
		return nil
	}
	err := t.tx.Rollback(ctx)
	t.finish()
	if err == nil || errors.Is(err, pgx.ErrTxClosed) || t.tx.Conn().IsClosed() {
		// A dead connection means the server has rolled back already.
		return nil
	}
	return fmt.Errorf("pgstore %s: rollback: %w", t.kind, err)
}
