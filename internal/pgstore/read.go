package pgstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const txKindRead = "read"

// readTx is a REPEATABLE READ READ ONLY transaction (design §9.1): every
// statement sees the snapshot taken by the first.
type readTx struct {
	tx      pgx.Tx
	metrics *Metrics
	span    trace.Span
	start   time.Time
	failed  bool
	closed  bool
}

var _ catalog.ReadTx = (*readTx)(nil)

// BeginRead implements catalog.DB.
func (s *Store) BeginRead(ctx context.Context) (catalog.ReadTx, error) {
	start := time.Now()
	_, span := tracer.Start(ctx, "pg.read")
	ptx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		s.metrics.observe(txKindRead, start, true)
		span.RecordError(err)
		span.SetStatus(codes.Error, "begin")
		span.End()
		return nil, fmt.Errorf("pgstore: begin read: %w", err)
	}
	return &readTx{tx: ptx, metrics: s.metrics, span: span, start: start}, nil
}

func (r *readTx) fail(stmt string, err error) error {
	r.failed = true
	r.span.RecordError(err)
	return fmt.Errorf("pgstore: read %s: %w", stmt, err)
}

const archiveSQL = `SELECT archive_id, format_version, schema_version, writer_epoch, holder_id,
	lease_expires_at, catalog_revision, created_at FROM archive WHERE id = 1`

func scanArchive(row pgx.Row) (catalog.ArchiveRow, error) {
	var (
		a       catalog.ArchiveRow
		holder  pgtype.UUID
		expires pgtype.Timestamptz
	)
	err := row.Scan(&a.ArchiveID, &a.FormatVersion, &a.SchemaVersion, &a.WriterEpoch, &holder,
		&expires, &a.CatalogRevision, &a.CreatedAt)
	if err != nil {
		return a, err
	}
	if holder.Valid {
		a.HolderID = holder.Bytes
	}
	if expires.Valid {
		a.LeaseExpiresAt = expires.Time
	}
	return a, nil
}

func (r *readTx) Archive(ctx context.Context) (catalog.ArchiveRow, error) {
	a, err := scanArchive(r.tx.QueryRow(ctx, archiveSQL))
	if err != nil {
		return a, r.fail("archive", err)
	}
	r.span.SetAttributes(attribute.Int64("revision", int64(a.CatalogRevision)))
	return a, nil
}

// Namespaces compare with COLLATE "C" so the order is bytewise, as
// storagefake's, whatever the database's collation.

func (r *readTx) SegmentsSince(ctx context.Context, rev uint64) ([]catalog.SegmentRow, error) {
	rows, err := r.tx.Query(ctx,
		`SELECT `+segmentCols+` FROM segments WHERE revision > $1
		 ORDER BY namespace COLLATE "C", segment_index`, clampSeq(rev))
	if err != nil {
		return nil, r.fail("segments", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (catalog.SegmentRow, error) {
		return scanSegment(row)
	})
	if err != nil {
		return nil, r.fail("segments", err)
	}
	return out, nil
}

func (r *readTx) Generations(ctx context.Context, ids []uint64) ([]catalog.GenerationRow, error) {
	rows, err := r.tx.Query(ctx,
		`SELECT generation_id, namespace, segment_index, header, footer_object_id, created_at, revision
		 FROM segment_generations WHERE generation_id = ANY($1::bigint[]) ORDER BY generation_id`, int64s(ids))
	if err != nil {
		return nil, r.fail("generations", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (catalog.GenerationRow, error) {
		var (
			g  catalog.GenerationRow
			ns string
		)
		err := row.Scan(&g.ID, &ns, &g.Segment, &g.Header, &g.FooterObjectID, &g.CreatedAt, &g.Revision)
		g.Namespace = catalog.Namespace(ns)
		return g, err
	})
	if err != nil {
		return nil, r.fail("generations", err)
	}
	return out, nil
}

func (r *readTx) GenerationBlocks(ctx context.Context, ids []uint64) ([]catalog.GenerationBlockRow, error) {
	rows, err := r.tx.Query(ctx,
		`SELECT generation_id, ordinal, object_id, compressed_length FROM generation_blocks
		 WHERE generation_id = ANY($1::bigint[]) ORDER BY generation_id, ordinal`, int64s(ids))
	if err != nil {
		return nil, r.fail("generation_blocks", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (catalog.GenerationBlockRow, error) {
		var b catalog.GenerationBlockRow
		err := row.Scan(&b.GenerationID, &b.Ordinal, &b.ObjectID, &b.CompressedLength)
		return b, err
	})
	if err != nil {
		return nil, r.fail("generation_blocks", err)
	}
	return out, nil
}

func (r *readTx) ActiveBlocksSince(ctx context.Context, rev uint64) ([]catalog.ActiveBlockRow, error) {
	rows, err := r.tx.Query(ctx,
		`SELECT `+activeBlockCols+` FROM active_segment_blocks WHERE revision > $1
		 ORDER BY namespace COLLATE "C", segment_index, ordinal`, clampSeq(rev))
	if err != nil {
		return nil, r.fail("active_blocks", err)
	}
	out, err := collectActiveBlocks(rows)
	if err != nil {
		return nil, r.fail("active_blocks", err)
	}
	return out, nil
}

func (r *readTx) ActiveBlockKeys(ctx context.Context) ([]catalog.ActiveBlockKey, error) {
	rows, err := r.tx.Query(ctx,
		`SELECT namespace, segment_index, ordinal FROM active_segment_blocks
		 ORDER BY namespace COLLATE "C", segment_index, ordinal`)
	if err != nil {
		return nil, r.fail("active_block_keys", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (catalog.ActiveBlockKey, error) {
		var (
			k  catalog.ActiveBlockKey
			ns string
		)
		err := row.Scan(&ns, &k.Segment, &k.Ordinal)
		k.Namespace = catalog.Namespace(ns)
		return k, err
	})
	if err != nil {
		return nil, r.fail("active_block_keys", err)
	}
	return out, nil
}

func (r *readTx) HotBatches(ctx context.Context, framesFrom uint64) ([]catalog.HotBatchRow, error) {
	rows, err := r.tx.Query(ctx,
		`SELECT first_seq, last_seq, event_count, min_witnessed_us, max_witnessed_us, epoch, revision,
		        committed_at, CASE WHEN $2 AND first_seq >= $1 THEN frame END, object_id, frame IS NOT NULL
		 FROM hot_batches ORDER BY first_seq`, clampSeq(framesFrom), framesFrom <= math.MaxInt64)
	if err != nil {
		return nil, r.fail("hot_batches", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (catalog.HotBatchRow, error) {
		var (
			h   catalog.HotBatchRow
			obj pgtype.Int8
		)
		err := row.Scan(&h.FirstSeq, &h.LastSeq, &h.EventCount, &h.MinWitnessedUS, &h.MaxWitnessedUS,
			&h.Epoch, &h.Revision, &h.CommittedAt, &h.Frame, &obj, &h.Inline)
		h.ObjectID = uint64(obj.Int64)
		if h.Inline && h.Frame == nil && framesFrom <= math.MaxInt64 && h.FirstSeq >= framesFrom {
			h.Frame = []byte{} // a loaded empty frame is still a frame
		}
		return h, err
	})
	if err != nil {
		return nil, r.fail("hot_batches", err)
	}
	return out, nil
}

func (r *readTx) Objects(ctx context.Context, ids []uint64) ([]catalog.ObjectRow, error) {
	rows, err := r.tx.Query(ctx,
		`SELECT `+objectCols+` FROM objects WHERE object_id = ANY($1::bigint[]) ORDER BY object_id`, int64s(ids))
	if err != nil {
		return nil, r.fail("objects", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (catalog.ObjectRow, error) {
		return scanObject(row)
	})
	if err != nil {
		return nil, r.fail("objects", err)
	}
	return out, nil
}

func (r *readTx) MetaGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	ks := make([][]byte, len(keys))
	for i, k := range keys {
		ks[i] = nonNil(k)
	}
	rows, err := r.tx.Query(ctx, `SELECT key, value FROM metadata_kv WHERE key = ANY($1::bytea[])`, ks)
	if err != nil {
		return nil, r.fail("meta_get", err)
	}
	out := map[string][]byte{}
	var k, v []byte
	_, err = pgx.ForEachRow(rows, []any{&k, &v}, func() error {
		out[string(k)] = nonNil(v)
		return nil
	})
	if err != nil {
		return nil, r.fail("meta_get", err)
	}
	return out, nil
}

func (r *readTx) Close(ctx context.Context) error {
	if r.closed {
		return nil
	}
	r.closed = true
	err := r.tx.Rollback(ctx)
	r.metrics.observe(txKindRead, r.start, r.failed)
	if r.failed {
		r.span.SetStatus(codes.Error, "failed")
	}
	r.span.End()
	if err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		return fmt.Errorf("pgstore: close read: %w", err)
	}
	return nil
}
