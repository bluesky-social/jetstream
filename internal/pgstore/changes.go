package pgstore

import (
	"context"
	"fmt"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

const txKindReadChanges = "read_changes"

// The ReadChanges statements. Each one that depends on an earlier result
// in catalog.ReadChangesTx computes it as a subquery instead, so all of them
// can go in one pipeline. $1 is the mirror's revision throughout.
const (
	// changedGensSQL is the generations of the sealed segments that changed.
	changedGensSQL = `SELECT current_generation_id FROM segments WHERE revision > $1 AND state = 'sealed'`
	// changedGenCountSQL counts them, to bound the statements that read
	// them. Overflow there leaves each of them empty.
	changedGenCountSQL = `SELECT count(DISTINCT current_generation_id) FROM segments
	WHERE revision > $1 AND state = 'sealed'`
	withinLimit = `(` + changedGenCountSQL + `) <= $2`
	// changedSQL is true when the catalog moved past $1. Statements with
	// no revision filter of their own read nothing on a tick that NOTIFY
	// or the poll timer woke for no change.
	changedSQL = `EXISTS (SELECT 1 FROM archive WHERE id = 1 AND catalog_revision > $1)`

	changesGenerationsSQL = `SELECT ` + generationCols + ` FROM segment_generations
	WHERE generation_id IN (` + changedGensSQL + `) AND ` + withinLimit + `
	ORDER BY generation_id`
	changesGenerationBlocksSQL = `SELECT generation_id, ordinal, object_id, compressed_length FROM generation_blocks
	WHERE generation_id IN (` + changedGensSQL + `) AND ` + withinLimit + `
	ORDER BY generation_id, ordinal`
	changesActiveBlockKeysSQL = `SELECT namespace, segment_index, ordinal FROM active_segment_blocks
	WHERE ` + changedSQL + `
	ORDER BY namespace COLLATE "C", segment_index, ordinal`
	// $1 and $2 are hotBatchArgs; $3 is the mirror's revision.
	changesHotBatchesSQL = `SELECT ` + hotBatchCols + ` FROM hot_batches
	WHERE EXISTS (SELECT 1 FROM archive WHERE id = 1 AND catalog_revision > $3)
	ORDER BY first_seq`
	changesObjectsSQL = `SELECT ` + objectCols + ` FROM objects
	WHERE ` + changedSQL + ` AND ` + withinLimit + ` AND object_id IN (
		SELECT footer_object_id FROM segment_generations WHERE generation_id IN (` + changedGensSQL + `)
		UNION ALL SELECT object_id FROM generation_blocks WHERE generation_id IN (` + changedGensSQL + `)
		UNION ALL SELECT object_id FROM active_segment_blocks WHERE revision > $1
		UNION ALL SELECT object_id FROM hot_batches WHERE object_id IS NOT NULL)
	ORDER BY object_id`
)

// ReadChanges implements catalog.DB. BEGIN, every statement, and COMMIT go
// in one pipeline: PostgreSQL runs them in order on one connection, so they
// share the snapshot BEGIN takes, and the tick pays one round trip where
// the ReadTx methods pay one per statement plus BEGIN and the rollback.
// Over the WAN link to the catalog that is the difference between about 20
// and 150 milliseconds, and every live event's visibility waits on it.
func (s *Store) ReadChanges(ctx context.Context, q catalog.ChangesQuery) (c catalog.Changes, err error) {
	start := time.Now()
	_, span := tracer.Start(ctx, "pg.read_changes")
	span.SetAttributes(attribute.Int64("since", clampSeq(q.Since)))
	defer func() {
		s.metrics.observe(txKindReadChanges, start, err != nil)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "read_changes")
		}
		span.End()
	}()

	since, limit := clampSeq(q.Since), catalog.MaxChangedGenerations
	b := &pgx.Batch{}
	b.Queue(`BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY`)
	b.Queue(archiveSQL)
	b.Queue(metaGetSQL, metaKeys(q.MetaKeys))
	b.Queue(changedGenCountSQL, since)
	b.Queue(segmentsSinceSQL, since)
	b.Queue(changesGenerationsSQL, since, limit)
	b.Queue(changesGenerationBlocksSQL, since, limit)
	b.Queue(activeBlocksSinceSQL, since)
	b.Queue(changesActiveBlockKeysSQL, since)
	b.Queue(changesHotBatchesSQL, append(hotBatchArgs(q.FramesFrom), since)...)
	b.Queue(changesObjectsSQL, since, limit)
	b.Queue(`COMMIT`)

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return catalog.Changes{}, fmt.Errorf("pgstore: read changes: %w", err)
	}
	defer conn.Release()
	br := conn.SendBatch(ctx, b)
	c, err = scanChanges(br, q)
	if cerr := br.Close(); err == nil && cerr != nil {
		err = cerr
	}
	if err != nil {
		// A failure skips the rest of the pipeline, COMMIT included, and
		// leaves the transaction open. Ending it keeps the connection: the
		// pool would destroy one still inside a transaction.
		if !conn.Conn().IsClosed() && conn.Conn().PgConn().TxStatus() != 'I' {
			_, _ = conn.Exec(context.WithoutCancel(ctx), `ROLLBACK`)
		}
		return catalog.Changes{}, fmt.Errorf("pgstore: read changes: %w", err)
	}
	span.SetAttributes(attribute.Int64("revision", int64(c.Archive.CatalogRevision)),
		attribute.Bool("overflow", c.Overflow))
	return c, nil
}

// scanChanges reads ReadChanges' results in queue order.
func scanChanges(br pgx.BatchResults, q catalog.ChangesQuery) (catalog.Changes, error) {
	var c catalog.Changes
	step := func(name string, err error) error {
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		return nil
	}
	if _, err := br.Exec(); err != nil {
		return c, step("begin", err)
	}
	var err error
	if c.Archive, err = scanArchive(br.QueryRow()); err != nil {
		return c, step("archive", err)
	}
	if c.Meta, err = queryRows(br, collectMeta); err != nil {
		return c, step("meta_get", err)
	}
	var changedGens int64
	if err := br.QueryRow().Scan(&changedGens); err != nil {
		return c, step("changed_generations", err)
	}
	c.Overflow = c.Archive.CatalogRevision > q.Since && changedGens > catalog.MaxChangedGenerations
	if c.Segments, err = queryRows(br, collectSegments); err != nil {
		return c, step("segments", err)
	}
	if c.Generations, err = queryRows(br, collectGenerations); err != nil {
		return c, step("generations", err)
	}
	if c.GenerationBlocks, err = queryRows(br, func(rows pgx.Rows) ([]catalog.GenerationBlockRow, error) {
		return pgx.CollectRows(rows, scanGenerationBlock)
	}); err != nil {
		return c, step("generation_blocks", err)
	}
	if c.ActiveBlocks, err = queryRows(br, collectActiveBlocks); err != nil {
		return c, step("active_blocks", err)
	}
	if c.ActiveBlockKeys, err = queryRows(br, collectActiveBlockKeys); err != nil {
		return c, step("active_block_keys", err)
	}
	if c.HotBatches, err = queryRows(br, func(rows pgx.Rows) ([]catalog.HotBatchRow, error) {
		return collectHotBatches(rows, q.FramesFrom)
	}); err != nil {
		return c, step("hot_batches", err)
	}
	if c.Objects, err = queryRows(br, collectObjects); err != nil {
		return c, step("objects", err)
	}
	if _, err := br.Exec(); err != nil {
		return c, step("commit", err)
	}
	switch {
	case c.Archive.CatalogRevision <= q.Since:
		return catalog.Changes{Archive: c.Archive, Meta: c.Meta}, nil
	case c.Overflow:
		return catalog.Changes{Archive: c.Archive, Meta: c.Meta, Overflow: true}, nil
	}
	return c, nil
}

func queryRows[T any](br pgx.BatchResults, collect func(pgx.Rows) (T, error)) (T, error) {
	rows, err := br.Query()
	if err != nil {
		var zero T
		return zero, err
	}
	return collect(rows)
}
