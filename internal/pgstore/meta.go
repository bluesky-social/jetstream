package pgstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// txKindMetaRead labels the autocommit metadata reads behind metastore/pg.
// They are separate from "read" (catalog snapshots) because a full-range
// metadata scan is the heaviest read a pod makes.
const txKindMetaRead = "meta_read"

// MetaKV is one metadata_kv row.
type MetaKV struct {
	Key, Value []byte
}

// MetaGet reads one metadata_kv key outside any transaction: it sees the
// latest commit. Writes go through a fenced catalog transaction instead
// (Tx.ApplyMeta), so there is no unfenced write counterpart.
func (s *Store) MetaGet(ctx context.Context, key []byte) ([]byte, bool, error) {
	start := time.Now()
	_, span := tracer.Start(ctx, "pg.meta_get")
	defer span.End()
	var v []byte
	err := s.pool.QueryRow(ctx, `SELECT value FROM metadata_kv WHERE key = $1`, nonNil(key)).Scan(&v)
	if errors.Is(err, pgx.ErrNoRows) {
		s.metrics.observe(txKindMetaRead, start, false)
		return nil, false, nil
	}
	s.metrics.observe(txKindMetaRead, start, err != nil)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "meta_get")
		return nil, false, fmt.Errorf("pgstore: meta get: %w", err)
	}
	return nonNil(v), true, nil
}

// MetaScan returns up to limit rows with lower <= key < upper in bytewise
// order (bytea compares as memcmp). A nil upper is unbounded. Callers page
// by passing the last key plus a zero byte as the next lower bound.
func (s *Store) MetaScan(ctx context.Context, lower, upper []byte, limit int) ([]MetaKV, error) {
	start := time.Now()
	_, span := tracer.Start(ctx, "pg.meta_scan")
	defer span.End()
	span.SetAttributes(attribute.Int("limit", limit))
	out, err := s.metaScan(ctx, lower, upper, limit)
	s.metrics.observe(txKindMetaRead, start, err != nil)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "meta_scan")
		return nil, fmt.Errorf("pgstore: meta scan: %w", err)
	}
	span.SetAttributes(attribute.Int("rows", len(out)))
	return out, nil
}

func (s *Store) metaScan(ctx context.Context, lower, upper []byte, limit int) ([]MetaKV, error) {
	// A nil upper encodes as NULL, which is what makes the bound optional.
	rows, err := s.pool.Query(ctx,
		`SELECT key, value FROM metadata_kv
		 WHERE key >= $1 AND ($2::bytea IS NULL OR key < $2)
		 ORDER BY key LIMIT $3`, nonNil(lower), upper, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MetaKV
	for rows.Next() {
		var kv MetaKV
		if err := rows.Scan(&kv.Key, &kv.Value); err != nil {
			return nil, err
		}
		kv.Key, kv.Value = nonNil(kv.Key), nonNil(kv.Value)
		out = append(out, kv)
	}
	return out, rows.Err()
}
