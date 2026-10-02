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

// MetaGetMany reads several metadata_kv keys in one autocommit query. It
// returns one value per key, in order, nil for an absent key; a present value
// is non-nil even when empty. Keys may repeat.
func (s *Store) MetaGetMany(ctx context.Context, keys [][]byte) ([][]byte, error) {
	start := time.Now()
	_, span := tracer.Start(ctx, "pg.meta_get_many")
	defer span.End()
	span.SetAttributes(attribute.Int("keys", len(keys)))
	out, err := s.metaGetMany(ctx, keys)
	s.metrics.observe(txKindMetaRead, start, err != nil)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "meta_get_many")
		return nil, fmt.Errorf("pgstore: meta get many: %w", err)
	}
	return out, nil
}

func (s *Store) metaGetMany(ctx context.Context, keys [][]byte) ([][]byte, error) {
	out := make([][]byte, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	at := make(map[string][]int, len(keys))
	params := make([][]byte, 0, len(keys))
	for i, key := range keys {
		k := string(key)
		if _, ok := at[k]; !ok {
			params = append(params, nonNil(key))
		}
		at[k] = append(at[k], i)
	}
	rows, err := s.pool.Query(ctx, `SELECT key, value FROM metadata_kv WHERE key = ANY($1::bytea[])`, params)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var k, v []byte
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		v = nonNil(v)
		for n, i := range at[string(k)] {
			if n > 0 {
				v = append([]byte{}, v...)
			}
			out[i] = v
		}
	}
	return out, rows.Err()
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
