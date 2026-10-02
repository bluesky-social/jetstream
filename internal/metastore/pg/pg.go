// Package pg is the metastore.Store over PostgreSQL's metadata_kv table
// (design §14.2). Reads are autocommit and see the latest commit. Writes
// never go to PostgreSQL directly: every commit runs through the caller's
// commit function, normally the leader session's catalog.Session.CommitMeta,
// so each one is a fenced transaction (§6.4) and a fenced-out pod cannot
// write metadata. A nil commit function gives a reader pod's read-only view.
package pg

import (
	"bytes"
	"context"

	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/pgstore"
)

// DefaultPageSize is the iterator's keyset page (plan S2.4).
const DefaultPageSize = 10_000

// Config configures a Store.
type Config struct {
	// DB is the PostgreSQL handle reads go through.
	DB *pgstore.Store
	// Commit applies a batch's ops atomically in a fenced transaction. It
	// is called only with a non-empty batch. Nil makes the store read-only:
	// every write returns metastore.ErrReadOnly.
	Commit func(ctx context.Context, ops []metastore.Op) error
	// PageSize is the iterator page size. Zero means DefaultPageSize.
	PageSize int
}

// Store implements metastore.Store.
type Store struct {
	cfg Config
}

var _ metastore.Store = (*Store)(nil)

// New returns a Store.
func New(cfg Config) *Store {
	if cfg.PageSize <= 0 {
		cfg.PageSize = DefaultPageSize
	}
	return &Store{cfg: cfg}
}

// Get implements metastore.Store.
func (s *Store) Get(ctx context.Context, key []byte) ([]byte, error) {
	v, ok, err := s.cfg.DB.MetaGet(ctx, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, metastore.ErrNotFound
	}
	return v, nil
}

// NewBatch implements metastore.Store. The ops reach PostgreSQL through
// catalog's Tx.ApplyMeta, which coalesces runs into set-based statements
// (pgstore.MetaBatch).
func (s *Store) NewBatch() metastore.Batch {
	return metastore.NewOpBatch(func(ctx context.Context, ops []metastore.Op) error {
		if s.cfg.Commit == nil {
			return metastore.ErrReadOnly
		}
		if len(ops) == 0 {
			return nil
		}
		return s.cfg.Commit(ctx, ops)
	})
}

// Set implements metastore.Store.
func (s *Store) Set(ctx context.Context, key, value []byte) error {
	b := s.NewBatch()
	b.Set(key, value)
	return b.Commit(ctx)
}

// Delete implements metastore.Store.
func (s *Store) Delete(ctx context.Context, key []byte) error {
	b := s.NewBatch()
	b.Delete(key)
	return b.Commit(ctx)
}

// NewIter implements metastore.Store. It pages by key, so it holds no
// transaction or cursor open between pages, and a page never sees a
// half-applied commit. Rows committed between pages may or may not appear,
// which is all the interface promises.
func (s *Store) NewIter(ctx context.Context, lower, upper []byte) (metastore.Iterator, error) {
	it := &iter{s: s, ctx: ctx, next: bytes.Clone(lower), upper: bytes.Clone(upper), pos: -1}
	if upper != nil && bytes.Compare(it.next, upper) >= 0 {
		it.done = true
		return it, nil
	}
	if err := it.fetch(); err != nil {
		return nil, err
	}
	return it, nil
}

type iter struct {
	s     *Store
	ctx   context.Context
	next  []byte // lower bound of the next page
	upper []byte
	page  []pgstore.MetaKV
	pos   int
	done  bool // no page after the current one
	err   error
}

func (it *iter) fetch() error {
	page, err := it.s.cfg.DB.MetaScan(it.ctx, it.next, it.upper, it.s.cfg.PageSize)
	if err != nil {
		return err
	}
	it.page, it.pos = page, -1
	if len(page) < it.s.cfg.PageSize {
		it.done = true
		return nil
	}
	// The smallest key after last is last followed by a zero byte.
	last := page[len(page)-1].Key
	it.next = append(bytes.Clone(last), 0)
	return nil
}

func (it *iter) Next() bool {
	if it.err != nil {
		return false
	}
	if it.pos+1 < len(it.page) {
		it.pos++
		return true
	}
	if it.done {
		it.pos = len(it.page)
		return false
	}
	if err := it.fetch(); err != nil {
		it.err = err
		return false
	}
	return it.Next()
}

func (it *iter) Key() []byte   { return it.page[it.pos].Key }
func (it *iter) Value() []byte { return it.page[it.pos].Value }
func (it *iter) Err() error    { return it.err }
func (it *iter) Close() error  { return nil }
