package storagefake

import (
	"bytes"
	"context"
	"slices"

	"github.com/bluesky-social/jetstream/internal/metastore"
)

// MetaStore returns a metastore.Store over metadata_kv. Reads see the
// latest committed state. Commits go through commit, normally a leader
// session's catalog.Session.CommitMeta, so every metadata write is a fenced
// transaction (design §6.4). A nil commit gives a reader pod's read-only
// store.
func (db *DB) MetaStore(commit func(ctx context.Context, ops []metastore.Op) error) metastore.Store {
	return &metaStore{db: db, commit: commit}
}

type metaStore struct {
	db     *DB
	cl     *Client
	commit func(ctx context.Context, ops []metastore.Op) error
}

// Get and NewIter check the client but take no scheduler turn. A meta read
// holds no catalog lock and sees one published state, so where it lands
// among other calls does not matter to it. Callers do hold their own locks
// across meta reads: the atmos verifier keeps a per-DID sync.Mutex through
// its syncstate loads. synctest does not count a goroutine waiting on a
// sync.Mutex as durably blocked, so a read parked in Seeded while another
// goroutine waits for that mutex would stop Seeded's synctest.Wait from
// ever returning.
func (m *metaStore) Get(_ context.Context, key []byte) ([]byte, error) {
	if err := m.cl.alive(); err != nil {
		return nil, err
	}
	v, ok := m.db.current().meta.get(string(key))
	if !ok {
		return nil, metastore.ErrNotFound
	}
	return bytes.Clone(v), nil
}

func (m *metaStore) NewBatch() metastore.Batch {
	return metastore.NewOpBatch(func(ctx context.Context, ops []metastore.Op) error {
		if m.commit == nil {
			return metastore.ErrReadOnly
		}
		if len(ops) == 0 {
			return nil
		}
		return m.commit(ctx, ops)
	})
}

func (m *metaStore) Set(ctx context.Context, key, value []byte) error {
	b := m.NewBatch()
	b.Set(key, value)
	return b.Commit(ctx)
}

func (m *metaStore) Delete(ctx context.Context, key []byte) error {
	b := m.NewBatch()
	b.Delete(key)
	return b.Commit(ctx)
}

// NewIter reads the range from one committed state, which is stronger than
// the interface promises.
func (m *metaStore) NewIter(_ context.Context, lower, upper []byte) (metastore.Iterator, error) {
	if err := m.cl.alive(); err != nil {
		return nil, err
	}
	s := m.db.current()
	ks := s.meta.keys()
	start, _ := slices.BinarySearch(ks, string(lower))
	end := len(ks)
	if upper != nil {
		end, _ = slices.BinarySearch(ks, string(upper))
	}
	it := &metaIter{pos: -1}
	for _, k := range ks[start:max(start, end)] {
		v, _ := s.meta.get(k)
		it.keys = append(it.keys, []byte(k))
		it.vals = append(it.vals, bytes.Clone(v))
	}
	return it, nil
}

type metaIter struct {
	keys, vals [][]byte
	pos        int
}

func (it *metaIter) Next() bool {
	if it.pos < len(it.keys) {
		it.pos++
	}
	return it.pos < len(it.keys)
}

func (it *metaIter) Key() []byte   { return it.keys[it.pos] }
func (it *metaIter) Value() []byte { return it.vals[it.pos] }
func (it *metaIter) Err() error    { return nil }
func (it *metaIter) Close() error  { return nil }
