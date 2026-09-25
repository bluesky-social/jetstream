// Package memstore is an in-memory metastore.Store for unit tests. It becomes
// storagefake's metadata_kv table in stage 2 (plan D3).
package memstore

import (
	"bytes"
	"context"
	"slices"
	"sync"

	"github.com/bluesky-social/jetstream/internal/metastore"
)

// Store is a concurrency-safe sorted in-memory map.
type Store struct {
	mu   sync.RWMutex
	data map[string][]byte
	// sorted caches the ordered key list; nil after any write so the next
	// iterator rebuilds it.
	sorted []string
}

var _ metastore.Store = (*Store)(nil)

// New returns an empty Store.
func New() *Store {
	return &Store{data: make(map[string][]byte)}
}

func (s *Store) Get(_ context.Context, key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[string(key)]
	if !ok {
		return nil, metastore.ErrNotFound
	}
	return bytes.Clone(v), nil
}

func (s *Store) Set(ctx context.Context, key, value []byte) error {
	b := s.NewBatch()
	b.Set(key, value)
	return b.Commit(ctx)
}

func (s *Store) Delete(ctx context.Context, key []byte) error {
	b := s.NewBatch()
	b.Delete(key)
	return b.Commit(ctx)
}

func (s *Store) NewBatch() metastore.Batch {
	return metastore.NewOpBatch(s.apply)
}

func (s *Store) apply(_ context.Context, ops []metastore.Op) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, op := range ops {
		switch op.Kind {
		case metastore.OpSet:
			// OpBatch already copied the value; an empty value must stay
			// present, so normalize nil to empty.
			v := op.Value
			if v == nil {
				v = []byte{}
			}
			s.data[string(op.Key)] = v
		case metastore.OpDelete:
			delete(s.data, string(op.Key))
		case metastore.OpDeleteRange:
			for k := range s.data {
				if k >= string(op.Key) && k < string(op.End) {
					delete(s.data, k)
				}
			}
		}
	}
	if len(ops) > 0 {
		s.sorted = nil
	}
	return nil
}

// NewIter snapshots the range at creation. That is stronger than the
// interface promises, which is fine for a test double.
func (s *Store) NewIter(_ context.Context, lower, upper []byte) (metastore.Iterator, error) {
	s.mu.Lock()
	if s.sorted == nil {
		s.sorted = make([]string, 0, len(s.data))
		for k := range s.data {
			s.sorted = append(s.sorted, k)
		}
		slices.Sort(s.sorted)
	}
	start, _ := slices.BinarySearch(s.sorted, string(lower))
	end := len(s.sorted)
	if upper != nil {
		end, _ = slices.BinarySearch(s.sorted, string(upper))
	}
	it := &iter{pos: -1}
	if start < end {
		it.keys = make([][]byte, 0, end-start)
		it.vals = make([][]byte, 0, end-start)
		for _, k := range s.sorted[start:end] {
			it.keys = append(it.keys, []byte(k))
			it.vals = append(it.vals, bytes.Clone(s.data[k]))
		}
	}
	s.mu.Unlock()
	return it, nil
}

type iter struct {
	keys, vals [][]byte
	pos        int
}

func (it *iter) Next() bool {
	if it.pos < len(it.keys) {
		it.pos++
	}
	return it.pos < len(it.keys)
}

func (it *iter) Key() []byte   { return it.keys[it.pos] }
func (it *iter) Value() []byte { return it.vals[it.pos] }
func (it *iter) Err() error    { return nil }
func (it *iter) Close() error  { return nil }
