// Package pebblestore is the local-mode metastore.Store: the Pebble database
// at <data-dir>/meta.pebble (docs/README.md §3.5), opened through
// internal/store so the jetstream_store_op_duration_seconds metrics keep
// their meaning.
//
// Every write syncs the WAL (store.SyncWrites), as before the metastore
// split. The only unsynced writes are the identity cache's, which go through
// SetNoSync/DeleteNoSync and never leave local mode.
package pebblestore

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"

	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// PebbleSubdir is the on-disk name of the metadata store relative to the data
// directory.
const PebbleSubdir = store.PebbleSubdir

// Metrics is the Pebble store's Prometheus state; see NewMetrics.
type Metrics = store.Metrics

// NewMetrics registers jetstream_store_op_duration_seconds against reg.
var NewMetrics = store.NewMetrics

// Store is a metastore.Store backed by Pebble. It is safe for concurrent use.
type Store struct {
	st      *store.Store
	dataDir string
	onDisk  bool
}

var (
	_ metastore.Store     = (*Store)(nil)
	_ metastore.DiskStats = (*Store)(nil)
)

type openOptions struct {
	fs vfs.FS
}

// Option configures Open.
type Option func(*openOptions)

// WithFS opens Pebble on fs instead of the process filesystem. Passing nil is
// a no-op.
func WithFS(fsys vfs.FS) Option {
	return func(o *openOptions) {
		if fsys != nil {
			o.fs = fsys
		}
	}
}

// Open opens (creating if necessary) <dataDir>/meta.pebble. m may be nil. The
// returned Store must be closed to release Pebble's file lock.
func Open(dataDir string, m *Metrics, opts ...Option) (*Store, error) {
	var o openOptions
	for _, opt := range opts {
		opt(&o)
	}
	st, err := store.Open(dataDir, m, store.WithFS(o.fs))
	if err != nil {
		return nil, err
	}
	return &Store{st: st, dataDir: dataDir, onDisk: o.fs == nil}, nil
}

// New wraps an already-open store. The caller keeps ownership: Close on the
// returned Store closes st. Transitional: jetstreamd opens the raw store once
// and hands it to both ported and unported packages until S1.6.
func New(st *store.Store, dataDir string) *Store {
	return &Store{st: st, dataDir: dataDir, onDisk: true}
}

// Close releases the database. Idempotent.
func (s *Store) Close() error { return s.st.Close() }

func (s *Store) Get(_ context.Context, key []byte) ([]byte, error) {
	val, closer, err := s.st.Get(key)
	if errors.Is(err, store.ErrNotFound) {
		return nil, metastore.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(val))
	copy(out, val)
	if err := closer.Close(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) Set(_ context.Context, key, value []byte) error {
	return s.st.Set(key, value, store.SyncWrites)
}

func (s *Store) Delete(_ context.Context, key []byte) error {
	return s.st.Delete(key, store.SyncWrites)
}

// SetNoSync writes without syncing the WAL. Only for rebuildable caches (the
// identity cache), where losing the last few writes on crash is harmless.
func (s *Store) SetNoSync(key, value []byte) error {
	return s.st.Set(key, value, pebble.NoSync)
}

// DeleteNoSync is the unsynced counterpart of Delete; see SetNoSync.
func (s *Store) DeleteNoSync(key []byte) error {
	return s.st.Delete(key, pebble.NoSync)
}

func (s *Store) NewBatch() metastore.Batch {
	return &batch{s: s, b: s.st.NewBatch()}
}

func (s *Store) NewIter(_ context.Context, lower, upper []byte) (metastore.Iterator, error) {
	it, err := s.st.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	return &iter{it: it}, nil
}

// DiskBytes reports the on-disk size of meta.pebble. It is zero when the
// store was opened on an alternate VFS.
func (s *Store) DiskBytes() (int64, error) {
	if !s.onDisk {
		return 0, nil
	}
	var total int64
	dir := filepath.Join(s.dataDir, PebbleSubdir)
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return fs.SkipAll
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		total += fi.Size()
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("pebblestore: walk %s: %w", dir, err)
	}
	return total, nil
}

// batch adapts *pebble.Batch. Pebble's staging methods only fail on a closed
// or read-only batch, so the first such error is latched and surfaced by
// Commit rather than widening the metastore.Batch signatures.
type batch struct {
	s    *Store
	b    *pebble.Batch
	err  error
	done bool
}

func (b *batch) Set(key, value []byte) {
	if b.err == nil {
		b.err = b.b.Set(key, value, nil)
	}
}

func (b *batch) Delete(key []byte) {
	if b.err == nil {
		b.err = b.b.Delete(key, nil)
	}
}

func (b *batch) DeleteRange(start, end []byte) {
	if b.err == nil {
		b.err = b.b.DeleteRange(start, end, nil)
	}
}

func (b *batch) Len() int { return int(b.b.Count()) }

func (b *batch) Commit(context.Context) error {
	if b.done {
		return metastore.ErrBatchCommitted
	}
	b.done = true
	defer func() { _ = b.b.Close() }()
	if b.err != nil {
		return fmt.Errorf("pebblestore: stage batch: %w", b.err)
	}
	return b.s.st.Commit(b.b, store.SyncWrites)
}

type iter struct {
	it      *pebble.Iterator
	started bool
}

func (i *iter) Next() bool {
	if !i.started {
		i.started = true
		return i.it.First()
	}
	return i.it.Next()
}

func (i *iter) Key() []byte   { return i.it.Key() }
func (i *iter) Value() []byte { return i.it.Value() }
func (i *iter) Err() error    { return i.it.Error() }
func (i *iter) Close() error  { return i.it.Close() }
