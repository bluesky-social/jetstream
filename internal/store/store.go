// Package store owns the lifecycle of the metadata pebble database
// at <data-dir>/meta.pebble (DESIGN.md §3.4 / §3.5).
//
// The package is deliberately keyspace-agnostic. It knows how to
// open and close the database, picks pebble configuration that fits
// jetstream's access patterns (point lookups for per-DID rows, range
// scans for the eventual segment manifest), and exposes the
// underlying *pebble.DB so consumers can compose batches, iterators,
// and snapshots without a sea of passthrough wrappers.
//
// Per-keyspace operations (e.g. repo/<did>, account/<did>,
// bootstrap/state) live in the package that owns that keyspace —
// they take a *Store and assemble keys themselves. That keeps this
// package small enough to reuse from compaction, replica state,
// timestamp import, etc., without each consumer growing a peer
// abstraction.
//
// Observability: *Store shadows pebble's hot-path Get/Set/Delete and
// adds an instrumented Commit(b, opts) so duration histograms cover
// every metadata-store touch. NewBatch / NewIter / Snapshot stay
// promoted from the embedded *pebble.DB unchanged — they're cheap
// and don't need per-call timing.
package store

import (
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
)

// PebbleSubdir is the on-disk name of the metadata store relative
// to the configured data directory. DESIGN.md §3.4 pins this layout
// so the on-disk format is stable across replicas.
const PebbleSubdir = "meta.pebble"

// Store is the typed handle to the metadata pebble database. It is
// safe for concurrent use; pebble itself is.
//
// The embedded *pebble.DB is exposed deliberately rather than
// hidden behind passthrough methods. Consumers (backfill, future
// compaction code, etc.) typically need NewBatch / NewIter /
// Snapshot directly, and re-exporting every method we'd need would
// be both unprincipled (which slice of pebble do we expose?) and a
// constant maintenance tax.
//
// The instrumented Get/Set/Delete/Commit methods on *Store shadow
// the equivalent embedded pebble methods so callers picking up
// *Store automatically observe the histogram. Operations off the
// hot path (NewBatch, NewIter, Snapshot) come through as plain
// promoted methods. metrics may be nil; in that case the observe
// calls are no-ops (see Metrics).
type Store struct {
	*pebble.DB
	metrics *Metrics
}

// Open opens (creating if necessary) the metadata pebble database
// at <dataDir>/meta.pebble. The data directory itself must already
// exist; pebble will create the inner db directory and any needed
// children.
//
// m may be nil, in which case the store records no metrics. This
// keeps tests cheap and lets callers that don't care about
// observability stay unwired.
//
// The returned Store must be Close()d to release file locks.
func Open(dataDir string, m *Metrics) (*Store, error) {
	if dataDir == "" {
		return nil, fmt.Errorf("store: Open: data dir is required")
	}

	path := filepath.Join(dataDir, PebbleSubdir)

	// We deliberately keep the pebble configuration close to defaults.
	// The metadata store carries one row per DID (~30M today) plus a
	// small handful of singletons; that fits the default LSM shape
	// comfortably. A bloom filter on point lookups keeps the hot
	// per-DID Get path off disk for negative lookups, which the
	// backfill seed step performs once per relay listRepos entry.
	opts := &pebble.Options{}
	opts.EnsureDefaults()
	for i := range opts.Levels {
		opts.Levels[i].FilterPolicy = bloom.FilterPolicy(10)
	}

	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, fmt.Errorf("store: open pebble at %s: %w", path, err)
	}

	return &Store{DB: db, metrics: m}, nil
}

// Close releases the metadata db. Idempotent: subsequent calls are
// no-ops.
func (s *Store) Close() error {
	if s.DB == nil {
		return nil
	}
	err := s.DB.Close()
	s.DB = nil
	if err != nil {
		return fmt.Errorf("store: close pebble: %w", err)
	}
	return nil
}

// Get is the instrumented version of *pebble.DB.Get. It shadows
// the promoted method so callers automatically observe the
// histogram. Outcome is classified as ok / notfound / error inside
// metrics.ObserveGet.
func (s *Store) Get(key []byte) ([]byte, io.Closer, error) {
	start := time.Now()
	val, closer, err := s.DB.Get(key)
	s.metrics.ObserveGet(start, err)
	return val, closer, err
}

// Set is the instrumented version of *pebble.DB.Set.
func (s *Store) Set(key, value []byte, opts *pebble.WriteOptions) error {
	start := time.Now()
	err := s.DB.Set(key, value, opts)
	s.metrics.ObserveSet(start, err)
	return err
}

// Delete is the instrumented version of *pebble.DB.Delete.
func (s *Store) Delete(key []byte, opts *pebble.WriteOptions) error {
	start := time.Now()
	err := s.DB.Delete(key, opts)
	s.metrics.ObserveDelete(start, err)
	return err
}

// Commit is the instrumented version of pebble.Batch.Commit. Use
// it in place of b.Commit(opts) so the duration histogram captures
// batch commits alongside single-key writes.
func (s *Store) Commit(b *pebble.Batch, opts *pebble.WriteOptions) error {
	start := time.Now()
	err := b.Commit(opts)
	s.metrics.ObserveBatchCommit(start, err)
	return err
}
