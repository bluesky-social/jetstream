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
package store

import (
	"fmt"
	"path/filepath"

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
// constant maintenance tax. Keep this small.
type Store struct {
	*pebble.DB
}

// Open opens (creating if necessary) the metadata pebble database
// at <dataDir>/meta.pebble. The data directory itself must already
// exist; pebble will create the inner db directory and any needed
// children.
//
// The returned Store must be Close()d to release file locks.
func Open(dataDir string) (*Store, error) {
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

	return &Store{DB: db}, nil
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
