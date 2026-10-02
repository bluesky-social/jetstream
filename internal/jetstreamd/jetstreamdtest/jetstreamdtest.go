// Package jetstreamdtest builds in-memory storage backends for tests that
// run a disaggregated jetstreamd.Runtime: storagefake for the catalog
// database and lease, memblob for the object store.
package jetstreamdtest

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/internal/leader"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/objstore/memblob"
	"github.com/bluesky-social/jetstream/internal/pgstore"
	"github.com/bluesky-social/jetstream/internal/storagefake"
)

// Backend is a fake backend and the pieces a test inspects.
type Backend struct {
	Backend *jetstreamd.StorageBackend
	DB      *storagefake.DB
	Blob    *memblob.Blob
}

// New returns an empty fake backend.
func New(cfg storagefake.Config) *Backend {
	db := storagefake.New(cfg)
	blob := memblob.New()
	// The fake starts with its archive row, so CreateArchive models an
	// empty database: the first call succeeds, and later ones refuse as
	// PostgreSQL would.
	var created atomic.Bool
	return &Backend{
		DB:   db,
		Blob: blob,
		Backend: &jetstreamd.StorageBackend{
			DB:       db,
			Listener: db,
			Blob:     blob,
			Archive:  func(context.Context) (catalog.ArchiveRow, error) { return db.Archive(), nil },
			CreateArchive: func(context.Context, [16]byte) error {
				if created.Swap(true) {
					return pgstore.ErrInitialized
				}
				return nil
			},
			NewLease:  func() leader.Locker { return db.NewLease() },
			MetaStore: db.MetaStore,
		},
	}
}

// InitNamespaces runs `jetstream storage init` on the fake, which leaves
// segment 0 in main and bootstrap_live.
func (b *Backend) InitNamespaces(ctx context.Context) error {
	if _, err := b.Backend.Init(ctx, time.Minute); err != nil {
		return fmt.Errorf("jetstreamdtest: %w", err)
	}
	return nil
}

// SeedPhase initializes main and records phase p, under a lease the call
// takes and releases, as a finished merge leaves the catalog.
func (b *Backend) SeedPhase(ctx context.Context, p lifecycle.Phase) error {
	lease := b.DB.NewLease()
	if err := lease.Acquire(ctx, time.Minute); err != nil {
		return fmt.Errorf("jetstreamdtest: acquire: %w", err)
	}
	defer func() { _ = lease.Release(context.WithoutCancel(ctx)) }()
	sess := catalog.NewSession(catalog.SessionConfig{DB: b.DB, Epoch: lease.Epoch()})
	if _, err := sess.InitNamespace(ctx, catalog.Main, nil); err != nil {
		return fmt.Errorf("jetstreamdtest: init main: %w", err)
	}
	meta := b.DB.MetaStore(func(ctx context.Context, ops []metastore.Op) error {
		_, err := sess.CommitMeta(ctx, ops)
		return err
	})
	if err := lifecycle.WritePhase(ctx, meta, p, time.Now()); err != nil {
		return fmt.Errorf("jetstreamdtest: write phase: %w", err)
	}
	return nil
}
