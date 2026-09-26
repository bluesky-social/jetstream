// Package jetstreamdtest builds in-memory storage backends for tests that
// run a disaggregated jetstreamd.Runtime: storagefake for the catalog
// database and lease, memblob for the object store.
package jetstreamdtest

import (
	"context"
	"fmt"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/internal/leader"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/objstore/memblob"
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
	return &Backend{
		DB:   db,
		Blob: blob,
		Backend: &jetstreamd.StorageBackend{
			DB:        db,
			Listener:  db,
			Blob:      blob,
			Archive:   func(context.Context) (catalog.ArchiveRow, error) { return db.Archive(), nil },
			NewLease:  func() leader.Locker { return db.NewLease() },
			MetaStore: db.MetaStore,
		},
	}
}

// InitNamespaces creates segment 0 in main and bootstrap_live, under a
// lease the call takes and releases, as `jetstream storage init` leaves a
// new catalog.
func (b *Backend) InitNamespaces(ctx context.Context) error {
	lease := b.DB.NewLease()
	if err := lease.Acquire(ctx, time.Minute); err != nil {
		return fmt.Errorf("jetstreamdtest: acquire: %w", err)
	}
	defer func() { _ = lease.Release(context.WithoutCancel(ctx)) }()
	sess := catalog.NewSession(catalog.SessionConfig{DB: b.DB, Epoch: lease.Epoch()})
	for _, ns := range []catalog.Namespace{catalog.Main, catalog.BootstrapLive} {
		if _, err := sess.InitNamespace(ctx, ns, nil); err != nil {
			return fmt.Errorf("jetstreamdtest: init %s: %w", ns, err)
		}
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
