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
	"github.com/bluesky-social/jetstream/internal/migrate"
	"github.com/bluesky-social/jetstream/internal/objstore/memblob"
	"github.com/bluesky-social/jetstream/internal/pgstore"
	"github.com/bluesky-social/jetstream/internal/storagefake"
)

// Backend is a fake backend and the pieces a test inspects.
type Backend struct {
	Backend *jetstreamd.StorageBackend
	DB      *storagefake.DB
	Blob    *memblob.Blob
	// Control is the migration control channel the backend hands out.
	Control *migrate.MemControl
}

// New returns an empty fake backend.
func New(cfg storagefake.Config) *Backend {
	db := storagefake.New(cfg)
	blob := memblob.New()
	// The fake starts with its archive row, so CreateArchive models an
	// empty database: the first call succeeds, and later ones refuse as
	// PostgreSQL would.
	var created atomic.Bool
	control := &migrate.MemControl{}
	return &Backend{
		DB:      db,
		Blob:    blob,
		Control: control,
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
			MigrationControl: func(context.Context) (migrate.Control, error) {
				return control, nil
			},
			MigrationOperator: func(context.Context) (jetstreamd.MigrationOperator, error) {
				return memOperator{control}, nil
			},
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

// InitMigration runs `jetstream storage init --migrate-from-local` on the
// fake: main segment 0 and migration/state seeding, with no phase.
func (b *Backend) InitMigration(ctx context.Context) error {
	if _, err := b.Backend.InitMigration(ctx, time.Minute); err != nil {
		return fmt.Errorf("jetstreamdtest: %w", err)
	}
	return nil
}

// SetMigration moves migration/state from one state to another, with meta,
// as the migrator does.
func (b *Backend) SetMigration(ctx context.Context, from, to catalog.MigrationState, meta ...metastore.Op) error {
	return b.withSession(ctx, func(sess *catalog.Session) error {
		_, err := sess.SetMigrationState(ctx, from, to, meta)
		return err
	})
}

func (b *Backend) withSession(ctx context.Context, fn func(*catalog.Session) error) error {
	lease := b.DB.NewLease()
	if err := lease.Acquire(ctx, time.Minute); err != nil {
		return fmt.Errorf("jetstreamdtest: acquire: %w", err)
	}
	defer func() { _ = lease.Release(context.WithoutCancel(ctx)) }()
	if err := fn(catalog.NewSession(catalog.SessionConfig{DB: b.DB, Epoch: lease.Epoch()})); err != nil {
		return fmt.Errorf("jetstreamdtest: %w", err)
	}
	return nil
}

// memOperator is jetstreamd.MigrationOperator over a MemControl.
type memOperator struct{ c *migrate.MemControl }

func (o memOperator) Request(_ context.Context, action string) (int64, error) {
	return o.c.Request(action), nil
}

func (o memOperator) Result(_ context.Context, id int64) (string, bool, bool, error) {
	r, answered, claimed := o.c.Result(id)
	return r, answered, claimed, nil
}

func (o memOperator) Withdraw(_ context.Context, id int64, result string) (bool, error) {
	return o.c.Withdraw(id, result), nil
}

func (o memOperator) Status(context.Context) ([]byte, time.Time, bool, error) {
	doc := o.c.LastStatus()
	return doc, time.Now(), doc != nil, nil
}
