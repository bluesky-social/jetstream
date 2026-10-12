package jetstreamd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/objstore"
	"github.com/bluesky-social/jetstream/internal/pgstore"
)

// InitStorage is `jetstream storage init` (design §15.1): it creates a new
// archive in the PostgreSQL and S3 that cfg names and returns its archive
// row. It refuses a database that already holds an archive.
func InitStorage(ctx context.Context, cfg StorageConfig) (catalog.ArchiveRow, error) {
	return initStorage(ctx, cfg, (*StorageBackend).Init)
}

// InitMigrationStorage is `jetstream storage init --migrate-from-local`: an
// archive for a local one to migrate into (migration plan §6.2).
func InitMigrationStorage(ctx context.Context, cfg StorageConfig) (catalog.ArchiveRow, error) {
	return initStorage(ctx, cfg, (*StorageBackend).InitMigration)
}

func initStorage(ctx context.Context, cfg StorageConfig, initFn func(*StorageBackend, context.Context, time.Duration) (catalog.ArchiveRow, error)) (catalog.ArchiveRow, error) {
	// Errors name variables, never values, so none can carry the password.
	switch {
	case cfg.PG.URL == "":
		return catalog.ArchiveRow{}, errors.New("storage init: JETSTREAM_PG_URL is required")
	case cfg.S3.Region == "":
		return catalog.ArchiveRow{}, errors.New("storage init: JETSTREAM_S3_REGION is required")
	case cfg.S3.Bucket == "":
		return catalog.ArchiveRow{}, errors.New("storage init: JETSTREAM_S3_BUCKET is required")
	case cfg.Leader.Lease <= 0:
		return catalog.ArchiveRow{}, fmt.Errorf("storage init: JETSTREAM_LEADER_LEASE must be > 0, got %s", cfg.Leader.Lease)
	}
	backend, err := openBackend(ctx, cfg, nil, nil)
	if err != nil {
		return catalog.ArchiveRow{}, fmt.Errorf("storage init: %w", err)
	}
	defer backend.Close()
	return initFn(backend, ctx, cfg.Leader.Lease)
}

// Init runs the design §15.1 steps on b, holding the writer lease for
// lease while it creates the first segments.
//
// The probe runs first, under the archive id about to be inserted: a bad
// credential or bucket then leaves the database empty, and init can run
// again once it is fixed. A failure after the archive row commits leaves
// an archive that init refuses; the first leader session creates any
// segment 0 still missing (design §10.10).
func (b *StorageBackend) Init(ctx context.Context, lease time.Duration) (catalog.ArchiveRow, error) {
	if b.CreateArchive == nil {
		return catalog.ArchiveRow{}, errors.New("storage init: backend cannot create an archive")
	}
	id := objstore.NewUUID()
	if err := objstore.Probe(ctx, b.Blob, id); err != nil {
		return catalog.ArchiveRow{}, fmt.Errorf("storage init: object store probe: %w", err)
	}
	if err := b.CreateArchive(ctx, id); err != nil {
		return catalog.ArchiveRow{}, fmt.Errorf("storage init: create archive: %w", err)
	}
	archive, err := b.Archive(ctx)
	if err != nil {
		return catalog.ArchiveRow{}, fmt.Errorf("storage init: check catalog: %w", err)
	}

	locker := b.NewLease()
	if err := locker.Acquire(ctx, lease); err != nil {
		return archive, fmt.Errorf("storage init: acquire the writer lease to create the first segments: %w", err)
	}
	// A failed release is not a failed init: the lease expires, which
	// only delays the first leader by one lease.
	defer func() { _ = locker.Release(context.WithoutCancel(ctx)) }()
	sess := catalog.NewSession(catalog.SessionConfig{DB: b.DB, Epoch: locker.Epoch()})
	for _, ns := range []catalog.Namespace{catalog.Main, catalog.BootstrapLive} {
		if _, err := sess.InitNamespace(ctx, ns, nil); err != nil {
			return archive, fmt.Errorf("storage init: create segment 0 in %s: %w", ns, err)
		}
	}
	return archive, nil
}

// InitMigration is Init for an archive a local one migrates into: Main's
// segment 0 and migration/state seeding, in one transaction, and no
// bootstrap_live, since a migrated archive is past merge. Unlike Init, it
// runs again on an archive row a failed run left behind, which it finishes;
// it still refuses any catalog that holds more than that.
func (b *StorageBackend) InitMigration(ctx context.Context, lease time.Duration) (catalog.ArchiveRow, error) {
	if b.CreateArchive == nil {
		return catalog.ArchiveRow{}, errors.New("storage init: backend cannot create an archive")
	}
	id := objstore.NewUUID()
	if err := objstore.Probe(ctx, b.Blob, id); err != nil {
		return catalog.ArchiveRow{}, fmt.Errorf("storage init: object store probe: %w", err)
	}
	if err := b.CreateArchive(ctx, id); err != nil && !errors.Is(err, pgstore.ErrInitialized) {
		return catalog.ArchiveRow{}, fmt.Errorf("storage init: create archive: %w", err)
	}
	archive, err := b.Archive(ctx)
	if err != nil {
		return catalog.ArchiveRow{}, fmt.Errorf("storage init: check catalog: %w", err)
	}
	locker := b.NewLease()
	if err := locker.Acquire(ctx, lease); err != nil {
		return archive, fmt.Errorf("storage init: acquire the writer lease to start the migration: %w", err)
	}
	defer func() { _ = locker.Release(context.WithoutCancel(ctx)) }()
	sess := catalog.NewSession(catalog.SessionConfig{DB: b.DB, Epoch: locker.Epoch()})
	if _, err := sess.InitMigration(ctx); err != nil {
		return archive, fmt.Errorf("storage init: start the migration: %w", err)
	}
	return archive, nil
}
