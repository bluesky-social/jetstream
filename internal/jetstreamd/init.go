package jetstreamd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/objstore"
)

// InitStorage is `jetstream storage init` (design §15.1): it creates a new
// archive in the PostgreSQL and S3 that cfg names and returns its archive
// row. It refuses a database that already holds an archive.
func InitStorage(ctx context.Context, cfg StorageConfig) (catalog.ArchiveRow, error) {
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
	return backend.Init(ctx, cfg.Leader.Lease)
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
