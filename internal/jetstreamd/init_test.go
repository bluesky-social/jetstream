package jetstreamd_test

import (
	"errors"
	"testing"
	"time"

	"github.com/jcalabro/atmos/streaming"
	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/internal/jetstreamd/jetstreamdtest"
	"github.com/bluesky-social/jetstream/internal/objstore/memblob"
	"github.com/bluesky-social/jetstream/internal/pgstore"
	"github.com/bluesky-social/jetstream/internal/storagefake"
)

// activeSegments maps each namespace to its active segment indexes.
func activeSegments(t *testing.T, db *storagefake.DB) map[catalog.Namespace][]uint64 {
	t.Helper()
	snap, err := db.Snapshot()
	require.NoError(t, err)
	out := map[catalog.Namespace][]uint64{}
	for _, s := range snap.Segments {
		if s.State == catalog.Active {
			out[s.Namespace] = append(out[s.Namespace], s.Index)
		}
	}
	return out
}

func TestStorageInit_CreatesFirstSegments(t *testing.T) {
	t.Parallel()
	fake := jetstreamdtest.New(storagefake.Config{})

	archive, err := fake.Backend.Init(t.Context(), time.Minute)
	require.NoError(t, err)
	require.Equal(t, fake.DB.Archive().ArchiveID, archive.ArchiveID)
	require.Equal(t, map[catalog.Namespace][]uint64{
		catalog.Main:          {0},
		catalog.BootstrapLive: {0},
	}, activeSegments(t, fake.DB))
	require.Empty(t, fake.Blob.Keys(), "the probe deletes its object")

	// Init released the lease, so the first leader need not wait it out.
	require.NoError(t, fake.DB.NewLease().Acquire(t.Context(), time.Minute))
	require.NoError(t, fake.DB.Violation())
}

func TestStorageInit_RefusesInitializedArchive(t *testing.T) {
	t.Parallel()
	fake := jetstreamdtest.New(storagefake.Config{})
	_, err := fake.Backend.Init(t.Context(), time.Minute)
	require.NoError(t, err)
	commits := fake.DB.Commits()

	_, err = fake.Backend.Init(t.Context(), time.Minute)
	require.ErrorIs(t, err, pgstore.ErrInitialized)
	require.Equal(t, commits, fake.DB.Commits(), "a refused init writes nothing")
}

// A bad bucket or credential fails the probe before the archive row
// exists, so init can run again once the operator fixes it.
func TestStorageInit_ProbeFailureLeavesDatabaseEmpty(t *testing.T) {
	t.Parallel()
	fake := jetstreamdtest.New(storagefake.Config{})
	good := fake.Backend.Blob
	putErr := errors.New("access denied")
	fake.Backend.Blob = memblob.New(memblob.WithFaultInjector(&memblob.KeyPrefixFault{
		Op: memblob.OpPut, Ordinal: 1, Err: putErr,
	}))

	_, err := fake.Backend.Init(t.Context(), time.Minute)
	require.ErrorIs(t, err, putErr)
	require.ErrorContains(t, err, "object store probe")
	require.Zero(t, fake.DB.Commits())

	fake.Backend.Blob = good
	_, err = fake.Backend.Init(t.Context(), time.Minute)
	require.NoError(t, err)
	require.Len(t, activeSegments(t, fake.DB), 2)
}

// A lease held elsewhere stops init after the archive row commits. The
// archive is then initialized with no segments, which the first leader
// session repairs (design §10.10).
func TestStorageInit_LeaseHeld(t *testing.T) {
	t.Parallel()
	fake := jetstreamdtest.New(storagefake.Config{})
	require.NoError(t, fake.DB.NewLease().Acquire(t.Context(), time.Minute))

	_, err := fake.Backend.Init(t.Context(), time.Minute)
	require.ErrorIs(t, err, streaming.ErrLockHeld)
	require.Empty(t, activeSegments(t, fake.DB))

	_, err = fake.Backend.Init(t.Context(), time.Minute)
	require.ErrorIs(t, err, pgstore.ErrInitialized)
}

func TestInitStorage_Validation(t *testing.T) {
	t.Parallel()
	valid := func() jetstreamd.StorageConfig {
		cfg := jetstreamd.DefaultStorageConfig()
		cfg.PG.URL = "postgres://jetstream:hunter2@127.0.0.1:1/jetstream"
		cfg.S3.Region = "us-east-1"
		cfg.S3.Bucket = "jetstream"
		return cfg
	}
	for _, tc := range []struct {
		name   string
		mutate func(*jetstreamd.StorageConfig)
		want   string
	}{
		{"pg url", func(c *jetstreamd.StorageConfig) { c.PG.URL = "" }, "JETSTREAM_PG_URL is required"},
		{"region", func(c *jetstreamd.StorageConfig) { c.S3.Region = "" }, "JETSTREAM_S3_REGION is required"},
		{"bucket", func(c *jetstreamd.StorageConfig) { c.S3.Bucket = "" }, "JETSTREAM_S3_BUCKET is required"},
		{"lease", func(c *jetstreamd.StorageConfig) { c.Leader.Lease = 0 }, "JETSTREAM_LEADER_LEASE must be > 0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := valid()
			tc.mutate(&cfg)
			_, err := jetstreamd.InitStorage(t.Context(), cfg)
			require.ErrorContains(t, err, tc.want)
			require.NotContains(t, err.Error(), "hunter2")
		})
	}
}

// Migration plan §6.2: init for a migration creates only main's segment 0
// and migration/state seeding, together; it finishes a run that failed
// after the archive row, and refuses any archive with more in it.
func TestStorageInit_MigrateFromLocal(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	fake := jetstreamdtest.New(storagefake.Config{})
	require.NoError(t, fake.Backend.CreateArchive(ctx, [16]byte{1}), "a failed run left the archive row")

	for range 2 {
		_, err := fake.Backend.InitMigration(ctx, time.Minute)
		require.NoError(t, err)
		require.Equal(t, map[catalog.Namespace][]uint64{catalog.Main: {0}}, activeSegments(t, fake.DB))
		require.NoError(t, fake.DB.Violation())
	}
	snap, err := fake.DB.Snapshot()
	require.NoError(t, err)
	require.Equal(t, []byte(catalog.MigrationSeeding), snap.Meta[catalog.MigrationStateKey])
	require.NoError(t, fake.DB.NewLease().Acquire(ctx, time.Minute), "init released the lease")

	plain := jetstreamdtest.New(storagefake.Config{})
	require.NoError(t, plain.InitNamespaces(ctx))
	_, err = plain.Backend.InitMigration(ctx, time.Minute)
	require.ErrorIs(t, err, catalog.ErrNotEmpty)
}
