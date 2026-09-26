package jetstreamd_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/internal/jetstreamd/jetstreamdtest"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/objstore"
	"github.com/bluesky-social/jetstream/internal/objstore/memblob"
	"github.com/bluesky-social/jetstream/internal/storagefake"
	"github.com/bluesky-social/jetstream/internal/xrpcapi"
)

func disaggOptions(t *testing.T, backend *jetstreamd.StorageBackend) jetstreamd.Options {
	t.Helper()
	storage := jetstreamd.DefaultStorageConfig()
	storage.Mode = jetstreamd.StorageDisaggregated
	storage.Leader.AcquireInterval = 10 * time.Millisecond
	return jetstreamd.Options{
		Storage:                        storage,
		StorageBackend:                 backend,
		MemoryLimit:                    8 << 30,
		Headless:                       true,
		RelayURL:                       "http://127.0.0.1:1",
		OTelServiceName:                "jetstream-test",
		LogLevel:                       "warn",
		LogFormat:                      "text",
		LogOutput:                      &bytes.Buffer{},
		ShutdownTimeout:                5 * time.Second,
		ClientDrainTimeout:             time.Second,
		CursorLookback:                 36 * time.Hour,
		PlanMaxDIDs:                    xrpcapi.DefaultPlanMaxDIDs,
		PlanMaxCollections:             xrpcapi.DefaultPlanMaxCollections,
		PlanMaxEntries:                 xrpcapi.DefaultPlanMaxEntries,
		PlanWholeSegmentThreshold:      xrpcapi.DefaultPlanWholeSegmentThreshold,
		SubscribeReadLogRetentionBytes: 1 << 20,
		SubscribeBlockCacheBytes:       1 << 20,
		SubscribeReadBatch:             128,
		SubscribeSlowWindow:            time.Second,
		SubscribeSlowMinRate:           5,
		CursorBlockIndexCacheSize:      32,
	}
}

// Design §17: the pod refuses to start without a memory limit, and when the
// budgets overrun it the error names each one.
func TestBuildDisaggregated_MemoryBudgets(t *testing.T) {
	t.Parallel()
	backend := jetstreamdtest.New(storagefake.Config{}).Backend

	t.Run("over", func(t *testing.T) {
		t.Parallel()
		opts := disaggOptions(t, backend)
		opts.MemoryLimit = 1 << 30
		_, err := jetstreamd.Build(t.Context(), opts)
		require.ErrorContains(t, err, "over 75% of GOMEMLIMIT")
		for _, name := range []string{
			"JETSTREAM_SUBSCRIBE_READ_LOG_RETENTION_BYTES", "JETSTREAM_SUBSCRIBE_BLOCK_CACHE_BYTES",
			"JETSTREAM_OBJECT_CACHE_BYTES", "JETSTREAM_HOT_PENDING_BYTES", "JETSTREAM_COMPACTION_MEMORY_BYTES",
		} {
			require.ErrorContains(t, err, name)
		}
	})
	t.Run("gc delay too short", func(t *testing.T) {
		t.Parallel()
		opts := disaggOptions(t, backend)
		opts.Storage.GC.Delay = opts.Storage.MaxViewAge
		_, err := jetstreamd.Build(t.Context(), opts)
		require.ErrorContains(t, err, "JETSTREAM_GC_DELAY")
	})
}

// A store that denies a read of the pod's own write fails startup as a
// configuration error rather than surfacing later as corruption.
func TestBuildDisaggregated_ObjectStoreCanary(t *testing.T) {
	t.Parallel()
	fake := jetstreamdtest.New(storagefake.Config{})
	fake.Backend.Blob = memblob.New(memblob.WithFaultInjector(&memblob.KeyPrefixFault{
		Prefix:  objstore.FormatUUID(fake.DB.Archive().ArchiveID) + "/probe/",
		Op:      memblob.OpGet,
		Ordinal: 1,
		Err:     errors.New("403 AccessDenied"),
	}))
	_, err := jetstreamd.Build(t.Context(), disaggOptions(t, fake.Backend))
	require.ErrorContains(t, err, "object store canary")
	require.ErrorContains(t, err, "AccessDenied")
}

// An injected backend stands in for the PostgreSQL and S3 settings.
func TestStorageValidate_BackendReplacesPGAndS3(t *testing.T) {
	t.Parallel()
	opts := disaggOptions(t, nil)
	require.ErrorContains(t, opts.Storage.Validate(opts), "JETSTREAM_PG_URL is required")
	opts.StorageBackend = jetstreamdtest.New(storagefake.Config{}).Backend
	require.NoError(t, opts.Storage.Validate(opts))
}

// A catalog in merging without its bootstrap_live namespace cannot come from
// any crash: merge's final transaction deletes the namespace and writes
// steady_state together. The session fails as corruption, which ends the
// process instead of retrying until the deadline.
func TestRunDisaggregated_MergingWithoutBootstrapLive(t *testing.T) {
	t.Parallel()
	fake := jetstreamdtest.New(storagefake.Config{})
	require.NoError(t, fake.SeedPhase(t.Context(), lifecycle.PhaseMerging))
	rt, err := jetstreamd.Build(t.Context(), disaggOptions(t, fake.Backend))
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	err = rt.Run(ctx)
	require.ErrorContains(t, err, "bootstrap_live has no segments")
	_, corrupt := catalog.IsCorruption(err)
	require.True(t, corrupt, "got %v", err)
	require.False(t, errors.Is(err, context.DeadlineExceeded), "corruption is fatal, not a retry until timeout")
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeCancel()
	require.NoError(t, rt.Close(closeCtx))
}
