package jetstreamd

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/importer"
	"github.com/bluesky-social/jetstream/internal/ingest/orchestrator"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/internal/xrpcapi"
	"github.com/stretchr/testify/require"
)

func TestRuntimePublicAddrBeforeRunIsEmpty(t *testing.T) {
	t.Parallel()

	t.Run("nil runtime", func(t *testing.T) {
		t.Parallel()
		var rt *Runtime
		require.Empty(t, rt.PublicAddr())
	})

	t.Run("zero value", func(t *testing.T) {
		t.Parallel()
		rt := &Runtime{}
		require.Empty(t, rt.PublicAddr())
	})

	t.Run("built before run", func(t *testing.T) {
		t.Parallel()
		rt, err := Build(t.Context(), testOptions(t))
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, rt.Close(context.Background()))
		})

		require.Empty(t, rt.PublicAddr())
	})
}

func TestOptionsValidateRejectsNegativeSegmentCache(t *testing.T) {
	t.Parallel()

	opts := testOptions(t)
	opts.SegmentCacheMaxAge = -time.Second
	_, err := Build(t.Context(), opts)
	require.ErrorContains(t, err, "SegmentCacheMaxAge must be >= 0")
}

func TestOptionsValidateRejectsNegativeBackfillWorkers(t *testing.T) {
	t.Parallel()

	opts := testOptions(t)
	opts.BackfillWorkers = -1
	_, err := Build(t.Context(), opts)
	require.ErrorContains(t, err, "BackfillWorkers must be >= 0")
}

func TestOptionsValidateRejectsNegativeBackfillBatchSize(t *testing.T) {
	t.Parallel()

	opts := testOptions(t)
	opts.BackfillBatchSize = -1
	_, err := Build(t.Context(), opts)
	require.ErrorContains(t, err, "BackfillBatchSize must be >= 0")
}

func TestOptionsValidateRejectsNegativeBackfillAsyncFlushWorkers(t *testing.T) {
	t.Parallel()

	opts := testOptions(t)
	opts.BackfillAsyncFlushWorkers = -1
	_, err := Build(t.Context(), opts)
	require.ErrorContains(t, err, "BackfillAsyncFlushWorkers must be >= 0")
}

func TestOptionsValidateRejectsNegativeCompactionRewriteWorkers(t *testing.T) {
	t.Parallel()

	opts := testOptions(t)
	opts.CompactionRewriteWorkers = -1
	_, err := Build(t.Context(), opts)
	require.ErrorContains(t, err, "CompactionRewriteWorkers must be >= 0")
}

func TestOptionsValidateAcceptsZeroPlanMaxEntries(t *testing.T) {
	t.Parallel()

	// 0 disables the planBackfill entry cap (unbounded plan size). It must
	// build successfully rather than being rejected as a misconfiguration.
	opts := testOptions(t)
	opts.PlanMaxEntries = 0
	rt, err := Build(t.Context(), opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, rt.Close(context.Background()))
	})
}

func TestOptionsValidateRejectsInvalidPlanLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		edit func(*Options)
		want string
	}{
		{
			name: "negative DIDs",
			edit: func(opts *Options) { opts.PlanMaxDIDs = -1 },
			want: "PlanMaxDIDs must be >= 0",
		},
		{
			name: "negative collections",
			edit: func(opts *Options) { opts.PlanMaxCollections = -1 },
			want: "PlanMaxCollections must be >= 0",
		},
		{
			name: "negative entries",
			edit: func(opts *Options) { opts.PlanMaxEntries = -1 },
			want: "PlanMaxEntries must be >= 0",
		},
		{
			name: "zero threshold",
			edit: func(opts *Options) { opts.PlanWholeSegmentThreshold = 0 },
			want: "PlanWholeSegmentThreshold must be > 0 and <= 1",
		},
		{
			name: "high threshold",
			edit: func(opts *Options) { opts.PlanWholeSegmentThreshold = 1.1 },
			want: "PlanWholeSegmentThreshold must be > 0 and <= 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			opts := testOptions(t)
			tt.edit(&opts)
			_, err := Build(t.Context(), opts)
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestBuild_CreatesImportDir(t *testing.T) {
	t.Parallel()

	t.Run("default under data dir", func(t *testing.T) {
		t.Parallel()
		opts := testOptions(t)
		opts.TimestampImportToken = "secret"
		rt, err := Build(t.Context(), opts)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, rt.Close(context.Background())) })

		info, statErr := os.Stat(filepath.Join(opts.DataDir, "imports"))
		require.NoError(t, statErr)
		require.True(t, info.IsDir())
		require.NotNil(t, rt.importer, "import manager wired even (endpoints 401 without token)")
	})

	t.Run("explicit dir", func(t *testing.T) {
		t.Parallel()
		opts := testOptions(t)
		dir := filepath.Join(t.TempDir(), "staged")
		opts.TimestampImportDir = dir
		rt, err := Build(t.Context(), opts)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, rt.Close(context.Background())) })

		info, statErr := os.Stat(dir)
		require.NoError(t, statErr)
		require.True(t, info.IsDir())
	})
}

// stubbornRunner ignores context cancellation and blocks until its gate is
// closed, modelling an import goroutine wedged in an uninterruptible syscall.
type stubbornRunner struct{ gate chan struct{} }

func (r *stubbornRunner) RunImport(context.Context, orchestrator.ImportJob) (orchestrator.ImportResult, error) {
	<-r.gate
	return orchestrator.ImportResult{}, context.Canceled
}

// TestClose_FailedImportDrainLeavesStoreOpen pins the Close contract for an
// undrained import goroutine: pebble must NOT be closed underneath it (a
// checkpoint write after Close panics), Close must surface the drain error,
// and a repeated Close must re-wait and finish the teardown once the goroutine
// exits — not skip straight to closing the store.
func TestClose_FailedImportDrainLeavesStoreOpen(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)

	gate := make(chan struct{})
	importDir := filepath.Join(dataDir, "imports")
	require.NoError(t, os.MkdirAll(importDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(importDir, "a.csv"), []byte("uri,timestamp,scope,cid\n"), 0o644))
	mgr, err := importer.New(importer.Config{
		Store:      st,
		Runner:     &stubbornRunner{gate: gate},
		ImportDir:  importDir,
		ScratchDir: filepath.Join(dataDir, "scratch"),
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	runCtx, cancelImport := context.WithCancel(context.Background())
	rt := &Runtime{
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		metaStore:    st,
		importer:     mgr,
		importRunCtx: runCtx,
		cancelImport: cancelImport,
	}

	_, err = mgr.Submit(runCtx, "a.csv")
	require.NoError(t, err)

	// First Close: the runner ignores the cancel, so the bounded drain fails.
	closeCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = rt.Close(closeCtx)
	require.ErrorContains(t, err, "import drain")

	// The store must still be open: the wedged goroutine will write its final
	// job record through it when it eventually returns.
	require.NoError(t, st.Set([]byte("probe"), []byte{1}, store.SyncWrites),
		"metadata store must remain open under an undrained import goroutine")

	// The goroutine exits; a repeated Close re-waits, then closes the store.
	close(gate)
	require.NoError(t, rt.Close(context.Background()))
	require.Nil(t, rt.metaStore, "second Close must close the store once drained")
}

func testOptions(t *testing.T) Options {
	t.Helper()
	return Options{
		PublicAddr:                     "127.0.0.1:0",
		DebugAddr:                      "127.0.0.1:0",
		DataDir:                        t.TempDir(),
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
