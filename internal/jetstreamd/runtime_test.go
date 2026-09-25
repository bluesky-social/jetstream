package jetstreamd

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/internal/xrpcapi"
	"github.com/jcalabro/atmos/streaming"
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

func TestGoroutineRoot_LogsPanicThenRethrows(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	rt := &Runtime{
		logger: slog.New(slog.NewJSONHandler(&buf, nil)),
	}

	var rec any
	func() {
		defer func() { rec = recover() }()
		_ = rt.goroutineRoot("test_root", func() error {
			panic("boom")
		})()
	}()

	require.Equal(t, "boom", rec)
	body := buf.String()
	require.Contains(t, body, `"msg":"panic in runtime goroutine"`)
	require.Contains(t, body, `"goroutine":"test_root"`)
	require.Contains(t, body, `"panic":"boom"`)
	require.Contains(t, body, `"panic_type":"string"`)
	require.Contains(t, body, `"stack":`)
	require.Contains(t, body, `"go_version":`)
	require.Contains(t, body, `"version":`)
	require.Contains(t, body, `"commit":`)
}

func TestOptionsValidateRejectsNegativeCompactionCacheGrace(t *testing.T) {
	t.Parallel()

	opts := testOptions(t)
	opts.CompactionCacheGrace = -time.Second
	_, err := Build(t.Context(), opts)
	require.ErrorContains(t, err, "CompactionCacheGrace must be >= 0")
}

func TestOptionsValidateRejectsNegativeBackfillFleetLimits(t *testing.T) {
	t.Parallel()

	for name, mutate := range map[string]func(*Options){
		"global downloads": func(o *Options) { o.BackfillGlobalDownloads = -1 },
		"host workers":     func(o *Options) { o.BackfillHostWorkers = -1 },
		"active hosts":     func(o *Options) { o.BackfillMaxActiveHosts = -1 },
		"roster cap":       func(o *Options) { o.BackfillMaxHosts = -1 },
	} {
		t.Run(name, func(t *testing.T) {
			opts := testOptions(t)
			mutate(&opts)
			_, err := Build(t.Context(), opts)
			require.ErrorContains(t, err, "must be >= 0")
		})
	}
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

	// 0 disables the planSnapshot entry cap (unbounded plan size). It must
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
			name: "DIDs above wire limit",
			edit: func(opts *Options) { opts.PlanMaxDIDs = xrpcapi.DefaultPlanMaxDIDs + 1 },
			want: "PlanMaxDIDs must be <= 10000",
		},
		{
			name: "collections above wire limit",
			edit: func(opts *Options) { opts.PlanMaxCollections = xrpcapi.DefaultPlanMaxCollections + 1 },
			want: "PlanMaxCollections must be <= 100",
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

func TestClose_CancelsAndDrainsRunBeforeClosingStores(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	require.NoError(t, lifecycle.WritePhase(st, lifecycle.PhaseSteadyState, time.Now().UTC()))
	require.NoError(t, st.Close())

	dialEntered := make(chan struct{})
	var dialOnce sync.Once
	opts := testOptions(t)
	opts.DataDir = dataDir
	opts.Headless = true
	opts.LiveDial = streaming.DialFunc(func(ctx context.Context, _ string, _ streaming.DialConfig) (streaming.Conn, *http.Response, error) {
		dialOnce.Do(func() { close(dialEntered) })
		<-ctx.Done()
		return nil, nil, ctx.Err()
	})

	rt, err := Build(t.Context(), opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = rt.Close(closeCtx)
	})

	done := make(chan error, 1)
	go func() { done <- rt.Run(t.Context()) }()

	select {
	case <-dialEntered:
	case err := <-done:
		t.Fatalf("runtime exited before live dial blocked: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("runtime never reached live dial")
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, rt.Close(closeCtx))

	select {
	case err := <-done:
		require.NoError(t, err, "Close should drive Run through a graceful shutdown")
	case <-time.After(5 * time.Second):
		t.Fatal("Runtime.Close returned before Runtime.Run drained")
	}
	require.Nil(t, rt.metaStore, "Close must close the store only after Run drains")
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
