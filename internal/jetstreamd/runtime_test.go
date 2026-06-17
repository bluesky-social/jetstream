package jetstreamd

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/crashpoint"
	"github.com/bluesky-social/jetstream-v2/internal/xrpcapi"
	"github.com/jcalabro/atmos"
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
			name: "zero entries",
			edit: func(opts *Options) { opts.PlanMaxEntries = 0 },
			want: "PlanMaxEntries must be positive",
		},
		{
			name: "negative entries",
			edit: func(opts *Options) { opts.PlanMaxEntries = -1 },
			want: "PlanMaxEntries must be positive",
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

func TestOptionsExposeAfterRepoCompleteHook(t *testing.T) {
	t.Parallel()

	var called atomic.Bool
	opts := testOptions(t)
	opts.AfterRepoComplete = func(context.Context, atmos.DID) error {
		called.Store(true)
		return nil
	}

	require.NotNil(t, opts.AfterRepoComplete)
	require.False(t, called.Load())
}

func TestOptionsExposeCrashInjector(t *testing.T) {
	t.Parallel()

	opts := testOptions(t)
	opts.CrashInjector = stubCrashInjector{}

	require.NotNil(t, opts.CrashInjector)
}

type stubCrashInjector struct{}

func (stubCrashInjector) SimulateCrash(context.Context, crashpoint.Point) error {
	return nil
}

func testOptions(t *testing.T) Options {
	t.Helper()
	return Options{
		PublicAddr:                "127.0.0.1:0",
		DebugAddr:                 "127.0.0.1:0",
		DataDir:                   t.TempDir(),
		RelayURL:                  "http://127.0.0.1:1",
		OTelServiceName:           "jetstream-test",
		LogLevel:                  "warn",
		LogFormat:                 "text",
		LogOutput:                 &bytes.Buffer{},
		ShutdownTimeout:           5 * time.Second,
		ClientDrainTimeout:        time.Second,
		CursorLookback:            36 * time.Hour,
		PlanMaxDIDs:               xrpcapi.DefaultPlanMaxDIDs,
		PlanMaxCollections:        xrpcapi.DefaultPlanMaxCollections,
		PlanMaxEntries:            xrpcapi.DefaultPlanMaxEntries,
		PlanWholeSegmentThreshold: xrpcapi.DefaultPlanWholeSegmentThreshold,
		SubscribeHotTailBytes:     1 << 20,
		SubscribeBlockCacheBytes:  1 << 20,
		SubscribeReadBatch:        128,
		SubscribeSlowWindow:       time.Second,
		SubscribeSlowMinRate:      5,
		CursorBlockIndexCacheSize: 32,
	}
}
