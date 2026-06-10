package jetstreamd

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/crashpoint"
	"github.com/jcalabro/atmos"
	"github.com/stretchr/testify/require"
)

func TestOptionsValidateRejectsNegativeSegmentCache(t *testing.T) {
	t.Parallel()

	opts := testOptions(t)
	opts.SegmentCacheMaxAge = -time.Second
	_, err := Build(t.Context(), opts)
	require.ErrorContains(t, err, "SegmentCacheMaxAge must be >= 0")
}

func TestOptionsValidateRejectsNegativeCompactionRewriteWorkers(t *testing.T) {
	t.Parallel()

	opts := testOptions(t)
	opts.CompactionRewriteWorkers = -1
	_, err := Build(t.Context(), opts)
	require.ErrorContains(t, err, "CompactionRewriteWorkers must be >= 0")
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
		SubscribeHotTailBytes:     1 << 20,
		SubscribeBlockCacheBytes:  1 << 20,
		SubscribeReadBatch:        128,
		SubscribeSlowWindow:       time.Second,
		SubscribeSlowMinRate:      5,
		CursorBlockIndexCacheSize: 32,
	}
}
