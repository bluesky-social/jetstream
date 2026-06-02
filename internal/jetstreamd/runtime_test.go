package jetstreamd

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOptionsValidateRejectsNegativeSegmentCache(t *testing.T) {
	t.Parallel()

	opts := testOptions(t)
	opts.SegmentCacheMaxAge = -time.Second
	_, err := Build(t.Context(), opts)
	require.ErrorContains(t, err, "SegmentCacheMaxAge must be >= 0")
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
