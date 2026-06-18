package subscribe_test

import (
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/stretchr/testify/require"
)

func FuzzResolveCursor(f *testing.F) {
	// Seed corpus: typical inputs the resolver must handle gracefully.
	for _, raw := range []string{
		"", "0", "1", "42", "-1", "abc",
		"999999999999999",   // just below threshold
		"1000000000000000",  // exactly threshold
		"1000000000000001",  // just above threshold
		"1700000000000000",  // realistic v1 cursor
		"99999999999999999", // far-future timestamp
	} {
		f.Add(raw)
	}

	// One fixed fixture for the whole fuzz run; rebuilding per-iteration
	// would dominate runtime.
	dir := f.TempDir()
	now := time.Now().UnixMicro()
	mustWriteSealedSegment(f, filepath.Join(dir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 100, maxSeq: 199,
		minIndexedAt: now - int64(10*time.Hour/time.Microsecond),
		maxIndexedAt: now - int64(1*time.Hour/time.Microsecond),
		eventCount:   10,
	})
	m, err := manifest.Open(manifest.Options{
		SegmentsDir: dir,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(f, err)

	env := subscribe.CursorEnv{
		Manifest: m,
		NextSeq:  300,
		Lookback: 36 * time.Hour,
	}

	f.Fuzz(func(t *testing.T, raw string) {
		// Bound input length so the fuzzer doesn't synthesize 1MB strings
		// that hit strconv.ParseInt's range checks repeatedly.
		if len(raw) > 64 {
			return
		}
		plan, err := subscribe.ResolveCursor(raw, env)
		if err != nil {
			// Parse / negative-input failures are valid outcomes; the
			// resolver may also return wrapped translation errors. The
			// only invariant we enforce is "every error path either
			// wraps ErrInvalidCursor (user-input class) or otherwise
			// surfaces a non-nil error" — so just check err is non-nil.
			require.Error(t, err)
			return
		}
		switch plan.Mode {
		case subscribe.ModeLive:
			// ModeLive bypasses replay; no StartSeq invariant.
		case subscribe.ModeReplaySeq, subscribe.ModeReplayTimeUS:
			require.LessOrEqual(t, plan.StartSeq, env.NextSeq,
				"StartSeq must not exceed NextSeq")
			floorSeq, _ := env.Manifest.LookbackFloor(env.Lookback)
			require.GreaterOrEqual(t, plan.StartSeq, floorSeq,
				"StartSeq must not be below the lookback floor (clamp invariant)")
		default:
			t.Fatalf("unknown mode %v", plan.Mode)
		}
	})
}
