package orchestrator

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/ingest/live"
	"github.com/stretchr/testify/require"
)

func TestMergeRunner_EmptySourceDir(t *testing.T) {
	t.Parallel()
	st := newOrchestratorTestStore(t)

	dataDir := t.TempDir()
	dstDir := filepath.Join(dataDir, "segments")
	srcDir := filepath.Join(dataDir, "backfill", "live_segments")
	require.NoError(t, os.MkdirAll(srcDir, 0o755))

	dst, err := ingest.Open(ingest.Config{
		SegmentsDir: dstDir,
		Store:       st,
		SeqKey:      live.SteadySeqKey,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dst.Close() })

	r := newMergeRunner(dst, st, srcDir, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)

	require.NoError(t, r.run(t.Context()))

	// Cursor never advanced (no sources).
	got, err := loadMergeCursor(st)
	require.NoError(t, err)
	require.Equal(t, uint64(0), got)
}

// TestMergeRunner_SourceIndexGap pins the contiguity check in run(): a
// hole in the seg_*.jss numbering at or above the persisted cursor is
// treated as corruption and surfaced as an error rather than silently
// skipped. The bootstrap-live consumer rotates contiguously, so a gap
// can only show up via filesystem corruption or operator interference.
func TestMergeRunner_SourceIndexGap(t *testing.T) {
	t.Parallel()
	st := newOrchestratorTestStore(t)

	dataDir := t.TempDir()
	dstDir := filepath.Join(dataDir, "segments")
	srcDir := filepath.Join(dataDir, "backfill", "live_segments")
	require.NoError(t, os.MkdirAll(srcDir, 0o755))

	// Synthesize a non-contiguous source set: index 0 and 2 present, 1 missing.
	for _, idx := range []uint64{0, 2} {
		w, err := ingest.Open(ingest.Config{
			SegmentsDir: srcDir,
			Store:       st,
			SeqKey:      live.BootstrapSeqKey,
			Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		})
		require.NoError(t, err)
		require.NoError(t, w.SealActiveAndClose())
		// w wrote seg_<current active idx>.jss; force the gap by renaming
		// the second file to seg_0000000002.jss after the first run sealed.
		if idx == 2 {
			cur := filepath.Join(srcDir, ingest.SegmentFilename(1))
			require.NoError(t, os.Rename(cur, filepath.Join(srcDir, ingest.SegmentFilename(2))))
		}
	}

	dst, err := ingest.Open(ingest.Config{
		SegmentsDir: dstDir,
		Store:       st,
		SeqKey:      live.SteadySeqKey,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dst.Close() })

	r := newMergeRunner(dst, st, srcDir, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	err = r.run(t.Context())
	require.ErrorContains(t, err, "source index gap")
}

// TestMergeRunner_DiscoverySkipsWhenNoBootstrapCursor verifies the
// no-op short-circuit in runDiscovery: when bootstrap_last_listrepos_cursor
// is absent, runDiscovery returns nil without consulting the relay.
//
// This is the path a debug short-circuit run hits (MaxBackfillRepos
// trips before listRepos pages past page 1, so MaybeSave never fires).
// It must be a clean no-op or merge cannot complete on debug runs.
func TestMergeRunner_DiscoverySkipsWhenNoBootstrapCursor(t *testing.T) {
	t.Parallel()
	st := newOrchestratorTestStore(t)

	dataDir := t.TempDir()
	dstDir := filepath.Join(dataDir, "segments")
	srcDir := filepath.Join(dataDir, "backfill", "live_segments")
	require.NoError(t, os.MkdirAll(srcDir, 0o755))

	dst, err := ingest.Open(ingest.Config{
		SegmentsDir: dstDir,
		Store:       st,
		SeqKey:      live.SteadySeqKey,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dst.Close() })

	r := newMergeRunner(dst, st, srcDir, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)

	// If runDiscovery were to actually call out (via the xrpc client it
	// would build internally), it would dereference the nil HTTPClient
	// or fail to dial 127.0.0.1:1 immediately. The assertion that it
	// returns nil proves the cursor-absent short-circuit fired before
	// any network construction.
	require.NoError(t, r.runDiscovery(t.Context(), "http://127.0.0.1:1", nil))
}
