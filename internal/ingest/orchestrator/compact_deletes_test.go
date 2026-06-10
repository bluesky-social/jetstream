package orchestrator

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/stretchr/testify/require"
)

func TestRunDeleteCompactionCallsPassHook(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dataDir, "segments"), 0o755))
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	var got []CompactionPassResult
	o := &Orchestrator{cfg: Config{
		DataDir:            dataDir,
		Store:              st,
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
		CompactionInterval: time.Hour,
		OnCompactionPass: func(result CompactionPassResult) {
			got = append(got, result)
		},
	}}

	require.NoError(t, o.runDeleteCompaction(t.Context(), compactionSteady))
	require.Len(t, got, 1)
	require.Equal(t, uint64(0), got[0].Watermark)
	require.NoError(t, got[0].Err)
}
