package oracle

import (
	"io"
	"log/slog"
	"testing"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestOracle_PowerLossStrictMemDropsUnsyncedState(t *testing.T) {
	t.Parallel()

	fs := vfs.NewStrictMem()
	dataDir := "/data"
	segmentsDir := fs.PathJoin(dataDir, "segments")
	require.NoError(t, fs.MkdirAll(dataDir, 0o755))
	syncStrictOracleDir(t, fs, "/")

	st, err := store.Open(dataDir, nil, store.WithFS(fs))
	require.NoError(t, err)
	w, err := ingest.Open(ingest.Config{
		DataDir:           dataDir,
		SegmentsDir:       segmentsDir,
		FS:                fs,
		Store:             st,
		MaxEventsPerBlock: 1,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	first := segment.Event{
		Kind:       segment.KindCreate,
		DID:        "did:plc:first",
		Collection: "app.bsky.feed.post",
		Rkey:       "first",
		Rev:        "0001",
	}
	require.NoError(t, w.Append(t.Context(), &first))
	require.Equal(t, uint64(1), first.Seq)

	fs.SetIgnoreSyncs(true)
	second := segment.Event{
		Kind:       segment.KindCreate,
		DID:        "did:plc:second",
		Collection: "app.bsky.feed.post",
		Rkey:       "second",
		Rev:        "0002",
	}
	require.NoError(t, w.Append(t.Context(), &second))
	require.Equal(t, uint64(2), second.Seq)
	require.NoError(t, w.Close())
	require.NoError(t, st.Close())

	fs.ResetToSyncedState()
	fs.SetIgnoreSyncs(false)

	st, err = store.Open(dataDir, nil, store.WithFS(fs))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	w, err = ingest.Open(ingest.Config{
		DataDir:           dataDir,
		SegmentsDir:       segmentsDir,
		FS:                fs,
		Store:             st,
		MaxEventsPerBlock: 1,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w.Close()) })
	require.Equal(t, uint64(2), w.NextSeq())

	observed, err := ObserveSegmentsFS(fs, dataDir)
	require.NoError(t, err)
	require.NoError(t, CheckInvariants(observed))
	require.Len(t, observed, 1)
	require.Equal(t, uint64(1), observed[0].Seq)
	require.Equal(t, "did:plc:first", observed[0].DID)
	require.Equal(t, "first", observed[0].Rkey)
}

func syncStrictOracleDir(t *testing.T, fs *vfs.MemFS, dir string) {
	t.Helper()
	f, err := fs.OpenDir(dir)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())
}
