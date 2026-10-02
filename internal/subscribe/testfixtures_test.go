package subscribe_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/catalog/local"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

type sealedFixture struct {
	minSeq, maxSeq                 uint64
	minWitnessedAt, maxWitnessedAt int64
	eventCount                     int
}

func mustWriteSealedSegment(tb testing.TB, path string, f sealedFixture) {
	tb.Helper()
	dir := filepath.Dir(path)
	require.NoError(tb, os.MkdirAll(dir, 0o755))
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 4096})
	require.NoError(tb, err)
	defer func() { _ = w.Close() }()

	for i := 0; i < f.eventCount; i++ {
		seq := f.minSeq + uint64(i)*((f.maxSeq-f.minSeq+1)/uint64(f.eventCount))
		if i == f.eventCount-1 {
			seq = f.maxSeq
		}
		ts := f.minWitnessedAt + int64(i)*((f.maxWitnessedAt-f.minWitnessedAt+1)/int64(f.eventCount))
		if i == f.eventCount-1 {
			ts = f.maxWitnessedAt
		}
		_, err := w.Append(segment.Event{
			Seq: seq, WitnessedAt: ts, Kind: segment.KindCreate,
			DID: "did:plc:fixture", Collection: "app.bsky.feed.post",
			Rkey: "abc", Rev: "rev", Payload: []byte{0xa0},
		})
		require.NoError(tb, err)
	}
	_, err = w.Seal()
	require.NoError(tb, err)
}

func mustOpenManifest(tb testing.TB, dir string) *manifest.Manifest {
	tb.Helper()
	m, err := manifest.Open(manifest.Options{
		SegmentsDir: dir,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(tb, err)
	return m
}

// mustCatalog returns a local catalog over segDir holding what is on disk,
// with w, when non-nil, attached as the main namespace's writer. A writer
// opened without Config.Catalog does not publish its seals to it.
func mustCatalog(tb testing.TB, segDir string, w *ingest.Writer) *local.Catalog {
	tb.Helper()
	c, err := local.New(local.Config{Dirs: map[catalog.Namespace]string{catalog.Main: segDir}})
	require.NoError(tb, err)
	require.NoError(tb, c.Refresh(context.Background()))
	if w != nil {
		c.AttachActive(catalog.Main, w)
	}
	return c
}
