package subscribe

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/catalog/local"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/metastore/pebblestore"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

type coldReaderSealedFixture struct {
	minSeq, maxSeq                 uint64
	minWitnessedAt, maxWitnessedAt int64
	eventCount                     int
}

func TestColdReader_InvalidateSegmentPurgesDecodedBlocks(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	mustWriteColdReaderSealedSegment(t, filepath.Join(segDir, "seg_0000000000.jss"), coldReaderSealedFixture{
		minSeq: 0, maxSeq: 9, minWitnessedAt: 1_000, maxWitnessedAt: 9_999, eventCount: 10,
	})
	cat := mustColdReaderCatalog(t, segDir)
	st, w := openColdReaderWriterAtTip(t, dir, 10)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })

	var writerPtr atomic.Pointer[ingest.Writer]
	writerPtr.Store(w)
	rd := NewColdReader(ColdReaderConfig{
		Catalog: cat, Fetcher: cat.Fetcher(), WriterRef: &writerPtr, BlockCacheBytes: 1 << 20,
	})

	batch, _, err := rd.Read(context.Background(), 0, 10)
	require.NoError(t, err)
	require.Len(t, batch, 10)
	require.Greater(t, rd.cache.bytes(), 0)

	rd.InvalidateSegment(0)

	require.Zero(t, rd.cache.bytes())
}

func mustWriteColdReaderSealedSegment(tb testing.TB, path string, f coldReaderSealedFixture) {
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

func mustColdReaderCatalog(tb testing.TB, segDir string) *local.Catalog {
	tb.Helper()
	c, err := local.New(local.Config{Dirs: map[catalog.Namespace]string{catalog.Main: segDir}})
	require.NoError(tb, err)
	require.NoError(tb, c.Refresh(context.Background()))
	return c
}

func openColdReaderWriterAtTip(t testing.TB, dir string, nextSeq uint64) (*pebblestore.Store, *ingest.Writer) {
	t.Helper()
	st, err := pebblestore.Open(dir, pebblestore.NewMetrics(prometheus.NewRegistry()))
	require.NoError(t, err)
	require.NoError(t, st.Set(context.Background(), []byte("seq/next"), coldReaderEncodeUint64LE(nextSeq)))

	w, err := ingest.Open(ingest.Config{
		SegmentsDir: filepath.Join(dir, "segments"),
		Store:       st,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:     ingest.NewMetrics(prometheus.NewRegistry()),
	})
	require.NoError(t, err)
	return st, w
}

func coldReaderEncodeUint64LE(v uint64) []byte {
	b := make([]byte, 8)
	for i := range b {
		b[i] = byte(v >> (8 * i))
	}
	return b
}
