package subscribe

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/manifest"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

type coldReaderSealedFixture struct {
	minSeq, maxSeq             uint64
	minIndexedAt, maxIndexedAt int64
	eventCount                 int
}

func TestColdReader_InvalidateSegmentPurgesDecodedBlocks(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	mustWriteColdReaderSealedSegment(t, filepath.Join(segDir, "seg_0000000000.jss"), coldReaderSealedFixture{
		minSeq: 0, maxSeq: 9, minIndexedAt: 1_000, maxIndexedAt: 9_999, eventCount: 10,
	})
	m := mustOpenColdReaderManifest(t, segDir)
	st, w := openColdReaderWriterAtTip(t, dir, 10)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })

	var writerPtr atomic.Pointer[ingest.Writer]
	writerPtr.Store(w)
	rd := NewColdReader(ColdReaderConfig{
		Manifest: m, WriterRef: &writerPtr, BlockCacheBytes: 1 << 20,
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
		ts := f.minIndexedAt + int64(i)*((f.maxIndexedAt-f.minIndexedAt+1)/int64(f.eventCount))
		if i == f.eventCount-1 {
			ts = f.maxIndexedAt
		}
		_, err := w.Append(segment.Event{
			Seq: seq, IndexedAt: ts, Kind: segment.KindCreate,
			DID: "did:plc:fixture", Collection: "app.bsky.feed.post",
			Rkey: "abc", Rev: "rev", Payload: []byte{0xa0},
		})
		require.NoError(tb, err)
	}
	_, err = w.Seal()
	require.NoError(tb, err)
}

func mustOpenColdReaderManifest(tb testing.TB, dir string) *manifest.Manifest {
	tb.Helper()
	m, err := manifest.Open(manifest.Options{
		SegmentsDir: dir,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(tb, err)
	return m
}

func openColdReaderWriterAtTip(t *testing.T, dir string, nextSeq uint64) (*store.Store, *ingest.Writer) {
	t.Helper()
	st, err := store.Open(dir, store.NewMetrics(prometheus.NewRegistry()))
	require.NoError(t, err)
	require.NoError(t, st.Set([]byte("seq/next"), coldReaderEncodeUint64LE(nextSeq), store.SyncWrites))

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
