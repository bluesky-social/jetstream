package subscribe_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/stretchr/testify/require"
)

func TestColdReadBatch_BoundedAndResumes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	mustWriteSealedSegment(t, filepath.Join(segDir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 0, maxSeq: 99, minWitnessedAt: 1_000, maxWitnessedAt: 100_000, eventCount: 100,
	})
	m := mustOpenManifest(t, segDir)
	st, w := openWriterAtTip(t, dir, 100)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })

	var writerPtr atomic.Pointer[ingest.Writer]
	writerPtr.Store(w)
	rd := subscribe.NewColdReader(subscribe.ColdReaderConfig{
		Manifest: m, WriterRef: &writerPtr, BlockCacheBytes: 1 << 20,
	})

	// First batch of 10 starting at seq 5.
	batch, next, err := rd.Read(context.Background(), 5, 10)
	require.NoError(t, err)
	require.Len(t, batch, 10)
	require.Equal(t, uint64(5), batch[0].Event.Seq)
	require.Equal(t, uint64(14), batch[9].Event.Seq)
	require.Equal(t, uint64(15), next)

	// Resume from next; verify contiguity.
	batch2, _, err := rd.Read(context.Background(), next, 10)
	require.NoError(t, err)
	require.Equal(t, uint64(15), batch2[0].Event.Seq)
}

func TestColdReadActiveMultiBatchAdvancesRangeWithoutPrefixRescan(t *testing.T) {
	t.Parallel()
	const blocks, perBlock = 12, 4
	_, w, rd := openActiveColdReader(t, blocks, perBlock)

	cursor := uint64(1)
	var priorStart uint64
	var got []uint64
	for range blocks {
		rng, ok := w.ActiveFlushedRange(cursor)
		require.True(t, ok)
		if priorStart != 0 {
			require.Greater(t, rng.StartOffset, priorStart,
				"each resumed cold batch must seek to a later active block")
		}
		priorStart = rng.StartOffset

		batch, next, err := rd.Read(context.Background(), cursor, perBlock)
		require.NoError(t, err)
		require.Len(t, batch, perBlock)
		for _, e := range batch {
			got = append(got, e.Event.Seq)
		}
		require.Greater(t, next, cursor)
		cursor = next
	}
	want := make([]uint64, blocks*perBlock)
	for i := range want {
		want[i] = uint64(i + 1)
	}
	require.Equal(t, want, got)
}

func BenchmarkColdReadActiveRange(b *testing.B) {
	for _, blocks := range []int{16, 64, 256} {
		b.Run(fmt.Sprintf("blocks=%d", blocks), func(b *testing.B) {
			_, _, rd := openActiveColdReader(b, blocks, 16)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				cursor := uint64(1)
				for cursor <= uint64(blocks*16) {
					batch, next, err := rd.Read(context.Background(), cursor, 64)
					if err != nil {
						b.Fatal(err)
					}
					if len(batch) == 0 || next <= cursor {
						b.Fatalf("non-advancing active replay at %d", cursor)
					}
					cursor = next
				}
			}
			b.ReportMetric(float64(blocks*16), "events/op")
		})
	}
}

func openActiveColdReader(tb testing.TB, blocks, perBlock int) (*store.Store, *ingest.Writer, *subscribe.ColdReader) {
	tb.Helper()
	dir := tb.TempDir()
	st, err := store.Open(dir, store.NewMetrics(prometheus.NewRegistry()))
	require.NoError(tb, err)
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:           filepath.Join(dir, "segments"),
		Store:                 st,
		Logger:                slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:               ingest.NewMetrics(prometheus.NewRegistry()),
		MaxEventsPerBlock:     perBlock,
		MaxSegmentBytes:       1 << 30,
		ReadLogRetentionBytes: 0,
	})
	require.NoError(tb, err)
	for range blocks {
		events := make([]segment.Event, perBlock)
		for i := range events {
			events[i] = segment.Event{Kind: segment.KindCreate, DID: "did:plc:active", Collection: "app.bsky.feed.post", Payload: []byte{0xa0}}
		}
		require.NoError(tb, w.AppendBatch(context.Background(), events))
	}
	require.NoError(tb, w.Flush(context.Background()))

	var writerPtr atomic.Pointer[ingest.Writer]
	writerPtr.Store(w)
	rd := subscribe.NewColdReader(subscribe.ColdReaderConfig{WriterRef: &writerPtr, BlockCacheBytes: 1 << 20})
	tb.Cleanup(func() { _ = w.Close(); _ = st.Close() })
	return st, w, rd
}

func TestColdReadBatch_ExhaustsBeforeMax(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	mustWriteSealedSegment(t, filepath.Join(segDir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 0, maxSeq: 9, minWitnessedAt: 1_000, maxWitnessedAt: 9_999, eventCount: 10,
	})
	m := mustOpenManifest(t, segDir)
	st, w := openWriterAtTip(t, dir, 10)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })

	var writerPtr atomic.Pointer[ingest.Writer]
	writerPtr.Store(w)
	rd := subscribe.NewColdReader(subscribe.ColdReaderConfig{
		Manifest: m, WriterRef: &writerPtr, BlockCacheBytes: 1 << 20,
	})
	batch, next, err := rd.Read(context.Background(), 8, 100) // only 8,9 remain
	require.NoError(t, err)
	require.Len(t, batch, 2)
	require.Equal(t, uint64(10), next, "next is one past the last available seq")
}
