package subscribe_test

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"

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
