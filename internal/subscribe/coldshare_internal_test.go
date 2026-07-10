package subscribe

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/stretchr/testify/require"
)

// newColdShareFixture builds a sealed 10-event segment plus a ColdReader
// over it, with the writer's readable-log floor above the segment so every
// read is served cold.
func newColdShareFixture(t *testing.T) *ColdReader {
	t.Helper()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	mustWriteColdReaderSealedSegment(t, filepath.Join(segDir, "seg_0000000000.jss"), coldReaderSealedFixture{
		minSeq: 0, maxSeq: 9, minWitnessedAt: 1_000, maxWitnessedAt: 9_999, eventCount: 10,
	})
	m := mustOpenColdReaderManifest(t, segDir)
	st, w := openColdReaderWriterAtTip(t, dir, 10)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })

	var writerPtr atomic.Pointer[ingest.Writer]
	writerPtr.Store(w)
	return NewColdReader(ColdReaderConfig{
		Manifest: m, WriterRef: &writerPtr, BlockCacheBytes: 1 << 20,
	})
}

// TestColdReader_EntriesSharedAcrossReads is the heart of #295 part 2: two
// cold reads over the same cached block (two concurrent subscribers
// replaying the same history) must return the SAME *Entry values, so the
// compress-once memoization engages on the cold path exactly like the hot
// path. Per-subscriber entries mean every zstd subscriber re-compresses
// every archived event.
func TestColdReader_EntriesSharedAcrossReads(t *testing.T) {
	t.Parallel()
	rd := newColdShareFixture(t)

	b1, _, err := rd.Read(context.Background(), 0, 10)
	require.NoError(t, err)
	require.Len(t, b1, 10)
	b2, _, err := rd.Read(context.Background(), 0, 10)
	require.NoError(t, err)
	require.Len(t, b2, 10)

	for i := range b1 {
		require.Same(t, b1[i], b2[i],
			"cold entries must be shared across subscribers while the block is cache-resident")
	}

	// The shared entry memoizes: both subscribers get the identical
	// compressed frame allocation, proving compression ran once.
	c1, err := b1[3].Compressed()
	require.NoError(t, err)
	c2, err := b2[3].Compressed()
	require.NoError(t, err)
	require.Equal(t, &c1[0], &c2[0], "memoized compressed frames must share one allocation")
}

// TestColdReader_MemoGrowthChargedToCache pins the byte-accounting contract:
// memoized JSON and compressed bodies materialize AFTER the block is
// inserted, so the entry must charge that growth back to the cache budget.
// Without it a cold zstd replay storm silently inflates the cache several
// times past its configured bound.
func TestColdReader_MemoGrowthChargedToCache(t *testing.T) {
	t.Parallel()
	rd := newColdShareFixture(t)

	batch, _, err := rd.Read(context.Background(), 0, 10)
	require.NoError(t, err)
	require.Len(t, batch, 10)

	before := rd.cache.bytes()
	require.Greater(t, before, 0)

	body, err := batch[0].Encoded()
	require.NoError(t, err)
	afterEncode := rd.cache.bytes()
	require.GreaterOrEqual(t, afterEncode, before+len(body),
		"memoized JSON bytes must be charged to the cache budget")

	frame, err := batch[0].Compressed()
	require.NoError(t, err)
	afterCompress := rd.cache.bytes()
	require.GreaterOrEqual(t, afterCompress, afterEncode+len(frame),
		"memoized compressed bytes must be charged to the cache budget")

	// Growth after eviction is a no-op: the entries are unreachable from
	// the cache once invalidated, so there is nothing to account.
	rd.InvalidateSegment(0)
	require.Zero(t, rd.cache.bytes())
	_, err = batch[1].Encoded()
	require.NoError(t, err)
	require.Zero(t, rd.cache.bytes(), "memo growth on an evicted block must not resurrect accounting")
}

// TestColdReader_MemoGrowthTriggersEviction proves the charged growth is
// live: when memo growth pushes the cache past its budget, eviction runs
// (the cache does not just track a number, it enforces the bound). The
// budget is sized to admit the raw decoded block but not the block plus
// its memoized bodies.
func TestColdReader_MemoGrowthTriggersEviction(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	mustWriteColdReaderSealedSegment(t, filepath.Join(segDir, "seg_0000000000.jss"), coldReaderSealedFixture{
		minSeq: 0, maxSeq: 9, minWitnessedAt: 1_000, maxWitnessedAt: 9_999, eventCount: 10,
	})
	m := mustOpenColdReaderManifest(t, segDir)
	st, w := openColdReaderWriterAtTip(t, dir, 10)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })

	var writerPtr atomic.Pointer[ingest.Writer]
	writerPtr.Store(w)

	// Learn the raw block's accounted size with an unconstrained cache.
	probe := NewColdReader(ColdReaderConfig{
		Manifest: m, WriterRef: &writerPtr, BlockCacheBytes: 1 << 20,
	})
	_, _, err := probe.Read(context.Background(), 0, 10)
	require.NoError(t, err)
	rawBytes := probe.cache.bytes()
	require.Greater(t, rawBytes, 0)

	// Budget = raw block + 1: insert fits, any memo growth overflows.
	rd := NewColdReader(ColdReaderConfig{
		Manifest: m, WriterRef: &writerPtr, BlockCacheBytes: rawBytes + 1,
	})
	batch, _, err := rd.Read(context.Background(), 0, 10)
	require.NoError(t, err)
	require.Len(t, batch, 10)
	require.Equal(t, rawBytes, rd.cache.bytes(), "raw block must be admitted under the budget")

	_, err = batch[0].Encoded()
	require.NoError(t, err)
	require.Zero(t, rd.cache.bytes(), "over-budget memo growth must evict, not accumulate")
}
