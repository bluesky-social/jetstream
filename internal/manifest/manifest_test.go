package manifest_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestOpen_EmptyDir(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m, err := manifest.Open(manifest.Options{
		SegmentsDir: dir,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	require.NotNil(t, m)
	require.Equal(t, 0, m.SegmentCount())
}

func TestOpen_SkipsActiveSegments(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mustWriteEmptyActiveSegment(t, filepath.Join(dir, "seg_0000000000.jss"))

	m, err := manifest.Open(manifest.Options{
		SegmentsDir: dir,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	require.Equal(t, 0, m.SegmentCount(), "active segments must be skipped at open time")
}

func TestOpen_LoadsSealedSegmentBounds(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 0, maxSeq: 99,
		minWitnessedAt: 1_700_000_000_000_000, maxWitnessedAt: 1_700_000_010_000_000,
		eventCount: 100,
	})
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000001.jss"), sealedFixture{
		minSeq: 100, maxSeq: 199,
		minWitnessedAt: 1_700_000_010_000_001, maxWitnessedAt: 1_700_000_020_000_000,
		eventCount: 100,
	})

	m, err := manifest.Open(manifest.Options{
		SegmentsDir: dir,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	require.Equal(t, 2, m.SegmentCount())

	bounds := m.AllBounds()
	require.Equal(t, uint64(0), bounds[0].Idx)
	require.Equal(t, uint64(0), bounds[0].MinSeq)
	require.Equal(t, uint64(99), bounds[0].MaxSeq)
	require.Equal(t, int64(1_700_000_000_000_000), bounds[0].MinWitnessedAt)
	require.Equal(t, int64(1_700_000_010_000_000), bounds[0].MaxWitnessedAt)

	require.Equal(t, uint64(1), bounds[1].Idx)
	require.Equal(t, uint64(100), bounds[1].MinSeq)
	require.Equal(t, uint64(199), bounds[1].MaxSeq)
}

func TestOpen_NonExistentDir(t *testing.T) {
	t.Parallel()
	_, err := manifest.Open(manifest.Options{
		SegmentsDir: filepath.Join(t.TempDir(), "does-not-exist"),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "manifest: list segments",
		"error must be wrapped with the manifest layer's prefix")
}

func TestOpenBackground_WaitReturnsLoadError(t *testing.T) {
	t.Parallel()
	m, err := manifest.OpenBackground(context.Background(), manifest.Options{
		SegmentsDir: filepath.Join(t.TempDir(), "does-not-exist"),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	err = m.Wait(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "manifest: list segments")
}

func TestOpen_CorruptSegmentAborts(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// Write a file that has the segment magic but a non-zero header
	// checksum that won't validate against any subsequent decode.
	// Easiest: write a sealed-looking file with nonsense bytes.
	path := filepath.Join(dir, "seg_0000000000.jss")
	buf := make([]byte, 256)
	copy(buf[0:4], []byte("jss0"))
	// Set checksum to a non-zero value so it's not classified as active.
	buf[4] = 0xFF
	require.NoError(t, os.WriteFile(path, buf, 0o644))

	_, err := manifest.Open(manifest.Options{
		SegmentsDir: dir,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.Error(t, err, "corrupt segment must abort startup")
	require.Contains(t, err.Error(), "manifest: read segment")
}

func TestSegmentForSeq(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 0, maxSeq: 99, minWitnessedAt: 1_700_000_000_000_000, maxWitnessedAt: 1_700_000_010_000_000, eventCount: 10,
	})
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000001.jss"), sealedFixture{
		minSeq: 100, maxSeq: 199, minWitnessedAt: 1_700_000_010_000_001, maxWitnessedAt: 1_700_000_020_000_000, eventCount: 10,
	})
	m := mustOpenManifest(t, dir)

	b, ok := m.SegmentForSeq(50)
	require.True(t, ok)
	require.Equal(t, uint64(0), b.Idx)

	b, ok = m.SegmentForSeq(100)
	require.True(t, ok)
	require.Equal(t, uint64(1), b.Idx)

	b, ok = m.SegmentForSeq(99)
	require.True(t, ok)
	require.Equal(t, uint64(0), b.Idx)

	_, ok = m.SegmentForSeq(1000)
	require.False(t, ok)

	empty := mustOpenManifest(t, t.TempDir())
	_, ok = empty.SegmentForSeq(0)
	require.False(t, ok)
}

func TestSegmentForTimeUS(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 0, maxSeq: 99, minWitnessedAt: 1_700_000_000_000_000, maxWitnessedAt: 1_700_000_010_000_000, eventCount: 10,
	})
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000001.jss"), sealedFixture{
		minSeq: 100, maxSeq: 199, minWitnessedAt: 1_700_000_010_000_001, maxWitnessedAt: 1_700_000_020_000_000, eventCount: 10,
	})
	m := mustOpenManifest(t, dir)

	b, ok := m.SegmentForTimeUS(1_600_000_000_000_000)
	require.True(t, ok)
	require.Equal(t, uint64(0), b.Idx)

	b, ok = m.SegmentForTimeUS(1_700_000_005_000_000)
	require.True(t, ok)
	require.Equal(t, uint64(0), b.Idx)

	b, ok = m.SegmentForTimeUS(1_700_000_010_000_001)
	require.True(t, ok)
	require.Equal(t, uint64(1), b.Idx)

	_, ok = m.SegmentForTimeUS(1_800_000_000_000_000)
	require.False(t, ok)
}

func TestLookbackFloor(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	now := time.Now().UnixMicro()
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 0, maxSeq: 99,
		minWitnessedAt: now - int64(48*time.Hour/time.Microsecond),
		maxWitnessedAt: now - int64(40*time.Hour/time.Microsecond),
		eventCount:     10,
	})
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000001.jss"), sealedFixture{
		minSeq: 100, maxSeq: 199,
		minWitnessedAt: now - int64(12*time.Hour/time.Microsecond),
		maxWitnessedAt: now - int64(1*time.Hour/time.Microsecond),
		eventCount:     10,
	})
	m := mustOpenManifest(t, dir)

	floorSeq, _ := m.LookbackFloor(36 * time.Hour)
	require.Equal(t, uint64(100), floorSeq, "36h floor lands at seg 1's MinSeq")

	floorSeq, _ = m.LookbackFloor(1 * time.Minute)
	require.Equal(t, uint64(100), floorSeq)

	floorSeq, _ = m.LookbackFloor(100 * time.Hour)
	require.Equal(t, uint64(0), floorSeq)

	emptySeq, emptyTime := mustOpenManifest(t, t.TempDir()).LookbackFloor(36 * time.Hour)
	require.Equal(t, uint64(0), emptySeq)
	require.Equal(t, int64(0), emptyTime)
}

func TestOnSegmentSealed(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m := mustOpenManifest(t, dir)
	require.Equal(t, 0, m.SegmentCount())

	path := filepath.Join(dir, "seg_0000000000.jss")
	mustWriteSealedSegment(t, path, sealedFixture{
		minSeq: 0, maxSeq: 99, minWitnessedAt: 1_700_000_000_000_000, maxWitnessedAt: 1_700_000_010_000_000, eventCount: 10,
	})

	require.NoError(t, m.OnSegmentSealed(0, path))
	require.Equal(t, 1, m.SegmentCount())

	b, ok := m.SegmentForSeq(50)
	require.True(t, ok)
	require.Equal(t, uint64(0), b.Idx)
}

func TestOnSegmentSealed_ReplacesExistingIdx(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg_0000000000.jss")
	mustWriteSealedSegment(t, path, sealedFixture{
		minSeq: 0, maxSeq: 99, minWitnessedAt: 1_700_000_000_000_000, maxWitnessedAt: 1_700_000_010_000_000, eventCount: 10,
	})
	m := mustOpenManifest(t, dir)
	require.Equal(t, 1, m.SegmentCount())

	require.NoError(t, m.OnSegmentSealed(0, path))
	require.Equal(t, 1, m.SegmentCount())
}

func TestOnSegmentCompacted_ReplacesResidentMetadata(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg_0000000000.jss")
	mustWriteSealedSegment(t, path, sealedFixture{
		minSeq: 0, maxSeq: 9, minWitnessedAt: 1_000, maxWitnessedAt: 9_999, eventCount: 10,
	})
	m := mustOpenManifest(t, dir)
	before, _, _ := m.ListFrom(0, 1)
	require.Len(t, before, 1)
	require.EqualValues(t, 10, before[0].EventCount)

	_, err := segment.Rewrite(path, func(ev *segment.Event) segment.RowDecision {
		if ev.Seq < 5 {
			return segment.RowDrop
		}
		return segment.RowKeep
	}, segment.RewriteOptions{})
	require.NoError(t, err)

	require.NoError(t, m.OnSegmentCompacted(0, path))

	after, _, _ := m.ListFrom(0, 1)
	require.Len(t, after, 1)
	require.EqualValues(t, 5, after[0].EventCount)
	require.NotEqual(t, before[0].Checksum, after[0].Checksum)

	blocks, err := m.BlockIndex(0)
	require.NoError(t, err)
	var events uint32
	for _, b := range blocks {
		events += b.EventCount
	}
	require.EqualValues(t, 5, events)
}

// TestGenerationAdvancesOnMutation pins the contract the Phase B import
// bucketer's DID->candidate-segments cache relies on: Generation() is a
// monotonic counter that strictly increases on every event that could change
// which segments (or blocks) a DID resolves to -- initial load, seal, and
// compaction refresh. A cache entry tagged with an older generation must be
// treated as stale. Read-only queries must NOT advance it (or the cache would
// never hit).
func TestGenerationAdvancesOnMutation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m := mustOpenManifest(t, dir)

	// Initial load of an empty dir still establishes a baseline generation.
	g0 := m.Generation()

	// Read-only queries must not move it.
	_ = m.SegmentCount()
	_, _ = m.SelectBlocksForDID("did:plc:whatever")
	require.Equal(t, g0, m.Generation(), "read-only queries must not advance the generation")

	path := filepath.Join(dir, "seg_0000000000.jss")
	mustWriteSealedSegment(t, path, sealedFixture{
		minSeq: 0, maxSeq: 9, minWitnessedAt: 1_000, maxWitnessedAt: 9_999, eventCount: 10,
	})
	require.NoError(t, m.OnSegmentSealed(0, path))
	g1 := m.Generation()
	require.Greater(t, g1, g0, "seal must advance the generation")

	_, err := segment.Rewrite(path, func(ev *segment.Event) segment.RowDecision {
		if ev.Seq < 5 {
			return segment.RowDrop
		}
		return segment.RowKeep
	}, segment.RewriteOptions{})
	require.NoError(t, err)
	require.NoError(t, m.OnSegmentCompacted(0, path))
	g2 := m.Generation()
	require.Greater(t, g2, g1, "compaction refresh must advance the generation")
}

// TestRefreshSegment_RejectedRefreshLeavesManifestUntouched pins the
// commit-or-nothing contract of a manifest refresh: a refresh rejected by the
// seq-monotonicity validation must leave BOTH the resident segment set and the
// generation exactly as they were. If the rejected metadata stayed resident
// while the generation did not move, serving paths would observe the corrupt
// set and generation-tagged caches (the import bucketer) would keep treating
// their entries as fresh against it.
func TestRefreshSegment_RejectedRefreshLeavesManifestUntouched(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path0 := filepath.Join(dir, "seg_0000000000.jss")
	path1 := filepath.Join(dir, "seg_0000000001.jss")
	mustWriteSealedSegment(t, path0, sealedFixture{
		minSeq: 0, maxSeq: 9, minWitnessedAt: 1_000, maxWitnessedAt: 1_999, eventCount: 10,
	})
	mustWriteSealedSegment(t, path1, sealedFixture{
		minSeq: 10, maxSeq: 19, minWitnessedAt: 2_000, maxWitnessedAt: 2_999, eventCount: 10,
	})
	m := mustOpenManifest(t, dir)
	genBefore := m.Generation()
	before, _, _ := m.ListFrom(0, 2)
	require.Len(t, before, 2)

	// Replace segment 1 on disk with a seq envelope overlapping segment 0 and
	// refresh it: validation must reject the refresh.
	overlapping := filepath.Join(dir, "overlap.jss.staging")
	mustWriteSealedSegment(t, overlapping, sealedFixture{
		minSeq: 5, maxSeq: 15, minWitnessedAt: 1_500, maxWitnessedAt: 2_500, eventCount: 10,
	})
	require.NoError(t, os.Rename(overlapping, path1))
	require.ErrorIs(t, m.OnSegmentSealed(1, path1), manifest.ErrSegmentSeqOverlap)

	require.Equal(t, genBefore, m.Generation(),
		"rejected refresh must not advance the generation")
	after, _, _ := m.ListFrom(0, 2)
	require.Equal(t, before, after,
		"rejected refresh must leave the resident segment set untouched")
}

func mustOpenManifest(t *testing.T, dir string) *manifest.Manifest {
	t.Helper()
	m, err := manifest.Open(manifest.Options{
		SegmentsDir: dir,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	return m
}

func TestBlockIndex_LoadsAndCaches(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg_0000000000.jss")
	mustWriteSealedSegment(t, path, sealedFixture{
		minSeq: 0, maxSeq: 9, minWitnessedAt: 1_700_000_000_000_000, maxWitnessedAt: 1_700_000_001_000_000, eventCount: 10,
	})

	reg := prometheus.NewRegistry()
	metrics := manifest.NewMetrics(reg)
	m, err := manifest.Open(manifest.Options{
		SegmentsDir:         dir,
		BlockIndexCacheSize: 4,
		Logger:              slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:             metrics,
	})
	require.NoError(t, err)
	require.Equal(t, 1, m.SegmentCount())

	blocks1, err := m.BlockIndex(0)
	require.NoError(t, err)
	require.NotEmpty(t, blocks1)

	blocks2, err := m.BlockIndex(0)
	require.NoError(t, err)
	require.Equal(t, blocks1, blocks2)

	require.Equal(t, float64(0), readCounter(t, reg, "jetstream_manifest_block_index_cache_misses_total"))
	require.Equal(t, float64(2), readCounter(t, reg, "jetstream_manifest_block_index_cache_hits_total"))
}

func TestBlockIndex_UnknownSegment(t *testing.T) {
	t.Parallel()
	m := mustOpenManifest(t, t.TempDir())
	_, err := m.BlockIndex(42)
	require.Error(t, err)
}

func TestBlockIndex_AllSegmentsStayResident(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for i := range 5 {
		mustWriteSealedSegment(t,
			filepath.Join(dir, fmt.Sprintf("seg_%010d.jss", i)),
			sealedFixture{
				minSeq:         uint64(i * 10),
				maxSeq:         uint64(i*10 + 9),
				minWitnessedAt: int64(1_700_000_000_000_000 + i*1_000_000),
				maxWitnessedAt: int64(1_700_000_000_000_000 + (i+1)*1_000_000),
				eventCount:     10,
			})
	}

	reg := prometheus.NewRegistry()
	m, err := manifest.Open(manifest.Options{
		SegmentsDir:         dir,
		BlockIndexCacheSize: 2,
		Logger:              slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:             manifest.NewMetrics(reg),
	})
	require.NoError(t, err)

	for _, idx := range []uint64{0, 1, 2} {
		_, err := m.BlockIndex(idx)
		require.NoError(t, err)
	}
	require.Equal(t, float64(3), readCounter(t, reg, "jetstream_manifest_block_index_cache_hits_total"))

	_, err = m.BlockIndex(0)
	require.NoError(t, err)
	require.Equal(t, float64(0), readCounter(t, reg, "jetstream_manifest_block_index_cache_misses_total"))
	require.Equal(t, float64(4), readCounter(t, reg, "jetstream_manifest_block_index_cache_hits_total"),
		"segment 0 should remain resident even when BlockIndexCacheSize is small")
}

func readCounter(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		return mf.GetMetric()[0].GetCounter().GetValue()
	}
	return 0
}
