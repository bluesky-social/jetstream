package manifest_test

import (
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/stretchr/testify/require"
)

// seedN writes n sealed segments with indices 0..n-1 into dir.
func seedN(t *testing.T, dir string, n int) {
	t.Helper()
	for i := range n {
		base := uint64(i)*100 + 1
		mustWriteSealedSegment(t, filepath.Join(dir, ingest.SegmentFilename(uint64(i))), sealedFixture{
			minSeq:       base,
			maxSeq:       base + 9,
			minIndexedAt: 1_700_000_000_000_000 + int64(i)*1_000_000,
			maxIndexedAt: 1_700_000_000_000_000 + int64(i)*1_000_000 + 500_000,
			eventCount:   10,
		})
	}
}

func TestManifest_ListFrom_Pagination(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	seedN(t, dir, 5) // indices 0..4
	m := mustOpenManifest(t, dir)

	page1, next, more := m.ListFrom(0, 2)
	require.Len(t, page1, 2)
	require.True(t, more)
	require.EqualValues(t, 0, page1[0].Idx)
	require.EqualValues(t, 1, page1[1].Idx)
	require.EqualValues(t, 2, next)

	page2, next2, more2 := m.ListFrom(next, 2)
	require.Len(t, page2, 2)
	require.True(t, more2)
	require.EqualValues(t, 2, page2[0].Idx)
	require.EqualValues(t, 3, page2[1].Idx)
	require.EqualValues(t, 4, next2)

	page3, _, more3 := m.ListFrom(next2, 2)
	require.Len(t, page3, 1)
	require.False(t, more3)
	require.EqualValues(t, 4, page3[0].Idx)

	require.NotZero(t, page1[0].Checksum)
	require.Positive(t, page1[0].SizeBytes)
	require.EqualValues(t, 10, page1[0].EventCount)
	require.EqualValues(t, 1, page1[0].MinSeq)
	require.EqualValues(t, 10, page1[0].MaxSeq)
}

func TestManifest_ListFrom_ZeroLimit(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	seedN(t, dir, 2)
	m := mustOpenManifest(t, dir)

	entries, _, more := m.ListFrom(0, 0)
	require.Empty(t, entries)
	require.False(t, more)
}

func TestManifest_SegmentByIdx(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	seedN(t, dir, 2)
	m := mustOpenManifest(t, dir)

	ref, ok := m.SegmentByIdx(1)
	require.True(t, ok)
	require.NotZero(t, ref.Checksum)
	require.Positive(t, ref.SizeBytes)
	require.FileExists(t, ref.Path)

	_, ok = m.SegmentByIdx(999)
	require.False(t, ok, "unknown index must return ok=false")
}

func TestManifest_ListFrom_Empty(t *testing.T) {
	t.Parallel()
	m := mustOpenManifest(t, t.TempDir())
	entries, next, more := m.ListFrom(0, 10)
	require.Empty(t, entries)
	require.Zero(t, next)
	require.False(t, more)
}

func TestManifest_ListFrom_StartBeyondEnd(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	seedN(t, dir, 5) // indices 0..4
	m := mustOpenManifest(t, dir)
	entries, next, more := m.ListFrom(9999, 10)
	require.Empty(t, entries)
	require.Zero(t, next)
	require.False(t, more)
}

func TestManifest_ListFrom_NonContiguousIndices(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// Write segments with a GAP in indices: 0, 1, 5, 10.
	for _, idx := range []uint64{0, 1, 5, 10} {
		base := idx*100 + 1
		mustWriteSealedSegment(t, filepath.Join(dir, ingest.SegmentFilename(idx)), sealedFixture{
			minSeq:       base,
			maxSeq:       base + 9,
			minIndexedAt: 1_700_000_000_000_000 + int64(idx)*1_000_000,
			maxIndexedAt: 1_700_000_000_000_000 + int64(idx)*1_000_000 + 500_000,
			eventCount:   10,
		})
	}
	m := mustOpenManifest(t, dir)

	// Starting at 5 must return [5, 10], skipping the gap, with no spurious entries.
	entries, _, more := m.ListFrom(5, 10)
	require.Len(t, entries, 2)
	require.EqualValues(t, 5, entries[0].Idx)
	require.EqualValues(t, 10, entries[1].Idx)
	require.False(t, more)

	// Starting at 2 (a gap) must resume at the next existing index, 5.
	entries2, _, _ := m.ListFrom(2, 1)
	require.Len(t, entries2, 1)
	require.EqualValues(t, 5, entries2[0].Idx)

	// SegmentByIdx for a gap index returns ok=false (not a neighbor).
	_, ok := m.SegmentByIdx(2)
	require.False(t, ok)
	ref, ok := m.SegmentByIdx(5)
	require.True(t, ok)
	require.FileExists(t, ref.Path)
}
