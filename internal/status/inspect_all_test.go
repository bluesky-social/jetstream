package status_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/status"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func TestInspectAll_NoRoots(t *testing.T) {
	t.Parallel()
	agg, err := status.InspectAll(nil, status.InspectAllOptions{})
	require.NoError(t, err)
	require.NotNil(t, agg)
	require.Empty(t, agg.Trees)
	require.Empty(t, agg.Collections)
	require.Empty(t, agg.Warnings)
	require.Equal(t, status.NetworkTotals{}, agg.Network)
}

func TestInspectAll_MissingRoot(t *testing.T) {
	t.Parallel()
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	agg, err := status.InspectAll([]string{missing}, status.InspectAllOptions{})
	require.NoError(t, err)
	require.Len(t, agg.Trees, 1)
	require.Equal(t, missing, agg.Trees[0].Dir)
	require.Equal(t, 0, agg.Trees[0].SealedCount)
	require.Equal(t, 0, agg.Trees[0].ActiveCount)
	require.Nil(t, agg.Trees[0].LatestSegment)
	require.Empty(t, agg.Collections)
	require.Empty(t, agg.Warnings)
}

func TestInspectAll_EmptyExistingRoot(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	agg, err := status.InspectAll([]string{dir}, status.InspectAllOptions{})
	require.NoError(t, err)
	require.Len(t, agg.Trees, 1)
	require.Equal(t, dir, agg.Trees[0].Dir)
	require.Equal(t, 0, agg.Trees[0].SealedCount+agg.Trees[0].ActiveCount)
}

// writeSealedSegment writes a deterministic sealed segment file at
// dir/seg_<idx>.jss containing the provided events and returns the
// path. The fixture mirrors segment/inspect_test.go::makeSealedFixture
// so we exercise the same writer/seal code paths the production
// pipeline uses.
func writeSealedSegment(t *testing.T, dir string, idx uint64, events []segment.Event) string {
	t.Helper()
	path := filepath.Join(dir, ingest.SegmentFilename(idx))
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 4})
	require.NoError(t, err)
	for i, ev := range events {
		full, err := w.Append(ev)
		require.NoError(t, err)
		if full {
			require.NoError(t, w.Flush())
		}
		if i == len(events)-1 && !full {
			require.NoError(t, w.Flush())
		}
	}
	_, err = w.Seal()
	require.NoError(t, err)
	return path
}

func TestInspectAll_SingleSegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	events := []segment.Event{
		{Seq: 10, IndexedAt: 1_700_000_000_000_000, Kind: segment.KindCreate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "a", Rev: "r1", Payload: []byte("p1")},
		{Seq: 11, IndexedAt: 1_700_000_000_500_000, Kind: segment.KindCreate, DID: "did:plc:bob", Collection: "app.bsky.feed.like", Rkey: "b", Rev: "r2", Payload: []byte("p2")},
		{Seq: 12, IndexedAt: 1_700_000_001_000_000, Kind: segment.KindCreate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "c", Rev: "r3", Payload: []byte("p3")},
	}
	writeSealedSegment(t, dir, 1, events)

	agg, err := status.InspectAll([]string{dir}, status.InspectAllOptions{})
	require.NoError(t, err)
	require.Empty(t, agg.Warnings)
	require.Len(t, agg.Trees, 1)

	tree := agg.Trees[0]
	require.Equal(t, dir, tree.Dir)
	require.Equal(t, 1, tree.SealedCount)
	require.Equal(t, 0, tree.ActiveCount)
	require.Equal(t, uint64(3), tree.EventCount)
	require.Greater(t, tree.BlockCount, uint64(0))
	require.Greater(t, tree.CompressedBytes, int64(0))
	require.Greater(t, tree.UncompressedBytes, int64(0))
	require.Greater(t, tree.DiskBytes, int64(0))
	require.Equal(t, uint64(10), tree.MinSeq)
	require.Equal(t, uint64(12), tree.MaxSeq)
	require.False(t, tree.MinIndexedAt.IsZero())
	require.False(t, tree.MaxIndexedAt.IsZero())
	require.True(t, tree.MaxIndexedAt.After(tree.MinIndexedAt) || tree.MaxIndexedAt.Equal(tree.MinIndexedAt))
	require.NotNil(t, tree.LatestSegment)
	require.Equal(t, uint64(1), tree.LatestSegment.Index)
	require.True(t, tree.LatestSegment.Sealed)

	require.Len(t, agg.Collections, 2)
	// Sorted by event count desc; post has 2, like has 1.
	require.Equal(t, "app.bsky.feed.post", agg.Collections[0].NSID)
	require.Equal(t, uint64(2), agg.Collections[0].EventCount)
	require.Equal(t, 1, agg.Collections[0].SegmentCount)
	require.Greater(t, agg.Collections[0].BlockCount, uint64(0))
	require.Equal(t, "app.bsky.feed.like", agg.Collections[1].NSID)
	require.Equal(t, uint64(1), agg.Collections[1].EventCount)

	require.Equal(t, 1, agg.Network.Segments)
	require.Equal(t, 1, agg.Network.SealedSegments)
	require.Equal(t, 0, agg.Network.ActiveSegments)
	require.Equal(t, uint64(3), agg.Network.Events)
	require.Equal(t, 2, agg.Network.Collections)
	require.Equal(t, uint64(10), agg.Network.MinSeq)
	require.Equal(t, uint64(12), agg.Network.MaxSeq)
}

func TestInspectAll_MultiTreeMergesCollections(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	segDir := filepath.Join(dataDir, "segments")
	liveDir := filepath.Join(dataDir, "backfill", "live_segments")
	require.NoError(t, os.MkdirAll(segDir, 0o755))
	require.NoError(t, os.MkdirAll(liveDir, 0o755))

	// Seg 1 in segments/: 2 posts, 1 like.
	writeSealedSegment(t, segDir, 1, []segment.Event{
		{Seq: 1, IndexedAt: 1_700_000_000_000_000, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "x", Rev: "r1", Payload: []byte("p")},
		{Seq: 2, IndexedAt: 1_700_000_001_000_000, Kind: segment.KindCreate, DID: "did:plc:b", Collection: "app.bsky.feed.post", Rkey: "y", Rev: "r2", Payload: []byte("p")},
		{Seq: 3, IndexedAt: 1_700_000_002_000_000, Kind: segment.KindCreate, DID: "did:plc:c", Collection: "app.bsky.feed.like", Rkey: "z", Rev: "r3", Payload: []byte("p")},
	})
	// Seg 2 in segments/: 1 post, 1 follow.
	writeSealedSegment(t, segDir, 2, []segment.Event{
		{Seq: 4, IndexedAt: 1_700_000_003_000_000, Kind: segment.KindCreate, DID: "did:plc:d", Collection: "app.bsky.feed.post", Rkey: "w", Rev: "r4", Payload: []byte("p")},
		{Seq: 5, IndexedAt: 1_700_000_004_000_000, Kind: segment.KindCreate, DID: "did:plc:e", Collection: "app.bsky.graph.follow", Rkey: "v", Rev: "r5", Payload: []byte("p")},
	})
	// Seg 1 in backfill/live_segments/: 1 post (overlaps NSID with segments/).
	writeSealedSegment(t, liveDir, 1, []segment.Event{
		{Seq: 6, IndexedAt: 1_700_000_005_000_000, Kind: segment.KindCreate, DID: "did:plc:f", Collection: "app.bsky.feed.post", Rkey: "u", Rev: "r6", Payload: []byte("p")},
	})

	agg, err := status.InspectAll([]string{segDir, liveDir}, status.InspectAllOptions{})
	require.NoError(t, err)
	require.Empty(t, agg.Warnings)
	require.Len(t, agg.Trees, 2)

	// Per-tree counters.
	require.Equal(t, 2, agg.Trees[0].SealedCount)
	require.Equal(t, uint64(5), agg.Trees[0].EventCount)
	require.Equal(t, 1, agg.Trees[1].SealedCount)
	require.Equal(t, uint64(1), agg.Trees[1].EventCount)

	// Network totals.
	require.Equal(t, 3, agg.Network.Segments)
	require.Equal(t, uint64(6), agg.Network.Events)
	require.Equal(t, uint64(1), agg.Network.MinSeq)
	require.Equal(t, uint64(6), agg.Network.MaxSeq)
	require.Equal(t, 3, agg.Network.Collections)

	// Collections merge: post=4 (3 segs), like=1 (1 seg), follow=1 (1 seg).
	require.Len(t, agg.Collections, 3)
	require.Equal(t, "app.bsky.feed.post", agg.Collections[0].NSID)
	require.Equal(t, uint64(4), agg.Collections[0].EventCount)
	require.Equal(t, 3, agg.Collections[0].SegmentCount)
	// Tiebreak between like (1) and follow (1) is NSID asc -> "app.bsky.feed.like" < "app.bsky.graph.follow".
	require.Equal(t, "app.bsky.feed.like", agg.Collections[1].NSID)
	require.Equal(t, "app.bsky.graph.follow", agg.Collections[2].NSID)
}

func TestInspectAll_CorruptNonTailFileIsWarning(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Three segments. We'll corrupt #2 (middle); #3 is the tail.
	writeSealedSegment(t, dir, 1, []segment.Event{
		{Seq: 1, IndexedAt: 1_700_000_000_000_000, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "x", Rev: "r1", Payload: []byte("p")},
	})
	corruptPath := writeSealedSegment(t, dir, 2, []segment.Event{
		{Seq: 2, IndexedAt: 1_700_000_001_000_000, Kind: segment.KindCreate, DID: "did:plc:b", Collection: "app.bsky.feed.post", Rkey: "y", Rev: "r2", Payload: []byte("p")},
	})
	writeSealedSegment(t, dir, 3, []segment.Event{
		{Seq: 3, IndexedAt: 1_700_000_002_000_000, Kind: segment.KindCreate, DID: "did:plc:c", Collection: "app.bsky.feed.post", Rkey: "z", Rev: "r3", Payload: []byte("p")},
	})

	// Corrupt the middle file by overwriting its magic.
	f, err := os.OpenFile(corruptPath, os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte("XXXX"), 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	agg, err := status.InspectAll([]string{dir}, status.InspectAllOptions{})
	require.NoError(t, err)
	require.Len(t, agg.Trees, 1)
	require.Equal(t, 2, agg.Trees[0].SealedCount, "corrupt file should be excluded from sealed count")
	require.Equal(t, uint64(2), agg.Trees[0].EventCount, "corrupt file's events should be excluded")
	require.Len(t, agg.Warnings, 1)
	require.Contains(t, agg.Warnings[0], corruptPath)
}

func TestInspectAll_CorruptTailFileIsSilent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	writeSealedSegment(t, dir, 1, []segment.Event{
		{Seq: 1, IndexedAt: 1_700_000_000_000_000, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "x", Rev: "r1", Payload: []byte("p")},
	})
	tailPath := writeSealedSegment(t, dir, 2, []segment.Event{
		{Seq: 2, IndexedAt: 1_700_000_001_000_000, Kind: segment.KindCreate, DID: "did:plc:b", Collection: "app.bsky.feed.post", Rkey: "y", Rev: "r2", Payload: []byte("p")},
	})

	// Corrupt the tail file.
	f, err := os.OpenFile(tailPath, os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte("XXXX"), 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	agg, err := status.InspectAll([]string{dir}, status.InspectAllOptions{})
	require.NoError(t, err)
	require.Len(t, agg.Trees, 1)
	require.Equal(t, 1, agg.Trees[0].SealedCount, "tail corruption excludes only the tail")
	require.Equal(t, uint64(1), agg.Trees[0].EventCount)
	require.Empty(t, agg.Warnings, "tail rotation race should be silent")
}

// writeActiveSegment writes a deterministic active (unsealed) segment
// file at dir/seg_<idx>.jss containing the provided events. The file's
// header.checksum field is left zero so segment.Inspect classifies it
// as active.
func writeActiveSegment(t *testing.T, dir string, idx uint64, events []segment.Event) string {
	t.Helper()
	path := filepath.Join(dir, ingest.SegmentFilename(idx))
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 4})
	require.NoError(t, err)
	for _, ev := range events {
		_, err := w.Append(ev)
		require.NoError(t, err)
	}
	require.NoError(t, w.Flush())
	// No Seal — leave the file active.
	return path
}

func TestInspectAll_SkipUnsealed(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	writeSealedSegment(t, dir, 1, []segment.Event{
		{Seq: 1, IndexedAt: 1_700_000_000_000_000, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "x", Rev: "r1", Payload: []byte("p")},
	})
	writeActiveSegment(t, dir, 2, []segment.Event{
		{Seq: 2, IndexedAt: 1_700_000_001_000_000, Kind: segment.KindCreate, DID: "did:plc:b", Collection: "app.bsky.feed.like", Rkey: "y", Rev: "r2", Payload: []byte("p")},
	})

	// Without skip: both files contribute.
	full, err := status.InspectAll([]string{dir}, status.InspectAllOptions{})
	require.NoError(t, err)
	require.Equal(t, 1, full.Trees[0].SealedCount)
	require.Equal(t, 1, full.Trees[0].ActiveCount)
	require.Equal(t, uint64(2), full.Trees[0].EventCount)
	require.Len(t, full.Collections, 2)
	require.NotNil(t, full.Trees[0].LatestSegment)
	require.Equal(t, uint64(2), full.Trees[0].LatestSegment.Index)
	require.False(t, full.Trees[0].LatestSegment.Sealed)

	// With skip: active file's events / collections are not folded.
	skipped, err := status.InspectAll([]string{dir}, status.InspectAllOptions{SkipUnsealed: true})
	require.NoError(t, err)
	require.Equal(t, 1, skipped.Trees[0].SealedCount)
	require.Equal(t, 1, skipped.Trees[0].ActiveCount, "active file should still be counted")
	require.Equal(t, uint64(1), skipped.Trees[0].EventCount, "active events excluded with SkipUnsealed")
	require.Len(t, skipped.Collections, 1, "active NSIDs excluded")
	require.Equal(t, "app.bsky.feed.post", skipped.Collections[0].NSID)
	require.Greater(t, skipped.Trees[0].DiskBytes, int64(0), "active file size still counted")
	require.NotNil(t, skipped.Trees[0].LatestSegment, "LatestSegment should be set even with SkipUnsealed")
	require.Equal(t, uint64(2), skipped.Trees[0].LatestSegment.Index)
	require.False(t, skipped.Trees[0].LatestSegment.Sealed)
}
