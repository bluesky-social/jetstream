package main

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/status"
	"github.com/stretchr/testify/require"
)

var updateGolden = flag.Bool("update-golden", false, "rewrite golden files from current renderer output")

func TestRenderInspectAll_Basic(t *testing.T) {
	t.Parallel()

	agg := &status.SegmentAggregate{
		Trees: []status.TreeAggregate{
			{
				Dir:               "/data/segments",
				SealedCount:       3,
				ActiveCount:       1,
				CompressedBytes:   2 * 1024 * 1024,
				UncompressedBytes: 6 * 1024 * 1024,
				DiskBytes:         3 * 1024 * 1024,
				EventCount:        1234,
				BlockCount:        12,
				MinSeq:            10,
				MaxSeq:            1243,
				MinIndexedAt:      time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC),
				MaxIndexedAt:      time.Date(2026, 5, 28, 0, 0, 0, 0, time.UTC),
				OldestMTime:       time.Date(2026, 5, 27, 0, 1, 0, 0, time.UTC),
				NewestMTime:       time.Date(2026, 5, 28, 0, 1, 0, 0, time.UTC),
				LatestSegment: &status.SegmentSummary{
					Index:           4,
					Sealed:          false,
					EventCount:      300,
					UniqueDIDCount:  150,
					BlockCount:      3,
					CollectionCount: 2,
					MinSeq:          944,
					MaxSeq:          1243,
					SizeBytes:       768 * 1024,
				},
			},
			{Dir: "/data/backfill/live_segments"},
		},
		Collections: []status.CollectionAggregate{
			{NSID: "app.bsky.feed.post", EventCount: 900, SegmentCount: 4, BlockCount: 9},
			{NSID: "app.bsky.feed.like", EventCount: 300, SegmentCount: 2, BlockCount: 2},
			{NSID: "app.bsky.graph.follow", EventCount: 34, SegmentCount: 1, BlockCount: 1},
		},
		Network: status.NetworkTotals{
			Segments:          4,
			SealedSegments:    3,
			ActiveSegments:    1,
			Blocks:            12,
			Events:            1234,
			Collections:       3,
			CompressedBytes:   2 * 1024 * 1024,
			UncompressedBytes: 6 * 1024 * 1024,
			DiskBytes:         3 * 1024 * 1024,
			MinSeq:            10,
			MaxSeq:            1243,
			MinIndexedAt:      time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC),
			MaxIndexedAt:      time.Date(2026, 5, 28, 0, 0, 0, 0, time.UTC),
		},
		Warnings: []string{
			"/data/segments/seg_0000000002.jss: corrupt segment: bad magic \"XXXX\"",
		},
	}

	generatedAt := time.Date(2026, 5, 28, 12, 34, 56, 0, time.UTC)
	var buf bytes.Buffer
	require.NoError(t, renderInspectAll(&buf, "/data", agg, generatedAt, 100))

	goldenPath := filepath.Join("testdata", "inspect_all_basic.golden")
	if *updateGolden {
		require.NoError(t, os.MkdirAll("testdata", 0o755))
		require.NoError(t, os.WriteFile(goldenPath, buf.Bytes(), 0o644))
		return
	}
	want, err := os.ReadFile(goldenPath)
	require.NoError(t, err, "missing golden; rerun with -update-golden")
	require.Equal(t, string(want), buf.String())
}
