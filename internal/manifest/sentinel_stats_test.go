package manifest_test

import (
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// TestSegmentStats_ExcludesSentinelCollections pins that the DID-marker sentinel
// collections ($account/$identity/$sync) the seal index tags marker-bearing
// blocks with do NOT surface as phantom collections in operator-facing stats.
// They are a planner selection hint, not real collection traffic.
func TestSegmentStats_ExcludesSentinelCollections(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// A segment with a real commit plus all three DID-level markers, so seal
	// tags the block with $account/$identity/$sync.
	mustWriteSealedSegmentWithEvents(t, filepath.Join(dir, "seg_0000000000.jss"), 4096, []segment.Event{
		{Seq: 1, WitnessedAt: 1_700_000_000_000_000, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r1", Rev: "rev1", Payload: []byte{0xa0}},
		{Seq: 2, WitnessedAt: 1_700_000_001_000_000, Kind: segment.KindIdentity, DID: "did:plc:a"},
		{Seq: 3, WitnessedAt: 1_700_000_002_000_000, Kind: segment.KindAccount, DID: "did:plc:a"},
		{Seq: 4, WitnessedAt: 1_700_000_003_000_000, Kind: segment.KindSync, DID: "did:plc:a"},
	})

	m, err := manifest.Open(manifest.Options{
		SegmentsDir: dir,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	stats := m.SegmentStats()
	names := make([]string, 0, len(stats.Collections))
	for _, c := range stats.Collections {
		names = append(names, c.NSID)
		require.False(t, segment.IsDIDMarkerSentinelCollection(c.NSID),
			"sentinel collection %q must not appear in operator collection stats", c.NSID)
	}
	require.Contains(t, names, "app.bsky.feed.post", "the real collection must still be reported")
}
