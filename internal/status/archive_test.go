package status_test

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/metastore/pebblestore"
	"github.com/bluesky-social/jetstream/internal/status"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// TestCollect_ArchiveMatchesInspectAll is the parity check for reading
// segment trees through the catalog: over the same files, the snapshot's
// aggregate equals the offline InspectAll directory scan, except for file
// mtimes, which a catalog view does not carry.
func TestCollect_ArchiveMatchesInspectAll(t *testing.T) {
	t.Parallel()

	for seed := range uint64(8) {
		rng := rand.New(rand.NewPCG(seed, 0xa11))
		dataDir := t.TempDir()
		st, err := pebblestore.Open(dataDir, nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = st.Close() })

		mainDir := filepath.Join(dataDir, "segments")
		liveDir := filepath.Join(dataDir, "backfill", "live_segments")
		require.NoError(t, os.MkdirAll(mainDir, 0o755))
		require.NoError(t, os.MkdirAll(liveDir, 0o755))

		seq := uint64(1)
		events := func(n int) []segment.Event {
			kinds := []segment.Kind{segment.KindCreate, segment.KindCreate, segment.KindDelete, segment.KindAccount, segment.KindIdentity, segment.KindSync}
			collections := []string{"app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.graph.follow"}
			out := make([]segment.Event, n)
			for i := range out {
				ev := segment.Event{
					Seq:         seq,
					WitnessedAt: 1_700_000_000_000_000 + int64(seq)*1000 + rng.Int64N(500),
					Kind:        kinds[rng.IntN(len(kinds))],
					DID:         fmt.Sprintf("did:plc:%024d", rng.IntN(6)),
				}
				if ev.Kind == segment.KindCreate || ev.Kind == segment.KindDelete {
					ev.Collection = collections[rng.IntN(len(collections))]
					ev.Rkey, ev.Rev = "r", "v"
					if ev.Kind == segment.KindCreate {
						ev.Payload = []byte("p")
					}
				}
				out[i] = ev
				seq++
			}
			return out
		}
		for idx := range uint64(1 + rng.IntN(3)) {
			writeSealedSegment(t, mainDir, idx, events(1+rng.IntN(12)))
		}
		writeActiveBlocks(t, mainDir, 3, events(1+rng.IntN(12)))
		if rng.IntN(2) == 0 {
			writeSealedSegment(t, liveDir, 0, events(1+rng.IntN(12)))
		}
		writeActiveBlocks(t, liveDir, 1, events(1+rng.IntN(12)))

		c, err := status.New(status.Options{Store: st, DataDir: dataDir, Archive: openArchive(t, dataDir)})
		require.NoError(t, err)
		snap, err := c.Snapshot(t.Context())
		require.NoError(t, err)

		want, err := status.InspectAll([]string{mainDir, liveDir}, status.InspectAllOptions{})
		require.NoError(t, err)
		for i := range want.Trees {
			want.Trees[i].OldestMTime, want.Trees[i].NewestMTime = time.Time{}, time.Time{}
		}
		require.Empty(t, snap.SegmentAggregate.Warnings, "seed %d", seed)
		require.Equal(t, want, snap.SegmentAggregate, "seed %d", seed)
	}
}

// writeActiveBlocks is writeActiveSegment for more events than one block
// holds, so the active segment has several flushed blocks.
func writeActiveBlocks(t *testing.T, dir string, idx uint64, events []segment.Event) {
	t.Helper()
	w, err := segment.New(segment.Config{Path: filepath.Join(dir, ingest.SegmentFilename(idx)), MaxEventsPerBlock: 4})
	require.NoError(t, err)
	for i, ev := range events {
		_, err := w.Append(ev)
		require.NoError(t, err)
		if i%4 == 3 {
			require.NoError(t, w.Flush())
		}
	}
	require.NoError(t, w.Flush())
}

func TestCollect_NoArchiveOrDataDir(t *testing.T) {
	t.Parallel()
	st, err := pebblestore.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	// Disaggregated mode has no local data dir; the trees are labeled by
	// namespace and hold only what an archive would report.
	c, err := status.New(status.Options{Store: st})
	require.NoError(t, err)
	snap, err := c.Snapshot(t.Context())
	require.NoError(t, err)
	require.Len(t, snap.SegmentAggregate.Trees, 2)
	require.Equal(t, "main", snap.SegmentAggregate.Trees[0].Dir)
	require.Equal(t, "bootstrap_live", snap.SegmentAggregate.Trees[1].Dir)
	require.Zero(t, snap.SegmentAggregate.Network.Segments)
}
