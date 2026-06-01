package status_test

import (
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream-v2/internal/ingest/live"
	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
	"github.com/bluesky-social/jetstream-v2/internal/manifest"
	"github.com/bluesky-social/jetstream-v2/internal/status"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/stretchr/testify/require"
)

func TestCollect_FreshDataDir(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	c, err := status.New(status.Options{
		Store:   st,
		DataDir: dataDir,
		Now:     func() time.Time { return time.Date(2026, 5, 25, 0, 0, 0, 0, time.UTC) },
	})
	require.NoError(t, err)

	snap, err := c.Snapshot(context.Background())
	require.NoError(t, err)
	require.NotNil(t, snap)

	// Empty data dir: no segments, no phase, no cursors.
	require.Equal(t, "", string(snap.Phase.Phase))
	require.True(t, snap.Phase.PhaseEnteredAt.IsZero())
	require.Equal(t, status.BackfillStats{}, snap.Backfill)
	require.Equal(t, status.LiveStats{}, snap.Live)
	require.NotNil(t, snap.SegmentAggregate)
	require.Len(t, snap.SegmentAggregate.Trees, 2)
	require.Equal(t, filepath.Join(dataDir, "segments"), snap.SegmentAggregate.Trees[0].Dir)
	require.Equal(t, filepath.Join(dataDir, "backfill", "live_segments"), snap.SegmentAggregate.Trees[1].Dir)
	require.Equal(t, 0, snap.SegmentAggregate.Trees[0].SealedCount+snap.SegmentAggregate.Trees[0].ActiveCount)
	require.Equal(t, 0, snap.SegmentAggregate.Trees[1].SealedCount+snap.SegmentAggregate.Trees[1].ActiveCount)
}

func TestCollect_BuildsFreshSnapshotEachCall(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	var mu sync.Mutex
	now := time.Date(2026, 5, 25, 0, 0, 0, 0, time.UTC)
	c, err := status.New(status.Options{
		Store:   st,
		DataDir: dataDir,
		Now: func() time.Time {
			mu.Lock()
			defer mu.Unlock()
			out := now
			now = now.Add(time.Second)
			return out
		},
	})
	require.NoError(t, err)

	a, err := c.Snapshot(context.Background())
	require.NoError(t, err)
	b, err := c.Snapshot(context.Background())
	require.NoError(t, err)
	require.NotSame(t, a, b, "snapshot pointer should change without a snapshot cache")
	require.NotEqual(t, a.GeneratedAt, b.GeneratedAt)
}

func TestCollect_PhaseAndEnteredAt(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	enteredAt := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, lifecycle.WritePhase(st, lifecycle.PhaseSteadyState, enteredAt))

	c, err := status.New(status.Options{
		Store:   st,
		DataDir: dataDir,
		Now:     func() time.Time { return enteredAt.Add(24 * time.Hour) },
	})
	require.NoError(t, err)
	snap, err := c.Snapshot(context.Background())
	require.NoError(t, err)

	require.Equal(t, lifecycle.PhaseSteadyState, snap.Phase.Phase)
	require.True(t, snap.Phase.PhaseEnteredAt.Equal(enteredAt))
}

func TestCollect_BackfillCounts(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	bs := backfill.NewStore(st, nil)
	ctx := context.Background()

	for i := range 5 {
		did := atmos.DID("did:plc:disc" + string(rune('a'+i)))
		require.NoError(t, bs.OnDiscover(ctx, atmossync.ListReposEntry{DID: did, Active: true}))
	}
	for i := range 3 {
		did := atmos.DID("did:plc:done" + string(rune('a'+i)))
		require.NoError(t, bs.OnDiscover(ctx, atmossync.ListReposEntry{DID: did, Active: true}))
		require.NoError(t, bs.OnComplete(ctx, did, &repo.Commit{Rev: "abcdef"}))
	}

	c, err := status.New(status.Options{Store: st, DataDir: dataDir})
	require.NoError(t, err)
	snap, err := c.Snapshot(ctx)
	require.NoError(t, err)

	require.Equal(t, uint64(8), snap.Backfill.TotalDIDs)
	require.Equal(t, uint64(5), snap.Backfill.Discovered)
	require.Equal(t, uint64(3), snap.Backfill.Complete)
	require.Equal(t, uint64(0), snap.Backfill.Failed)
	require.InDelta(t, 37.5, snap.Backfill.PercentComplete, 0.001)
}

func TestCollect_LiveCursors(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	require.NoError(t, live.SaveUpstreamCursor(st, live.CursorKey, 1234567))

	var seqBuf [8]byte
	binary.LittleEndian.PutUint64(seqBuf[:], 4242)
	require.NoError(t, st.Set([]byte(live.SteadySeqKey), seqBuf[:], store.SyncWrites))
	binary.LittleEndian.PutUint64(seqBuf[:], 1111)
	require.NoError(t, st.Set([]byte(live.BootstrapSeqKey), seqBuf[:], store.SyncWrites))

	c, err := status.New(status.Options{Store: st, DataDir: dataDir})
	require.NoError(t, err)
	snap, err := c.Snapshot(context.Background())
	require.NoError(t, err)

	require.Equal(t, int64(1234567), snap.Live.UpstreamCursor)
	require.Equal(t, uint64(4242), snap.Live.NextSeq)
	require.Equal(t, uint64(1111), snap.Live.BootstrapSeq)
}

func TestCollect_PebbleKeyspaces(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	require.NoError(t, st.Set([]byte("repo/a"), []byte("x"), store.SyncWrites))
	require.NoError(t, st.Set([]byte("repo/b"), []byte("x"), store.SyncWrites))
	require.NoError(t, st.Set([]byte("sync/chain/a"), []byte("x"), store.SyncWrites))
	require.NoError(t, st.Set([]byte("sync/host/a"), []byte("x"), store.SyncWrites))
	require.NoError(t, st.Set([]byte("relay/other"), []byte("x"), store.SyncWrites))
	require.NoError(t, st.Set([]byte("sync/identity/a"), []byte("x"), store.SyncWrites))

	c, err := status.New(status.Options{Store: st, DataDir: dataDir})
	require.NoError(t, err)
	snap, err := c.Snapshot(context.Background())
	require.NoError(t, err)

	require.Equal(t, uint64(2), snap.Pebble.KeyspaceCounts["repo/"])
	require.Equal(t, uint64(1), snap.Pebble.KeyspaceCounts["sync/chain/"])
	require.Equal(t, uint64(1), snap.Pebble.KeyspaceCounts["sync/host/"])
	require.Equal(t, uint64(1), snap.Pebble.KeyspaceCounts["relay/"])
	_, hasIdentity := snap.Pebble.KeyspaceCounts["sync/identity/"]
	require.False(t, hasIdentity, "sync/identity/ must not be exposed")
}

func TestCollect_WithManifestSkipsRepoScan(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	segmentsDir := filepath.Join(dataDir, "segments")
	require.NoError(t, os.MkdirAll(segmentsDir, 0o755))
	require.NoError(t, makeSealedStatusSegment(filepath.Join(segmentsDir, "seg_0000000000.jss")))

	mft, err := manifest.Open(manifest.Options{
		SegmentsDir: segmentsDir,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	require.NoError(t, st.Set([]byte("repo/did:plc:corrupt"), []byte("not json"), store.SyncWrites))
	require.NoError(t, backfill.SaveCounts(st, backfill.Counts{
		Total: 10, Discovered: 3, Complete: 6, Failed: 1,
	}))

	c, err := status.New(status.Options{Store: st, DataDir: dataDir, Manifest: mft})
	require.NoError(t, err)
	snap, err := c.Snapshot(context.Background())
	require.NoError(t, err)

	require.Equal(t, uint64(10), snap.Backfill.TotalDIDs)
	require.Equal(t, uint64(6), snap.Backfill.Complete)
	require.NotNil(t, snap.SegmentAggregate)
	require.Len(t, snap.SegmentAggregate.Trees, 2)
	require.Equal(t, 1, snap.SegmentAggregate.Trees[0].SealedCount)
	require.Equal(t, uint64(2), snap.SegmentAggregate.Trees[0].LatestSegment.EventCount)
	require.Equal(t, uint64(2), snap.SegmentAggregate.Network.Events)
	require.Greater(t, snap.SegmentAggregate.Trees[0].CompressedBytes, int64(0))
	_, hasRepoCount := snap.Pebble.KeyspaceCounts["repo/"]
	require.False(t, hasRepoCount, "manifest-backed status must not count Pebble keyspaces")
}

func TestCollect_WithManifestIncludesWritableTails(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	segmentsDir := filepath.Join(dataDir, "segments")
	liveDir := filepath.Join(dataDir, "backfill", "live_segments")
	require.NoError(t, os.MkdirAll(segmentsDir, 0o755))
	require.NoError(t, os.MkdirAll(liveDir, 0o755))
	require.NoError(t, makeSealedStatusSegment(filepath.Join(segmentsDir, "seg_0000000000.jss")))

	mft, err := manifest.Open(manifest.Options{
		SegmentsDir: segmentsDir,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	writeActiveSegment(t, segmentsDir, 1, []segment.Event{
		{Seq: 3, IndexedAt: 1_700_000_000_000_003, Kind: segment.KindCreate, DID: "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb", Collection: "app.bsky.feed.like", Rkey: "r", Rev: "v", Payload: []byte("p")},
	})
	writeActiveSegment(t, liveDir, 0, []segment.Event{
		{Seq: 4, IndexedAt: 1_700_000_000_000_004, Kind: segment.KindCreate, DID: "did:plc:cccccccccccccccccccccccc", Collection: "app.bsky.graph.follow", Rkey: "r", Rev: "v", Payload: []byte("p")},
	})

	require.NoError(t, backfill.SaveCounts(st, backfill.Counts{Total: 10, Discovered: 10, Complete: 8}))

	c, err := status.New(status.Options{Store: st, DataDir: dataDir, Manifest: mft})
	require.NoError(t, err)
	snap, err := c.Snapshot(context.Background())
	require.NoError(t, err)

	require.Len(t, snap.SegmentAggregate.Trees, 2)
	require.Equal(t, 1, snap.SegmentAggregate.Trees[0].SealedCount)
	require.Equal(t, 1, snap.SegmentAggregate.Trees[0].ActiveCount)
	require.Equal(t, uint64(3), snap.SegmentAggregate.Trees[0].EventCount)
	require.NotNil(t, snap.SegmentAggregate.Trees[0].LatestSegment)
	require.False(t, snap.SegmentAggregate.Trees[0].LatestSegment.Sealed)
	require.Equal(t, uint64(1), snap.SegmentAggregate.Trees[0].LatestSegment.Index)

	require.Equal(t, 0, snap.SegmentAggregate.Trees[1].SealedCount)
	require.Equal(t, 1, snap.SegmentAggregate.Trees[1].ActiveCount)
	require.Equal(t, uint64(1), snap.SegmentAggregate.Trees[1].EventCount)

	require.Equal(t, 3, snap.SegmentAggregate.Network.Segments)
	require.Equal(t, 1, snap.SegmentAggregate.Network.SealedSegments)
	require.Equal(t, 2, snap.SegmentAggregate.Network.ActiveSegments)
	require.Equal(t, uint64(4), snap.SegmentAggregate.Network.Events)
	require.Equal(t, 3, snap.SegmentAggregate.Network.Collections)
}

func TestCollect_CursorLookback_NoManifest(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	c, err := status.New(status.Options{
		Store:          st,
		DataDir:        dataDir,
		CursorLookback: 24 * time.Hour,
		Manifest:       nil, // No manifest wired in
	})
	require.NoError(t, err)
	snap, err := c.Snapshot(context.Background())
	require.NoError(t, err)

	// Should still report the configured lookback, but other fields are zero.
	require.Equal(t, 24*time.Hour, snap.CursorLookback.ConfiguredLookback)
	require.Equal(t, 0, snap.CursorLookback.ManifestSegmentCount)
	require.Equal(t, uint64(0), snap.CursorLookback.OldestRetainedSeq)
	require.True(t, snap.CursorLookback.OldestRetainedAt.IsZero())
}

func makeSealedStatusSegment(path string) error {
	w, err := segment.New(segment.Config{
		Path:              path,
		MaxEventsPerBlock: 2,
	})
	if err != nil {
		return err
	}
	for i := range 2 {
		if _, err := w.Append(segment.Event{
			Seq:        uint64(i + 1),
			Kind:       segment.KindCreate,
			DID:        "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
			IndexedAt:  1_700_000_000_000_000 + int64(i),
			Collection: "app.bsky.feed.post",
			Payload:    []byte("hello"),
		}); err != nil {
			return err
		}
	}
	_, err = w.Seal()
	return err
}

func TestCollect_CursorLookback_Disabled(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	c, err := status.New(status.Options{
		Store:          st,
		DataDir:        dataDir,
		CursorLookback: 0, // Disabled
	})
	require.NoError(t, err)
	snap, err := c.Snapshot(context.Background())
	require.NoError(t, err)

	// All fields should be zero when disabled.
	require.Equal(t, time.Duration(0), snap.CursorLookback.ConfiguredLookback)
	require.Equal(t, 0, snap.CursorLookback.ManifestSegmentCount)
	require.Equal(t, uint64(0), snap.CursorLookback.OldestRetainedSeq)
	require.True(t, snap.CursorLookback.OldestRetainedAt.IsZero())
}
