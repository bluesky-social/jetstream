package local_test

import (
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/catalog/local"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/metastore/memstore"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestLocal_RewriteSegmentPublishesNewGeneration(t *testing.T) {
	t.Parallel()

	s := newSwarm(t, 7)
	for len(s.sealed) == 0 {
		s.step()
	}
	require.NoError(t, s.w.Close())
	s.durable = append(s.durable, s.pending...)
	s.pending = nil
	target := s.sealed[0]

	// Keeping every row is not a rewrite and publishes nothing.
	rev := s.cat.Snapshot().Revision()
	res, v, err := s.cat.RewriteSegment(catalog.Main, target.Index,
		func(*segment.Event) segment.RowDecision { return segment.RowKeep }, segment.RewriteOptions{})
	require.NoError(t, err)
	require.False(t, res.Rewritten)
	require.Zero(t, v)
	require.Equal(t, rev, s.cat.Snapshot().Revision())

	res, v, err = s.cat.RewriteSegment(catalog.Main, target.Index, func(ev *segment.Event) segment.RowDecision {
		if ev.Seq == target.MinSeq() {
			return segment.RowDrop
		}
		return segment.RowKeep
	}, segment.RewriteOptions{})
	require.NoError(t, err)
	require.True(t, res.Rewritten)
	require.Equal(t, target.Index, v.Index)
	require.NotEqual(t, target.Generation, v.Generation)
	info, err := s.fs.Stat(s.cat.Path(catalog.Main, target.Index))
	require.NoError(t, err)
	require.Equal(t, info.Size(), v.Size)

	// The next snapshot reads the new generation without a Refresh.
	view := s.cat.Snapshot()
	require.Greater(t, view.Revision(), rev)
	require.Equal(t, eventSeqs(s.durable[1:]), eventSeqs(refsEvents(t, view, s.cat.Fetcher(), 0)))
}

func TestLocal_RemoveStaleTemps(t *testing.T) {
	t.Parallel()

	fs := vfs.NewMem()
	cat := newCatalog(t, fs)
	require.NoError(t, cat.RemoveStaleTemps(catalog.Main), "a missing directory is empty")

	require.NoError(t, fs.MkdirAll(segDir, 0o755))
	for _, name := range []string{"00000000000000000001.jss.tmp", "00000000000000000001.jss", "notes.tmp"} {
		f, err := fs.Create(fs.PathJoin(segDir, name))
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}
	require.NoError(t, cat.RemoveStaleTemps(catalog.Main))
	names, err := fs.List(segDir)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"00000000000000000001.jss", "notes.tmp"}, names)
}

func TestLocal_DeleteNamespaceRemovesRoot(t *testing.T) {
	t.Parallel()

	fs := vfs.NewMem()
	cat, err := local.New(local.DataDirConfig(fs, "/data"))
	require.NoError(t, err)
	w, err := ingest.Open(ingest.Config{
		SegmentsDir: "/data/backfill/live_segments", FS: fs, Store: memstore.New(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		Catalog: cat, Namespace: catalog.BootstrapLive, SeqKey: "live_segments/seq/next",
	})
	require.NoError(t, err)
	ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "1", WitnessedAt: 1}
	require.NoError(t, w.Append(t.Context(), &ev))
	require.NoError(t, w.SealActiveAndClose())
	require.NotEmpty(t, cat.Snapshot().Segments(catalog.BootstrapLive))

	require.NoError(t, cat.DeleteNamespace(catalog.BootstrapLive))
	_, err = fs.Stat("/data/backfill")
	require.True(t, oserror.IsNotExist(err), "the whole root goes, not just the segment directory: %v", err)
	require.Empty(t, cat.Snapshot().Segments(catalog.BootstrapLive))
	require.NoError(t, cat.Refresh(t.Context()))
	require.Empty(t, cat.Snapshot().Segments(catalog.BootstrapLive))

	require.NoError(t, cat.DeleteNamespace(catalog.BootstrapLive), "deleting a gone namespace is a no-op")
	require.Error(t, cat.DeleteNamespace("nowhere"))
}

func TestLocal_RootWithoutDirRejected(t *testing.T) {
	t.Parallel()

	_, err := local.New(local.Config{FS: vfs.NewMem(), Roots: map[catalog.Namespace]string{catalog.Main: "/data"}})
	require.Error(t, err)
}

// TestLocal_RefreshKeepsConcurrentSeals races Refresh against a writer
// sealing segments. A Refresh that scanned before a seal must not commit
// over it: the next scan would bring the segment back, but snapshots in
// between would miss a sealed segment.
func TestLocal_RefreshKeepsConcurrentSeals(t *testing.T) {
	t.Parallel()

	s := newSwarm(t, 3)
	var wg sync.WaitGroup
	done := make(chan struct{})
	wg.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			require.NoError(t, s.cat.Refresh(t.Context()))
		}
	})
	defer func() {
		close(done)
		wg.Wait()
	}()

	ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "1", WitnessedAt: 1}
	for range 100 {
		e := ev
		require.NoError(t, s.w.Append(t.Context(), &e))
		require.NoError(t, s.w.ForceRotate(t.Context()))

		have := map[uint64]bool{}
		for _, v := range s.cat.Snapshot().Segments(catalog.Main) {
			have[v.Index] = v.State == catalog.Sealed
		}
		for _, v := range s.sealed {
			require.True(t, have[v.Index], "sealed segment %d missing from the snapshot", v.Index)
		}
	}
}
