package local_test

import (
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/catalog/local"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/metastore/memstore"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

const segDir = "/data/segments"

type swarm struct {
	t   *testing.T
	rng *rand.Rand
	fs  vfs.FS
	st  *memstore.Store
	cat *local.Catalog
	cfg ingest.Config
	w   *ingest.Writer

	// durable is every event the writer has made durable, in seq order.
	durable []segment.Event
	pending []segment.Event
	sealed  []catalog.SegmentView
}

func newSwarm(t *testing.T, seed uint64) *swarm {
	s := &swarm{
		t:   t,
		rng: rand.New(rand.NewPCG(seed, 0xca7a1)),
		fs:  vfs.NewMem(),
		st:  memstore.New(),
	}
	s.cat = newCatalog(t, s.fs)
	s.cat.OnSealed(catalog.Main, func(v catalog.SegmentView, path string) error {
		require.Equal(t, filepath.Join(segDir, ingest.SegmentFilename(v.Index)), path)
		s.sealed = append(s.sealed, v)
		return nil
	})
	s.cfg = ingest.Config{
		SegmentsDir:       segDir,
		FS:                s.fs,
		Store:             s.st,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxEventsPerBlock: 1 + s.rng.IntN(6),
		MaxSegmentBytes:   int64(64 + s.rng.IntN(1024)),
		Catalog:           s.cat,
	}
	s.open()
	return s
}

func newCatalog(t *testing.T, fs vfs.FS) *local.Catalog {
	c, err := local.New(local.Config{FS: fs, Dirs: map[catalog.Namespace]string{
		catalog.Main:          segDir,
		catalog.BootstrapLive: "/data/backfill/live_segments",
	}})
	require.NoError(t, err)
	return c
}

func (s *swarm) open() {
	w, err := ingest.Open(s.cfg)
	require.NoError(s.t, err)
	s.w = w
}

// markDurable moves pending events the writer has made durable.
func (s *swarm) markDurable() {
	durableNext := s.w.ReadLog().DurableSeq()
	i := 0
	for ; i < len(s.pending) && s.pending[i].Seq < durableNext; i++ {
		s.durable = append(s.durable, s.pending[i])
	}
	s.pending = s.pending[i:]
}

func (s *swarm) step() string {
	ctx := s.t.Context()
	switch op := s.rng.IntN(10); {
	case op < 6:
		n := 1 + s.rng.IntN(8)
		for range n {
			ev := segment.Event{
				Kind: segment.KindCreate, DID: fmt.Sprintf("did:plc:%d", s.rng.IntN(4)),
				Collection: "app.bsky.feed.post", Rkey: fmt.Sprint(s.rng.Uint32()), Rev: "r",
				WitnessedAt: int64(len(s.durable) + len(s.pending) + 1),
				Payload:     []byte{byte(s.rng.Uint32())},
			}
			require.NoError(s.t, s.w.Append(ctx, &ev))
			s.pending = append(s.pending, ev)
		}
		s.markDurable()
		return fmt.Sprintf("append %d", n)
	case op < 7:
		require.NoError(s.t, s.w.Flush(ctx))
		s.markDurable()
		return "flush"
	case op < 8:
		require.NoError(s.t, s.w.ForceRotate(ctx))
		s.markDurable()
		return "rotate"
	default:
		require.NoError(s.t, s.w.Close())
		s.durable = append(s.durable, s.pending...)
		s.pending = nil
		s.open()
		return "reopen"
	}
}

// refsEvents decodes every ref RefsFrom(seq) yields.
func refsEvents(t *testing.T, view catalog.CatalogView, f catalog.Fetcher, seq uint64) []segment.Event {
	var out []segment.Event
	var prevMax uint64
	first := true
	for ref := range view.RefsFrom(catalog.Main, seq) {
		require.GreaterOrEqual(t, ref.MaxSeq, seq)
		if !first {
			require.Greater(t, ref.MinSeq, prevMax, "refs out of order")
		}
		first, prevMax = false, ref.MaxSeq
		evs, err := catalog.DecodeRef(t.Context(), f, ref)
		require.NoError(t, err)
		// Compaction may empty a block but never moves its envelope.
		for _, ev := range evs {
			require.GreaterOrEqual(t, ev.Seq, ref.MinSeq)
			require.LessOrEqual(t, ev.Seq, ref.MaxSeq)
		}
		out = append(out, evs...)
	}
	return out
}

func eventSeqs(evs []segment.Event) []uint64 {
	out := make([]uint64, len(evs))
	for i := range evs {
		out[i] = evs[i].Seq
	}
	return out
}

// TestLocal_SwarmMatchesWriter drives a real ingest writer through random
// appends, flushes, rotations, and reopens, and after every step checks the
// catalog against what the writer made durable: RefsFrom from any seq
// yields exactly the durable events from the block holding that seq on,
// through the attached writer and through a fresh catalog that only scans
// the directory.
func TestLocal_SwarmMatchesWriter(t *testing.T) {
	t.Parallel()

	for seed := range uint64(12) {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			t.Parallel()
			s := newSwarm(t, seed)
			for i := range 60 {
				op := s.step()
				what := fmt.Sprintf("step %d (%s)", i, op)

				view := s.cat.Snapshot()
				got := refsEvents(t, view, s.cat.Fetcher(), 0)
				require.Equal(t, eventSeqs(s.durable), eventSeqs(got), what)
				require.Equal(t, s.durable, got, what)

				var tip uint64
				if len(s.durable) > 0 {
					tip = s.durable[len(s.durable)-1].Seq + 1
				}
				require.Equal(t, tip, view.TipSeq(catalog.Main), what)

				if len(s.durable) > 0 {
					seq := s.durable[s.rng.IntN(len(s.durable))].Seq
					from := refsEvents(t, view, s.cat.Fetcher(), seq)
					require.NotEmpty(t, from)
					require.LessOrEqual(t, from[0].Seq, seq, what)
					require.Equal(t, s.durable[len(s.durable)-len(from):], from, what)
				}

				fresh := newCatalog(t, s.fs)
				require.NoError(t, fresh.Refresh(t.Context()))
				require.Equal(t, s.durable, refsEvents(t, fresh.Snapshot(), fresh.Fetcher(), 0), what+": directory scan")

				segs := view.Segments(catalog.Main)
				for j, v := range segs {
					if j < len(segs)-1 {
						require.Equal(t, catalog.Sealed, v.State, what)
					}
				}
			}
			require.NoError(t, s.w.Close())
			s.durable = append(s.durable, s.pending...)
			require.Equal(t, s.durable, refsEvents(t, s.cat.Snapshot(), s.cat.Fetcher(), 0), "a closed writer still reports its durable tail")
			require.NotEmpty(t, s.sealed, "the swarm should rotate at least once")
			for _, v := range s.sealed {
				require.NotZero(t, v.Generation)
				require.Equal(t, catalog.Main, v.Namespace)
			}
		})
	}
}

// TestLocal_CompactionMakesRefsStale checks the generation pin: a ref taken
// before a rewrite fails with ErrStaleRef, and the next view's refs read the
// rewritten blocks.
func TestLocal_CompactionMakesRefsStale(t *testing.T) {
	t.Parallel()

	s := newSwarm(t, 99)
	for len(s.sealed) == 0 {
		s.step()
	}
	require.NoError(t, s.w.Close())
	s.durable = append(s.durable, s.pending...)
	s.pending = nil

	target := s.sealed[0]
	before := s.cat.Snapshot()
	var stale catalog.BlockRef
	for ref := range before.RefsFrom(catalog.Main, 0) {
		stale = ref
		break
	}
	require.Equal(t, target.Index, stale.Segment)

	path := s.cat.Path(catalog.Main, target.Index)
	res, err := segment.Rewrite(path, func(ev *segment.Event) segment.RowDecision {
		if ev.Seq == target.MinSeq() {
			return segment.RowDrop
		}
		return segment.RowKeep
	}, segment.RewriteOptions{FS: s.fs})
	require.NoError(t, err)
	require.True(t, res.Rewritten)

	// The stale fetch reloads the segment, so a reader's retry sees the new
	// generation before the compactor gets around to Reload.
	_, err = s.cat.Fetcher().Fetch(t.Context(), stale)
	require.ErrorIs(t, err, catalog.ErrStaleRef)
	after := s.cat.Snapshot()
	require.Greater(t, after.Revision(), before.Revision())

	// The compactor's own Reload of the same generation changes nothing.
	require.NoError(t, s.cat.Reload(catalog.Main, target.Index))
	require.Equal(t, after.Revision(), s.cat.Snapshot().Revision())
	got := refsEvents(t, after, s.cat.Fetcher(), 0)
	require.Equal(t, eventSeqs(s.durable[1:]), eventSeqs(got))

	// A full Refresh agrees with the targeted Reload.
	fresh := newCatalog(t, s.fs)
	require.NoError(t, fresh.Refresh(t.Context()))
	require.Equal(t, eventSeqs(got), eventSeqs(refsEvents(t, fresh.Snapshot(), fresh.Fetcher(), 0)))
}

func TestLocal_SealActiveAndCloseLeavesNoActive(t *testing.T) {
	t.Parallel()

	fs := vfs.NewMem()
	cat := newCatalog(t, fs)
	w, err := ingest.Open(ingest.Config{
		SegmentsDir: "/data/backfill/live_segments", FS: fs, Store: memstore.New(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		Catalog: cat, Namespace: catalog.BootstrapLive, SeqKey: "live_segments/seq/next",
	})
	require.NoError(t, err)
	ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "1", WitnessedAt: 1}
	require.NoError(t, w.Append(t.Context(), &ev))
	require.NoError(t, w.SealActiveAndClose())

	view := cat.Snapshot()
	segs := view.Segments(catalog.BootstrapLive)
	require.Len(t, segs, 1)
	require.Equal(t, catalog.Sealed, segs[0].State)
	require.Equal(t, ev.Seq+1, view.TipSeq(catalog.BootstrapLive))
	require.Zero(t, view.TipSeq(catalog.Main))

	cat.DropNamespace(catalog.BootstrapLive)
	require.Empty(t, cat.Snapshot().Segments(catalog.BootstrapLive))
}

// listHookFS runs hook once, after taking a directory listing and before
// returning it, so a test can change the directory behind a scan.
type listHookFS struct {
	vfs.FS
	hook func()
}

func (f *listHookFS) List(dir string) ([]string, error) {
	names, err := f.FS.List(dir)
	if hook := f.hook; hook != nil && dir == segDir {
		f.hook = nil
		hook()
	}
	return names, err
}

// TestLocal_RefreshKeepsSegmentsSealedDuringScan is the startup race: the
// runtime refreshes the catalog in the background while writers already
// publish seals, and a seal that lands after the scan listed the directory
// must survive the scan's result.
func TestLocal_RefreshKeepsSegmentsSealedDuringScan(t *testing.T) {
	t.Parallel()

	s := newSwarm(t, 7)
	for len(s.sealed) == 0 {
		s.step()
	}
	hfs := &listHookFS{FS: s.fs}
	cat, err := local.New(local.Config{FS: hfs, Dirs: map[catalog.Namespace]string{catalog.Main: segDir}})
	require.NoError(t, err)

	hfs.hook = func() {
		before := len(s.sealed)
		for len(s.sealed) < before+2 {
			s.step()
		}
		// The writer publishes to s.cat; forward its seals the way a writer
		// attached to cat would.
		for _, v := range s.sealed[before:] {
			require.NoError(t, cat.Sealed(v))
		}
	}
	require.NoError(t, cat.Refresh(t.Context()))
	require.Nil(t, hfs.hook, "the hook should have run")

	got := cat.Snapshot().Segments(catalog.Main)
	require.NotEmpty(t, got)
	last := s.sealed[len(s.sealed)-1]
	require.Equal(t, last.Index, got[len(got)-1].Index, "a segment sealed during the scan was dropped")
	for _, v := range s.sealed {
		found := false
		for _, g := range got {
			found = found || g.Index == v.Index
		}
		require.True(t, found, "sealed segment %d missing", v.Index)
	}
}
