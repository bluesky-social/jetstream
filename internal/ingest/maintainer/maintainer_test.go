package maintainer_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/maintainer"
	"github.com/bluesky-social/jetstream/internal/objstore/memblob"
	"github.com/bluesky-social/jetstream/internal/objstore/objcache"
	"github.com/bluesky-social/jetstream/internal/objstore/protocol"
	"github.com/bluesky-social/jetstream/internal/storagefake"
	"github.com/bluesky-social/jetstream/segment"
)

const blockEvents = 8

type env struct {
	t      *testing.T
	db     *storagefake.DB
	lease  *storagefake.Lease
	s      *catalog.Session
	up     *protocol.Uploader
	reader *protocol.Reader
	cache  *objcache.Cache
	gets   *getCounter
}

// getCounter counts blob GETs, so a test can tell cache hits from reads.
type getCounter struct{ n atomic.Int64 }

func (g *getCounter) BeforeBlobOp(op memblob.Op, _ string) (memblob.FaultKind, error) {
	if op == memblob.OpGet {
		g.n.Add(1)
	}
	return memblob.FaultNone, nil
}

func newEnv(t *testing.T, cached bool) *env {
	t.Helper()
	db := storagefake.New(storagefake.Config{})
	e := &env{t: t, db: db, lease: db.NewLease(), gets: &getCounter{}}
	require.NoError(t, e.lease.Acquire(t.Context(), time.Hour))
	e.s = catalog.NewSession(catalog.SessionConfig{DB: db, Epoch: e.lease.Epoch()})
	_, err := e.s.InitNamespace(t.Context(), catalog.Main, nil)
	require.NoError(t, err)
	if cached {
		e.cache = objcache.New(objcache.Config{MaxBytes: 64 << 20})
	}
	blob := memblob.New(memblob.WithFaultInjector(e.gets))
	archive := db.Archive().ArchiveID
	e.up, err = protocol.NewUploader(protocol.UploaderConfig{Blob: blob, ArchiveID: archive, GCDelay: time.Hour, OrphanAge: time.Hour})
	require.NoError(t, err)
	e.reader, err = protocol.NewReader(protocol.ReaderConfig{Rows: protocol.DBRows{DB: db}, Blob: blob, ArchiveID: archive, Cache: e.cache})
	require.NoError(t, err)
	t.Cleanup(func() {
		if !t.Failed() {
			require.NoError(t, db.Violation(), "catalog invariants")
		}
	})
	return e
}

// restart ends the leader session and starts the next one, as the election
// loop does after a session-ending failure.
func (e *env) restart() {
	e.t.Helper()
	require.NoError(e.t, e.lease.Release(e.t.Context()))
	require.NoError(e.t, e.lease.Acquire(e.t.Context(), time.Hour))
	e.s = catalog.NewSession(catalog.SessionConfig{DB: e.db, Epoch: e.lease.Epoch()})
}

func (e *env) maintainer(cfg maintainer.Config) *maintainer.Maintainer {
	e.t.Helper()
	cfg.Session = e.s
	if cfg.Uploader == nil {
		cfg.Uploader = e.up
	}
	cfg.Objects = e.reader
	cfg.Cache = e.cache
	cfg.MaxEventsPerBlock = blockEvents
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	m, err := maintainer.Open(e.t.Context(), cfg)
	require.NoError(e.t, err)
	return m
}

func (e *env) writer(sink ingest.BlockSink, hc ingest.HotConfig) *ingest.Writer {
	e.t.Helper()
	hc.Session = e.s
	hc.Sink = sink
	hc.BlockMaxAge = time.Hour
	if hc.BatchMaxEvents == 0 {
		hc.BatchMaxEvents = 3
	}
	// Two blocks: appends stall unless the maintainer's folds release them.
	hc.MaxUnfoldedEvents = 2 * blockEvents
	w, err := ingest.Open(ingest.Config{
		MaxEventsPerBlock: blockEvents,
		Hot:               &hc,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(e.t, err)
	return w
}

func (e *env) snapshot() *catalog.Snapshot {
	e.t.Helper()
	snap, err := e.db.Snapshot()
	require.NoError(e.t, err)
	return snap
}

func testEvents(rng *rand.Rand, n int) []segment.Event {
	colls := []string{"app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.graph.follow"}
	evs := make([]segment.Event, n)
	for i := range evs {
		payload := make([]byte, 100+rng.IntN(200))
		for j := range payload {
			payload[j] = byte(rng.Uint32())
		}
		evs[i] = segment.Event{
			WitnessedAt: time.Now().UnixMicro(),
			Kind:        segment.KindCreate,
			DID:         fmt.Sprintf("did:plc:u%d", rng.IntN(5)),
			Collection:  colls[rng.IntN(len(colls))],
			Rkey:        fmt.Sprintf("r%d", rng.Uint32()),
			Rev:         "3l",
			Payload:     payload,
		}
	}
	return evs
}

func appendAll(t *testing.T, ctx context.Context, w *ingest.Writer, evs []segment.Event) {
	t.Helper()
	for i := range evs {
		require.NoError(t, w.Append(ctx, &evs[i]))
	}
}

// generation is one sealed generation as a reader would serve it.
type generation struct {
	header, footer []byte
	frames         [][]byte
}

// file lays the generation out as the segment file it describes.
func (g generation) file() []byte {
	out := bytes.Clone(g.header)
	for _, f := range g.frames {
		out = binary.LittleEndian.AppendUint64(out, uint64(len(f)))
		out = append(out, f...)
	}
	return append(out, g.footer...)
}

// generations returns main's sealed generations in segment order.
func (e *env) generations(snap *catalog.Snapshot) []generation {
	e.t.Helper()
	ctx := e.t.Context()
	var out []generation
	for _, seg := range snap.Segments {
		if seg.Namespace != catalog.Main || seg.State != catalog.Sealed {
			continue
		}
		row := snap.Generations[seg.GenerationID]
		footer, err := e.reader.Get(ctx, row.FooterObjectID)
		require.NoError(e.t, err)
		g := generation{header: row.Header, footer: footer}
		for i, gb := range snap.GenerationBlocks[seg.GenerationID] {
			require.Equal(e.t, i, gb.Ordinal)
			frame, err := e.reader.Get(ctx, gb.ObjectID)
			require.NoError(e.t, err)
			g.frames = append(g.frames, frame)
		}
		out = append(out, g)
	}
	return out
}

// verify serves g as a virtual file, checks its metadata, and returns its
// events.
func verify(t *testing.T, g generation) []segment.Event {
	t.Helper()
	r, err := segment.OpenReaderParts(g.header, g.footer, func(i int) ([]byte, error) { return g.frames[i], nil }, segment.ReaderOptions{})
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close()) }()
	require.NoError(t, segment.VerifySealedMetadata(r))
	var out []segment.Event
	for i := range r.Blocks() {
		evs, err := r.DecodeBlock(i)
		require.NoError(t, err)
		out = append(out, evs...)
	}
	return out
}

// localSeal writes evs through local mode's segment writer, flushing at the
// same block size, and returns the sealed file.
func localSeal(t *testing.T, evs []segment.Event) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), "seg")
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: blockEvents})
	require.NoError(t, err)
	for _, ev := range evs {
		full, err := w.Append(ev)
		require.NoError(t, err)
		if full {
			require.NoError(t, w.Flush())
		}
	}
	_, err = w.Seal()
	require.NoError(t, err)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}

func requireNoActiveState(t *testing.T, snap *catalog.Snapshot) {
	t.Helper()
	require.Empty(t, snap.HotBatches)
	require.Empty(t, snap.ActiveBlocks)
}

func TestMaintainer_SealMatchesLocal(t *testing.T) {
	t.Parallel()
	for _, cached := range []bool{true, false} {
		t.Run(fmt.Sprintf("cached=%v", cached), func(t *testing.T) {
			t.Parallel()
			e := newEnv(t, cached)
			const maxSegment = 4000
			m := e.maintainer(maintainer.Config{MaxSegmentBytes: maxSegment, ReadConcurrency: 2})
			sink := &recSink{BlockSink: m}
			w := e.writer(sink, ingest.HotConfig{})
			evs := testEvents(rand.New(rand.NewPCG(1, 1)), 7*blockEvents+3)
			appendAll(t, t.Context(), w, evs)
			// Deliver every full block and let the folds and size-rule
			// seals they trigger finish, so only ForceRotate's work is
			// counted below.
			require.NoError(t, w.Flush(t.Context()))
			require.NoError(t, m.Sync(t.Context()))
			before := e.gets.n.Load()
			require.NoError(t, w.ForceRotate(t.Context()))
			gets := e.gets.n.Load() - before
			require.NoError(t, w.Close())
			require.NoError(t, m.Close())
			sink.requireAllFolded(t)

			snap := e.snapshot()
			requireNoActiveState(t, snap)
			gens := e.generations(snap)
			require.GreaterOrEqual(t, len(gens), 3, "the size rule and ForceRotate both sealed")
			next := 0
			for gi, g := range gens {
				got := verify(t, g)
				want := evs[next : next+len(got)]
				require.Equal(t, uint64(next+1), got[0].Seq)
				require.Equal(t, localSeal(t, want), g.file(), "generation %d", gi)
				next += len(got)

				// Local mode's rule: rotate after the first block that
				// takes the framed bytes to MaxSegmentBytes.
				framed := int64(len(g.file()) - len(g.header) - len(g.footer))
				if gi < len(gens)-1 {
					last := int64(8 + len(g.frames[len(g.frames)-1]))
					require.GreaterOrEqual(t, framed, int64(maxSegment))
					require.Less(t, framed-last, int64(maxSegment))
				} else {
					require.Less(t, framed, int64(maxSegment), "ForceRotate sealed the remainder")
				}
			}
			require.Equal(t, len(evs), next)

			// ForceRotate's fold and footer uploads each read back once. A
			// cached seal reads no block from the blob store.
			want := int64(2)
			if !cached {
				want += int64(len(gens[len(gens)-1].frames))
			}
			require.Equal(t, want, gets)
		})
	}
}

func TestMaintainer_FoldDedupsWholeBlockPointerBatch(t *testing.T) {
	t.Parallel()
	e := newEnv(t, true)
	reg := prometheus.NewRegistry()
	metrics := maintainer.NewMetrics(reg)
	m := e.maintainer(maintainer.Config{Metrics: metrics})
	sink := &recSink{BlockSink: m}
	w := e.writer(sink, ingest.HotConfig{Uploader: e.up, BulkChunkMaxEvents: blockEvents})
	rng := rand.New(rand.NewPCG(2, 2))

	// One bulk chunk fills the block in a single pointer batch.
	bulk := testEvents(rng, blockEvents)
	require.NoError(t, w.AppendBatch(ingest.WithClass(t.Context(), ingest.ClassBulk), bulk))
	require.NoError(t, w.Flush(t.Context()))
	require.NoError(t, m.Sync(t.Context()))
	batches := sink.batches()
	require.Len(t, batches, 1)
	require.NotZero(t, batches[0].ObjectID)

	snap := e.snapshot()
	require.Empty(t, snap.HotBatches)
	require.Len(t, snap.ActiveBlocks, 1)
	require.Equal(t, batches[0].ObjectID, snap.ActiveBlocks[0].ObjectID, "the fold references the pointer batch's object")
	require.Len(t, snap.Objects, 1, "nothing was uploaded for the fold")
	require.InDelta(t, 1, testutil.ToFloat64(metrics.Folds.WithLabelValues("dedup")), 0)

	// Live inline batches have no object to reuse.
	appendAll(t, t.Context(), w, testEvents(rng, blockEvents))
	require.NoError(t, w.Close())
	require.NoError(t, m.Sync(t.Context()))
	require.NoError(t, m.Close())
	snap = e.snapshot()
	require.Len(t, snap.ActiveBlocks, 2)
	require.Len(t, snap.Objects, 2)
	require.InDelta(t, 1, testutil.ToFloat64(metrics.Folds.WithLabelValues("uploaded")), 0)
}

// recSink records the first closed block's batches, and counts the blocks
// delivered and the Folded calls the maintainer made for them.
type recSink struct {
	ingest.BlockSink
	mu        sync.Mutex
	first     []ingest.HotBatchInfo
	delivered int
	folded    atomic.Int64
}

func (r *recSink) BlockClosed(b ingest.ClosedBlock) {
	r.mu.Lock()
	if r.first == nil {
		r.first = b.Batches
	}
	r.delivered++
	r.mu.Unlock()
	folded := b.Folded
	b.Folded = func() {
		r.folded.Add(1)
		folded()
	}
	r.BlockSink.BlockClosed(b)
}

func (r *recSink) batches() []ingest.HotBatchInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.first
}

// requireAllFolded checks every delivered block was released from the
// writer's unfolded cap.
func (r *recSink) requireAllFolded(t *testing.T) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	require.NotZero(t, r.delivered)
	require.EqualValues(t, r.delivered, r.folded.Load())
}

func TestMaintainer_SealCrash(t *testing.T) {
	t.Parallel()
	for _, kind := range []storagefake.FaultKind{storagefake.FaultCommitFails, storagefake.FaultCommitLost} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			e := newEnv(t, false)
			var failures atomic.Int64
			m := e.maintainer(maintainer.Config{OnFailure: func(error) { failures.Add(1) }})
			w := e.writer(m, ingest.HotConfig{})
			evs := testEvents(rand.New(rand.NewPCG(3, 3)), 2*blockEvents)
			appendAll(t, t.Context(), w, evs)
			require.NoError(t, w.Close())
			require.NoError(t, m.Sync(t.Context()))

			before := e.snapshot()
			require.Len(t, before.ActiveBlocks, 2)
			fault := &storagefake.Fault{Kind: kind, TxKind: catalog.TxSeal, Ordinal: 1}
			e.db.InjectFaults(fault)
			err := m.Rotate(t.Context())
			require.ErrorIs(t, err, catalog.ErrSessionEnded)
			require.True(t, fault.Fired())
			require.ErrorIs(t, m.Sync(t.Context()), catalog.ErrSessionEnded, "a failed maintainer refuses later work")
			require.ErrorIs(t, m.Close(), catalog.ErrSessionEnded)
			require.EqualValues(t, 1, failures.Load())
			require.Error(t, e.s.Err())

			after := e.snapshot()
			if kind == storagefake.FaultCommitFails {
				// The footer object was uploaded, but nothing references it:
				// GC's to reclaim.
				require.Equal(t, before.Segments, after.Segments)
				require.Equal(t, before.Generations, after.Generations)
				require.Equal(t, before.GenerationBlocks, after.GenerationBlocks)
				require.Equal(t, before.ActiveBlocks, after.ActiveBlocks)
				require.Equal(t, before.HotBatches, after.HotBatches)
			}

			// The next session seals from what actually committed.
			e.restart()
			m2 := e.maintainer(maintainer.Config{})
			require.NoError(t, m2.Rotate(t.Context()))
			require.NoError(t, m2.Close())
			snap := e.snapshot()
			requireNoActiveState(t, snap)
			gens := e.generations(snap)
			require.Len(t, gens, 1)
			got := verify(t, gens[0])
			require.Len(t, got, len(evs))
			require.Equal(t, localSeal(t, evs), gens[0].file())
		})
	}
}

// gatedUploader holds uploads until gate closes, reporting each start.
type gatedUploader struct {
	up      ingest.ObjectUploader
	started chan struct{}
	gate    chan struct{}
}

func (g *gatedUploader) Upload(ctx context.Context, s *catalog.Session, objs [][]byte) ([]catalog.ObjectRef, error) {
	g.started <- struct{}{}
	<-g.gate
	return g.up.Upload(ctx, s, objs)
}

func TestMaintainer_CloseLeavesQueuedBlocks(t *testing.T) {
	t.Parallel()
	e := newEnv(t, false)
	gu := &gatedUploader{up: e.up, started: make(chan struct{}, 4), gate: make(chan struct{})}
	m := e.maintainer(maintainer.Config{Uploader: gu})
	w := e.writer(m, ingest.HotConfig{})
	appendAll(t, t.Context(), w, testEvents(rand.New(rand.NewPCG(4, 4)), 2*blockEvents))
	require.NoError(t, w.Close())

	<-gu.started // the first fold is in flight; the second block is queued
	closed := make(chan error, 1)
	go func() { closed <- m.Close() }()
	requireBlocked(t, closed)
	close(gu.gate)
	require.NoError(t, <-closed)
	require.ErrorIs(t, m.Rotate(t.Context()), maintainer.ErrClosed)

	snap := e.snapshot()
	require.Len(t, snap.ActiveBlocks, 1, "the in-flight fold finished")
	require.Equal(t, uint64(blockEvents+1), snap.HotBatches[0].FirstSeq, "the queued block stays hot for the rebuild")
	require.Equal(t, uint64(2*blockEvents), snap.HotBatches[len(snap.HotBatches)-1].LastSeq)
}

func requireBlocked(t *testing.T, ch <-chan error) {
	t.Helper()
	select {
	case err := <-ch:
		t.Fatalf("returned early: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
}
