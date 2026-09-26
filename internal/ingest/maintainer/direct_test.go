package maintainer_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/maintainer"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/storagefake"
	"github.com/bluesky-social/jetstream/segment"
)

// directWriter opens a direct writer for ns in the current session, with a
// Segment over the namespace's active segment as its sealer.
func (e *env) directWriter(ns catalog.Namespace, maxSegment int64, cfg ingest.Config, dc ingest.DirectConfig) *ingest.Writer {
	e.t.Helper()
	seg, err := maintainer.OpenSegment(e.t.Context(), maintainer.SegmentConfig{
		Session:         e.s,
		Namespace:       ns,
		Uploader:        e.up,
		Objects:         e.reader,
		Cache:           e.cache,
		MaxSegmentBytes: maxSegment,
		ReadConcurrency: 2,
	})
	require.NoError(e.t, err)
	dc.Session, dc.Uploader, dc.Sealer = e.s, e.up, seg
	cfg.Namespace = ns
	cfg.Direct = &dc
	if cfg.MaxEventsPerBlock == 0 {
		cfg.MaxEventsPerBlock = blockEvents
	}
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	w, err := ingest.Open(cfg)
	require.NoError(e.t, err)
	return w
}

func (e *env) seqKey(ns catalog.Namespace) uint64 {
	e.t.Helper()
	v, err := e.db.MetaStore(nil).Get(context.Background(), []byte(catalog.SeqKey(ns)))
	if errors.Is(err, metastore.ErrNotFound) {
		return 1
	}
	require.NoError(e.t, err)
	return binary.LittleEndian.Uint64(v)
}

// directState is a namespace's catalog as direct mode left it: its sealed
// generations' events and framed sizes, and its active blocks' events, all in
// segment order.
type directState struct {
	sealed      [][]segment.Event
	sealedBytes []int64
	lastBlock   []int64 // framed size of each generation's last block
	active      []segment.Event
	activeRows  int
}

func (e *env) directState(ns catalog.Namespace) directState {
	e.t.Helper()
	ctx := e.t.Context()
	snap := e.snapshot()
	require.Empty(e.t, snap.HotBatches, "direct mode commits no hot batch")
	var st directState
	activeIdx := ^uint64(0)
	for _, seg := range snap.Segments {
		if seg.Namespace != ns {
			continue
		}
		if seg.State == catalog.Active {
			activeIdx = seg.Index
			continue
		}
		require.Equal(e.t, catalog.Sealed, seg.State)
		row := snap.Generations[seg.GenerationID]
		footer, err := e.reader.Get(ctx, row.FooterObjectID)
		require.NoError(e.t, err)
		g := generation{header: row.Header, footer: footer}
		for _, gb := range snap.GenerationBlocks[seg.GenerationID] {
			frame, err := e.reader.Get(ctx, gb.ObjectID)
			require.NoError(e.t, err)
			g.frames = append(g.frames, frame)
		}
		st.sealed = append(st.sealed, verify(e.t, g))
		st.sealedBytes = append(st.sealedBytes, int64(len(g.file())-len(g.header)-len(g.footer)))
		st.lastBlock = append(st.lastBlock, int64(8+len(g.frames[len(g.frames)-1])))
	}
	require.NotEqual(e.t, ^uint64(0), activeIdx, "%s has an active segment", ns)
	for _, r := range snap.ActiveBlocks {
		if r.Namespace != ns {
			continue
		}
		require.Equal(e.t, activeIdx, r.Segment, "active blocks belong to the active segment")
		require.Equal(e.t, st.activeRows, r.Ordinal)
		st.activeRows++
		frame, err := e.reader.Get(ctx, r.ObjectID)
		require.NoError(e.t, err)
		evs, err := segment.DecodeBlockFrame(frame)
		require.NoError(e.t, err)
		st.active = append(st.active, evs...)
	}
	return st
}

// events is every committed event in seq order.
func (st directState) events() []segment.Event {
	var out []segment.Event
	for _, g := range st.sealed {
		out = append(out, g...)
	}
	return append(out, st.active...)
}

// directHook checks the durable batch hook contract in direct mode: one call
// per block, nextSeq grows, the prepare value is the freeze-time sample, and
// afterCommit runs after the block's transaction and in order.
type directHook struct {
	t       *testing.T
	e       *env
	ns      catalog.Namespace
	lastApp atomic.Uint64

	mu            sync.Mutex
	last          uint64
	blocks        int
	forces        int
	committed     int
	lastCommitted uint64
	doneErrs      []error
}

func (h *directHook) onAppend(ev *segment.Event) error {
	h.lastApp.Store(ev.Seq)
	return nil
}

func (h *directHook) prepare() any { return h.lastApp.Load() }

func (h *directHook) hook(_ context.Context, b metastore.Batch, nextSeq uint64, force bool, pv any) (func(), func(error), error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if force {
		h.forces++
		require.GreaterOrEqual(h.t, nextSeq, h.last)
	} else {
		h.blocks++
		require.Greater(h.t, nextSeq, h.last)
		require.Equal(h.t, nextSeq-1, pv, "prepare value belongs to this block")
		require.Less(h.t, h.e.seqKey(h.ns), nextSeq, "the hook runs before its block commits")
	}
	h.last = nextSeq
	b.Set([]byte("test/hook/"+string(h.ns)), catalog.EncodeSeq(nextSeq))
	after := func() {
		require.GreaterOrEqual(h.t, h.e.seqKey(h.ns), nextSeq, "afterCommit runs after the commit")
		h.mu.Lock()
		defer h.mu.Unlock()
		require.GreaterOrEqual(h.t, nextSeq, h.lastCommitted, "afterCommit runs in order")
		h.lastCommitted = nextSeq
		h.committed++
	}
	done := func(err error) {
		h.mu.Lock()
		h.doneErrs = append(h.doneErrs, err)
		h.mu.Unlock()
	}
	return after, done, nil
}

func TestDirect_Swarm(t *testing.T) {
	t.Parallel()
	iterations := 16
	if !testing.Short() {
		iterations = 200
	}
	for i := range iterations {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			runDirectSwarm(t, rand.New(rand.NewPCG(uint64(i), 0xd1ec7)))
		})
	}
}

func runDirectSwarm(t *testing.T, rng *rand.Rand) {
	e := newEnv(t, rng.IntN(2) == 0)
	ns := catalog.Main
	if rng.IntN(2) == 0 {
		ns = catalog.BootstrapLive
		_, err := e.s.InitNamespace(t.Context(), ns, nil)
		require.NoError(t, err)
	}
	maxSegment := int64(1500 + rng.IntN(8000))
	maxBlock := 1 + rng.IntN(16)
	dc := ingest.DirectConfig{UploadConcurrency: 1 + rng.IntN(4), MaxPendingBlocks: rng.IntN(4)}

	var want []segment.Event
	var wantMu sync.Mutex
	record := func(evs ...segment.Event) {
		wantMu.Lock()
		defer wantMu.Unlock()
		want = append(want, evs...)
	}

	// Two sessions, so the second resumes the first's seq key and active
	// segment.
	for session := range 2 {
		if session > 0 {
			e.restart()
		}
		h := &directHook{t: t, e: e, ns: ns}
		start := e.seqKey(ns)
		w := e.directWriter(ns, maxSegment, ingest.Config{
			MaxEventsPerBlock:        maxBlock,
			OnAppend:                 h.onAppend,
			OnDurableBatch:           h.hook,
			DurableBatchPrepareValue: h.prepare,
		}, dc)
		require.Equal(t, start, w.NextSeq(), "a session starts at the committed seq key")

		var wg sync.WaitGroup
		for p := range 1 + rng.IntN(3) {
			prng := rand.New(rand.NewPCG(rng.Uint64(), uint64(p)))
			wg.Go(func() {
				var mine uint64
				for range 5 + prng.IntN(30) {
					switch prng.IntN(8) {
					case 0, 1, 2:
						evs := testEvents(prng, 1)
						require.NoError(t, w.Append(t.Context(), &evs[0]))
						record(evs...)
						mine = evs[0].Seq
					case 3, 4:
						evs := testEvents(prng, 1+prng.IntN(2*maxBlock))
						require.NoError(t, w.AppendBatch(t.Context(), evs))
						for i := 1; i < len(evs); i++ {
							require.Equal(t, evs[i-1].Seq+1, evs[i].Seq, "a batch's seqs are contiguous")
						}
						record(evs...)
						mine = evs[len(evs)-1].Seq
					case 5:
						require.NoError(t, w.Flush(t.Context()))
						require.Greater(t, e.seqKey(ns), mine, "Flush acks after the commit")
						require.Greater(t, w.ReadLog().DurableSeq(), mine)
					case 6:
						require.NoError(t, w.DrainDurability(t.Context()))
						require.Greater(t, e.seqKey(ns), mine)
					case 7:
						if prng.IntN(3) == 0 {
							require.NoError(t, w.ForceRotate(t.Context()))
							require.Greater(t, e.seqKey(ns), mine)
						}
					}
				}
			})
		}
		wg.Wait()
		next := w.NextSeq()
		if session == 1 && rng.IntN(2) == 0 {
			require.NoError(t, w.SealActiveAndClose())
			require.Zero(t, e.directState(ns).activeRows, "SealActiveAndClose leaves no active block")
		} else {
			require.NoError(t, w.Close())
		}
		require.Equal(t, next, e.seqKey(ns), "Close commits everything appended")
		require.Equal(t, next, w.ReadLog().DurableSeq())

		h.mu.Lock()
		require.Equal(t, h.blocks+h.forces, h.committed)
		for _, err := range h.doneErrs {
			require.NoError(t, err)
		}
		h.mu.Unlock()
		v, err := e.db.MetaStore(nil).Get(t.Context(), []byte("test/hook/"+string(ns)))
		require.NoError(t, err)
		require.Equal(t, next, binary.LittleEndian.Uint64(v), "hook metadata commits with its block")
	}

	st := e.directState(ns)
	got := st.events()
	require.Len(t, got, len(want))
	bySeq := make(map[uint64]segment.Event, len(want))
	for _, ev := range want {
		bySeq[ev.Seq] = ev
	}
	for i, ev := range got {
		require.Equal(t, uint64(i+1), ev.Seq, "committed seqs are gap-free from 1")
		requireSameEvent(t, bySeq[ev.Seq], ev)
	}
	// The rotation rule seals as soon as a block takes the segment to the
	// threshold, so no generation holds a block past it. ForceRotate seals
	// short ones.
	for i := range st.sealed {
		require.Less(t, st.sealedBytes[i]-st.lastBlock[i], maxSegment, "generation %d", i)
	}
	if ns == catalog.BootstrapLive {
		require.Equal(t, uint64(1), e.seqKey(catalog.Main), "main is untouched")
	}
}

// A failed block commit ends the writer and the session. The next session
// resumes at the committed seq key; a lost commit's block counts.
func TestDirect_CommitFailure(t *testing.T) {
	t.Parallel()
	for _, kind := range []storagefake.FaultKind{storagefake.FaultCommitFails, storagefake.FaultCommitLost} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			e := newEnv(t, false)
			fault := &storagefake.Fault{Kind: kind, TxKind: catalog.TxBlock, Ordinal: 3}
			e.db.InjectFaults(fault)
			var failures atomic.Int32
			h := &directHook{t: t, e: e, ns: catalog.Main}
			w := e.directWriter(catalog.Main, 1<<20, ingest.Config{
				OnDurableBatch:           h.hook,
				OnAppend:                 h.onAppend,
				DurableBatchPrepareValue: h.prepare,
			}, ingest.DirectConfig{
				UploadConcurrency: 1,
				OnFailure:         func(error) { failures.Add(1) },
			})
			rng := rand.New(rand.NewPCG(3, 3))
			var appendErr error
			for _, ev := range testEvents(rng, 6*blockEvents) {
				if appendErr = w.Append(t.Context(), &ev); appendErr != nil {
					break
				}
			}
			require.Error(t, errors.Join(appendErr, w.Flush(t.Context())))
			require.True(t, fault.Fired())
			ev := testEvents(rng, 1)[0]
			require.Error(t, w.Append(t.Context(), &ev), "the failure is sticky")
			require.Error(t, w.Close())
			require.Equal(t, int32(1), failures.Load())
			require.Error(t, e.s.Err(), "the session ended")

			want := uint64(2*blockEvents + 1)
			if kind == storagefake.FaultCommitLost {
				want += blockEvents // applied, result unknown
			}
			require.Equal(t, want, e.seqKey(catalog.Main))
			require.Less(t, w.ReadLog().DurableSeq(), uint64(3*blockEvents+1))
			h.mu.Lock()
			require.NotEmpty(t, h.doneErrs)
			require.Error(t, h.doneErrs[len(h.doneErrs)-1], "afterDone sees the failed commit")
			h.mu.Unlock()

			e.restart()
			w2 := e.directWriter(catalog.Main, 1<<20, ingest.Config{}, ingest.DirectConfig{})
			require.Equal(t, want, w2.NextSeq())
			evs := testEvents(rng, blockEvents)
			require.NoError(t, w2.AppendBatch(t.Context(), evs))
			require.Equal(t, want, evs[0].Seq)
			require.NoError(t, w2.Close())
			require.Len(t, e.directState(catalog.Main).events(), int(want)-1+blockEvents)
		})
	}
}

// A block's commit that lands after the segment reached the threshold, in a
// session that ended before sealing, is sealed by the next session before it
// commits anything else.
func TestDirect_SealsLeftoverFullSegment(t *testing.T) {
	t.Parallel()
	e := newEnv(t, false)
	fault := &storagefake.Fault{Kind: storagefake.FaultCommitFails, TxKind: catalog.TxSeal, Ordinal: 1}
	e.db.InjectFaults(fault)
	w := e.directWriter(catalog.Main, 1, ingest.Config{}, ingest.DirectConfig{})
	evs := testEvents(rand.New(rand.NewPCG(4, 4)), blockEvents)
	require.NoError(t, w.AppendBatch(t.Context(), evs))
	require.Error(t, w.Flush(t.Context()))
	require.Error(t, w.Close())
	require.True(t, fault.Fired())
	st := e.directState(catalog.Main)
	require.Empty(t, st.sealed)
	require.Equal(t, 1, st.activeRows, "the block committed; its seal failed")

	e.restart()
	w2 := e.directWriter(catalog.Main, 1, ingest.Config{}, ingest.DirectConfig{})
	evs2 := testEvents(rand.New(rand.NewPCG(5, 5)), 1)
	require.NoError(t, w2.Append(t.Context(), &evs2[0]))
	require.NoError(t, w2.Close())
	st = e.directState(catalog.Main)
	require.Len(t, st.sealed, 2, "the leftover full segment sealed first, then the new block's")
	require.Len(t, st.sealed[0], blockEvents)
	require.Zero(t, st.activeRows)
}

// Direct mode refuses to open main over hot batches: it precedes hot mode.
func TestDirect_RefusesHotBatches(t *testing.T) {
	t.Parallel()
	e := newEnv(t, false)
	hw := e.writer(&dropSink{}, ingest.HotConfig{})
	evs := testEvents(rand.New(rand.NewPCG(6, 6)), 2)
	appendAll(t, t.Context(), hw, evs)
	require.NoError(t, hw.Close())

	seg, err := maintainer.OpenSegment(t.Context(), maintainer.SegmentConfig{Session: e.s, Uploader: e.up, Objects: e.reader})
	require.NoError(t, err)
	_, err = ingest.Open(ingest.Config{
		Direct: &ingest.DirectConfig{Session: e.s, Uploader: e.up, Sealer: seg},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	_, corrupt := catalog.IsCorruption(err)
	require.True(t, corrupt, "got %v", err)
}

func TestDirect_ConfigValidation(t *testing.T) {
	t.Parallel()
	e := newEnv(t, false)
	seg, err := maintainer.OpenSegment(t.Context(), maintainer.SegmentConfig{Session: e.s, Uploader: e.up, Objects: e.reader})
	require.NoError(t, err)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ok := func() ingest.Config {
		return ingest.Config{Direct: &ingest.DirectConfig{Session: e.s, Uploader: e.up, Sealer: seg}, Logger: logger}
	}
	for name, mut := range map[string]func(*ingest.Config){
		"hot too":        func(c *ingest.Config) { c.Hot = &ingest.HotConfig{Session: e.s} },
		"no session":     func(c *ingest.Config) { c.Direct.Session = nil },
		"no uploader":    func(c *ingest.Config) { c.Direct.Uploader = nil },
		"no sealer":      func(c *ingest.Config) { c.Direct.Sealer = nil },
		"no logger":      func(c *ingest.Config) { c.Logger = nil },
		"bad namespace":  func(c *ingest.Config) { c.Namespace = "nope" },
		"wrong seq key":  func(c *ingest.Config) { c.SeqKey = catalog.BootstrapLiveSeqKey },
		"seq lease":      func(c *ingest.Config) { c.ReserveClientVisibleSeqs = true },
		"async flush":    func(c *ingest.Config) { c.AsyncFlushWorkers = 2 },
		"negative limit": func(c *ingest.Config) { c.Direct.MaxPendingBlocks = -1 },
		"huge block":     func(c *ingest.Config) { c.MaxEventsPerBlock = 1 << 30 },
	} {
		cfg := ok()
		mut(&cfg)
		_, err := ingest.Open(cfg)
		require.ErrorIs(t, err, ingest.ErrInvalidConfig, name)
	}
}
