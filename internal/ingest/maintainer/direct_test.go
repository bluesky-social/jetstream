package maintainer_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/crashpoint"
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
// per block, nextSeq grows, the prepare value is the freeze-time sample,
// afterCommit runs after the block's transaction and in order, afterDone runs
// in order, and at most one batch is open when the next is staged.
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
	open          int
	lastDone      uint64
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
	require.LessOrEqual(h.t, h.open, 1, "the writer stages one batch ahead of the commit")
	h.open++
	if force {
		h.forces++
		require.GreaterOrEqual(h.t, nextSeq, h.last)
		// assert, not require: this runs on the committer goroutine.
		assert.Less(h.t, h.lastApp.Load(), nextSeq, "a forced checkpoint covers every appended event")
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
		defer h.mu.Unlock()
		require.GreaterOrEqual(h.t, nextSeq, h.lastDone, "afterDone runs in order")
		h.lastDone = nextSeq
		h.open--
		h.doneErrs = append(h.doneErrs, err)
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

// skewed stamps events up to 5ms early, as a source that stamps before it
// waits for the writer does.
func skewed(rng *rand.Rand, evs []segment.Event) []segment.Event {
	for i := range evs {
		evs[i].WitnessedAt -= int64(rng.IntN(5000))
	}
	return evs
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
						evs := skewed(prng, testEvents(prng, 1))
						require.NoError(t, w.Append(t.Context(), &evs[0]))
						record(evs...)
						mine = evs[0].Seq
					case 3, 4:
						evs := skewed(prng, testEvents(prng, 1+prng.IntN(2*maxBlock)))
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
		if i > 0 {
			require.GreaterOrEqual(t, ev.WitnessedAt, got[i-1].WitnessedAt, "witnessed_at is monotonic with seq")
		}
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

// A direct session's floor comes from the previous session's active blocks,
// or from the sealed generation's header once no active block is left.
func TestDirect_WitnessedMonotonicAcrossSessions(t *testing.T) {
	t.Parallel()
	e := newEnv(t, false)
	rng := rand.New(rand.NewPCG(3, 3))
	appendAt := func(w *ingest.Writer, us int64) int64 {
		ev := testEvents(rng, 1)[0]
		ev.WitnessedAt = us
		require.NoError(t, w.Append(t.Context(), &ev))
		return ev.WitnessedAt
	}
	open := func() *ingest.Writer {
		return e.directWriter(catalog.Main, 1<<20, ingest.Config{}, ingest.DirectConfig{})
	}
	w := open()
	require.Equal(t, int64(1000), appendAt(w, 1000))
	require.Equal(t, int64(1000), appendAt(w, 900))
	require.NoError(t, w.Close())

	e.restart()
	w = open()
	require.Equal(t, int64(1000), appendAt(w, 500), "the floor comes from the active blocks")
	require.Equal(t, int64(2000), appendAt(w, 2000))
	require.NoError(t, w.SealActiveAndClose())

	e.restart()
	w = open()
	require.Equal(t, int64(2000), appendAt(w, 10), "the floor comes from the sealed generation")
	require.NoError(t, w.Close())

	var got []int64
	for _, ev := range e.directState(catalog.Main).events() {
		got = append(got, ev.WitnessedAt)
	}
	require.Equal(t, []int64{1000, 1000, 1000, 2000, 2000}, got)
}

// DrainDurability's checkpoint covers every event appended before it commits,
// as local mode's does: appends wait while the checkpoint is queued behind
// earlier blocks. The backfill completion batcher relies on this, and fails
// the writer when a forced checkpoint leaves out an appended completion.
func TestDirect_DrainHoldsAppends(t *testing.T) {
	t.Parallel()
	e := newEnv(t, false)
	rng := rand.New(rand.NewPCG(7, 7))
	var lastApp atomic.Uint64
	blockHeld := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var held atomic.Bool
	var forced atomic.Bool
	w := e.directWriter(catalog.Main, 1<<20, ingest.Config{
		OnAppend: func(ev *segment.Event) error { lastApp.Store(ev.Seq); return nil },
		OnDurableBatch: func(_ context.Context, _ metastore.Batch, nextSeq uint64, force bool, _ any) (func(), func(error), error) {
			if force {
				forced.Store(true)
				assert.Less(t, lastApp.Load(), nextSeq, "the checkpoint covers every appended event")
				return nil, nil, nil
			}
			if held.CompareAndSwap(false, true) {
				close(blockHeld)
				<-release
			}
			return nil, nil, nil
		},
	}, ingest.DirectConfig{})

	evs := testEvents(rng, 2)
	require.NoError(t, w.Append(t.Context(), &evs[0]))
	drained := make(chan error, 1)
	go func() { drained <- w.DrainDurability(t.Context()) }()
	<-blockHeld // the drain froze the open block; its checkpoint waits behind it
	appended := make(chan error, 1)
	go func() { appended <- w.Append(t.Context(), &evs[1]) }()
	select {
	case err := <-appended:
		unblock()
		t.Fatalf("append finished while a checkpoint was queued (err=%v)", err)
	case <-time.After(50 * time.Millisecond):
	}
	unblock()
	require.NoError(t, <-drained)
	require.NoError(t, <-appended)
	require.True(t, forced.Load())
	require.NoError(t, w.Close())
	require.Len(t, e.directState(catalog.Main).events(), 2)
}

// An append cancelled while it waits for room under MaxPendingBlocks
// returns ErrAppendCancelled and leaves the writer and session usable: the
// backfill cap cancels repos mid-wait, and that must not end the leader
// session.
func TestDirect_CancelledWaitKeepsWriter(t *testing.T) {
	t.Parallel()
	e := newEnv(t, false)
	rng := rand.New(rand.NewPCG(11, 11))
	blockHeld := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var held atomic.Bool
	var failures atomic.Int32
	w := e.directWriter(catalog.Main, 1<<20, ingest.Config{
		OnDurableBatch: func(context.Context, metastore.Batch, uint64, bool, any) (func(), func(error), error) {
			if held.CompareAndSwap(false, true) {
				close(blockHeld)
				<-release
			}
			return nil, nil, nil
		},
	}, ingest.DirectConfig{
		UploadConcurrency: 1,
		MaxPendingBlocks:  1,
		OnFailure:         func(error) { failures.Add(1) },
	})

	evs := testEvents(rng, 2*blockEvents)
	require.NoError(t, w.AppendBatch(t.Context(), evs[:blockEvents]))
	<-blockHeld // the full block is pending, so the next append waits

	ctx, cancel := context.WithCancel(t.Context())
	appended := make(chan error, 1)
	go func() { appended <- w.AppendBatch(ctx, evs[blockEvents:]) }()
	select {
	case err := <-appended:
		t.Fatalf("append finished with no room (err=%v)", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	err := <-appended
	require.ErrorIs(t, err, ingest.ErrAppendCancelled)
	require.ErrorIs(t, err, context.Canceled)

	unblock()
	require.NoError(t, w.AppendBatch(t.Context(), evs[blockEvents:]))
	require.NoError(t, w.Close())
	require.NoError(t, e.s.Err(), "the session survives")
	require.Zero(t, failures.Load())
	require.Len(t, e.directState(catalog.Main).events(), 2*blockEvents)
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

// crashFunc is a crashpoint.Injector from a function.
type crashFunc func(context.Context, crashpoint.Point) error

func (f crashFunc) SimulateCrash(ctx context.Context, p crashpoint.Point) error { return f(ctx, p) }

// hookLog is a durable batch hook that records its calls and callbacks in
// order. fail, if set, fails the n-th call.
type hookLog struct {
	mu    sync.Mutex
	log   []string
	calls int
	fail  map[int]error
	// called receives each call's number.
	called chan int
}

func newHookLog() *hookLog { return &hookLog{called: make(chan int, 64)} }

func (h *hookLog) note(format string, args ...any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.log = append(h.log, fmt.Sprintf(format, args...))
}

func (h *hookLog) entries() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.log...)
}

func (h *hookLog) hook(_ context.Context, b metastore.Batch, nextSeq uint64, force bool, _ any) (func(), func(error), error) {
	h.mu.Lock()
	h.calls++
	n, err := h.calls, h.fail[h.calls]
	h.log = append(h.log, fmt.Sprintf("hook %d", n))
	h.mu.Unlock()
	h.called <- n
	if !force {
		b.Set([]byte("test/hook"), catalog.EncodeSeq(nextSeq))
	}
	done := func(err error) { h.note("done %d: %v", n, err) }
	if err != nil {
		return nil, done, err
	}
	return func() { h.note("commit %d", n) }, done, nil
}

// waitCalled waits for the hook's n-th call, failing the test if it does not
// come while the writer holds a commit for it.
func (h *hookLog) waitCalled(t *testing.T, n int) {
	for {
		select {
		case got := <-h.called:
			if got >= n {
				return
			}
		case <-time.After(10 * time.Second):
			t.Errorf("hook call %d did not run while the commit before it waited", n)
			return
		}
	}
}

// holdCommits holds the i-th block's commit until holds[i] returns.
func holdCommits(holds ...func()) crashpoint.Injector {
	var mu sync.Mutex
	n := 0
	return crashFunc(func(_ context.Context, p crashpoint.Point) error {
		if p != crashpoint.AfterDirectBlockUploadBeforeCommit {
			return nil
		}
		mu.Lock()
		i := n
		n++
		mu.Unlock()
		if i < len(holds) {
			holds[i]()
		}
		return nil
	})
}

// The writer runs the next block's hook while the block before it commits:
// the backfill hook reads the catalog, and so does a commit. Hooks,
// commits, and callbacks still run in order.
func TestDirect_HookOverlapsPreviousCommit(t *testing.T) {
	t.Parallel()
	e := newEnv(t, false)
	h := newHookLog()
	w := e.directWriter(catalog.Main, 1<<20, ingest.Config{OnDurableBatch: h.hook}, ingest.DirectConfig{
		Crash: holdCommits(func() { h.waitCalled(t, 2) }),
	})
	evs := testEvents(rand.New(rand.NewPCG(21, 21)), 3*blockEvents)
	require.NoError(t, w.AppendBatch(t.Context(), evs))
	require.NoError(t, w.Flush(t.Context()))
	require.Equal(t, []string{
		"hook 1", "hook 2", "commit 1", "done 1: <nil>",
		"commit 2", "done 2: <nil>", "commit 3", "done 3: <nil>",
	}, slices.DeleteFunc(h.entries(), func(s string) bool { return s == "hook 3" }))
	require.Equal(t, uint64(3*blockEvents+1), e.seqKey(catalog.Main))
	require.NoError(t, w.Close())
	require.Len(t, e.directState(catalog.Main).events(), 3*blockEvents)
}

// A failed commit fails the batch staged behind it, which never commits,
// and nothing after it runs the hook.
func TestDirect_FailedCommitFailsStagedBatch(t *testing.T) {
	t.Parallel()
	e := newEnv(t, false)
	fault := &storagefake.Fault{Kind: storagefake.FaultCommitFails, TxKind: catalog.TxBlock, Ordinal: 1}
	e.db.InjectFaults(fault)
	h := newHookLog()
	var failures atomic.Int32
	w := e.directWriter(catalog.Main, 1<<20, ingest.Config{OnDurableBatch: h.hook}, ingest.DirectConfig{
		Crash:     holdCommits(func() { h.waitCalled(t, 2) }),
		OnFailure: func(error) { failures.Add(1) },
	})
	evs := testEvents(rand.New(rand.NewPCG(22, 22)), 3*blockEvents)
	require.NoError(t, w.AppendBatch(t.Context(), evs))
	require.Error(t, w.Flush(t.Context()))
	require.Error(t, w.Close())
	require.True(t, fault.Fired())
	require.Equal(t, int32(1), failures.Load())
	log := h.entries()
	require.Len(t, log, 4, "%q", log)
	require.Equal(t, []string{"hook 1", "hook 2"}, log[:2])
	require.Contains(t, log[2], "done 1: ")
	require.Contains(t, log[3], "done 2: ")
	require.NotContains(t, log[2], "<nil>")
	require.NotContains(t, log[3], "<nil>", "the staged batch failed with the writer")
	require.Equal(t, uint64(1), e.seqKey(catalog.Main))
	_, err := e.db.MetaStore(nil).Get(t.Context(), []byte("test/hook"))
	require.ErrorIs(t, err, metastore.ErrNotFound)
}

// A hook that fails while the block before it commits leaves that block's
// commit alone, then fails the writer. Nothing after it runs the hook, even
// while the committer has yet to reach the failure.
func TestDirect_HookFailureAfterOverlappedCommit(t *testing.T) {
	t.Parallel()
	e := newEnv(t, false)
	h := newHookLog()
	boom := errors.New("hook failed")
	h.fail = map[int]error{2: boom}
	w := e.directWriter(catalog.Main, 1<<20, ingest.Config{OnDurableBatch: h.hook}, ingest.DirectConfig{
		Crash: holdCommits(func() { h.waitCalled(t, 2) }, func() { time.Sleep(50 * time.Millisecond) }),
	})
	evs := testEvents(rand.New(rand.NewPCG(23, 23)), 3*blockEvents)
	require.NoError(t, w.AppendBatch(t.Context(), evs))
	require.ErrorIs(t, w.Flush(t.Context()), boom)
	require.ErrorIs(t, w.Close(), boom)
	require.Equal(t, []string{
		"hook 1", "hook 2", "commit 1", "done 1: <nil>", "done 2: ingest: on_durable_batch: hook failed",
	}, h.entries(), "the block before commits; nothing after runs the hook")
	require.Equal(t, uint64(blockEvents+1), e.seqKey(catalog.Main))
}

// A block whose upload failed is not staged, and fails the writer once the
// block before it commits.
func TestDirect_UploadFailureAfterOverlappedCommit(t *testing.T) {
	t.Parallel()
	e := newEnv(t, false)
	h := newHookLog()
	boom := errors.New("upload failed")
	held := make(chan struct{})
	release := make(chan struct{})
	uploadFailed := make(chan struct{})
	var failUpload atomic.Bool
	var once sync.Once
	crash := crashFunc(func(_ context.Context, p crashpoint.Point) error {
		switch p {
		case crashpoint.AfterDirectBlockCutBeforeUpload:
			if failUpload.Load() {
				close(uploadFailed)
				return boom
			}
		case crashpoint.AfterDirectBlockUploadBeforeCommit:
			once.Do(func() {
				close(held)
				<-release
			})
		}
		return nil
	})
	w := e.directWriter(catalog.Main, 1<<20, ingest.Config{OnDurableBatch: h.hook}, ingest.DirectConfig{Crash: crash})
	rng := rand.New(rand.NewPCG(24, 24))
	require.NoError(t, w.AppendBatch(t.Context(), testEvents(rng, blockEvents)))
	<-held
	failUpload.Store(true)
	require.NoError(t, w.AppendBatch(t.Context(), testEvents(rng, blockEvents)))
	<-uploadFailed
	close(release)
	require.ErrorIs(t, w.Flush(t.Context()), boom)
	require.ErrorIs(t, w.Close(), boom)
	require.Equal(t, []string{"hook 1", "commit 1", "done 1: <nil>"}, h.entries())
	require.Equal(t, uint64(blockEvents+1), e.seqKey(catalog.Main))
}

// A seal reads back only the blocks an earlier session committed: this
// session's were indexed as they committed. Either way the generation is
// byte-identical to a local seal of its events.
func TestDirect_SealReadsBackOnlyEarlierSessionsBlocks(t *testing.T) {
	t.Parallel()
	e := newEnv(t, false)
	rng := rand.New(rand.NewPCG(31, 31))
	w := e.directWriter(catalog.Main, 1<<20, ingest.Config{}, ingest.DirectConfig{})
	require.NoError(t, w.AppendBatch(t.Context(), testEvents(rng, 3*blockEvents)))
	require.NoError(t, w.Close())

	e.restart()
	w = e.directWriter(catalog.Main, 1<<20, ingest.Config{}, ingest.DirectConfig{})
	require.NoError(t, w.AppendBatch(t.Context(), testEvents(rng, 2*blockEvents)))
	require.NoError(t, w.Flush(t.Context()))
	before := e.gets.n.Load()
	require.NoError(t, w.ForceRotate(t.Context()))
	require.Equal(t, int64(3+1), e.gets.n.Load()-before, "the earlier session's 3 blocks, and the footer upload's read-back")
	require.NoError(t, w.Close())

	gens := e.generations(e.snapshot())
	require.Len(t, gens, 1)
	got := verify(t, gens[0])
	require.Len(t, got, 5*blockEvents)
	require.Equal(t, localSeal(t, got), gens[0].file())
}

// A seal fails on a committed block whose frame does not index, rather than
// seal a footer that leaves it out.
func TestSegment_SealFailsOnUnindexableBlock(t *testing.T) {
	t.Parallel()
	e := newEnv(t, false)
	seg, err := maintainer.OpenSegment(t.Context(), maintainer.SegmentConfig{Session: e.s, Uploader: e.up, Objects: e.reader})
	require.NoError(t, err)
	c := catalog.BlockCommit{Segment: seg.Index(), Ordinal: 0, ObjectID: 1}
	require.NoError(t, seg.Committed(c, catalog.ObjectRef{ID: 1}, []byte("not a zstd frame")))
	err = seg.Seal(t.Context())
	require.ErrorContains(t, err, "index block 0 (object 1)")
	require.Equal(t, 1, seg.Blocks(), "nothing sealed")
	require.Empty(t, e.generations(e.snapshot()))
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
