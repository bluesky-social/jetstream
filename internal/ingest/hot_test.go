package ingest

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
	"testing/synctest"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/objstore/memblob"
	"github.com/bluesky-social/jetstream/internal/objstore/protocol"
	"github.com/bluesky-social/jetstream/internal/storagefake"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// hotEnv is one leader session over storagefake and an in-memory blob
// store. Build it inside a synctest bubble: the writer's age cuts and the
// fake database both read the bubble's clock.
type hotEnv struct {
	t      *testing.T
	db     *storagefake.DB
	lock   *storagefake.Lease
	s      *catalog.Session
	up     *protocol.Uploader
	reader *protocol.Reader
	meta   metastore.Store
}

func newHotEnv(t *testing.T) *hotEnv {
	t.Helper()
	db := storagefake.New(storagefake.Config{})
	e := &hotEnv{t: t, db: db, lock: db.NewLease(), meta: db.MetaStore(nil)}
	require.NoError(t, e.lock.Acquire(t.Context(), time.Hour))
	e.newSession()
	_, err := e.s.InitNamespace(t.Context(), catalog.Main, nil)
	require.NoError(t, err)
	blob := memblob.New()
	archive := db.Archive().ArchiveID
	e.up, err = protocol.NewUploader(protocol.UploaderConfig{Blob: blob, ArchiveID: archive, GCDelay: time.Hour, OrphanAge: time.Hour})
	require.NoError(t, err)
	e.reader, err = protocol.NewReader(protocol.ReaderConfig{Rows: protocol.DBRows{DB: db}, Blob: blob, ArchiveID: archive})
	require.NoError(t, err)
	t.Cleanup(func() {
		if !t.Failed() {
			require.NoError(t, db.Violation(), "catalog invariants")
		}
	})
	return e
}

func (e *hotEnv) newSession() {
	e.s = catalog.NewSession(catalog.SessionConfig{DB: e.db, Epoch: e.lock.Epoch()})
}

func (e *hotEnv) open(cfg Config) *Writer {
	e.t.Helper()
	if cfg.Hot == nil {
		cfg.Hot = &HotConfig{}
	}
	cfg.Hot.Session = e.s
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	w, err := Open(cfg)
	require.NoError(e.t, err)
	return w
}

// committedNext is seq/next as committed.
func (e *hotEnv) committedNext() uint64 {
	e.t.Helper()
	v, err := e.meta.Get(context.Background(), []byte(catalog.MainSeqKey))
	if errors.Is(err, metastore.ErrNotFound) {
		return 1
	}
	require.NoError(e.t, err)
	return binary.LittleEndian.Uint64(v)
}

func (e *hotEnv) rows() []catalog.HotBatchRow {
	e.t.Helper()
	ctx := context.Background()
	tx, err := e.db.BeginRead(ctx)
	require.NoError(e.t, err)
	defer func() { _ = tx.Close(ctx) }()
	rows, err := tx.HotBatches(ctx, 0)
	require.NoError(e.t, err)
	return rows
}

// frameEvents decodes a hot batch row's events.
func (e *hotEnv) frameEvents(row catalog.HotBatchRow) []segment.Event {
	e.t.Helper()
	frame := row.Frame
	if !row.Inline {
		var err error
		frame, err = e.reader.Get(context.Background(), row.ObjectID)
		require.NoError(e.t, err)
	}
	evs, err := segment.DecodeBlockFrame(frame)
	require.NoError(e.t, err)
	return evs
}

// resume builds the open block that session start would rebuild when every
// hot batch fits in one block: what a maintainer's rebuild hands a writer.
func (e *hotEnv) resume() *OpenBlock {
	e.t.Helper()
	rows := e.rows()
	if len(rows) == 0 {
		return nil
	}
	r := &OpenBlock{OpenedAt: rows[0].CommittedAt}
	for _, row := range rows {
		r.Events = append(r.Events, e.frameEvents(row)...)
		r.Batches = append(r.Batches, HotBatchInfo{FirstSeq: row.FirstSeq, LastSeq: row.LastSeq, ObjectID: row.ObjectID})
	}
	return r
}

// requireTiles checks the committed rows cover [1, next) with no gap or
// overlap (design §9.3 invariants 1, 5, 6).
func requireTiles(t *testing.T, rows []catalog.HotBatchRow, next uint64) {
	t.Helper()
	want := uint64(1)
	for _, r := range rows {
		require.Equal(t, want, r.FirstSeq, "rows must tile the seq space")
		require.Equal(t, r.LastSeq-r.FirstSeq+1, uint64(r.EventCount))
		want = r.LastSeq + 1
	}
	require.Equal(t, next, want, "rows end at seq/next")
}

type recSink struct {
	// fold, if set, stands in for the maintainer: it gets each block as it
	// closes and calls Folded when it likes.
	fold func(ClosedBlock)

	mu      sync.Mutex
	blocks  []ClosedBlock
	rotates int
}

func (s *recSink) BlockClosed(b ClosedBlock) {
	s.mu.Lock()
	s.blocks = append(s.blocks, b)
	s.mu.Unlock()
	if s.fold != nil {
		s.fold(b)
	}
}

func (s *recSink) Rotate(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rotates++
	return nil
}

func (s *recSink) snapshot() ([]ClosedBlock, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ClosedBlock(nil), s.blocks...), s.rotates
}

func testEvent(rng *rand.Rand, producer int) segment.Event {
	payload := make([]byte, rng.IntN(300))
	for i := range payload {
		payload[i] = byte(rng.Uint32())
	}
	return segment.Event{
		WitnessedAt: time.Now().UnixMicro(),
		Kind:        segment.KindCreate,
		DID:         fmt.Sprintf("did:plc:p%d", producer),
		Collection:  "app.bsky.feed.post",
		Rkey:        fmt.Sprintf("r%d", rng.Uint32()),
		Rev:         "3l",
		Payload:     payload,
	}
}

func requireSameEvent(t *testing.T, want, got segment.Event) {
	t.Helper()
	require.Equal(t, want.Seq, got.Seq)
	require.Equal(t, want.WitnessedAt, got.WitnessedAt)
	require.Equal(t, want.Kind, got.Kind)
	require.Equal(t, want.DID, got.DID)
	require.Equal(t, want.Collection, got.Collection)
	require.Equal(t, want.Rkey, got.Rkey)
	require.Equal(t, want.Rev, got.Rev)
	require.Equal(t, len(want.Payload), len(got.Payload))
	if len(want.Payload) > 0 {
		require.Equal(t, want.Payload, got.Payload)
	}
}

// hookRec is a DurableBatchHook that checks the §10.4 contract as it runs:
// nextSeq only grows, the prepare value is the one sampled at freeze, and
// afterCommit sees its commit durable.
type hookRec struct {
	t       *testing.T
	env     *hotEnv
	log     atomic.Pointer[ReadableLog]
	lastApp atomic.Uint64 // last seq OnAppend saw

	mu        sync.Mutex
	last      uint64
	batches   int
	forces    int
	doneErrs  []error
	committed int
}

func (r *hookRec) onAppend(ev *segment.Event) error {
	r.lastApp.Store(ev.Seq)
	return nil
}

func (r *hookRec) prepare() any { return r.lastApp.Load() }

func (r *hookRec) hook(_ context.Context, b metastore.Batch, nextSeq uint64, force bool, pv any) (func(), func(error), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if force {
		r.forces++
		require.GreaterOrEqual(r.t, nextSeq, r.last)
	} else {
		r.batches++
		require.Greater(r.t, nextSeq, r.last)
	}
	r.last = nextSeq
	// The sample was taken when the batch froze, after its last append and
	// before any later one.
	got, ok := pv.(uint64)
	require.True(r.t, ok)
	require.Equal(r.t, nextSeq-1, got, "prepare value belongs to this batch")
	b.Set([]byte("test/hook"), catalog.EncodeSeq(nextSeq))
	if l := r.log.Load(); l != nil {
		require.Equal(r.t, r.env.committedNext(), l.DurableSeq(), "the read log waits for the commit")
	}
	after := func() {
		require.Equal(r.t, nextSeq, r.env.committedNext(), "afterCommit runs after the commit")
		if l := r.log.Load(); l != nil {
			require.Equal(r.t, nextSeq, l.DurableSeq())
		}
		r.mu.Lock()
		r.committed++
		r.mu.Unlock()
	}
	done := func(err error) {
		r.mu.Lock()
		r.doneErrs = append(r.doneErrs, err)
		r.mu.Unlock()
	}
	return after, done, nil
}

func TestHot_Swarm(t *testing.T) {
	t.Parallel()
	iterations := 24
	if !testing.Short() {
		iterations = 400
	}
	for i := range iterations {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				runHotSwarm(t, rand.New(rand.NewPCG(uint64(i), 0x407b)))
			})
		})
	}
}

func runHotSwarm(t *testing.T, rng *rand.Rand) {
	env := newHotEnv(t)
	// The maintainer folds each block after a random delay, so the
	// unfolded cap both blocks and releases.
	var folds sync.WaitGroup
	foldDelay := rand.New(rand.NewPCG(rng.Uint64(), 1))
	var foldMu sync.Mutex
	sink := &recSink{fold: func(b ClosedBlock) {
		foldMu.Lock()
		d := time.Duration(foldDelay.IntN(30)) * time.Millisecond
		foldMu.Unlock()
		folds.Go(func() {
			time.Sleep(d)
			b.Folded()
		})
	}}
	rec := &hookRec{t: t, env: env}
	maxBlock := 1 + rng.IntN(48)
	hc := &HotConfig{
		Sink:           sink,
		BatchMaxEvents: 1 + rng.IntN(32),
		BatchMaxBytes:  int64(200 + rng.IntN(4000)),
		BatchMaxAge:    time.Duration(1+rng.IntN(40)) * time.Millisecond,
		BlockMaxAge:    time.Duration(20+rng.IntN(400)) * time.Millisecond,
		// Admission (design §10.5): a bucket from starved to ample, and
		// caps from a few events to effectively unbounded.
		InlineBytesPerSec:  []int64{1, 2000, 20000, -1, 0}[rng.IntN(5)],
		OverflowMaxEvents:  1 + rng.IntN(64),
		OverflowMaxBytes:   int64(500 + rng.IntN(20000)),
		OverflowMaxAge:     time.Duration(1+rng.IntN(200)) * time.Millisecond,
		BulkChunkMaxEvents: 1 + rng.IntN(16),
		BulkPendingBytes:   int64(1 + rng.IntN(8000)),
		PendingBytes:       int64(1 + rng.IntN(16000)),
		MaxUnfoldedEvents:  int64(maxBlock + rng.IntN(3*maxBlock)),
	}
	pointers := rng.IntN(2) == 0
	if pointers {
		hc.Uploader = env.up
	}
	w := env.open(Config{
		MaxEventsPerBlock:        maxBlock,
		Hot:                      hc,
		OnAppend:                 rec.onAppend,
		OnDurableBatch:           rec.hook,
		DurableBatchPrepareValue: rec.prepare,
	})
	rec.log.Store(w.ReadLog())

	var mu sync.Mutex
	appended := map[uint64]segment.Event{}
	classes := map[uint64]Class{}
	record := func(c Class, evs ...segment.Event) {
		mu.Lock()
		defer mu.Unlock()
		for _, ev := range evs {
			appended[ev.Seq] = ev
			classes[ev.Seq] = c
		}
	}

	var wg sync.WaitGroup
	producers := 1 + rng.IntN(3)
	for p := range producers {
		prng := rand.New(rand.NewPCG(rng.Uint64(), uint64(p)))
		wg.Go(func() {
			class := Class(prng.IntN(2))
			var mine uint64
			for range 5 + prng.IntN(40) {
				if prng.IntN(8) == 0 {
					class = Class(prng.IntN(2))
				}
				ctx := WithClass(t.Context(), class)
				switch prng.IntN(6) {
				case 0, 1:
					ev := testEvent(prng, p)
					require.NoError(t, w.Append(ctx, &ev))
					record(class, ev)
					mine = ev.Seq
				case 2:
					evs := make([]segment.Event, 1+prng.IntN(10))
					for i := range evs {
						evs[i] = testEvent(prng, p)
					}
					require.NoError(t, w.AppendBatch(ctx, evs))
					record(class, evs...)
					mine = evs[len(evs)-1].Seq
				case 3:
					time.Sleep(time.Duration(prng.IntN(30)) * time.Millisecond)
				case 4:
					require.NoError(t, w.Flush(ctx))
					// A Flush ack never precedes the commit of what came
					// before it, and the read log never runs ahead of it.
					require.Greater(t, env.committedNext(), mine)
					require.LessOrEqual(t, w.ReadLog().DurableSeq(), env.committedNext())
					require.Greater(t, w.ReadLog().DurableSeq(), mine)
				case 5:
					if prng.IntN(4) == 0 {
						require.NoError(t, w.DrainDurability(ctx))
						require.Greater(t, env.committedNext(), mine)
					}
				}
			}
		})
	}
	wg.Wait()
	next := w.NextSeq()
	if rng.IntN(4) == 0 {
		require.NoError(t, w.ForceRotate(t.Context()))
	}
	require.NoError(t, w.Close())
	folds.Wait()

	// Close commits everything appended.
	require.Equal(t, next, env.committedNext())
	require.Equal(t, uint64(len(appended))+1, next)
	require.Equal(t, next, w.ReadLog().DurableSeq())
	v, err := env.meta.Get(context.Background(), []byte("test/hook"))
	require.NoError(t, err)
	require.Equal(t, next, binary.LittleEndian.Uint64(v), "hook metadata commits with its batch")

	rows := env.rows()
	requireTiles(t, rows, next)
	byFirst := map[uint64]catalog.HotBatchRow{}
	for _, r := range rows {
		byFirst[r.FirstSeq] = r
		c := classes[r.FirstSeq]
		for s := r.FirstSeq; s <= r.LastSeq; s++ {
			require.Equal(t, c, classes[s], "a batch holds one class")
		}
		switch {
		case !pointers:
			require.True(t, r.Inline, "without an uploader every batch is inline")
		case c == ClassBulk:
			require.False(t, r.Inline, "bulk batches are pointers")
			require.LessOrEqual(t, int(r.EventCount), hc.BulkChunkMaxEvents, "a bulk batch is at most one chunk")
		case r.Inline:
			require.LessOrEqual(t, int(r.EventCount), hc.BatchMaxEvents)
		default:
			require.LessOrEqual(t, int(r.EventCount), max(hc.BatchMaxEvents, hc.OverflowMaxEvents), "a live pointer batch is at most an overflow batch")
		}
		evs := env.frameEvents(r)
		require.Len(t, evs, int(r.EventCount))
		for _, ev := range evs {
			requireSameEvent(t, appended[ev.Seq], ev)
		}
	}

	rec.mu.Lock()
	require.Equal(t, len(rows), rec.batches, "the hook runs once per batch")
	require.Equal(t, rec.batches+rec.forces, rec.committed)
	for _, err := range rec.doneErrs {
		require.NoError(t, err)
	}
	rec.mu.Unlock()

	// Closed blocks arrive in order, cover whole committed batches, and
	// never exceed the block limit. Whatever follows the last one is the
	// open block Close left for the next session.
	blocks, _ := sink.snapshot()
	want := uint64(1)
	for _, b := range blocks {
		require.Equal(t, want, b.FirstSeq)
		require.Len(t, b.Events, int(b.LastSeq-b.FirstSeq+1))
		require.LessOrEqual(t, len(b.Events), maxBlock)
		bs := b.FirstSeq
		for _, bi := range b.Batches {
			require.Equal(t, bs, bi.FirstSeq)
			row, ok := byFirst[bi.FirstSeq]
			require.True(t, ok, "block batch %d has a row", bi.FirstSeq)
			require.Equal(t, row.LastSeq, bi.LastSeq, "a batch never crosses a block boundary")
			require.Equal(t, row.ObjectID, bi.ObjectID)
			bs = bi.LastSeq + 1
		}
		require.Equal(t, b.LastSeq+1, bs)
		for i, ev := range b.Events {
			require.Equal(t, b.FirstSeq+uint64(i), ev.Seq)
			requireSameEvent(t, appended[ev.Seq], ev)
		}
		want = b.LastSeq + 1
	}
	require.Less(t, int(next-want), maxBlock+1, "the open tail is under one block")
}

// Age cuts: a lone event commits after the batch age without a Flush, and
// its block closes after the block age.
func TestHot_AgeCuts(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		env := newHotEnv(t)
		sink := &recSink{}
		w := env.open(Config{Hot: &HotConfig{Sink: sink, BatchMaxAge: 10 * time.Millisecond, BlockMaxAge: 100 * time.Millisecond}})
		defer func() { require.NoError(t, w.Close()) }()

		ev := testEvent(rand.New(rand.NewPCG(1, 1)), 0)
		require.NoError(t, w.Append(t.Context(), &ev))
		synctest.Wait()
		require.Equal(t, uint64(1), env.committedNext(), "nothing commits before the batch age")

		time.Sleep(10 * time.Millisecond)
		synctest.Wait()
		require.Equal(t, uint64(2), env.committedNext())
		blocks, _ := sink.snapshot()
		require.Empty(t, blocks)

		time.Sleep(90 * time.Millisecond)
		synctest.Wait()
		blocks, _ = sink.snapshot()
		require.Len(t, blocks, 1)
		require.Equal(t, uint64(1), blocks[0].LastSeq)

		// The next event opens a new block with fresh deadlines.
		ev2 := testEvent(rand.New(rand.NewPCG(1, 2)), 0)
		require.NoError(t, w.Append(t.Context(), &ev2))
		time.Sleep(99 * time.Millisecond)
		synctest.Wait()
		blocks, _ = sink.snapshot()
		require.Len(t, blocks, 1)
		time.Sleep(time.Millisecond)
		synctest.Wait()
		blocks, _ = sink.snapshot()
		require.Len(t, blocks, 2)
	})
}

// A class change always cuts the batch, and ForceRotate hands the sink the
// open block before asking it to rotate.
func TestHot_ClassChangeAndRotate(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		env := newHotEnv(t)
		sink := &recSink{}
		w := env.open(Config{Hot: &HotConfig{Sink: sink, Uploader: env.up}})
		rng := rand.New(rand.NewPCG(2, 2))
		for _, c := range []Class{ClassLive, ClassLive, ClassBulk, ClassBulk, ClassBulk, ClassLive} {
			ev := testEvent(rng, 0)
			require.NoError(t, w.Append(WithClass(t.Context(), c), &ev))
		}
		require.NoError(t, w.ForceRotate(t.Context()))
		rows := env.rows()
		requireTiles(t, rows, 7)
		require.Len(t, rows, 3)
		require.True(t, rows[0].Inline)
		require.False(t, rows[1].Inline)
		require.True(t, rows[2].Inline)
		blocks, rotates := sink.snapshot()
		require.Len(t, blocks, 1)
		require.Equal(t, 1, rotates)
		require.Equal(t, []Class{ClassLive, ClassBulk, ClassLive},
			[]Class{blocks[0].Batches[0].Class, blocks[0].Batches[1].Class, blocks[0].Batches[2].Class})
		require.NotZero(t, blocks[0].Batches[1].ObjectID)
		require.NoError(t, w.SealActiveAndClose())
		_, rotates = sink.snapshot()
		require.Equal(t, 2, rotates)
	})
}

// A failed or unknown-result commit ends the writer and the session. What
// committed still tiles the seq space, the next session resumes at the
// committed seq/next, and the seqs the failed session assigned past it are
// reassigned (design §10.2).
func TestHot_CommitFailure(t *testing.T) {
	t.Parallel()
	for _, kind := range []storagefake.FaultKind{storagefake.FaultCommitFails, storagefake.FaultCommitLost} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				env := newHotEnv(t)
				fault := &storagefake.Fault{Kind: kind, TxKind: catalog.TxHotBatch, Ordinal: 3}
				env.db.InjectFaults(fault)
				rec := &hookRec{t: t, env: env}
				var failures atomic.Int32
				w := env.open(Config{
					Hot:                      &HotConfig{BatchMaxEvents: 5, OnFailure: func(error) { failures.Add(1) }},
					OnAppend:                 rec.onAppend,
					DurableBatchPrepareValue: rec.prepare,
				})
				rng := rand.New(rand.NewPCG(3, 3))
				for range 30 {
					ev := testEvent(rng, 0)
					if err := w.Append(t.Context(), &ev); err != nil {
						break
					}
				}
				require.Error(t, w.Flush(t.Context()))
				require.True(t, fault.Fired())
				ev := testEvent(rng, 0)
				require.Error(t, w.Append(t.Context(), &ev), "the failure is sticky")
				require.Error(t, w.Close())
				require.Equal(t, int32(1), failures.Load())
				require.Error(t, env.s.Err(), "the session ended")

				want := uint64(11)
				if kind == storagefake.FaultCommitLost {
					want = 16 // applied, result unknown
				}
				require.Equal(t, want, env.committedNext())
				requireTiles(t, env.rows(), want)
				require.Less(t, w.ReadLog().DurableSeq(), uint64(16))

				env.newSession()
				w2 := env.open(Config{Hot: &HotConfig{BatchMaxEvents: 5, Resume: env.resume()}})
				require.Equal(t, want, w2.NextSeq())
				ev2 := testEvent(rng, 0)
				require.NoError(t, w2.Append(t.Context(), &ev2))
				require.Equal(t, want, ev2.Seq)
				require.NoError(t, w2.Close())
				requireTiles(t, env.rows(), want+1)
			})
		})
	}
}

// A hook failure afterDone sees the commit error; a hook error ends the
// writer without a transaction.
func TestHot_HookFailure(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		env := newHotEnv(t)
		boom := errors.New("boom")
		w := env.open(Config{
			Hot: &HotConfig{},
			OnDurableBatch: func(context.Context, metastore.Batch, uint64, bool, any) (func(), func(error), error) {
				return nil, nil, boom
			},
		})
		ev := testEvent(rand.New(rand.NewPCG(4, 4)), 0)
		require.NoError(t, w.Append(t.Context(), &ev))
		require.ErrorIs(t, w.Flush(t.Context()), boom)
		require.ErrorIs(t, env.s.Err(), catalog.ErrSessionEnded)
		require.ErrorIs(t, w.Close(), boom)
		require.Empty(t, env.rows())
	})
}

// A writer resumed over a rebuilt open block keeps filling it, and the
// closed block carries the earlier session's batches and events.
func TestHot_ResumeContinuesOpenBlock(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		env := newHotEnv(t)
		rng := rand.New(rand.NewPCG(7, 7))
		var evs []segment.Event
		appendN := func(w *Writer, ctx context.Context, n int) {
			for range n {
				ev := testEvent(rng, 0)
				require.NoError(t, w.Append(ctx, &ev))
				evs = append(evs, ev)
			}
		}
		w := env.open(Config{MaxEventsPerBlock: 16, Hot: &HotConfig{Uploader: env.up, BatchMaxEvents: 3}})
		appendN(w, t.Context(), 4)
		appendN(w, WithClass(t.Context(), ClassBulk), 2)
		require.NoError(t, w.Close())
		prior := env.rows()
		require.Len(t, prior, 3)
		require.NotZero(t, prior[2].ObjectID, "a pointer batch resumes with its object")

		env.newSession()
		time.Sleep(time.Second)
		m := NewMetrics(prometheus.NewRegistry())
		sink := &recSink{}
		resume := env.resume()
		w2 := env.open(Config{MaxEventsPerBlock: 16, Metrics: m, Hot: &HotConfig{
			Sink: sink, BatchMaxEvents: 3, BlockMaxAge: time.Hour, MaxUnfoldedEvents: 16, Resume: resume,
		}})
		require.Equal(t, 6.0, testutil.ToFloat64(m.HotUnfoldedEvents), "resumed events count as unfolded")
		require.Equal(t, uint64(7), w2.ReadLog().FloorSeq(), "resumed events are already readable from the catalog")
		appendN(w2, t.Context(), 10)
		require.NoError(t, w2.Flush(t.Context()))
		synctest.Wait()
		blocks, _ := sink.snapshot()
		require.Len(t, blocks, 1)
		b := blocks[0]
		require.Equal(t, uint64(1), b.FirstSeq)
		require.Equal(t, uint64(16), b.LastSeq)
		require.Equal(t, resume.OpenedAt, b.OpenedAt, "the block's age runs from the rebuilt open block")
		require.Len(t, b.Events, 16)
		for i := range evs {
			requireSameEvent(t, evs[i], b.Events[i])
		}
		for i, row := range prior {
			require.Equal(t, HotBatchInfo{FirstSeq: row.FirstSeq, LastSeq: row.LastSeq, ObjectID: row.ObjectID}, b.Batches[i])
		}
		require.Equal(t, uint64(7), b.Batches[len(prior)].FirstSeq)
		require.NoError(t, w2.Close())
	})
}

// The resumed block's age cut runs from its OpenedAt, not from the open.
func TestHot_ResumeAgeCut(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		env := newHotEnv(t)
		w := env.open(Config{})
		ev := testEvent(rand.New(rand.NewPCG(8, 8)), 0)
		require.NoError(t, w.Append(t.Context(), &ev))
		require.NoError(t, w.Close())

		env.newSession()
		time.Sleep(60 * time.Millisecond)
		sink := &recSink{}
		w2 := env.open(Config{Hot: &HotConfig{Sink: sink, BlockMaxAge: 100 * time.Millisecond, Resume: env.resume()}})
		defer func() { require.NoError(t, w2.Close()) }()
		time.Sleep(39 * time.Millisecond)
		synctest.Wait()
		blocks, _ := sink.snapshot()
		require.Empty(t, blocks)
		time.Sleep(time.Millisecond)
		synctest.Wait()
		blocks, _ = sink.snapshot()
		require.Len(t, blocks, 1)
		require.Equal(t, uint64(1), blocks[0].LastSeq)
	})
}

// The writer refuses to open over hot batches it was not handed exactly.
func TestHot_ResumeValidation(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		env := newHotEnv(t)
		w := env.open(Config{Hot: &HotConfig{BatchMaxEvents: 2}})
		rng := rand.New(rand.NewPCG(9, 9))
		for range 5 {
			ev := testEvent(rng, 0)
			require.NoError(t, w.Append(t.Context(), &ev))
		}
		require.NoError(t, w.Close())
		env.newSession()
		logger := slog.New(slog.NewTextHandler(io.Discard, nil))

		cases := map[string]func(r *OpenBlock) (*OpenBlock, int){
			"missing":        func(*OpenBlock) (*OpenBlock, int) { return nil, 0 },
			"batch count":    func(r *OpenBlock) (*OpenBlock, int) { r.Batches = r.Batches[1:]; return r, 0 },
			"batch range":    func(r *OpenBlock) (*OpenBlock, int) { r.Batches[1].LastSeq++; return r, 0 },
			"batch object":   func(r *OpenBlock) (*OpenBlock, int) { r.Batches[2].ObjectID = 9; return r, 0 },
			"event count":    func(r *OpenBlock) (*OpenBlock, int) { r.Events = r.Events[:4]; return r, 0 },
			"event seq":      func(r *OpenBlock) (*OpenBlock, int) { r.Events[3].Seq = 9; return r, 0 },
			"full block":     func(r *OpenBlock) (*OpenBlock, int) { return r, 5 },
			"empty":          func(r *OpenBlock) (*OpenBlock, int) { r.Events = nil; return r, 0 },
			"over the block": func(r *OpenBlock) (*OpenBlock, int) { return r, 4 },
		}
		for name, mutate := range cases {
			r, maxEvents := mutate(env.resume())
			_, err := Open(Config{Logger: logger, MaxEventsPerBlock: maxEvents, Hot: &HotConfig{Session: env.s, Resume: r}})
			require.ErrorIs(t, err, ErrInvalidConfig, name)
		}
		w2, err := Open(Config{Logger: logger, Hot: &HotConfig{Session: env.s, Resume: env.resume()}})
		require.NoError(t, err)
		require.NoError(t, w2.Close())
	})
}

func TestHot_ConfigValidation(t *testing.T) {
	t.Parallel()
	s := catalog.NewSession(catalog.SessionConfig{DB: storagefake.New(storagefake.Config{}), Epoch: 1})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for name, cfg := range map[string]Config{
		"no session":  {Hot: &HotConfig{}},
		"seq key":     {Hot: &HotConfig{Session: s}, SeqKey: catalog.BootstrapLiveSeqKey},
		"namespace":   {Hot: &HotConfig{Session: s}, Namespace: catalog.BootstrapLive},
		"lease":       {Hot: &HotConfig{Session: s}, ReserveClientVisibleSeqs: true},
		"async":       {Hot: &HotConfig{Session: s}, AsyncFlushWorkers: 2},
		"negative":    {Hot: &HotConfig{Session: s, BatchMaxAge: -1}},
		"block limit": {Hot: &HotConfig{Session: s}, MaxEventsPerBlock: 1 << 30},
		"overflow":    {Hot: &HotConfig{Session: s, OverflowMaxEvents: -1}},
		"bulk chunk":  {Hot: &HotConfig{Session: s, BulkChunkMaxEvents: -1}},
		"permits":     {Hot: &HotConfig{Session: s, BulkPendingBytes: -1}},
		"pending":     {Hot: &HotConfig{Session: s, PendingBytes: -1}},
		"unfolded":    {Hot: &HotConfig{Session: s, MaxUnfoldedEvents: 3}, MaxEventsPerBlock: 4},
		"default blk": {Hot: &HotConfig{Session: s, MaxUnfoldedEvents: defaultMaxEventsPerBlock - 1}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg.Logger = logger
			_, err := Open(cfg)
			require.ErrorIs(t, err, ErrInvalidConfig)
		})
	}
}
