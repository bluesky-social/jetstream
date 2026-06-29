package client

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// memBuffer is the engine Buffer backed by an in-memory list, for tests.
type memBuffer struct {
	mu     sync.Mutex
	frames []LiveFrame
}

func (b *memBuffer) Append(frames []LiveFrame) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, f := range frames {
		b.frames = append(b.frames, LiveFrame{Seq: f.Seq, Data: append([]byte(nil), f.Data...)})
	}
	return nil
}

func (b *memBuffer) Replay(ctx context.Context, after gt.Option[uint64]) func(yield func(LiveFrame, error) bool) {
	return func(yield func(LiveFrame, error) bool) {
		b.mu.Lock()
		snap := append([]LiveFrame(nil), b.frames...)
		b.mu.Unlock()
		for _, f := range snap {
			if after.HasVal() && f.Seq <= after.Val() {
				continue
			}
			if !yield(f, nil) {
				return
			}
		}
	}
}

func (b *memBuffer) Truncate(uint64) error { return nil }
func (b *memBuffer) Close() error          { return nil }

// engineHarness wires an archive XRPC server (plan + segment/block) and a
// scripted live websocket transport into a configured Engine.
type engineHarness struct {
	as        *archiveServer
	planned   uint64
	planEntry []planSeg // segments to name in the plan, in order
	liveSteps []readStep
}

type planSeg struct {
	name           string
	index          uint32
	minSeq, maxSeq uint64
}

func newEngineHarness(t *testing.T) *engineHarness {
	return &engineHarness{as: newArchiveServer(t)}
}

func (h *engineHarness) installHandlers() {
	h.as.mux.HandleFunc("/xrpc/network.bsky.jetstream.planBackfill", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(h.planJSON()))
	})
}

func (h *engineHarness) planJSON() string {
	var segs []string
	for _, s := range h.planEntry {
		segs = append(segs, fmt.Sprintf(
			`{"name":%q,"index":%d,"checksum":"deadbeefdeadbeef","minSeq":%d,"maxSeq":%d,"mode":"segment"}`,
			s.name, s.index, s.minSeq, s.maxSeq))
	}
	return fmt.Sprintf(`{"plannedThroughSeq":%d,"segments":[%s],"stats":{"segmentsExamined":%d,"segmentsMatched":%d,"blocksMatched":0,"entries":%d}}`,
		h.planned, strings.Join(segs, ","), len(h.planEntry), len(h.planEntry), len(h.planEntry))
}

func (h *engineHarness) cfg() Config {
	conn := &scriptedConn{steps: h.liveSteps}
	dial, _ := scriptedDialer(conn)
	return Config{
		Host:        h.as.srv.URL,
		Request:     PlanRequest{AfterSeq: 0},
		Backfill:    true,
		BatchSize:   1,
		Concurrency: 4,
		Buffer:      &memBuffer{},
		XRPC:        &xrpc.Client{Host: h.as.srv.URL},
		Dial:        dial,
		// Tiny reconnect backoff: the scripted transport EOFs after its frames,
		// and the engine reconnect-loops until the test cancels. A short floor
		// keeps the test fast without real-time waits.
		LiveBackoffMin: time.Millisecond,
	}
}

// runUntilDone drives the engine until done(seenSeqs) is true, then cancels and
// returns all emitted events. A 5s safety net fails the test rather than
// hanging.
func (h *engineHarness) runUntilDone(t *testing.T, cfg Config, what string, done func(seen map[uint64]bool) bool) []Event {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		mu     sync.Mutex
		events []Event
		seen   = map[uint64]bool{}
	)
	eng := NewEngine(cfg)
	finished := make(chan struct{})
	emitBatch := func(batch []Event) bool {
		mu.Lock()
		events = append(events, batch...)
		for _, ev := range batch {
			seen[ev.Seq] = true
		}
		reached := done(seen)
		mu.Unlock()
		if reached {
			cancel()
			return false
		}
		return true
	}
	go func() {
		defer close(finished)
		// Drive the BACKFILL FAST PATH (RunWithBackfill) so the existing
		// backfill/cutover/ordering/suppression tests exercise the same code path
		// production uses: a per-block transform that boxes the block's []Event,
		// and an Emit that unboxes and feeds the same emitBatch. The live path is
		// unchanged. Without this the fast path would ship with only root-level
		// coverage; routing the engine tests through it closes that gap.
		eng.RunWithBackfill(ctx, emitBatch,
			func(error) bool { return true },
			BackfillSink{
				Transform: func(_ int, evs []Event) any {
					if len(evs) == 0 {
						return nil
					}
					return append([]Event(nil), evs...)
				},
				Emit: func(res EntryResult) bool {
					batch, _ := res.Payload.([]Event)
					return emitBatch(batch)
				},
			},
		)
	}()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		cancel()
		<-finished
		t.Fatalf("engine did not reach %s within 5s", what)
	}
	mu.Lock()
	defer mu.Unlock()
	return append([]Event(nil), events...)
}

// runUntil drives the engine until it has emitted wantSeqs distinct seqs.
func (h *engineHarness) runUntil(t *testing.T, cfg Config, wantSeqs int) []Event {
	t.Helper()
	return h.runUntilDone(t, cfg, fmt.Sprintf("%d distinct seqs", wantSeqs),
		func(seen map[uint64]bool) bool { return len(seen) >= wantSeqs })
}

// runUntilSeq drives the engine until the given seq has been emitted.
func (h *engineHarness) runUntilSeq(t *testing.T, cfg Config, seq uint64) []Event {
	t.Helper()
	return h.runUntilDone(t, cfg, fmt.Sprintf("seq %d", seq),
		func(seen map[uint64]bool) bool { return seen[seq] })
}

// TestEngineActiveSegmentGap is the headline correctness guard (#87): records
// in (plannedThroughSeq, M] live ONLY in the active, unsealed segment and are
// NOT downloadable from the archive. They must be delivered by the live tail.
// Starting the live tail at M (instead of plannedThroughSeq) would drop them.
func TestEngineActiveSegmentGap(t *testing.T) {
	t.Parallel()
	h := newEngineHarness(t)

	// Sealed archive: seqs 1..10 in one segment. plannedThroughSeq = 10.
	var sealed []segment.Event
	for i := uint64(1); i <= 10; i++ {
		sealed = append(sealed, makeCreate(t, i, "did:plc:a", "app.bsky.feed.post", "r"+itoaU(i)))
	}
	h.as.addSegment(t, "seg_0000000000.jss", sealed)
	h.planned = 10
	h.planEntry = []planSeg{{name: "seg_0000000000.jss", index: 0, minSeq: 1, maxSeq: 10}}

	// The active segment holds seqs 11..15 above the sealed tip; the live tail
	// must deliver 11..15.

	// Live tail (from plannedThroughSeq-margin) delivers the active-segment
	// records 11..15 plus steady-state 16..18.
	for i := uint64(11); i <= 18; i++ {
		h.liveSteps = append(h.liveSteps, readStep{
			data: liveCommitFrame(t, i, "did:plc:a", "create", "app.bsky.feed.post", "r"+itoaU(i), true),
		})
	}
	h.installHandlers()

	events := h.runUntil(t, h.cfg(), 18)

	// Every seq 1..18 must appear, with NONE of the active-segment gap (11..15)
	// dropped. Dedup means each seq appears exactly once.
	got := uniqueSeqs(events)
	var want []uint64
	for i := uint64(1); i <= 18; i++ {
		want = append(want, i)
	}
	require.Equal(t, want, got, "no record gap across sealed->active->live; gap (10,15] must be live-delivered")
}

// TestEngineEmptyArchiveCutoverDeliversFirstEvent is a regression guard for the
// empty-archive first-event drop. A freshly bootstrapped server has NO sealed
// segments: planBackfill returns an empty plan with plannedThroughSeq=0, so the
// backfill downloads nothing and the live tail covers the WHOLE stream from the
// first-ever event (seq 1). It must be delivered exactly once — not swallowed by
// a dedup floor seeded as if the (empty) backfill had already covered it.
func TestEngineEmptyArchiveCutoverDeliversFirstEvent(t *testing.T) {
	t.Parallel()
	h := newEngineHarness(t)

	// Empty sealed archive: no segments, no plan entries, plannedThroughSeq=0.
	// This is the freshly-bootstrapped state.
	h.planned = 0
	h.planEntry = nil

	// The live tail carries the entire stream from the first-ever event (seq 1).
	for i := uint64(1); i <= 4; i++ {
		h.liveSteps = append(h.liveSteps, readStep{
			data: liveCommitFrame(t, i, "did:plc:a", "create", "app.bsky.feed.post", "r"+itoaU(i), true),
		})
	}
	h.installHandlers()

	events := h.runUntilSeq(t, h.cfg(), 4)

	// Assert on the RAW (non-deduped) seq list so the test fails on BOTH a
	// dropped first event (the bug) AND a double-delivered one (the buffer drain
	// and the post-flip forward path overlapping): the empty-archive cutover
	// must deliver every event exactly once, in order.
	require.Equal(t, []uint64{1, 2, 3, 4}, seqs(events),
		"empty-archive cutover must deliver the first-ever live event (seq 1) exactly once")
}

// TestEngineBackfillOnly covers the one-time-dump path: with BackfillOnly the
// engine plans, downloads + emits the sealed archive, and returns WITHOUT ever
// dialing the live websocket. The run self-terminates when the download
// completes (the test never cancels ctx), and the live dial count stays zero —
// the property that distinguishes a dump from backfill+cutover.
func TestEngineBackfillOnly(t *testing.T) {
	t.Parallel()
	h := newEngineHarness(t)

	var sealed []segment.Event
	for i := uint64(1); i <= 6; i++ {
		sealed = append(sealed, makeCreate(t, i, "did:plc:a", "app.bsky.feed.post", "r"+itoaU(i)))
	}
	h.as.addSegment(t, "seg_0000000000.jss", sealed)
	h.planned = 6
	h.planEntry = []planSeg{{name: "seg_0000000000.jss", index: 0, minSeq: 1, maxSeq: 6}}
	// Script a live frame too: if the engine wrongly started the live tail it
	// would dial and deliver seq 7, which the assertions below would catch.
	h.liveSteps = append(h.liveSteps, readStep{
		data: liveCommitFrame(t, 7, "did:plc:a", "create", "app.bsky.feed.post", "r7", true),
	})
	h.installHandlers()

	conn := &scriptedConn{steps: h.liveSteps}
	dial, dials := scriptedDialer(conn)
	cfg := h.cfg()
	cfg.BackfillOnly = true
	cfg.Dial = dial

	// Background ctx: NOT cancelled by the test. A backfill-only run must end on
	// its own when the download finishes, not block on a live tail.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var (
		mu     sync.Mutex
		events []Event
	)
	eng := NewEngine(cfg)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		eng.Run(ctx,
			func(batch []Event) bool {
				mu.Lock()
				events = append(events, batch...)
				mu.Unlock()
				return true
			},
			func(error) bool { return true },
		)
	}()

	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("backfill-only engine did not return on its own (blocked on live tail?)")
	}

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []uint64{1, 2, 3, 4, 5, 6}, uniqueSeqs(events), "all sealed seqs emitted, no live seq 7")
	require.Equal(t, 0, *dials, "backfill-only must never dial the live websocket")
}

// TestEngineBackfillThenLiveOrdering asserts backfill rows precede live rows
// and the whole stream is in seq order with the overlap deduped.
func TestEngineBackfillThenLiveOrdering(t *testing.T) {
	t.Parallel()
	h := newEngineHarness(t)
	var sealed []segment.Event
	for i := uint64(1); i <= 4; i++ {
		sealed = append(sealed, makeCreate(t, i, "did:plc:a", "app.bsky.feed.post", "r"+itoaU(i)))
	}
	h.as.addSegment(t, "seg_0000000000.jss", sealed)
	h.planned = 4
	h.planEntry = []planSeg{{name: "seg_0000000000.jss", index: 0, minSeq: 1, maxSeq: 4}}

	// Live re-delivers 3,4 (rewind-margin overlap) then 5,6.
	for i := uint64(3); i <= 6; i++ {
		h.liveSteps = append(h.liveSteps, readStep{
			data: liveCommitFrame(t, i, "did:plc:a", "create", "app.bsky.feed.post", "r"+itoaU(i), true),
		})
	}
	h.installHandlers()

	events := h.runUntil(t, h.cfg(), 6)
	require.Equal(t, []uint64{1, 2, 3, 4, 5, 6}, uniqueSeqs(events))
}

// TestEngineFastPathBlockAlignedBatches verifies the fast-path batch-shape
// contract (#142): when the production-style transform chunks each decoded block
// by BatchSize, batches are block-aligned — every batch is non-empty and
// <= BatchSize, at most one undersized batch per block, and the per-batch
// LastCursor (max seq) is monotonic non-decreasing across the backfill. This
// mirrors what realEngine.run does, asserted at the batch boundary (the engine
// harness flattens batches and would not catch a shape regression).
func TestEngineFastPathBlockAlignedBatches(t *testing.T) {
	t.Parallel()
	h := newEngineHarness(t)
	// 3 segments × 6 events (3 blocks of 2 each at the archive's MaxEventsPerBlock=2).
	const nSeg = 3
	for s := range nSeg {
		var sealed []segment.Event
		for i := range 6 {
			seq := uint64(s*100 + i + 1)
			sealed = append(sealed, makeCreate(t, seq, "did:plc:a", "app.bsky.feed.post", "r"+itoaU(seq)))
		}
		h.as.addSegment(t, segName(s), sealed)
		h.planEntry = append(h.planEntry, planSeg{name: segName(s), index: uint32(s), minSeq: uint64(s*100 + 1), maxSeq: uint64(s*100 + 6)})
	}
	h.planned = 0 // no live cutover needed for this shape test
	h.installHandlers()

	const batchSize = 4
	cfg := h.cfg()
	cfg.BatchSize = batchSize
	cfg.BackfillOnly = true // pure backfill: just exercise the batch shaping

	eng := NewEngine(cfg)
	var batchSizes []int
	var lastCursors []uint64
	var lastCursor uint64
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Production-style transform: chunk each block's events by BatchSize into
	// "batches", carried as [][]Event so the test sees batch boundaries.
	eng.RunWithBackfill(ctx,
		func([]Event) bool { return true }, // legacy emitBatch unused on the fast path
		func(error) bool { return true },
		BackfillSink{
			Transform: func(_ int, evs []Event) any {
				if len(evs) == 0 {
					return nil
				}
				var batches [][]Event
				for i := 0; i < len(evs); i += batchSize {
					end := min(i+batchSize, len(evs))
					batches = append(batches, append([]Event(nil), evs[i:end]...))
				}
				return batches
			},
			Emit: func(res EntryResult) bool {
				batches, _ := res.Payload.([][]Event)
				for _, b := range batches {
					require.NotEmpty(t, b, "no empty batch may be emitted")
					require.LessOrEqual(t, len(b), batchSize, "batch must not exceed BatchSize")
					batchSizes = append(batchSizes, len(b))
					var mx uint64
					for _, ev := range b {
						if ev.Seq > mx {
							mx = ev.Seq
						}
					}
					require.GreaterOrEqual(t, mx, lastCursor, "LastCursor must be monotonic non-decreasing")
					lastCursor = mx
					lastCursors = append(lastCursors, mx)
				}
				return true
			},
		},
	)

	// 18 events total. Each block is 2 events (< batchSize 4), so block-alignment
	// yields one 2-event batch per block = 9 batches, all size 2.
	require.Equal(t, nSeg*6, sumInts(batchSizes), "every event delivered exactly once")
	for _, sz := range batchSizes {
		require.LessOrEqual(t, sz, batchSize)
	}
	require.True(t, isNonDecreasing(lastCursors), "per-batch LastCursors must be monotonic: %v", lastCursors)
}

func sumInts(xs []int) int {
	s := 0
	for _, x := range xs {
		s += x
	}
	return s
}

func isNonDecreasing(xs []uint64) bool {
	for i := 1; i < len(xs); i++ {
		if xs[i] < xs[i-1] {
			return false
		}
	}
	return true
}

// TestEngineBackfillCreateThenLiveDeleteConverges verifies the
// eventually-consistent model (design §5.1): the backfill emits a historical
// create with NO suppression (even though it is later deleted), and the delete
// arrives as its own row on the live tail so a folding consumer converges. We
// no longer hide the create at delivery time.
func TestEngineBackfillCreateThenLiveDeleteConverges(t *testing.T) {
	t.Parallel()
	h := newEngineHarness(t)

	// Sealed creates of r1 and r2.
	h.as.addSegment(t, "seg_0000000000.jss", []segment.Event{
		makeCreate(t, 2, "did:plc:a", "app.bsky.feed.post", "r1"),
		makeCreate(t, 3, "did:plc:a", "app.bsky.feed.post", "r2"),
	})
	h.planned = 3
	h.planEntry = []planSeg{{name: "seg_0000000000.jss", index: 0, minSeq: 2, maxSeq: 3}}

	// A live delete of r1 arrives after the backfill. It flows through as its own
	// row rather than retroactively hiding the create.
	h.liveSteps = append(h.liveSteps, readStep{
		data: liveCommitFrame(t, 4, "did:plc:a", "delete", "app.bsky.feed.post", "r1", false),
	})
	h.installHandlers()

	// Drive until the live delete (seq 4) is emitted.
	events := h.runUntilSeq(t, h.cfg(), 4)

	// The backfill create of r1 IS emitted (no suppression)...
	var sawCreateR1, sawDeleteR1 bool
	for _, ev := range events {
		if ev.Kind != KindCommit || ev.Commit.Rkey != "r1" {
			continue
		}
		switch ev.Commit.Operation {
		case OpCreate:
			sawCreateR1 = true
		case OpDelete:
			sawDeleteR1 = true
		}
	}
	require.True(t, sawCreateR1, "backfill must emit the create of r1 (no suppression)")
	// ...and the delete arrives so a folding consumer converges.
	require.True(t, sawDeleteR1, "live delete of r1 must be delivered")
	require.True(t, hasRkey(events, "r2"), "r2 must be emitted")
}

// TestEngineLiveOnly covers the no-backfill path: Subscribe with no seq bound
// tails live directly, with the max-latency flusher delivering low-volume
// batches promptly.
func TestEngineLiveOnly(t *testing.T) {
	t.Parallel()
	conn := &scriptedConn{steps: []readStep{
		{data: liveCommitFrame(t, 1, "did:plc:a", "create", "app.bsky.feed.post", "r1", true)},
		{data: liveCommitFrame(t, 2, "did:plc:a", "create", "app.bsky.feed.post", "r2", true)},
	}}
	dial, _ := scriptedDialer(conn)
	cfg := Config{
		Host:           "https://h",
		Backfill:       false,
		BatchSize:      64, // larger than the stream: only the flusher delivers
		MaxBatchDelay:  time.Millisecond,
		LiveBackoffMin: time.Millisecond,
		Dial:           dial,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var (
		mu     sync.Mutex
		events []Event
	)
	eng := NewEngine(cfg)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		eng.Run(ctx,
			func(batch []Event) bool {
				mu.Lock()
				events = append(events, batch...)
				done := len(events) >= 2
				mu.Unlock()
				if done {
					cancel()
					return false
				}
				return true
			},
			func(error) bool { return true },
		)
	}()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		cancel()
		<-finished
		t.Fatal("live-only engine did not deliver within 5s")
	}
	require.Equal(t, []uint64{1, 2}, uniqueSeqs(events))
}

// TestEngineLiveOnlyAppliesCollectionFilter is a regression guard: in the
// pure live-only path (no backfill bound) the client must apply the caller's
// collection filter, exactly as the backfill+cutover path does. The server
// streams ALL collections to /subscribe-v2 (the client does not forward
// wantedCollections on the wire), so the engine itself must drop events whose
// collection the caller did not ask for. Before the fix, runLiveOnly forwarded
// every event straight to the batcher, so a --collection=app.bsky.feed.post
// tail leaked likes, reposts, and unrelated lexicons.
func TestEngineLiveOnlyAppliesCollectionFilter(t *testing.T) {
	t.Parallel()
	// Mixed live stream: only the two posts must survive the filter.
	conn := &scriptedConn{steps: []readStep{
		{data: liveCommitFrame(t, 1, "did:plc:a", "create", "app.bsky.feed.post", "r1", true)},
		{data: liveCommitFrame(t, 2, "did:plc:a", "create", "app.bsky.feed.like", "r2", true)},
		{data: liveCommitFrame(t, 3, "did:plc:a", "create", "app.bsky.feed.repost", "r3", true)},
		{data: liveCommitFrame(t, 4, "did:plc:a", "create", "place.stream.livestream", "r4", true)},
		{data: liveCommitFrame(t, 5, "did:plc:a", "create", "app.bsky.feed.post", "r5", true)},
	}}
	dial, _ := scriptedDialer(conn)
	cfg := Config{
		Host:           "https://h",
		Request:        PlanRequest{Collections: []string{"app.bsky.feed.post"}},
		Backfill:       false,
		BatchSize:      1,
		MaxBatchDelay:  time.Millisecond,
		LiveBackoffMin: time.Millisecond,
		Dial:           dial,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var (
		mu     sync.Mutex
		events []Event
	)
	eng := NewEngine(cfg)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		eng.Run(ctx,
			func(batch []Event) bool {
				mu.Lock()
				events = append(events, batch...)
				// Two posts (seq 1 and 5) are expected; stop once seq 5 lands.
				done := false
				for _, ev := range events {
					if ev.Seq == 5 {
						done = true
					}
				}
				mu.Unlock()
				if done {
					cancel()
					return false
				}
				return true
			},
			func(error) bool { return true },
		)
	}()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		cancel()
		<-finished
		t.Fatal("live-only filtered engine did not deliver within 5s")
	}

	mu.Lock()
	defer mu.Unlock()
	for _, ev := range events {
		require.Equal(t, KindCommit, ev.Kind)
		require.Equal(t, "app.bsky.feed.post", ev.Commit.Collection,
			"live-only path must drop non-matching collections; leaked seq=%d collection=%s",
			ev.Seq, ev.Commit.Collection)
	}
	require.Equal(t, []uint64{1, 5}, uniqueSeqs(events),
		"only the app.bsky.feed.post events (seq 1 and 5) must be delivered")
}

// TestEngineLiveOnlyCollectionFilterDeliversAccountIdentity guards the uniform
// delivery contract: with a collection filter set, the live-only path still
// surfaces #account and #identity events (they carry no collection and bypass
// the collection filter — the consumer's only signal to purge a dead account).
// Only non-matching commits are dropped.
func TestEngineLiveOnlyCollectionFilterDeliversAccountIdentity(t *testing.T) {
	t.Parallel()
	conn := &scriptedConn{steps: []readStep{
		{data: liveCommitFrame(t, 1, "did:plc:a", "create", "app.bsky.feed.post", "r1", true)},
		{data: liveIdentityFrame(2, "did:plc:a", "alice.test")},
		{data: liveAccountFrame(3, "did:plc:a", true, "")},
		{data: liveCommitFrame(t, 4, "did:plc:a", "create", "app.bsky.feed.like", "r4", true)},
		{data: liveCommitFrame(t, 5, "did:plc:a", "create", "app.bsky.feed.post", "r5", true)},
	}}
	dial, _ := scriptedDialer(conn)
	cfg := Config{
		Host:           "https://h",
		Request:        PlanRequest{Collections: []string{"app.bsky.feed.post"}},
		Backfill:       false,
		BatchSize:      1,
		MaxBatchDelay:  time.Millisecond,
		LiveBackoffMin: time.Millisecond,
		Dial:           dial,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var (
		mu     sync.Mutex
		events []Event
	)
	eng := NewEngine(cfg)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		eng.Run(ctx,
			func(batch []Event) bool {
				mu.Lock()
				events = append(events, batch...)
				done := false
				for _, ev := range events {
					if ev.Seq == 5 {
						done = true
					}
				}
				mu.Unlock()
				if done {
					cancel()
					return false
				}
				return true
			},
			func(error) bool { return true },
		)
	}()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		cancel()
		<-finished
		t.Fatal("filtered live-only engine did not deliver within 5s")
	}

	mu.Lock()
	defer mu.Unlock()
	for _, ev := range events {
		// Any commit that survives must match the collection filter; the
		// non-matching like commit (seq 4) must have been dropped.
		if ev.Kind == KindCommit {
			require.Equal(t, "app.bsky.feed.post", ev.Commit.Collection,
				"non-matching commit leaked under a collection filter; seq=%d", ev.Seq)
		}
	}
	require.Equal(t, []uint64{1, 2, 3, 5}, uniqueSeqs(events),
		"matching commits (seq 1, 5) plus identity (seq 2) and account (seq 3) survive; the like commit (seq 4) is dropped")
}

// TestEngineLiveOnlyNoFilterDeliversAccountIdentity guards the other side of
// #142: with no collection filter, account and identity events ARE delivered.
func TestEngineLiveOnlyNoFilterDeliversAccountIdentity(t *testing.T) {
	t.Parallel()
	conn := &scriptedConn{steps: []readStep{
		{data: liveCommitFrame(t, 1, "did:plc:a", "create", "app.bsky.feed.post", "r1", true)},
		{data: liveIdentityFrame(2, "did:plc:a", "alice.test")},
		{data: liveAccountFrame(3, "did:plc:a", true, "")},
	}}
	dial, _ := scriptedDialer(conn)
	cfg := Config{
		Host:           "https://h",
		Request:        PlanRequest{}, // no filters
		Backfill:       false,
		BatchSize:      1,
		MaxBatchDelay:  time.Millisecond,
		LiveBackoffMin: time.Millisecond,
		Dial:           dial,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var (
		mu     sync.Mutex
		kinds  []Kind
		events []Event
	)
	eng := NewEngine(cfg)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		eng.Run(ctx,
			func(batch []Event) bool {
				mu.Lock()
				events = append(events, batch...)
				for _, ev := range batch {
					kinds = append(kinds, ev.Kind)
				}
				done := false
				for _, ev := range events {
					if ev.Seq == 3 {
						done = true
					}
				}
				mu.Unlock()
				if done {
					cancel()
					return false
				}
				return true
			},
			func(error) bool { return true },
		)
	}()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		cancel()
		<-finished
		t.Fatal("unfiltered live-only engine did not deliver within 5s")
	}

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []uint64{1, 2, 3}, uniqueSeqs(events),
		"all events flow when no collection filter is set")
	require.Contains(t, kinds, KindIdentity, "identity must be delivered with no filter")
	require.Contains(t, kinds, KindAccount, "account must be delivered with no filter")
}

// TestEngineLiveOnlyBreakOnQuietTail is a regression guard: a consumer that
// breaks the iterator after one event, on a tail that then goes quiet (no more
// frames), must let Run return promptly. The stop is propagated by the batch
// flusher's yield, not by a subsequent live event (there are none). Without
// the onStop->cancel wiring the engine blocked until ctx cancel.
func TestEngineLiveOnlyBreakOnQuietTail(t *testing.T) {
	t.Parallel()
	// One frame, then the scripted conn EOFs and reconnect-loops forever (a
	// quiet tail). The consumer takes the first event and stops.
	conn := &scriptedConn{steps: []readStep{
		{data: liveCommitFrame(t, 1, "did:plc:a", "create", "app.bsky.feed.post", "r1", true)},
	}}
	dial, _ := scriptedDialer(conn)
	cfg := Config{
		Host:           "https://h",
		Backfill:       false,
		BatchSize:      1,
		MaxBatchDelay:  time.Millisecond,
		LiveBackoffMin: time.Millisecond,
		Dial:           dial,
	}

	eng := NewEngine(cfg)
	done := make(chan struct{})
	go func() {
		defer close(done)
		eng.Run(context.Background(), // NOT cancelled by the test: the engine must self-unwind
			func([]Event) bool { return false }, // stop after the first batch
			func(error) bool { return true },
		)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("engine did not unwind after consumer stop on a quiet tail")
	}
}

// failOnNthAppendBuffer wraps memBuffer and fails the Nth Append call, modeling
// a durable-buffer write failure during the cutover. The frame whose append
// fails (and every later frame) is therefore NOT persisted.
type failOnNthAppendBuffer struct {
	memBuffer
	failAt  int // 1-based append index to fail
	appends atomic.Int64
}

func (b *failOnNthAppendBuffer) Append(frames []LiveFrame) error {
	if int(b.appends.Add(1)) == b.failAt {
		return fmt.Errorf("simulated buffer append failure")
	}
	return b.memBuffer.Append(frames)
}

// TestEngineCutoverAppendFailureSurfaces is the B3 regression guard: a live
// buffer Append failure during the buffering phase must be surfaced as an
// error, not silently swallowed. Before the fix, onLive returned false, the
// live consumer exited cleanly (errEmitStop -> nil), and flipAndDrain replayed
// a truncated buffer — silently dropping every un-persisted live frame and
// exiting "successfully". That is the silent-data-loss class we forbid.
func TestEngineCutoverAppendFailureSurfaces(t *testing.T) {
	t.Parallel()
	h := newEngineHarness(t)

	// Small sealed archive (seqs 1..2), plannedThroughSeq = 2.
	h.as.addSegment(t, "seg_0000000000.jss", []segment.Event{
		makeCreate(t, 1, "did:plc:a", "app.bsky.feed.post", "r1"),
		makeCreate(t, 2, "did:plc:a", "app.bsky.feed.post", "r2"),
	})
	h.planned = 2
	h.planEntry = []planSeg{{name: "seg_0000000000.jss", index: 0, minSeq: 1, maxSeq: 2}}

	// Live frames above the sealed tip that the cutover must not lose.
	for i := uint64(3); i <= 5; i++ {
		h.liveSteps = append(h.liveSteps, readStep{
			data: liveCommitFrame(t, i, "did:plc:a", "create", "app.bsky.feed.post", "r"+itoaU(i), true),
		})
	}
	h.installHandlers()

	// Gate getSegment until at least one live frame has been appended, so the
	// failing append happens during the buffering phase (before flipAndDrain).
	gate := make(chan struct{})
	h.as.mu.Lock()
	h.as.segGate = gate
	h.as.mu.Unlock()

	buf := &failOnNthAppendBuffer{failAt: 1}
	cfg := h.cfg()
	cfg.Buffer = buf

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var (
		mu      sync.Mutex
		gotErr  error
		opened  = make(chan struct{})
		onceOpn sync.Once
	)
	eng := NewEngine(cfg)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		eng.Run(ctx,
			func([]Event) bool { return true },
			func(err error) bool {
				mu.Lock()
				if gotErr == nil {
					gotErr = err
				}
				mu.Unlock()
				return true
			},
		)
	}()

	// Release the backfill once the first append has been attempted (and failed).
	go func() {
		for {
			if buf.appends.Load() >= 1 {
				onceOpn.Do(func() { close(opened) })
				return
			}
			select {
			case <-finished:
				onceOpn.Do(func() { close(opened) })
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()
	select {
	case <-opened:
	case <-time.After(5 * time.Second):
		t.Fatal("no live append attempted within 5s")
	}
	close(gate)

	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		cancel()
		<-finished
		t.Fatal("engine did not return within 5s after cutover append failure")
	}

	mu.Lock()
	defer mu.Unlock()
	require.Error(t, gotErr, "a cutover buffer append failure must surface as an error, not be silently swallowed")
	require.Contains(t, gotErr.Error(), "append live buffer")
}

// replayErrBuffer appends normally but always fails Replay, modeling a durable
// buffer that cannot be read back at cutover time.
type replayErrBuffer struct {
	memBuffer
}

func (b *replayErrBuffer) Replay(context.Context, gt.Option[uint64]) func(yield func(LiveFrame, error) bool) {
	return func(yield func(LiveFrame, error) bool) {
		yield(LiveFrame{}, fmt.Errorf("simulated replay failure"))
	}
}

// TestEngineCutoverReplayErrorUnwinds is the B2 regression guard: when
// flipAndDrain returns a replay error, the engine must cancel the live consumer
// and return, rather than fall into liveWG.Wait() and block until the parent
// ctx is cancelled. The tail is quiet after the buffered frames, so the only
// thing that can unwind the live goroutine is an explicit stopLive — exactly
// what the buggy fall-through omitted. The test does NOT cancel ctx; the engine
// must self-unwind.
func TestEngineCutoverReplayErrorUnwinds(t *testing.T) {
	t.Parallel()
	h := newEngineHarness(t)

	h.as.addSegment(t, "seg_0000000000.jss", []segment.Event{
		makeCreate(t, 1, "did:plc:a", "app.bsky.feed.post", "r1"),
		makeCreate(t, 2, "did:plc:a", "app.bsky.feed.post", "r2"),
	})
	h.planned = 2
	h.planEntry = []planSeg{{name: "seg_0000000000.jss", index: 0, minSeq: 1, maxSeq: 2}}

	// One live frame above the tip so the buffering phase has something to
	// append; after it the scripted conn EOFs and the tail goes quiet.
	h.liveSteps = append(h.liveSteps, readStep{
		data: liveCommitFrame(t, 3, "did:plc:a", "create", "app.bsky.feed.post", "r3", true),
	})
	h.installHandlers()

	cfg := h.cfg()
	cfg.Buffer = &replayErrBuffer{}

	var sawErr atomic.Bool
	eng := NewEngine(cfg)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		// Background ctx: NOT cancelled by the test. The engine must unwind on
		// its own via the replay-error branch's stopLive.
		eng.Run(context.Background(),
			func([]Event) bool { return true },
			func(err error) bool {
				if err != nil {
					sawErr.Store(true)
				}
				return true
			},
		)
	}()

	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("engine did not unwind after a cutover replay error (deadlock on liveWG.Wait)")
	}
	require.True(t, sawErr.Load(), "the replay error must be surfaced before unwinding")
}

// TestEngineLiveOnlyErrorRejectStopsBatching is the B4 regression guard: in the
// live-only path, when the consumer rejects an emitted error (emitErr returns
// false), batching must stop and no batch may be delivered afterward. Before
// the fix, the error was emitted directly (bypassing the batcher), so a buffered
// event could still be flushed to emitBatch after the consumer had already
// stopped — violating the "yield never called after stop" contract.
func TestEngineLiveOnlyErrorRejectStopsBatching(t *testing.T) {
	t.Parallel()

	// First a normal event (buffered, since BatchSize is large), then an error
	// frame. The consumer rejects the error; the final flush must NOT deliver
	// the buffered event after the rejection.
	conn := &scriptedConn{steps: []readStep{
		{data: liveCommitFrame(t, 1, "did:plc:a", "create", "app.bsky.feed.post", "r1", true)},
		{data: []byte(`{"seq":2,"kind":"commit","commit":{"operation":"create","collection":"c","rkey":"r2"}}`)}, // malformed: missing record_cbor -> error frame
	}}
	dial, _ := scriptedDialer(conn)
	cfg := Config{
		Host:           "https://h",
		Backfill:       false,
		BatchSize:      64, // large: events stay buffered until flush/stop
		MaxBatchDelay:  time.Hour,
		LiveBackoffMin: time.Millisecond,
		Dial:           dial,
	}

	var (
		mu          sync.Mutex
		batchAfter  bool
		errRejected bool
	)
	eng := NewEngine(cfg)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		eng.Run(context.Background(),
			func([]Event) bool {
				mu.Lock()
				defer mu.Unlock()
				if errRejected {
					batchAfter = true // a batch emitted AFTER the error was rejected
				}
				return true
			},
			func(error) bool {
				mu.Lock()
				errRejected = true
				mu.Unlock()
				return false // reject: stop the stream on the first error
			},
		)
	}()

	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("engine did not unwind after the consumer rejected an error")
	}
	mu.Lock()
	defer mu.Unlock()
	require.True(t, errRejected, "the error must have been surfaced to the consumer")
	require.False(t, batchAfter, "no batch may be emitted after the consumer rejected an error")
}

func uniqueSeqs(events []Event) []uint64 {
	seen := map[uint64]bool{}
	var out []uint64
	for _, e := range events {
		if seen[e.Seq] {
			continue
		}
		seen[e.Seq] = true
		out = append(out, e.Seq)
	}
	return out
}

func hasRkey(events []Event, rkey string) bool {
	for _, e := range events {
		if e.Kind == KindCommit && e.Commit.Rkey == rkey {
			return true
		}
	}
	return false
}

func itoaU(n uint64) string {
	return strconv.FormatUint(n, 10)
}
