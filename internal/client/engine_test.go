package client

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/overlay"
	"github.com/bluesky-social/jetstream/internal/tombstone"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/xrpc"
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

func (b *memBuffer) Replay(ctx context.Context, from uint64) func(yield func(LiveFrame, error) bool) {
	return func(yield func(LiveFrame, error) bool) {
		b.mu.Lock()
		snap := append([]LiveFrame(nil), b.frames...)
		b.mu.Unlock()
		for _, f := range snap {
			if f.Seq <= from {
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

// engineHarness wires an archive XRPC server (overlay + plan + segment/block)
// and a scripted live websocket transport into a configured Engine.
type engineHarness struct {
	as        *archiveServer
	overlayW  uint64
	overlayM  uint64
	overlay   tombstone.Snapshot
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
	return &engineHarness{as: newArchiveServer(t), overlay: emptySnapshot()}
}

func (h *engineHarness) installHandlers() {
	h.as.mux.HandleFunc("/xrpc/network.bsky.jetstream.getTombstones", func(w http.ResponseWriter, r *http.Request) {
		blob := overlay.Encode(h.overlay, h.overlayW, h.overlayM)
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(blob)
	})
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
	go func() {
		defer close(finished)
		eng.Run(ctx,
			func(batch []Event) bool {
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
			},
			func(error) bool { return true },
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

	// Overlay M = 15: the tombstone horizon sits above the sealed tip because
	// the active segment holds seqs 11..15. The live tail must deliver 11..15.
	h.overlayW = 0
	h.overlayM = 15

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
	h.overlayM = 4

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

// TestEngineLiveDeleteSuppressesBackfillCreate verifies the eager combined-set
// suppression across the cutover: a delete arriving on the live tail during
// backfill must suppress a historical create downloaded afterwards.
func TestEngineLiveDeleteSuppressesBackfillCreate(t *testing.T) {
	t.Parallel()
	h := newEngineHarness(t)

	// Sealed create of (did:plc:a, post, r1) at seq 2.
	h.as.addSegment(t, "seg_0000000000.jss", []segment.Event{
		makeCreate(t, 2, "did:plc:a", "app.bsky.feed.post", "r1"),
		makeCreate(t, 3, "did:plc:a", "app.bsky.feed.post", "r2"),
	})
	h.planned = 3
	h.planEntry = []planSeg{{name: "seg_0000000000.jss", index: 0, minSeq: 2, maxSeq: 3}}

	// Overlay already knows r1 was deleted at seq 50 (> create seq 2): the
	// backfill create of r1 must be suppressed; r2 survives.
	h.overlay = recordTombstoneSnapshot("did:plc:a", "app.bsky.feed.post", "r1", 50)
	h.overlayW = 0
	h.overlayM = 50
	h.installHandlers()

	// r2 is at seq 3; once it arrives the backfill has emitted everything it
	// will (r1 is suppressed), so wait for seq 3 then assert r1 never appeared.
	events := h.runUntilSeq(t, h.cfg(), 3)
	for _, ev := range events {
		if ev.Kind == KindCommit && ev.Commit.Rkey == "r1" && ev.Commit.Operation == OpCreate {
			t.Fatalf("suppressed create of r1 (deleted at seq 50) was emitted at seq %d", ev.Seq)
		}
	}
	// r2 (no tombstone) must be present.
	require.True(t, hasRkey(events, "r2"), "unsuppressed r2 must be emitted")
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
