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
	}
}

// run executes the engine until the short-lived context expires (the live
// consumer reconnect-loops on script EOF), returning all emitted events.
func (h *engineHarness) run(t *testing.T, cfg Config) []Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()

	var (
		mu     sync.Mutex
		events []Event
	)
	eng := NewEngine(cfg)
	done := make(chan struct{})
	go func() {
		defer close(done)
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
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("engine did not finish within 5s")
	}
	mu.Lock()
	defer mu.Unlock()
	return append([]Event(nil), events...)
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

	events := h.run(t, h.cfg())

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

	events := h.run(t, h.cfg())
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

	events := h.run(t, h.cfg())
	for _, ev := range events {
		if ev.Kind == KindCommit && ev.Commit.Rkey == "r1" && ev.Commit.Operation == OpCreate {
			t.Fatalf("suppressed create of r1 (deleted at seq 50) was emitted at seq %d", ev.Seq)
		}
	}
	// r2 (no tombstone) must be present.
	require.True(t, hasRkey(events, "r2"), "unsuppressed r2 must be emitted")
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
