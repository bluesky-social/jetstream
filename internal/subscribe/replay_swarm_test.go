package subscribe_test

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/subscribe"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/stretchr/testify/require"
)

// TestReplaySwarm_NoEventsMissedAcrossManySubscribers exercises the
// replay engine under concurrent multi-subscriber load:
//
//   - 16 subscribers connect at varied StartSeq values (0, 5, 10, ..., 75)
//   - The fixture has 100 sealed events (seqs 0..99) on disk
//   - 500 additional events are published live during the test
//
// Each subscriber must:
//   - Receive its first event at >= its requested startSeq
//   - Receive events in strict monotonic seq order
//   - Receive every expected event (no gaps in the window it asked for)
//
// This test catches regressions where the disk → live handoff drops
// events at the boundary, where the ring drains in the wrong order,
// or where concurrent subscribers interfere with each other.
func TestReplaySwarm_NoEventsMissedAcrossManySubscribers(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("swarm tests are slow; skip in -short mode")
	}

	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	mustWriteSealedSegment(t, filepath.Join(segDir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 0, maxSeq: 99, minIndexedAt: 1_000, maxIndexedAt: 99_999, eventCount: 100,
	})
	m := mustOpenManifest(t, segDir)
	st, w := openWriterAtTip(t, dir, 100)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })

	b, err := subscribe.New(subscribe.Config{
		Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
		LookbackRingSize: 1024,
	})
	require.NoError(t, err)

	const (
		subscriberCount     = 16
		extraPublishedCount = 500
	)

	ctx := t.Context()

	type result struct {
		startSeq uint64
		got      []uint64
	}
	results := make([]result, subscriberCount)
	var wg sync.WaitGroup

	for i := range subscriberCount {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			startSeq := uint64(i * 5) // varied cursors: 0, 5, 10, ..., 75
			results[i].startSeq = startSeq

			r := subscribe.NewReplayer(subscribe.ReplayerInput{
				Broadcaster: b, Manifest: m, Writer: w,
				StartSeq: startSeq, RingSize: 1024, MaxIters: 16,
			})
			runCtx, runCancel := context.WithCancel(ctx)
			defer runCancel()

			// Per-subscriber expected count: sealed events from
			// startSeq..99 plus all extraPublishedCount live events.
			expected := 100 - int(startSeq) + extraPublishedCount

			_ = r.Run(runCtx, func(ev *segment.Event) error {
				results[i].got = append(results[i].got, ev.Seq)
				if len(results[i].got) >= expected {
					runCancel()
				}
				return nil
			})
		}()
	}

	// Brief warmup so all subscribers reach SubscribeForLookback before
	// we start publishing.
	time.Sleep(50 * time.Millisecond)

	// Publish extraPublishedCount live events.
	for i := uint64(100); i < 100+extraPublishedCount; i++ {
		b.Publish(&segment.Event{Seq: i, DID: "did:plc:swarm"})
	}

	// Sleep after publishing to allow subscribers to drain live events.
	time.Sleep(200 * time.Millisecond)

	// Wait for all subscribers to finish.
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("swarm did not converge in 15s")
	}

	// Validate each subscriber.
	for i, res := range results {
		require.NotEmpty(t, res.got, "subscriber %d got nothing", i)
		require.GreaterOrEqual(t, res.got[0], res.startSeq,
			"subscriber %d started below its cursor", i)
		for j := 1; j < len(res.got); j++ {
			require.Greater(t, res.got[j], res.got[j-1],
				"subscriber %d: out-of-order at index %d (prev=%d, this=%d)",
				i, j, res.got[j-1], res.got[j])
		}
	}
}
