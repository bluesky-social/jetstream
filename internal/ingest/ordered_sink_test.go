package ingest

import (
	"context"
	"sync"
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// These tests pin the SetOrderedEventSink contract (#244): every event
// appended through a shared Writer — regardless of producer or append API —
// reaches the sink exactly once, in dense global seq order. The /subscribe
// hot ring's index math depends on this.

func sinkEvent(did string) segment.Event {
	return segment.Event{
		Kind: segment.KindCreate, DID: did,
		Collection: "app.bsky.feed.post", Rkey: "rkey", Rev: "3ke6kg3wk3e22",
		Payload: []byte{0xa0},
	}
}

func requireDense(t *testing.T, seqs []uint64) {
	t.Helper()
	require.NotEmpty(t, seqs)
	for i := 1; i < len(seqs); i++ {
		require.Equalf(t, seqs[i-1]+1, seqs[i],
			"sink delivery not dense at position %d: %d -> %d", i, seqs[i-1], seqs[i])
	}
}

// TestOrderedSink_AppendAndBatchInterleaved: single goroutine mixing Append
// and AppendBatch must deliver one dense seq stream.
func TestOrderedSink_AppendAndBatchInterleaved(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{})

	var got []uint64
	w.SetOrderedEventSink(func(ev *segment.Event) { got = append(got, ev.Seq) })

	ev := sinkEvent("did:plc:single")
	require.NoError(t, w.Append(context.Background(), &ev))
	batch := []segment.Event{sinkEvent("did:plc:batch"), sinkEvent("did:plc:batch"), sinkEvent("did:plc:batch")}
	require.NoError(t, w.AppendBatch(context.Background(), batch))
	ev2 := sinkEvent("did:plc:single")
	require.NoError(t, w.Append(context.Background(), &ev2))

	require.Len(t, got, 5)
	requireDense(t, got)
	require.Equal(t, uint64(1), got[0])
}

// TestOrderedSink_ConcurrentProducersStayOrdered is the #244 regression
// property: a live-consumer-shaped producer (per-event Append) and a
// retry-runner-shaped producer (AppendBatch) hammering the same writer must
// never deliver to the sink out of seq order or with gaps. Run with -race.
func TestOrderedSink_ConcurrentProducersStayOrdered(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{})

	// The sink runs under drainMu, so appends serialize deliveries; no
	// extra locking should be needed. The race detector verifies that
	// claim — if drainMu ever stopped covering the sink, `got` would race.
	var got []uint64
	w.SetOrderedEventSink(func(ev *segment.Event) { got = append(got, ev.Seq) })

	const (
		liveEvents   = 500
		retryBatches = 50
		batchSize    = 10
	)
	var wg sync.WaitGroup
	wg.Go(func() {
		for range liveEvents {
			ev := sinkEvent("did:plc:live")
			require.NoError(t, w.Append(context.Background(), &ev))
		}
	})
	wg.Go(func() {
		for range retryBatches {
			batch := make([]segment.Event, batchSize)
			for i := range batch {
				batch[i] = sinkEvent("did:plc:retry")
			}
			require.NoError(t, w.AppendBatch(context.Background(), batch))
		}
	})
	wg.Wait()

	require.Len(t, got, liveEvents+retryBatches*batchSize)
	requireDense(t, got)
}

// TestOrderedSink_AsyncWriterRejected: the sink is mutually exclusive with
// AsyncFlushWorkers — the async paths would advertise events to the sink
// before their prepared block is cold-readable (#249). Installing a sink on
// an async writer is invalid internal wiring and must fail loud at startup,
// not surface later as a subscriber gap. The #248 readable-log refactor
// removes this restriction along with the mode dichotomy itself.
func TestOrderedSink_AsyncWriterRejected(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{AsyncFlushWorkers: 2})

	require.PanicsWithValue(t,
		"ingest: SetOrderedEventSink is not supported with AsyncFlushWorkers (see issue #249)",
		func() { w.SetOrderedEventSink(func(*segment.Event) {}) })

	// The writer itself stays usable — only the sink install is rejected.
	ev := sinkEvent("did:plc:async")
	require.NoError(t, w.Append(context.Background(), &ev))
	require.Equal(t, uint64(1), ev.Seq)
}

// TestOrderedSink_NotInstalledIsFree: a writer with no sink behaves as
// before (nil check only).
func TestOrderedSink_NotInstalledIsFree(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{})
	ev := sinkEvent("did:plc:nosink")
	require.NoError(t, w.Append(context.Background(), &ev))
	require.Equal(t, uint64(1), ev.Seq)
}
