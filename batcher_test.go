package jetstream

import (
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestBatcherEmitErrorStopsAndFiresOnStop is the B4 regression guard at the
// batcher level: when the consumer rejects an emitted error, the batcher must
// become inert (stop) AND fire onStop exactly once, mirroring a rejected batch.
// Before the fix, errors bypassed the batcher entirely (emitted directly by the
// engine, concurrently with the flusher), so a rejected error neither stopped
// batching nor unwound a quiet tail via onStop.
func TestBatcherEmitErrorStopsAndFiresOnStop(t *testing.T) {
	t.Parallel()

	var emittedBatches int
	b := newBatcher(8,
		func([]Event) bool { emittedBatches++; return true },
		func(error) bool { return false }, // consumer rejects the error
	)
	// A second fire would double-close and panic, so this channel also guards
	// the "exactly once" half of the contract.
	stopFired := make(chan struct{})
	b.setOnStop(func() { close(stopFired) })

	require.True(t, b.add(Event{Seq: 1}), "first add accepted")
	require.False(t, b.emitError(errors.New("boom")), "rejected error must return false")
	require.True(t, b.stopped(), "a rejected error must stop the batcher")

	// onStop fires asynchronously (go b.onStop()); wait for it rather than
	// assuming it ran.
	select {
	case <-stopFired:
	case <-time.After(2 * time.Second):
		t.Fatal("onStop did not fire after a rejected error")
	}

	// Confirm the batcher is now inert.
	require.False(t, b.add(Event{Seq: 2}), "adds after stop are no-ops")
	require.False(t, b.flush(), "flush after stop is a no-op stop")
}

// TestBatcherEmitErrorFlushesPendingFirst verifies an error never jumps ahead
// of events already buffered: emitError flushes the pending batch before
// emitting the error, preserving delivery order.
func TestBatcherEmitErrorFlushesPendingFirst(t *testing.T) {
	t.Parallel()

	var (
		mu    sync.Mutex
		order []string
	)
	b := newBatcher(8,
		func(batch []Event) bool {
			mu.Lock()
			order = append(order, "batch")
			mu.Unlock()
			return true
		},
		func(error) bool {
			mu.Lock()
			order = append(order, "error")
			mu.Unlock()
			return true
		},
	)

	require.True(t, b.add(Event{Seq: 1}))
	require.True(t, b.add(Event{Seq: 2}))
	require.True(t, b.emitError(errors.New("hiccup")))

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"batch", "error"}, order,
		"buffered events must flush before the error is emitted")
}

// TestBatcherNilEmitErrIsNoop guards the optional-emitErr path: a batcher built
// without an error sink treats emitError as a successful no-op (after flushing).
func TestBatcherNilEmitErrIsNoop(t *testing.T) {
	t.Parallel()
	b := newBatcher(8, func([]Event) bool { return true }, nil)
	require.True(t, b.emitError(errors.New("x")))
	require.False(t, b.stopped())
}

// TestBatcherHugeSizeDoesNotPanic guards the clamped buffer preallocation: any
// positive WithBatchSize is accepted, so a size near MaxInt must not reach
// make() as a capacity (an out-of-range cap panics, crashing the engine —
// never-crash contract). Both the constructor and the post-flush reallocation
// take the clamped path.
func TestBatcherHugeSizeDoesNotPanic(t *testing.T) {
	t.Parallel()
	var emitted [][]Event
	b := newBatcher(math.MaxInt,
		func(batch []Event) bool { emitted = append(emitted, batch); return true },
		func(error) bool { return true },
	)
	require.True(t, b.add(Event{Seq: 1}))
	require.True(t, b.flush(), "flush must emit, not panic, at huge sizes")
	require.True(t, b.add(Event{Seq: 2}), "the post-flush buffer must be usable")
	require.True(t, b.flush())
	require.Equal(t, [][]Event{{{Seq: 1}}, {{Seq: 2}}}, emitted)
}
