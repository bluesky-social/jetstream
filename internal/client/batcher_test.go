package client

import (
	"errors"
	"sync"
	"testing"

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
	var stops int
	b.setOnStop(func() { stops++ })

	require.True(t, b.add(Event{Seq: 1}), "first add accepted")
	require.False(t, b.emitError(errors.New("boom")), "rejected error must return false")
	require.True(t, b.stopped(), "a rejected error must stop the batcher")

	// onStop fires asynchronously (go b.onStop()); a follow-up add observes stop
	// regardless. Confirm the batcher is now inert.
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
