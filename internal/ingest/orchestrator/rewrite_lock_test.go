package orchestrator

import (
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newLockTestOrchestrator builds a minimal Orchestrator usable for exercising
// withRewriteLock in isolation (no Run, no subsystems).
func newLockTestOrchestrator(t *testing.T) *Orchestrator {
	t.Helper()
	return &Orchestrator{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// TestWithRewriteLock_MutualExclusion is the design §3.3/§6 H anchor: two
// concurrent rewrite passes (a delete-compaction and a timestamp import, here
// stand-in closures) must never execute their segment-mutating bodies at the
// same time. A shared not-atomic counter incremented/decremented inside the
// lock with an overlap detector proves it: if the lock failed, the two
// goroutines would observe inside > 1.
func TestWithRewriteLock_MutualExclusion(t *testing.T) {
	t.Parallel()
	o := newLockTestOrchestrator(t)

	var inside atomic.Int32
	var maxObserved atomic.Int32
	var wg sync.WaitGroup

	body := func() error {
		n := inside.Add(1)
		for { // record the high-water mark of concurrent occupancy
			m := maxObserved.Load()
			if n <= m || maxObserved.CompareAndSwap(m, n) {
				break
			}
		}
		// Hold briefly to widen the race window a missing lock would expose.
		time.Sleep(time.Millisecond)
		inside.Add(-1)
		return nil
	}

	const goroutines = 8
	const iterations = 50
	for range goroutines {
		wg.Go(func() {
			for range iterations {
				require.NoError(t, o.withRewriteLock(body))
			}
		})
	}
	wg.Wait()

	require.EqualValues(t, 1, maxObserved.Load(),
		"withRewriteLock must serialize: at most one body inside at a time")
}

// TestWithRewriteLock_PropagatesError confirms the helper returns the body's
// error unchanged and still releases the lock (a second acquire succeeds).
func TestWithRewriteLock_PropagatesError(t *testing.T) {
	t.Parallel()
	o := newLockTestOrchestrator(t)

	sentinel := io.ErrUnexpectedEOF
	err := o.withRewriteLock(func() error { return sentinel })
	require.ErrorIs(t, err, sentinel)

	// Lock must have been released despite the error.
	done := make(chan struct{})
	go func() {
		_ = o.withRewriteLock(func() error { return nil })
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("withRewriteLock did not release the lock after an error")
	}
}
