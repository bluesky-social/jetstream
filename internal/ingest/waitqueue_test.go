package ingest

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

var errQueueOwnerClosed = errors.New("closed")

// queueOwner uses a waitQueue the way the direct writer does: takers wait
// their turn for a unit of room, and a release wakes the oldest.
type queueOwner struct {
	mu     sync.Mutex
	q      waitQueue
	room   int
	closed bool
	taken  []int // taker ids, in the order they took room
	wakes  int   // waits a signal or wakeAll ended
}

func newQueueOwner() *queueOwner {
	o := &queueOwner{}
	o.q = waitQueue{mu: &o.mu, ready: func() bool { return o.room > 0 }}
	return o
}

// take signals only once it took room, leaving a waiter whose wait ended
// any other way to the queue's own handling.
func (o *queueOwner) take(ctx context.Context, id int) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	turn := false
	for {
		if o.room > 0 && (turn || o.q.len() == 0) {
			o.room--
			o.taken = append(o.taken, id)
			o.q.signal()
			return nil
		}
		if o.closed {
			return errQueueOwnerClosed
		}
		if err := o.q.wait(ctx, turn); err != nil {
			return err
		}
		o.wakes++
		turn = true
	}
}

func (o *queueOwner) release(n int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.room += n
	o.q.signal()
}

func (o *queueOwner) close() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.closed = true
	o.q.wakeAll()
}

// state is (room, waiters, taken, wakes) at a quiescent point.
func (o *queueOwner) state() (room, waiters int, taken []int, wakes int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.room, o.q.len(), append([]int(nil), o.taken...), o.wakes
}

// startTakers starts takers 0..n-1 one at a time, each queued before the
// next starts, and returns their results.
func startTakers(t *testing.T, o *queueOwner, n int, ctxs ...context.Context) []chan error {
	t.Helper()
	res := make([]chan error, n)
	for i := range n {
		ctx := t.Context()
		if i < len(ctxs) && ctxs[i] != nil {
			ctx = ctxs[i]
		}
		res[i] = make(chan error, 1)
		go func() { res[i] <- o.take(ctx, i) }()
		synctest.Wait()
	}
	return res
}

// A release wakes the oldest waiter alone, and a taker that leaves room
// wakes the next: waiters take room in the order they arrived, and nobody
// wakes to find nothing left.
func TestWaitQueue_SignalWakesOldestOnly(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		o := newQueueOwner()
		res := startTakers(t, o, 5)
		_, waiters, _, _ := o.state()
		require.Equal(t, 5, waiters)

		o.release(1)
		synctest.Wait()
		room, waiters, taken, wakes := o.state()
		require.Equal(t, []int{0}, taken)
		require.Equal(t, 1, wakes, "one unit of room wakes one waiter")
		require.Equal(t, 4, waiters)
		require.Zero(t, room)
		require.NoError(t, <-res[0])

		o.release(3)
		synctest.Wait()
		room, waiters, taken, wakes = o.state()
		require.Equal(t, []int{0, 1, 2, 3}, taken)
		require.Equal(t, 4, wakes, "each taker that left room woke exactly the next")
		require.Equal(t, 1, waiters)
		require.Zero(t, room)

		o.close()
		synctest.Wait()
		require.ErrorIs(t, <-res[4], errQueueOwnerClosed)
		_, waiters, _, _ = o.state()
		require.Zero(t, waiters)
	})
}

// A waiter whose ctx ends leaves the queue; the turn goes past it.
func TestWaitQueue_CancelledWaiterLeavesQueue(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		o := newQueueOwner()
		ctx, cancel := context.WithCancel(t.Context())
		res := startTakers(t, o, 3, nil, ctx)
		cancel()
		synctest.Wait()
		require.ErrorIs(t, <-res[1], context.Canceled)
		_, waiters, _, _ := o.state()
		require.Equal(t, 2, waiters)

		o.release(2)
		synctest.Wait()
		_, waiters, taken, wakes := o.state()
		require.Equal(t, []int{0, 2}, taken)
		require.Equal(t, 2, wakes)
		require.Zero(t, waiters)
	})
}

// A waiter whose ctx ends after a signal chose it passes the turn on:
// without that, the room would sit unused while the next waiter waits.
func TestWaitQueue_CancelAfterSignalPassesTurn(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		o := newQueueOwner()
		ctx, cancel := context.WithCancel(t.Context())
		res := startTakers(t, o, 2, ctx)
		// The cancellation wakes taker 0 first; it blocks on mu while the
		// release chooses it.
		o.mu.Lock()
		cancel()
		o.room++
		o.q.signal()
		o.mu.Unlock()
		synctest.Wait()
		require.ErrorIs(t, <-res[0], context.Canceled)
		require.NoError(t, <-res[1])
		room, waiters, taken, _ := o.state()
		require.Equal(t, []int{1}, taken)
		require.Zero(t, waiters)
		require.Zero(t, room)
	})
}

// A woken waiter that finds the room gone waits again at the front, ahead
// of everyone who arrived after it.
func TestWaitQueue_RequeueAtFront(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		o := newQueueOwner()
		res := startTakers(t, o, 2)
		o.mu.Lock()
		o.room++
		o.q.signal()
		o.room-- // taken before taker 0 runs
		o.mu.Unlock()
		synctest.Wait()
		_, waiters, taken, _ := o.state()
		require.Empty(t, taken)
		require.Equal(t, 2, waiters)

		o.release(1)
		synctest.Wait()
		_, waiters, taken, _ = o.state()
		require.Equal(t, []int{0}, taken)
		require.Equal(t, 1, waiters)
		require.NoError(t, <-res[0])
		o.close()
		require.ErrorIs(t, <-res[1], errQueueOwnerClosed)
	})
}

// Random takers, cancellations, releases, and arrivals never leave room
// unused while someone waits, and every unit of room is taken once.
func TestWaitQueue_Swarm(t *testing.T) {
	t.Parallel()
	for seed := range uint64(32) {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) { runWaitQueueSwarm(t, seed) })
		})
	}
}

func runWaitQueueSwarm(t *testing.T, seed uint64) {
	rng := rand.New(rand.NewPCG(seed, 0x9a17))
	o := newQueueOwner()
	n := 1 + rng.IntN(64)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var ok, cancelled int
	var unexpected []error
	for i := range n {
		ctx := t.Context()
		if rng.IntN(4) == 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, time.Duration(rng.IntN(50))*time.Millisecond)
			defer cancel()
		}
		arrive := time.Duration(rng.IntN(40)) * time.Millisecond
		wg.Go(func() {
			time.Sleep(arrive)
			err := o.take(ctx, i)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				ok++
			case errors.Is(err, context.DeadlineExceeded):
				cancelled++
			default:
				unexpected = append(unexpected, err)
			}
		})
	}
	released := 0
	for released < n {
		k := 1 + rng.IntN(3)
		o.release(k)
		released += k
		time.Sleep(time.Duration(rng.IntN(5)) * time.Millisecond)
		synctest.Wait()
		room, waiters, _, _ := o.state()
		require.False(t, room > 0 && waiters > 0, "room %d unused while %d wait", room, waiters)
	}
	wg.Wait()
	require.Empty(t, unexpected)
	room, waiters, taken, _ := o.state()
	require.Zero(t, waiters)
	require.Equal(t, n, ok+cancelled)
	require.Len(t, taken, ok)
	require.Equal(t, released-ok, room, "every unit of room is taken at most once")
}

// BenchmarkWaitQueue hands one unit of room at a time to a crowd of
// waiters, as committed blocks do to waiting appends, and times each
// handoff: with the queue, one waiter wakes per unit; with the broadcast
// the direct writer used before, every waiter does.
func BenchmarkWaitQueue(b *testing.B) {
	for _, waiters := range []int{8, 64, 500} {
		b.Run(fmt.Sprintf("broadcast/waiters=%d", waiters), func(b *testing.B) {
			benchmarkHandoff(b, waiters, newBroadcastRoom())
		})
		b.Run(fmt.Sprintf("queue/waiters=%d", waiters), func(b *testing.B) {
			o := newQueueOwner()
			benchmarkHandoff(b, waiters, &queueRoom{o: o})
		})
	}
}

type benchRoom interface {
	take(ctx context.Context) error // nil once it took a unit
	release()
	stop()
}

type queueRoom struct{ o *queueOwner }

func (r *queueRoom) take(ctx context.Context) error { return r.o.take(ctx, 0) }
func (r *queueRoom) release()                       { r.o.release(1) }
func (r *queueRoom) stop()                          { r.o.close() }

// broadcastRoom is the direct writer's wait before waitQueue: one channel,
// closed and replaced on every change.
type broadcastRoom struct {
	mu      sync.Mutex
	room    int
	stopped bool
	waiters int
	changed chan struct{}
}

func newBroadcastRoom() *broadcastRoom { return &broadcastRoom{changed: make(chan struct{})} }

func (r *broadcastRoom) take(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for r.room == 0 {
		if r.stopped {
			return errQueueOwnerClosed
		}
		ch := r.changed
		r.waiters++
		r.mu.Unlock()
		<-ch
		r.mu.Lock()
		r.waiters--
	}
	r.room--
	return nil
}

func (r *broadcastRoom) signalLocked() {
	if r.waiters > 0 {
		close(r.changed)
		r.changed = make(chan struct{})
	}
}

func (r *broadcastRoom) release() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.room++
	r.signalLocked()
}

func (r *broadcastRoom) stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopped = true
	r.signalLocked()
}

func benchmarkHandoff(b *testing.B, waiters int, r benchRoom) {
	taken := make(chan struct{})
	var wg sync.WaitGroup
	for range waiters {
		wg.Go(func() {
			for r.take(b.Context()) == nil {
				taken <- struct{}{}
			}
		})
	}
	cpu := processCPU(b)
	for b.Loop() {
		r.release()
		<-taken
	}
	b.ReportMetric(float64(processCPU(b)-cpu)/float64(b.N), "cpu-ns/op")
	r.stop()
	wg.Wait()
}

// processCPU is the process's user and system CPU time so far.
func processCPU(b *testing.B) time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		b.Fatal(err)
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}
