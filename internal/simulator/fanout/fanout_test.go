package fanout

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRegistry_DeliversToSubscriber(t *testing.T) {
	t.Parallel()
	r := New(8)
	sub := r.Subscribe()
	defer sub.Close()

	r.Publish([]byte("hello"))
	select {
	case msg := <-sub.Events():
		require.Equal(t, []byte("hello"), msg)
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestRegistry_DropsWhenSubscriberFull(t *testing.T) {
	t.Parallel()
	r := New(1)
	sub := r.Subscribe()
	defer sub.Close()

	r.Publish([]byte("a"))
	r.Publish([]byte("b")) // drop

	require.Equal(t, uint64(1), sub.Drops())
}

func TestRegistry_TotalDropsSumsAttached(t *testing.T) {
	t.Parallel()
	r := New(1)
	a := r.Subscribe()
	defer a.Close()
	b := r.Subscribe()
	defer b.Close()

	r.Publish([]byte("x")) // fills both buffers (cap 1)
	r.Publish([]byte("y")) // drop on both
	r.Publish([]byte("z")) // drop on both

	require.Equal(t, uint64(4), r.TotalDrops(), "2 drops each across 2 attached subscribers")
}

func TestRegistry_TotalDropsSurvivesClose(t *testing.T) {
	t.Parallel()
	// The harness asserts TotalDrops()==0 AFTER the runtime (and its
	// subscriber) has shut down. A drop count that vanished when the
	// subscriber detached would make that assertion vacuously zero, so the
	// registry must retain a detached subscriber's drops for the run's
	// lifetime. This test fails if that accumulation regresses.
	r := New(1)
	sub := r.Subscribe()

	r.Publish([]byte("a")) // fills the cap-1 buffer
	r.Publish([]byte("b")) // drop
	require.Equal(t, uint64(1), r.TotalDrops())

	sub.Close()
	require.Equal(t, uint64(1), r.TotalDrops(),
		"a detached subscriber's drops must still be counted (assertion would be vacuous otherwise)")
}

func TestRegistry_TotalDropsSurvivesCloseAll(t *testing.T) {
	t.Parallel()
	r := New(1)
	r.Subscribe()

	r.Publish([]byte("a")) // fills the cap-1 buffer
	r.Publish([]byte("b")) // drop
	require.Equal(t, uint64(1), r.TotalDrops())

	r.CloseAll()
	require.Equal(t, uint64(1), r.TotalDrops(),
		"CloseAll must fold detached subscribers' drops into the lifetime total")
}

func TestRegistry_FanOut(t *testing.T) {
	t.Parallel()
	r := New(8)
	const n = 4
	subs := make([]*Subscriber, n)
	for i := range n {
		subs[i] = r.Subscribe()
	}
	defer func() {
		for _, s := range subs {
			s.Close()
		}
	}()

	r.Publish([]byte("x"))
	var got atomic.Int32
	for _, s := range subs {
		select {
		case <-s.Events():
			got.Add(1)
		case <-time.After(time.Second):
			t.Fatal("timeout")
		}
	}
	require.Equal(t, int32(n), got.Load())
}

func TestRegistry_PublishAndCloseRace(t *testing.T) {
	t.Parallel()
	r := New(4)
	const subs = 32
	const publishes = 200

	// Pre-create subscribers; we'll close them concurrently with Publish.
	all := make([]*Subscriber, subs)
	for i := range subs {
		all[i] = r.Subscribe()
	}

	// Drain receivers so the channels aren't backpressuring publishers.
	done := make(chan struct{})
	defer close(done)
	for _, s := range all {
		go func(s *Subscriber) {
			for {
				select {
				case <-s.Events():
				case <-done:
					return
				}
			}
		}(s)
	}

	var wg sync.WaitGroup

	wg.Go(func() {
		for range publishes {
			r.Publish([]byte("x"))
		}
	})

	wg.Go(func() {
		for _, s := range all {
			s.Close()
		}
	})

	wg.Wait()
	// Survival is the assertion: no panic on send-after-close.
}

func TestRegistry_CloseAllSurvivesConcurrentPublish(t *testing.T) {
	t.Parallel()
	r := New(4)
	for range 16 {
		r.Subscribe()
	}

	var wg sync.WaitGroup

	wg.Go(func() {
		for range 200 {
			r.Publish([]byte("y"))
		}
	})

	wg.Go(func() {
		r.CloseAll()
	})

	wg.Wait()
}
