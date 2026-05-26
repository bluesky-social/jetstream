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
