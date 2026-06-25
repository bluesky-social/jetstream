// Package fanout owns the in-memory pub/sub that the simulator's
// traffic generator broadcasts to and the websocket subscribeRepos
// handler reads from. Buffered per-subscriber channel; drop-on-
// overflow signals a slow consumer that should reconnect from
// cursor (the relay path bookkeeping lives in the relay handler,
// not here).
package fanout

import (
	"sync"
	"sync/atomic"
)

// Registry holds all currently-attached subscribers.
type Registry struct {
	bufSize int

	mu   sync.RWMutex
	subs map[*Subscriber]struct{}

	// detachedDrops accumulates the drop counts of subscribers that have
	// since detached (Close / CloseAll), so TotalDrops reflects the whole
	// run's loss rather than only the subscribers still attached. A
	// reconnecting consumer detaches its old subscriber on every reconnect,
	// and the relay handler detaches on shutdown, so without this an
	// after-the-fact "zero drops" assertion would read vacuously zero.
	detachedDrops atomic.Uint64
}

// Subscriber is one consumer's view onto the broadcast.
type Subscriber struct {
	ch     chan []byte
	drops  atomic.Uint64
	closed atomic.Bool

	registry *Registry
}

// New constructs a Registry whose subscribers' outbound channels are
// sized at bufSize.
func New(bufSize int) *Registry {
	if bufSize < 1 {
		bufSize = 1
	}
	return &Registry{
		bufSize: bufSize,
		subs:    make(map[*Subscriber]struct{}),
	}
}

// Subscribe registers a fresh subscriber.
func (r *Registry) Subscribe() *Subscriber {
	s := &Subscriber{
		ch:       make(chan []byte, r.bufSize),
		registry: r,
	}
	r.mu.Lock()
	r.subs[s] = struct{}{}
	r.mu.Unlock()
	return s
}

// Publish sends one frame to every attached subscriber. Drops
// non-blockingly into any subscriber whose buffer is full.
//
// Holds the read lock for the entire send loop to ensure that no
// subscriber Close can run concurrently and close a channel while
// we're sending to it.
func (r *Registry) Publish(frame []byte) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for s := range r.subs {
		select {
		case s.ch <- frame:
		default:
			s.drops.Add(1)
		}
	}
}

// TotalDrops sums dropped-frame counts across the registry's whole lifetime:
// every currently-attached subscriber plus those that have detached. A slow
// consumer whose buffer fills shows up here; used by tests to assert lossless
// delivery even after the consumer (and its subscriber) has gone away.
func (r *Registry) TotalDrops() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	total := r.detachedDrops.Load()
	for s := range r.subs {
		total += s.drops.Load()
	}
	return total
}

// CloseAll closes every subscriber. Used at simulator shutdown.
func (r *Registry) CloseAll() {
	r.mu.Lock()
	for s := range r.subs {
		s.markClosed()
	}
	r.subs = make(map[*Subscriber]struct{})
	r.mu.Unlock()
}

// Events is the receive-only outbound channel for this subscriber.
func (s *Subscriber) Events() <-chan []byte {
	return s.ch
}

// Drops returns the number of dropped frames observed by this
// subscriber.
func (s *Subscriber) Drops() uint64 {
	return s.drops.Load()
}

// Close detaches the subscriber from the registry. Idempotent. Its drop
// count is folded into the registry's lifetime accumulator before it
// detaches, so TotalDrops still reflects this subscriber's losses.
func (s *Subscriber) Close() {
	if !s.closed.CompareAndSwap(false, true) {
		return
	}
	s.registry.mu.Lock()
	s.registry.detachedDrops.Add(s.drops.Load())
	delete(s.registry.subs, s)
	s.registry.mu.Unlock()
	close(s.ch)
}

func (s *Subscriber) markClosed() {
	if s.closed.CompareAndSwap(false, true) {
		s.registry.detachedDrops.Add(s.drops.Load())
		close(s.ch)
	}
}
