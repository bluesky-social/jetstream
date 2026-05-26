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

// Close detaches the subscriber from the registry. Idempotent.
func (s *Subscriber) Close() {
	if !s.closed.CompareAndSwap(false, true) {
		return
	}
	s.registry.mu.Lock()
	delete(s.registry.subs, s)
	s.registry.mu.Unlock()
	close(s.ch)
}

func (s *Subscriber) markClosed() {
	if s.closed.CompareAndSwap(false, true) {
		close(s.ch)
	}
}
