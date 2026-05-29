package subscribe

import (
	"sync"

	"github.com/bluesky-social/jetstream-v2/segment"
)

// RingBuf is a bounded FIFO ring of *segment.Event pointers used by
// the lookback replay engine to capture live events while disk replay
// runs. Push returns false on overflow rather than blocking — the
// replay engine treats overflow as the signal to restart replay at an
// updated cursor.
//
// One writer (broadcaster.Publish, after a phase-pointer load) and
// one drainer (replay goroutine) operate on the same ring. The mutex
// is held briefly across each operation; the structure is small
// enough that lock-free atomics aren't worth the complexity for the
// per-subscriber slot.
type RingBuf struct {
	mu   sync.Mutex
	buf  []*segment.Event
	head int // next index to read (Pop)
	tail int // next index to write (Push)
	n    int // current count
	cap  int

	// sealed is set by SealAndDrain at the lookback→live handoff. Once
	// sealed, Push refuses the event and reports sealed=true so the
	// producer reroutes it to the live channel instead of stranding it
	// in a ring the replay goroutine has stopped reading.
	sealed bool
}

// NewRingBuf returns a ring buffer with the given capacity. capacity
// must be > 0.
func NewRingBuf(capacity int) *RingBuf {
	if capacity <= 0 {
		panic("subscribe: ringbuf capacity must be > 0")
	}
	return &RingBuf{
		buf: make([]*segment.Event, capacity),
		cap: capacity,
	}
}

// Push appends ev to the back of the ring. Returns ok=false if the
// ring is full (ev not stored). Returns sealed=true if the ring has
// been sealed for the live handoff (ev not stored; the caller must
// route ev to the live channel instead). sealed takes precedence over
// the full check: a sealed ring never accepts another event.
func (r *RingBuf) Push(ev *segment.Event) (ok bool, sealed bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sealed {
		return false, true
	}
	if r.n == r.cap {
		return false, false
	}
	r.buf[r.tail] = ev
	r.tail = (r.tail + 1) % r.cap
	r.n++
	return true, false
}

// Pop removes and returns the front of the ring. Returns (nil, false)
// when the ring is empty.
func (r *RingBuf) Pop() (*segment.Event, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.n == 0 {
		return nil, false
	}
	ev := r.buf[r.head]
	r.buf[r.head] = nil
	r.head = (r.head + 1) % r.cap
	r.n--
	return ev, true
}

// SealAndDrain atomically marks the ring sealed and returns every
// buffered event in FIFO order, leaving the ring empty. After this
// call every Push reports sealed=true, so the producer reroutes
// further events to the live channel. Holding the lock across both
// the seal flag and the drain closes the window where an event could
// land in the ring after the replay goroutine stopped reading it —
// the source of permanent event loss at the lookback→live handoff.
func (r *RingBuf) SealAndDrain() []*segment.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sealed = true
	if r.n == 0 {
		return nil
	}
	out := make([]*segment.Event, r.n)
	for i := 0; i < r.n; i++ {
		out[i] = r.buf[(r.head+i)%r.cap]
		r.buf[(r.head+i)%r.cap] = nil
	}
	r.head = 0
	r.tail = 0
	r.n = 0
	return out
}

// Len returns the current number of buffered events.
func (r *RingBuf) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n
}

// Cap returns the ring's fixed capacity.
func (r *RingBuf) Cap() int { return r.cap }
