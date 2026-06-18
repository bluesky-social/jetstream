package client

import "sync"

// batcher groups events into batches by count, with a max-latency flush so a
// low-volume live tail does not hold events indefinitely (the design's
// tens-of-milliseconds delivery goal) and a final flush for the partial tail
// when the stream ends.
//
// It is safe for concurrent use: add is called from the backfill goroutine and
// (after cutover) the live goroutine, while a periodic flusher may fire from a
// timer goroutine. The mutex also serializes the downstream emit so the
// iterator's yield is never called concurrently.
type batcher struct {
	mu     sync.Mutex
	size   int
	buf    []Event
	emit   func(batch []Event) bool
	stop   bool   // set once emit returned false; further adds are no-ops
	onStop func() // fired exactly once when the batcher first stops
	onced  bool   // guards onStop
}

func newBatcher(size int, emit func(batch []Event) bool) *batcher {
	if size < 1 {
		size = 1
	}
	return &batcher{size: size, emit: emit, buf: make([]Event, 0, size)}
}

// setOnStop registers a callback fired exactly once when the consumer first
// asks to stop (emit returns false). The engine uses it to cancel the live
// tail so a quiet steady-state stream still unwinds promptly: the periodic
// flusher's yield returns false even when no live event is arriving.
func (b *batcher) setOnStop(fn func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.onStop = fn
}

// fireStopLocked invokes onStop once. Caller holds b.mu.
func (b *batcher) fireStopLocked() {
	if b.onced {
		return
	}
	b.onced = true
	if b.onStop != nil {
		go b.onStop()
	}
}

// add appends ev, emitting a full batch when size is reached. Returns false if
// the consumer asked to stop (emit returned false), after which the batcher is
// inert.
func (b *batcher) add(ev Event) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.stop {
		return false
	}
	b.buf = append(b.buf, ev)
	if len(b.buf) >= b.size {
		return b.flushLocked()
	}
	return true
}

// flush emits any buffered events as one batch. A no-op when empty. Returns
// false if the consumer asked to stop.
func (b *batcher) flush() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.flushLocked()
}

func (b *batcher) flushLocked() bool {
	if b.stop {
		return false
	}
	if len(b.buf) == 0 {
		return true
	}
	batch := b.buf
	b.buf = make([]Event, 0, b.size)
	if !b.emit(batch) {
		b.stop = true
		b.fireStopLocked()
		return false
	}
	return true
}

// stopped reports whether the consumer asked to stop.
func (b *batcher) stopped() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stop
}
