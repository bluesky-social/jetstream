package client

// batcher groups events into batches by count, with an explicit flush for the
// partial tail (e.g. when the live tail goes idle or the stream ends). It does
// no locking: the engine calls it from a single emission goroutine at a time.
type batcher struct {
	size int
	buf  []Event
	emit func(batch []Event) bool
	stop bool // set once emit returned false; further adds are no-ops
}

func newBatcher(size int, emit func(batch []Event) bool) *batcher {
	if size < 1 {
		size = 1
	}
	return &batcher{size: size, emit: emit, buf: make([]Event, 0, size)}
}

// add appends ev, emitting a full batch when size is reached. Returns false if
// the consumer asked to stop (emit returned false), after which the batcher is
// inert.
func (b *batcher) add(ev Event) bool {
	if b.stop {
		return false
	}
	b.buf = append(b.buf, ev)
	if len(b.buf) >= b.size {
		return b.flush()
	}
	return true
}

// flush emits any buffered events as one batch. A no-op when empty. Returns
// false if the consumer asked to stop.
func (b *batcher) flush() bool {
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
		return false
	}
	return true
}

// stopped reports whether the consumer asked to stop.
func (b *batcher) stopped() bool { return b.stop }
