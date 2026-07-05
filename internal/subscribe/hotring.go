package subscribe

// hotRing is a byte-bounded FIFO of recent *Entry, indexed by seq.
// Entries are appended in strictly increasing, dense seq order (ingest
// assigns dense seqs). Eviction drops the oldest entries until the total
// approxBytes is within budget. NOT concurrency-safe: Tail owns the lock.
type hotRing struct {
	buf      []*Entry // buf[i] holds seq baseSeq+uint64(i); FIFO via slice
	baseSeq  uint64   // seq of buf[0]; 0 when empty
	tipSeq   uint64   // next seq to be appended (one past newest)
	curBytes int
	maxBytes int
	hasData  bool
}

func newHotRing(maxBytes int) *hotRing {
	if maxBytes <= 0 {
		panic("subscribe: hotRing maxBytes must be > 0")
	}
	return &hotRing{maxBytes: maxBytes}
}

// append adds e to the back of the ring and evicts from the front until
// the byte budget holds. e.Event.Seq must equal r.tipSeq when the ring is
// non-empty (dense, monotonic) — lookup's index math depends on it. The
// first append seeds baseSeq.
//
// A non-dense append (gap or regression) reports reset=true: the ring
// drops all residency and restarts at e. Retaining entries across a hole
// would serve WRONG-SEQ events at every cursor past it (#244). Dense
// delivery is owned by the ingest writer's ordered append hook; this is
// the defense-in-depth backstop for any producer that bypasses it,
// degrading the dropped window to a cold (disk) read instead of
// corrupting the fanout.
func (r *hotRing) append(e *Entry) (reset bool) {
	if r.hasData && e.Event.Seq != r.tipSeq {
		reset = true
		clear(r.buf)
		r.buf = r.buf[:0]
		r.curBytes = 0
		r.hasData = false
	}
	if !r.hasData {
		r.baseSeq = e.Event.Seq
		r.tipSeq = e.Event.Seq
		r.hasData = true
	}
	r.buf = append(r.buf, e)
	r.curBytes += e.approxBytes()
	r.tipSeq = e.Event.Seq + 1
	r.evict()
	return reset
}

// evict drops oldest entries until curBytes <= maxBytes. Always keeps at
// least one entry so the newest event is always servable from the ring.
func (r *hotRing) evict() {
	for r.curBytes > r.maxBytes && len(r.buf) > 1 {
		old := r.buf[0]
		r.curBytes -= old.approxBytes()
		r.buf[0] = nil
		r.buf = r.buf[1:]
		r.baseSeq++
	}
}

// lookup returns the resident entries with Seq >= cursor in seq order.
// ok=false means the ring cannot serve this cursor from memory: either the
// ring is empty, or cursor is below the oldest resident seq (evicted), or
// cursor is at/beyond the resident tip (nothing newer yet). The caller
// (Tail) uses the authoritative tip to decide whether an ok=false means
// "read cold from disk" or "block at the live tip".
func (r *hotRing) lookup(cursor uint64) (entries []*Entry, ok bool) {
	if !r.hasData {
		return nil, false
	}
	if cursor < r.baseSeq || cursor >= r.tipSeq {
		return nil, false
	}
	// Bounds guard: with the dense invariant (append resets on any gap)
	// len(buf) always equals tipSeq-baseSeq, making this unreachable. It
	// stays as the last line of defense because the failure mode of index
	// drift is not an error return but serving the WRONG event — or an
	// out-of-range panic in the caller while it holds the tail mutex,
	// wedging every subscriber and the ingest hot path (#244).
	idx := int(cursor - r.baseSeq)
	if idx < 0 || idx >= len(r.buf) {
		return nil, false
	}
	return r.buf[idx:], true
}

// has reports whether the ring holds any resident entries.
func (r *hotRing) has() bool { return r.hasData }

func (r *hotRing) bytes() int   { return r.curBytes }
func (r *hotRing) len() int     { return len(r.buf) }
func (r *hotRing) tip() uint64  { return r.tipSeq }
func (r *hotRing) base() uint64 { return r.baseSeq }
