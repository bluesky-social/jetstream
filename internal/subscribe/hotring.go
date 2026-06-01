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
// non-empty (dense, monotonic). The first append seeds baseSeq.
func (r *hotRing) append(e *Entry) {
	if !r.hasData {
		r.baseSeq = e.Event.Seq
		r.tipSeq = e.Event.Seq
		r.hasData = true
	}
	r.buf = append(r.buf, e)
	r.curBytes += e.approxBytes()
	r.tipSeq = e.Event.Seq + 1
	r.evict()
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
	idx := int(cursor - r.baseSeq)
	return r.buf[idx:], true
}

// has reports whether the ring holds any resident entries.
func (r *hotRing) has() bool { return r.hasData }

func (r *hotRing) bytes() int   { return r.curBytes }
func (r *hotRing) len() int     { return len(r.buf) }
func (r *hotRing) tip() uint64  { return r.tipSeq }
func (r *hotRing) base() uint64 { return r.baseSeq }
