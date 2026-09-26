package ingest

import (
	"context"
	"sync"
	"time"

	"github.com/bluesky-social/jetstream/segment"
)

// Hot mode admission control (design §10.5). Rule by rule:
//
//  1. Live appenders count themselves in liveWaiting before taking mu. A
//     bulk appender takes mu for one chunk at a time and, between chunks,
//     yields while the counter is non-zero.
//  2. A class change cuts the batch (appendLocked).
//  3. and 4. A live batch commits inline if the token bucket pays for it.
//     Otherwise it goes into overflow and freezes as a pointer batch
//     (cutLocked).
//  5. Bulk batches are pointer batches, at most one chunk long.
//  6. Bulk chunks take permits for their raw bytes, released at commit.
//  7. The writer bounds its uploads in flight (prepare); the blob store
//     bounds PUTs process-wide. A bulk chunk also waits while as many bulk
//     batches are frozen and uncommitted as uploads may be in flight.
//     Commits are in seq order, so a live batch waits for every bulk batch
//     frozen ahead of it: the byte pool alone let hundreds queue for the
//     upload slots, and live latency grew to seconds (design §22.2).
//  8. and 9. Every append waits while frozen-uncommitted bytes or
//     committed-but-unfolded events are over their caps.
//
// Waiters sleep on changed, which is closed and replaced whenever something
// they wait on may have moved: a commit, a fold, a live append finishing, a
// failure, or Close.

// appendBulk appends events as bulk chunks: at most BulkChunkMaxEvents, and
// never past the open block.
func (h *hotWriter) appendBulk(ctx context.Context, events []segment.Event) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for len(events) > 0 {
		n, err := h.admitLocked(ctx, ClassBulk, events)
		if err != nil {
			return err
		}
		for i := range events[:n] {
			if err = h.appendLocked(ClassBulk, &events[i]); err != nil {
				break
			}
		}
		h.settleCreditLocked()
		if err != nil {
			return err
		}
		events = events[n:]
	}
	return nil
}

// admitLocked waits, releasing mu while it does, until an append of class
// may proceed. For a bulk append it returns the size of the next chunk of
// events and holds that chunk's permits in bulkCredit.
func (h *hotWriter) admitLocked(ctx context.Context, class Class, events []segment.Event) (int, error) {
	var start time.Time
	for {
		if err := h.usableLocked(); err != nil {
			h.cfg.Metrics.incAppendErrors()
			return 0, err
		}
		var n int
		var raw int64
		if class == ClassBulk {
			n, raw = h.bulkChunkLocked(events)
		}
		if h.admissibleLocked(class, raw) {
			if class == ClassBulk {
				h.bulkPermits += raw
				h.bulkCredit = raw
			}
			var waited time.Duration
			if !start.IsZero() {
				waited = time.Since(start)
			}
			h.cfg.Metrics.observeAdmissionWait(class, waited)
			return n, nil
		}
		if start.IsZero() {
			start = time.Now()
		}
		if err := h.waitLocked(ctx); err != nil {
			return 0, err
		}
	}
}

func (h *hotWriter) admissibleLocked(class Class, raw int64) bool {
	switch {
	case h.pending[ClassLive]+h.pending[ClassBulk] > h.hot.PendingBytes:
		return false
	case h.unfoldedLocked() > uint64(h.hot.MaxUnfoldedEvents):
		return false
	case class == ClassLive:
		return true
	case h.liveWaiting.Load() > 0:
		return false
	case h.bulkFrozen >= h.hot.UploadConcurrency:
		return false
	case h.bulkPermits > 0 && h.bulkPermits+raw > h.hot.BulkPendingBytes:
		// A chunk larger than the whole pool is admitted alone.
		return false
	}
	return true
}

// bulkChunkLocked sizes the next bulk chunk so that it fits the open bulk
// batch and the open block, and returns its raw bytes.
func (h *hotWriter) bulkChunkLocked(events []segment.Event) (int, int64) {
	room := h.hot.BulkChunkMaxEvents
	if b := h.batch; b != nil && b.class == ClassBulk {
		room -= b.n
	}
	blockRoom := h.cfg.MaxEventsPerBlock
	if h.block != nil {
		blockRoom -= len(h.block.events)
	}
	n := min(len(events), room, blockRoom)
	var raw int64
	for i := range events[:n] {
		raw += rawEventBytes(&events[i])
	}
	return n, raw
}

// settleCreditLocked hands the chunk's permits to the open bulk batch, or
// releases them when no open batch holds any of the chunk's events. A
// batch the chunk froze already took them (freezeLocked).
func (h *hotWriter) settleCreditLocked() {
	if h.bulkCredit == 0 {
		return
	}
	if b := h.batch; b != nil && b.class == ClassBulk {
		b.permit += h.bulkCredit
	} else {
		h.bulkPermits -= h.bulkCredit
		h.signalLocked()
	}
	h.bulkCredit = 0
}

func (h *hotWriter) unfoldedLocked() uint64 {
	if h.foldedNext >= h.committedNext {
		return 0
	}
	return h.committedNext - h.foldedNext
}

func (h *hotWriter) addPendingLocked(c Class, delta int64) {
	h.pending[c] += delta
	h.cfg.Metrics.setHotPending(c, h.pending[c])
}

// committed releases what a batch held once it commits.
func (h *hotWriter) committed(b *hotBatch) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.addPendingLocked(b.class, -b.raw)
	h.bulkPermits -= b.permit
	if b.class == ClassBulk {
		h.bulkFrozen--
	}
	h.committedNext = b.last() + 1
	h.cfg.Metrics.setHotUnfolded(h.unfoldedLocked())
	h.signalLocked()
}

// folded releases a folded block's events from the unfolded cap. Folds run
// in seq order, so the watermark only moves forward.
func (h *hotWriter) folded(last uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if last+1 <= h.foldedNext {
		return
	}
	h.foldedNext = last + 1
	h.cfg.Metrics.setHotUnfolded(h.unfoldedLocked())
	h.signalLocked()
}

// waitLocked releases mu until the next signal or ctx's end.
func (h *hotWriter) waitLocked(ctx context.Context) error {
	ch := h.changed
	h.waiters++
	h.mu.Unlock()
	var err error
	select {
	case <-ch:
	case <-ctx.Done():
		err = ctx.Err()
	}
	h.mu.Lock()
	h.waiters--
	return err
}

func (h *hotWriter) signalLocked() {
	if h.waiters == 0 {
		return
	}
	close(h.changed)
	h.changed = make(chan struct{})
}

// tokenBucket pays for live inline batches in encoded frame bytes (design
// §10.5 rule 3). The frame is not encoded yet when a batch freezes, so a
// batch takes its raw bytes, an upper bound in practice, and settles the
// difference once prepare has encoded it. The rate then holds over frame
// bytes, as configured.
type tokenBucket struct {
	metrics *Metrics

	mu     sync.Mutex
	rate   float64 // bytes per second; also the burst
	tokens float64
	last   time.Time
}

func newTokenBucket(rate int64, m *Metrics) *tokenBucket {
	b := &tokenBucket{metrics: m, rate: float64(rate), tokens: float64(rate), last: time.Now()}
	m.setHotInlineTokens(b.tokens)
	return b
}

func (b *tokenBucket) refillLocked() {
	now := time.Now()
	if d := now.Sub(b.last); d > 0 {
		b.tokens = min(b.rate, b.tokens+b.rate*d.Seconds())
	}
	b.last = now
}

// take takes n tokens if the bucket has them.
func (b *tokenBucket) take(n int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillLocked()
	ok := b.tokens >= float64(n)
	if ok {
		b.tokens -= float64(n)
	}
	b.metrics.setHotInlineTokens(b.tokens)
	return ok
}

// settle returns delta tokens, the estimate minus the frame's size. A
// frame larger than its estimate draws the bucket below zero.
func (b *tokenBucket) settle(delta int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillLocked()
	b.tokens = min(b.rate, b.tokens+float64(delta))
	b.metrics.setHotInlineTokens(b.tokens)
}
