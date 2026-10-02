package ingest

import (
	"fmt"
	"sync"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/segment"
)

// DefaultReadLogRetentionBytes bounds the durable tail retained in the writer's
// in-memory read log. Events at or above the durable watermark are pinned even
// when this budget is zero.
const DefaultReadLogRetentionBytes int64 = 256 << 20

// ReadLogEntry is one stable event handle held by the writer-owned readable
// log.
type ReadLogEntry = catalog.LogEntry

// ReadableLog is the writer-owned ordered log of appended events. Entries are
// present from seq allocation until eviction, and eviction never advances the
// floor beyond the durable watermark.
//
// A writer's log is dense. A FollowerLog may also hold vacant seqs (nil
// entries): seqs compaction removed from a sealed block before the follower
// read it (design §11.1). Readers skip them.
type ReadableLog struct {
	mu       sync.RWMutex
	entries  []*ReadLogEntry
	baseSeq  uint64
	tipSeq   uint64
	durable  uint64
	curBytes int64
	// pinnedBytes is the running sum of entry bytes for entries at or
	// above durable (Seq >= durable) — the events retained because they
	// are not yet durably flushed. Maintained incrementally so
	// publishMetricsLocked stays O(1): a prior per-append linear scan of
	// every resident entry made append O(n) and, as the resident set grew
	// to the byte cap (~500k entries), throttled live ingest into an
	// O(n^2) collapse that could not keep pace with the firehose.
	pinnedBytes int64
	maxBytes    int64
	notify      chan struct{}
	metrics     *Metrics
}

func newReadableLog(nextSeq uint64, maxBytes int64, metrics *Metrics) *ReadableLog {
	if maxBytes < 0 {
		maxBytes = 0
	}
	l := &ReadableLog{
		baseSeq:  nextSeq,
		tipSeq:   nextSeq,
		durable:  nextSeq,
		maxBytes: maxBytes,
		notify:   make(chan struct{}),
		metrics:  metrics,
	}
	l.publishMetricsLocked()
	return l
}

func (l *ReadableLog) append(ev *segment.Event) {
	if l == nil {
		return
	}
	l.appendEntry(catalog.NewLogEntry(ev))
}

// appendEntry is append for a caller that already copied the event, so hot
// mode's open block can share the entry's copy instead of making another.
func (l *ReadableLog) appendEntry(entry *ReadLogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if seq := entry.Event().Seq; seq != l.tipSeq {
		panic(fmt.Sprintf("ingest: readable log append seq %d, want %d", seq, l.tipSeq))
	}
	l.entries = append(l.entries, entry)
	l.tipSeq++
	l.curBytes += entry.ApproxBytes()
	// A freshly appended entry has Seq == old tipSeq >= durable, so it is
	// pinned until a later advanceDurable moves past it.
	l.pinnedBytes += entry.ApproxBytes()
	l.evictLocked()
	old := l.notify
	l.notify = make(chan struct{})
	l.publishMetricsLocked()
	close(old)
}

// skip advances the tip to nextSeq, leaving the seqs in between vacant.
func (l *ReadableLog) skip(nextSeq uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if nextSeq < l.tipSeq {
		panic(fmt.Sprintf("ingest: readable log skip to %d, below tip %d", nextSeq, l.tipSeq))
	}
	if nextSeq == l.tipSeq {
		return
	}
	for ; l.tipSeq < nextSeq; l.tipSeq++ {
		l.entries = append(l.entries, nil)
	}
	l.evictLocked()
	l.publishMetricsLocked()
	// A reader parked at the old tip must not wait on a vacancy; the next
	// append wakes it either way, so the notify channel is left alone.
}

func (l *ReadableLog) advanceDurable(nextSeq uint64) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if nextSeq < l.durable {
		panic(fmt.Sprintf("ingest: readable log durable regression %d -> %d", l.durable, nextSeq))
	}
	if nextSeq > l.tipSeq {
		panic(fmt.Sprintf("ingest: readable log durable %d beyond tip %d", nextSeq, l.tipSeq))
	}
	// Entries with Seq in [old durable, nextSeq) transition from pinned to
	// durable; drop their bytes from the pinned accumulator. They remain
	// resident (subject to eviction) but no longer count as pinned.
	for seq := l.durable; seq < nextSeq; seq++ {
		if seq < l.baseSeq {
			continue // already evicted; never counted
		}
		idx := seq - l.baseSeq
		if idx < uint64(len(l.entries)) {
			l.pinnedBytes -= l.entries[idx].ApproxBytes()
		}
	}
	l.durable = nextSeq
	l.evictLocked()
	l.publishMetricsLocked()
}

func (l *ReadableLog) evictLocked() {
	// Vacant entries cost nothing against the budget, but a leading run of
	// them below durable is dropped regardless so it cannot pin the slice.
	for len(l.entries) > 0 && l.baseSeq < l.durable && (l.curBytes > l.maxBytes || l.entries[0] == nil) {
		evicted := l.entries[0]
		l.curBytes -= evicted.ApproxBytes()
		l.entries[0] = nil
		l.entries = l.entries[1:]
		l.baseSeq++
	}
	if len(l.entries) == 0 {
		l.baseSeq = l.tipSeq
	}
	if l.baseSeq > l.durable {
		panic(fmt.Sprintf("ingest: readable log floor %d advanced beyond durable %d", l.baseSeq, l.durable))
	}
}

// ReadFrom returns a copied resident suffix for cursor when cursor is inside
// the log. If cursor is at or above TipSeq, it returns atTip=true with a notify
// channel that closes on the next append. If cursor is below FloorSeq, callers
// must read cold durable storage.
func (l *ReadableLog) ReadFrom(cursor uint64, max int) (entries []*ReadLogEntry, notify <-chan struct{}, ok bool, atTip bool) {
	if l == nil {
		return nil, nil, false, false
	}
	if max <= 0 {
		max = 1
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	if cursor >= l.baseSeq && cursor < l.tipSeq {
		idx := cursor - l.baseSeq
		if idx >= uint64(len(l.entries)) {
			panic(fmt.Sprintf("ingest: readable log corrupt index %d len %d base %d tip %d", idx, len(l.entries), l.baseSeq, l.tipSeq))
		}
		for _, e := range l.entries[idx:] {
			if e == nil {
				continue
			}
			if entries == nil {
				entries = make([]*ReadLogEntry, 0, min(max, len(l.entries)-int(idx)))
			}
			entries = append(entries, e)
			if len(entries) == max {
				break
			}
		}
		if len(entries) > 0 {
			return entries, nil, true, false
		}
		// Only vacancies remain up to the tip: wait there.
		return nil, l.notify, false, true
	}
	if cursor >= l.tipSeq {
		return nil, l.notify, false, true
	}
	return nil, nil, false, false
}

func (l *ReadableLog) FloorSeq() uint64 {
	if l == nil {
		return 0
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.baseSeq
}

func (l *ReadableLog) TipSeq() uint64 {
	if l == nil {
		return 0
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.tipSeq
}

func (l *ReadableLog) DurableSeq() uint64 {
	if l == nil {
		return 0
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.durable
}

func (l *ReadableLog) PendingForDID(did string) []segment.Event {
	if l == nil {
		return nil
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make([]segment.Event, 0)
	for _, entry := range l.entries {
		ev := entry.Event()
		if ev == nil || ev.Seq < l.durable || ev.DID != did {
			continue
		}
		cp := *ev
		cp.Payload = append([]byte(nil), ev.Payload...)
		out = append(out, cp)
	}
	return out
}

func (l *ReadableLog) publishMetricsLocked() {
	if l.metrics == nil {
		return
	}
	pinned := l.pinnedBytes
	l.metrics.setReadLogBytes(l.curBytes)
	l.metrics.setReadLogPinnedBytes(pinned)
	l.metrics.setReadLogPinnedOverrunBytes(maxInt64(0, pinned-l.maxBytes))
	l.metrics.setReadLogFloorSeq(l.baseSeq)
	l.metrics.setReadLogDurableSeq(l.durable)
}

func maxInt64(a, b int64) int64 {
	if a >= b {
		return a
	}
	return b
}

var _ catalog.HotLog = (*ReadableLog)(nil)
