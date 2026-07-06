package ingest

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/bluesky-social/jetstream/segment"
)

// DefaultReadLogRetentionBytes bounds the durable tail retained in the writer's
// in-memory read log. Events at or above the durable watermark are pinned even
// when this budget is zero.
const DefaultReadLogRetentionBytes int64 = 256 << 20

// ReadLogEntry is one stable event handle held by the writer-owned readable
// log. Subscribe stores its encode-once memo in the opaque slot so ingest stays
// wire-format agnostic.
type ReadLogEntry struct {
	event segment.Event
	bytes int64
	memo  atomic.Pointer[any]
}

// Event returns the resident event. Callers must treat it as immutable.
func (e *ReadLogEntry) Event() *segment.Event {
	if e == nil {
		return nil
	}
	return &e.event
}

// ApproxBytes returns the entry's retention-budget estimate.
func (e *ReadLogEntry) ApproxBytes() int64 {
	if e == nil {
		return 0
	}
	return e.bytes
}

// LoadMemo returns the opaque memo stored by another package.
func (e *ReadLogEntry) LoadMemo() any {
	if e == nil {
		return nil
	}
	p := e.memo.Load()
	if p == nil {
		return nil
	}
	return *p
}

// LoadOrStoreMemo stores memo exactly once and returns the winning value.
func (e *ReadLogEntry) LoadOrStoreMemo(memo any) any {
	if e == nil || memo == nil {
		return nil
	}
	p := &memo
	if e.memo.CompareAndSwap(nil, p) {
		return memo
	}
	return e.LoadMemo()
}

// ReadableLog is the writer-owned ordered log of appended events. Entries are
// present from seq allocation until eviction, and eviction never advances the
// floor beyond the durable watermark.
type ReadableLog struct {
	mu       sync.RWMutex
	entries  []*ReadLogEntry
	baseSeq  uint64
	tipSeq   uint64
	durable  uint64
	curBytes int64
	maxBytes int64
	notify   chan struct{}
	metrics  *Metrics
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
	entry := newReadLogEntry(ev)

	l.mu.Lock()
	defer l.mu.Unlock()
	if ev.Seq != l.tipSeq {
		panic(fmt.Sprintf("ingest: readable log append seq %d, want %d", ev.Seq, l.tipSeq))
	}
	l.entries = append(l.entries, entry)
	l.tipSeq++
	l.curBytes += entry.bytes
	l.evictLocked()
	old := l.notify
	l.notify = make(chan struct{})
	l.publishMetricsLocked()
	close(old)
}

func newReadLogEntry(ev *segment.Event) *ReadLogEntry {
	cp := *ev
	cp.Payload = append([]byte(nil), ev.Payload...)
	bytes := int64(len(cp.Payload) + len(cp.DID) + len(cp.Collection) + len(cp.Rkey) + len(cp.Rev) + 128)
	return &ReadLogEntry{event: cp, bytes: bytes}
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
	l.durable = nextSeq
	l.evictLocked()
	l.publishMetricsLocked()
}

func (l *ReadableLog) evictLocked() {
	for len(l.entries) > 0 && l.baseSeq < l.durable && l.curBytes > l.maxBytes {
		evicted := l.entries[0]
		l.curBytes -= evicted.bytes
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
		out := l.entries[idx:]
		if len(out) > max {
			out = out[:max]
		}
		entries = make([]*ReadLogEntry, len(out))
		copy(entries, out)
		return entries, nil, true, false
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
		if ev.Seq < l.durable || ev.DID != did {
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
	pinned := int64(0)
	for _, entry := range l.entries {
		if entry.event.Seq >= l.durable {
			pinned += entry.bytes
		}
	}
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
