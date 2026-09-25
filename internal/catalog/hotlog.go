package catalog

import (
	"sync/atomic"

	"github.com/bluesky-social/jetstream/segment"
)

// HotLog is the in-memory ordered log of recent events that serves live
// subscribers (design §11.4). Seqs in [FloorSeq, TipSeq) are resident; below
// FloorSeq a reader must go through the CatalogView. Local mode's HotLog is
// the ingest writer's readable log; the disaggregated follower feeds one from
// committed hot batches.
//
// The local readable log also answers PendingForDID for repo export. That is
// local-only: in disaggregated mode everything committed is already reachable
// through BlockRefs (design §11.9), so it is not part of this interface.
type HotLog interface {
	// ReadFrom returns up to max resident entries starting at cursor, with
	// ok=true. At or above TipSeq it returns atTip=true and a channel closed
	// on the next append. Below FloorSeq both are false.
	ReadFrom(cursor uint64, max int) (entries []*LogEntry, notify <-chan struct{}, ok bool, atTip bool)
	// FloorSeq is the smallest resident seq. Everything below it is durable.
	FloorSeq() uint64
	// TipSeq is one past the newest resident seq.
	TipSeq() uint64
	// DurableSeq is one past the newest seq whose block and metadata have
	// both committed.
	DurableSeq() uint64
}

// LogEntry is one stable event handle held by a HotLog. Subscribe stores its
// encode-once memo in the opaque slot so the log stays wire-format agnostic.
type LogEntry struct {
	event segment.Event
	bytes int64
	memo  atomic.Pointer[any]
}

// NewLogEntry copies ev, including its payload, into a new entry so the log
// never aliases a producer's buffers.
func NewLogEntry(ev *segment.Event) *LogEntry {
	cp := *ev
	cp.Payload = append([]byte(nil), ev.Payload...)
	bytes := int64(len(cp.Payload) + len(cp.DID) + len(cp.Collection) + len(cp.Rkey) + len(cp.Rev) + 128)
	return &LogEntry{event: cp, bytes: bytes}
}

// Event returns the resident event. Callers must treat it as immutable.
func (e *LogEntry) Event() *segment.Event {
	if e == nil {
		return nil
	}
	return &e.event
}

// ApproxBytes returns the entry's retention-budget estimate.
func (e *LogEntry) ApproxBytes() int64 {
	if e == nil {
		return 0
	}
	return e.bytes
}

// LoadMemo returns the opaque memo stored by another package.
func (e *LogEntry) LoadMemo() any {
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
func (e *LogEntry) LoadOrStoreMemo(memo any) any {
	if e == nil || memo == nil {
		return nil
	}
	p := &memo
	if e.memo.CompareAndSwap(nil, p) {
		return memo
	}
	return e.LoadMemo()
}
