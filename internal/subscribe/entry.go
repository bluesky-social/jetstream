package subscribe

import (
	"sync"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/segment"
)

// Entry is one event visible to subscribe readers: the decoded event plus a
// lazily-memoized wire encoding shared across subscribers while it is resident.
// Encode runs at most once per Entry; the result (bytes or sentinel error)
// is cached so the shared path reproduces deliverEvent's branching exactly.
type Entry struct {
	Event *segment.Event

	once sync.Once
	body []byte
	err  error

	v2Once sync.Once
	v2Body []byte
	v2Err  error

	compressedOnce sync.Once
	compressedBody []byte
	compressedErr  error

	compressedV2Once sync.Once
	compressedV2Body []byte
	compressedV2Err  error

	// encodeFn defaults to Encode; overridable in tests.
	encodeFn func(*segment.Event) ([]byte, error)

	// encodeV2Fn defaults to EncodeV2; overridable in tests.
	encodeV2Fn func(*segment.Event) ([]byte, error)
}

// newEntry wraps ev. The event's Payload is treated as read-only (it may
// alias a shared decompressed block buffer).
func newEntry(ev *segment.Event) *Entry {
	return &Entry{Event: ev, encodeFn: Encode}
}

func entryFromReadLog(le *ingest.ReadLogEntry) *Entry {
	if memo := le.LoadMemo(); memo != nil {
		if e, ok := memo.(*Entry); ok {
			return e
		}
	}
	e := newEntry(le.Event())
	if memo := le.LoadOrStoreMemo(e); memo != nil {
		if got, ok := memo.(*Entry); ok {
			return got
		}
	}
	return e
}

// Encoded returns the memoized wire encoding for this entry. The first
// caller runs the encode; all others return the cached result. err may be
// errSkipEvent (caller advances without sending) or an encode error
// (caller skips + logs), matching deliverEvent.
func (e *Entry) Encoded() ([]byte, error) {
	e.once.Do(func() {
		fn := e.encodeFn
		if fn == nil {
			fn = Encode
		}
		e.body, e.err = fn(e.Event)
	})
	return e.body, e.err
}

// EncodedV2 returns the memoized /subscribe-v2 wire encoding for this entry.
func (e *Entry) EncodedV2() ([]byte, error) {
	e.v2Once.Do(func() {
		fn := e.encodeV2Fn
		if fn == nil {
			fn = EncodeV2
		}
		e.v2Body, e.v2Err = fn(e.Event)
	})
	return e.v2Body, e.v2Err
}

// Compressed returns the memoized v1-shape JSON encoding compressed as a
// single zstd frame with the custom dictionary. It derives from Encoded
// (so the JSON encode is never duplicated) and runs the compression at
// most once per Entry, shared across every caught-up zstd subscriber. The
// error contract matches Encoded: errSkipEvent (advance, don't send) or an
// encode error (skip + log) is returned unchanged.
func (e *Entry) Compressed() ([]byte, error) {
	e.compressedOnce.Do(func() {
		body, err := e.Encoded()
		if err != nil {
			e.compressedErr = err
			return
		}
		e.compressedBody = compressFrame(body)
	})
	return e.compressedBody, e.compressedErr
}

// CompressedV2 is Compressed for the /subscribe-v2 wire shape.
func (e *Entry) CompressedV2() ([]byte, error) {
	e.compressedV2Once.Do(func() {
		body, err := e.EncodedV2()
		if err != nil {
			e.compressedV2Err = err
			return
		}
		e.compressedV2Body = compressFrame(body)
	})
	return e.compressedV2Body, e.compressedV2Err
}

// approxBytes estimates the entry's memory footprint for retention accounting:
// the payload plus the small fixed-size string fields. The
// memoized encodings are intentionally excluded — they are bounded by the
// same retention window (evicted FIFO with the entry) and counting them would
// double-count the shared bytes. An entry may memoize up to four payloads
// (v1/v2 JSON × plain/zstd) when a mix of subscriber types is
// connected; the off-budget overhang stays O(ring length).
func (e *Entry) approxBytes() int {
	ev := e.Event
	return len(ev.Payload) + len(ev.DID) + len(ev.Collection) +
		len(ev.Rkey) + len(ev.Rev) + entryFixedOverhead
}

// entryFixedOverhead approximates the per-entry struct + pointer overhead
// so a flood of tiny events still has a bounded count in the ring.
const entryFixedOverhead = 128
