package subscribe

import (
	"sync"

	"github.com/bluesky-social/jetstream-v2/segment"
)

// Entry is one event held in the hot ring: the decoded event plus a
// lazily-memoized wire encoding shared across every caught-up subscriber.
// Encode runs at most once per Entry; the result (bytes or sentinel error)
// is cached so the shared path reproduces deliverEvent's branching exactly.
type Entry struct {
	Event *segment.Event

	once sync.Once
	body []byte
	err  error

	extendedOnce sync.Once
	extendedBody []byte
	extendedErr  error

	// encodeFn defaults to Encode; overridable in tests.
	encodeFn func(*segment.Event) ([]byte, error)

	// encodeExtendedFn defaults to EncodeExtended; overridable in tests.
	encodeExtendedFn func(*segment.Event) ([]byte, error)
}

// newEntry wraps ev. The event's Payload is treated as read-only (it may
// alias a shared decompressed block buffer).
func newEntry(ev *segment.Event) *Entry {
	return &Entry{Event: ev, encodeFn: Encode}
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

// EncodedExtended returns the memoized extended wire encoding for this entry.
func (e *Entry) EncodedExtended() ([]byte, error) {
	e.extendedOnce.Do(func() {
		fn := e.encodeExtendedFn
		if fn == nil {
			fn = EncodeExtended
		}
		e.extendedBody, e.extendedErr = fn(e.Event)
	})
	return e.extendedBody, e.extendedErr
}

// approxBytes estimates the entry's memory footprint for the hot ring's
// byte budget: the payload plus the small fixed-size string fields. The
// memoized encoding is intentionally excluded — it is bounded by the same
// ring and counting it would double-count the shared bytes.
func (e *Entry) approxBytes() int {
	ev := e.Event
	return len(ev.Payload) + len(ev.DID) + len(ev.Collection) +
		len(ev.Rkey) + len(ev.Rev) + entryFixedOverhead
}

// entryFixedOverhead approximates the per-entry struct + pointer overhead
// so a flood of tiny events still has a bounded count in the ring.
const entryFixedOverhead = 128
