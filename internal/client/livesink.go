package client

import (
	"context"
	"fmt"
	"sync"
)

// liveSink absorbs the live tail during the backfill-to-live cutover. It has
// two phases:
//
//	buffering — while the backfill downloads, every live event is appended to
//	            the buffer (raw frame, for durable replay) and every live
//	            tombstone is folded into the suppressor so later-downloaded
//	            historical rows are suppressed by tombstones already seen live.
//	forwarding — after flipAndDrain, live events are passed straight to the
//	             batcher in the main emission goroutine.
//
// The phase flip is serialized against onLive by mu, so the live consumer
// goroutine can never append to the buffer once forwarding has begun: events
// that arrive during the flip are emitted directly instead. This keeps a
// single, ordered emission path with no concurrent yield.
type liveSink struct {
	buf        Buffer
	suppressor *Suppressor
	matcher    *Matcher

	mu         sync.Mutex
	forwarding bool
	forward    func(Event) bool // set at flip; called under mu while forwarding
	// fatalErr records a buffering-phase failure (e.g. a buffer append error)
	// that stopped the live consumer. It is surfaced by flipAndDrain so the
	// engine aborts the cutover rather than replaying a truncated buffer and
	// silently dropping the live frames that never got persisted.
	fatalErr error
}

func newLiveSink(buf Buffer, suppressor *Suppressor, matcher *Matcher) *liveSink {
	return &liveSink{buf: buf, suppressor: suppressor, matcher: matcher}
}

// onLive is the live consumer's emit callback. raw is the verbatim JSON frame
// (nil on an error report). During buffering it stores the raw frame and folds
// tombstones; after the flip it forwards the decoded event directly. It returns
// false only to stop the live consumer (when the downstream batcher stops).
func (s *liveSink) onLive(ev *Event, raw []byte, err error) bool {
	if err != nil {
		// A live read/decode hiccup during cutover: keep tailing. The engine's
		// error path surfaces fatal conditions elsewhere.
		return true
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Always fold tombstones so the suppressor reflects live deletes/updates
	// regardless of phase (a delete arriving now must suppress a historical
	// create downloaded later).
	s.observeTombstone(ev)

	if s.forwarding {
		// Filter + suppress, then forward straight to the batcher.
		if !s.wantLive(ev) {
			return true
		}
		return s.forward(*ev)
	}

	// Buffering: persist the verbatim frame for replay at drain time. Storing
	// the raw bytes (re-decoded on replay) avoids a lossy re-marshal of the
	// decoded event.
	if aErr := s.buf.Append([]LiveFrame{{Seq: ev.Seq, Data: append([]byte(nil), raw...)}}); aErr != nil {
		// A buffer append failure is fatal to the cutover guarantee: the buffer
		// is now missing this frame (and the consumer is about to exit, so it
		// will miss every later frame too). Record it and stop the live
		// consumer; flipAndDrain surfaces fatalErr instead of replaying a
		// truncated buffer.
		s.fatalErr = fmt.Errorf("jetstream: append live buffer seq=%d: %w", ev.Seq, aErr)
		return false
	}
	return true
}

// flipAndDrain transitions from buffering to forwarding: it drains buffered
// frames with seq > throughSeq (the sealed tip the backfill already covered),
// emitting each via emit (filtered + suppressed, deduped), then installs the
// forward path so subsequent live events go straight through. Held under mu so
// no live event is lost or doubly-delivered across the flip.
func (s *liveSink) flipAndDrain(ctx context.Context, throughSeq uint64, emit func(Event) bool, emitErr func(error) bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// A buffering-phase append failure means the buffer is incomplete; aborting
	// here is mandatory — replaying it would silently drop the un-persisted live
	// frames. Install a stop-forwarder so any late live event unwinds the
	// consumer rather than re-entering the buffering branch.
	if s.fatalErr != nil {
		s.forwarding = true
		s.forward = func(Event) bool { return false }
		return s.fatalErr
	}

	var lastDrained uint64
	for fr, err := range s.buf.Replay(ctx, throughSeq) {
		if err != nil {
			return err
		}
		ev, decErr := decodeLiveFrame(fr.Data)
		if decErr != nil {
			emitErr(fmt.Errorf("jetstream: corrupt buffered live frame seq=%d: %w", fr.Seq, decErr))
			continue
		}
		lastDrained = fr.Seq
		if !s.wantLive(&ev) {
			continue
		}
		if !emit(ev) {
			// Consumer stopped mid-drain; install a no-op forward so onLive
			// stops the live consumer on its next call.
			s.forwarding = true
			s.forward = func(Event) bool { return false }
			return nil
		}
	}

	// Install the forward path. Dedup against the highest seq drained so a live
	// event still sitting in the consumer's pipeline (already buffered AND
	// about to arrive again post-flip) is not double-emitted.
	lastForwarded := max(throughSeq, lastDrained)
	s.forward = func(ev Event) bool {
		if ev.Seq <= lastForwarded {
			return true // dedup overlap
		}
		lastForwarded = ev.Seq
		return emit(ev)
	}
	s.forwarding = true
	return nil
}

// wantLive applies the exact filter and tombstone suppression to a live event.
// Tombstones (deletes), identity, account, and sync always pass the suppressor
// (only materializations are dropped); the matcher applies the user's filters.
func (s *liveSink) wantLive(ev *Event) bool {
	se := segmentViewOf(ev)
	if s.matcher != nil && !s.matcher.Wants(&se) {
		return false
	}
	if drop, _ := s.suppressor.ShouldDrop(&se); drop {
		return false
	}
	return true
}

// observeTombstone folds a live event into the suppressor if it is a tombstone.
func (s *liveSink) observeTombstone(ev *Event) {
	se := segmentViewOf(ev)
	_ = s.suppressor.ObserveLive(&se)
}
