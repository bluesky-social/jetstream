package client

import (
	"context"
	"fmt"
	"sync"

	"github.com/jcalabro/gt"
)

// liveSink absorbs the live tail during the backfill-to-live cutover. It has
// two phases:
//
//	buffering — while the backfill downloads, every live event is appended to
//	            the buffer (raw frame, for durable replay).
//	forwarding — after flipAndDrain, live events are passed straight to the
//	             batcher in the main emission goroutine.
//
// The phase flip is serialized against onLive by mu, so the live consumer
// goroutine can never append to the buffer once forwarding has begun: events
// that arrive during the flip are emitted directly instead. This keeps a
// single, ordered emission path with no concurrent yield.
type liveSink struct {
	buf     Buffer
	matcher *Matcher
	mode    recordDecodeMode // raw vs. map record decode for drained buffered frames

	mu         sync.Mutex
	forwarding bool
	forward    func(Event) bool // set at flip; called under mu while forwarding
	// fatalErr records a buffering-phase failure (e.g. a buffer append error)
	// that stopped the live consumer. It is surfaced by flipAndDrain so the
	// engine aborts the cutover rather than replaying a truncated buffer and
	// silently dropping the live frames that never got persisted.
	fatalErr error
}

func newLiveSink(buf Buffer, matcher *Matcher, mode recordDecodeMode) *liveSink {
	return &liveSink{buf: buf, matcher: matcher, mode: mode}
}

// onLive is the live consumer's emit callback. raw is the verbatim JSON frame
// (nil on an error report). During buffering it stores the raw frame; after the
// flip it forwards the decoded event directly. It returns false only to stop
// the live consumer (when the downstream batcher stops).
func (s *liveSink) onLive(ev *Event, raw []byte, err error) bool {
	if err != nil {
		// A live read/decode hiccup during cutover: keep tailing. The engine's
		// error path surfaces fatal conditions elsewhere.
		return true
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.forwarding {
		// Filter, then forward straight to the batcher.
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
// frames after coveredThrough (the sealed tip the backfill already covered),
// emitting each via emit (filtered, deduped), then installs the
// forward path so subsequent live events go straight through. Held under mu so
// no live event is lost or doubly-delivered across the flip.
//
// coveredThrough is an optional so the 0-based seq space's 0 is not overloaded:
// Some(seq) means the backfill emitted through seq (drain seq > coveredThrough,
// and dedup post-flip overlap at or below it); None means the backfill covered
// NOTHING (empty archive), so the buffer drains from the very beginning —
// including the first-ever event at seq 0 — and the post-flip forward path has
// no lower dedup floor until it has itself delivered an event.
func (s *liveSink) flipAndDrain(ctx context.Context, coveredThrough gt.Option[uint64], emit func(Event) bool, emitErr func(error) bool) error {
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

	// lastDelivered tracks the highest seq actually drained, as the post-flip
	// dedup floor. Seed it from coveredThrough (None when the backfill covered
	// nothing) so a drain that emits nothing leaves the floor unset and a
	// post-flip seq 0 is not swallowed.
	lastDelivered := coveredThrough
	for fr, err := range s.buf.Replay(ctx, coveredThrough) {
		if err != nil {
			return err
		}
		ev, decErr := decodeLiveFrame(fr.Data, s.mode)
		if decErr != nil {
			emitErr(fmt.Errorf("jetstream: corrupt buffered live frame seq=%d: %w", fr.Seq, decErr))
			continue
		}
		lastDelivered = gt.Some(fr.Seq)
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

	// Install the forward path. Dedup against the highest seq drained (or the
	// coveredThrough floor) so a live event still sitting in the consumer's
	// pipeline (already buffered AND about to arrive again post-flip) is not
	// double-emitted. A None floor (empty archive, nothing drained) lets the
	// first post-flip event — even seq 0 — through.
	lastForwarded := lastDelivered
	s.forward = func(ev Event) bool {
		if lastForwarded.HasVal() && ev.Seq <= lastForwarded.Val() {
			return true // dedup overlap
		}
		lastForwarded = gt.Some(ev.Seq)
		return emit(ev)
	}
	s.forwarding = true
	return nil
}

// wantLive applies the caller's exact DID/collection/seq filter to a live
// event. A nil matcher matches everything.
func (s *liveSink) wantLive(ev *Event) bool {
	if s.matcher == nil {
		return true
	}
	se := segmentViewOf(ev)
	return s.matcher.Wants(&se)
}
