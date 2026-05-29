package subscribe

import (
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/bluesky-social/jetstream-v2/segment"
)

// Broadcaster fans live events out to N subscribers. Construct with New;
// Publish from a single goroutine; Subscribe and unsubscribe from any
// goroutine. Safe under concurrent use.
type Broadcaster struct {
	cfg     Config
	logger  *slog.Logger
	metrics *Metrics

	mu          sync.RWMutex
	subscribers map[uint64]*subscriber
	nextID      uint64
}

// subscriberPhase, when non-nil, signals "this subscriber is in
// lookback mode": Publish writes events into ring rather than into
// events. Set to nil to switch back to live mode.
type subscriberPhase struct {
	ring     *RingBuf
	overflow atomic.Bool
}

// subscriber is one connected websocket client's slot in the broadcaster.
// events is buffered so a stalled receiver can't backpressure Publish;
// done is closed exactly once when this subscriber is retired (slow-drop
// or client disconnect) so the handler's writer loop can exit without
// polling. once guards the close-and-account work in dropByID against
// concurrent unsubscribe and overflow paths racing each other.
type subscriber struct {
	id     uint64
	events chan *segment.Event
	done   chan struct{}
	once   sync.Once
	phase  atomic.Pointer[subscriberPhase] // nil = live mode
}

// New validates cfg and returns a Broadcaster ready to Publish.
func New(cfg Config) (*Broadcaster, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg.applyDefaults()

	return &Broadcaster{
		cfg:         cfg,
		logger:      cfg.Logger.With(slog.String("component", "subscribe/broadcaster")),
		metrics:     cfg.Metrics,
		subscribers: make(map[uint64]*subscriber),
	}, nil
}

// Subscribe registers a new subscriber. Returns:
//
//   - the channel from which the subscriber reads *segment.Event,
//   - a "done" channel closed exactly once when this subscriber is
//     dropped (slow-drop or unsubscribe), so the handler can exit its
//     writer loop without polling,
//   - an unsubscribe func that's safe to call multiple times.
//
// The events channel is buffered to cfg.SubscriberBufferSize; on overflow
// the broadcaster drops the subscriber rather than blocking Publish.
func (b *Broadcaster) Subscribe() (<-chan *segment.Event, <-chan struct{}, func()) {
	b.mu.Lock()
	id := b.nextID
	b.nextID++
	s := &subscriber{
		id:     id,
		events: make(chan *segment.Event, b.cfg.SubscriberBufferSize),
		done:   make(chan struct{}),
	}
	b.subscribers[id] = s
	b.mu.Unlock()

	b.metrics.incSubscribers()

	return s.events, s.done, func() { b.dropByID(id, dropReasonClient) }
}

// Publish fans evt out to all current subscribers. Non-blocking: if a
// subscriber's channel is full, that subscriber is dropped (its done
// channel is closed; subsequent receives on its event channel will
// block forever, which the handler avoids by selecting on done first).
//
// Safe to call from a single producer goroutine. The aliasing rule on
// live.Config.OnEvent applies: evt must not be retained across the call.
func (b *Broadcaster) Publish(evt *segment.Event) {
	b.metrics.incEventsPublished()

	// Snapshot under read lock so registrations don't block sends.
	b.mu.RLock()
	subs := make([]*subscriber, 0, len(b.subscribers))
	for _, s := range b.subscribers {
		subs = append(subs, s)
	}
	b.mu.RUnlock()

	for _, s := range subs {
		// Each subscriber gets a fresh struct so the receiver can hold
		// it past Publish's return without aliasing the OnEvent caller's
		// stack frame. Payload is shared (read-only at this layer).
		e := *evt
		if ph := s.phase.Load(); ph != nil {
			ok, sealed := ph.ring.Push(&e)
			if ok {
				continue
			}
			if !sealed {
				// Genuine overflow: the replay goroutine is too slow.
				ph.overflow.Store(true)
				continue
			}
			// Ring sealed at the live handoff: the replay goroutine has
			// stopped reading it. Fall through to the live channel so the
			// event is delivered rather than stranded. The replay loop has
			// already cleared the phase pointer for subsequent Publishes;
			// this branch only covers the single in-flight event that
			// observed the phase pointer before the clear.
		}
		select {
		case s.events <- &e:
			b.metrics.observeQueueDepth(len(s.events))
		default:
			b.dropByID(s.id, dropReasonOverflow)
		}
	}
}

type dropReason int

const (
	dropReasonClient dropReason = iota + 1
	dropReasonOverflow
)

// dropByID retires a subscriber: removes it from the map and closes its
// done channel exactly once. Safe to call from Publish (overflow path),
// from the unsubscribe func returned by Subscribe, or both racing — the
// once.Do guarantees idempotence and the events channel is intentionally
// left open so concurrent Publish snapshots cannot panic with
// "send on closed channel".
func (b *Broadcaster) dropByID(id uint64, reason dropReason) {
	b.mu.Lock()
	s, ok := b.subscribers[id]
	if !ok {
		b.mu.Unlock()
		return
	}
	delete(b.subscribers, id)
	b.mu.Unlock()

	s.once.Do(func() {
		close(s.done)
		b.metrics.decSubscribers()
		switch reason {
		case dropReasonOverflow:
			b.metrics.incSlowDrops()
			b.logger.Warn("dropped slow subscriber",
				"subscriber_id", id,
				"buffer_size", b.cfg.SubscriberBufferSize,
			)
		case dropReasonClient:
			b.metrics.incCleanDisconnects()
		}
	})
}

// SubscribeForLookback registers a new subscriber in lookback mode
// from the start. The returned ring receives every Publish until
// SwitchToLive is called. The returned id is the broadcaster's
// internal subscriber id (used to clear the phase or unsubscribe).
//
// ringSize must be > 0; it sets the per-subscriber bounded buffer
// capacity for the lookback window. On overflow, the broadcaster
// sets the phase's overflow flag, which the replay engine observes
// and uses to trigger replay restart.
func (b *Broadcaster) SubscribeForLookback(ringSize int) (uint64, *RingBuf) {
	b.mu.Lock()
	id := b.nextID
	b.nextID++
	s := &subscriber{
		id:     id,
		events: make(chan *segment.Event, b.cfg.SubscriberBufferSize),
		done:   make(chan struct{}),
	}
	ph := &subscriberPhase{ring: NewRingBuf(ringSize)}
	s.phase.Store(ph)
	b.subscribers[id] = s
	b.mu.Unlock()

	b.metrics.incSubscribers()
	return id, ph.ring
}

// SwitchToLive completes the lookback→live handoff for subID. It
// clears the phase pointer (so steady-state Publishes skip the ring
// lock), then atomically seals and drains the ring. It returns the
// live events channel plus the events that were still buffered in the
// ring at seal time, in FIFO (seq) order. Returns (nil, nil) if subID
// is not a known subscriber.
//
// The seal-and-drain is the crux of the no-loss guarantee: a Publish
// that loaded the (pre-clear) phase pointer and is about to Push
// either lands in the ring before SealAndDrain takes the lock (so it
// is in the returned slice) or observes the sealed flag and reroutes
// to the live channel. There is no window in which an event is pushed
// into a ring nobody drains. The caller MUST emit the returned drained
// events before pumping the live channel to preserve seq order.
func (b *Broadcaster) SwitchToLive(subID uint64) (<-chan *segment.Event, []*segment.Event) {
	b.mu.RLock()
	s, ok := b.subscribers[subID]
	b.mu.RUnlock()
	if !ok {
		return nil, nil
	}
	ph := s.phase.Load()
	s.phase.Store(nil)
	if ph == nil {
		return s.events, nil
	}
	drained := ph.ring.SealAndDrain()
	return s.events, drained
}

// SubscriberOverflowed reports whether the subscriber's lookback ring
// has dropped at least one event due to the writer outpacing the
// replay engine. The replay engine polls this between blocks.
// Returns false for live-mode subscribers (no phase) and unknown ids.
func (b *Broadcaster) SubscriberOverflowed(subID uint64) bool {
	b.mu.RLock()
	s, ok := b.subscribers[subID]
	b.mu.RUnlock()
	if !ok {
		return false
	}
	ph := s.phase.Load()
	if ph == nil {
		return false
	}
	return ph.overflow.Load()
}

// ResetSubscriberOverflow clears the overflow flag and replaces the
// ring with a fresh one. Called by the replay engine after dropping
// a saturated ring and restarting replay at an updated cursor.
// Returns nil if subID is not a known subscriber.
func (b *Broadcaster) ResetSubscriberOverflow(subID uint64, ringSize int) *RingBuf {
	b.mu.RLock()
	s, ok := b.subscribers[subID]
	b.mu.RUnlock()
	if !ok {
		return nil
	}
	ph := &subscriberPhase{ring: NewRingBuf(ringSize)}
	s.phase.Store(ph)
	return ph.ring
}

// Unsubscribe drops the subscriber identified by subID, mirroring the
// closure returned by Subscribe. Needed because SubscribeForLookback's
// return shape is different (uses id+ring rather than the standard
// channel+done+closure tuple).
func (b *Broadcaster) Unsubscribe(subID uint64) {
	b.dropByID(subID, dropReasonClient)
}
