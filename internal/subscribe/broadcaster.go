package subscribe

import (
	"log/slog"
	"sync"

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
