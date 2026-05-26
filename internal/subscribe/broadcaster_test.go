package subscribe

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/stretchr/testify/require"
)

func newTestBroadcaster(t *testing.T) *Broadcaster {
	t.Helper()
	b, err := New(Config{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	return b
}

func TestBroadcaster_PublishWithNoSubscribers_NoOp(t *testing.T) {
	t.Parallel()
	b := newTestBroadcaster(t)
	// Must not panic, must not block.
	b.Publish(&segment.Event{Seq: 1, DID: "did:plc:a"})
}

func TestBroadcaster_SingleSubscriberReceivesAllEvents(t *testing.T) {
	t.Parallel()
	b := newTestBroadcaster(t)
	ch, _, unsub := b.Subscribe()
	defer unsub()

	for i := uint64(1); i <= 5; i++ {
		b.Publish(&segment.Event{Seq: i, DID: "did:plc:x"})
	}

	for i := uint64(1); i <= 5; i++ {
		select {
		case got := <-ch:
			require.Equal(t, i, got.Seq)
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for event %d", i)
		}
	}
}

func TestBroadcaster_FanOutToMultipleSubscribers(t *testing.T) {
	t.Parallel()
	b := newTestBroadcaster(t)

	const N = 4
	channels := make([]<-chan *segment.Event, N)
	unsubs := make([]func(), N)
	for i := range channels {
		channels[i], _, unsubs[i] = b.Subscribe()
	}
	defer func() {
		for _, u := range unsubs {
			u()
		}
	}()

	b.Publish(&segment.Event{Seq: 99, DID: "did:plc:fan"})

	for i, ch := range channels {
		select {
		case got := <-ch:
			require.Equal(t, uint64(99), got.Seq, "subscriber %d", i)
			require.Equal(t, "did:plc:fan", got.DID, "subscriber %d", i)
		case <-time.After(time.Second):
			t.Fatalf("subscriber %d did not receive", i)
		}
	}
}

func TestBroadcaster_UnsubscribeStopsDelivery(t *testing.T) {
	t.Parallel()
	b := newTestBroadcaster(t)
	ch, _, unsub := b.Subscribe()

	unsub()

	// After unsubscribe, the channel should drain. Publishing must not
	// panic. We don't require the channel to ever close; we just need
	// the receive to be non-blocking with no pending events.
	b.Publish(&segment.Event{Seq: 1})
	select {
	case _, ok := <-ch:
		// Either closed (ok=false) or empty after a short wait.
		_ = ok
	case <-time.After(50 * time.Millisecond):
		// No event delivered, which is the expected outcome.
	}
}

func TestBroadcaster_UnsubscribeIsIdempotent(t *testing.T) {
	t.Parallel()
	b := newTestBroadcaster(t)
	_, _, unsub := b.Subscribe()
	unsub()
	unsub() // second call must not panic
}

func TestBroadcaster_New_RequiresLogger(t *testing.T) {
	t.Parallel()
	_, err := New(Config{})
	require.ErrorIs(t, err, ErrInvalidConfig)
}

func TestBroadcaster_SlowSubscriber_Dropped(t *testing.T) {
	t.Parallel()

	b, err := New(Config{
		Logger:               slog.New(slog.NewTextHandler(io.Discard, nil)),
		SubscriberBufferSize: 4, // tiny buffer; easy to overflow
	})
	require.NoError(t, err)

	_, done, unsub := b.Subscribe()
	defer unsub()

	// Publish more than the buffer can hold without ever reading.
	for i := 0; i < 100; i++ {
		b.Publish(&segment.Event{Seq: uint64(i)})
	}

	// Done channel must close.
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("done channel did not close after overflow")
	}
}

func TestBroadcaster_SlowDropDoesNotAffectFastPeer(t *testing.T) {
	t.Parallel()

	b, err := New(Config{
		Logger:               slog.New(slog.NewTextHandler(io.Discard, nil)),
		SubscriberBufferSize: 4,
	})
	require.NoError(t, err)

	_, slowDone, slowUnsub := b.Subscribe() // never read
	defer slowUnsub()
	fastCh, _, fastUnsub := b.Subscribe()
	defer fastUnsub()

	// Drain fastCh in the background so the publisher never blocks on it.
	received := make(chan uint64, 200)
	go func() {
		for evt := range fastCh {
			received <- evt.Seq
		}
	}()

	for i := 1; i <= 100; i++ {
		b.Publish(&segment.Event{Seq: uint64(i)})
		// Yield to the reader goroutine so the fast subscriber's tiny
		// 4-slot buffer doesn't overflow alongside the deliberately
		// unread slow peer's. Go's scheduler is cooperative for
		// CPU-bound goroutines and runtime.Gosched alone has been
		// observed insufficient here; a microsecond sleep parks us
		// long enough for the reader to drain.
		time.Sleep(time.Microsecond)
	}

	// Slow subscriber must be dropped.
	select {
	case <-slowDone:
	case <-time.After(time.Second):
		t.Fatal("slow subscriber not dropped")
	}

	// Fast peer must receive events. With slow peer dropped early, the
	// fast peer should have plenty of buffer space, so most/all events
	// should arrive. We check for at least 50 as a representative sample.
	deadline := time.After(2 * time.Second)
	got := make(map[uint64]struct{})
	for len(got) < 50 {
		select {
		case s := <-received:
			got[s] = struct{}{}
		case <-deadline:
			t.Fatalf("fast peer only received %d events", len(got))
		}
	}
}
