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
	fastCh, fastDone, fastUnsub := b.Subscribe()
	defer fastUnsub()

	// Drain the fast peer synchronously between publishes. This makes the
	// test deterministic: fastCh has depth 0 entering each Publish call,
	// so its 4-slot buffer cannot overflow regardless of scheduling. The
	// slow peer never reads, so its buffer fills and it is dropped on the
	// first Publish that overflows it. Earlier versions of this test used
	// a background reader plus a microsecond sleep to "yield"; that's a
	// hope, not synchronization, and flaked under CPU contention.
	const N = 100
	for i := uint64(1); i <= N; i++ {
		b.Publish(&segment.Event{Seq: i})
		select {
		case got := <-fastCh:
			require.Equal(t, i, got.Seq, "fast peer event %d", i)
		case <-fastDone:
			t.Fatalf("fast peer dropped unexpectedly at i=%d", i)
		case <-time.After(time.Second):
			t.Fatalf("fast peer did not receive event %d", i)
		}
	}

	// Slow peer must have been dropped well before now (its 4-slot buffer
	// fills on the 4th Publish; the 5th drops it).
	select {
	case <-slowDone:
	case <-time.After(time.Second):
		t.Fatal("slow subscriber not dropped")
	}
}
