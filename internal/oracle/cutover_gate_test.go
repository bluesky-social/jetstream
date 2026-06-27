package oracle

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// tipServer is a minimal stand-in for the simulator's /_oracle/firehose-tip
// endpoint, serving a settable seq so the gate's fetchTip path is exercised
// for real (not stubbed).
func tipServer(t *testing.T, tip *atomic.Int64) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(rw).Encode(struct {
			Seq int64 `json:"seq"`
		}{Seq: tip.Load()})
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func liveEvent(seq int64) *segment.Event {
	return &segment.Event{Kind: segment.KindCreate, UpstreamRelayCursor: seq}
}

// TestCutoverGateBlocksUntilTipDelivered is the core regression guard for
// #114: the gate must NOT release the cutover barrier until every frame up
// to the relay tip has been durably archived. This is exactly the property
// whose absence let an undrained chain tail vanish at cutover. We drive the
// archive observations on a goroutine and assert the gate stays blocked
// until the last seq lands.
func TestCutoverGateBlocksUntilTipDelivered(t *testing.T) {
	t.Parallel()

	var tip atomic.Int64
	tip.Store(5)
	gate := newCutoverDeliveryGate(tipServer(t, &tip), 5*time.Second)

	// Observe 1..4 but withhold 5: the gate must stay blocked on the gap.
	for s := int64(1); s <= 4; s++ {
		gate.observe(liveEvent(s))
	}

	released := make(chan error, 1)
	go func() { released <- gate.waitDelivered(context.Background()) }()

	select {
	case err := <-released:
		t.Fatalf("gate released before tip seq 5 was delivered (err=%v)", err)
	case <-time.After(100 * time.Millisecond):
		// Correct: still blocked on the missing seq 5.
	}

	// Deliver the final frame; the gate must now release cleanly.
	gate.observe(liveEvent(5))
	select {
	case err := <-released:
		require.NoError(t, err, "gate must release once the tip is contiguously delivered")
	case <-time.After(2 * time.Second):
		t.Fatal("gate did not release after the tip frame was delivered")
	}
}

// TestCutoverGateFloorsAtLowestObserved proves the recovering-child case: a
// child that resumes bootstrap at a persisted cursor C only re-observes
// C+1..tip (frames 1..C are already durable from the prior child). The gate
// must floor contiguity at the lowest observed seq, not at 1, or it would
// hang forever waiting for frames that will never be redelivered.
func TestCutoverGateFloorsAtLowestObserved(t *testing.T) {
	t.Parallel()

	var tip atomic.Int64
	tip.Store(10)
	gate := newCutoverDeliveryGate(tipServer(t, &tip), 2*time.Second)

	// Resume at cursor 6: observe only 7..10 (1..6 durable from before).
	for s := int64(7); s <= 10; s++ {
		gate.observe(liveEvent(s))
	}

	require.NoError(t, gate.waitDelivered(context.Background()),
		"gate must release when the resumed window 7..tip is contiguous (floor at lowest observed)")
}

// TestCutoverGateNoLiveOps covers the nil-coordinator / no-traffic case: a
// run that generated no live ops has tip=0, so the gate is a no-op and must
// not block (there is nothing to deliver).
func TestCutoverGateNoLiveOps(t *testing.T) {
	t.Parallel()

	var tip atomic.Int64 // stays 0
	gate := newCutoverDeliveryGate(tipServer(t, &tip), time.Second)

	require.NoError(t, gate.waitDelivered(context.Background()),
		"gate must release immediately when no live ops were generated (tip=0)")
}

// TestCutoverGateTimesOutLoud proves the gate fails LOUD rather than
// silently proceeding when the chain never lands: if the tip is never
// contiguously delivered, waitDelivered must return an error (which the
// child surfaces) rather than releasing the barrier on an undrained tail.
func TestCutoverGateTimesOutLoud(t *testing.T) {
	t.Parallel()

	var tip atomic.Int64
	tip.Store(3)
	gate := newCutoverDeliveryGate(tipServer(t, &tip), 50*time.Millisecond)

	// Deliver a gappy set (missing seq 2) so contiguity to tip=3 is never met.
	gate.observe(liveEvent(1))
	gate.observe(liveEvent(3))

	err := gate.waitDelivered(context.Background())
	require.Error(t, err, "gate must fail loud when the tip is never contiguously delivered")
	require.Contains(t, err.Error(), "timeout")
}

// TestCutoverGateRespectsContextCancel ensures a cancelled run unblocks the
// gate promptly with the context error (clean shutdown), not a timeout hang.
func TestCutoverGateRespectsContextCancel(t *testing.T) {
	t.Parallel()

	var tip atomic.Int64
	tip.Store(2)
	gate := newCutoverDeliveryGate(tipServer(t, &tip), time.Minute)
	gate.observe(liveEvent(1)) // seq 2 never arrives

	ctx, cancel := context.WithCancel(context.Background())
	released := make(chan error, 1)
	go func() { released <- gate.waitDelivered(ctx) }()

	cancel()
	select {
	case err := <-released:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("gate did not observe context cancellation")
	}
}
