package subscribe

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestBroadcaster_Shutdown_InvokesEveryRegisteredCloser is the core
// graceful-drain contract: Shutdown fires every registered connection's
// close function and returns nil once they all complete within budget.
func TestBroadcaster_Shutdown_InvokesEveryRegisteredCloser(t *testing.T) {
	t.Parallel()
	b := newTestBroadcaster(t)

	const N = 5
	var closed [N]atomic.Bool
	for i := range N {
		_, ok := b.RegisterConn(func() { closed[i].Store(true) })
		require.True(t, ok, "RegisterConn must succeed before shutdown")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, b.Shutdown(ctx))

	for i := range N {
		require.True(t, closed[i].Load(), "closer %d was not invoked", i)
	}
}

// TestBroadcaster_Shutdown_DeregisteredConnNotClosed proves a connection
// that disconnected normally (and deregistered) is not closed again by
// Shutdown.
func TestBroadcaster_Shutdown_DeregisteredConnNotClosed(t *testing.T) {
	t.Parallel()
	b := newTestBroadcaster(t)

	var closedCount atomic.Int32
	id, ok := b.RegisterConn(func() { closedCount.Add(1) })
	require.True(t, ok)
	b.DeregisterConn(id)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, b.Shutdown(ctx))
	require.Equal(t, int32(0), closedCount.Load(),
		"a deregistered connection must not be closed by Shutdown")
}

// TestBroadcaster_Shutdown_BoundedByContext proves Shutdown does not hang
// on a wedged connection: if a closer blocks past the deadline, Shutdown
// returns the context error rather than blocking forever.
func TestBroadcaster_Shutdown_BoundedByContext(t *testing.T) {
	t.Parallel()
	b := newTestBroadcaster(t)

	release := make(chan struct{})
	t.Cleanup(func() { close(release) }) // unblock the wedged closer at teardown
	_, ok := b.RegisterConn(func() { <-release })
	require.True(t, ok)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := b.Shutdown(ctx)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.DeadlineExceeded,
		"Shutdown must surface the deadline, not block on a wedged connection")
	require.Less(t, elapsed, time.Second, "Shutdown must return near the deadline")
}

// TestBroadcaster_RegisterConn_RejectedAfterShutdown proves a connection
// arriving after drain has begun is told to close itself (ok=false)
// rather than being registered into a drain that already snapshotted its
// closers.
func TestBroadcaster_RegisterConn_RejectedAfterShutdown(t *testing.T) {
	t.Parallel()
	b := newTestBroadcaster(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, b.Shutdown(ctx))

	_, ok := b.RegisterConn(func() {})
	require.False(t, ok, "RegisterConn must be rejected once draining")
}

// TestBroadcaster_Shutdown_Idempotent proves a second Shutdown is a
// no-op that returns promptly.
func TestBroadcaster_Shutdown_Idempotent(t *testing.T) {
	t.Parallel()
	b := newTestBroadcaster(t)

	var calls atomic.Int32
	_, ok := b.RegisterConn(func() { calls.Add(1) })
	require.True(t, ok)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, b.Shutdown(ctx))
	require.NoError(t, b.Shutdown(ctx))
	require.Equal(t, int32(1), calls.Load(), "closer must be invoked exactly once across repeated Shutdown")
}
