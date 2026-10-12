package subscribe_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/stretchr/testify/require"
)

// DrainOver closes every connection, spaced over the window, and admits no
// new one; ending ctx closes the rest at once.
func TestTail_DrainOver(t *testing.T) {
	t.Parallel()
	for _, cancelEarly := range []bool{false, true} {
		tail, err := subscribe.New(subscribe.Config{Logger: discardLogger()}, nil, nil)
		require.NoError(t, err)
		require.Equal(t, subscribe.DefaultCloseReason, tail.CloseReason())
		var mu sync.Mutex
		var closed []time.Time
		var wg sync.WaitGroup
		for range 4 {
			wg.Add(1)
			_, ok := tail.RegisterConn(func() {
				mu.Lock()
				closed = append(closed, time.Now())
				mu.Unlock()
				wg.Done()
			})
			require.True(t, ok)
		}
		ctx, cancel := context.WithCancel(t.Context())
		if cancelEarly {
			cancel()
			require.ErrorIs(t, tail.DrainOver(ctx, time.Hour, "moved"), context.Canceled)
		} else {
			start := time.Now()
			require.NoError(t, tail.DrainOver(ctx, 200*time.Millisecond, "moved"))
			require.GreaterOrEqual(t, time.Since(start), 150*time.Millisecond, "closes are spread")
		}
		cancel()
		wg.Wait()
		require.Len(t, closed, 4)
		require.Equal(t, "moved", tail.CloseReason(), "drained subscribers learn why")
		_, ok := tail.RegisterConn(func() {})
		require.False(t, ok, "no new connection while drained")
		require.NoError(t, tail.Shutdown(t.Context()))
	}
}
