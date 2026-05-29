package subscribe_test

import (
	"sync"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/subscribe"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/stretchr/testify/require"
)

func TestRingBuf_PushPop_FIFO(t *testing.T) {
	t.Parallel()
	r := subscribe.NewRingBuf(4)
	for i := range uint64(4) {
		ok, sealed := r.Push(&segment.Event{Seq: i})
		require.True(t, ok)
		require.False(t, sealed)
	}
	for i := range uint64(4) {
		ev, ok := r.Pop()
		require.True(t, ok)
		require.Equal(t, i, ev.Seq)
	}
	_, ok := r.Pop()
	require.False(t, ok, "pop on empty returns false")
}

func TestRingBuf_PushReturnsFalseOnFull(t *testing.T) {
	t.Parallel()
	r := subscribe.NewRingBuf(2)
	ok, _ := r.Push(&segment.Event{Seq: 1})
	require.True(t, ok)
	ok, _ = r.Push(&segment.Event{Seq: 2})
	require.True(t, ok)
	ok, sealed := r.Push(&segment.Event{Seq: 3})
	require.False(t, ok, "push on full must return ok=false")
	require.False(t, sealed, "full is not sealed")
}

func TestRingBuf_LenAndCap(t *testing.T) {
	t.Parallel()
	r := subscribe.NewRingBuf(8)
	require.Equal(t, 8, r.Cap())
	require.Equal(t, 0, r.Len())
	r.Push(&segment.Event{Seq: 1})
	r.Push(&segment.Event{Seq: 2})
	require.Equal(t, 2, r.Len())
	r.Pop()
	require.Equal(t, 1, r.Len())
}

func TestRingBuf_ConcurrentPushPop(t *testing.T) {
	t.Parallel()
	r := subscribe.NewRingBuf(64)
	const N = 1000
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		i := uint64(0)
		for i < N {
			if ok, _ := r.Push(&segment.Event{Seq: i}); ok {
				i++
			}
		}
	}()
	go func() {
		defer wg.Done()
		seen := uint64(0)
		for seen < N {
			if ev, ok := r.Pop(); ok {
				require.Equal(t, seen, ev.Seq, "pop must observe FIFO order")
				seen++
			}
		}
	}()
	wg.Wait()
}

func TestRingBuf_SealAndDrainReturnsBuffered(t *testing.T) {
	t.Parallel()
	r := subscribe.NewRingBuf(4)
	for i := uint64(1); i <= 3; i++ {
		ok, sealed := r.Push(&segment.Event{Seq: i})
		require.True(t, ok)
		require.False(t, sealed)
	}
	got := r.SealAndDrain()
	require.Len(t, got, 3)
	require.Equal(t, uint64(1), got[0].Seq)
	require.Equal(t, uint64(3), got[2].Seq)
	require.Equal(t, 0, r.Len(), "seal-and-drain empties the ring")
}

func TestRingBuf_PushAfterSealReportsSealed(t *testing.T) {
	t.Parallel()
	r := subscribe.NewRingBuf(4)
	_ = r.SealAndDrain()
	ok, sealed := r.Push(&segment.Event{Seq: 1})
	require.False(t, ok, "sealed ring stores nothing")
	require.True(t, sealed, "Push must report sealed so the producer reroutes to live")
	require.Equal(t, 0, r.Len())
}

func TestRingBuf_PanicOnZeroCapacity(t *testing.T) {
	t.Parallel()
	require.Panics(t, func() { subscribe.NewRingBuf(0) })
}

func TestRingBuf_PanicOnNegativeCapacity(t *testing.T) {
	t.Parallel()
	require.Panics(t, func() { subscribe.NewRingBuf(-1) })
}
