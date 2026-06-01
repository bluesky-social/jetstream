package subscribe

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/stretchr/testify/require"
)

func TestEntry_EncodedMemoizesOnce(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	ev := &segment.Event{
		Seq: 7, IndexedAt: 1000, Kind: segment.KindIdentity,
		DID: "did:plc:x", Payload: nil,
	}
	e := newEntry(ev)
	e.encodeFn = func(ev *segment.Event) ([]byte, error) {
		calls.Add(1)
		return []byte(`{"ok":true}`), nil
	}

	const N = 50
	var wg sync.WaitGroup
	results := make([][]byte, N)
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			body, err := e.Encoded()
			require.NoError(t, err)
			results[i] = body
		}(i)
	}
	wg.Wait()

	require.Equal(t, int64(1), calls.Load(), "encode must run exactly once")
	for i := 0; i < N; i++ {
		require.Equal(t, []byte(`{"ok":true}`), results[i])
	}
}

func TestEntry_MemoizesSkipSentinel(t *testing.T) {
	t.Parallel()
	ev := &segment.Event{Seq: 9, Kind: segment.KindSync, DID: "did:plc:s"}
	e := newEntry(ev)
	body, err := e.Encoded()
	require.ErrorIs(t, err, errSkipEvent)
	require.Nil(t, body)
	// Second call returns the same memoized sentinel.
	body2, err2 := e.Encoded()
	require.ErrorIs(t, err2, errSkipEvent)
	require.Nil(t, body2)
}
