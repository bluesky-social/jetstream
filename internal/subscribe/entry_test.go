package subscribe

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/klauspost/compress/zstd"
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

func TestEntry_MemoizesSimpleAndExtendedIndependently(t *testing.T) {
	t.Parallel()
	var simpleCalls atomic.Int64
	var extendedCalls atomic.Int64
	e := newEntry(&segment.Event{Seq: 1, Kind: segment.KindDelete, DID: "did:plc:s"})
	e.encodeFn = func(*segment.Event) ([]byte, error) {
		simpleCalls.Add(1)
		return []byte(`{"mode":"simple"}`), nil
	}
	e.encodeExtendedFn = func(*segment.Event) ([]byte, error) {
		extendedCalls.Add(1)
		return []byte(`{"mode":"extended"}`), nil
	}

	for range 3 {
		body, err := e.Encoded()
		require.NoError(t, err)
		require.Equal(t, []byte(`{"mode":"simple"}`), body)

		body, err = e.EncodedExtended()
		require.NoError(t, err)
		require.Equal(t, []byte(`{"mode":"extended"}`), body)
	}

	require.Equal(t, int64(1), simpleCalls.Load())
	require.Equal(t, int64(1), extendedCalls.Load())
}

func TestEntry_CompressedMemoizesOnceAndDecodes(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	e := newEntry(&segment.Event{Seq: 3, Kind: segment.KindIdentity, DID: "did:plc:c"})
	e.encodeFn = func(*segment.Event) ([]byte, error) {
		calls.Add(1)
		return []byte(`{"did":"did:plc:c","kind":"identity"}`), nil
	}

	const N = 50
	var wg sync.WaitGroup
	results := make([][]byte, N)
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			body, err := e.Compressed()
			require.NoError(t, err)
			results[i] = body
		}(i)
	}
	wg.Wait()

	require.Equal(t, int64(1), calls.Load(), "underlying JSON encode must run exactly once")
	for i := 0; i < N; i++ {
		require.Equal(t, results[0], results[i], "all callers see the same memoized frame")
	}

	dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(zstdDictionary))
	require.NoError(t, err)
	defer dec.Close()
	got, err := dec.DecodeAll(results[0], nil)
	require.NoError(t, err)
	require.Equal(t, []byte(`{"did":"did:plc:c","kind":"identity"}`), got)
}

// TestEntry_CompressedPropagatesSkipSentinel ensures the compressed path
// surfaces errSkipEvent unchanged (so the loop advances without sending),
// exactly like the JSON path.
func TestEntry_CompressedPropagatesSkipSentinel(t *testing.T) {
	t.Parallel()
	e := newEntry(&segment.Event{Seq: 9, Kind: segment.KindSync, DID: "did:plc:s"})
	body, err := e.Compressed()
	require.ErrorIs(t, err, errSkipEvent)
	require.Nil(t, body)
}
