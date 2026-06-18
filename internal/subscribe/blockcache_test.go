package subscribe

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func decodedFixture(seqs ...uint64) []segment.Event {
	out := make([]segment.Event, len(seqs))
	for i, s := range seqs {
		out[i] = segment.Event{Seq: s, Kind: segment.KindCreate, DID: "did:plc:b", Payload: []byte{0xa0}}
	}
	return out
}

func TestBlockCache_GetOrDecode_RunsDecodeOnce(t *testing.T) {
	t.Parallel()
	c := newBlockCache(1 << 20)
	var calls atomic.Int64
	decode := func() ([]segment.Event, error) {
		calls.Add(1)
		return decodedFixture(1, 2, 3), nil
	}

	const N = 20
	var wg sync.WaitGroup
	wg.Add(N)
	for range N {
		go func() {
			defer wg.Done()
			evs, err := c.getOrDecode(blockKey{segIdx: 0, blockIdx: 0}, decode)
			require.NoError(t, err)
			require.Len(t, evs, 3)
		}()
	}
	wg.Wait()
	require.Equal(t, int64(1), calls.Load(), "decode runs once across concurrent hits")
}

func TestBlockCache_EvictsByByteBudget(t *testing.T) {
	t.Parallel()
	c := newBlockCache(200)
	mk := func(seg uint64) func() ([]segment.Event, error) {
		return func() ([]segment.Event, error) { return decodedFixture(seg*10, seg*10+1, seg*10+2), nil }
	}
	for seg := range uint64(10) {
		_, err := c.getOrDecode(blockKey{segIdx: seg, blockIdx: 0}, mk(seg))
		require.NoError(t, err)
	}
	require.LessOrEqual(t, c.bytes(), 200, "cache must respect byte budget")

	// The earliest key was evicted: a re-get re-decodes.
	var redecoded atomic.Bool
	_, err := c.getOrDecode(blockKey{segIdx: 0, blockIdx: 0}, func() ([]segment.Event, error) {
		redecoded.Store(true)
		return decodedFixture(0, 1, 2), nil
	})
	require.NoError(t, err)
	require.True(t, redecoded.Load(), "evicted block must re-decode on next access")
}

func TestBlockCache_InvalidateSegmentForcesRedecode(t *testing.T) {
	t.Parallel()
	c := newBlockCache(1 << 20)
	key := c.keyForBlock(7, 11, 0)

	var calls atomic.Int64
	evs, err := c.getOrDecode(key, func() ([]segment.Event, error) {
		calls.Add(1)
		return decodedFixture(1), nil
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), evs[0].Seq)

	c.invalidateSegment(7)

	key = c.keyForBlock(7, 11, 0)
	evs, err = c.getOrDecode(key, func() ([]segment.Event, error) {
		calls.Add(1)
		return decodedFixture(2), nil
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), evs[0].Seq)
	require.Equal(t, int64(2), calls.Load(), "invalidated segment must re-decode")
}

func TestBlockCache_InvalidateSegmentPreventsInflightInsert(t *testing.T) {
	t.Parallel()
	c := newBlockCache(1 << 20)
	key := c.keyForBlock(9, 22, 0)
	oldKey := key

	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)
		_, err := c.getOrDecode(key, func() ([]segment.Event, error) {
			close(started)
			<-release
			return decodedFixture(1), nil
		})
		require.NoError(t, err)
	}()

	<-started
	c.invalidateSegment(9)
	close(release)
	<-done

	c.mu.Lock()
	_, staleInserted := c.items[oldKey]
	c.mu.Unlock()
	require.False(t, staleInserted, "old in-flight decode must not populate the resident cache")

	key = c.keyForBlock(9, 22, 0)
	var fresh atomic.Bool
	evs, err := c.getOrDecode(key, func() ([]segment.Event, error) {
		fresh.Store(true)
		return decodedFixture(2), nil
	})
	require.NoError(t, err)
	require.True(t, fresh.Load(), "old in-flight decode must not populate the new generation")
	require.Equal(t, uint64(2), evs[0].Seq)
}

func TestBlockCache_DecodeErrorNotCached(t *testing.T) {
	t.Parallel()
	c := newBlockCache(1 << 20)
	_, err := c.getOrDecode(blockKey{segIdx: 1, blockIdx: 0}, func() ([]segment.Event, error) {
		return nil, assertErr
	})
	require.ErrorIs(t, err, assertErr)
	// A decode error must not poison the slot.
	evs, err := c.getOrDecode(blockKey{segIdx: 1, blockIdx: 0}, func() ([]segment.Event, error) {
		return decodedFixture(5), nil
	})
	require.NoError(t, err)
	require.Len(t, evs, 1)
}

var assertErr = errorString("decode failed")

type errorString string

func (e errorString) Error() string { return string(e) }
