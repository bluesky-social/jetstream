package subscribe

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// testKey is a cache key for block 0 of segment seg.
func testKey(seg uint64) blockKey {
	return blockKey{id: localBlockID{ns: catalog.Main, seg: seg}, seg: seg}
}

func testRef(seg, gen uint64) catalog.BlockRef {
	return catalog.BlockRef{Namespace: catalog.Main, Segment: seg, Generation: gen}
}

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
			evs, err := c.getOrDecode(testKey(0), decode)
			require.NoError(t, err)
			require.Len(t, evs, 3)
		}()
	}
	wg.Wait()
	require.Equal(t, int64(1), calls.Load(), "decode runs once across concurrent hits")
}

func TestBlockCache_EvictsByByteBudget(t *testing.T) {
	t.Parallel()
	const budget = 1000
	c := newBlockCache(budget)
	mk := func(seg uint64) func() ([]segment.Event, error) {
		return func() ([]segment.Event, error) { return decodedFixture(seg*10, seg*10+1, seg*10+2), nil }
	}
	for seg := range uint64(10) {
		_, err := c.getOrDecode(testKey(seg), mk(seg))
		require.NoError(t, err)
	}
	require.LessOrEqual(t, c.bytes(), budget, "cache must respect byte budget")

	// The earliest key was evicted: a re-get re-decodes.
	var redecoded atomic.Bool
	_, err := c.getOrDecode(testKey(0), func() ([]segment.Event, error) {
		redecoded.Store(true)
		return decodedFixture(0, 1, 2), nil
	})
	require.NoError(t, err)
	require.True(t, redecoded.Load(), "evicted block must re-decode on next access")
}

func TestBlockCache_InvalidateSegmentForcesRedecode(t *testing.T) {
	t.Parallel()
	c := newBlockCache(1 << 20)
	key := c.keyForRef(testRef(7, 11))

	var calls atomic.Int64
	evs, err := c.getOrDecode(key, func() ([]segment.Event, error) {
		calls.Add(1)
		return decodedFixture(1), nil
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), evs[0].Event.Seq)

	c.invalidateSegment(7)

	key = c.keyForRef(testRef(7, 11))
	evs, err = c.getOrDecode(key, func() ([]segment.Event, error) {
		calls.Add(1)
		return decodedFixture(2), nil
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), evs[0].Event.Seq)
	require.Equal(t, int64(2), calls.Load(), "invalidated segment must re-decode")
}

func TestBlockCache_InvalidateSegmentPreventsInflightInsert(t *testing.T) {
	t.Parallel()
	c := newBlockCache(1 << 20)
	key := c.keyForRef(testRef(9, 22))
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

	key = c.keyForRef(testRef(9, 22))
	var fresh atomic.Bool
	evs, err := c.getOrDecode(key, func() ([]segment.Event, error) {
		fresh.Store(true)
		return decodedFixture(2), nil
	})
	require.NoError(t, err)
	require.True(t, fresh.Load(), "old in-flight decode must not populate the new generation")
	require.Equal(t, uint64(2), evs[0].Event.Seq)
}

func TestBlockCache_DecodeErrorNotCached(t *testing.T) {
	t.Parallel()
	c := newBlockCache(1 << 20)
	_, err := c.getOrDecode(testKey(1), func() ([]segment.Event, error) {
		return nil, assertErr
	})
	require.ErrorIs(t, err, assertErr)
	// A decode error must not poison the slot.
	evs, err := c.getOrDecode(testKey(1), func() ([]segment.Event, error) {
		return decodedFixture(5), nil
	})
	require.NoError(t, err)
	require.Len(t, evs, 1)
}

var assertErr = errorString("decode failed")

type errorString string

func (e errorString) Error() string { return string(e) }

// TestBlockCache_KeepsBlockInUse pins that a block a subscriber is reading is
// not evicted by its own insert or memo growth, even when it alone outgrows
// the budget. Evicting it made every cold batch of a large-record block
// re-fetch and re-decode the whole block. The next insert still evicts it.
func TestBlockCache_KeepsBlockInUse(t *testing.T) {
	t.Parallel()
	c := newBlockCache(600)
	var decodes atomic.Int64
	get := func(seg uint64) []*Entry {
		evs, err := c.getOrDecode(testKey(seg), func() ([]segment.Event, error) {
			decodes.Add(1)
			return decodedFixture(seg*10, seg*10+1, seg*10+2), nil
		})
		require.NoError(t, err)
		return evs
	}

	get(1)
	get(2) // two ~414-byte blocks: 1 is evicted, 2 stays though over budget
	require.EqualValues(t, 2, decodes.Load())
	get(2)
	require.EqualValues(t, 2, decodes.Load(), "the block just inserted stays cached")

	// Memoized encodes push block 2 far past the budget; it must survive.
	c.grow(testKey(2), 5000)
	get(2)
	require.EqualValues(t, 2, decodes.Load(), "memo growth must not evict the block being encoded")

	// A different block's insert reclaims the overshoot.
	get(3)
	require.EqualValues(t, 3, decodes.Load())
	get(2)
	require.EqualValues(t, 4, decodes.Load(), "the next insert evicts the oversized block")
}

// TestBlockCache_GrowEvictsOthersFirst pins that memo growth on one block
// evicts the least recently used other blocks rather than itself.
func TestBlockCache_GrowEvictsOthersFirst(t *testing.T) {
	t.Parallel()
	c := newBlockCache(1500)
	for seg := range uint64(3) {
		_, err := c.getOrDecode(testKey(seg), func() ([]segment.Event, error) { return decodedFixture(seg), nil })
		require.NoError(t, err)
	}
	// Block 0 is least recently used; growing it makes it most recent, so
	// block 1 is the one evicted.
	c.grow(testKey(0), 1200) // 3x138 + 1200 > 1500
	require.LessOrEqual(t, c.bytes(), 1500)
	c.mu.Lock()
	_, has0 := c.items[testKey(0)]
	_, has1 := c.items[testKey(1)]
	c.mu.Unlock()
	require.True(t, has0)
	require.False(t, has1)
}
