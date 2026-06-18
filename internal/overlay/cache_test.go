package overlay

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bluesky-social/jetstream/internal/tombstone"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// fakeSource is a tombstone source + watermark for tests.
type fakeSource struct {
	mu  sync.Mutex
	set *tombstone.Set
	wm  uint64
	gen atomic.Uint64 // bumps on every mutation, drives dirtiness
}

func (f *fakeSource) SnapshotRange(low, high uint64) tombstone.Snapshot {
	return f.set.SnapshotRange(low, high)
}
func (f *fakeSource) Watermark() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.wm
}
func (f *fakeSource) Dirty() uint64 { return f.gen.Load() }

func segEvt(t *testing.T, seq uint64, did, coll, rkey string) segment.Event {
	t.Helper()
	return segment.Event{Seq: seq, Kind: segment.KindDelete, DID: did, Collection: coll, Rkey: rkey}
}

func TestCacheBuildsOnFirstAccess(t *testing.T) {
	t.Parallel()
	set := tombstone.New()
	ev := segEvt(t, 110, "did:plc:a", "app.bsky.feed.post", "r1")
	require.NoError(t, set.Observe(&ev))
	src := &fakeSource{set: set, wm: 100}
	src.gen.Store(1)

	c := NewCache(src, nil)
	blob := c.Current()
	require.NotNil(t, blob)
	require.Equal(t, uint64(100), blob.Watermark)
	require.Equal(t, uint64(110), blob.MaxSeq)
	require.NotEmpty(t, blob.ETag)

	w, m, snap, err := Decode(blob.Bytes)
	require.NoError(t, err)
	require.Equal(t, uint64(100), w)
	require.Equal(t, uint64(110), m)
	require.Len(t, snap.Records, 1)
}

func TestCacheRebuildOnlyWhenDirty(t *testing.T) {
	t.Parallel()
	set := tombstone.New()
	src := &fakeSource{set: set, wm: 0}
	src.gen.Store(1)
	c := NewCache(src, nil)

	first := c.Current()
	// No mutation -> Rebuild is a no-op, same blob pointer.
	c.Rebuild()
	require.Same(t, first, c.Current())

	// Mutate -> dirty -> rebuild swaps.
	ev := segEvt(t, 5, "did:plc:a", "c", "r")
	require.NoError(t, set.Observe(&ev))
	src.gen.Add(1)
	c.Rebuild()
	require.NotSame(t, first, c.Current())
}

func TestCacheConcurrentServeAndRebuild(t *testing.T) {
	t.Parallel()
	set := tombstone.New()
	src := &fakeSource{set: set, wm: 0}
	src.gen.Store(1)
	c := NewCache(src, nil)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 8 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
					b := c.Current()
					_, _, _, err := Decode(b.Bytes)
					require.NoError(t, err)
				}
			}
		})
	}
	for i := range 200 {
		ev := segEvt(t, uint64(i+1), "did:plc:a", "c", "r"+string(rune('a'+i%26)))
		require.NoError(t, set.Observe(&ev))
		src.gen.Add(1)
		c.Rebuild()
	}
	close(stop)
	wg.Wait()
}
