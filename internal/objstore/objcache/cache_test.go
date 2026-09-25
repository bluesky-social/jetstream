package objcache_test

import (
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream/internal/objstore/objcache"
	"github.com/bluesky-social/jetstream/internal/obs"
)

func obj(i, n int) ([sha256.Size]byte, []byte) {
	data := make([]byte, n)
	copy(data, fmt.Sprint(i))
	return sha256.Sum256(data), data
}

func TestLRU(t *testing.T) {
	t.Parallel()
	mem := obs.NewMemoryMetrics(prometheus.NewRegistry())
	c := objcache.New(objcache.Config{MaxBytes: 300, Memory: mem})
	limit, used := mem.Gauges(obs.BudgetObjectCache)
	require.InDelta(t, 300, testutil.ToFloat64(limit), 0)

	s0, d0 := obj(0, 100)
	s1, d1 := obj(1, 100)
	s2, d2 := obj(2, 100)
	s3, d3 := obj(3, 100)
	c.Add(s0, d0)
	c.Add(s1, d1)
	c.Add(s2, d2)
	require.InDelta(t, 300, testutil.ToFloat64(used), 0)

	// Touch 0 so 1 is the eviction victim.
	got, ok := c.Get(s0)
	require.True(t, ok)
	require.Equal(t, d0, got)
	c.Add(s3, d3)
	_, ok = c.Get(s1)
	require.False(t, ok)
	for _, s := range [][sha256.Size]byte{s0, s2, s3} {
		_, ok := c.Get(s)
		require.True(t, ok)
	}
	require.EqualValues(t, 300, c.Size())
	require.InDelta(t, 300, testutil.ToFloat64(used), 0)

	// Re-adding a present entry does not double count.
	c.Add(s3, d3)
	require.EqualValues(t, 300, c.Size())

	// One large entry evicts as many as it needs.
	s4, d4 := obj(4, 250)
	c.Add(s4, d4)
	require.EqualValues(t, 250, c.Size())
	require.InDelta(t, 250, testutil.ToFloat64(used), 0)
}

func TestOversizeAndDisabled(t *testing.T) {
	t.Parallel()
	c := objcache.New(objcache.Config{MaxBytes: 10})
	s, d := obj(0, 11)
	c.Add(s, d)
	_, ok := c.Get(s)
	require.False(t, ok)
	require.Zero(t, c.Size())

	for _, c := range []*objcache.Cache{nil, objcache.New(objcache.Config{})} {
		s, d := obj(1, 1)
		c.Add(s, d)
		_, ok := c.Get(s)
		require.False(t, ok)
		require.Zero(t, c.Size())
	}
}

func TestConcurrent(t *testing.T) {
	t.Parallel()
	c := objcache.New(objcache.Config{MaxBytes: 1000})
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 200 {
				s, d := obj(g*1000+i%20, 50)
				c.Add(s, d)
				if got, ok := c.Get(s); ok {
					require.Equal(t, d, got)
				}
			}
		})
	}
	wg.Wait()
	require.LessOrEqual(t, c.Size(), int64(1000))
}
