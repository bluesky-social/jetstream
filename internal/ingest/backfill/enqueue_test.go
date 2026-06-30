package backfill

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/stretchr/testify/require"
)

func TestSeenCache_LRUEviction(t *testing.T) {
	t.Parallel()
	// One shard, capacity 2, so eviction order is deterministic.
	c := newSeenCache(2, 1)

	c.store("a")
	c.store("b")
	require.True(t, c.seen("a"))
	require.True(t, c.seen("b"))

	// Touch "a" so "b" becomes least-recently-used, then insert "c": "b" evicts.
	require.True(t, c.seen("a"))
	c.store("c")
	require.True(t, c.seen("a"))
	require.True(t, c.seen("c"))
	require.False(t, c.seen("b"))
}

func TestSeenCache_StoreIsIdempotent(t *testing.T) {
	t.Parallel()
	c := newSeenCache(4, 1)
	c.store("a")
	c.store("a")
	c.store("a")
	require.True(t, c.seen("a"))
	// Re-storing must not have created duplicate nodes that distort the LRU.
	shard := c.shardFor("a")
	shard.mu.Lock()
	require.Len(t, shard.items, 1)
	shard.mu.Unlock()
}

func TestSeenCache_RemoveAllowsReinsert(t *testing.T) {
	t.Parallel()
	c := newSeenCache(4, 1)
	c.store("a")
	require.True(t, c.seen("a"))
	c.remove("a")
	require.False(t, c.seen("a"))
	c.store("a")
	require.True(t, c.seen("a"))
}

func TestSeenCache_ShardCountRoundsUpToPowerOfTwo(t *testing.T) {
	t.Parallel()
	c := newSeenCache(1000, 60)
	require.Len(t, c.shards, 64)
	require.Equal(t, uint64(63), c.mask)
}

// newEnqueueTestStore builds a Store over a fresh pebble with metrics so the
// enqueue counters can be asserted.
func newEnqueueTestStore(t *testing.T) *Store {
	t.Helper()
	return newTestStore(t)
}

func TestLiveEnqueuer_EnqueuesNetNewDID(t *testing.T) {
	t.Parallel()
	s := newEnqueueTestStore(t)
	e := NewLiveEnqueuer(LiveEnqueuerConfig{Store: s})
	did := "did:plc:netnew"

	e.Observe(did)
	require.Equal(t, 1, e.QueueLen(), "unknown DID should be queued")

	// Drain one item synchronously via the worker loop with a short deadline.
	runEnqueuerOnce(t, e)

	rs, err := s.readRepoStatus(atmos.DID(did))
	require.NoError(t, err)
	require.NotNil(t, rs)
	require.Equal(t, StatusPending, rs.Backfill.Status)
	require.True(t, rs.Active)
	require.True(t, rs.Backfill.NextAttemptAt.IsZero(), "pending row must be immediately due")
}

func TestLiveEnqueuer_BurstSameDIDCollapses(t *testing.T) {
	t.Parallel()
	s := newEnqueueTestStore(t)
	e := NewLiveEnqueuer(LiveEnqueuerConfig{Store: s})
	did := "did:plc:burst"

	// Simulate a #sync replay: a long run of events for one DID.
	for range 1_000_000 {
		e.Observe(did)
	}
	require.Equal(t, 1, e.QueueLen(), "a same-DID burst must enqueue exactly once")
}

func TestLiveEnqueuer_KnownDIDNotReQueued(t *testing.T) {
	t.Parallel()
	s := newEnqueueTestStore(t)
	e := NewLiveEnqueuer(LiveEnqueuerConfig{Store: s})
	a, b := "did:plc:a", "did:plc:b"

	// Interleave two DIDs so the lock-free last-DID path does not absorb them.
	e.Observe(a)
	e.Observe(b)
	e.Observe(a)
	e.Observe(b)
	require.Equal(t, 2, e.QueueLen(), "each distinct DID should be queued once")
}

func TestLiveEnqueuer_EmptyDIDIgnored(t *testing.T) {
	t.Parallel()
	s := newEnqueueTestStore(t)
	e := NewLiveEnqueuer(LiveEnqueuerConfig{Store: s})
	e.Observe("")
	require.Equal(t, 0, e.QueueLen())
}

func TestLiveEnqueuer_ChannelFullDropsAndDoesNotCache(t *testing.T) {
	t.Parallel()
	s := newEnqueueTestStore(t)
	// Tiny queue, no worker draining: the 1st DID fills it, the 2nd drops.
	e := NewLiveEnqueuer(LiveEnqueuerConfig{Store: s, QueueSize: 1})

	e.Observe("did:plc:first")
	require.Equal(t, 1, e.QueueLen())

	e.Observe("did:plc:dropped")
	require.Equal(t, 1, e.QueueLen(), "second DID must be dropped, not queued")

	// A dropped DID must NOT be cached, so a later event for it retries.
	require.False(t, e.cache.seen("did:plc:dropped"))

	// Drain the first so there's room, then re-observe the dropped DID.
	<-e.queue
	e.Observe("did:plc:dropped")
	require.Equal(t, 1, e.QueueLen(), "dropped DID should enqueue on retry once space frees")
}

func TestLiveEnqueuer_Idempotent_ExistingRowIsNoOp(t *testing.T) {
	t.Parallel()
	s := newEnqueueTestStore(t)
	ctx := context.Background()
	did := atmos.DID("did:plc:exists")

	// Pre-existing complete row: enqueue must not overwrite it.
	require.NoError(t, s.putRepoStatus(did, &RepoStatus{
		Backfill: RepoBackfillStatus{Status: StatusComplete, Rev: "rev-1"},
		Active:   true,
	}))

	created, err := s.EnqueueNetNewRepo(ctx, did, true)
	require.NoError(t, err)
	require.False(t, created, "existing row must not be re-created")

	rs, err := s.readRepoStatus(did)
	require.NoError(t, err)
	require.Equal(t, StatusComplete, rs.Backfill.Status, "existing status must be preserved")
	require.Equal(t, "rev-1", rs.Backfill.Rev)
}

func TestEnqueueNetNewRepo_CreatesPendingAndCounts(t *testing.T) {
	t.Parallel()
	s := newEnqueueTestStore(t)
	ctx := context.Background()
	did := atmos.DID("did:plc:pending")

	created, err := s.EnqueueNetNewRepo(ctx, did, true)
	require.NoError(t, err)
	require.True(t, created)

	// Second call is a durable no-op.
	created, err = s.EnqueueNetNewRepo(ctx, did, true)
	require.NoError(t, err)
	require.False(t, created)

	counts, ok, err := LoadCounts(s.db)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), counts.Total)
	require.Equal(t, uint64(1), counts.Pending)
	require.Equal(t, uint64(0), counts.Failed)

	// Scanned counts must agree with the maintained aggregate.
	scanned, err := CountStatuses(s.db)
	require.NoError(t, err)
	require.Equal(t, counts, scanned)
}

func TestEnqueueNetNewRepo_ConcurrentSameDIDCreatesOnce(t *testing.T) {
	t.Parallel()
	s := newEnqueueTestStore(t)
	ctx := context.Background()
	did := atmos.DID("did:plc:race")

	const goroutines = 32
	var wg sync.WaitGroup
	var createdCount int
	var mu sync.Mutex
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			created, err := s.EnqueueNetNewRepo(ctx, did, true)
			require.NoError(t, err)
			if created {
				mu.Lock()
				createdCount++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	require.Equal(t, 1, createdCount, "exactly one goroutine must create the row")
	counts, ok, err := LoadCounts(s.db)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), counts.Total)
	require.Equal(t, uint64(1), counts.Pending)
}

// runEnqueuerOnce runs the worker until it has drained the queue, then cancels.
func runEnqueuerOnce(t *testing.T, e *LiveEnqueuer) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		_ = e.Run(ctx)
		close(done)
	}()
	deadline := time.After(2 * time.Second)
	for e.QueueLen() > 0 {
		select {
		case <-deadline:
			t.Fatal("enqueuer worker did not drain queue")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	// Give the worker a moment to finish the in-flight item's durable write.
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done
}

// fmtDID is a tiny helper so table tests can generate distinct DIDs.
func fmtDID(i int) string { return fmt.Sprintf("did:plc:gen-%d", i) }

func TestSeenCache_DistinctDIDsAcrossShards(t *testing.T) {
	t.Parallel()
	c := newSeenCache(4096, 64)
	const n = 2000
	for i := range n {
		c.store(fmtDID(i))
	}
	// All fit under capacity, so all should still be present.
	present := 0
	for i := range n {
		if c.seen(fmtDID(i)) {
			present++
		}
	}
	require.Equal(t, n, present)
}
