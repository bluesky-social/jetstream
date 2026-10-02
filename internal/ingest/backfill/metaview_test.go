package backfill

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/metastore/memstore"
	"github.com/stretchr/testify/require"
)

// countingStore counts the round trips a metastore.Store serves, so tests can
// pin how many reads and commits a batched path makes.
type countingStore struct {
	metastore.Store
	gets, getManys, commits atomic.Int64
	// commitDelay, when set, slows every commit, standing in for a
	// PostgreSQL transaction so concurrent writers queue behind it.
	commitDelay time.Duration
}

func (s *countingStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	s.gets.Add(1)
	return s.Store.Get(ctx, key)
}

func (s *countingStore) GetMany(ctx context.Context, keys [][]byte) ([][]byte, error) {
	s.getManys.Add(1)
	return s.Store.GetMany(ctx, keys)
}

func (s *countingStore) NewBatch() metastore.Batch {
	return &countingBatch{Batch: s.Store.NewBatch(), s: s}
}

func (s *countingStore) reset() {
	s.gets.Store(0)
	s.getManys.Store(0)
	s.commits.Store(0)
}

type countingBatch struct {
	metastore.Batch
	s *countingStore
}

func (b *countingBatch) Commit(ctx context.Context) error {
	b.s.commits.Add(1)
	if b.s.commitDelay > 0 {
		time.Sleep(b.s.commitDelay)
	}
	return b.Batch.Commit(ctx)
}

func TestMetaView_ReadsItsWritesWithoutRoundTrips(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := &countingStore{Store: memstore.New()}
	require.NoError(t, db.Set(ctx, []byte("a"), []byte("1")))
	require.NoError(t, db.Set(ctx, []byte("b"), []byte("2")))
	require.NoError(t, db.Set(ctx, []byte("r/1"), []byte("x")))
	require.NoError(t, db.Set(ctx, []byte("r/2"), []byte("y")))

	v := newMetaView(db)
	require.NoError(t, v.prefetch(ctx, [][]byte{[]byte("a"), []byte("a"), []byte("missing"), []byte("r/1")}))
	require.Equal(t, int64(1), db.getManys.Load())
	// Known keys, present or absent, cost nothing more.
	require.NoError(t, v.prefetch(ctx, [][]byte{[]byte("a"), []byte("missing")}))
	require.Equal(t, int64(1), db.getManys.Load())

	got, err := v.Get(ctx, []byte("a"))
	require.NoError(t, err)
	require.Equal(t, "1", string(got))
	got[0] = 'X' // a caller's copy must not alias the cache
	got, err = v.Get(ctx, []byte("a"))
	require.NoError(t, err)
	require.Equal(t, "1", string(got))
	_, err = v.Get(ctx, []byte("missing"))
	require.ErrorIs(t, err, metastore.ErrNotFound)
	require.Zero(t, db.gets.Load())

	// A miss falls through once, then is cached.
	_, err = v.Get(ctx, []byte("b"))
	require.NoError(t, err)
	_, err = v.Get(ctx, []byte("b"))
	require.NoError(t, err)
	require.Equal(t, int64(1), db.gets.Load())

	b := v.batch(db.NewBatch())
	b.Set([]byte("a"), []byte("staged"))
	b.Set([]byte("empty"), nil)
	b.Delete([]byte("b"))
	b.DeleteRange([]byte("r/"), []byte("r0"))
	b.Set([]byte("r/2"), []byte("again"))

	got, err = v.Get(ctx, []byte("a"))
	require.NoError(t, err)
	require.Equal(t, "staged", string(got))
	got, err = v.Get(ctx, []byte("empty"))
	require.NoError(t, err, "an empty staged value is present")
	require.Empty(t, got)
	_, err = v.Get(ctx, []byte("b"))
	require.ErrorIs(t, err, metastore.ErrNotFound)
	_, err = v.Get(ctx, []byte("r/1"))
	require.ErrorIs(t, err, metastore.ErrNotFound, "a staged DeleteRange hides the key")
	vals, err := v.GetMany(ctx, [][]byte{[]byte("r/2"), []byte("r/3"), []byte("empty")})
	require.NoError(t, err)
	require.Equal(t, "again", string(vals[0]), "a Set after the DeleteRange is visible")
	require.Nil(t, vals[1], "a key in a deleted range is known absent without a read")
	require.NotNil(t, vals[2])
	require.Equal(t, int64(1), db.getManys.Load())
	require.Equal(t, int64(1), db.gets.Load())

	// Nothing reaches the store until the batch commits.
	stored, err := db.Store.Get(ctx, []byte("a"))
	require.NoError(t, err)
	require.Equal(t, "1", string(stored))
	require.NoError(t, b.Commit(ctx))
	stored, err = db.Store.Get(ctx, []byte("r/2"))
	require.NoError(t, err)
	require.Equal(t, "again", string(stored))
	_, err = db.Store.Get(ctx, []byte("r/1"))
	require.ErrorIs(t, err, metastore.ErrNotFound)
}

func TestMetaView_RefusesBypassingWrites(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	v := newMetaView(memstore.New())
	_, err := v.NewIter(ctx, nil, nil)
	require.ErrorIs(t, err, errViewMisuse)
	require.ErrorIs(t, v.Set(ctx, []byte("k"), []byte("v")), errViewMisuse)
	require.ErrorIs(t, v.Delete(ctx, []byte("k")), errViewMisuse)
	b := v.NewBatch()
	b.Set([]byte("k"), []byte("v"))
	require.ErrorIs(t, b.Commit(ctx), errViewMisuse)
}

// TestGroupCommitter_CoalescesQueuedWrites holds the leader inside its commit
// while more writers queue: they must all ride the next commit, each must see
// the result exactly once, and committed hooks run only for a success.
func TestGroupCommitter_CoalescesQueuedWrites(t *testing.T) {
	t.Parallel()
	var g groupCommitter
	inFirst := make(chan struct{})
	release := make(chan struct{})
	errSecond := errors.New("second group failed")
	var mu sync.Mutex
	var groups []int
	commitGroup := func(group []*groupWrite) error {
		mu.Lock()
		groups = append(groups, len(group))
		n := len(groups)
		mu.Unlock()
		if n == 1 {
			close(inFirst)
			<-release
			return nil
		}
		return errSecond
	}

	var committed atomic.Int32
	write := func() *groupWrite { return &groupWrite{committed: func() { committed.Add(1) }} }
	leaderErr := make(chan error, 1)
	go func() { leaderErr <- g.commit(write(), commitGroup) }()
	<-inFirst

	const queued = 10
	errs := make(chan error, queued)
	for range queued {
		go func() { errs <- g.commit(write(), commitGroup) }()
	}
	require.Eventually(t, func() bool {
		g.mu.Lock()
		defer g.mu.Unlock()
		return len(g.pending) == queued
	}, 5*time.Second, time.Millisecond)
	close(release)

	require.NoError(t, <-leaderErr)
	for range queued {
		require.ErrorIs(t, <-errs, errSecond, "every member of a failed group gets its error")
	}
	mu.Lock()
	require.Equal(t, []int{1, queued}, groups)
	mu.Unlock()
	require.Equal(t, int32(1), committed.Load(), "committed runs only for the successful group")
	g.mu.Lock()
	require.False(t, g.active, "the lead is released once the queue drains")
	g.mu.Unlock()
}

// TestGroupCommitter_ManyWritersNeverStall runs many concurrent writers
// through a slow commit: every writer returns, and the commits number far
// fewer than the writes.
func TestGroupCommitter_ManyWritersNeverStall(t *testing.T) {
	t.Parallel()
	var g groupCommitter
	var commits, written atomic.Int64
	commitGroup := func(group []*groupWrite) error {
		commits.Add(1)
		written.Add(int64(len(group)))
		time.Sleep(time.Millisecond)
		return nil
	}
	const writers, each = 32, 20
	var wg sync.WaitGroup
	for range writers {
		wg.Go(func() {
			for range each {
				require.NoError(t, g.commit(&groupWrite{}, commitGroup))
			}
		})
	}
	wg.Wait()
	require.Equal(t, int64(writers*each), written.Load())
	require.Less(t, commits.Load(), int64(writers*each))
}
