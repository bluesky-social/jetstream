package storagefake_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/catalog/catalogtest"
	"github.com/bluesky-social/jetstream/internal/leader"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/metastore/storetest"
	"github.com/bluesky-social/jetstream/internal/storagefake"
	"github.com/jcalabro/atmos/streaming"
	"github.com/stretchr/testify/require"
)

func TestContract(t *testing.T) {
	t.Parallel()
	catalogtest.Run(t, func(t *testing.T) catalogtest.Backend {
		db := storagefake.New(storagefake.Config{})
		return catalogtest.Backend{
			DB:        db,
			NewLocker: func() leader.Locker { return db.NewLease() },
			Listener:  db,
		}
	})
}

// leaderSession acquires the lease and returns a session for its epoch.
func leaderSession(t *testing.T, db *storagefake.DB) *catalog.Session {
	t.Helper()
	l := db.NewLease()
	require.NoError(t, l.Acquire(t.Context(), time.Hour))
	return catalog.NewSession(catalog.SessionConfig{DB: db, Epoch: l.Epoch()})
}

func TestMetaStoreContract(t *testing.T) {
	t.Parallel()
	storetest.Run(t, func(t *testing.T) metastore.Store {
		db := storagefake.New(storagefake.Config{})
		s := leaderSession(t, db)
		return db.MetaStore(func(ctx context.Context, ops []metastore.Op) error {
			_, err := s.CommitMeta(ctx, ops)
			return err
		})
	})
}

func TestMetaStoreReadOnly(t *testing.T) {
	t.Parallel()
	db := storagefake.New(storagefake.Config{})
	ctx := t.Context()
	s := leaderSession(t, db)
	_, err := s.CommitMeta(ctx, []metastore.Op{{Kind: metastore.OpSet, Key: []byte("k"), Value: []byte("v")}})
	require.NoError(t, err)

	ro := db.MetaStore(nil)
	v, err := ro.Get(ctx, []byte("k"))
	require.NoError(t, err)
	require.Equal(t, "v", string(v))
	require.ErrorIs(t, ro.Set(ctx, []byte("k"), []byte("x")), metastore.ErrReadOnly)
	require.ErrorIs(t, ro.Delete(ctx, []byte("k")), metastore.ErrReadOnly)
}

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func TestLease(t *testing.T) {
	t.Parallel()
	clk := &clock{now: time.Unix(1_000_000, 0)}
	db := storagefake.New(storagefake.Config{Now: clk.Now})
	ctx := t.Context()
	a, b := db.NewLease(), db.NewLease()
	require.NotEqual(t, a.Holder(), b.Holder())

	require.NoError(t, a.Acquire(ctx, 10*time.Second))
	require.Equal(t, uint64(1), a.Epoch())
	require.ErrorIs(t, b.Acquire(ctx, 10*time.Second), streaming.ErrLockHeld)
	require.ErrorIs(t, a.Acquire(ctx, 10*time.Second), streaming.ErrLockHeld, "acquire is not reentrant")
	require.ErrorIs(t, b.Renew(ctx, 10*time.Second), streaming.ErrNotHolder)

	clk.Advance(9 * time.Second)
	require.NoError(t, a.Renew(ctx, 10*time.Second))
	clk.Advance(9 * time.Second)
	require.ErrorIs(t, b.Acquire(ctx, 10*time.Second), streaming.ErrLockHeld, "renew extended the lease")

	// Expiry: renewing at the deadline fails, and another holder takes over
	// with the next epoch.
	clk.Advance(time.Second)
	require.ErrorIs(t, a.Renew(ctx, 10*time.Second), streaming.ErrNotHolder)
	require.NoError(t, b.Acquire(ctx, 10*time.Second))
	require.Equal(t, uint64(2), b.Epoch())
	require.ErrorIs(t, a.Release(ctx), streaming.ErrNotHolder, "an old holder cannot release the new lease")
	arch := db.Archive()
	require.Equal(t, uint64(2), arch.WriterEpoch)
	require.Equal(t, b.Holder(), arch.HolderID)

	require.NoError(t, b.Release(ctx))
	arch = db.Archive()
	require.Equal(t, [16]byte{}, arch.HolderID)
	require.Equal(t, uint64(2), arch.WriterEpoch, "release keeps the epoch")
	require.NoError(t, a.Acquire(ctx, time.Second))
	require.Equal(t, uint64(3), a.Epoch())
}

func TestAcquireWaitsForLeaderTransaction(t *testing.T) {
	t.Parallel()
	db := storagefake.New(storagefake.Config{})
	ctx := t.Context()
	a := db.NewLease()
	require.NoError(t, a.Acquire(ctx, time.Nanosecond))
	tx, err := db.Begin(ctx, catalog.TxMetadata)
	require.NoError(t, err)
	_, ok, err := tx.FenceBump(ctx, a.Epoch())
	require.NoError(t, err)
	require.True(t, ok)

	// The lease expired, but the takeover waits on the row lock.
	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	b := db.NewLease()
	require.ErrorIs(t, b.Acquire(short, time.Hour), context.DeadlineExceeded)

	done := make(chan error, 1)
	go func() { done <- b.Acquire(ctx, time.Hour) }()
	require.NoError(t, tx.Commit(ctx))
	require.NoError(t, <-done)
	require.Equal(t, a.Epoch()+1, b.Epoch())
}

func TestFenceWaitHonorsContext(t *testing.T) {
	t.Parallel()
	db := storagefake.New(storagefake.Config{})
	ctx := t.Context()
	s := leaderSession(t, db)
	tx1, err := db.Begin(ctx, catalog.TxMetadata)
	require.NoError(t, err)
	_, ok, err := tx1.FenceBump(ctx, s.Epoch())
	require.NoError(t, err)
	require.True(t, ok)

	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	tx2, err := db.Begin(short, catalog.TxMetadata)
	require.NoError(t, err)
	_, _, err = tx2.FenceBump(short, s.Epoch())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, tx2.Rollback(ctx))
	require.NoError(t, tx1.Commit(ctx))
}

func setMeta(ctx context.Context, s *catalog.Session, k, v string) error {
	_, err := s.CommitMeta(ctx, []metastore.Op{{Kind: metastore.OpSet, Key: []byte(k), Value: []byte(v)}})
	return err
}

func metaValue(t *testing.T, db *storagefake.DB, k string) (string, bool) {
	t.Helper()
	v, err := db.MetaStore(nil).Get(t.Context(), []byte(k))
	if errors.Is(err, metastore.ErrNotFound) {
		return "", false
	}
	require.NoError(t, err)
	return string(v), true
}

func TestFaultCommitFails(t *testing.T) {
	t.Parallel()
	db := storagefake.New(storagefake.Config{})
	s := leaderSession(t, db)
	f := &storagefake.Fault{Kind: storagefake.FaultCommitFails, TxKind: catalog.TxMetadata, Ordinal: 2}
	db.InjectFaults(f)
	require.NoError(t, setMeta(t.Context(), s, "a", "1"))
	rev := db.Archive().CatalogRevision
	err := setMeta(t.Context(), s, "b", "2")
	require.ErrorIs(t, err, catalog.ErrSessionEnded)
	require.True(t, f.Fired())
	require.Empty(t, db.Unfired())
	_, found := metaValue(t, db, "b")
	require.False(t, found)
	require.Equal(t, rev, db.Archive().CatalogRevision)
	require.ErrorIs(t, setMeta(t.Context(), s, "c", "3"), catalog.ErrSessionEnded, "a failed session is never retried")
}

func TestFaultCommitLost(t *testing.T) {
	t.Parallel()
	db := storagefake.New(storagefake.Config{})
	s := leaderSession(t, db)
	f := &storagefake.Fault{Kind: storagefake.FaultCommitLost, Ordinal: 1}
	db.InjectFaults(f)
	err := setMeta(t.Context(), s, "a", "1")
	require.ErrorIs(t, err, catalog.ErrSessionEnded)
	require.ErrorIs(t, err, storagefake.ErrCommitLost)
	v, found := metaValue(t, db, "a")
	require.True(t, found, "a lost commit result still applied")
	require.Equal(t, "1", v)
}

func TestFaultConnLost(t *testing.T) {
	t.Parallel()
	for _, stmt := range []int{0, 1, 2} {
		t.Run(fmt.Sprint("statement ", stmt), func(t *testing.T) {
			t.Parallel()
			db := storagefake.New(storagefake.Config{})
			s := leaderSession(t, db)
			f := &storagefake.Fault{Kind: storagefake.FaultConnLost, Ordinal: 1, Statement: stmt}
			db.InjectFaults(f)
			rev := db.Archive().CatalogRevision
			err := setMeta(t.Context(), s, "a", "1")
			require.ErrorIs(t, err, storagefake.ErrConnLost)
			require.ErrorIs(t, err, catalog.ErrSessionEnded)
			require.True(t, f.Fired())
			_, found := metaValue(t, db, "a")
			require.False(t, found)
			require.Equal(t, rev, db.Archive().CatalogRevision)
			// The dead connection released the row lock.
			require.NoError(t, setMeta(t.Context(), catalog.NewSession(catalog.SessionConfig{DB: db, Epoch: s.Epoch()}), "b", "2"))
		})
	}
}

func TestFaultNotifyLost(t *testing.T) {
	t.Parallel()
	db := storagefake.New(storagefake.Config{})
	s := leaderSession(t, db)
	ctx, cancel := context.WithCancel(t.Context())
	ch, err := db.Listen(ctx)
	require.NoError(t, err)
	f := &storagefake.Fault{Kind: storagefake.FaultNotifyLost, Ordinal: 1}
	db.InjectFaults(f)
	require.NoError(t, setMeta(ctx, s, "a", "1"))
	require.NoError(t, setMeta(ctx, s, "b", "2"))
	rev := db.Archive().CatalogRevision
	require.Equal(t, rev, <-ch, "only the second commit notifies")
	require.True(t, f.Fired())
	cancel()
	for range ch {
	}
}

func TestFaultSlowRead(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		db := storagefake.New(storagefake.Config{})
		s := leaderSession(t, db)
		db.InjectFaults(&storagefake.Fault{Kind: storagefake.FaultSlowRead, Ordinal: 1, Delay: time.Minute})
		require.NoError(t, setMeta(t.Context(), s, "a", "1"))
		start := time.Now()
		var r catalog.ReadTx
		done := make(chan struct{})
		go func() {
			defer close(done)
			var err error
			r, err = db.BeginRead(t.Context())
			require.NoError(t, err)
		}()
		synctest.Wait()
		// The snapshot is taken after the delay, so it sees this commit.
		require.NoError(t, setMeta(t.Context(), s, "a", "2"))
		<-done
		require.Equal(t, time.Minute, time.Since(start))
		got, err := r.MetaGet(t.Context(), [][]byte{[]byte("a")})
		require.NoError(t, err)
		require.Equal(t, "2", string(got["a"]))
		require.Empty(t, db.Unfired())
	})
}

func TestUnfired(t *testing.T) {
	t.Parallel()
	db := storagefake.New(storagefake.Config{})
	s := leaderSession(t, db)
	f := &storagefake.Fault{Kind: storagefake.FaultCommitFails, TxKind: catalog.TxHotBatch, Ordinal: 1}
	db.InjectFaults(f)
	require.NoError(t, setMeta(t.Context(), s, "a", "1"))
	require.Equal(t, []*storagefake.Fault{f}, db.Unfired(), "a fault for another transaction kind does not fire")
}

// An unfenced write really commits and the invariant check reports it
// (plan D1: the fake emulates PostgreSQL, it does not re-implement the
// scripts' checks).
func TestInvariantViolationRecorded(t *testing.T) {
	t.Parallel()
	var seen atomic.Int32
	db := storagefake.New(storagefake.Config{OnViolation: func(uint64, error) { seen.Add(1) }})
	ctx := t.Context()
	s := leaderSession(t, db)
	_, err := s.InitNamespace(ctx, catalog.Main, nil)
	require.NoError(t, err)
	require.NoError(t, db.Violation())

	tx, err := db.Begin(ctx, catalog.TxHotBatch)
	require.NoError(t, err)
	_, ok, err := tx.FenceBump(ctx, s.Epoch())
	require.NoError(t, err)
	require.True(t, ok)
	// A batch whose seq range skips the stored counter.
	require.NoError(t, tx.InsertHotBatch(ctx, catalog.HotBatchRow{
		FirstSeq: 5, LastSeq: 5, EventCount: 1, Epoch: s.Epoch(), Revision: 1, Frame: []byte("f"),
	}))
	require.NoError(t, tx.Commit(ctx))
	require.Error(t, db.Violation())
	require.Equal(t, int32(1), seen.Load())
}

// The seeded scheduler replays the same interleaving at the storage
// boundary for the same seed (plan D4).
func TestSeededDeterminism(t *testing.T) {
	t.Parallel()
	run := func(seed uint64) (trace []string, final string) {
		synctest.Test(t, func(t *testing.T) {
			sched := storagefake.NewSeeded(seed)
			db := storagefake.New(storagefake.Config{Scheduler: sched, Now: func() time.Time { return time.Unix(0, 0) }})
			ctx, cancel := context.WithCancel(t.Context())
			schedDone := make(chan struct{})
			go func() { defer close(schedDone); sched.Run(ctx) }()

			lease := db.NewLease()
			require.NoError(t, lease.Acquire(storagefake.WithActor(ctx, "lease"), time.Hour))
			s := catalog.NewSession(catalog.SessionConfig{DB: db, Epoch: lease.Epoch()})
			var wg sync.WaitGroup
			for w := range 3 {
				wg.Go(func() {
					actx := storagefake.WithActor(ctx, fmt.Sprint("writer-", w))
					for i := range 3 {
						_ = setMeta(actx, s, "k", fmt.Sprint(w, "/", i))
					}
				})
			}
			wg.Go(func() {
				actx := storagefake.WithActor(ctx, "reader")
				for range 3 {
					r, err := db.BeginRead(actx)
					if err == nil {
						_, _ = r.MetaGet(actx, [][]byte{[]byte("k")})
						_ = r.Close(actx)
					}
				}
			})
			wg.Wait()
			cancel()
			<-schedDone
			trace = sched.Trace()
			final, _ = metaValue(t, db, "k")
			require.NoError(t, s.Err())
		})
		return trace, final
	}
	t1, f1 := run(1)
	t2, f2 := run(1)
	require.NotEmpty(t, t1)
	require.Equal(t, t1, t2)
	require.Equal(t, f1, f2)

	distinct := map[string]bool{}
	for seed := range uint64(8) {
		tr, _ := run(seed + 100)
		distinct[fmt.Sprint(tr)] = true
	}
	require.Greater(t, len(distinct), 1, "different seeds explore different interleavings")
}
