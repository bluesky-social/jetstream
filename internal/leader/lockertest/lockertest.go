// Package lockertest is the leader.Locker contract suite (plan S2.3, audit
// finding 6). It runs against leader.Local (the subset that applies), the
// storagefake lease in every `just`, and the PostgreSQL lease under
// `just test-storage`, so the election loop's assumptions hold for every
// backend the oracle stands in for.
package lockertest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/leader"
	"github.com/jcalabro/atmos/streaming"
	"github.com/stretchr/testify/require"
)

// Backend is one fresh lock.
type Backend struct {
	// NewLocker returns a locker on the shared lock with its own holder ID,
	// as a separate process would have.
	NewLocker func() leader.Locker
	// Exclusive is false for a lock that is always held (leader.Local);
	// only the lifecycle test applies to it.
	Exclusive bool
	// Advance moves the lock's clock forward by d: a fake clock's Advance,
	// or a sleep for a real one.
	Advance func(d time.Duration)
	// Lease is the lease duration the tests use. A real clock needs one
	// long enough to outlast a statement; a fake clock can use anything.
	Lease time.Duration
	// Fence runs a fenced write transaction at epoch (design §6.4) and
	// reports whether the fence passed. Nil skips the fence test.
	Fence func(ctx context.Context, epoch uint64) (bool, error)
}

// CatalogFence is a Backend.Fence over a catalog database.
func CatalogFence(db catalog.DB) func(ctx context.Context, epoch uint64) (bool, error) {
	return func(ctx context.Context, epoch uint64) (bool, error) {
		tx, err := db.Begin(ctx, catalog.TxMetadata)
		if err != nil {
			return false, err
		}
		_, ok, err := tx.FenceBump(ctx, epoch)
		if err != nil || !ok {
			_ = tx.Rollback(ctx)
			return false, err
		}
		return true, tx.Commit(ctx)
	}
}

// Run runs every applicable contract test, each against its own Backend.
func Run(t *testing.T, newBackend func(t *testing.T) Backend) {
	tests := []struct {
		name      string
		exclusive bool
		fn        func(t *testing.T, b Backend)
	}{
		{"Lifecycle", false, testLifecycle},
		{"AcquireHeld", true, testAcquireHeld},
		{"RenewExtends", true, testRenewExtends},
		{"RenewAfterExpiry", true, testRenewAfterExpiry},
		{"RenewAfterTakeover", true, testRenewAfterTakeover},
		{"ReleaseNotHolder", true, testReleaseNotHolder},
		{"EpochIncreases", true, testEpochIncreases},
		{"RacingAcquire", true, testRacingAcquire},
		{"OldEpochFenced", true, testOldEpochFenced},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend(t)
			if tc.exclusive && !b.Exclusive {
				t.Skip("lock is not exclusive")
			}
			tc.fn(t, b)
		})
	}
}

func ctxT(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// Acquire, renew, release, and acquire again all succeed for a sole
// locker, and a held lock always has a positive epoch.
func testLifecycle(t *testing.T, b Backend) {
	ctx := ctxT(t)
	l := b.NewLocker()
	require.NoError(t, l.Acquire(ctx, b.Lease))
	require.Positive(t, l.Epoch())
	require.NoError(t, l.Renew(ctx, b.Lease))
	require.NoError(t, l.Release(ctx))
	require.NoError(t, l.Acquire(ctx, b.Lease))
	require.Positive(t, l.Epoch())
	require.NoError(t, l.Release(ctx))
}

func testAcquireHeld(t *testing.T, b Backend) {
	ctx := ctxT(t)
	a, o := b.NewLocker(), b.NewLocker()
	require.NoError(t, a.Acquire(ctx, b.Lease))
	require.ErrorIs(t, o.Acquire(ctx, b.Lease), streaming.ErrLockHeld)
	require.ErrorIs(t, a.Acquire(ctx, b.Lease), streaming.ErrLockHeld, "acquire is not reentrant")
	require.ErrorIs(t, o.Renew(ctx, b.Lease), streaming.ErrNotHolder, "a locker that never acquired cannot renew")
}

func testRenewExtends(t *testing.T, b Backend) {
	ctx := ctxT(t)
	a, o := b.NewLocker(), b.NewLocker()
	require.NoError(t, a.Acquire(ctx, b.Lease))
	b.Advance(b.Lease * 6 / 10)
	require.NoError(t, a.Renew(ctx, b.Lease))
	b.Advance(b.Lease * 6 / 10)
	require.ErrorIs(t, o.Acquire(ctx, b.Lease), streaming.ErrLockHeld, "the renewal outlived the first lease")
	require.NoError(t, a.Renew(ctx, b.Lease))
}

// A holder whose lease expired cannot renew, even with no one else holding.
func testRenewAfterExpiry(t *testing.T, b Backend) {
	ctx := ctxT(t)
	a := b.NewLocker()
	require.NoError(t, a.Acquire(ctx, b.Lease))
	b.Advance(b.Lease * 12 / 10)
	require.ErrorIs(t, a.Renew(ctx, b.Lease), streaming.ErrNotHolder)
	// It can only come back through Acquire, as a new epoch.
	e := a.Epoch()
	require.NoError(t, a.Acquire(ctx, b.Lease))
	require.Greater(t, a.Epoch(), e)
}

func testRenewAfterTakeover(t *testing.T, b Backend) {
	ctx := ctxT(t)
	a, o := b.NewLocker(), b.NewLocker()
	require.NoError(t, a.Acquire(ctx, b.Lease))
	b.Advance(b.Lease * 12 / 10)
	require.NoError(t, o.Acquire(ctx, b.Lease))
	require.ErrorIs(t, a.Renew(ctx, b.Lease), streaming.ErrNotHolder)
	require.NoError(t, o.Renew(ctx, b.Lease))
}

func testReleaseNotHolder(t *testing.T, b Backend) {
	ctx := ctxT(t)
	a, o, c := b.NewLocker(), b.NewLocker(), b.NewLocker()
	require.ErrorIs(t, a.Release(ctx), streaming.ErrNotHolder, "never acquired")
	require.NoError(t, a.Acquire(ctx, b.Lease))
	require.ErrorIs(t, o.Release(ctx), streaming.ErrNotHolder, "another locker's lease")
	b.Advance(b.Lease * 12 / 10)
	require.NoError(t, o.Acquire(ctx, b.Lease))
	require.ErrorIs(t, a.Release(ctx), streaming.ErrNotHolder, "an old holder cannot release the new lease")
	require.ErrorIs(t, c.Acquire(ctx, b.Lease), streaming.ErrLockHeld, "the new holder still holds it")
	require.NoError(t, o.Release(ctx))
	require.ErrorIs(t, o.Release(ctx), streaming.ErrNotHolder, "release is not idempotent")
	require.NoError(t, c.Acquire(ctx, b.Lease), "release frees the lock without waiting for expiry")
}

func testEpochIncreases(t *testing.T, b Backend) {
	ctx := ctxT(t)
	a, o := b.NewLocker(), b.NewLocker()
	var last uint64
	step := func(l leader.Locker, expire bool) {
		t.Helper()
		require.NoError(t, l.Acquire(ctx, b.Lease))
		require.Greater(t, l.Epoch(), last)
		last = l.Epoch()
		if expire {
			b.Advance(b.Lease * 12 / 10)
		} else {
			require.NoError(t, l.Release(ctx))
		}
	}
	step(a, false)
	step(o, false)
	step(a, true)
	step(o, true)
	step(o, false) // the same locker again
	step(a, false)
}

func testRacingAcquire(t *testing.T, b Backend) {
	ctx := ctxT(t)
	const n = 8
	lockers := make([]leader.Locker, n)
	for i := range lockers {
		lockers[i] = b.NewLocker()
	}
	errs := make([]error, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, l := range lockers {
		wg.Go(func() {
			<-start
			errs[i] = l.Acquire(ctx, b.Lease)
		})
	}
	close(start)
	wg.Wait()
	winners := 0
	for _, err := range errs {
		if err == nil {
			winners++
			continue
		}
		require.ErrorIs(t, err, streaming.ErrLockHeld)
	}
	require.Equal(t, 1, winners)
}

// Once a new holder acquires, the old epoch's fence fails: this, not lease
// timing, is what keeps a paused old leader from writing (design §6.3).
func testOldEpochFenced(t *testing.T, b Backend) {
	if b.Fence == nil {
		t.Skip("backend has no fence")
	}
	ctx := ctxT(t)
	a, o := b.NewLocker(), b.NewLocker()
	require.NoError(t, a.Acquire(ctx, b.Lease))
	old := a.Epoch()
	ok, err := b.Fence(ctx, old)
	require.NoError(t, err)
	require.True(t, ok)

	b.Advance(b.Lease * 12 / 10)
	ok, err = b.Fence(ctx, old)
	require.NoError(t, err)
	require.True(t, ok, "expiry alone does not fence: only a new epoch does")

	require.NoError(t, o.Acquire(ctx, b.Lease))
	ok, err = b.Fence(ctx, old)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = b.Fence(ctx, o.Epoch())
	require.NoError(t, err)
	require.True(t, ok)
}
