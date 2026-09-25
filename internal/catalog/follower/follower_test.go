package follower_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/catalog/follower"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/storagefake"
)

// Seqs folded, and even sealed, between two ticks reach the log through
// RefsFrom before any later hot batch, exactly once (design §11.1).
func TestFollower_FoldBetweenTicks(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.setPhase(lifecycle.PhaseSteadyState)
	f, _, m := r.follower(followerOpts{manifest: true})
	ctx := t.Context()
	require.NoError(t, f.Refresh(ctx))
	require.NoError(t, f.Ready(ctx))
	require.Equal(t, uint64(1), f.Log().FloorSeq(), "an empty archive's log starts at seq 1")

	r.commitHot(3, false) // 1-3
	require.NoError(t, f.Refresh(ctx))

	r.commitHot(3, true)  // 4-6
	r.commitHot(3, false) // 7-9
	r.fold(2)             // block 1-6
	r.commitHot(3, false) // 10-12
	require.NoError(t, f.Refresh(ctx))
	r.requireStream(logEvents(t, f, 1), 1)

	r.fold(2)            // block 7-12
	r.seal()             // segment 0
	r.commitHot(3, true) // 13-15
	require.NoError(t, f.Refresh(ctx))
	r.requireStream(logEvents(t, f, 1), 1)

	r.commitHot(3, false) // 16-18
	require.NoError(t, f.Refresh(ctx))
	r.commitHot(3, false) // 19-21
	r.fold(3)             // block 13-21
	r.seal()              // segment 1
	require.NoError(t, f.Refresh(ctx))
	r.requireStream(logEvents(t, f, 1), 1)
	r.requireStream(coldEvents(t, f, 1), 1)

	require.Len(t, m.SegmentChecksums(), 2, "both seals reached the manifest")
	require.Equal(t, uint64(22), f.Snapshot().TipSeq(catalog.Main))
}

// Property: over a random mix of inline and pointer batches, folds, seals,
// and skipped ticks, with the follower starting against a non-empty archive,
// the readable log is exactly the committed stream from the start tip on,
// and the cold path reads back the whole archive.
func TestFollower_LogMatchesCommittedStream(t *testing.T) {
	t.Parallel()
	for seed := range uint64(12) {
		t.Run("", func(t *testing.T) {
			t.Parallel()
			rng := rand.New(rand.NewPCG(seed, 0x5eed))
			r := newRig(t)
			r.setPhase(lifecycle.PhaseSteadyState)
			step := func() {
				switch n := rng.IntN(10); {
				case n < 5 || len(r.hot) == 0:
					r.commitHot(1+rng.IntN(4), rng.IntN(3) == 0)
				case n < 8:
					r.fold(1 + rng.IntN(len(r.hot)))
				case len(r.active) > 0:
					r.seal()
				}
			}
			for range rng.IntN(30) {
				step()
			}
			f, _, m := r.follower(followerOpts{manifest: true})
			ctx := t.Context()
			require.NoError(t, f.Refresh(ctx))
			start := uint64(len(r.events)) + 1
			require.Equal(t, start, f.Log().FloorSeq(), "the log starts at the tip")
			for range 60 {
				step()
				if rng.IntN(3) == 0 {
					require.NoError(t, f.Refresh(ctx))
				}
			}
			require.NoError(t, f.Refresh(ctx))
			r.requireStream(logEvents(t, f, start), start)
			if len(r.events) > 0 {
				r.requireStream(coldEvents(t, f, 1), 1)
			}
			require.Len(t, m.SegmentChecksums(), int(r.seg))
		})
	}
}

// A lost NOTIFY delays the follower by at most one poll interval.
func TestFollower_LostNotifyCoveredByPoll(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r := newRig(t)
		r.setPhase(lifecycle.PhaseSteadyState)
		f, metrics, _ := r.follower(followerOpts{listen: true})
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- f.Run(ctx) }()
		synctest.Wait()
		require.NoError(t, f.Ready(ctx))

		r.commitHot(2, false)
		synctest.Wait()
		require.Equal(t, uint64(3), f.Log().TipSeq(), "NOTIFY wakes the follower")
		notified := testutil.ToFloat64(metrics.NotifyReceived)
		require.Positive(t, notified)

		fault := &storagefake.Fault{Kind: storagefake.FaultNotifyLost, TxKind: catalog.TxHotBatch, Ordinal: 1}
		r.db.InjectFaults(fault)
		r.commitHot(2, false)
		synctest.Wait()
		require.True(t, fault.Fired())
		require.Equal(t, uint64(3), f.Log().TipSeq(), "no NOTIFY, no tick yet")
		require.Equal(t, notified, testutil.ToFloat64(metrics.NotifyReceived))

		time.Sleep(follower.DefaultPollInterval)
		synctest.Wait()
		require.Equal(t, uint64(5), f.Log().TipSeq(), "the poll picked the batch up")
		r.requireStream(logEvents(t, f, 1), 1)

		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
	})
}

// A follower whose reads stall longer than MaxViewAge reports not ready, and
// recovers once a tick completes.
func TestFollower_SlowFollowerLosesReadiness(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r := newRig(t)
		r.setPhase(lifecycle.PhaseSteadyState)
		f, metrics, _ := r.follower(followerOpts{})
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- f.Run(ctx) }()
		synctest.Wait()
		require.NoError(t, f.Ready(ctx))

		r.db.InjectFaults(&storagefake.Fault{Kind: storagefake.FaultSlowRead, Ordinal: 1, Delay: 2 * follower.DefaultMaxViewAge})
		time.Sleep(follower.DefaultMaxViewAge + time.Second)
		require.ErrorIs(t, f.Ready(ctx), follower.ErrStale)
		require.Greater(t, testutil.ToFloat64(metrics.Lag), follower.DefaultMaxViewAge.Seconds())

		time.Sleep(follower.DefaultMaxViewAge)
		synctest.Wait()
		require.NoError(t, f.Ready(ctx))
		require.Empty(t, r.db.Unfired())

		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
	})
}

// Readiness needs steady state; the log and the manifest start there.
func TestFollower_BootstrapThenSteadyState(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	f, _, m := r.follower(followerOpts{manifest: true})
	ctx := t.Context()
	require.ErrorIs(t, f.Ready(ctx), follower.ErrLoading)

	r.setPhase(lifecycle.PhaseBootstrap)
	r.commitHot(2, false)
	r.fold(1)
	r.seal()
	require.NoError(t, f.Refresh(ctx))
	require.ErrorIs(t, f.Ready(ctx), lifecycle.ErrBootstrapInProgress)
	require.Nil(t, f.Log(), "no readable log before steady state")

	r.commitHot(2, true)
	r.setPhase(lifecycle.PhaseSteadyState)
	require.NoError(t, f.Refresh(ctx))
	require.NoError(t, f.Ready(ctx))
	require.Equal(t, uint64(5), f.Log().FloorSeq())
	require.Len(t, m.SegmentChecksums(), 1)
	r.requireStream(coldEvents(t, f, 1), 1)

	r.commitHot(1, false)
	require.NoError(t, f.Refresh(ctx))
	r.requireStream(logEvents(t, f, 5), 5)
}

// Another archive's catalog stops the follower instead of serving it.
func TestFollower_ForeignArchiveIsFatal(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	f, err := follower.New(follower.Config{DB: r.db, Blob: r.blob, ArchiveID: [16]byte{1}})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	err = f.Run(ctx)
	require.Error(t, err)
	require.False(t, errors.Is(err, context.DeadlineExceeded), "Run returned %v", err)
	require.ErrorContains(t, err, "archive_id")
}

// A sealed ref from a replaced generation is stale; the block cache keys a
// sealed block by its object's SHA-256 and does not cache active blocks.
func TestFollower_FetcherStaleAndCacheKeys(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.setPhase(lifecycle.PhaseSteadyState)
	r.commitHot(2, false)
	r.fold(1)
	r.seal()
	r.commitHot(2, false)
	r.fold(1)
	r.commitHot(2, false)
	f, _, _ := r.follower(followerOpts{})
	require.NoError(t, f.Refresh(t.Context()))

	var kinds []string
	for ref := range f.Snapshot().RefsFrom(catalog.Main, 1) {
		key, ok := f.BlockCacheKey(ref)
		switch ref.Loc.(type) {
		case catalog.InlineBlock:
			kinds = append(kinds, "inline")
			require.True(t, ok)
		case catalog.ObjectBlock:
			if ref.Generation != 0 {
				kinds = append(kinds, "sealed")
				require.True(t, ok)
				require.IsType(t, [32]byte{}, key)
				stale := ref
				stale.Generation++
				_, err := f.Fetch(t.Context(), stale)
				require.ErrorIs(t, err, catalog.ErrStaleRef)
			} else {
				kinds = append(kinds, "active")
				require.False(t, ok)
			}
		}
	}
	require.Equal(t, []string{"sealed", "active", "inline"}, kinds)
}

// The compaction deadline rides along with every tick; a malformed value
// only loses the hint.
func TestFollower_CompactionDeadline(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.setPhase(lifecycle.PhaseSteadyState)
	f, _, _ := r.follower(followerOpts{})
	ctx := t.Context()
	require.NoError(t, f.Refresh(ctx))
	_, ok := f.NextCompactionAt()
	require.False(t, ok)

	at := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	setDeadline := func(v []byte) {
		_, err := r.s.CommitMeta(ctx, []metastore.Op{{Kind: metastore.OpSet, Key: []byte(catalog.CompactionDeadlineKey), Value: v}})
		require.NoError(t, err)
		require.NoError(t, f.Refresh(ctx))
	}
	setDeadline(catalog.EncodeCompactionDeadline(at))
	got, ok := f.NextCompactionAt()
	require.True(t, ok)
	require.True(t, at.Equal(got))

	setDeadline(catalog.EncodeCompactionDeadline(time.Time{}))
	_, ok = f.NextCompactionAt()
	require.False(t, ok)

	setDeadline([]byte("junk"))
	_, ok = f.NextCompactionAt()
	require.False(t, ok)
	require.NoError(t, f.Ready(ctx))
}
