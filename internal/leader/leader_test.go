package leader

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/streaming"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// fakeLocker scripts lock results. Each hook, when set, decides one call;
// otherwise the call succeeds. Acquire bumps the epoch on success, as the
// lease SQL does.
type fakeLocker struct {
	mu       sync.Mutex
	epoch    uint64
	acquires int
	renews   int
	releases []uint64 // epoch at each Release
	inCall   bool

	acquire func(n int) error
	renew   func(ctx context.Context, n int) error
	release func(ctx context.Context) error
}

func (f *fakeLocker) enter() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.inCall {
		panic("fakeLocker: concurrent lock calls")
	}
	f.inCall = true
}

func (f *fakeLocker) exit() {
	f.mu.Lock()
	f.inCall = false
	f.mu.Unlock()
}

func (f *fakeLocker) Acquire(_ context.Context, lease time.Duration) error {
	f.enter()
	defer f.exit()
	f.mu.Lock()
	f.acquires++
	n, hook := f.acquires, f.acquire
	f.mu.Unlock()
	if hook != nil {
		if err := hook(n); err != nil {
			return err
		}
	}
	f.mu.Lock()
	f.epoch++
	f.mu.Unlock()
	return nil
}

func (f *fakeLocker) Renew(ctx context.Context, _ time.Duration) error {
	f.enter()
	defer f.exit()
	f.mu.Lock()
	f.renews++
	n, hook := f.renews, f.renew
	f.mu.Unlock()
	if hook != nil {
		return hook(ctx, n)
	}
	return nil
}

func (f *fakeLocker) Release(ctx context.Context) error {
	f.enter()
	defer f.exit()
	f.mu.Lock()
	f.releases = append(f.releases, f.epoch)
	hook := f.release
	f.mu.Unlock()
	if hook != nil {
		return hook(ctx)
	}
	return nil
}

func (f *fakeLocker) Epoch() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.epoch
}

func (f *fakeLocker) releasedEpochs() []uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uint64(nil), f.releases...)
}

// sessionEnd records how one session ended.
type sessionEnd struct {
	epoch   uint64
	started time.Duration
	ended   time.Duration
}

// recorder runs sessions that block until cancelled, or return what next
// says, and records when each started and ended relative to t0.
type recorder struct {
	t0   time.Time
	mu   sync.Mutex
	ends []sessionEnd
	// next, when set, decides session n (1-based): a nil error blocks until
	// ctx is cancelled.
	next func(n int) error
	// stop is cancelled when session stopAt starts, which then sees the
	// shutdown and ends.
	stop   context.CancelFunc
	stopAt int
}

func (r *recorder) session(ctx context.Context, epoch uint64) error {
	start := time.Since(r.t0)
	r.mu.Lock()
	n := len(r.ends) + 1
	r.mu.Unlock()
	if n == r.stopAt && r.stop != nil {
		r.stop()
	}
	var err error
	if r.next != nil {
		err = r.next(n)
	}
	if err == nil {
		<-ctx.Done()
		err = ctx.Err()
	}
	r.mu.Lock()
	r.ends = append(r.ends, sessionEnd{epoch: epoch, started: start, ended: time.Since(r.t0)})
	r.mu.Unlock()
	return err
}

func (r *recorder) endsCopy() []sessionEnd {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]sessionEnd(nil), r.ends...)
}

func newRecorder(stop context.CancelFunc, stopAt int) *recorder {
	return &recorder{t0: time.Now(), stop: stop, stopAt: stopAt}
}

func TestRun_RenewFailureCancelsAfterLease(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := bubbleCtx(t)
		defer cancel()
		lk := &fakeLocker{renew: func(_ context.Context, n int) error {
			if n <= 3 {
				return errors.New("connection refused")
			}
			return nil
		}}
		rec := newRecorder(cancel, 2)
		reg := prometheus.NewRegistry()
		m := NewMetrics(reg)

		require.NoError(t, Run(ctx, Config{Locker: lk, Metrics: m}, rec.session))

		ends := rec.endsCopy()
		require.Len(t, ends, 2)
		// Renews at 1s and 2s fail and are retried; the one at 3s finds a
		// full lease without a successful renew.
		require.Equal(t, sessionEnd{epoch: 1, started: 0, ended: 3 * time.Second}, ends[0])
		// The next session starts one acquire interval later, under a new
		// epoch, and renews normally until shutdown.
		require.Equal(t, uint64(2), ends[1].epoch)
		require.Equal(t, 3*time.Second+DefaultAcquireInterval, ends[1].started)
		require.Equal(t, []uint64{1, 2}, lk.releasedEpochs())
		require.InDelta(t, 1, testutil.ToFloat64(m.LeaseLostTotal), 0)
		require.InDelta(t, 3, testutil.ToFloat64(m.RenewErrors), 0)
		require.InDelta(t, 1, testutil.ToFloat64(m.SessionsTotal.WithLabelValues(reasonLeaseLost)), 0)
		require.InDelta(t, 1, testutil.ToFloat64(m.SessionsTotal.WithLabelValues(reasonShutdown)), 0)
		require.InDelta(t, 0, testutil.ToFloat64(m.IsLeader), 0)
	})
}

func TestRun_TransientRenewFailureKeepsSession(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := bubbleCtx(t)
		defer cancel()
		// Every other renew fails: never a full lease without success.
		lk := &fakeLocker{renew: func(_ context.Context, n int) error {
			if n%2 == 1 {
				return errors.New("timeout")
			}
			return nil
		}}
		rec := newRecorder(nil, 1)
		go func() {
			time.Sleep(time.Minute)
			cancel()
		}()
		require.NoError(t, Run(ctx, Config{Locker: lk}, rec.session))
		ends := rec.endsCopy()
		require.Len(t, ends, 1)
		require.Equal(t, time.Minute, ends[0].ended)
	})
}

func TestRun_ErrNotHolderCancelsImmediately(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := bubbleCtx(t)
		defer cancel()
		lk := &fakeLocker{renew: func(_ context.Context, n int) error {
			if n == 1 {
				return fmt.Errorf("renew: %w", streaming.ErrNotHolder)
			}
			return nil
		}}
		rec := newRecorder(cancel, 2)
		require.NoError(t, Run(ctx, Config{Locker: lk}, rec.session))
		ends := rec.endsCopy()
		require.Len(t, ends, 2)
		require.Equal(t, DefaultRenewInterval, ends[0].ended)
		require.Equal(t, uint64(2), ends[1].epoch)
	})
}

func TestRun_HungRenewBoundedByLease(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := bubbleCtx(t)
		defer cancel()
		lk := &fakeLocker{renew: func(ctx context.Context, n int) error {
			if n == 1 {
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		}}
		rec := newRecorder(cancel, 2)
		require.NoError(t, Run(ctx, Config{Locker: lk}, rec.session))
		ends := rec.endsCopy()
		require.Len(t, ends, 2)
		require.Equal(t, DefaultLease, ends[0].ended)
	})
}

func TestRun_FatalErrorExits(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		corrupt := fmt.Errorf("decode block: %w", segment.ErrCorruptSegment)
		lk := &fakeLocker{}
		rec := newRecorder(nil, 0)
		rec.next = func(int) error {
			time.Sleep(2 * time.Second)
			return corrupt
		}
		reg := prometheus.NewRegistry()
		m := NewMetrics(reg)

		err := Run(t.Context(), Config{Locker: lk, Metrics: m}, rec.session)
		require.ErrorIs(t, err, segment.ErrCorruptSegment)
		require.Len(t, rec.endsCopy(), 1)
		require.Equal(t, []uint64{1}, lk.releasedEpochs(), "a fatal session still releases the lock")
		require.InDelta(t, 1, testutil.ToFloat64(m.SessionsTotal.WithLabelValues(reasonFatal)), 0)
	})
}

func TestRun_FatalErrorWinsOverLeaseLoss(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		lk := &fakeLocker{renew: func(context.Context, int) error { return streaming.ErrNotHolder }}
		corrupt := fmt.Errorf("verify: %w", segment.ErrChecksumMismatch)
		err := Run(t.Context(), Config{Locker: lk}, func(ctx context.Context, _ uint64) error {
			<-ctx.Done()
			return corrupt
		})
		require.ErrorIs(t, err, segment.ErrChecksumMismatch)
	})
}

func TestRun_RestartableErrorStartsNewSession(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := bubbleCtx(t)
		defer cancel()
		lk := &fakeLocker{}
		rec := newRecorder(cancel, 2)
		rec.next = func(n int) error {
			if n == 1 {
				time.Sleep(time.Second)
				return fmt.Errorf("fenced: %w", ErrRestartSession)
			}
			return nil
		}
		require.NoError(t, Run(ctx, Config{Locker: lk}, rec.session))
		ends := rec.endsCopy()
		require.Len(t, ends, 2)
		require.Equal(t, uint64(2), ends[1].epoch)
		require.Equal(t, time.Second+DefaultAcquireInterval, ends[1].started)
	})
}

func TestRun_CustomFatalClassifier(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := bubbleCtx(t)
		defer cancel()
		transient := errors.New("s3: 503")
		lk := &fakeLocker{}
		rec := newRecorder(cancel, 2)
		rec.next = func(n int) error {
			if n == 1 {
				return transient
			}
			return nil
		}
		fatal := func(err error) bool { return !errors.Is(err, transient) }
		require.NoError(t, Run(ctx, Config{Locker: lk, Fatal: fatal}, rec.session))
		require.Len(t, rec.endsCopy(), 2)
	})
}

// A deadline error from inside the session is not a cancellation by the loop,
// so it is classified like any other error.
func TestRun_InternalDeadlineIsClassified(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		err := Run(t.Context(), Config{Locker: &fakeLocker{}}, func(context.Context, uint64) error {
			return fmt.Errorf("fetch: %w", context.DeadlineExceeded)
		})
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestRun_ReleaseIsBestEffort(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := bubbleCtx(t)
		defer cancel()
		var releaseCtxAlive []bool
		lk := &fakeLocker{release: func(rctx context.Context) error {
			releaseCtxAlive = append(releaseCtxAlive, rctx.Err() == nil)
			if len(releaseCtxAlive) == 1 {
				// Hang: bounded by ReleaseTimeout.
				<-rctx.Done()
				return rctx.Err()
			}
			return errors.New("connection reset")
		}}
		rec := newRecorder(nil, 0)
		rec.next = func(n int) error {
			if n == 1 {
				return ErrRestartSession
			}
			return nil
		}
		go func() {
			// Session 2 starts after the hung release and one acquire
			// interval; shut down while it runs.
			time.Sleep(DefaultReleaseTimeout + DefaultAcquireInterval + time.Second)
			cancel()
		}()
		reg := prometheus.NewRegistry()
		m := NewMetrics(reg)

		require.NoError(t, Run(ctx, Config{Locker: lk, Metrics: m}, rec.session))
		ends := rec.endsCopy()
		require.Len(t, ends, 2)
		require.Equal(t, DefaultReleaseTimeout+DefaultAcquireInterval, ends[1].started)
		// The shutdown release still gets a live context.
		require.Equal(t, []bool{true, true}, releaseCtxAlive)
		require.InDelta(t, 2, testutil.ToFloat64(m.ReleaseErrors), 0)
	})
}

func TestRun_NotHolderOnReleaseIsQuiet(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := bubbleCtx(t)
		lk := &fakeLocker{release: func(context.Context) error { return streaming.ErrNotHolder }}
		reg := prometheus.NewRegistry()
		m := NewMetrics(reg)
		rec := newRecorder(nil, 0)
		go func() {
			time.Sleep(time.Second)
			cancel()
		}()
		require.NoError(t, Run(ctx, Config{Locker: lk, Metrics: m}, rec.session))
		require.InDelta(t, 0, testutil.ToFloat64(m.ReleaseErrors), 0)
	})
}

func TestRun_AcquireRetriesUntilHeld(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := bubbleCtx(t)
		defer cancel()
		lk := &fakeLocker{acquire: func(n int) error {
			switch n {
			case 1, 2:
				return streaming.ErrLockHeld
			case 3:
				return errors.New("dial tcp: connection refused")
			}
			return nil
		}}
		reg := prometheus.NewRegistry()
		m := NewMetrics(reg)
		rec := newRecorder(cancel, 1)
		rec.next = func(int) error { return ErrRestartSession }
		require.NoError(t, Run(ctx, Config{Locker: lk, Metrics: m}, rec.session))
		ends := rec.endsCopy()
		require.Len(t, ends, 1)
		require.Equal(t, 3*DefaultAcquireInterval, ends[0].started)
		require.Equal(t, uint64(1), ends[0].epoch)
		// Only the infrastructure failure counts; a held lock is normal.
		require.InDelta(t, 1, testutil.ToFloat64(m.AcquireErrors), 0)
	})
}

func TestRun_ShutdownWhileWaitingForLock(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := bubbleCtx(t)
		lk := &fakeLocker{acquire: func(int) error { return streaming.ErrLockHeld }}
		go func() {
			time.Sleep(10 * time.Second)
			cancel()
		}()
		require.NoError(t, Run(ctx, Config{Locker: lk}, func(context.Context, uint64) error {
			t.Fatal("session must not start without the lock")
			return nil
		}))
		require.Empty(t, lk.releasedEpochs())
	})
}

func TestRun_LocalHoldsEpochOneAndNeverRenews(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := bubbleCtx(t)
		defer cancel()
		var epochs []uint64
		errc := make(chan error, 1)
		go func() {
			errc <- Run(ctx, Config{Locker: Local{}}, func(ctx context.Context, epoch uint64) error {
				epochs = append(epochs, epoch)
				if len(epochs) == 1 {
					return ErrRestartSession
				}
				<-ctx.Done()
				return ctx.Err()
			})
		}()
		time.Sleep(time.Second)
		// With no renew ticker, the bubble goes idle: nothing but the
		// session's ctx wait is pending.
		synctest.Wait()
		cancel()
		require.NoError(t, <-errc)
		require.Equal(t, []uint64{1, 1}, epochs)
	})
}

func TestRun_RejectsNilInputs(t *testing.T) {
	t.Parallel()
	require.Error(t, Run(t.Context(), Config{}, func(context.Context, uint64) error { return nil }))
	require.Error(t, Run(t.Context(), Config{Locker: Local{}}, nil))
}

// bubbleCtx caps a test at an hour of fake time. A renew ticker keeps a
// bubble's clock running forever, so without the cap a regression that
// never ends a session hangs instead of failing.
func bubbleCtx(t *testing.T) (context.Context, context.CancelFunc) {
	return context.WithTimeout(t.Context(), time.Hour)
}

// TestIsLocal pins the no-renew fast path to both forms of Local: a *Local
// satisfies Locker, and missing it would start a ticker renewing nothing.
func TestIsLocal(t *testing.T) {
	t.Parallel()
	require.True(t, isLocal(Local{}))
	require.True(t, isLocal(&Local{}))
	require.False(t, isLocal(&fakeLocker{}))
}

type sessionFatalErr struct{}

func (sessionFatalErr) Error() string      { return "corrupt" }
func (sessionFatalErr) SessionFatal() bool { return true }

func TestDefaultFatal(t *testing.T) {
	t.Parallel()
	require.True(t, DefaultFatal(errors.New("unclassified")))
	require.False(t, DefaultFatal(fmt.Errorf("fenced: %w", ErrRestartSession)))
	require.True(t, DefaultFatal(sessionFatalErr{}))
	// A restart marker wrapped around corruption does not downgrade it.
	require.True(t, DefaultFatal(fmt.Errorf("%w: %w", ErrRestartSession, sessionFatalErr{})))
}
