package leader

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jcalabro/atmos/streaming"
)

// Defaults match atmos's streaming lock loop (design §6.2).
const (
	DefaultLease           = 3 * time.Second
	DefaultRenewInterval   = 1 * time.Second
	DefaultAcquireInterval = 500 * time.Millisecond
	DefaultReleaseTimeout  = 5 * time.Second
)

// Locker is the writer lock. It is the atmos DistributedLocker contract plus
// the writer epoch, because Acquire only returns an error. Epoch is valid
// after a successful Acquire and stays fixed until the next one. Run never
// calls Acquire, Renew, and Release concurrently.
type Locker interface {
	streaming.DistributedLocker
	Epoch() uint64
}

// Local is the local-mode locker. A local data directory has exactly one
// writer, so the lock is always held and the epoch is always 1.
type Local struct {
	streaming.NoopLock
}

// Epoch implements Locker.
func (Local) Epoch() uint64 { return 1 }

// ErrRestartSession marks a session error that a fresh session can recover
// from by rebuilding from durable state: lease loss, a fence failure, or a
// write whose commit result is unknown. The default Fatal treats every
// other error as fatal, so an error nobody classified keeps the crash-loud
// rule.
var ErrRestartSession = errors.New("leader: restart session")

// SessionFunc runs one writer session. It blocks until the session ends and
// must not return until every goroutine it started has exited, because the
// next session may start as soon as it returns. ctx is cancelled on lease
// loss or process shutdown.
type SessionFunc func(ctx context.Context, epoch uint64) error

// Config configures Run. Zero durations take the package defaults.
type Config struct {
	Locker Locker

	Lease           time.Duration
	RenewInterval   time.Duration
	AcquireInterval time.Duration
	ReleaseTimeout  time.Duration

	// Fatal reports whether a session error must end the process instead of
	// starting a new session. Nil means every error not wrapping
	// ErrRestartSession is fatal.
	Fatal func(error) bool

	Logger  *slog.Logger
	Metrics *Metrics
}

func (c Config) withDefaults() Config {
	if c.Lease <= 0 {
		c.Lease = DefaultLease
	}
	if c.RenewInterval <= 0 {
		c.RenewInterval = DefaultRenewInterval
	}
	if c.AcquireInterval <= 0 {
		c.AcquireInterval = DefaultAcquireInterval
	}
	if c.ReleaseTimeout <= 0 {
		c.ReleaseTimeout = DefaultReleaseTimeout
	}
	if c.Fatal == nil {
		c.Fatal = func(err error) bool { return !errors.Is(err, ErrRestartSession) }
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return c
}

// Run is the election loop. It returns nil once ctx is cancelled and the
// current session, if any, has ended and released the lock. It returns a
// non-nil error only when a session ends with an error cfg.Fatal classifies
// as fatal; the caller exits the process non-zero.
func Run(ctx context.Context, cfg Config, session SessionFunc) error {
	if cfg.Locker == nil {
		return errors.New("leader: nil Locker")
	}
	if session == nil {
		return errors.New("leader: nil SessionFunc")
	}
	cfg = cfg.withDefaults()
	for {
		acquiredAt, ok := acquire(ctx, cfg)
		if !ok {
			return nil
		}
		if err := runOnce(ctx, cfg, session, acquiredAt); err != nil {
			return err
		}
		if !sleep(ctx, cfg.AcquireInterval) {
			return nil
		}
	}
}

// acquire polls the lock until it is held and returns the start time of the
// winning attempt, a lower bound on when the lease began. It reports false
// when ctx is cancelled first.
func acquire(ctx context.Context, cfg Config) (time.Time, bool) {
	for {
		if ctx.Err() != nil {
			return time.Time{}, false
		}
		start := time.Now()
		err := cfg.Locker.Acquire(ctx, cfg.Lease)
		if err == nil {
			return start, true
		}
		if ctx.Err() != nil {
			return time.Time{}, false
		}
		if !errors.Is(err, streaming.ErrLockHeld) {
			cfg.Metrics.acquireError()
			cfg.Logger.Warn("leader: acquire failed", "err", err)
		}
		if !sleep(ctx, cfg.AcquireInterval) {
			return time.Time{}, false
		}
	}
}

// runOnce runs one session under a held lock and releases the lock
// afterwards. It returns the session error only when it is fatal.
func runOnce(ctx context.Context, cfg Config, session SessionFunc, acquiredAt time.Time) error {
	epoch := cfg.Locker.Epoch()
	cfg.Metrics.sessionStarted(epoch)
	cfg.Logger.Info("leader: session starting", "epoch", epoch)

	sessionCtx, cancel := context.WithCancel(ctx)
	lost := make(chan struct{})
	renewDone := make(chan struct{})
	if _, local := cfg.Locker.(Local); local {
		// Nothing to renew. Skipping the ticker also keeps a synctest bubble
		// free of a timer that fires forever.
		close(renewDone)
	} else {
		go func() {
			defer close(renewDone)
			renew(sessionCtx, cfg, cancel, lost, acquiredAt)
		}()
	}

	err := session(sessionCtx, epoch)
	// A cancellation error is benign only when this loop cancelled the
	// session. One from inside the session, such as a timed-out call, is
	// classified like any other error.
	cancelled := sessionCtx.Err() != nil && isCancellation(err)
	cancel()
	<-renewDone

	// Release even after a lost lease: a renew that failed on a transient
	// error may still hold the row, and Release is fenced by epoch and
	// holder so it cannot free a successor's lock.
	releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.ReleaseTimeout)
	if rerr := cfg.Locker.Release(releaseCtx); rerr != nil && !errors.Is(rerr, streaming.ErrNotHolder) {
		cfg.Metrics.releaseError()
		cfg.Logger.Warn("leader: release failed", "epoch", epoch, "err", rerr)
	}
	releaseCancel()

	leaseLost := false
	select {
	case <-lost:
		leaseLost = true
	default:
	}

	switch {
	case err != nil && !cancelled && cfg.Fatal(err):
		// Checked before lease loss and shutdown: corruption found while the
		// lease was going away is still corruption.
		cfg.Metrics.sessionEnded(reasonFatal)
		cfg.Logger.Error("leader: session ended with a fatal error", "epoch", epoch, "err", err)
		return fmt.Errorf("leader: session epoch %d: %w", epoch, err)
	case leaseLost:
		cfg.Metrics.sessionEnded(reasonLeaseLost)
		cfg.Logger.Warn("leader: lease lost; session ended", "epoch", epoch, "err", err)
	case ctx.Err() != nil:
		cfg.Metrics.sessionEnded(reasonShutdown)
		cfg.Logger.Info("leader: session stopped for shutdown", "epoch", epoch)
	default:
		cfg.Metrics.sessionEnded(reasonRestart)
		cfg.Logger.Warn("leader: session ended; starting a new one", "epoch", epoch, "err", err)
	}
	return nil
}

// renew extends the lease every RenewInterval. It closes lost and cancels
// the session on ErrNotHolder, or once no renew has succeeded for a full
// lease. Each call is bounded by that same deadline, so a hung Renew cannot
// keep a session alive past its lease.
func renew(ctx context.Context, cfg Config, cancel context.CancelFunc, lost chan struct{}, acquiredAt time.Time) {
	// lastOK is always the start of the successful call, never its end: the
	// lock extended the lease no earlier than that, so the local deadline
	// never outlives the lock's.
	lastOK := acquiredAt
	loseLease := func(err error) {
		cfg.Metrics.leaseLost()
		cfg.Logger.Warn("leader: lease lost", "err", err)
		close(lost)
		cancel()
	}
	ticker := time.NewTicker(cfg.RenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		deadline := lastOK.Add(cfg.Lease)
		start := time.Now()
		callCtx, callCancel := context.WithDeadline(ctx, deadline)
		err := cfg.Locker.Renew(callCtx, cfg.Lease)
		callCancel()
		if ctx.Err() != nil {
			return
		}
		switch {
		case err == nil:
			lastOK = start
		case errors.Is(err, streaming.ErrNotHolder):
			loseLease(err)
			return
		default:
			cfg.Metrics.renewError()
			if !time.Now().Before(deadline) {
				loseLease(fmt.Errorf("no successful renew for %s: %w", cfg.Lease, err))
				return
			}
			cfg.Logger.Warn("leader: renew failed; retrying", "err", err)
		}
	}
}

func isCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
