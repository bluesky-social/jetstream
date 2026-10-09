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
	DefaultStandbyBackoff  = 10 * time.Second
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

// isLocal accepts both forms because *Local satisfies Locker too, and missing
// it would renew a lease that does not exist.
func isLocal(l Locker) bool {
	switch l.(type) {
	case Local, *Local:
		return true
	}
	return false
}

// ErrRestartSession marks a session error that a fresh session can recover
// from by rebuilding from durable state: lease loss, a fence failure, or a
// write whose commit result is unknown. The default Fatal treats every
// other error as fatal, so an error nobody classified keeps the crash-loud
// rule.
var ErrRestartSession = errors.New("leader: restart session")

// ErrStandby is what a session returns when it finds, after taking the
// lease, that this process must not write yet: the catalog is still being
// migrated from a local archive (specs/notes/2026-10-09-local-to-disagg-
// migration.md §6.2). Run releases the lease at once and waits
// StandbyBackoff before trying again, so the writer that owns the catalog
// gets it back.
var ErrStandby = errors.New("leader: standing by")

// DefaultFatal is the default Config.Fatal. An error is fatal unless it
// wraps ErrRestartSession or ErrStandby, and always fatal when it wraps an error whose
// SessionFatal method reports true. The second rule keeps storage
// corruption fatal even if some layer also wrapped it with a restart
// marker.
func DefaultFatal(err error) bool {
	var f interface{ SessionFatal() bool }
	if errors.As(err, &f) && f.SessionFatal() {
		return true
	}
	return !errors.Is(err, ErrRestartSession) && !errors.Is(err, ErrStandby)
}

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
	// StandbyBackoff is the wait after a session returns ErrStandby.
	StandbyBackoff time.Duration

	// MayAcquire, when set, is asked before every acquire attempt. While it
	// reports false the loop does not try to take the lease, only waits
	// AcquireInterval and asks again. It must not block for long.
	MayAcquire func(ctx context.Context) bool

	// Fatal reports whether a session error must end the process instead of
	// starting a new session. Nil means DefaultFatal.
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
	if c.StandbyBackoff <= 0 {
		c.StandbyBackoff = DefaultStandbyBackoff
	}
	if c.Fatal == nil {
		c.Fatal = DefaultFatal
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
		standby, err := runOnce(ctx, cfg, session, acquiredAt)
		if err != nil {
			return err
		}
		wait := cfg.AcquireInterval
		if standby {
			wait = cfg.StandbyBackoff
		}
		if !sleep(ctx, wait) {
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
		if cfg.MayAcquire != nil && !cfg.MayAcquire(ctx) {
			cfg.Metrics.acquireSkipped()
			if !sleep(ctx, cfg.AcquireInterval) {
				return time.Time{}, false
			}
			continue
		}
		start := time.Now()
		err := cfg.Locker.Acquire(ctx, cfg.Lease)
		if err == nil {
			if time.Since(start) < cfg.Lease/2 {
				return start, true
			}
			if confirmed, ok := confirm(ctx, cfg); ok {
				return confirmed, true
			}
			if !sleep(ctx, cfg.AcquireInterval) {
				return time.Time{}, false
			}
			continue
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

// confirm renews a lease whose Acquire took at least half of it. A stalled
// store can commit an Acquire long after the call started, and the local
// lease clock starts at the call, so the first scheduled renew would find
// its deadline already gone and end a session that just began. Renewing now
// restarts the clock from a known point. This call is not bounded by the
// local deadline, because no session is writing yet and the lock checks
// expiry itself. It returns the renew's start time, or false after
// releasing the lock when the renew failed.
func confirm(ctx context.Context, cfg Config) (time.Time, bool) {
	cfg.Metrics.slowAcquire()
	start := time.Now()
	callCtx, callCancel := context.WithTimeout(ctx, cfg.Lease)
	err := cfg.Locker.Renew(callCtx, cfg.Lease)
	callCancel()
	if err == nil {
		return start, true
	}
	if ctx.Err() != nil {
		return time.Time{}, false
	}
	cfg.Logger.Warn("leader: lease acquired too slowly to keep", "epoch", cfg.Locker.Epoch(), "err", err)
	if errors.Is(err, streaming.ErrNotHolder) {
		return time.Time{}, false
	}
	cfg.Metrics.renewError()
	// The renew may have failed after the store kept the lease. Release is
	// fenced by epoch and holder, so it cannot free a successor's lock.
	releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.ReleaseTimeout)
	if rerr := cfg.Locker.Release(releaseCtx); rerr != nil && !errors.Is(rerr, streaming.ErrNotHolder) {
		cfg.Metrics.releaseError()
		cfg.Logger.Warn("leader: release failed", "epoch", cfg.Locker.Epoch(), "err", rerr)
	}
	releaseCancel()
	return time.Time{}, false
}

// runOnce runs one session under a held lock and releases the lock
// afterwards. It returns the session error only when it is fatal, and
// standby when the session ended with ErrStandby.
func runOnce(ctx context.Context, cfg Config, session SessionFunc, acquiredAt time.Time) (standby bool, _ error) {
	epoch := cfg.Locker.Epoch()
	cfg.Metrics.sessionStarted(epoch)
	cfg.Logger.Info("leader: session starting", "epoch", epoch)

	sessionCtx, cancel := context.WithCancel(ctx)
	lost := make(chan struct{})
	renewDone := make(chan struct{})
	if isLocal(cfg.Locker) {
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
		return false, fmt.Errorf("leader: session epoch %d: %w", epoch, err)
	case errors.Is(err, ErrStandby):
		cfg.Metrics.sessionEnded(reasonStandby)
		cfg.Logger.Info("leader: standing by; lease released", "epoch", epoch, "err", err, "retry_in", cfg.StandbyBackoff)
		return true, nil
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
	return false, nil
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
