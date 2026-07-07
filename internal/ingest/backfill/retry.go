package backfill

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos"
	atmosrepo "github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"golang.org/x/sync/errgroup"
)

const (
	DefaultFailedRepoRetryInterval    = 4 * time.Hour
	DefaultFailedRepoRetryWorkers     = 16
	DefaultFailedRepoRetryHostWorkers = 4
	DefaultFailedRepoRetryMaxDelay    = 7 * 24 * time.Hour

	failedRepoRetryUnknownHost = "unknown"
)

type RetryConfig struct {
	Store      *store.Store
	Writer     *ingest.Writer
	HTTPClient *http.Client
	RelayURL   string
	Logger     *slog.Logger
	Metrics    *Metrics

	// DropMetrics is the shared ingest validation-drop counter family,
	// forwarded to the SegmentHandler. Optional.
	DropMetrics *ingest.DropMetrics

	// BackfillStore, when non-nil, is the shared *Store the runner uses for
	// all metadata reads/writes instead of constructing its own over Store.
	// Tests use this to inspect the same helper instance they seeded.
	BackfillStore *Store

	Interval    time.Duration
	Workers     int
	HostWorkers int
	MaxDelay    time.Duration

	now            func() time.Time
	jitter         jitterFunc
	eligibleStatus func(Status) bool
}

type retryCandidate struct {
	DID   atmos.DID
	Host  string
	Retry int
}

type retryRunner struct {
	cfg        RetryConfig
	syncClient *atmossync.Client
	handler    *SegmentHandler
	store      *Store

	hostMu     sync.Mutex
	hostLimit  map[string]chan struct{}
	hostParked map[string]time.Time
}

func RunFailedRepoRetry(ctx context.Context, cfg RetryConfig) error {
	r, err := newRetryRunner(cfg)
	if err != nil {
		return err
	}
	if r.cfg.Interval == 0 {
		<-ctx.Done()
		return nil
	}

	timer := time.NewTimer(r.cfg.Interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			if err := r.runPass(ctx); err != nil {
				if errors.Is(err, context.Canceled) && ctx.Err() != nil {
					return nil
				}
				return err
			}
			timer.Reset(r.cfg.Interval)
		}
	}
}

// RunPendingRepoRetryPass performs one immediate retry scan for pending repos.
// Merge uses this for bootstrap-recovery rows that must be materialized above
// the captured live tail before serving ungates.
func RunPendingRepoRetryPass(ctx context.Context, cfg RetryConfig) error {
	cfg.eligibleStatus = func(st Status) bool { return st == StatusPending }
	r, err := newRetryRunner(cfg)
	if err != nil {
		return err
	}
	return r.runPass(ctx)
}

func newRetryRunner(cfg RetryConfig) (*retryRunner, error) {
	if cfg.Store == nil {
		return nil, fmt.Errorf("backfill: retry: Store is required")
	}
	if cfg.Writer == nil {
		return nil, fmt.Errorf("backfill: retry: Writer is required")
	}
	if cfg.HTTPClient == nil {
		return nil, fmt.Errorf("backfill: retry: HTTPClient is required")
	}
	if cfg.RelayURL == "" {
		return nil, fmt.Errorf("backfill: retry: RelayURL is required")
	}
	if cfg.Logger == nil {
		return nil, fmt.Errorf("backfill: retry: Logger is required")
	}
	if cfg.Interval < 0 {
		return nil, fmt.Errorf("backfill: retry: Interval must be >= 0")
	}
	if cfg.Workers <= 0 {
		cfg.Workers = DefaultFailedRepoRetryWorkers
	}
	if cfg.HostWorkers <= 0 {
		cfg.HostWorkers = DefaultFailedRepoRetryHostWorkers
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = DefaultFailedRepoRetryMaxDelay
	}
	if cfg.now == nil {
		cfg.now = time.Now
	}
	if cfg.jitter == nil {
		cfg.jitter = rand.Int64N
	}

	xc := &xrpc.Client{
		Host:       cfg.RelayURL,
		HTTPClient: gt.Some(cfg.HTTPClient),
		Retry:      gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)}),
	}
	st := cfg.BackfillStore
	if st == nil {
		st = NewStore(cfg.Store, cfg.Metrics)
	}
	handler := NewSegmentHandler(cfg.Writer, cfg.Logger, cfg.Metrics)
	handler.SetDropMetrics(cfg.DropMetrics)
	return &retryRunner{
		cfg:        cfg,
		syncClient: atmossync.NewClient(atmossync.Options{Client: xc}),
		handler:    handler,
		store:      st,
		hostLimit:  make(map[string]chan struct{}),
		hostParked: make(map[string]time.Time),
	}, nil
}

func (r *retryRunner) runPass(ctx context.Context) error {
	start := r.cfg.now()
	r.cfg.Metrics.incRetryPasses()
	r.cfg.Logger.InfoContext(ctx, "starting failed repo retry pass",
		"workers", r.cfg.Workers,
		"host_workers", r.cfg.HostWorkers,
	)

	jobs := make(chan retryCandidate, r.cfg.Workers*2)
	g, gctx := errgroup.WithContext(ctx)
	for range r.cfg.Workers {
		g.Go(func() error {
			for cand := range jobs {
				if err := r.processCandidate(gctx, cand); err != nil {
					return err
				}
			}
			return nil
		})
	}

	scanErr := r.scanDue(gctx, r.cfg.now(), func(cand retryCandidate) error {
		r.cfg.Metrics.incRetryCandidates()
		if until, ok := r.hostParkedUntil(cand.Host, r.cfg.now()); ok {
			r.cfg.Metrics.incRetrySkippedHostParked()
			return r.store.DeferRetryAttempt(gctx, cand.DID, until)
		}
		select {
		case <-gctx.Done():
			return gctx.Err()
		case jobs <- cand:
			return nil
		}
	})
	close(jobs)
	if scanErr != nil {
		if waitErr := g.Wait(); waitErr != nil {
			return waitErr
		}
		return scanErr
	}
	if err := g.Wait(); err != nil {
		return err
	}
	r.cfg.Logger.InfoContext(ctx, "failed repo retry pass complete", "duration", r.cfg.now().Sub(start))
	return nil
}

func (r *retryRunner) scanDue(ctx context.Context, now time.Time, yield func(retryCandidate) error) error {
	prefix := []byte(repoKeyPrefix)
	it, err := r.cfg.Store.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: store.PrefixUpperBound(prefix),
	})
	if err != nil {
		return fmt.Errorf("backfill: retry: open repo iter: %w", err)
	}
	defer func() { _ = it.Close() }()

	for it.First(); it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		val, err := it.ValueAndErr()
		if err != nil {
			return fmt.Errorf("backfill: retry: read repo value: %w", err)
		}
		rs, err := decodeRepoStatus(val)
		if err != nil {
			return err
		}
		eligibleStatus := r.cfg.eligibleStatus
		if eligibleStatus == nil {
			eligibleStatus = isRetryEligibleStatus
		}
		if !eligibleStatus(rs.Backfill.Status) || !rs.Active {
			continue
		}
		if !rs.Backfill.NextAttemptAt.IsZero() && rs.Backfill.NextAttemptAt.After(now) {
			continue
		}
		did, err := atmos.ParseDID(strings.TrimPrefix(string(it.Key()), repoKeyPrefix))
		if err != nil {
			return fmt.Errorf("backfill: retry: invalid repo key %q: %w", string(it.Key()), err)
		}
		host := rs.Host
		if host == "" {
			host = failedRepoRetryUnknownHost
		}
		if err := yield(retryCandidate{DID: did, Host: host, Retry: rs.Backfill.RetryCount}); err != nil {
			return err
		}
	}
	if err := it.Error(); err != nil {
		return fmt.Errorf("backfill: retry: iter repo: %w", err)
	}
	return nil
}

func (r *retryRunner) processCandidate(ctx context.Context, cand retryCandidate) error {
	if until, ok := r.hostParkedUntil(cand.Host, r.cfg.now()); ok {
		r.cfg.Metrics.incRetrySkippedHostParked()
		return r.store.DeferRetryAttempt(ctx, cand.DID, until)
	}
	release, err := r.acquireHost(ctx, cand.Host)
	if err != nil {
		return err
	}
	defer release()
	if until, ok := r.hostParkedUntil(cand.Host, r.cfg.now()); ok {
		r.cfg.Metrics.incRetrySkippedHostParked()
		return r.store.DeferRetryAttempt(ctx, cand.DID, until)
	}

	r.cfg.Metrics.incRetryAttempts()
	host, err := r.tryRepo(ctx, cand.DID)
	if err == nil {
		r.cfg.Metrics.incRetrySucceeded()
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if isLocalRetryError(err) {
		return err
	}

	next := r.nextAttemptAt(err, cand.Retry)
	failHost := retryFailureHost(cand.Host, host)
	if xrpc.IsRateLimited(err) {
		r.parkHost(failHost, next)
	}
	if storeErr := r.store.RecordRetryFailure(ctx, cand.DID, failHost, err, next); storeErr != nil {
		return storeErr
	}
	r.cfg.Logger.WarnContext(ctx, "failed repo retry attempt failed",
		"did", string(cand.DID),
		"host", failHost,
		"next_attempt_at", next,
		"err", err,
	)
	return nil
}

func (r *retryRunner) tryRepo(ctx context.Context, did atmos.DID) (string, error) {
	body, host, err := r.syncClient.GetRepoStreamHost(ctx, did, "")
	if err != nil {
		return host, err
	}
	defer func() { _ = body.Close() }()

	// LoadCompleteFromCAR (not LoadFromCAR) verifies the downloaded full repo
	// is structurally complete. A getRepo CAR truncated exactly on a block
	// boundary parses cleanly but omits referenced blocks; LoadCompleteFromCAR
	// surfaces that as a transient (io.ErrUnexpectedEOF) error so this retry
	// pass re-defers the DID rather than completing it on a partial repo.
	rp, commit, err := atmosrepo.LoadCompleteFromCAR(bufio.NewReader(body))
	if err != nil {
		return host, err
	}
	if err := r.handler.HandleRepoResync(ctx, did, rp, commit); err != nil {
		return host, err
	}
	if err := r.cfg.Writer.DrainDurability(ctx); err != nil {
		return host, fmt.Errorf("backfill: retry: drain durable repo rows: %w", err)
	}
	if err := r.store.OnComplete(ctx, did, host, commit); err != nil {
		return host, fmt.Errorf("backfill: retry: complete repo: %w", err)
	}
	return host, nil
}

func (r *retryRunner) acquireHost(ctx context.Context, host string) (func(), error) {
	r.hostMu.Lock()
	ch := r.hostLimit[host]
	if ch == nil {
		ch = make(chan struct{}, r.cfg.HostWorkers)
		r.hostLimit[host] = ch
	}
	r.hostMu.Unlock()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case ch <- struct{}{}:
		return func() { <-ch }, nil
	}
}

func (r *retryRunner) isHostParked(host string, now time.Time) bool {
	_, ok := r.hostParkedUntil(host, now)
	return ok
}

func (r *retryRunner) hostParkedUntil(host string, now time.Time) (time.Time, bool) {
	r.hostMu.Lock()
	defer r.hostMu.Unlock()
	until, ok := r.hostParked[host]
	if !ok {
		return time.Time{}, false
	}
	if now.Before(until) {
		return until, true
	}
	delete(r.hostParked, host)
	return time.Time{}, false
}

func (r *retryRunner) parkHost(host string, until time.Time) {
	r.hostMu.Lock()
	defer r.hostMu.Unlock()
	if old, ok := r.hostParked[host]; ok && old.After(until) {
		return
	}
	r.hostParked[host] = until
}

func (r *retryRunner) nextAttemptAt(err error, retryCount int) time.Time {
	now := r.cfg.now().UTC()
	if xrpc.IsRateLimited(err) {
		if ra := xrpc.RetryAfter(err); !ra.IsZero() && ra.After(now) {
			// Clamp a server-directed reset to MaxDelay. parkHost suppresses
			// every repo on this host until this instant, so a buggy or
			// hostile upstream sending a far-future RateLimit-Reset must not
			// be able to park a host past the configured ceiling. Mirrors the
			// bootstrap path's clamp in selectedRateLimitDelay.
			max := now.Add(r.cfg.MaxDelay)
			if ra.After(max) {
				return max
			}
			return ra.UTC()
		}
	}
	return now.Add(selectedBackoffDelay(r.cfg.Interval, r.cfg.MaxDelay, retryCount, r.cfg.jitter)).UTC()
}

func retryFailureHost(candidateHost, responseHost string) string {
	if host, ok := hostBucketFromAuthority(responseHost); ok {
		return host
	}
	if candidateHost != "" {
		return candidateHost
	}
	return failedRepoRetryUnknownHost
}

// isRetryEligibleStatus reports whether a repo row should be picked up by a
// steady-state retry pass. Only failed rows are eligible: they represent repos
// that were discovered by listRepos but failed their original download. A live
// first-sighting is not enough evidence to issue getRepo; that recovery belongs
// to an explicit #sync from the PDS operator.
func isRetryEligibleStatus(st Status) bool {
	return st == StatusFailed
}

// isRetryFailureRecordableStatus reports whether a retry attempt that was
// already selected may record transient failure/backoff. StatusPending is
// included for the explicit post-merge pending pass; the steady-state scanner
// still excludes pending rows via isRetryEligibleStatus.
func isRetryFailureRecordableStatus(st Status) bool {
	return st == StatusFailed || st == StatusPending
}

func isLocalRetryError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "ingest:") ||
		strings.Contains(msg, "append batch") ||
		strings.Contains(msg, "drain durable repo rows") ||
		strings.Contains(msg, "complete repo")
}
