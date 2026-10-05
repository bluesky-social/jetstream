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
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/jcalabro/atmos"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
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
	// DefaultPendingRepoPassWorkers is the post-merge pending pass's worker
	// count. The pass can carry millions of repos (every restart during
	// bootstrap defers its in-flight ones), and per-host concurrency and
	// rate-limit parks, not this, bound what each PDS sees.
	DefaultPendingRepoPassWorkers = 256

	failedRepoRetryUnknownHost = "unknown"

	// failedRepoRetryFirstPassDelay is how soon after start the steady loop
	// runs its first pass, when that is sooner than Interval: repos that
	// failed during bootstrap should not wait a full interval after merge.
	failedRepoRetryFirstPassDelay = 5 * time.Minute

	// A rate limit with no reset in the future parks its host for
	// hostParkBaseDelay, doubling with each consecutive park up to
	// hostParkMaxDelay. The PDS limiters seen in production clear within
	// seconds; parking for the retry interval (hours) idled whole hosts.
	hostParkBaseDelay = time.Second
	hostParkMaxDelay  = time.Minute

	// pendingPassMaxPark caps a server-directed park in the pending pass,
	// which waits parks out in memory rather than deferring past them.
	pendingPassMaxPark = 10 * time.Minute
	// pendingPassMaxRateLimitAttempts is how many rate-limited attempts a
	// pending-pass candidate gets before it is recorded failed, for the
	// steady loop to retry.
	pendingPassMaxRateLimitAttempts = 8
	// pendingPassMaxHostParks is how many consecutive parks with no success
	// in between a host gets before the pending pass gives up on it and
	// records its remaining repos failed (about 11 minutes of parks at the
	// backoff above).
	pendingPassMaxHostParks = 16
)

type RetryConfig struct {
	Store         metastore.Store
	Writer        *ingest.Writer
	HTTPClient    *http.Client
	RelayURL      string
	Logger        *slog.Logger
	Metrics       *Metrics
	NewHostClient func(string) (*atmossync.Client, error)

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

	// DownloadTimeout bounds one retry attempt's network phase (getRepo
	// + CAR read). Zero → atmos backfill.DefaultDownloadTimeout (5m).
	// Negative disables the bound, leaving only the transport's own
	// guards (gttp's 30m wall-clock backstop). Without this, a giant or
	// slow-serving repo occupies a host-limited retry slot for up to
	// the transport backstop on every pass, forever — the retry runner
	// bypasses atmos's backfill.Engine and so does not inherit its
	// DownloadTimeout (observed with pds1.podping.at during the #299
	// incident).
	DownloadTimeout time.Duration

	now            func() time.Time
	jitter         jitterFunc
	eligibleStatus func(Status) bool
	// parkBase and parkMax bound the backoff of a rate-limit park with no
	// reset; zero means hostParkBaseDelay and hostParkMaxDelay.
	parkBase, parkMax time.Duration
	// pendingPass is RunPendingRepoRetryPass: it selects pending rows
	// whatever their NextAttemptAt, records completions in the writer's
	// block commits, and waits out a host's rate limit with the candidate
	// in memory rather than deferring it to a pass that will never come.
	pendingPass bool
}

type retryCandidate struct {
	DID atmos.DID
	// Host is the concurrency/parking attribution key. PDS is the validated
	// direct-routing hostname; empty PDS deliberately falls back to RelayURL
	// for rows written before discovery-time PDS stamping existed.
	Host  string
	PDS   string
	Retry int
	// rlAttempts counts the pending pass's rate-limited attempts.
	rlAttempts int
}

type retryRunner struct {
	cfg        RetryConfig
	syncClient *atmossync.Client
	handler    *SegmentHandler
	store      *Store

	hostMu     sync.Mutex
	hostLimit  map[string]chan struct{}
	hostParked map[string]time.Time
	// hostParks counts each host's consecutive rate-limit parks, reset by
	// a success; hostLastErr is its latest rate-limit error, and
	// hostAbandoned the hosts the pending pass gave up on.
	hostParks     map[string]int
	hostLastErr   map[string]error
	hostAbandoned map[string]bool
	clientMu      sync.Mutex
	hostClients   map[string]*atmossync.Client

	// completions records the pending pass's completions in the writer's
	// block commits; nil in the steady loop.
	completions *completionBatcher
	// writes batches failure and deferral writes while a pass runs; nil
	// outside one, when each is written directly.
	writes *retryWrites
}

func RunFailedRepoRetry(ctx context.Context, cfg RetryConfig) error {
	// Retries and sync 1.1 resyncs append whole repos: in hot mode they are
	// bulk and yield to the firehose (design §10.5).
	ctx = ingest.WithClass(ctx, ingest.ClassBulk)
	r, err := newRetryRunner(cfg)
	if err != nil {
		return err
	}
	if err := r.store.SeedCounts(ctx); err != nil {
		return err
	}
	if r.cfg.Interval == 0 {
		<-ctx.Done()
		return nil
	}

	timer := time.NewTimer(min(r.cfg.Interval, failedRepoRetryFirstPassDelay))
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
//
// Like bootstrap (Run), it records each completion in the block commit that
// makes the repo's last row durable, so it needs no durability barrier or
// metadata transaction per repo. It returns once every pending row has
// completed or been recorded failed for the steady loop, and the writer has
// made every appended row and completion durable. It installs a durable
// batch hook on cfg.Writer.
func RunPendingRepoRetryPass(ctx context.Context, cfg RetryConfig) error {
	cfg.eligibleStatus = func(st Status) bool { return st == StatusPending }
	cfg.pendingPass = true
	r, err := newRetryRunner(cfg)
	if err != nil {
		return err
	}
	if err := r.store.SeedCounts(ctx); err != nil {
		return err
	}
	r.store.SetWriterPipelines(cfg.Writer.PipelinesDurableBatches())
	completions := NewCompletionBatcher(r.store, cfg.Metrics)
	r.store.SetCompletionBatcher(completions)
	cfg.Writer.SetDurableBatchHook(completions.StageDurable)
	r.handler.SetCompletionBatcher(completions)
	r.completions = completions

	// Completions of repos whose last block is still open wait for the
	// next block commit; the drainer cuts one when the pass goes quiet.
	passCtx, cancelPass := context.WithCancel(ctx)
	defer cancelPass()
	var drainErr error
	drainerDone := make(chan struct{})
	go func() {
		defer close(drainerDone)
		err := runPeriodicDurabilityDrain(passCtx, defaultDurabilityDrainInterval, completions.hasPendingDurability, func(ctx context.Context) error {
			if err := cfg.Writer.DrainDurability(ctx); err != nil {
				return err
			}
			cfg.Metrics.incForcedCheckpointFlushes()
			return nil
		})
		if err != nil && passCtx.Err() == nil {
			drainErr = fmt.Errorf("backfill: pending pass: periodic durability drain: %w", err)
			cancelPass()
		}
	}()
	err = r.runPass(passCtx)
	cancelPass()
	<-drainerDone
	if drainErr != nil {
		return drainErr
	}
	if err != nil {
		return err
	}
	if err := cfg.Writer.DrainDurability(ctx); err != nil {
		return fmt.Errorf("backfill: pending pass: drain durability: %w", err)
	}
	if completions.hasPendingDurability() {
		return errors.New("backfill: pending pass: completions still queued after the final durability drain")
	}
	return nil
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
	if cfg.DownloadTimeout == 0 {
		cfg.DownloadTimeout = atmosbackfill.DefaultDownloadTimeout
	}
	if cfg.now == nil {
		cfg.now = time.Now
	}
	if cfg.jitter == nil {
		cfg.jitter = rand.Int64N
	}
	if cfg.parkBase <= 0 {
		cfg.parkBase = hostParkBaseDelay
	}
	if cfg.parkMax <= 0 {
		cfg.parkMax = hostParkMaxDelay
	}
	if cfg.NewHostClient == nil {
		cfg.NewHostClient = NewHostClientBuilder(cfg.RelayURL, cfg.HTTPClient)
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
		cfg:           cfg,
		syncClient:    atmossync.NewClient(atmossync.Options{Client: xc}),
		handler:       handler,
		store:         st,
		hostLimit:     make(map[string]chan struct{}),
		hostParked:    make(map[string]time.Time),
		hostParks:     make(map[string]int),
		hostLastErr:   make(map[string]error),
		hostAbandoned: make(map[string]bool),
		hostClients:   make(map[string]*atmossync.Client),
	}, nil
}

// runPass collects every due candidate, then works through them from
// per-host queues (retryScheduler). Collecting first keeps the scan from
// stalling behind a parked host, and failure and deferral writes are
// batched (retryWrites).
func (r *retryRunner) runPass(ctx context.Context) error {
	start := r.cfg.now()
	r.cfg.Metrics.incRetryPasses()
	r.cfg.Logger.InfoContext(ctx, "starting failed repo retry pass",
		"workers", r.cfg.Workers,
		"host_workers", r.cfg.HostWorkers,
		"pending_pass", r.cfg.pendingPass,
	)

	var cands []retryCandidate
	interned := make(map[string]string)
	intern := func(s string) string {
		if v, ok := interned[s]; ok {
			return v
		}
		interned[s] = s
		return s
	}
	if err := r.scanDue(ctx, r.cfg.now(), func(cand retryCandidate) error {
		r.cfg.Metrics.incRetryCandidates()
		cand.Host, cand.PDS = intern(cand.Host), intern(cand.PDS)
		cands = append(cands, cand)
		return nil
	}); err != nil {
		return err
	}
	r.cfg.Logger.InfoContext(ctx, "failed repo retry pass collected candidates",
		"candidates", len(cands),
		"hosts", len(interned),
	)

	r.writes = &retryWrites{store: r.store}
	defer func() { r.writes = nil }()
	sched := newRetryScheduler(cands, r.cfg.HostWorkers, r.cfg.pendingPass, r.hostParkedUntil, r.cfg.now, r.cfg.Metrics)
	workers := min(r.cfg.Workers, len(cands))
	cands = nil // the scheduler's host queues hold them now
	g, gctx := errgroup.WithContext(ctx)
	for range workers {
		g.Go(func() error {
			for {
				cand, ok, err := sched.next(gctx)
				if err != nil || !ok {
					return err
				}
				cand, requeue, err := r.attemptCandidate(gctx, cand)
				if err != nil {
					return err
				}
				sched.done(cand, requeue)
				if err := r.writes.flush(gctx, true); err != nil {
					return err
				}
			}
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	if err := r.writes.flush(ctx, false); err != nil {
		return err
	}
	r.cfg.Logger.InfoContext(ctx, "failed repo retry pass complete", "duration", r.cfg.now().Sub(start))
	return nil
}

func (r *retryRunner) scanDue(ctx context.Context, now time.Time, yield func(retryCandidate) error) error {
	prefix := []byte(repoKeyPrefix)
	it, err := r.cfg.Store.NewIter(ctx, prefix, metastore.PrefixUpperBound(prefix))
	if err != nil {
		return fmt.Errorf("backfill: retry: open repo iter: %w", err)
	}
	defer func() { _ = it.Close() }()

	for it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		val := it.Value()
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
		// The pending pass is the only pass pending rows get, so a deferral
		// an earlier pass left on one must not hide it.
		if !r.cfg.pendingPass && !rs.Backfill.NextAttemptAt.IsZero() && rs.Backfill.NextAttemptAt.After(now) {
			continue
		}
		did, err := atmos.ParseDID(strings.TrimPrefix(string(it.Key()), repoKeyPrefix))
		if err != nil {
			return fmt.Errorf("backfill: retry: invalid repo key %q: %w", string(it.Key()), err)
		}
		// Routing uses PDS; parking/concurrency attribution prefers rs.Host,
		// which a fallback failure updates to the actually-responding host
		// (for direct rows they are the same bucket, so this is a no-op).
		pds := rs.PDS
		host := rs.Host
		if host == "" {
			host = pds
		}
		if host == "" {
			host = failedRepoRetryUnknownHost
		}
		if err := yield(retryCandidate{DID: did, Host: host, PDS: pds, Retry: rs.Backfill.RetryCount}); err != nil {
			return err
		}
	}
	if err := it.Err(); err != nil {
		return fmt.Errorf("backfill: retry: iter repo: %w", err)
	}
	return nil
}

// processCandidate attempts one candidate outside a pass's scheduler.
func (r *retryRunner) processCandidate(ctx context.Context, cand retryCandidate) error {
	_, _, err := r.attemptCandidate(ctx, cand)
	return err
}

// attemptCandidate tries one candidate and records the outcome. requeue
// reports a pending-pass candidate that a rate limit sent back to wait for
// its host; the returned candidate carries its updated attempt count.
func (r *retryRunner) attemptCandidate(ctx context.Context, cand retryCandidate) (retryCandidate, bool, error) {
	if r.cfg.pendingPass {
		if lastErr, ok := r.abandoned(cand.Host); ok {
			return cand, false, r.recordFailure(ctx, cand, cand.Host, lastErr)
		}
	}
	if until, ok := r.hostParkedUntil(cand.Host, r.cfg.now()); ok {
		return r.skipParked(ctx, cand, until)
	}
	release, err := r.acquireHost(ctx, cand.Host)
	if err != nil {
		return cand, false, err
	}
	defer release()
	if until, ok := r.hostParkedUntil(cand.Host, r.cfg.now()); ok {
		return r.skipParked(ctx, cand, until)
	}
	if until := r.clientParkedUntil(cand.PDS); !until.IsZero() {
		// The PDS client has getRepo parked on a quota a response reported
		// spent. The download would wait that out holding this worker and
		// the host slot for up to a rate-limit window; park the host and
		// skip instead, as after a 429.
		r.parkHost(cand.Host, until)
		return r.skipParked(ctx, cand, until)
	}

	r.cfg.Metrics.incRetryAttempts()
	host, viaFallback, err := r.tryRepo(ctx, cand)
	if err == nil {
		r.cfg.Metrics.incRetrySucceeded()
		r.hostSucceeded(cand.Host)
		return cand, false, nil
	}
	if ctx.Err() != nil {
		return cand, false, ctx.Err()
	}
	if isLocalRetryError(err) {
		return cand, false, err
	}

	// Direct attempts park/record under the roster hostname — that is the
	// key candidates are scanned and parked by, so parking anything else
	// would not suppress same-host work. A failure after a relay fallback,
	// though, came from a different host than the (stale) stamp: attribute
	// it to the responding host so the genuinely rate-limited PDS parks
	// instead of the stamp.
	failHost := cand.PDS
	if failHost == "" || viaFallback {
		failHost = retryFailureHost(cand.Host, host)
	}
	if xrpc.IsRateLimited(err) {
		until := r.parkRateLimited(failHost, err)
		// cand.Host is the key candidates were scanned and gated under; when
		// it differs from the failure bucket (stale stamp vs responding host,
		// in either direction), park it too so queued same-set candidates
		// skip instead of continuing into the rate-limited upstream.
		if cand.Host != "" && cand.Host != failHost {
			r.parkRateLimitedUntil(cand.Host, err, until)
		}
		if r.cfg.pendingPass {
			cand.rlAttempts++
			if cand.rlAttempts < pendingPassMaxRateLimitAttempts {
				return cand, true, nil
			}
		}
	}
	return cand, false, r.recordFailure(ctx, cand, failHost, err)
}

// skipParked handles a candidate whose host is parked: the pending pass
// requeues it to wait the park out, and the steady loop defers its row past
// the park.
func (r *retryRunner) skipParked(ctx context.Context, cand retryCandidate, until time.Time) (retryCandidate, bool, error) {
	r.cfg.Metrics.incRetrySkippedHostParked()
	if r.cfg.pendingPass {
		return cand, true, nil
	}
	d := retryDeferral{did: cand.DID, next: until}
	if r.writes != nil {
		r.writes.deferAttempt(d)
		return cand, false, nil
	}
	return cand, false, r.store.deferRetryAttempts(ctx, []retryDeferral{d})
}

// recordFailure records a failed attempt, due again after the row's backoff.
func (r *retryRunner) recordFailure(ctx context.Context, cand retryCandidate, failHost string, err error) error {
	next := r.nextAttemptAt(err, cand.Retry)
	r.cfg.Logger.WarnContext(ctx, "failed repo retry attempt failed",
		"did", string(cand.DID),
		"host", failHost,
		"next_attempt_at", next,
		"err", err,
	)
	f := retryFailure{did: cand.DID, host: failHost, err: err, next: next}
	if r.writes != nil {
		r.writes.fail(f)
		return nil
	}
	return r.store.recordRetryFailures(ctx, []retryFailure{f})
}

func (r *retryRunner) tryRepo(ctx context.Context, cand retryCandidate) (string, bool, error) {
	rp, commit, host, viaFallback, err := r.download(ctx, cand)
	if err != nil {
		return host, viaFallback, err
	}
	if err := r.handler.HandleRepoResync(ctx, cand.DID, rp, commit); err != nil {
		return host, viaFallback, err
	}
	completionHost := cand.PDS
	if completionHost == "" || viaFallback {
		// Fallback success means the recorded PDS was stale (migration):
		// attribute completion to the host that actually served the CAR.
		completionHost = host
	}
	// Repair the routing stamp so future passes go direct to the host that
	// actually serves this repo instead of re-walking the fallback.
	restamp := ""
	if viaFallback && completionHost != "" && completionHost != cand.PDS {
		if bucket, ok := hostBucketFromAuthority(completionHost); ok {
			restamp = bucket
		}
	}
	if r.completions != nil {
		// The completion commits with the block holding the repo's last
		// row. "complete repo" keeps a failure here inside
		// isLocalRetryError.
		if err := r.completions.queueCompleteRestamp(ctx, cand.DID, completionHost, restamp, commit); err != nil {
			return host, viaFallback, fmt.Errorf("backfill: retry: complete repo: %w", err)
		}
		return host, viaFallback, nil
	}
	if err := r.cfg.Writer.DrainDurability(ctx); err != nil {
		return host, viaFallback, fmt.Errorf("backfill: retry: drain durable repo rows: %w", err)
	}
	if restamp != "" {
		// "complete repo" keeps this inside isLocalRetryError: a local
		// metadata-write failure after durable ingestion must abort the
		// pass, not be recorded as an upstream failure and re-ingested.
		if err := r.store.updateRepoHostActive(cand.DID, restamp, true); err != nil {
			return host, viaFallback, fmt.Errorf("backfill: retry: complete repo: restamp PDS: %w", err)
		}
	}
	if err := r.store.OnComplete(ctx, cand.DID, completionHost, commit); err != nil {
		return host, viaFallback, fmt.Errorf("backfill: retry: complete repo: %w", err)
	}
	return host, viaFallback, nil
}

// download fetches and parses one repo under the per-attempt
// DownloadTimeout. Only the network phase (getRepo + CAR read) runs
// under the deadline; the caller's handler/durability work runs under
// the parent ctx — it must not be killed by a network budget.
//
// A timeout that is ours (parent ctx still healthy) surfaces as the
// deadline error wrapped in a distinguishing message; processCandidate
// records it as an ordinary retry failure with backoff, same as any
// transport error. Mirrors atmos backfill.Engine.download, which the
// retry runner bypasses.
func (r *retryRunner) download(ctx context.Context, cand retryCandidate) (*atmosrepo.Repo, *atmosrepo.Commit, string, bool, error) {
	dlCtx := ctx
	if r.cfg.DownloadTimeout > 0 {
		var cancel context.CancelFunc
		dlCtx, cancel = context.WithTimeout(ctx, r.cfg.DownloadTimeout)
		defer cancel()
	}

	viaFallback := cand.PDS == ""
	client, err := r.clientForHost(cand.PDS)
	if err != nil {
		// An unroutable stamp (validation failure, builder error) must not
		// permanently strand the DID: fall back to the relay's 302, which
		// tracks the current PDS.
		viaFallback = true
		client = r.syncClient
	}
	body, host, err := client.GetRepoStreamHost(dlCtx, cand.DID, "")
	if err != nil && !viaFallback && isRepoNotFoundError(err) {
		// The stamped PDS authoritatively lacks the repo — a stale stamp
		// from a pre-migration discovery. The relay redirect tracks the
		// account's current PDS; on success tryRepo re-stamps. Without
		// this, RecordRetryFailure would treat the direct RepoNotFound as
		// terminal and mark an undownloaded migrated repo complete.
		viaFallback = true
		body, host, err = r.syncClient.GetRepoStreamHost(dlCtx, cand.DID, "")
	}
	if err != nil {
		return nil, nil, host, viaFallback, r.classifyDownloadErr(dlCtx, ctx, err)
	}
	defer func() { _ = body.Close() }()

	// LoadCompleteFromCAR (not LoadFromCAR) verifies the downloaded full repo
	// is structurally complete. A getRepo CAR truncated exactly on a block
	// boundary parses cleanly but omits referenced blocks; LoadCompleteFromCAR
	// surfaces that as a transient (io.ErrUnexpectedEOF) error so this retry
	// pass re-defers the DID rather than completing it on a partial repo.
	rp, commit, err := atmosrepo.LoadCompleteFromCAR(bufio.NewReader(body))
	if err != nil {
		return nil, nil, host, viaFallback, r.classifyDownloadErr(dlCtx, ctx, err)
	}
	// The retry path routes directly to untrusted PDSes and bypasses the
	// atmos engine (which performs this same check): a CAR whose commit
	// identifies a different DID must not be resynced under cand.DID.
	if rp.DID != cand.DID {
		return nil, nil, host, viaFallback, fmt.Errorf("backfill: retry: getRepo DID mismatch: requested %s, CAR commit is %s", cand.DID, rp.DID)
	}
	return rp, commit, host, viaFallback, nil
}

func (r *retryRunner) clientForHost(host string) (*atmossync.Client, error) {
	if host == "" || host == failedRepoRetryUnknownHost {
		return r.syncClient, nil
	}
	r.clientMu.Lock()
	defer r.clientMu.Unlock()
	if client := r.hostClients[host]; client != nil {
		return client, nil
	}
	client, err := r.cfg.NewHostClient(host)
	if err != nil {
		return nil, fmt.Errorf("backfill: retry: build PDS client %s: %w", host, err)
	}
	if client == nil {
		return nil, fmt.Errorf("backfill: retry: build PDS client %s: returned nil client", host)
	}
	r.hostClients[host] = client
	return client, nil
}

// clientParkedUntil reports when the client for pds next sends getRepo, or
// the zero time if it is not parked or has no usable client.
func (r *retryRunner) clientParkedUntil(pds string) time.Time {
	client, err := r.clientForHost(pds)
	if err != nil {
		return time.Time{}
	}
	return client.GetRepoRateLimitedUntil()
}

// classifyDownloadErr annotates a download failure caused by OUR
// per-attempt budget (dlCtx expired, parent ctx healthy) so the log
// line and stored LastError identify the slow download rather than a
// generic "context deadline exceeded". Other errors pass through.
func (r *retryRunner) classifyDownloadErr(dlCtx, parent context.Context, err error) error {
	if dlCtx.Err() != nil && parent.Err() == nil {
		return fmt.Errorf("backfill: retry: repo download exceeded DownloadTimeout (%s): %w",
			r.cfg.DownloadTimeout, err)
	}
	return err
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
	r.parkHostLocked(host, until)
}

func (r *retryRunner) parkHostLocked(host string, until time.Time) {
	if old, ok := r.hostParked[host]; ok && old.After(until) {
		return
	}
	r.hostParked[host] = until
}

// parkRateLimited parks host after a rate-limited attempt and returns when
// the park ends: at the server's reset when it gave one in the future
// (clamped), otherwise after a short backoff that doubles with each
// consecutive park. Workers already in flight when the park began don't
// lengthen it.
func (r *retryRunner) parkRateLimited(host string, err error) time.Time {
	r.hostMu.Lock()
	defer r.hostMu.Unlock()
	now := r.cfg.now().UTC()
	if until, ok := r.hostParked[host]; ok && now.Before(until) {
		r.hostLastErr[host] = err
		return until
	}
	r.hostParks[host]++
	until := r.rateLimitParkEnd(err, r.hostParks[host], now)
	r.hostLastErr[host] = err
	r.parkHostLocked(host, until)
	return until
}

// parkRateLimitedUntil parks host until the instant another host's
// rate-limit park chose, recording err as its latest rate limit.
func (r *retryRunner) parkRateLimitedUntil(host string, err error, until time.Time) {
	r.hostMu.Lock()
	defer r.hostMu.Unlock()
	if parked, ok := r.hostParked[host]; !ok || !r.cfg.now().Before(parked) {
		r.hostParks[host]++
	}
	r.hostLastErr[host] = err
	r.parkHostLocked(host, until)
}

func (r *retryRunner) rateLimitParkEnd(err error, parks int, now time.Time) time.Time {
	maxPark := r.cfg.MaxDelay
	if r.cfg.pendingPass {
		maxPark = pendingPassMaxPark
	}
	if reset := xrpc.RetryAfter(err); !reset.IsZero() && reset.After(now) {
		if limit := now.Add(maxPark); reset.After(limit) {
			return limit
		}
		return reset.UTC()
	}
	delay := r.cfg.parkMax
	if shift := parks - 1; shift < 16 {
		delay = min(r.cfg.parkBase<<shift, r.cfg.parkMax)
	}
	if half := int64(delay) / 2; half > 0 {
		delay += time.Duration(r.cfg.jitter(half))
	}
	return now.Add(min(delay, maxPark))
}

// hostSucceeded resets host's consecutive-park count.
func (r *retryRunner) hostSucceeded(host string) {
	r.hostMu.Lock()
	defer r.hostMu.Unlock()
	delete(r.hostParks, host)
}

// abandoned reports whether the pending pass has given up on host after
// pendingPassMaxHostParks consecutive parks, and the rate limit that ended
// it. It counts each host once.
func (r *retryRunner) abandoned(host string) (error, bool) {
	r.hostMu.Lock()
	defer r.hostMu.Unlock()
	if r.hostParks[host] < pendingPassMaxHostParks {
		return nil, false
	}
	if !r.hostAbandoned[host] {
		r.hostAbandoned[host] = true
		r.cfg.Metrics.incRetryHostsAbandoned()
		r.cfg.Logger.Warn("pending repo pass abandoned a rate-limited host; its remaining repos are recorded failed for the steady-state retry",
			"host", host,
			"consecutive_parks", r.hostParks[host],
			"err", r.hostLastErr[host],
		)
	}
	return r.hostLastErr[host], true
}

func (r *retryRunner) nextAttemptAt(err error, retryCount int) time.Time {
	now := r.cfg.now().UTC()
	if xrpc.IsRateLimited(err) {
		if ra := xrpc.RetryAfter(err); !ra.IsZero() && ra.After(now) {
			// Clamp a server-directed reset to MaxDelay: a buggy or hostile
			// upstream sending a far-future RateLimit-Reset must not push
			// the row past the configured ceiling. Mirrors the bootstrap
			// path's clamp in selectedRateLimitDelay.
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
