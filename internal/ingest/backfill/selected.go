package backfill

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"math/bits"
	"math/rand/v2"
	"time"

	"github.com/jcalabro/atmos"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	atmosidentity "github.com/jcalabro/atmos/identity"
	atmosrepo "github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
)

type selectedReposConfig struct {
	Repos            []atmos.DID
	Store            *Store
	Handler          *SegmentHandler
	SyncClient       *atmossync.Client
	IdentityResolver atmosidentity.Resolver
	Metrics          *Metrics
	OnError          func(atmos.DID, error)

	MaxRetries     int
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration

	// jitter sources backoff jitter; nil defaults to rand.Int64N.
	jitter jitterFunc
}

const (
	selectedDefaultMaxRetries        = 3
	selectedDefaultRetryRateLimitMax = atmosbackfill.DefaultRetryRateLimitMaxAttempts
	selectedDefaultRetryBaseDelay    = time.Second
	selectedDefaultRetryMaxDelay     = 30 * time.Second
	selectedRetryRateLimitCeiling    = 330 * time.Second
)

var errSelectedOnCompleteRecorded = errors.New("selected repo backfill: OnComplete recording failed; handler already ran")

func runSelectedRepos(ctx context.Context, cfg selectedReposConfig) error {
	if cfg.jitter == nil {
		cfg.jitter = rand.Int64N
	}
	r := &selectedRunner{cfg: cfg}
	for _, did := range cfg.Repos {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := r.reconcileAndProcess(ctx, did); err != nil {
			return err
		}
	}
	return nil
}

type selectedRunner struct {
	cfg       selectedReposConfig
	completed int64
}

func (r *selectedRunner) reconcileAndProcess(ctx context.Context, did atmos.DID) error {
	entry := atmossync.ListReposEntry{DID: did, Active: true}
	rec, err := r.cfg.Store.Lookup(ctx, did)
	if err != nil {
		return fmt.Errorf("selected repo backfill: store lookup %s: %w", did, err)
	}

	if rec.State == atmosbackfill.StateUnknown {
		if err := r.cfg.Store.OnDiscover(ctx, entry); err != nil {
			return fmt.Errorf("selected repo backfill: store on_discover %s: %w", did, err)
		}
	} else if !rec.Active {
		if err := r.cfg.Store.OnUpdate(ctx, entry); err != nil {
			return fmt.Errorf("selected repo backfill: store on_update %s: %w", did, err)
		}
	}

	if err := r.recordIdentityMetadata(ctx, did); err != nil {
		return err
	}
	if rec.State == atmosbackfill.StateComplete {
		return nil
	}
	r.processRepo(ctx, did)
	return nil
}

func (r *selectedRunner) recordIdentityMetadata(ctx context.Context, did atmos.DID) error {
	doc, err := r.cfg.IdentityResolver.ResolveDID(ctx, did)
	if err != nil {
		r.reportIdentityMetadataError(did, fmt.Errorf("backfill: resolve selected repo identity: %w", err))
		return nil
	}
	ident, err := atmosidentity.IdentityFromDocument(doc)
	if err != nil {
		r.reportIdentityMetadataError(did, fmt.Errorf("backfill: parse selected repo identity: %w", err))
		return nil
	}

	pds := ident.PDSEndpoint()
	host, ok := normalizeHostBucket(pds)
	if !ok {
		host = HostBucketInvalidPDS
	}

	handle := ""
	if ident.Handle != "" && ident.Handle != atmos.HandleInvalid {
		handle = string(ident.Handle)
	}
	if err := r.cfg.Store.recordIdentityResolution(ctx, did, IdentityResolution{
		Handle: handle,
		PDS:    pds,
		Host:   host,
	}); err != nil {
		return fmt.Errorf("selected repo backfill: record identity metadata %s: %w", did, err)
	}
	return nil
}

func (r *selectedRunner) reportIdentityMetadataError(did atmos.DID, err error) {
	if r.cfg.OnError != nil {
		r.cfg.OnError(did, err)
	}
}

// processRepo mirrors the atmos engine's two-budget retry loop (see
// atmos backfill/engine.go): ordinary transient errors draw on
// maxRetries with capped backoff, while a 429 is treated as
// backpressure — it sleeps for the server-directed reset (clamped to
// selectedRetryRateLimitCeiling) and draws on a separate, larger
// rate-limit budget, never failing for "the reset exceeds the cap."
func (r *selectedRunner) processRepo(ctx context.Context, did atmos.DID) {
	maxRetries := selectedDefaultMaxRetries
	if r.cfg.MaxRetries > 0 {
		maxRetries = r.cfg.MaxRetries
	}
	baseDelay := selectedDefaultRetryBaseDelay
	if r.cfg.RetryBaseDelay > 0 {
		baseDelay = r.cfg.RetryBaseDelay
	}
	maxDelay := selectedDefaultRetryMaxDelay
	if r.cfg.RetryMaxDelay > 0 {
		maxDelay = r.cfg.RetryMaxDelay
	}
	rlMaxAttempts := selectedDefaultRetryRateLimitMax

	transientAttempt := 0
	rlAttempt := 0
	attempts := 0

	for {
		host, err := r.tryRepo(ctx, did)
		attempts++
		if err == nil {
			return
		}
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, errSelectedOnCompleteRecorded) {
			return
		}

		var delay time.Duration
		if xrpc.IsRateLimited(err) {
			if rlAttempt >= rlMaxAttempts {
				r.recordFail(ctx, did, host, fmt.Errorf("backfill: still rate limited after %d attempts: %w", rlAttempt+1, err), attempts)
				return
			}
			rlAttempt++
			delay = selectedRateLimitDelay(err, baseDelay, rlAttempt, r.cfg.jitter)
		} else {
			if !xrpc.IsTransient(err) || transientAttempt >= maxRetries {
				r.recordFail(ctx, did, host, err, attempts)
				return
			}
			delay = selectedBackoffDelay(baseDelay, maxDelay, transientAttempt, r.cfg.jitter)
			transientAttempt++
		}

		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

// selectedRateLimitDelay mirrors atmos's rateLimitDelay: honor the
// server-directed reset (clamped), else exponential backoff on baseDelay.
func selectedRateLimitDelay(err error, baseDelay time.Duration, rlAttempt int, jitter jitterFunc) time.Duration {
	if ra := xrpc.RetryAfter(err); !ra.IsZero() {
		if wait := time.Until(ra); wait > 0 {
			if wait > selectedRetryRateLimitCeiling {
				wait = selectedRetryRateLimitCeiling
			}
			return wait
		}
	}
	delay := max(selectedBackoffDelay(baseDelay, selectedRetryRateLimitCeiling, rlAttempt-1, jitter), baseDelay)
	return delay
}

// jitterFunc returns a pseudo-random value in [0, n). Injectable so tests
// and deterministic harnesses can seed backoff jitter; production uses
// rand.Int64N.
type jitterFunc func(n int64) int64

func selectedBackoffDelay(base, maxDelay time.Duration, attempt int, jitter jitterFunc) time.Duration {
	delay := maxDelay
	if base > 0 && attempt < bits.LeadingZeros64(uint64(base)) {
		shifted := base << attempt
		if shifted < maxDelay {
			delay = shifted
		}
	}
	if half := int64(delay) / 2; half > 0 {
		delay += time.Duration(jitter(half))
	}
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}

// tryRepo downloads via the relay SyncClient (302→PDS), parses, and
// hands the repo to the handler. It returns the host the CAR came from
// (post-redirect) so a failure can be attributed even though no identity
// resolution happens on this path. Commit signatures are not verified
// (this debug path mirrors the bootstrap engine's relay-trusted default).
func (r *selectedRunner) tryRepo(ctx context.Context, did atmos.DID) (string, error) {
	body, host, err := r.cfg.SyncClient.GetRepoStreamHost(ctx, did, "")
	if err != nil {
		return host, err
	}
	defer func() { _ = body.Close() }()

	rp, commit, err := atmosrepo.LoadFromCAR(bufio.NewReader(body))
	if err != nil {
		return host, err
	}
	if err := r.cfg.Handler.HandleRepo(ctx, did, rp, commit); err != nil {
		return host, err
	}
	if err := r.cfg.Store.OnComplete(ctx, did, host, commit); err != nil {
		if r.cfg.OnError != nil {
			r.cfg.OnError(did, fmt.Errorf("backfill: store on_complete: %w", err))
		}
		return host, errSelectedOnCompleteRecorded
	}
	r.completed++
	r.cfg.Metrics.setProgressCompleted(r.completed)
	return host, nil
}

func (r *selectedRunner) recordFail(ctx context.Context, did atmos.DID, host string, err error, attempts int) {
	if r.cfg.OnError != nil {
		r.cfg.OnError(did, err)
	}
	if storeErr := r.cfg.Store.OnFail(ctx, did, host, err, attempts); storeErr != nil {
		if r.cfg.OnError != nil {
			r.cfg.OnError(did, fmt.Errorf("backfill: store on_fail: %w", storeErr))
		}
	}
}
