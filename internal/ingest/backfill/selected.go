package backfill

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"math/bits"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/jcalabro/atmos"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	"github.com/jcalabro/atmos/identity"
	atmosrepo "github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
)

type selectedReposConfig struct {
	Repos      []atmos.DID
	Store      *Store
	Handler    *SegmentHandler
	SyncClient *atmossync.Client
	Directory  *identity.Directory
	HTTPClient *http.Client
	Metrics    *Metrics
	OnError    func(atmos.DID, error)

	MaxRetries     int
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
}

const (
	selectedDefaultMaxRetries     = 5
	selectedDefaultRetryBaseDelay = time.Second
	selectedDefaultRetryMaxDelay  = 30 * time.Second
)

var errSelectedOnCompleteRecorded = errors.New("selected repo backfill: OnComplete recording failed; handler already ran")

func runSelectedRepos(ctx context.Context, cfg selectedReposConfig) error {
	r := &selectedRunner{
		cfg:        cfg,
		pdsClients: make(map[string]*atmossync.Client),
	}
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
	cfg        selectedReposConfig
	pdsClients map[string]*atmossync.Client
	completed  int64
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

	if rec.State == atmosbackfill.StateComplete {
		return nil
	}
	r.processRepo(ctx, did)
	return nil
}

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

	for attempt := range maxRetries + 1 {
		err := r.tryRepo(ctx, did)
		if err == nil {
			return
		}
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, errSelectedOnCompleteRecorded) {
			return
		}

		if !xrpc.IsTransient(err) || attempt >= maxRetries {
			r.recordFail(ctx, did, err, attempt+1)
			return
		}

		delay := selectedBackoffDelay(baseDelay, maxDelay, attempt)
		if ra := xrpc.RetryAfter(err); !ra.IsZero() {
			wait := time.Until(ra)
			if wait > maxDelay {
				r.recordFail(ctx, did, fmt.Errorf("server requested %s delay exceeds RetryMaxDelay %s: %w", wait, maxDelay, err), attempt+1)
				return
			}
			if wait > delay {
				delay = wait
			}
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

func selectedBackoffDelay(base, maxDelay time.Duration, attempt int) time.Duration {
	delay := maxDelay
	if base > 0 && attempt < bits.LeadingZeros64(uint64(base)) {
		shifted := base << attempt
		if shifted < maxDelay {
			delay = shifted
		}
	}
	if half := int64(delay) / 2; half > 0 {
		delay += time.Duration(rand.Int64N(half))
	}
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}

func (r *selectedRunner) tryRepo(ctx context.Context, did atmos.DID) error {
	sc := r.syncClientForRepo(ctx, did)
	body, err := sc.GetRepoStream(ctx, did, "")
	if err != nil {
		return err
	}
	defer func() { _ = body.Close() }()

	rp, commit, err := atmosrepo.LoadFromCAR(bufio.NewReader(body))
	if err != nil {
		return err
	}
	if r.cfg.Directory != nil {
		if err := sc.VerifyCommit(ctx, commit); err != nil {
			return err
		}
	}
	if err := r.cfg.Handler.HandleRepo(ctx, did, rp, commit); err != nil {
		return err
	}
	if err := r.cfg.Store.OnComplete(ctx, did, commit); err != nil {
		if r.cfg.OnError != nil {
			r.cfg.OnError(did, fmt.Errorf("backfill: store on_complete: %w", err))
		}
		return errSelectedOnCompleteRecorded
	}
	r.completed++
	r.cfg.Metrics.setProgressCompleted(r.completed)
	return nil
}

func (r *selectedRunner) recordFail(ctx context.Context, did atmos.DID, err error, attempts int) {
	if r.cfg.OnError != nil {
		r.cfg.OnError(did, err)
	}
	if storeErr := r.cfg.Store.OnFail(ctx, did, err, attempts); storeErr != nil {
		if r.cfg.OnError != nil {
			r.cfg.OnError(did, fmt.Errorf("backfill: store on_fail: %w", storeErr))
		}
	}
}

func (r *selectedRunner) syncClientForRepo(ctx context.Context, did atmos.DID) *atmossync.Client {
	if r.cfg.Directory == nil {
		return r.cfg.SyncClient
	}

	doc, err := r.cfg.Directory.Resolver.ResolveDID(ctx, did)
	if err != nil {
		return r.cfg.SyncClient
	}

	var pds string
	for _, svc := range doc.Service {
		if svc.Type == "AtprotoPersonalDataServer" {
			pds = svc.ServiceEndpoint
			break
		}
	}
	if pds == "" {
		return r.cfg.SyncClient
	}

	if sc, ok := r.pdsClients[pds]; ok {
		return sc
	}
	xc := &xrpc.Client{
		Host:       pds,
		HTTPClient: gt.Some(r.cfg.HTTPClient),
		Retry:      gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)}),
	}
	sc := atmossync.NewClient(atmossync.Options{
		Client:    xc,
		Directory: gt.Some(r.cfg.Directory),
	})
	r.pdsClients[pds] = sc
	return sc
}
