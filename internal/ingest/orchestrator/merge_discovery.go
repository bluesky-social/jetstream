package orchestrator

import (
	"context"
	"fmt"
	"net/http"
	"time"

	jsbackfill "github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/jcalabro/atmos"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	atmosrepo "github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
)

type discoveryStore struct {
	atmosbackfill.Store
	base   *jsbackfill.Store
	runner *mergeRunner
}

func (s discoveryStore) OnDiscover(ctx context.Context, host string, entry atmossync.ListReposEntry) error {
	if err := s.base.OnDiscoverForRetry(ctx, host, entry); err != nil {
		return err
	}
	s.runner.metrics.incMergeDIDsDiscoveredPostBootstrap()
	s.runner.logger.InfoContext(ctx, "discovered post-bootstrap DID", "did", entry.DID, "pds", host)
	return nil
}

func (s discoveryStore) HostCursor(ctx context.Context, hostname string) (string, bool, error) {
	return s.base.HostDiscoveryCursor(ctx, hostname)
}

// OnHostExhausted keeps the base bookkeeping but surfaces the miss loudly:
// an exhausted host at cutover means its never-listed DIDs have no marker
// rows and nothing downstream re-enumerates it (steady state has no sweep).
// Policy: report and proceed — a permanently dead PDS must not wedge the
// merge transition forever (same degraded-completion stance as bootstrap).
func (s discoveryStore) OnHostExhausted(ctx context.Context, hostname string, cause error, attempts int) error {
	s.runner.metrics.incMergeDiscoveryHostsExhausted()
	s.runner.logger.WarnContext(ctx, "merge discovery exhausted a PDS host; its unlisted DIDs are not marked for retry",
		"host", hostname, "attempts", attempts, "err", cause)
	return s.Store.OnHostExhausted(ctx, hostname, cause, attempts)
}

type discoveryLimits struct {
	maxHosts       int
	maxActiveHosts int
	retryDelay     time.Duration
}

func (r *mergeRunner) runDiscovery(ctx context.Context, relayURL string, httpClient *http.Client) error {
	return r.runDiscoveryWithClient(ctx, relayURL, httpClient, nil, discoveryLimits{})
}

func (r *mergeRunner) runDiscoveryWithClient(ctx context.Context, relayURL string, httpClient *http.Client, newHostClient func(string) (*atmossync.Client, error), limits discoveryLimits) error {
	return obs.Span(ctx, func(ctx context.Context) error {
		xc := &xrpc.Client{Host: relayURL, HTTPClient: gt.Some(httpClient), Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
		relay := atmossync.NewClient(atmossync.Options{Client: xc})
		if newHostClient == nil {
			newHostClient = jsbackfill.NewHostClientBuilder(relayURL, httpClient)
		}
		base := jsbackfill.NewStore(r.store, nil)
		store := discoveryStore{Store: base.AtmosStore(), base: base, runner: r}
		opts := atmosbackfill.Options{
			Relay: relay, NewHostClient: gt.Some(newHostClient), Store: store,
			Handler: atmosbackfill.HandlerFunc(func(context.Context, atmos.DID, *atmosrepo.Repo, *atmosrepo.Commit) error {
				return nil
			}),
			// 3 attempts, not 1: cutover discovery is the last enumeration
			// this deployment runs (no steady-state sweep), so a single
			// transient 5xx must not silently drop a host's tail DIDs. The
			// backoff is short — this sweep gates serving, and a host that
			// fails three spaced probes lands in OnHostExhausted anyway.
			DiscoverOnly: gt.Some(true), HostMaxAttempts: gt.Some(3),
			HostBackoffBase: gt.Some(2 * time.Second), HostBackoffMax: gt.Some(10 * time.Second),
		}
		// The operator's roster/concurrency bounds apply to this fresh
		// listHosts crawl exactly as they do to bootstrap.
		if limits.maxHosts > 0 {
			opts.MaxHosts = gt.Some(limits.maxHosts)
		}
		if limits.maxActiveHosts > 0 {
			opts.MaxActiveHosts = gt.Some(limits.maxActiveHosts)
		}
		if limits.retryDelay > 0 {
			opts.HostBackoffBase = gt.Some(limits.retryDelay)
		}
		if err := atmosbackfill.NewEngine(opts).Run(ctx); err != nil {
			return fmt.Errorf("orchestrator: merge: PDS discovery: %w", err)
		}
		return nil
	})
}
