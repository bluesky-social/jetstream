package orchestrator

import (
	"context"
	"fmt"
	"net/http"

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

func (r *mergeRunner) runDiscovery(ctx context.Context, relayURL string, httpClient *http.Client) error {
	return r.runDiscoveryWithClient(ctx, relayURL, httpClient, nil)
}

func (r *mergeRunner) runDiscoveryWithClient(ctx context.Context, relayURL string, httpClient *http.Client, newHostClient func(string) (*atmossync.Client, error)) error {
	return obs.Span(ctx, func(ctx context.Context) error {
		xc := &xrpc.Client{Host: relayURL, HTTPClient: gt.Some(httpClient), Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
		relay := atmossync.NewClient(atmossync.Options{Client: xc})
		if newHostClient == nil {
			newHostClient = jsbackfill.NewHostClientBuilder(relayURL, httpClient)
		}
		base := jsbackfill.NewStore(r.store, nil)
		store := discoveryStore{Store: base.AtmosStore(), base: base, runner: r}
		engine := atmosbackfill.NewEngine(atmosbackfill.Options{
			Relay: relay, NewHostClient: gt.Some(newHostClient), Store: store,
			Handler: atmosbackfill.HandlerFunc(func(context.Context, atmos.DID, *atmosrepo.Repo, *atmosrepo.Commit) error {
				return nil
			}),
			DiscoverOnly: gt.Some(true), HostMaxAttempts: gt.Some(1),
		})
		if err := engine.Run(ctx); err != nil {
			return fmt.Errorf("orchestrator: merge: PDS discovery: %w", err)
		}
		return nil
	})
}
