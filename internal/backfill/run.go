// Package backfill: run.go is the entrypoint cmd/jetstream calls
// from its errgroup. Run constructs the atmos engine and drives it
// to completion. Returns nil on clean drain (every DID either skipped
// at Complete or downloaded + recorded), the engine's error
// otherwise.
package backfill

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/jcalabro/atmos"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	"github.com/jcalabro/atmos/identity"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
)

// Config carries everything Run needs. The fields are required unless
// noted otherwise.
type Config struct {
	// Store is the shared metadata pebble db.
	Store *store.Store

	// RelayURL is the upstream relay base URL (e.g. https://bsky.network).
	RelayURL string

	// Logger is the structured logger; required (no sensible default
	// for an ingestion service that needs failure-mode visibility).
	Logger *slog.Logger

	// Metrics is optional; nil means we still run, just without
	// /metrics counters incrementing.
	Metrics *Metrics

	// HTTPClient is shared across the relay xrpc client, the identity
	// resolver, and the per-PDS pool inside the engine. nil = a fresh
	// 30s-timeout default client.
	HTTPClient *http.Client
}

// progressLogInterval bounds how chatty the INFO progress log is. We
// can revisit this once we have real production data — at ~30M DIDs
// total, every 1k completions is ~30k log lines for a full backfill,
// which is reasonable.
const progressLogInterval = 1_000

// directoryCacheCapacity is the LRU size for the identity cache. The
// network has ~30M DIDs; caching all of them is wasteful on a
// bootstrap that will visit each at most a few times. 100k covers
// any hot working set with plenty of headroom.
const directoryCacheCapacity = 100_000

// directoryCacheTTL keeps cache entries cheaply reusable for the
// duration of a backfill without growing stale enough to miss key
// rotations during the run.
const directoryCacheTTL = 24 * time.Hour

// Run drives the atmos backfill engine to completion. It blocks until
// the engine drains or ctx is cancelled. Safe to call multiple times
// across process restarts: each call constructs a fresh Engine
// (atmos engines are single-shot) and resumes by skipping rows
// already at StatusComplete via Store.Lookup.
func Run(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return err
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}

	dir := &identity.Directory{
		Resolver: &identity.DefaultResolver{
			HTTPClient: gt.Some(httpClient),
		},
		Cache: identity.NewLRUCache(directoryCacheCapacity, directoryCacheTTL),
	}

	return runWithDirectory(ctx, cfg, httpClient, dir)
}

// runWithDirectory is the production entry point's internal worker.
// Tests inject a stub resolver via the Directory parameter so we can
// avoid spinning up a real PLC.
func runWithDirectory(ctx context.Context, cfg Config, httpClient *http.Client, dir *identity.Directory) error {
	xc := &xrpc.Client{
		Host:       cfg.RelayURL,
		HTTPClient: gt.Some(httpClient),
		Retry:      gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)}),
	}
	sc := atmossync.NewClient(atmossync.Options{
		Client:    xc,
		Directory: gt.Some(dir),
	})

	st := NewStore(cfg.Store, cfg.Metrics)
	handler := NewLogHandler(cfg.Logger)
	logger := cfg.Logger

	engine := atmosbackfill.NewEngine(atmosbackfill.Options{
		SyncClient: sc,
		Store:      st,
		Handler:    handler,
		Directory:  gt.Some(dir),
		HTTPClient: gt.Some(httpClient),
		OnError: gt.Some(func(did atmos.DID, err error) {
			logger.Warn("backfill: repo failed", "did", string(did), "err", err)
		}),
		OnProgress: gt.Some(func(stats atmosbackfill.Stats) {
			if stats.Completed%progressLogInterval == 0 {
				logger.Info("backfill: progress", "completed", stats.Completed)
			}
		}),
	})

	logger.Info("backfill: starting", "relay", cfg.RelayURL)
	err := engine.Run(ctx)
	if err != nil {
		logger.Error("backfill: engine returned error", "err", err)
		return fmt.Errorf("backfill: %w", err)
	}
	logger.Info("backfill: engine drained")
	return nil
}

func (cfg Config) validate() error {
	if cfg.Store == nil {
		return fmt.Errorf("backfill: Config.Store is required")
	}
	if cfg.RelayURL == "" {
		return fmt.Errorf("backfill: Config.RelayURL is required")
	}
	if cfg.Logger == nil {
		return fmt.Errorf("backfill: Config.Logger is required")
	}
	return nil
}
