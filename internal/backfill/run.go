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

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
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

	// Writer is the active-segment writer used by SegmentHandler. Required.
	Writer *ingest.Writer

	// The HTTP client to use while fetching repos, talking to the relay, etc.
	// Should be tuned for bulk repo downloads.
	HTTPClient *http.Client

	// The identity directory to use while doing backfill.
	Directory *identity.Directory

	// RelayURL is the upstream relay base URL (e.g. https://bsky.network).
	RelayURL string

	// Logger is the structured logger; required (no sensible default
	// for an ingestion service that needs failure-mode visibility).
	Logger *slog.Logger

	// Metrics is optional; nil means we still run, just without
	// /metrics counters incrementing.
	Metrics *Metrics
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

	// Per atmos Options.SyncClient docs: disable xrpc retries because
	// the engine's retry/backoff loop is the only retry source we
	// want. Otherwise xrpc and the engine compound retries on
	// transient 503s, multiplying load against PDSes.
	xc := &xrpc.Client{
		Host:       cfg.RelayURL,
		HTTPClient: gt.Some(cfg.HTTPClient),
		Retry:      gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)}),
	}

	sc := atmossync.NewClient(atmossync.Options{
		Client:    xc,
		Directory: gt.Some(cfg.Directory),
	})

	st := NewStore(cfg.Store, cfg.Metrics)
	handler := NewSegmentHandler(cfg.Writer, cfg.Logger)
	logger := cfg.Logger

	startCursor, err := LoadListReposCursor(cfg.Store)
	if err != nil {
		return fmt.Errorf("backfill: %w", err)
	}
	if startCursor != "" {
		logger.Info("backfill: resuming from saved cursor", "cursor", startCursor)
	}

	engine := atmosbackfill.NewEngine(atmosbackfill.Options{
		SyncClient:  sc,
		Store:       st,
		Handler:     handler,
		Directory:   gt.Some(cfg.Directory),
		HTTPClient:  gt.Some(cfg.HTTPClient),
		StartCursor: gt.Some(startCursor),
		OnPageComplete: gt.Some(func(cursor string) error {
			return SaveListReposCursor(cfg.Store, cursor)
		}),
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
	if err := engine.Run(ctx); err != nil {
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
	if cfg.Writer == nil {
		return fmt.Errorf("backfill: Config.Writer is required")
	}
	if cfg.HTTPClient == nil {
		return fmt.Errorf("backfill: Config.HTTPClient is required")
	}
	if cfg.Directory == nil {
		return fmt.Errorf("backfill: Config.Directory is required")
	}
	if cfg.RelayURL == "" {
		return fmt.Errorf("backfill: Config.RelayURL is required")
	}
	if cfg.Logger == nil {
		return fmt.Errorf("backfill: Config.Logger is required")
	}
	return nil
}
