// Package backfill: run.go is the entrypoint cmd/jetstream calls
// from its errgroup. Run constructs the atmos engine and drives it
// to completion. Returns nil on clean drain (every DID either skipped
// at Complete or downloaded + recorded), the engine's error
// otherwise.
package backfill

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/crashpoint"
	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/obs"
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

	// AfterRepoComplete is a test-only restart hook invoked after a
	// repo completion row is durably written. Leave nil in production.
	AfterRepoComplete func(context.Context, atmos.DID) error

	// CrashInjector is a test-only deterministic crash simulator. Leave nil in
	// production.
	CrashInjector crashpoint.Injector

	// MaxRepos, when > 0, is a debug-only ceiling on the number of
	// repos this Run will fully download before stopping early and
	// returning nil so the orchestrator can advance to the merge
	// phase. Counts atmos progress callbacks (Stats.Completed), i.e.
	// repos transitioned to StateComplete during this Run; pre-Complete
	// repos from a prior Run are skipped before they reach the
	// counter and do not count.
	//
	// Intended for fast local-dev iteration against the production
	// relay (millions of users); leave 0 in production. The persisted
	// listRepos cursor will not advance past the page on which the
	// limit trips, so subsequent runs without the flag re-walk from
	// the same point.
	MaxRepos int

	// BackfillWorkers, when > 0, overrides atmos's repo download worker count.
	// Zero leaves atmos on its default.
	BackfillWorkers int

	// BackfillBatchSize, when > 0, overrides atmos's listRepos-entry batch size.
	// Zero leaves atmos on its default.
	BackfillBatchSize int

	// BackfillRepos, when non-empty, is a debug-only explicit DID list
	// to download during bootstrap instead of walking listRepos. This is
	// intended for targeted production smoke tests against a known repo.
	// The normal Store, Handler, retry, verification, and completion
	// paths are still used; only discovery is replaced.
	BackfillRepos []atmos.DID

	// MaxRetries, RetryBaseDelay, and RetryMaxDelay tune the engine's
	// per-DID retry/backoff loop for transient getRepo failures. A zero
	// value means "use atmos's default" (5 retries, 1s base, 30s cap),
	// so production leaves all three at their zero value. The oracle
	// fault-injection harness sets RetryBaseDelay to a sub-millisecond
	// value so injected transient 503s recover without paying atmos's
	// 1s production backoff per fault. MaxRetries is intentionally NOT
	// lowered there: the fault budget per DID stays well inside the
	// default retry count, and shrinking it would risk turning a
	// transient fault into a spurious permanent failure.
	MaxRetries     int
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
}

// Run drives the atmos backfill engine to completion. It blocks until
// the engine drains or ctx is cancelled. Safe to call multiple times
// across process restarts: each call constructs a fresh Engine
// (atmos engines are single-shot) and resumes by skipping rows
// already at StatusComplete via Store.Lookup.
func Run(ctx context.Context, cfg Config) error {
	return obs.Span(ctx, func(ctx context.Context) error {
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

		runCtx, cancelRun := context.WithCancel(ctx)
		defer cancelRun()

		var fatalMu sync.Mutex
		var fatalErr error
		recordFatal := func(err error) {
			fatalMu.Lock()
			if fatalErr == nil {
				fatalErr = err
			}
			fatalMu.Unlock()
			cancelRun()
		}
		loadFatal := func() error {
			fatalMu.Lock()
			defer fatalMu.Unlock()
			return fatalErr
		}

		st := NewStore(cfg.Store, cfg.Metrics)
		st.afterComplete = cfg.AfterRepoComplete
		st.afterCompleteError = recordFatal
		st.crashInjector = cfg.CrashInjector
		directory := directoryWithRecordingResolver(cfg.Directory, st, recordFatal)

		sc := atmossync.NewClient(atmossync.Options{
			Client:    xc,
			Directory: gt.Some(directory),
		})

		handler := NewSegmentHandler(cfg.Writer, cfg.Logger, cfg.Metrics)
		handler.onWriterError = recordFatal
		logger := cfg.Logger.With(slog.String("component", "backfill/run"))

		if len(cfg.BackfillRepos) > 0 {
			if err := DeleteBootstrapLastListReposCursor(cfg.Store); err != nil {
				return fmt.Errorf("backfill: selected repo mode: %w", err)
			}
			logger.InfoContext(ctx, "starting selected repo backfill",
				"relay", cfg.RelayURL,
				"repos", len(cfg.BackfillRepos),
			)
			err := runSelectedRepos(runCtx, selectedReposConfig{
				Repos:          cfg.BackfillRepos,
				Store:          st,
				Handler:        handler,
				SyncClient:     sc,
				Directory:      directory,
				HTTPClient:     cfg.HTTPClient,
				Metrics:        cfg.Metrics,
				MaxRetries:     cfg.MaxRetries,
				RetryBaseDelay: cfg.RetryBaseDelay,
				RetryMaxDelay:  cfg.RetryMaxDelay,
				OnError: func(did atmos.DID, err error) {
					if !shouldLogBackfillError(err) {
						return
					}
					logger.WarnContext(ctx, "repo failed", "did", string(did), "err", err)
					if errors.Is(err, errIdentityDiagnosticsPersistence) {
						recordFatal(err)
					}
				},
			})
			if err != nil {
				if fatal := loadFatal(); fatal != nil {
					logger.ErrorContext(ctx, "selected repo backfill aborted after local writer error", "err", fatal)
					return fmt.Errorf("backfill: %w", fatal)
				}
				logger.ErrorContext(ctx, "selected repo backfill returned error", "err", err)
				return fmt.Errorf("backfill: %w", err)
			}
			if fatal := loadFatal(); fatal != nil {
				logger.ErrorContext(ctx, "selected repo backfill aborted after local writer error", "err", fatal)
				return fmt.Errorf("backfill: %w", fatal)
			}
			return nil
		}

		startCursor, err := LoadListReposCursor(cfg.Store)
		if err != nil {
			return fmt.Errorf("backfill: %w", err)
		}
		if startCursor != "" {
			logger.InfoContext(ctx, "resuming from saved cursor", "cursor", startCursor)
		}

		// runCtx is what we hand to the atmos engine. We cancel it for
		// local writer failures and for MaxRepos so workers stop before
		// atmos can record a per-DID failure for infrastructure state.
		// limitTripped distinguishes "debug ceiling reached" (return nil)
		// from "outer ctx cancelled" (propagate).
		var limitTripped atomic.Bool

		engineOpts := atmosbackfill.Options{
			SyncClient:  sc,
			Store:       st,
			Handler:     handler,
			Directory:   gt.Some(directory),
			HTTPClient:  gt.Some(cfg.HTTPClient),
			StartCursor: gt.Some(startCursor),
			OnPageComplete: gt.Some(func(cursor string) error {
				if err := SaveListReposCursor(cfg.Store, cursor); err != nil {
					return err
				}
				// Persist the last non-empty cursor under a sibling
				// key so the merge phase can resume listRepos to
				// discover DIDs born during bootstrap. The
				// MaybeSave helper short-circuits on cursor=="".
				return MaybeSaveBootstrapLastListReposCursor(cfg.Store, cursor)
			}),
			OnError: gt.Some(func(did atmos.DID, err error) {
				if !shouldLogBackfillError(err) {
					return
				}
				logger.WarnContext(ctx, "repo failed", "did", string(did), "err", err)
				if errors.Is(err, errIdentityDiagnosticsPersistence) {
					recordFatal(err)
				}
			}),
			OnProgress: gt.Some(func(stats atmosbackfill.Stats) {
				cfg.Metrics.setProgressCompleted(stats.Completed)
				if cfg.MaxRepos > 0 && stats.Completed >= int64(cfg.MaxRepos) {
					if limitTripped.CompareAndSwap(false, true) {
						logger.WarnContext(ctx, "max-backfill-repos limit reached; stopping backfill early",
							"limit", cfg.MaxRepos,
							"completed", stats.Completed,
						)
						cancelRun()
					}
				}
			}),
		}

		// Only override atmos's retry defaults when explicitly configured;
		// a zero value leaves the engine on its production defaults (5
		// retries, 1s base, 30s cap). The oracle harness sets a tiny
		// RetryBaseDelay so injected transient faults recover quickly.
		if cfg.MaxRetries > 0 {
			engineOpts.MaxRetries = gt.Some(cfg.MaxRetries)
		}
		if cfg.BackfillWorkers > 0 {
			engineOpts.Workers = gt.Some(cfg.BackfillWorkers)
		}
		if cfg.BackfillBatchSize > 0 {
			engineOpts.BatchSize = gt.Some(cfg.BackfillBatchSize)
		}
		if cfg.RetryBaseDelay > 0 {
			engineOpts.RetryBaseDelay = gt.Some(cfg.RetryBaseDelay)
		}
		if cfg.RetryMaxDelay > 0 {
			engineOpts.RetryMaxDelay = gt.Some(cfg.RetryMaxDelay)
		}
		engine := atmosbackfill.NewEngine(engineOpts)

		logger.InfoContext(ctx, "starting", "relay", cfg.RelayURL, "max_repos", cfg.MaxRepos, "workers", cfg.BackfillWorkers, "batch_size", cfg.BackfillBatchSize)
		if err := engine.Run(runCtx); err != nil {
			if fatal := loadFatal(); fatal != nil {
				logger.ErrorContext(ctx, "engine aborted after local writer error", "err", fatal)
				return fmt.Errorf("backfill: %w", fatal)
			}
			// Internal limit-driven cancel: the outer ctx is still healthy,
			// only our derived runCtx was cancelled. Treat as a clean drain
			// so the orchestrator advances to merge.
			if limitTripped.Load() && errors.Is(err, context.Canceled) && ctx.Err() == nil {
				logger.InfoContext(ctx, "engine drained early via max-backfill-repos")
				return nil
			}
			logger.ErrorContext(ctx, "engine returned error", "err", err)
			return fmt.Errorf("backfill: %w", err)
		}
		if fatal := loadFatal(); fatal != nil {
			logger.ErrorContext(ctx, "engine aborted after local writer error", "err", fatal)
			return fmt.Errorf("backfill: %w", fatal)
		}

		logger.InfoContext(ctx, "engine drained")
		return nil
	})
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
	if cfg.BackfillWorkers < 0 {
		return fmt.Errorf("backfill: Config.BackfillWorkers must be >= 0")
	}
	if cfg.BackfillBatchSize < 0 {
		return fmt.Errorf("backfill: Config.BackfillBatchSize must be >= 0")
	}
	if cfg.Logger == nil {
		return fmt.Errorf("backfill: Config.Logger is required")
	}
	return nil
}
