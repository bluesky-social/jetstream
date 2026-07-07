// run.go is the entrypoint cmd/jetstream calls
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
	"sync"
	"time"

	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/jcalabro/atmos"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	atmosidentity "github.com/jcalabro/atmos/identity"
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

	// RelayURL is the upstream relay base URL (e.g. https://bsky.network).
	RelayURL string

	// Logger is the structured logger; required (no sensible default
	// for an ingestion service that needs failure-mode visibility).
	Logger *slog.Logger

	// Metrics is optional; nil means we still run, just without
	// /metrics counters incrementing.
	Metrics *Metrics

	// DropMetrics is the shared ingest validation-drop counter family,
	// forwarded to the SegmentHandler. Optional.
	DropMetrics *ingest.DropMetrics

	// AfterRepoComplete is a test-only restart hook invoked after a
	// repo completion row is durably written. Leave nil in production.
	AfterRepoComplete func(context.Context, atmos.DID) error

	// CrashInjector is a test-only deterministic crash simulator. Leave nil in
	// production.
	CrashInjector crashpoint.Injector

	// MaxRepos, when > 0, is a debug-only ceiling on the number of
	// active, not-yet-complete listRepos entries this Run will select
	// and download before returning nil so the orchestrator can advance
	// to the merge phase. Pre-Complete repos from a prior Run are skipped
	// before they reach the counter and do not count.
	//
	// Intended for fast local-dev iteration against the production
	// relay (millions of users); leave 0 in production. This mode does
	// not advance the durable listRepos cursor because it is an intentional
	// debug-scoped partial crawl, not resumable whole-network bootstrap.
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

	// IdentityResolver resolves selected BackfillRepos DIDs so status
	// diagnostics can persist declared handle and PDS metadata without
	// walking listRepos. Required when BackfillRepos is non-empty; normal
	// whole-network backfill does not use it.
	IdentityResolver atmosidentity.Resolver

	// MaxRetries, RetryBaseDelay, and RetryMaxDelay tune the engine's
	// per-DID retry/backoff loop for transient getRepo failures. A zero
	// value means "use atmos's default" (3 retries, 1s base, 30s cap),
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
		drainDurability := func() error {
			cfg.Metrics.incForcedCheckpointFlushes()
			if err := cfg.Writer.DrainDurability(ctx); err != nil {
				return fmt.Errorf("backfill: drain durable completions: %w", err)
			}
			if fatal := loadFatal(); fatal != nil {
				logger := cfg.Logger.With(slog.String("component", "backfill/run"))
				logger.ErrorContext(ctx, "backfill aborted during durable completion drain", "err", fatal)
				return fmt.Errorf("backfill: %w", fatal)
			}
			return nil
		}
		var lastNonEmptyListReposCursor string
		var batchCursorSaved bool
		rememberPageCursor := func(cursor string) error {
			if cursor != "" {
				lastNonEmptyListReposCursor = cursor
			}
			return nil
		}
		saveBatchCursor := func(cursor string) error {
			if err := drainDurability(); err != nil {
				return err
			}
			// Persist the last non-empty cursor under a sibling key so the
			// merge phase can resume listRepos to discover DIDs born during
			// bootstrap. OnBatchComplete's final cursor can be empty even
			// when its completed batch covered earlier non-empty pages, so
			// OnPageComplete only records the latest candidate in memory;
			// this callback is still the only durable persistence point.
			bootstrapCursor := cursor
			if bootstrapCursor == "" {
				bootstrapCursor = lastNonEmptyListReposCursor
			}
			if err := SaveListReposCheckpoint(cfg.Store, cursor, bootstrapCursor); err != nil {
				return err
			}
			batchCursorSaved = true
			return nil
		}
		finishCleanEngineDrain := func() error {
			if err := drainDurability(); err != nil {
				return err
			}
			if batchCursorSaved {
				return nil
			}
			return SaveListReposCheckpoint(cfg.Store, "", "")
		}

		st := NewStore(cfg.Store, cfg.Metrics)
		st.afterComplete = cfg.AfterRepoComplete
		st.afterCompleteError = recordFatal
		st.crashInjector = cfg.CrashInjector
		completions := NewCompletionBatcher(st, cfg.Metrics)
		st.SetCompletionBatcher(completions)
		cfg.Writer.SetDurableBatchHook(completions.StageDurable)

		// Backfill downloads via the relay (SyncClient), which 302-redirects
		// to each account's PDS; the engine does not resolve DID→PDS and does
		// not verify commit signatures (VerifyCommits stays off). The host the
		// CAR came from is surfaced to Store.OnComplete/OnFail for per-host
		// attribution, so no identity resolution is needed on this path.
		sc := atmossync.NewClient(atmossync.Options{Client: xc})

		handler := NewSegmentHandler(cfg.Writer, cfg.Logger, cfg.Metrics)
		handler.onWriterError = recordFatal
		handler.SetCompletionBatcher(completions)
		handler.SetDropMetrics(cfg.DropMetrics)
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
				Repos:            cfg.BackfillRepos,
				Store:            st,
				Handler:          handler,
				SyncClient:       sc,
				IdentityResolver: cfg.IdentityResolver,
				Metrics:          cfg.Metrics,
				MaxRetries:       cfg.MaxRetries,
				RetryBaseDelay:   cfg.RetryBaseDelay,
				RetryMaxDelay:    cfg.RetryMaxDelay,
				OnError: func(did atmos.DID, err error) {
					if !shouldLogBackfillError(err) {
						return
					}
					logger.WarnContext(ctx, "repo failed", "did", string(did), "err", err)
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
			return drainDurability()
		}

		startCursor, err := LoadListReposCursor(cfg.Store)
		if err != nil {
			return fmt.Errorf("backfill: %w", err)
		}
		if startCursor != "" {
			logger.InfoContext(ctx, "resuming from saved cursor", "cursor", startCursor)
		}

		if cfg.MaxRepos > 0 {
			if err := DeleteBootstrapLastListReposCursor(cfg.Store); err != nil {
				return fmt.Errorf("backfill: limited repo mode: %w", err)
			}
			logger.InfoContext(ctx, "starting limited listRepos backfill",
				"relay", cfg.RelayURL,
				"max_repos", cfg.MaxRepos,
			)
			repos, err := collectLimitedListRepos(runCtx, sc, st, startCursor, cfg.MaxRepos)
			if err != nil {
				if fatal := loadFatal(); fatal != nil {
					logger.ErrorContext(ctx, "limited listRepos backfill aborted after local writer error", "err", fatal)
					return fmt.Errorf("backfill: %w", fatal)
				}
				logger.ErrorContext(ctx, "limited listRepos backfill discovery returned error", "err", err)
				return fmt.Errorf("backfill: %w", err)
			}
			logger.InfoContext(ctx, "collected limited listRepos backfill repos",
				"requested", cfg.MaxRepos,
				"repos", len(repos),
			)
			err = runSelectedRepos(runCtx, selectedReposConfig{
				Repos:          repos,
				Store:          st,
				Handler:        handler,
				SyncClient:     sc,
				Metrics:        cfg.Metrics,
				MaxRetries:     cfg.MaxRetries,
				RetryBaseDelay: cfg.RetryBaseDelay,
				RetryMaxDelay:  cfg.RetryMaxDelay,
				OnError: func(did atmos.DID, err error) {
					if !shouldLogBackfillError(err) {
						return
					}
					logger.WarnContext(ctx, "repo failed", "did", string(did), "err", err)
				},
			})
			if err != nil {
				if fatal := loadFatal(); fatal != nil {
					logger.ErrorContext(ctx, "limited listRepos backfill aborted after local writer error", "err", fatal)
					return fmt.Errorf("backfill: %w", fatal)
				}
				logger.ErrorContext(ctx, "limited listRepos backfill returned error", "err", err)
				return fmt.Errorf("backfill: %w", err)
			}
			if fatal := loadFatal(); fatal != nil {
				logger.ErrorContext(ctx, "limited listRepos backfill aborted after local writer error", "err", fatal)
				return fmt.Errorf("backfill: %w", fatal)
			}
			return drainDurability()
		}

		engineOpts := atmosbackfill.Options{
			SyncClient:      sc,
			Store:           st,
			Handler:         handler,
			StartCursor:     gt.Some(startCursor),
			OnBatchComplete: gt.Some(saveBatchCursor),
			OnPageComplete:  gt.Some(rememberPageCursor),
			OnError: gt.Some(func(did atmos.DID, err error) {
				if !shouldLogBackfillError(err) {
					return
				}
				logger.WarnContext(ctx, "repo failed", "did", string(did), "err", err)
			}),
			OnProgress: gt.Some(func(stats atmosbackfill.Stats) {
				cfg.Metrics.setProgressCompleted(stats.Completed)
			}),
		}

		// Only override atmos's retry defaults when explicitly configured;
		// a zero value leaves the engine on its production defaults (3
		// transient retries, 1s base, 30s cap, plus the separate 429
		// rate-limit budget). The oracle harness sets a tiny RetryBaseDelay
		// so injected transient faults recover quickly.
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
			logger.ErrorContext(ctx, "engine returned error", "err", err)
			return fmt.Errorf("backfill: %w", err)
		}
		if fatal := loadFatal(); fatal != nil {
			logger.ErrorContext(ctx, "engine aborted after local writer error", "err", fatal)
			return fmt.Errorf("backfill: %w", fatal)
		}

		logger.InfoContext(ctx, "engine drained")
		return finishCleanEngineDrain()
	})
}

func collectLimitedListRepos(ctx context.Context, sc *atmossync.Client, st *Store, startCursor string, maxRepos int) ([]atmos.DID, error) {
	if maxRepos <= 0 {
		return nil, nil
	}

	repos := make([]atmos.DID, 0, maxRepos)
	for page, err := range sc.ListRepos(ctx, int64(maxRepos), startCursor) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err != nil {
			return nil, fmt.Errorf("limited listRepos: %w", err)
		}
		for _, entry := range page.Entries {
			if !entry.Active {
				continue
			}
			rec, err := st.Lookup(ctx, entry.DID)
			if err != nil {
				return nil, fmt.Errorf("limited listRepos lookup %s: %w", entry.DID, err)
			}
			if rec.State == atmosbackfill.StateComplete {
				continue
			}
			repos = append(repos, entry.DID)
			if len(repos) >= maxRepos {
				return repos, nil
			}
		}
	}
	return repos, nil
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
	if cfg.RelayURL == "" {
		return fmt.Errorf("backfill: Config.RelayURL is required")
	}
	if cfg.BackfillWorkers < 0 {
		return fmt.Errorf("backfill: Config.BackfillWorkers must be >= 0")
	}
	if cfg.BackfillBatchSize < 0 {
		return fmt.Errorf("backfill: Config.BackfillBatchSize must be >= 0")
	}
	if len(cfg.BackfillRepos) > 0 && cfg.IdentityResolver == nil {
		return fmt.Errorf("backfill: Config.IdentityResolver is required when Config.BackfillRepos is set")
	}
	if cfg.Logger == nil {
		return fmt.Errorf("backfill: Config.Logger is required")
	}
	return nil
}
