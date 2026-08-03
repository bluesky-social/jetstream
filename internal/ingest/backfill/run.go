package backfill

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Config struct {
	Store       *store.Store
	Writer      *ingest.Writer
	HTTPClient  *http.Client
	RelayURL    string
	Logger      *slog.Logger
	Metrics     *Metrics
	DropMetrics *ingest.DropMetrics

	AfterRepoComplete func(context.Context, atmos.DID) error
	CrashInjector     crashpoint.Injector

	MaxRepos          int
	GlobalDownloads   int
	HostWorkers       int
	MaxActiveHosts    int
	MaxHosts          int
	BackfillBatchSize int

	// BackfillWorkers is the retired relay-global worker knob. It is kept in
	// the internal struct for one release so startup can reject it loudly.
	BackfillWorkers int

	BackfillRepos    []atmos.DID
	IdentityResolver atmosidentity.Resolver

	MaxRetries     int
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration

	// NewHostClient is injected by the simulator/oracle. Production leaves it
	// nil and uses validated HTTPS hostnames over HTTPClient's shared pool.
	NewHostClient func(hostname string) (*atmossync.Client, error)
}

// Run drives a complete listHosts -> direct per-PDS bootstrap. Per-host
// cursors and terminal host state are staged into the writer's durable Pebble
// batch behind the segment fsync they describe.
func Run(ctx context.Context, cfg Config) error {
	return obs.Span(ctx, func(ctx context.Context) error {
		if err := cfg.validate(); err != nil {
			return err
		}
		if err := RejectRetiredCursors(cfg.Store); err != nil {
			return err
		}

		relayXRPC := &xrpc.Client{
			Host: cfg.RelayURL, HTTPClient: gt.Some(cfg.HTTPClient),
			Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)}),
		}
		relay := atmossync.NewClient(atmossync.Options{Client: relayXRPC})

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
		completions := NewCompletionBatcher(st, cfg.Metrics)
		st.SetCompletionBatcher(completions)
		cfg.Writer.SetDurableBatchHook(completions.StageDurable)

		handler := NewSegmentHandler(cfg.Writer, cfg.Logger, cfg.Metrics)
		handler.onWriterError = recordFatal
		handler.SetCompletionBatcher(completions)
		handler.SetDropMetrics(cfg.DropMetrics)
		logger := cfg.Logger.With(slog.String("component", "backfill/run"))
		if cfg.BackfillWorkers != 0 {
			logger.WarnContext(ctx, "JETSTREAM_BACKFILL_WORKERS is retired and ignored; use JETSTREAM_BACKFILL_GLOBAL_DOWNLOADS and JETSTREAM_BACKFILL_HOST_WORKERS_MAX")
		}

		drain := func() error {
			if err := cfg.Writer.DrainDurability(ctx); err != nil {
				return fmt.Errorf("backfill: drain durability: %w", err)
			}
			if fatal := loadFatal(); fatal != nil {
				return fmt.Errorf("backfill: %w", fatal)
			}
			return nil
		}

		if len(cfg.BackfillRepos) > 0 {
			logger.InfoContext(ctx, "starting selected repo backfill", "repos", len(cfg.BackfillRepos))
			err := runSelectedRepos(runCtx, selectedReposConfig{
				Repos: cfg.BackfillRepos, Store: st, Handler: handler,
				SyncClient: relay, IdentityResolver: cfg.IdentityResolver, Metrics: cfg.Metrics,
				MaxRetries: cfg.MaxRetries, RetryBaseDelay: cfg.RetryBaseDelay, RetryMaxDelay: cfg.RetryMaxDelay,
				OnError: func(did atmos.DID, err error) {
					if shouldLogBackfillError(err) {
						logger.WarnContext(ctx, "repo failed", "did", string(did), "err", err)
					}
				},
			})
			if err != nil {
				if fatal := loadFatal(); fatal != nil {
					return fmt.Errorf("backfill: %w", fatal)
				}
				return fmt.Errorf("backfill: %w", err)
			}
			return drain()
		}

		newHostClient := cfg.NewHostClient
		if newHostClient == nil {
			newHostClient = NewHostClientBuilder(cfg.RelayURL, cfg.HTTPClient)
		}

		var limited atomic.Bool
		engineOpts := atmosbackfill.Options{
			Relay: relay, NewHostClient: gt.Some(newHostClient), Store: st.AtmosStore(), Handler: handler,
			OnError: gt.Some(func(did atmos.DID, err error) {
				if shouldLogBackfillError(err) {
					logger.WarnContext(ctx, "repo failed", "did", string(did), "err", err)
				}
			}),
			OnProgress: gt.Some(func(stats atmosbackfill.Stats) {
				cfg.Metrics.observeProgress(stats)
				if cfg.MaxRepos > 0 && stats.Completed+stats.Failed >= int64(cfg.MaxRepos) && limited.CompareAndSwap(false, true) {
					cancelRun()
				}
			}),
			OnHostState: gt.Some(func(host atmosbackfill.HostInfo, state atmosbackfill.HostState, attempts int, err error) {
				cfg.Metrics.observeHostState(host, state, attempts)
				traceHostState(ctx, host, state, attempts, err)
				if state == atmosbackfill.HostStateExhausted {
					logger.WarnContext(ctx, "PDS host exhausted", "host", host.Hostname, "attempts", attempts, "err", err)
				}
			}),
			OnHostnameRejected: gt.Some(func(host string, err error) {
				cfg.Metrics.incHostnameRejected()
				logger.WarnContext(ctx, "rejected listHosts hostname", "host", host, "err", err)
			}),
			OnRosterCapped:     gt.Some(func(limit int) { cfg.Metrics.incRosterCapHit() }),
			OnDownloadSlotWait: gt.Some(func(wait time.Duration) { cfg.Metrics.observeDownloadSlotWait(wait) }),
		}
		if cfg.GlobalDownloads > 0 {
			engineOpts.GlobalDownloads = gt.Some(cfg.GlobalDownloads)
		}
		if cfg.HostWorkers > 0 {
			engineOpts.HostWorkers = gt.Some(cfg.HostWorkers)
		}
		if cfg.MaxActiveHosts > 0 {
			engineOpts.MaxActiveHosts = gt.Some(cfg.MaxActiveHosts)
		}
		if cfg.MaxHosts > 0 {
			engineOpts.MaxHosts = gt.Some(cfg.MaxHosts)
		}
		if cfg.BackfillBatchSize > 0 {
			engineOpts.BatchSize = gt.Some(cfg.BackfillBatchSize)
		}
		if cfg.MaxRetries > 0 {
			engineOpts.MaxRetries = gt.Some(cfg.MaxRetries)
		}
		if cfg.RetryBaseDelay > 0 {
			engineOpts.RetryBaseDelay = gt.Some(cfg.RetryBaseDelay)
			engineOpts.HostBackoffBase = gt.Some(cfg.RetryBaseDelay)
		}
		if cfg.RetryMaxDelay > 0 {
			engineOpts.RetryMaxDelay = gt.Some(cfg.RetryMaxDelay)
			engineOpts.HostBackoffMax = gt.Some(cfg.RetryMaxDelay)
		}

		logger.InfoContext(ctx, "starting PDS-direct backfill", "relay", cfg.RelayURL,
			"global_downloads", cfg.GlobalDownloads, "host_workers", cfg.HostWorkers,
			"max_active_hosts", cfg.MaxActiveHosts, "batch_size", cfg.BackfillBatchSize)
		err := atmosbackfill.NewEngine(engineOpts).Run(runCtx)
		if err != nil && (!limited.Load() || !errors.Is(err, context.Canceled)) {
			if fatal := loadFatal(); fatal != nil {
				return fmt.Errorf("backfill: %w", fatal)
			}
			return fmt.Errorf("backfill: %w", err)
		}
		if err := drain(); err != nil {
			return err
		}
		logger.InfoContext(ctx, "PDS-direct backfill drained", "limited", limited.Load())
		return nil
	})
}

func traceHostState(ctx context.Context, host atmosbackfill.HostInfo, state atmosbackfill.HostState, attempts int, cause error) {
	_, span := obs.Tracer("ingest/backfill").Start(ctx, "host.lifecycle",
		trace.WithAttributes(
			attribute.String("hostname", host.Hostname),
			attribute.String("state", string(state)),
			attribute.Int("attempts", attempts),
			attribute.String("relay_status", host.RelayStatus),
		))
	if cause != nil {
		span.RecordError(cause)
		span.SetStatus(codes.Error, cause.Error())
	}
	span.End()
}

func NewHostClientBuilder(relayURL string, httpClient *http.Client) func(string) (*atmossync.Client, error) {
	relayParsed, _ := url.Parse(relayURL)
	return func(hostname string) (*atmossync.Client, error) {
		var hostURL string
		if relayParsed != nil && relayParsed.Scheme == "http" && hostname == relayParsed.Host && isLoopbackHost(relayParsed.Hostname()) {
			hostURL = "http://" + hostname
		} else {
			if err := atmosbackfill.ValidateHostname(hostname); err != nil {
				return nil, err
			}
			hostURL = "https://" + strings.ToLower(hostname)
		}
		xc := &xrpc.Client{Host: hostURL, HTTPClient: gt.Some(httpClient), Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
		return atmossync.NewClient(atmossync.Options{Client: xc}), nil
	}
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
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
	if cfg.Logger == nil {
		return fmt.Errorf("backfill: Config.Logger is required")
	}
	for name, value := range map[string]int{"GlobalDownloads": cfg.GlobalDownloads, "HostWorkers": cfg.HostWorkers, "MaxActiveHosts": cfg.MaxActiveHosts, "MaxHosts": cfg.MaxHosts, "BackfillBatchSize": cfg.BackfillBatchSize} {
		if value < 0 {
			return fmt.Errorf("backfill: Config.%s must be >= 0", name)
		}
	}
	if len(cfg.BackfillRepos) > 0 && cfg.IdentityResolver == nil {
		return fmt.Errorf("backfill: Config.IdentityResolver is required when Config.BackfillRepos is set")
	}
	return nil
}
