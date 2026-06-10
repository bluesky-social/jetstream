package jetstreamd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	identcache "github.com/bluesky-social/jetstream-v2/internal/identity"
	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream-v2/internal/ingest/live"
	"github.com/bluesky-social/jetstream-v2/internal/ingest/orchestrator"
	"github.com/bluesky-social/jetstream-v2/internal/ingest/syncstate"
	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
	"github.com/bluesky-social/jetstream-v2/internal/manifest"
	"github.com/bluesky-social/jetstream-v2/internal/obs"
	"github.com/bluesky-social/jetstream-v2/internal/server"
	"github.com/bluesky-social/jetstream-v2/internal/status"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/internal/subscribe"
	"github.com/bluesky-social/jetstream-v2/internal/version"
	"github.com/bluesky-social/jetstream-v2/internal/web"
	"github.com/bluesky-social/jetstream-v2/internal/xrpcapi"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/jcalabro/jttp"
	"golang.org/x/sync/errgroup"
)

// Runtime is one fully constructed jetstream daemon instance.
type Runtime struct {
	opts Options

	processLogger *slog.Logger
	logger        *slog.Logger

	tracerShutdown obs.TracerShutdown
	cancelManifest context.CancelFunc
	metaStore      *store.Store
	manifest       *manifest.Manifest
	tail           *subscribe.Tail
	verifier       *atmossync.Verifier
	orchestrator   *orchestrator.Orchestrator
	server         *server.Server

	closeMu sync.Mutex
}

// Build constructs the production service graph without starting listeners or
// ingestion. Call Run to drive the graph, then Close during shutdown.
func Build(ctx context.Context, opts Options) (*Runtime, error) {
	if opts.SegmentCacheMaxAge < 0 {
		return nil, fmt.Errorf("serve: --segment-cache-max-age must be >= 0 (SegmentCacheMaxAge must be >= 0), got %s", opts.SegmentCacheMaxAge)
	}

	processLogger, err := obs.BuildLoggerFromStrings(opts.LogOutput, opts.LogLevel, opts.LogFormat)
	if err != nil {
		return nil, err
	}
	// processLogger is the bare per-process logger; downstream
	// subsystems (orchestrator, server, verifier callback) receive it
	// AS-IS so each can set its own `component` without slog stacking
	// duplicate keys (slog.With appends rather than replacing).
	//
	// logger is a component=main wrapper for runtime-level log lines.
	logger := processLogger.With(slog.String("component", "main"))
	slog.SetDefault(logger)

	info := version.Get()
	logger.Info("startup",
		"version", info.Version,
		"commit", info.Commit,
		"built", info.Date,
	)

	rt := &Runtime{
		opts:          opts,
		processLogger: processLogger,
		logger:        logger,
	}
	cleanupTimeout := opts.ShutdownTimeout
	if cleanupTimeout <= 0 {
		cleanupTimeout = 30 * time.Second
	}
	fail := func(err error) (*Runtime, error) {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		rt.closeWithLogging(cleanupCtx)
		return nil, err
	}

	tracerShutdown, err := obs.SetupTracing(ctx, obs.TracingConfig{
		ServiceName: opts.OTelServiceName,
	})
	if err != nil {
		return nil, fmt.Errorf("setup tracing: %w", err)
	}
	rt.tracerShutdown = tracerShutdown

	metrics := obs.NewMetrics()
	storeMetrics := store.NewMetrics(metrics.Registry)
	segmentMetrics := segment.NewMetrics(metrics.Registry)
	verifierMetrics := obs.NewVerifierMetrics(metrics.Registry)
	subscribeMetrics := subscribe.NewMetrics(metrics.Registry)
	manifestMetrics := manifest.NewMetrics(metrics.Registry)

	if err := os.MkdirAll(opts.DataDir, 0o755); err != nil {
		return fail(fmt.Errorf("serve: create data dir %s: %w", opts.DataDir, err))
	}

	segmentsDir := filepath.Join(opts.DataDir, "segments")
	if err := os.MkdirAll(segmentsDir, 0o755); err != nil {
		return fail(fmt.Errorf("serve: create segments dir %s: %w", segmentsDir, err))
	}

	metaStore, err := store.Open(opts.DataDir, storeMetrics)
	if err != nil {
		return fail(err)
	}
	rt.metaStore = metaStore

	manifestCtx, cancelManifest := context.WithCancel(ctx)
	rt.cancelManifest = cancelManifest
	mft, err := manifest.OpenBackground(manifestCtx, manifest.Options{
		SegmentsDir:         segmentsDir,
		BlockIndexCacheSize: opts.CursorBlockIndexCacheSize,
		Logger:              processLogger,
		Metrics:             manifestMetrics,
	})
	if err != nil {
		return fail(fmt.Errorf("serve: open manifest: %w", err))
	}
	rt.manifest = mft

	// writerPtr is published by the orchestrator once the steady-state
	// live consumer opens its ingest.Writer; the cursor handler reads it
	// atomically. Before steady-state the lifecycle.IsSteadyState gate
	// returns 503, so the nil-pointer window is harmless.
	var writerPtr atomic.Pointer[ingest.Writer]

	statusCollector, err := status.New(status.Options{
		Store:          metaStore,
		DataDir:        opts.DataDir,
		Manifest:       mft,
		CursorLookback: opts.CursorLookback,
	})
	if err != nil {
		return fail(fmt.Errorf("serve: build status collector: %w", err))
	}

	statusHandler, err := web.New(web.Options{
		Snapshotter: statusCollector,
		RepoActions: web.NewRepoActions(opts.DataDir, opts.RelayURL),
		Logger:      processLogger,
	})
	if err != nil {
		return fail(fmt.Errorf("serve: build status handler: %w", err))
	}

	// Verifier setup (shared across phases). The verifier itself is
	// owned by the orchestrator's per-phase live consumers, but we
	// construct it here because its async-error drain is a sibling
	// goroutine in the top-level errgroup -- it's a process-wide
	// observability concern.
	relayHTTPURL, err := live.DeriveRelayHTTPURL(opts.RelayURL)
	if err != nil {
		return fail(fmt.Errorf("serve: derive relay HTTP URL: %w", err))
	}

	xrpcClient := &xrpc.Client{
		Host:       relayHTTPURL,
		HTTPClient: gt.Some(jttp.New(xrpc.BulkDownloadOpts()...)),
	}

	resolver := &identity.DefaultResolver{}
	if opts.PLCURL != "" {
		resolver.PLCURL = gt.Some(opts.PLCURL)
		// atmos's default resolver client enables jttp.WithStrictSSRFProtection,
		// which refuses loopback even on the initial request. When the
		// operator points us at a local PLC (e.g. the dev simulator at
		// http://localhost:7777), use a non-strict client so the dial
		// succeeds.
		resolver.HTTPClient = gt.Some(jttp.New(xrpc.ATProtoOpts(10 * time.Second)...))
	}
	directory := &identity.Directory{
		Resolver:               resolver,
		Cache:                  identcache.New(metaStore, identcache.DefaultTTL),
		SkipHandleVerification: true,
	}

	stateStore := syncstate.New(metaStore)
	syncClient := atmossync.NewClient(atmossync.Options{Client: xrpcClient})

	coldRd := subscribe.NewColdReader(subscribe.ColdReaderConfig{
		Manifest:        mft,
		WriterRef:       &writerPtr,
		BlockCacheBytes: opts.SubscribeBlockCacheBytes,
	})
	tail, err := subscribe.New(subscribe.Config{
		Logger:       processLogger,
		Metrics:      subscribeMetrics,
		HotTailBytes: opts.SubscribeHotTailBytes,
		ReadBatch:    opts.SubscribeReadBatch,
		SlowWindow:   opts.SubscribeSlowWindow,
		SlowMinRate:  opts.SubscribeSlowMinRate,
	}, coldRd, func() uint64 {
		if w := writerPtr.Load(); w != nil {
			return w.NextSeq()
		}
		return 0
	})
	if err != nil {
		return fail(fmt.Errorf("serve: build subscribe tail: %w", err))
	}
	rt.tail = tail

	verifierLogger := processLogger.With(slog.String("component", "verifier"))
	verifier, err := atmossync.NewVerifier(atmossync.VerifierOptions{
		Directory:  directory,
		StateStore: stateStore,
		SyncClient: gt.Some(syncClient),
		OnVerificationFailure: gt.Some(func(did atmos.DID, vErr error) {
			verifierMetrics.IncFailure(obs.Classify(vErr))
			verifierLogger.Warn("verification failure",
				"did", did,
				"err", vErr,
			)
		}),
	})
	if err != nil {
		return fail(fmt.Errorf("serve: build verifier: %w", err))
	}
	rt.verifier = verifier

	// The orchestrator owns all ingestion-lifecycle subsystems
	// (backfill engine, bootstrap-time live consumer, steady-state
	// live consumer). The runtime is no longer phase-aware.
	onSteadyStateEvent := func(ev *segment.Event) {
		tail.Append(ev)
		if opts.OnSteadyStateEvent != nil {
			opts.OnSteadyStateEvent(ev)
		}
	}
	orch, err := orchestrator.New(orchestrator.Config{
		DataDir:    opts.DataDir,
		Store:      metaStore,
		RelayURL:   opts.RelayURL,
		HTTPClient: xrpcClient.HTTPClient.Val(),
		Directory:  directory,
		Verifier:   verifier,
		// Bare logger; orchestrator.New attaches component=orchestrator
		// itself, and its children (live, ingest, backfill) attach
		// their own component on top of the bare parent.
		Logger:                 processLogger,
		Metrics:                orchestrator.NewMetrics(metrics.Registry),
		IngestMetrics:          ingest.NewMetrics(metrics.Registry),
		LiveMetrics:            live.NewMetrics(metrics.Registry),
		BackfillMetrics:        backfill.NewMetrics(metrics.Registry),
		SegmentMetrics:         segmentMetrics,
		OnEvent:                onSteadyStateEvent,
		OnBootstrapLiveEvent:   opts.OnBootstrapLiveEvent,
		MaxBackfillRepos:       opts.MaxBackfillRepos,
		BackfillRepos:          opts.BackfillRepos,
		SkipMergeDiscovery:     opts.SkipMergeDiscovery,
		BackfillRetryBaseDelay: opts.BackfillRetryBaseDelay,
		LiveReconnectBackoff:   opts.LiveReconnectBackoff,
		IngestOnAfterSeal:      mft.OnSegmentSealed,
		BarrierAfterBootstrap:  phaseBarrier(opts.BarrierAfterBootstrap),
		BarrierAfterMerge:      phaseBarrier(opts.BarrierAfterMerge),
		AfterRepoComplete:      opts.AfterRepoComplete,
		CrashInjector:          opts.CrashInjector,
		OnSteadyStateWriter: func(w *ingest.Writer) {
			writerPtr.Store(w)
		},
	})
	if err != nil {
		return fail(fmt.Errorf("serve: build orchestrator: %w", err))
	}
	rt.orchestrator = orch

	srv := server.New(server.Config{
		PublicAddr:      opts.PublicAddr,
		DebugAddr:       opts.DebugAddr,
		ShutdownTimeout: opts.ShutdownTimeout,
		StatusHandler:   statusHandler,
	}, processLogger, metrics)

	// HandlerDeps.WriterRef is read at request time via writerPtr.Load();
	// before steady-state, lifecycle.IsSteadyState gates with 503 so
	// nil-pointer reads are harmless.
	srv.RegisterPublicRoute("GET /subscribe", subscribe.NewHandler(subscribe.Subscription{
		Tail:      tail,
		Store:     metaStore,
		Manifest:  mft,
		WriterRef: &writerPtr,
		Logger:    processLogger,
		Metrics:   subscribeMetrics,
		Lookback:  opts.CursorLookback,
	}))

	// XRPC surface: whole-file segment download + listing. The atmos
	// xrpcserver routes /xrpc/{nsid}; mounting at the "/xrpc/" subtree
	// lets it own every jetstream NSID. Backed by the in-memory manifest,
	// which only tracks sealed (immutable) segments.
	xrpcSrv := xrpcapi.NewWithReadyAndCache(mft, processLogger, func(ctx context.Context) error {
		if !lifecycle.IsSteadyState(metaStore) {
			return errors.New("bootstrap in progress")
		}
		if err := mft.Wait(ctx); err != nil {
			return fmt.Errorf("manifest warming up: %w", err)
		}
		return nil
	}, opts.SegmentCacheMaxAge)
	srv.RegisterPublicRoute("/xrpc/", xrpcSrv.Handler())
	rt.server = srv

	return rt, nil
}

// Run starts the constructed service graph and blocks until shutdown or a
// fatal subsystem error.
func (r *Runtime) Run(ctx context.Context) error {
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		<-gctx.Done()
		r.cancelManifestLoad()
		return nil
	})

	g.Go(func() error {
		if err := r.manifest.Wait(gctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("manifest load: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		return r.server.Run(gctx)
	})

	g.Go(func() error {
		return r.orchestrator.Run(gctx)
	})

	// Graceful client drain. Live websocket subscribers are hijacked
	// connections, so http.Server.Shutdown neither tracks nor closes
	// them -- without this they'd be severed abruptly (no close frame) on
	// process exit. On shutdown we send each subscriber a StatusGoingAway
	// close frame and wait up to ClientDrainTimeout for them to leave
	// cleanly. We keep this in the errgroup so g.Wait() blocks on the
	// drain: the process must not exit out from under a half-sent close
	// handshake. The drain context is rooted at Background (not gctx,
	// which is already cancelled by the time we drain) and bounded by the
	// option, so a wedged client can't delay exit past the budget.
	g.Go(func() error {
		<-gctx.Done()
		drainCtx, drainCancel := context.WithTimeout(context.Background(), r.opts.ClientDrainTimeout)
		defer drainCancel()
		if err := r.tail.Shutdown(drainCtx); err != nil {
			r.logger.Warn("client drain did not complete within budget; severing remaining subscribers", "err", err)
		}
		return nil
	})

	verifierLogger := r.processLogger.With(slog.String("component", "verifier"))
	// Verifier async-error drain. Verification failures are
	// diagnostic, not fatal -- they typically reflect adversarial or
	// malformed PDS input, which is invalid user data, not a
	// jetstream bug. We warn-log and the OnVerificationFailure hook
	// fires for operator visibility, but never crash.
	g.Go(func() error {
		for {
			select {
			case <-gctx.Done():
				return nil
			case err, ok := <-r.verifier.AsyncErrors():
				if !ok {
					return nil
				}
				verifierLogger.Warn("async error", "err", err)
			}
		}
	})

	// A caller-driven shutdown surfaces as context.Canceled from
	// the errgroup: the orchestrator's steady-state consumer and the HTTP
	// server both return ctx.Err() on graceful shutdown. The caller owns
	// process signal handling, so we suppress cancellation only when it came
	// from the caller's context.
	runErr := g.Wait()
	if errors.Is(runErr, context.Canceled) && ctx.Err() != nil {
		runErr = nil
	}
	return runErr
}

// Close tears down resources owned by the runtime. It is safe to call once
// after Run returns, and repeated calls are ignored for already-closed fields.
func (r *Runtime) Close(ctx context.Context) error {
	r.cancelManifestLoad()

	r.closeMu.Lock()
	defer r.closeMu.Unlock()

	var errs []error
	if r.verifier != nil {
		if err := r.verifier.Close(); err != nil {
			r.logger.Error("verifier close", "err", err)
			errs = append(errs, fmt.Errorf("verifier close: %w", err))
		}
		r.verifier = nil
	}
	if r.metaStore != nil {
		if err := r.metaStore.Close(); err != nil {
			r.logger.Error("close metadata store", "err", err)
			errs = append(errs, fmt.Errorf("close metadata store: %w", err))
		}
		r.metaStore = nil
	}
	if r.tracerShutdown != nil {
		if err := r.tracerShutdown(ctx); err != nil {
			r.logger.Error("tracer shutdown failed", "err", err)
			errs = append(errs, fmt.Errorf("tracer shutdown: %w", err))
		}
		r.tracerShutdown = nil
	}
	return errors.Join(errs...)
}

// Options returns the resolved options used to build this runtime.
func (r *Runtime) Options() Options {
	return r.opts
}

func (r *Runtime) closeWithLogging(ctx context.Context) {
	if err := r.Close(ctx); err != nil {
		r.logger.Error("runtime cleanup failed", "err", err)
	}
}

func (r *Runtime) cancelManifestLoad() {
	r.closeMu.Lock()
	cancel := r.cancelManifest
	r.cancelManifest = nil
	r.closeMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func phaseBarrier(barrier PhaseBarrier) orchestrator.PhaseBarrier {
	if barrier == nil {
		return nil
	}
	return func(ctx context.Context) error {
		return barrier(ctx)
	}
}
