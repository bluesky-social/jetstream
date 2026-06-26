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

	identcache "github.com/bluesky-social/jetstream/internal/identity"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/ingest/live"
	"github.com/bluesky-social/jetstream/internal/ingest/orchestrator"
	"github.com/bluesky-social/jetstream/internal/ingest/syncstate"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/internal/overlay"
	"github.com/bluesky-social/jetstream/internal/server"
	"github.com/bluesky-social/jetstream/internal/status"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/bluesky-social/jetstream/internal/tombstone"
	"github.com/bluesky-social/jetstream/internal/version"
	"github.com/bluesky-social/jetstream/internal/web"
	"github.com/bluesky-social/jetstream/internal/xrpcapi"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/jcalabro/jttp"
	"golang.org/x/sync/errgroup"
)

// overlayRebuildInterval bounds how stale the served overlay blob can
// be relative to the live tail (spec §5.5). The blob's reported maxSeq
// is always honest; staleness only adds a few seconds of live replay on
// the consumer side.
const overlayRebuildInterval = 2 * time.Second

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
	overlayCache   *overlay.Cache
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
	if opts.CompactionInterval < 0 {
		return nil, fmt.Errorf("serve: --compaction-interval must be >= 0 (CompactionInterval must be >= 0), got %s", opts.CompactionInterval)
	}
	if opts.OverlayRebuildInterval < 0 {
		return nil, fmt.Errorf("serve: overlay rebuild interval must be >= 0 (OverlayRebuildInterval must be >= 0), got %s", opts.OverlayRebuildInterval)
	}
	if opts.CompactionTombstoneCap < 0 {
		return nil, fmt.Errorf("serve: --compaction-tombstone-cap must be >= 0 (CompactionTombstoneCap must be >= 0), got %d", opts.CompactionTombstoneCap)
	}
	if opts.BackfillWorkers < 0 {
		return nil, fmt.Errorf("serve: --backfill-workers must be >= 0 (BackfillWorkers must be >= 0), got %d", opts.BackfillWorkers)
	}
	if opts.BackfillBatchSize < 0 {
		return nil, fmt.Errorf("serve: --backfill-batch-size must be >= 0 (BackfillBatchSize must be >= 0), got %d", opts.BackfillBatchSize)
	}
	if opts.BackfillAsyncFlushWorkers < 0 {
		return nil, fmt.Errorf("serve: --backfill-async-flush-workers must be >= 0 (BackfillAsyncFlushWorkers must be >= 0), got %d", opts.BackfillAsyncFlushWorkers)
	}
	if opts.FailedRepoRetryInterval < 0 {
		return nil, fmt.Errorf("serve: --failed-repo-retry-interval must be >= 0 (FailedRepoRetryInterval must be >= 0), got %s", opts.FailedRepoRetryInterval)
	}
	if opts.FailedRepoRetryWorkers < 0 {
		return nil, fmt.Errorf("serve: --failed-repo-retry-workers must be >= 0 (FailedRepoRetryWorkers must be >= 0), got %d", opts.FailedRepoRetryWorkers)
	}
	if opts.FailedRepoRetryHostWorkers < 0 {
		return nil, fmt.Errorf("serve: --failed-repo-retry-host-workers must be >= 0 (FailedRepoRetryHostWorkers must be >= 0), got %d", opts.FailedRepoRetryHostWorkers)
	}
	if opts.FailedRepoRetryMaxDelay < 0 {
		return nil, fmt.Errorf("serve: --failed-repo-retry-max-delay must be >= 0 (FailedRepoRetryMaxDelay must be >= 0), got %s", opts.FailedRepoRetryMaxDelay)
	}
	if opts.CompactionRewriteWorkers < 0 {
		return nil, fmt.Errorf("serve: --compaction-rewrite-workers must be >= 0 (CompactionRewriteWorkers must be >= 0), got %d", opts.CompactionRewriteWorkers)
	}
	if opts.PlanMaxDIDs < 0 {
		return nil, fmt.Errorf("serve: --plan-max-dids must be >= 0 (PlanMaxDIDs must be >= 0), got %d", opts.PlanMaxDIDs)
	}
	if opts.PlanMaxCollections < 0 {
		return nil, fmt.Errorf("serve: --plan-max-collections must be >= 0 (PlanMaxCollections must be >= 0), got %d", opts.PlanMaxCollections)
	}
	if opts.PlanMaxEntries <= 0 {
		return nil, fmt.Errorf("serve: --plan-max-entries must be positive (PlanMaxEntries must be positive), got %d", opts.PlanMaxEntries)
	}
	if opts.PlanWholeSegmentThreshold <= 0 || opts.PlanWholeSegmentThreshold > 1 {
		return nil, fmt.Errorf("serve: --plan-whole-segment-threshold must be > 0 and <= 1 (PlanWholeSegmentThreshold must be > 0 and <= 1), got %g", opts.PlanWholeSegmentThreshold)
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
	segmentMetrics := obs.NewSegmentMetrics(metrics.Registry)
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

	// Verifier setup (shared across phases). The verifier itself is
	// owned by the orchestrator's per-phase live consumers, but we
	// construct it here because its async-error drain is a sibling
	// goroutine in the top-level errgroup -- it's a process-wide
	// observability concern.
	relayHTTPURL, err := live.DeriveRelayHTTPURL(opts.RelayURL)
	if err != nil {
		return fail(fmt.Errorf("serve: derive relay HTTP URL: %w", err))
	}

	// transportOpt, when an in-process transport is injected, routes every
	// jttp client through it instead of a real socket (deterministic harness).
	var transportOpt []jttp.Option
	if opts.HTTPTransport != nil {
		transportOpt = []jttp.Option{jttp.WithTransport(opts.HTTPTransport)}
	}

	backfillMetrics := backfill.NewMetrics(metrics.Registry)
	xrpcClient := &xrpc.Client{
		Host:       relayHTTPURL,
		HTTPClient: gt.Some(jttp.New(append(xrpc.BulkDownloadOpts(), transportOpt...)...)),
	}

	resolver := &identity.DefaultResolver{}
	if opts.PLCURL != "" {
		resolver.PLCURL = gt.Some(opts.PLCURL)
	}
	if opts.PLCURL != "" || opts.HTTPTransport != nil {
		// atmos's default resolver client enables jttp.WithStrictSSRFProtection,
		// which refuses loopback even on the initial request. When the
		// operator points us at a local PLC (e.g. the dev simulator at
		// http://localhost:7777), use a non-strict client so the dial
		// succeeds. We also install this client whenever an in-process
		// HTTPTransport is injected (even with the default PLC URL): the
		// transport is the RoundTripper for every outbound client per
		// Options.HTTPTransport, so identity/PLC resolution must route
		// through it too -- otherwise a "socket-free" runtime silently
		// dials the real network for resolution.
		resolver.HTTPClient = gt.Some(jttp.New(append(xrpc.ATProtoOpts(10*time.Second), transportOpt...)...))
	}
	directory := &identity.Directory{
		Resolver:               resolver,
		Cache:                  identcache.New(metaStore, identcache.DefaultTTL),
		SkipHandleVerification: true,
	}

	statusCollector, err := status.New(status.Options{
		Store:            metaStore,
		DataDir:          opts.DataDir,
		Manifest:         mft,
		CursorLookback:   opts.CursorLookback,
		IdentityResolver: resolver,
	})
	if err != nil {
		return fail(fmt.Errorf("serve: build status collector: %w", err))
	}

	statusHandler, err := web.New(web.Options{
		Snapshotter:                statusCollector,
		RepoActions:                web.NewRepoActions(opts.DataDir, resolver, newManifestSelector(mft), pendingEventsForDID(&writerPtr)),
		DisableRepoActionRateLimit: opts.DisableRepoActionRateLimits,
		Logger:                     processLogger,
	})
	if err != nil {
		return fail(fmt.Errorf("serve: build status handler: %w", err))
	}

	stateStore := syncstate.New(metaStore)
	tombstones := tombstone.New()
	overlayMetrics := obs.NewOverlayMetrics(metrics.Registry)
	overlayCache := overlay.NewCache(overlaySource{set: tombstones, store: metaStore}, overlayMetrics)
	rt.overlayCache = overlayCache
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
	}, coldRd.Read, func() uint64 {
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
	onSegmentCompacted := func(idx uint64, path string) error {
		if err := mft.OnSegmentCompacted(idx, path); err != nil {
			return err
		}
		coldRd.InvalidateSegment(idx)
		return nil
	}
	onCompactionPass := func(result orchestrator.CompactionPassResult) {
		// The compaction pass just evicted tombstones <= the new
		// watermark; rebuild the overlay so served W/M reflect it.
		overlayCache.Rebuild()
		if opts.OnCompactionPass != nil {
			opts.OnCompactionPass(CompactionPassResult{Watermark: result.Watermark, Err: result.Err})
		}
	}
	orch, err := orchestrator.New(orchestrator.Config{
		DataDir:        opts.DataDir,
		Store:          metaStore,
		RelayURL:       opts.RelayURL,
		HTTPClient:     xrpcClient.HTTPClient.Val(),
		Directory:      directory,
		Verifier:       verifier,
		SyncStateStore: stateStore,
		Tombstones:     tombstones,
		// Bare logger; orchestrator.New attaches component=orchestrator
		// itself, and its children (live, ingest, backfill) attach
		// their own component on top of the bare parent.
		Logger:                     processLogger,
		Metrics:                    orchestrator.NewMetrics(metrics.Registry, tombstones),
		IngestMetrics:              ingest.NewMetrics(metrics.Registry),
		LiveMetrics:                live.NewMetrics(metrics.Registry),
		BackfillMetrics:            backfillMetrics,
		SegmentMetrics:             segmentMetrics,
		OnEvent:                    onSteadyStateEvent,
		OnBootstrapLiveEvent:       opts.OnBootstrapLiveEvent,
		MaxBackfillRepos:           opts.MaxBackfillRepos,
		BackfillWorkers:            opts.effectiveBackfillWorkers(),
		BackfillBatchSize:          opts.effectiveBackfillBatchSize(),
		BackfillAsyncFlushWorkers:  opts.BackfillAsyncFlushWorkers,
		BackfillRepos:              opts.BackfillRepos,
		SkipMergeDiscovery:         opts.SkipMergeDiscovery,
		BackfillRetryBaseDelay:     opts.BackfillRetryBaseDelay,
		FailedRepoRetryInterval:    opts.FailedRepoRetryInterval,
		FailedRepoRetryWorkers:     opts.FailedRepoRetryWorkers,
		FailedRepoRetryHostWorkers: opts.FailedRepoRetryHostWorkers,
		FailedRepoRetryMaxDelay:    opts.FailedRepoRetryMaxDelay,
		LiveReconnectBackoff:       opts.LiveReconnectBackoff,
		LiveDial:                   opts.LiveDial,
		IngestOnAfterSeal:          mft.OnSegmentSealed,
		OnSegmentCompacted:         onSegmentCompacted,
		SegmentManifestChecksums:   mft.SegmentChecksums,
		CompactionInterval:         opts.CompactionInterval,
		CompactionTombstoneCap:     opts.CompactionTombstoneCap,
		CompactionRewriteWorkers:   opts.CompactionRewriteWorkers,
		OnCompactionPass:           onCompactionPass,
		OnBeforeCompactionPass:     opts.OnBeforeCompactionPass,
		BarrierBeforeCutover:       phaseBarrier(opts.BarrierBeforeCutover),
		BarrierAfterBootstrap:      phaseBarrier(opts.BarrierAfterBootstrap),
		BarrierAfterMerge:          phaseBarrier(opts.BarrierAfterMerge),
		AfterRepoComplete:          opts.AfterRepoComplete,
		CrashInjector:              opts.CrashInjector,
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
		PublicListener:  opts.PublicListener,
		DebugListener:   opts.DebugListener,
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
	srv.RegisterPublicRoute("GET /subscribe-v2", subscribe.NewHandler(subscribe.Subscription{
		Tail:                       tail,
		Store:                      metaStore,
		Manifest:                   mft,
		WriterRef:                  &writerPtr,
		Logger:                     processLogger,
		Metrics:                    subscribeMetrics,
		Lookback:                   opts.CursorLookback,
		EmitResyncReplacementRows:  true,
		FilterIdentityByCollection: true,
	}))

	// XRPC surface: whole-file segment download + listing. The atmos
	// xrpcserver routes /xrpc/{nsid}; mounting at the "/xrpc/" subtree
	// lets it own every jetstream NSID. Backed by the in-memory manifest,
	// which only tracks sealed (immutable) segments.
	xrpcMetrics := xrpcapi.NewMetrics(metrics.Registry)
	xrpcSrv := xrpcapi.New(xrpcapi.Config{
		Src:    mft,
		Logger: processLogger,
		Ready: func(ctx context.Context) error {
			if !lifecycle.IsSteadyState(metaStore) {
				return errors.New("bootstrap in progress")
			}
			if err := mft.Wait(ctx); err != nil {
				return fmt.Errorf("manifest warming up: %w", err)
			}
			return nil
		},
		CacheMaxAge: opts.SegmentCacheMaxAge,
		Overlay:     overlayCache,
		Plan: xrpcapi.PlanConfig{
			MaxDIDs:               opts.PlanMaxDIDs,
			MaxCollections:        opts.PlanMaxCollections,
			MaxEntries:            opts.PlanMaxEntries,
			WholeSegmentThreshold: opts.PlanWholeSegmentThreshold,
		},
		Metrics: xrpcMetrics,
		Tracer:  obs.Tracer("xrpcapi"),
	})
	srv.RegisterPublicRoute("/xrpc/", xrpcSrv.Handler())
	rt.server = srv

	return rt, nil
}

// PublicAddr returns the bound public listener address, or "" before Run binds.
func (r *Runtime) PublicAddr() string {
	if r == nil || r.server == nil {
		return ""
	}
	return r.server.PublicAddr()
}

// Run starts the constructed service graph and blocks until shutdown or a
// fatal subsystem error.
func (r *Runtime) Run(ctx context.Context) error {
	g, gctx := errgroup.WithContext(ctx)
	overlayInterval := r.opts.OverlayRebuildInterval
	if overlayInterval == 0 {
		overlayInterval = overlayRebuildInterval
	}

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

	if !r.opts.Headless {
		g.Go(func() error {
			return r.server.Run(gctx)
		})
	}

	g.Go(func() error {
		return r.orchestrator.Run(gctx)
	})

	g.Go(func() error {
		err := r.overlayCache.RunTicker(gctx, overlayInterval)
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
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
	// Note: promoted sync state is NOT flushed here. The consumer's own
	// Close flushes it after its writer has durably fsynced every
	// appended row; flushing from Runtime.Close would commit promoted
	// state even when the consumer's writer.Close failed, letting
	// verifier state run ahead of the archive. Pending (unpromoted)
	// entries are deliberately dropped — their events' rows were never
	// archived and redelivery re-verifies them.
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
