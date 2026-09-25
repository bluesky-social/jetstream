package jetstreamd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/gttp"
	"github.com/bluesky-social/jetstream/internal/catalog"
	localcatalog "github.com/bluesky-social/jetstream/internal/catalog/local"
	identcache "github.com/bluesky-social/jetstream/internal/identity"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/ingest/live"
	"github.com/bluesky-social/jetstream/internal/ingest/orchestrator"
	"github.com/bluesky-social/jetstream/internal/ingest/syncstate"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/metastore/pebblestore"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/internal/repoexport"
	"github.com/bluesky-social/jetstream/internal/server"
	"github.com/bluesky-social/jetstream/internal/status"
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
	"golang.org/x/sync/errgroup"
)

// Runtime is one fully constructed jetstream daemon instance.
type Runtime struct {
	opts Options

	processLogger *slog.Logger
	logger        *slog.Logger

	tracerShutdown obs.TracerShutdown
	cancelManifest context.CancelFunc
	metaStore      *pebblestore.Store
	manifest       *manifest.Manifest
	catalogLoad    *backgroundLoad
	tail           *subscribe.Tail
	verifier       *atmossync.Verifier
	orchestrator   *orchestrator.Orchestrator
	server         *server.Server

	runMu     sync.Mutex
	runCancel context.CancelFunc
	runDone   chan struct{}

	closeMu sync.Mutex
}

// Build constructs the production service graph without starting listeners or
// ingestion. Call Run to drive the graph, then Close during shutdown.
func Build(ctx context.Context, opts Options) (*Runtime, error) {
	if opts.CompactionInterval < 0 {
		return nil, fmt.Errorf("serve: --compaction-interval must be >= 0 (CompactionInterval must be >= 0), got %s", opts.CompactionInterval)
	}
	if opts.CompactionCacheGrace < 0 {
		return nil, fmt.Errorf("serve: --compaction-cache-grace must be >= 0 (CompactionCacheGrace must be >= 0), got %s", opts.CompactionCacheGrace)
	}
	if opts.CompactionTombstoneCap < 0 {
		return nil, fmt.Errorf("serve: --compaction-tombstone-cap must be >= 0 (CompactionTombstoneCap must be >= 0), got %d", opts.CompactionTombstoneCap)
	}
	for name, value := range map[string]int{"backfill-global-downloads": opts.BackfillGlobalDownloads, "backfill-host-workers-max": opts.BackfillHostWorkers, "backfill-max-active-hosts": opts.BackfillMaxActiveHosts, "backfill-max-hosts": opts.BackfillMaxHosts} {
		if value < 0 {
			return nil, fmt.Errorf("serve: --%s must be >= 0, got %d", name, value)
		}
	}
	if opts.BackfillBatchSize < 0 {
		return nil, fmt.Errorf("serve: --backfill-batch-size must be >= 0 (BackfillBatchSize must be >= 0), got %d", opts.BackfillBatchSize)
	}
	if opts.BackfillAsyncFlushWorkers < 0 {
		return nil, fmt.Errorf("serve: --backfill-async-flush-workers must be >= 0 (BackfillAsyncFlushWorkers must be >= 0), got %d", opts.BackfillAsyncFlushWorkers)
	}
	if opts.BootstrapLiveMaxSegmentBytes < 0 {
		return nil, fmt.Errorf("serve: BootstrapLiveMaxSegmentBytes must be >= 0, got %d", opts.BootstrapLiveMaxSegmentBytes)
	}
	if opts.BootstrapLiveMaxEventsPerBlock < 0 {
		return nil, fmt.Errorf("serve: BootstrapLiveMaxEventsPerBlock must be >= 0, got %d", opts.BootstrapLiveMaxEventsPerBlock)
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
	if opts.PlanMaxDIDs > xrpcapi.DefaultPlanMaxDIDs {
		return nil, fmt.Errorf("serve: --plan-max-dids must be <= %d (PlanMaxDIDs must be <= %d), got %d", xrpcapi.DefaultPlanMaxDIDs, xrpcapi.DefaultPlanMaxDIDs, opts.PlanMaxDIDs)
	}
	if opts.PlanMaxCollections < 0 {
		return nil, fmt.Errorf("serve: --plan-max-collections must be >= 0 (PlanMaxCollections must be >= 0), got %d", opts.PlanMaxCollections)
	}
	if opts.PlanMaxCollections > xrpcapi.DefaultPlanMaxCollections {
		return nil, fmt.Errorf("serve: --plan-max-collections must be <= %d (PlanMaxCollections must be <= %d), got %d", xrpcapi.DefaultPlanMaxCollections, xrpcapi.DefaultPlanMaxCollections, opts.PlanMaxCollections)
	}
	if opts.PlanMaxEntries < 0 {
		return nil, fmt.Errorf("serve: --plan-max-entries must be >= 0 (PlanMaxEntries must be >= 0), got %d", opts.PlanMaxEntries)
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
	storeMetrics := pebblestore.NewMetrics(metrics.Registry)
	segmentMetrics := obs.NewSegmentMetrics(metrics.Registry)
	verifierMetrics := obs.NewVerifierMetrics(metrics.Registry)
	subscribeMetrics := subscribe.NewMetrics(metrics.Registry)
	manifestMetrics := manifest.NewMetrics(metrics.Registry)
	liveMetrics := live.NewMetrics(metrics.Registry)

	if err := mkdirAllRuntimeFS(opts.StorageFS, opts.DataDir, 0o755); err != nil {
		return fail(fmt.Errorf("serve: create data dir %s: %w", opts.DataDir, err))
	}
	obs.RegisterDataDirFreeBytes(metrics.Registry, opts.DataDir)

	segmentsDir := filepath.Join(opts.DataDir, "segments")
	if err := mkdirAllRuntimeFS(opts.StorageFS, segmentsDir, 0o755); err != nil {
		return fail(fmt.Errorf("serve: create segments dir %s: %w", segmentsDir, err))
	}

	metaStore, err := pebblestore.Open(opts.DataDir, storeMetrics, pebblestore.WithFS(opts.StorageFS))
	if err != nil {
		return fail(err)
	}
	rt.metaStore = metaStore
	var metaKV metastore.Store = metaStore
	if opts.StoreFaultInjector != nil {
		metaKV = metastore.WithFaults(metaStore, opts.StoreFaultInjector)
	}

	manifestCtx, cancelManifest := context.WithCancel(ctx)
	rt.cancelManifest = cancelManifest
	mft, err := manifest.OpenBackground(manifestCtx, manifest.Options{
		SegmentsDir:         segmentsDir,
		FS:                  opts.StorageFS,
		BlockIndexCacheSize: opts.CursorBlockIndexCacheSize,
		Logger:              processLogger,
		Metrics:             manifestMetrics,
	})
	if err != nil {
		return fail(fmt.Errorf("serve: open manifest: %w", err))
	}
	rt.manifest = mft

	// The local catalog hears about every seal from the ingest writers and
	// feeds main-namespace seals to the manifest.
	segCatalog, err := localcatalog.New(localcatalog.Config{
		FS: opts.StorageFS,
		Dirs: map[catalog.Namespace]string{
			catalog.Main:          segmentsDir,
			catalog.BootstrapLive: filepath.Join(opts.DataDir, "backfill", "live_segments"),
		},
		// Merge cleanup removes the whole backfill tree.
		Roots: map[catalog.Namespace]string{catalog.BootstrapLive: filepath.Join(opts.DataDir, "backfill")},
	})
	if err != nil {
		return fail(fmt.Errorf("serve: build segment catalog: %w", err))
	}
	segCatalog.OnSealed(catalog.Main, func(v catalog.SegmentView, path string) error {
		return manifest.ApplySegmentFile(mft, opts.StorageFS, v.Index, path)
	})
	// Like the manifest, the catalog loads what is on disk in the
	// background. Writers may publish seals meanwhile; Refresh keeps them.
	// Readers wait on catalogLoad before their first view.
	catalogLoad := startBackgroundLoad(manifestCtx, segCatalog.Refresh)
	rt.catalogLoad = catalogLoad

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
	// gttp client through it instead of a real socket (deterministic harness).
	var transportOpt []gttp.Option
	if opts.HTTPTransport != nil {
		transportOpt = []gttp.Option{gttp.WithTransport(opts.HTTPTransport)}
	}

	backfillMetrics := backfill.NewMetrics(metrics.Registry)
	xrpcClient := &xrpc.Client{
		Host:       relayHTTPURL,
		HTTPClient: gt.Some(gttp.New(append(xrpc.BulkDownloadOpts(), transportOpt...)...)),
	}

	resolver := newIdentityResolver(opts)
	directory := &identity.Directory{
		Resolver:               resolver,
		Cache:                  identcache.New(identcache.NewPebbleKV(metaStore), identcache.DefaultTTL),
		SkipHandleVerification: true,
	}

	stateStore := syncstate.New(metaKV)
	tombstones := tombstone.New()
	// This state is owned and updated by the orchestrator, but xrpcapi sees
	// only its read-only deadline surface. It starts unknown, so archive
	// responses remain no-cache until steady-state scheduling is live.
	compactionSchedule := orchestrator.NewCompactionScheduleState()
	syncClient := atmossync.NewClient(atmossync.Options{Client: xrpcClient})

	coldRd := subscribe.NewColdReader(subscribe.ColdReaderConfig{
		Catalog:         segCatalog,
		Fetcher:         segCatalog.Fetcher(),
		Ready:           catalogLoad.Wait,
		WriterRef:       &writerPtr,
		BlockCacheBytes: opts.SubscribeBlockCacheBytes,
		Metrics:         subscribeMetrics,
	})
	tail, err := subscribe.New(subscribe.Config{
		Logger:      processLogger,
		Metrics:     subscribeMetrics,
		ReadBatch:   opts.SubscribeReadBatch,
		SlowWindow:  opts.SubscribeSlowWindow,
		SlowMinRate: opts.SubscribeSlowMinRate,
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
	//
	// The /subscribe tail reads the steady writer's readable log (wired in
	// OnSteadyStateWriter below), not the live consumer's OnEvent hook: every
	// producer sharing the steady writer — the live consumer AND the failed-repo
	// retry runner — must become visible through the writer-owned seq stream.
	// OnEvent remains the live-consumer-only observation hook for tests/oracle.
	onSteadyStateEvent := func(ev *segment.Event) {
		if opts.OnSteadyStateEvent != nil {
			opts.OnSteadyStateEvent(ev)
		}
	}
	onSegmentCompacted := func(idx uint64, path string) error {
		if err := manifest.ApplySegmentFile(mft, opts.StorageFS, idx, path); err != nil {
			return err
		}
		if err := segCatalog.Reload(catalog.Main, idx); err != nil {
			return err
		}
		coldRd.InvalidateSegment(idx)
		return nil
	}
	onCompactionPass := func(result orchestrator.CompactionPassResult) {
		if opts.OnCompactionPass != nil {
			opts.OnCompactionPass(CompactionPassResult{Watermark: result.Watermark, Err: result.Err})
		}
	}
	var backfillNewHostClient func(string) (*atmossync.Client, error)
	if opts.HTTPTransport != nil {
		backfillNewHostClient = func(hostname string) (*atmossync.Client, error) {
			xc := &xrpc.Client{Host: "http://" + hostname, HTTPClient: xrpcClient.HTTPClient, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
			return atmossync.NewClient(atmossync.Options{Client: xc}), nil
		}
	}
	orch, err := orchestrator.New(orchestrator.Config{
		DataDir:        opts.DataDir,
		FS:             opts.StorageFS,
		Store:          metaKV,
		RelayURL:       opts.RelayURL,
		HTTPClient:     xrpcClient.HTTPClient.Val(),
		Directory:      directory,
		Verifier:       verifier,
		SyncStateStore: stateStore,
		Tombstones:     tombstones,
		// Bare logger; orchestrator.New attaches component=orchestrator
		// itself, and its children (live, ingest, backfill) attach
		// their own component on top of the bare parent.
		Logger:                         processLogger,
		Metrics:                        orchestrator.NewMetrics(metrics.Registry, tombstones),
		IngestMetrics:                  ingest.NewMetrics(metrics.Registry),
		LiveMetrics:                    liveMetrics,
		DropMetrics:                    ingest.NewDropMetrics(metrics.Registry),
		BackfillMetrics:                backfillMetrics,
		SegmentMetrics:                 segmentMetrics,
		OnEvent:                        onSteadyStateEvent,
		OnBootstrapLiveEvent:           opts.OnBootstrapLiveEvent,
		MaxBackfillRepos:               opts.MaxBackfillRepos,
		BackfillGlobalDownloads:        opts.effectiveBackfillGlobalDownloads(),
		BackfillHostWorkers:            opts.effectiveBackfillHostWorkers(),
		BackfillMaxActiveHosts:         opts.effectiveBackfillMaxActiveHosts(),
		BackfillMaxHosts:               opts.effectiveBackfillMaxHosts(),
		BackfillWorkers:                opts.BackfillWorkers,
		BackfillNewHostClient:          backfillNewHostClient,
		BackfillBatchSize:              opts.effectiveBackfillBatchSize(),
		BackfillAsyncFlushWorkers:      opts.BackfillAsyncFlushWorkers,
		ReadLogRetentionBytes:          int64(opts.effectiveSubscribeReadLogRetentionBytes()),
		BootstrapLiveMaxSegmentBytes:   opts.BootstrapLiveMaxSegmentBytes,
		BootstrapLiveMaxEventsPerBlock: opts.BootstrapLiveMaxEventsPerBlock,
		BackfillRepos:                  opts.BackfillRepos,
		SkipMergeDiscovery:             opts.SkipMergeDiscovery,
		BackfillRetryBaseDelay:         opts.BackfillRetryBaseDelay,
		FailedRepoRetryInterval:        opts.FailedRepoRetryInterval,
		FailedRepoRetryWorkers:         opts.FailedRepoRetryWorkers,
		FailedRepoRetryHostWorkers:     opts.FailedRepoRetryHostWorkers,
		FailedRepoRetryMaxDelay:        opts.FailedRepoRetryMaxDelay,
		LiveReconnectBackoff:           opts.LiveReconnectBackoff,
		LiveDial:                       opts.LiveDial,
		Catalog:                        segCatalog,
		OnSegmentCompacted:             onSegmentCompacted,
		SegmentManifestChecksums:       mft.SegmentChecksums,
		CompactionInterval:             opts.CompactionInterval,
		CompactionSchedule:             compactionSchedule,
		CompactionTombstoneCap:         opts.CompactionTombstoneCap,
		CompactionRewriteWorkers:       opts.CompactionRewriteWorkers,
		OnCompactionPass:               onCompactionPass,
		OnBeforeCompactionPass:         opts.OnBeforeCompactionPass,
		BarrierBeforeCutover:           phaseBarrier(opts.BarrierBeforeCutover),
		BarrierAfterBootstrap:          phaseBarrier(opts.BarrierAfterBootstrap),
		BarrierAfterMerge:              phaseBarrier(opts.BarrierAfterMerge),
		AfterRepoComplete:              opts.AfterRepoComplete,
		CrashInjector:                  opts.CrashInjector,
		SegmentIOFaultInjector:         opts.SegmentIOFaultInjector,
		OnSteadyStateWriter: func(w *ingest.Writer) {
			// Fires after the steady writer opens and before any producer
			// (live consumer, retry runner, compactor) starts, so subscribers
			// read the writer-owned log from its first event.
			tail.SetReadLogSource(func() *ingest.ReadableLog { return w.ReadLog() })
			writerPtr.Store(w)
		},
	})
	if err != nil {
		return fail(fmt.Errorf("serve: build orchestrator: %w", err))
	}
	rt.orchestrator = orch

	// Status and repo verification read segments through the catalog, so
	// they wait for its background load like the cold reader does.
	statusCollector, err := status.New(status.Options{
		Store:                 metaKV,
		DataDir:               opts.DataDir,
		Archive:               segCatalog,
		ArchiveReady:          catalogLoad.Wait,
		Manifest:              mft,
		CursorLookback:        opts.CursorLookback,
		IdentityResolver:      resolver,
		LastSeenUpstreamEvent: liveMetrics.LastSeenUpstreamEvent,
		Writer: func() *ingest.Writer {
			return writerPtr.Load()
		},
	})
	if err != nil {
		return fail(fmt.Errorf("serve: build status collector: %w", err))
	}
	repoArchive := repoexport.Archive{
		Catalog:  segCatalog,
		Fetcher:  segCatalog.Fetcher(),
		Selector: repoexport.FooterSelector{Source: segCatalog, Primary: newManifestSelector(mft)},
		Ready:    catalogLoad.Wait,
	}
	statusHandler, err := web.New(web.Options{
		Snapshotter:                statusCollector,
		RepoActions:                web.NewRepoActions(repoArchive, resolver, pendingEventsForDID(&writerPtr)),
		DisableRepoActionRateLimit: opts.DisableRepoActionRateLimits,
		Logger:                     processLogger,
	})
	if err != nil {
		return fail(fmt.Errorf("serve: build status handler: %w", err))
	}

	srv := server.New(server.Config{
		PublicAddr:      opts.PublicAddr,
		DebugAddr:       opts.DebugAddr,
		ShutdownTimeout: opts.ShutdownTimeout,
		StatusHandler:   statusHandler,
		PublicListener:  opts.PublicListener,
		DebugListener:   opts.DebugListener,
	}, processLogger, metrics)

	// HandlerDeps.WriterRef is read at request time via writerPtr.Load();
	// before steady-state, the phase readiness gate answers 503 so
	// nil-pointer reads are harmless.
	phaseReady := lifecycle.SteadyState(metaKV)
	srv.RegisterPublicRoute("GET /subscribe", subscribe.NewHandler(subscribe.Subscription{
		Tail:      tail,
		Ready:     phaseReady,
		Manifest:  mft,
		Catalog:   segCatalog,
		Fetcher:   segCatalog.Fetcher(),
		WriterRef: &writerPtr,
		Logger:    processLogger,
		Metrics:   subscribeMetrics,
		Lookback:  opts.CursorLookback,
	}))
	// The v2 stream serves at its lexicon-canonical XRPC path (proposal
	// 0015; #318). The Go 1.22 mux prefers this exact pattern over the
	// "/xrpc/" xrpcserver subtree registered below, so the bespoke
	// handler owns this one NSID while atmos xrpcserver keeps the rest.
	srv.RegisterPublicRoute("GET /xrpc/network.bsky.jetstream.subscribeEvents", subscribe.NewHandler(subscribe.Subscription{
		Tail:      tail,
		Ready:     phaseReady,
		Manifest:  mft,
		Catalog:   segCatalog,
		Fetcher:   segCatalog.Fetcher(),
		WriterRef: &writerPtr,
		Logger:    processLogger,
		Metrics:   subscribeMetrics,
		Lookback:  opts.CursorLookback,
		V2:        true,
	}))

	// XRPC surface: whole-file segment download + listing. The atmos
	// xrpcserver routes /xrpc/{nsid}; mounting at the "/xrpc/" subtree
	// lets it own every jetstream NSID. Backed by the in-memory manifest,
	// which only tracks sealed (immutable) segments.
	xrpcMetrics := xrpcapi.NewMetrics(metrics.Registry)
	xrpcSrv := xrpcapi.New(xrpcapi.Config{
		Src:    mft,
		Logger: processLogger,
		Ready: lifecycle.AllReady(phaseReady, lifecycle.ReadinessFunc(func(ctx context.Context) error {
			if err := mft.Wait(ctx); err != nil {
				return fmt.Errorf("manifest warming up: %w", err)
			}
			return nil
		})),
		CompactionCacheGrace: opts.CompactionCacheGrace,
		CompactionDeadline:   compactionSchedule,
		Plan: xrpcapi.PlanConfig{
			MaxDIDs:               opts.PlanMaxDIDs,
			MaxCollections:        opts.PlanMaxCollections,
			MaxEntries:            opts.PlanMaxEntries,
			WholeSegmentThreshold: opts.PlanWholeSegmentThreshold,
		},
		Metrics: xrpcMetrics,
		Tracer:  obs.Tracer("xrpcapi"),
		Dictionary: xrpcapi.DictionaryConfig{
			ID:    subscribe.DictionaryV2ID,
			Bytes: subscribe.DictionaryV2(),
		},
	})
	// Atmos v0.3.7 registers queries as GET-only. Keep HEAD narrowly scoped to
	// the two existing archive query URLs; the exact public-mux routes win over
	// the generic XRPC subtree without changing websocket or other XRPC methods.
	srv.RegisterPublicRoute("HEAD /xrpc/network.bsky.jetstream.getSegment", xrpcSrv.HeadHandler("network.bsky.jetstream.getSegment"))
	srv.RegisterPublicRoute("HEAD /xrpc/network.bsky.jetstream.getBlock", xrpcSrv.HeadHandler("network.bsky.jetstream.getBlock"))
	srv.RegisterPublicRoute("/xrpc/", xrpcSrv.Handler())
	rt.server = srv

	return rt, nil
}

func newIdentityResolver(opts Options) *identity.DefaultResolver {
	plcOpts := append(xrpc.ATProtoOpts(10*time.Second), gttp.WithNoRedirects())
	webOpts := xrpc.ATProtoOpts(10 * time.Second)
	if opts.HTTPTransport != nil {
		// HTTPTransport is a deterministic, socket-free test seam. DNS
		// preflight would escape that seam and make simulator-only hostnames
		// depend on the machine resolver, so the injected transport owns policy
		// enforcement in this mode.
		plcOpts = append(plcOpts, gttp.WithTransport(opts.HTTPTransport))
		webOpts = append(webOpts, gttp.WithTransport(opts.HTTPTransport))
	} else {
		webOpts = append(webOpts, gttp.WithStrictSSRFProtection(), gttp.WithNoProxy())
	}

	resolver := &identity.DefaultResolver{
		HTTPClient:    gt.Some(gttp.New(webOpts...)),
		PLCHTTPClient: gt.Some(gttp.New(plcOpts...)),
	}
	if opts.PLCURL != "" {
		resolver.PLCURL = gt.Some(opts.PLCURL)
	}
	return resolver
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
func (r *Runtime) Run(ctx context.Context) (runErr error) {
	runCtx, cancelRun := context.WithCancel(ctx)
	runDone := make(chan struct{})
	r.runMu.Lock()
	if r.runDone != nil {
		r.runMu.Unlock()
		cancelRun()
		return errors.New("runtime: Run called more than once")
	}
	r.runCancel = cancelRun
	r.runDone = runDone
	r.runMu.Unlock()
	defer func() {
		cancelRun()
		r.runMu.Lock()
		r.runCancel = nil
		close(runDone)
		r.runMu.Unlock()
	}()

	g, gctx := errgroup.WithContext(runCtx)

	g.Go(r.goroutineRoot("manifest_cancel", func() error {
		<-gctx.Done()
		r.cancelManifestLoad()
		return nil
	}))

	g.Go(r.goroutineRoot("manifest_wait", func() error {
		if err := r.manifest.Wait(gctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("manifest load: %w", err)
		}
		return nil
	}))

	g.Go(r.goroutineRoot("catalog_wait", func() error {
		if err := r.catalogLoad.Wait(gctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("catalog load: %w", err)
		}
		return nil
	}))

	if !r.opts.Headless {
		g.Go(r.goroutineRoot("http_server", func() error {
			return r.server.Run(gctx)
		}))
	}

	g.Go(r.goroutineRoot("orchestrator", func() error {
		return r.orchestrator.Run(gctx)
	}))

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
	g.Go(r.goroutineRoot("client_drain", func() error {
		<-gctx.Done()
		drainCtx, drainCancel := context.WithTimeout(context.Background(), r.opts.ClientDrainTimeout)
		defer drainCancel()
		if err := r.tail.Shutdown(drainCtx); err != nil {
			r.logger.Warn("client drain did not complete within budget; severing remaining subscribers", "err", err)
		}
		return nil
	}))

	verifierLogger := r.processLogger.With(slog.String("component", "verifier"))
	// Verifier async-error drain. Verification failures are
	// diagnostic, not fatal -- they typically reflect adversarial or
	// malformed PDS input, which is invalid user data, not a
	// jetstream bug. We warn-log and the OnVerificationFailure hook
	// fires for operator visibility, but never crash.
	g.Go(r.goroutineRoot("verifier_async_errors", func() error {
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
	}))

	// Graceful shutdown surfaces as context.Canceled from the errgroup: the
	// orchestrator's steady-state consumer and the HTTP server both return
	// ctx.Err(). Suppress cancellation only when it came from the runtime's run
	// context, either caller cancellation or Runtime.Close.
	runErr = g.Wait()
	if errors.Is(runErr, context.Canceled) && runCtx.Err() != nil {
		runErr = nil
	}
	return runErr
}

func (r *Runtime) goroutineRoot(name string, fn func() error) func() error {
	return func() error {
		defer func() {
			if rec := recover(); rec != nil {
				info := version.Get()
				logger := slog.Default()
				if r != nil && r.logger != nil {
					logger = r.logger
				}
				logger.Error("panic in runtime goroutine",
					"goroutine", name,
					"panic", fmt.Sprint(rec),
					"panic_type", fmt.Sprintf("%T", rec),
					"stack", string(debug.Stack()),
					"go_version", runtime.Version(),
					"version", info.Version,
					"commit", info.Commit,
					"built", info.Date,
				)
				panic(rec)
			}
		}()
		return fn()
	}
}

// Close tears down resources owned by the runtime. It cancels and drains Run
// before closing shared stores; repeated calls are ignored for already-closed
// fields.
func (r *Runtime) Close(ctx context.Context) error {
	r.cancelManifestLoad()

	var errs []error

	runDrained := true
	if err := r.stopRun(ctx); err != nil {
		runDrained = false
		r.logger.Error("runtime run did not drain within budget; leaving shared stores open", "err", err)
		errs = append(errs, fmt.Errorf("runtime run drain: %w", err))
	}

	r.closeMu.Lock()
	defer r.closeMu.Unlock()

	if r.verifier != nil && runDrained {
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
	if r.metaStore != nil && runDrained {
		if err := r.metaStore.Close(); err != nil {
			r.logger.Error("close metadata store", "err", err)
			errs = append(errs, fmt.Errorf("close metadata store: %w", err))
		}
		r.metaStore = nil
	}
	if r.tracerShutdown != nil && runDrained {
		if err := r.tracerShutdown(ctx); err != nil {
			r.logger.Error("tracer shutdown failed", "err", err)
			errs = append(errs, fmt.Errorf("tracer shutdown: %w", err))
		}
		r.tracerShutdown = nil
	}
	return errors.Join(errs...)
}

func (r *Runtime) stopRun(ctx context.Context) error {
	r.runMu.Lock()
	cancel := r.runCancel
	done := r.runDone
	r.runMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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

// backgroundLoad is a startup load run off the critical path, such as the
// catalog's initial directory scan. Wait is the readiness gate.
type backgroundLoad struct {
	done chan struct{}
	err  error
}

func startBackgroundLoad(ctx context.Context, load func(context.Context) error) *backgroundLoad {
	l := &backgroundLoad{done: make(chan struct{})}
	go func() {
		defer close(l.done)
		l.err = load(ctx)
	}()
	return l
}

// Wait blocks until the load finishes and returns its error, or ctx.Err()
// if the caller stops waiting first.
func (l *backgroundLoad) Wait(ctx context.Context) error {
	select {
	case <-l.done:
		return l.err
	case <-ctx.Done():
		return ctx.Err()
	}
}
