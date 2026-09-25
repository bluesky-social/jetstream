package jetstreamd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"runtime/debug"
	"strings"
	"time"

	"github.com/bluesky-social/gttp"
	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/catalog/follower"
	identcache "github.com/bluesky-social/jetstream/internal/identity"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/ingest/live"
	"github.com/bluesky-social/jetstream/internal/ingest/maintainer"
	"github.com/bluesky-social/jetstream/internal/ingest/orchestrator"
	"github.com/bluesky-social/jetstream/internal/leader"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/internal/metastore"
	metapg "github.com/bluesky-social/jetstream/internal/metastore/pg"
	"github.com/bluesky-social/jetstream/internal/objstore"
	"github.com/bluesky-social/jetstream/internal/objstore/objcache"
	"github.com/bluesky-social/jetstream/internal/objstore/protocol"
	"github.com/bluesky-social/jetstream/internal/objstore/s3"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/internal/pgstore"
	"github.com/bluesky-social/jetstream/internal/repoexport"
	"github.com/bluesky-social/jetstream/internal/server"
	"github.com/bluesky-social/jetstream/internal/status"
	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/bluesky-social/jetstream/internal/web"
	"github.com/bluesky-social/jetstream/internal/xrpcapi"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/identity"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"golang.org/x/sync/errgroup"
)

// StorageBackend is the shared storage a disaggregated pod runs on: the
// catalog database, its NOTIFY channel, the object store, and the writer
// lease. Build opens PostgreSQL and S3 from Options.Storage when
// Options.StorageBackend is nil; tests pass storagefake and memblob.
type StorageBackend struct {
	DB catalog.DB
	// Listener delivers catalog NOTIFY revisions. Nil means followers only
	// poll.
	Listener catalog.Listener
	Blob     objstore.Blob
	// Archive checks the schema and format versions and returns the
	// archive row.
	Archive func(ctx context.Context) (catalog.ArchiveRow, error)
	// NewLease returns this pod's writer lease over the archive row.
	NewLease func() leader.Locker
	// MetaStore returns the metadata store over DB. Writes go through
	// commit; a nil commit gives a read-only store.
	MetaStore func(commit func(ctx context.Context, ops []metastore.Op) error) metastore.Store
	// Now is the clock the follower ages its view by. Nil means time.Now.
	Now func() time.Time
	// Close releases the backend after the runtime stops. May be nil.
	Close func()
}

// identityCacheEntries bounds the pod-local identity LRU. Disaggregated
// pods have no disk for local mode's persistent identity cache (plan
// finding 8), so a restarted pod re-resolves; the bound keeps the cache
// small beside the §17 budgets.
const identityCacheEntries = 100_000

// memoryHeadroom is the share of GOMEMLIMIT the budgets may claim (design
// §17). The rest covers everything unbudgeted: request buffers, decoded
// events in flight, and the Go runtime.
const memoryHeadroom = 0.75

// disaggregated is the per-process state only disaggregated mode has.
type disaggregated struct {
	backend  *StorageBackend
	follower *follower.Follower
	uploader *protocol.Uploader
	// objects reads objects for the maintainer's seals straight from the
	// catalog, not through the follower's mirror, which may lag the
	// leader's own commits.
	objects        *protocol.Reader
	cache          *objcache.Cache
	catalogMetrics *catalog.Metrics
	maintMetrics   *maintainer.Metrics
}

type memoryBudget struct {
	name  string
	bytes int64
}

// memoryBudgets lists the configurable budgets design §17 sums at start.
func (o Options) memoryBudgets() []memoryBudget {
	blockCache := int64(o.SubscribeBlockCacheBytes)
	if blockCache <= 0 {
		blockCache = subscribe.DefaultBlockCacheBytes
	}
	return []memoryBudget{
		{"JETSTREAM_SUBSCRIBE_READ_LOG_RETENTION_BYTES", int64(o.effectiveSubscribeReadLogRetentionBytes())},
		{"JETSTREAM_SUBSCRIBE_BLOCK_CACHE_BYTES", blockCache},
		{"JETSTREAM_OBJECT_CACHE_BYTES", o.Storage.ObjectCacheBytes},
		{"JETSTREAM_HOT_PENDING_BYTES", o.Storage.Hot.PendingBytes},
		{"JETSTREAM_COMPACTION_MEMORY_BYTES", o.Storage.CompactionMemoryBytes},
	}
}

// memoryLimit is GOMEMLIMIT, or Options.MemoryLimit when a test sets it.
// math.MaxInt64 is the runtime's "no limit".
func (o Options) memoryLimit() int64 {
	if o.MemoryLimit > 0 {
		return o.MemoryLimit
	}
	return debug.SetMemoryLimit(-1)
}

// checkMemoryBudgets refuses a pod whose budgets exceed memoryHeadroom of
// the memory limit, listing each one so the operator sees what to shrink.
func checkMemoryBudgets(limit int64, budgets []memoryBudget) error {
	if limit <= 0 || limit == math.MaxInt64 {
		return errors.New("serve: GOMEMLIMIT must be set when JETSTREAM_STORAGE=disaggregated (design §17)")
	}
	var total int64
	parts := make([]string, 0, len(budgets))
	for _, b := range budgets {
		total += b.bytes
		parts = append(parts, fmt.Sprintf("%s=%d", b.name, b.bytes))
	}
	if allowed := int64(float64(limit) * memoryHeadroom); total > allowed {
		return fmt.Errorf("serve: memory budgets total %d bytes, over %.0f%% of GOMEMLIMIT (%d of %d bytes): %s",
			total, memoryHeadroom*100, allowed, limit, strings.Join(parts, " "))
	}
	return nil
}

// openBackend connects to the PostgreSQL and S3 that cfg names.
func openBackend(ctx context.Context, cfg StorageConfig, pgMetrics *pgstore.Metrics, s3Metrics *s3.Metrics) (*StorageBackend, error) {
	pg, err := pgstore.Open(ctx, pgstore.Config{URL: cfg.PG.URL, MaxConns: int32(cfg.PG.MaxConns), Metrics: pgMetrics})
	if err != nil {
		return nil, fmt.Errorf("serve: connect to PostgreSQL: %w", err)
	}
	blob, err := s3.New(ctx, s3.Config{
		Endpoint:          cfg.S3.Endpoint,
		Region:            cfg.S3.Region,
		Bucket:            cfg.S3.Bucket,
		Prefix:            cfg.S3.Prefix,
		PathStyle:         cfg.S3.PathStyle,
		UploadConcurrency: cfg.S3.UploadConcurrency,
		ReadConcurrency:   cfg.S3.ReadConcurrency,
		RetryTimeout:      cfg.S3.RetryTimeout,
		Metrics:           s3Metrics,
	})
	if err != nil {
		pg.Close()
		return nil, fmt.Errorf("serve: open S3: %w", err)
	}
	return &StorageBackend{
		DB:       pg,
		Listener: pg,
		Blob:     blob,
		Archive:  pg.CheckVersions,
		NewLease: func() leader.Locker { return pg.NewLease() },
		MetaStore: func(commit func(context.Context, []metastore.Op) error) metastore.Store {
			return metapg.New(metapg.Config{DB: pg, Commit: commit})
		},
		Close: pg.Close,
	}, nil
}

// buildDisaggregated is Build for JETSTREAM_STORAGE=disaggregated, in the
// design §15.2 start order: budgets, schema check, first catalog load and
// footers, budgets again with the manifest measured. HTTP and the election
// loop start in Run.
func buildDisaggregated(ctx context.Context, opts Options, processLogger, logger *slog.Logger) (*Runtime, error) {
	st := opts.Storage
	// Logged whole, redacted, so the operator sees what the pod will use
	// even when a check below refuses it.
	logger.Info("storage config", "storage", st)

	limit := opts.memoryLimit()
	budgets := opts.memoryBudgets()
	if err := checkMemoryBudgets(limit, budgets); err != nil {
		return nil, err
	}
	if err := xrpcapi.CheckGCDelay(st.GC.Delay, st.MaxViewAge, st.MaxArchiveResponseDuration); err != nil {
		return nil, fmt.Errorf("serve: JETSTREAM_GC_DELAY: %w", err)
	}

	rt := &Runtime{
		opts:          opts,
		processLogger: processLogger,
		logger:        logger,
		deadline:      &compactionDeadline{},
		disagg:        &disaggregated{},
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

	tracerShutdown, err := obs.SetupTracing(ctx, obs.TracingConfig{ServiceName: opts.OTelServiceName})
	if err != nil {
		return nil, fmt.Errorf("setup tracing: %w", err)
	}
	rt.tracerShutdown = tracerShutdown

	metrics := obs.NewMetrics()
	reg := metrics.Registry
	segmentMetrics := obs.NewSegmentMetrics(reg)
	verifierMetrics := obs.NewVerifierMetrics(reg)
	subscribeMetrics := subscribe.NewMetrics(reg)
	liveMetrics := live.NewMetrics(reg)
	ingestMetrics := ingest.NewMetrics(reg)
	memoryMetrics := obs.NewMemoryMetrics(reg)
	catalogMetrics := catalog.NewMetrics(reg)
	protocolMetrics := protocol.NewMetrics(reg)
	pgMetrics := pgstore.NewMetrics(reg)
	s3Metrics := s3.NewMetrics(reg)
	rt.disagg.catalogMetrics = catalogMetrics
	rt.disagg.maintMetrics = maintainer.NewMetrics(reg)
	for _, b := range budgets {
		limitGauge, _ := memoryMetrics.Gauges(b.name)
		limitGauge.Set(float64(b.bytes))
	}

	backend := opts.StorageBackend
	if backend == nil {
		if backend, err = openBackend(ctx, st, pgMetrics, s3Metrics); err != nil {
			return fail(err)
		}
	}
	rt.disagg.backend = backend
	archive, err := backend.Archive(ctx)
	if err != nil {
		return fail(fmt.Errorf("serve: check catalog: %w", err))
	}
	logger.Info("catalog", "archive_id", objstore.FormatUUID(archive.ArchiveID), "schema_version", archive.SchemaVersion, "format_version", archive.FormatVersion)
	// Every pod reads objects, and any pod may become leader and write
	// them, so each checks all three permissions before its first read can
	// mistake a denied GET for corruption.
	if err := objstore.Probe(ctx, backend.Blob, archive.ArchiveID); err != nil {
		return fail(fmt.Errorf("serve: object store canary: %w", err))
	}

	cache := objcache.New(objcache.Config{MaxBytes: st.ObjectCacheBytes, Memory: memoryMetrics})
	rt.disagg.cache = cache
	uploader, err := protocol.NewUploader(protocol.UploaderConfig{
		Blob:        backend.Blob,
		ArchiveID:   archive.ArchiveID,
		GCDelay:     st.GC.Delay,
		OrphanAge:   st.GC.OrphanAge,
		Concurrency: st.S3.UploadConcurrency,
		Metrics:     protocolMetrics,
	})
	if err != nil {
		return fail(fmt.Errorf("serve: build uploader: %w", err))
	}
	rt.disagg.uploader = uploader
	objects, err := protocol.NewReader(protocol.ReaderConfig{
		Rows:           protocol.DBRows{DB: backend.DB},
		Blob:           backend.Blob,
		ArchiveID:      archive.ArchiveID,
		Cache:          cache,
		Metrics:        protocolMetrics,
		CatalogMetrics: catalogMetrics,
	})
	if err != nil {
		return fail(fmt.Errorf("serve: build object reader: %w", err))
	}
	rt.disagg.objects = objects

	metaKV := backend.MetaStore(nil)
	mft, err := manifest.NewRemote(manifest.Options{
		BlockIndexCacheSize: opts.CursorBlockIndexCacheSize,
		Logger:              processLogger,
		Metrics:             manifest.NewMetrics(reg),
	})
	if err != nil {
		return fail(fmt.Errorf("serve: build manifest: %w", err))
	}
	rt.manifest = mft
	f, err := follower.New(follower.Config{
		DB:              backend.DB,
		Listener:        backend.Listener,
		Blob:            backend.Blob,
		ArchiveID:       archive.ArchiveID,
		Cache:           cache,
		Manifest:        mft,
		PollInterval:    st.CatalogPollInterval,
		MaxViewAge:      st.MaxViewAge,
		ReadConcurrency: st.S3.ReadConcurrency,
		ReadLogBytes:    int64(opts.effectiveSubscribeReadLogRetentionBytes()),
		Logger:          processLogger,
		Metrics:         follower.NewMetrics(reg),
		CatalogMetrics:  catalogMetrics,
		ProtocolMetrics: protocolMetrics,
		IngestMetrics:   ingestMetrics,
		Now:             backend.Now,
	})
	if err != nil {
		return fail(fmt.Errorf("serve: build catalog follower: %w", err))
	}
	rt.disagg.follower = f

	// First full load (design §15.2 step 3). In steady state it loads every
	// footer into the manifest, so the budgets can be checked again with
	// the manifest's real size.
	if err := f.Refresh(ctx); err != nil {
		return fail(fmt.Errorf("serve: first catalog load: %w", err))
	}
	if err := f.Ready(ctx); err == nil {
		resident := mft.ResidentBytes()
		limitGauge, used := memoryMetrics.Gauges("manifest")
		limitGauge.Set(float64(resident))
		used.Set(float64(resident))
		if err := checkMemoryBudgets(limit, append(budgets, memoryBudget{"manifest and footers (measured)", resident})); err != nil {
			return fail(err)
		}
	} else {
		// Footers load on the first steady-state tick, which bootstrap and
		// merge (stage 3) must precede; until then there is nothing to
		// measure.
		logger.Warn("catalog not ready after first load; manifest not measured", "err", err)
	}

	relayHTTPURL, err := live.DeriveRelayHTTPURL(opts.RelayURL)
	if err != nil {
		return fail(fmt.Errorf("serve: derive relay HTTP URL: %w", err))
	}
	var transportOpt []gttp.Option
	if opts.HTTPTransport != nil {
		transportOpt = []gttp.Option{gttp.WithTransport(opts.HTTPTransport)}
	}
	xrpcClient := &xrpc.Client{
		Host:       relayHTTPURL,
		HTTPClient: gt.Some(gttp.New(append(xrpc.BulkDownloadOpts(), transportOpt...)...)),
	}
	resolver := newIdentityResolver(opts)
	directory := &identity.Directory{
		Resolver:               resolver,
		Cache:                  identity.NewLRUCache(identityCacheEntries, identcache.DefaultTTL),
		SkipHandleVerification: true,
	}
	var backfillNewHostClient func(string) (*atmossync.Client, error)
	if opts.HTTPTransport != nil {
		backfillNewHostClient = func(hostname string) (*atmossync.Client, error) {
			xc := &xrpc.Client{Host: "http://" + hostname, HTTPClient: xrpcClient.HTTPClient, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
			return atmossync.NewClient(atmossync.Options{Client: xc}), nil
		}
	}

	// Readers serve the follower's mirror and readable log, never a
	// writer's: a leader pod's own subscribers see its commits after the
	// doorbell tick, like every other pod's.
	coldRd := subscribe.NewColdReader(subscribe.ColdReaderConfig{
		Catalog:         f,
		Fetcher:         f,
		Ready:           f.Ready,
		Floor:           f.LogFloor,
		Keyer:           f,
		BlockCacheBytes: opts.SubscribeBlockCacheBytes,
		Metrics:         subscribeMetrics,
	})
	tail, err := subscribe.New(subscribe.Config{
		Logger:      processLogger,
		Metrics:     subscribeMetrics,
		ReadBatch:   opts.SubscribeReadBatch,
		SlowWindow:  opts.SubscribeSlowWindow,
		SlowMinRate: opts.SubscribeSlowMinRate,
	}, coldRd.Read, f.NextSeq)
	if err != nil {
		return fail(fmt.Errorf("serve: build subscribe tail: %w", err))
	}
	tail.SetReadLogSource(f.Log)
	rt.tail = tail

	onSteadyStateEvent := func(ev *segment.Event) {
		if opts.OnSteadyStateEvent != nil {
			opts.OnSteadyStateEvent(ev)
		}
	}
	rt.orchMetrics = orchestrator.NewMetrics(reg)
	rt.leaderMetrics = leader.NewMetrics(reg)
	rt.sessions = &sessionFactory{
		directory:       directory,
		syncClient:      atmossync.NewClient(atmossync.Options{Client: xrpcClient}),
		verifierMetrics: verifierMetrics,
		logger:          processLogger,
		orch: orchestrator.Config{
			RelayURL:      opts.RelayURL,
			HTTPClient:    xrpcClient.HTTPClient.Val(),
			Directory:     directory,
			Logger:        processLogger,
			Metrics:       rt.orchMetrics,
			IngestMetrics: ingestMetrics,
			LiveMetrics:   liveMetrics,
			DropMetrics:   ingest.NewDropMetrics(reg),
			// Bootstrap is stage 3; only the retry runner uses these.
			BackfillMetrics:         backfill.NewMetrics(reg),
			BackfillNewHostClient:   backfillNewHostClient,
			BackfillGlobalDownloads: opts.effectiveBackfillGlobalDownloads(),
			BackfillHostWorkers:     opts.effectiveBackfillHostWorkers(),
			BackfillMaxActiveHosts:  opts.effectiveBackfillMaxActiveHosts(),
			BackfillMaxHosts:        opts.effectiveBackfillMaxHosts(),
			BackfillRetryBaseDelay:  opts.BackfillRetryBaseDelay,
			SegmentMetrics:          segmentMetrics,
			OnEvent:                 onSteadyStateEvent,
			// Subscribers read the follower's log. The writer's keeps only
			// events not yet committed, which PendingBytes bounds.
			ReadLogRetentionBytes:      0,
			SteadyMaxEventsPerBlock:    opts.SteadyMaxEventsPerBlock,
			FailedRepoRetryInterval:    opts.FailedRepoRetryInterval,
			FailedRepoRetryWorkers:     opts.FailedRepoRetryWorkers,
			FailedRepoRetryHostWorkers: opts.FailedRepoRetryHostWorkers,
			FailedRepoRetryMaxDelay:    opts.FailedRepoRetryMaxDelay,
			LiveReconnectBackoff:       opts.LiveReconnectBackoff,
			LiveDial:                   opts.LiveDial,
			CrashInjector:              opts.CrashInjector,
		},
	}

	statusCollector, err := status.New(status.Options{
		Store:                 metaKV,
		Archive:               f,
		ArchiveReady:          f.Ready,
		Manifest:              mft,
		CursorLookback:        opts.CursorLookback,
		IdentityResolver:      resolver,
		LastSeenUpstreamEvent: liveMetrics.LastSeenUpstreamEvent,
	})
	if err != nil {
		return fail(fmt.Errorf("serve: build status collector: %w", err))
	}
	repoArchive := repoexport.Archive{
		Catalog:  f,
		Fetcher:  f,
		Selector: repoexport.FooterSelector{Source: f, Primary: newManifestSelector(mft)},
		Ready:    f.Ready,
	}
	statusHandler, err := web.New(web.Options{
		Snapshotter:                statusCollector,
		RepoActions:                web.NewRepoActions(repoArchive, resolver, pendingEventsForDID(nil)),
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

	for _, route := range []struct {
		pattern string
		v2      bool
	}{
		{"GET /subscribe", false},
		{"GET /xrpc/network.bsky.jetstream.subscribeEvents", true},
	} {
		srv.RegisterPublicRoute(route.pattern, subscribe.NewHandler(subscribe.Subscription{
			Tail:     tail,
			Ready:    f,
			Manifest: mft,
			Catalog:  f,
			Fetcher:  f,
			Seqs:     f,
			Logger:   processLogger,
			Metrics:  subscribeMetrics,
			Lookback: opts.CursorLookback,
			V2:       route.v2,
		}))
	}
	xrpcSrv := xrpcapi.New(xrpcapi.Config{
		Src:                  mft,
		Opener:               xrpcapi.ObjectOpener{Gens: f, Objects: f.Objects()},
		Logger:               processLogger,
		Ready:                f,
		CompactionCacheGrace: opts.CompactionCacheGrace,
		CompactionDeadline:   f,
		Plan: xrpcapi.PlanConfig{
			MaxDIDs:               opts.PlanMaxDIDs,
			MaxCollections:        opts.PlanMaxCollections,
			MaxEntries:            opts.PlanMaxEntries,
			WholeSegmentThreshold: opts.PlanWholeSegmentThreshold,
		},
		Metrics:             xrpcapi.NewMetrics(reg),
		Tracer:              obs.Tracer("xrpcapi"),
		MaxResponseDuration: st.MaxArchiveResponseDuration,
		Sync:                f,
		Dictionary: xrpcapi.DictionaryConfig{
			ID:    subscribe.DictionaryV2ID,
			Bytes: subscribe.DictionaryV2(),
		},
	})
	srv.RegisterPublicRoute("HEAD /xrpc/network.bsky.jetstream.getSegment", xrpcSrv.HeadHandler("network.bsky.jetstream.getSegment"))
	srv.RegisterPublicRoute("HEAD /xrpc/network.bsky.jetstream.getBlock", xrpcSrv.HeadHandler("network.bsky.jetstream.getBlock"))
	srv.RegisterPublicRoute("/xrpc/", xrpcSrv.Handler())
	rt.server = srv
	return rt, nil
}

// runHotSession is the disaggregated leader.SessionFunc (design §10.9): a
// fenced metadata store, the maintainer's rebuild, then the steady-state
// orchestrator with its writer in hot mode.
func (r *Runtime) runHotSession(ctx context.Context, epoch uint64) error {
	d := r.disagg
	sess := catalog.NewSession(catalog.SessionConfig{
		DB:            d.backend.DB,
		Epoch:         epoch,
		Metrics:       d.catalogMetrics,
		LeaderMetrics: r.leaderMetrics,
	})
	return sessionError(r.hotSession(ctx, epoch, sess), sess)
}

func (r *Runtime) hotSession(ctx context.Context, epoch uint64, sess *catalog.Session) error {
	d := r.disagg
	st := r.opts.Storage
	var meta metastore.Store = &sessionStore{
		Store: d.backend.MetaStore(func(ctx context.Context, ops []metastore.Op) error {
			_, err := sess.CommitMeta(ctx, ops)
			return err
		}),
		sess: sess,
	}
	if r.opts.StoreFaultInjector != nil {
		meta = metastore.WithFaults(meta, r.opts.StoreFaultInjector)
	}
	phase, err := lifecycle.ReadPhase(ctx, meta)
	if err != nil {
		return fmt.Errorf("serve: read phase: %w", err)
	}
	if phase != lifecycle.PhaseSteadyState {
		return fmt.Errorf("serve: disaggregated mode needs phase %q, found %q: bootstrap and merge in disaggregated mode are not available yet", lifecycle.PhaseSteadyState, phase)
	}

	// A failed writer or maintainer has already ended the session; the
	// cancel stops the rest of it, and sessionError reports why.
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	onFailure := func(error) { cancel() }
	m, err := maintainer.Open(sctx, maintainer.Config{
		Session:           sess,
		Uploader:          d.uploader,
		Objects:           d.objects,
		Cache:             d.cache,
		MaxEventsPerBlock: r.opts.SteadyMaxEventsPerBlock,
		ReadConcurrency:   st.S3.ReadConcurrency,
		Logger:            r.processLogger,
		Metrics:           d.maintMetrics,
		OnFailure:         onFailure,
	})
	if err != nil {
		return fmt.Errorf("serve: open maintainer: %w", err)
	}
	// The writer closes inside the orchestrator's Run, before the
	// maintainer, so an append blocked on the unfolded cap is released
	// first.
	defer func() {
		if cerr := m.Close(); cerr != nil {
			r.logger.Warn("maintainer close", "epoch", epoch, "err", cerr)
		}
	}()
	resume, err := m.Rebuild(sctx, maintainer.RebuildConfig{BlockMaxAge: st.BlockMaxAge})
	if err != nil {
		return fmt.Errorf("serve: rebuild hot state: %w", err)
	}
	s, err := r.sessions.buildWith(meta, &ingest.HotConfig{
		Session:           sess,
		Uploader:          d.uploader,
		Sink:              m,
		Resume:            resume,
		BatchMaxAge:       st.Hot.BatchMaxAge,
		BlockMaxAge:       st.BlockMaxAge,
		UploadConcurrency: st.S3.UploadConcurrency,
		InlineBytesPerSec: st.Hot.InlineBytesPerSec,
		BulkPendingBytes:  st.Hot.BulkPendingBytes,
		PendingBytes:      st.Hot.PendingBytes,
		MaxUnfoldedEvents: int64(st.Hot.MaxUnfoldedEvents),
		// The doorbell makes this pod's own readers see a commit without
		// waiting for NOTIFY or the poll.
		OnCommit:  func(uint64) { d.follower.Doorbell() },
		OnFailure: onFailure,
	})
	if err != nil {
		return err
	}
	return r.runWriterSession(sctx, epoch, s)
}

// sessionError is what a hot session reports to the leader loop. When the
// catalog session ended first, its error is the cause and everything after
// it, often a cancellation, is fallout; corruption from elsewhere still
// wins so it stays fatal.
func sessionError(err error, sess *catalog.Session) error {
	if err == nil {
		return nil
	}
	if _, ok := catalog.IsCorruption(err); ok {
		return err
	}
	if serr := sess.Err(); serr != nil {
		return serr
	}
	return err
}

// runDisaggregated starts disaggregated mode's goroutines: the catalog
// follower, and writer sessions under the PostgreSQL lease.
func (r *Runtime) runDisaggregated(gctx context.Context, g *errgroup.Group) {
	d := r.disagg
	g.Go(r.goroutineRoot("catalog_follower", func() error {
		if err := d.follower.Run(gctx); err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("catalog follower: %w", err)
		}
		return nil
	}))

	st := r.opts.Storage
	g.Go(r.goroutineRoot("writer_sessions", func() error {
		return leader.Run(gctx, leader.Config{
			Locker:          d.backend.NewLease(),
			Lease:           st.Leader.Lease,
			RenewInterval:   st.Leader.RenewInterval,
			AcquireInterval: st.Leader.AcquireInterval,
			Logger:          r.processLogger.With(slog.String("component", "leader")),
			Metrics:         r.leaderMetrics,
		}, r.runHotSession)
	}))
}

// closeDisaggregated releases the backend once Run has drained, so no
// session or follower query is still using it.
func (r *Runtime) closeDisaggregated() {
	if r.disagg == nil || r.disagg.backend == nil {
		return
	}
	if r.disagg.backend.Close != nil {
		r.disagg.backend.Close()
	}
	r.disagg.backend = nil
}
