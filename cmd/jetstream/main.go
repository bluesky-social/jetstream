// Command jetstream is the entry point for the jetstream-v2 process.
//
// # Configuration surface
//
// All flags can be set via the matching JETSTREAM_* env var (see Sources on
// each flag below). On top of those, the OpenTelemetry SDK reads a number of
// standard OTEL_* env vars directly — we don't wrap them as flags because
// they are well-defined by the OTEL spec and the exporter library already
// reads them. The most useful ones are listed below; the full set is
// documented at https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/
//
// Endpoint and transport (read by otlptracehttp at construction time):
//
//	OTEL_EXPORTER_OTLP_ENDPOINT          base endpoint for all OTLP signals.
//	                                     Setting either this or the traces-
//	                                     specific variant is what activates
//	                                     a real exporter; if neither is set
//	                                     we install a no-op tracer provider.
//	                                     Example: https://otel-collector:4318
//	OTEL_EXPORTER_OTLP_TRACES_ENDPOINT   traces-only override.
//	OTEL_EXPORTER_OTLP_PROTOCOL          http/protobuf (default) or http/json.
//	OTEL_EXPORTER_OTLP_HEADERS           comma-separated key=value pairs
//	                                     attached to every export request,
//	                                     e.g. for vendor auth tokens.
//	OTEL_EXPORTER_OTLP_TRACES_HEADERS    traces-only override.
//	OTEL_EXPORTER_OTLP_COMPRESSION       gzip or none.
//	OTEL_EXPORTER_OTLP_TIMEOUT           per-export timeout, default 10s.
//	OTEL_EXPORTER_OTLP_INSECURE          true to skip TLS (dev/local only).
//	OTEL_EXPORTER_OTLP_CERTIFICATE       path to a CA cert for verifying
//	                                     the collector.
//
// Sampling and resource attributes (read by the SDK):
//
//	OTEL_TRACES_SAMPLER                  parentbased_always_on (default),
//	                                     parentbased_traceidratio, etc.
//	OTEL_TRACES_SAMPLER_ARG              ratio for ratio-based samplers,
//	                                     e.g. 0.05 for 5% sampling.
//	OTEL_RESOURCE_ATTRIBUTES             comma-separated key=value pairs
//	                                     merged into the resource alongside
//	                                     service.name. Common keys:
//	                                     deployment.environment,
//	                                     service.namespace, service.instance.id.
//	OTEL_SERVICE_NAME                    overrides the --otel-service-name
//	                                     flag's default.
//
// Batching (read by the batch span processor):
//
//	OTEL_BSP_SCHEDULE_DELAY              ms between batched exports, default
//	                                     5000.
//	OTEL_BSP_MAX_QUEUE_SIZE              max in-memory spans before drop,
//	                                     default 2048.
//	OTEL_BSP_MAX_EXPORT_BATCH_SIZE       max spans per export, default 512.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"syscall"
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
	"github.com/urfave/cli/v3"
	"golang.org/x/sync/errgroup"
)

func main() {
	if err := newApp().Run(context.Background(), os.Args); err != nil {
		// Errors are already logged at the point they originate; this is
		// just the last-resort exit. We write to stderr without slog to
		// avoid double-formatting.
		fmt.Fprintln(os.Stderr, "jetstream:", err)
		os.Exit(1)
	}
}

// newApp builds the root command tree. Split out from main so tests can
// invoke it without going through os.Exit.
//
// Process-wide concerns (log level, log format) live on the root command as
// persistent flags so every present and future subcommand inherits them.
// urfave/cli v3 makes root flags persistent by default; subcommand actions
// can read them with cmd.String(...) the same way they read local flags.
// Concretely this means both `jetstream --log-level=debug serve` and
// `JETSTREAM_LOG_LEVEL=debug jetstream serve` work.
func newApp() *cli.Command {
	info := version.Get()
	return &cli.Command{
		Name:    "jetstream",
		Usage:   "Full-network archive and streaming service for atproto",
		Version: fmt.Sprintf("%s (commit %s, built %s)", info.Version, info.Commit, info.Date),
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "log-level",
				Usage:   "Log level (debug|info|warn|error)",
				Sources: cli.EnvVars("JETSTREAM_LOG_LEVEL"),
				Value:   "info",
			},
			&cli.StringFlag{
				Name:    "log-format",
				Usage:   "Log handler format (text|json)",
				Sources: cli.EnvVars("JETSTREAM_LOG_FORMAT"),
				Value:   "json",
			},
		},
		Commands: []*cli.Command{
			serveCommand(),
			versionCommand(),
			inspectSegmentCommand(),
			inspectAllCommand(),
		},
	}
}

// versionCommand prints the same build metadata that --version emits, but as
// a real subcommand. Two reasons this is worth its own command:
//
//  1. Composability. `jetstream version | jq` and similar patterns are
//     awkward when the only way to print the version is a flag — flags get
//     intercepted before our action runs, and v3 sends --version to its
//     own VersionPrinter which always writes to stderr-ish formatting.
//
//  2. Future-proofing. When we want machine-readable output (`--format=json`
//     for CI to ingest), or to print version info plus runtime metadata
//     (Go version, GOOS/GOARCH, etc.), having a real command gives us a
//     place to add flags without overloading the root.
func versionCommand() *cli.Command {
	return &cli.Command{
		Name:  "version",
		Usage: "Print build version information",
		Action: func(_ context.Context, cmd *cli.Command) error {
			info := version.Get()
			_, err := fmt.Fprintf(
				cmd.Root().Writer,
				"jetstream version %s (commit %s, built %s)\n",
				info.Version,
				info.Commit,
				info.Date,
			)
			return err
		},
	}
}

func serveCommand() *cli.Command {
	return &cli.Command{
		Name:  "serve",
		Usage: "Run the jetstream HTTP server",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "addr",
				Usage:   "Bind address for the public HTTP listener",
				Sources: cli.EnvVars("JETSTREAM_ADDR"),
				Value:   ":8080",
			},
			&cli.StringFlag{
				Name:    "debug-addr",
				Usage:   "Bind address for the debug HTTP listener (metrics, pprof, health)",
				Sources: cli.EnvVars("JETSTREAM_DEBUG_ADDR"),
				Value:   ":6060",
			},
			&cli.StringFlag{
				Name:    "otel-service-name",
				Usage:   "Resource service.name attribute for emitted spans",
				Sources: cli.EnvVars("OTEL_SERVICE_NAME"),
				Value:   "jetstream",
			},
			&cli.DurationFlag{
				Name:    "shutdown-timeout",
				Usage:   "Maximum time allowed for graceful shutdown after a signal is received",
				Sources: cli.EnvVars("JETSTREAM_SHUTDOWN_TIMEOUT"),
				Value:   30 * time.Second,
			},
			&cli.DurationFlag{
				Name:    "client-drain-timeout",
				Usage:   "Maximum time allowed for in-progress websocket subscribers to receive a clean close frame and disconnect before the process exits",
				Sources: cli.EnvVars("JETSTREAM_CLIENT_DRAIN_TIMEOUT"),
				Value:   10 * time.Second,
			},
			&cli.StringFlag{
				Name:    "relay-url",
				Usage:   "Base URL of the upstream relay",
				Sources: cli.EnvVars("JETSTREAM_RELAY_URL"),
				Value:   "https://bsky.network",
			},
			&cli.StringFlag{
				Name:    "plc-url",
				Usage:   "Base URL of the PLC directory; empty uses atmos's default (https://plc.directory)",
				Sources: cli.EnvVars("JETSTREAM_PLC_URL"),
				Value:   "",
			},
			&cli.StringFlag{
				Name:    "data-dir",
				Usage:   "Path to the data directory; the metadata store lives at <data-dir>/meta.pebble",
				Sources: cli.EnvVars("JETSTREAM_DATA_DIR"),
				Value:   "./data",
			},
			&cli.IntFlag{
				Name:    "max-backfill-repos",
				Usage:   "DEBUG ONLY: stop the backfill phase after N successfully downloaded repos and proceed to merge. 0 = unlimited (production default). Intended for fast local-dev iteration against the production relay's millions of users; safe to leave set in production but unnecessary there.",
				Sources: cli.EnvVars("JETSTREAM_MAX_BACKFILL_REPOS"),
				Value:   0,
			},
			&cli.BoolFlag{
				Name:    "skip-merge-discovery",
				Usage:   "DEBUG ONLY: skip the end-of-merge listRepos rescan that discovers accounts created during the merge phase. Intended for fast local-dev iteration against the production relay's millions of users; safe to leave set in production but unnecessary there.",
				Sources: cli.EnvVars("JETSTREAM_SKIP_MERGE_DISCOVERY"),
				Value:   false,
			},
			&cli.DurationFlag{
				Name:    "cursor-lookback",
				Usage:   "Maximum age for ?cursor= replay. Cursors older than this are clamped to the floor. 0 disables cursor lookback (cursor query parameter resolves to live tip).",
				Sources: cli.EnvVars("JETSTREAM_CURSOR_LOOKBACK"),
				Value:   36 * time.Hour,
			},
			&cli.DurationFlag{
				Name:    "segment-cache-max-age",
				Usage:   "Cache-Control max-age for XRPC segment downloads. 0 requires caches to revalidate every request.",
				Sources: cli.EnvVars("JETSTREAM_SEGMENT_CACHE_MAX_AGE"),
				Value:   0,
			},
			&cli.IntFlag{
				Name:    "subscribe-hot-tail-bytes",
				Usage:   "Byte budget of the in-memory hot-tail ring that fans live events to /subscribe clients.",
				Sources: cli.EnvVars("JETSTREAM_SUBSCRIBE_HOT_TAIL_BYTES"),
				Value:   256 << 20,
			},
			&cli.IntFlag{
				Name:    "subscribe-block-cache-bytes",
				Usage:   "Decoded-byte budget of the shared cold-path block cache (sealed + flushed blocks).",
				Sources: cli.EnvVars("JETSTREAM_SUBSCRIBE_BLOCK_CACHE_BYTES"),
				Value:   64 << 20,
			},
			&cli.IntFlag{
				Name:    "subscribe-read-batch",
				Usage:   "Max events returned per ReadFrom call to a /subscribe client.",
				Sources: cli.EnvVars("JETSTREAM_SUBSCRIBE_READ_BATCH"),
				Value:   1024,
			},
			&cli.DurationFlag{
				Name:    "subscribe-slow-window",
				Usage:   "Sustained window over which an adversarially-slow /subscribe client is judged before being dropped.",
				Sources: cli.EnvVars("JETSTREAM_SUBSCRIBE_SLOW_WINDOW"),
				Value:   60 * time.Second,
			},
			&cli.FloatFlag{
				Name:    "subscribe-slow-min-rate",
				Usage:   "Events/sec floor below which a far-behind /subscribe client is considered adversarially slow.",
				Sources: cli.EnvVars("JETSTREAM_SUBSCRIBE_SLOW_MIN_RATE"),
				Value:   5,
			},
			&cli.IntFlag{
				Name:    "cursor-block-index-cache-size",
				Usage:   "Deprecated compatibility no-op: sealed segment metadata is always resident in the manifest.",
				Sources: cli.EnvVars("JETSTREAM_CURSOR_BLOCK_INDEX_CACHE_SIZE"),
				Value:   32,
			},
		},
		Action: runServe,
	}
}

func runServe(ctx context.Context, cmd *cli.Command) error {
	processLogger, err := obs.BuildLoggerFromStrings(os.Stderr, cmd.String("log-level"), cmd.String("log-format"))
	if err != nil {
		return err
	}
	// processLogger is the bare per-process logger; downstream
	// subsystems (orchestrator, server, verifier callback) receive it
	// AS-IS so each can set its own `component` without slog stacking
	// duplicate keys (slog.With appends rather than replacing).
	//
	// logger is a component=main wrapper for cmd/jetstream's own log
	// lines.
	logger := processLogger.With(slog.String("component", "main"))
	slog.SetDefault(logger)

	info := version.Get()
	logger.Info("startup",
		"version", info.Version,
		"commit", info.Commit,
		"built", info.Date,
	)

	tracerShutdown, err := obs.SetupTracing(ctx, obs.TracingConfig{
		ServiceName: cmd.String("otel-service-name"),
	})
	if err != nil {
		return fmt.Errorf("setup tracing: %w", err)
	}

	metrics := obs.NewMetrics()
	storeMetrics := store.NewMetrics(metrics.Registry)
	segmentMetrics := segment.NewMetrics(metrics.Registry)
	verifierMetrics := obs.NewVerifierMetrics(metrics.Registry)
	subscribeMetrics := subscribe.NewMetrics(metrics.Registry)
	manifestMetrics := manifest.NewMetrics(metrics.Registry)

	dataDir := cmd.String("data-dir")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return fmt.Errorf("serve: create data dir %s: %w", dataDir, err)
	}
	segmentCacheMaxAge := cmd.Duration("segment-cache-max-age")
	if segmentCacheMaxAge < 0 {
		return fmt.Errorf("serve: --segment-cache-max-age must be >= 0, got %s", segmentCacheMaxAge)
	}

	segmentsDir := filepath.Join(dataDir, "segments")
	if err := os.MkdirAll(segmentsDir, 0o755); err != nil {
		return fmt.Errorf("serve: create segments dir %s: %w", segmentsDir, err)
	}

	metaStore, err := store.Open(dataDir, storeMetrics)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := metaStore.Close(); cerr != nil {
			logger.Error("close metadata store", "err", cerr)
		}
	}()

	runCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	mft, err := manifest.OpenBackground(runCtx, manifest.Options{
		SegmentsDir:         segmentsDir,
		BlockIndexCacheSize: cmd.Int("cursor-block-index-cache-size"),
		Logger:              processLogger,
		Metrics:             manifestMetrics,
	})
	if err != nil {
		return fmt.Errorf("serve: open manifest: %w", err)
	}

	// writerPtr is published by the orchestrator once the steady-state
	// live consumer opens its ingest.Writer; the cursor handler reads it
	// atomically. Before steady-state the lifecycle.IsSteadyState gate
	// returns 503, so the nil-pointer window is harmless.
	var writerPtr atomic.Pointer[ingest.Writer]

	statusCollector, err := status.New(status.Options{
		Store:          metaStore,
		DataDir:        dataDir,
		Manifest:       mft,
		CursorLookback: cmd.Duration("cursor-lookback"),
	})
	if err != nil {
		return fmt.Errorf("serve: build status collector: %w", err)
	}

	statusHandler, err := web.New(web.Options{
		Snapshotter: statusCollector,
		Logger:      processLogger,
	})
	if err != nil {
		return fmt.Errorf("serve: build status handler: %w", err)
	}

	// Verifier setup (shared across phases). The verifier itself is
	// owned by the orchestrator's per-phase live consumers, but we
	// construct it here because its async-error drain is a sibling
	// goroutine in the top-level errgroup — it's a process-wide
	// observability concern.
	relayHTTPURL, err := live.DeriveRelayHTTPURL(cmd.String("relay-url"))
	if err != nil {
		return fmt.Errorf("serve: derive relay HTTP URL: %w", err)
	}

	xrpcClient := &xrpc.Client{
		Host:       relayHTTPURL,
		HTTPClient: gt.Some(jttp.New(xrpc.BulkDownloadOpts()...)),
	}

	resolver := &identity.DefaultResolver{}
	if u := cmd.String("plc-url"); u != "" {
		resolver.PLCURL = gt.Some(u)
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
		BlockCacheBytes: cmd.Int("subscribe-block-cache-bytes"),
	})
	tail, err := subscribe.New(subscribe.Config{
		Logger:       processLogger,
		Metrics:      subscribeMetrics,
		HotTailBytes: cmd.Int("subscribe-hot-tail-bytes"),
		ReadBatch:    cmd.Int("subscribe-read-batch"),
		SlowWindow:   cmd.Duration("subscribe-slow-window"),
		SlowMinRate:  cmd.Float("subscribe-slow-min-rate"),
	}, coldRd, func() uint64 {
		if w := writerPtr.Load(); w != nil {
			return w.NextSeq()
		}
		return 0
	})
	if err != nil {
		return fmt.Errorf("serve: build subscribe tail: %w", err)
	}

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
		return fmt.Errorf("serve: build verifier: %w", err)
	}
	defer func() {
		if cerr := verifier.Close(); cerr != nil {
			logger.Error("verifier close", "err", cerr)
		}
	}()

	// The orchestrator owns all ingestion-lifecycle subsystems
	// (backfill engine, bootstrap-time live consumer, steady-state
	// live consumer). cmd/jetstream is no longer phase-aware.
	orch, err := orchestrator.New(orchestrator.Config{
		DataDir:    dataDir,
		Store:      metaStore,
		RelayURL:   cmd.String("relay-url"),
		HTTPClient: xrpcClient.HTTPClient.Val(),
		Directory:  directory,
		Verifier:   verifier,
		// Bare logger; orchestrator.New attaches component=orchestrator
		// itself, and its children (live, ingest, backfill) attach
		// their own component on top of the bare parent.
		Logger:             processLogger,
		Metrics:            orchestrator.NewMetrics(metrics.Registry),
		IngestMetrics:      ingest.NewMetrics(metrics.Registry),
		LiveMetrics:        live.NewMetrics(metrics.Registry),
		BackfillMetrics:    backfill.NewMetrics(metrics.Registry),
		SegmentMetrics:     segmentMetrics,
		OnEvent:            tail.Append,
		MaxBackfillRepos:   cmd.Int("max-backfill-repos"),
		SkipMergeDiscovery: cmd.Bool("skip-merge-discovery"),
		IngestOnAfterSeal:  mft.OnSegmentSealed,
		OnSteadyStateWriter: func(w *ingest.Writer) {
			writerPtr.Store(w)
		},
	})
	if err != nil {
		return fmt.Errorf("serve: build orchestrator: %w", err)
	}

	srv := server.New(server.Config{
		PublicAddr:      cmd.String("addr"),
		DebugAddr:       cmd.String("debug-addr"),
		ShutdownTimeout: cmd.Duration("shutdown-timeout"),
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
		Lookback:  cmd.Duration("cursor-lookback"),
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
	}, segmentCacheMaxAge)
	srv.RegisterPublicRoute("/xrpc/", xrpcSrv.Handler())

	g, gctx := errgroup.WithContext(runCtx)

	g.Go(func() error {
		if err := mft.Wait(gctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("manifest load: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		return srv.Run(gctx)
	})

	g.Go(func() error {
		return orch.Run(gctx)
	})

	// Graceful client drain. Live websocket subscribers are hijacked
	// connections, so http.Server.Shutdown neither tracks nor closes
	// them — without this they'd be severed abruptly (no close frame) on
	// process exit. On shutdown we send each subscriber a StatusGoingAway
	// close frame and wait up to client-drain-timeout for them to leave
	// cleanly. We keep this in the errgroup so g.Wait() blocks on the
	// drain: the process must not exit out from under a half-sent close
	// handshake. The drain context is rooted at Background (not gctx,
	// which is already cancelled by the time we drain) and bounded by the
	// flag, so a wedged client can't delay exit past the budget.
	g.Go(func() error {
		<-gctx.Done()
		drainCtx, drainCancel := context.WithTimeout(context.Background(), cmd.Duration("client-drain-timeout"))
		defer drainCancel()
		if err := tail.Shutdown(drainCtx); err != nil {
			logger.Warn("client drain did not complete within budget; severing remaining subscribers", "err", err)
		}
		return nil
	})

	// Verifier async-error drain. Verification failures are
	// diagnostic, not fatal — they typically reflect adversarial or
	// malformed PDS input, which is invalid user data, not a
	// jetstream bug. We warn-log and the OnVerificationFailure hook
	// fires for operator visibility, but never crash.
	g.Go(func() error {
		for {
			select {
			case <-gctx.Done():
				return nil
			case err, ok := <-verifier.AsyncErrors():
				if !ok {
					return nil
				}
				verifierLogger.Warn("async error", "err", err)
			}
		}
	})

	// A signal-driven shutdown surfaces as context.Canceled from the
	// errgroup: the orchestrator's steady-state consumer and the HTTP
	// server both return ctx.Err() on graceful shutdown. We don't want
	// the user to see "context canceled" when they hit Ctrl-C.
	runErr := g.Wait()
	if errors.Is(runErr, context.Canceled) && runCtx.Err() != nil {
		runErr = nil
	}

	// Shut tracing down with a fresh, bounded context so we can still flush
	// pending spans even though runCtx has been cancelled.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cmd.Duration("shutdown-timeout"))
	defer cancel()

	if err := tracerShutdown(shutdownCtx); err != nil {
		logger.Error("tracer shutdown failed", "err", err)
	}

	return runErr
}
