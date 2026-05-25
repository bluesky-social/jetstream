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
	"syscall"
	"time"

	identcache "github.com/bluesky-social/jetstream-v2/internal/identity"
	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream-v2/internal/ingest/live"
	"github.com/bluesky-social/jetstream-v2/internal/ingest/orchestrator"
	"github.com/bluesky-social/jetstream-v2/internal/ingest/syncstate"
	"github.com/bluesky-social/jetstream-v2/internal/obs"
	"github.com/bluesky-social/jetstream-v2/internal/server"
	"github.com/bluesky-social/jetstream-v2/internal/status"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/internal/version"
	"github.com/bluesky-social/jetstream-v2/internal/web"
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
			&cli.StringFlag{
				Name:    "relay-url",
				Usage:   "Base URL of the upstream relay",
				Sources: cli.EnvVars("JETSTREAM_RELAY_URL"),
				Value:   "https://bsky.network",
			},
			&cli.StringFlag{
				Name:    "data-dir",
				Usage:   "Path to the data directory; the metadata store lives at <data-dir>/meta.pebble",
				Sources: cli.EnvVars("JETSTREAM_DATA_DIR"),
				Value:   "./data",
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

	dataDir := cmd.String("data-dir")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return fmt.Errorf("serve: create data dir %s: %w", dataDir, err)
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

	statusCollector, err := status.New(status.Options{
		Store:   metaStore,
		DataDir: dataDir,
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

	directory := &identity.Directory{
		Resolver:               &identity.DefaultResolver{},
		Cache:                  identcache.New(metaStore, identcache.DefaultTTL),
		SkipHandleVerification: true,
	}

	stateStore := syncstate.New(metaStore)
	syncClient := atmossync.NewClient(atmossync.Options{Client: xrpcClient})

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
		Logger:          processLogger,
		Metrics:         orchestrator.NewMetrics(metrics.Registry),
		IngestMetrics:   ingest.NewMetrics(metrics.Registry),
		LiveMetrics:     live.NewMetrics(metrics.Registry),
		BackfillMetrics: backfill.NewMetrics(metrics.Registry),
		SegmentMetrics:  segmentMetrics,
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

	runCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	g, gctx := errgroup.WithContext(runCtx)

	g.Go(func() error {
		return srv.Run(gctx)
	})

	g.Go(func() error {
		return orch.Run(gctx)
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
