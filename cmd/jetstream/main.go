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
	"syscall"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/backfill"
	"github.com/bluesky-social/jetstream-v2/internal/identitycache"
	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
	"github.com/bluesky-social/jetstream-v2/internal/livestream"
	"github.com/bluesky-social/jetstream-v2/internal/obs"
	"github.com/bluesky-social/jetstream-v2/internal/server"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/internal/syncstate"
	"github.com/bluesky-social/jetstream-v2/internal/version"
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
	logger, err := obs.BuildLoggerFromStrings(os.Stderr, cmd.String("log-level"), cmd.String("log-format"))
	if err != nil {
		return err
	}
	slog.SetDefault(logger)

	info := version.Get()
	logger.Info("starting jetstream",
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

	dataDir := cmd.String("data-dir")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return fmt.Errorf("serve: create data dir %s: %w", dataDir, err)
	}

	metaStore, err := store.Open(dataDir)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := metaStore.Close(); cerr != nil {
			logger.Error("close metadata store", "err", cerr)
		}
	}()

	phase, err := lifecycle.ReadPhase(metaStore)
	if err != nil {
		return fmt.Errorf("serve: read phase: %w", err)
	}
	if phase == "" {
		phase = lifecycle.PhaseBootstrap
		if err := lifecycle.WritePhase(metaStore, phase); err != nil {
			return fmt.Errorf("serve: write phase: %w", err)
		}
	}
	if phase == lifecycle.PhaseSteadyState {
		return fmt.Errorf("serve: steady-state phase not yet supported; the merge step has not been implemented")
	}

	ingestMetrics := ingest.NewMetrics(metrics.Registry)
	ingestWriter, err := ingest.Open(ingest.Config{
		SegmentsDir: filepath.Join(dataDir, "segments"),
		Store:       metaStore,
		Logger:      logger,
		Metrics:     ingestMetrics,
	})
	if err != nil {
		return fmt.Errorf("ingest open: %w", err)
	}
	defer func() {
		if cerr := ingestWriter.Close(); cerr != nil {
			logger.Error("close ingest writer", "err", cerr)
		}
	}()

	// Sync 1.1 verifier graph. Construction lives here so the future
	// merge-step consumer can share the same primitives.
	//
	// Defer registration order in this function determines shutdown
	// order via LIFO unwind. The full chain, in unwind order, is:
	//   1. liveConsumer.Close() — stops the firehose pump first so
	//      no new events arrive at the verifier or ingest writer.
	//   2. verifier.Close()      — drains the resync worker pool with
	//      no inbound traffic; safe to do while metaStore is still
	//      open, since workers hold references to syncstate.
	//   3. ingestWriter.Close()  — flushes the active segment block.
	//   4. metaStore.Close()     — last; releases the pebble db file
	//      lock that all three above depend on.
	relayHTTPURL, err := livestream.DeriveRelayHTTPURL(cmd.String("relay-url"))
	if err != nil {
		return fmt.Errorf("serve: derive relay HTTP URL: %w", err)
	}

	// BulkDownloadOpts (no wall-clock timeout, but TTFB / idle / min-rate
	// guards) is required for getRepo against the largest accounts —
	// some carry ~1 GiB of CAR data that would trip xrpc.NewHTTPClient's
	// 30s default. atmos's atp reference impl uses the same pattern.
	xrpcClient := &xrpc.Client{
		Host:       relayHTTPURL,
		HTTPClient: gt.Some(jttp.New(xrpc.BulkDownloadOpts()...)),
	}

	// SkipHandleVerification: the firehose verifier only needs the
	// account's atproto signing key, which lives on the DID document.
	// The bidirectional handle check (DNS plus an HTTPS GET to the
	// user's domain) is the dominant per-resolution cost and is
	// orthogonal to commit signature verification — turning it off is
	// the single biggest throughput win for verifier consumers (atmos
	// docs § identity.NewInMemoryDirectory).
	directory := &identity.Directory{
		Resolver:               &identity.DefaultResolver{},
		Cache:                  identitycache.New(metaStore, identitycache.DefaultTTL),
		SkipHandleVerification: true,
	}

	stateStore := syncstate.New(metaStore)

	syncClient := atmossync.NewClient(atmossync.Options{Client: xrpcClient})

	verifier, err := atmossync.NewVerifier(atmossync.VerifierOptions{
		Directory:  directory,
		StateStore: stateStore,
		SyncClient: gt.Some(syncClient),
		// OnVerificationFailure surfaces every verification failure
		// at WARN regardless of policy or whether a subsequent resync
		// repairs the chain. This is the operator-visible audit trail
		// for "verifier rejected something"; the downstream effect
		// (drop, resync, etc.) is decided by Policy and observable
		// via Stats() / AsyncErrors(). Per atmos's contract this hook
		// fires AFTER the per-DID mutex is released, so it is safe to
		// call other Verifier methods from inside; we only log.
		OnVerificationFailure: gt.Some(func(did atmos.DID, vErr error) {
			logger.Warn("verifier failure",
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

	liveConsumer, err := livestream.Open(livestream.Config{
		SegmentsDir: filepath.Join(dataDir, "backfill", "live_segments"),
		Store:       metaStore,
		SeqKey:      "live_segments/seq/next",
		CursorKey:   "relay/cursor",
		RelayURL:    cmd.String("relay-url"),
		Logger:      logger,
		Metrics:     livestream.NewMetrics(metrics.Registry),
		Verifier:    verifier,
	})
	if err != nil {
		return fmt.Errorf("livestream open: %w", err)
	}
	defer func() {
		if cerr := liveConsumer.Close(); cerr != nil {
			logger.Error("close live consumer", "err", cerr)
		}
	}()

	srv := server.New(server.Config{
		PublicAddr:      cmd.String("addr"),
		DebugAddr:       cmd.String("debug-addr"),
		ShutdownTimeout: cmd.Duration("shutdown-timeout"),
	}, logger, metrics)

	// Cancel runCtx on SIGINT/SIGTERM. signal.NotifyContext gives us a clean
	// idiomatic way to propagate the signal as a context cancellation, which
	// both the server's Run loop and the bootstrap pipeline already know how
	// to handle.
	runCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Run the HTTP server, the bootstrap pipeline, the live firehose
	// consumer, and the verifier async-error drain as four siblings under
	// one errgroup. Any one failing cancels the others — we don't want a
	// server that's silently serving 503s while bootstrap has died, and
	// we don't want to keep enumerating the relay if the public listener
	// is dead.
	//
	// During bootstrap phase, the backfill goroutine draining is the signal
	// that flips the public surface from "503 — bootstrapping" to live
	// serving.
	g, gctx := errgroup.WithContext(runCtx)

	// Start the main web server
	g.Go(func() error {
		return srv.Run(gctx)
	})

	// Start the backfiller to do initial repo download for a fresh
	// jetstream instance, or resume from where a prior process left
	// off (DESIGN.md §4.1). On clean drain this goroutine returns nil
	// and the HTTP server keeps running; on engine failure the
	// errgroup cancels the server too.
	g.Go(func() error {
		return backfill.Run(gctx, backfill.Config{
			Store:    metaStore,
			Writer:   ingestWriter,
			RelayURL: cmd.String("relay-url"),
			Logger:   logger,
			Metrics:  backfill.NewMetrics(metrics.Registry),
		})
	})

	// Start the live firehose consumer, writing events to live_segments.
	g.Go(func() error {
		return liveConsumer.Run(gctx)
	})

	// Drain verifier async errors. Logs at WARN; does not abort the
	// errgroup since AsyncErrors are diagnostic — a failed resync
	// triggers another verification failure on the next commit, which
	// is its own observability path. Returns nil on context cancel
	// or channel close (verifier is closing).
	g.Go(func() error {
		for {
			select {
			case <-gctx.Done():
				return nil
			case err, ok := <-verifier.AsyncErrors():
				if !ok {
					return nil
				}
				logger.Warn("verifier async error", "err", err)
			}
		}
	})

	// A signal-driven shutdown surfaces as context.Canceled from the
	// bootstrap goroutine because the HTTP server intercepts it as a
	// graceful shutdown. We don't want the user to see "context canceled"
	// when they hit Ctrl-C.
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
