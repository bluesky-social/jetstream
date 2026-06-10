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
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/jetstreamd"
	"github.com/bluesky-social/jetstream-v2/internal/version"
	"github.com/jcalabro/atmos"
	"github.com/urfave/cli/v3"
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
			verifyRepoCommand(),
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
			&cli.StringFlag{
				Name:    "backfill-repos",
				Usage:   "DEBUG ONLY: comma-separated DID list to backfill instead of walking listRepos. Empty = normal production behavior.",
				Sources: cli.EnvVars("JETSTREAM_BACKFILL_REPOS"),
				Value:   "",
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
			&cli.DurationFlag{
				Name:    "compaction-interval",
				Usage:   "Interval between steady-state delete/update compaction passes. 0 disables compaction, including the merge-tail pass.",
				Sources: cli.EnvVars("JETSTREAM_COMPACTION_INTERVAL"),
				Value:   4 * time.Hour,
			},
			&cli.IntFlag{
				Name:    "compaction-tombstone-cap",
				Usage:   "Maximum tombstone entries retained before an early compaction pass is triggered.",
				Sources: cli.EnvVars("JETSTREAM_COMPACTION_TOMBSTONE_CAP"),
				Value:   32_000_000,
			},
		},
		Action: runServe,
	}
}

func serveOptionsFromCommand(cmd *cli.Command) (jetstreamd.Options, error) {
	backfillRepos, err := parseBackfillRepos(cmd.String("backfill-repos"))
	if err != nil {
		return jetstreamd.Options{}, err
	}
	maxBackfillRepos := cmd.Int("max-backfill-repos")
	if len(backfillRepos) > 0 && maxBackfillRepos > 0 {
		return jetstreamd.Options{}, fmt.Errorf("serve: --backfill-repos cannot be combined with --max-backfill-repos")
	}

	return jetstreamd.Options{
		PublicAddr:                cmd.String("addr"),
		DebugAddr:                 cmd.String("debug-addr"),
		DataDir:                   cmd.String("data-dir"),
		RelayURL:                  cmd.String("relay-url"),
		PLCURL:                    cmd.String("plc-url"),
		OTelServiceName:           cmd.String("otel-service-name"),
		LogLevel:                  cmd.String("log-level"),
		LogFormat:                 cmd.String("log-format"),
		LogOutput:                 os.Stderr,
		ShutdownTimeout:           cmd.Duration("shutdown-timeout"),
		ClientDrainTimeout:        cmd.Duration("client-drain-timeout"),
		MaxBackfillRepos:          maxBackfillRepos,
		BackfillRepos:             backfillRepos,
		SkipMergeDiscovery:        cmd.Bool("skip-merge-discovery"),
		CursorLookback:            cmd.Duration("cursor-lookback"),
		SegmentCacheMaxAge:        cmd.Duration("segment-cache-max-age"),
		SubscribeHotTailBytes:     cmd.Int("subscribe-hot-tail-bytes"),
		SubscribeBlockCacheBytes:  cmd.Int("subscribe-block-cache-bytes"),
		SubscribeReadBatch:        cmd.Int("subscribe-read-batch"),
		SubscribeSlowWindow:       cmd.Duration("subscribe-slow-window"),
		SubscribeSlowMinRate:      cmd.Float("subscribe-slow-min-rate"),
		CursorBlockIndexCacheSize: cmd.Int("cursor-block-index-cache-size"),
		CompactionInterval:        cmd.Duration("compaction-interval"),
		CompactionTombstoneCap:    cmd.Int("compaction-tombstone-cap"),
	}, nil
}

func parseBackfillRepos(raw string) ([]atmos.DID, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]atmos.DID, 0, len(parts))
	seen := make(map[atmos.DID]struct{}, len(parts))
	for i, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			return nil, fmt.Errorf("serve: --backfill-repos contains empty entry at position %d", i+1)
		}
		did, err := atmos.ParseDID(trimmed)
		if err != nil {
			return nil, fmt.Errorf("serve: --backfill-repos entry %d: %w", i+1, err)
		}
		if _, ok := seen[did]; ok {
			return nil, fmt.Errorf("serve: --backfill-repos duplicate DID %s", did)
		}
		seen[did] = struct{}{}
		out = append(out, did)
	}
	return out, nil
}

func runServe(ctx context.Context, cmd *cli.Command) error {
	runCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts, err := serveOptionsFromCommand(cmd)
	if err != nil {
		return err
	}
	rt, err := jetstreamd.Build(runCtx, opts)
	if err != nil {
		return err
	}
	runErr := rt.Run(runCtx)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cmd.Duration("shutdown-timeout"))
	defer cancel()
	_ = rt.Close(shutdownCtx)
	return runErr
}
