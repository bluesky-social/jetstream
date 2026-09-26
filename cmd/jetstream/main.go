// Command jetstream is the entry point for the jetstream process.
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
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/internal/objstore"
	"github.com/bluesky-social/jetstream/internal/version"
	"github.com/bluesky-social/jetstream/internal/xrpcapi"
	"github.com/jcalabro/atmos"
	"github.com/urfave/cli/v3"
)

const jetstreamEnvPrefix = "JETSTREAM_"

var knownForeignJetstreamEnvPrefixes = []string{
	// auto-injected by kubernetes
	"JETSTREAM_APP_",

	// cmd/simulator binary
	"JETSTREAM_SIM_",

	// client api keys, which might just be set ambiently in dev environments perhaps
	"JETSTREAM_API_KEY",

	// test harnesses (just test-storage: PG URL, S3 endpoints)
	"JETSTREAM_TEST_",
}

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
	return newAppWithEnviron(os.Environ)
}

func newAppWithEnviron(environ func() []string) *cli.Command {
	if environ == nil {
		environ = os.Environ
	}
	info := version.Get()
	return &cli.Command{
		Name:    "jetstream",
		Usage:   "Full-network archive and streaming service for atproto",
		Version: fmt.Sprintf("%s (commit %s, built %s)", info.Version, info.Commit, info.Date),
		Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
			return ctx, rejectUnknownJetstreamEnvVars(cmd.Root(), environ())
		},
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
			storageCommand(),
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
	cmd := &cli.Command{
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
				Usage:   "Bind address for the debug HTTP listener (metrics, pprof, health). Empty disables it.",
				Sources: cli.EnvVars("JETSTREAM_DEBUG_ADDR"),
				Value:   "",
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
				Value:   5 * time.Second,
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
				Usage:   "Path to the data directory; the metadata store lives at <data-dir>/meta.pebble. Local storage only: must be unset with --storage=disaggregated",
				Sources: cli.EnvVars("JETSTREAM_DATA_DIR"),
				Value:   "./data",
			},
			&cli.IntFlag{
				Name:    "max-backfill-repos",
				Usage:   "DEBUG ONLY: select and download up to N repos from listRepos and proceed to merge. 0 = unlimited (production default).",
				Sources: cli.EnvVars("JETSTREAM_MAX_BACKFILL_REPOS"),
				Value:   0,
			},
			&cli.IntFlag{
				Name:    "backfill-workers",
				Usage:   "Deprecated and ignored; use the fleet concurrency flags.",
				Sources: cli.EnvVars("JETSTREAM_BACKFILL_WORKERS"),
				Hidden:  true,
			},
			&cli.IntFlag{
				Name: "backfill-global-downloads", Usage: "Fleet-wide in-flight direct-PDS getRepo limit.",
				Sources: cli.EnvVars("JETSTREAM_BACKFILL_GLOBAL_DOWNLOADS"), Value: jetstreamd.DefaultBackfillGlobalDownloads,
			},
			&cli.IntFlag{
				Name: "backfill-host-workers-max", Usage: "Maximum direct-PDS download workers per host.",
				Sources: cli.EnvVars("JETSTREAM_BACKFILL_HOST_WORKERS_MAX"), Value: jetstreamd.DefaultBackfillHostWorkers,
			},
			&cli.IntFlag{
				Name: "backfill-max-active-hosts", Usage: "Maximum concurrently active PDS listRepos loops.",
				Sources: cli.EnvVars("JETSTREAM_BACKFILL_MAX_ACTIVE_HOSTS"), Value: jetstreamd.DefaultBackfillMaxActiveHosts,
			},
			&cli.IntFlag{
				Name: "backfill-max-hosts", Usage: "Maximum accepted listHosts roster entries.",
				Sources: cli.EnvVars("JETSTREAM_BACKFILL_MAX_HOSTS"), Value: jetstreamd.DefaultBackfillMaxHosts,
			},
			&cli.IntFlag{
				Name:    "backfill-batch-size",
				Usage:   "Per-host listRepos checkpoint granularity. 0 uses the production default.",
				Sources: cli.EnvVars("JETSTREAM_BACKFILL_BATCH_SIZE"),
				Value:   jetstreamd.DefaultBackfillBatchSize,
			},
			&cli.IntFlag{
				Name:    "backfill-async-flush-workers",
				Usage:   "Async compression workers for bootstrap backfill segment flushes. 0 disables async flushing.",
				Sources: cli.EnvVars("JETSTREAM_BACKFILL_ASYNC_FLUSH_WORKERS"),
				Value:   jetstreamd.DefaultBackfillAsyncFlushWorkers,
			},
			&cli.StringFlag{
				Name:    "backfill-repos",
				Usage:   "DEBUG ONLY: comma-separated DID list to backfill instead of walking listRepos. Empty = normal production behavior.",
				Sources: cli.EnvVars("JETSTREAM_BACKFILL_REPOS"),
				Value:   "",
			},
			&cli.BoolFlag{
				Name:    "skip-merge-discovery",
				Usage:   "DEBUG ONLY: skip the end-of-merge listRepos rescan that discovers accounts created during the merge phase. Automatically enabled by --max-backfill-repos and --backfill-repos for fast local iteration.",
				Sources: cli.EnvVars("JETSTREAM_SKIP_MERGE_DISCOVERY"),
				Value:   false,
			},
			&cli.DurationFlag{
				Name:    "failed-repo-retry-interval",
				Usage:   "Steady-state interval for scanning and retrying repos that failed initial backfill. 0 disables background failed-repo retry.",
				Sources: cli.EnvVars("JETSTREAM_FAILED_REPO_RETRY_INTERVAL"),
				Value:   jetstreamd.DefaultFailedRepoRetryInterval,
			},
			&cli.IntFlag{
				Name:    "failed-repo-retry-workers",
				Usage:   "Global worker count for steady-state failed-repo retry. 0 uses the production default.",
				Sources: cli.EnvVars("JETSTREAM_FAILED_REPO_RETRY_WORKERS"),
				Value:   jetstreamd.DefaultFailedRepoRetryWorkers,
			},
			&cli.IntFlag{
				Name:    "failed-repo-retry-host-workers",
				Usage:   "Maximum concurrent failed-repo retry requests per known PDS host. 0 uses the production default.",
				Sources: cli.EnvVars("JETSTREAM_FAILED_REPO_RETRY_HOST_WORKERS"),
				Value:   jetstreamd.DefaultFailedRepoRetryHostWorkers,
			},
			&cli.DurationFlag{
				Name:    "failed-repo-retry-max-delay",
				Usage:   "Maximum per-repo failed-repo retry backoff delay.",
				Sources: cli.EnvVars("JETSTREAM_FAILED_REPO_RETRY_MAX_DELAY"),
				Value:   jetstreamd.DefaultFailedRepoRetryMaxDelay,
			},
			&cli.BoolFlag{
				Name:    "disable-repo-action-rate-limits",
				Usage:   "Disable per-source-IP rate limits for expensive status-page repo actions such as repo verification.",
				Sources: cli.EnvVars("JETSTREAM_DISABLE_REPO_ACTION_RATE_LIMITS"),
				Value:   false,
			},
			&cli.DurationFlag{
				Name:    "cursor-lookback",
				Usage:   "Maximum age for ?cursor= replay. Cursors older than this are clamped to the floor. 0 disables cursor lookback (cursor query parameter resolves to live tip).",
				Sources: cli.EnvVars("JETSTREAM_CURSOR_LOOKBACK"),
				Value:   36 * time.Hour,
			},
			&cli.DurationFlag{
				Name:    "compaction-cache-grace",
				Usage:   "Grace period added to the next compaction start for dynamic XRPC archive cache lifetimes. Unknown, disabled, or failed scheduling remains no-cache.",
				Sources: cli.EnvVars("JETSTREAM_COMPACTION_CACHE_GRACE"),
				Value:   jetstreamd.DefaultCompactionCacheGrace,
			},
			&cli.IntFlag{
				Name:    "plan-max-dids",
				Usage:   "Maximum distinct DIDs accepted by planSnapshot. 0 disables non-empty DID filters.",
				Sources: cli.EnvVars("JETSTREAM_PLAN_MAX_DIDS"),
				Value:   xrpcapi.DefaultPlanMaxDIDs,
			},
			&cli.IntFlag{
				Name:    "plan-max-collections",
				Usage:   "Maximum distinct collections accepted by planSnapshot. 0 disables non-empty collection filters.",
				Sources: cli.EnvVars("JETSTREAM_PLAN_MAX_COLLECTIONS"),
				Value:   xrpcapi.DefaultPlanMaxCollections,
			},
			&cli.IntFlag{
				Name:    "plan-max-entries",
				Usage:   "Maximum response work entries a single planSnapshot page may contain. When matched work exceeds it the plan is truncated at a work-unit boundary and the caller paginates via plannedThroughSeq. 0 disables pagination (one unbounded page).",
				Sources: cli.EnvVars("JETSTREAM_PLAN_MAX_ENTRIES"),
				Value:   xrpcapi.DefaultPlanMaxEntries,
			},
			&cli.FloatFlag{
				Name:    "plan-whole-segment-threshold",
				Usage:   "Selected-block density at or above which planSnapshot returns a whole segment instead of block ranges.",
				Sources: cli.EnvVars("JETSTREAM_PLAN_WHOLE_SEGMENT_THRESHOLD"),
				Value:   xrpcapi.DefaultPlanWholeSegmentThreshold,
			},
			&cli.IntFlag{
				Name:    "subscribe-read-log-retention-bytes",
				Usage:   "Byte budget of the writer readable log retained for /subscribe clients.",
				Sources: cli.EnvVars("JETSTREAM_SUBSCRIBE_READ_LOG_RETENTION_BYTES"),
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
			&cli.IntFlag{
				Name:    "compaction-rewrite-workers",
				Usage:   "Maximum sealed segments rewritten concurrently during compaction. 0 uses min(NumCPU, 8).",
				Sources: cli.EnvVars("JETSTREAM_COMPACTION_REWRITE_WORKERS"),
				Value:   0,
			},
		},
		Action: runServe,
	}
	cmd.Flags = append(cmd.Flags, storageFlags()...)
	return cmd
}

// storageCommand groups the commands that administer disaggregated storage.
func storageCommand() *cli.Command {
	return &cli.Command{
		Name:  "storage",
		Usage: "Administer disaggregated storage (PostgreSQL catalog plus S3 objects)",
		Commands: []*cli.Command{
			{
				Name:   "init",
				Usage:  "Create a new archive in the PostgreSQL database and S3 bucket that the storage flags name (design §15.1). Refuses a database that already holds an archive.",
				Flags:  storageInitFlags(),
				Action: runStorageInit,
			},
		},
	}
}

// storageInitFlags is the subset of storageFlags that init reads: the
// connections and the lease it holds while creating the first segments.
func storageInitFlags() []cli.Flag {
	var out []cli.Flag
	for _, f := range storageFlags() {
		name := f.Names()[0]
		if strings.HasPrefix(name, "pg-") || strings.HasPrefix(name, "s3-") || name == "leader-lease" {
			out = append(out, f)
		}
	}
	return out
}

// runStorageInit creates the archive whatever --storage says: init only
// ever targets disaggregated storage, and requiring the flag would add a
// step without catching a mistake.
func runStorageInit(ctx context.Context, cmd *cli.Command) error {
	runCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, _, err := storageConfigFromCommand(cmd)
	if err != nil {
		return err
	}
	archive, err := jetstreamd.InitStorage(runCtx, cfg)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(cmd.Root().Writer, "initialized archive %s\n", objstore.FormatUUID(archive.ArchiveID))
	return err
}

// storageFlagCategory groups the disaggregated-storage flags in --help.
const storageFlagCategory = "Disaggregated storage (design §18)"

// storageFlags declares every design §18 variable. They are declared even
// before the runtime reads them all so that none is rejected as an unknown
// JETSTREAM_* variable. The `storage` subcommands reuse them.
//
// --pg-url holds a password. urfave/cli renders defaults before reading env
// sources, so --help shows no value today; HideDefault keeps it that way if
// that ever changes. Everything else logs it only through
// pgstore.RedactURL.
func storageFlags() []cli.Flag {
	def := jetstreamd.DefaultStorageConfig()
	cat := storageFlagCategory
	return []cli.Flag{
		&cli.StringFlag{
			Name: "storage", Category: cat, Usage: "Storage backend: local (segments and Pebble under --data-dir) or disaggregated (PostgreSQL catalog plus S3 objects, no local disk; --data-dir must be unset)",
			Sources: cli.EnvVars("JETSTREAM_STORAGE"), Value: string(def.Mode),
		},
		&cli.StringFlag{
			Name: "pg-url", Category: cat, Usage: "PostgreSQL connection string (secret: set it in the environment, never in .env)",
			Sources: cli.EnvVars("JETSTREAM_PG_URL"), HideDefault: true,
		},
		&cli.IntFlag{
			Name: "pg-max-conns", Category: cat, Usage: "PostgreSQL connection pool size",
			Sources: cli.EnvVars("JETSTREAM_PG_MAX_CONNS"), Value: def.PG.MaxConns,
		},
		&cli.StringFlag{
			Name: "s3-endpoint", Category: cat, Usage: "S3 endpoint URL; empty uses the AWS default. Credentials come from the AWS SDK default chain (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, web identity, instance metadata)",
			Sources: cli.EnvVars("JETSTREAM_S3_ENDPOINT"),
		},
		&cli.StringFlag{
			Name: "s3-region", Category: cat, Usage: "S3 region",
			Sources: cli.EnvVars("JETSTREAM_S3_REGION"),
		},
		&cli.StringFlag{
			Name: "s3-bucket", Category: cat, Usage: "S3 bucket",
			Sources: cli.EnvVars("JETSTREAM_S3_BUCKET"),
		},
		&cli.StringFlag{
			Name: "s3-prefix", Category: cat, Usage: "S3 key prefix",
			Sources: cli.EnvVars("JETSTREAM_S3_PREFIX"),
		},
		&cli.BoolFlag{
			Name: "s3-path-style", Category: cat, Usage: "Use path-style S3 addressing (SeaweedFS, MinIO)",
			Sources: cli.EnvVars("JETSTREAM_S3_PATH_STYLE"),
		},
		&cli.IntFlag{
			Name: "s3-upload-concurrency", Category: cat, Usage: "Concurrent S3 uploads",
			Sources: cli.EnvVars("JETSTREAM_S3_UPLOAD_CONCURRENCY"), Value: def.S3.UploadConcurrency,
		},
		&cli.IntFlag{
			Name: "s3-read-concurrency", Category: cat, Usage: "Concurrent S3 GETs for footer load and prefetch",
			Sources: cli.EnvVars("JETSTREAM_S3_READ_CONCURRENCY"), Value: def.S3.ReadConcurrency,
		},
		&cli.DurationFlag{
			Name: "s3-retry-timeout", Category: cat, Usage: "Give up on one S3 operation, retries included, after this long",
			Sources: cli.EnvVars("JETSTREAM_S3_RETRY_TIMEOUT"), Value: def.S3.RetryTimeout,
		},
		&cli.DurationFlag{
			Name: "leader-lease", Category: cat, Usage: "Writer lease duration",
			Sources: cli.EnvVars("JETSTREAM_LEADER_LEASE"), Value: def.Leader.Lease,
		},
		&cli.DurationFlag{
			Name: "leader-renew-interval", Category: cat, Usage: "Writer lease renew period; must be shorter than --leader-lease",
			Sources: cli.EnvVars("JETSTREAM_LEADER_RENEW_INTERVAL"), Value: def.Leader.RenewInterval,
		},
		&cli.DurationFlag{
			Name: "leader-acquire-interval", Category: cat, Usage: "Writer lease acquire attempt period",
			Sources: cli.EnvVars("JETSTREAM_LEADER_ACQUIRE_INTERVAL"), Value: def.Leader.AcquireInterval,
		},
		&cli.DurationFlag{
			Name: "hot-batch-max-age", Category: cat, Usage: "Hot-mode batch age cut",
			Sources: cli.EnvVars("JETSTREAM_HOT_BATCH_MAX_AGE"), Value: def.Hot.BatchMaxAge,
		},
		&cli.DurationFlag{
			Name: "block-max-age", Category: cat, Usage: "Hot-mode open block age cut",
			Sources: cli.EnvVars("JETSTREAM_BLOCK_MAX_AGE"), Value: def.BlockMaxAge,
		},
		&cli.IntFlag{
			Name: "hot-inline-bytes-per-sec", Category: cat, Usage: "Live inline token bucket rate, in bytes per second",
			Sources: cli.EnvVars("JETSTREAM_HOT_INLINE_BYTES_PER_SEC"), Value: int(def.Hot.InlineBytesPerSec),
		},
		&cli.IntFlag{
			Name: "hot-bulk-pending-bytes", Category: cat, Usage: "Bulk (backfill and retry) pending-byte permits",
			Sources: cli.EnvVars("JETSTREAM_HOT_BULK_PENDING_BYTES"), Value: int(def.Hot.BulkPendingBytes),
		},
		&cli.IntFlag{
			Name: "hot-pending-bytes", Category: cat, Usage: "Total frozen-but-uncommitted byte cap",
			Sources: cli.EnvVars("JETSTREAM_HOT_PENDING_BYTES"), Value: int(def.Hot.PendingBytes),
		},
		&cli.IntFlag{
			Name: "hot-max-unfolded-events", Category: cat, Usage: "Committed-but-unfolded event cap",
			Sources: cli.EnvVars("JETSTREAM_HOT_MAX_UNFOLDED_EVENTS"), Value: def.Hot.MaxUnfoldedEvents,
		},
		&cli.DurationFlag{
			Name: "catalog-poll-interval", Category: cat, Usage: "Catalog follower poll period; covers a lost NOTIFY",
			Sources: cli.EnvVars("JETSTREAM_CATALOG_POLL_INTERVAL"), Value: def.CatalogPollInterval,
		},
		&cli.DurationFlag{
			Name: "max-view-age", Category: cat, Usage: "Report not ready when the catalog mirror is older than this",
			Sources: cli.EnvVars("JETSTREAM_MAX_VIEW_AGE"), Value: def.MaxViewAge,
		},
		&cli.DurationFlag{
			Name: "max-archive-response-duration", Category: cat, Usage: "Cut off one archive response after this long",
			Sources: cli.EnvVars("JETSTREAM_MAX_ARCHIVE_RESPONSE_DURATION"), Value: def.MaxArchiveResponseDuration,
		},
		&cli.DurationFlag{
			Name: "gc-interval", Category: cat, Usage: "Object GC period",
			Sources: cli.EnvVars("JETSTREAM_GC_INTERVAL"), Value: def.GC.Interval,
		},
		&cli.DurationFlag{
			Name: "gc-delay", Category: cat, Usage: "Age an object must stay unreferenced before GC deletes it",
			Sources: cli.EnvVars("JETSTREAM_GC_DELAY"), Value: def.GC.Delay,
		},
		&cli.DurationFlag{
			Name: "gc-orphan-age", Category: cat, Usage: "Age of an uploading object before GC deletes it",
			Sources: cli.EnvVars("JETSTREAM_GC_ORPHAN_AGE"), Value: def.GC.OrphanAge,
		},
		&cli.IntFlag{
			Name: "object-cache-bytes", Category: cat, Usage: "Compressed object cache budget",
			Sources: cli.EnvVars("JETSTREAM_OBJECT_CACHE_BYTES"), Value: int(def.ObjectCacheBytes),
		},
		&cli.IntFlag{
			Name: "compaction-memory-bytes", Category: cat, Usage: "Compaction working-set budget",
			Sources: cli.EnvVars("JETSTREAM_COMPACTION_MEMORY_BYTES"), Value: int(def.CompactionMemoryBytes),
		},
	}
}

// storageConfigFromCommand resolves the storage flags. In disaggregated mode
// it returns an empty data dir, and refuses an explicitly set one: --data-dir
// has a default, so only IsSet tells a deliberate choice from the default.
func storageConfigFromCommand(cmd *cli.Command) (jetstreamd.StorageConfig, string, error) {
	cfg := jetstreamd.StorageConfig{
		Mode: jetstreamd.StorageMode(cmd.String("storage")),
		PG: jetstreamd.PGConfig{
			URL:      cmd.String("pg-url"),
			MaxConns: cmd.Int("pg-max-conns"),
		},
		S3: jetstreamd.S3Config{
			Endpoint:          cmd.String("s3-endpoint"),
			Region:            cmd.String("s3-region"),
			Bucket:            cmd.String("s3-bucket"),
			Prefix:            cmd.String("s3-prefix"),
			PathStyle:         cmd.Bool("s3-path-style"),
			UploadConcurrency: cmd.Int("s3-upload-concurrency"),
			ReadConcurrency:   cmd.Int("s3-read-concurrency"),
			RetryTimeout:      cmd.Duration("s3-retry-timeout"),
		},
		Leader: jetstreamd.LeaderConfig{
			Lease:           cmd.Duration("leader-lease"),
			RenewInterval:   cmd.Duration("leader-renew-interval"),
			AcquireInterval: cmd.Duration("leader-acquire-interval"),
		},
		Hot: jetstreamd.HotConfig{
			BatchMaxAge:       cmd.Duration("hot-batch-max-age"),
			InlineBytesPerSec: int64(cmd.Int("hot-inline-bytes-per-sec")),
			BulkPendingBytes:  int64(cmd.Int("hot-bulk-pending-bytes")),
			PendingBytes:      int64(cmd.Int("hot-pending-bytes")),
			MaxUnfoldedEvents: cmd.Int("hot-max-unfolded-events"),
		},
		BlockMaxAge:                cmd.Duration("block-max-age"),
		CatalogPollInterval:        cmd.Duration("catalog-poll-interval"),
		MaxViewAge:                 cmd.Duration("max-view-age"),
		MaxArchiveResponseDuration: cmd.Duration("max-archive-response-duration"),
		GC: jetstreamd.GCConfig{
			Interval:  cmd.Duration("gc-interval"),
			Delay:     cmd.Duration("gc-delay"),
			OrphanAge: cmd.Duration("gc-orphan-age"),
		},
		ObjectCacheBytes:      int64(cmd.Int("object-cache-bytes")),
		CompactionMemoryBytes: int64(cmd.Int("compaction-memory-bytes")),
	}
	dataDir := cmd.String("data-dir")
	if cfg.Disaggregated() {
		if cmd.IsSet("data-dir") {
			return jetstreamd.StorageConfig{}, "", fmt.Errorf("serve: JETSTREAM_DATA_DIR (--data-dir) must be unset when JETSTREAM_STORAGE=disaggregated")
		}
		dataDir = ""
	}
	return cfg, dataDir, nil
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

	storage, dataDir, err := storageConfigFromCommand(cmd)
	if err != nil {
		return jetstreamd.Options{}, err
	}

	skipMergeDiscovery := cmd.Bool("skip-merge-discovery")
	if maxBackfillRepos > 0 || len(backfillRepos) > 0 {
		skipMergeDiscovery = true
	}

	return jetstreamd.Options{
		PublicAddr:                     cmd.String("addr"),
		DebugAddr:                      cmd.String("debug-addr"),
		DataDir:                        dataDir,
		Storage:                        storage,
		RelayURL:                       cmd.String("relay-url"),
		PLCURL:                         cmd.String("plc-url"),
		OTelServiceName:                cmd.String("otel-service-name"),
		LogLevel:                       cmd.String("log-level"),
		LogFormat:                      cmd.String("log-format"),
		LogOutput:                      cmd.Root().ErrWriter,
		ShutdownTimeout:                cmd.Duration("shutdown-timeout"),
		ClientDrainTimeout:             cmd.Duration("client-drain-timeout"),
		MaxBackfillRepos:               maxBackfillRepos,
		BackfillWorkers:                cmd.Int("backfill-workers"),
		BackfillGlobalDownloads:        cmd.Int("backfill-global-downloads"),
		BackfillHostWorkers:            cmd.Int("backfill-host-workers-max"),
		BackfillMaxActiveHosts:         cmd.Int("backfill-max-active-hosts"),
		BackfillMaxHosts:               cmd.Int("backfill-max-hosts"),
		BackfillBatchSize:              cmd.Int("backfill-batch-size"),
		BackfillAsyncFlushWorkers:      cmd.Int("backfill-async-flush-workers"),
		BackfillRepos:                  backfillRepos,
		SkipMergeDiscovery:             skipMergeDiscovery,
		FailedRepoRetryInterval:        cmd.Duration("failed-repo-retry-interval"),
		FailedRepoRetryWorkers:         cmd.Int("failed-repo-retry-workers"),
		FailedRepoRetryHostWorkers:     cmd.Int("failed-repo-retry-host-workers"),
		FailedRepoRetryMaxDelay:        cmd.Duration("failed-repo-retry-max-delay"),
		DisableRepoActionRateLimits:    cmd.Bool("disable-repo-action-rate-limits"),
		CursorLookback:                 cmd.Duration("cursor-lookback"),
		CompactionCacheGrace:           cmd.Duration("compaction-cache-grace"),
		PlanMaxDIDs:                    cmd.Int("plan-max-dids"),
		PlanMaxCollections:             cmd.Int("plan-max-collections"),
		PlanMaxEntries:                 cmd.Int("plan-max-entries"),
		PlanWholeSegmentThreshold:      cmd.Float("plan-whole-segment-threshold"),
		SubscribeReadLogRetentionBytes: cmd.Int("subscribe-read-log-retention-bytes"),
		SubscribeBlockCacheBytes:       cmd.Int("subscribe-block-cache-bytes"),
		SubscribeReadBatch:             cmd.Int("subscribe-read-batch"),
		SubscribeSlowWindow:            cmd.Duration("subscribe-slow-window"),
		SubscribeSlowMinRate:           cmd.Float("subscribe-slow-min-rate"),
		CursorBlockIndexCacheSize:      cmd.Int("cursor-block-index-cache-size"),
		CompactionInterval:             cmd.Duration("compaction-interval"),
		CompactionTombstoneCap:         cmd.Int("compaction-tombstone-cap"),
		CompactionRewriteWorkers:       cmd.Int("compaction-rewrite-workers"),
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
	// Run suppresses caller-driven cancellation to nil, so on a clean signal
	// shutdown Close's error (e.g. a run that did not drain in time) is the only failure
	// signal left — it must reach the exit status, not be swallowed.
	closeErr := rt.Close(shutdownCtx)
	if runErr != nil {
		return runErr
	}
	return closeErr
}

func rejectUnknownJetstreamEnvVars(root *cli.Command, environ []string) error {
	unknown := unknownJetstreamEnvVars(root, environ)
	switch len(unknown) {
	case 0:
		return nil
	case 1:
		return fmt.Errorf("unrecognized %s environment variable %s", jetstreamEnvPrefix, unknown[0])
	default:
		return fmt.Errorf("unrecognized %s environment variables: %s", jetstreamEnvPrefix, strings.Join(unknown, ", "))
	}
}

func unknownJetstreamEnvVars(root *cli.Command, environ []string) []string {
	known := knownEnvVars(root)
	seen := make(map[string]struct{})
	var unknown []string
	for _, entry := range environ {
		key, _, _ := strings.Cut(entry, "=")
		if !strings.HasPrefix(key, jetstreamEnvPrefix) {
			continue
		}
		if hasAnyPrefix(key, knownForeignJetstreamEnvPrefixes) {
			continue
		}
		if _, ok := known[key]; ok {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		unknown = append(unknown, key)
	}
	sort.Strings(unknown)
	return unknown
}

func hasAnyPrefix(key string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

func knownEnvVars(root *cli.Command) map[string]struct{} {
	out := make(map[string]struct{})
	walkCommands(root, func(cmd *cli.Command) {
		for _, flag := range cmd.Flags {
			docFlag, ok := flag.(cli.DocGenerationFlag)
			if !ok {
				continue
			}
			for _, key := range docFlag.GetEnvVars() {
				out[key] = struct{}{}
			}
		}
	})
	return out
}

func walkCommands(cmd *cli.Command, visit func(*cli.Command)) {
	visit(cmd)
	for _, sub := range cmd.Commands {
		walkCommands(sub, visit)
	}
}
