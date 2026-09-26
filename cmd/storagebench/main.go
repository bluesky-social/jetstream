// Command storagebench measures disaggregated storage (design §22) against
// real PostgreSQL and S3, normally the `just up` environment: `just
// storagebench write` drives the hot writer at production rates, `just
// storagebench footers` times a pod start over a pop1-sized synthetic
// catalog, `just storagebench bootstrap` drives a bootstrapping leader's
// writers and reports fenced transactions per second, and `just storagebench
// retryscan` times the failed-repo retry pass's scan of metadata_kv. Each run creates a scratch database and a fresh object prefix,
// and drops the database when it finishes.
//
// The PostgreSQL URL holds a password, so it is only ever printed through
// pgstore.RedactURL.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/catalog/follower"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/leader"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/internal/objstore"
	"github.com/bluesky-social/jetstream/internal/objstore/objcache"
	"github.com/bluesky-social/jetstream/internal/objstore/protocol"
	"github.com/bluesky-social/jetstream/internal/objstore/s3"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/internal/pgstore"
	"github.com/bluesky-social/jetstream/internal/pgstore/pgfixture"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/urfave/cli/v3"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := newApp().Run(ctx, os.Args); err != nil {
		fmt.Fprintln(os.Stderr, "storagebench:", err)
		os.Exit(1)
	}
}

// stopProfile ends the --cpuprofile profile, if one is running.
var stopProfile = func() {}

func newApp() *cli.Command {
	return &cli.Command{
		Name:  "storagebench",
		Usage: "Measure disaggregated storage against PostgreSQL and S3 (design §22)",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "pg-url",
				Usage:   "PostgreSQL maintenance URL; its role needs CREATEDB. Each run uses a scratch database",
				Sources: cli.EnvVars("JETSTREAM_BENCH_PG_URL"),
			},
			&cli.StringFlag{Name: "s3-endpoint", Sources: cli.EnvVars("JETSTREAM_S3_ENDPOINT")},
			&cli.StringFlag{Name: "s3-region", Value: "us-east-1", Sources: cli.EnvVars("JETSTREAM_S3_REGION")},
			&cli.StringFlag{Name: "s3-bucket", Sources: cli.EnvVars("JETSTREAM_S3_BUCKET")},
			&cli.BoolFlag{Name: "s3-path-style", Value: true, Sources: cli.EnvVars("JETSTREAM_S3_PATH_STYLE")},
			&cli.IntFlag{Name: "pg-max-conns", Value: pgstore.DefaultMaxConns},
			&cli.BoolFlag{Name: "keep", Usage: "Keep the scratch database instead of dropping it"},
			&cli.StringFlag{Name: "log-level", Value: "warn", Usage: "debug|info|warn|error, for the components under test"},
			&cli.StringFlag{Name: "cpuprofile", Usage: "Write a CPU profile of the whole run to this file"},
		},
		Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
			path := cmd.String("cpuprofile")
			if path == "" {
				return ctx, nil
			}
			f, err := os.Create(path)
			if err != nil {
				return ctx, err
			}
			if err := pprof.StartCPUProfile(f); err != nil {
				_ = f.Close()
				return ctx, err
			}
			stopProfile = func() {
				pprof.StopCPUProfile()
				_ = f.Close()
			}
			return ctx, nil
		},
		After: func(context.Context, *cli.Command) error {
			stopProfile()
			return nil
		},
		Commands: []*cli.Command{
			writeCommand(), footersCommand(), calibrateCommand(), bootstrapCommand(), retryScanCommand(),
		},
	}
}

// bench is one run's scratch catalog and object prefix.
type bench struct {
	logger  *slog.Logger
	pgURL   string // the scratch database: holds the password
	pg      *pgstore.Store
	blob    objstore.Blob
	archive catalog.ArchiveRow
	close   func()
}

func openBench(ctx context.Context, cmd *cli.Command) (*bench, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(cmd.String("log-level"))); err != nil {
		return nil, fmt.Errorf("--log-level: %w", err)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	admin := cmd.String("pg-url")
	switch {
	case admin == "":
		return nil, errors.New("--pg-url (JETSTREAM_BENCH_PG_URL) is required")
	case cmd.String("s3-endpoint") == "" || cmd.String("s3-bucket") == "":
		return nil, errors.New("--s3-endpoint and --s3-bucket (JETSTREAM_S3_ENDPOINT, JETSTREAM_S3_BUCKET) are required")
	}

	url, drop, err := pgfixture.CreateDatabase(ctx, admin)
	if err != nil {
		return nil, err
	}
	b := &bench{logger: logger, pgURL: url}
	b.close = func() {
		if b.pg != nil {
			b.pg.Close()
		}
		if cmd.Bool("keep") {
			fmt.Printf("kept scratch database %s\n", pgstore.RedactURL(url))
			return
		}
		dctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := drop(dctx); err != nil {
			fmt.Fprintln(os.Stderr, "storagebench:", err)
		}
	}
	fail := func(err error) (*bench, error) {
		b.close()
		return nil, err
	}
	if b.pg, err = b.openStore(ctx, cmd, nil); err != nil {
		return fail(err)
	}
	var id [16]byte
	_, _ = rand.Read(id[:])
	if err := b.pg.Initialize(ctx, id); err != nil {
		return fail(err)
	}
	if b.archive, err = b.pg.CheckVersions(ctx); err != nil {
		return fail(err)
	}

	var prefix [8]byte
	_, _ = rand.Read(prefix[:])
	// Credentials come from the AWS default chain, as in production.
	b.blob, err = s3.New(ctx, s3.Config{
		Endpoint:  cmd.String("s3-endpoint"),
		Region:    cmd.String("s3-region"),
		Bucket:    cmd.String("s3-bucket"),
		Prefix:    "storagebench/" + hex.EncodeToString(prefix[:]) + "/",
		PathStyle: cmd.Bool("s3-path-style"),
	})
	if err != nil {
		return fail(err)
	}
	if err := objstore.Probe(ctx, b.blob, b.archive.ArchiveID); err != nil {
		return fail(err)
	}
	fmt.Printf("database %s, archive %s, bucket %s at %s\n", pgstore.RedactURL(url),
		objstore.FormatUUID(b.archive.ArchiveID), cmd.String("s3-bucket"), cmd.String("s3-endpoint"))
	return b, nil
}

// openStore opens another pool on the scratch database, as a second pod
// would.
func (b *bench) openStore(ctx context.Context, cmd *cli.Command, m *pgstore.Metrics) (*pgstore.Store, error) {
	return pgstore.Open(ctx, pgstore.Config{URL: b.pgURL, MaxConns: int32(cmd.Int("pg-max-conns")), Metrics: m})
}

// pod is one simulated pod's metrics, on its own registry so two pods can
// share a process.
type pod struct {
	reg      *prometheus.Registry
	catalog  *catalog.Metrics
	protocol *protocol.Metrics
	ingest   *ingest.Metrics
	follower *follower.Metrics
	manifest *manifest.Metrics
	memory   *obs.MemoryMetrics
	leader   *leader.Metrics
}

func newPod() *pod {
	reg := prometheus.NewRegistry()
	return &pod{
		reg:      reg,
		catalog:  catalog.NewMetrics(reg),
		protocol: protocol.NewMetrics(reg),
		ingest:   ingest.NewMetrics(reg),
		follower: follower.NewMetrics(reg),
		manifest: manifest.NewMetrics(reg),
		memory:   obs.NewMemoryMetrics(reg),
		leader:   leader.NewMetrics(reg),
	}
}

// newFollower builds a pod's catalog follower over db, with a remote
// manifest so it loads footers the way a pod does at start.
func (b *bench) newFollower(p *pod, db *pgstore.Store, cache *objcache.Cache, pollInterval, maxViewAge time.Duration, readConcurrency int) (*follower.Follower, *manifest.Manifest, error) {
	mft, err := manifest.NewRemote(manifest.Options{Logger: b.logger, Metrics: p.manifest})
	if err != nil {
		return nil, nil, err
	}
	f, err := follower.New(follower.Config{
		DB:              db,
		Listener:        db,
		Blob:            b.blob,
		ArchiveID:       b.archive.ArchiveID,
		Cache:           cache,
		Manifest:        mft,
		PollInterval:    pollInterval,
		MaxViewAge:      maxViewAge,
		ReadConcurrency: readConcurrency,
		ReadLogBytes:    256 << 20,
		Logger:          b.logger,
		Metrics:         p.follower,
		CatalogMetrics:  p.catalog,
		ProtocolMetrics: p.protocol,
		IngestMetrics:   p.ingest,
	})
	if err != nil {
		return nil, nil, err
	}
	return f, mft, nil
}
