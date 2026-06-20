// Command simulator is a development-only fake atproto network: PLC,
// a single PDS, and a relay (firehose) under one HTTP listener. It
// exists so jetstream can iterate locally without depending on
// bsky.network or plc.directory. Not shipped to users; not in the
// Dockerfile.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/internal/simulator/fanout"
	simhttp "github.com/bluesky-social/jetstream/internal/simulator/http"
	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/urfave/cli/v3"
	"golang.org/x/sync/errgroup"
)

func main() {
	if err := newApp().Run(context.Background(), os.Args); err != nil {
		fmt.Fprintln(os.Stderr, "simulator:", err)
		os.Exit(1)
	}
}

func newApp() *cli.Command {
	return &cli.Command{
		Name:  "simulator",
		Usage: "Local atproto simulator for jetstream development",
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
		Commands: []*cli.Command{serveCommand()},
	}
}

func serveCommand() *cli.Command {
	return &cli.Command{
		Name:  "serve",
		Usage: "Run the simulator (PLC + PDS + relay)",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "addr", Usage: "Public HTTP listener", Sources: cli.EnvVars("JETSTREAM_SIM_ADDR"), Value: ":7777"},
			&cli.StringFlag{Name: "data-dir", Usage: "Pebble db location (must not be ./data)", Sources: cli.EnvVars("JETSTREAM_SIM_DATA_DIR"), Value: "./data/simulator"},
			&cli.BoolFlag{Name: "reset", Usage: "Wipe data-dir before opening; re-bootstraps the world", Sources: cli.EnvVars("JETSTREAM_SIM_RESET")},
			&cli.Uint64Flag{Name: "seed", Usage: "Global RNG seed", Sources: cli.EnvVars("JETSTREAM_SIM_SEED"), Value: 42},
			&cli.IntFlag{Name: "accounts", Usage: "Number of simulated accounts", Sources: cli.EnvVars("JETSTREAM_SIM_ACCOUNTS"), Value: 10000},
			&cli.IntFlag{Name: "initial-records-per-account", Usage: "Records per account at bootstrap", Sources: cli.EnvVars("JETSTREAM_SIM_INITIAL_RECORDS"), Value: 5},
			&cli.FloatFlag{Name: "commits-per-sec", Usage: "Mean live event rate", Sources: cli.EnvVars("JETSTREAM_SIM_COMMITS_PER_SEC"), Value: 10},
			&cli.FloatFlag{Name: "traffic-rate-multiplier", Usage: "Scales commits-per-sec without touching distribution shape", Sources: cli.EnvVars("JETSTREAM_SIM_TRAFFIC_RATE_MULTIPLIER"), Value: 1},
			&cli.IntFlag{Name: "firehose-history", Usage: "Ring-buffered events for cursor replay", Sources: cli.EnvVars("JETSTREAM_SIM_FIREHOSE_HISTORY"), Value: 10000},
			&cli.StringFlag{Name: "public-url", Usage: "Externally-reachable base URL (advertised in DID docs); empty derives from --addr", Sources: cli.EnvVars("JETSTREAM_SIM_PUBLIC_URL"), Value: ""},
			&cli.DurationFlag{Name: "shutdown-timeout", Usage: "Maximum graceful-shutdown time", Value: 30 * time.Second},
		},
		Action: runServe,
	}
}

func runServe(ctx context.Context, cmd *cli.Command) error {
	processLogger, err := obs.BuildLoggerFromStrings(os.Stderr, cmd.String("log-level"), cmd.String("log-format"))
	if err != nil {
		return err
	}
	logger := processLogger.With(slog.String("component", "simulator/main"))
	slog.SetDefault(logger)

	cfg := world.Config{
		DataDir:         cmd.String("data-dir"),
		Reset:           cmd.Bool("reset"),
		Seed:            cmd.Uint64("seed"),
		Accounts:        cmd.Int("accounts"),
		InitialRecords:  cmd.Int("initial-records-per-account"),
		CommitsPerSec:   cmd.Float("commits-per-sec"),
		RateMultiplier:  cmd.Float("traffic-rate-multiplier"),
		FirehoseHistory: cmd.Int("firehose-history"),
	}

	w, err := world.New(ctx, cfg)
	if err != nil {
		return fmt.Errorf("simulator: %w", err)
	}
	defer func() {
		if cerr := w.Close(); cerr != nil {
			logger.Error("world close failed", "err", cerr)
		}
	}()

	wantBootstrap, err := w.EnsureSeed()
	if err != nil {
		return err
	}
	if wantBootstrap {
		if err := w.Bootstrap(ctx, processLogger); err != nil {
			return err
		}
	}

	// Live RNG namespaces away from the bootstrap stream (0xb007) so
	// adding/removing live events in the future doesn't shift bootstrap
	// content; cf. world/bootstrap.go.
	rng := rand.New(rand.NewPCG(cfg.Seed^0xfeedf00d, cfg.Seed^0xc0ffee))
	fan := fanout.New(1024)
	if err := w.AttachRuntime(rng, fan); err != nil {
		return err
	}

	publicURL := cmd.String("public-url")
	if publicURL == "" {
		publicURL = "http://" + bindHost(cmd.String("addr"))
	}

	mux := simhttp.NewHandler(w, publicURL)
	httpSrv := &http.Server{
		Addr:              cmd.String("addr"),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	runCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	g, gctx := errgroup.WithContext(runCtx)

	g.Go(func() error {
		logger.InfoContext(gctx, "http listening", "addr", cmd.String("addr"))
		err := httpSrv.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	})

	g.Go(func() error {
		<-gctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cmd.Duration("shutdown-timeout"))
		defer cancel()
		return httpSrv.Shutdown(shutdownCtx)
	})

	g.Go(func() error {
		return w.RunTraffic(gctx, processLogger)
	})

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

// bindHost converts a Go listen address (":7777", "host:port") into a
// hostname:port pair we can advertise in DID documents. A bare ":port"
// is mapped to localhost — that's what peers running on the same
// machine should target.
func bindHost(addr string) string {
	if addr == "" {
		return "localhost:7777"
	}
	if addr[0] == ':' {
		return "localhost" + addr
	}
	return addr
}
