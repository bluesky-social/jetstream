package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"

	"github.com/bluesky-social/jetstream"
	"github.com/urfave/cli/v3"
)

// subscribeCommand drives the public jetstream client: a live tail by default,
// or a filtered backfill that cuts over to live when a seq bound is given.
func subscribeCommand() *cli.Command {
	return &cli.Command{
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "host",
				Usage: "Jetstream host (bare host, host:port, or http(s):// URL); bare hosts default to https except loopback, which defaults to http",
				Value: "localhost:8080",
			},
			&cli.StringSliceFlag{
				Name:  "collection",
				Usage: "Collection filter (exact NSID or 'ns.*' wildcard); may be repeated",
			},
			&cli.StringSliceFlag{
				Name:  "did",
				Usage: "DID filter; may be repeated",
			},
			&cli.IntFlag{
				Name:  "after-seq",
				Usage: "Backfill lower bound (exclusive). Setting this (even to 0) enables backfill.",
				Value: -1,
			},
			&cli.IntFlag{
				Name:  "before-seq",
				Usage: "Backfill upper bound (inclusive); 0 means unset",
				Value: 0,
			},
			&cli.BoolFlag{
				Name:  "backfill-only",
				Usage: "Dump the matched sealed archive and exit; do not start the live tail. Requires --after-seq and/or --before-seq.",
			},
			&cli.IntFlag{
				Name:  "live-cursor",
				Usage: "Resume a pure live tail from this cursor (ignored when backfilling)",
				Value: 0,
			},
			&cli.IntFlag{
				Name:  "batch-size",
				Usage: "Max events per delivered batch",
				Value: 64,
			},
			&cli.IntFlag{
				Name:  "download-concurrency",
				Usage: "Bounded concurrency for sealed segment/block downloads (0 = auto-size from CPU count)",
				Value: 0,
			},
			&cli.StringFlag{
				Name:  "live-buffer-file",
				Usage: "Path to a durable JSONL live buffer (default: in-memory)",
			},
			&cli.BoolFlag{
				Name:  "print",
				Usage: "Print each event as JSON instead of throughput stats",
			},
			&cli.BoolFlag{
				Name:  "typed-likes-client",
				Usage: "Decode records via the typed fast path (skips the generic map). Requires exactly --collection=app.bsky.feed.like; reports typed-decode throughput.",
			},
			&cli.DurationFlag{
				Name:  "report-interval",
				Usage: "How often to print throughput stats (when not --print)",
				Value: time.Second,
			},
			&cli.DurationFlag{
				Name:  "duration",
				Usage: "Optional total run duration; 0 runs until interrupted",
				Value: 0,
			},
			&cli.IntFlag{
				Name:  "gc-percent",
				Usage: "GOGC target for the run (higher = less frequent GC, more RAM). Default tuned for backfill throughput; ignored if GOGC is set in the environment.",
				Value: defaultGCPercent,
			},
			&cli.StringFlag{
				Name:  "debug-pprof-addr",
				Usage: "If set (e.g. localhost:6061), serve net/http/pprof for memory investigation",
			},
			&cli.DurationFlag{
				Name:  "debug-mem-interval",
				Usage: "If >0, periodically log runtime MemStats + RSS to stderr",
			},
			&cli.StringFlag{
				Name:  "debug-profile-dir",
				Usage: "Directory for heap/goroutine profile dumps (default: temp dir)",
			},
			&cli.IntFlag{
				Name:  "debug-rss-limit-mib",
				Usage: "If >0, a watchdog dumps profiles and exits(0) when RSS exceeds this many MiB, preserving valid pprof data instead of OOM-killing",
			},
		},
		Action: runSubscribe,
	}
}

func runSubscribe(ctx context.Context, cmd *cli.Command) error {
	out := cmd.Root().Writer
	if out == nil {
		out = os.Stdout
	}

	// Tune GC before the run. A backfill is allocation-heavy (one record map +
	// CBOR clone per event) but its live set is small and bounded, so the default
	// GOGC=100 collects far too often relative to available RAM — GC was ~⅓ of
	// client CPU at high decode concurrency (#142). Raising the target trades RAM
	// for throughput. Honors an explicit GOGC env var (we skip tuning then).
	tuneGC(cmd.Int("gc-percent"))

	opts := []jetstream.Option{
		jetstream.WithBatchSize(cmd.Int("batch-size")),
	}
	// --download-concurrency=0 (the default) means "let the library auto-size
	// from the CPU count"; only forward an explicit positive override so the
	// library default applies otherwise.
	if dc := cmd.Int("download-concurrency"); dc > 0 {
		opts = append(opts, jetstream.WithDownloadConcurrency(dc))
	}
	if c := cmd.StringSlice("collection"); len(c) > 0 {
		opts = append(opts, jetstream.WithCollections(c))
	}
	if d := cmd.StringSlice("did"); len(d) > 0 {
		opts = append(opts, jetstream.WithDIDs(d))
	}
	// Reject negative cursors explicitly rather than silently dropping them:
	// --after-seq uses -1 as its documented "unset" sentinel, so only < -1 is
	// invalid; --before-seq and --live-cursor default to 0, so any negative is
	// invalid. Silently ignoring a negative --before-seq would turn a requested
	// bounded backfill into an unbounded one with no signal.
	if before := cmd.Int("before-seq"); before < 0 {
		return fmt.Errorf("--before-seq must be >= 0, got %d", before)
	}
	if lc := cmd.Int("live-cursor"); lc < 0 {
		return fmt.Errorf("--live-cursor must be >= 0, got %d", lc)
	}
	if a := cmd.Int("after-seq"); a < -1 {
		return fmt.Errorf("--after-seq must be >= -1 (-1 = unset), got %d", a)
	}

	if a := cmd.Int("after-seq"); a >= 0 {
		opts = append(opts, jetstream.WithAfterSeq(uint64(a)))
	}
	if before := cmd.Int("before-seq"); before > 0 {
		opts = append(opts, jetstream.WithBeforeSeq(uint64(before)))
	}
	if lc := cmd.Int("live-cursor"); lc > 0 {
		opts = append(opts, jetstream.WithLiveCursor(uint64(lc)))
	}

	// --backfill-only is a one-time archive dump: it requires a backfill bound
	// (--after-seq enables backfill even at 0; --before-seq enables it when > 0).
	// Reject the no-bound case here with a clear CLI message rather than letting
	// the library's validation surface a less actionable error.
	if cmd.Bool("backfill-only") {
		hasBound := cmd.Int("after-seq") >= 0 || cmd.Int("before-seq") > 0
		if !hasBound {
			return fmt.Errorf("--backfill-only requires --after-seq and/or --before-seq")
		}
		opts = append(opts, jetstream.WithBackfillOnly())
	}

	if path := cmd.String("live-buffer-file"); path != "" {
		buf, err := jetstream.NewFileLiveBuffer(path)
		if err != nil {
			return err
		}
		opts = append(opts, jetstream.WithLiveBuffer(buf))
	}

	// --typed-likes-client decodes records straight into bsky.FeedLike via the
	// raw-record fast path (no generic map). It only makes sense for a single
	// known record type, so require exactly --collection=app.bsky.feed.like, and
	// enable WithRawRecords so the engine skips the map build on the workers.
	typedLikes := cmd.Bool("typed-likes-client")
	if typedLikes {
		cols := cmd.StringSlice("collection")
		if len(cols) != 1 || cols[0] != "app.bsky.feed.like" {
			return fmt.Errorf("--typed-likes-client requires exactly --collection=app.bsky.feed.like")
		}
		opts = append(opts, jetstream.WithRawRecords())
	}

	client, err := jetstream.Subscribe(cmd.String("host"), opts...)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	sigCtx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()
	runCtx := sigCtx
	if d := cmd.Duration("duration"); d > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(sigCtx, d)
		defer cancel()
	}

	stopDebug, err := startDebug(runCtx, debugConfig{
		pprofAddr:      cmd.String("debug-pprof-addr"),
		sampleInterval: cmd.Duration("debug-mem-interval"),
		profileDir:     cmd.String("debug-profile-dir"),
		rssLimitMiB:    cmd.Int("debug-rss-limit-mib"),
	})
	if err != nil {
		return err
	}
	defer stopDebug()

	if typedLikes {
		return reportTypedLikeThroughput(runCtx, out, client, cmd.Duration("report-interval"))
	}
	if cmd.Bool("print") {
		return printEvents(runCtx, out, client)
	}
	return reportThroughput(runCtx, out, client, cmd.Duration("report-interval"))
}

// printEvents writes each event as a JSON line.
func printEvents(ctx context.Context, out io.Writer, client *jetstream.Client) error {
	enc := json.NewEncoder(out)
	for batch, err := range client.Events(ctx) {
		if err != nil {
			if errors.Is(err, jetstream.ErrFatal) {
				return fmt.Errorf("event stream aborted: %w", err)
			}
			fmt.Fprintln(os.Stderr, "event error:", err)
			continue
		}
		for _, ev := range batch.Events() {
			if err := enc.Encode(ev); err != nil {
				return err
			}
		}
	}
	return nil
}

// reportThroughput consumes events and prints periodic event/throughput stats.
func reportThroughput(ctx context.Context, out io.Writer, client *jetstream.Client, interval time.Duration) error {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	var events uint64
	var lastCursor uint64
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	start := time.Now()
	lastAt := start
	lastEvents := uint64(0)
	report := func(label string) {
		now := time.Now()
		secs := now.Sub(lastAt).Seconds()
		if secs <= 0 {
			secs = 1
		}
		eps := uint64(float64(events-lastEvents)/secs + 0.5)
		_, _ = fmt.Fprintf(out, "%s elapsed=%s events=%s events_per_second=%s last_cursor=%d\n",
			label, now.Sub(start).Round(time.Second), formatCount(events), formatCount(eps), lastCursor)
		lastAt = now
		lastEvents = events
	}

	done := ctx.Done()
	stream, stop := iterPull(client.Events(ctx))
	defer stop()
	for {
		select {
		case <-done:
			report("final")
			return nil
		case <-ticker.C:
			report("stats")
		case item, ok := <-stream:
			if !ok {
				report("final")
				return nil
			}
			if item.err != nil {
				if errors.Is(item.err, jetstream.ErrFatal) {
					report("final")
					return fmt.Errorf("event stream aborted: %w", item.err)
				}
				fmt.Fprintln(os.Stderr, "event error:", item.err)
				continue
			}
			events += uint64(len(item.batch.Events()))
			if c := item.batch.LastCursor(); c > lastCursor {
				lastCursor = c
			}
		}
	}
}

// streamItem carries one batch-or-error from the iterator pump.
type streamItem struct {
	batch *jetstream.Batch
	err   error
}

// iterPull adapts the push-style Events iterator into a channel so the stats
// loop can also select on a ticker. The returned stop cancels the pump.
func iterPull(seq func(func(*jetstream.Batch, error) bool)) (<-chan streamItem, func()) {
	ch := make(chan streamItem)
	done := make(chan struct{})
	go func() {
		defer close(ch)
		seq(func(b *jetstream.Batch, err error) bool {
			select {
			case ch <- streamItem{batch: b, err: err}:
				return true
			case <-done:
				return false
			}
		})
	}()
	return ch, func() { close(done) }
}
