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
				Usage: "Bounded concurrency for sealed segment/block downloads",
				Value: 8,
			},
			&cli.StringFlag{
				Name:  "live-buffer-file",
				Usage: "Path to a durable JSONL live buffer (default: in-memory)",
			},
			&cli.BoolFlag{
				Name:  "print",
				Usage: "Print each event as JSON instead of throughput stats",
			},
			&cli.DurationFlag{
				Name:  "report-interval",
				Usage: "How often to print throughput stats (when not --print)",
				Value: 5 * time.Second,
			},
			&cli.DurationFlag{
				Name:  "duration",
				Usage: "Optional total run duration; 0 runs until interrupted",
				Value: 0,
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

	opts := []jetstream.Option{
		jetstream.WithBatchSize(cmd.Int("batch-size")),
		jetstream.WithDownloadConcurrency(cmd.Int("download-concurrency")),
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

	if path := cmd.String("live-buffer-file"); path != "" {
		buf, err := jetstream.NewFileLiveBuffer(path)
		if err != nil {
			return err
		}
		opts = append(opts, jetstream.WithLiveBuffer(buf))
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
		eps := float64(events-lastEvents) / secs
		_, _ = fmt.Fprintf(out, "%s elapsed=%s events=%s eps=%.0f last_cursor=%d\n",
			label, now.Sub(start).Round(time.Second), formatCount(events), eps, lastCursor)
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
