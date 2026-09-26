package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/urfave/cli/v3"
)

func calibrateCommand() *cli.Command {
	return &cli.Command{
		Name:  "calibrate",
		Usage: "Encode synthetic blocks offline and print their shape, to compare against production segments",
		Flags: []cli.Flag{
			&cli.IntFlag{Name: "blocks", Value: 16},
			&cli.Uint64Flag{Name: "seed", Value: 1},
			&cli.Uint64Flag{Name: "did-universe", Value: 40_000_000, Validator: atLeastOne[uint64]},
		},
		Action: runCalibrate,
	}
}

func runCalibrate(_ context.Context, cmd *cli.Command) error {
	gen := newGenerator(cmd.Uint64("seed"), cmd.Uint64("did-universe"))
	n := cmd.Int("blocks")
	now := time.Now()
	for _, profile := range []struct {
		name string
		next func(k int) []segment.Event
	}{
		{"live", func(int) []segment.Event {
			out := make([]segment.Event, segment.DefaultMaxEventsPerBlock)
			for i := range out {
				out[i] = gen.live(now, int64(i+1))
			}
			return out
		}},
		{"bulk", func(int) []segment.Event {
			var out []segment.Event
			for len(out) < segment.DefaultMaxEventsPerBlock {
				out = append(out, gen.repo(now, gen.repoSize())...)
			}
			return out[:segment.DefaultMaxEventsPerBlock]
		}},
	} {
		bb, err := segment.NewBlockBuilder(0)
		if err != nil {
			return err
		}
		var (
			compressed, uncompressed, events, dids int
			colls                                  = map[string]int{}
		)
		for k := range n {
			seen := map[string]struct{}{}
			for _, ev := range profile.next(k) {
				if _, err := bb.Append(ev); err != nil {
					return err
				}
				seen[ev.DID] = struct{}{}
				colls[ev.Collection]++
			}
			frame, info := bb.Encode()
			compressed += len(frame)
			uncompressed += int(info.UncompressedSize)
			events += int(info.EventCount)
			dids += len(seen)
		}
		fmt.Printf("%s: %d blocks of %d events\n", profile.name, n, segment.DefaultMaxEventsPerBlock)
		rows := [][2]string{
			{"compressed bytes/event", fmt.Sprintf("%.1f", float64(compressed)/float64(events))},
			{"uncompressed bytes/event", fmt.Sprintf("%.1f", float64(uncompressed)/float64(events))},
			{"compressed bytes/block", fmt.Sprintf("%.0f", float64(compressed)/float64(n))},
			{"blocks per 256MiB segment", fmt.Sprintf("%.0f", float64(256<<20)/(float64(compressed)/float64(n)+8))},
			{"unique DIDs/block", fmt.Sprintf("%.0f", float64(dids)/float64(n))},
		}
		for _, c := range sortedKeys(colls) {
			if share := float64(colls[c]) / float64(events); share >= 0.005 {
				rows = append(rows, [2]string{c, fmt.Sprintf("%.3f", share)})
			}
		}
		printKV(os.Stdout, rows)
	}
	return nil
}
