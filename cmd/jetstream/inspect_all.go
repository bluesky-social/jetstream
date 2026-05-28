package main

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/status"
	"github.com/urfave/cli/v3"
)

// inspectAllCommand wires up `jetstream inspect-all`. The command is a
// thin shell over status.InspectAll + renderInspectAll: aggregation
// lives in internal/status, this layer only owns CLI flag wiring and
// the text renderer.
//
// Mirrors inspectSegmentCommand: same file structure, same rendering
// conventions (plain text, RFC3339-micros UTC, comma-grouped counts,
// 1024-base byte sizes), same errWriter pattern.
func inspectAllCommand() *cli.Command {
	return &cli.Command{
		Name:  "inspect-all",
		Usage: "Print a database-wide summary of every segment file under --data-dir",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "data-dir",
				Usage:   "Path to the data directory; the inspected trees are <data-dir>/segments and <data-dir>/backfill/live_segments",
				Sources: cli.EnvVars("JETSTREAM_DATA_DIR"),
				Value:   "./data",
			},
			&cli.BoolFlag{
				Name:  "skip-unsealed",
				Usage: "Skip frame-walking active (unsealed) segments. Faster but excludes their events from aggregates; the file size is still counted.",
				Value: false,
			},
			&cli.IntFlag{
				Name:  "collections-truncate",
				Usage: "Truncate the per-collection table when distinct-NSID count exceeds this many rows (0 = no truncation).",
				Value: 100,
			},
		},
		Action: func(_ context.Context, cmd *cli.Command) error {
			truncate := cmd.Int("collections-truncate")
			if truncate < 0 {
				return fmt.Errorf("inspect-all: --collections-truncate must be >= 0, got %d", truncate)
			}

			dataDir := cmd.String("data-dir")
			roots := []string{
				filepath.Join(dataDir, "segments"),
				filepath.Join(dataDir, "backfill", "live_segments"),
			}
			agg, err := status.InspectAll(roots, status.InspectAllOptions{
				SkipUnsealed: cmd.Bool("skip-unsealed"),
			})
			if err != nil {
				return err
			}
			return renderInspectAll(cmd.Root().Writer, dataDir, agg, time.Now().UTC(), truncate)
		},
	}
}

// renderInspectAll writes the human + LLM-pasteable text report to w.
//
// Layout: header, network totals, per-tree summaries, per-collection
// table, optional warnings. Sections are blank-line separated.
// Numbers are decimal with comma group separators; bytes are 1024-base.
// Timestamps are RFC3339 micros UTC.
func renderInspectAll(w io.Writer, dataDir string, agg *status.SegmentAggregate, generatedAt time.Time, truncate int) error {
	bw := &errWriter{w: w}

	bw.printf("inspect-all\n")
	bw.printf("data-dir: %s\n", dataDir)
	bw.printf("generated: %s\n", formatTime(generatedAt))

	renderNetwork(bw, agg.Network)
	renderTrees(bw, agg.Trees)
	renderCollections(bw, agg.Collections, truncate)
	renderWarnings(bw, agg.Warnings)

	return bw.err
}

func renderNetwork(bw *errWriter, n status.NetworkTotals) {
	bw.printf("\nnetwork totals:\n")
	bw.printf("  segments:               %d (%d sealed, %d active)\n",
		n.Segments, n.SealedSegments, n.ActiveSegments)
	bw.printf("  blocks:                 %s\n", humanInt(n.Blocks))
	bw.printf("  events:                 %s\n", humanInt(n.Events))
	bw.printf("  collections:            %d distinct NSIDs\n", n.Collections)
	if n.Events > 0 {
		bw.printf("  seq range:              [%d, %d]\n", n.MinSeq, n.MaxSeq)
		bw.printf("  indexed_at range:       %s → %s\n",
			formatTime(n.MinIndexedAt), formatTime(n.MaxIndexedAt))
	}
	bw.printf("  payload (uncompressed): %s\n", formatBytes(n.UncompressedBytes))
	bw.printf("  payload (compressed):   %s\n", formatBytes(n.CompressedBytes))
	bw.printf("  disk usage:             %s\n", formatBytes(n.DiskBytes))
	if n.CompressedBytes > 0 {
		ratio := float64(n.UncompressedBytes) / float64(n.CompressedBytes)
		bw.printf("  compression ratio:      %.2fx\n", ratio)
	}
}

func renderTrees(bw *errWriter, trees []status.TreeAggregate) {
	bw.printf("\ntrees:\n")
	if len(trees) == 0 {
		bw.printf("  (none)\n")
		return
	}
	for i, t := range trees {
		bw.printf("  [%d] %s\n", i, t.Dir)
		if t.SealedCount+t.ActiveCount == 0 {
			bw.printf("        (empty)\n")
			continue
		}
		bw.printf("        files:        %d sealed + %d active\n", t.SealedCount, t.ActiveCount)
		bw.printf("        events:       %s\n", humanInt(t.EventCount))
		bw.printf("        blocks:       %s\n", humanInt(t.BlockCount))
		if t.EventCount > 0 {
			bw.printf("        seq range:    [%d, %d]\n", t.MinSeq, t.MaxSeq)
			bw.printf("        indexed_at:   %s → %s\n",
				formatTime(t.MinIndexedAt), formatTime(t.MaxIndexedAt))
		}
		if !t.OldestMTime.IsZero() {
			bw.printf("        oldest mtime: %s\n", formatTime(t.OldestMTime))
			bw.printf("        newest mtime: %s\n", formatTime(t.NewestMTime))
		}
		bw.printf("        compressed:   %s\n", formatBytes(t.CompressedBytes))
		bw.printf("        uncompressed: %s\n", formatBytes(t.UncompressedBytes))
		bw.printf("        disk:         %s\n", formatBytes(t.DiskBytes))
		if ls := t.LatestSegment; ls != nil {
			state := "active"
			if ls.Sealed {
				state = "sealed"
			}
			bw.printf("        latest:       idx=%d %s events=%s blocks=%d size=%s\n",
				ls.Index, state, humanInt(ls.EventCount), ls.BlockCount, formatBytes(ls.SizeBytes))
		}
	}
}

func renderCollections(bw *errWriter, cols []status.CollectionAggregate, truncate int) {
	bw.printf("\ncollections (%d distinct NSIDs):\n", len(cols))
	if len(cols) == 0 {
		bw.printf("  (none)\n")
		return
	}

	// Compute column widths from the rows we'll actually print so the
	// table aligns even if NSIDs vary in length. Min widths give a
	// readable header even on a tiny dataset.
	nsidW := len("NSID")
	for _, c := range cols {
		if len(c.NSID) > nsidW {
			nsidW = len(c.NSID)
		}
	}

	emit := func(idx int, c status.CollectionAggregate) {
		bw.printf("  [%3d] %-*s  events: %12s  segments: %5d  blocks: %12s\n",
			idx, nsidW, c.NSID, humanInt(c.EventCount), c.SegmentCount, humanInt(c.BlockCount))
	}

	n := len(cols)
	if truncate == 0 || n <= truncate {
		for i, c := range cols {
			emit(i, c)
		}
		return
	}
	half := truncate / 2
	for i := range half {
		emit(i, cols[i])
	}
	bw.printf("  ... (%d rows omitted) ...\n", n-2*half)
	for i := n - half; i < n; i++ {
		emit(i, cols[i])
	}
}

func renderWarnings(bw *errWriter, warns []string) {
	if len(warns) == 0 {
		return
	}
	bw.printf("\nwarnings (%d):\n", len(warns))
	for _, w := range warns {
		bw.printf("  %s\n", w)
	}
}
