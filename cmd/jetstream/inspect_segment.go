package main

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/urfave/cli/v3"
)

// inspectSegmentCommand wires up `jetstream inspect-segment <path>`.
//
// The command is a thin shell over segment.Inspect + renderInspection:
// all parsing and aggregation lives in the segment package; this layer
// only owns CLI flag wiring and the text renderer.
func inspectSegmentCommand() *cli.Command {
	return &cli.Command{
		Name:      "inspect-segment",
		Usage:     "Print a plain-text summary of a sealed or active segment file",
		ArgsUsage: "<path>",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "blocks",
				Usage: "Per-block detail level: summary | table | full",
				Value: "table",
			},
			&cli.IntFlag{
				Name:  "blocks-truncate",
				Usage: "Truncate the per-block table when block_count exceeds this many rows (0 = no truncation)",
				Value: 100,
			},
		},
		Action: func(_ context.Context, cmd *cli.Command) error {
			args := cmd.Args()
			if args.Len() != 1 {
				return fmt.Errorf("inspect-segment: expected exactly one path argument, got %d", args.Len())
			}
			path := args.First()

			mode := cmd.String("blocks")
			switch mode {
			case "summary", "table", "full":
			default:
				return fmt.Errorf("inspect-segment: --blocks must be one of summary|table|full, got %q", mode)
			}
			truncate := cmd.Int("blocks-truncate")
			if truncate < 0 {
				return fmt.Errorf("inspect-segment: --blocks-truncate must be >= 0, got %d", truncate)
			}

			ins, err := segment.Inspect(path)
			if err != nil {
				return err
			}
			return renderInspection(cmd.Root().Writer, ins, mode, truncate)
		},
	}
}

// renderInspection writes the human + LLM-pasteable text report for ins to w.
//
// Layout: header summary, footer layout, collections, blocks. Sections
// are blank-line separated. Numbers are decimal except absolute file
// offsets (always 0x-hex). Timestamps are RFC3339 micros in UTC.
func renderInspection(w io.Writer, ins *segment.Inspection, blocksMode string, blocksTruncate int) error {
	bw := &errWriter{w: w}

	bw.printf("file: %s\n", ins.Path)
	bw.printf("size: %d bytes\n", ins.FileSize)
	if ins.Sealed {
		bw.printf("state: sealed\n")
	} else {
		bw.printf("state: active (unsealed; block walk)\n")
	}
	bw.printf("magic: jss0\n")
	if ins.Sealed {
		bw.printf("version: %d\n", ins.Header.Version)
		valid := "valid"
		if !ins.ChecksumValid {
			valid = "invalid"
		}
		bw.printf("checksum: 0x%016x (%s)\n", ins.Header.Checksum, valid)
	} else {
		bw.printf("version: -\n")
		bw.printf("checksum: 0x0 (active)\n")
	}

	if ins.PartialError != nil {
		bw.printf("\nWARNING: partial inspection — %v\n", ins.PartialError)
	}

	bw.printf("\nheader summary:\n")
	if ins.Sealed {
		bw.printf("  block_count:       %d\n", ins.Header.BlockCount)
		bw.printf("  event_count:       %d\n", ins.Header.EventCount)
		bw.printf("  unique_did_count:  %d\n", ins.Header.UniqueDIDCount)
	} else {
		bw.printf("  block_count:       %d (discovered via block walk)\n", len(ins.Blocks))
		bw.printf("  event_count:       %d (from walk)\n", ins.TotalEvents)
		bw.printf("  unique_did_count:  %d (from walk; not durable until seal)\n", ins.UniqueDIDCount)
	}
	bw.printf("  seq range:         [%d, %d]\n", ins.MinSeq, ins.MaxSeq)
	bw.printf("  indexed_at range:  [%s, %s]\n",
		formatMicros(ins.MinIndexedAt), formatMicros(ins.MaxIndexedAt))

	bw.printf("\n")
	if ins.Sealed {
		bw.printf("footer layout (all offsets absolute; block_index_offset is also the footer start):\n")
		bw.printf("  block_index_offset:      0x%016x  block_index_size:       %d bytes\n",
			ins.Header.BlockIndexOffset, ins.BlockIndexBytes)
		bw.printf("  did_bloom_offset:        0x%016x  segment_bloom_size:     %d bytes\n",
			ins.Header.DIDBloomOffset, ins.SegmentBloomBytes)
		bw.printf("  block_did_bloom_offset:  0x%016x  per_block_blooms:       %d x %d bytes (incl. 8B region header)\n",
			ins.Header.BlockDIDBloomOffset, ins.Header.BlockCount, ins.PerBlockBloomBytes)
		bw.printf("  collection_index_offset: 0x%016x  collection_index_size:  %d bytes\n",
			ins.Header.CollectionIndexOffset, ins.CollectionIndexBytes)
		bw.printf("  end_of_file:             0x%016x\n", uint64(ins.FileSize))
	} else {
		bw.printf("footer layout: not present (active file)\n")
	}

	bw.printf("\ncollections (%d distinct NSIDs):\n", len(ins.Collections))
	if len(ins.Collections) == 0 {
		bw.printf("  (none)\n")
	} else {
		blockCounts := make([]int, len(ins.Collections))
		for _, ids := range ins.BlockCollections {
			for _, id := range ids {
				if int(id) < len(blockCounts) {
					blockCounts[id]++
				}
			}
		}
		// Inspection guarantees CollectionEventCounts is parallel-indexed
		// with Collections (segment.Inspect populates both from the same
		// source). A length mismatch would indicate a bug upstream;
		// surface it rather than silently zero-fill so the operator can
		// see something is wrong with the file.
		eventCounts := ins.CollectionEventCounts
		if len(eventCounts) != len(ins.Collections) {
			return fmt.Errorf(
				"inspect-segment: CollectionEventCounts len %d != Collections len %d",
				len(eventCounts), len(ins.Collections))
		}
		nsidWidth := 0
		eventsWidth := len("events")
		blocksWidth := len("blocks")
		for i, n := range ins.Collections {
			if len(n) > nsidWidth {
				nsidWidth = len(n)
			}
			if w := len(strconv.FormatUint(uint64(eventCounts[i]), 10)); w > eventsWidth {
				eventsWidth = w
			}
			if w := len(strconv.Itoa(blockCounts[i])); w > blocksWidth {
				blocksWidth = w
			}
		}
		// Sort by descending event count so the operator's eye lands on
		// the noisiest collections first. The original index in
		// ins.Collections is preserved as the printed [id] so cross-refs
		// to per-block "collections:" lists in --blocks=full still match.
		order := make([]int, len(ins.Collections))
		for i := range order {
			order[i] = i
		}
		sort.SliceStable(order, func(a, b int) bool {
			return eventCounts[order[a]] > eventCounts[order[b]]
		})
		for _, i := range order {
			bw.printf("  [%3d] %-*s  events: %*d  blocks: %*d\n",
				i, nsidWidth, ins.Collections[i],
				eventsWidth, eventCounts[i],
				blocksWidth, blockCounts[i])
		}
	}

	if blocksMode == "summary" {
		return bw.err
	}

	bw.printf("\nblocks (%d total):\n", len(ins.Blocks))
	bw.printf("  idx       offset  comp_size  uncomp_size  events     min_seq     max_seq                  min_indexed_at                  max_indexed_at  cols\n")

	emitRow := func(i int) {
		b := ins.Blocks[i]
		cols := 0
		if i < len(ins.BlockCollections) {
			cols = len(ins.BlockCollections[i])
		}
		bw.printf("  %3d  0x%010x  %9d  %11d  %6d  %10d  %10d  %30s  %30s  %4d\n",
			i, b.Offset, b.CompressedSize, b.UncompressedSize,
			b.EventCount, b.MinSeq, b.MaxSeq,
			formatMicros(b.MinIndexedAt), formatMicros(b.MaxIndexedAt),
			cols)
		if blocksMode == "full" && i < len(ins.BlockCollections) && len(ins.BlockCollections[i]) > 0 {
			names := make([]string, 0, len(ins.BlockCollections[i]))
			for _, id := range ins.BlockCollections[i] {
				if int(id) < len(ins.Collections) {
					names = append(names, ins.Collections[id])
				}
			}
			bw.printf("       collections: %s\n", strings.Join(names, ", "))
		}
	}

	n := len(ins.Blocks)
	if blocksTruncate == 0 || blocksMode == "full" || n <= blocksTruncate {
		for i := range ins.Blocks {
			emitRow(i)
		}
	} else {
		half := blocksTruncate / 2
		for i := range half {
			emitRow(i)
		}
		bw.printf("  ... (%d rows omitted) ...\n", n-2*half)
		for i := n - half; i < n; i++ {
			emitRow(i)
		}
	}

	return bw.err
}
