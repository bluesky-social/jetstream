// Package status — InspectAll aggregates segment files under one or
// more on-disk roots into a single rendering-agnostic value. It is
// the shared backbone of the /status HTTP page (via Collector.build)
// and the `jetstream inspect-all` CLI subcommand.
//
// InspectAll has no Pebble dependency: it walks the filesystem and
// calls segment.Inspect per file. The CLI can therefore call it
// against a data dir on a host where no jetstream serve process is
// running.
package status

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/segment"
)

// SegmentAggregate is the rendering-agnostic, database-wide view of
// all segment files under one or more roots. Built by InspectAll;
// consumed by Collector.build (for the /status snapshot) and the
// inspect-all CLI renderer.
type SegmentAggregate struct {
	// Trees is one entry per input root, in input order. A missing
	// or empty root yields an entry with zero counters.
	Trees []TreeAggregate

	// Collections is the per-NSID rollup across all trees. Sorted by
	// EventCount descending with NSID ascending as tiebreak.
	Collections []CollectionAggregate

	// Network is the database-wide rollup across all trees.
	Network NetworkTotals

	// Warnings is one entry per per-file Inspect failure that was
	// tolerated and excluded from the aggregates. The highest-idx
	// file in each tree may fail silently (rotation race) and does
	// not produce a warning. Format: "<path>: <err>".
	Warnings []string
}

// TreeAggregate is a per-root rollup. Replaces the old
// SegmentTreeStats; supersets it with the new aggregate counters.
type TreeAggregate struct {
	Dir               string
	SealedCount       int
	ActiveCount       int
	CompressedBytes   int64 // sum of block compressed sizes
	UncompressedBytes int64 // sum of block uncompressed sizes
	DiskBytes         int64 // sum of file sizes (incl. headers/footer/indexes)
	EventCount        uint64
	BlockCount        uint64
	OldestMTime       time.Time
	NewestMTime       time.Time
	MinSeq            uint64 // 0 if no records
	MaxSeq            uint64
	MinIndexedAt      time.Time // zero if no records
	MaxIndexedAt      time.Time
	LatestSegment     *SegmentSummary
}

// CollectionAggregate is one row per distinct NSID seen anywhere in
// the scanned trees. SegmentCount/BlockCount only count segments and
// blocks that actually mention this NSID.
type CollectionAggregate struct {
	NSID         string
	EventCount   uint64
	SegmentCount int
	BlockCount   uint64
}

// NetworkTotals is the database-wide rollup across all trees.
type NetworkTotals struct {
	Segments          int
	SealedSegments    int
	ActiveSegments    int
	Blocks            uint64
	Events            uint64
	Collections       int
	CompressedBytes   int64
	UncompressedBytes int64
	DiskBytes         int64
	MinSeq            uint64
	MaxSeq            uint64
	MinIndexedAt      time.Time
	MaxIndexedAt      time.Time
}

// InspectAllOptions controls the scan.
type InspectAllOptions struct {
	// SkipUnsealed skips the active-file frame walk for any segment
	// whose header checksum is zero. The file is still counted (size
	// + ActiveCount++) but no per-block or per-collection data is
	// folded in. Useful for fast operator surveys.
	SkipUnsealed bool
}

// InspectAll walks each root in roots, calls segment.Inspect on every
// seg_*.jss file, and folds the results into a *SegmentAggregate.
//
// A root that does not exist on disk yields a TreeAggregate with the
// dir set and all counters zero — not an error. A root that exists
// but cannot be readdir'd is fatal.
//
// Per-file segment.Inspect failures are tolerated and recorded in
// SegmentAggregate.Warnings; the failing file is excluded from
// aggregates. The single highest-idx file in each tree is allowed to
// fail silently to tolerate a rotation race during a live scan.
func InspectAll(roots []string, opts InspectAllOptions) (*SegmentAggregate, error) {
	agg := &SegmentAggregate{}
	collections := make(map[string]*CollectionAggregate)

	for _, root := range roots {
		tree, warns, err := scanTree(root, opts, collections)
		if err != nil {
			return nil, err
		}
		agg.Trees = append(agg.Trees, tree)
		agg.Warnings = append(agg.Warnings, warns...)
	}

	agg.Collections = materializeCollections(collections)
	agg.Network = computeNetworkTotals(agg.Trees, len(agg.Collections))
	return agg, nil
}

// scanTree readdirs root, calls segment.Inspect on each seg_*.jss
// file, folds results into a TreeAggregate, and updates the shared
// collections map. Per-file errors update agg.Warnings via a closure
// passed in by the caller — but for v1 we keep warnings local and
// return them from scanTree, then merge in InspectAll.
func scanTree(root string, opts InspectAllOptions, collections map[string]*CollectionAggregate) (TreeAggregate, []string, error) {
	tree := TreeAggregate{Dir: root}
	var warnings []string

	entries, err := os.ReadDir(root)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return tree, nil, nil
		}
		return TreeAggregate{}, nil, fmt.Errorf("status: readdir %s: %w", root, err)
	}

	type segFile struct {
		idx  uint64
		path string
		info os.FileInfo
	}
	var files []segFile
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		idx, ok := ingest.ParseSegmentIndex(e.Name())
		if !ok {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			return TreeAggregate{}, nil, fmt.Errorf("status: stat %s: %w", e.Name(), err)
		}
		files = append(files, segFile{idx: idx, path: filepath.Join(root, e.Name()), info: fi})
	}
	if len(files) == 0 {
		return tree, nil, nil
	}
	sort.Slice(files, func(i, j int) bool { return files[i].idx < files[j].idx })

	tree.OldestMTime = files[0].info.ModTime()
	tree.NewestMTime = files[0].info.ModTime()

	tailIdx := len(files) - 1
	for i, f := range files {
		mt := f.info.ModTime()
		if mt.Before(tree.OldestMTime) {
			tree.OldestMTime = mt
		}
		if mt.After(tree.NewestMTime) {
			tree.NewestMTime = mt
		}

		ins, err := segment.Inspect(f.path)
		if err != nil {
			// Tail rotation race: silently skip.
			if i != tailIdx {
				warnings = append(warnings, fmt.Sprintf("%s: %v", f.path, err))
			}
			continue
		}

		tree.DiskBytes += ins.FileSize
		if ins.Sealed {
			tree.SealedCount++
		} else {
			tree.ActiveCount++
		}

		// LatestSegment: highest-idx file that we successfully
		// inspected. Loop is in idx-asc order, so unconditional set.
		// This runs even under SkipUnsealed because the per-file summary
		// is already in `ins` regardless of whether we fold its blocks.
		tree.LatestSegment = &SegmentSummary{
			Index:           f.idx,
			Sealed:          ins.Sealed,
			EventCount:      ins.TotalEvents,
			UniqueDIDCount:  ins.UniqueDIDCount,
			BlockCount:      uint32(len(ins.Blocks)),
			CollectionCount: len(ins.Collections),
			MinSeq:          ins.MinSeq,
			MaxSeq:          ins.MaxSeq,
			MinIndexedAt:    microsToTime(ins.MinIndexedAt),
			MaxIndexedAt:    microsToTime(ins.MaxIndexedAt),
			SizeBytes:       ins.FileSize,
		}

		// SkipUnsealed: still count file size + ActiveCount above and
		// keep LatestSegment populated, but skip per-block / per-collection
		// folding for the cheap-survey use case.
		if !ins.Sealed && opts.SkipUnsealed {
			continue
		}

		foldInspection(&tree, ins, collections)
	}

	return tree, warnings, nil
}

// foldInspection accumulates one segment's Inspection into the
// running tree aggregate and updates the shared per-NSID collections
// map. Callers handle bookkeeping concerns (warnings, file sizing,
// LatestSegment) — this function only owns the per-block and
// per-collection arithmetic.
func foldInspection(tree *TreeAggregate, ins *segment.Inspection, collections map[string]*CollectionAggregate) {
	tree.EventCount += ins.TotalEvents
	tree.BlockCount += uint64(len(ins.Blocks))

	for _, b := range ins.Blocks {
		tree.CompressedBytes += int64(b.CompressedSize)
		tree.UncompressedBytes += int64(b.UncompressedSize)
	}

	if ins.TotalEvents > 0 {
		if tree.MinSeq == 0 || ins.MinSeq < tree.MinSeq {
			tree.MinSeq = ins.MinSeq
		}
		if ins.MaxSeq > tree.MaxSeq {
			tree.MaxSeq = ins.MaxSeq
		}
		minIA := microsToTime(ins.MinIndexedAt)
		maxIA := microsToTime(ins.MaxIndexedAt)
		if !minIA.IsZero() && (tree.MinIndexedAt.IsZero() || minIA.Before(tree.MinIndexedAt)) {
			tree.MinIndexedAt = minIA
		}
		if maxIA.After(tree.MaxIndexedAt) {
			tree.MaxIndexedAt = maxIA
		}
	}

	// Per-block per-NSID block contribution: walk
	// ins.BlockCollections once, count blocks per NSID-id, then
	// add into collections[NSID].BlockCount. This guarantees one
	// block-count increment per (segment, block) pair that contains
	// the NSID; no double-counting.
	blockCountsByID := make(map[uint32]uint64, len(ins.Collections))
	for _, ids := range ins.BlockCollections {
		for _, id := range ids {
			blockCountsByID[id]++
		}
	}

	// Per-NSID event + block rollup. ins.Collections is bounded by the
	// segment file format — collection IDs in BlockCollections are
	// uint32 — so len(ins.Collections) <= MaxUint32 holds and the
	// uint32(i) cast below is safe.
	for i, nsid := range ins.Collections {
		agg, ok := collections[nsid]
		if !ok {
			agg = &CollectionAggregate{NSID: nsid}
			collections[nsid] = agg
		}
		var events uint32
		if i < len(ins.CollectionEventCounts) {
			events = ins.CollectionEventCounts[i]
		}
		agg.EventCount += uint64(events)
		agg.SegmentCount++
		agg.BlockCount += blockCountsByID[uint32(i)]
	}
}

// materializeCollections converts the scan-time map into a slice
// sorted by EventCount desc with NSID asc tiebreak.
func materializeCollections(m map[string]*CollectionAggregate) []CollectionAggregate {
	if len(m) == 0 {
		return nil
	}
	out := make([]CollectionAggregate, 0, len(m))
	for _, v := range m {
		out = append(out, *v)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].EventCount != out[j].EventCount {
			return out[i].EventCount > out[j].EventCount
		}
		return out[i].NSID < out[j].NSID
	})
	return out
}

// computeNetworkTotals sums the per-tree counters into a single
// NetworkTotals. Bounds (MinSeq/MinIndexedAt/MaxSeq/MaxIndexedAt) are
// only contributed by trees whose own counters are non-zero so empty
// trees do not pull min bounds to zero.
func computeNetworkTotals(trees []TreeAggregate, collectionCount int) NetworkTotals {
	tot := NetworkTotals{Collections: collectionCount}
	for _, t := range trees {
		tot.Segments += t.SealedCount + t.ActiveCount
		tot.SealedSegments += t.SealedCount
		tot.ActiveSegments += t.ActiveCount
		tot.Blocks += t.BlockCount
		tot.Events += t.EventCount
		tot.CompressedBytes += t.CompressedBytes
		tot.UncompressedBytes += t.UncompressedBytes
		tot.DiskBytes += t.DiskBytes

		if t.EventCount == 0 {
			continue
		}
		if tot.MinSeq == 0 || t.MinSeq < tot.MinSeq {
			tot.MinSeq = t.MinSeq
		}
		if t.MaxSeq > tot.MaxSeq {
			tot.MaxSeq = t.MaxSeq
		}
		if !t.MinIndexedAt.IsZero() && (tot.MinIndexedAt.IsZero() || t.MinIndexedAt.Before(tot.MinIndexedAt)) {
			tot.MinIndexedAt = t.MinIndexedAt
		}
		if t.MaxIndexedAt.After(tot.MaxIndexedAt) {
			tot.MaxIndexedAt = t.MaxIndexedAt
		}
	}
	return tot
}

func microsToTime(us int64) time.Time {
	if us == 0 {
		return time.Time{}
	}
	return time.UnixMicro(us).UTC()
}
