package status

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream-v2/internal/ingest/live"
	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
	"github.com/bluesky-social/jetstream-v2/internal/manifest"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/internal/version"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/cockroachdb/pebble"
)

// keyspacePrefixes lists the pebble prefixes the status page exposes
// in PebbleStats.KeyspaceCounts. sync/identity/ is intentionally
// excluded from the public surface.
var keyspacePrefixes = []string{
	"repo/",
	"sync/chain/",
	"sync/host/",
	"relay/",
}

func collectProcess(now time.Time, startedAt time.Time) ProcessInfo {
	info := version.Get()
	return ProcessInfo{
		Version:   info.Version,
		Commit:    info.Commit,
		BuiltAt:   info.Date,
		StartedAt: startedAt,
		Uptime:    now.Sub(startedAt),
		GoVersion: runtime.Version(),
	}
}

func collectPhase(s *store.Store) (PhaseInfo, error) {
	p, err := lifecycle.ReadPhase(s)
	if err != nil {
		return PhaseInfo{}, err
	}
	at, err := lifecycle.ReadPhaseEnteredAt(s)
	if err != nil {
		return PhaseInfo{}, err
	}
	return PhaseInfo{Phase: p, PhaseEnteredAt: at}, nil
}

func collectLive(s *store.Store) (LiveStats, error) {
	cur, err := live.LoadUpstreamCursor(s, live.CursorKey)
	if err != nil {
		return LiveStats{}, err
	}
	nextSeq, _, err := s.GetUint64LE(live.SteadySeqKey)
	if err != nil {
		return LiveStats{}, err
	}
	bootSeq, _, err := s.GetUint64LE(live.BootstrapSeqKey)
	if err != nil {
		return LiveStats{}, err
	}
	return LiveStats{
		UpstreamCursor: cur,
		NextSeq:        nextSeq,
		BootstrapSeq:   bootSeq,
	}, nil
}

func collectBackfill(s *store.Store) (BackfillStats, error) {
	counts, err := backfill.CountStatuses(s)
	if err != nil {
		return BackfillStats{}, err
	}
	cursor, err := backfill.LoadListReposCursor(s)
	if err != nil {
		return BackfillStats{}, err
	}
	pct := 0.0
	if counts.Total > 0 {
		pct = float64(counts.Complete) / float64(counts.Total) * 100
	}
	return BackfillStats{
		TotalDIDs:       counts.Total,
		Discovered:      counts.Discovered,
		Complete:        counts.Complete,
		Failed:          counts.Failed,
		PercentComplete: pct,
		ListReposCursor: cursor,
	}, nil
}

func collectBackfillFast(s *store.Store) (BackfillStats, error) {
	cursor, err := backfill.LoadListReposCursor(s)
	if err != nil {
		return BackfillStats{}, err
	}
	// Exact counts require scanning every repo/<did> row. On production
	// instances /status uses only the optional precomputed aggregate so
	// snapshot builds stay cheap; missing aggregates render as zeros.
	counts, ok, err := backfill.LoadCounts(s)
	if err != nil {
		return BackfillStats{}, err
	}
	pct := 0.0
	if ok && counts.Total > 0 {
		pct = float64(counts.Complete) / float64(counts.Total) * 100
	}
	return BackfillStats{
		TotalDIDs:       counts.Total,
		Discovered:      counts.Discovered,
		Complete:        counts.Complete,
		Failed:          counts.Failed,
		PercentComplete: pct,
		ListReposCursor: cursor,
	}, nil
}

func collectManifestSegmentTree(ms manifest.SegmentTreeStats) SegmentTreeStats {
	stats := SegmentTreeStats{
		Dir:               ms.Dir,
		SealedCount:       ms.SealedCount,
		CompressedBytes:   ms.CompressedBytes,
		UncompressedBytes: ms.UncompressedBytes,
		OldestMTime:       ms.OldestMTime,
		NewestMTime:       ms.NewestMTime,
	}
	if ms.LatestSegment != nil {
		stats.LatestSegment = &SegmentSummary{
			Index:           ms.LatestSegment.Index,
			Sealed:          true,
			EventCount:      ms.LatestSegment.EventCount,
			UniqueDIDCount:  ms.LatestSegment.UniqueDIDCount,
			BlockCount:      ms.LatestSegment.BlockCount,
			CollectionCount: ms.LatestSegment.CollectionCount,
			MinSeq:          ms.LatestSegment.MinSeq,
			MaxSeq:          ms.LatestSegment.MaxSeq,
			MinIndexedAt:    microsToTime(ms.LatestSegment.MinIndexedAt),
			MaxIndexedAt:    microsToTime(ms.LatestSegment.MaxIndexedAt),
			SizeBytes:       ms.LatestSegment.SizeBytes,
		}
	}
	return stats
}

func collectSegmentTree(dir string) (SegmentTreeStats, error) {
	stats := SegmentTreeStats{Dir: dir}

	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return stats, nil
		}
		return stats, fmt.Errorf("status: readdir %s: %w", dir, err)
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
			return stats, fmt.Errorf("status: stat %s: %w", e.Name(), err)
		}
		files = append(files, segFile{idx: idx, path: filepath.Join(dir, e.Name()), info: fi})
	}
	if len(files) == 0 {
		return stats, nil
	}
	sort.Slice(files, func(i, j int) bool { return files[i].idx < files[j].idx })

	stats.OldestMTime = files[0].info.ModTime()
	stats.NewestMTime = files[0].info.ModTime()

	for i, f := range files {
		stats.CompressedBytes += f.info.Size()
		mt := f.info.ModTime()
		if mt.Before(stats.OldestMTime) {
			stats.OldestMTime = mt
		}
		if mt.After(stats.NewestMTime) {
			stats.NewestMTime = mt
		}

		qs, err := segment.QuickStats(f.path)
		if err != nil {
			// Latest file may be torn during rotation; tolerate it. Note:
			// we already added f.info.Size() to CompressedBytes above, so
			// CompressedBytes can briefly include bytes that have no matching
			// UncompressedBytes contribution. Acceptable for helpful status
			// data; a later request after rotation completes will reconcile.
			if i == len(files)-1 {
				continue
			}
			return stats, fmt.Errorf("status: quickstats %s: %w", f.path, err)
		}
		stats.UncompressedBytes += qs.UncompressedBytes

		if qs.Sealed {
			stats.SealedCount++
		} else {
			stats.ActiveCount++
		}
	}

	// Latest-segment summary (cheap full Inspect on one file).
	latest := files[len(files)-1]
	if summary, err := buildSegmentSummary(latest.path, latest.idx, latest.info.Size()); err == nil {
		stats.LatestSegment = summary
	}

	return stats, nil
}

func buildSegmentSummary(path string, idx uint64, size int64) (*SegmentSummary, error) {
	ins, err := segment.Inspect(path)
	if err != nil {
		return nil, err
	}
	return &SegmentSummary{
		Index:           idx,
		Sealed:          ins.Sealed,
		EventCount:      ins.TotalEvents,
		UniqueDIDCount:  ins.UniqueDIDCount,
		BlockCount:      uint32(len(ins.Blocks)),
		CollectionCount: len(ins.Collections),
		MinSeq:          ins.MinSeq,
		MaxSeq:          ins.MaxSeq,
		MinIndexedAt:    microsToTime(ins.MinIndexedAt),
		MaxIndexedAt:    microsToTime(ins.MaxIndexedAt),
		SizeBytes:       size,
	}, nil
}

func microsToTime(us int64) time.Time {
	if us == 0 {
		return time.Time{}
	}
	return time.UnixMicro(us).UTC()
}

func collectPebble(s *store.Store, dataDir string) (PebbleStats, error) {
	stats := PebbleStats{KeyspaceCounts: make(map[string]uint64, len(keyspacePrefixes))}

	// On-disk size of meta.pebble/.
	pebbleDir := filepath.Join(dataDir, store.PebbleSubdir)
	if err := filepath.WalkDir(pebbleDir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return fs.SkipAll
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		stats.DiskBytes += fi.Size()
		return nil
	}); err != nil {
		return PebbleStats{}, fmt.Errorf("status: walk %s: %w", pebbleDir, err)
	}

	// Per-prefix key counts.
	for _, prefix := range keyspacePrefixes {
		c, err := countKeysWithPrefix(s, prefix)
		if err != nil {
			return PebbleStats{}, err
		}
		stats.KeyspaceCounts[prefix] = c
	}
	return stats, nil
}

func collectPebbleFast() PebbleStats {
	return PebbleStats{KeyspaceCounts: make(map[string]uint64, len(keyspacePrefixes))}
}

func countKeysWithPrefix(s *store.Store, prefix string) (uint64, error) {
	lower := []byte(prefix)
	upper := store.PrefixUpperBound(lower)

	it, err := s.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return 0, fmt.Errorf("status: open iter %q: %w", prefix, err)
	}
	defer func() { _ = it.Close() }()

	var n uint64
	for it.First(); it.Valid(); it.Next() {
		n++
	}
	if err := it.Error(); err != nil {
		return 0, fmt.Errorf("status: iter %q: %w", prefix, err)
	}
	return n, nil
}

// build composes the gather functions into a Snapshot. ctx is checked
// once at entry — gather functions do not currently accept ctx, so
// per-section checks would be theater. If/when individual gather
// functions take ctx (e.g. context-aware pebble iteration), add the
// per-section checks back.
func build(ctx context.Context, opts Options, startedAt time.Time) (*Snapshot, error) {
	now := opts.Now()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	phase, err := collectPhase(opts.Store)
	if err != nil {
		return nil, err
	}

	liveStats, err := collectLive(opts.Store)
	if err != nil {
		return nil, err
	}

	var (
		bf       BackfillStats
		segs     SegmentTreeStats
		livesegs SegmentTreeStats
		pdb      PebbleStats
	)
	if opts.Manifest != nil {
		if err := opts.Manifest.Wait(ctx); err != nil {
			return nil, err
		}
		bf, err = collectBackfillFast(opts.Store)
		if err != nil {
			return nil, err
		}
		segs = collectManifestSegmentTree(opts.Manifest.SegmentStats())
		livesegs = SegmentTreeStats{Dir: filepath.Join(opts.DataDir, "backfill", "live_segments")}
		pdb = collectPebbleFast()
	} else {
		bf, err = collectBackfill(opts.Store)
		if err != nil {
			return nil, err
		}
		segs, err = collectSegmentTree(filepath.Join(opts.DataDir, "segments"))
		if err != nil {
			return nil, err
		}
		livesegs, err = collectSegmentTree(filepath.Join(opts.DataDir, "backfill", "live_segments"))
		if err != nil {
			return nil, err
		}
		pdb, err = collectPebble(opts.Store, opts.DataDir)
		if err != nil {
			return nil, err
		}
	}

	snap := &Snapshot{
		GeneratedAt: now,
		Process:     collectProcess(now, startedAt),
		Phase:       phase,
		Backfill:    bf,
		Live:        liveStats,
		Segments:    segs,
		LiveSegs:    livesegs,
		Pebble:      pdb,
	}

	if opts.Manifest != nil {
		snap.CursorLookback.ConfiguredLookback = opts.CursorLookback
		snap.CursorLookback.ManifestSegmentCount = opts.Manifest.SegmentCount()
		if opts.CursorLookback > 0 {
			seq, ts := opts.Manifest.LookbackFloor(opts.CursorLookback)
			snap.CursorLookback.OldestRetainedSeq = seq
			if ts != 0 {
				snap.CursorLookback.OldestRetainedAt = time.UnixMicro(ts)
			}
		}
	} else {
		// No manifest wired in — leave CursorLookback at its zero value.
		// ConfiguredLookback may still be set so the operator sees the
		// flag value even before steady-state.
		snap.CursorLookback.ConfiguredLookback = opts.CursorLookback
	}

	return snap, nil
}
