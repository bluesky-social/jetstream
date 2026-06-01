package status

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"runtime"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream-v2/internal/ingest/live"
	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
	"github.com/bluesky-social/jetstream-v2/internal/manifest"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/internal/version"
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
	// instances /status uses the maintained aggregate so snapshot
	// builds stay cheap; missing aggregates render as zeros.
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

func collectManifestSegmentAggregate(ms manifest.SegmentTreeStats, liveDir string) *SegmentAggregate {
	tree := TreeAggregate{
		Dir:               ms.Dir,
		SealedCount:       ms.SealedCount,
		ActiveCount:       ms.ActiveCount,
		CompressedBytes:   ms.CompressedBytes,
		UncompressedBytes: ms.UncompressedBytes,
		DiskBytes:         ms.DiskBytes,
		EventCount:        ms.EventCount,
		BlockCount:        ms.BlockCount,
		OldestMTime:       ms.OldestMTime,
		NewestMTime:       ms.NewestMTime,
		MinSeq:            ms.MinSeq,
		MaxSeq:            ms.MaxSeq,
		MinIndexedAt:      microsToTime(ms.MinIndexedAt),
		MaxIndexedAt:      microsToTime(ms.MaxIndexedAt),
	}
	if ms.LatestSegment != nil {
		tree.LatestSegment = &SegmentSummary{
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

	collections := make([]CollectionAggregate, 0, len(ms.Collections))
	for _, c := range ms.Collections {
		collections = append(collections, CollectionAggregate{
			NSID:         c.NSID,
			EventCount:   c.EventCount,
			SegmentCount: c.SegmentCount,
			BlockCount:   c.BlockCount,
		})
	}

	agg := &SegmentAggregate{
		Trees: []TreeAggregate{
			tree,
			{Dir: liveDir},
		},
		Collections: collections,
	}
	agg.Network = computeNetworkTotals(agg.Trees, len(agg.Collections))
	return agg
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
		bf  BackfillStats
		agg *SegmentAggregate
		pdb PebbleStats
	)

	roots := []string{
		filepath.Join(opts.DataDir, "segments"),
		filepath.Join(opts.DataDir, "backfill", "live_segments"),
	}
	if opts.Manifest != nil {
		if err := opts.Manifest.Wait(ctx); err != nil {
			return nil, err
		}
		bf, err = collectBackfillFast(opts.Store)
		if err != nil {
			return nil, err
		}
		agg = collectManifestSegmentAggregate(opts.Manifest.SegmentStats(), roots[1])
		pdb = collectPebbleFast()
	} else {
		bf, err = collectBackfill(opts.Store)
		if err != nil {
			return nil, err
		}
		agg, err = InspectAll(roots, InspectAllOptions{})
		if err != nil {
			return nil, err
		}
		pdb, err = collectPebble(opts.Store, opts.DataDir)
		if err != nil {
			return nil, err
		}
	}
	if len(agg.Trees) != 2 {
		return nil, fmt.Errorf("status: segment aggregate has %d trees, expected 2 (segments + backfill/live_segments); the /status template assumes this shape", len(agg.Trees))
	}

	snap := &Snapshot{
		GeneratedAt:      now,
		Process:          collectProcess(now, startedAt),
		Phase:            phase,
		Backfill:         bf,
		Live:             liveStats,
		SegmentAggregate: agg,
		Pebble:           pdb,
	}

	snap.CursorLookback.ConfiguredLookback = opts.CursorLookback
	if opts.Manifest != nil && opts.CursorLookback > 0 {
		snap.CursorLookback.ManifestSegmentCount = opts.Manifest.SegmentCount()
		seq, ts := opts.Manifest.LookbackFloor(opts.CursorLookback)
		snap.CursorLookback.OldestRetainedSeq = seq
		if ts != 0 {
			snap.CursorLookback.OldestRetainedAt = time.UnixMicro(ts)
		}
	}

	return snap, nil
}
