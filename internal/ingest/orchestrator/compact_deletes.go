package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/crashpoint"
	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/obs"
	"github.com/bluesky-social/jetstream-v2/internal/tombstone"
	"github.com/bluesky-social/jetstream-v2/segment"
	"golang.org/x/sync/errgroup"
)

type compactionMode uint8

const (
	compactionMergeTail compactionMode = iota
	compactionSteady
)

// defaultCompactionBloomNarrowMaxDIDs bounds the candidate-DID bloom
// prefilter (spec §5 step 5): probing is O(segments × candidates), so
// beyond this many distinct DIDs (merge-tail passes run millions) it
// costs more than the block decodes it saves and nearly every segment
// matches anyway — skip narrowing and exact-scan.
const defaultCompactionBloomNarrowMaxDIDs = 100_000

// minCompactionTriggerSpacing bounds how often cap-triggered early
// passes can run back-to-back. Steady passes force-rotate the active
// segment first, so a pass normally evicts everything at or below the
// fresh seal — but defense-in-depth: if eviction ever falls short
// (e.g. a failed pass), the consumer re-fires the trigger on the next
// event and this floor keeps the compactor from spinning full passes.
const minCompactionTriggerSpacing = 30 * time.Second

type sealedCompactionSegment struct {
	ingest.SegmentFile
	header segment.Header
}

type compactionRewriteResult struct {
	file            sealedCompactionSegment
	result          segment.RewriteResult
	droppedByReason map[string]uint64
}

// runDeleteCompaction executes one compaction pass. liveWriter is the
// steady-state consumer's writer, force-rotated at the top of every
// steady pass so rows deleted while their segment was still active are
// compacted by this pass instead of waiting for a size-based rotation;
// merge-tail passes hand nil (that tree is sealed before the pass).
func (o *Orchestrator) runDeleteCompaction(ctx context.Context, mode compactionMode, liveWriter *ingest.Writer) (retErr error) {
	if o.cfg.CompactionInterval == 0 {
		// Compaction is disabled
		return nil
	}

	start := time.Now()
	var finalWatermark uint64
	defer func() {
		o.cfg.Metrics.observeCompactionPass(start, retErr)
		if o.cfg.OnCompactionPass != nil {
			o.cfg.OnCompactionPass(CompactionPassResult{Watermark: finalWatermark, Err: retErr})
		}
	}()

	return obs.Span(ctx, func(ctx context.Context) error {
		segmentsDir := filepath.Join(o.cfg.DataDir, "segments")
		if err := removeStaleCompactionTemps(segmentsDir); err != nil {
			return err
		}

		// Seal the active segment first so this pass covers rows deleted
		// while their segment was still active. Ordering is what makes
		// this race-free: tombstone Observe runs as the writer's OnAppend
		// hook under the writer mutex (see live.Open), so by the time
		// ForceRotate returns, every event in the just-sealed file has
		// its tombstone in the live set; events appended afterwards land
		// in the new active segment with seqs above the sealed header's
		// MaxSeq, i.e. above this pass's target watermark, and their
		// tombstones survive eviction (Evict is bounded by chunkEnd).
		// A no-op when the active segment is empty.
		if liveWriter != nil {
			if err := liveWriter.ForceRotate(ctx); err != nil {
				return fmt.Errorf("orchestrator: compaction: force rotate active segment: %w", err)
			}
		}

		watermark, _, err := loadCompactionWatermark(o.cfg.Store)
		if err != nil {
			return err
		}
		finalWatermark = watermark

		sealed, targetWatermark, err := o.listSealedCompactionSegments(segmentsDir)
		if err != nil {
			return err
		}
		if mode == compactionSteady {
			// Heals the rewrite-succeeded/refresh-failed crash window
			// (spec §5 step 2). Cheap: compares each manifest entry's
			// resident checksum against the header the sweep above
			// already read; only mismatches re-read metadata.
			if err := o.reconcileCompactionManifest(sealed); err != nil {
				return err
			}
		}
		if targetWatermark <= watermark {
			o.cfg.Metrics.setCompactionWatermarkLag(0)
			return nil
		}
		o.cfg.Metrics.setCompactionWatermarkLag(compactionWatermarkLagSeconds(sealed, watermark))

		current := watermark
		for current < targetWatermark {
			var snap tombstone.Snapshot
			var chunkEnd uint64
			if mode == compactionSteady && o.cfg.Tombstones != nil {
				snap = o.cfg.Tombstones.SnapshotRange(current, targetWatermark)
				chunkEnd = targetWatermark
			} else {
				var err error
				snap, chunkEnd, err = o.collectCompactionTombstones(ctx, sealed, current, targetWatermark)
				if err != nil {
					return err
				}
			}
			if chunkEnd <= current {
				chunkEnd = targetWatermark
			}

			if !snap.Empty() {
				if err := o.applyCompactionChunk(ctx, sealed, snap, chunkEnd, mode); err != nil {
					return err
				}
			}
			// Counted only after the chunk applied: a failed pass is
			// retried with the same snapshot, and counting up front
			// would inflate tombstones_collected_total on every retry.
			o.cfg.Metrics.addCompactionTombstones("record", len(snap.Records))
			didsByReason := map[string]int{}
			for _, ts := range snap.DIDs {
				didsByReason[ts.Reason]++
			}
			for reason, n := range didsByReason {
				o.cfg.Metrics.addCompactionTombstones(reason, n)
			}
			if err := o.simulateCrash(ctx, crashpoint.AfterCompactionRewriteBeforeWatermark); err != nil {
				return err
			}

			if err := saveCompactionWatermark(o.cfg.Store, chunkEnd); err != nil {
				return err
			}
			o.cfg.Metrics.setCompactionWatermark(chunkEnd)
			finalWatermark = chunkEnd
			o.cfg.Metrics.setCompactionWatermarkLag(compactionWatermarkLagSeconds(sealed, chunkEnd))
			if o.cfg.Tombstones != nil {
				o.cfg.Tombstones.Evict(chunkEnd)
			}
			if err := o.simulateCrash(ctx, crashpoint.AfterCompactionChunkWatermark); err != nil {
				return err
			}
			current = chunkEnd
		}
		o.logger.InfoContext(ctx, "delete compaction pass complete",
			"watermark", targetWatermark,
			"mode", mode,
		)
		return nil
	})
}

// listSealedCompactionSegments sweeps segmentsDir, returning every
// sealed segment with its parsed header plus the pass's target
// watermark: the max seq across sealed segments with events. Active
// segments are skipped; the watermark never advances past them (spec
// §4 — their tombstones stay in the live set, which evicts only ≤ the
// committed watermark, and re-apply after seal). Steady passes
// force-rotate the live writer before this sweep, so the only active
// segment left holds events appended after the pass began.
func (o *Orchestrator) listSealedCompactionSegments(segmentsDir string) ([]sealedCompactionSegment, uint64, error) {
	files, err := ingest.SegmentFiles(segmentsDir)
	if err != nil {
		return nil, 0, fmt.Errorf("orchestrator: compaction: list segments: %w", err)
	}
	sealed := make([]sealedCompactionSegment, 0, len(files))
	var targetWatermark uint64
	for _, f := range files {
		r, err := segment.Open(segment.ReaderConfig{Path: f.Path, SkipChecksum: true})
		if err != nil {
			if errors.Is(err, segment.ErrActiveSegment) {
				continue
			}
			return nil, 0, fmt.Errorf("orchestrator: compaction: open %s: %w", f.Path, err)
		}
		h := r.Header()
		_ = r.Close()
		sealed = append(sealed, sealedCompactionSegment{SegmentFile: f, header: h})
		if h.EventCount > 0 && h.MaxSeq > targetWatermark {
			targetWatermark = h.MaxSeq
		}
	}
	return sealed, targetWatermark, nil
}

// reconcileCompactionManifest re-fires the manifest refresh path for
// every sealed segment whose on-disk header checksum differs from the
// manifest's resident entry (or that the manifest is missing). The
// checksum compare keeps no-op passes cheap and makes
// manifest_reconciled_total a true heal counter.
func (o *Orchestrator) reconcileCompactionManifest(sealed []sealedCompactionSegment) error {
	if o.cfg.OnSegmentCompacted == nil {
		return nil
	}
	var resident map[uint64]uint64
	if o.cfg.SegmentManifestChecksums != nil {
		resident = o.cfg.SegmentManifestChecksums()
	}
	for _, f := range sealed {
		if resident != nil {
			if sum, ok := resident[f.Idx]; ok && sum == f.header.Checksum {
				continue
			}
		}
		if err := o.cfg.OnSegmentCompacted(f.Idx, f.Path); err != nil {
			return fmt.Errorf("orchestrator: compaction: reconcile manifest %s: %w", f.Path, err)
		}
		o.cfg.Metrics.incCompactionManifestReconciled()
	}
	return nil
}

// reconcileCompactionManifestFromDisk is the one-shot reconcile at the
// merge→steady transition (spec §7): the merge-tail pass is manifest-
// oblivious, so serving must not ungate until every manifest entry
// matches its on-disk header.
func (o *Orchestrator) reconcileCompactionManifestFromDisk(segmentsDir string) error {
	sealed, _, err := o.listSealedCompactionSegments(segmentsDir)
	if err != nil {
		return err
	}
	return o.reconcileCompactionManifest(sealed)
}

func (o *Orchestrator) collectCompactionTombstones(ctx context.Context, sealed []sealedCompactionSegment, watermark, targetWatermark uint64) (tombstone.Snapshot, uint64, error) {
	snap := tombstone.Snapshot{Records: make(map[tombstone.RecordKey]uint64), DIDs: make(map[string]tombstone.DIDTombstone)}
	chunkEnd := targetWatermark
	capEntries := o.cfg.CompactionTombstoneCap
	for _, f := range sealed {
		if f.header.MaxSeq <= watermark || f.header.MinSeq > targetWatermark {
			continue
		}
		if err := ctx.Err(); err != nil {
			return tombstone.Snapshot{}, 0, err
		}
		r, err := segment.Open(segment.ReaderConfig{Path: f.Path, SkipChecksum: true})
		if err != nil {
			return tombstone.Snapshot{}, 0, fmt.Errorf("orchestrator: compaction: open source %s: %w", f.Path, err)
		}
		blocks := r.Blocks()
		for i := range blocks {
			// Block-index bounds survive rewrites as historical
			// supersets (spec §6), so skipping on them can only skip
			// blocks with no rows inside the window.
			if blocks[i].MaxSeq <= watermark || blocks[i].MinSeq > targetWatermark {
				continue
			}
			events, err := r.DecodeBlock(i)
			if err != nil {
				_ = r.Close()
				return tombstone.Snapshot{}, 0, fmt.Errorf("orchestrator: compaction: decode source %s block %d: %w", f.Path, i, err)
			}
			part, err := tombstone.FoldRange(events, watermark, targetWatermark)
			if err != nil {
				_ = r.Close()
				return tombstone.Snapshot{}, 0, fmt.Errorf("orchestrator: compaction: fold %s block %d: %w", f.Path, i, err)
			}
			snap.Merge(part)
		}
		_ = r.Close()
		if capEntries > 0 && len(snap.Records)+len(snap.DIDs) >= capEntries {
			chunkEnd = min(f.header.MaxSeq, targetWatermark)
			break
		}
	}
	return snap, chunkEnd, nil
}

func (o *Orchestrator) applyCompactionChunk(ctx context.Context, sealed []sealedCompactionSegment, snap tombstone.Snapshot, chunkEnd uint64, mode compactionMode) error {
	candidateDIDs := compactionCandidateDIDs(snap)
	maxNarrow := o.cfg.CompactionBloomNarrowMaxDIDs
	if maxNarrow <= 0 {
		maxNarrow = defaultCompactionBloomNarrowMaxDIDs
	}
	if len(candidateDIDs) > maxNarrow {
		candidateDIDs = nil
	}
	workers := o.cfg.CompactionRewriteWorkers
	if workers <= 0 {
		workers = defaultCompactionRewriteWorkers()
	}
	workers = min(workers, len(sealed))
	if workers <= 0 {
		return nil
	}

	jobs := make(chan sealedCompactionSegment)
	var (
		mu      sync.Mutex
		results []compactionRewriteResult
	)
	g, gctx := errgroup.WithContext(ctx)
	for range workers {
		g.Go(func() error {
			for f := range jobs {
				if err := gctx.Err(); err != nil {
					return err
				}
				o.cfg.Metrics.incCompactionSegmentsExamined()
				droppedByReason := map[string]uint64{}
				res, err := segment.Rewrite(f.Path, func(ev *segment.Event) segment.RowDecision {
					if ev.Seq > chunkEnd {
						return segment.RowKeep
					}
					if drop, reason := snap.ShouldDrop(ev); drop {
						droppedByReason[reason]++
						return segment.RowDrop
					}
					return segment.RowKeep
				}, segment.RewriteOptions{CrashInjector: o.cfg.CrashInjector, CandidateDIDs: candidateDIDs})
				if err != nil {
					return fmt.Errorf("orchestrator: compaction: rewrite %s: %w", f.Path, err)
				}
				mu.Lock()
				results = append(results, compactionRewriteResult{file: f, result: res, droppedByReason: droppedByReason})
				mu.Unlock()
			}
			return nil
		})
	}
	for _, f := range sealed {
		select {
		case jobs <- f:
		case <-gctx.Done():
			close(jobs)
			return g.Wait()
		}
	}
	close(jobs)
	if err := g.Wait(); err != nil {
		return err
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].file.Idx < results[j].file.Idx
	})

	for _, r := range results {
		if r.result.Rewritten {
			o.cfg.Metrics.incCompactionSegmentsRewritten()
			for reason, n := range r.droppedByReason {
				o.cfg.Metrics.addCompactionRowsDropped(reason, n)
			}
			if info, err := os.Stat(r.file.Path); err == nil {
				o.cfg.Metrics.addCompactionBytesRewritten(info.Size())
			}
			o.logger.Info("compaction rewrote segment",
				"segment", r.file.Path,
				"rows_dropped", r.result.RowsDropped,
				"blocks_touched", r.result.BlocksTouched,
			)
			if mode == compactionSteady && o.cfg.OnSegmentCompacted != nil {
				if err := o.cfg.OnSegmentCompacted(r.file.Idx, r.file.Path); err != nil {
					return fmt.Errorf("orchestrator: compaction: refresh manifest: %w", err)
				}
				o.cfg.Metrics.incCompactionManifestReconciled()
			}
		} else {
			o.cfg.Metrics.incCompactionSegmentsClean()
		}
	}
	return nil
}

// compactionCandidateDIDs returns the distinct DIDs across both
// tombstone maps, used by segment.Rewrite's segment-level bloom
// prefilter. Order is irrelevant to the bloom probe.
func compactionCandidateDIDs(snap tombstone.Snapshot) []string {
	seen := make(map[string]struct{}, len(snap.Records)+len(snap.DIDs))
	dids := make([]string, 0, len(snap.Records)+len(snap.DIDs))
	for key := range snap.Records {
		if key.DID == "" {
			continue
		}
		if _, ok := seen[key.DID]; ok {
			continue
		}
		seen[key.DID] = struct{}{}
		dids = append(dids, key.DID)
	}
	for did := range snap.DIDs {
		if did == "" {
			continue
		}
		if _, ok := seen[did]; ok {
			continue
		}
		seen[did] = struct{}{}
		dids = append(dids, did)
	}
	return dids
}

func defaultCompactionRewriteWorkers() int {
	return min(runtime.NumCPU(), 8)
}

func (o *Orchestrator) runSteadyCompactor(ctx context.Context, liveWriter *ingest.Writer) error {
	if o.cfg.CompactionInterval == 0 {
		<-ctx.Done()
		return ctx.Err()
	}

	// Spec §5 failure policy: a failed pass aborts without advancing
	// the watermark and the next pass retries; watermark_lag_seconds
	// is the operator's paging signal. A pass error must never tear
	// down the daemon (it would cancel the live consumer and turn a
	// transient IO error into an ingestion outage / crash loop).
	var lastPass time.Time
	runPass := func() {
		if err := o.runDeleteCompaction(ctx, compactionSteady, liveWriter); err != nil && ctx.Err() == nil {
			o.logger.ErrorContext(ctx, "steady compaction pass failed; will retry", "err", err)
		}
		lastPass = time.Now()
	}

	timer := time.NewTimer(o.cfg.CompactionInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-o.compactionTrigger:
			if !lastPass.IsZero() && time.Since(lastPass) < minCompactionTriggerSpacing {
				continue
			}

			runPass()

			o.cfg.Metrics.incCompactionEarlyPass()
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(o.cfg.CompactionInterval)
		case <-timer.C:
			runPass()
			timer.Reset(o.cfg.CompactionInterval)
		}
	}
}

func (o *Orchestrator) rebuildLiveTombstones(ctx context.Context) error {
	if o.cfg.Tombstones == nil || o.cfg.CompactionInterval == 0 {
		return nil
	}

	segmentsDir := filepath.Join(o.cfg.DataDir, "segments")
	watermark, _, err := loadCompactionWatermark(o.cfg.Store)
	if err != nil {
		return err
	}

	files, err := ingest.SegmentFiles(segmentsDir)
	if err != nil {
		return fmt.Errorf("orchestrator: compaction: rebuild tombstones list: %w", err)
	}

	snap := tombstone.Snapshot{Records: make(map[tombstone.RecordKey]uint64), DIDs: make(map[string]tombstone.DIDTombstone)}
	for _, f := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		r, err := segment.Open(segment.ReaderConfig{Path: f.Path, SkipChecksum: true})
		if err == nil {
			// Segments and blocks entirely at or below the watermark
			// are already physically compacted; their tombstones can
			// contribute nothing (rebuild cost must scale with the
			// watermark backlog, not the archive — spec §3.4).
			h := r.Header()
			if h.MaxSeq <= watermark {
				_ = r.Close()
				continue
			}
			blocks := r.Blocks()
			for i := range blocks {
				if blocks[i].MaxSeq <= watermark {
					continue
				}
				events, err := r.DecodeBlock(i)
				if err != nil {
					_ = r.Close()
					return fmt.Errorf("orchestrator: compaction: rebuild decode %s block %d: %w", f.Path, i, err)
				}
				part, err := tombstone.Fold(events, watermark)
				if err != nil {
					_ = r.Close()
					return fmt.Errorf("orchestrator: compaction: rebuild fold %s block %d: %w", f.Path, i, err)
				}
				snap.Merge(part)
			}
			_ = r.Close()
			continue
		}
		if !errors.Is(err, segment.ErrActiveSegment) {
			return fmt.Errorf("orchestrator: compaction: rebuild open %s: %w", f.Path, err)
		}
		if err := segment.WalkActive(f.Path, func(events []segment.Event) error {
			part, err := tombstone.Fold(events, watermark)
			if err != nil {
				return err
			}
			snap.Merge(part)
			return nil
		}); err != nil {
			return fmt.Errorf("orchestrator: compaction: rebuild walk active %s: %w", f.Path, err)
		}
	}

	o.cfg.Tombstones.Replace(snap)
	o.logger.InfoContext(ctx, "rebuilt live tombstone set",
		"record_tombstones", len(snap.Records),
		"did_tombstones", len(snap.DIDs),
		"watermark", watermark,
	)

	return nil
}

func compactionWatermarkLagSeconds(sealed []sealedCompactionSegment, watermark uint64) float64 {
	var tipIndexedAt int64
	var watermarkIndexedAt int64
	var oldestIndexedAt int64
	for _, f := range sealed {
		if f.header.EventCount == 0 {
			continue
		}
		if f.header.MaxIndexedAt > tipIndexedAt {
			tipIndexedAt = f.header.MaxIndexedAt
		}
		if oldestIndexedAt == 0 || f.header.MinIndexedAt < oldestIndexedAt {
			oldestIndexedAt = f.header.MinIndexedAt
		}
		if f.header.MaxSeq <= watermark && f.header.MaxIndexedAt > watermarkIndexedAt {
			watermarkIndexedAt = f.header.MaxIndexedAt
		}
	}

	if watermarkIndexedAt == 0 {
		// Nothing compacted yet: without this floor the gauge would
		// report tip-since-epoch (a false ~50-year spike on the
		// operator paging signal). The honest lag is the span of
		// uncompacted data.
		watermarkIndexedAt = oldestIndexedAt
	}

	if tipIndexedAt <= watermarkIndexedAt {
		return 0
	}

	return float64(tipIndexedAt-watermarkIndexedAt) / 1_000_000
}

// removeStaleCompactionTemps deletes leftover *.jss.tmp files from a
// crashed rewrite. Runs at process startup (Orchestrator.Run) and at
// the start of every pass. A missing directory is fine — bootstrap has
// not created it yet.
func removeStaleCompactionTemps(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("orchestrator: compaction: readdir tmp cleanup: %w", err)
	}

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".jss.tmp") {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("orchestrator: compaction: remove tmp %s: %w", e.Name(), err)
		}
	}

	return nil
}
