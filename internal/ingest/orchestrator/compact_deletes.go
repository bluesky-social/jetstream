package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/obs"
	"github.com/bluesky-social/jetstream-v2/internal/tombstone"
	"github.com/bluesky-social/jetstream-v2/segment"
)

type compactionMode uint8

const (
	compactionMergeTail compactionMode = iota
	compactionSteady
)

type sealedCompactionSegment struct {
	ingest.SegmentFile
	header segment.Header
}

func (o *Orchestrator) runDeleteCompaction(ctx context.Context, mode compactionMode) error {
	if o.cfg.CompactionInterval == 0 {
		return nil
	}
	start := time.Now()
	var retErr error
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
			retErr = err
			return err
		}

		watermark, _, err := loadCompactionWatermark(o.cfg.Store)
		if err != nil {
			retErr = err
			return err
		}
		finalWatermark = watermark
		files, err := ingest.SegmentFiles(segmentsDir)
		if err != nil {
			retErr = fmt.Errorf("orchestrator: compaction: list segments: %w", err)
			return retErr
		}

		sealed := make([]sealedCompactionSegment, 0, len(files))
		var targetWatermark uint64
		for _, f := range files {
			r, err := segment.Open(segment.ReaderConfig{Path: f.Path, SkipChecksum: true})
			if err != nil {
				if errors.Is(err, segment.ErrActiveSegment) {
					continue
				}
				retErr = fmt.Errorf("orchestrator: compaction: open %s: %w", f.Path, err)
				return retErr
			}
			h := r.Header()
			_ = r.Close()
			sealed = append(sealed, sealedCompactionSegment{SegmentFile: f, header: h})
			if h.EventCount > 0 && h.MaxSeq > targetWatermark {
				targetWatermark = h.MaxSeq
			}
		}
		if mode == compactionSteady && o.cfg.OnSegmentCompacted != nil {
			for _, f := range sealed {
				if err := o.cfg.OnSegmentCompacted(f.Idx, f.Path); err != nil {
					retErr = fmt.Errorf("orchestrator: compaction: reconcile manifest: %w", err)
					return retErr
				}
				o.cfg.Metrics.incCompactionManifestReconciled()
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
					retErr = err
					return err
				}
			}
			if chunkEnd <= current {
				chunkEnd = targetWatermark
			}
			o.cfg.Metrics.addCompactionTombstones("record", len(snap.Records))
			didsByReason := map[string]int{}
			for _, ts := range snap.DIDs {
				didsByReason[ts.Reason]++
			}
			for reason, n := range didsByReason {
				o.cfg.Metrics.addCompactionTombstones(reason, n)
			}

			if !snap.Empty() {
				if err := o.applyCompactionChunk(ctx, sealed, snap, chunkEnd, mode); err != nil {
					retErr = err
					return err
				}
			}

			if err := saveCompactionWatermark(o.cfg.Store, chunkEnd); err != nil {
				retErr = err
				return err
			}
			o.cfg.Metrics.setCompactionWatermark(chunkEnd)
			finalWatermark = chunkEnd
			o.cfg.Metrics.setCompactionWatermarkLag(compactionWatermarkLagSeconds(sealed, chunkEnd))
			if o.cfg.Tombstones != nil {
				o.cfg.Tombstones.Evict(chunkEnd)
				o.cfg.Metrics.setCompactionTombstoneSet(o.cfg.Tombstones.Len())
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
		for i := range int(r.Header().BlockCount) {
			events, err := r.DecodeBlock(i)
			if err != nil {
				_ = r.Close()
				return tombstone.Snapshot{}, 0, fmt.Errorf("orchestrator: compaction: decode source %s block %d: %w", f.Path, i, err)
			}
			part, err := tombstone.FoldRange(events, watermark, targetWatermark)
			if err != nil {
				_ = r.Close()
				return tombstone.Snapshot{}, 0, err
			}
			snap.Merge(part)
		}
		_ = r.Close()
		if capEntries > 0 && len(snap.Records)+len(snap.DIDs) >= capEntries {
			chunkEnd = f.header.MaxSeq
			if chunkEnd > targetWatermark {
				chunkEnd = targetWatermark
			}
			break
		}
	}
	return snap, chunkEnd, nil
}

func (o *Orchestrator) applyCompactionChunk(ctx context.Context, sealed []sealedCompactionSegment, snap tombstone.Snapshot, chunkEnd uint64, mode compactionMode) error {
	for _, f := range sealed {
		if err := ctx.Err(); err != nil {
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
		}, segment.RewriteOptions{})
		if err != nil {
			return fmt.Errorf("orchestrator: compaction: rewrite %s: %w", f.Path, err)
		}
		if res.Rewritten {
			o.cfg.Metrics.incCompactionSegmentsRewritten()
			for reason, n := range droppedByReason {
				o.cfg.Metrics.addCompactionRowsDropped(reason, n)
			}
			if info, err := os.Stat(f.Path); err == nil {
				o.cfg.Metrics.addCompactionBytesRewritten(info.Size())
			}
			if mode == compactionSteady && o.cfg.OnSegmentCompacted != nil {
				if err := o.cfg.OnSegmentCompacted(f.Idx, f.Path); err != nil {
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

func (o *Orchestrator) runSteadyCompactor(ctx context.Context) error {
	if o.cfg.CompactionInterval == 0 {
		<-ctx.Done()
		return ctx.Err()
	}
	timer := time.NewTimer(o.cfg.CompactionInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-o.compactionTrigger:
			if err := o.runDeleteCompaction(ctx, compactionSteady); err != nil {
				return err
			}
			o.cfg.Metrics.incCompactionEarlyPass()
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(o.cfg.CompactionInterval)
		case <-timer.C:
			if err := o.runDeleteCompaction(ctx, compactionSteady); err != nil {
				return err
			}
			timer.Reset(o.cfg.CompactionInterval)
		}
	}
}

func (o *Orchestrator) rebuildLiveTombstones(ctx context.Context) error {
	if o.cfg.Tombstones == nil {
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
			for i := range int(r.Header().BlockCount) {
				events, err := r.DecodeBlock(i)
				if err != nil {
					_ = r.Close()
					return fmt.Errorf("orchestrator: compaction: rebuild decode %s block %d: %w", f.Path, i, err)
				}
				part, err := tombstone.Fold(events, watermark)
				if err != nil {
					_ = r.Close()
					return err
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
	o.cfg.Metrics.setCompactionTombstoneSet(o.cfg.Tombstones.Len())
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
	for _, f := range sealed {
		if f.header.EventCount == 0 {
			continue
		}
		if f.header.MaxIndexedAt > tipIndexedAt {
			tipIndexedAt = f.header.MaxIndexedAt
		}
		if f.header.MaxSeq <= watermark && f.header.MaxIndexedAt > watermarkIndexedAt {
			watermarkIndexedAt = f.header.MaxIndexedAt
		}
	}
	if tipIndexedAt <= watermarkIndexedAt {
		return 0
	}
	return float64(tipIndexedAt-watermarkIndexedAt) / 1_000_000
}

func removeStaleCompactionTemps(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("orchestrator: compaction: readdir tmp cleanup: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".tmp" {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("orchestrator: compaction: remove tmp %s: %w", e.Name(), err)
		}
	}
	return nil
}

func listCompactionRefreshSegments(dir string) ([]ingest.SegmentFile, error) {
	files, err := ingest.SegmentFiles(dir)
	if err != nil {
		return nil, fmt.Errorf("orchestrator: compaction: list refresh segments: %w", err)
	}
	return files, nil
}
