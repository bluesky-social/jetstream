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

func (o *Orchestrator) runDeleteCompaction(ctx context.Context, mode compactionMode) error {
	if o.cfg.CompactionInterval == 0 {
		return nil
	}
	return obs.Span(ctx, func(ctx context.Context) error {
		segmentsDir := filepath.Join(o.cfg.DataDir, "segments")
		if err := removeStaleCompactionTemps(segmentsDir); err != nil {
			return err
		}

		watermark, _, err := loadCompactionWatermark(o.cfg.Store)
		if err != nil {
			return err
		}
		files, err := ingest.SegmentFiles(segmentsDir)
		if err != nil {
			return fmt.Errorf("orchestrator: compaction: list segments: %w", err)
		}

		type sealedSegment struct {
			ingest.SegmentFile
			header segment.Header
		}
		sealed := make([]sealedSegment, 0, len(files))
		var targetWatermark uint64
		for _, f := range files {
			r, err := segment.Open(segment.ReaderConfig{Path: f.Path, SkipChecksum: true})
			if err != nil {
				if errors.Is(err, segment.ErrActiveSegment) {
					continue
				}
				return fmt.Errorf("orchestrator: compaction: open %s: %w", f.Path, err)
			}
			h := r.Header()
			_ = r.Close()
			sealed = append(sealed, sealedSegment{SegmentFile: f, header: h})
			if h.EventCount > 0 && h.MaxSeq > targetWatermark {
				targetWatermark = h.MaxSeq
			}
		}
		if mode == compactionSteady && o.cfg.OnSegmentCompacted != nil {
			for _, f := range sealed {
				if err := o.cfg.OnSegmentCompacted(f.Idx, f.Path); err != nil {
					return fmt.Errorf("orchestrator: compaction: reconcile manifest: %w", err)
				}
			}
		}
		if targetWatermark <= watermark {
			return nil
		}

		snap := tombstone.Snapshot{Records: make(map[tombstone.RecordKey]uint64), DIDs: make(map[string]uint64)}
		for _, f := range sealed {
			if f.header.MaxSeq <= watermark || f.header.MinSeq > targetWatermark {
				continue
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			r, err := segment.Open(segment.ReaderConfig{Path: f.Path, SkipChecksum: true})
			if err != nil {
				return fmt.Errorf("orchestrator: compaction: open source %s: %w", f.Path, err)
			}
			for i := range int(r.Header().BlockCount) {
				events, err := r.DecodeBlock(i)
				if err != nil {
					_ = r.Close()
					return fmt.Errorf("orchestrator: compaction: decode source %s block %d: %w", f.Path, i, err)
				}
				part, err := tombstone.Fold(events, watermark)
				if err != nil {
					_ = r.Close()
					return err
				}
				snap.Merge(part)
			}
			_ = r.Close()
		}

		if !snap.Empty() {
			for _, f := range sealed {
				if err := ctx.Err(); err != nil {
					return err
				}
				res, err := segment.Rewrite(f.Path, func(ev *segment.Event) segment.RowDecision {
					if ev.Seq > targetWatermark {
						return segment.RowKeep
					}
					if drop, _ := snap.ShouldDrop(ev); drop {
						return segment.RowDrop
					}
					return segment.RowKeep
				}, segment.RewriteOptions{})
				if err != nil {
					return fmt.Errorf("orchestrator: compaction: rewrite %s: %w", f.Path, err)
				}
				if res.Rewritten && mode == compactionSteady && o.cfg.OnSegmentCompacted != nil {
					if err := o.cfg.OnSegmentCompacted(f.Idx, f.Path); err != nil {
						return fmt.Errorf("orchestrator: compaction: refresh manifest: %w", err)
					}
				}
			}
		}

		if err := saveCompactionWatermark(o.cfg.Store, targetWatermark); err != nil {
			return err
		}
		if o.cfg.Tombstones != nil {
			o.cfg.Tombstones.Evict(targetWatermark)
		}
		o.logger.InfoContext(ctx, "delete compaction pass complete",
			"watermark", targetWatermark,
			"record_tombstones", len(snap.Records),
			"did_tombstones", len(snap.DIDs),
			"mode", mode,
		)
		return nil
	})
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
	snap := tombstone.Snapshot{Records: make(map[tombstone.RecordKey]uint64), DIDs: make(map[string]uint64)}
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
	o.logger.InfoContext(ctx, "rebuilt live tombstone set",
		"record_tombstones", len(snap.Records),
		"did_tombstones", len(snap.DIDs),
		"watermark", watermark,
	)
	return nil
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
