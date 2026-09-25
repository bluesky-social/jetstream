package orchestrator

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/internal/tombstone"
	"github.com/bluesky-social/jetstream/segment"
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
	size            int64 // rewritten file size, when result.Rewritten
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
	if mode == compactionSteady && o.cfg.CompactionSchedule != nil {
		// The active pass replaces the scheduled timestamp so newly issued
		// responses receive only the configured grace period while rewrites run.
		o.cfg.CompactionSchedule.beginPass(start)
	}
	var finalWatermark uint64
	defer func() {
		o.cfg.Metrics.observeCompactionPass(start, retErr)
		if o.cfg.OnCompactionPass != nil {
			o.cfg.OnCompactionPass(CompactionPassResult{Watermark: finalWatermark, Err: retErr})
		}
	}()

	return obs.Span(ctx, func(ctx context.Context) error {
		segs := o.segments()
		// Compaction is the only segment rewriter and passes never overlap,
		// so any *.jss.tmp seen here is genuinely stale.
		if err := segs.RemoveStaleTemps(catalog.Main); err != nil {
			return fmt.Errorf("orchestrator: compaction: %w", err)
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
		// The writers publish their seals, but a data dir opened with no
		// writer attached has sealed segments only the directory knows.
		if err := segs.Refresh(ctx); err != nil {
			return fmt.Errorf("orchestrator: compaction: refresh segment catalog: %w", err)
		}

		watermark, _, err := loadCompactionWatermark(o.cfg.Store)
		if err != nil {
			return err
		}
		finalWatermark = watermark

		sealed, targetWatermark := o.listSealedCompactionSegments(segs.Snapshot())
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

		// Fire the pre-rewrite hook now: the active segment is sealed (force-
		// rotated above), targetWatermark is known, and no rewrite has run yet,
		// so a snapshot here captures the full pre-compaction stream the pass
		// is about to subtract from. Guarded to passes that advance the
		// watermark (targetWatermark > watermark, checked above).
		if o.cfg.OnBeforeCompactionPass != nil {
			o.cfg.OnBeforeCompactionPass(targetWatermark)
		}

		current := watermark
		for current < targetWatermark {
			// Fold tombstones from sealed rows in (current,
			// targetWatermark]. The in-memory Set keeps each
			// key's global maximum seq, which may be above this
			// window. Filtering that value out would lose the
			// earlier in-window tombstone and leave a superseded
			// row behind (#100).
			//
			// Use the Set only for cap triggering and size
			// metrics; disk folding preserves the window's actual
			// tombstones.
			snap, chunkEnd, err := o.collectCompactionTombstones(ctx, sealed, current, targetWatermark)
			if err != nil {
				return err
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

// listSealedCompactionSegments returns every sealed main segment in view
// with its header, plus the pass's target watermark: the max seq across
// sealed segments with events. Active segments are skipped; the watermark
// never advances past them (spec §4 — their tombstones stay in the live
// set, which evicts only ≤ the committed watermark, and re-apply after
// seal). Steady passes force-rotate the live writer before taking view,
// so the only active segment left holds events appended after the pass
// began.
func (o *Orchestrator) listSealedCompactionSegments(view catalog.CatalogView) ([]sealedCompactionSegment, uint64) {
	segs := view.Segments(catalog.Main)
	sealed := make([]sealedCompactionSegment, 0, len(segs))
	var targetWatermark uint64
	for _, v := range segs {
		if v.State != catalog.Sealed {
			continue
		}
		h := v.Header
		f := ingest.SegmentFile{Idx: v.Index, Path: o.segments().Path(catalog.Main, v.Index)}
		sealed = append(sealed, sealedCompactionSegment{SegmentFile: f, header: h})
		if h.EventCount > 0 && h.MaxSeq > targetWatermark {
			targetWatermark = h.MaxSeq
		}
	}
	return sealed, targetWatermark
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
func (o *Orchestrator) reconcileCompactionManifestFromDisk(ctx context.Context) error {
	segs := o.segments()
	if err := segs.Refresh(ctx); err != nil {
		return fmt.Errorf("orchestrator: compaction: refresh segment catalog: %w", err)
	}
	sealed, _ := o.listSealedCompactionSegments(segs.Snapshot())
	return o.reconcileCompactionManifest(sealed)
}

func (o *Orchestrator) collectCompactionTombstones(ctx context.Context, sealed []sealedCompactionSegment, watermark, targetWatermark uint64) (tombstone.Snapshot, uint64, error) {
	snap := tombstone.Snapshot{Records: make(map[tombstone.RecordKey]uint64), DIDs: make(map[string]tombstone.DIDTombstone)}
	chunkEnd := targetWatermark
	capEntries := o.cfg.CompactionTombstoneCap
	// A fresh view per chunk: earlier chunks' rewrites changed the
	// generations the pass-start view named.
	view := o.segments().Snapshot()
	fetcher := o.segments().Fetcher()
	current := make(map[uint64]catalog.SegmentView)
	for _, v := range view.Segments(catalog.Main) {
		current[v.Index] = v
	}
	for _, f := range sealed {
		if f.header.MaxSeq <= watermark || f.header.MinSeq > targetWatermark {
			continue
		}
		if err := ctx.Err(); err != nil {
			return tombstone.Snapshot{}, 0, err
		}
		v, ok := current[f.Idx]
		if !ok || v.State != catalog.Sealed {
			return tombstone.Snapshot{}, 0, fmt.Errorf("orchestrator: compaction: sealed segment %s left the catalog mid-pass", f.Path)
		}
		for ref := range segmentRefs(view, v) {
			// Block-index bounds survive rewrites as historical
			// supersets (spec §6), so skipping on them can only skip
			// blocks with no rows inside the window.
			if ref.MaxSeq <= watermark || ref.MinSeq > targetWatermark {
				continue
			}
			events, err := catalog.DecodeRef(ctx, fetcher, ref)
			if err != nil {
				return tombstone.Snapshot{}, 0, fmt.Errorf("orchestrator: compaction: decode source %s block %d: %w", f.Path, ref.Block, err)
			}
			part, err := tombstone.FoldRange(events, watermark, targetWatermark)
			if err != nil {
				return tombstone.Snapshot{}, 0, fmt.Errorf("orchestrator: compaction: fold %s block %d: %w", f.Path, ref.Block, err)
			}
			snap.Merge(part)
		}
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

	segs := o.segments()
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
				res, rewritten, err := segs.RewriteSegment(catalog.Main, f.Idx, func(ev *segment.Event) segment.RowDecision {
					if ev.Seq > chunkEnd {
						return segment.RowKeep
					}
					if drop, reason := snap.ShouldDrop(ev); drop {
						droppedByReason[reason]++
						return segment.RowDrop
					}
					return segment.RowKeep
				}, segment.RewriteOptions{
					CrashInjector:   crashpoint.ForSegment(o.cfg.CrashInjector),
					IOFaultInjector: o.cfg.SegmentIOFaultInjector,
					CandidateDIDs:   candidateDIDs,
				})
				if err != nil {
					return ingest.WrapDiskFull(o.cfg.DataDir, "rewriting segment during compaction",
						fmt.Errorf("orchestrator: compaction: rewrite %s: %w", f.Path, err))
				}
				mu.Lock()
				results = append(results, compactionRewriteResult{file: f, result: res, size: rewritten.Size, droppedByReason: droppedByReason})
				mu.Unlock()
			}
			return nil
		})
	}
sendLoop:
	for _, f := range sealed {
		select {
		case jobs <- f:
		case <-gctx.Done():
			break sendLoop
		}
	}
	close(jobs)
	waitErr := g.Wait()
	// A cancel can land while every worker is mid-rewrite or idle: the send
	// loop drops the undelivered segments and the workers return nil, so
	// g.Wait() alone can be nil for a chunk that was cut short. Fold the
	// context in — returning nil here would let the caller commit the chunk
	// watermark and evict tombstones for rewrites that never ran, leaving
	// superseded rows below the watermark alive permanently.
	if waitErr == nil {
		waitErr = ctx.Err()
	}
	if waitErr != nil {
		return waitErr
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
			o.cfg.Metrics.addCompactionBytesRewritten(r.size)
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
		if o.cfg.CompactionSchedule != nil {
			o.cfg.CompactionSchedule.disable()
		}
		<-ctx.Done()
		return ctx.Err()
	}

	if o.cfg.CompactionSchedule != nil {
		o.cfg.CompactionSchedule.completePass(time.Now().Add(o.cfg.CompactionInterval))
	}

	// Spec §5 failure policy: a failed pass aborts without advancing
	// the watermark and the next pass retries; watermark_lag_seconds
	// is the operator's paging signal. A pass error must never tear
	// down the daemon (it would cancel the live consumer and turn a
	// transient IO error into an ingestion outage / crash loop).
	var lastPass time.Time
	runPass := func() {
		err := o.runDeleteCompaction(ctx, compactionSteady, liveWriter)
		completed := time.Now()
		if o.cfg.CompactionSchedule != nil {
			if err != nil {
				// A failed pass may have rewritten some files before the
				// durable watermark failed. Keep the schedule unknown until
				// the next pass starts rather than advertise freshness.
				o.cfg.CompactionSchedule.failPass()
			} else {
				o.cfg.CompactionSchedule.completePass(completed.Add(o.cfg.CompactionInterval))
			}
		}
		if err != nil && ctx.Err() == nil {
			o.logger.ErrorContext(ctx, "steady compaction pass failed; will retry", "err", err)
		}
		lastPass = completed
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

	watermark, _, err := loadCompactionWatermark(o.cfg.Store)
	if err != nil {
		return err
	}
	segs := o.segments()
	if err := segs.Refresh(ctx); err != nil {
		return fmt.Errorf("orchestrator: compaction: rebuild tombstones refresh: %w", err)
	}
	view, fetcher := segs.Snapshot(), segs.Fetcher()

	// Blocks entirely at or below the watermark are already physically
	// compacted; their tombstones can contribute nothing (rebuild cost must
	// scale with the watermark backlog, not the archive — spec §3.4), so
	// start past them. The refs run through the active tail's durable
	// blocks too.
	snap := tombstone.Snapshot{Records: make(map[tombstone.RecordKey]uint64), DIDs: make(map[string]tombstone.DIDTombstone)}
	for ref := range view.RefsFrom(catalog.Main, watermark+1) {
		if err := ctx.Err(); err != nil {
			return err
		}
		events, err := catalog.DecodeRef(ctx, fetcher, ref)
		if err != nil {
			return fmt.Errorf("orchestrator: compaction: rebuild decode segment %d block %d: %w", ref.Segment, ref.Block, err)
		}
		part, err := tombstone.Fold(events, watermark)
		if err != nil {
			return fmt.Errorf("orchestrator: compaction: rebuild fold segment %d block %d: %w", ref.Segment, ref.Block, err)
		}
		snap.Merge(part)
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
	var tipWitnessedAt int64
	var watermarkWitnessedAt int64
	var oldestWitnessedAt int64
	for _, f := range sealed {
		if f.header.EventCount == 0 {
			continue
		}
		if f.header.MaxWitnessedAt > tipWitnessedAt {
			tipWitnessedAt = f.header.MaxWitnessedAt
		}
		if oldestWitnessedAt == 0 || f.header.MinWitnessedAt < oldestWitnessedAt {
			oldestWitnessedAt = f.header.MinWitnessedAt
		}
		if f.header.MaxSeq <= watermark && f.header.MaxWitnessedAt > watermarkWitnessedAt {
			watermarkWitnessedAt = f.header.MaxWitnessedAt
		}
	}

	if watermarkWitnessedAt == 0 {
		// Nothing compacted yet: without this floor the gauge would
		// report tip-since-epoch (a false ~50-year spike on the
		// operator paging signal). The honest lag is the span of
		// uncompacted data.
		watermarkWitnessedAt = oldestWitnessedAt
	}

	if tipWitnessedAt <= watermarkWitnessedAt {
		return 0
	}

	return float64(tipWitnessedAt-watermarkWitnessedAt) / 1_000_000
}
