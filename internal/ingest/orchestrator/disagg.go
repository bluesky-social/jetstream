package orchestrator

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/objstore"
)

// Disaggregated runs the lifecycle on the shared catalog (design §10.10):
// bootstrap and merge write in direct mode, and steady state in hot mode.
type Disaggregated struct {
	// Session is the leader session. Merge's final transaction commits in
	// it.
	Session *catalog.Session
	// Direct opens direct mode for one writer in ns. The sealer it returns
	// is loaded from the catalog when Direct is called, so at most one
	// writer per namespace may use it at a time.
	Direct func(ctx context.Context, ns catalog.Namespace) (*ingest.DirectConfig, error)
	// Hot opens hot mode for the steady-state writer. Run calls it once,
	// when steady state starts; the caller releases what it opened after
	// Run returns.
	Hot func(ctx context.Context) (*ingest.HotConfig, error)
	// Objects reads the bootstrap_live blocks merge drains:
	// protocol.Reader.
	Objects objstore.Store
}

// mergeSource is the bootstrap_live namespace as merge reads it.
type mergeSource interface {
	// segments returns bootstrap_live's segments in index order.
	segments(ctx context.Context) ([]sourceSegment, error)
	// fetcher reads the blocks the segments' refs point at.
	fetcher() catalog.Fetcher
}

// sourceSegment is one bootstrap_live segment and its blocks' refs.
type sourceSegment struct {
	index  uint64
	state  catalog.SegmentState
	blocks int
	refs   []catalog.BlockRef
}

// localMergeSource reads bootstrap_live through a local segment catalog.
type localMergeSource struct{ cat SegmentCatalog }

func (s localMergeSource) segments(context.Context) ([]sourceSegment, error) {
	view := s.cat.Snapshot()
	var out []sourceSegment
	for _, v := range view.Segments(catalog.BootstrapLive) {
		out = append(out, sourceSegment{
			index:  v.Index,
			state:  v.State,
			blocks: len(v.Blocks),
			refs:   slices.Collect(segmentRefs(view, v)),
		})
	}
	return out, nil
}

func (s localMergeSource) fetcher() catalog.Fetcher { return s.cat.Fetcher() }

// catalogMergeSource reads bootstrap_live from the shared catalog. Merge
// needs only the blocks' objects, in order, so it reads the catalog rows
// and skips the footers.
type catalogMergeSource struct {
	db      catalog.DB
	objects objstore.Store
}

func (s catalogMergeSource) segments(ctx context.Context) (out []sourceSegment, err error) {
	rtx, err := s.db.BeginRead(ctx)
	if err != nil {
		return nil, fmt.Errorf("orchestrator: merge: read bootstrap_live: %w", err)
	}
	defer func() { _ = rtx.Close(ctx) }()
	segs, err := rtx.SegmentsSince(ctx, 0)
	if err != nil {
		return nil, fmt.Errorf("orchestrator: merge: read bootstrap_live segments: %w", err)
	}
	var genIDs []uint64
	for _, row := range segs {
		if row.Namespace != catalog.BootstrapLive {
			continue
		}
		out = append(out, sourceSegment{index: row.Index, state: row.State})
		if row.State == catalog.Sealed {
			genIDs = append(genIDs, row.GenerationID)
		}
	}
	byGen := make(map[uint64][]catalog.GenerationBlockRow, len(genIDs))
	if len(genIDs) > 0 {
		blocks, err := rtx.GenerationBlocks(ctx, genIDs)
		if err != nil {
			return nil, fmt.Errorf("orchestrator: merge: read bootstrap_live generation blocks: %w", err)
		}
		for _, b := range blocks {
			byGen[b.GenerationID] = append(byGen[b.GenerationID], b)
		}
	}
	active, err := rtx.ActiveBlocksSince(ctx, 0)
	if err != nil {
		return nil, fmt.Errorf("orchestrator: merge: read bootstrap_live active blocks: %w", err)
	}
	byActive := make(map[uint64][]catalog.ActiveBlockRow)
	for _, b := range active {
		if b.Namespace == catalog.BootstrapLive {
			byActive[b.Segment] = append(byActive[b.Segment], b)
		}
	}

	genOf := make(map[uint64]uint64, len(segs))
	for _, row := range segs {
		if row.Namespace == catalog.BootstrapLive {
			genOf[row.Index] = row.GenerationID
		}
	}
	for i := range out {
		seg := &out[i]
		ref := func(ordinal int, objectID uint64) catalog.BlockRef {
			return catalog.BlockRef{
				Namespace:  catalog.BootstrapLive,
				Segment:    seg.index,
				Block:      ordinal,
				Generation: genOf[seg.index],
				Loc:        catalog.ObjectBlock{ObjectID: objectID},
			}
		}
		if seg.state == catalog.Sealed {
			for j, b := range byGen[genOf[seg.index]] {
				if b.Ordinal != j {
					return nil, catalog.Corruptf(catalog.SourceInvariant, "bootstrap_live segment %d block %d is at ordinal %d", seg.index, j, b.Ordinal)
				}
				seg.refs = append(seg.refs, ref(b.Ordinal, b.ObjectID))
			}
		} else {
			for j, b := range byActive[seg.index] {
				if b.Ordinal != j {
					return nil, catalog.Corruptf(catalog.SourceInvariant, "bootstrap_live segment %d block %d is at ordinal %d", seg.index, j, b.Ordinal)
				}
				seg.refs = append(seg.refs, ref(b.Ordinal, b.ObjectID))
			}
		}
		seg.blocks = len(seg.refs)
	}
	return out, nil
}

func (s catalogMergeSource) fetcher() catalog.Fetcher { return objectFetcher{s.objects} }

// objectFetcher fetches object-stored blocks. Get verifies the object's
// length and SHA-256.
type objectFetcher struct{ objects objstore.Store }

func (f objectFetcher) Fetch(ctx context.Context, ref catalog.BlockRef) ([]byte, error) {
	loc, ok := ref.Loc.(catalog.ObjectBlock)
	if !ok {
		return nil, fmt.Errorf("orchestrator: merge: %s segment %d block %d is not in the object store (%T)", ref.Namespace, ref.Segment, ref.Block, ref.Loc)
	}
	return f.objects.Get(ctx, loc.ObjectID)
}

// writeInitialPhaseDisaggregated writes phase=bootstrap on a new catalog.
// `storage init` created segment 0 in both namespaces (design §15.1), but an
// init that ended after inserting the archive row did not, and refuses to
// run again, so this creates any that are missing. The phase write rides in
// the last transaction: a crash before it leaves the phase absent, and the
// next start repeats both steps.
func (o *Orchestrator) writeInitialPhaseDisaggregated(ctx context.Context, enteredAt time.Time) error {
	sess := o.cfg.Disaggregated.Session
	if _, err := sess.InitNamespace(ctx, catalog.Main, nil); err != nil {
		return fmt.Errorf("orchestrator: init %s: %w", catalog.Main, err)
	}
	b := metastore.NewOpBatch(nil)
	if err := lifecycle.StagePhase(b, lifecycle.PhaseBootstrap, enteredAt); err != nil {
		return err
	}
	if _, err := sess.InitNamespace(ctx, catalog.BootstrapLive, b.Ops()); err != nil {
		return fmt.Errorf("orchestrator: init %s and write initial phase: %w", catalog.BootstrapLive, err)
	}
	return nil
}

// finishMergeDisaggregated is merge's final transaction (design §10.10): it
// deletes the bootstrap_live namespace and its metadata keys and writes
// phase=steady_state, so a crash leaves either a resumable merge or a
// finished one.
func (o *Orchestrator) finishMergeDisaggregated(ctx context.Context) error {
	b := metastore.NewOpBatch(nil)
	b.Delete([]byte(catalog.BootstrapLiveSeqKey))
	b.Delete([]byte(mergeNextSourceIdxKey))
	if err := lifecycle.StagePhase(b, lifecycle.PhaseSteadyState, time.Now().UTC()); err != nil {
		return err
	}
	start := time.Now()
	if _, err := o.cfg.Disaggregated.Session.DeleteNamespace(ctx, catalog.BootstrapLive, b.Ops()); err != nil {
		return fmt.Errorf("orchestrator: merge: delete bootstrap_live: %w", err)
	}
	o.cfg.Metrics.observeState("write_phase_steady", time.Since(start).Seconds())
	return nil
}

// openDirect opens a direct-mode writer in ns.
func (o *Orchestrator) openDirect(ctx context.Context, ns catalog.Namespace, metrics *ingest.Metrics) (*ingest.Writer, error) {
	dc, err := o.cfg.Disaggregated.Direct(ctx, ns)
	if err != nil {
		return nil, fmt.Errorf("orchestrator: open direct mode in %s: %w", ns, err)
	}
	w, err := ingest.Open(ingest.Config{
		Store:          o.cfg.Store,
		Logger:         o.cfg.Logger,
		Metrics:        metrics,
		SegmentMetrics: o.cfg.SegmentMetrics,
		Namespace:      ns,
		Direct:         dc,
	})
	if err != nil {
		return nil, fmt.Errorf("orchestrator: open direct writer in %s: %w", ns, err)
	}
	return w, nil
}

// sealBootstrapLive seals bootstrap_live's active segment. It does nothing
// when that segment has no blocks.
func (o *Orchestrator) sealBootstrapLive(ctx context.Context) error {
	w, err := o.openDirect(ctx, catalog.BootstrapLive, nil)
	if err != nil {
		return err
	}
	if err := w.SealActiveAndClose(); err != nil {
		return fmt.Errorf("orchestrator: seal bootstrap-live segment: %w", err)
	}
	return nil
}

// runMergeDisaggregated is runMerge on the shared catalog (design §10.10).
// It drains bootstrap_live into main in direct mode, then commits the final
// transaction. Every step before that transaction commits on its own and is
// safe to repeat, so a crash anywhere resumes here.
//
// Merge-tail compaction and its manifest reconcile are skipped: compaction
// is off in disaggregated mode until stage 4 (D5).
func (o *Orchestrator) runMergeDisaggregated(ctx context.Context) error {
	d := o.cfg.Disaggregated
	src := catalogMergeSource{db: d.Session.DB(), objects: d.Objects}
	segs, err := src.segments(ctx)
	if err != nil {
		return err
	}
	// The final transaction deletes bootstrap_live in the same transaction
	// as the phase write, so no crash leaves merging without it. Local
	// mode's restart-after-cleanup guard has nothing to recover here.
	if len(segs) == 0 {
		return catalog.Corruptf(catalog.SourceInvariant, "phase is %s but bootstrap_live has no segments", lifecycle.PhaseMerging)
	}
	// Seal guard: a crash at crashpoint.AfterBootstrapLiveCloseBeforeSeal
	// leaves bootstrap_live's active segment unsealed, and the drain reads
	// only sealed sources.
	if last := segs[len(segs)-1]; last.state != catalog.Sealed && last.blocks > 0 {
		if err := o.sealBootstrapLive(ctx); err != nil {
			return fmt.Errorf("orchestrator: merge: %w", err)
		}
		o.logger.InfoContext(ctx, "sealed active bootstrap-live source before merge", "segment", last.index)
	}

	dst, err := o.openDirect(ctx, catalog.Main, o.cfg.IngestMetrics)
	if err != nil {
		return fmt.Errorf("orchestrator: merge: %w", err)
	}
	if err := initCompactionWatermarkFloor(o.cfg.Store, dst.NextSeq()); err != nil {
		if cerr := dst.Close(); cerr != nil {
			o.logger.WarnContext(ctx, "dst writer close after compaction watermark init failure", "err", cerr)
		}
		return err
	}
	runner := newMergeRunner(dst, o.cfg.Store, src, o.cfg.Logger, o.cfg.Metrics, o.cfg.CrashInjector)
	if err := runner.run(ctx); err != nil {
		if cerr := dst.Close(); cerr != nil {
			o.logger.WarnContext(ctx, "dst writer close after merge error", "err", cerr)
		}
		return err
	}
	if err := o.runPendingRepoRetryPass(ctx, dst); err != nil {
		if cerr := dst.Close(); cerr != nil {
			o.logger.WarnContext(ctx, "dst writer close after pending retry error", "err", cerr)
		}
		return err
	}
	// Close, not seal: the steady-state hot writer continues main's active
	// segment.
	if err := dst.Close(); err != nil {
		return fmt.Errorf("orchestrator: merge: close dst: %w", err)
	}

	if err := o.simulateCrash(ctx, crashpoint.AfterMergeDstSealBeforeDiscovery); err != nil {
		return err
	}
	if err := o.runMergeDiscovery(ctx, runner); err != nil {
		return err
	}
	if err := o.simulateCrash(ctx, crashpoint.AfterMergeDiscoveryBeforeCleanup); err != nil {
		return err
	}
	if err := o.finishMergeDisaggregated(ctx); err != nil {
		return err
	}
	return o.simulateCrash(ctx, crashpoint.AfterMergeCleanupComplete)
}
