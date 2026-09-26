// merge.go owns the State 5 cutover step that
// drains data/backfill/live_segments/ into data/segments/. Spec:
// specs/notes/2026-05-27-merge-phase-design.md.

package orchestrator

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/ingest/live"
	"github.com/bluesky-social/jetstream/internal/obs"
)

// runMerge is the cutover state machine's State 5: drain
// data/backfill/live_segments/ into data/segments/. Per the spec:
//
//  1. Restart guard: if the bootstrap_live namespace has no segments,
//     the prior run started cleanup; finish removing the namespace,
//     delete the cursor keys (they may still be set if the prior run
//     died between the removal and the deletes), and return.
//  2. Open the destination ingest.Writer on data/segments/ with
//     SeqKey=live.SteadySeqKey so survivors continue monotonically
//     from where backfill left off.
//  3. Build a mergeRunner and drive its drain loop. On error, best-
//     effort Close the dst writer (NOT seal — partial-merge active
//     must not be marked terminally sealed).
//  4. On success: SealActiveAndClose the dst writer, run new-DID
//     discovery (listRepos resume), delete the bootstrap_live
//     namespace (the backfill tree), delete both cursor keys.
func (o *Orchestrator) runMerge(ctx context.Context) error {
	return obs.Span(ctx, func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		start := time.Now()
		defer func() { o.cfg.Metrics.observeState("merge", time.Since(start).Seconds()) }()
		if o.cfg.Disaggregated != nil {
			return o.runMergeDisaggregated(ctx)
		}

		liveSegmentsDir := filepath.Join(o.cfg.DataDir, "backfill", "live_segments")
		segmentsDir := filepath.Join(o.cfg.DataDir, "segments")

		segs := o.segments()
		if err := segs.Refresh(ctx); err != nil {
			return fmt.Errorf("orchestrator: merge: refresh segment catalog: %w", err)
		}

		// Restart-after-cleanup guard. The bootstrap-live writer creates its
		// first segment when it opens, before phase=merging is written, so
		// an empty namespace means a prior run reached cleanup, after
		// discovery.
		if len(segs.Snapshot().Segments(catalog.BootstrapLive)) == 0 {
			// The prior process removed the backfill tree but may have died
			// (e.g. SIGKILL, page cache intact) before that dirent removal was
			// made durable. Observing the namespace as empty does not prove the
			// removal reached stable storage, and the cursor deletes below are
			// SyncWrites (durable immediately). DeleteNamespace fsyncs the data
			// dir even when the tree is gone, so a power loss here cannot leave
			// the cursors deleted while the backfill tree reappears — which
			// would skip this guard next boot and re-drain from cursor 0,
			// duplicating already-merged events.
			if err := segs.DeleteNamespace(catalog.BootstrapLive); err != nil {
				return fmt.Errorf("orchestrator: merge: remove backfill dir in restart-after-cleanup guard: %w", err)
			}
			if err := deleteMergeCursor(o.cfg.Store); err != nil {
				return err
			}
			return nil
		}
		// Seal guard: a crash at crashpoint.AfterBootstrapLiveCloseBeforeSeal
		// (finishBootstrap closed the bootstrap-live consumer but died before
		// re-opening it to seal) leaves the source namespace with an unsealed
		// trailing segment. The drain loop reads only sealed sources, so seal
		// it here before draining.
		if err := o.sealActiveMergeSource(ctx, segs, liveSegmentsDir); err != nil {
			return err
		}

		dst, err := ingest.Open(ingest.Config{
			SegmentsDir:                segmentsDir,
			DataDir:                    o.cfg.DataDir,
			FS:                         o.cfg.FS,
			Store:                      o.cfg.Store,
			SeqKey:                     live.SteadySeqKey,
			ReserveClientVisibleSeqs:   true,
			UnreservedSeqsUnobservable: true,
			Logger:                     o.cfg.Logger,
			Metrics:                    o.cfg.IngestMetrics,
			SegmentMetrics:             o.cfg.SegmentMetrics,
			Catalog:                    o.segments(),
			Namespace:                  catalog.Main,
			SegmentIOFaultInjector:     o.cfg.SegmentIOFaultInjector,
		})
		if err != nil {
			return fmt.Errorf("orchestrator: merge: open dst writer: %w", err)
		}
		if err := initCompactionWatermarkFloor(o.cfg.Store, dst.NextSeq()); err != nil {
			if cerr := dst.Close(); cerr != nil {
				o.logger.WarnContext(ctx, "dst writer close after compaction watermark init failure", "err", cerr)
			}
			return err
		}

		runner := newMergeRunner(dst, o.cfg.Store, localMergeSource{segs}, o.cfg.Logger, o.cfg.Metrics, o.cfg.CrashInjector)

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

		if err := dst.SealActiveAndClose(); err != nil {
			return fmt.Errorf("orchestrator: merge: seal dst: %w", err)
		}

		if err := o.runDeleteCompaction(ctx, compactionMergeTail, nil); err != nil {
			return fmt.Errorf("orchestrator: merge-tail compaction: %w", err)
		}
		// One-shot manifest reconcile (spec §7): the merge-tail pass is
		// manifest-oblivious, so before serving ungates every manifest
		// entry must match its on-disk header. Reconcile failure aborts
		// the transition — internal-state correctness, crash-loud.
		if err := o.reconcileCompactionManifestFromDisk(ctx); err != nil {
			return fmt.Errorf("orchestrator: merge-tail compaction manifest reconcile: %w", err)
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

		// DeleteNamespace makes the backfill-subtree removal durable before
		// the merge cursors are deleted. deleteMergeCursor commits durably,
		// so without that a power loss could leave the cursor deletion
		// durable while the data/backfill dirent removal is not. On restart
		// the phase is still PhaseMerging, live_segments would reappear, the
		// restart-after-cleanup guard would be skipped, and the drain would
		// re-run from cursor 0 — appending already-merged events into
		// data/segments and corrupting the archive.
		if err := segs.DeleteNamespace(catalog.BootstrapLive); err != nil {
			return fmt.Errorf("orchestrator: merge: remove backfill dir: %w", err)
		}
		if err := deleteMergeCursor(o.cfg.Store); err != nil {
			return err
		}
		if err := o.simulateCrash(ctx, crashpoint.AfterMergeCleanupComplete); err != nil {
			return err
		}
		return nil
	})
}

// sealActiveMergeSource ensures the trailing source segment is sealed
// before the drain loop reads it. Normally finishBootstrap seals it at
// cutover, but a crash at crashpoint.AfterBootstrapLiveCloseBeforeSeal
// can leave it active. Only the latest segment can be unsealed: the
// bootstrap-live writer holds exactly one active segment and rotation
// seals the old file before opening the next, so checking the last
// segment is sufficient. Idempotent — a no-op when the trailing
// segment is already sealed.
func (o *Orchestrator) sealActiveMergeSource(ctx context.Context, segs SegmentCatalog, liveSegmentsDir string) error {
	src := segs.Snapshot().Segments(catalog.BootstrapLive)
	if len(src) == 0 {
		return nil
	}
	latest := src[len(src)-1]
	if latest.State == catalog.Sealed {
		return nil
	}

	w, err := ingest.Open(ingest.Config{
		SegmentsDir:            liveSegmentsDir,
		DataDir:                o.cfg.DataDir,
		FS:                     o.cfg.FS,
		Store:                  o.cfg.Store,
		SeqKey:                 live.BootstrapSeqKey,
		Logger:                 o.cfg.Logger,
		Metrics:                nil,
		SegmentMetrics:         o.cfg.SegmentMetrics,
		MaxSegmentBytes:        0,
		Catalog:                segs,
		Namespace:              catalog.BootstrapLive,
		SegmentIOFaultInjector: o.cfg.SegmentIOFaultInjector,
	})
	if err != nil {
		return fmt.Errorf("orchestrator: merge: reopen active source for seal: %w", err)
	}
	if err := w.SealActiveAndClose(); err != nil {
		return fmt.Errorf("orchestrator: merge: seal active source: %w", err)
	}
	o.logger.InfoContext(ctx, "sealed active bootstrap-live source before merge", "segment", latest.Index)
	return nil
}

// runPendingRepoRetryPass repairs repos left pending. Bootstrap restart can
// defer pre-existing not_started rows to pending (#262). Repair them only
// after the captured live tail has merged, so the synthetic sync +
// replacement rows land above any stale account tombstones that were
// replayed from live_segments.
func (o *Orchestrator) runPendingRepoRetryPass(ctx context.Context, dst *ingest.Writer) error {
	if err := backfill.RunPendingRepoRetryPass(ctx, backfill.RetryConfig{
		Store:         o.cfg.Store,
		Writer:        dst,
		HTTPClient:    o.cfg.HTTPClient,
		RelayURL:      o.cfg.RelayURL,
		Logger:        o.cfg.Logger,
		Metrics:       o.cfg.BackfillMetrics,
		DropMetrics:   o.cfg.DropMetrics,
		NewHostClient: o.cfg.BackfillNewHostClient,
		Interval:      o.cfg.FailedRepoRetryInterval,
		Workers:       o.cfg.FailedRepoRetryWorkers,
		HostWorkers:   o.cfg.FailedRepoRetryHostWorkers,
		MaxDelay:      o.cfg.FailedRepoRetryMaxDelay,
	}); err != nil {
		return fmt.Errorf("orchestrator: merge: pending repo retry: %w", err)
	}
	return nil
}

// runMergeDiscovery runs new-DID discovery unless SkipMergeDiscovery.
func (o *Orchestrator) runMergeDiscovery(ctx context.Context, runner *mergeRunner) error {
	if o.cfg.SkipMergeDiscovery {
		return nil
	}
	limits := discoveryLimits{
		maxHosts:       o.cfg.BackfillMaxHosts,
		maxActiveHosts: o.cfg.BackfillMaxActiveHosts,
		retryDelay:     o.cfg.MergeDiscoveryRetryBaseDelay,
	}
	return runner.runDiscoveryWithClient(ctx, o.cfg.RelayURL, o.cfg.HTTPClient, o.cfg.BackfillNewHostClient, limits)
}
