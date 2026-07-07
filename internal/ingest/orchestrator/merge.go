// merge.go owns the State 5 cutover step that
// drains data/backfill/live_segments/ into data/segments/. Spec:
// specs/notes/2026-05-27-merge-phase-design.md.

package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/ingest/live"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/segment"
)

// runMerge is the cutover state machine's State 5: drain
// data/backfill/live_segments/ into data/segments/. Per the spec:
//
//  1. Restart guard: if data/backfill/live_segments/ is gone, the
//     prior run finished cleanup; just delete the cursor keys (they
//     may still be set if the prior run died between RemoveAll and
//     the deletes) and return.
//  2. Open the destination ingest.Writer on data/segments/ with
//     SeqKey=live.SteadySeqKey so survivors continue monotonically
//     from where backfill left off.
//  3. Build a mergeRunner and drive its drain loop. On error, best-
//     effort Close the dst writer (NOT seal — partial-merge active
//     must not be marked terminally sealed).
//  4. On success: SealActiveAndClose the dst writer, run new-DID
//     discovery (listRepos resume), RemoveAll the backfill tree,
//     delete both cursor keys.
func (o *Orchestrator) runMerge(ctx context.Context) error {
	return obs.Span(ctx, func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		start := time.Now()
		defer func() { o.cfg.Metrics.observeState("merge", time.Since(start).Seconds()) }()

		liveSegmentsDir := filepath.Join(o.cfg.DataDir, "backfill", "live_segments")
		segmentsDir := filepath.Join(o.cfg.DataDir, "segments")

		// Restart-after-cleanup guard.
		if _, err := os.Stat(liveSegmentsDir); errors.Is(err, os.ErrNotExist) {
			if err := deleteMergeCursor(o.cfg.Store); err != nil {
				return err
			}
			if err := backfill.DeleteBootstrapLastListReposCursor(o.cfg.Store); err != nil {
				return err
			}
			return nil
		} else if err != nil {
			return fmt.Errorf("orchestrator: merge: stat live_segments: %w", err)
		}
		// Seal guard: a crash at crashpoint.AfterBootstrapLiveCloseBeforeSeal
		// (finishBootstrap closed the bootstrap-live consumer but died before
		// re-opening it to seal) leaves the source tree with an unsealed
		// trailing segment. The drain loop's segment.Open would reject it
		// with ErrActiveSegment, so seal it here before draining.
		if err := o.sealActiveMergeSource(ctx, liveSegmentsDir); err != nil {
			return err
		}

		dst, err := ingest.Open(ingest.Config{
			SegmentsDir:            segmentsDir,
			DataDir:                o.cfg.DataDir,
			Store:                  o.cfg.Store,
			SeqKey:                 live.SteadySeqKey,
			Logger:                 o.cfg.Logger,
			Metrics:                o.cfg.IngestMetrics,
			SegmentMetrics:         o.cfg.SegmentMetrics,
			OnAfterSeal:            o.cfg.IngestOnAfterSeal,
			SegmentIOFaultInjector: o.cfg.SegmentIOFaultInjector,
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

		runner := newMergeRunner(dst, o.cfg.Store, liveSegmentsDir, o.cfg.Logger, o.cfg.Metrics, o.cfg.CrashInjector)

		if err := runner.run(ctx); err != nil {
			if cerr := dst.Close(); cerr != nil {
				o.logger.WarnContext(ctx, "dst writer close after merge error", "err", cerr)
			}
			return err
		}

		// Bootstrap restart can defer pre-existing not_started rows to pending
		// (#262). Repair them only after the captured live tail has merged, so
		// the synthetic sync + replacement rows land above any stale account
		// tombstones that were replayed from live_segments.
		if err := backfill.RunPendingRepoRetryPass(ctx, backfill.RetryConfig{
			Store:       o.cfg.Store,
			Writer:      dst,
			HTTPClient:  o.cfg.HTTPClient,
			RelayURL:    o.cfg.RelayURL,
			Logger:      o.cfg.Logger,
			Metrics:     o.cfg.BackfillMetrics,
			DropMetrics: o.cfg.DropMetrics,
			Interval:    o.cfg.FailedRepoRetryInterval,
			Workers:     o.cfg.FailedRepoRetryWorkers,
			HostWorkers: o.cfg.FailedRepoRetryHostWorkers,
			MaxDelay:    o.cfg.FailedRepoRetryMaxDelay,
		}); err != nil {
			if cerr := dst.Close(); cerr != nil {
				o.logger.WarnContext(ctx, "dst writer close after pending retry error", "err", cerr)
			}
			return fmt.Errorf("orchestrator: merge: pending repo retry: %w", err)
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
		if err := o.reconcileCompactionManifestFromDisk(segmentsDir); err != nil {
			return fmt.Errorf("orchestrator: merge-tail compaction manifest reconcile: %w", err)
		}

		if err := o.simulateCrash(ctx, crashpoint.AfterMergeDstSealBeforeDiscovery); err != nil {
			return err
		}

		if !o.cfg.SkipMergeDiscovery {
			if err := runner.runDiscovery(ctx, o.cfg.RelayURL, o.cfg.HTTPClient); err != nil {
				return err
			}
		}
		if err := o.simulateCrash(ctx, crashpoint.AfterMergeDiscoveryBeforeCleanup); err != nil {
			return err
		}

		if err := os.RemoveAll(filepath.Join(o.cfg.DataDir, "backfill")); err != nil {
			return fmt.Errorf("orchestrator: merge: remove backfill dir: %w", err)
		}
		if err := deleteMergeCursor(o.cfg.Store); err != nil {
			return err
		}
		if err := backfill.DeleteBootstrapLastListReposCursor(o.cfg.Store); err != nil {
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
// seals the old file before opening the next, so checking
// files[len-1] is sufficient. Idempotent — a no-op when the trailing
// segment is already sealed.
func (o *Orchestrator) sealActiveMergeSource(ctx context.Context, liveSegmentsDir string) error {
	files, err := ingest.SegmentFiles(liveSegmentsDir)
	if err != nil {
		return fmt.Errorf("orchestrator: merge: list source segments before seal guard: %w", err)
	}
	if len(files) == 0 {
		return nil
	}

	latest := files[len(files)-1]
	rd, err := segment.Open(segment.ReaderConfig{Path: latest.Path})
	if err == nil {
		return rd.Close()
	}
	if !errors.Is(err, segment.ErrActiveSegment) {
		return fmt.Errorf("orchestrator: merge: inspect source segment %s: %w", latest.Path, err)
	}

	w, err := ingest.Open(ingest.Config{
		SegmentsDir:            liveSegmentsDir,
		DataDir:                o.cfg.DataDir,
		Store:                  o.cfg.Store,
		SeqKey:                 live.BootstrapSeqKey,
		Logger:                 o.cfg.Logger,
		Metrics:                nil,
		SegmentMetrics:         o.cfg.SegmentMetrics,
		MaxSegmentBytes:        0,
		SegmentIOFaultInjector: o.cfg.SegmentIOFaultInjector,
	})
	if err != nil {
		return fmt.Errorf("orchestrator: merge: reopen active source for seal: %w", err)
	}
	if err := w.SealActiveAndClose(); err != nil {
		return fmt.Errorf("orchestrator: merge: seal active source: %w", err)
	}
	o.logger.InfoContext(ctx, "sealed active bootstrap-live source before merge", "segment", latest.Idx)
	return nil
}
