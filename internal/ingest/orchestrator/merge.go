// Package orchestrator: merge.go owns the State 5 cutover step that
// drains data/backfill/live_segments/ into data/segments/. Spec:
// docs/superpowers/specs/2026-05-27-merge-phase-design.md.
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream-v2/internal/ingest/live"
	"github.com/bluesky-social/jetstream-v2/internal/obs"
)

// killAfterSealBeforeDiscovery, when non-nil, is invoked between
// dst.SealActiveAndClose and runner.runDiscovery. Test-only;
// production paths leave it nil.
//
// Used to reproduce the spec §5.3 "after SealActiveAndClose" crash
// window: the destination is fully sealed but discovery, RemoveAll,
// and the cursor deletes have not yet run. On restart, the source-
// loop is empty (cursor at len), seal is idempotent, discovery
// re-runs, and cleanup completes.
var killAfterSealBeforeDiscovery func() error

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

		dst, err := ingest.Open(ingest.Config{
			SegmentsDir:    segmentsDir,
			Store:          o.cfg.Store,
			SeqKey:         live.SteadySeqKey,
			Logger:         o.cfg.Logger,
			Metrics:        o.cfg.IngestMetrics,
			SegmentMetrics: o.cfg.SegmentMetrics,
		})
		if err != nil {
			return fmt.Errorf("orchestrator: merge: open dst writer: %w", err)
		}

		runner := newMergeRunner(dst, o.cfg.Store, liveSegmentsDir, o.cfg.Logger, o.cfg.Metrics)

		if err := runner.run(ctx); err != nil {
			if cerr := dst.Close(); cerr != nil {
				o.logger.WarnContext(ctx, "dst writer close after merge error", "err", cerr)
			}
			return err
		}

		if err := dst.SealActiveAndClose(); err != nil {
			return fmt.Errorf("orchestrator: merge: seal dst: %w", err)
		}

		if killAfterSealBeforeDiscovery != nil {
			if err := killAfterSealBeforeDiscovery(); err != nil {
				return err
			}
		}

		if err := runner.runDiscovery(ctx, o.cfg.RelayURL, o.cfg.HTTPClient); err != nil {
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
