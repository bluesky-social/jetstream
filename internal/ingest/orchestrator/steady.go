package orchestrator

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/ingest/live"
	"github.com/bluesky-social/jetstream/internal/ingest/repair"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/jcalabro/atmos"
	atmosrepo "github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"golang.org/x/sync/errgroup"
)

// runSteadyState opens a live.Consumer pointed at <DataDir>/segments
// and runs it until ctx is cancelled. Returns ctx.Err() on a clean
// shutdown; the underlying error otherwise.
//
// live.Consumer constructs and owns its own *ingest.Writer pointed
// at the same SegmentsDir. We deliberately do NOT open a second
// writer here: two writers sharing one active segment would race on
// the file offset, hand out duplicate seq numbers, and clobber each
// other's "seq/next" on close.
//
// The consumer's internal ingest.Open runs ScanMaxSeq against the
// active segment in <DataDir>/segments and reconciles the in-memory
// nextSeq against pebble's "seq/next", so steady-state continues
// exactly where the backfill writer left off.
//
// Pebble keys "seq/next" and "relay/cursor" are the steady-state
// defaults; the upstream firehose resumes from the bootstrap-time
// consumer's last persisted cursor and at-least-once delivery
// covers the at-most-one-block overlap.
func (o *Orchestrator) runSteadyState(ctx context.Context) error {
	return obs.Span(ctx, func(ctx context.Context) error {
		// Set the phase gauge before any I/O. On the resume-from-
		// PhaseSteadyState path Run dispatches here directly without
		// going through writeSteadyStatePhase, so this is the only place
		// the gauge gets set on that path.
		o.cfg.Metrics.setPhase(PhaseGaugeSteadyState)

		segmentsDir := filepath.Join(o.cfg.DataDir, "segments")
		if err := o.rebuildLiveTombstones(ctx); err != nil {
			return err
		}

		// With compaction disabled nothing ever evicts the live set, so
		// feeding it would leak memory without bound; leave it detached.
		tombstones := o.cfg.Tombstones
		tombstoneCap := o.cfg.CompactionTombstoneCap
		if o.cfg.CompactionInterval == 0 {
			tombstones = nil
			tombstoneCap = 0
		}
		var stageStore repair.StageStore
		switch {
		case o.cfg.Disaggregated != nil && o.cfg.Disaggregated.StageBlob != nil:
			stageStore = repair.NewBlobStageStore(o.cfg.Disaggregated.StageBlob, o.cfg.Disaggregated.StagePrefix)
		case o.cfg.DataDir != "":
			stageStore = repair.NewFSStageStore(o.cfg.FS, filepath.Join(o.cfg.DataDir, "repair"))
		default:
			stageStore = repair.NewMemStageStore()
		}

		rep, err := repair.New(repair.Config{
			Store:          o.cfg.Store,
			Stage:          stageStore,
			SyncStateStore: o.cfg.SyncStateStore,
			DropMetrics:    o.cfg.DropMetrics,
			Metrics:        o.cfg.RepairMetrics,
			Logger:         o.cfg.Logger,
			OnEvent:        o.cfg.OnEvent,
			FetchRepo:      o.makeRepairFetcher(),
		})
		if err != nil {
			return fmt.Errorf("orchestrator: init repair coordinator: %w", err)
		}
		// Restore active per-DID gates and pending verifier checkpoints BEFORE
		// opening the live consumer / starting the verifier or retry runner.
		if err := rep.Recover(ctx); err != nil {
			return fmt.Errorf("orchestrator: recover repair coordinator: %w", err)
		}

		cfg := live.Config{
			DataDir:                  o.cfg.DataDir,
			SegmentsDir:              segmentsDir,
			FS:                       o.cfg.FS,
			Store:                    o.cfg.Store,
			SeqKey:                   live.SteadySeqKey,
			ReserveClientVisibleSeqs: true,
			CursorKey:                live.CursorKey,
			RelayURL:                 o.cfg.RelayURL,
			// Bare cfg.Logger; live.Open sets its own component.
			Logger:                o.cfg.Logger,
			Metrics:               o.cfg.LiveMetrics,
			DropMetrics:           o.cfg.DropMetrics,
			WriterMetrics:         o.cfg.IngestMetrics,
			Verifier:              o.cfg.Verifier,
			SyncStateStore:        o.cfg.SyncStateStore,
			Repair:                rep,
			Tombstones:            tombstones,
			TombstoneCap:          tombstoneCap,
			CompactionTrigger:     o.compactionTrigger,
			SegmentMetrics:        o.cfg.SegmentMetrics,
			ReadLogRetentionBytes: o.cfg.ReadLogRetentionBytes,
			MaxEventsPerBlock:     o.cfg.SteadyMaxEventsPerBlock,
			MaxSegmentBytes:       o.cfg.SteadyMaxSegmentBytes,
			OnEvent:               o.cfg.OnEvent,
			OnUpstreamEventSeen:   o.cfg.LiveMetrics.NoteLastSeenUpstreamEvent,
			Namespace:             catalog.Main,
			ReconnectBackoff:      o.cfg.LiveReconnectBackoff,
			Dial:                  o.cfg.LiveDial,

			SegmentIOFaultInjector: o.cfg.SegmentIOFaultInjector,
		}
		if d := o.cfg.Disaggregated; d != nil {
			// Hot batches commit seq/next with their rows, so there is no
			// seq lease, local directory, or local catalog.
			hot, err := d.Hot(ctx)
			if err != nil {
				return fmt.Errorf("orchestrator: open hot mode: %w", err)
			}
			cfg.Hot = hot
			cfg.DataDir, cfg.SegmentsDir, cfg.FS = "", "", nil
			cfg.ReserveClientVisibleSeqs = false
		} else {
			cfg.Catalog = o.segments()
		}
		c, err := live.Open(cfg)
		if err != nil {
			return fmt.Errorf("orchestrator: open steady-state live consumer: %w", err)
		}
		defer func() {
			if cerr := c.Close(); cerr != nil {
				o.logger.ErrorContext(ctx, "close steady-state live consumer", "err", cerr)
			}
		}()

		if o.cfg.OnSteadyStateWriter != nil {
			o.cfg.OnSteadyStateWriter(c.Writer())
		}

		o.logger.InfoContext(ctx, "steady-state consumer running")

		g, gctx := errgroup.WithContext(ctx)

		// Run the main loop
		g.Go(func() error { return c.Run(gctx) })

		// Run the off-hot-path repair / ClassBulk publisher loop.
		g.Go(func() error {
			err := rep.Run(gctx)
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		})

		// Run the compactor loop
		g.Go(func() error {
			err := o.runSteadyCompactor(gctx, c.Writer())
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		})

		if o.cfg.FailedRepoRetryInterval > 0 {
			g.Go(func() error {
				err := backfill.RunFailedRepoRetry(gctx, backfill.RetryConfig{
					Store:         o.cfg.Store,
					Writer:        c.Writer(),
					Repair:        rep,
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
				})
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return err
			})

		}

		return g.Wait()
	})
}

// makeRepairFetcher returns a verified getRepo callback used only when
// resuming a crashed PhaseQueued repair job whose in-memory resync event is no
// longer present.
func (o *Orchestrator) makeRepairFetcher() repair.FetchRepoFunc {
	if o.cfg.RelayURL == "" || o.cfg.HTTPClient == nil {
		return nil
	}
	xc := &xrpc.Client{
		Host:       o.cfg.RelayURL,
		HTTPClient: gt.Some(o.cfg.HTTPClient),
	}
	var opts atmossync.Options
	opts.Client = xc
	if o.cfg.Directory != nil {
		opts.Directory = gt.Some(o.cfg.Directory)
	}
	sc := atmossync.NewClient(opts)
	dir := o.cfg.Directory
	return func(ctx context.Context, did atmos.DID) (*atmosrepo.Repo, *atmosrepo.Commit, error) {
		body, err := sc.GetRepoStream(ctx, did, "")
		if err != nil {
			return nil, nil, err
		}
		defer func() { _ = body.Close() }()
		rp, commit, err := atmosrepo.LoadFromCAR(bufio.NewReader(body))
		if err != nil {
			return nil, nil, err
		}
		if commit.DID != string(did) {
			return nil, nil, fmt.Errorf("repair: fetched commit DID %q does not match %q", commit.DID, did)
		}
		if dir != nil {
			id, err := dir.LookupDID(ctx, did)
			if err != nil {
				return nil, nil, fmt.Errorf("repair: resolve %s for signature verify: %w", did, err)
			}
			pub, err := id.PublicKey()
			if err != nil {
				return nil, nil, fmt.Errorf("repair: public key for %s: %w", did, err)
			}
			if err := commit.VerifySignature(pub); err != nil {
				dir.Purge(ctx, did)
				id2, err2 := dir.LookupDID(ctx, did)
				if err2 != nil {
					return nil, nil, fmt.Errorf("repair: re-resolve %s: %w", did, err2)
				}
				pub2, err2 := id2.PublicKey()
				if err2 != nil {
					return nil, nil, fmt.Errorf("repair: public key retry for %s: %w", did, err2)
				}
				if err2 := commit.VerifySignature(pub2); err2 != nil {
					return nil, nil, fmt.Errorf("repair: verify commit signature for %s: %w", did, err2)
				}
			}
		}
		return rp, commit, nil
	}
}
