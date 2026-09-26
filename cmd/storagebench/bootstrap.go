package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/ingest/maintainer"
	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/metastore"
	metapg "github.com/bluesky-social/jetstream/internal/metastore/pg"
	"github.com/bluesky-social/jetstream/internal/objstore/objcache"
	"github.com/bluesky-social/jetstream/internal/objstore/protocol"
	"github.com/bluesky-social/jetstream/internal/pgstore/pgfixture"
	"github.com/jcalabro/atmos"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	atmosrepo "github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/urfave/cli/v3"
	"golang.org/x/sync/errgroup"
)

func bootstrapCommand() *cli.Command {
	return &cli.Command{
		Name:  "bootstrap",
		Usage: "Drive bootstrap's leader writes and report fenced transactions per second (design §22)",
		Description: `Runs the three writers a bootstrapping leader runs, all in one catalog
session so they share the fence:

  - discovery: --hosts crawls, each calling the backfill store's Lookup and
    OnDiscover for every listed DID, as the atmos engine does. OnDiscover is
    one metadata transaction per DID.
  - downloads: --downloaders workers append each discovered repo to main in
    direct mode and complete it through the completion batcher, so
    completions ride in block commits.
  - live: --live-rate events/s into bootstrap_live in direct mode, each block
    commit carrying the relay cursor.

Downloads are local and unpaced unless --bulk-rate caps them, so the run
finds the rate the fence allows rather than the rate a real network would
deliver.`,
		Flags: []cli.Flag{
			&cli.DurationFlag{Name: "duration", Value: time.Minute},
			&cli.IntFlag{Name: "hosts", Value: 8, Validator: atLeastOne[int], Usage: "Concurrent listRepos crawls"},
			&cli.IntFlag{Name: "downloaders", Value: 16, Validator: atLeastOne[int]},
			&cli.Float64Flag{Name: "bulk-rate", Usage: "Cap on repo events/s appended to main; zero is unpaced"},
			&cli.Float64Flag{Name: "live-rate", Value: 330, Usage: "Live events/s into bootstrap_live"},
			&cli.Uint64Flag{Name: "seed", Value: 1},
			&cli.Uint64Flag{Name: "did-universe", Value: 40_000_000, Validator: atLeastOne[uint64], Usage: "Distinct accounts live traffic draws from"},
			&cli.Uint64Flag{Name: "pop1-repos", Value: 40_000_000, Usage: "Repos a pop1 bootstrap discovers, for the extrapolation"},
		},
		Action: runBootstrap,
	}
}

// bootstrapStats is what one bootstrap run measured.
type bootstrapStats struct {
	hosts, downloaders int
	bulkRate, liveRate float64
	pop1Repos          float64

	txn                      *txnStats
	discover                 samples // Lookup plus OnDiscover, per DID
	discovered, repos        atomic.Int64
	bulk, live               atomic.Int64
	mainBatches, liveBatches atomic.Int64
	start, end               time.Time
	walStart, wal            uint64
}

func runBootstrap(ctx context.Context, cmd *cli.Command) error {
	b, err := openBench(ctx, cmd)
	if err != nil {
		return err
	}
	defer b.close()

	st := jetstreamd.DefaultStorageConfig()
	stats := &bootstrapStats{
		txn:         newTxnStats(),
		pop1Repos:   float64(cmd.Uint64("pop1-repos")),
		liveRate:    cmd.Float64("live-rate"),
		bulkRate:    cmd.Float64("bulk-rate"),
		hosts:       cmd.Int("hosts"),
		downloaders: cmd.Int("downloaders"),
	}
	var cur atomic.Pointer[txnStats]
	cur.Store(newTxnStats()) // setup, not reported

	lease := b.pg.NewLease()
	if err := lease.Acquire(ctx, time.Hour); err != nil {
		return err
	}
	p := newPod()
	sess := catalog.NewSession(catalog.SessionConfig{
		DB:            &timedDB{DB: b.pg, stats: cur.Load},
		Epoch:         lease.Epoch(),
		Metrics:       p.catalog,
		LeaderMetrics: p.leader,
	})
	for _, ns := range catalog.Namespaces {
		if _, err := sess.InitNamespace(ctx, ns, nil); err != nil {
			return err
		}
	}
	meta := metapg.New(metapg.Config{DB: b.pg, Commit: func(ctx context.Context, ops []metastore.Op) error {
		_, err := sess.CommitMeta(ctx, ops)
		return err
	}})
	if err := lifecycle.WritePhase(ctx, meta, lifecycle.PhaseBootstrap, time.Now()); err != nil {
		return err
	}

	cache := objcache.New(objcache.Config{MaxBytes: st.ObjectCacheBytes, Memory: p.memory})
	uploader, err := protocol.NewUploader(protocol.UploaderConfig{
		Blob:        b.blob,
		ArchiveID:   b.archive.ArchiveID,
		GCDelay:     st.GC.Delay,
		OrphanAge:   st.GC.OrphanAge,
		Concurrency: st.S3.UploadConcurrency,
		Metrics:     p.protocol,
	})
	if err != nil {
		return err
	}
	objects, err := protocol.NewReader(protocol.ReaderConfig{
		Rows:           protocol.DBRows{DB: b.pg},
		Blob:           b.blob,
		ArchiveID:      b.archive.ArchiveID,
		Cache:          cache,
		Metrics:        p.protocol,
		CatalogMetrics: p.catalog,
	})
	if err != nil {
		return err
	}

	bm := backfill.NewMetrics(p.reg)
	store := backfill.NewStore(meta, bm)
	if err := store.SeedCounts(ctx); err != nil {
		return err
	}
	completions := backfill.NewCompletionBatcher(store, bm)
	store.SetCompletionBatcher(completions)
	crawl := store.AtmosStore()
	hosts := make([]string, stats.hosts)
	for i := range hosts {
		hosts[i] = fmt.Sprintf("pds%d.storagebench.invalid", i)
		if err := crawl.OnHost(ctx, atmosbackfill.HostInfo{Hostname: hosts[i], RelayStatus: "active"}); err != nil {
			return err
		}
	}

	gctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	onFailure := func(err error) { cancel(err) }
	openDirect := func(ns catalog.Namespace, batches *atomic.Int64, hook ingest.DurableBatchHook) (*ingest.Writer, error) {
		sealer, err := maintainer.OpenSegment(ctx, maintainer.SegmentConfig{
			Session:         sess,
			Namespace:       ns,
			Uploader:        uploader,
			Objects:         objects,
			Cache:           cache,
			ReadConcurrency: st.S3.ReadConcurrency,
		})
		if err != nil {
			return nil, err
		}
		return ingest.Open(ingest.Config{
			Store:     meta,
			Logger:    b.logger,
			Namespace: ns,
			OnDurableBatch: func(ctx context.Context, mb metastore.Batch, next uint64, force bool, v any) (func(), func(error), error) {
				batches.Add(1)
				return hook(ctx, mb, next, force, v)
			},
			Direct: &ingest.DirectConfig{
				Session:           sess,
				Uploader:          uploader,
				Sealer:            sealer,
				UploadConcurrency: st.S3.UploadConcurrency,
				OnFailure:         onFailure,
			},
		})
	}
	mainW, err := openDirect(catalog.Main, &stats.mainBatches, completions.StageDurable)
	if err != nil {
		return err
	}
	closeMain := sync.OnceValue(mainW.Close)
	defer func() { _ = closeMain() }()
	var lastUpstream atomic.Int64
	liveW, err := openDirect(catalog.BootstrapLive, &stats.liveBatches,
		func(_ context.Context, mb metastore.Batch, _ uint64, _ bool, _ any) (func(), func(error), error) {
			if c := lastUpstream.Load(); c > 0 {
				mb.Set([]byte(catalog.RelayCursorKey), metastore.EncodeVersionedUint64LE(1, uint64(c)))
			}
			return nil, nil, nil
		})
	if err != nil {
		return err
	}
	closeLive := sync.OnceValue(liveW.Close)
	defer func() { _ = closeLive() }()

	fmt.Printf("bootstrap: %d hosts, %d downloaders, bulk %s, %.0f live ev/s for %s\n",
		stats.hosts, stats.downloaders, rateString(stats.bulkRate), stats.liveRate, cmd.Duration("duration"))
	if stats.walStart, err = pgfixture.WALPosition(gctx, b.pg); err != nil {
		return err
	}
	stats.start = time.Now()
	cur.Store(stats.txn)
	runCtx, stop := context.WithTimeout(gctx, cmd.Duration("duration"))
	defer stop()
	runErr := driveBootstrap(runCtx, cmd, stats, crawl, store, completions, hosts, mainW, liveW, &lastUpstream)
	stats.end = time.Now()
	cur.Store(newTxnStats()) // shutdown, not reported
	if runErr == nil {
		wal, err := pgfixture.WALPosition(gctx, b.pg)
		if err != nil {
			return err
		}
		stats.wal = wal - stats.walStart
	}
	merr, lerr := closeMain(), closeLive()
	if cause := context.Cause(gctx); cause != nil && !errors.Is(cause, context.Canceled) {
		runErr = errors.Join(runErr, cause)
	}
	if runErr == nil {
		reportBootstrap(os.Stdout, stats)
	}
	return errors.Join(runErr, merr, lerr, sess.Err())
}

func rateString(r float64) string {
	if r <= 0 {
		return "unpaced"
	}
	return fmt.Sprintf("%.0f ev/s", r)
}

// driveBootstrap runs discovery, downloads, and live appends until ctx
// ends. Only a failure is an error: the deadline is how a run finishes.
func driveBootstrap(ctx context.Context, cmd *cli.Command, stats *bootstrapStats, crawl atmosbackfill.Store,
	store *backfill.Store, completions interface {
		RecordWatermark(atmos.DID, uint64, bool)
	}, hosts []string, mainW, liveW *ingest.Writer, lastUpstream *atomic.Int64,
) error {
	seed, universe := cmd.Uint64("seed"), cmd.Uint64("did-universe")
	type job struct {
		did  atmos.DID
		host string
	}
	// The engine's per-host job channel holds two jobs per worker.
	jobs := make(chan job, 2*stats.downloaders)
	g, gctx := errgroup.WithContext(ctx)
	// Once the run is ending, a failed call is its echo, not a failure.
	done := func(err error) error {
		select {
		case <-gctx.Done():
			return nil
		default:
			return err
		}
	}

	var next atomic.Uint64
	for _, host := range hosts {
		g.Go(func() error {
			for gctx.Err() == nil {
				did := atmos.DID(didFor(next.Add(1)))
				start := time.Now()
				rec, err := crawl.Lookup(gctx, did)
				if err != nil {
					return done(err)
				}
				if rec.State == atmosbackfill.StateUnknown {
					if err := crawl.OnDiscover(gctx, host, atmossync.ListReposEntry{DID: did, Active: true}); err != nil {
						return done(err)
					}
				}
				stats.discover.add(time.Since(start))
				stats.discovered.Add(1)
				select {
				case jobs <- job{did: did, host: host}:
				case <-gctx.Done():
					return nil
				}
			}
			return nil
		})
	}

	var claimed atomic.Int64
	start := time.Now()
	for i := range stats.downloaders {
		gen := newGenerator(seed^uint64(i+1)<<16, universe)
		g.Go(func() error {
			for {
				var j job
				select {
				case j = <-jobs:
				case <-gctx.Done():
					return nil
				}
				n := gen.repoSize()
				if stats.bulkRate > 0 {
					before := claimed.Add(int64(n)) - int64(n)
					due := start.Add(time.Duration(float64(before) / stats.bulkRate * float64(time.Second)))
					select {
					case <-gctx.Done():
						return nil
					case <-time.After(time.Until(due)):
					}
				}
				evs := gen.repo(time.Now(), n)
				for k := range evs {
					evs[k].DID = string(j.did)
				}
				if err := mainW.AppendBatch(gctx, evs); err != nil {
					return done(err)
				}
				// NextSeq is at least one past this repo's last event, so
				// the watermark never lets a completion commit early.
				completions.RecordWatermark(j.did, mainW.NextSeq()-1, true)
				if err := store.OnComplete(gctx, j.did, j.host, &atmosrepo.Commit{DID: string(j.did), Rev: evs[0].Rev}); err != nil {
					return done(err)
				}
				stats.repos.Add(1)
				stats.bulk.Add(int64(n))
			}
		})
	}

	// The backfill runner drains durability every 30s while completions
	// wait, so a quiet block still commits them.
	g.Go(func() error {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-gctx.Done():
				return nil
			case <-t.C:
			}
			if err := mainW.DrainDurability(gctx); err != nil {
				return done(err)
			}
		}
	})

	gen := newGenerator(seed, universe)
	g.Go(func() error {
		if stats.liveRate <= 0 {
			return nil
		}
		t := time.NewTicker(time.Millisecond)
		defer t.Stop()
		var sent int64
		for {
			select {
			case <-gctx.Done():
				return nil
			case <-t.C:
			}
			for due := int64(stats.liveRate*time.Since(start).Seconds()) - sent; due > 0; due-- {
				sent++
				ev := gen.live(time.Now(), sent)
				if err := liveW.Append(gctx, &ev); err != nil {
					return done(err)
				}
				lastUpstream.Store(sent)
				stats.live.Add(1)
			}
		}
	})
	return g.Wait()
}

func reportBootstrap(out *os.File, s *bootstrapStats) {
	secs := s.end.Sub(s.start).Seconds()
	rate := func(n int64) string { return fmt.Sprintf("%d (%.0f/s)", n, float64(n)/secs) }
	rows := [][2]string{
		{"duration", s.end.Sub(s.start).Round(time.Millisecond).String()},
		{"discovered DIDs", rate(s.discovered.Load())},
		{"discovery per DID", s.discover.summary().String()},
		{"repos appended", rate(s.repos.Load())},
		{"bulk events", rate(s.bulk.Load())},
		{"live events", rate(s.live.Load())},
		{"durable batches", fmt.Sprintf("main %d, bootstrap_live %d", s.mainBatches.Load(), s.liveBatches.Load())},
	}
	var (
		all  int
		busy time.Duration
	)
	for _, k := range s.txn.kinds() {
		tot := s.txn.total[k].summary()
		commit := s.txn.commit[k].summary()
		all += tot.n
		busy += commit.mean * time.Duration(commit.n)
		rows = append(rows,
			[2]string{"txn " + string(k), fmt.Sprintf("%.1f/s", float64(tot.n)/secs)},
			[2]string{"txn " + string(k) + " total", tot.String()},
			[2]string{"txn " + string(k) + " commit", commit.String()},
		)
	}
	// Every leader transaction holds the fence row from its first statement
	// to its commit. A total also counts the wait for the row, so it
	// overstates the hold; the summed commits are a lower bound on it.
	rows = append(rows,
		[2]string{"fenced txn", fmt.Sprintf("%d (%.1f/s)", all, float64(all)/secs)},
		[2]string{"fence held (commits)", fmt.Sprintf(">= %.0f%%", 100*busy.Seconds()/secs)},
		[2]string{"WAL", fmt.Sprintf("%s (%s/s)", mib(float64(s.wal)), mib(float64(s.wal)/secs))},
	)
	if d := float64(s.discovered.Load()) / secs; d > 0 && s.pop1Repos > 0 {
		rows = append(rows, [2]string{"pop1 discovery at this rate",
			fmt.Sprintf("%.1fh for %.0fM repos", s.pop1Repos/d/3600, s.pop1Repos/1e6)})
	}
	printKV(out, rows)
}
