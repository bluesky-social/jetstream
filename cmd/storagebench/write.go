package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/catalog/follower"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/maintainer"
	"github.com/bluesky-social/jetstream/internal/ingest/syncstate"
	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/metastore"
	metapg "github.com/bluesky-social/jetstream/internal/metastore/pg"
	"github.com/bluesky-social/jetstream/internal/objstore/objcache"
	"github.com/bluesky-social/jetstream/internal/objstore/protocol"
	"github.com/bluesky-social/jetstream/internal/pgstore/pgfixture"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/urfave/cli/v3"
	"golang.org/x/sync/errgroup"
)

func writeCommand() *cli.Command {
	return &cli.Command{
		Name:  "write",
		Usage: "Drive the hot writer through phases of live and bulk traffic, and report PG, WAL, seal, and end-to-end latency",
		Description: `Each --phase is name:rate:duration[:bulk[:bulk_rate]], for example
load:3000:5m or bulk:3000:0:4000000. rate is live events/s. A phase with
bulk > 0 also runs bulk resync workers until they have appended that many
events, at most bulk_rate events/s if one is given, and lasts until both its
duration and its bulk are done.

The writer runs with the production storage defaults. Two followers read
the result: the leader pod's (doorbell and NOTIFY) and another pod's (its
own pool, NOTIFY only). End-to-end latency is from an event's witness time
to its delivery through a follower's readable log.`,
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:  "phase",
				Value: []string{"idle:10:1m", "pop1:330:3m", "load:3000:6m", "bulk:3000:0:4000000"},
			},
			&cli.Uint64Flag{Name: "seed", Value: 1},
			&cli.Uint64Flag{Name: "did-universe", Value: 40_000_000, Validator: atLeastOne[uint64], Usage: "Distinct accounts live traffic draws from"},
			&cli.DurationFlag{Name: "progress", Value: 10 * time.Second, Usage: "How often to print progress within a phase"},
			&cli.IntFlag{Name: "bulk-workers", Value: 4, Validator: atLeastOne[int]},
			&cli.Int64Flag{Name: "bulk-pending-bytes", Usage: "Override JETSTREAM_HOT_BULK_PENDING_BYTES's default"},
		},
		Action: runWrite,
	}
}

type phaseSpec struct {
	name string
	rate float64
	dur  time.Duration
	bulk int64
	// bulkRate caps bulk events/s; zero appends as fast as admission allows.
	bulkRate float64
}

func parsePhase(s string) (phaseSpec, error) {
	parts := strings.Split(s, ":")
	if len(parts) < 3 || len(parts) > 5 {
		return phaseSpec{}, fmt.Errorf("phase %q: want name:rate:duration[:bulk[:bulk_rate]]", s)
	}
	p := phaseSpec{name: parts[0]}
	var err error
	if p.rate, err = strconv.ParseFloat(parts[1], 64); err != nil || p.rate < 0 {
		return phaseSpec{}, fmt.Errorf("phase %q: bad rate", s)
	}
	if parts[2] != "0" {
		if p.dur, err = time.ParseDuration(parts[2]); err != nil || p.dur < 0 {
			return phaseSpec{}, fmt.Errorf("phase %q: bad duration", s)
		}
	}
	if len(parts) >= 4 {
		if p.bulk, err = strconv.ParseInt(parts[3], 10, 64); err != nil || p.bulk < 0 {
			return phaseSpec{}, fmt.Errorf("phase %q: bad bulk count", s)
		}
	}
	if len(parts) == 5 {
		if p.bulkRate, err = strconv.ParseFloat(parts[4], 64); err != nil || p.bulkRate < 0 {
			return phaseSpec{}, fmt.Errorf("phase %q: bad bulk rate", s)
		}
	}
	if p.dur == 0 && p.bulk == 0 {
		return phaseSpec{}, fmt.Errorf("phase %q: needs a duration or a bulk count", s)
	}
	return p, nil
}

// phaseStats is what one phase measured. The writer's hooks record into
// whichever phase is current.
type phaseStats struct {
	spec          phaseSpec
	txn           *txnStats
	e2e           [2]samples // leader pod, other pod
	fellBehind    [2]atomic.Int64
	folds, seals  samples
	live, bulk    atomic.Int64
	metaOps       atomic.Int64 // metadata ops the durable hook staged
	metaBatches   atomic.Int64
	maxQueued     atomic.Int64
	maxUnfolded   atomic.Int64
	batches       batchCounts // hot batches committed during the phase
	start, end    time.Time
	walStart, wal uint64
	dbBytes       int64
}

func storeMax(a *atomic.Int64, v int64) {
	for {
		cur := a.Load()
		if v <= cur || a.CompareAndSwap(cur, v) {
			return
		}
	}
}

type durablePrepare struct {
	cursor int64
	sync   *syncstate.Snapshot
}

func runWrite(ctx context.Context, cmd *cli.Command) error {
	var specs []phaseSpec
	for _, s := range cmd.StringSlice("phase") {
		p, err := parsePhase(s)
		if err != nil {
			return err
		}
		specs = append(specs, p)
	}
	b, err := openBench(ctx, cmd)
	if err != nil {
		return err
	}
	defer b.close()

	st := jetstreamd.DefaultStorageConfig()
	if n := cmd.Int64("bulk-pending-bytes"); n > 0 {
		st.Hot.BulkPendingBytes = n
	}
	var cur atomic.Pointer[phaseStats]
	cur.Store(&phaseStats{txn: newTxnStats()}) // setup, not reported

	lease := b.pg.NewLease()
	if err := lease.Acquire(ctx, time.Hour); err != nil {
		return err
	}
	leaderPod := newPod()
	sess := catalog.NewSession(catalog.SessionConfig{
		DB:            &timedDB{DB: b.pg, stats: func() *txnStats { return cur.Load().txn }},
		Epoch:         lease.Epoch(),
		Metrics:       leaderPod.catalog,
		LeaderMetrics: leaderPod.leader,
	})
	if _, err := sess.InitNamespace(ctx, catalog.Main, nil); err != nil {
		return err
	}
	meta := metapg.New(metapg.Config{DB: b.pg, Commit: func(ctx context.Context, ops []metastore.Op) error {
		_, err := sess.CommitMeta(ctx, ops)
		return err
	}})
	if err := lifecycle.WritePhase(ctx, meta, lifecycle.PhaseSteadyState, time.Now()); err != nil {
		return err
	}

	cache := objcache.New(objcache.Config{MaxBytes: st.ObjectCacheBytes, Memory: leaderPod.memory})
	uploader, err := protocol.NewUploader(protocol.UploaderConfig{
		Blob:        b.blob,
		ArchiveID:   b.archive.ArchiveID,
		GCDelay:     st.GC.Delay,
		OrphanAge:   st.GC.OrphanAge,
		Concurrency: st.S3.UploadConcurrency,
		Metrics:     leaderPod.protocol,
	})
	if err != nil {
		return err
	}
	objects, err := protocol.NewReader(protocol.ReaderConfig{
		Rows:           protocol.DBRows{DB: b.pg},
		Blob:           b.blob,
		ArchiveID:      b.archive.ArchiveID,
		Cache:          cache,
		Metrics:        leaderPod.protocol,
		CatalogMetrics: leaderPod.catalog,
	})
	if err != nil {
		return err
	}

	mm := maintainer.NewMetrics(leaderPod.reg)
	mm.FoldDuration = observedHistogram{mm.FoldDuration, func(d time.Duration) { cur.Load().folds.add(d) }}
	mm.SealDuration = observedHistogram{mm.SealDuration, func(d time.Duration) { cur.Load().seals.add(d) }}

	gctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	onFailure := func(err error) { cancel(err) }
	m, err := maintainer.Open(gctx, maintainer.Config{
		Session:         sess,
		Uploader:        uploader,
		Objects:         objects,
		Cache:           cache,
		ReadConcurrency: st.S3.ReadConcurrency,
		Logger:          b.logger,
		Metrics:         mm,
		OnFailure:       onFailure,
	})
	if err != nil {
		return err
	}
	defer func() { _ = m.Close() }()
	resume, err := m.Rebuild(gctx, maintainer.RebuildConfig{BlockMaxAge: st.BlockMaxAge})
	if err != nil {
		return err
	}

	// The other pod has its own pool, as it would in production.
	otherPod := newPod()
	otherDB, err := b.openStore(ctx, cmd, nil)
	if err != nil {
		return err
	}
	defer otherDB.Close()
	otherCache := objcache.New(objcache.Config{MaxBytes: st.ObjectCacheBytes, Memory: otherPod.memory})
	var followers [2]*follower.Follower
	if followers[0], _, err = b.newFollower(leaderPod, b.pg, cache, st.CatalogPollInterval, st.MaxViewAge, st.S3.ReadConcurrency); err != nil {
		return err
	}
	if followers[1], _, err = b.newFollower(otherPod, otherDB, otherCache, st.CatalogPollInterval, st.MaxViewAge, st.S3.ReadConcurrency); err != nil {
		return err
	}

	state := syncstate.New(meta)
	var (
		lastUpstream atomic.Int64
		committed    atomic.Uint64
	)
	w, err := ingest.Open(ingest.Config{
		Logger:  b.logger,
		Metrics: leaderPod.ingest,
		// The live consumer's bookkeeping: a chain-state upsert for each
		// commit, and the relay cursor, both staged into the batch that
		// makes the events durable.
		OnAppend: func(ev *segment.Event) error {
			if ev.UpstreamRelayCursor == 0 {
				return nil
			}
			did := atmos.DID(ev.DID)
			if err := state.SaveChain(ctx, did, atmossync.ChainState{Rev: ev.Rev, Data: cbor.ComputeCID(cbor.CodecDagCBOR, []byte(ev.DID+ev.Rev))}); err != nil {
				return err
			}
			state.PromoteChain(did, ev.Rev)
			lastUpstream.Store(ev.UpstreamRelayCursor)
			return nil
		},
		DurableBatchPrepareValue: func() any {
			return durablePrepare{cursor: lastUpstream.Load(), sync: state.Snapshot()}
		},
		OnDurableBatch: func(_ context.Context, mb metastore.Batch, next uint64, _ bool, v any) (func(), func(error), error) {
			p, ok := v.(durablePrepare)
			if !ok {
				return nil, nil, fmt.Errorf("durable batch prepare value has type %T", v)
			}
			if p.cursor > 0 {
				mb.Set([]byte(catalog.RelayCursorKey), metastore.EncodeVersionedUint64LE(1, uint64(p.cursor)))
			}
			if p.sync != nil {
				state.StageSnapshot(mb, p.sync)
			}
			ps := cur.Load()
			ps.metaOps.Add(int64(mb.Len()))
			ps.metaBatches.Add(1)
			return func() {
					state.CommitStaged()
					committed.Store(next)
				}, func(err error) {
					if err != nil {
						state.AbortStaged()
					}
				}, nil
		},
		Hot: &ingest.HotConfig{
			Session:           sess,
			Uploader:          uploader,
			Sink:              m,
			Resume:            resume,
			BatchMaxAge:       st.Hot.BatchMaxAge,
			BlockMaxAge:       st.BlockMaxAge,
			UploadConcurrency: st.S3.UploadConcurrency,
			InlineBytesPerSec: st.Hot.InlineBytesPerSec,
			BulkPendingBytes:  st.Hot.BulkPendingBytes,
			PendingBytes:      st.Hot.PendingBytes,
			MaxUnfoldedEvents: int64(st.Hot.MaxUnfoldedEvents),
			OnCommit:          func(uint64) { followers[0].Doorbell() },
			OnFailure:         onFailure,
		},
	})
	if err != nil {
		return err
	}
	closeWriter := sync.OnceValue(w.Close)
	defer func() { _ = closeWriter() }()

	g, gctx := errgroup.WithContext(gctx)
	for _, f := range followers {
		g.Go(func() error {
			if err := f.Run(gctx); err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		})
	}
	for i, f := range followers {
		if err := waitLog(gctx, f); err != nil {
			return err
		}
		g.Go(func() error { return subscribe(gctx, f, i, &cur) })
	}
	g.Go(func() error {
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		progress := max(1, int(cmd.Duration("progress")/(100*time.Millisecond)))
		for n := 1; ; n++ {
			select {
			case <-gctx.Done():
				return nil
			case <-t.C:
			}
			p := cur.Load()
			queued, unfolded := gaugeValue(mm.QueuedBlocks), gaugeValue(leaderPod.ingest.HotUnfoldedEvents)
			storeMax(&p.maxQueued, int64(queued))
			storeMax(&p.maxUnfolded, int64(unfolded))
			if n%progress == 0 && p.spec.name != "" {
				fmt.Printf("  %s: +%s live=%d bulk=%d next_seq=%d committed=%d visible=%d/%d pending=%s/%s unfolded=%.0f queued_blocks=%.0f\n",
					p.spec.name, time.Since(p.start).Round(time.Second), p.live.Load(), p.bulk.Load(),
					w.NextSeq(), committed.Load(), followers[0].Log().TipSeq(), followers[1].Log().TipSeq(),
					mib(gaugeValue(leaderPod.ingest.HotPendingBytes.WithLabelValues(ingest.ClassLive.String()))),
					mib(gaugeValue(leaderPod.ingest.HotPendingBytes.WithLabelValues(ingest.ClassBulk.String()))),
					unfolded, queued)
			}
		}
	})

	var (
		results  []*phaseStats
		upstream int64
		runErr   error
	)
	for i, spec := range specs {
		p := &phaseStats{spec: spec, txn: newTxnStats()}
		if p.walStart, err = pgfixture.WALPosition(gctx, b.pg); err != nil {
			runErr = err
			break
		}
		p.start = time.Now()
		p.batches = hotBatchCounts(leaderPod.ingest)
		cur.Store(p)
		fmt.Printf("phase %s: %.0f live ev/s for %s, %d bulk events\n", spec.name, spec.rate, spec.dur, spec.bulk)
		if err := runPhase(gctx, w, p, cmd, uint64(i), &upstream); err != nil {
			runErr = err
			break
		}
		// Admission returns before events are durable, so a phase ends
		// when both pods can serve everything it appended.
		if err := waitVisible(gctx, w, followers[:]); err != nil {
			runErr = err
			break
		}
		p.end = time.Now()
		p.batches = hotBatchCounts(leaderPod.ingest).sub(p.batches)
		wal, err := pgfixture.WALPosition(gctx, b.pg)
		if err != nil {
			runErr = err
			break
		}
		p.wal = wal - p.walStart
		if p.dbBytes, err = pgfixture.DatabaseBytes(gctx, b.pg); err != nil {
			runErr = err
			break
		}
		results = append(results, p)
		report(os.Stdout, p)
	}

	// Let the maintainer fold and upload what the phases left queued, so
	// shutdown does not cancel it mid-upload.
	if runErr == nil {
		runErr = waitMaintainer(gctx, mm, leaderPod)
	}
	cerr := closeWriter()
	cancel(nil)
	gerr := g.Wait()
	if cause := context.Cause(gctx); runErr != nil && cause != nil && !errors.Is(cause, context.Canceled) {
		runErr = cause
	}
	fmt.Printf("\nsummary\n")
	summarize(os.Stdout, results)
	return errors.Join(runErr, cerr, gerr, sess.Err())
}

// waitMaintainer waits until the maintainer has folded every committed
// batch and has no block queued for upload.
func waitMaintainer(ctx context.Context, mm *maintainer.Metrics, p *pod) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	for gaugeValue(mm.QueuedBlocks) > 0 || gaugeValue(p.ingest.HotUnfoldedEvents) > 0 {
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for the maintainer to drain: %w", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	return nil
}

// waitLog waits for f's first steady-state mirror.
func waitLog(ctx context.Context, f *follower.Follower) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	for f.Log() == nil {
		if err := f.Refresh(ctx); err != nil {
			return fmt.Errorf("first follower refresh: %w", err)
		}
		if f.Log() == nil {
			select {
			case <-ctx.Done():
				return fmt.Errorf("follower has no readable log: %w", ctx.Err())
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	return nil
}

// waitVisible waits until every follower's log reaches the writer's next
// seq.
func waitVisible(ctx context.Context, w *ingest.Writer, fs []*follower.Follower) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	want := w.NextSeq()
	for _, f := range fs {
		for f.Log().TipSeq() < want {
			select {
			case <-ctx.Done():
				return fmt.Errorf("waiting for seq %d to be visible (at %d): %w", want, f.Log().TipSeq(), ctx.Err())
			case <-time.After(5 * time.Millisecond):
			}
		}
	}
	return nil
}

// subscribe tails f's readable log as a live subscriber would, recording
// each live event's end-to-end latency.
func subscribe(ctx context.Context, f *follower.Follower, pod int, cur *atomic.Pointer[phaseStats]) error {
	cursor := f.Log().TipSeq()
	for {
		l := f.Log()
		entries, notify, ok, atTip := l.ReadFrom(cursor, 4096)
		switch {
		case ok:
			now := time.Now().UnixMicro()
			p := cur.Load()
			for _, e := range entries {
				ev := e.Event()
				if isLive(ev) {
					p.e2e[pod].add(time.Duration(now-ev.WitnessedAt) * time.Microsecond)
				}
				cursor = ev.Seq + 1
			}
		case atTip:
			select {
			case <-ctx.Done():
				return nil
			case <-notify:
			}
		default:
			// Below the log's floor: a real subscriber would read cold
			// storage; this one skips ahead and counts it.
			cur.Load().fellBehind[pod].Add(1)
			cursor = l.FloorSeq()
		}
	}
}

// runPhase paces live appends at p's rate, one event at a time as the
// firehose consumer appends them, beside bulk workers appending whole
// repos.
func runPhase(ctx context.Context, w *ingest.Writer, p *phaseStats, cmd *cli.Command, phase uint64, upstream *int64) error {
	seed := cmd.Uint64("seed")
	universe := cmd.Uint64("did-universe")
	g, gctx := errgroup.WithContext(ctx)
	if p.spec.bulk > 0 {
		var claimed atomic.Int64
		start := time.Now()
		for i := range cmd.Int("bulk-workers") {
			gen := newGenerator(seed^(phase<<32)^uint64(i+1)<<16, universe)
			g.Go(func() error {
				bctx := ingest.WithClass(gctx, ingest.ClassBulk)
				for {
					n := gen.repoSize()
					before := claimed.Add(int64(n)) - int64(n)
					if before >= p.spec.bulk {
						return nil
					}
					// The repo that crosses the target is cut short, so a
					// phase appends exactly its bulk count.
					n = int(min(int64(n), p.spec.bulk-before))
					if p.spec.bulkRate > 0 {
						due := start.Add(time.Duration(float64(before) / p.spec.bulkRate * float64(time.Second)))
						select {
						case <-gctx.Done():
							return nil
						case <-time.After(time.Until(due)):
						}
					}
					if err := w.AppendBatch(bctx, gen.repo(time.Now(), n)); err != nil {
						return err
					}
					p.bulk.Add(int64(n))
				}
			})
		}
	}
	gen := newGenerator(seed^(phase<<32), universe)
	g.Go(func() error {
		t := time.NewTicker(time.Millisecond)
		defer t.Stop()
		start := time.Now()
		var sent int64
		for {
			select {
			case <-gctx.Done():
				return nil
			case <-t.C:
			}
			elapsed := time.Since(start)
			if elapsed >= p.spec.dur && p.bulk.Load() >= p.spec.bulk {
				return nil
			}
			for due := int64(p.spec.rate*elapsed.Seconds()) - sent; due > 0; due-- {
				*upstream++
				ev := gen.live(time.Now(), *upstream)
				if err := w.Append(gctx, &ev); err != nil {
					return err
				}
				sent++
				p.live.Add(1)
			}
		}
	})
	return g.Wait()
}

func report(out *os.File, p *phaseStats) {
	secs := p.end.Sub(p.start).Seconds()
	rows := [][2]string{
		{"duration", p.end.Sub(p.start).Round(time.Millisecond).String()},
		{"live events", fmt.Sprintf("%d (%.0f/s)", p.live.Load(), float64(p.live.Load())/secs)},
		{"bulk events", fmt.Sprintf("%d (%.0f/s)", p.bulk.Load(), float64(p.bulk.Load())/secs)},
	}
	for _, k := range p.txn.kinds() {
		rows = append(rows,
			[2]string{"txn " + string(k) + " total", p.txn.total[k].summary().String()},
			[2]string{"txn " + string(k) + " commit", p.txn.commit[k].summary().String()},
		)
	}
	rows = append(rows,
		[2]string{"e2e leader pod", fmt.Sprintf("%s fell_behind=%d", p.e2e[0].summary(), p.fellBehind[0].Load())},
		[2]string{"e2e other pod", fmt.Sprintf("%s fell_behind=%d", p.e2e[1].summary(), p.fellBehind[1].Load())},
		[2]string{"staged metadata", fmt.Sprintf("%d ops in %d batches (%.1f per batch, %.2f per live event)",
			p.metaOps.Load(), p.metaBatches.Load(), float64(p.metaOps.Load())/float64(max(1, p.metaBatches.Load())),
			float64(p.metaOps.Load())/float64(max(1, p.live.Load())))},
		[2]string{"hot batches", fmt.Sprintf("live %.0f inline, %.0f pointer; bulk %.0f inline, %.0f pointer; %.1f per transaction",
			p.batches[0][0], p.batches[0][1], p.batches[1][0], p.batches[1][1],
			(p.batches[0][0]+p.batches[0][1]+p.batches[1][0]+p.batches[1][1])/float64(max(1, p.txn.total[catalog.TxHotBatch].summary().n)))},
		[2]string{"fold", p.folds.summary().String()},
		[2]string{"seal", p.seals.summary().String()},
		[2]string{"max queued blocks", strconv.FormatInt(p.maxQueued.Load(), 10)},
		[2]string{"max unfolded events", strconv.FormatInt(p.maxUnfolded.Load(), 10)},
		[2]string{"WAL", fmt.Sprintf("%s (%s/s, %s/day at this rate)", mib(float64(p.wal)), mib(float64(p.wal)/secs), gib(float64(p.wal)/secs*86400))},
		[2]string{"database size", mib(float64(p.dbBytes))},
	)
	printKV(out, rows)
}

func summarize(out *os.File, results []*phaseStats) {
	for _, p := range results {
		secs := p.end.Sub(p.start).Seconds()
		batch := p.txn.commit[catalog.TxHotBatch].summary()
		_, _ = fmt.Fprintf(out, "  %-6s live=%.0f/s bulk=%.0f/s batch_commit_p50=%s p99=%s e2e_p50=%s p99=%s other_p99=%s WAL/day=%s seals=%d\n",
			p.spec.name, float64(p.live.Load())/secs, float64(p.bulk.Load())/secs,
			ms(batch.p50), ms(batch.p99), ms(p.e2e[0].summary().p50), ms(p.e2e[0].summary().p99), ms(p.e2e[1].summary().p99),
			gib(float64(p.wal)/secs*86400), p.seals.summary().n)
	}
}

// batchCounts is hot batches by class (live, bulk) and storage (inline,
// pointer).
type batchCounts [2][2]float64

func hotBatchCounts(m *ingest.Metrics) batchCounts {
	var c batchCounts
	for i, class := range []ingest.Class{ingest.ClassLive, ingest.ClassBulk} {
		for j, storage := range []string{"inline", "pointer"} {
			c[i][j] = counterValue(m.HotBatches.WithLabelValues(class.String(), storage))
		}
	}
	return c
}

func (c batchCounts) sub(o batchCounts) batchCounts {
	for i := range c {
		for j := range c[i] {
			c[i][j] -= o[i][j]
		}
	}
	return c
}

func gib(b float64) string { return fmt.Sprintf("%.2fGiB", b/(1<<30)) }
