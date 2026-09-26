package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/catalog/follower"
	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/metastore"
	metapg "github.com/bluesky-social/jetstream/internal/metastore/pg"
	"github.com/bluesky-social/jetstream/internal/objstore/objcache"
	"github.com/bluesky-social/jetstream/internal/objstore/protocol"
	"github.com/bluesky-social/jetstream/internal/pgstore/pgfixture"
	"github.com/bluesky-social/jetstream/internal/tombstone"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/urfave/cli/v3"
	"golang.org/x/sync/errgroup"
)

func compactionCommand() *cli.Command {
	return &cli.Command{
		Name:  "compaction",
		Usage: "Seal real segments below and above a compaction watermark, then measure the tombstone rebuild and a pass's block fetches",
		Description: `The catalog holds --backfill-segments of backfill-profile segments
(whole repos, DIDs drawn from the universe), then --live-segments of the live
mix, and the compaction watermark W sits at the end of those. Above it,
--window-segments of live traffic are the backlog the next pass compacts.
Their deletes target records created earlier: --recent-delete-share of them
a record from the same segment's recent traffic, the rest a record anywhere
in the history below W.

Two measurements follow, each from a fresh pod with a cold object cache:

  rebuild  orchestrator.rebuildLiveTombstones, the leader's session-start
           fold of every block above W, once one block at a time and again
           with --rebuild-workers concurrent fetches (16 in production).
  pass     one compaction pass, per window fraction (--steps halvings of
           the backlog): the tombstone collection, which decodes every
           window block, then segment.SparseRewrite of every sealed segment
           with the production drop rule and probe limit. Nothing is
           published, so every step rewrites the same generations.

The rewrite's fetch fraction depends on the distinct tombstone DIDs, which
grow with the backlog: past about 4M/blocks-per-segment probes that hit a
segment's bloom, SparseRewrite stops narrowing and reads every block.`,
		Flags: []cli.Flag{
			&cli.IntFlag{Name: "backfill-segments", Value: 3},
			&cli.IntFlag{Name: "live-segments", Value: 3},
			&cli.IntFlag{Name: "window-segments", Value: 4, Usage: "4 segments of 680 blocks are about 1h at 3,000 events/s"},
			&cli.IntFlag{Name: "blocks-per-segment", Value: 680, Validator: atLeastOne[int]},
			&cli.IntFlag{Name: "events-per-block", Value: segment.DefaultMaxEventsPerBlock, Validator: atLeastOne[int]},
			&cli.Float64Flag{Name: "recent-delete-share", Value: 0.5},
			&cli.IntFlag{Name: "steps", Value: 4, Validator: atLeastOne[int]},
			&cli.IntFlag{Name: "rebuild-workers", Value: 16, Usage: "the orchestrator's tombstoneRebuildConcurrency", Validator: atLeastOne[int]},
			&cli.Uint64Flag{Name: "did-universe", Value: 40_000_000, Validator: atLeastOne[uint64]},
			&cli.Uint64Flag{Name: "seed", Value: 1},
			&cli.IntFlag{Name: "workers", Value: min(runtime.NumCPU(), 8), Validator: atLeastOne[int]},
		},
		Action: runCompaction,
	}
}

// Segment profiles, in catalog order.
const (
	profileBackfill = "backfill"
	profileLive     = "live"
	profileWindow   = "window"
)

// recordSampleSize bounds each segment's sample of its creates, which later
// deletes draw targets from.
const recordSampleSize = 100_000

// realSegment is a sealed segment whose blocks were uploaded.
type realSegment struct {
	builtSegment
	// sample is a uniform sample of the segment's creates.
	sample []tombstone.RecordKey
}

type compactionBench struct {
	*bench
	uploader *protocol.Uploader
	sess     *catalog.Session
	epoch    uint64

	universe       uint64
	blocks, events int
	recentShare    float64
}

func runCompaction(ctx context.Context, cmd *cli.Command) error {
	var (
		backfills = cmd.Int("backfill-segments")
		lives     = cmd.Int("live-segments")
		windows   = cmd.Int("window-segments")
		workers   = cmd.Int("workers")
	)
	if backfills < 0 || lives < 0 || windows < 1 || backfills+lives < 1 {
		return errors.New("need --window-segments >= 1 and at least one history segment")
	}
	if cmd.Int("events-per-block") > segment.DefaultMaxEventsPerBlock {
		return fmt.Errorf("--events-per-block exceeds %d", segment.DefaultMaxEventsPerBlock)
	}
	b, err := openBench(ctx, cmd)
	if err != nil {
		return err
	}
	defer b.close()
	st := jetstreamd.DefaultStorageConfig()

	lease := b.pg.NewLease()
	if err := lease.Acquire(ctx, time.Hour); err != nil {
		return err
	}
	cb := &compactionBench{
		bench:       b,
		epoch:       lease.Epoch(),
		universe:    cmd.Uint64("did-universe"),
		blocks:      cmd.Int("blocks-per-segment"),
		events:      cmd.Int("events-per-block"),
		recentShare: cmd.Float64("recent-delete-share"),
	}
	cb.sess = catalog.NewSession(catalog.SessionConfig{DB: b.pg, Epoch: cb.epoch})
	if _, err := cb.sess.InitNamespace(ctx, catalog.Main, nil); err != nil {
		return err
	}
	if cb.uploader, err = protocol.NewUploader(protocol.UploaderConfig{
		Blob:        b.blob,
		ArchiveID:   b.archive.ArchiveID,
		GCDelay:     st.GC.Delay,
		OrphanAge:   st.GC.OrphanAge,
		Concurrency: st.S3.UploadConcurrency,
	}); err != nil {
		return err
	}

	perSeg := uint64(cb.blocks) * uint64(cb.events)
	profiles := make([]string, 0, backfills+lives+windows)
	for range backfills {
		profiles = append(profiles, profileBackfill)
	}
	for range lives {
		profiles = append(profiles, profileLive)
	}
	for range windows {
		profiles = append(profiles, profileWindow)
	}
	history := backfills + lives
	watermark := uint64(history) * perSeg
	fmt.Printf("sealing %d backfill, %d live, and %d window segments of %d blocks x %d events; W=%d\n",
		backfills, lives, windows, cb.blocks, cb.events, watermark)
	start := time.Now()
	// History first: the window's deletes draw from its samples.
	var pool []tombstone.RecordKey
	if err := cb.seed(ctx, cmd.Uint64("seed"), profiles, 0, history, workers, &pool); err != nil {
		return errors.Join(err, cb.sess.Err())
	}
	if err := cb.seed(ctx, cmd.Uint64("seed"), profiles, history, len(profiles), workers, &pool); err != nil {
		return errors.Join(err, cb.sess.Err())
	}
	if _, err := pgfixture.MarkObjectsAvailable(ctx, b.pg); err != nil {
		return err
	}
	meta := metapg.New(metapg.Config{DB: b.pg, Commit: func(ctx context.Context, ops []metastore.Op) error {
		_, err := cb.sess.CommitMeta(ctx, ops)
		return err
	}})
	if err := lifecycle.WritePhase(ctx, meta, lifecycle.PhaseSteadyState, time.Now()); err != nil {
		return err
	}
	if _, err := cb.sess.CommitMeta(ctx, []metastore.Op{
		{Kind: metastore.OpSet, Key: []byte(catalog.MainSeqKey), Value: catalog.EncodeSeq(uint64(len(profiles))*perSeg + 1)},
		{Kind: metastore.OpSet, Key: []byte(catalog.RelayCursorKey), Value: metastore.EncodeVersionedUint64LE(1, 1)},
	}); err != nil {
		return err
	}
	fmt.Printf("sealed in %s\n", time.Since(start).Round(time.Second))

	if err := cb.measureRebuild(ctx, st, watermark, 1); err != nil {
		return err
	}
	if err := cb.measureRebuild(ctx, st, watermark, cmd.Int("rebuild-workers")); err != nil {
		return err
	}
	return cb.measurePass(ctx, st, profiles, watermark, cmd.Int("steps"), workers)
}

// seed builds, uploads, and seals segments [from, to) in index order.
// Workers build in any order; the committer seals in order.
func (cb *compactionBench) seed(ctx context.Context, seed uint64, profiles []string, from, to, workers int, pool *[]tombstone.RecordKey) error {
	perSeg := uint64(cb.blocks) * uint64(cb.events)
	window := make(chan struct{}, workers)
	out := make([]chan realSegment, to)
	for i := from; i < to; i++ {
		out[i] = make(chan realSegment, 1)
	}
	history := *pool
	var next atomic.Int64
	next.Store(int64(from))
	g, gctx := errgroup.WithContext(ctx)
	for range workers {
		g.Go(func() error {
			for {
				select {
				case window <- struct{}{}:
				case <-gctx.Done():
					return nil
				}
				i := int(next.Add(1) - 1)
				if i >= to {
					return nil
				}
				gen := newGenerator(seed+uint64(i), cb.universe)
				s, err := cb.buildSegment(gctx, gen, profiles[i], 1+uint64(i)*perSeg, history)
				if err != nil {
					return fmt.Errorf("segment %d: %w", i, err)
				}
				out[i] <- s
			}
		})
	}
	start := time.Now()
	g.Go(func() error {
		for i := from; i < to; i++ {
			var s realSegment
			select {
			case s = <-out[i]:
			case <-gctx.Done():
				return nil
			}
			<-window
			if err := commitSegment(gctx, cb.bench, cb.epoch, uint64(i), s.builtSegment); err != nil {
				return fmt.Errorf("commit segment %d: %w", i, err)
			}
			*pool = append(*pool, s.sample...)
			fmt.Printf("  segment %d (%s) sealed, %s\n", i, profiles[i], time.Since(start).Round(time.Second))
		}
		return nil
	})
	return g.Wait()
}

// buildSegment encodes one segment of the profile's traffic, uploads its
// blocks and footer, and samples its creates.
func (cb *compactionBench) buildSegment(ctx context.Context, gen *generator, profile string, firstSeq uint64, history []tombstone.RecordKey) (realSegment, error) {
	bb, err := segment.NewBlockBuilder(cb.events)
	if err != nil {
		return realSegment{}, err
	}
	var (
		s      realSegment
		frames = make([][]byte, 0, cb.blocks+1)
		// recent holds the segment's creates since the start, for deletes
		// of recent records; sample is a reservoir over the same.
		recent  []tombstone.RecordKey
		creates int
		repo    []segment.Event
		seq     = firstSeq
		at      = time.Now().Add(-24 * time.Hour)
	)
	next := func() segment.Event {
		now := at.Add(time.Duration(seq-firstSeq) * time.Second / 3000)
		if profile == profileBackfill {
			if len(repo) == 0 {
				repo = gen.repo(now, gen.repoSize())
				did := didFor(gen.rng.Uint64N(cb.universe))
				for i := range repo {
					repo[i].DID = did
				}
			}
			ev := repo[0]
			repo = repo[1:]
			return ev
		}
		ev := gen.live(now, int64(seq))
		if profile == profileWindow && ev.Kind == segment.KindDelete {
			var k tombstone.RecordKey
			switch {
			case len(recent) > 0 && (len(history) == 0 || gen.rng.Float64() < cb.recentShare):
				k = recent[gen.rng.IntN(len(recent))]
			case len(history) > 0:
				k = history[gen.rng.IntN(len(history))]
			default:
				return ev
			}
			ev.DID, ev.Collection, ev.Rkey = k.DID, k.Collection, k.Rkey
		}
		return ev
	}
	for range cb.blocks {
		for range cb.events {
			ev := next()
			ev.Seq = seq
			seq++
			if ev.Kind == segment.KindCreate {
				k := tombstone.RecordKey{DID: ev.DID, Collection: ev.Collection, Rkey: ev.Rkey}
				if profile == profileWindow {
					recent = append(recent, k)
				}
				creates++
				if len(s.sample) < recordSampleSize {
					s.sample = append(s.sample, k)
				} else if j := gen.rng.IntN(creates); j < recordSampleSize {
					s.sample[j] = k
				}
			}
			if _, err := bb.Append(ev); err != nil {
				return realSegment{}, err
			}
		}
		frame, info := bb.Encode()
		frames = append(frames, frame)
		s.infos = append(s.infos, info)
	}
	header, footer, _, err := segment.BuildSealed(segment.SliceFrameSource(frames))
	if err != nil {
		return realSegment{}, err
	}
	refs, err := cb.uploader.Upload(ctx, cb.sess, append(frames, footer))
	if err != nil {
		return realSegment{}, err
	}
	s.header, s.bytes = header, int64(len(footer))
	s.blocks, s.footer = refs[:len(frames)], refs[len(frames)]
	return s, nil
}

// startPod is a fresh pod's follower, caught up, with a cold object cache.
func (cb *compactionBench) startPod(ctx context.Context, st jetstreamd.StorageConfig) (*follower.Follower, func(), error) {
	p := newPod()
	cache := objcache.New(objcache.Config{MaxBytes: st.ObjectCacheBytes, Memory: p.memory})
	f, _, err := cb.newFollower(p, cb.pg, cache, st.CatalogPollInterval, st.MaxViewAge, st.S3.ReadConcurrency)
	if err != nil {
		return nil, nil, err
	}
	for {
		if err := f.Refresh(ctx); err != nil {
			return nil, nil, err
		}
		if f.Ready(ctx) == nil {
			break
		}
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
	return f, func() { runtime.KeepAlive(f) }, nil
}

// measureRebuild times orchestrator.rebuildLiveTombstones's fold of every
// block above the watermark, from a fresh pod, with workers blocks in
// flight.
func (cb *compactionBench) measureRebuild(ctx context.Context, st jetstreamd.StorageConfig, watermark uint64, workers int) error {
	f, done, err := cb.startPod(ctx, st)
	if err != nil {
		return err
	}
	defer done()
	refs := slices.Collect(f.Snapshot().RefsFrom(catalog.Main, watermark+1))
	fetcher := f.Fetcher()
	var (
		fetchNS, foldNS atomic.Int64
		bytes           atomic.Int64
		events          atomic.Int64
	)
	parts := make([]tombstone.Snapshot, len(refs))
	start := time.Now()
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)
	for i, ref := range refs {
		g.Go(func() error {
			t0 := time.Now()
			frame, err := fetcher.Fetch(gctx, ref)
			if err != nil {
				return err
			}
			t1 := time.Now()
			evs, err := segment.DecodeBlockFrame(frame)
			if err != nil {
				return err
			}
			part, err := tombstone.Fold(evs, watermark)
			if err != nil {
				return err
			}
			fetchNS.Add(int64(t1.Sub(t0)))
			foldNS.Add(int64(time.Since(t1)))
			bytes.Add(int64(len(frame)))
			events.Add(int64(len(evs)))
			parts[i] = part
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	snap := tombstone.Snapshot{Records: make(map[tombstone.RecordKey]uint64), DIDs: make(map[string]tombstone.DIDTombstone)}
	for _, part := range parts {
		snap.Merge(part)
	}
	elapsed := time.Since(start)
	n := time.Duration(max(1, len(refs)))
	fmt.Printf("\ntombstone rebuild, %d worker(s), fresh pod\n", workers)
	printKV(os.Stdout, [][2]string{
		{"blocks above W", fmt.Sprintf("%d (%s, %d events)", len(refs), mib(float64(bytes.Load())), events.Load())},
		{"tombstones", fmt.Sprintf("%d record, %d DID", len(snap.Records), len(snap.DIDs))},
		{"elapsed", elapsed.Round(time.Millisecond).String()},
		{"per block", fmt.Sprintf("fetch %s, decode and fold %s (means)", ms(time.Duration(fetchNS.Load())/n), ms(time.Duration(foldNS.Load())/n))},
	})
	return nil
}

// passStep is one window fraction's compaction chunk.
type passStep struct {
	target uint64
	snap   tombstone.Snapshot
}

// profileCount accumulates one profile's rewrite results.
type profileCount struct {
	segments, skipped, dense, rewritten int
	blocks, fetched, touched            int
	rows                                int
	bloomHits                           int
}

// measurePass runs a compaction pass's reads for each window fraction:
// collectCompactionTombstones over (W, target], then
// disaggCompactionRewriter's floor check and segment.SparseRewrite of every
// sealed segment.
func (cb *compactionBench) measurePass(ctx context.Context, st jetstreamd.StorageConfig, profiles []string, watermark uint64, steps, workers int) error {
	f, done, err := cb.startPod(ctx, st)
	if err != nil {
		return err
	}
	defer done()
	view := f.Snapshot()
	segs := view.Segments(catalog.Main)
	tip := view.TipSeq(catalog.Main) - 1
	passSteps := make([]passStep, steps)
	for k := range passSteps {
		// Halvings of the backlog, largest last.
		passSteps[k].target = watermark + (tip-watermark)>>(steps-1-k)
		passSteps[k].snap = tombstone.Snapshot{Records: make(map[tombstone.RecordKey]uint64), DIDs: make(map[string]tombstone.DIDTombstone)}
	}

	// Every block in (W, target] is decoded; one decode serves every step.
	start := time.Now()
	var windowBlocks int
	fetcher := f.Fetcher()
	for ref := range view.RefsFrom(catalog.Main, watermark+1) {
		evs, err := catalog.DecodeRef(ctx, fetcher, ref)
		if err != nil {
			return err
		}
		windowBlocks++
		for k := range passSteps {
			if ref.MinSeq > passSteps[k].target {
				continue
			}
			part, err := tombstone.FoldRange(evs, watermark, passSteps[k].target)
			if err != nil {
				return err
			}
			passSteps[k].snap.Merge(part)
		}
	}
	collect := time.Since(start)
	fmt.Printf("\ncompaction pass reads, fresh pod: %d window blocks decoded for tombstones in %s\n", windowBlocks, collect.Round(time.Millisecond))

	for k, step := range passSteps {
		chunkEnd := step.target
		rule := step.snap.Compile(chunkEnd)
		floor := min(maxSnapSeq(step.snap), chunkEnd+1)
		dids := snapDIDs(step.snap)
		counts := map[string]*profileCount{}
		for _, p := range []string{profileBackfill, profileLive, profileWindow} {
			counts[p] = &profileCount{}
		}
		var (
			mu            sync.Mutex
			collectBlocks int
		)
		for _, v := range segs {
			if v.State == catalog.Sealed && v.Header.MaxSeq > watermark && v.Header.MinSeq <= chunkEnd {
				for _, b := range v.Blocks {
					if b.MaxSeq > watermark && b.MinSeq <= chunkEnd {
						collectBlocks++
					}
				}
			}
		}
		start := time.Now()
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(workers)
		for _, v := range segs {
			if v.State != catalog.Sealed {
				continue
			}
			c := counts[profiles[v.Index]]
			c.segments++
			c.blocks += int(v.Header.BlockCount)
			if v.Header.EventCount == 0 || v.Header.MinSeq >= floor {
				c.skipped++
				continue
			}
			g.Go(func() error {
				r, err := cb.rewrite(gctx, f, v.Index, rule, dids)
				if err != nil {
					return fmt.Errorf("rewrite main/%d: %w", v.Index, err)
				}
				mu.Lock()
				defer mu.Unlock()
				c.fetched += r.fetched
				c.bloomHits += r.bloomHits
				c.rows += r.rows
				c.touched += r.touched
				if r.dense {
					c.dense++
				}
				if r.touched > 0 {
					c.rewritten++
				}
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}
		elapsed := time.Since(start)

		probes := rule.Len()
		fmt.Printf("\nwindow fraction 1/%d: (W, W+%d], %d record and %d DID tombstones on %d DIDs\n",
			1<<(steps-1-k), chunkEnd-watermark, len(step.snap.Records), len(step.snap.DIDs), probes)
		rows := [][2]string{{"tombstone collection", fmt.Sprintf("%d blocks, all decoded", collectBlocks)}}
		var total, fetched int
		for _, p := range []string{profileBackfill, profileLive, profileWindow} {
			c := counts[p]
			if c.segments == 0 {
				continue
			}
			total += c.blocks
			fetched += c.fetched
			rows = append(rows, [2]string{p + " rewrite", fmt.Sprintf(
				"%d/%d blocks fetched (%.1f%%), %d touched; %d segments, %d skipped by floor, %d dense; mean %d bloom-hit probes; %d rows dropped",
				c.fetched, c.blocks, 100*float64(c.fetched)/float64(max(1, c.blocks)), c.touched,
				c.segments, c.skipped, c.dense, c.bloomHits/max(1, c.segments-c.skipped), c.rows)})
		}
		rows = append(rows,
			[2]string{"rewrite fetches", fmt.Sprintf("%d/%d blocks (%.1f%%), %d with the collection", fetched, total, 100*float64(fetched)/float64(max(1, total)), fetched+collectBlocks)},
			[2]string{"rewrite elapsed", fmt.Sprintf("%s over %d workers", elapsed.Round(time.Millisecond), workers)})
		printKV(os.Stdout, rows)
	}
	return nil
}

// rewriteResult is one segment rewrite's counts.
type rewriteResult struct {
	fetched, touched, rows int
	// bloomHits is the tombstone DIDs the segment bloom does not rule out;
	// dense means SparseRewrite stopped narrowing.
	bloomHits int
	dense     bool
}

// rewrite is orchestrator.rewriteSegmentDisaggregated without the upload
// and publish.
func (cb *compactionBench) rewrite(ctx context.Context, f *follower.Follower, idx uint64, rule *segment.Tombstones, dids []string) (rewriteResult, error) {
	parts, found, err := f.GenerationParts(ctx, idx)
	if err != nil {
		return rewriteResult{}, err
	}
	if !found {
		return rewriteResult{}, errors.New("segment left the catalog")
	}
	footer, err := f.Objects().Get(ctx, parts.Footer.ID)
	if err != nil {
		return rewriteResult{}, err
	}
	fetch := func(i int) ([]byte, error) {
		return f.Objects().Get(ctx, parts.Blocks[i].ID)
	}
	var out rewriteResult
	if r, err := segment.OpenReaderParts(parts.Header, footer, fetch, segment.ReaderOptions{}); err != nil {
		return rewriteResult{}, err
	} else if bloom := r.SegmentBloom(); bloom != nil {
		for _, did := range dids {
			if did == "" || bloom.TestString(did) {
				out.bloomHits++
			}
		}
		out.dense = rule.Len() > segment.DefaultSparseProbeLimit || out.bloomHits*len(parts.Blocks) > segment.DefaultSparseProbeLimit
	}
	res, err := segment.SparseRewrite(parts.Header, footer, fetch, rule, segment.SparseOptions{Name: fmt.Sprintf("main/%d", idx)})
	if err != nil {
		return rewriteResult{}, err
	}
	out.fetched = res.BlocksFetched
	out.touched = len(res.Frames)
	out.rows = int(res.RowsDropped)
	return out, nil
}

// snapDIDs is the distinct DIDs with any tombstone in snap: the probes
// SparseRewrite tests against each segment's bloom.
func snapDIDs(snap tombstone.Snapshot) []string {
	seen := make(map[string]struct{}, len(snap.DIDs))
	for k := range snap.Records {
		seen[k.DID] = struct{}{}
	}
	for did := range snap.DIDs {
		seen[did] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for did := range seen {
		out = append(out, did)
	}
	return out
}

// maxSnapSeq is orchestrator.maxTombstoneSeq.
func maxSnapSeq(snap tombstone.Snapshot) uint64 {
	var top uint64
	for _, seq := range snap.Records {
		top = max(top, seq)
	}
	for _, ts := range snap.DIDs {
		top = max(top, ts.Seq)
	}
	return top
}
