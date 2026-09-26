package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/metastore"
	metapg "github.com/bluesky-social/jetstream/internal/metastore/pg"
	"github.com/bluesky-social/jetstream/internal/objstore/objcache"
	"github.com/bluesky-social/jetstream/internal/objstore/protocol"
	"github.com/bluesky-social/jetstream/internal/pgstore/pgfixture"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/urfave/cli/v3"
	"golang.org/x/sync/errgroup"
)

func footersCommand() *cli.Command {
	return &cli.Command{
		Name:  "footers",
		Usage: "Seal a synthetic catalog of footers, then time a pod start's footer load and measure its memory",
		Description: `Each synthetic segment has --blocks-per-segment blocks of
--dids-per-block events, each a distinct DID, so the footer's blooms are
sized like a real segment's. Only footers are uploaded: a pod start loads
nothing else, so block rows name objects that do not exist.

pop1's sealed segments mix profiles. Backfill blocks hold few DIDs (repos
are appended whole); live blocks hold about 3,500 of 4,096 events' DIDs.
Run each profile and scale by resident bytes per footer byte.`,
		Flags: []cli.Flag{
			&cli.IntFlag{Name: "segments", Value: 7000},
			&cli.IntFlag{Name: "blocks-per-segment", Value: 680, Usage: "A 256MiB segment of production-shaped blocks has about 680"},
			&cli.IntFlag{Name: "dids-per-block", Value: 64},
			&cli.Uint64Flag{Name: "did-universe", Value: 40_000_000, Validator: atLeastOne[uint64]},
			&cli.Uint64Flag{Name: "seed", Value: 1},
			&cli.IntFlag{Name: "workers", Value: runtime.NumCPU()},
			&cli.StringFlag{Name: "heapprofile", Usage: "Write a heap profile taken once the pod is ready to this file"},
		},
		Action: runFooters,
	}
}

// builtSegment is one synthetic segment, ready to commit.
type builtSegment struct {
	header []byte
	footer catalog.ObjectRef
	bytes  int64
	infos  []segment.BlockInfo
}

func runFooters(ctx context.Context, cmd *cli.Command) error {
	var (
		segments = cmd.Int("segments")
		blocks   = cmd.Int("blocks-per-segment")
		dids     = cmd.Int("dids-per-block")
		workers  = max(1, cmd.Int("workers"))
	)
	if segments < 1 || blocks < 1 || dids < 1 || dids > segment.DefaultMaxEventsPerBlock {
		return fmt.Errorf("need --segments >= 1, --blocks-per-segment >= 1, and --dids-per-block in [1, %d]", segment.DefaultMaxEventsPerBlock)
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
	epoch := lease.Epoch()
	sess := catalog.NewSession(catalog.SessionConfig{DB: b.pg, Epoch: epoch})
	if _, err := sess.InitNamespace(ctx, catalog.Main, nil); err != nil {
		return err
	}
	uploader, err := protocol.NewUploader(protocol.UploaderConfig{
		Blob:        b.blob,
		ArchiveID:   b.archive.ArchiveID,
		GCDelay:     st.GC.Delay,
		OrphanAge:   st.GC.OrphanAge,
		Concurrency: st.S3.UploadConcurrency,
	})
	if err != nil {
		return err
	}

	fmt.Printf("sealing %d segments of %d blocks x %d DIDs\n", segments, blocks, dids)
	perSeg := uint64(blocks) * uint64(dids)
	base := time.Now().Add(-time.Duration(segments) * time.Hour)
	// Workers build and upload segments in any order; the committer seals
	// them in index order, as the catalog requires. The window bounds how
	// far building runs ahead.
	window := make(chan struct{}, 2*workers)
	out := make([]chan builtSegment, segments)
	for i := range out {
		out[i] = make(chan builtSegment, 1)
	}
	var next atomic.Int64
	g, gctx := errgroup.WithContext(ctx)
	for w := range workers {
		gen := newGenerator(cmd.Uint64("seed")+uint64(w), cmd.Uint64("did-universe"))
		g.Go(func() error {
			for {
				select {
				case window <- struct{}{}:
				case <-gctx.Done():
					return nil
				}
				i := int(next.Add(1) - 1)
				if i >= segments {
					return nil
				}
				s, err := buildSegment(gctx, gen, uploader, sess, 1+uint64(i)*perSeg, base.Add(time.Duration(i)*time.Hour), blocks, dids)
				if err != nil {
					return fmt.Errorf("segment %d: %w", i, err)
				}
				out[i] <- s
			}
		})
	}
	var footerBytes int64
	start := time.Now()
	g.Go(func() error {
		for i := range segments {
			var s builtSegment
			select {
			case s = <-out[i]:
			case <-gctx.Done():
				return nil
			}
			<-window
			if err := commitSegment(gctx, b, epoch, uint64(i), s); err != nil {
				return fmt.Errorf("commit segment %d: %w", i, err)
			}
			footerBytes += s.bytes
			if (i+1)%500 == 0 || i+1 == segments {
				fmt.Printf("  %d/%d sealed, %s of footers, %s\n", i+1, segments, mib(float64(footerBytes)), time.Since(start).Round(time.Second))
			}
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return errors.Join(err, sess.Err())
	}
	if _, err := pgfixture.MarkObjectsAvailable(ctx, b.pg); err != nil {
		return err
	}
	meta := metapg.New(metapg.Config{DB: b.pg, Commit: func(ctx context.Context, ops []metastore.Op) error {
		_, err := sess.CommitMeta(ctx, ops)
		return err
	}})
	if err := lifecycle.WritePhase(ctx, meta, lifecycle.PhaseSteadyState, time.Now()); err != nil {
		return err
	}
	if _, err := sess.CommitMeta(ctx, []metastore.Op{
		{Kind: metastore.OpSet, Key: []byte(catalog.MainSeqKey), Value: catalog.EncodeSeq(uint64(segments)*perSeg + 1)},
		{Kind: metastore.OpSet, Key: []byte(catalog.RelayCursorKey), Value: metastore.EncodeVersionedUint64LE(1, 1)},
	}); err != nil {
		return err
	}
	dbBytes, err := pgfixture.DatabaseBytes(ctx, b.pg)
	if err != nil {
		return err
	}
	fmt.Printf("sealed in %s\n", time.Since(start).Round(time.Second))

	// The pod start: a fresh pool, cache, and follower, measured from the
	// first catalog read to readiness.
	p := newPod()
	db, err := b.openStore(ctx, cmd, nil)
	if err != nil {
		return err
	}
	defer db.Close()
	cache := objcache.New(objcache.Config{MaxBytes: st.ObjectCacheBytes, Memory: p.memory})
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	loadStart := time.Now()
	f, mft, err := b.newFollower(p, db, cache, st.CatalogPollInterval, st.MaxViewAge, st.S3.ReadConcurrency)
	if err != nil {
		return err
	}
	if err := f.Refresh(ctx); err != nil {
		return err
	}
	for f.Ready(ctx) != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
		if err := f.Refresh(ctx); err != nil {
			return err
		}
	}
	load := time.Since(loadStart)
	resident := mft.ResidentBytes()
	runtime.GC()
	runtime.ReadMemStats(&after)
	heap := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	if path := cmd.String("heapprofile"); path != "" {
		if err := writeHeapProfile(path); err != nil {
			return err
		}
	}
	sealed := len(f.Snapshot().Segments(catalog.Main)) - 1
	runtime.KeepAlive(f)

	// A leader's session start also checks the cheap invariants over one
	// whole-catalog read (maintainer.Rebuild), which is O(archive).
	checkStart := time.Now()
	if err := checkCatalog(ctx, db); err != nil {
		return err
	}
	check := time.Since(checkStart)

	fmt.Printf("\npod start over %d sealed segments\n", sealed)
	printKV(os.Stdout, [][2]string{
		{"footers", fmt.Sprintf("%s (%s per segment)", mib(float64(footerBytes)), mib(float64(footerBytes)/float64(segments)))},
		{"load to ready", load.Round(time.Millisecond).String()},
		{"load rate", fmt.Sprintf("%s/s", mib(float64(footerBytes)/load.Seconds()))},
		{"manifest resident", fmt.Sprintf("%s (%.2f bytes per footer byte)", mib(float64(resident)), float64(resident)/float64(footerBytes))},
		{"object cache", fmt.Sprintf("%s (footers stay cached, up to JETSTREAM_OBJECT_CACHE_BYTES)", mib(float64(cache.Size())))},
		{"heap growth", fmt.Sprintf("%s (%.2f bytes per footer byte; %.2f without the cache)", mib(float64(heap)),
			float64(heap)/float64(footerBytes), float64(heap-cache.Size())/float64(footerBytes))},
		{"leader invariant check", check.Round(time.Millisecond).String()},
		{"database size", mib(float64(dbBytes))},
	})
	return nil
}

// checkCatalog runs the leader's session-start invariant check.
func checkCatalog(ctx context.Context, db catalog.DB) error {
	rtx, err := db.BeginRead(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = rtx.Close(ctx) }()
	snap, err := catalog.LoadSnapshot(ctx, rtx)
	if err != nil {
		return err
	}
	return catalog.CheckInvariants(snap, catalog.InvariantOptions{Cheap: true})
}

// buildSegment encodes one segment's blocks, keeps what the catalog rows
// need, and uploads the footer.
func buildSegment(ctx context.Context, gen *generator, uploader *protocol.Uploader, sess *catalog.Session, firstSeq uint64, at time.Time, blocks, dids int) (builtSegment, error) {
	bb, err := segment.NewBlockBuilder(dids)
	if err != nil {
		return builtSegment{}, err
	}
	frames := make([][]byte, 0, blocks)
	infos := make([]segment.BlockInfo, 0, blocks)
	seq := firstSeq
	for j := range blocks {
		for k := range dids {
			// Deletes carry no payload, which the footer does not index.
			ev := segment.Event{
				Seq:         seq,
				WitnessedAt: at.Add(time.Duration(j*dids+k) * time.Millisecond).UnixMicro(),
				Kind:        segment.KindDelete,
				DID:         didFor(gen.rng.Uint64N(gen.universe)),
				Collection:  gen.collection(),
				Rkey:        gen.tid(at),
				Rev:         gen.tid(at),
			}
			if _, err := bb.Append(ev); err != nil {
				return builtSegment{}, err
			}
			seq++
		}
		frame, info := bb.Encode()
		frames = append(frames, frame)
		infos = append(infos, info)
	}
	header, footer, _, err := segment.BuildSealed(segment.SliceFrameSource(frames))
	if err != nil {
		return builtSegment{}, err
	}
	refs, err := uploader.Upload(ctx, sess, [][]byte{footer})
	if err != nil {
		return builtSegment{}, err
	}
	return builtSegment{header: header, footer: refs[0], bytes: int64(len(footer)), infos: infos}, nil
}

// commitSegment writes the rows catalog.Session.Seal leaves behind for
// segment idx, in one fenced transaction, without the active blocks it
// would have folded first: one block commit per block would make a
// pop1-sized fixture take hours. Block objects are rows only.
func commitSegment(ctx context.Context, b *bench, epoch, idx uint64, s builtSegment) (err error) {
	tx, err := b.pg.Begin(ctx, catalog.TxSeal)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(context.Background())
		}
	}()
	rev, ok, err := tx.FenceBump(ctx, epoch)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("lost the lease")
	}
	objs := make([]catalog.NewObject, len(s.infos))
	for i, info := range s.infos {
		_, _ = rand.Read(objs[i].Key[:])
		_, _ = rand.Read(objs[i].SHA256[:])
		objs[i].Length = int64(info.CompressedSize)
	}
	ids, err := tx.InsertObjects(ctx, objs)
	if err != nil {
		return err
	}
	if s.footer.Pending {
		if _, err := tx.SetObjectAvailable(ctx, s.footer.ID); err != nil {
			return err
		}
	}
	gen, err := tx.InsertGeneration(ctx, catalog.GenerationRow{
		Namespace:      catalog.Main,
		Segment:        idx,
		Header:         s.header,
		FooterObjectID: s.footer.ID,
		Revision:       rev,
	})
	if err != nil {
		return err
	}
	rows := make([]catalog.GenerationBlockRow, len(ids))
	for i, id := range ids {
		rows[i] = catalog.GenerationBlockRow{GenerationID: gen, Ordinal: i, ObjectID: id, CompressedLength: int64(s.infos[i].CompressedSize)}
	}
	if err := tx.InsertGenerationBlocks(ctx, rows); err != nil {
		return err
	}
	if ok, err := tx.SealSegment(ctx, catalog.Main, idx, gen, rev); err != nil {
		return err
	} else if !ok {
		return errors.New("segment was not active")
	}
	if err := tx.InsertSegment(ctx, catalog.SegmentRow{Namespace: catalog.Main, Index: idx + 1, State: catalog.Active, Revision: rev}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func writeHeapProfile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := pprof.WriteHeapProfile(f); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
