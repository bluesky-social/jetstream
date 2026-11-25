package oracle

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/catalog/follower"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/ingest/live"
	"github.com/bluesky-social/jetstream/internal/ingest/maintainer"
	"github.com/bluesky-social/jetstream/internal/ingest/repair"
	"github.com/bluesky-social/jetstream/internal/ingest/syncstate"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/objstore"
	"github.com/bluesky-social/jetstream/internal/objstore/memblob"
	"github.com/bluesky-social/jetstream/internal/objstore/protocol"
	"github.com/bluesky-social/jetstream/internal/simulator/fanout"
	simhttp "github.com/bluesky-social/jetstream/internal/simulator/http"
	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/bluesky-social/jetstream/internal/storagefake"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/streaming"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// latencyScheduler injects realistic cross-region catalog commit RTT on
// storagefake without modifying production storagefake behavior.
type latencyScheduler struct {
	commitDelay time.Duration
}

func (s *latencyScheduler) Yield(ctx context.Context, point string) error {
	if point != "commit" || s.commitDelay <= 0 {
		return nil
	}
	t := time.NewTimer(s.commitDelay)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// delayedFoldSink simulates serialized disaggregated block folding latency
// (matching maintainer's sequential fold loop) so hotWriter's
// MaxUnfoldedEvents cap genuinely exercises backpressure during giant resyncs.
type delayedFoldSink struct {
	delay time.Duration
	ch    chan ingest.ClosedBlock
	done  chan struct{}
}

func newDelayedFoldSink(delay time.Duration) *delayedFoldSink {
	s := &delayedFoldSink{
		delay: delay,
		ch:    make(chan ingest.ClosedBlock, 128),
		done:  make(chan struct{}),
	}
	go func() {
		defer close(s.done)
		for blk := range s.ch {
			if s.delay > 0 {
				time.Sleep(s.delay)
			}
			if blk.Folded != nil {
				blk.Folded()
			}
		}
	}()
	return s
}

func (s *delayedFoldSink) BlockClosed(blk ingest.ClosedBlock) {
	s.ch <- blk
}

func (s *delayedFoldSink) Rotate(context.Context) error { return nil }

func (s *delayedFoldSink) wait() {
	close(s.ch)
	<-s.done
}

func sessionMetaStore(db *storagefake.DB, sess *catalog.Session) metastore.Store {
	return db.MetaStore(func(ctx context.Context, ops []metastore.Op) error {
		_, err := sess.CommitMeta(ctx, ops)
		return err
	})
}

func readDisaggMainEvents(t *testing.T, ctx context.Context, db *storagefake.DB, blob objstore.Blob) []segment.Event {
	t.Helper()
	f, err := follower.New(follower.Config{
		DB:              db.Observer("resync-stall-check"),
		Blob:            blob,
		ArchiveID:       db.Archive().ArchiveID,
		Logger:          slog.New(slog.DiscardHandler),
		Metrics:         follower.NewMetrics(prometheus.NewRegistry()),
		ReadConcurrency: 4,
	})
	require.NoError(t, err)
	require.NoError(t, f.Refresh(ctx))
	view := f.Snapshot()
	var out []segment.Event
	for ref := range view.RefsFrom(catalog.Main, 1) {
		evs, err := catalog.DecodeRef(ctx, f, ref)
		require.NoError(t, err)
		for _, ev := range evs {
			ev.Payload = bytes.Clone(ev.Payload)
			out = append(out, ev)
		}
	}
	return out
}

func newResyncStallWorld(t *testing.T, ctx context.Context, accounts int, giantIdx, giantInitialRecords int) (*world.World, *httptest.Server, *identity.Directory, *xrpc.Client) {
	t.Helper()
	cfg := world.DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Accounts = accounts
	cfg.InitialRecords = 0
	cfg.InitialRecordsMax = 0
	w, err := world.New(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	_, err = w.EnsureSeed()
	require.NoError(t, err)
	require.NoError(t, w.Bootstrap(ctx, slog.New(slog.DiscardHandler)))
	require.NoError(t, w.AttachRuntime(rand.New(rand.NewPCG(13, 37)), fanout.New(4096)))

	// Populate giantIdx with giantInitialRecords records in multi-op batches
	// BEFORE the consumer subscribes so the simulator's repo MST is large.
	const batchOps = 50
	for n := 0; n < giantInitialRecords; {
		take := min(batchOps, giantInitialRecords-n)
		specs := make([]world.TargetedOpSpec, take)
		for i := range take {
			specs[i] = world.TargetedOpSpec{
				Action:     "create",
				Collection: "app.bsky.feed.post",
				Rkey:       fmt.Sprintf("seed%05d", n+i),
			}
		}
		_, _, err := w.GenerateMultiOpCommitForTest(ctx, giantIdx, specs)
		require.NoError(t, err)
		n += take
	}

	srv := httptest.NewServer(nil)
	srv.Config.Handler = simhttp.NewHandlerWithOptions(w, srv.URL, simhttp.HandlerOptions{})
	t.Cleanup(srv.Close)

	dir := &identity.Directory{
		Resolver: &identity.DefaultResolver{
			PLCURL:     gt.Some(srv.URL),
			HTTPClient: gt.Some(http.DefaultClient),
		},
		SkipHandleVerification: true,
	}
	xc := &xrpc.Client{Host: srv.URL, HTTPClient: gt.Some(http.DefaultClient)}
	return w, srv, dir, xc
}

// TestOracle_ResyncNoLiveTailStall_Disagg reproduces the PD #5913 failure mode:
// a giant diverging DID triggers a multi-block resync on disaggregated
// hotWriter with realistic catalog commit RTT, tight UploadConcurrency, and
// tight MaxUnfoldedEvents while unrelated live DIDs emit steady traffic and
// the diverging DID itself receives post-divergence commits + deletes.
//
// Proves:
//  1. Unrelated DIDs' ClassLive commits interleave freely between the giant
//     DID's KindSync and final KindCreateResync (zero live-tail stall).
//  2. Zero relay reconnects or stream errors.
//  3. Zero same-DID interleaving: giant DID's KindSync < all KindCreateResync
//     < post-resync live delete & create.
//  4. Final archive state folds to exact simulator ground truth for all DIDs.
func TestOracle_ResyncNoLiveTailStall_Disagg(t *testing.T) {
	t.Parallel()

	for _, mode := range []string{"chain_break", "sync_event"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
			defer cancel()

			const (
				giantIdx     = 0
				liveIdx1     = 1
				liveIdx2     = 2
				giantRecords = 900
			)
			w, srv, dir, xc := newResyncStallWorld(t, ctx, 3, giantIdx, giantRecords)

			giantAcct, err := w.LoadAccount(giantIdx)
			require.NoError(t, err)
			liveAcct1, err := w.LoadAccount(liveIdx1)
			require.NoError(t, err)
			liveAcct2, err := w.LoadAccount(liveIdx2)
			require.NoError(t, err)

			// Seed initial relay cursor at the current tip so the consumer only
			// consumes live traffic from here forward (not the 900-record seed
			// history, mirroring steady state after backfill).
			startTip := w.CurrentSeq()

			db := storagefake.New(storagefake.Config{
				Scheduler: &latencyScheduler{commitDelay: 20 * time.Millisecond},
				OnViolation: func(rev uint64, err error) {
					t.Errorf("catalog invariant at rev %d: %v", rev, err)
				},
			})
			blob := memblob.New()
			lease := db.NewLease()
			require.NoError(t, lease.Acquire(ctx, time.Hour))
			sess := catalog.NewSession(catalog.SessionConfig{DB: db, Epoch: lease.Epoch()})
			_, err = sess.InitNamespace(ctx, catalog.Main, nil)
			require.NoError(t, err)

			metaStore := sessionMetaStore(db, sess)
			require.NoError(t, metaStore.Set(ctx, []byte(live.CursorKey), encodeCursorForTest(startTip)))

			up, err := protocol.NewUploader(protocol.UploaderConfig{
				Blob:      blob,
				ArchiveID: db.Archive().ArchiveID,
				GCDelay:   time.Hour,
				OrphanAge: time.Hour,
			})
			require.NoError(t, err)

			reg := prometheus.NewRegistry()
			ingestMetrics := ingest.NewMetrics(reg)
			liveMetrics := live.NewMetrics(reg)
			repairMetrics := repair.NewMetrics(reg)
			dropMetrics := ingest.NewDropMetrics(reg)

			stateStore := syncstate.New(metaStore)
			verifier, err := atmossync.NewVerifier(atmossync.VerifierOptions{
				Directory:  dir,
				StateStore: stateStore,
				SyncClient: gt.Some(atmossync.NewClient(atmossync.Options{Client: xc, Directory: gt.Some(dir)})),
			})
			require.NoError(t, err)
			t.Cleanup(func() { _ = verifier.Close() })

			stagePrefix := objstore.FormatUUID(db.Archive().ArchiveID) + "/repair/"
			stageStore := repair.NewBlobStageStore(blob, stagePrefix)

			var liveDelivered atomic.Int64
			rec := newEventLogRecorder()
			rep, err := repair.New(repair.Config{
				Store:             metaStore,
				Stage:             stageStore,
				SyncStateStore:    stateStore,
				DropMetrics:       dropMetrics,
				Metrics:           repairMetrics,
				Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
				ChunkSize:         64,
				MetaFlushInterval: 20 * time.Millisecond,
				OnEvent: func(ev *segment.Event) {
					rec.Observe(ev)
				},
			})
			require.NoError(t, err)
			require.NoError(t, rep.Recover(ctx))

			sink := newDelayedFoldSink(30 * time.Millisecond)
			hotCfg := &ingest.HotConfig{
				Session:            sess,
				Uploader:           up,
				Sink:               sink,
				BatchMaxAge:        5 * time.Millisecond,
				BlockMaxAge:        25 * time.Millisecond,
				MaxCommitBatches:   1,
				UploadConcurrency:  2,
				BulkChunkMaxEvents: 64,
				MaxUnfoldedEvents:  128,
			}

			consumer, err := live.Open(live.Config{
				Store:             metaStore,
				SeqKey:            catalog.MainSeqKey,
				CursorKey:         live.CursorKey,
				RelayURL:          srv.URL,
				Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
				Verifier:          verifier,
				SyncStateStore:    stateStore,
				Repair:            rep,
				Metrics:           liveMetrics,
				DropMetrics:       dropMetrics,
				WriterMetrics:     ingestMetrics,
				MaxEventsPerBlock: 64,
				Hot:               hotCfg,
				ReconnectBackoff: &streaming.BackoffPolicy{
					InitialDelay: gt.Some(time.Millisecond),
					MaxDelay:     gt.Some(time.Millisecond),
					Multiplier:   gt.Some(1.0),
					Jitter:       gt.Some(false),
				},
				OnEvent: func(ev *segment.Event) {
					rec.Observe(ev)
					if ev.DID == string(liveAcct1.DID) || ev.DID == string(liveAcct2.DID) {
						liveDelivered.Add(1)
					}
				},
			})
			require.NoError(t, err)

			runCtx, runCancel := context.WithCancel(ctx)
			consumerDone := make(chan error, 1)
			repairDone := make(chan error, 1)
			go func() { consumerDone <- consumer.Run(runCtx) }()
			go func() { repairDone <- rep.Run(runCtx) }()

			// Step 1: Establish baseline chain state on all 3 accounts via live
			// commits so the verifier knows giantDID's chain head.
			_, _, err = w.GenerateRecordOpForTest(ctx, giantIdx, "create", "app.bsky.feed.post", "baseline-giant")
			require.NoError(t, err)
			_, _, err = w.GenerateRecordOpForTest(ctx, liveIdx1, "create", "app.bsky.feed.post", "baseline-live1")
			require.NoError(t, err)
			_, _, err = w.GenerateRecordOpForTest(ctx, liveIdx2, "create", "app.bsky.feed.post", "baseline-live2")
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				return liveDelivered.Load() >= 2 && consumer.LastUpstreamSeq() >= w.CurrentSeq()
			}, 10*time.Second, 5*time.Millisecond, "baseline commits did not land")
			// Step 2: Trigger divergence on the 900-record giant DID!
			switch mode {
			case "chain_break":
				_, err = w.GenerateSilentMutationThenCommitForTest(ctx, giantIdx)
			case "sync_event":
				_, err = w.GenerateSilentMutationThenSyncForTest(ctx, giantIdx)
			}
			require.NoError(t, err)

			// Wait until the per-DID repair gate activates for giantDID
			// (meaning the resync event has arrived on Consumer.Run and been
			// handed off to the background ClassBulk repair worker).
			require.Eventually(t, func() bool {
				return rep.IsGated(giantAcct.DID)
			}, 10*time.Second, 2*time.Millisecond, "giant DID never entered repair gate")

			// Step 3: Immediately emit steady unrelated live commits on
			// liveIdx1 & liveIdx2 (1-record accounts, <1ms to generate in the
			// simulator) BEFORE doing the slower 900-record MST mutations on
			// giantIdx, so ClassLive commits reliably overlap the entire
			// multi-block ClassBulk publish window even under heavy -race load.
			unrelatedSent := 0
			const minUnrelated = 15
			const maxUnrelated = 60
			for unrelatedSent < maxUnrelated && (unrelatedSent < minUnrelated || rep.IsGated(giantAcct.DID)) {
				idx := liveIdx1
				if unrelatedSent%2 == 1 {
					idx = liveIdx2
				}
				_, _, gerr := w.GenerateRecordOpForTest(ctx, idx, "create", "app.bsky.feed.post", fmt.Sprintf("live%03d", unrelatedSent))
				require.NoError(t, gerr)
				unrelatedSent++
				if unrelatedSent == 5 {
					// Emit post-divergence delete + create on giantDID while
					// the repair gate is active so they route through its
					// durable inbox and replay in order after KindCreateResync.
					_, _, err = w.GenerateRecordOpForTest(ctx, giantIdx, "delete", "app.bsky.feed.post", "seed00010")
					require.NoError(t, err)
					_, _, err = w.GenerateRecordOpForTest(ctx, giantIdx, "create", "app.bsky.feed.post", "post-resync-create")
					require.NoError(t, err)
				}
				time.Sleep(15 * time.Millisecond)
			}

			// Step 4: Wait for giantDID repair to complete and all live traffic
			// to reach durable storage.
			finalTip := w.CurrentSeq()
			require.Eventually(t, func() bool {
				return !rep.IsGated(giantAcct.DID) &&
					consumer.LastUpstreamSeq() >= finalTip &&
					liveDelivered.Load() >= int64(2+unrelatedSent)
			}, 30*time.Second, 10*time.Millisecond, "repair or live traffic did not finish")

			require.NoError(t, rep.WaitIdle(ctx))
			runCancel()
			require.ErrorIs(t, <-consumerDone, context.Canceled)
			require.ErrorIs(t, <-repairDone, context.Canceled)
			require.NoError(t, consumer.Close())
			sink.wait()

			// Step 5: Inspect the full durable archive through the real follower
			// read path.
			evs := readDisaggMainEvents(t, ctx, db, blob)
			obsEvs := disaggObserved(evs)
			require.NoError(t, CheckInvariants(obsEvs))

			// Locate giantDID's KindSync, last KindCreateResync, and post-resync
			// live ops, plus count unrelated live events that landed strictly
			// BETWEEN KindSync and the final KindCreateResync.
			var giantSyncSeq, giantLastResyncSeq uint64
			var giantDeleteSeq, giantCreateSeq uint64
			for _, ev := range evs {
				if ev.DID != string(giantAcct.DID) {
					continue
				}
				switch {
				case ev.Kind == segment.KindSync:
					require.Zero(t, giantSyncSeq, "expected exactly one KindSync for giantDID")
					giantSyncSeq = ev.Seq
				case ev.Kind == segment.KindCreateResync:
					if ev.Seq > giantLastResyncSeq {
						giantLastResyncSeq = ev.Seq
					}
				case ev.Kind == segment.KindDelete && ev.Rkey == "seed00010":
					giantDeleteSeq = ev.Seq
				case ev.Kind == segment.KindCreate && ev.Rkey == "post-resync-create":
					giantCreateSeq = ev.Seq
				}
			}
			require.NotZero(t, giantSyncSeq, "giantDID must have a KindSync row")
			require.Greater(t, giantLastResyncSeq, giantSyncSeq, "giantDID must have KindCreateResync rows after KindSync")

			// Zero same-DID interleaving: post-resync delete & create MUST land
			// strictly AFTER the last KindCreateResync row (or be covered by
			// the snapshot if atmos fetched after them).
			if giantDeleteSeq != 0 {
				require.Greater(t, giantDeleteSeq, giantLastResyncSeq,
					"post-resync delete must not interleave before giantDID's final KindCreateResync")
			}
			if giantCreateSeq != 0 {
				require.Greater(t, giantCreateSeq, giantLastResyncSeq,
					"post-resync create must not interleave before giantDID's final KindCreateResync")
			}

			// Anti-vacuity non-stall proof: unrelated live commits MUST have
			// landed strictly interleaved during giantDID's ClassBulk publish!
			interleavedUnrelated := 0
			for _, ev := range evs {
				if (ev.DID == string(liveAcct1.DID) || ev.DID == string(liveAcct2.DID)) &&
					ev.Seq > giantSyncSeq && ev.Seq < giantLastResyncSeq {
					interleavedUnrelated++
				}
			}
			require.GreaterOrEqual(t, interleavedUnrelated, 3,
				"unrelated ClassLive commits must interleave during giant ClassBulk resync (got %d between seq %d and %d)",
				interleavedUnrelated, giantSyncSeq, giantLastResyncSeq)

			// Zero reconnects and both admission classes genuinely exercised.
			require.Zero(t, testutil.ToFloat64(liveMetrics.Reconnects), "relay must see zero reconnects")
			require.Greater(t, testutil.ToFloat64(repairMetrics.BulkChunks), 5.0, "giant resync must span multiple ClassBulk chunks")
			require.Greater(t, testutil.ToFloat64(ingestMetrics.HotBatches.WithLabelValues("bulk", "pointer"))+
				testutil.ToFloat64(ingestMetrics.HotBatches.WithLabelValues("bulk", "inline")), 0.0)
			require.Greater(t, testutil.ToFloat64(ingestMetrics.HotBatches.WithLabelValues("live", "inline"))+
				testutil.ToFloat64(ingestMetrics.HotBatches.WithLabelValues("live", "pointer")), 0.0)

			// Exact fold convergence against simulator ground truth for ALL DIDs
			// (including post-resync delete & create on giantDID!).
			gotState, err := Reconstruct(EventsSortedBySeq(obsEvs))
			require.NoError(t, err)
			wantState, err := GroundTruthFromWorld(w)
			require.NoError(t, err)
			require.NoError(t, Compare(wantState, gotState),
				"archive fold must converge identically to simulator ground truth for all DIDs")
		})
	}
}

// TestOracle_FailedRepoRetryConcurrentLiveNoInterleaving_Disagg proves that
// steady-state RunFailedRepoRetry publishing via ClassBulk behind the shared
// per-DID gate never interleaves with concurrent ClassLive events for the SAME
// DID (closing the ClassLive vs ClassBulk same-DID race where a live delete
// could slip between retry chunks and get resurrected).
func TestOracle_FailedRepoRetryConcurrentLiveNoInterleaving_Disagg(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	const (
		retryIdx     = 0
		otherIdx     = 1
		retryRecords = 320
	)
	w, srv, dir, xc := newResyncStallWorld(t, ctx, 2, retryIdx, retryRecords)
	retryAcct, err := w.LoadAccount(retryIdx)
	require.NoError(t, err)
	otherAcct, err := w.LoadAccount(otherIdx)
	require.NoError(t, err)

	startTip := w.CurrentSeq()

	db := storagefake.New(storagefake.Config{
		Scheduler: &latencyScheduler{commitDelay: 10 * time.Millisecond},
		OnViolation: func(rev uint64, err error) {
			t.Errorf("catalog invariant at rev %d: %v", rev, err)
		},
	})
	blob := memblob.New()
	lease := db.NewLease()
	require.NoError(t, lease.Acquire(ctx, time.Hour))
	sess := catalog.NewSession(catalog.SessionConfig{DB: db, Epoch: lease.Epoch()})
	_, err = sess.InitNamespace(ctx, catalog.Main, nil)
	require.NoError(t, err)

	metaStore := sessionMetaStore(db, sess)
	require.NoError(t, metaStore.Set(ctx, []byte(live.CursorKey), encodeCursorForTest(startTip)))

	// Seed retryAcct as StatusFailed due immediately so RunFailedRepoRetry picks it up.
	bfStore := backfill.NewStore(metaStore, nil)
	require.NoError(t, bfStore.SeedCounts(ctx))
	require.NoError(t, bfStore.OnDiscover(ctx, atmossync.ListReposEntry{DID: retryAcct.DID, Active: true}))
	require.NoError(t, bfStore.OnFail(ctx, retryAcct.DID, "", errors.New("seed failure"), 1))
	rawExisting, err := metaStore.Get(ctx, []byte("repo/"+string(retryAcct.DID)))
	require.NoError(t, err)
	rs, err := backfill.DecodeRepoStatus(rawExisting)
	require.NoError(t, err)
	rs.Backfill.NextAttemptAt = time.Now().Add(-time.Minute)
	rawRS, err := backfill.EncodeRepoStatus(rs)
	require.NoError(t, err)
	require.NoError(t, metaStore.Set(ctx, []byte("repo/"+string(retryAcct.DID)), rawRS))

	up, err := protocol.NewUploader(protocol.UploaderConfig{
		Blob:      blob,
		ArchiveID: db.Archive().ArchiveID,
		GCDelay:   time.Hour,
		OrphanAge: time.Hour,
	})
	require.NoError(t, err)

	reg := prometheus.NewRegistry()
	stateStore := syncstate.New(metaStore)
	verifier, err := atmossync.NewVerifier(atmossync.VerifierOptions{
		Directory:  dir,
		StateStore: stateStore,
		SyncClient: gt.Some(atmossync.NewClient(atmossync.Options{Client: xc, Directory: gt.Some(dir)})),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = verifier.Close() })

	stageStore := repair.NewBlobStageStore(blob, objstore.FormatUUID(db.Archive().ArchiveID)+"/repair/")
	repairMetrics := repair.NewMetrics(reg)

	gateEntered := make(chan struct{})
	resumeBulk := make(chan struct{})
	var gateOnce sync.Once
	rep, err := repair.New(repair.Config{
		Store:             metaStore,
		Stage:             stageStore,
		SyncStateStore:    stateStore,
		Metrics:           repairMetrics,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		ChunkSize:         48,
		MetaFlushInterval: 20 * time.Millisecond,
		OnAfterChunkCommit: func(hctx context.Context, did atmos.DID, nextRow, _ int64) error {
			if did == retryAcct.DID && nextRow == 48 {
				gateOnce.Do(func() { close(gateEntered) })
				// Hold chunk 2 deterministically until the test injects live
				// commits/deletes on retryAcct and Consumer.Run captures them
				// into the durable inbox while ClassBulk is in flight.
				select {
				case <-resumeBulk:
				case <-hctx.Done():
					return hctx.Err()
				}
			}
			return nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, rep.Recover(ctx))

	sink := newDelayedFoldSink(10 * time.Millisecond)
	var otherDelivered atomic.Int64
	consumer, err := live.Open(live.Config{
		Store:             metaStore,
		SeqKey:            catalog.MainSeqKey,
		CursorKey:         live.CursorKey,
		RelayURL:          srv.URL,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:          verifier,
		SyncStateStore:    stateStore,
		Repair:            rep,
		Metrics:           live.NewMetrics(reg),
		WriterMetrics:     ingest.NewMetrics(reg),
		MaxEventsPerBlock: 48,
		Hot: &ingest.HotConfig{
			Session:            sess,
			Uploader:           up,
			Sink:               sink,
			BatchMaxAge:        5 * time.Millisecond,
			BlockMaxAge:        20 * time.Millisecond,
			MaxCommitBatches:   1,
			UploadConcurrency:  2,
			BulkChunkMaxEvents: 48,
			MaxUnfoldedEvents:  96,
		},
		OnEvent: func(ev *segment.Event) {
			if ev.DID == string(otherAcct.DID) {
				otherDelivered.Add(1)
			}
		},
	})
	require.NoError(t, err)

	runCtx, runCancel := context.WithCancel(ctx)
	consumerDone := make(chan error, 1)
	repairDone := make(chan error, 1)
	retryDone := make(chan error, 1)
	go func() { consumerDone <- consumer.Run(runCtx) }()
	go func() { repairDone <- rep.Run(runCtx) }()
	go func() {
		retryDone <- backfill.RunFailedRepoRetry(runCtx, backfill.RetryConfig{
			Store:         metaStore,
			BackfillStore: bfStore,
			Writer:        consumer.Writer(),
			Repair:        rep,
			HTTPClient:    http.DefaultClient,
			RelayURL:      srv.URL,
			Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
			Metrics:       backfill.NewMetrics(reg),
			Interval:      50 * time.Millisecond,
			Workers:       1,
			HostWorkers:   1,
			MaxDelay:      time.Second,
		})
	}()

	// Wait until RunFailedRepoRetry has entered the per-DID gate and committed
	// chunk 1 of retryAcct (so retryAcct is actively mid-ClassBulk publish!).
	select {
	case <-gateEntered:
	case <-ctx.Done():
		t.Fatal("timed out waiting for RunFailedRepoRetry to start publishing")
	}

	// Emit a live DELETE of a record that is in a LATER ClassBulk chunk of
	// retryAcct (seed00300) + a live CREATE on retryAcct, plus unrelated live
	// commits on otherAcct while chunk 2 is paused!
	_, _, err = w.GenerateRecordOpForTest(ctx, retryIdx, "delete", "app.bsky.feed.post", "seed00300")
	require.NoError(t, err)
	_, _, err = w.GenerateRecordOpForTest(ctx, retryIdx, "create", "app.bsky.feed.post", "live-during-retry")
	require.NoError(t, err)
	for i := range 8 {
		_, _, err = w.GenerateRecordOpForTest(ctx, otherIdx, "create", "app.bsky.feed.post", fmt.Sprintf("other%02d", i))
		require.NoError(t, err)
	}

	// Prove both retryAcct live events were captured into the durable inbox AND
	// all 8 unrelated live commits landed BEFORE chunk 2 even resumes!
	midTip := w.CurrentSeq()
	require.Eventually(t, func() bool {
		return consumer.LastUpstreamSeq() >= midTip &&
			otherDelivered.Load() >= 8 &&
			testutil.ToFloat64(repairMetrics.InboxCaptured) >= 2.0
	}, 10*time.Second, 5*time.Millisecond, "live events during retry did not land / route to inbox while chunk 2 was paused")
	close(resumeBulk)

	finalTip := w.CurrentSeq()
	require.Eventually(t, func() bool {
		raw, gerr := metaStore.Get(ctx, []byte("repo/"+string(retryAcct.DID)))
		if gerr != nil {
			return false
		}
		st, derr := backfill.DecodeRepoStatus(raw)
		return derr == nil && st.Backfill.Status == backfill.StatusComplete &&
			!rep.IsGated(retryAcct.DID) &&
			consumer.LastUpstreamSeq() >= finalTip &&
			otherDelivered.Load() >= 8
	}, 30*time.Second, 10*time.Millisecond, "failed-repo retry or live events did not finish")

	require.NoError(t, rep.WaitIdle(ctx))
	runCancel()
	require.ErrorIs(t, <-consumerDone, context.Canceled)
	require.ErrorIs(t, <-repairDone, context.Canceled)
	require.NoError(t, <-retryDone)
	require.NoError(t, consumer.Close())
	sink.wait()

	evs := readDisaggMainEvents(t, ctx, db, blob)
	obsEvs := disaggObserved(evs)
	require.NoError(t, CheckInvariants(obsEvs))

	// Verify ZERO same-DID interleaving for retryAcct:
	// KindSync < ALL KindCreateResync < live delete(seed00300) & live create!
	var syncSeq, lastResyncSeq, delSeq, createSeq uint64
	for _, ev := range evs {
		if ev.DID != string(retryAcct.DID) {
			continue
		}
		switch {
		case ev.Kind == segment.KindSync:
			syncSeq = ev.Seq
		case ev.Kind == segment.KindCreateResync:
			if ev.Seq > lastResyncSeq {
				lastResyncSeq = ev.Seq
			}
		case ev.Kind == segment.KindDelete && ev.Rkey == "seed00300":
			delSeq = ev.Seq
		case ev.Kind == segment.KindCreate && ev.Rkey == "live-during-retry":
			createSeq = ev.Seq
		}
	}
	require.NotZero(t, syncSeq)
	require.Greater(t, lastResyncSeq, syncSeq)
	require.Greater(t, delSeq, lastResyncSeq, "live delete must land AFTER all ClassBulk KindCreateResync chunks")
	require.Greater(t, createSeq, lastResyncSeq, "live create must land AFTER all ClassBulk KindCreateResync chunks")

	// Exact fold convergence proves seed00300 stayed deleted (NOT resurrected
	// by a later KindCreateResync chunk) and live-during-retry is present!
	gotState, err := Reconstruct(EventsSortedBySeq(obsEvs))
	require.NoError(t, err)
	wantState, err := GroundTruthFromWorld(w)
	require.NoError(t, err)
	require.NoError(t, Compare(wantState, gotState))
	require.GreaterOrEqual(t, testutil.ToFloat64(repairMetrics.InboxCaptured), 2.0)
	require.GreaterOrEqual(t, testutil.ToFloat64(repairMetrics.InboxDrained), 2.0)
}

// TestOracle_ResyncCrashResumeMidPublish_Disagg injects a leader crash /
// session failover after KindSync + 2 replacement chunks have committed in
// disaggregated mode, then starts a fresh leader epoch over the same
// storagefake DB and BlobStageStore.
//
// Proves:
//  1. Session 2 restores the gate and resumes from committed NextRow without
//     re-emitting KindSync or restarting from row 0.
//  2. Pre-crash durable inbox entries and post-restart live commits both
//     converge identically to simulator ground truth.
func TestOracle_ResyncCrashResumeMidPublish_Disagg(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	const (
		giantIdx     = 0
		giantRecords = 350
	)
	w, srv, dir, xc := newResyncStallWorld(t, ctx, 2, giantIdx, giantRecords)
	giantAcct, err := w.LoadAccount(giantIdx)
	require.NoError(t, err)

	startTip := w.CurrentSeq()

	db := storagefake.New(storagefake.Config{
		OnViolation: func(rev uint64, err error) {
			t.Errorf("catalog invariant at rev %d: %v", rev, err)
		},
	})
	blob := memblob.New()
	stagePrefix := objstore.FormatUUID(db.Archive().ArchiveID) + "/repair/"

	// Session 1: crash after chunk 2 commits (NextRow == 128).
	{
		lease1 := db.NewLease()
		require.NoError(t, lease1.Acquire(ctx, time.Hour))
		sess1 := catalog.NewSession(catalog.SessionConfig{DB: db, Epoch: lease1.Epoch()})
		_, err = sess1.InitNamespace(ctx, catalog.Main, nil)
		require.NoError(t, err)

		meta1 := sessionMetaStore(db, sess1)
		require.NoError(t, meta1.Set(ctx, []byte(live.CursorKey), encodeCursorForTest(startTip)))

		up1, err := protocol.NewUploader(protocol.UploaderConfig{
			Blob: blob, ArchiveID: db.Archive().ArchiveID, GCDelay: time.Hour, OrphanAge: time.Hour,
		})
		require.NoError(t, err)

		ss1 := syncstate.New(meta1)
		v1, err := atmossync.NewVerifier(atmossync.VerifierOptions{
			Directory:  dir,
			StateStore: ss1,
			SyncClient: gt.Some(atmossync.NewClient(atmossync.Options{Client: xc, Directory: gt.Some(dir)})),
		})
		require.NoError(t, err)

		errCrash := errors.New("leader crash after chunk 2")
		crashed := make(chan struct{})
		var crashOnce sync.Once
		rep1, err := repair.New(repair.Config{
			Store:             meta1,
			Stage:             repair.NewBlobStageStore(blob, stagePrefix),
			SyncStateStore:    ss1,
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
			ChunkSize:         64,
			MetaFlushInterval: 10 * time.Millisecond,
			OnAfterChunkCommit: func(_ context.Context, did atmos.DID, nextRow, _ int64) error {
				if did == giantAcct.DID && nextRow >= 128 {
					crashOnce.Do(func() { close(crashed) })
					return errCrash
				}
				return nil
			},
		})
		require.NoError(t, err)
		require.NoError(t, rep1.Recover(ctx))

		c1, err := live.Open(live.Config{
			Store:             meta1,
			SeqKey:            catalog.MainSeqKey,
			CursorKey:         live.CursorKey,
			RelayURL:          srv.URL,
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
			Verifier:          v1,
			SyncStateStore:    ss1,
			Repair:            rep1,
			MaxEventsPerBlock: 64,
			Hot: &ingest.HotConfig{
				Session:            sess1,
				Uploader:           up1,
				BatchMaxAge:        5 * time.Millisecond,
				BlockMaxAge:        20 * time.Millisecond,
				UploadConcurrency:  2,
				BulkChunkMaxEvents: 64,
			},
		})
		require.NoError(t, err)

		s1Ctx, s1Cancel := context.WithCancel(ctx)
		c1Done := make(chan error, 1)
		r1Done := make(chan error, 1)
		go func() { c1Done <- c1.Run(s1Ctx) }()
		go func() { r1Done <- rep1.Run(s1Ctx) }()

		// Baseline commit + trigger #sync divergence + pre-crash live delete.
		_, _, err = w.GenerateRecordOpForTest(ctx, giantIdx, "create", "app.bsky.feed.post", "pre-sync")
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			return c1.LastUpstreamSeq() >= w.CurrentSeq()
		}, 10*time.Second, 5*time.Millisecond)

		_, err = w.GenerateSilentMutationThenSyncForTest(ctx, giantIdx)
		require.NoError(t, err)

		// Emit a post-sync live delete so it lands in the durable inbox before
		// or right around the crash boundary.
		_, _, err = w.GenerateRecordOpForTest(ctx, giantIdx, "delete", "app.bsky.feed.post", "seed00250")
		require.NoError(t, err)

		select {
		case rErr := <-r1Done:
			require.ErrorIs(t, rErr, errCrash)
		case <-ctx.Done():
			t.Fatal("timed out waiting for session 1 simulated crash")
		}
		<-crashed
		s1Cancel()
		require.ErrorIs(t, <-c1Done, context.Canceled)
		require.NoError(t, c1.Close())
		require.NoError(t, v1.Close())
		require.NoError(t, lease1.Release(ctx))
	}

	// Session 2: brand-new leader epoch over the SAME storagefake.DB & blob.
	// Run maintainer.Rebuild first (design §10.9) just like jetstreamd failover.
	lease2 := db.NewLease()
	require.NoError(t, lease2.Acquire(ctx, time.Hour))
	sess2 := catalog.NewSession(catalog.SessionConfig{DB: db, Epoch: lease2.Epoch()})
	meta2 := sessionMetaStore(db, sess2)
	up2, err := protocol.NewUploader(protocol.UploaderConfig{
		Blob: blob, ArchiveID: db.Archive().ArchiveID, GCDelay: time.Hour, OrphanAge: time.Hour,
	})
	require.NoError(t, err)
	reader2, err := protocol.NewReader(protocol.ReaderConfig{
		Rows: protocol.DBRows{DB: db}, Blob: blob, ArchiveID: db.Archive().ArchiveID,
	})
	require.NoError(t, err)
	maint2, err := maintainer.Open(ctx, maintainer.Config{
		Session:           sess2,
		Uploader:          up2,
		Objects:           reader2,
		MaxEventsPerBlock: 64,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	defer func() { _ = maint2.Close() }()
	resume2, err := maint2.Rebuild(ctx, maintainer.RebuildConfig{BlockMaxAge: time.Hour})
	require.NoError(t, err)

	reg2 := prometheus.NewRegistry()
	repairMetrics2 := repair.NewMetrics(reg2)
	ss2 := syncstate.New(meta2)
	v2, err := atmossync.NewVerifier(atmossync.VerifierOptions{
		Directory:  dir,
		StateStore: ss2,
		SyncClient: gt.Some(atmossync.NewClient(atmossync.Options{Client: xc, Directory: gt.Some(dir)})),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = v2.Close() })

	rep2, err := repair.New(repair.Config{
		Store:             meta2,
		Stage:             repair.NewBlobStageStore(blob, stagePrefix),
		SyncStateStore:    ss2,
		Metrics:           repairMetrics2,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		ChunkSize:         64,
		MetaFlushInterval: 10 * time.Millisecond,
	})
	require.NoError(t, err)
	require.NoError(t, rep2.Recover(ctx))
	require.True(t, rep2.IsGated(giantAcct.DID), "session 2 must restore gate before starting verifier")
	require.Equal(t, 1.0, testutil.ToFloat64(repairMetrics2.JobsResumed))

	c2, err := live.Open(live.Config{
		Store:             meta2,
		SeqKey:            catalog.MainSeqKey,
		CursorKey:         live.CursorKey,
		RelayURL:          srv.URL,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:          v2,
		SyncStateStore:    ss2,
		Repair:            rep2,
		MaxEventsPerBlock: 64,
		Hot: &ingest.HotConfig{
			Session:            sess2,
			Uploader:           up2,
			Sink:               maint2,
			Resume:             resume2,
			BatchMaxAge:        5 * time.Millisecond,
			BlockMaxAge:        20 * time.Millisecond,
			UploadConcurrency:  2,
			BulkChunkMaxEvents: 64,
		},
	})
	require.NoError(t, err)

	s2Ctx, s2Cancel := context.WithCancel(ctx)
	c2Done := make(chan error, 1)
	r2Done := make(chan error, 1)
	go func() { c2Done <- c2.Run(s2Ctx) }()
	go func() { r2Done <- rep2.Run(s2Ctx) }()

	// Emit one more post-failover live commit on giantDID!
	_, _, err = w.GenerateRecordOpForTest(ctx, giantIdx, "create", "app.bsky.feed.post", "post-failover-create")
	require.NoError(t, err)

	finalTip := w.CurrentSeq()
	require.Eventually(t, func() bool {
		return !rep2.IsGated(giantAcct.DID) && c2.LastUpstreamSeq() >= finalTip
	}, 30*time.Second, 10*time.Millisecond, "session 2 did not finish resumed repair and live commit")

	require.NoError(t, rep2.WaitIdle(ctx))
	s2Cancel()
	require.ErrorIs(t, <-c2Done, context.Canceled)
	require.ErrorIs(t, <-r2Done, context.Canceled)
	require.NoError(t, c2.Close())

	evs := readDisaggMainEvents(t, ctx, db, blob)
	obsEvs := disaggObserved(evs)
	require.NoError(t, CheckInvariants(obsEvs))

	// Across BOTH leader sessions, KindSync MUST appear EXACTLY ONCE!
	syncCount := 0
	for _, ev := range evs {
		if ev.DID == string(giantAcct.DID) && ev.Kind == segment.KindSync {
			syncCount++
		}
	}
	require.Equal(t, 1, syncCount, "failover resume must continue from committed NextRow without re-emitting KindSync")

	gotState, err := Reconstruct(EventsSortedBySeq(obsEvs))
	require.NoError(t, err)
	wantState, err := GroundTruthFromWorld(w)
	require.NoError(t, err)
	require.NoError(t, Compare(wantState, gotState),
		"resumed archive must converge identically to simulator ground truth across leader failover")
}

func encodeCursorForTest(seq int64) []byte {
	var b [9]byte
	b[0] = 1 // cursorV1
	v := uint64(seq)
	b[1] = byte(v)
	b[2] = byte(v >> 8)
	b[3] = byte(v >> 16)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 32)
	b[6] = byte(v >> 40)
	b[7] = byte(v >> 48)
	b[8] = byte(v >> 56)
	return b[:]
}
