package repair_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/live"
	"github.com/bluesky-social/jetstream/internal/ingest/repair"
	"github.com/bluesky-social/jetstream/internal/ingest/syncstate"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/metastore/memstore"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/mst"
	atmosrepo "github.com/jcalabro/atmos/repo"
	"github.com/jcalabro/atmos/streaming"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func testRecordCBOR(t *testing.T, text string) []byte {
	t.Helper()
	b, err := cbor.Marshal(map[string]any{
		"$type": "app.bsky.feed.post",
		"text":  text,
	})
	require.NoError(t, err)
	return b
}

func makeSignedRepo(t *testing.T, did atmos.DID, rev string, records map[string]string) (*atmosrepo.Repo, *atmosrepo.Commit) {
	t.Helper()
	priv, err := crypto.GenerateP256()
	require.NoError(t, err)

	store := mst.NewMemBlockStore()
	tree := mst.NewTree(store)
	for path, text := range records {
		raw := testRecordCBOR(t, text)
		cid := cbor.ComputeCID(cbor.CodecDagCBOR, raw)
		require.NoError(t, store.PutBlock(cid, raw))
		require.NoError(t, tree.Insert(path, cid))
	}
	rootCID, err := tree.WriteBlocks(store)
	require.NoError(t, err)

	commit := &atmosrepo.Commit{
		DID:     string(did),
		Version: 3,
		Data:    rootCID,
		Rev:     rev,
	}
	require.NoError(t, commit.Sign(priv))
	return &atmosrepo.Repo{
		Store: store,
		Tree:  tree,
	}, commit
}

type recordedStream struct {
	mu     sync.Mutex
	events []segment.Event
}

func (r *recordedStream) observe(ev *segment.Event) {
	cp := *ev
	cp.Payload = append([]byte(nil), ev.Payload...)
	r.mu.Lock()
	r.events = append(r.events, cp)
	r.mu.Unlock()
}

func (r *recordedStream) snapshot() []segment.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]segment.Event, len(r.events))
	copy(out, r.events)
	return out
}

func openLocalRepairStack(
	t *testing.T,
	store metastore.Store,
	stage repair.StageStore,
	segFS vfs.FS,
	segDir string,
	chunkSize int,
	onAfterChunk func(ctx context.Context, did atmos.DID, nextRow, totalRows int64) error,
	fetchRepo repair.FetchRepoFunc,
) (*repair.Coordinator, *syncstate.StateStore, *ingest.Writer, *repair.Metrics, *recordedStream) {
	t.Helper()
	ss := syncstate.New(store)
	metrics := repair.NewMetrics(prometheus.NewRegistry())
	rec := &recordedStream{}

	rep, err := repair.New(repair.Config{
		Store:              store,
		Stage:              stage,
		SyncStateStore:     ss,
		Metrics:            metrics,
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnEvent:            rec.observe,
		ChunkSize:          chunkSize,
		MetaFlushInterval:  -1, // explicit drain in unit tests
		OnAfterChunkCommit: onAfterChunk,
		FetchRepo:          fetchRepo,
	})
	require.NoError(t, err)

	type prep struct {
		sync   *syncstate.Snapshot
		repair *repair.MetaSnapshot
	}

	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       segDir,
		FS:                segFS,
		Store:             store,
		SeqKey:            live.SteadySeqKey,
		MaxEventsPerBlock: 16,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnAppend: func(ev *segment.Event) error {
			switch ev.Kind {
			case segment.KindAccount:
				ss.RecordAccountSeq(atmos.DID(ev.DID), ev.UpstreamRelayCursor)
			case segment.KindIdentity:
				ss.RecordIdentitySeq(atmos.DID(ev.DID), ev.UpstreamRelayCursor)
			}
			return rep.OnAppend(ev)
		},
		DurableBatchPrepareValue: func() any {
			return prep{
				sync:   ss.Snapshot(),
				repair: rep.SnapshotMeta(),
			}
		},
		OnDurableBatch: func(_ context.Context, b metastore.Batch, _ uint64, _ bool, prepareValue any) (func(), func(error), error) {
			p, ok := prepareValue.(prep)
			if !ok {
				return nil, nil, fmt.Errorf("unexpected prepare type %T", prepareValue)
			}
			ssStaged := false
			if p.sync != nil {
				ss.StageSnapshot(b, p.sync)
				ssStaged = true
			}
			repStaged := rep.StageMetaSnapshot(b, p.repair)
			return func() {
					if ssStaged {
						ss.CommitStaged()
					}
					if repStaged {
						rep.CommitMetaStaged()
					}
				}, func(err error) {
					if err != nil {
						if ssStaged {
							ss.AbortStaged()
						}
						if repStaged {
							rep.AbortMetaStaged()
						}
					}
				}, nil
		},
	})
	require.NoError(t, err)
	rep.AttachWriter(w)
	return rep, ss, w, metrics, rec
}

func TestCoordinator_GateInboxRevFilteringControlOrderAndCutoff(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store := memstore.New()
	stage := repair.NewMemStageStore()
	segFS := vfs.NewMem()
	rep, ss, w, metrics, rec := openLocalRepairStack(t, store, stage, segFS, "/segments", 2, nil, nil)
	defer func() { _ = w.Close() }()

	did := atmos.DID("did:plc:target000000000000000000")
	otherDID := atmos.DID("did:plc:other0000000000000000000")

	rev41 := "3l3qo2vutsw2b"
	rev42 := "3l3qo2vutsw2c"
	rev43 := "3l3qo2vutsw2d"

	// Open the gate via BeginFailedRetry (simulating a retry downloading while
	// live events arrive for both target DID and an unrelated DID).
	handle, err := rep.BeginFailedRetry(ctx, did)
	require.NoError(t, err)
	require.True(t, rep.IsGated(did))
	require.False(t, rep.IsGated(otherDID))

	// Unrelated DID bypasses the gate immediately.
	otherEv := []segment.Event{{
		Kind:       segment.KindCreate,
		DID:        string(otherDID),
		Collection: "app.bsky.feed.post",
		Rkey:       "other1",
		Rev:        rev41,
		Payload:    testRecordCBOR(t, "unrelated"),
	}}
	handled, err := rep.InterceptLiveGroup(10, otherEv)
	require.NoError(t, err)
	require.False(t, handled, "unrelated DID must stay on ClassLive without waiting")

	// Target DID receives:
	//  - rev41 (covered by snapshot rev42 -> must be skipped on drain)
	//  - duplicate seq=11 (must be deduped)
	//  - rev42 (covered by snapshot rev42 -> must be skipped on drain)
	//  - #account event (control event -> must be preserved in delivery order)
	//  - #identity event (control event -> must be preserved in delivery order)
	//  - rev43 delete of "rec1" + create of "rec3" (newer than rev42 -> must win)
	acctPayload, err := (&comatproto.SyncSubscribeRepos_Account{DID: string(did), Seq: 13, Active: true, Time: "2026-11-25T00:00:00Z"}).MarshalCBOR()
	require.NoError(t, err)
	identPayload, err := (&comatproto.SyncSubscribeRepos_Identity{DID: string(did), Seq: 14, Time: "2026-11-25T00:00:00Z"}).MarshalCBOR()
	require.NoError(t, err)

	fakeCID := cbor.ComputeCID(cbor.CodecDagCBOR, []byte{0xa0})
	require.NoError(t, ss.SaveChain(ctx, did, atmossync.ChainState{Rev: rev41, Data: fakeCID}))
	require.NoError(t, ss.SaveChain(ctx, did, atmossync.ChainState{Rev: rev42, Data: fakeCID}))
	require.NoError(t, ss.SaveHosting(ctx, did, atmossync.HostingState{Seq: 13, Active: true}))
	require.NoError(t, ss.SaveChain(ctx, did, atmossync.ChainState{Rev: rev43, Data: fakeCID}))

	for _, tc := range []struct {
		seq  int64
		evts []segment.Event
	}{
		{11, []segment.Event{{Kind: segment.KindCreate, UpstreamRelayCursor: 11, DID: string(did), Collection: "app.bsky.feed.post", Rkey: "old41", Rev: rev41, Payload: testRecordCBOR(t, "41")}}},
		{11, []segment.Event{{Kind: segment.KindCreate, UpstreamRelayCursor: 11, DID: string(did), Collection: "app.bsky.feed.post", Rkey: "old41", Rev: rev41, Payload: testRecordCBOR(t, "41-dup")}}},
		{12, []segment.Event{{Kind: segment.KindCreate, UpstreamRelayCursor: 12, DID: string(did), Collection: "app.bsky.feed.post", Rkey: "old42", Rev: rev42, Payload: testRecordCBOR(t, "42")}}},
		{13, []segment.Event{{Kind: segment.KindAccount, UpstreamRelayCursor: 13, DID: string(did), Payload: acctPayload}}},
		{14, []segment.Event{{Kind: segment.KindIdentity, UpstreamRelayCursor: 14, DID: string(did), Payload: identPayload}}},
		{15, []segment.Event{
			{Kind: segment.KindDelete, UpstreamRelayCursor: 15, DID: string(did), Collection: "app.bsky.feed.post", Rkey: "rec1", Rev: rev43},
			{Kind: segment.KindCreate, UpstreamRelayCursor: 15, DID: string(did), Collection: "app.bsky.feed.post", Rkey: "rec3", Rev: rev43, Payload: testRecordCBOR(t, "new43")},
		}},
	} {
		h, err := rep.InterceptLiveGroup(tc.seq, tc.evts)
		require.NoError(t, err)
		require.True(t, h)
	}

	require.Equal(t, 5.0, testutil.ToFloat64(metrics.InboxCaptured))
	require.Equal(t, 1.0, testutil.ToFloat64(metrics.InboxDeduped))

	// Publish snapshot at rev42 containing rec1 and rec2.
	rp, commit := makeSignedRepo(t, did, rev42, map[string]string{
		"app.bsky.feed.post/rec1": "from-snapshot-1",
		"app.bsky.feed.post/rec2": "from-snapshot-2",
	})
	require.NoError(t, handle.PublishRepo(ctx, rp, commit))
	require.False(t, rep.IsGated(did), "gate must reopen after inbox drains")
	require.Zero(t, stage.Count(), "staged artifact must be cleaned up after completion")

	evts := rec.snapshot()
	// Expect:
	//  0: KindSync (rev42)
	//  1..2: KindCreateResync (rec1, rec2)
	//  3: KindAccount (seq 13)
	//  4: KindIdentity (seq 14)
	//  5: KindDelete (rec1, rev43)
	//  6: KindCreate (rec3, rev43)
	require.Len(t, evts, 7)
	require.Equal(t, segment.KindSync, evts[0].Kind)
	require.Equal(t, rev42, evts[0].Rev)
	require.Equal(t, segment.KindCreateResync, evts[1].Kind)
	require.Equal(t, segment.KindCreateResync, evts[2].Kind)
	require.Equal(t, segment.KindAccount, evts[3].Kind)
	require.Equal(t, int64(13), evts[3].UpstreamRelayCursor)
	require.Equal(t, segment.KindIdentity, evts[4].Kind)
	require.Equal(t, int64(14), evts[4].UpstreamRelayCursor)
	require.Equal(t, segment.KindDelete, evts[5].Kind)
	require.Equal(t, "rec1", evts[5].Rkey)
	require.Equal(t, segment.KindCreate, evts[6].Kind)
	require.Equal(t, "rec3", evts[6].Rkey)
	require.Equal(t, 2.0, testutil.ToFloat64(metrics.InboxSkipped))
	require.Equal(t, 3.0, testutil.ToFloat64(metrics.InboxDrained))

	// SyncStateStore must now hold durable rev43 and hosting seq 13.
	appliedChain, err := syncstate.New(store).LoadAppliedChain(ctx, did)
	require.NoError(t, err)
	require.NotNil(t, appliedChain)
	require.Equal(t, rev43, appliedChain.Rev)
	appliedHost, err := syncstate.New(store).LoadAppliedHosting(ctx, did)
	require.NoError(t, err)
	require.NotNil(t, appliedHost)
	require.Equal(t, int64(13), appliedHost.Seq)

	// Completed cutoff (rev42) drops late older repo commits, allows newer ones
	// and allows account/identity control events.
	staleHandled, err := rep.InterceptLiveGroup(20, []segment.Event{{
		Kind: segment.KindCreate, DID: string(did), Collection: "app.bsky.feed.post", Rkey: "late", Rev: rev41, Payload: testRecordCBOR(t, "stale"),
	}})
	require.NoError(t, err)
	require.True(t, staleHandled, "stale rev <= cutoff must be dropped")
	require.Equal(t, 1.0, testutil.ToFloat64(metrics.CutoffDropped))

	freshHandled, err := rep.InterceptLiveGroup(21, []segment.Event{{
		Kind: segment.KindCreate, DID: string(did), Collection: "app.bsky.feed.post", Rkey: "fresh", Rev: rev43, Payload: testRecordCBOR(t, "fresh"),
	}})
	require.NoError(t, err)
	require.False(t, freshHandled, "rev > cutoff must flow normally to ClassLive")
}

func TestCoordinator_CrashResumeMidPublishDoesNotReEmitKindSync(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store := memstore.New()
	stageFS := vfs.NewMem()
	stage := repair.NewFSStageStore(stageFS, "/repair")
	segFS := vfs.NewMem()
	segDir := filepath.Join("/", "segments")

	did := atmos.DID("did:plc:crash0000000000000000000")
	rev42 := "3l3qo2vutsw2c"
	rev43 := "3l3qo2vutsw2d"

	// Build a 10-record repo (11 total staged rows including KindSync).
	records := make(map[string]string, 10)
	for i := range 10 {
		records[fmt.Sprintf("app.bsky.feed.post/rk%02d", i)] = fmt.Sprintf("body-%d", i)
	}
	rp, commit := makeSignedRepo(t, did, rev42, records)

	// Session 1: crash immediately after the first 3-row chunk commits
	// (KindSync + 2 KindCreateResync rows).
	errCrash := errors.New("simulated crash after first chunk")
	var session1Rec *recordedStream
	{
		rep1, ss1, w1, _, rec1 := openLocalRepairStack(t, store, stage, segFS, segDir, 3,
			func(_ context.Context, _ atmos.DID, nextRow, totalRows int64) error {
				require.Equal(t, int64(3), nextRow)
				require.Equal(t, int64(11), totalRows)
				return errCrash
			}, nil)
		session1Rec = rec1

		handle, err := rep1.BeginFailedRetry(ctx, did)
		require.NoError(t, err)

		// Capture a post-snapshot live delete into the durable inbox BEFORE crash.
		fakeCID := cbor.ComputeCID(cbor.CodecDagCBOR, []byte{0xa0})
		require.NoError(t, ss1.SaveChain(ctx, did, atmossync.ChainState{Rev: rev43, Data: fakeCID}))
		handled, err := rep1.InterceptLiveGroup(99, []segment.Event{{
			Kind:                segment.KindDelete,
			UpstreamRelayCursor: 99,
			DID:                 string(did),
			Collection:          "app.bsky.feed.post",
			Rkey:                "rk05",
			Rev:                 rev43,
		}})
		require.NoError(t, err)
		require.True(t, handled)

		err = handle.PublishRepo(ctx, rp, commit)
		require.ErrorIs(t, err, errCrash)
		require.NoError(t, w1.Close())
	}

	// Session 2: brand-new Coordinator + StateStore + Writer over the SAME
	// store, stage, and segments.
	rep2, ss2, w2, metrics2, rec2 := openLocalRepairStack(t, store, stage, segFS, segDir, 3, nil, nil)
	defer func() { _ = w2.Close() }()

	require.NoError(t, rep2.Recover(ctx))
	require.True(t, rep2.IsGated(did), "gate must be restored before consumer starts")
	require.Equal(t, 1.0, testutil.ToFloat64(metrics2.JobsResumed))

	// Pending verifier checkpoint from durable inbox must be restored for the
	// verifier WITHOUT being promoted to applied state yet.
	restoredPending, err := ss2.LoadChain(ctx, did)
	require.NoError(t, err)
	require.NotNil(t, restoredPending)
	require.Equal(t, rev43, restoredPending.Rev)
	restoredApplied, err := ss2.LoadAppliedChain(ctx, did)
	require.NoError(t, err)
	require.Nil(t, restoredApplied, "restored inbox checkpoint must stay pending until drained")

	// Run session 2 worker until idle.
	runCtx, cancel := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- rep2.Run(runCtx) }()

	require.NoError(t, rep2.WaitIdle(ctx))
	cancel()
	require.ErrorIs(t, <-runDone, context.Canceled)

	require.False(t, rep2.IsGated(did))

	// Combine rows appended across both sessions and verify:
	//  - Exactly 1 KindSync row total (never re-emitted on resume!)
	//  - Exactly 10 KindCreateResync rows (no restart from row 0!)
	//  - Followed by the drained rev43 KindDelete of rk05!
	all := append(session1Rec.snapshot(), rec2.snapshot()...)
	require.Len(t, all, 12)

	syncCount := 0
	resyncCount := 0
	for i, ev := range all {
		switch ev.Kind {
		case segment.KindSync:
			syncCount++
			require.Equal(t, 0, i, "KindSync must only appear at index 0")
		case segment.KindCreateResync:
			resyncCount++
		}
	}
	require.Equal(t, 1, syncCount, "resume must not re-emit KindSync")
	require.Equal(t, 10, resyncCount, "resume must continue from committed NextRow without duplicating rows")
	require.Equal(t, segment.KindDelete, all[len(all)-1].Kind)
	require.Equal(t, "rk05", all[len(all)-1].Rkey)
	require.Equal(t, rev43, all[len(all)-1].Rev)

	// After drain completes, rev43 is now durably applied.
	finalApplied, err := syncstate.New(store).LoadAppliedChain(ctx, did)
	require.NoError(t, err)
	require.NotNil(t, finalApplied)
	require.Equal(t, rev43, finalApplied.Rev)
}

func TestCoordinator_FailedRetryAbortStaleSnapshotAndConcurrency(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store := memstore.New()
	stage := repair.NewMemStageStore()
	segFS := vfs.NewMem()
	rep, ss, w, _, rec := openLocalRepairStack(t, store, stage, segFS, "/segments", 4, nil, nil)
	defer func() { _ = w.Close() }()

	did := atmos.DID("did:plc:retry0000000000000000000")
	rev40 := "3l3qo2vutsw2a"
	rev41 := "3l3qo2vutsw2b"
	rev42 := "3l3qo2vutsw2c"

	// 1. Concurrent BeginFailedRetry on an already-gated DID returns ErrAlreadyGated.
	h1, err := rep.BeginFailedRetry(ctx, did)
	require.NoError(t, err)
	_, err = rep.BeginFailedRetry(ctx, did)
	require.ErrorIs(t, err, repair.ErrAlreadyGated)

	// Capture a live event while h1 is downloading, then abort h1 (download failed).
	handled, err := rep.InterceptLiveGroup(5, []segment.Event{{
		Kind:                segment.KindCreate,
		UpstreamRelayCursor: 5,
		DID:                 string(did),
		Collection:          "app.bsky.feed.post",
		Rkey:                "during-failed-dl",
		Rev:                 rev41,
		Payload:             testRecordCBOR(t, "survives-abort"),
	}})
	require.NoError(t, err)
	require.True(t, handled)
	require.NoError(t, h1.Abort(ctx))
	require.False(t, rep.IsGated(did))

	// The buffered live event must replay cleanly with ZERO KindSync emitted.
	afterAbort := rec.snapshot()
	require.Len(t, afterAbort, 1)
	require.Equal(t, segment.KindCreate, afterAbort[0].Kind)
	require.Equal(t, "during-failed-dl", afterAbort[0].Rkey)

	// 2. Stale snapshot (commit.Rev < appliedRev) must be refused without KindSync.
	fakeCID := cbor.ComputeCID(cbor.CodecDagCBOR, []byte{0xa0})
	require.NoError(t, ss.SaveChain(ctx, did, atmossync.ChainState{Rev: rev42, Data: fakeCID}))
	ss.PromoteChain(did, rev42)
	require.NoError(t, w.DrainDurability(ctx))

	h2, err := rep.BeginFailedRetry(ctx, did)
	require.NoError(t, err)
	staleRP, staleCommit := makeSignedRepo(t, did, rev40, map[string]string{
		"app.bsky.feed.post/stale": "should-not-publish",
	})
	err = h2.PublishRepo(ctx, staleRP, staleCommit)
	require.ErrorIs(t, err, repair.ErrStaleSnapshot, "stale snapshot must fail with ErrStaleSnapshot so caller does not mark StatusComplete")
	require.False(t, rep.IsGated(did))
	require.Len(t, rec.snapshot(), 1, "stale snapshot must not emit KindSync or overwrite newer archive")
}

func TestCoordinator_EnqueueLiveResyncRunsOffHotPathAndDrainsInbox(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store := memstore.New()
	stage := repair.NewMemStageStore()
	segFS := vfs.NewMem()
	rep, _, w, metrics, rec := openLocalRepairStack(t, store, stage, segFS, "/segments", 4, nil, nil)
	defer func() { _ = w.Close() }()

	did := atmos.DID("did:plc:sync00000000000000000000")
	rev42 := "3l3qo2vutsw2c"
	rev43 := "3l3qo2vutsw2d"

	// Enqueue an empty #sync resync (KindSync row only) — must activate gate
	// immediately without writing synchronously to w.
	evt := streaming.Event{
		Seq:    100,
		Resync: streaming.ResyncSyncEvent,
		Sync: &comatproto.SyncSubscribeRepos_Sync{
			DID:  string(did),
			Rev:  rev42,
			Time: "2026-11-25T00:00:00Z",
		},
	}
	require.NoError(t, rep.EnqueueLiveResync(evt, time.Now().UnixMicro()))
	require.True(t, rep.IsGated(did))
	require.Empty(t, rec.snapshot(), "EnqueueLiveResync must not append synchronously on Consumer.Run")

	// Live commit arriving while queued goes to durable inbox.
	handled, err := rep.InterceptLiveGroup(101, []segment.Event{{
		Kind:                segment.KindCreate,
		UpstreamRelayCursor: 101,
		DID:                 string(did),
		Collection:          "app.bsky.feed.post",
		Rkey:                "after-sync",
		Rev:                 rev43,
		Payload:             testRecordCBOR(t, "after"),
	}})
	require.NoError(t, err)
	require.True(t, handled)

	runCtx, cancel := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- rep.Run(runCtx) }()

	require.NoError(t, rep.WaitIdle(ctx))
	cancel()
	require.ErrorIs(t, <-runDone, context.Canceled)

	evts := rec.snapshot()
	require.Len(t, evts, 2)
	require.Equal(t, segment.KindSync, evts[0].Kind)
	require.Equal(t, rev42, evts[0].Rev)
	require.Equal(t, segment.KindCreate, evts[1].Kind)
	require.Equal(t, "after-sync", evts[1].Rkey)
	require.Equal(t, 1.0, testutil.ToFloat64(metrics.JobsEnqueued.WithLabelValues(string(repair.SourceSyncEvent))))
	require.Equal(t, 1.0, testutil.ToFloat64(metrics.JobsCompleted.WithLabelValues("succeeded")))
}

// Regression test for roast finding R-d5505e: a live resync arriving while a
// failed-repo retry owns PhaseQueued must record FollowUpRev instead of racing
// the RetryHandle, and run cleanly once the retry aborts.
func TestCoordinator_LiveResyncDuringQueuedFailedRetryTriggersFollowUp(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store := memstore.New()
	stage := repair.NewMemStageStore()
	segFS := vfs.NewMem()

	did := atmos.DID("did:plc:followup0000000000000000")
	rev42 := "3l3qo2vutsw2c"

	var fetchCalled sync.WaitGroup
	fetchCalled.Add(1)
	var fetchOnce sync.Once
	rep, _, w, metrics, rec := openLocalRepairStack(t, store, stage, segFS, "/segments", 4, nil,
		func(_ context.Context, fetchDID atmos.DID) (*atmosrepo.Repo, *atmosrepo.Commit, error) {
			require.Equal(t, did, fetchDID)
			fetchOnce.Do(func() { fetchCalled.Done() })
			rp, commit := makeSignedRepo(t, did, rev42, map[string]string{
				"app.bsky.feed.post/from-followup": "recovered",
			})
			return rp, commit, nil
		},
	)
	defer func() { _ = w.Close() }()

	runCtx, cancel := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- rep.Run(runCtx) }()

	// Failed-repo retry opens a PhaseQueued gate.
	handle, err := rep.BeginFailedRetry(ctx, did)
	require.NoError(t, err)

	// While retry is still downloading (PhaseQueued), a live #sync resync arrives.
	evt := streaming.Event{
		Seq:    200,
		Resync: streaming.ResyncSyncEvent,
		Sync: &comatproto.SyncSubscribeRepos_Sync{
			DID:  string(did),
			Rev:  rev42,
			Time: "2026-11-25T00:00:00Z",
		},
	}
	require.NoError(t, rep.EnqueueLiveResync(evt, time.Now().UnixMicro()))

	// Retry download fails and aborts; the recorded FollowUpRev must trigger
	// a follow-up repair job via FetchRepo instead of dropping the resync.
	require.NoError(t, handle.Abort(ctx))

	fetchCalled.Wait()
	require.NoError(t, rep.WaitIdle(ctx))
	cancel()
	require.ErrorIs(t, <-runDone, context.Canceled)

	evts := rec.snapshot()
	require.Len(t, evts, 2)
	require.Equal(t, segment.KindSync, evts[0].Kind)
	require.Equal(t, rev42, evts[0].Rev)
	require.Equal(t, segment.KindCreateResync, evts[1].Kind)
	require.Equal(t, "from-followup", evts[1].Rkey)
	require.Equal(t, 1.0, testutil.ToFloat64(metrics.JobsCompleted.WithLabelValues("aborted")))
	require.Equal(t, 1.0, testutil.ToFloat64(metrics.JobsCompleted.WithLabelValues("succeeded")))
}
