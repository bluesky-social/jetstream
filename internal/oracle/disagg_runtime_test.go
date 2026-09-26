package oracle

import (
	"context"
	"encoding/binary"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream"
	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/internal/jetstreamd/jetstreamdtest"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/objstore/protocol"
	simhttp "github.com/bluesky-social/jetstream/internal/simulator/http"
	"github.com/bluesky-social/jetstream/internal/storagefake"
	"github.com/bluesky-social/jetstream/segment"
)

// TestDisagg_RuntimeSteadyState is the happy path of a disaggregated pod
// (plan S2.16): from a seeded steady-state catalog, the pod takes the lease,
// resumes the firehose at relay/cursor, commits hot batches, and serves the
// seeded archive plus the new live rows to the real client through its
// follower. The layer 3 oracle (S2.18) adds pods and faults on top.
func TestDisagg_RuntimeSteadyState(t *testing.T) {
	t.Parallel()
	runDisaggHappyPath(t)
}

func runDisaggHappyPath(t *testing.T) {
	const liveAfter = 24

	w := newRestartWorld(t, Config{
		Seed:              53,
		Accounts:          seedTestAccounts,
		MinInitialRecords: 2,
		MaxInitialRecords: 5,
		LiveEventsSteady:  seedTestLive + liveAfter,
	})
	t.Cleanup(func() { require.NoError(t, w.Close()) })

	fake := jetstreamdtest.New(storagefake.Config{
		Invariants:  catalog.InvariantOptions{MaxEventsPerBlock: seedTestBlock},
		OnViolation: func(rev uint64, err error) { t.Errorf("catalog invariant at revision %d: %v", rev, err) },
	})
	db := fake.DB
	lease := db.NewLease()
	require.NoError(t, lease.Acquire(t.Context(), time.Hour))
	up, err := protocol.NewUploader(protocol.UploaderConfig{Blob: fake.Blob, ArchiveID: db.Archive().ArchiveID, GCDelay: time.Hour, OrphanAge: time.Hour})
	require.NoError(t, err)
	seeded, err := SeedCatalog(t.Context(), SeedCatalogConfig{
		World:             w,
		LiveEvents:        seedTestLive,
		Session:           catalog.NewSession(catalog.SessionConfig{DB: db, Epoch: lease.Epoch()}),
		Uploader:          up,
		MaxEventsPerBlock: seedTestBlock,
		MaxSegmentBytes:   seedTestMaxSegment,
	})
	require.NoError(t, err)
	require.NoError(t, lease.Release(t.Context()))

	simLn := newPipeListener()
	simSrv := &http.Server{Handler: simhttp.NewHandlerWithOptions(w, disaggSimURL, simhttp.HandlerOptions{})}
	go func() { _ = simSrv.Serve(simLn) }()
	t.Cleanup(func() { _ = simSrv.Close() })

	publicLn := newPipeListener()
	sessions := make(chan uint64, 4)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	opts := disaggPodOptions(fake.Backend, publicLn, simLn.httpClient(), testWriter{t: t})
	opts.OnSessionStart = func(epoch uint64) { sessions <- epoch }
	rt, err := jetstreamd.Build(ctx, opts)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		require.NoError(t, rt.Close(closeCtx))
		require.NoError(t, <-done)
	})

	select {
	case epoch := <-sessions:
		require.Greater(t, epoch, lease.Epoch(), "the pod's session fences the seeder's")
	case err := <-done:
		t.Fatalf("runtime exited before its first session: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("no writer session started")
	}

	var added []segment.Event
	for range liveAfter {
		frame, err := w.GenerateOneForTest(ctx)
		require.NoError(t, err)
		evt, err := decodeOracleFirehoseFrame(frame)
		require.NoError(t, err)
		rows, err := expectedSegmentEventsFromFirehoseEvent(w, evt)
		require.NoError(t, err)
		added = append(added, rows...)
	}
	require.NotEmpty(t, added)
	want := seeded.NextSeq + uint64(len(added)) - 1

	ground, err := GroundTruthFromWorld(w)
	require.NoError(t, err)
	client, err := jetstream.Subscribe("http://jetstream.invalid",
		jetstream.WithHTTPClient(publicLn.httpClient()),
		jetstream.WithAfterSeq(0),
		jetstream.WithBatchSize(64),
	)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
	defer readCancel()
	var got []ObservedEvent
	for batch, err := range client.Events(readCtx) {
		require.NoError(t, err)
		for _, ev := range batch.Events() {
			got = append(got, observedEventFromClient(t, ev))
		}
		if len(got) > 0 && got[len(got)-1].Seq >= want {
			break
		}
	}
	require.Len(t, got, int(want), "every seeded and live row, once each")
	for i, ev := range got {
		require.Equal(t, uint64(i+1), ev.Seq, "seqs are dense from 1")
	}
	require.NoError(t, CheckInvariants(got))
	model, err := Reconstruct(got)
	require.NoError(t, err)
	require.NoError(t, Compare(ground, model))

	// The rows past the seed reached the catalog as hot batches, not
	// through any local file.
	snap, err := db.Snapshot()
	require.NoError(t, err)
	next, err := catalog.DecodeSeq(catalog.MainSeqKey, snap.Meta[catalog.MainSeqKey], true)
	require.NoError(t, err)
	require.Equal(t, want+1, next)
	require.NoError(t, db.Violation())
}

// TestDisagg_RuntimeLifecycle is the full lifecycle of a disaggregated pod
// (plan S3.2, S3.3): from an empty initialized catalog it backfills the
// simulator's repos and archives bootstrap-time live events in direct mode,
// merges bootstrap_live into main, and then commits steady-state hot
// batches. The client reads the whole archive back, and it must reconstruct
// the simulator's final state.
func TestDisagg_RuntimeLifecycle(t *testing.T) {
	t.Parallel()
	const (
		beforeStart   = 12 // superseded by backfill; merge's rev filter drops most
		beforeCutover = 16 // newer than every download; all survive merge
		steady        = 16
	)

	w := newRestartWorld(t, Config{
		Seed:                71,
		Accounts:            seedTestAccounts,
		MinInitialRecords:   2,
		MaxInitialRecords:   5,
		LiveEventsBootstrap: beforeStart + beforeCutover,
		LiveEventsSteady:    steady,
	})
	t.Cleanup(func() { require.NoError(t, w.Close()) })

	fake := jetstreamdtest.New(storagefake.Config{
		OnViolation: func(rev uint64, err error) { t.Errorf("catalog invariant at revision %d: %v", rev, err) },
	})
	db := fake.DB
	require.NoError(t, fake.InitNamespaces(t.Context()))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// generate emits n firehose events and returns the rows each should
	// archive.
	generate := func(ctx context.Context, n int) ([]disaggKey, error) {
		var keys []disaggKey
		for range n {
			frame, err := w.GenerateOneForTest(ctx)
			if err != nil {
				return nil, err
			}
			evt, err := decodeOracleFirehoseFrame(frame)
			if err != nil {
				return nil, err
			}
			rows, err := expectedSegmentEventsFromFirehoseEvent(w, evt)
			if err != nil {
				return nil, err
			}
			for _, ev := range rows {
				keys = append(keys, disaggKeyOf(observedFromSegment(ev)))
			}
		}
		return keys, nil
	}
	// The bootstrap-live consumer replays the firehose from seq 1.
	_, err := generate(ctx, beforeStart)
	require.NoError(t, err)

	simLn := newPipeListener()
	simSrv := &http.Server{Handler: simhttp.NewHandlerWithOptions(w, disaggSimURL, simhttp.HandlerOptions{})}
	go func() { _ = simSrv.Serve(simLn) }()
	t.Cleanup(func() { _ = simSrv.Close() })

	gate := newCutoverDeliveryGate(disaggSimURL, 0)
	var survivors []disaggKey
	merged := make(chan struct{})
	publicLn := newPipeListener()
	opts := disaggPodOptions(fake.Backend, publicLn, simLn.httpClient(), testWriter{t: t})
	opts.BackfillRetryBaseDelay = time.Millisecond
	opts.OnBootstrapLiveEvent = gate.observe
	opts.BarrierBeforeCutover = func(ctx context.Context) error {
		// Backfill is done, so these events are newer than every repo it
		// downloaded. Cutover waits until bootstrap_live holds all of them.
		var err error
		if survivors, err = generate(ctx, beforeCutover); err != nil {
			return err
		}
		tip := w.CurrentSeq()
		for !gate.contiguousToTip(tip) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Millisecond):
			}
		}
		return nil
	}
	opts.BarrierAfterMerge = func(context.Context) error {
		close(merged)
		return nil
	}
	rt, err := jetstreamd.Build(ctx, opts)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		require.NoError(t, rt.Close(closeCtx))
		require.NoError(t, <-done)
	})

	select {
	case <-merged:
	case err := <-done:
		t.Fatalf("runtime exited before merge finished: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("merge did not finish")
	}
	require.NotEmpty(t, survivors)

	// Merge's final transaction removed bootstrap_live and its keys, and
	// wrote the phase.
	snap, err := db.Snapshot()
	require.NoError(t, err)
	for _, row := range snap.Segments {
		require.NotEqual(t, catalog.BootstrapLive, row.Namespace, "bootstrap_live segment %d survived merge", row.Index)
	}
	mergedNext, err := catalog.DecodeSeq(catalog.MainSeqKey, snap.Meta[catalog.MainSeqKey], true)
	require.NoError(t, err)
	_, found := snap.Meta[catalog.BootstrapLiveSeqKey]
	require.False(t, found, "%s survived merge", catalog.BootstrapLiveSeqKey)
	meta := db.MetaStore(nil)
	phase, err := lifecycle.ReadPhase(ctx, meta)
	require.NoError(t, err)
	require.Equal(t, lifecycle.PhaseSteadyState, phase)
	_, found, err = metastore.GetUint64LE(ctx, meta, "merge/next_source_idx")
	require.NoError(t, err)
	require.False(t, found, "the merge cursor survived merge")

	steadyKeys, err := generate(ctx, steady)
	require.NoError(t, err)
	require.NotEmpty(t, steadyKeys)
	tip := w.CurrentSeq()
	var next uint64
	require.Eventually(t, func() bool {
		snap, err := db.Snapshot()
		require.NoError(t, err)
		cursor := snap.Meta[catalog.RelayCursorKey]
		if len(cursor) != 9 || int64(binary.LittleEndian.Uint64(cursor[1:])) < tip {
			return false
		}
		next, err = catalog.DecodeSeq(catalog.MainSeqKey, snap.Meta[catalog.MainSeqKey], true)
		require.NoError(t, err)
		return true
	}, 30*time.Second, 10*time.Millisecond, "the steady-state writer never committed relay cursor %d", tip)

	ground, err := GroundTruthFromWorld(w)
	require.NoError(t, err)
	client, err := jetstream.Subscribe("http://jetstream.invalid",
		jetstream.WithHTTPClient(publicLn.httpClient()),
		jetstream.WithAfterSeq(0),
		jetstream.WithBatchSize(64),
	)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	readCtx, readCancel := context.WithTimeout(ctx, 30*time.Second)
	defer readCancel()
	var got []ObservedEvent
	for batch, err := range client.Events(readCtx) {
		require.NoError(t, err)
		for _, ev := range batch.Events() {
			got = append(got, observedEventFromClient(t, ev))
		}
		if len(got) > 0 && got[len(got)-1].Seq >= next-1 {
			break
		}
	}
	require.Len(t, got, int(next-1), "every committed row, once each")
	for i, ev := range got {
		require.Equal(t, uint64(i+1), ev.Seq, "seqs are dense from 1")
	}
	require.NoError(t, CheckInvariants(got))
	model, err := Reconstruct(got)
	require.NoError(t, err)
	require.NoError(t, Compare(ground, model))

	archived := make(map[disaggKey]uint64, len(got))
	for _, ev := range got {
		k := disaggKeyOf(ev)
		k.Seq = 0 // expected rows have no seq yet
		archived[k] = ev.Seq
	}
	for _, k := range survivors {
		seq, ok := archived[k]
		require.True(t, ok, "bootstrap-live row %+v is not in the archive", k)
		require.Less(t, seq, mergedNext, "bootstrap-live row %+v reached main after merge, not through it", k)
	}
	for _, k := range steadyKeys {
		seq, ok := archived[k]
		require.True(t, ok, "steady-state row %+v is not in the archive", k)
		require.GreaterOrEqual(t, seq, mergedNext, "steady-state row %+v", k)
	}
	require.NoError(t, db.Violation())
}
