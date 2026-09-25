package oracle

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream"
	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/internal/jetstreamd/jetstreamdtest"
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
