package oracle

import (
	"context"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/live"
	"github.com/bluesky-social/jetstream/internal/ingest/syncstate"
	"github.com/bluesky-social/jetstream/internal/simulator/fanout"
	simhttp "github.com/bluesky-social/jetstream/internal/simulator/http"
	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/streaming"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// live_tail_harness_test.go is the shared fixture for oracle tiers that
// drive the REAL live consumer (real websocket, real atmos pipeline,
// pebble-backed verifier state) against the simulator relay with wire
// faults armed. The #205 replay tier (replay_fault_test.go) and the
// #206 frame-adversity tier (frame_fault_test.go) both build on it.
//
// Usage shape:
//
//	h := newLiveTailHarness(t, ctx)
//	...generate traffic on h.World...
//	...arm faults on h.Faults...
//	h.StartConsumer(t, ctx)
//	...wait for fault-fired / durable-row barriers...
//	h.StopConsumer(t)
//	events := h.ObservedEvents(t)
type liveTailHarness struct {
	World  *world.World
	Faults *simhttp.FaultPlan
	URL    string

	// Consumer-side fixtures, populated by StartConsumer.
	Metrics     *live.Metrics
	DropMetrics *ingest.DropMetrics
	Recorder    *eventLogRecorder

	verifier  *atmossync.Verifier
	dataDir   string
	consumer  *live.Consumer
	runCancel context.CancelFunc
	runErr    chan error
}

// newLiveTailHarness stands up world + simulator relay + verifier. The
// world has 2 accounts and NO bootstrap-seeded records: this harness
// archives the live tail only (no backfill), so ground truth must be
// derivable purely from firehose traffic.
func newLiveTailHarness(t *testing.T, ctx context.Context) *liveTailHarness {
	t.Helper()

	cfg := world.DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Accounts = 2
	cfg.InitialRecords = 0
	cfg.InitialRecordsMax = 0
	w, err := world.New(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	_, err = w.EnsureSeed()
	require.NoError(t, err)
	require.NoError(t, w.Bootstrap(ctx, slog.Default()))
	require.NoError(t, w.AttachRuntime(rand.New(rand.NewPCG(7, 11)), fanout.New(1024)))

	faults := simhttp.NewFaultPlan()
	srv := httptest.NewServer(nil)
	srv.Config.Handler = simhttp.NewHandlerWithOptions(w, srv.URL, simhttp.HandlerOptions{Faults: faults})
	t.Cleanup(srv.Close)

	st, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	stateStore := syncstate.New(st)

	directory := &identity.Directory{
		Resolver: &identity.DefaultResolver{
			PLCURL: gt.Some(srv.URL),
			// Plain client: jttp's SSRF protection blocks the loopback
			// httptest server by design.
			HTTPClient: gt.Some(http.DefaultClient),
		},
		SkipHandleVerification: true,
	}
	xc := &xrpc.Client{Host: srv.URL, HTTPClient: gt.Some(http.DefaultClient)}
	verifier, err := atmossync.NewVerifier(atmossync.VerifierOptions{
		Directory:  directory,
		StateStore: stateStore,
		SyncClient: gt.Some(atmossync.NewClient(atmossync.Options{Client: xc, Directory: gt.Some(directory)})),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = verifier.Close() })

	h := &liveTailHarness{
		World:    w,
		Faults:   faults,
		URL:      srv.URL,
		verifier: verifier,
		dataDir:  t.TempDir(),
	}
	h.openConsumer(t, st, stateStore)
	return h
}

func (h *liveTailHarness) openConsumer(t *testing.T, st *store.Store, stateStore *syncstate.PebbleStateStore) {
	t.Helper()
	reg := prometheus.NewRegistry()
	h.Metrics = live.NewMetrics(reg)
	h.DropMetrics = ingest.NewDropMetrics(reg)
	h.Recorder = newEventLogRecorder()
	consumer, err := live.Open(live.Config{
		SegmentsDir:    filepath.Join(h.dataDir, "segments"),
		Store:          st,
		SeqKey:         live.SteadySeqKey,
		CursorKey:      live.CursorKey,
		RelayURL:       h.URL,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:       h.verifier,
		SyncStateStore: stateStore,
		Metrics:        h.Metrics,
		DropMetrics:    h.DropMetrics,
		// Fast reconnect: fault scenarios that kill the connection
		// (oversized frames tripping the client read limit) must not pay
		// atmos's production 1s base delay.
		ReconnectBackoff: &streaming.BackoffPolicy{
			InitialDelay: gt.Some(time.Millisecond),
			MaxDelay:     gt.Some(time.Millisecond),
			Multiplier:   gt.Some(1.0),
			Jitter:       gt.Some(false),
		},
		OnEvent: func(ev *segment.Event) { h.Recorder.Observe(ev) },
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = consumer.Close() })
	h.consumer = consumer
}

// StartConsumer launches the consumer's Run loop. Call after traffic is
// generated and faults are armed, so the fresh cursor=0 subscription
// deterministically replays every pre-generated frame through the fault
// writer.
func (h *liveTailHarness) StartConsumer(t *testing.T, ctx context.Context) {
	t.Helper()
	require.Nil(t, h.runErr, "StartConsumer called twice")
	runCtx, cancel := context.WithCancel(ctx)
	h.runCancel = cancel
	h.runErr = make(chan error, 1)
	go func() { h.runErr <- h.consumer.Run(runCtx) }()
}

// StopConsumer cancels Run, waits for it to return, and closes the
// consumer so the active segment flushes durably before ObservedEvents.
func (h *liveTailHarness) StopConsumer(t *testing.T) {
	t.Helper()
	h.runCancel()
	select {
	case <-h.runErr:
	case <-time.After(10 * time.Second):
		t.Fatal("consumer.Run did not return after cancel")
	}
	require.NoError(t, h.consumer.Close())
}

// ObservedEvents scans the durable segment files. Call after StopConsumer.
func (h *liveTailHarness) ObservedEvents(t *testing.T) []ObservedEvent {
	t.Helper()
	events, err := ObserveSegments(h.dataDir)
	require.NoError(t, err)
	return events
}

// assertLiveTailConverged folds the durable stream and compares it to
// world ground truth — the harness-independent final-state check every
// live-tail fault scenario ends with.
func assertLiveTailConverged(t *testing.T, h *liveTailHarness, events []ObservedEvent, msg string) {
	t.Helper()
	require.NoError(t, CheckInvariants(events))
	got, err := Reconstruct(EventsSortedBySeq(events))
	require.NoError(t, err)
	want, err := GroundTruthFromWorld(h.World)
	require.NoError(t, err)
	require.NoError(t, Compare(want, got), msg)
}
