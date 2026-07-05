package oracle

import (
	"context"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest/live"
	"github.com/bluesky-social/jetstream/internal/ingest/syncstate"
	"github.com/bluesky-social/jetstream/internal/simulator/fanout"
	simhttp "github.com/bluesky-social/jetstream/internal/simulator/http"
	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/identity"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// replay_fault_test.go is the #205 oracle tier: relay seq duplicates and
// regressions. atmos's gap check is forward-only (seq > last+1), so a
// relay that re-delivers frames — a duplicate burst, or a whole window
// replayed by a relay restored from backup — sails through undetected.
// The archive contract under replay is:
//
//   - #commit/#sync replays are silently dropped by the verifier's
//     rev-replay protection (rev <= persisted chain rev);
//   - #account replays are dropped by the consumer's applied-hosting-seq
//     guard (#231) — without it a stale account-delete re-archives ABOVE
//     a later reactivate+recreate and every fold erases live records;
//   - therefore the durable stream is EXACTLY the once-per-frame
//     expansion of the world's firehose: zero storage bloat, structural
//     invariants hold, final state converges.
//
// Each scenario drives the REAL live consumer (real websocket, real atmos
// pipeline, pebble-backed verifier state) against the simulator relay
// with a replay fault armed, over a traffic shape that puts an
// account-delete + reactivate + recreate inside the replayed window —
// the shape that corrupts state if any protection regresses. Anti-vacuity:
// the fault must fire, frames must actually be re-delivered, and the #231
// guard must report drops (proving the account replay reached it).

// replayScenarioTraffic drives the world through the corrupting shape and
// returns the account-0 record rkey recreated after reactivation.
func replayScenarioTraffic(t *testing.T, ctx context.Context, w *world.World) {
	t.Helper()
	_, err := w.GenerateOneForTest(ctx)
	require.NoError(t, err)
	_, err = w.GenerateOneForTest(ctx)
	require.NoError(t, err)
	_, err = w.GenerateAccountDeleteForTest(ctx, 0)
	require.NoError(t, err)
	_, err = w.GenerateAccountReactivateForTest(ctx, 0)
	require.NoError(t, err)
	_, _, err = w.GenerateRecordOpForTest(ctx, 0, "create", "app.bsky.feed.post", "recreated1")
	require.NoError(t, err)
}

// runReplayFaultScenario stands up world + simulator relay + real live
// consumer, arms the given replay fault to fire after every real frame
// has been delivered, generates one post-replay commit to prove the
// stream continues, and returns the observed archive plus fixtures.
func runReplayFaultScenario(t *testing.T, fault simhttp.SubscribeReposReplayFault) (
	[]ObservedEvent, []EventLogRow, []EventLogRow, *world.World, *simhttp.FaultPlan, *live.Metrics,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	cfg := world.DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Accounts = 2
	// No bootstrap-seeded records: this harness archives the live tail
	// only (no backfill), so ground truth must be derivable purely from
	// firehose traffic.
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

	replayScenarioTraffic(t, ctx, w)
	preReplayTip := w.CurrentSeq()

	// Fire after the connection has delivered every pre-replay frame, so
	// the replayed window deterministically contains the account
	// lifecycle. AfterFrames counts frames on the connection (fresh
	// cursor=0 subscription => all of history).
	fault.AfterFrames = int(preReplayTip)
	faults.SetSubscribeReposReplaySchedule([]simhttp.SubscribeReposReplayFault{fault})

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

	dataDir := t.TempDir()
	metrics := live.NewMetrics(prometheus.NewRegistry())
	recorder := newEventLogRecorder()
	var mu sync.Mutex
	var appended int
	consumer, err := live.Open(live.Config{
		SegmentsDir:    filepath.Join(dataDir, "segments"),
		Store:          st,
		SeqKey:         live.SteadySeqKey,
		CursorKey:      live.CursorKey,
		RelayURL:       srv.URL,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:       verifier,
		SyncStateStore: stateStore,
		Metrics:        metrics,
		OnEvent: func(ev *segment.Event) {
			recorder.Observe(ev)
			mu.Lock()
			appended++
			mu.Unlock()
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = consumer.Close() })

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- consumer.Run(runCtx) }()

	// Wait for the replay fault to fire (all real frames delivered first
	// by construction), then generate one post-replay commit and wait for
	// its rows — a durable-append barrier proving the consumer processed
	// everything the relay sent, including the replayed window that
	// preceded it on the same ordered websocket.
	require.Eventually(t, func() bool {
		return faults.SubscribeReposReplaysFired() == 1
	}, 30*time.Second, 10*time.Millisecond, "replay fault never fired")

	_, err = w.GenerateOneForTest(ctx)
	require.NoError(t, err)
	finalTip := w.CurrentSeq()
	expected, err := ExpectedEventLogFromFirehose(w, 0, int(finalTip))
	require.NoError(t, err)
	require.True(t, recorder.waitForRowCount(ctx, 0, finalTip, len(expected)),
		"timed out waiting for %d expected durable rows", len(expected))

	runCancel()
	select {
	case <-runErr:
	case <-time.After(10 * time.Second):
		t.Fatal("consumer.Run did not return after cancel")
	}
	require.NoError(t, consumer.Close())

	events, err := ObserveSegments(dataDir)
	require.NoError(t, err)
	observed := recorder.RowsByUpstreamCursor(0, finalTip)
	return events, observed, expected, w, faults, metrics
}

// assertReplayScenarioContracts runs the #205 contract bundle over one
// replay scenario's results.
func assertReplayScenarioContracts(
	t *testing.T,
	events []ObservedEvent,
	observed, expected []EventLogRow,
	w *world.World,
	faults *simhttp.FaultPlan,
	metrics *live.Metrics,
	wantReplayedFrames int,
) {
	t.Helper()

	// Anti-vacuity: the fault fired and re-delivered real frames, and the
	// #231 consumer guard saw at least one replayed #account (proving the
	// dangerous frame class reached it rather than the run passing on an
	// account-free window).
	require.Equal(t, 1, faults.SubscribeReposReplaysFired(), "replay fault must fire exactly once")
	require.Equal(t, wantReplayedFrames, faults.SubscribeReposReplayedFrames(),
		"replay fault must re-deliver the exact scheduled window")
	require.GreaterOrEqual(t, testutil.ToFloat64(metrics.ReplayedAccountsDrop), 1.0,
		"the replayed window must exercise the #account replay guard")

	// Structural invariants on the physical stream: unique, strictly
	// increasing jetstream seqs, valid commit revs.
	require.NoError(t, CheckInvariants(events))

	// Storage bloat is not merely bounded — it is ZERO: the verifier
	// rev-replay-drops duplicate commits and the consumer drops replayed
	// account events, so the durable stream must be exactly the
	// once-per-frame expansion of the world's firehose.
	require.NoError(t, CompareEventLogMultiset(expected, observed),
		"durable rows must match the expected once-per-frame event log exactly (no replay bloat, no loss)")

	// Final state: folding the durable stream converges to world truth.
	// This is the assertion that catches the #231 corruption — a replayed
	// account-delete archived above the recreate erases it from the fold.
	got, err := Reconstruct(EventsSortedBySeq(events))
	require.NoError(t, err)
	want, err := GroundTruthFromWorld(w)
	require.NoError(t, err)
	require.NoError(t, Compare(want, got),
		"final state must converge to world ground truth under seq replay")
}

// TestOracle_RelaySeqDuplicates covers duplicate-N mode: the relay
// re-sends the last 3 frames (account-delete, reactivate, recreate)
// verbatim after delivering them once.
func TestOracle_RelaySeqDuplicates(t *testing.T) {
	t.Parallel()
	events, observed, expected, w, faults, metrics := runReplayFaultScenario(t,
		simhttp.SubscribeReposReplayFault{DuplicateLast: 3})
	assertReplayScenarioContracts(t, events, observed, expected, w, faults, metrics, 3)
}

// TestOracle_RelaySeqRegression covers regress-to-K mode: the relay
// replays the whole retained window above seq 2 — commits, the account
// lifecycle, everything — as a relay restored from backup would.
func TestOracle_RelaySeqRegression(t *testing.T) {
	t.Parallel()
	events, observed, expected, w, faults, metrics := runReplayFaultScenario(t,
		simhttp.SubscribeReposReplayFault{RegressToSeq: 2})
	// The pre-replay tip is 5 (2 commits + delete + reactivate + recreate),
	// so regressing to 2 re-delivers seqs 3..5.
	assertReplayScenarioContracts(t, events, observed, expected, w, faults, metrics, 3)
}
