package oracle

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/internal/simulator/fanout"
	simhttp "github.com/bluesky-social/jetstream/internal/simulator/http"
	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/bluesky-social/jetstream/internal/xrpcapi"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/streaming"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// TestOracle_DefaultLifecycle drives the full jetstreamd lifecycle (bootstrap
// -> merge -> steady -> compaction -> client-observer serving tiers ->
// shutdown) entirely inside a testing/synctest bubble with NO sockets and the
// fake clock: the upstream firehose is fed in-memory via LiveDial, all outbound
// HTTP (getRepo/listRepos/PLC) is served in-process via HTTPTransport, and the
// runtime's public surface (which the observer tier consumes) is served over a
// pipe-backed listener. CI runs exactly this — the test you run locally is the
// test CI runs, free of wall-clock skew. One bubble per process (see the guard).
//
// nolint:paralleltest // synctest.Test forbids t.Parallel inside the bubble.
func TestOracle_DefaultLifecycle(t *testing.T) {
	if synctestBubbleUsed.Swap(true) {
		t.Skip("oracle synctest tier must run one bubble per process; " +
			"re-run as a separate `go test` invocation, not -count>1")
	}
	synctest.Test(t, testOracleDefaultLifecycle)
}

func testOracleDefaultLifecycle(t *testing.T) {
	cfg, err := defaultLifecycleConfig(os.LookupEnv, testing.Short())
	require.NoError(t, err)
	if cfg.Mode == "stress" && testing.Short() {
		t.Skip("skipping stress oracle under -short")
	}

	// The synctest fake clock starts at 2000-01-01 UTC, but the simulator
	// stamps commit revs at its logical-clock epoch (~2023). atmos's verifier
	// rejects a rev >5m in the future, so advance the bubble clock past the
	// epoch before any event flows. See advanceClockToSimulatorEpoch.
	advanceClockToSimulatorEpoch()

	// Pace bulk event generation so the in-process consumer keeps up under the
	// fake clock (see generateN). Cleared on return so it never leaks to a
	// non-bubble tier.
	bubbleDrain = synctest.Wait
	defer func() { bubbleDrain = nil }()

	trace, tracePath, closeTrace := newOracleTrace(t, "oracle-trace.jsonl")
	defer closeTrace()
	recordTraceOrError(t, trace, "run_start", map[string]any{
		"mode":                  cfg.Mode,
		"seed":                  cfg.Seed,
		"go_version":            runtime.Version(),
		"gomaxprocs":            runtime.GOMAXPROCS(0),
		"accounts":              cfg.Accounts,
		"min_initial_records":   cfg.MinInitialRecords,
		"max_initial_records":   cfg.MaxInitialRecords,
		"live_events_bootstrap": cfg.LiveEventsBootstrap,
		"live_events_steady":    cfg.LiveEventsSteady,
		"fault_mode":            cfg.FaultMode,
	})

	simCfg := world.DefaultConfig()
	simCfg.DataDir = t.TempDir()
	simCfg.Seed = cfg.Seed
	simCfg.Accounts = cfg.Accounts
	simCfg.InitialRecords = 0
	simCfg.InitialRecordsMin = cfg.MinInitialRecords
	simCfg.InitialRecordsMax = cfg.MaxInitialRecords
	simCfg.FirehoseHistory = max(10_000, cfg.LiveEventsBootstrap+cfg.LiveEventsSteady+1024)
	recordTraceOrError(t, trace, "simulator_config", map[string]any{
		"seed":                simCfg.Seed,
		"accounts":            simCfg.Accounts,
		"initial_records":     simCfg.InitialRecords,
		"initial_records_min": simCfg.InitialRecordsMin,
		"initial_records_max": simCfg.InitialRecordsMax,
		"firehose_history":    simCfg.FirehoseHistory,
	})

	w, err := world.New(t.Context(), simCfg)
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()
	_, err = w.EnsureSeed()
	require.NoError(t, err)
	require.NoError(t, w.Bootstrap(t.Context(), slog.Default()))
	// Size the firehose fanout buffer to comfortably exceed the run's total
	// event volume. The simulator fanout drops frames on a full per-subscriber
	// buffer (it models a lossy relay), and in the bubble a dropped frame is
	// lost for good — the in-process consumer never reconnects+replays the way
	// a real socket disconnect would, so a drop silently breaks the exact-count
	// acks. A closed-system test has a known bound; size to it and assert zero
	// drops at shutdown (below) so any future overflow fails loud, not silent.
	fanoutBuf := max(8192, simCfg.FirehoseHistory*2)
	fan := fanout.New(fanoutBuf)
	require.NoError(t, w.AttachRuntime(
		rand.New(rand.NewPCG(cfg.Seed^0xfeedf00d, cfg.Seed^0xc0ffee)),
		fan,
	))

	faultPlan, err := BuildSwarmFaultPlan(w, cfg)
	require.NoError(t, err)
	// Fail loud at plan construction if the swarm ever schedules more
	// retry-consuming faults for a DID than the backfill engine's attempt
	// budget allows (#109): a budget-exceeding plan would turn a faulted
	// repo into a confusing backfill timeout instead of a clear failure,
	// and silently diverge the durable model from the simulator world.
	require.NoErrorf(t, faultPlan.CheckWithinRetryBudget(),
		"swarm fault plan must stay within the backfill retry budget: mode=%s seed=%d", cfg.Mode, cfg.Seed)
	recordTraceOrError(t, trace, "fault_plan", map[string]any{
		"scheduled_get_repo_http_failures":           faultPlan.TotalGetRepoHTTPFailures(),
		"scheduled_get_repo_http_failure_dids":       len(faultPlan.GetRepoHTTPFailures),
		"scheduled_get_repo_car_truncations":         faultPlan.TotalGetRepoCARTruncations(),
		"scheduled_get_repo_car_truncation_dids":     len(faultPlan.GetRepoCARTruncations),
		"subscribe_repos_disconnect_threshold_count": len(faultPlan.SubscribeReposDisconnectThresholds),
	})

	// Serve the simulator in-process over a pipe-backed listener (no socket).
	// A real http.Server is used (not a ResponseRecorder RoundTripper) so CAR
	// streaming and the getRepo CAR-truncation fault — a mid-stream connection
	// reset — keep their real wire behavior. simURL is synthetic; the pipe
	// client routes by connection, not host.
	const simURL = "http://sim.invalid"
	simLn := newPipeListener()
	simSrv := &http.Server{Handler: simhttp.NewHandlerWithOptions(w, simURL, simhttp.HandlerOptions{
		Faults: faultPlan.SimulatorFaults,
	})}
	simServeDone := make(chan struct{})
	go func() {
		defer close(simServeDone)
		_ = simSrv.Serve(simLn)
	}()
	simClient := simLn.httpClient()

	dataDir := t.TempDir()
	afterBootstrap := newPhaseGate()
	afterMerge := newPhaseGate()
	bootstrapAck := newSeqAck()
	steadyAck := newSeqAck()
	asyncResyncAck := newSyncTombstoneAck()
	compaction := newCompactionPassRecorder()
	overDrop := newCompactionOverDropRecorder(dataDir)
	bootstrapEventLog := newEventLogRecorder()
	steadyEventLog := newEventLogRecorder()
	ctx, cancel := context.WithCancel(t.Context())
	emittedBootstrapAccountDelete := false
	emittedBootstrapIdentity := false
	bootstrapTraffic := newBootstrapTrafficGenerator(cfg.Accounts, cfg.LiveEventsBootstrap, bootstrapAck, oracleWaitTimeout(cfg), func(ctx context.Context) (int64, error) {
		if !emittedBootstrapAccountDelete {
			emittedBootstrapAccountDelete = true
			_, err := w.GenerateAccountDeleteForTest(ctx, 0)
			if err != nil {
				return 0, err
			}
			return w.CurrentSeq(), nil
		}
		if !emittedBootstrapIdentity {
			// One deterministic polite #identity during bootstrap-live, so
			// the bootstrap tier archives the kind regardless of what the
			// random mix draws (#202 anti-vacuity is injection-keyed).
			// Account 1, not 0: the injection above deleted account 0, and
			// this closure runs off the test goroutine so it must not call
			// pickActiveOracleAccount (t.Fatal). Every oracle mode has >= 4
			// accounts and only account 0 is ever deleted here.
			emittedBootstrapIdentity = true
			_, err := w.GenerateIdentityForTest(ctx, 1, false)
			if err != nil {
				return 0, err
			}
			return w.CurrentSeq(), nil
		}
		_, err := w.GenerateOneForTest(ctx)
		if err != nil {
			return 0, err
		}
		return w.CurrentSeq(), nil
	})
	liveReconnectBackoff := &streaming.BackoffPolicy{
		InitialDelay: gt.Some(time.Millisecond),
		MaxDelay:     gt.Some(time.Millisecond),
		Multiplier:   gt.Some(1.0),
		Jitter:       gt.Some(false),
	}

	// The runtime's public surface is served over a pipe-backed listener; the
	// observer tier (client backfill, typed backfill) reaches it through this
	// listener's in-process client. Debug listener likewise — its client is
	// how the harness scrapes /metrics (first metric-based oracle assert,
	// #202's enqueuer gate).
	runtimePublicLn := newPipeListener()
	runtimeDebugLn := newPipeListener()
	obsClient := runtimePublicLn.httpClient()
	debugClient := runtimeDebugLn.httpClient()

	rt, err := jetstreamd.Build(ctx, jetstreamd.Options{
		DataDir:            dataDir,
		RelayURL:           simURL,
		PLCURL:             simURL,
		OTelServiceName:    "jetstream-oracle",
		LogLevel:           "warn",
		LogFormat:          "text",
		LogOutput:          testWriter{t: t},
		ShutdownTimeout:    5 * time.Second,
		ClientDrainTimeout: time.Second,
		// In-process transport for the full bubble: pipe listeners for the
		// runtime's public/debug servers, in-memory firehose dial, and an
		// HTTP transport that serves the simulator with no socket.
		PublicListener: runtimePublicLn,
		DebugListener:  runtimeDebugLn,
		LiveDial:       subscribeReposDial(simClient),
		HTTPTransport:  simClient.Transport,
		// Keep injected transient getRepo 503s fast: a sub-millisecond
		// retry backoff means each fault adds microseconds, not atmos's
		// 1s production base delay, so the swarm sweep stays inside its
		// per-seed timeout budget even at stress scale.
		BackfillRetryBaseDelay: time.Millisecond,
		LiveReconnectBackoff:   liveReconnectBackoff,
		// Activate the net-new DID enqueuer (issue #188 wiring) inside the
		// bubble so the malformed-DID #identity injection exercises the
		// LiveEnqueuer's validation gate end-to-end (#202). The retry scan
		// itself is a no-op between scans under the fake clock (nothing
		// pending), so the only live machinery this adds per event is the
		// enqueuer's lock-free Observe.
		FailedRepoRetryInterval:   time.Hour,
		CursorLookback:            36 * time.Hour,
		SegmentCacheMaxAge:        0,
		PlanMaxDIDs:               xrpcapi.DefaultPlanMaxDIDs,
		PlanMaxCollections:        xrpcapi.DefaultPlanMaxCollections,
		PlanMaxEntries:            xrpcapi.DefaultPlanMaxEntries,
		PlanWholeSegmentThreshold: xrpcapi.DefaultPlanWholeSegmentThreshold,
		SubscribeHotTailBytes:     16 << 20,
		SubscribeBlockCacheBytes:  16 << 20,
		SubscribeReadBatch:        1024,
		SubscribeSlowWindow:       time.Second,
		SubscribeSlowMinRate:      1,
		CursorBlockIndexCacheSize: 32,
		CompactionInterval:        time.Hour,
		CompactionTombstoneCap:    1,
		BarrierBeforeCutover:      bootstrapTraffic.WaitDelivered,
		BarrierAfterBootstrap:     afterBootstrap.Barrier,
		BarrierAfterMerge:         afterMerge.Barrier,
		OnBeforeCompactionPass: func(targetWatermark uint64) {
			compaction.ObserveStart()
			overDrop.ObserveBefore(targetWatermark)
		},
		OnCompactionPass: func(result jetstreamd.CompactionPassResult) {
			// Pair the post-rewrite snapshot with the pre-rewrite one before
			// recording the pass, so the over-drop check sees a consistent
			// before/after for this pass.
			overDrop.ObserveAfter(result)
			compaction.Observe(result)
			recordTraceOrError(t, trace, "compaction_pass", map[string]any{
				"watermark": result.Watermark,
				"err":       traceErr(result.Err),
			})
		},
		OnBootstrapLiveEvent: func(ev *segment.Event) {
			bootstrapAck.Observe(ev)
			bootstrapEventLog.Observe(ev)
			recordTraceOrError(t, trace, "bootstrap_live_event", traceSegmentEvent(ev))
		},
		OnSteadyStateEvent: func(ev *segment.Event) {
			steadyAck.Observe(ev)
			asyncResyncAck.Observe(ev)
			steadyEventLog.Observe(ev)
			recordTraceOrError(t, trace, "steady_state_event", traceSegmentEvent(ev))
		},
		AfterRepoComplete: func(ctx context.Context, did atmos.DID) error {
			err := bootstrapTraffic.AfterRepoComplete(ctx, did)
			completed, generated := bootstrapTraffic.Snapshot()
			recordTraceOrError(t, trace, "backfill_repo_complete", map[string]any{
				"did":       string(did),
				"completed": completed,
				"generated": generated,
				"target":    cfg.LiveEventsBootstrap,
				"err":       traceErr(err),
			})
			return err
		},
	})
	require.NoError(t, err)

	run := &runtimeRun{exited: make(chan struct{})}
	go func() {
		run.err = rt.Run(ctx)
		close(run.exited)
	}()

	// Bootstrap traffic runs on its own goroutine (see bootstrapTrafficGenerator
	// for why it must NOT run under the backfill writer lock). It blocks on the
	// start signal until the first repo completes, then paces generation against
	// bootstrapAck off-lock. It exits on ctx cancel; we drain it before the
	// bubble fn returns so no goroutine is left blocked.
	bootstrapTrafficDone := make(chan struct{})
	go func() {
		defer close(bootstrapTrafficDone)
		if err := bootstrapTraffic.Run(ctx); err != nil && ctx.Err() == nil {
			t.Errorf("bootstrap traffic generator: mode=%s seed=%d err=%v", cfg.Mode, cfg.Seed, err)
		}
	}()

	runDone := false
	defer func() {
		cancel()
		<-bootstrapTrafficDone
		if !runDone {
			waitForRuntimeExit(t, cfg, run)
			recordTraceOrError(t, trace, "runtime_exit", map[string]any{
				"phase": "cleanup",
				"err":   traceErr(run.err),
			})
			runDone = true
		}
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		require.NoError(t, rt.Close(closeCtx))
		// Reap pooled keep-alive connections on both pipe transports. Over
		// net.Pipe an idle HTTP/1.1 keep-alive conn parks its client readLoop/
		// writeLoop and server conn.serve in durably-blocking reads that never
		// exit on their own; without this the bubble fn returns with those
		// goroutines alive and synctest panics "blocked goroutines remain".
		obsClient.CloseIdleConnections()
		debugClient.CloseIdleConnections()
		simClient.CloseIdleConnections()
		// Tear down the in-process simulator server and drain its goroutine:
		// every bubble goroutine must exit before the bubble function returns.
		_ = simSrv.Shutdown(closeCtx)
		_ = simLn.Close()
		<-simServeDone
	}()

	recordTraceOrError(t, trace, "phase", map[string]any{"phase": "after-bootstrap", "marker": "wait_begin"})
	waitForBarrier(t, cfg, "after-bootstrap", afterBootstrap, run)
	recordTraceOrError(t, trace, "phase", map[string]any{"phase": "after-bootstrap", "marker": "barrier_reached"})
	recordGetRepoFaults(t, trace, "after-bootstrap", faultPlan)
	assertFaultPlanFired(t, cfg, faultPlan)
	bootstrapTargetSeq := w.CurrentSeq()
	assertFirehoseEventLogMatches(t, trace, w, bootstrapEventLog, 0, bootstrapTargetSeq, "after-bootstrap")
	assertNoPermanentCursorGap(t, bootstrapEventLog, 0, bootstrapTargetSeq, cfg, "after-bootstrap")
	assertBootstrapOracleMatches(t, dataDir, w, cfg)
	recordTraceOrError(t, trace, "phase", map[string]any{"phase": "after-bootstrap", "marker": "release"})
	afterBootstrap.Release()
	recordTraceOrError(t, trace, "phase", map[string]any{"phase": "after-bootstrap", "marker": "after_release"})

	recordTraceOrError(t, trace, "phase", map[string]any{"phase": "after-merge", "marker": "wait_begin"})
	waitForBarrier(t, cfg, "after-merge", afterMerge, run)
	recordTraceOrError(t, trace, "phase", map[string]any{"phase": "after-merge", "marker": "barrier_reached"})
	assertOracleMatches(t, dataDir, w, cfg, "after-merge")
	afterMergeCompaction := compaction.Last(t)
	assertCompacted(t, dataDir, afterMergeCompaction.Watermark, cfg, "after-merge")
	faultPlan.ArmSubscribeReposDisconnects()
	recordTraceOrError(t, trace, "phase", map[string]any{"phase": "after-merge", "marker": "release"})
	afterMerge.Release()
	recordTraceOrError(t, trace, "phase", map[string]any{"phase": "after-merge", "marker": "after_release"})

	publicURL := waitForRuntimePublicURL(t, cfg, rt, run)
	passesBeforeSteady := compaction.Count()

	steadyStartSeq := w.CurrentSeq()
	generateN(t, w, cfg.LiveEventsSteady)
	// Deterministic #202 identity injections, independent of the random
	// mix: a handle-change payload (the optional-field shape) and a
	// malformed-DID frame (#identity bodies are not signature-verified
	// upstream, so this reaches ingest as-is; it must archive
	// byte-faithfully AND be rejected by the net-new enqueuer's DID
	// validation — asserted after shutdown below).
	identityIdx := pickActiveOracleAccount(t, w, cfg)
	_, err = w.GenerateIdentityForTest(t.Context(), identityIdx, true)
	require.NoError(t, err)
	_, err = w.GenerateMalformedIdentityForTest(t.Context())
	require.NoError(t, err)
	syncIdx := pickActiveOracleAccount(t, w, cfg)
	_, err = w.GenerateSilentMutationThenSyncForTest(t.Context(), syncIdx)
	require.NoError(t, err)
	targetSeq := w.CurrentSeq()
	recordTraceOrError(t, trace, "steady_target", map[string]any{"target_seq": targetSeq})
	steadyAck.Wait(t, cfg, targetSeq, run, oracleWaitTimeout(cfg))
	assertFirehoseEventLogMatches(t, trace, w, steadyEventLog, steadyStartSeq, targetSeq, "steady-state")
	assertNoPermanentCursorGap(t, steadyEventLog, steadyStartSeq, targetSeq, cfg, "steady-state")
	assertIdentityArchived(t, cfg, bootstrapEventLog, steadyEventLog, identityDID(t, w, identityIdx))
	// The malformed-DID identity must be rejected by the net-new
	// enqueuer's validation gate (metric via the debug /metrics surface,
	// scraped while the runtime is still serving), and at least one VALID
	// DID must have been enqueued net-new — proving the gate rejects
	// selectively rather than rejecting everything (the two-sided
	// anti-vacuity pair).
	assertEnqueueInvalidDIDFired(t, cfg, debugClient)

	asyncIdx := pickActiveOracleAccount(t, w, cfg)
	_, err = w.GenerateSilentMutationThenCommitForTest(t.Context(), asyncIdx)
	require.NoError(t, err)
	asyncEntry, _, err := w.ListReposPage(asyncIdx, 1)
	require.NoError(t, err)
	require.Len(t, asyncEntry, 1)
	asyncResyncAck.Wait(t, cfg, string(asyncEntry[0].DID), asyncEntry[0].Rev, run, oracleWaitTimeout(cfg))
	steadyCompaction := compaction.WaitAfter(t, cfg, run, passesBeforeSteady, oracleWaitTimeout(cfg))
	require.Greaterf(t, steadyCompaction.Watermark, afterMergeCompaction.Watermark,
		"steady compaction watermark did not advance: mode=%s seed=%d after_merge_watermark=%d steady_watermark=%d",
		cfg.Mode, cfg.Seed, afterMergeCompaction.Watermark, steadyCompaction.Watermark)
	// Client-driven historical observation (#77): drive the REAL public client
	// through the full archive path (plan -> segment/block download -> live
	// cutover) and assert the compaction contract on what it replayed. This replaces the bespoke /subscribe?cursor=0 whole-archive
	// replay (the live-tail-transport misuse for sealed history #77 flagged,
	// and the leading cause of the served CheckCompacted flakiness that #94's
	// bisection was built to triage). It runs ALONGSIDE the direct segment
	// observers above (which stay as the storage tier that distinguishes a
	// server bug from a client bug). On a CheckCompacted failure we still run
	// #94's disk-vs-serving bisection to classify durable-defect vs.
	// serving/client artifact.
	assertClientBackfillCompacted(t, cfg, run, trace, obsClient, dataDir, w, compaction, publicURL, steadyCompaction.Watermark, "steady-state-client-backfill")

	// #146: drive the REAL client through the typed fast path (worker-parallel
	// decode into bsky.FeedLike) over the same sealed range and assert it decodes
	// likes cleanly and surfaces exactly the same like set as the map path.
	assertTypedLikeBackfill(t, cfg, run, obsClient, publicURL, steadyCompaction.Watermark)

	// steadyAck.Wait above guarantees every steady-state cursor up to
	// targetSeq has been durably appended (OnEvent fires post-Append), so
	// no event is still in flight when we cancel. The steady-state writer
	// buffers pending events until a block fills or shutdown closes the
	// consumer; this assertion verifies that graceful shutdown durably
	// flushes the generated live events.
	recordTraceOrError(t, trace, "shutdown_start", map[string]any{"phase": "steady-state-shutdown-flush"})
	cancel()
	waitForRuntimeExit(t, cfg, run)
	recordTraceOrError(t, trace, "runtime_exit", map[string]any{
		"phase": "steady-state-shutdown-flush",
		"err":   traceErr(run.err),
	})
	runDone = true
	recordTraceOrError(t, trace, "phase", map[string]any{"phase": "final-assertions", "marker": "begin"})
	// Anti-vacuity: the fanout models a lossy relay (drop-on-full per
	// subscriber, fanout.go Publish), and in the bubble a dropped frame is
	// lost for good — the in-process consumer never reconnects+replays the way
	// a real socket disconnect would, so a drop silently breaks the exact-count
	// acks rather than failing loud. We sized fanoutBuf (above) to exceed the
	// run's total volume precisely so drops stay at zero; assert that here so a
	// future overflow (or a buffer-sizing regression) fails loudly instead of
	// corrupting the reconstructed model. The runtime has exited, so the relay
	// handler's subscriber has detached and its drop count is folded into the
	// registry's lifetime accumulator (TotalDrops survives detach).
	require.Zerof(t, fan.TotalDrops(),
		"fanout dropped frames: mode=%s seed=%d drops=%d (a full per-subscriber buffer silently lost a frame; raise fanoutBuf or investigate a slow consumer)",
		cfg.Mode, cfg.Seed, fan.TotalDrops())
	recordSubscribeReposFaults(t, trace, "steady-state-shutdown-flush", faultPlan)
	assertSubscribeReposFaultPlanFired(t, cfg, faultPlan)
	assertOracleMatches(t, dataDir, w, cfg, "steady-state-shutdown-flush")
	assertCompacted(t, dataDir, compaction.Last(t).Watermark, cfg, "steady-state-shutdown-flush")
	// Compaction over-drop / data-loss check (#100): every compaction pass
	// must have preserved each row the documented filter says survives at its
	// watermark. The runtime has exited, so all passes are captured and none
	// can race this assertion.
	overDrop.Assert(t, cfg, trace, "steady-state-shutdown-flush")

	recordTraceOrError(t, trace, "phase", map[string]any{"phase": "final-assertions", "marker": "done"})
	assertTraceContainsKinds(t, tracePath,
		"run_start",
		"simulator_config",
		"fault_plan",
		"phase",
		"compaction_pass",
		"backfill_repo_complete",
		"faults_fired",
		"event_log_compare",
		"bootstrap_live_event",
		"steady_state_event",
		"client_backfill_start",
		"client_backfill_done",
		"steady_target",
		"shutdown_start",
		"runtime_exit",
		"compaction_over_drop_check",
	)
}

func defaultLifecycleConfig(lookupenv func(string) (string, bool), short bool) (Config, error) {
	if !short {
		return ParseConfigFromLookupEnv(lookupenv)
	}
	if _, ok := lookupenv(envOracleMode); ok {
		return ParseConfigFromLookupEnv(lookupenv)
	}
	return ParseConfigFromLookupEnv(func(key string) (string, bool) {
		if key == envOracleMode {
			return "fast", true
		}
		return lookupenv(key)
	})
}

// identityDID resolves the DID of the world account the harness injected
// its polite identity frames for.
func identityDID(t *testing.T, w *world.World, idx int) string {
	t.Helper()
	acct, err := w.LoadAccount(idx)
	require.NoError(t, err)
	return string(acct.DID)
}

// assertIdentityArchived is #202's archived-≥1-of-the-kind anti-vacuity
// bundle, keyed to the DETERMINISTIC injections rather than the random
// mix (a future swarm tier may draw identity weight 0; the injections
// are what this assert owns):
//   - bootstrap tier archived ≥1 KindIdentity row (the bootstrap
//     injection at minimum);
//   - the steady handle-change injection for wantDID archived with a
//     non-empty payload;
//   - the malformed-DID injection archived byte-faithfully (the archive
//     is faithful; the enqueuer — asserted separately — is the
//     validation boundary, per docs/README.md §4.4's drop contract
//     applying to spec-invalid REPO PATHS, not identity DIDs).
func assertIdentityArchived(t *testing.T, cfg Config, bootstrap, steady *eventLogRecorder, wantDID string) {
	t.Helper()

	countKind := func(r *eventLogRecorder) (total int, byDID map[string]int) {
		byDID = make(map[string]int)
		for _, ev := range r.snapshotEvents() {
			if ev.Kind == segment.KindIdentity {
				total++
				byDID[ev.DID]++
			}
		}
		return total, byDID
	}

	bootTotal, _ := countKind(bootstrap)
	require.GreaterOrEqualf(t, bootTotal, 1,
		"bootstrap tier archived no KindIdentity rows: mode=%s seed=%d (the deterministic bootstrap injection is gone or the ingest path dropped it)",
		cfg.Mode, cfg.Seed)

	steadyTotal, steadyByDID := countKind(steady)
	require.GreaterOrEqualf(t, steadyByDID[wantDID], 1,
		"steady handle-change identity injection for %s never archived: mode=%s seed=%d",
		wantDID, cfg.Mode, cfg.Seed)
	require.GreaterOrEqualf(t, steadyByDID[world.MalformedIdentityDID], 1,
		"malformed-DID identity injection never archived: mode=%s seed=%d (the archive must stay byte-faithful; validation happens at the enqueuer)",
		cfg.Mode, cfg.Seed)
	require.GreaterOrEqualf(t, steadyTotal, 2,
		"steady tier archived fewer KindIdentity rows than injected: mode=%s seed=%d", cfg.Mode, cfg.Seed)
}

// assertEnqueueInvalidDIDFired scrapes the runtime's debug /metrics
// through the in-bubble pipe listener and asserts the two-sided
// net-new enqueuer contract (#202): the malformed-DID identity tripped
// the validation gate (invalid ≥ 1), and valid DIDs passed it to the
// durable boundary (already-known ≥ 1 — every world DID has a repo row
// from bootstrap backfill, so a passing valid DID surfaces there, and
// a gate that rejected everything could never reach it).
func assertEnqueueInvalidDIDFired(t *testing.T, cfg Config, debugClient *http.Client) {
	t.Helper()

	metrics := scrapeDebugMetrics(t, cfg, debugClient,
		"jetstream_backfill_net_new_invalid_did_total",
		"jetstream_backfill_net_new_already_known_total",
	)
	require.GreaterOrEqualf(t, metrics["jetstream_backfill_net_new_invalid_did_total"], 1.0,
		"net-new enqueuer never rejected the malformed identity DID: mode=%s seed=%d (gate is dead or the malformed injection never reached Observe)",
		cfg.Mode, cfg.Seed)
	require.GreaterOrEqualf(t, metrics["jetstream_backfill_net_new_already_known_total"], 1.0,
		"net-new enqueuer never passed a valid DID to the durable boundary: mode=%s seed=%d (a gate rejecting everything would make the invalid-DID assert vacuous)",
		cfg.Mode, cfg.Seed)
}

// scrapeDebugMetrics fetches /metrics from the debug listener and
// returns the values of the requested un-labeled series. Metrics the
// scrape does not contain report as 0 (prometheus counters are only
// exposed after registration, which these are at Build time).
func scrapeDebugMetrics(t *testing.T, cfg Config, client *http.Client, names ...string) map[string]float64 {
	t.Helper()

	// Host is synthetic: the pipe transport routes by connection.
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://debug.invalid/metrics", nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoErrorf(t, err, "scrape debug /metrics: mode=%s seed=%d", cfg.Mode, cfg.Seed)
	defer func() { _ = resp.Body.Close() }()
	require.Equalf(t, http.StatusOK, resp.StatusCode, "debug /metrics status: mode=%s seed=%d", cfg.Mode, cfg.Seed)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	want := make(map[string]struct{}, len(names))
	for _, n := range names {
		want[n] = struct{}{}
	}
	out := make(map[string]float64, len(names))
	for line := range strings.Lines(string(body)) {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, valStr, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		if _, wanted := want[name]; !wanted {
			continue
		}
		val, err := strconv.ParseFloat(valStr, 64)
		require.NoErrorf(t, err, "parse metric %s value %q", name, valStr)
		out[name] = val
	}
	return out
}

// assertFaultPlanFired verifies the fault injection actually happened.
// In swarm mode it first requires the plan to be NON-empty: a zero-fault
// plan would make the "all faults fired" check below pass vacuously,
// silently hiding a config or planner regression that disabled
// injection. It then requires every scheduled getRepo HTTP fault and CAR
// truncation to have fired, which holds because backfill touches every DID at
// least once (per-DID download) and atmos's retry loop consumes each transient
// failure; the after-bootstrap barrier only releases after backfill has fully
// drained, so no scheduled fault is still pending when this runs.
func assertFaultPlanFired(t *testing.T, cfg Config, plan *SwarmFaultPlan) {
	t.Helper()

	if cfg.FaultMode == FaultModeSwarm {
		require.NotEmpty(t, plan.GetRepoHTTPFailures,
			"swarm mode must schedule at least one getRepo HTTP fault; empty plan means injection is disabled")
		require.NotEmpty(t, plan.GetRepoCARTruncations,
			"swarm mode must schedule at least one getRepo CAR truncation; empty plan means injection is disabled")
	}
	require.Empty(t, plan.UnfiredGetRepoHTTPFailures(), "configured getRepo HTTP faults must fire")
	require.Empty(t, plan.UnfiredGetRepoCARTruncations(), "configured getRepo CAR truncation faults must fire")
}

func assertSubscribeReposFaultPlanFired(t *testing.T, cfg Config, plan *SwarmFaultPlan) {
	t.Helper()

	if cfg.FaultMode != FaultModeSwarm {
		return
	}
	require.NotEmpty(t, plan.SubscribeReposDisconnectThresholds,
		"swarm mode must schedule subscribeRepos disconnect thresholds")
	require.GreaterOrEqual(t, plan.SimulatorFaults.SubscribeReposDisconnects(), 1,
		"configured subscribeRepos disconnect fault must fire")
	require.GreaterOrEqual(t, plan.SimulatorFaults.SubscribeReposConnections(), 2,
		"subscribeRepos disconnect must be followed by a reconnect")
}

func recordGetRepoFaults(t *testing.T, trace *Trace, phase string, plan *SwarmFaultPlan) {
	t.Helper()

	unfiredHTTP := plan.UnfiredGetRepoHTTPFailures()
	unfiredCAR := plan.UnfiredGetRepoCARTruncations()
	recordTraceOrError(t, trace, "faults_fired", map[string]any{
		"phase":                                phase,
		"scheduled_get_repo_http_failures":     plan.TotalGetRepoHTTPFailures(),
		"fired_get_repo_http_failures":         totalGetRepoHTTPFailuresFired(plan),
		"unfired_get_repo_http_failure_dids":   len(unfiredHTTP),
		"unfired_get_repo_http_failures":       totalIntMap(unfiredHTTP),
		"scheduled_get_repo_car_truncations":   plan.TotalGetRepoCARTruncations(),
		"fired_get_repo_car_truncations":       totalGetRepoCARTruncationsFired(plan),
		"unfired_get_repo_car_truncation_dids": len(unfiredCAR),
		"unfired_get_repo_car_truncations":     totalIntMap(unfiredCAR),
	})
}

func recordSubscribeReposFaults(t *testing.T, trace *Trace, phase string, plan *SwarmFaultPlan) {
	t.Helper()

	var thresholdCount, connections, disconnects int
	if plan != nil {
		thresholdCount = len(plan.SubscribeReposDisconnectThresholds)
	}
	if plan != nil && plan.SimulatorFaults != nil {
		connections = plan.SimulatorFaults.SubscribeReposConnections()
		disconnects = plan.SimulatorFaults.SubscribeReposDisconnects()
	}
	recordTraceOrError(t, trace, "faults_fired", map[string]any{
		"phase": phase,
		"subscribe_repos_disconnect_threshold_count": thresholdCount,
		"subscribe_repos_connections":                connections,
		"subscribe_repos_disconnects":                disconnects,
	})
}

func assertFirehoseEventLogMatches(
	t *testing.T,
	trace *Trace,
	w *world.World,
	observed *eventLogRecorder,
	cursor int64,
	target int64,
	phase string,
) {
	t.Helper()

	limit := int(target - cursor)
	require.Positivef(t, limit, "%s expected positive firehose comparison range cursor=%d target=%d", phase, cursor, target)
	want, err := ExpectedEventLogFromFirehose(w, cursor, limit)
	require.NoErrorf(t, err, "%s: build expected event log cursor=%d target=%d", phase, cursor, target)

	// The seqAck the caller waited on keys on the SET of upstream cursors, so it
	// fires as soon as the target cursor first appears. But a #sync divergence
	// fans one upstream cursor out into a KindSync tombstone plus N
	// KindCreateResync replacement rows that all share that cursor (see
	// internal/ingest/live/events.go convertSync), and the recorder is fed
	// asynchronously by the live callback. A one-shot snapshot can therefore
	// race ahead of the trailing replacement rows. Wait (signal-driven, not a
	// wall-clock poll) until the observed count reaches the deterministic
	// expected count (the world is quiescent across this comparison window),
	// then run the authoritative multiset compare. The wait is a deadlock
	// GUARD with a long deadline: a genuinely dropped row never reaches the
	// count and surfaces as a clear TIMEOUT (not a confusing multiset
	// mismatch from a short poll deadline racing a slow runner); a duplicate
	// or wrong row reaches/overshoots the count and is caught by the compare.
	// #27/#106 conversion.
	waitCtx, cancelWait := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancelWait()
	reached := observed.waitForRowCount(waitCtx, cursor, target, len(want))
	got := observed.RowsByUpstreamCursor(cursor, target)
	require.Truef(t, reached,
		"%s: timed out waiting for firehose event log cursor=%d target=%d expected=%d observed=%d (a dropped row never reaches the expected count)",
		phase, cursor, target, len(want), len(got))
	err = CompareEventLogMultiset(want, got)
	recordTraceOrError(t, trace, "event_log_compare", map[string]any{
		"phase":          phase,
		"cursor":         cursor,
		"target_seq":     target,
		"expected_count": len(want),
		"observed_count": len(got),
		"err":            traceErr(err),
	})
	require.NoErrorf(t, err, "%s: compare firehose event log cursor=%d target=%d expected=%d observed=%d",
		phase, cursor, target, len(want), len(got))
}

// assertNoPermanentCursorGap is the anti-vacuity guard for boundary frame loss
// (R3). It asserts — directly and without waiting — that every upstream relay
// cursor the simulator generated in (after, through] was observed by the
// consumer (and therefore durably archived, since OnEvent fires post-Append).
//
// This is the loud, immediate backstop for the "graceful cutover dropped an
// in-flight frame" class of bug: the boundary loss is normally PREVENTED by the
// harness backpressure + BarrierBeforeCutover (which hold the bootstrap-live
// consumer abreast of generation and gate the cutover on full delivery), but
// that is a timing guarantee. If a future change weakens it, the dropped cursor
// is permanently lost (re-delivery is rev-replay-dropped by the shared
// verifier), and the existing multiset compare only surfaces it as a 5-minute
// TIMEOUT. This guard fails fast at quiescence with the exact missing cursors,
// so the regression cannot hide behind pacing. Run only after the relevant ack
// has fired, so a present-count check is not racing in-flight delivery.
func assertNoPermanentCursorGap(t *testing.T, observed *eventLogRecorder, after, through int64, cfg Config, phase string) {
	t.Helper()

	seen := observed.ObservedUpstreamCursors(after, through)
	var missing []int64
	for c := after + 1; c <= through; c++ {
		if _, ok := seen[c]; !ok {
			missing = append(missing, c)
		}
	}
	require.Emptyf(t, missing,
		"%s mode=%s seed=%d: %d upstream cursor(s) permanently lost in (%d,%d] — a graceful-cutover/backpressure regression dropped in-flight frames (first missing: %v)",
		phase, cfg.Mode, cfg.Seed, len(missing), after, through, firstN(missing, 10))
}

func firstN(xs []int64, n int) []int64 {
	if len(xs) <= n {
		return xs
	}
	return xs[:n]
}

func totalGetRepoHTTPFailuresFired(plan *SwarmFaultPlan) int {
	if plan == nil || plan.SimulatorFaults == nil {
		return 0
	}
	var total int
	for did := range plan.GetRepoHTTPFailures {
		total += plan.SimulatorFaults.GetRepoHTTPFailuresFired(did)
	}
	return total
}

func totalGetRepoCARTruncationsFired(plan *SwarmFaultPlan) int {
	if plan == nil || plan.SimulatorFaults == nil {
		return 0
	}
	var total int
	for did := range plan.GetRepoCARTruncations {
		total += plan.SimulatorFaults.GetRepoCARTruncationsFired(did)
	}
	return total
}

func totalIntMap(values map[string]int) int {
	var total int
	for _, value := range values {
		total += value
	}
	return total
}

// bisectServedCompactedFailure classifies a client-backfill CheckCompacted
// failure (the paginated planBackfill -> getSegment/getBlock -> /subscribe-v2
// cutover path real clients use) by re-running the identical check against the
// on-disk segments at the SAME watermark, then fails with a self-classifying
// verdict (DURABLE_DEFECT vs SERVING_DEFECT vs INCONCLUSIVE). It snapshots the
// compaction-pass count around the on-disk scan so a pass that races the scan
// downgrades a clean disk result to INCONCLUSIVE rather than asserting a
// (possibly torn) clean read meant "no durable defect". See bisect.go.
func bisectServedCompactedFailure(
	t *testing.T,
	trace *Trace,
	dataDir string,
	cfg Config,
	compaction *compactionPassRecorder,
	watermark uint64,
	servedErr error,
) {
	t.Helper()

	// Bracket the scan on BOTH the completed-pass and started-pass counts.
	// A pass that straddles the scan (begins before, ends after) moves
	// neither completed endpoint during the bracket, but DOES move the
	// started count, so taking the max of the two deltas detects an
	// in-flight rewrite that a completed-only bracket would miss (#106).
	completedBefore := compaction.Count()
	startedBefore := compaction.StartedCount()
	disk, err := ObserveSegments(dataDir)
	require.NoErrorf(t, err, "bisect: observe on-disk segments mode=%s seed=%d watermark=%d", cfg.Mode, cfg.Seed, watermark)
	disk = EventsSortedBySeq(disk)
	passesDuringScan := max(compaction.Count()-completedBefore, compaction.StartedCount()-startedBefore)

	v := ClassifyCompactedFailure(servedErr, disk, watermark, passesDuringScan)
	recordTraceOrError(t, trace, "compacted_bisection", map[string]any{
		"mode":               cfg.Mode,
		"seed":               cfg.Seed,
		"watermark":          watermark,
		"verdict":            string(v.Verdict),
		"served_err":         servedErr.Error(),
		"disk_err":           traceErr(v.DiskErr),
		"passes_during_scan": passesDuringScan,
		"disk_event_count":   len(disk),
	})
	require.Failf(t, "client-backfill compacted check failed",
		"mode=%s seed=%d: %v", cfg.Mode, cfg.Seed, v.Err())
}

func assertOracleMatches(t *testing.T, dataDir string, w *world.World, cfg Config, phase string) {
	t.Helper()
	assertOracleMatchesImpl(t, dataDir, w, cfg, phase, false)
}

// assertOracleMatchesAfterReplay is the crash-recovery variant: it asserts the
// same final-state correctness (Compare) but checks only the structural
// invariants that survive an idempotent at-least-once replay (unique/increasing
// seq, commit-has-rev), NOT per-DID rev-monotonicity-by-seq. A crash recovered
// at a merge replay boundary legitimately re-emits already-merged survivors at
// fresh higher seqs carrying their original lower revs; that is not corruption
// (Compare still converges), so the strict monotonic check must not run here.
// Per-record rev correctness and at-least-once coverage are owned by the
// event-log / chain-coverage tiers.
func assertOracleMatchesAfterReplay(t *testing.T, dataDir string, w *world.World, cfg Config, phase string) {
	t.Helper()
	assertOracleMatchesImpl(t, dataDir, w, cfg, phase, true)
}

func assertOracleMatchesImpl(t *testing.T, dataDir string, w *world.World, cfg Config, phase string, afterReplay bool) {
	t.Helper()

	want, err := GroundTruthFromWorld(w)
	require.NoErrorf(t, err, "%s mode=%s seed=%d: build ground truth", phase, cfg.Mode, cfg.Seed)
	events, err := ObserveSegments(dataDir)
	require.NoErrorf(t, err, "%s mode=%s seed=%d: observe segments", phase, cfg.Mode, cfg.Seed)
	if afterReplay {
		require.NoErrorf(t, CheckStructuralInvariants(events), "%s mode=%s seed=%d: check structural invariants", phase, cfg.Mode, cfg.Seed)
	} else {
		require.NoErrorf(t, CheckInvariants(events), "%s mode=%s seed=%d: check invariants", phase, cfg.Mode, cfg.Seed)
	}
	got, err := Reconstruct(EventsSortedBySeq(events))
	require.NoErrorf(t, err, "%s mode=%s seed=%d: reconstruct observed events", phase, cfg.Mode, cfg.Seed)
	require.NoErrorf(t, Compare(want, got), "%s mode=%s seed=%d: compare oracle model", phase, cfg.Mode, cfg.Seed)

	t.Logf("%s: oracle matched %d observed events in mode=%s seed=%d", phase, len(events), cfg.Mode, cfg.Seed)
}

func assertCompacted(t *testing.T, dataDir string, watermark uint64, cfg Config, phase string) {
	t.Helper()

	events, err := ObserveSegments(dataDir)
	require.NoErrorf(t, err, "%s mode=%s seed=%d: observe segments for compaction", phase, cfg.Mode, cfg.Seed)
	require.NoErrorf(t, CheckCompacted(EventsSortedBySeq(events), watermark),
		"%s mode=%s seed=%d: check compacted watermark=%d", phase, cfg.Mode, cfg.Seed, watermark)
}

func assertBootstrapOracleMatches(t *testing.T, dataDir string, w *world.World, cfg Config) {
	t.Helper()

	want, err := GroundTruthFromWorld(w)
	require.NoErrorf(t, err, "after-bootstrap mode=%s seed=%d: build ground truth", cfg.Mode, cfg.Seed)
	events, err := ObserveBootstrapSegments(dataDir)
	require.NoErrorf(t, err, "after-bootstrap mode=%s seed=%d: observe bootstrap segments", cfg.Mode, cfg.Seed)
	got, err := Reconstruct(events)
	require.NoErrorf(t, err, "after-bootstrap mode=%s seed=%d: reconstruct observed events", cfg.Mode, cfg.Seed)
	require.NoErrorf(t, Compare(want, got), "after-bootstrap mode=%s seed=%d: compare oracle model", cfg.Mode, cfg.Seed)

	t.Logf("after-bootstrap: oracle matched %d observed events in mode=%s seed=%d", len(events), cfg.Mode, cfg.Seed)
}

func recordTraceOrError(t *testing.T, trace *Trace, kind string, data map[string]any) {
	t.Helper()
	if err := recordTrace(trace, kind, data); err != nil {
		t.Errorf("record oracle trace %q: %v", kind, err)
	}
}

func traceSegmentEvent(ev *segment.Event) map[string]any {
	if ev == nil {
		return map[string]any{"nil": true}
	}
	out := map[string]any{
		"seq":                   ev.Seq,
		"witnessed_at":          ev.WitnessedAt,
		"indexed_at":            ev.IndexedAt,
		"upstream_relay_cursor": ev.UpstreamRelayCursor,
		"kind":                  eventLogKind(ev.Kind),
		"kind_code":             uint8(ev.Kind),
		"did":                   ev.DID,
		"collection":            ev.Collection,
		"rkey":                  ev.Rkey,
		"rev":                   ev.Rev,
	}
	if ev.Payload != nil {
		out["payload"] = tracePayload(ev.Payload)
	}
	return out
}

func traceErr(err error) any {
	if err == nil {
		return nil
	}
	return err.Error()
}

type phaseGate struct {
	entered     chan struct{}
	release     chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
}

func newPhaseGate() *phaseGate {
	return &phaseGate{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (g *phaseGate) Barrier(ctx context.Context) error {
	g.enterOnce.Do(func() { close(g.entered) })
	select {
	case <-g.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *phaseGate) Release() {
	g.releaseOnce.Do(func() { close(g.release) })
}

type runtimeRun struct {
	exited chan struct{}
	err    error
}

type compactionPassRecorder struct {
	mu      sync.Mutex
	results []jetstreamd.CompactionPassResult
	// started counts passes that have BEGUN real rewrite work (fired via
	// OnBeforeCompactionPass), as opposed to results which counts COMPLETED
	// passes (OnCompactionPass, in a defer at pass end). The bisect bracket
	// needs started to detect a pass that STRADDLES the on-disk scan (begins
	// before the scan, ends after it): such a pass increments neither
	// Count() endpoint during the bracket, so a completed-only counter reads
	// passesDuringScan==0 and mislabels a possibly-torn clean read as
	// SERVING_DEFECT instead of INCONCLUSIVE (#106).
	started int
}

func newCompactionPassRecorder() *compactionPassRecorder {
	return &compactionPassRecorder{}
}

func (r *compactionPassRecorder) Observe(result jetstreamd.CompactionPassResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.results = append(r.results, result)
}

// ObserveStart records that a compaction pass has begun real rewrite work.
// Wired to OnBeforeCompactionPass.
func (r *compactionPassRecorder) ObserveStart() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.started++
}

func (r *compactionPassRecorder) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.results)
}

// StartedCount returns the number of passes that have begun real rewrite
// work. Paired with Count() it brackets a straddling pass: if either the
// started or completed count moves across the scan, a rewrite was in
// flight during it.
func (r *compactionPassRecorder) StartedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.started
}

func (r *compactionPassRecorder) Last(t *testing.T) jetstreamd.CompactionPassResult {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	require.NotEmpty(t, r.results, "expected at least one compaction pass")
	last := r.results[len(r.results)-1]
	require.NoError(t, last.Err, "latest compaction pass failed")
	return last
}

func (r *compactionPassRecorder) WaitAfter(t *testing.T, cfg Config, run *runtimeRun, after int, timeout time.Duration) jetstreamd.CompactionPassResult {
	t.Helper()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()

	for {
		r.mu.Lock()
		if len(r.results) > after {
			last := r.results[len(r.results)-1]
			if last.Err != nil {
				r.mu.Unlock()
				t.Fatalf("compaction pass after %d failed: mode=%s seed=%d err=%v",
					after, cfg.Mode, cfg.Seed, last.Err)
			}
			r.mu.Unlock()
			return last
		}
		seen := len(r.results)
		r.mu.Unlock()

		select {
		case <-run.exited:
			t.Fatalf("runtime exited while waiting for compaction pass after %d: mode=%s seed=%d seen=%d err=%v",
				after, cfg.Mode, cfg.Seed, seen, run.err)
		case <-timer.C:
			t.Fatalf("timeout waiting for compaction pass after %d: mode=%s seed=%d seen=%d",
				after, cfg.Mode, cfg.Seed, seen)
		case <-tick.C:
		}
	}
}

// compactionOverDropPass holds the pre- and post-compaction sealed segment
// streams for a single compaction pass that did real work, captured so the
// harness can metamorphically prove the pass did not over-drop a surviving row
// (issue #100 / oracle.md compaction invariant). Compaction is strictly
// subtractive, so every row present before the pass that the documented
// compaction filter says must SURVIVE at the watermark must still be present
// after the pass; a missing survivor is a data-loss defect that final-state
// convergence and the pre-compaction event-log tier cannot see.
type compactionOverDropPass struct {
	watermark uint64
	pre       []EventLogRow
	post      []EventLogRow
}

// compactionOverDropRecorder pairs the pre-rewrite and post-rewrite sealed
// segment snapshots captured by the OnBeforeCompactionPass / OnCompactionPass
// hooks. Both hooks fire serially on the compactor goroutine, so a single
// pending slot pairs them 1:1; the hooks only read files and store rows, while
// the verdict (a pure comparison plus require) runs on the test goroutine.
type compactionOverDropRecorder struct {
	dataDir string

	mu          sync.Mutex
	havePending bool
	pendingW    uint64
	pendingPre  []EventLogRow
	scanErr     error
	passes      []compactionOverDropPass
}

func newCompactionOverDropRecorder(dataDir string) *compactionOverDropRecorder {
	return &compactionOverDropRecorder{dataDir: dataDir}
}

// ObserveBefore captures the pre-compaction sealed stream (rows with
// seq <= targetWatermark). It fires after the active segment is force-rotated
// and sealed but before any rewrite, so the snapshot is the complete stream the
// pass is about to subtract from. Sealed-only and seq-bounded, so it does not
// race the concurrent live writer appending rows above the watermark.
func (r *compactionOverDropRecorder) ObserveBefore(targetWatermark uint64) {
	rows, err := r.scanSealed(targetWatermark)
	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil {
		r.scanErr = errors.Join(r.scanErr, fmt.Errorf("pre-compaction scan watermark=%d: %w", targetWatermark, err))
		return
	}
	r.havePending = true
	r.pendingW = targetWatermark
	r.pendingPre = rows
}

// ObserveAfter pairs the post-compaction sealed stream with the pending
// pre-snapshot. It is a no-op when no pre-snapshot is pending (a no-op pass
// never fired ObserveBefore) or when the pass failed (a partially-advanced
// watermark would make the metamorphic relation ambiguous; the harness fails
// such a pass through the compaction-pass recorder instead).
func (r *compactionOverDropRecorder) ObserveAfter(result jetstreamd.CompactionPassResult) {
	r.mu.Lock()
	pending := r.havePending
	pendingW := r.pendingW
	pre := r.pendingPre
	r.havePending = false
	r.pendingPre = nil
	r.mu.Unlock()

	if !pending || result.Err != nil {
		return
	}

	// Cross-check the scan bound against the watermark the pass actually
	// committed. A successful watermark-advancing pass commits exactly the
	// targetWatermark ObserveBefore was handed, so a mismatch means the
	// OnBeforeCompactionPass hook fed this recorder a stale/wrong bound —
	// and a stale bound BELOW the pass's drops makes pre == post trivially
	// true, silently disarming the over-drop check while every other oracle
	// tier stays green (#226). Fail loud instead of comparing blind.
	if result.Watermark != pendingW {
		r.mu.Lock()
		defer r.mu.Unlock()
		// "oracle:" prefix so the mutation driver's note-grep surfaces this
		// as the kill reason instead of "see log".
		r.scanErr = errors.Join(r.scanErr, fmt.Errorf(
			"oracle: over-drop recorder watermark mismatch: pre-pass hook saw %d but the pass committed %d (recorder scans bounded at the wrong seq would vacuously pass)",
			pendingW, result.Watermark))
		return
	}

	post, err := r.scanSealed(pendingW)
	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil {
		r.scanErr = errors.Join(r.scanErr, fmt.Errorf("post-compaction scan watermark=%d: %w", pendingW, err))
		return
	}
	r.passes = append(r.passes, compactionOverDropPass{watermark: pendingW, pre: pre, post: post})
}

func (r *compactionOverDropRecorder) scanSealed(watermark uint64) ([]EventLogRow, error) {
	events, err := ObserveSealedSegments(r.dataDir)
	if err != nil {
		return nil, err
	}
	bounded := make([]ObservedEvent, 0, len(events))
	for _, ev := range events {
		if ev.Seq <= watermark {
			bounded = append(bounded, ev)
		}
	}
	return NormalizeEventLog(EventsSortedBySeq(bounded)), nil
}

// Assert proves no compaction pass over-dropped (or under-dropped) a row.
// For each captured pass it checks that the post-compaction stream equals the
// pre-compaction stream after applying the documented compaction filter at the
// watermark, as a multiset (block/segment order need not match). Anti-vacuity:
// at least one pass must have been observed and verified a non-empty surviving
// set, so a run where the hooks never fired or every window was empty fails
// loudly rather than passing silently.
func (r *compactionOverDropRecorder) Assert(t *testing.T, cfg Config, trace *Trace, phase string) {
	t.Helper()

	r.mu.Lock()
	require.NoErrorf(t, r.scanErr, "%s mode=%s seed=%d: compaction over-drop snapshot scan", phase, cfg.Mode, cfg.Seed)
	passes := r.passes
	r.mu.Unlock()

	var survivorsChecked int
	for _, p := range passes {
		survivors := filterCompactedExpectedRows(p.pre, p.watermark)
		survivorsChecked += len(survivors)
		dropped := len(p.pre) - len(survivors)
		err := CompareEventLogsCompactedMultiset(p.pre, p.post, p.watermark)
		recordTraceOrError(t, trace, "compaction_over_drop_check", map[string]any{
			"phase":          phase,
			"watermark":      p.watermark,
			"pre_count":      len(p.pre),
			"post_count":     len(p.post),
			"survivor_count": len(survivors),
			"dropped_count":  dropped,
			"err":            traceErr(err),
		})
		require.NoErrorf(t, err,
			"%s mode=%s seed=%d: compaction over-drop at watermark=%d (pre=%d post=%d survivors=%d dropped=%d)",
			phase, cfg.Mode, cfg.Seed, p.watermark, len(p.pre), len(p.post), len(survivors), dropped)
	}

	require.NotEmptyf(t, passes,
		"%s mode=%s seed=%d: no compaction pass observed for over-drop check (anti-vacuity)", phase, cfg.Mode, cfg.Seed)
	require.Positivef(t, survivorsChecked,
		"%s mode=%s seed=%d: compaction over-drop check verified no surviving rows (anti-vacuity)", phase, cfg.Mode, cfg.Seed)
}

func waitForBarrier(t *testing.T, cfg Config, name string, gate *phaseGate, run *runtimeRun) {
	t.Helper()

	timer := time.NewTimer(oracleWaitTimeout(cfg))
	defer timer.Stop()
	select {
	case <-gate.entered:
		return
	case <-run.exited:
		t.Fatalf("%s barrier not reached before runtime exited: mode=%s seed=%d err=%v", name, cfg.Mode, cfg.Seed, run.err)
	case <-timer.C:
		t.Fatalf("%s barrier not reached before timeout: mode=%s seed=%d", name, cfg.Mode, cfg.Seed)
	}
}

func oracleWaitTimeout(cfg Config) time.Duration {
	// The after-bootstrap barrier waits for the initial-record backfill
	// (accounts × MaxInitialRecords), which dominates bootstrap cost and is far
	// heavier than the live-event scaling below accounts for. A 60s floor was
	// too tight for stress mode on slower CI runners (the arc runner timed out
	// at exactly 60s). 5m gives ample headroom while staying well under the
	// per-seed `-timeout 30m` cap; a genuine hang is still caught promptly by
	// the run.exited select in waitForBarrier.
	timeout := 5 * time.Minute
	if cfg.LiveEventsBootstrap > 0 {
		scaled := time.Duration(cfg.LiveEventsBootstrap/100) * time.Second
		if scaled > timeout {
			timeout = scaled
		}
	}
	return timeout
}

func generateN(t *testing.T, w *world.World, n int) {
	t.Helper()

	for i := range n {
		_, err := w.GenerateOneForTest(t.Context())
		require.NoError(t, err)
		// In a synctest bubble a tight generate loop emits all N events in zero
		// fake-time, building a firehose backlog the consumer hasn't drained;
		// a later (re)subscribe then overruns the relay's bounded replay window
		// and skips the gap. Periodically yield so the consumer keeps pace,
		// mirroring the real-time interleaving a socket run gets for free. A
		// no-op outside a bubble (restart tier), so it stays correct there.
		if (i+1)%128 == 0 {
			drain()
		}
	}
}

func pickActiveOracleAccount(t *testing.T, w *world.World, cfg Config) int {
	t.Helper()
	for idx := range cfg.Accounts {
		deleted, err := w.IsAccountDeleted(idx)
		require.NoError(t, err)
		if !deleted {
			return idx
		}
	}
	t.Fatal("oracle requires at least one active account for sync divergence")
	return 0
}

// seqAck tracks a gap-free watermark over the steady-state cursors
// surfaced by OnEvent (which fires only after a durable Append). The
// harness waits until every cursor up to the target seq has been durably
// appended — not just the max — because the steady-state consumer
// completes batches out of order under parallelism >1.
type seqAck struct {
	mu     sync.Mutex
	seen   map[int64]struct{}
	target int64
	done   chan struct{}
	once   sync.Once
}

type syncTombstoneAck struct {
	mu      sync.Mutex
	seen    map[string]struct{}
	target  string
	done    chan struct{}
	waiting bool
	once    sync.Once
}

func newSeqAck() *seqAck {
	return &seqAck{
		seen: make(map[int64]struct{}),
		done: make(chan struct{}),
	}
}

func newSyncTombstoneAck() *syncTombstoneAck {
	return &syncTombstoneAck{
		seen: make(map[string]struct{}),
		done: make(chan struct{}),
	}
}

func (a *syncTombstoneAck) Observe(ev *segment.Event) {
	if ev == nil || ev.Kind != segment.KindSync || ev.DID == "" || ev.Rev == "" {
		return
	}
	a.mu.Lock()
	a.seen[syncTombstoneKey(ev.DID, ev.Rev)] = struct{}{}
	a.maybeDoneLocked()
	a.mu.Unlock()
}

func (a *syncTombstoneAck) Wait(t *testing.T, cfg Config, did, rev string, run *runtimeRun, timeout time.Duration) {
	t.Helper()

	a.mu.Lock()
	a.target = syncTombstoneKey(did, rev)
	a.waiting = true
	a.maybeDoneLocked()
	a.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-a.done:
		return
	case <-run.exited:
		t.Fatalf("steady-state ingestion stopped before observing async resync tombstone did=%s rev=%s: mode=%s seed=%d err=%v",
			did, rev, cfg.Mode, cfg.Seed, run.err)
	case <-timer.C:
		t.Fatalf("timeout waiting for async resync tombstone did=%s rev=%s: mode=%s seed=%d",
			did, rev, cfg.Mode, cfg.Seed)
	}
}

func (a *syncTombstoneAck) maybeDoneLocked() {
	if !a.waiting || a.target == "" {
		return
	}
	if _, ok := a.seen[a.target]; ok {
		a.once.Do(func() { close(a.done) })
	}
}

func syncTombstoneKey(did, rev string) string {
	return did + "\x00" + rev
}

func (a *seqAck) Observe(ev *segment.Event) {
	if ev.UpstreamRelayCursor <= 0 {
		return
	}
	a.mu.Lock()
	a.seen[ev.UpstreamRelayCursor] = struct{}{}
	a.maybeDoneLocked()
	a.mu.Unlock()
}

func (a *seqAck) Wait(t *testing.T, cfg Config, target int64, run *runtimeRun, timeout time.Duration) {
	t.Helper()

	a.mu.Lock()
	a.target = target
	a.maybeDoneLocked()
	a.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-a.done:
		return
	case <-run.exited:
		t.Fatalf("steady-state ingestion stopped before observing contiguous upstream seq %d: mode=%s seed=%d seen=%d highest_contiguous=%d err=%v",
			target, cfg.Mode, cfg.Seed, a.seenCount(), a.highestContiguous(), run.err)
	case <-timer.C:
		t.Fatalf("timeout waiting for contiguous steady-state upstream seq %d: mode=%s seed=%d seen=%d highest_contiguous=%d",
			target, cfg.Mode, cfg.Seed, a.seenCount(), a.highestContiguous())
	}
}

func (a *seqAck) WaitContiguousFrom(ctx context.Context, start, target int64, timeout time.Duration) error {
	if target < start {
		return nil
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()

	for {
		if a.highestContiguousFrom(start) >= target {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return fmt.Errorf("timeout waiting for contiguous upstream seq %d from %d: seen=%d highest_contiguous=%d",
				target, start, a.seenCount(), a.highestContiguousFrom(start))
		case <-tick.C:
		}
	}
}

// maybeDoneLocked signals completion once every steady-state cursor from
// the lowest observed up to the target is durably appended. OnEvent fires
// only after the writer's durable Append, so a present cursor is one the
// shutdown flush will persist. Requiring a gap-free run (rather than just
// the MAX cursor) is the crux: the steady-state consumer runs with
// parallelism >1 and completes a batch out of order, so the max can be
// appended while a lower seq is still in flight. Cancelling on the max
// would drop that in-flight event and leave a record missing from the
// reconstructed model. The target is the last generated event, so by the
// time it appears every earlier batch — hence the true lowest cursor — has
// already been observed; min(seen) reliably equals the steady-state start.
func (a *seqAck) maybeDoneLocked() {
	if a.contiguousToTargetLocked() {
		a.once.Do(func() { close(a.done) })
	}
}

func (a *seqAck) contiguousToTargetLocked() bool {
	if a.target <= 0 {
		return false
	}
	if _, ok := a.seen[a.target]; !ok {
		return false
	}
	lo := a.target
	for c := range a.seen {
		if c < lo {
			lo = c
		}
	}
	for c := lo; c <= a.target; c++ {
		if _, ok := a.seen[c]; !ok {
			return false
		}
	}
	return true
}

func (a *seqAck) seenCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.seen)
}

func (a *seqAck) highestContiguous() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.seen) == 0 {
		return 0
	}
	lo := int64(-1)
	for c := range a.seen {
		if lo == -1 || c < lo {
			lo = c
		}
	}
	h := lo - 1
	for {
		if _, ok := a.seen[h+1]; !ok {
			return h
		}
		h++
	}
}

func (a *seqAck) highestContiguousFrom(start int64) int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	h := start - 1
	for {
		if _, ok := a.seen[h+1]; !ok {
			return h
		}
		h++
	}
}

func waitForRuntimeExit(t *testing.T, cfg Config, run *runtimeRun) {
	t.Helper()

	select {
	case <-run.exited:
		require.NoErrorf(t, run.err, "runtime exit mode=%s seed=%d", cfg.Mode, cfg.Seed)
	case <-time.After(10 * time.Second):
		t.Fatalf("runtime did not exit after cancellation: mode=%s seed=%d", cfg.Mode, cfg.Seed)
	}
}

type testWriter struct {
	t *testing.T
}

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Helper()
	w.t.Logf("%s", p)
	return len(p), nil
}

// bootstrapTrafficGenerator emits the bootstrap-phase live traffic (an
// account-delete tombstone followed by ordinary commits) that the
// bootstrap-live consumer must archive concurrently with backfill.
//
// CRITICAL (R2 deadlock fix): generation and its backpressure wait run on a
// DEDICATED goroutine (Run), NOT inside AfterRepoComplete. AfterRepoComplete is
// the backfill writer's OnDurableBatch afterDone callback, invoked while
// ingest.Writer.mu + drainMu are held (writer.go commitDurableBatchLocked); a
// blocking wait there deadlocks because sibling backfill workers then block on
// w.mu, and a goroutine blocked on a Mutex is NOT durably-blocked under
// testing/synctest, so the fake clock freezes and the bootstrap-live consumer
// can never deliver the awaited cursors. Run does the same batch-generate-then-
// WaitContiguousFrom pacing off the writer lock: backpressure (which masks the
// fanout boundary race — see R3) is preserved, but nothing blocks under the
// lock. AfterRepoComplete is reduced to a non-blocking signal/counter.
type bootstrapTrafficGenerator struct {
	accounts int
	target   int
	generate func(context.Context) (int64, error)
	ack      *seqAck
	timeout  time.Duration

	startOnce sync.Once
	started   chan struct{}
	delivered chan struct{} // closed by Run once all target events are delivered

	mu        sync.Mutex
	completed int
	generated int
}

func newBootstrapTrafficGenerator(accounts, target int, ack *seqAck, timeout time.Duration, generate func(context.Context) (int64, error)) *bootstrapTrafficGenerator {
	return &bootstrapTrafficGenerator{
		accounts:  accounts,
		target:    target,
		generate:  generate,
		ack:       ack,
		timeout:   timeout,
		started:   make(chan struct{}),
		delivered: make(chan struct{}),
	}
}

// WaitDelivered blocks until Run has generated and durably delivered every
// bootstrap event (or ctx is cancelled). Wired to the orchestrator's
// BarrierBeforeCutover so the cutover does not cancel the bootstrap-live
// consumer until all injected bootstrap traffic has been archived — the
// delivery guarantee the old in-callback wait provided implicitly.
func (g *bootstrapTrafficGenerator) WaitDelivered(ctx context.Context) error {
	if g == nil || g.target <= 0 {
		return nil
	}
	select {
	case <-g.delivered:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run generates the bootstrap live traffic on its own goroutine, blocking only
// on bubble channels (the start signal, the world generators, and the off-lock
// WaitContiguousFrom). It waits for the first AfterRepoComplete (so backfill is
// underway and the bootstrap-live consumer is subscribed) before starting, then
// emits the target events in small batches, waiting after each batch until every
// cursor up to the last generated seq has been durably delivered. That wait is
// the backpressure that keeps the consumer abreast of generation (masking the
// fanout boundary race) and, because Run finishes well before backfill drains,
// it also ensures all bootstrap events reach the bootstrap-live consumer before
// the cutover cancels it. Returns nil on clean ctx cancellation.
func (g *bootstrapTrafficGenerator) Run(ctx context.Context) error {
	if g == nil || g.target <= 0 {
		return nil
	}

	select {
	case <-g.started:
	case <-ctx.Done():
		return nil
	}

	// Mirror the old per-completion cadence (~ceil(target/accounts) events
	// between waits) so the consumer is paced the same way it was under the
	// in-callback wait, just off the writer lock.
	batch := max(1, (g.target+g.accounts-1)/g.accounts)
	var generated int
	for generated < g.target {
		n := min(batch, g.target-generated)
		var lastSeq int64
		for range n {
			seq, err := g.generate(ctx)
			if err != nil {
				return err
			}
			lastSeq = seq
			generated++
			g.mu.Lock()
			g.generated = generated
			g.mu.Unlock()
		}
		if lastSeq > 0 {
			if err := g.ack.WaitContiguousFrom(ctx, 1, lastSeq, g.timeout); err != nil {
				return err
			}
		}
	}
	// Every target event is generated and (per the final WaitContiguousFrom)
	// durably delivered. Release the pre-cutover barrier.
	close(g.delivered)
	return nil
}

// AfterRepoComplete is the backfill afterDone hook. It runs UNDER the ingest
// writer lock, so it must NOT block: it only kicks the generator goroutine on
// the first call and bumps the completed counter for the trace.
func (g *bootstrapTrafficGenerator) AfterRepoComplete(_ context.Context, _ atmos.DID) error {
	if g == nil || g.target <= 0 {
		return nil
	}
	g.startOnce.Do(func() { close(g.started) })
	g.mu.Lock()
	g.completed++
	g.mu.Unlock()
	return nil
}

func (g *bootstrapTrafficGenerator) Snapshot() (completed, generated int) {
	if g == nil {
		return 0, 0
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	return g.completed, g.generated
}
