package oracle

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http/httptest"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/jetstreamd"
	"github.com/bluesky-social/jetstream-v2/internal/simulator/fanout"
	simhttp "github.com/bluesky-social/jetstream-v2/internal/simulator/http"
	"github.com/bluesky-social/jetstream-v2/internal/simulator/world"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/streaming"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// nolint:paralleltest
func TestOracle_DefaultLifecycle(t *testing.T) {
	cfg, err := ParseConfigFromLookupEnv(os.LookupEnv)
	require.NoError(t, err)
	if cfg.Mode == "stress" && testing.Short() {
		t.Skip("skipping stress oracle under -short")
	}

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
	require.NoError(t, w.AttachRuntime(
		rand.New(rand.NewPCG(cfg.Seed^0xfeedf00d, cfg.Seed^0xc0ffee)),
		fanout.New(4096),
	))

	faultPlan, err := BuildSwarmFaultPlan(w, cfg)
	require.NoError(t, err)
	recordTraceOrError(t, trace, "fault_plan", map[string]any{
		"scheduled_get_repo_http_failures":           faultPlan.TotalGetRepoHTTPFailures(),
		"scheduled_get_repo_http_failure_dids":       len(faultPlan.GetRepoHTTPFailures),
		"scheduled_get_repo_car_truncations":         faultPlan.TotalGetRepoCARTruncations(),
		"scheduled_get_repo_car_truncation_dids":     len(faultPlan.GetRepoCARTruncations),
		"subscribe_repos_disconnect_threshold_count": len(faultPlan.SubscribeReposDisconnectThresholds),
	})

	srv := httptest.NewServer(nil)
	defer srv.Close()
	srv.Config.Handler = simhttp.NewHandlerWithOptions(w, srv.URL, simhttp.HandlerOptions{
		Faults: faultPlan.SimulatorFaults,
	})

	dataDir := t.TempDir()
	afterBootstrap := newPhaseGate()
	afterMerge := newPhaseGate()
	bootstrapAck := newSeqAck()
	steadyAck := newSeqAck()
	asyncResyncAck := newSyncTombstoneAck()
	compaction := newCompactionPassRecorder()
	bootstrapEventLog := newEventLogRecorder()
	steadyEventLog := newEventLogRecorder()
	ctx, cancel := context.WithCancel(t.Context())
	emittedBootstrapAccountDelete := false
	bootstrapTraffic := newBootstrapTrafficGenerator(cfg.Accounts, cfg.LiveEventsBootstrap, func(ctx context.Context) (int64, error) {
		if !emittedBootstrapAccountDelete {
			emittedBootstrapAccountDelete = true
			_, err := w.GenerateAccountDeleteForTest(ctx, 0)
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
	bootstrapTraffic.afterBatch = func(ctx context.Context, targetSeq int64) error {
		return bootstrapAck.WaitContiguousFrom(ctx, 1, targetSeq, oracleWaitTimeout(cfg))
	}
	liveReconnectBackoff := &streaming.BackoffPolicy{
		InitialDelay: gt.Some(time.Millisecond),
		MaxDelay:     gt.Some(time.Millisecond),
		Multiplier:   gt.Some(1.0),
		Jitter:       gt.Some(false),
	}

	rt, err := jetstreamd.Build(ctx, jetstreamd.Options{
		PublicAddr:         "127.0.0.1:0",
		DebugAddr:          "127.0.0.1:0",
		DataDir:            dataDir,
		RelayURL:           srv.URL,
		PLCURL:             srv.URL,
		OTelServiceName:    "jetstream-oracle",
		LogLevel:           "warn",
		LogFormat:          "text",
		LogOutput:          testWriter{t: t},
		ShutdownTimeout:    5 * time.Second,
		ClientDrainTimeout: time.Second,
		// Keep injected transient getRepo 503s fast: a sub-millisecond
		// retry backoff means each fault adds microseconds, not atmos's
		// 1s production base delay, so the swarm sweep stays inside its
		// per-seed timeout budget even at stress scale.
		BackfillRetryBaseDelay:    time.Millisecond,
		LiveReconnectBackoff:      liveReconnectBackoff,
		CursorLookback:            36 * time.Hour,
		SegmentCacheMaxAge:        0,
		SubscribeHotTailBytes:     16 << 20,
		SubscribeBlockCacheBytes:  16 << 20,
		SubscribeReadBatch:        1024,
		SubscribeSlowWindow:       time.Second,
		SubscribeSlowMinRate:      1,
		CursorBlockIndexCacheSize: 32,
		CompactionInterval:        time.Hour,
		CompactionTombstoneCap:    1,
		BarrierAfterBootstrap:     afterBootstrap.Barrier,
		BarrierAfterMerge:         afterMerge.Barrier,
		OnCompactionPass: func(result jetstreamd.CompactionPassResult) {
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

	runDone := false
	defer func() {
		cancel()
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
	}()

	recordTraceOrError(t, trace, "phase", map[string]any{"phase": "after-bootstrap", "marker": "wait_begin"})
	waitForBarrier(t, cfg, "after-bootstrap", afterBootstrap, run)
	recordTraceOrError(t, trace, "phase", map[string]any{"phase": "after-bootstrap", "marker": "barrier_reached"})
	recordGetRepoFaults(t, trace, "after-bootstrap", faultPlan)
	assertFaultPlanFired(t, cfg, faultPlan)
	bootstrapTargetSeq := w.CurrentSeq()
	assertFirehoseEventLogMatches(t, trace, w, bootstrapEventLog, 0, bootstrapTargetSeq, "after-bootstrap")
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
	_ = collectSubscribeReplay(t, cfg, run, trace, publicURL, 0, afterMergeCompaction.Watermark)
	passesBeforeSteady := compaction.Count()

	steadyStartSeq := w.CurrentSeq()
	generateN(t, w, cfg.LiveEventsSteady)
	syncIdx := pickActiveOracleAccount(t, w, cfg)
	_, err = w.GenerateSilentMutationThenSyncForTest(t.Context(), syncIdx)
	require.NoError(t, err)
	targetSeq := w.CurrentSeq()
	recordTraceOrError(t, trace, "steady_target", map[string]any{"target_seq": targetSeq})
	steadyAck.Wait(t, cfg, targetSeq, run, 30*time.Second)
	assertFirehoseEventLogMatches(t, trace, w, steadyEventLog, steadyStartSeq, targetSeq, "steady-state")

	asyncIdx := pickActiveOracleAccount(t, w, cfg)
	_, err = w.GenerateSilentMutationThenCommitForTest(t.Context(), asyncIdx)
	require.NoError(t, err)
	asyncEntry, _, err := w.ListReposPage(asyncIdx, 1)
	require.NoError(t, err)
	require.Len(t, asyncEntry, 1)
	asyncResyncAck.Wait(t, cfg, string(asyncEntry[0].DID), asyncEntry[0].Rev, run, 30*time.Second)
	steadyCompaction := compaction.WaitAfter(t, cfg, run, passesBeforeSteady, 30*time.Second)
	require.Greaterf(t, steadyCompaction.Watermark, afterMergeCompaction.Watermark,
		"steady compaction watermark did not advance: mode=%s seed=%d after_merge_watermark=%d steady_watermark=%d",
		cfg.Mode, cfg.Seed, afterMergeCompaction.Watermark, steadyCompaction.Watermark)
	served := collectSubscribeReplay(t, cfg, run, trace, publicURL, 0, steadyCompaction.Watermark)
	require.NoErrorf(t, CheckCompacted(served, steadyCompaction.Watermark),
		"served subscribe replay compacted check failed: mode=%s seed=%d watermark=%d",
		cfg.Mode, cfg.Seed, steadyCompaction.Watermark)

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
	recordSubscribeReposFaults(t, trace, "steady-state-shutdown-flush", faultPlan)
	assertSubscribeReposFaultPlanFired(t, cfg, faultPlan)
	assertOracleMatches(t, dataDir, w, cfg, "steady-state-shutdown-flush")
	assertCompacted(t, dataDir, compaction.Last(t).Watermark, cfg, "steady-state-shutdown-flush")
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
		"subscribe_replay_start",
		"subscribe_replay_event",
		"subscribe_replay_done",
		"steady_target",
		"shutdown_start",
		"runtime_exit",
	)
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
	got := observed.RowsByUpstreamCursor(cursor, target)
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

func assertOracleMatches(t *testing.T, dataDir string, w *world.World, cfg Config, phase string) {
	t.Helper()

	want, err := GroundTruthFromWorld(w)
	require.NoErrorf(t, err, "%s mode=%s seed=%d: build ground truth", phase, cfg.Mode, cfg.Seed)
	events, err := ObserveSegments(dataDir)
	require.NoErrorf(t, err, "%s mode=%s seed=%d: observe segments", phase, cfg.Mode, cfg.Seed)
	require.NoErrorf(t, CheckInvariants(events), "%s mode=%s seed=%d: check invariants", phase, cfg.Mode, cfg.Seed)
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
		"indexed_at":            ev.IndexedAt,
		"rendered_at":           ev.RenderedAt,
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

func traceObservedEvent(ev ObservedEvent) map[string]any {
	out := map[string]any{
		"seq":        ev.Seq,
		"indexed_at": ev.IndexedAt,
		"kind":       eventLogKind(ev.Kind),
		"kind_code":  uint8(ev.Kind),
		"did":        ev.DID,
		"collection": ev.Collection,
		"rkey":       ev.Rkey,
		"rev":        ev.Rev,
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
}

func newCompactionPassRecorder() *compactionPassRecorder {
	return &compactionPassRecorder{}
}

func (r *compactionPassRecorder) Observe(result jetstreamd.CompactionPassResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.results = append(r.results, result)
}

func (r *compactionPassRecorder) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.results)
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

	for range n {
		_, err := w.GenerateOneForTest(t.Context())
		require.NoError(t, err)
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

type bootstrapTrafficGenerator struct {
	mu         sync.Mutex
	accounts   int
	target     int
	completed  int
	generated  int
	generate   func(context.Context) (int64, error)
	afterBatch func(context.Context, int64) error
}

func newBootstrapTrafficGenerator(accounts, target int, generate func(context.Context) (int64, error)) *bootstrapTrafficGenerator {
	return &bootstrapTrafficGenerator{
		accounts: accounts,
		target:   target,
		generate: generate,
	}
}

func (g *bootstrapTrafficGenerator) AfterRepoComplete(ctx context.Context, _ atmos.DID) error {
	if g == nil || g.target <= 0 {
		return nil
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	g.completed++
	remainingEvents := g.target - g.generated
	if remainingEvents <= 0 {
		return nil
	}

	remainingAccounts := max(1, g.accounts-g.completed+1)
	n := (remainingEvents + remainingAccounts - 1) / remainingAccounts
	var lastSeq int64
	for range n {
		seq, err := g.generate(ctx)
		if err != nil {
			return err
		}
		lastSeq = seq
		g.generated++
	}
	if g.afterBatch != nil && lastSeq > 0 {
		return g.afterBatch(ctx, lastSeq)
	}
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
