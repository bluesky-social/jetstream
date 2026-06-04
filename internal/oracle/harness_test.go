package oracle

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/jetstreamd"
	"github.com/bluesky-social/jetstream-v2/internal/simulator/fanout"
	simhttp "github.com/bluesky-social/jetstream-v2/internal/simulator/http"
	"github.com/bluesky-social/jetstream-v2/internal/simulator/world"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/jcalabro/atmos"
	"github.com/stretchr/testify/require"
)

// nolint:paralleltest
func TestOracle_DefaultLifecycle(t *testing.T) {
	cfg, err := ParseConfigFromLookupEnv(os.LookupEnv)
	require.NoError(t, err)
	if cfg.Mode == "stress" && testing.Short() {
		t.Skip("skipping stress oracle under -short")
	}

	simCfg := world.DefaultConfig()
	simCfg.DataDir = t.TempDir()
	simCfg.Seed = cfg.Seed
	simCfg.Accounts = cfg.Accounts
	simCfg.InitialRecords = 0
	simCfg.InitialRecordsMin = cfg.MinInitialRecords
	simCfg.InitialRecordsMax = cfg.MaxInitialRecords
	simCfg.FirehoseHistory = max(10_000, cfg.LiveEventsBootstrap+cfg.LiveEventsSteady+1024)

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

	srv := httptest.NewServer(nil)
	defer srv.Close()
	srv.Config.Handler = simhttp.NewHandler(w, srv.URL)

	dataDir := t.TempDir()
	afterBootstrap := newPhaseGate()
	afterMerge := newPhaseGate()
	bootstrapAck := newSeqAck()
	steadyAck := newSeqAck()
	ctx, cancel := context.WithCancel(t.Context())
	bootstrapTraffic := newBootstrapTrafficGenerator(cfg.Accounts, cfg.LiveEventsBootstrap, func(ctx context.Context) (int64, error) {
		_, err := w.GenerateOneForTest(ctx)
		if err != nil {
			return 0, err
		}
		return w.CurrentSeq(), nil
	})
	bootstrapTraffic.afterBatch = func(ctx context.Context, targetSeq int64) error {
		return bootstrapAck.WaitContiguousFrom(ctx, 1, targetSeq, oracleWaitTimeout(cfg))
	}

	rt, err := jetstreamd.Build(ctx, jetstreamd.Options{
		PublicAddr:                "127.0.0.1:0",
		DebugAddr:                 "127.0.0.1:0",
		DataDir:                   dataDir,
		RelayURL:                  srv.URL,
		PLCURL:                    srv.URL,
		OTelServiceName:           "jetstream-oracle",
		LogLevel:                  "warn",
		LogFormat:                 "text",
		LogOutput:                 testWriter{t: t},
		ShutdownTimeout:           5 * time.Second,
		ClientDrainTimeout:        time.Second,
		CursorLookback:            36 * time.Hour,
		SegmentCacheMaxAge:        0,
		SubscribeHotTailBytes:     16 << 20,
		SubscribeBlockCacheBytes:  16 << 20,
		SubscribeReadBatch:        1024,
		SubscribeSlowWindow:       time.Second,
		SubscribeSlowMinRate:      1,
		CursorBlockIndexCacheSize: 32,
		BarrierAfterBootstrap:     afterBootstrap.Barrier,
		BarrierAfterMerge:         afterMerge.Barrier,
		OnBootstrapLiveEvent:      bootstrapAck.Observe,
		OnSteadyStateEvent:        steadyAck.Observe,
		AfterRepoComplete:         bootstrapTraffic.AfterRepoComplete,
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
			runDone = true
		}
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		require.NoError(t, rt.Close(closeCtx))
	}()

	waitForBarrier(t, cfg, "after-bootstrap", afterBootstrap, run)
	assertBootstrapOracleMatches(t, dataDir, w, cfg)
	afterBootstrap.Release()

	waitForBarrier(t, cfg, "after-merge", afterMerge, run)
	assertOracleMatches(t, dataDir, w, cfg, "after-merge")
	afterMerge.Release()

	generateN(t, w, cfg.LiveEventsSteady)
	targetSeq := w.CurrentSeq()
	steadyAck.Wait(t, cfg, targetSeq, run, 30*time.Second)

	// steadyAck.Wait above guarantees every steady-state cursor up to
	// targetSeq has been durably appended (OnEvent fires post-Append), so
	// no event is still in flight when we cancel. The steady-state writer
	// buffers pending events until a block fills or shutdown closes the
	// consumer; this assertion verifies that graceful shutdown durably
	// flushes the generated live events.
	cancel()
	waitForRuntimeExit(t, cfg, run)
	runDone = true
	assertOracleMatches(t, dataDir, w, cfg, "steady-state-shutdown-flush")
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

func newSeqAck() *seqAck {
	return &seqAck{
		seen: make(map[int64]struct{}),
		done: make(chan struct{}),
	}
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
