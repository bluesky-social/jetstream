package oracle

import (
	"context"
	"testing"
	"time"

	simhttp "github.com/bluesky-social/jetstream/internal/simulator/http"
	"github.com/bluesky-social/jetstream/internal/simulator/world"
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
// Each scenario drives the shared live-tail harness (see
// live_tail_harness_test.go) with a replay fault armed, over a traffic
// shape that puts an account-delete + reactivate + recreate inside the
// replayed window — the shape that corrupts state if any protection
// regresses. Anti-vacuity: the fault must fire, frames must actually be
// re-delivered, and the #231 guard must report drops (proving the
// account replay reached it).

// replayScenarioTraffic drives the world through the corrupting shape:
// two commits, then account-0 delete + reactivate + recreate.
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

// runReplayFaultScenario arms the given replay fault to fire after every
// real frame has been delivered, generates one post-replay commit to
// prove the stream continues, and returns the observed archive plus
// fixtures.
func runReplayFaultScenario(t *testing.T, fault simhttp.SubscribeReposReplayFault) (
	[]ObservedEvent, []EventLogRow, []EventLogRow, *liveTailHarness,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	h := newLiveTailHarness(t, ctx)
	replayScenarioTraffic(t, ctx, h.World)
	preReplayTip := h.World.CurrentSeq()

	// Fire after the connection has delivered every pre-replay frame, so
	// the replayed window deterministically contains the account
	// lifecycle. AfterFrames counts frames on the connection (fresh
	// cursor=0 subscription => all of history).
	fault.AfterFrames = int(preReplayTip)
	h.Faults.SetSubscribeReposReplaySchedule([]simhttp.SubscribeReposReplayFault{fault})

	h.StartConsumer(t, ctx)

	// Wait for the replay fault to fire (all real frames delivered first
	// by construction), then generate one post-replay commit and wait for
	// its rows — a durable-append barrier proving the consumer processed
	// everything the relay sent, including the replayed window that
	// preceded it on the same ordered websocket.
	require.Eventually(t, func() bool {
		return h.Faults.SubscribeReposReplaysFired() == 1
	}, 30*time.Second, 10*time.Millisecond, "replay fault never fired")

	_, err := h.World.GenerateOneForTest(ctx)
	require.NoError(t, err)
	finalTip := h.World.CurrentSeq()
	expected, err := ExpectedEventLogFromFirehose(h.World, 0, int(finalTip))
	require.NoError(t, err)
	require.True(t, h.Recorder.waitForRowCount(ctx, 0, finalTip, len(expected)),
		"timed out waiting for %d expected durable rows", len(expected))

	h.StopConsumer(t)

	events := h.ObservedEvents(t)
	observed := h.Recorder.RowsByUpstreamCursor(0, finalTip)
	return events, observed, expected, h
}

// assertReplayScenarioContracts runs the #205 contract bundle over one
// replay scenario's results.
func assertReplayScenarioContracts(
	t *testing.T,
	events []ObservedEvent,
	observed, expected []EventLogRow,
	h *liveTailHarness,
	wantReplayedFrames int,
) {
	t.Helper()

	// Anti-vacuity: the fault fired and re-delivered real frames, and the
	// #231 consumer guard saw at least one replayed #account (proving the
	// dangerous frame class reached it rather than the run passing on an
	// account-free window).
	require.Equal(t, 1, h.Faults.SubscribeReposReplaysFired(), "replay fault must fire exactly once")
	require.Equal(t, wantReplayedFrames, h.Faults.SubscribeReposReplayedFrames(),
		"replay fault must re-deliver the exact scheduled window")
	require.GreaterOrEqual(t, testutil.ToFloat64(h.Metrics.ReplayedAccountsDrop), 1.0,
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
	assertLiveTailConverged(t, h, events,
		"final state must converge to world ground truth under seq replay")
}

// TestOracle_RelaySeqDuplicates covers duplicate-N mode: the relay
// re-sends the last 3 frames (account-delete, reactivate, recreate)
// verbatim after delivering them once.
func TestOracle_RelaySeqDuplicates(t *testing.T) {
	t.Parallel()
	events, observed, expected, h := runReplayFaultScenario(t,
		simhttp.SubscribeReposReplayFault{DuplicateLast: 3})
	assertReplayScenarioContracts(t, events, observed, expected, h, 3)
}

// TestOracle_RelaySeqRegression covers regress-to-K mode: the relay
// replays the whole retained window above seq 2 — commits, the account
// lifecycle, everything — as a relay restored from backup would.
func TestOracle_RelaySeqRegression(t *testing.T) {
	t.Parallel()
	events, observed, expected, h := runReplayFaultScenario(t,
		simhttp.SubscribeReposReplayFault{RegressToSeq: 2})
	// The pre-replay tip is 5 (2 commits + delete + reactivate + recreate),
	// so regressing to 2 re-delivers seqs 3..5.
	assertReplayScenarioContracts(t, events, observed, expected, h, 3)
}
