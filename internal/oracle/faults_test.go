package oracle

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/stretchr/testify/require"
)

// TestBuildSwarmFaultPlanIsDeterministicAndBounded validates fault
// PLAN GENERATION only: that BuildSwarmFaultPlan produces a deterministic,
// bounded, correctly-shaped schedule. The low-index bias of DID selection is
// covered separately by TestSkewedIndexBiasesTowardLowIndices. This does NOT
// exercise injection or jetstream's recovery — that is covered end-to-end by
// TestOracle_DefaultLifecycle (swarm mode) and at the simulator-handler
// boundary by TestPDS_GetRepoFaultHandlerServesTransient503ThenCAR.
func TestBuildSwarmFaultPlanIsDeterministicAndBounded(t *testing.T) {
	t.Parallel()

	w := newFaultPlanWorld(t, 12)
	cfg := Config{
		Mode:                "fast",
		Seed:                123,
		Accounts:            12,
		MinInitialRecords:   1,
		MaxInitialRecords:   4,
		LiveEventsBootstrap: 4,
		LiveEventsSteady:    25,
		FaultMode:           FaultModeSwarm,
	}

	first, err := BuildSwarmFaultPlan(w, cfg)
	require.NoError(t, err)
	second, err := BuildSwarmFaultPlan(w, cfg)
	require.NoError(t, err)

	// Determinism: same seed + same world => identical schedule.
	require.Equal(t, first.GetRepoHTTPFailures, second.GetRepoHTTPFailures)
	require.Equal(t, first.GetRepoResponseFailures, second.GetRepoResponseFailures)
	require.Equal(t, first.GetRepoCARTruncations, second.GetRepoCARTruncations)

	// Exact swarm contract for a multi-DID world: precisely two distinct
	// DIDs, one "hot" with a raw failure + typed response failure and one
	// secondary with a raw failure. Asserting
	// the exact shape (not just ">= 2 DIDs" / "max > 1") pins the planner
	// so a regression that, say, faulted every DID once or collapsed both
	// onto a single DID would fail here.
	require.Len(t, first.GetRepoHTTPFailures, 2, "swarm raw-HTTP faults exactly two distinct DIDs")
	require.Equal(t, 2, first.TotalGetRepoHTTPFailures(), "1 (hot) + 1 (secondary)")
	require.Len(t, first.GetRepoResponseFailures, 1, "swarm typed-response faults the hot DID once")
	require.Equal(t, 1, first.TotalGetRepoResponseFailures())
	require.Len(t, first.GetRepoCARTruncations, 1, "swarm faults one DID with a truncated CAR")
	require.Equal(t, 1, first.TotalGetRepoCARTruncations())

	counts := make([]int, 0, 2)
	for _, c := range first.GetRepoHTTPFailures {
		counts = append(counts, c)
	}
	slices.Sort(counts)
	require.Equal(t, []int{1, 1}, counts, "one hot DID (1 raw failure) and one secondary DID (1 raw failure)")
}

// TestSwarmFaultPlanWithinRetryBudget pins the #109 invariant: the swarm
// plan leaves every faulted DID at least one clean getRepo attempt, keyed
// off the real backfill retry budget. The hot DID's 3 faults (1 HTTP + 1
// typed response + 1 CAR) sit one below the 4-attempt ceiling; the guard must
// accept that and reject a plan that reaches/exceeds the ceiling.
func TestSwarmFaultPlanWithinRetryBudget(t *testing.T) {
	t.Parallel()

	w := newFaultPlanWorld(t, 12)
	cfg := Config{
		Mode:                "fast",
		Seed:                123,
		Accounts:            12,
		MinInitialRecords:   1,
		MaxInitialRecords:   4,
		LiveEventsBootstrap: 4,
		LiveEventsSteady:    25,
		FaultMode:           FaultModeSwarm,
	}
	plan, err := BuildSwarmFaultPlan(w, cfg)
	require.NoError(t, err)

	// The real plan is within budget (hot DID = 3 faults < 4 attempts).
	require.NoError(t, plan.CheckWithinRetryBudget())

	// A nil plan and the no-fault plan are trivially within budget.
	require.NoError(t, (*SwarmFaultPlan)(nil).CheckWithinRetryBudget())

	// Pushing a DID's combined faults to the attempt ceiling must trip the
	// guard. Find the hot DID (the one with the typed response failure) and
	// add CAR truncations until consumed == DefaultMaxRetries+1.
	var hot string
	for did := range plan.GetRepoResponseFailures {
		hot = did
		break
	}
	require.NotEmpty(t, hot, "expected a hot DID with a typed response failure")
	// hot already has 1 HTTP + 1 typed response + 1 CAR = 3; one more CAR
	// truncation -> 4 == ceiling.
	plan.GetRepoCARTruncations[hot]++
	require.Error(t, plan.CheckWithinRetryBudget(),
		"a plan that consumes all 4 attempts for a DID must be rejected")
}

// TestSkewedIndexBiasesTowardLowIndices pins skewedIndex's low-index bias so a
// regression that replaced it with uniform selection (e.g. plain rng.IntN(n))
// fails loudly. The min-of-three-draws scheme pulls the mean well below the
// uniform mean (n-1)/2 and makes index 0 by far the most common outcome. A
// fixed-seed RNG keeps the assertions deterministic rather than statistical.
func TestSkewedIndexBiasesTowardLowIndices(t *testing.T) {
	t.Parallel()

	const n, samples = 12, 10000
	rng := rand.New(rand.NewPCG(1, 2))
	counts := make([]int, n)
	sum := 0
	for range samples {
		i := skewedIndex(rng, n)
		require.GreaterOrEqual(t, i, 0)
		require.Less(t, i, n)
		counts[i]++
		sum += i
	}

	// Uniform selection would give mean (n-1)/2 = 5.5 and counts[0] ~= samples/n.
	require.Less(t, float64(sum)/samples, 4.0,
		"min-of-3 skew must pull the mean well below the uniform mean of 5.5")
	require.Greater(t, counts[0], 2*samples/n,
		"index 0 must be selected far more often than uniform frequency")
	require.Greater(t, counts[0], counts[n-1],
		"low indices must be favoured over high indices")

	// n <= 1 is the degenerate single-DID case: always index 0, no RNG draws.
	require.Equal(t, 0, skewedIndex(rand.New(rand.NewPCG(1, 2)), 1))
	require.Equal(t, 0, skewedIndex(rand.New(rand.NewPCG(1, 2)), 0))
}

func TestBuildSwarmFaultPlanSubscribeDisconnectSchedule(t *testing.T) {
	t.Parallel()

	w := newFaultPlanWorld(t, 12)
	cfg := Config{
		Seed:                456,
		Accounts:            12,
		MinInitialRecords:   1,
		MaxInitialRecords:   4,
		LiveEventsBootstrap: 25,
		LiveEventsSteady:    25,
		FaultMode:           FaultModeSwarm,
	}

	first, err := BuildSwarmFaultPlan(w, cfg)
	require.NoError(t, err)
	second, err := BuildSwarmFaultPlan(w, cfg)
	require.NoError(t, err)

	require.Equal(t, first.SubscribeReposDisconnectThresholds, second.SubscribeReposDisconnectThresholds)
	require.Len(t, first.SubscribeReposDisconnectThresholds, 8)
	for _, threshold := range first.SubscribeReposDisconnectThresholds {
		require.GreaterOrEqual(t, threshold, 2)
		require.Less(t, threshold, cfg.LiveEventsSteady,
			"threshold must be below the deliverable steady-state frame budget")
		require.LessOrEqual(t, threshold, cfg.LiveEventsSteady/2,
			"threshold must be capped so the first disconnect is guaranteed to fire")
	}
	require.Greater(t, distinctInts(first.SubscribeReposDisconnectThresholds), 1,
		"exponential schedule should not collapse to a constant")
}

func TestBuildSubscribeReposDisconnectScheduleRejectsDegenerateSteadyCounts(t *testing.T) {
	t.Parallel()

	// steady=5 is the last rejected value: maxThreshold=2, floor=2, so
	// maxThreshold <= floor. steady=6 is the first accepted value (pinned in
	// the Accepts test below), so this exactly straddles the transition.
	for _, steady := range []int{1, 2, 3, 4, 5} {
		t.Run(fmt.Sprintf("steady_%d", steady), func(t *testing.T) {
			t.Parallel()
			_, err := buildSubscribeReposDisconnectSchedule(123, steady)
			require.Error(t, err)
		})
	}
}

func TestBuildSubscribeReposDisconnectScheduleAcceptsSupportedModes(t *testing.T) {
	t.Parallel()

	// steady=6 is the first accepted value (maxThreshold=3 > floor=2); 12
	// covers the fast preset, while 25 and 5000 cover the full schedule.
	for _, steady := range []int{6, 12, 25, 5000} {
		t.Run(fmt.Sprintf("steady_%d", steady), func(t *testing.T) {
			t.Parallel()
			got, err := buildSubscribeReposDisconnectSchedule(123, steady)
			require.NoError(t, err)
			wantLen := subscribeDisconnectScheduleK
			if steady < 25 {
				wantLen = max(1, steady/4)
			}
			require.Len(t, got, wantLen)
		})
	}
}

// TestBuildSwarmFaultPlanSingleDIDWorld covers the edge case where the
// world has only one account: there is no distinct secondary DID, so the
// planner faults only the hot DID and does not loop forever searching for
// a second one.
func TestBuildSwarmFaultPlanSingleDIDWorld(t *testing.T) {
	t.Parallel()

	w := newFaultPlanWorld(t, 1)
	cfg := Config{
		Seed:             123,
		Accounts:         1,
		LiveEventsSteady: 25,
		FaultMode:        FaultModeSwarm,
	}

	plan, err := BuildSwarmFaultPlan(w, cfg)
	require.NoError(t, err)
	require.Len(t, plan.GetRepoHTTPFailures, 1, "single-DID world raw-HTTP faults only the hot DID")
	require.Equal(t, 1, plan.TotalGetRepoHTTPFailures())
	require.Len(t, plan.GetRepoResponseFailures, 1, "single-DID world typed-response faults only the hot DID")
	require.Equal(t, 1, plan.TotalGetRepoResponseFailures())
	require.Len(t, plan.GetRepoCARTruncations, 1, "single-DID world truncates only the hot DID")
	require.Equal(t, 1, plan.TotalGetRepoCARTruncations())
}

func TestBuildSwarmFaultPlanSkipsRepoUnavailableDIDs(t *testing.T) {
	t.Parallel()

	w := newFaultPlanWorld(t, 8)
	for idx := range 7 {
		require.NoError(t, w.SetRepoUnavailableForTest(idx, "suspended"))
	}
	acct, err := w.LoadAccount(7)
	require.NoError(t, err)

	cfg := Config{
		Seed:             123,
		Accounts:         8,
		LiveEventsSteady: 25,
		FaultMode:        FaultModeSwarm,
	}
	plan, err := BuildSwarmFaultPlan(w, cfg)
	require.NoError(t, err)
	require.Equal(t, map[string]int{string(acct.DID): 1}, plan.GetRepoHTTPFailures)
	require.Equal(t, map[string]int{string(acct.DID): 1}, plan.GetRepoResponseFailures)
	require.Equal(t, map[string]int{string(acct.DID): 1}, plan.GetRepoCARTruncations)
}

func TestBuildSwarmFaultPlanNoopsWhenFaultModeNone(t *testing.T) {
	t.Parallel()

	w := newFaultPlanWorld(t, 4)
	cfg := Config{
		Seed:      123,
		Accounts:  4,
		FaultMode: FaultModeNone,
	}

	plan, err := BuildSwarmFaultPlan(w, cfg)
	require.NoError(t, err)
	require.Empty(t, plan.GetRepoHTTPFailures)
	require.Zero(t, plan.TotalGetRepoHTTPFailures())
	require.Empty(t, plan.GetRepoResponseFailures)
	require.Zero(t, plan.TotalGetRepoResponseFailures())
	require.Empty(t, plan.GetRepoCARTruncations)
	require.Zero(t, plan.TotalGetRepoCARTruncations())
}

func newFaultPlanWorld(t *testing.T, accounts int) *world.World {
	t.Helper()

	cfg := world.DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Accounts = accounts
	cfg.InitialRecords = 1
	w, err := world.New(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	_, err = w.EnsureSeed()
	require.NoError(t, err)
	require.NoError(t, w.Bootstrap(context.Background(), slog.Default()))
	return w
}

func distinctInts(values []int) int {
	seen := map[int]struct{}{}
	for _, v := range values {
		seen[v] = struct{}{}
	}
	return len(seen)
}
