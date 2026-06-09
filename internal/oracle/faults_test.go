package oracle

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/simulator/world"
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

	// Exact swarm contract for a multi-DID world: precisely two distinct
	// DIDs, one "hot" with 2 failures and one secondary with 1. Asserting
	// the exact shape (not just ">= 2 DIDs" / "max > 1") pins the planner
	// so a regression that, say, faulted every DID once or collapsed both
	// onto a single DID would fail here.
	require.Len(t, first.GetRepoHTTPFailures, 2, "swarm faults exactly two distinct DIDs")
	require.Equal(t, 3, first.TotalGetRepoHTTPFailures(), "2 (hot) + 1 (secondary)")

	counts := make([]int, 0, 2)
	for _, c := range first.GetRepoHTTPFailures {
		counts = append(counts, c)
	}
	slices.Sort(counts)
	require.Equal(t, []int{1, 2}, counts, "one hot DID (2 failures) and one secondary DID (1 failure)")
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

	// steady=6 is the first accepted value (maxThreshold=3 > floor=2); 25 and
	// 5000 cover the fast and stress presets.
	for _, steady := range []int{6, 25, 5000} {
		t.Run(fmt.Sprintf("steady_%d", steady), func(t *testing.T) {
			t.Parallel()
			got, err := buildSubscribeReposDisconnectSchedule(123, steady)
			require.NoError(t, err)
			require.Len(t, got, 8)
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
	require.Len(t, plan.GetRepoHTTPFailures, 1, "single-DID world faults only the hot DID")
	require.Equal(t, 2, plan.TotalGetRepoHTTPFailures())
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
