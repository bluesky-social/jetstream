package oracle

import (
	"fmt"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/bluesky-social/jetstream/internal/crashpoint"
)

// #29: seeded, predicate-driven restart kill points.
//
// The fixed crash tiers (TestOracle_RestartCrashPointsDoNotLoseRecords,
// TestOracle_RestartChainCrashConsistency) always kill at the FIRST hit of a
// hardcoded crashpoint. This tier instead chooses the kill point by a seeded
// predicate over a *trace-event-count* coordinate — the (crashpoint, 1-based
// occurrence ordinal) pair — so a crash can land "between" named crashpoints
// (e.g. after the 3rd repo completes), exploring interleavings the fixed set
// never reaches. Per the issue Notes, occurrence-count kill points are
// preferred over wall-clock timing: they are deterministic given the seed and
// reproduce exactly.
//
// Each selected kill still drives the full durable chain through crash +
// recovery and asserts the same invariants as the fixed tier (final-state
// convergence + chain durability), so a predicate-selected interleaving that
// loses or corrupts a durable intermediate fails loudly.

// killOption is one entry in the predicate's selection space: a crashpoint and
// the maximum occurrence ordinal that is reliably reachable for it in the
// restart-chain scenario (Accounts=4, single source segment). maxOrdinal is
// kept conservative so a seeded pick is always reachable — an unreachable
// ordinal would hang the child until timeout, which the harness surfaces, but
// we do not want a flaky kill selection.
type killOption struct {
	point      crashpoint.Point
	maxOrdinal int
}

// killSpace is the reachable (crashpoint, ordinal) selection space.
//   - AfterRepoComplete fires once per backfilled repo (Accounts=4 → ordinals
//     1..4), the canonical trace-event-count predicate.
//   - The merge/bootstrap/compaction seams fire once per run in this scenario
//     (single source segment), so ordinal is pinned to 1; they still vary the
//     PHASE the kill lands in, which is the other predicate dimension the issue
//     asks for.
var killSpace = []killOption{
	{point: crashpoint.AfterRepoComplete, maxOrdinal: 4},
	{point: crashpoint.AfterBootstrapLiveCloseBeforeSeal, maxOrdinal: 1},
	{point: crashpoint.AfterMergeDstFlushBeforeSourceCommit, maxOrdinal: 1},
	{point: crashpoint.AfterMergeDstSealBeforeDiscovery, maxOrdinal: 1},
	{point: crashpoint.AfterCompactionRewriteBeforeWatermark, maxOrdinal: 1},
}

// killDecision is a fully-resolved predicate selection: which crashpoint and
// which occurrence. It is a pure function of the seed (selectKill), so a CI
// failure replays the exact same kill.
type killDecision struct {
	point   crashpoint.Point
	ordinal int
}

func (d killDecision) String() string {
	return fmt.Sprintf("%s#%d", d.point, d.ordinal)
}

// selectKill is the seeded predicate: it deterministically maps a seed to a
// (crashpoint, ordinal) kill decision over killSpace. Same seed → identical
// decision; different seeds explore different points and occurrences.
func selectKill(seed uint64) killDecision {
	rng := rand.New(rand.NewPCG(seed^0x6b696c6c70, seed^0x7072656431))
	opt := killSpace[rng.IntN(len(killSpace))]
	ordinal := 1 + rng.IntN(opt.maxOrdinal)
	return killDecision{point: opt.point, ordinal: ordinal}
}

// TestOracle_RestartPredicateKill drives a seeded predicate-selected kill
// through the durable chain crash + recovery, then asserts the full chain
// durability bundle. It runs ONE deterministic decision for push CI (fixed
// seed, so the selection and the failing repro are stable), and a multi-kill
// sweep when JETSTREAM_ORACLE_SEED is set (nightly/manual), each iteration
// drawing a fresh decision so the sweep explores the kill space.
//
// nolint:paralleltest
func TestOracle_RestartPredicateKill(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping restart oracle under -short")
	}

	for i, seed := range predicateKillSeeds() {
		decision := selectKill(seed)
		t.Run(decision.String(), func(t *testing.T) {
			label := fmt.Sprintf("predicate-kill-%s-seed%d", decision, seed)
			t.Logf("predicate kill decision: seed=%d point=%s ordinal=%d", seed, decision.point, decision.ordinal)

			run := runChainThroughCrashAt(t, label, predicateSeedIdx(i, seed), decision.point, decision.ordinal)

			// The predicate-selected interleaving must still converge and keep
			// every durable intermediate (replay-aware: a merge-boundary crash
			// may re-emit survivors out of per-DID rev order — benign).
			assertOracleMatchesAfterReplay(t, run.dataDir, run.w, run.cfg, label)
			assertChainDurable(t, run.dataDir, run.coord, label)

			// Red-first power: the shape-B delete tombstone must be present and
			// load-bearing for coverage at this kill point too.
			rc := recordChainForShape(t, run.spec, shapeBfCreateDelete)
			cov := chainCoverage(t, run.dataDir, run.coord)
			assertCoverageFailsWithoutRow(t, cov, "delete", rc.collection, rc.rkey)
		})
	}
}

// predicateKillSeeds returns the seeds the predicate-kill tier sweeps. Without
// JETSTREAM_ORACLE_SEED it returns a single fixed seed so push CI runs one
// deterministic kill; with it set, it returns that seed plus a small
// deterministic fan (seed, seed+1, ...) so a nightly/manual run does multiple
// random kills (DoD: "nightly/manual mode can run multiple random kills").
func predicateKillSeeds() []uint64 {
	const fixed = 0x29ce11 // stable push-CI seed (issue #29)
	raw, ok := os.LookupEnv(envOracleSeed)
	if !ok || raw == "" {
		return []uint64{fixed}
	}
	var base uint64
	if err := parseUint64Env(func(string) (string, bool) { return raw, true }, envOracleSeed, &base); err != nil {
		return []uint64{fixed}
	}
	const sweep = 5
	seeds := make([]uint64, sweep)
	for i := range seeds {
		seeds[i] = base + uint64(i)
	}
	return seeds
}

// predicateSeedIdx derives the world/chain seed index for iteration i. It must
// vary per iteration (so each kill runs a different world) while staying a pure
// function of (i, seed) for replay. restartSeed honors JETSTREAM_ORACLE_SEED;
// offsetting by i keeps successive sweep iterations on distinct worlds.
func predicateSeedIdx(i int, _ uint64) int {
	return i
}

// TestSelectKillIsDeterministicAndInBounds locks the predicate contract without
// the subprocess cost: same seed → identical decision, the chosen ordinal is
// always within the crashpoint's reachable [1, maxOrdinal], and over many seeds
// the predicate actually exercises more than one crashpoint and more than one
// ordinal (anti-vacuity — a constant selector would defeat the tier's purpose).
func TestSelectKillIsDeterministicAndInBounds(t *testing.T) {
	t.Parallel()

	maxByPoint := make(map[crashpoint.Point]int, len(killSpace))
	for _, opt := range killSpace {
		maxByPoint[opt.point] = opt.maxOrdinal
	}

	seenPoints := make(map[crashpoint.Point]struct{})
	seenOrdinalsAbove1 := false
	for s := range 500 {
		seed := uint64(s)
		d := selectKill(seed)
		// Deterministic.
		if again := selectKill(seed); again != d {
			t.Fatalf("selectKill(%d) not deterministic: %v vs %v", seed, d, again)
		}
		// In a known crashpoint with a reachable ordinal.
		maxOrd, ok := maxByPoint[d.point]
		if !ok {
			t.Fatalf("selectKill(%d) chose unknown crashpoint %q", seed, d.point)
		}
		if d.ordinal < 1 || d.ordinal > maxOrd {
			t.Fatalf("selectKill(%d) ordinal %d out of [1,%d] for %q", seed, d.ordinal, maxOrd, d.point)
		}
		seenPoints[d.point] = struct{}{}
		if d.ordinal > 1 {
			seenOrdinalsAbove1 = true
		}
	}
	if len(seenPoints) < 2 {
		t.Fatalf("predicate exercises only %d crashpoint(s); expected variety across the kill space", len(seenPoints))
	}
	if !seenOrdinalsAbove1 {
		t.Fatal("predicate never selected an ordinal > 1; the trace-event-count dimension is unexercised")
	}
}
