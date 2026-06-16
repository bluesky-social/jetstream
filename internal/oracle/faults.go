package oracle

import (
	"fmt"
	"math"
	"math/rand/v2"
	"net/http"

	simhttp "github.com/bluesky-social/jetstream-v2/internal/simulator/http"
	"github.com/bluesky-social/jetstream-v2/internal/simulator/world"
)

// oracleFaultSeedSalt derives the fault planner's RNG seed from cfg.Seed
// (see BuildSwarmFaultPlan) so the fault schedule is decoupled from every
// other oracle/simulator RNG that also keys off cfg.Seed — otherwise two
// independent stochastic processes seeded identically could correlate.
// The specific value is arbitrary; any fixed non-zero constant works.
// Changing it reshuffles which DIDs get faulted for a given seed, so
// don't change it casually if a failing seed is being bisected.
const oracleFaultSeedSalt uint64 = 0xf00d_0f17_1eaf_cafe

const oracleSubscribeFaultSeedSalt uint64 = 0x5ab5_c41b_51e7_f00d

const (
	subscribeDisconnectScheduleK    = 8
	subscribeDisconnectMaxScheduleK = 64
)

// Compile-time assertion that the schedule length stays within its ceiling.
// This fails to compile (a negative constant converted to uint) if
// subscribeDisconnectScheduleK is ever raised above
// subscribeDisconnectMaxScheduleK, which would let a single oracle run arm an
// unreasonable number of disconnect thresholds.
const _ = uint(subscribeDisconnectMaxScheduleK - subscribeDisconnectScheduleK)

// SwarmFaultPlan is the oracle's deterministic fault schedule plus the
// simulator plan used to enforce it. GetRepoHTTPFailures and
// GetRepoCARTruncations are the oracle's authoritative records of what was
// scheduled (DID -> failure count); SimulatorFaults is the live plan the HTTP
// handler consults and that counts how many of those failures actually fired.
type SwarmFaultPlan struct {
	SimulatorFaults                    *simhttp.FaultPlan
	GetRepoHTTPFailures                map[string]int
	GetRepoCARTruncations              map[string]int
	SubscribeReposDisconnectThresholds []int
}

// BuildSwarmFaultPlan builds the deterministic fault schedule for an
// oracle run. FaultModeNone returns an empty plan (no DIDs scheduled).
// FaultModeSwarm schedules a small, bounded set of transient getRepo failures:
// two 503s plus one truncated CAR body on a hot DID, and one 503 on a distinct
// secondary DID when the world has more than one account. The budget is
// deliberately inside atmos's default retry count so every faulted repo still
// completes, leaving the durable model identical to the simulator world. An
// unknown mode is an error.
func BuildSwarmFaultPlan(w *world.World, cfg Config) (*SwarmFaultPlan, error) {
	plan := &SwarmFaultPlan{
		SimulatorFaults:       simhttp.NewFaultPlan(),
		GetRepoHTTPFailures:   map[string]int{},
		GetRepoCARTruncations: map[string]int{},
	}
	if cfg.FaultMode == FaultModeNone {
		return plan, nil
	}
	if cfg.FaultMode != FaultModeSwarm {
		return nil, fmt.Errorf("oracle: unknown fault mode %q", cfg.FaultMode)
	}

	subscribeThresholds, err := buildSubscribeReposDisconnectSchedule(cfg.Seed, cfg.LiveEventsSteady)
	if err != nil {
		return nil, err
	}
	plan.SubscribeReposDisconnectThresholds = subscribeThresholds

	dids, err := worldDIDs(w)
	if err != nil {
		return nil, err
	}
	if len(dids) == 0 {
		return plan, nil
	}

	rng := rand.New(rand.NewPCG(cfg.Seed^oracleFaultSeedSalt, cfg.Seed+oracleFaultSeedSalt))
	hot := dids[skewedIndex(rng, len(dids))]
	plan.addGetRepoHTTPFailures(hot, 2)
	plan.addGetRepoCARTruncations(hot, 1)

	if len(dids) > 1 {
		secondary := hot
		for secondary == hot {
			secondary = dids[skewedIndex(rng, len(dids))]
		}
		plan.addGetRepoHTTPFailures(secondary, 1)
	}

	return plan, nil
}

// ArmSubscribeReposDisconnects installs the plan's subscribeRepos disconnect
// thresholds into the live simulator fault plan. It is a no-op if the plan or
// its simulator faults are nil.
func (p *SwarmFaultPlan) ArmSubscribeReposDisconnects() {
	if p == nil || p.SimulatorFaults == nil {
		return
	}
	p.SimulatorFaults.SetSubscribeReposDisconnectSchedule(p.SubscribeReposDisconnectThresholds)
}

func buildSubscribeReposDisconnectSchedule(seed uint64, steadyEvents int) ([]int, error) {
	const k = subscribeDisconnectScheduleK
	// floor is clamped to >= 2 so a threshold of 0 or 1 can never make the
	// fault fire before the consumer has made progress (a livelock boundary).
	// maxThreshold caps each draw at half the steady-state budget so the first
	// disconnect is guaranteed to fire within the deliverable frames.
	floor := max(2, steadyEvents/16)
	maxThreshold := steadyEvents / 2
	mean := float64(steadyEvents) / 4
	// This single guard is the binding constraint: maxThreshold = steadyEvents/2
	// and floor = max(2, steadyEvents/16), so maxThreshold <= floor exactly when
	// steadyEvents is too small to leave room between the livelock floor and the
	// half-budget cap (steadyEvents <= 5). It also subsumes the maxThreshold > 0
	// and mean > 0 conditions, since clearing it forces steadyEvents >= 6.
	if maxThreshold <= floor {
		return nil, fmt.Errorf("oracle: subscribeRepos disconnect cap %d must be > floor %d (steady events %d too small)",
			maxThreshold, floor, steadyEvents)
	}

	rng := rand.New(rand.NewPCG(seed^oracleSubscribeFaultSeedSalt, seed+oracleSubscribeFaultSeedSalt))
	out := make([]int, 0, k)
	for range k {
		draw := int(math.Round(rng.ExpFloat64() * mean))
		draw = max(floor, min(maxThreshold, draw))
		out = append(out, draw)
	}
	return out, nil
}

func (p *SwarmFaultPlan) addGetRepoHTTPFailures(did string, count int) {
	p.GetRepoHTTPFailures[did] += count
	p.SimulatorFaults.AddGetRepoHTTPFailures(did, http.StatusServiceUnavailable, count)
}

func (p *SwarmFaultPlan) addGetRepoCARTruncations(did string, count int) {
	p.GetRepoCARTruncations[did] += count
	p.SimulatorFaults.AddGetRepoCARTruncations(did, count)
}

// TotalGetRepoHTTPFailures returns the total number of scheduled getRepo HTTP
// failures across all DIDs. It returns 0 for a nil plan.
func (p *SwarmFaultPlan) TotalGetRepoHTTPFailures() int {
	if p == nil {
		return 0
	}
	var total int
	for _, count := range p.GetRepoHTTPFailures {
		total += count
	}
	return total
}

// TotalGetRepoCARTruncations returns the total number of scheduled getRepo CAR
// truncation faults across all DIDs. It returns 0 for a nil plan.
func (p *SwarmFaultPlan) TotalGetRepoCARTruncations() int {
	if p == nil {
		return 0
	}
	var total int
	for _, count := range p.GetRepoCARTruncations {
		total += count
	}
	return total
}

// UnfiredGetRepoHTTPFailures returns, per DID, how many scheduled getRepo
// HTTP faults have NOT yet fired (want - got). A DID whose faults all
// fired (got == want) is omitted, so an empty result means every
// scheduled fault fired — that is the success condition the harness
// asserts. A negative entry would mean a DID fired MORE faults than
// scheduled, which can only happen if the same plan is reused across
// runs; it is surfaced rather than hidden so such misuse fails loudly.
// Call this only after backfill has drained (see assertFaultPlanFired):
// the per-DID counts it reads are still being mutated by getRepo workers
// until then.
func (p *SwarmFaultPlan) UnfiredGetRepoHTTPFailures() map[string]int {
	out := map[string]int{}
	if p == nil {
		return out
	}
	for did, want := range p.GetRepoHTTPFailures {
		got := p.SimulatorFaults.GetRepoHTTPFailuresFired(did)
		if got != want {
			out[did] = want - got
		}
	}
	return out
}

// UnfiredGetRepoCARTruncations returns, per DID, how many scheduled getRepo
// CAR truncation faults have NOT yet fired (want - got). See
// UnfiredGetRepoHTTPFailures for the assertion semantics.
func (p *SwarmFaultPlan) UnfiredGetRepoCARTruncations() map[string]int {
	out := map[string]int{}
	if p == nil {
		return out
	}
	for did, want := range p.GetRepoCARTruncations {
		got := p.SimulatorFaults.GetRepoCARTruncationsFired(did)
		if got != want {
			out[did] = want - got
		}
	}
	return out
}

func worldDIDs(w *world.World) ([]string, error) {
	page, _, err := w.ListReposPage(0, w.AccountCount())
	if err != nil {
		return nil, fmt.Errorf("oracle: list simulator DIDs for fault plan: %w", err)
	}
	dids := make([]string, 0, len(page))
	for _, entry := range page {
		dids = append(dids, string(entry.DID))
	}
	return dids, nil
}

// skewedIndex returns an index in [0, n) biased toward 0 by taking the
// minimum of three uniform draws.
//
// This is a stress-targeting heuristic, not a model of getRepo load:
// getRepo is a once-per-DID backfill operation, so unlike the
// simulator's live traffic it has no Zipfian frequency to match. We bias
// toward low-index DIDs only because those are the simulator's busiest
// accounts under its Zipfian live-traffic generator (zipfian() in
// world/distributions.go favours index 0), so faulting them concentrates
// injected failures on the repos that will also see the most steady-state
// activity — the accounts where a backfill/live-recovery interaction is
// most likely to surface. Uniform selection would dilute that signal
// across cold accounts. The bias is intentionally mild; any DID can still
// be chosen.
func skewedIndex(rng *rand.Rand, n int) int {
	if n <= 1 {
		return 0
	}
	idx := rng.IntN(n)
	for range 2 {
		idx = min(idx, rng.IntN(n))
	}
	return idx
}
