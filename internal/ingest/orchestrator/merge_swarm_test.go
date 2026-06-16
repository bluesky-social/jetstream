package orchestrator

import (
	"errors"
	"math/rand/v2"
	"strconv"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/crashpoint"
	"github.com/bluesky-social/jetstream-v2/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/stretchr/testify/require"
)

const (
	swarmSmokeIters = 10
	swarmFullIters  = 1000
)

// TestMerge_Swarm generates random scenarios and asserts the merge
// preserves the spec's invariants under variation in: event count,
// per-DID rev sequences, BackfillRev cutoffs, source-segment splits,
// and kill-point injections.
//
//nolint:paralleltest
func TestMerge_Swarm(t *testing.T) {
	iters := swarmFullIters
	if testing.Short() {
		iters = swarmSmokeIters
	}

	for i := range iters {
		t.Run("iter-"+strconv.Itoa(i), func(t *testing.T) {
			runSwarmIteration(t, rand.New(rand.NewPCG(uint64(i+1), uint64(i+2))))
		})
	}
}

// scenario captures one swarm-generated case so failures can be
// re-run deterministically by reusing the same seed.
type scenario struct {
	dids          []string
	backfillRevs  map[string]string
	sourceEvents  [][]segment.Event
	expectSurvive map[string]int    // did → minimum count of survivors expected
	lastSrcRev    map[string]string // did → last surviving rev in source iteration order
}

func generateScenario(rng *rand.Rand) scenario {
	const dids = 5
	s := scenario{backfillRevs: map[string]string{}, expectSurvive: map[string]int{}, lastSrcRev: map[string]string{}}
	for i := range dids {
		s.dids = append(s.dids, "did:plc:"+strconv.Itoa(i))
		// Random BackfillRev cutoff (or no cutoff for ~25% of DIDs).
		if rng.IntN(4) != 0 {
			s.backfillRevs[s.dids[i]] = "rev-" + paddedHex(rng.IntN(100))
		}
	}

	totalEvents := 50 + rng.IntN(450)
	srcCount := 1 + rng.IntN(3)
	perSrc := make([][]segment.Event, srcCount)
	revCounters := map[string]int{}

	for k := range totalEvents {
		did := s.dids[rng.IntN(len(s.dids))]
		revCounters[did] += 1 + rng.IntN(5)
		rev := "rev-" + paddedHex(revCounters[did])
		var kind segment.Kind
		switch rng.IntN(7) {
		case 0:
			kind = segment.KindCreate
		case 1:
			kind = segment.KindUpdate
		case 2:
			kind = segment.KindDelete
		case 3:
			kind = segment.KindCreateResync
		case 4:
			kind = segment.KindIdentity
		case 5:
			kind = segment.KindAccount
		default:
			kind = segment.KindSync
		}
		ev := segment.Event{
			IndexedAt:  int64(1000 + k),
			Kind:       kind,
			DID:        did,
			Collection: "app.bsky.feed.post",
			Rkey:       "rkey-" + strconv.Itoa(k),
			Rev:        rev,
			Payload:    []byte("p"),
		}
		// Non-commit kinds carry no rev in production.
		if kind == segment.KindIdentity || kind == segment.KindAccount || kind == segment.KindSync {
			ev.Rev = ""
		}
		srcIdx := rng.IntN(srcCount)
		perSrc[srcIdx] = append(perSrc[srcIdx], ev)

		// Predict survival.
		survives := true
		if isCommitKind(kind) {
			if cutoff, ok := s.backfillRevs[did]; ok && rev != "" && rev <= cutoff {
				survives = false
			}
		}
		if survives {
			s.expectSurvive[did]++
		}
	}
	s.sourceEvents = perSrc

	// Compute expected per-DID last surviving rev by simulating the merge
	// runner's iteration order: process each source segment in order, and
	// within each segment, process events in their stored order.
	for _, srcEvs := range perSrc {
		for i := range srcEvs {
			ev := &srcEvs[i]
			if !isCommitKind(ev.Kind) || ev.Rev == "" {
				continue
			}
			cutoff, ok := s.backfillRevs[ev.DID]
			if ok && ev.Rev <= cutoff {
				continue // dropped
			}
			s.lastSrcRev[ev.DID] = ev.Rev
		}
	}
	return s
}

func paddedHex(n int) string {
	const w = 5
	out := strconv.FormatInt(int64(n), 36)
	for len(out) < w {
		out = "0" + out
	}
	return out
}

func runSwarmIteration(t *testing.T, rng *rand.Rand) {
	s := generateScenario(rng)
	fix := newMergeFixture(t, s.sourceEvents, s.backfillRevs)

	require.NoError(t, lifecycle.WritePhase(fix.store, lifecycle.PhaseMerging, time.Now().UTC()))

	// 30% chance of a kill-point injection on the flush-before-commit path.
	// On crash, restart and run merge to completion.
	if rng.IntN(10) < 3 {
		fix.cfg.CrashInjector = pointErrorInjector{
			point: crashpoint.AfterMergeDstFlushBeforeSourceCommit,
			err:   errors.New("swarm kill"),
		}
		o, err := New(fix.cfg)
		require.NoError(t, err)
		// Assert the checkpoint actually fired. Without this, a scenario
		// that drains zero source segments (e.g. every event filtered) —
		// or a regression that deletes the simulateCrash call entirely —
		// would silently skip the crash and make the recovery assertions
		// below vacuous while still passing.
		require.ErrorContains(t, o.runMerge(t.Context()), "swarm kill",
			"crash must fire at AfterMergeDstFlushBeforeSourceCommit")
		fix.cfg.CrashInjector = nil
	}

	o, err := New(fix.cfg)
	require.NoError(t, err)
	require.NoError(t, o.runMerge(t.Context()))

	allEvs := readDestEvents(t, fix.dataDir)

	// Invariant 1: every expected survivor present at least once.
	gotByDID := map[string]int{}
	for _, e := range allEvs {
		gotByDID[e.DID]++
		// Invariant 2: no commit event with rev <= BackfillRev.
		if isCommitKind(e.Kind) && e.Rev != "" {
			if cutoff, ok := s.backfillRevs[e.DID]; ok {
				require.Greater(t, e.Rev, cutoff, "leaked covered commit %s/%s", e.DID, e.Rev)
			}
		}
	}
	for did, want := range s.expectSurvive {
		require.GreaterOrEqual(t, gotByDID[did], want, "missing survivors for %s", did)
	}

	// Invariant 3: strict monotonic seqs across all destination events.
	for i := 1; i < len(allEvs); i++ {
		require.Greater(t, allEvs[i].Seq, allEvs[i-1].Seq)
	}

	// Invariant 4: cursors absent.
	cur, err := loadMergeCursor(fix.store)
	require.NoError(t, err)
	require.Equal(t, uint64(0), cur)

	// Invariant 5+6: per-DID Rev advanced; Backfill.Rev unchanged.
	for did, lastRev := range s.lastSrcRev {
		val, closer, err := fix.store.Get(backfill.RepoKey(did))
		if errors.Is(err, store.ErrNotFound) {
			continue
		}
		require.NoError(t, err)
		rs, err := backfill.DecodeRepoStatus(val)
		_ = closer.Close()
		require.NoError(t, err)
		if origBF, ok := s.backfillRevs[did]; ok {
			require.Equal(t, origBF, rs.Backfill.Rev, "Backfill.Rev mutated for %s", did)
			if lastRev > origBF {
				require.Equal(t, lastRev, rs.Rev, "top-level Rev should reflect last surviving for %s", did)
			}
		} else {
			require.Equal(t, lastRev, rs.Rev, "top-level Rev should reflect last surviving for %s", did)
		}
	}

	// Invariant 7: every survivor has IndexedAt > max source IndexedAt.
	// generator caps total events at 50 + IntN(450) = 499.
	// IndexedAts are 1000..1000+totalEvents-1, so the strict upper
	// bound is 1499. Use 1499 directly so an off-by-one regression
	// here surfaces as a test failure.
	const maxSrcIndexedAt int64 = 1499
	for _, e := range allEvs {
		require.Greater(t, e.IndexedAt, maxSrcIndexedAt)
	}
}
