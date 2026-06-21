package oracle

import (
	"fmt"
	"math/rand/v2"

	"github.com/jcalabro/atmos"
)

// chainShape names a pinned durable-intermediate shape. Every shape in
// pinnedShapes is generated on every seed so no post-restart assertion
// is ever vacuous; the seed only varies the specifics (which account,
// collection, rkey, payload) — see specs/notes/2026-06-20-restart-tier-
// intermediates-plan.md §2.1.
type chainShape string

const (
	// shapeLiveCUD is R_live create→update→delete: a record born in the
	// live window (no backfill create), fully mutated then tombstoned.
	// Exercises lost-intermediate coverage and the compaction contract.
	shapeLiveCUD chainShape = "live-create-update-delete"
	// shapeLiveDeleteRecreate is R_live create→delete→recreate on the
	// SAME rkey: the no-permanent-tombstone fixture. The recreate above
	// the tombstone must reconstruct as visible.
	shapeLiveDeleteRecreate chainShape = "live-delete-recreate"
	// shapeBfCreateUpdate is R_bf create(@backfill)→update(live): the
	// record exists at backfill (its create lands as a KindCreate at the
	// repo head rev) and is then superseded by a live update at a HIGHER
	// rev. Exercises the merge rev-filter survival boundary — the live
	// update must survive (rev > BackfillRev) and supersede the
	// backfilled create at compaction.
	shapeBfCreateUpdate chainShape = "bf-create-update"
	// shapeBfCreateDelete is R_bf create(@backfill)→delete(live): the
	// backfilled create is tombstoned by a live delete (rev > BackfillRev).
	// The tombstone survives the merge and supersedes the backfilled
	// create at compaction; final state is absent. The convergence-hiding
	// lost-CREATE power (§180-182) needs the create to survive uncompacted
	// (straddle create≤W / delete>W) and is delivered by B-crash, not the
	// no-crash path.
	shapeBfCreateDelete chainShape = "bf-create-delete"
)

// chainOrigin records whether a chain record existed at backfill (R_bf)
// or is born entirely in the live window (R_live). It governs which of a
// record's ops are generated pre-spawn (the backfill seed) vs. in the
// getRepo-served hook (the durable intermediates).
type chainOrigin string

const (
	originLive     chainOrigin = "live"
	originBackfill chainOrigin = "backfill"
)

// pinnedShapes is the always-present set: every shape is generated on
// every seed so no post-restart assertion is ever vacuous. Per-shape
// issues (A,B,C,D,F,…) extend this set as they land.
var pinnedShapes = []chainShape{shapeLiveCUD, shapeLiveDeleteRecreate, shapeBfCreateUpdate, shapeBfCreateDelete}

// recordChain is one record's full op sequence on a single
// (accountIdx, collection, rkey). origin records whether the record
// existed at backfill (R_bf) or is born live (R_live).
type recordChain struct {
	shape      chainShape
	origin     chainOrigin
	accountIdx int
	collection string
	rkey       string
	// ops is the ordered action sequence (create/update/delete). For
	// shapeLiveDeleteRecreate the final create reuses rkey. For an R_bf
	// record, ops[0] is the backfill seed (generated pre-spawn) and the
	// rest are durable live intermediates.
	ops []string
}

// backfillOps are the ops generated BEFORE the child spawns, so they are
// captured by the getRepo snapshot at the repo head rev. For R_bf that is
// the seed create; for R_live there are none.
func (rc recordChain) backfillOps() []string {
	if rc.origin == originBackfill && len(rc.ops) > 0 {
		return rc.ops[:1]
	}
	return nil
}

// liveOps are the durable intermediates generated AFTER getRepo is served
// (rev > BackfillRev). These are the rows event-log coverage tracks.
func (rc recordChain) liveOps() []string {
	if rc.origin == originBackfill && len(rc.ops) > 0 {
		return rc.ops[1:]
	}
	return rc.ops
}

// chainSpec is the full seed-derived plan of durable intermediates to
// inject after the chain DID's getRepo is served. It is a pure function
// of the seed (deriveChainSpec): same seed → identical spec (so a CI
// failure replays exactly); different seed → different specifics (so the
// sweep explores the state space).
type chainSpec struct {
	seed    uint64
	records []recordChain
}

// chainCollections is the set of collections a chain record may use.
// Kept independent of the world package's unexported weights so the spec
// stays a pure seed→plan function with no world dependency.
var chainCollections = []string{
	"app.bsky.feed.post",
	"app.bsky.feed.like",
	"app.bsky.graph.follow",
	"app.bsky.feed.repost",
}

// deriveChainSpec builds the seed-derived chain plan. accounts is the
// number of accounts in the world; the chain DID is chosen from
// [0,accounts) by the seed. It is pure and deterministic in (seed,
// accounts). Every shape in pinnedShapes is always present.
func deriveChainSpec(seed uint64, accounts int) chainSpec {
	rng := rand.New(rand.NewPCG(seed^0x6c6976656368, seed^0x636861696e73))

	// Pick a single chain DID (the host of every record chain). Keeping
	// all chains on one DID keeps intra-DID rev ordering simple and
	// leaves the other DIDs as pure-backfill regression witnesses.
	chainAccountIdx := rng.IntN(max(1, accounts))

	spec := chainSpec{seed: seed}
	for _, shape := range pinnedShapes {
		coll := chainCollections[rng.IntN(len(chainCollections))]
		rkey := deriveChainRkey(rng)
		var ops []string
		origin := originLive
		switch shape {
		case shapeLiveCUD:
			ops = []string{"create", "update", "delete"}
		case shapeLiveDeleteRecreate:
			ops = []string{"create", "delete", "create"}
		case shapeBfCreateUpdate:
			// Seed create lands at backfill; the live update is the
			// durable intermediate that must survive the rev-filter.
			origin = originBackfill
			ops = []string{"create", "update"}
		case shapeBfCreateDelete:
			// Seed create lands at backfill; the live delete tombstone is
			// the durable intermediate. Final state absent.
			origin = originBackfill
			ops = []string{"create", "delete"}
		default:
			panic(fmt.Sprintf("oracle: unhandled pinned chain shape %q", shape))
		}
		spec.records = append(spec.records, recordChain{
			shape:      shape,
			origin:     origin,
			accountIdx: chainAccountIdx,
			collection: coll,
			rkey:       rkey,
			ops:        ops,
		})
	}
	return spec
}

// chainAccountIdx returns the (single) account hosting the chains.
func (s chainSpec) chainAccountIdx() int {
	if len(s.records) == 0 {
		return 0
	}
	return s.records[0].accountIdx
}

// deriveChainRkey returns a fresh TID-shaped rkey from the chain RNG, so
// rkeys vary by seed but replay identically. Mirrors world.newRkey's TID
// construction without depending on the world package.
func deriveChainRkey(rng *rand.Rand) string {
	micros := int64(2000)*int64(3600)*1_000_000 + rng.Int64N(1<<40)
	clockID := rng.UintN(1024)
	return string(atmos.NewTID(micros, clockID))
}
