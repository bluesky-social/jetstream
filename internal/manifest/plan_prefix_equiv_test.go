package manifest_test

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// nsidUniverse is a fixed set of collections spanning several overlapping
// namespaces. Prefix equivalence must hold for every namespace boundary in it.
var nsidUniverse = []string{
	"app.bsky.feed.post",
	"app.bsky.feed.like",
	"app.bsky.feed.repost",
	"app.bsky.graph.follow",
	"app.bsky.graph.block",
	"app.bsky.actor.profile",
	"com.example.foo.bar",
	"com.example.foo.baz",
	"com.example.qux.thing",
}

// prefixesUnderTest are namespace prefixes (each ending in ".") to probe. The
// list deliberately mixes broad and narrow prefixes, prefixes that match
// everything, and a prefix that matches nothing in the universe.
var prefixesUnderTest = []string{
	"app.bsky.feed.",
	"app.bsky.graph.",
	"app.bsky.",
	"com.example.foo.",
	"com.example.",
	"com.",
	"net.nonexistent.", // matches nothing
}

// archivedNSIDsUnderPrefix returns every universe NSID covered by prefix.
func archivedNSIDsUnderPrefix(prefix string) []string {
	var out []string
	for _, nsid := range nsidUniverse {
		if strings.HasPrefix(nsid, prefix) {
			out = append(out, nsid)
		}
	}
	return out
}

// TestPlanBackfill_PrefixEquivalentToEnumeratedExact is the core correctness
// property for wildcard support: for any archive and any namespace prefix P,
// planning with CollectionPrefixes=[P] must produce a byte-identical plan
// (segments, block ranges, modes, and stats) to planning with the explicit set
// of every archived NSID under P. This directly proves that in-planner prefix
// matching is equivalent to expanding the wildcard against the global
// collection union and exact-matching.
func TestPlanBackfill_PrefixEquivalentToEnumeratedExact(t *testing.T) {
	t.Parallel()

	// Multiple fixed seeds give a small deterministic swarm: each builds a
	// different randomized-but-reproducible archive layout.
	seeds := []int64{1, 2, 3, 7, 42, 1337}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			m := buildRandomArchive(t, seed)

			for _, prefix := range prefixesUnderTest {
				exact := archivedNSIDsUnderPrefix(prefix)

				prefixReq := planReq()
				prefixReq.CollectionPrefixes = []string{prefix}
				prefixPlan, err := m.PlanBackfill(prefixReq)
				require.NoError(t, err)

				if len(exact) == 0 {
					// A prefix that matches no archived NSID must match
					// NOTHING, not everything. The enumerated-exact oracle
					// cannot model this: Collections=[] means match-all, so
					// the equivalence only holds for non-empty expansions.
					// This boundary is precisely where "empty list == all"
					// would be a correctness bug, so we assert match-nothing
					// directly (coverage horizon and examined count are still
					// reported).
					require.Empty(t, prefixPlan.Segments,
						"seed=%d prefix=%q: prefix matching nothing must yield no segments", seed, prefix)
					require.Zero(t, prefixPlan.Stats.SegmentsMatched,
						"seed=%d prefix=%q: prefix matching nothing must match no segments", seed, prefix)
					continue
				}

				exactReq := planReq()
				exactReq.Collections = exact
				exactPlan, err := m.PlanBackfill(exactReq)
				require.NoError(t, err)

				require.Equal(t, exactPlan, prefixPlan,
					"seed=%d prefix=%q: prefix plan must equal enumerated exact plan (exact set=%v)",
					seed, prefix, exact)
			}
		})
	}
}

// buildRandomArchive writes several sealed segments full of events drawn from
// nsidUniverse with reproducible per-seed randomness, then opens a manifest.
func buildRandomArchive(t *testing.T, seed int64) *manifest.Manifest {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	dir := t.TempDir()

	dids := []string{"did:plc:a", "did:plc:b", "did:plc:c", "did:plc:d"}
	numSegments := 2 + rng.Intn(3) // 2..4 segments
	var seq uint64
	for segIdx := range numSegments {
		numEvents := 3 + rng.Intn(8) // 3..10 events per segment
		events := make([]segment.Event, 0, numEvents)
		for range numEvents {
			seq++
			nsid := nsidUniverse[rng.Intn(len(nsidUniverse))]
			did := dids[rng.Intn(len(dids))]
			events = append(events, planEvent(seq, did, nsid))
		}
		// Vary block packing so some segments coalesce and some don't.
		maxPerBlock := 1 + rng.Intn(3)
		mustWriteSealedSegmentWithEvents(t, filepath.Join(dir, ingest.SegmentFilename(uint64(segIdx))), maxPerBlock, events)
	}
	return openManifestDir(t, dir)
}
