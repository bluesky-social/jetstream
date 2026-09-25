package segment

import (
	"math/rand/v2"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// swarmAxes are the feature axes a swarm profile flips.
type swarmAxes struct {
	singleBlock      bool
	manyBlocks       bool
	allSameDID       bool
	heavyTailDIDs    bool
	singleCollection bool
	manyCollections  bool
	tinyPayloads     bool
	largePayloads    bool
}

// genSwarmSegment draws one swarm profile from r: each axis flips with
// p=0.5 (forcing at least one if all roll false), and the returned
// events and block size realize it.
func genSwarmSegment(r *rand.Rand) (axes swarmAxes, events []Event, maxPerBlock int) {
	axes = swarmAxes{
		singleBlock:      r.Float64() < 0.5,
		manyBlocks:       r.Float64() < 0.5,
		allSameDID:       r.Float64() < 0.5,
		heavyTailDIDs:    r.Float64() < 0.5,
		singleCollection: r.Float64() < 0.5,
		manyCollections:  r.Float64() < 0.5,
		tinyPayloads:     r.Float64() < 0.5,
		largePayloads:    r.Float64() < 0.5,
	}

	// Force at least one axis on so we don't repeat the all-defaults
	// case (which the property test in Task 17 already covers).
	any := axes.singleBlock || axes.manyBlocks || axes.allSameDID ||
		axes.heavyTailDIDs || axes.singleCollection || axes.manyCollections ||
		axes.tinyPayloads || axes.largePayloads
	if !any {
		axes.manyBlocks = true
	}

	maxPerBlock = 4
	if axes.singleBlock {
		maxPerBlock = 64
	}
	nEvents := 4 + r.IntN(20)
	if axes.manyBlocks {
		nEvents = 16 + r.IntN(48)
		maxPerBlock = 2
	}

	dids := []string{"did:plc:a", "did:plc:b", "did:plc:c", "did:plc:d"}
	colls := []string{"a", "b", "c"}
	payloadSize := 4
	if axes.tinyPayloads {
		payloadSize = 1
	}
	if axes.largePayloads {
		payloadSize = 256
	}

	for i := 0; i < nEvents; i++ {
		did := dids[r.IntN(len(dids))]
		if axes.allSameDID {
			did = dids[0]
		} else if axes.heavyTailDIDs {
			if r.Float64() < 0.9 {
				did = dids[0]
			}
		}
		coll := "app.bsky.feed." + colls[r.IntN(len(colls))]
		if axes.singleCollection {
			coll = "app.bsky.feed.post"
		} else if axes.manyCollections {
			coll = "app.bsky.feed." + string(rune('a'+r.IntN(20)))
		}
		payload := make([]byte, payloadSize)
		for j := range payload {
			payload[j] = byte(r.IntN(256))
		}
		events = append(events, Event{
			Seq: uint64(i + 1), WitnessedAt: int64(i),
			Kind: Kind(1 + r.IntN(7)),
			DID:  did, Collection: coll, Rkey: "k", Rev: "rev",
			Payload: payload,
		})
	}
	return axes, events, maxPerBlock
}

// writeSwarmSegment writes events through a Writer, flushing whenever a
// block fills, and seals the file.
func writeSwarmSegment(t *testing.T, path string, events []Event, maxPerBlock int) SealResult {
	t.Helper()
	w, err := New(Config{Path: path, MaxEventsPerBlock: maxPerBlock})
	require.NoError(t, err)
	for i, ev := range events {
		full, err := w.Append(ev)
		require.NoErrorf(t, err, "append %d", i)
		if full {
			require.NoError(t, w.Flush())
		}
	}
	res, err := w.Seal()
	require.NoError(t, err)
	return res
}

// TestSealSwarm explores feature-axis combinations to surface
// interactions the deterministic tests miss. Each iteration draws a
// profile (genSwarmSegment), builds a sealed segment under it, and
// round-trips it through a Reader.
func TestSealSwarm(t *testing.T) {
	t.Parallel()

	iters := 30
	if !testing.Short() {
		iters = 1000
	}

	r := rand.New(rand.NewPCG(7, 11))

	for it := range iters {
		axes, events, maxPerBlock := genSwarmSegment(r)
		path := filepath.Join(t.TempDir(), "seg.jss")
		writeSwarmSegment(t, path, events, maxPerBlock)

		rdr, err := Open(ReaderConfig{Path: path})
		require.NoErrorf(t, err, "iter %d open", it)

		var got []Event
		for i := range rdr.Blocks() {
			evs, err := rdr.DecodeBlock(i)
			require.NoErrorf(t, err, "iter %d block %d", it, i)
			got = append(got, evs...)
		}
		require.Lenf(t, got, len(events), "iter %d", it)
		for i := range events {
			require.Truef(t, eventsEqual(events[i], got[i]),
				"iter %d event %d mismatch (axes=%+v)", it, i, axes)
		}

		require.NoError(t, rdr.Close())
	}
}
