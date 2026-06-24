package client

import (
	"math/rand/v2"
	"testing"

	"github.com/bluesky-social/jetstream/internal/tombstone"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

func accountDeletePayload(did string, seq int64) []byte {
	acct := comatproto.SyncSubscribeRepos_Account{
		DID: did, Active: false, Status: gt.Some("deleted"), Seq: seq, Time: "t",
	}
	payload, _ := acct.MarshalCBOR()
	return payload
}

func accountDeletedTest(payload []byte) (bool, error) {
	var acct comatproto.SyncSubscribeRepos_Account
	if err := acct.UnmarshalCBOR(payload); err != nil {
		return false, err
	}
	return !acct.Active && acct.Status.ValOr("") == "deleted", nil
}

// TestReconstructionSwarm is a seeded property test mirroring the oracle's
// CheckOverlayReconstruction contract: for a randomly generated event stream,
// the set of records the client EMITS (materialization rows surviving the
// combined tombstone suppression) must exactly equal the independently-derived
// ground-truth live set. Runs many seeds to shake out suppression edge cases.
func TestReconstructionSwarm(t *testing.T) {
	t.Parallel()
	for seed := uint64(1); seed <= 200; seed++ {
		seed := seed
		t.Run("seed", func(t *testing.T) {
			t.Parallel()
			rng := rand.New(rand.NewPCG(seed, 0x9e3779b9))
			events := genStream(rng, 80)

			// The client holds the union of overlay + live tombstones; fold the
			// whole stream to get that combined set (as the engine would after
			// seeding from the overlay and tailing live).
			snap, err := tombstone.FoldRange(events, 0, ^uint64(0))
			require.NoError(t, err)
			snap = ensureSnapshotMaps(snap)
			sup := NewSuppressor()
			sup.base.Store(&snap)

			emitted := map[tombstone.RecordKey]uint64{}
			for i := range events {
				ev := &events[i]
				if !ev.Kind.IsMaterialization() {
					continue
				}
				if drop, _ := sup.ShouldDrop(ev); drop {
					continue
				}
				key := tombstone.RecordKey{DID: ev.DID, Collection: ev.Collection, Rkey: ev.Rkey}
				if ev.Seq > emitted[key] {
					emitted[key] = ev.Seq
				}
			}

			want := groundTruthLive(events)
			require.Equal(t, want, emitted, "seed %d: client reconstruction != ground-truth live set", seed)
		})
	}
}

// genStream produces a random but valid event stream: ascending seqs, a small
// DID/collection/rkey space (so collisions, deletes, updates, and account
// deletes interleave meaningfully), and the full kind mix.
func genStream(rng *rand.Rand, n int) []segment.Event {
	dids := []string{"did:plc:a", "did:plc:b", "did:plc:c"}
	colls := []string{"app.bsky.feed.post", "app.bsky.feed.like"}
	rkeys := []string{"r1", "r2", "r3"}

	out := make([]segment.Event, 0, n)
	for i := range n {
		seq := uint64(i + 1)
		did := dids[rng.IntN(len(dids))]
		ev := segment.Event{Seq: seq, DID: did, IndexedAt: int64(seq)}
		switch rng.IntN(10) {
		case 0, 1, 2, 3, 4: // create
			ev.Kind = segment.KindCreate
			ev.Collection = colls[rng.IntN(len(colls))]
			ev.Rkey = rkeys[rng.IntN(len(rkeys))]
		case 5, 6: // update
			ev.Kind = segment.KindUpdate
			ev.Collection = colls[rng.IntN(len(colls))]
			ev.Rkey = rkeys[rng.IntN(len(rkeys))]
		case 7, 8: // delete
			ev.Kind = segment.KindDelete
			ev.Collection = colls[rng.IntN(len(colls))]
			ev.Rkey = rkeys[rng.IntN(len(rkeys))]
		case 9: // account delete (DID tombstone)
			ev.Kind = segment.KindAccount
			ev.Payload = accountDeletePayload(did, int64(seq))
		}
		out = append(out, ev)
	}
	return out
}

// groundTruthLive folds the stream into the set of records live at the end,
// independently of the client's suppression machinery: the highest create/
// update seq per key, unless a later delete for that key or a later account
// delete for that DID removes it.
func groundTruthLive(events []segment.Event) map[tombstone.RecordKey]uint64 {
	type rec struct {
		seq  uint64
		live bool
	}
	latest := map[tombstone.RecordKey]*rec{}
	didKill := map[string]uint64{} // did -> highest account-delete seq

	for i := range events {
		ev := &events[i]
		switch ev.Kind {
		case segment.KindCreate, segment.KindUpdate, segment.KindCreateResync:
			key := tombstone.RecordKey{DID: ev.DID, Collection: ev.Collection, Rkey: ev.Rkey}
			r := latest[key]
			if r == nil {
				r = &rec{}
				latest[key] = r
			}
			if ev.Seq >= r.seq {
				r.seq = ev.Seq
				r.live = true
			}
		case segment.KindDelete:
			key := tombstone.RecordKey{DID: ev.DID, Collection: ev.Collection, Rkey: ev.Rkey}
			r := latest[key]
			if r == nil {
				r = &rec{}
				latest[key] = r
			}
			if ev.Seq >= r.seq {
				r.seq = ev.Seq
				r.live = false
			}
		case segment.KindAccount:
			if deleted, _ := accountDeletedTest(ev.Payload); deleted && ev.Seq > didKill[ev.DID] {
				didKill[ev.DID] = ev.Seq
			}
		}
	}

	out := map[tombstone.RecordKey]uint64{}
	for key, r := range latest {
		if r.live && didKill[key.DID] <= r.seq {
			out[key] = r.seq
		}
	}
	return out
}
