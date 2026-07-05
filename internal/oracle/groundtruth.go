package oracle

import (
	"fmt"

	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/repo"
)

// GroundTruthFromWorld builds the authoritative final-state Model directly from
// the simulator world's repos, skipping deleted accounts, for comparison
// against the reconstructed model. Records the world's adversarial ledger
// marks as intentional gate drops (#204) are excluded: the world commits each
// lie into its own MST for real (the CAR/signature pipeline must stay honest),
// but jetstream is required never to archive them. One-directional-safe — a
// wrongly-archived lie still fails Compare as an extra observed record.
func GroundTruthFromWorld(w *world.World) (*Model, error) {
	out := &Model{Accounts: make(map[string]RepoSnapshot, w.AccountCount())}
	for idx := range w.AccountCount() {
		acct, err := w.LoadAccount(idx)
		if err != nil {
			return nil, err
		}
		deleted, err := w.IsAccountDeleted(idx)
		if err != nil {
			return nil, err
		}
		if deleted {
			continue
		}
		_, unavailable, err := w.RepoUnavailableStatus(idx)
		if err != nil {
			return nil, err
		}
		if unavailable {
			continue
		}
		rp, _, err := w.LoadRepo(idx)
		if err != nil {
			return nil, err
		}
		snap, err := snapshotRepo(string(acct.DID), rp)
		if err != nil {
			return nil, err
		}
		out.Accounts[string(acct.DID)] = snap
	}
	newAdversarialFilter(w.AdversarialLedger().Entries()).FilterGroundTruth(out)
	return out, nil
}

func snapshotRepo(did string, rp *repo.Repo) (RepoSnapshot, error) {
	snap := RepoSnapshot{Records: map[RecordKey]RecordValue{}}
	err := rp.Tree.Walk(func(key string, cid cbor.CID) error {
		collection, rkey := repo.SplitMSTKey(key)
		payload, err := rp.Store.GetBlock(cid)
		if err != nil {
			return fmt.Errorf("oracle: get block %s/%s: %w", collection, rkey, err)
		}
		snap.Records[RecordKey{DID: did, Collection: collection, Rkey: rkey}] =
			RecordValue{Payload: append([]byte(nil), payload...)}
		return nil
	})
	return snap, err
}
