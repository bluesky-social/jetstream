package oracle

import (
	"fmt"

	"github.com/bluesky-social/jetstream-v2/internal/simulator/world"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/repo"
)

func GroundTruthFromWorld(w *world.World) (*Model, error) {
	out := &Model{Accounts: make(map[string]RepoSnapshot, w.AccountCount())}
	for idx := range w.AccountCount() {
		acct, err := w.LoadAccount(idx)
		if err != nil {
			return nil, err
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
