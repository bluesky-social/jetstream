package oracle

import "github.com/bluesky-social/jetstream-v2/segment"

func Reconstruct(events []ObservedEvent) (*Model, error) {
	model := &Model{Accounts: make(map[string]RepoSnapshot)}
	for _, ev := range events {
		if ev.Kind == segment.KindAccount {
			deleted, err := oracleAccountDeleted(ev.Payload)
			if err != nil {
				return nil, err
			}
			if deleted {
				model.Accounts[ev.DID] = RepoSnapshot{Records: make(map[RecordKey]RecordValue)}
			}
			continue
		}
		if !isCommitKind(ev.Kind) {
			continue
		}

		snap := model.Accounts[ev.DID]
		if snap.Records == nil {
			snap.Records = make(map[RecordKey]RecordValue)
		}
		model.Accounts[ev.DID] = snap

		key := RecordKey{DID: ev.DID, Collection: ev.Collection, Rkey: ev.Rkey}
		switch ev.Kind {
		case segment.KindCreate, segment.KindUpdate:
			snap.Records[key] = RecordValue{
				Rev:     ev.Rev,
				Payload: append([]byte(nil), ev.Payload...),
			}
		case segment.KindDelete:
			delete(snap.Records, key)
		}
	}
	return model, nil
}

func isCommitKind(kind segment.Kind) bool {
	return kind == segment.KindCreate || kind == segment.KindUpdate || kind == segment.KindDelete
}
