package oracle

import (
	"fmt"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/jcalabro/atmos/api/comatproto"
)

type oracleDIDTombstone struct {
	seq    uint64
	reason string
}

// CheckCompacted verifies the compaction guarantee independently of the
// production compactor: no surviving create/update row at or below watermark is
// superseded by a record or DID tombstone at or below the same watermark.
func CheckCompacted(events []ObservedEvent, watermark uint64) error {
	recordTombstones := make(map[RecordKey]uint64)
	didTombstones := make(map[string]oracleDIDTombstone)

	for _, ev := range events {
		if ev.Seq > watermark {
			continue
		}
		switch ev.Kind {
		case segment.KindDelete, segment.KindUpdate:
			key := RecordKey{DID: ev.DID, Collection: ev.Collection, Rkey: ev.Rkey}
			if ev.Seq > recordTombstones[key] {
				recordTombstones[key] = ev.Seq
			}
		case segment.KindSync:
			if ev.Seq > didTombstones[ev.DID].seq {
				didTombstones[ev.DID] = oracleDIDTombstone{seq: ev.Seq, reason: "sync"}
			}
		case segment.KindAccount:
			deleted, err := oracleAccountDeleted(ev.Payload)
			if err != nil {
				return fmt.Errorf("oracle: compacted check account payload did=%s seq=%d: %w", ev.DID, ev.Seq, err)
			}
			if deleted && ev.Seq > didTombstones[ev.DID].seq {
				didTombstones[ev.DID] = oracleDIDTombstone{seq: ev.Seq, reason: "account"}
			}
		}
	}

	for _, ev := range events {
		if ev.Seq > watermark || !ev.Kind.IsMaterialization() {
			continue
		}
		if ts := didTombstones[ev.DID]; ts.seq > ev.Seq {
			return fmt.Errorf("oracle: superseded %s row survived: did=%s seq=%d tombstone_seq=%d",
				ts.reason, ev.DID, ev.Seq, ts.seq)
		}
		key := RecordKey{DID: ev.DID, Collection: ev.Collection, Rkey: ev.Rkey}
		if seq := recordTombstones[key]; seq > ev.Seq {
			return fmt.Errorf("oracle: superseded record row survived: did=%s collection=%s rkey=%s seq=%d tombstone_seq=%d",
				ev.DID, ev.Collection, ev.Rkey, ev.Seq, seq)
		}
	}

	return nil
}

func oracleAccountDeleted(payload []byte) (bool, error) {
	var acc comatproto.SyncSubscribeRepos_Account
	if err := acc.UnmarshalCBOR(payload); err != nil {
		return false, err
	}
	return !acc.Active && acc.Status.HasVal() && acc.Status.Val() == "deleted", nil
}
