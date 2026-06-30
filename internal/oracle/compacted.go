package oracle

import (
	"fmt"
	"sort"
	"strings"

	"github.com/bluesky-social/jetstream/segment"
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
			return fmt.Errorf("oracle: superseded %s row survived: did=%s seq=%d tombstone_seq=%d watermark=%d%s",
				ts.reason, ev.DID, ev.Seq, ts.seq, watermark, didTimeline(events, ev.DID, watermark))
		}
		key := RecordKey{DID: ev.DID, Collection: ev.Collection, Rkey: ev.Rkey}
		if seq := recordTombstones[key]; seq > ev.Seq {
			return fmt.Errorf("oracle: superseded record row survived: did=%s collection=%s rkey=%s seq=%d tombstone_seq=%d watermark=%d%s",
				ev.DID, ev.Collection, ev.Rkey, ev.Seq, seq, watermark, didTimeline(events, ev.DID, watermark))
		}
	}

	return nil
}

// didTimeline renders every on-disk row for did in seq order, marking each
// row's relation to the watermark, so a superseded-survivor failure is
// self-diagnosing from the test artifact alone (#186). A rare durable
// compaction defect that only reproduces under heavy scheduling contention is
// otherwise undebuggable from a bare "seq=2 tombstone_seq=13" message: the
// reviewer cannot tell whether the killer tombstone was even on disk, which
// segment/seq band it landed in, or whether the survivor sits above or below W.
func didTimeline(events []ObservedEvent, did string, watermark uint64) string {
	rows := make([]ObservedEvent, 0, len(events))
	for _, ev := range events {
		if ev.DID == did {
			rows = append(rows, ev)
		}
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].Seq < rows[j].Seq })

	var b strings.Builder
	fmt.Fprintf(&b, "\n  timeline for %s (watermark=%d, %d rows):", did, watermark, len(rows))
	for _, ev := range rows {
		rel := "<=W"
		if ev.Seq > watermark {
			rel = ">W"
		}
		fmt.Fprintf(&b, "\n    seq=%d kind=%s %s", ev.Seq, eventLogKind(ev.Kind), rel)
		if ev.Collection != "" || ev.Rkey != "" {
			fmt.Fprintf(&b, " %s/%s", ev.Collection, ev.Rkey)
		}
	}
	return b.String()
}

func oracleAccountDeleted(payload []byte) (bool, error) {
	var acc comatproto.SyncSubscribeRepos_Account
	if err := acc.UnmarshalCBOR(payload); err != nil {
		return false, err
	}
	return !acc.Active && acc.Status.HasVal() && acc.Status.Val() == "deleted", nil
}
