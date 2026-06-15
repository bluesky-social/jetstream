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

type CompactionChunkObservation struct {
	StartWatermark   uint64
	TargetWatermark  uint64
	ChunkEnd         uint64
	RecordTombstones []CompactionRecordTombstone
	DIDTombstones    []CompactionDIDTombstone
}

type CompactionRecordTombstone struct {
	DID        string
	Collection string
	Rkey       string
	Seq        uint64
}

type CompactionDIDTombstone struct {
	DID    string
	Seq    uint64
	Reason string
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
		if ev.Seq > watermark || (ev.Kind != segment.KindCreate && ev.Kind != segment.KindUpdate) {
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

func CheckCompactionChunks(events []ObservedEvent, chunks []CompactionChunkObservation) error {
	for i, chunk := range chunks {
		recordTombstones := make(map[RecordKey]uint64, len(chunk.RecordTombstones))
		for _, ts := range chunk.RecordTombstones {
			key := RecordKey{DID: ts.DID, Collection: ts.Collection, Rkey: ts.Rkey}
			if ts.Seq > recordTombstones[key] {
				recordTombstones[key] = ts.Seq
			}
		}
		didTombstones := make(map[string]oracleDIDTombstone, len(chunk.DIDTombstones))
		for _, ts := range chunk.DIDTombstones {
			if ts.Seq > didTombstones[ts.DID].seq {
				didTombstones[ts.DID] = oracleDIDTombstone{seq: ts.Seq, reason: ts.Reason}
			}
		}
		for _, ev := range events {
			if ev.Seq <= chunk.StartWatermark || ev.Seq > chunk.ChunkEnd || (ev.Kind != segment.KindCreate && ev.Kind != segment.KindUpdate) {
				continue
			}
			if ts := didTombstones[ev.DID]; ts.seq > ev.Seq {
				return fmt.Errorf("oracle: compaction chunk %d superseded %s row survived: did=%s seq=%d tombstone_seq=%d chunk_start=%d chunk_end=%d target_watermark=%d",
					i, ts.reason, ev.DID, ev.Seq, ts.seq, chunk.StartWatermark, chunk.ChunkEnd, chunk.TargetWatermark)
			}
			key := RecordKey{DID: ev.DID, Collection: ev.Collection, Rkey: ev.Rkey}
			if seq := recordTombstones[key]; seq > ev.Seq {
				return fmt.Errorf("oracle: compaction chunk %d superseded record row survived: did=%s collection=%s rkey=%s seq=%d tombstone_seq=%d chunk_start=%d chunk_end=%d target_watermark=%d",
					i, ev.DID, ev.Collection, ev.Rkey, ev.Seq, seq, chunk.StartWatermark, chunk.ChunkEnd, chunk.TargetWatermark)
			}
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
