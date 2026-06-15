package oracle

import (
	"fmt"

	"github.com/bluesky-social/jetstream-v2/internal/tombstone"
	"github.com/bluesky-social/jetstream-v2/segment"
)

// toSegmentEvent adapts an ObservedEvent to the segment.Event shape the
// tombstone package operates on (it only reads Seq/Kind/DID/Collection/
// Rkey/Payload for folding and ShouldDrop).
func toSegmentEvent(ev ObservedEvent) segment.Event {
	return segment.Event{
		Seq:        ev.Seq,
		IndexedAt:  ev.IndexedAt,
		Kind:       ev.Kind,
		DID:        ev.DID,
		Collection: ev.Collection,
		Rkey:       ev.Rkey,
		Rev:        ev.Rev,
		Payload:    ev.Payload,
	}
}

// CheckOverlayReconstruction verifies the full client coverage contract:
// given all observed events, a compaction watermark W, the overlay
// snapshot the server would serve (covering (W, M]), and M, the set of
// record rows a client would EMIT must exactly equal the ground-truth
// live set derived independently from the same event stream.
//
//   - segments cover seq <= W  (compaction physically removed superseded
//     create/update rows; the survivors are the latest)
//   - overlay covers (W, M]
//   - live tail covers (M, inf)
//
// A client emits a create/update row unless a tombstone with strictly
// greater seq supersedes it. We reconstruct that decision from the three
// sources and compare against groundTruthLive.
func CheckOverlayReconstruction(events []ObservedEvent, w, m uint64, overlaySnap tombstone.Snapshot) error {
	ground := groundTruthLive(events)

	// Live tombstones cover (M, inf): this is what the client receives on
	// the /subscribe tail when it resumes from cursor=M.
	segEvents := make([]segment.Event, 0, len(events))
	for _, ev := range events {
		segEvents = append(segEvents, toSegmentEvent(ev))
	}
	liveTomb, err := tombstone.FoldRange(segEvents, m, ^uint64(0))
	if err != nil {
		return err
	}

	emitted := make(map[tombstone.RecordKey]uint64)
	for i := range events {
		ev := &events[i]
		if ev.Kind != segment.KindCreate && ev.Kind != segment.KindUpdate {
			continue
		}
		key := tombstone.RecordKey{DID: ev.DID, Collection: ev.Collection, Rkey: ev.Rkey}
		se := toSegmentEvent(*ev)
		switch {
		case ev.Seq <= w:
			// Rows <= W survive in segments only if they are the
			// ground-truth-latest (compaction removed superseded ones).
			if gseq, ok := ground[key]; ok && gseq == ev.Seq {
				emitted[key] = maxU64(emitted[key], ev.Seq)
			}
		case ev.Seq <= m:
			if drop, _ := overlaySnap.ShouldDrop(&se); !drop {
				emitted[key] = maxU64(emitted[key], ev.Seq)
			}
		default: // ev.Seq > m
			if drop, _ := liveTomb.ShouldDrop(&se); !drop {
				emitted[key] = maxU64(emitted[key], ev.Seq)
			}
		}
	}

	for key, seq := range emitted {
		gseq, ok := ground[key]
		if !ok {
			return fmt.Errorf("oracle overlay: emitted a record that ground truth deleted: %v seq=%d", key, seq)
		}
		if gseq != seq {
			return fmt.Errorf("oracle overlay: emitted stale version %v seq=%d (live seq=%d)", key, seq, gseq)
		}
	}
	for key, gseq := range ground {
		if _, ok := emitted[key]; !ok {
			return fmt.Errorf("oracle overlay: failed to emit a live record: %v seq=%d", key, gseq)
		}
	}
	return nil
}

// groundTruthLive folds the entire event stream into the set of records
// live at the end, mapping tombstone.RecordKey -> latest create/update
// seq. A delete, account-delete, or sync at a higher seq removes it.
func groundTruthLive(events []ObservedEvent) map[tombstone.RecordKey]uint64 {
	type rec struct {
		seq  uint64
		live bool
	}
	latest := make(map[tombstone.RecordKey]*rec)
	didKill := make(map[string]uint64) // did -> highest account-delete/sync seq
	for i := range events {
		ev := &events[i]
		switch ev.Kind {
		case segment.KindCreate, segment.KindUpdate:
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
		case segment.KindSync:
			if ev.Seq > didKill[ev.DID] {
				didKill[ev.DID] = ev.Seq
			}
		case segment.KindAccount:
			deleted, _ := oracleAccountDeleted(ev.Payload)
			if deleted && ev.Seq > didKill[ev.DID] {
				didKill[ev.DID] = ev.Seq
			}
		}
	}
	out := make(map[tombstone.RecordKey]uint64)
	for key, r := range latest {
		if r.live && didKill[key.DID] <= r.seq {
			out[key] = r.seq
		}
	}
	return out
}

func maxU64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
