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
// A client emits, per record key, the highest-seq create/update row that
// is surfaced AND not suppressed by a higher-seq tombstone.
//
// Precondition: events is the post-compaction physical segment stream
// (plus the live tail / active segment). Create/update rows superseded by
// a tombstone at or below W have already been physically removed by
// compaction, so they are absent here. The client therefore never sees
// those rows nor their (evicted) tombstones — consistent. Every
// create/update row still present is surfaced to the client.
//
// The client holds ALL its tombstones simultaneously: the overlay covers
// (W, M] and the live tail covers (M, inf). A record created in (W, M]
// can be deleted by a tombstone in (M, inf) and vice versa, so the
// combined set — not a per-seq-range subset — must be applied to every
// surfaced row.
func CheckOverlayReconstruction(events []ObservedEvent, w, m uint64, overlaySnap tombstone.Snapshot) error {
	ground := groundTruthLive(events)

	// Live tombstones cover (M, inf): what the client receives on the
	// /subscribe tail when it resumes from cursor=M.
	segEvents := make([]segment.Event, 0, len(events))
	for _, ev := range events {
		segEvents = append(segEvents, toSegmentEvent(ev))
	}
	liveTomb, err := tombstone.FoldRange(segEvents, m, ^uint64(0))
	if err != nil {
		return err
	}

	// combined = overlay((W,M]) ∪ live((M,inf)), taking the max seq per
	// key. This is the full suppression set a client holds at once.
	combined := tombstone.Snapshot{
		Records: make(map[tombstone.RecordKey]uint64),
		DIDs:    make(map[string]tombstone.DIDTombstone),
	}
	combined.Merge(overlaySnap)
	combined.Merge(liveTomb)

	emitted := make(map[tombstone.RecordKey]uint64)
	for i := range events {
		ev := &events[i]
		if ev.Kind != segment.KindCreate && ev.Kind != segment.KindUpdate {
			continue
		}
		key := tombstone.RecordKey{DID: ev.DID, Collection: ev.Collection, Rkey: ev.Rkey}
		se := toSegmentEvent(*ev)
		if drop, _ := combined.ShouldDrop(&se); !drop {
			emitted[key] = maxU64(emitted[key], ev.Seq)
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
