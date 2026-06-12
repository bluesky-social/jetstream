// Package tombstone maintains the live in-memory delete/update
// tombstone set consumed by the delete compactor (and, later, the
// read-path overlay). See the compaction spec §3/§3.4: a tombstone is
// a key plus the max seq of any superseding event; suppression drops
// Create/Update rows with a strictly smaller seq.
package tombstone

import (
	"fmt"
	"strings"
	"sync"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/jcalabro/atmos/api/comatproto"
)

type RecordKey struct {
	DID        string
	Collection string
	Rkey       string
}

type DIDTombstone struct {
	Seq    uint64
	Reason string
}

type Snapshot struct {
	Records map[RecordKey]uint64
	DIDs    map[string]DIDTombstone
}

// entryOverheadBytes approximates the fixed per-entry cost beyond the
// key strings themselves: string headers, the seq value, and amortized
// Go map bucket overhead. Used for the tombstone_set_bytes gauge and
// deliberately conservative rather than exact.
const entryOverheadBytes = 64

type Set struct {
	mu      sync.RWMutex
	records map[RecordKey]uint64
	dids    map[string]DIDTombstone
	bytes   int64
}

func New() *Set {
	return &Set{
		records: make(map[RecordKey]uint64),
		dids:    make(map[string]DIDTombstone),
	}
}

func (s *Set) Observe(ev *segment.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	added, err := observeLocked(s.records, s.dids, ev)
	s.bytes += added
	return err
}

func (s *Set) Snapshot(maxSeq uint64) Snapshot {
	return s.SnapshotRange(0, maxSeq)
}

func (s *Set) SnapshotRange(lowExclusive, highInclusive uint64) Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := Snapshot{
		Records: make(map[RecordKey]uint64, len(s.records)),
		DIDs:    make(map[string]DIDTombstone, len(s.dids)),
	}
	for k, seq := range s.records {
		if seq > lowExclusive && seq <= highInclusive {
			out.Records[k] = seq
		}
	}
	for did, ts := range s.dids {
		if ts.Seq > lowExclusive && ts.Seq <= highInclusive {
			out.DIDs[did] = ts
		}
	}
	return out
}

func (s *Set) Evict(maxSeq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, seq := range s.records {
		if seq <= maxSeq {
			delete(s.records, k)
			s.bytes -= recordEntryBytes(k)
		}
	}
	for did, ts := range s.dids {
		if ts.Seq <= maxSeq {
			delete(s.dids, did)
			s.bytes -= didEntryBytes(did)
		}
	}
}

func (s *Set) Replace(snapshot Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = make(map[RecordKey]uint64, len(snapshot.Records))
	s.bytes = 0
	for k, seq := range snapshot.Records {
		s.records[k] = seq
		s.bytes += recordEntryBytes(k)
	}
	s.dids = make(map[string]DIDTombstone, len(snapshot.DIDs))
	for did, ts := range snapshot.DIDs {
		s.dids[did] = ts
		s.bytes += didEntryBytes(did)
	}
}

func (s *Set) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.records) + len(s.dids)
}

// ApproxBytes reports the approximate resident size of the set: key
// string bytes plus a fixed per-entry overhead. Feeds the
// tombstone_set_bytes gauge.
func (s *Set) ApproxBytes() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bytes
}

func recordEntryBytes(k RecordKey) int64 {
	return int64(len(k.DID)+len(k.Collection)+len(k.Rkey)) + entryOverheadBytes
}

func didEntryBytes(did string) int64 {
	return int64(len(did)) + entryOverheadBytes
}

func Fold(events []segment.Event, watermark uint64) (Snapshot, error) {
	return FoldRange(events, watermark, ^uint64(0))
}

func FoldRange(events []segment.Event, lowExclusive, highInclusive uint64) (Snapshot, error) {
	out := Snapshot{Records: make(map[RecordKey]uint64), DIDs: make(map[string]DIDTombstone)}
	for i := range events {
		if events[i].Seq <= lowExclusive || events[i].Seq > highInclusive {
			continue
		}
		if _, err := observeLocked(out.Records, out.DIDs, &events[i]); err != nil {
			return Snapshot{}, err
		}
	}
	return out, nil
}

func (s Snapshot) Empty() bool {
	return len(s.Records) == 0 && len(s.DIDs) == 0
}

func (s Snapshot) ShouldDrop(ev *segment.Event) (bool, string) {
	if ev.Kind != segment.KindCreate && ev.Kind != segment.KindUpdate {
		return false, ""
	}
	if ts, ok := s.DIDs[ev.DID]; ok && ts.Seq > ev.Seq {
		return true, ts.Reason
	}
	key := RecordKey{DID: ev.DID, Collection: ev.Collection, Rkey: ev.Rkey}
	if seq, ok := s.Records[key]; ok && seq > ev.Seq {
		return true, "record"
	}
	return false, ""
}

func (s Snapshot) Merge(other Snapshot) {
	for k, seq := range other.Records {
		if seq > s.Records[k] {
			s.Records[k] = seq
		}
	}
	for did, ts := range other.DIDs {
		if ts.Seq > s.DIDs[did].Seq {
			s.DIDs[did] = ts
		}
	}
}

// observeLocked folds one event into the maps and returns the
// approximate bytes added by any newly inserted entry.
//
// Inserted keys are CLONED: events decoded from segment blocks alias
// the block's decompressed buffer (segment decode contract), and a
// retained map key would pin the entire multi-MB buffer for the life
// of the entry. Lookups probe with the aliased strings first so the
// clone is paid only on first insert.
func observeLocked(records map[RecordKey]uint64, dids map[string]DIDTombstone, ev *segment.Event) (int64, error) {
	switch ev.Kind {
	case segment.KindDelete, segment.KindUpdate:
		probe := RecordKey{DID: ev.DID, Collection: ev.Collection, Rkey: ev.Rkey}
		if old, ok := records[probe]; ok {
			if ev.Seq > old {
				// Updating via the aliased probe key keeps the
				// originally-inserted (cloned) key in the map.
				records[probe] = ev.Seq
			}
			return 0, nil
		}
		key := RecordKey{
			DID:        strings.Clone(ev.DID),
			Collection: strings.Clone(ev.Collection),
			Rkey:       strings.Clone(ev.Rkey),
		}
		records[key] = ev.Seq
		return recordEntryBytes(key), nil
	case segment.KindSync:
		if old, ok := dids[ev.DID]; ok {
			if ev.Seq > old.Seq {
				dids[ev.DID] = DIDTombstone{Seq: ev.Seq, Reason: "sync"}
			}
			return 0, nil
		}
		did := strings.Clone(ev.DID)
		dids[did] = DIDTombstone{Seq: ev.Seq, Reason: "sync"}
		return didEntryBytes(did), nil
	case segment.KindAccount:
		deleted, err := accountDeleted(ev.Payload)
		if err != nil {
			return 0, fmt.Errorf("tombstone: decode account did=%s seq=%d: %w", ev.DID, ev.Seq, err)
		}
		if !deleted {
			return 0, nil
		}
		if old, ok := dids[ev.DID]; ok {
			if ev.Seq > old.Seq {
				dids[ev.DID] = DIDTombstone{Seq: ev.Seq, Reason: "account"}
			}
			return 0, nil
		}
		did := strings.Clone(ev.DID)
		dids[did] = DIDTombstone{Seq: ev.Seq, Reason: "account"}
		return didEntryBytes(did), nil
	}
	return 0, nil
}

func accountDeleted(payload []byte) (bool, error) {
	var acc comatproto.SyncSubscribeRepos_Account
	if err := acc.UnmarshalCBOR(payload); err != nil {
		return false, err
	}
	return !acc.Active && acc.Status.HasVal() && acc.Status.Val() == "deleted", nil
}
