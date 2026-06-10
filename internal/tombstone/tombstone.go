package tombstone

import (
	"fmt"
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

type Set struct {
	mu      sync.RWMutex
	records map[RecordKey]uint64
	dids    map[string]DIDTombstone
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
	return observeLocked(s.records, s.dids, ev)
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
		}
	}
	for did, ts := range s.dids {
		if ts.Seq <= maxSeq {
			delete(s.dids, did)
		}
	}
}

func (s *Set) Replace(snapshot Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = make(map[RecordKey]uint64, len(snapshot.Records))
	for k, seq := range snapshot.Records {
		s.records[k] = seq
	}
	s.dids = make(map[string]DIDTombstone, len(snapshot.DIDs))
	for did, ts := range snapshot.DIDs {
		s.dids[did] = ts
	}
}

func (s *Set) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.records) + len(s.dids)
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
		if err := observeLocked(out.Records, out.DIDs, &events[i]); err != nil {
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

func observeLocked(records map[RecordKey]uint64, dids map[string]DIDTombstone, ev *segment.Event) error {
	switch ev.Kind {
	case segment.KindDelete, segment.KindUpdate:
		key := RecordKey{DID: ev.DID, Collection: ev.Collection, Rkey: ev.Rkey}
		if ev.Seq > records[key] {
			records[key] = ev.Seq
		}
	case segment.KindSync:
		if ev.Seq > dids[ev.DID].Seq {
			dids[ev.DID] = DIDTombstone{Seq: ev.Seq, Reason: "sync"}
		}
	case segment.KindAccount:
		deleted, err := accountDeleted(ev.Payload)
		if err != nil {
			return fmt.Errorf("tombstone: decode account did=%s seq=%d: %w", ev.DID, ev.Seq, err)
		}
		if deleted && ev.Seq > dids[ev.DID].Seq {
			dids[ev.DID] = DIDTombstone{Seq: ev.Seq, Reason: "account"}
		}
	}
	return nil
}

func accountDeleted(payload []byte) (bool, error) {
	var acc comatproto.SyncSubscribeRepos_Account
	if err := acc.UnmarshalCBOR(payload); err != nil {
		return false, err
	}
	return !acc.Active && acc.Status.HasVal() && acc.Status.Val() == "deleted", nil
}
