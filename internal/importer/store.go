package importer

// store.go is the importer's pebble keyspace (design Q-RESUME). Layout:
//
//	import/current                 -> job id of the current (possibly running) job
//	import/job/<id>/meta           -> JSON-encoded Record
//	import/job/<id>/seg/<idx>      -> per-segment done marker (Phase C checkpoint)
//
// The per-segment markers under seg/ are the resume done-set: a restarted job
// loads them and skips those segments. The idempotency of RunImport is the
// backstop, so a marker that failed to persist only costs a re-scan.

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/cockroachdb/pebble"
)

const (
	importCurrentKey = "import/current"
	importJobPrefix  = "import/job/"
)

func recordKey(id string) string { return importJobPrefix + id + "/meta" }
func segPrefix(id string) string { return importJobPrefix + id + "/seg/" }
func segKey(id string, idx uint64) []byte {
	// Fixed-width big-endian idx keeps the seg/ range scannable in order,
	// though order is irrelevant to the done-set (it is loaded into a map).
	prefix := []byte(segPrefix(id))
	buf := make([]byte, len(prefix)+8)
	copy(buf, prefix)
	binary.BigEndian.PutUint64(buf[len(prefix):], idx)
	return buf
}

func (m *Manager) putRecordLocked(rec *Record) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("importer: marshal record %s: %w", rec.ID, err)
	}
	if err := m.store.Set([]byte(recordKey(rec.ID)), data, store.SyncWrites); err != nil {
		return fmt.Errorf("importer: persist record %s: %w", rec.ID, err)
	}
	return nil
}

func (m *Manager) getRecord(id string) (Record, bool, error) {
	val, closer, err := m.store.Get([]byte(recordKey(id)))
	if errors.Is(err, store.ErrNotFound) {
		return Record{}, false, nil
	}
	if err != nil {
		return Record{}, false, fmt.Errorf("importer: get record %s: %w", id, err)
	}
	defer func() { _ = closer.Close() }()
	var rec Record
	if err := json.Unmarshal(val, &rec); err != nil {
		return Record{}, false, fmt.Errorf("importer: unmarshal record %s: %w", id, err)
	}
	return rec, true, nil
}

func (m *Manager) setCurrent(id string) error {
	if err := m.store.Set([]byte(importCurrentKey), []byte(id), store.SyncWrites); err != nil {
		return fmt.Errorf("importer: set current job: %w", err)
	}
	return nil
}

func (m *Manager) getCurrent() (string, bool, error) {
	val, closer, err := m.store.Get([]byte(importCurrentKey))
	if errors.Is(err, store.ErrNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("importer: get current job: %w", err)
	}
	defer func() { _ = closer.Close() }()
	return string(val), true, nil
}

func (m *Manager) clearCurrent() error {
	if err := m.store.Delete([]byte(importCurrentKey), store.SyncWrites); err != nil {
		return fmt.Errorf("importer: clear current job: %w", err)
	}
	return nil
}

// checkpointSegmentLocked durably marks segment idx done for job id. Called
// under m.mu.
func (m *Manager) checkpointSegmentLocked(id string, idx uint64) error {
	if err := m.store.Set(segKey(id, idx), []byte{1}, store.SyncWrites); err != nil {
		return fmt.Errorf("importer: checkpoint segment %d for job %s: %w", idx, id, err)
	}
	return nil
}

// loadDoneSegments reads the per-segment done-set for job id.
func (m *Manager) loadDoneSegments(id string) (map[uint64]struct{}, error) {
	prefix := []byte(segPrefix(id))
	it, err := m.store.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: store.PrefixUpperBound(prefix),
	})
	if err != nil {
		return nil, fmt.Errorf("importer: open seg iter for job %s: %w", id, err)
	}
	defer func() { _ = it.Close() }()

	done := make(map[uint64]struct{})
	for it.First(); it.Valid(); it.Next() {
		key := it.Key()
		if len(key) != len(prefix)+8 {
			return nil, fmt.Errorf("importer: malformed seg key for job %s (len %d)", id, len(key))
		}
		idx := binary.BigEndian.Uint64(key[len(prefix):])
		done[idx] = struct{}{}
	}
	if err := it.Error(); err != nil {
		return nil, fmt.Errorf("importer: iterate seg keys for job %s: %w", id, err)
	}
	return done, nil
}
