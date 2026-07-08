package timestamp

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/jcalabro/atmos/cbor"
)

// RuleStoreSubdir is the separate Pebble instance holding durable timestamp
// import intent. It is deliberately outside meta.pebble: this keyspace is
// large, cold, and rebuildable from a re-import.
const RuleStoreSubdir = "import-rules"

const (
	ruleKeyAllVersions byte = 'a'
	ruleKeySpecific    byte = 's'
	ruleKeyHasSpecific byte = 'p'
	ruleKeyCollection  byte = 'c'
	ruleSep            byte = 0

	defaultRuleSSTChunkRows = 1_000_000
)

// RuleStore owns the durable imported-timestamp rule map and the resident
// collection filter used to keep the append hot path off Pebble for unrelated
// collections.
type RuleStore struct {
	db *pebble.DB
	fs vfs.FS

	mu          sync.RWMutex
	collections map[string]struct{}
}

// RuleStoreConfig configures OpenRuleStore.
type RuleStoreConfig struct {
	DataDir string
	FS      vfs.FS
}

// OpenRuleStore opens <data-dir>/import-rules, creating it if needed.
func OpenRuleStore(cfg RuleStoreConfig) (*RuleStore, error) {
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("timestamp: rule store data dir is required")
	}
	path := filepath.Join(cfg.DataDir, RuleStoreSubdir)
	if cfg.FS != nil {
		if err := cfg.FS.MkdirAll(path, 0o755); err != nil {
			return nil, fmt.Errorf("timestamp: create rule store dir %s: %w", path, err)
		}
	}

	opts := &pebble.Options{}
	opts.EnsureDefaults()
	if cfg.FS != nil {
		opts.FS = cfg.FS
	}
	for i := range opts.Levels {
		opts.Levels[i].FilterPolicy = bloom.FilterPolicy(10)
	}
	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, fmt.Errorf("timestamp: open rule store at %s: %w", path, err)
	}
	rs := &RuleStore{db: db, fs: cfg.FS, collections: make(map[string]struct{})}
	if err := rs.ReloadCollections(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return rs, nil
}

// Close releases the rule-store Pebble handle. Idempotent.
func (s *RuleStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	if err != nil {
		return fmt.Errorf("timestamp: close rule store: %w", err)
	}
	return nil
}

// ReloadCollections refreshes the resident collection filter from durable meta
// keys. Call after a successful bulk ingest.
func (s *RuleStore) ReloadCollections() error {
	prefix := []byte{ruleKeyCollection}
	it, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return fmt.Errorf("timestamp: open rule collection iterator: %w", err)
	}
	defer func() { _ = it.Close() }()

	next := make(map[string]struct{})
	for it.First(); it.Valid(); it.Next() {
		key := it.Key()
		if len(key) <= 1 {
			return fmt.Errorf("timestamp: corrupt empty collection rule key")
		}
		next[string(append([]byte(nil), key[1:]...))] = struct{}{}
	}
	if err := it.Error(); err != nil {
		return fmt.Errorf("timestamp: iterate rule collections: %w", err)
	}
	s.mu.Lock()
	s.collections = next
	s.mu.Unlock()
	return nil
}

// HasRules reports whether any import rule collection is resident.
func (s *RuleStore) HasRules() bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.collections) > 0
}

// Stamp applies any imported timestamp rule to ev. It is intended to run from
// ingest.Writer before the event is buffered. A lookup failure is a local
// persistence failure and is returned; callers must not silently fall back to
// witnessed_at.
func (s *RuleStore) Stamp(ctx context.Context, ev *segment.Event) error {
	if s == nil || !ev.Kind.IsMaterialization() {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.RLock()
	_, collectionImported := s.collections[ev.Collection]
	s.mu.RUnlock()
	if !collectionImported {
		return nil
	}

	var ts int64
	hasSpecific, err := s.has(keyPath(ruleKeyHasSpecific, ev.DID, ev.Collection, ev.Rkey))
	if err != nil {
		return err
	}
	if hasSpecific {
		got := cbor.ComputeCID(cbor.CodecDagCBOR, ev.Payload)
		ts, err = s.getTimestamp(keySpecific(ev.DID, ev.Collection, ev.Rkey, got))
		if err != nil {
			return err
		}
	}
	if ts == 0 {
		ts, err = s.getTimestamp(keyPath(ruleKeyAllVersions, ev.DID, ev.Collection, ev.Rkey))
		if err != nil {
			return err
		}
	}
	if ts != 0 {
		ev.IndexedAt = ts
	}
	return nil
}

func (s *RuleStore) has(key []byte) (bool, error) {
	val, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("timestamp: rule lookup: %w", err)
	}
	_ = val
	if err := closer.Close(); err != nil {
		return false, fmt.Errorf("timestamp: close rule lookup: %w", err)
	}
	return true, nil
}

func (s *RuleStore) getTimestamp(key []byte) (int64, error) {
	val, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("timestamp: rule lookup: %w", err)
	}
	if len(val) != 8 {
		_ = closer.Close()
		return 0, fmt.Errorf("timestamp: corrupt rule timestamp width %d", len(val))
	}
	ts := int64(binary.LittleEndian.Uint64(val))
	if err := closer.Close(); err != nil {
		return 0, fmt.Errorf("timestamp: close rule lookup: %w", err)
	}
	return ts, nil
}

// RuleIngestConfig configures ImportRulesFromCSV.
type RuleIngestConfig struct {
	CSVPath    string
	ScratchDir string
	ChunkRows  int
}

// RuleIngestResult reports durable rule-map build counters.
type RuleIngestResult struct {
	Parse       Stats
	Rules       uint64
	SSTables    int
	BytesIngest uint64
}

// ImportRulesFromCSV streams cfg.CSVPath, builds sorted local SSTables, ingests
// them into the rule store, and refreshes the resident collection filter. Later
// chunks are ingested after earlier chunks, so duplicate keys follow CSV
// last-write-wins semantics across the whole file.
func (s *RuleStore) ImportRulesFromCSV(ctx context.Context, cfg RuleIngestConfig) (RuleIngestResult, error) {
	if cfg.CSVPath == "" {
		return RuleIngestResult{}, fmt.Errorf("timestamp: rules import CSVPath is required")
	}
	if cfg.ScratchDir == "" {
		return RuleIngestResult{}, fmt.Errorf("timestamp: rules import ScratchDir is required")
	}
	chunkRows := cfg.ChunkRows
	if chunkRows <= 0 {
		chunkRows = defaultRuleSSTChunkRows
	}
	fs := s.fs
	if fs == nil {
		fs = vfs.Default
	}
	if err := fs.MkdirAll(cfg.ScratchDir, 0o755); err != nil {
		return RuleIngestResult{}, fmt.Errorf("timestamp: create rule scratch dir: %w", err)
	}

	f, err := os.Open(cfg.CSVPath)
	if err != nil {
		return RuleIngestResult{}, fmt.Errorf("timestamp: open import csv for rules: %w", err)
	}
	defer func() { _ = f.Close() }()

	builder := &ruleSSTBuilder{
		store:     s,
		scratch:   cfg.ScratchDir,
		chunkRows: chunkRows,
	}
	parseStats, err := Parse(f, Options{
		OnRow: func(row Row) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return builder.Add(row)
		},
	})
	result := RuleIngestResult{Parse: parseStats, Rules: builder.rules, SSTables: builder.sstables, BytesIngest: builder.bytesIngest}
	if err != nil {
		return result, fmt.Errorf("timestamp: parse rules: %w", err)
	}
	bytesIngest, err := builder.Flush()
	result.Rules = builder.rules
	result.SSTables = builder.sstables
	result.BytesIngest = builder.bytesIngest + bytesIngest
	if err != nil {
		return result, err
	}
	if err := builder.Ingest(); err != nil {
		return result, err
	}
	if err := s.ReloadCollections(); err != nil {
		return result, err
	}
	return result, nil
}

type ruleEntry struct {
	key []byte
	val [8]byte
}

type ruleSSTBuilder struct {
	store       *RuleStore
	scratch     string
	chunkRows   int
	chunk       []ruleEntry
	chunkSeq    int
	rules       uint64
	sstables    int
	bytesIngest uint64
	paths       []string
}

func (b *ruleSSTBuilder) Add(row Row) error {
	var val [8]byte
	binary.LittleEndian.PutUint64(val[:], uint64(row.TimestampMicros))

	b.chunk = append(b.chunk, ruleEntry{key: collectionKey(row.Collection), val: markerValue()})
	switch row.Scope {
	case ScopeAllVersions:
		b.chunk = append(b.chunk, ruleEntry{key: keyPath(ruleKeyAllVersions, row.DID, row.Collection, row.Rkey), val: val})
	case ScopeSpecificVersion:
		b.chunk = append(b.chunk,
			ruleEntry{key: keyPath(ruleKeyHasSpecific, row.DID, row.Collection, row.Rkey), val: markerValue()},
			ruleEntry{key: keySpecific(row.DID, row.Collection, row.Rkey, row.CID), val: val},
		)
	default:
		return fmt.Errorf("timestamp: unknown rule scope %s", row.Scope)
	}
	b.rules++
	if len(b.chunk) >= b.chunkRows {
		n, err := b.Flush()
		b.bytesIngest += n
		return err
	}
	return nil
}

func markerValue() [8]byte {
	var v [8]byte
	v[0] = 1
	return v
}

func (b *ruleSSTBuilder) Flush() (uint64, error) {
	if len(b.chunk) == 0 {
		return 0, nil
	}
	sort.SliceStable(b.chunk, func(i, j int) bool {
		return bytes.Compare(b.chunk[i].key, b.chunk[j].key) < 0
	})

	path := filepath.Join(b.scratch, fmt.Sprintf("rules_%06d.sst", b.chunkSeq))
	b.chunkSeq++
	if err := writeRuleSST(b.store.fs, path, b.chunk); err != nil {
		return 0, err
	}
	fs := b.store.fs
	if fs == nil {
		fs = vfs.Default
	}
	info, err := fs.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("timestamp: stat rule sst: %w", err)
	}
	b.sstables++
	b.paths = append(b.paths, path)
	b.chunk = b.chunk[:0]
	return uint64(info.Size()), nil
}

func (b *ruleSSTBuilder) Ingest() error {
	for _, path := range b.paths {
		if err := b.store.db.Ingest([]string{path}); err != nil {
			return fmt.Errorf("timestamp: ingest rule sst: %w", err)
		}
	}
	return nil
}

func writeRuleSST(fs vfs.FS, path string, entries []ruleEntry) error {
	if fs == nil {
		fs = vfs.Default
	}
	f, err := fs.Create(path)
	if err != nil {
		return fmt.Errorf("timestamp: create rule sst: %w", err)
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{
		FilterPolicy: bloom.FilterPolicy(10),
		FilterType:   pebble.TableFilter,
	})
	var prev []byte
	for i := 0; i < len(entries); {
		e := entries[i]
		j := i + 1
		for j < len(entries) && bytes.Equal(entries[j].key, e.key) {
			e = entries[j] // last in this CSV chunk wins
			j++
		}
		if prev != nil && bytes.Compare(prev, e.key) >= 0 {
			_ = w.Close()
			return fmt.Errorf("timestamp: rule sst keys out of order")
		}
		if err := w.Set(e.key, e.val[:]); err != nil {
			_ = w.Close()
			return fmt.Errorf("timestamp: write rule sst: %w", err)
		}
		prev = e.key
		i = j
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("timestamp: close rule sst: %w", err)
	}
	return nil
}

func keyPath(prefix byte, did, collection, rkey string) []byte {
	buf := make([]byte, 0, 1+len(did)+len(collection)+len(rkey)+3)
	buf = append(buf, prefix)
	buf = append(buf, did...)
	buf = append(buf, ruleSep)
	buf = append(buf, collection...)
	buf = append(buf, ruleSep)
	buf = append(buf, rkey...)
	return buf
}

func keySpecific(did, collection, rkey string, cid cbor.CID) []byte {
	buf := keyPath(ruleKeySpecific, did, collection, rkey)
	buf = append(buf, ruleSep)
	return cid.AppendBytes(buf)
}

func collectionKey(collection string) []byte {
	buf := make([]byte, 0, 1+len(collection))
	buf = append(buf, ruleKeyCollection)
	buf = append(buf, collection...)
	return buf
}

func prefixUpperBound(prefix []byte) []byte {
	out := append([]byte(nil), prefix...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 0xff {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}
