package timestamp

// bucket.go is Phase B: route each validated Row to the sealed segments whose
// resident DID bloom says the row's repo may live there, and append the row's
// source-CSV byte offset to a per-segment offset file. Phase C (M5) reopens
// each segment, replays its offset file against the plain CSV, and applies one
// segment.Patch per touched segment.
//
// Two mechanical-sympathy structures keep this streaming and bounded, mirroring
// the #188 LiveEnqueuer:
//
//  1. A bounded LRU DID->candidate-segments cache absorbs the recommended
//     DID-grouped input to ~one bloom selection per distinct DID. Unsorted
//     input stays correct, just with more cache misses (each a cheap resident
//     bloom test, no disk I/O).
//  2. A bounded LRU file-descriptor pool caps open offset files; an evicted
//     segment's file is reopened O_APPEND on its next hit.
//
// Cache coherence with the manifest (the correctness core). SelectBlocksForDID
// is one-sided against the manifest's CURRENT resident set, but the manifest
// can seal/compact segments concurrently. A cached selection could therefore
// name a stale segment set. We gate every cache entry on the manifest's
// Generation() counter: an entry is trusted only while the generation it was
// computed under still matches. Crucially we read the generation BEFORE calling
// SelectBlocksForDID and cache that pre-read value, so the cached selection is
// never from an OLDER manifest than its tag -- a refresh racing the selection
// only ever makes the entry look stale (a safe recompute), never falsely fresh
// (which could drop a newly-sealed segment). See selectFor for the proof.
//
// Point-in-time limitation (documented, not a bug): a single streaming pass
// routes each row against the manifest as it stood when the row was processed.
// A segment sealed AFTER a row is bucketed cannot retroactively receive that
// row. Import is idempotent and re-runnable (design §3.4), so a re-run catches
// it; this is a property of single-pass streaming, not of the cache.

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/bluesky-social/jetstream/internal/manifest"
)

// Selector is the subset of *manifest.Manifest the bucketer needs. Defined as
// an interface so tests can drive generation changes and selection results
// deterministically (including a manifest that refreshes mid-stream).
type Selector interface {
	// Generation returns the manifest's monotonic mutation counter.
	Generation() uint64
	// SelectBlocksForDID returns the sealed segments (and candidate blocks)
	// that may contain did, computed from resident blooms with no disk I/O.
	SelectBlocksForDID(did string) ([]manifest.SegmentBlockSelection, error)
}

// The live manifest must satisfy Selector so M5 can pass *manifest.Manifest
// directly. This is a compile-time contract, not a runtime one.
var _ Selector = (*manifest.Manifest)(nil)

// Defaults for the two bounded LRUs. Both are working-set caches, not indexes;
// an eviction costs at most one redundant recompute (DID cache) or one
// reopen (FD pool).
const (
	// DefaultDIDCacheSize bounds distinct DIDs held in the candidate cache.
	// DID-grouped input needs only a handful live at once; this ceiling
	// protects pathologically-shuffled input from unbounded growth.
	DefaultDIDCacheSize = 100_000

	// DefaultOpenFileLimit bounds simultaneously-open per-segment offset
	// files. A wide fan-out (a DID present in many segments) or a segment
	// sweep still works; least-recently-appended files are closed and
	// reopened O_APPEND on demand.
	DefaultOpenFileLimit = 256

	// offsetRecordSize is the on-disk width of one appended offset (uint64 LE).
	offsetRecordSize = 8
)

// BucketStats reports Phase B outcomes for job status and tests.
type BucketStats struct {
	// RowsRouted is the number of valid rows that matched at least one
	// candidate segment and had their offset appended somewhere.
	RowsRouted uint64

	// RowsNoCandidate is the number of valid rows whose DID matched no sealed
	// segment (present only in the active segment, or genuinely unknown). Not
	// an error: the row is valid, it simply routes nowhere on this pass.
	RowsNoCandidate uint64

	// OffsetsWritten is the total offset appends across all segments (the
	// fan-out sum; a row present in N segments contributes N).
	OffsetsWritten uint64

	// DIDCacheHits/Misses track candidate-cache effectiveness. A miss that
	// recomputed due to a generation change is also a StaleEvictions increment.
	DIDCacheHits      uint64
	DIDCacheMisses    uint64
	StaleEvictions    uint64
	SegmentsTouched   int // distinct segments that received >=1 offset
	SelectorCallCount uint64
}

// Bucketer routes rows to per-segment offset files. Not safe for concurrent
// use: it is driven serially from timestamp.Parse's OnRow callback (one pass,
// one goroutine). The Selector it wraps may be mutated concurrently by other
// goroutines (the compactor); that is handled by generation-gating, not by
// locking the bucketer.
type Bucketer struct {
	sel    Selector
	jobDir string

	didCacheSize  int
	openFileLimit int

	// didCache: DID -> *cacheEntry, with an LRU eviction list.
	didCache map[string]*list.Element
	didLRU   *list.List

	// files: segment Idx -> *fdEntry, with its own LRU eviction list.
	files   map[uint64]*list.Element
	fileLRU *list.List

	// touched records every segment Idx that has received an offset, so the
	// stats' distinct count survives FD-pool eviction.
	touched map[uint64]struct{}

	stats BucketStats
	buf   [offsetRecordSize]byte // reused offset-encoding scratch
}

// asCacheEntry / asFDEntry unwrap a list element's any-typed Value. A wrong
// type is an internal invariant violation (we only ever push these types onto
// their respective lists), so we crash rather than corrupt.
func asCacheEntry(el *list.Element) *cacheEntry {
	e, ok := el.Value.(*cacheEntry)
	if !ok {
		panic(fmt.Sprintf("timestamp: didLRU element holds %T, not *cacheEntry", el.Value))
	}
	return e
}

func asFDEntry(el *list.Element) *fdEntry {
	e, ok := el.Value.(*fdEntry)
	if !ok {
		panic(fmt.Sprintf("timestamp: fileLRU element holds %T, not *fdEntry", el.Value))
	}
	return e
}

// cacheEntry is one DID's cached candidate segments, tagged with the manifest
// generation the selection was computed under.
type cacheEntry struct {
	did        string
	generation uint64
	candidates []segmentRef
}

// segmentRef is the minimum Phase C needs: which segment (Idx) and where it
// lives (Path). Block-level bloom pruning is a Phase C refinement (it re-tests
// per event after decode), so Phase B keeps only the segment identity.
type segmentRef struct {
	idx  uint64
	path string
}

// fdEntry is one open per-segment offset file.
type fdEntry struct {
	idx uint64
	f   *os.File
}

// BucketerConfig configures NewBucketer. Selector and JobDir are required.
type BucketerConfig struct {
	Selector Selector
	// JobDir is the directory where per-segment offset files are written
	// (data/imports/<job>/). It must already exist.
	JobDir string
	// DIDCacheSize / OpenFileLimit override the defaults when > 0.
	DIDCacheSize  int
	OpenFileLimit int
}

// NewBucketer constructs a Bucketer. It errors if Selector or JobDir is empty,
// or if JobDir is not a writable directory.
func NewBucketer(cfg BucketerConfig) (*Bucketer, error) {
	if cfg.Selector == nil {
		return nil, fmt.Errorf("timestamp: Bucketer requires a Selector")
	}
	if cfg.JobDir == "" {
		return nil, fmt.Errorf("timestamp: Bucketer requires a JobDir")
	}
	info, err := os.Stat(cfg.JobDir)
	if err != nil {
		return nil, fmt.Errorf("timestamp: bucketer job dir: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("timestamp: bucketer job dir %q is not a directory", cfg.JobDir)
	}
	didCacheSize := cfg.DIDCacheSize
	if didCacheSize <= 0 {
		didCacheSize = DefaultDIDCacheSize
	}
	openFileLimit := cfg.OpenFileLimit
	if openFileLimit <= 0 {
		openFileLimit = DefaultOpenFileLimit
	}
	return &Bucketer{
		sel:           cfg.Selector,
		jobDir:        cfg.JobDir,
		didCacheSize:  didCacheSize,
		openFileLimit: openFileLimit,
		didCache:      make(map[string]*list.Element),
		didLRU:        list.New(),
		files:         make(map[uint64]*list.Element),
		fileLRU:       list.New(),
		touched:       make(map[uint64]struct{}),
	}, nil
}

// Route is the timestamp.Parse OnRow callback: it resolves row.DID's candidate
// segments and appends row.Offset to each one's offset file. An error here
// aborts the parse (a write failure is not something to skip past).
func (b *Bucketer) Route(row Row) error {
	candidates, err := b.selectFor(row.DID)
	if err != nil {
		return err
	}
	if len(candidates) == 0 {
		b.stats.RowsNoCandidate++
		return nil
	}
	for _, c := range candidates {
		if err := b.appendOffset(c, row.Offset); err != nil {
			return err
		}
		b.stats.OffsetsWritten++
	}
	b.stats.RowsRouted++
	return nil
}

// selectFor returns row.DID's candidate segments, using the generation-gated
// cache. See the file header for the coherence argument; the load-bearing line
// is that gen is sampled BEFORE SelectBlocksForDID so the cached selection is
// never from an older manifest than its tag.
func (b *Bucketer) selectFor(did string) ([]segmentRef, error) {
	curGen := b.sel.Generation()
	if el, ok := b.didCache[did]; ok {
		entry := asCacheEntry(el)
		if entry.generation == curGen {
			b.didLRU.MoveToFront(el)
			b.stats.DIDCacheHits++
			return entry.candidates, nil
		}
		// Stale: the manifest advanced since this entry was computed. Drop it
		// and recompute rather than risk a selection that predates a seal.
		b.stats.StaleEvictions++
		b.removeDIDEntry(el)
	}
	b.stats.DIDCacheMisses++

	// Sample the generation to tag the entry with BEFORE selecting. If a
	// refresh commits between here and the select, the select observes a
	// generation >= genForTag, so the cached selection is at worst newer than
	// its tag -- and a later lookup at the newer generation will treat the tag
	// as stale and recompute. It can never be older than the tag, which is the
	// case that would drop a segment.
	genForTag := curGen
	sels, err := b.sel.SelectBlocksForDID(did)
	if err != nil {
		return nil, fmt.Errorf("timestamp: select segments for %s: %w", did, err)
	}
	b.stats.SelectorCallCount++

	candidates := make([]segmentRef, len(sels))
	for i, s := range sels {
		candidates[i] = segmentRef{idx: s.Idx, path: s.Path}
	}

	entry := &cacheEntry{did: did, generation: genForTag, candidates: candidates}
	el := b.didLRU.PushFront(entry)
	b.didCache[did] = el
	b.evictDIDIfNeeded()
	return candidates, nil
}

func (b *Bucketer) removeDIDEntry(el *list.Element) {
	entry := asCacheEntry(el)
	delete(b.didCache, entry.did)
	b.didLRU.Remove(el)
}

func (b *Bucketer) evictDIDIfNeeded() {
	for b.didLRU.Len() > b.didCacheSize {
		oldest := b.didLRU.Back()
		if oldest == nil {
			return
		}
		b.removeDIDEntry(oldest)
	}
}

// appendOffset writes off (uint64 LE) to seg's offset file, opening/reopening
// it through the bounded FD pool as needed.
func (b *Bucketer) appendOffset(seg segmentRef, off int64) error {
	f, err := b.fileFor(seg.idx)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(b.buf[:], uint64(off))
	if _, err := f.Write(b.buf[:]); err != nil {
		return fmt.Errorf("timestamp: append offset to segment %d: %w", seg.idx, err)
	}
	b.touched[seg.idx] = struct{}{}
	return nil
}

// fileFor returns the open offset file for segment idx, opening it O_APPEND
// (creating on first use) and evicting the least-recently-used file if the
// pool is full. Files are opened O_APPEND so a reopen after eviction resumes at
// the end, making the pool transparent to correctness.
func (b *Bucketer) fileFor(idx uint64) (*os.File, error) {
	if el, ok := b.files[idx]; ok {
		b.fileLRU.MoveToFront(el)
		return asFDEntry(el).f, nil
	}
	if err := b.evictFileIfNeeded(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(b.offsetPath(idx), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("timestamp: open offset file for segment %d: %w", idx, err)
	}
	el := b.fileLRU.PushFront(&fdEntry{idx: idx, f: f})
	b.files[idx] = el
	return f, nil
}

func (b *Bucketer) evictFileIfNeeded() error {
	for b.fileLRU.Len() >= b.openFileLimit {
		oldest := b.fileLRU.Back()
		if oldest == nil {
			return nil
		}
		fe := asFDEntry(oldest)
		if err := fe.f.Close(); err != nil {
			return fmt.Errorf("timestamp: close evicted offset file for segment %d: %w", fe.idx, err)
		}
		delete(b.files, fe.idx)
		b.fileLRU.Remove(oldest)
	}
	return nil
}

// OffsetFileName is the per-segment offset file's base name for segment idx.
// Exported so Phase C (M5) can locate the files a job produced.
func OffsetFileName(idx uint64) string {
	return fmt.Sprintf("offsets_%010d.bin", idx)
}

func (b *Bucketer) offsetPath(idx uint64) string {
	return filepath.Join(b.jobDir, OffsetFileName(idx))
}

// Close flushes and closes every open offset file. It must be called after the
// parse completes (and before Phase C reads the files). Returns the first close
// error, if any, after attempting to close all files.
func (b *Bucketer) Close() error {
	var firstErr error
	for el := b.fileLRU.Front(); el != nil; el = el.Next() {
		fe := asFDEntry(el)
		if err := fe.f.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("timestamp: close offset file for segment %d: %w", fe.idx, err)
		}
	}
	b.files = make(map[uint64]*list.Element)
	b.fileLRU.Init()
	return firstErr
}

// Stats returns a snapshot of the bucketer's counters. SegmentsTouched is
// filled from the distinct-segment set at call time.
func (b *Bucketer) Stats() BucketStats {
	s := b.stats
	s.SegmentsTouched = len(b.touched)
	return s
}
