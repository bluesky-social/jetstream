package manifest

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/segment"
)

// SegmentBounds is the projection of segment.Header that the manifest
// keeps in memory for every sealed segment. Idx and Path identify the
// file on disk; the four bound fields support cursor-resolution lookups
// without touching the file again.
type SegmentBounds struct {
	Idx          uint64
	Path         string
	MinSeq       uint64
	MaxSeq       uint64
	MinIndexedAt int64
	MaxIndexedAt int64
}

// Options configures Open. SegmentsDir is required; the rest have
// safe zero-value defaults.
type Options struct {
	// SegmentsDir is the directory holding seg_*.jss files. Required.
	SegmentsDir string

	// BlockIndexCacheSize is the LRU capacity for per-segment block
	// indices. 0 disables caching (every BlockIndex call hits disk).
	// Used only by the BlockIndex method (added in a later task);
	// harmless before then.
	BlockIndexCacheSize int

	// Logger is required.
	Logger *slog.Logger

	// Metrics is optional. nil disables metric updates.
	Metrics *Metrics
}

// Manifest is the in-memory authoritative view of every sealed segment
// in SegmentsDir, plus a lazy block-index LRU.
type Manifest struct {
	opts Options

	mu     sync.RWMutex
	bounds []SegmentBounds // sorted by Idx ascending

	// Block-index LRU lives on the struct so the field is type-stable
	// across tasks even though it stays unused until the LRU task.
	blockIdxLRUMu sync.Mutex
	blockIdxLRU   *blockIndexLRU // nil until the LRU task wires it up
}

// Open scans dir, parses every sealed seg_*.jss file's fixed header,
// and returns a Manifest ready for queries. Active segments (those with
// a zero checksum at offset 4..11) are silently skipped; corrupt files
// produce a wrapped error and abort startup.
func Open(opts Options) (*Manifest, error) {
	if opts.SegmentsDir == "" {
		return nil, fmt.Errorf("manifest: SegmentsDir is required")
	}
	if opts.Logger == nil {
		return nil, fmt.Errorf("manifest: Logger is required")
	}
	logger := opts.Logger.With(slog.String("component", "manifest"))

	files, err := ingest.SegmentFiles(opts.SegmentsDir)
	if err != nil {
		return nil, fmt.Errorf("manifest: list segments: %w", err)
	}

	m := &Manifest{
		opts:   opts,
		bounds: make([]SegmentBounds, 0, len(files)),
	}

	for _, f := range files {
		bounds, ok, err := readSealedBounds(f.Idx, f.Path)
		if err != nil {
			return nil, fmt.Errorf("manifest: read segment %s: %w", f.Path, err)
		}
		if !ok {
			continue
		}
		m.bounds = append(m.bounds, bounds)
	}

	// Defensive: ingest.SegmentFiles already sorts ascending by Idx, but
	// guard the invariant the rest of this package depends on.
	sort.Slice(m.bounds, func(i, j int) bool {
		return m.bounds[i].Idx < m.bounds[j].Idx
	})

	if opts.BlockIndexCacheSize > 0 {
		m.blockIdxLRU = newBlockIndexLRU(opts.BlockIndexCacheSize)
	}

	if opts.Metrics != nil {
		opts.Metrics.SegmentsLoaded.Set(float64(len(m.bounds)))
	}
	logger.Info("opened",
		"segments_dir", opts.SegmentsDir,
		"sealed_segments", len(m.bounds),
	)
	return m, nil
}

// SegmentCount returns the number of sealed segments tracked.
func (m *Manifest) SegmentCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.bounds)
}

// AllBounds returns a fresh copy of the bounds slice. Useful for tests
// and operator surface (status page).
func (m *Manifest) AllBounds() []SegmentBounds {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]SegmentBounds, len(m.bounds))
	copy(out, m.bounds)
	return out
}

// readSealedBounds opens path with the segment Reader. The bool is
// false (with nil error) iff the file is an active (unsealed) segment.
func readSealedBounds(idx uint64, path string) (SegmentBounds, bool, error) {
	// SkipChecksum=true at startup keeps cost bounded; operators who
	// want full integrity checks run the inspect-segment command.
	r, err := segment.Open(segment.ReaderConfig{Path: path, SkipChecksum: true})
	if err != nil {
		if isActiveSegmentSentinel(err) {
			return SegmentBounds{}, false, nil
		}
		return SegmentBounds{}, false, err
	}
	defer func() { _ = r.Close() }()

	h := r.Header()
	return SegmentBounds{
		Idx:          idx,
		Path:         path,
		MinSeq:       h.MinSeq,
		MaxSeq:       h.MaxSeq,
		MinIndexedAt: h.MinIndexedAt,
		MaxIndexedAt: h.MaxIndexedAt,
	}, true, nil
}

// isActiveSegmentSentinel checks whether the error from segment.Open
// indicates an active (unsealed) segment file, which we skip during
// manifest startup.
func isActiveSegmentSentinel(err error) bool {
	return errors.Is(err, segment.ErrActiveSegment)
}

// SegmentForSeq returns the bounds of the segment that contains seq.
// Returns (zero, false) if seq is past the live tip (newer than every
// sealed segment) or if the manifest is empty.
//
// "Contains" means MinSeq <= seq <= MaxSeq. The bounds slice is sorted
// by Idx ascending, which (because seq is monotonic across rotations)
// is also sorted by MinSeq ascending. We binary-search by MaxSeq:
// the first segment whose MaxSeq >= seq is the candidate. If that
// segment's MinSeq is also <= seq, it's a hit.
func (m *Manifest) SegmentForSeq(seq uint64) (SegmentBounds, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.bounds) == 0 {
		return SegmentBounds{}, false
	}
	i := sort.Search(len(m.bounds), func(i int) bool {
		return m.bounds[i].MaxSeq >= seq
	})
	if i == len(m.bounds) {
		return SegmentBounds{}, false
	}
	if seq < m.bounds[i].MinSeq {
		// Pathological: gap between segments. Should not happen in
		// practice (seq is contiguous across rotations) but we don't
		// silently lie about coverage.
		return SegmentBounds{}, false
	}
	return m.bounds[i], true
}

// SegmentForTimeUS returns the smallest sealed segment whose
// MaxIndexedAt >= timeUS. If timeUS is older than every sealed
// segment, returns the first segment (caller then clamps to the
// lookback floor). Returns (zero, false) only if timeUS is newer
// than every sealed segment, or the manifest is empty.
//
// Timestamp ranges across segments may overlap slightly in the
// presence of clock skew on the upstream relay; we still pick the
// smallest segment whose MaxIndexedAt covers the request, which
// gives the earliest possible event with indexed_at >= timeUS.
func (m *Manifest) SegmentForTimeUS(timeUS int64) (SegmentBounds, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.bounds) == 0 {
		return SegmentBounds{}, false
	}
	i := sort.Search(len(m.bounds), func(i int) bool {
		return m.bounds[i].MaxIndexedAt >= timeUS
	})
	if i == len(m.bounds) {
		return SegmentBounds{}, false
	}
	return m.bounds[i], true
}

// LookbackFloor returns the (seq, time_us) of the oldest event still
// retained under the given lookback duration, computed against the
// segment bounds and the current wall clock.
//
// The result is conservative: we return the MinSeq / MinIndexedAt of
// the segment that contains (or is newer than) the floor timestamp,
// not the exact event with indexed_at >= floor. This means cursor
// clamps may yield up to one segment's worth of extra lookback,
// never less.
//
// Returns (0, 0) when there are no sealed segments — the cursor
// resolver treats that as "no on-disk floor; replay the active
// segment from the beginning."
func (m *Manifest) LookbackFloor(lookback time.Duration) (uint64, int64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.bounds) == 0 {
		return 0, 0
	}
	floorTimeUS := time.Now().UnixMicro() - lookback.Microseconds()
	i := sort.Search(len(m.bounds), func(i int) bool {
		return m.bounds[i].MaxIndexedAt >= floorTimeUS
	})
	if i == len(m.bounds) {
		// All segments are older than the floor; clamp to the freshest
		// segment's MinSeq (lookback is shorter than retention skew).
		last := m.bounds[len(m.bounds)-1]
		return last.MinSeq, last.MinIndexedAt
	}
	return m.bounds[i].MinSeq, m.bounds[i].MinIndexedAt
}

// OnSegmentSealed publishes a freshly-sealed segment into the manifest.
// Wired through internal/ingest.Writer.Config.OnAfterSeal. Re-publishing
// an existing idx replaces the entry in place (idempotent for repeated
// callbacks; the on-disk state is authoritative).
//
// Reads the segment's fixed header to extract bounds — one pread on
// the metadata region, not a full file scan.
func (m *Manifest) OnSegmentSealed(idx uint64, path string) error {
	bounds, ok, err := readSealedBounds(idx, path)
	if err != nil {
		return fmt.Errorf("manifest: on_segment_sealed: %w", err)
	}
	if !ok {
		return fmt.Errorf("manifest: on_segment_sealed: %s appears active (zero checksum)", path)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	i := sort.Search(len(m.bounds), func(i int) bool {
		return m.bounds[i].Idx >= idx
	})
	switch {
	case i < len(m.bounds) && m.bounds[i].Idx == idx:
		m.bounds[i] = bounds
	case i == len(m.bounds):
		m.bounds = append(m.bounds, bounds)
	default:
		m.bounds = append(m.bounds, SegmentBounds{})
		copy(m.bounds[i+1:], m.bounds[i:])
		m.bounds[i] = bounds
	}

	if m.opts.Metrics != nil {
		m.opts.Metrics.SegmentsLoaded.Set(float64(len(m.bounds)))
	}
	return nil
}

// blockIndexLRU is a small bounded LRU of segment.BlockInfo slices,
// keyed by segment idx. Capacity is set at construction; on overflow,
// the least-recently-used entry is evicted.
//
// We hand-roll rather than pulling in golang-lru: the surface area
// is tiny (Get/Put), our hot path needs no extra allocations beyond
// the underlying slice itself, and an external dep would be the only
// non-stdlib import in this package.
type blockIndexLRU struct {
	cap   int
	order []uint64 // most-recent at the back
	data  map[uint64][]segment.BlockInfo
}

func newBlockIndexLRU(capacity int) *blockIndexLRU {
	return &blockIndexLRU{
		cap:   capacity,
		order: make([]uint64, 0, capacity),
		data:  make(map[uint64][]segment.BlockInfo, capacity),
	}
}

func (l *blockIndexLRU) Get(idx uint64) ([]segment.BlockInfo, bool) {
	v, ok := l.data[idx]
	if !ok {
		return nil, false
	}
	l.touch(idx)
	return v, true
}

func (l *blockIndexLRU) Put(idx uint64, blocks []segment.BlockInfo) {
	if _, ok := l.data[idx]; ok {
		l.data[idx] = blocks
		l.touch(idx)
		return
	}
	if len(l.order) >= l.cap {
		evicted := l.order[0]
		l.order = l.order[1:]
		delete(l.data, evicted)
	}
	l.order = append(l.order, idx)
	l.data[idx] = blocks
}

// touch moves idx to the back of the order slice (most recent).
// Caller must guarantee idx is in l.data.
func (l *blockIndexLRU) touch(idx uint64) {
	for i, v := range l.order {
		if v == idx {
			l.order = append(l.order[:i], l.order[i+1:]...)
			l.order = append(l.order, idx)
			return
		}
	}
}

// BlockIndex returns the cached []BlockInfo for segment idx, loading
// it via segment.Open on cache miss. Returns an error if idx is not
// a known segment, or if the segment file fails to open.
//
// The returned slice is shared across callers; treat it as read-only.
func (m *Manifest) BlockIndex(idx uint64) ([]segment.BlockInfo, error) {
	m.mu.RLock()
	var path string
	var found bool
	for _, b := range m.bounds {
		if b.Idx == idx {
			path = b.Path
			found = true
			break
		}
	}
	m.mu.RUnlock()
	if !found {
		return nil, fmt.Errorf("manifest: unknown segment idx %d", idx)
	}

	m.blockIdxLRUMu.Lock()
	defer m.blockIdxLRUMu.Unlock()

	if m.blockIdxLRU == nil {
		// Cache disabled (BlockIndexCacheSize <= 0). Load from disk
		// and return without storing.
		blocks, err := loadBlockIndex(path)
		if err != nil {
			return nil, err
		}
		if m.opts.Metrics != nil {
			m.opts.Metrics.BlockIndexCacheMissesTotal.Inc()
		}
		return blocks, nil
	}

	if cached, ok := m.blockIdxLRU.Get(idx); ok {
		if m.opts.Metrics != nil {
			m.opts.Metrics.BlockIndexCacheHitsTotal.Inc()
		}
		return cached, nil
	}

	if m.opts.Metrics != nil {
		m.opts.Metrics.BlockIndexCacheMissesTotal.Inc()
	}
	start := time.Now()
	blocks, err := loadBlockIndex(path)
	if err != nil {
		return nil, err
	}
	if m.opts.Metrics != nil {
		m.opts.Metrics.BlockIndexLoadSeconds.Observe(time.Since(start).Seconds())
	}
	m.blockIdxLRU.Put(idx, blocks)
	return blocks, nil
}

// loadBlockIndex opens path with segment.Open and returns its block
// index. SkipChecksum=true keeps the per-load cost bounded; the
// startup pass already validated structural integrity.
func loadBlockIndex(path string) ([]segment.BlockInfo, error) {
	r, err := segment.Open(segment.ReaderConfig{Path: path, SkipChecksum: true})
	if err != nil {
		return nil, fmt.Errorf("manifest: open %s: %w", path, err)
	}
	defer func() { _ = r.Close() }()
	return r.Blocks(), nil
}
