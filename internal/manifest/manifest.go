package manifest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/jcalabro/gloom"
	"github.com/jcalabro/gt"
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

// SegmentMetadata is the immutable metadata manifest keeps resident for
// every sealed segment. It intentionally excludes decoded event payloads
// and the open file descriptor; callers that need block bodies still open
// a segment.Reader for the short duration of the read.
type SegmentMetadata struct {
	SegmentBounds

	FileSize int64
	ModTime  time.Time
	Header   segment.Header

	Blocks                []segment.BlockInfo
	SegmentBloom          *gloom.Filter
	BlockBlooms           []*gloom.Filter
	Collections           []string
	CollectionEventCounts []uint32
	BlockCollections      [][]uint32
}

// SegmentTreeStats is the manifest-owned aggregate view used by operator
// surfaces such as /status. It is deliberately small and cheap to copy.
type SegmentTreeStats struct {
	Dir               string
	SealedCount       int
	ActiveCount       int
	CompressedBytes   int64
	UncompressedBytes int64
	DiskBytes         int64
	EventCount        uint64
	BlockCount        uint64
	OldestMTime       time.Time
	NewestMTime       time.Time
	MinSeq            uint64
	MaxSeq            uint64
	MinIndexedAt      int64
	MaxIndexedAt      int64
	LatestSegment     *SegmentSummary
	Collections       []CollectionStats
}

// SegmentSummary is the latest-segment projection used by status surfaces.
type SegmentSummary struct {
	Index           uint64
	EventCount      uint64
	UniqueDIDCount  uint32
	BlockCount      uint32
	CollectionCount int
	MinSeq          uint64
	MaxSeq          uint64
	MinIndexedAt    int64
	MaxIndexedAt    int64
	SizeBytes       int64
}

// CollectionStats is the manifest-owned aggregate view for one NSID.
type CollectionStats struct {
	NSID         string
	EventCount   uint64
	SegmentCount int
	BlockCount   uint64
}

// SegmentListEntry is the lightweight per-segment row returned by ListFrom.
// It deliberately excludes blooms, block indexes, and the file path.
type SegmentListEntry struct {
	Idx          uint64
	SizeBytes    int64
	Checksum     uint64
	EventCount   uint32
	MinSeq       uint64
	MaxSeq       uint64
	MinIndexedAt int64
	MaxIndexedAt int64
}

// SegmentFileRef is what a download handler needs to serve one sealed
// segment: the on-disk path plus the immutable metadata used for ETag,
// Last-Modified, and Content-Length.
type SegmentFileRef struct {
	Path      string
	Checksum  uint64
	ModTime   time.Time
	SizeBytes int64
}

// Options configures Open. SegmentsDir is required; the rest have
// safe zero-value defaults.
type Options struct {
	// SegmentsDir is the directory holding seg_*.jss files. Required.
	SegmentsDir string

	// BlockIndexCacheSize is retained for flag compatibility. Block
	// indices are now always loaded into the manifest at startup and on
	// segment seal.
	BlockIndexCacheSize int

	// Logger is required.
	Logger *slog.Logger

	// Metrics is optional. nil disables metric updates.
	Metrics *Metrics
}

// Manifest is the in-memory authoritative view of every sealed segment
// in SegmentsDir.
type Manifest struct {
	opts Options

	mu       sync.RWMutex
	segments []SegmentMetadata // sorted by Idx ascending
	loadErr  error
	ready    chan struct{}
}

// Open scans dir, parses every sealed seg_*.jss file's fixed header,
// and returns a Manifest ready for queries. Active segments (those with
// a zero checksum at offset 4..11) are silently skipped; corrupt files
// produce a wrapped error and abort startup.
func Open(opts Options) (*Manifest, error) {
	m, err := newManifest(opts)
	if err != nil {
		return nil, err
	}
	if err := m.load(context.Background()); err != nil {
		m.finishLoad(err)
		return nil, err
	}
	m.finishLoad(nil)
	return m, nil
}

// OpenBackground returns a Manifest immediately and starts loading
// sealed-segment metadata in a background goroutine. Callers that need
// manifest data must call Wait(ctx) or use Manifest methods, which
// block until the initial load completes.
func OpenBackground(ctx context.Context, opts Options) (*Manifest, error) {
	m, err := newManifest(opts)
	if err != nil {
		return nil, err
	}
	go func() {
		m.finishLoad(m.load(ctx))
	}()
	return m, nil
}

func newManifest(opts Options) (*Manifest, error) {
	if opts.SegmentsDir == "" {
		return nil, fmt.Errorf("manifest: SegmentsDir is required")
	}
	if opts.Logger == nil {
		return nil, fmt.Errorf("manifest: Logger is required")
	}
	return &Manifest{
		opts:     opts,
		segments: nil,
		ready:    make(chan struct{}),
	}, nil
}

func (m *Manifest) load(ctx context.Context) error {
	logger := m.opts.Logger.With(slog.String("component", "manifest"))

	files, err := ingest.SegmentFiles(m.opts.SegmentsDir)
	if err != nil {
		return fmt.Errorf("manifest: list segments: %w", err)
	}

	loadConcurrency := manifestLoadConcurrency()
	loaded, err := loadSealedMetadata(ctx, files, loadConcurrency, m.opts.Metrics)
	if err != nil {
		return err
	}

	m.mu.Lock()
	m.segments = loaded

	// Defensive: ingest.SegmentFiles already sorts ascending by Idx, but
	// guard the invariant the rest of this package depends on.
	sort.Slice(m.segments, func(i, j int) bool {
		return m.segments[i].Idx < m.segments[j].Idx
	})
	segmentCount := len(m.segments)
	m.mu.Unlock()

	if m.opts.Metrics != nil {
		m.opts.Metrics.SegmentsLoaded.Set(float64(segmentCount))
	}
	logger.Info("opened",
		"segments_dir", m.opts.SegmentsDir,
		"sealed_segments", segmentCount,
		"load_concurrency", loadConcurrency,
	)
	return nil
}

func (m *Manifest) finishLoad(err error) {
	m.mu.Lock()
	m.loadErr = err
	m.mu.Unlock()
	close(m.ready)
}

// Wait blocks until the initial manifest load finishes. It returns the
// load error, if any, or ctx.Err() if the caller stops waiting first.
func (m *Manifest) Wait(ctx context.Context) error {
	select {
	case <-m.ready:
		m.mu.RLock()
		defer m.mu.RUnlock()
		return m.loadErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manifest) waitReady() error {
	<-m.ready
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.loadErr
}

func manifestLoadConcurrency() int {
	n := runtime.GOMAXPROCS(0)
	if n < 1 {
		return 1
	}
	return n
}

type metadataLoadResult struct {
	meta   SegmentMetadata
	sealed bool
}

func loadSealedMetadata(ctx context.Context, files []ingest.SegmentFile, concurrency int, metrics *Metrics) ([]SegmentMetadata, error) {
	results, err := gt.ConcurrentN(ctx, files, concurrency, func(f ingest.SegmentFile) (metadataLoadResult, error) {
		start := time.Now()
		meta, ok, err := readSealedMetadata(f.Idx, f.Path, false)
		if err != nil {
			return metadataLoadResult{}, fmt.Errorf("manifest: read segment %s: %w", f.Path, err)
		}
		if !ok {
			return metadataLoadResult{}, nil
		}
		if metrics != nil {
			metrics.BlockIndexLoadSeconds.Observe(time.Since(start).Seconds())
		}
		return metadataLoadResult{meta: meta, sealed: true}, nil
	})
	if err != nil {
		return nil, err
	}

	out := make([]SegmentMetadata, 0, len(files))
	for _, result := range results {
		if result.sealed {
			out = append(out, result.meta)
		}
	}
	return out, nil
}

// SegmentCount returns the number of sealed segments tracked.
func (m *Manifest) SegmentCount() int {
	if err := m.waitReady(); err != nil {
		return 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.segments)
}

// AllBounds returns a fresh copy of the bounds slice. Useful for tests
// and operator surface (status page).
func (m *Manifest) AllBounds() []SegmentBounds {
	if err := m.waitReady(); err != nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]SegmentBounds, len(m.segments))
	for i := range m.segments {
		out[i] = m.segments[i].SegmentBounds
	}
	return out
}

// SegmentByIdx resolves a single sealed segment for download. ok is false
// when no sealed segment with that index is resident in the manifest
// (covers both never-existed and not-yet-sealed).
func (m *Manifest) SegmentByIdx(idx uint64) (SegmentFileRef, bool) {
	if err := m.waitReady(); err != nil {
		return SegmentFileRef{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	i := sort.Search(len(m.segments), func(i int) bool {
		return m.segments[i].Idx >= idx
	})
	if i >= len(m.segments) || m.segments[i].Idx != idx {
		return SegmentFileRef{}, false
	}
	meta := &m.segments[i]
	return SegmentFileRef{
		Path:      meta.Path,
		Checksum:  meta.Header.Checksum,
		ModTime:   meta.ModTime,
		SizeBytes: meta.FileSize,
	}, true
}

// ListFrom returns up to limit sealed-segment entries with Idx >= startIdx
// in ascending index order. more reports whether further entries remain
// beyond the returned page; when more is true, nextIdx is the index to pass
// as the next startIdx. When more is false, nextIdx is zero and undefined.
func (m *Manifest) ListFrom(startIdx uint64, limit int) (entries []SegmentListEntry, nextIdx uint64, more bool) {
	if limit <= 0 {
		return nil, 0, false
	}
	if err := m.waitReady(); err != nil {
		return nil, 0, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	start := sort.Search(len(m.segments), func(i int) bool {
		return m.segments[i].Idx >= startIdx
	})

	end := min(start+limit, len(m.segments))

	entries = make([]SegmentListEntry, 0, end-start)
	for i := start; i < end; i++ {
		meta := &m.segments[i]
		entries = append(entries, SegmentListEntry{
			Idx:          meta.Idx,
			SizeBytes:    meta.FileSize,
			Checksum:     meta.Header.Checksum,
			EventCount:   meta.Header.EventCount,
			MinSeq:       meta.Header.MinSeq,
			MaxSeq:       meta.Header.MaxSeq,
			MinIndexedAt: meta.Header.MinIndexedAt,
			MaxIndexedAt: meta.Header.MaxIndexedAt,
		})
	}

	if end < len(m.segments) {
		return entries, m.segments[end].Idx, true
	}
	return entries, 0, false
}

// SegmentStats returns the in-memory aggregate view of all sealed segments.
func (m *Manifest) SegmentStats() SegmentTreeStats {
	if err := m.waitReady(); err != nil {
		return SegmentTreeStats{Dir: m.opts.SegmentsDir}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := SegmentTreeStats{Dir: m.opts.SegmentsDir, SealedCount: len(m.segments)}
	if len(m.segments) == 0 {
		return stats
	}
	stats.OldestMTime = m.segments[0].ModTime
	stats.NewestMTime = m.segments[0].ModTime
	collections := make(map[string]*CollectionStats)
	for i := range m.segments {
		meta := &m.segments[i]
		stats.DiskBytes += meta.FileSize
		stats.EventCount += uint64(meta.Header.EventCount)
		stats.BlockCount += uint64(len(meta.Blocks))
		for _, b := range meta.Blocks {
			stats.CompressedBytes += int64(b.CompressedSize)
			stats.UncompressedBytes += int64(b.UncompressedSize)
		}
		if meta.Header.EventCount > 0 {
			if stats.MinSeq == 0 || meta.Header.MinSeq < stats.MinSeq {
				stats.MinSeq = meta.Header.MinSeq
			}
			if meta.Header.MaxSeq > stats.MaxSeq {
				stats.MaxSeq = meta.Header.MaxSeq
			}
			if stats.MinIndexedAt == 0 || meta.Header.MinIndexedAt < stats.MinIndexedAt {
				stats.MinIndexedAt = meta.Header.MinIndexedAt
			}
			if meta.Header.MaxIndexedAt > stats.MaxIndexedAt {
				stats.MaxIndexedAt = meta.Header.MaxIndexedAt
			}
		}
		if meta.ModTime.Before(stats.OldestMTime) {
			stats.OldestMTime = meta.ModTime
		}
		if meta.ModTime.After(stats.NewestMTime) {
			stats.NewestMTime = meta.ModTime
		}

		blockCountsByID := make(map[uint32]uint64, len(meta.Collections))
		for _, ids := range meta.BlockCollections {
			for _, id := range ids {
				blockCountsByID[id]++
			}
		}
		for i, nsid := range meta.Collections {
			agg, ok := collections[nsid]
			if !ok {
				agg = &CollectionStats{NSID: nsid}
				collections[nsid] = agg
			}
			var events uint32
			if i < len(meta.CollectionEventCounts) {
				events = meta.CollectionEventCounts[i]
			}
			agg.EventCount += uint64(events)
			agg.SegmentCount++
			agg.BlockCount += blockCountsByID[uint32(i)]
		}
	}

	latest := &m.segments[len(m.segments)-1]
	stats.LatestSegment = &SegmentSummary{
		Index:           latest.Idx,
		EventCount:      uint64(latest.Header.EventCount),
		UniqueDIDCount:  latest.Header.UniqueDIDCount,
		BlockCount:      uint32(len(latest.Blocks)),
		CollectionCount: len(latest.Collections),
		MinSeq:          latest.Header.MinSeq,
		MaxSeq:          latest.Header.MaxSeq,
		MinIndexedAt:    latest.Header.MinIndexedAt,
		MaxIndexedAt:    latest.Header.MaxIndexedAt,
		SizeBytes:       latest.FileSize,
	}
	stats.Collections = make([]CollectionStats, 0, len(collections))
	for _, c := range collections {
		stats.Collections = append(stats.Collections, *c)
	}
	sort.Slice(stats.Collections, func(i, j int) bool {
		if stats.Collections[i].EventCount != stats.Collections[j].EventCount {
			return stats.Collections[i].EventCount > stats.Collections[j].EventCount
		}
		return stats.Collections[i].NSID < stats.Collections[j].NSID
	})
	return stats
}

// readSealedMetadata opens path with the segment Reader. The bool is
// false (with nil error) iff the file is an active (unsealed) segment.
func readSealedMetadata(idx uint64, path string, verifyChecksum bool) (SegmentMetadata, bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		return SegmentMetadata{}, false, fmt.Errorf("stat: %w", err)
	}
	// SkipChecksum=true on the startup/seal paths keeps cost bounded;
	// the compacted-refresh path verifies (OnSegmentCompacted) and
	// operators who want full integrity checks run inspect-segment.
	r, err := segment.Open(segment.ReaderConfig{Path: path, SkipChecksum: !verifyChecksum})
	if err != nil {
		if isActiveSegmentSentinel(err) {
			return SegmentMetadata{}, false, nil
		}
		return SegmentMetadata{}, false, err
	}
	defer func() { _ = r.Close() }()

	h := r.Header()
	blocks := r.Blocks()
	blockCollections := make([][]uint32, len(blocks))
	for i := range blocks {
		ids, err := r.BlockCollections(i)
		if err != nil {
			return SegmentMetadata{}, false, err
		}
		blockCollections[i] = ids
	}
	blockBlooms, err := r.LoadAllBlockBlooms()
	if err != nil {
		return SegmentMetadata{}, false, err
	}

	return SegmentMetadata{
		SegmentBounds: SegmentBounds{
			Idx:          idx,
			Path:         path,
			MinSeq:       h.MinSeq,
			MaxSeq:       h.MaxSeq,
			MinIndexedAt: h.MinIndexedAt,
			MaxIndexedAt: h.MaxIndexedAt,
		},
		FileSize:              info.Size(),
		ModTime:               info.ModTime(),
		Header:                h,
		Blocks:                blocks,
		SegmentBloom:          r.SegmentBloom(),
		BlockBlooms:           blockBlooms,
		Collections:           r.Collections(),
		CollectionEventCounts: r.CollectionEventCounts(),
		BlockCollections:      blockCollections,
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
	if err := m.waitReady(); err != nil {
		return SegmentBounds{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.segments) == 0 {
		return SegmentBounds{}, false
	}
	i := sort.Search(len(m.segments), func(i int) bool {
		return m.segments[i].MaxSeq >= seq
	})
	if i == len(m.segments) {
		return SegmentBounds{}, false
	}
	if seq < m.segments[i].MinSeq {
		// Pathological: gap between segments. Should not happen in
		// practice (seq is contiguous across rotations) but we don't
		// silently lie about coverage.
		return SegmentBounds{}, false
	}
	return m.segments[i].SegmentBounds, true
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
	if err := m.waitReady(); err != nil {
		return SegmentBounds{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.segments) == 0 {
		return SegmentBounds{}, false
	}
	i := sort.Search(len(m.segments), func(i int) bool {
		return m.segments[i].MaxIndexedAt >= timeUS
	})
	if i == len(m.segments) {
		return SegmentBounds{}, false
	}
	return m.segments[i].SegmentBounds, true
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
	if err := m.waitReady(); err != nil {
		return 0, 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.segments) == 0 {
		return 0, 0
	}
	floorTimeUS := time.Now().UnixMicro() - lookback.Microseconds()
	i := sort.Search(len(m.segments), func(i int) bool {
		return m.segments[i].MaxIndexedAt >= floorTimeUS
	})
	if i == len(m.segments) {
		// All segments are older than the floor; clamp to the freshest
		// segment's MinSeq (lookback is shorter than retention skew).
		last := m.segments[len(m.segments)-1]
		return last.MinSeq, last.MinIndexedAt
	}
	return m.segments[i].MinSeq, m.segments[i].MinIndexedAt
}

// OnSegmentSealed publishes a freshly-sealed segment into the manifest.
// Wired through internal/ingest.Writer.Config.OnAfterSeal. Re-publishing
// an existing idx replaces the entry in place (idempotent for repeated
// callbacks; the on-disk state is authoritative).
func (m *Manifest) OnSegmentSealed(idx uint64, path string) error {
	return m.refreshSegment(idx, path, false)
}

// OnSegmentCompacted re-publishes a sealed segment after a compaction
// rewrite. Unlike OnSegmentSealed it verifies the file's header/footer
// checksum (cheap — the xxh3 covers header+footer bytes, not block
// data) as an integrity gate on the just-rewritten file before its
// metadata reaches serving paths.
func (m *Manifest) OnSegmentCompacted(idx uint64, path string) error {
	return m.refreshSegment(idx, path, true)
}

// SegmentChecksums returns the resident header checksum of every
// sealed segment keyed by index, as one snapshot. Used by the
// compactor's manifest reconcile to detect stale entries without
// re-reading full segment metadata.
func (m *Manifest) SegmentChecksums() map[uint64]uint64 {
	if err := m.waitReady(); err != nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[uint64]uint64, len(m.segments))
	for i := range m.segments {
		out[m.segments[i].Idx] = m.segments[i].Header.Checksum
	}
	return out
}

func (m *Manifest) refreshSegment(idx uint64, path string, verifyChecksum bool) error {
	if err := m.waitReady(); err != nil {
		return err
	}
	start := time.Now()
	meta, ok, err := readSealedMetadata(idx, path, verifyChecksum)
	if err != nil {
		return fmt.Errorf("manifest: refresh segment: %w", err)
	}
	if !ok {
		return fmt.Errorf("manifest: refresh segment: %s appears active (zero checksum)", path)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	i := sort.Search(len(m.segments), func(i int) bool {
		return m.segments[i].Idx >= idx
	})
	switch {
	case i < len(m.segments) && m.segments[i].Idx == idx:
		m.segments[i] = meta
	case i == len(m.segments):
		m.segments = append(m.segments, meta)
	default:
		m.segments = append(m.segments, SegmentMetadata{})
		copy(m.segments[i+1:], m.segments[i:])
		m.segments[i] = meta
	}

	if m.opts.Metrics != nil {
		m.opts.Metrics.SegmentsLoaded.Set(float64(len(m.segments)))
		m.opts.Metrics.BlockIndexLoadSeconds.Observe(time.Since(start).Seconds())
	}
	return nil
}

// BlockIndex returns the resident []BlockInfo for segment idx.
//
// The returned slice is shared across callers; treat it as read-only.
func (m *Manifest) BlockIndex(idx uint64) ([]segment.BlockInfo, error) {
	if err := m.waitReady(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for i := range m.segments {
		if m.segments[i].Idx == idx {
			if m.opts.Metrics != nil {
				m.opts.Metrics.BlockIndexCacheHitsTotal.Inc()
			}
			return m.segments[i].Blocks, nil
		}
	}
	if m.opts.Metrics != nil {
		m.opts.Metrics.BlockIndexCacheMissesTotal.Inc()
	}
	return nil, fmt.Errorf("manifest: unknown segment idx %d", idx)
}

// SegmentBlockSelection identifies, for one sealed segment, the blocks
// that may contain a given DID. Blocks holds ascending block indices
// into the segment's on-disk block array; Path locates the file.
type SegmentBlockSelection struct {
	Idx    uint64
	Path   string
	Blocks []int
}

// SelectBlocksForDID returns, for every sealed segment that may contain
// did, the blocks within it that may hold the DID -- computed entirely
// from the resident segment and per-block DID blooms, without opening a
// single segment file. This is the in-memory cache that lets repo
// reconstruction open only the few segments an account actually touches
// instead of every sealed segment on disk.
//
// The selection inherits SelectBlocksForDID's one-sided contract: no
// false negatives (every block that holds did is included), possible
// false positives (a returned block may not hold did; the caller
// filters per-event after decode). Segments whose segment-level bloom
// misses are omitted entirely. Results are ascending by segment Idx;
// the returned slices are fresh and safe for the caller to retain.
func (m *Manifest) SelectBlocksForDID(did string) ([]SegmentBlockSelection, error) {
	if err := m.waitReady(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	var out []SegmentBlockSelection
	for i := range m.segments {
		seg := &m.segments[i]
		blocks := segment.SelectBlocksForDID(seg.SegmentBloom, seg.BlockBlooms, did)
		if len(blocks) == 0 {
			continue
		}
		out = append(out, SegmentBlockSelection{
			Idx:    seg.Idx,
			Path:   seg.Path,
			Blocks: blocks,
		})
	}
	return out, nil
}

// ActiveSegmentPaths returns the paths of seg_*.jss files in SegmentsDir
// that are NOT resident in the manifest -- i.e. the active (unsealed)
// segment, plus any segment sealed so recently that OnSegmentSealed has
// not yet refreshed the manifest. The manifest only gains a segment's
// blooms at seal time, so its flushed-but-unsealed blocks are invisible
// to SelectBlocksForDID; callers that need a complete view (repo
// reconstruction) must scan these files directly.
//
// Returning a just-sealed file here is safe and deliberate: the caller
// replays it through a path that handles both active and sealed files,
// so a seal racing this call causes idempotent double-coverage, never a
// missed block. Ascending by path; empty when every on-disk segment is
// resident.
func (m *Manifest) ActiveSegmentPaths() ([]string, error) {
	if err := m.waitReady(); err != nil {
		return nil, err
	}
	files, err := ingest.SegmentFiles(m.opts.SegmentsDir)
	if err != nil {
		return nil, fmt.Errorf("manifest: list active segments: %w", err)
	}

	m.mu.RLock()
	resident := make(map[uint64]struct{}, len(m.segments))
	for i := range m.segments {
		resident[m.segments[i].Idx] = struct{}{}
	}
	m.mu.RUnlock()

	var out []string
	for _, f := range files {
		if _, ok := resident[f.Idx]; ok {
			continue
		}
		out = append(out, f.Path)
	}
	return out, nil
}

// SegmentBloom returns the resident segment-level DID bloom for segment idx.
func (m *Manifest) SegmentBloom(idx uint64) (*gloom.Filter, error) {
	if err := m.waitReady(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for i := range m.segments {
		if m.segments[i].Idx == idx {
			return m.segments[i].SegmentBloom, nil
		}
	}
	return nil, fmt.Errorf("manifest: unknown segment idx %d", idx)
}

// BlockBloom returns the resident per-block DID bloom for segment idx/block idx.
// The returned filter is shared across callers; treat it as read-only.
func (m *Manifest) BlockBloom(segIdx uint64, blockIdx int) (*gloom.Filter, error) {
	if err := m.waitReady(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for i := range m.segments {
		if m.segments[i].Idx != segIdx {
			continue
		}
		if blockIdx < 0 || blockIdx >= len(m.segments[i].BlockBlooms) {
			return nil, fmt.Errorf("manifest: segment %d block %d out of range", segIdx, blockIdx)
		}
		return m.segments[i].BlockBlooms[blockIdx], nil
	}
	return nil, fmt.Errorf("manifest: unknown segment idx %d", segIdx)
}

// BlockCollections returns the resident per-block collection ids for segment idx/block idx.
// The returned slice is shared across callers; treat it as read-only.
func (m *Manifest) BlockCollections(segIdx uint64, blockIdx int) ([]uint32, error) {
	if err := m.waitReady(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for i := range m.segments {
		if m.segments[i].Idx != segIdx {
			continue
		}
		if blockIdx < 0 || blockIdx >= len(m.segments[i].BlockCollections) {
			return nil, fmt.Errorf("manifest: segment %d block %d out of range", segIdx, blockIdx)
		}
		return m.segments[i].BlockCollections[blockIdx], nil
	}
	return nil, fmt.Errorf("manifest: unknown segment idx %d", segIdx)
}
