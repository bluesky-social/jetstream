package status

import (
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
)

// Snapshot is the rendering-agnostic view of jetstream's state at a
// single moment. Treated as immutable after construction; consumers
// share a *Snapshot pointer without locks.
type Snapshot struct {
	GeneratedAt      time.Time
	Process          ProcessInfo
	Phase            PhaseInfo
	Backfill         BackfillStats
	Live             LiveStats
	SegmentAggregate *SegmentAggregate
	Pebble           PebbleStats
	CursorLookback   CursorLookbackStats
}

// ProcessInfo carries the per-process build + uptime context.
type ProcessInfo struct {
	Version   string
	Commit    string
	BuiltAt   string
	StartedAt time.Time
	Uptime    time.Duration
	GoVersion string
}

// PhaseInfo holds the current persisted phase plus its transition
// timestamp. PhaseEnteredAt is zero if no phase/entered_at key exists
// (a process that pre-dates the field, or a fresh data dir).
type PhaseInfo struct {
	Phase          lifecycle.Phase
	PhaseEnteredAt time.Time
}

// BackfillStats summarizes the repo/ keyspace.
type BackfillStats struct {
	TotalDIDs       uint64
	Discovered      uint64
	Complete        uint64
	Failed          uint64
	PercentComplete float64
	ListReposCursor string
}

// LiveStats summarizes the upstream cursor and seq counters.
type LiveStats struct {
	UpstreamCursor int64
	NextSeq        uint64
	BootstrapSeq   uint64
}

// SegmentSummary mirrors segment.Inspection's user-facing fields,
// converted to time.Time so renderers don't have to know about
// unix-micros.
type SegmentSummary struct {
	Index           uint64
	Sealed          bool
	EventCount      uint64
	UniqueDIDCount  uint32
	BlockCount      uint32
	CollectionCount int
	MinSeq          uint64
	MaxSeq          uint64
	MinIndexedAt    time.Time
	MaxIndexedAt    time.Time
	SizeBytes       int64
}

// PebbleStats summarizes meta.pebble/ on disk plus per-prefix key
// counts.
type PebbleStats struct {
	DiskBytes      int64
	KeyspaceCounts map[string]uint64
}

// CursorLookbackStats summarizes the cursor-replay observability
// surface: configured lookback duration, sealed-segment count, and
// the oldest seq that's still within the lookback window. Empty
// (zero values) when the manifest is unavailable or cursor lookback
// is disabled.
type CursorLookbackStats struct {
	// ConfiguredLookback is the operator-set --cursor-lookback duration.
	// Zero means cursor replay is disabled.
	ConfiguredLookback time.Duration

	// ManifestSegmentCount is the number of sealed segments tracked
	// in the in-memory manifest.
	ManifestSegmentCount int

	// OldestRetainedSeq is the smallest seq still within the lookback
	// window. Computed from manifest.LookbackFloor.
	OldestRetainedSeq uint64

	// OldestRetainedAt is the corresponding timestamp. Zero when no
	// sealed segments exist.
	OldestRetainedAt time.Time
}
