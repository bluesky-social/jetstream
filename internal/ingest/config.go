package ingest

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/segment"
)

// defaultMaxSegmentBytes is the rotation threshold. DESIGN.md §3.1.1
// names ~256MB as the target sealed-segment size. Operator-tunable
// via Config.MaxSegmentBytes.
const defaultMaxSegmentBytes int64 = 256 << 20

// defaultMaxEventsPerBlock mirrors segment.DefaultMaxEventsPerBlock.
const defaultMaxEventsPerBlock = segment.DefaultMaxEventsPerBlock

// Config controls Writer behavior.
type Config struct {
	// SegmentsDir is the directory holding seg_*.jss files (typically
	// <data-dir>/segments). Required. Created if missing.
	SegmentsDir string

	// Store is the shared metadata pebble db. Required.
	Store *store.Store

	// MaxSegmentBytes is the rotation threshold in compressed bytes
	// after the 256-byte reserved header. Default 256<<20 when zero.
	// Negative values are rejected.
	MaxSegmentBytes int64

	// MaxEventsPerBlock is forwarded to segment.Writer. Default
	// segment.DefaultMaxEventsPerBlock when zero.
	MaxEventsPerBlock int

	// SeqKey is the pebble key holding the writer's seq counter.
	// Default "seq/next" preserves backfill-writer behavior. The
	// live_segments consumer overrides this with
	// "live_segments/seq/next" so the two counters do not collide
	// when a single pebble store is shared between multiple writers.
	SeqKey string

	// OnAfterFlush, if non-nil, runs after each block flush has
	// completed: segment.Flush has fsynced and SeqKey has been
	// pebble.Sync'd. Errors propagate up through Append. A nil hook
	// is a no-op. Used by the live consumer to advance "relay/cursor"
	// with the same per-block cadence as seq/next.
	//
	// Hooks must not call back into the Writer (that would deadlock
	// on the writer mutex) or perform unbounded I/O (that would stall
	// every Append in the active worker pool).
	OnAfterFlush func(ctx context.Context) error

	// OnAfterSeal, if non-nil, runs after a successful segment seal
	// inside rotateLocked: segment.Writer.Seal has fsynced the footer
	// and finalized the fixed header before this hook fires. The hook
	// receives the just-sealed segment's numeric index and on-disk
	// path. Errors propagate up through Append and abort the rotation;
	// the segment file is sealed and closed by Seal before this hook
	// runs, so a hook failure leaves the writer with no usable active
	// segment — subsequent Appends will fail. Callers that want to
	// recover should Close the writer and reopen.
	//
	// Used by internal/manifest to publish the newly-sealed segment
	// into its in-memory bounds slice without polling the directory.
	//
	// Hooks must not call back into the Writer (that would deadlock
	// on the writer mutex) or perform unbounded I/O (that would stall
	// every Append in the active worker pool).
	OnAfterSeal func(idx uint64, path string) error

	// Logger is required (no sensible default for an ingestion
	// component whose failure modes need visibility).
	Logger *slog.Logger

	// Metrics is optional; nil means no /metrics counters incrementing.
	Metrics *Metrics

	// SegmentMetrics is forwarded to every segment.New call this writer
	// makes (initial open, post-rotation new active). Optional.
	SegmentMetrics *segment.Metrics
}

func (c *Config) validate() error {
	if c.SegmentsDir == "" {
		return fmt.Errorf("%w: SegmentsDir is required", ErrInvalidConfig)
	}
	if c.Store == nil {
		return fmt.Errorf("%w: Store is required", ErrInvalidConfig)
	}
	if c.Logger == nil {
		return fmt.Errorf("%w: Logger is required", ErrInvalidConfig)
	}
	if c.MaxSegmentBytes < 0 {
		return fmt.Errorf("%w: MaxSegmentBytes must be >= 0 (got %d)",
			ErrInvalidConfig, c.MaxSegmentBytes)
	}
	if c.MaxEventsPerBlock < 0 {
		return fmt.Errorf("%w: MaxEventsPerBlock must be >= 0 (got %d)",
			ErrInvalidConfig, c.MaxEventsPerBlock)
	}
	return nil
}

func (c *Config) applyDefaults() {
	if c.MaxSegmentBytes == 0 {
		c.MaxSegmentBytes = defaultMaxSegmentBytes
	}
	if c.MaxEventsPerBlock == 0 {
		c.MaxEventsPerBlock = defaultMaxEventsPerBlock
	}
	if c.SeqKey == "" {
		c.SeqKey = "seq/next"
	}
}
