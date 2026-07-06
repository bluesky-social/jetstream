package ingest

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble"
)

// DurableBatchHook stages block-specific metadata into the same synced Pebble
// batch that persists the writer's next sequence after a segment block is
// durable. prepareValue is the value sampled by DurableBatchPrepareValue before
// the block was detached/flushed.
type DurableBatchHook func(ctx context.Context, b *pebble.Batch, nextSeq uint64, force bool, prepareValue any) (afterCommit func(), afterDone func(error), err error)

// defaultMaxSegmentBytes is the rotation threshold. DESIGN.md §3.1.1
// names ~256MB as the target sealed-segment size. Operator-tunable
// via Config.MaxSegmentBytes.
const defaultMaxSegmentBytes int64 = 256 << 20

// defaultMaxEventsPerBlock mirrors segment.DefaultMaxEventsPerBlock.
const defaultMaxEventsPerBlock = segment.DefaultMaxEventsPerBlock

// Config controls Writer behavior.
type Config struct {
	// DataDir is the root jetstream data directory. Optional outside the
	// production orchestrator; when set it is included in fatal persistence
	// errors such as ENOSPC.
	DataDir string

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

	// AsyncFlushWorkers enables a backfill-oriented pipeline that detaches
	// full segment blocks under the writer mutex, compresses them outside the
	// mutex, then commits them in order before AppendBatch returns.
	AsyncFlushWorkers int

	// ReadLogRetentionBytes bounds the durable tail retained in the writer's
	// readable log. Events that are not yet durable are pinned regardless of
	// this budget. Zero is legal and means "retain only pinned events". Negative
	// values are rejected.
	ReadLogRetentionBytes int64

	// SeqKey is the pebble key holding the writer's seq counter.
	// Default "seq/next" preserves backfill-writer behavior. The
	// live_segments consumer overrides this with
	// "live_segments/seq/next" so the two counters do not collide
	// when a single pebble store is shared between multiple writers.
	SeqKey string

	// OnDurableBatch, if non-nil, stages extra metadata into the same synced
	// Pebble batch that persists SeqKey after a segment block has been fsynced.
	// The hook may return an afterCommit callback, which runs only after the
	// batch commit succeeds, and an afterDone callback, which runs after the
	// batch commit attempt on both success and failure.
	//
	// The hook, afterCommit callback, and afterDone callback run under the
	// writer mutex. Hooks must not call back into the Writer (that would
	// deadlock) or perform unbounded I/O (that would stall every Append in the
	// active worker pool).
	//
	// force is true only on the drain/terminal commit paths (DrainDurability,
	// Close, SealActiveAndClose) and means "commit the seq/next checkpoint even
	// though no new block was just flushed." It does NOT license the hook to
	// stage metadata whose backing events are not yet durable: a hook that ties
	// metadata to event durability (e.g. backfill repo completion) must still
	// gate every staged row on nextSeq, never on force, or it would mark data
	// durable ahead of its segment fsync (violating DESIGN.md §3.1.1 ordering).
	OnDurableBatch DurableBatchHook

	// DurableBatchPrepareValue, if non-nil, is sampled while the writer mutex is
	// held immediately before a block is detached/flushed. The sampled value is
	// carried to OnDurableBatch for that specific block, including async flush
	// jobs. Force/terminal durable commits sample it at commit time. This is for
	// metadata that must be tied to the block's prepare-time view, such as the
	// live relay cursor watermark.
	//
	// The sampler must not call back into the Writer and must be cheap.
	DurableBatchPrepareValue func() any

	// OnAppend, if non-nil, runs synchronously inside Append after
	// the event's Seq is assigned and BEFORE the block can flush or
	// the segment can seal, under the writer mutex. This ordering is
	// load-bearing for the compaction tombstone set: any seq visible
	// in a sealed on-disk header has already passed through OnAppend,
	// so a concurrently-running compaction pass that discovers the
	// sealed file can never compute a watermark covering an event the
	// hook has not yet observed. An error fails the Append.
	//
	// Hooks must not call back into the Writer (deadlock on w.mu) and
	// must be cheap — they run on the hot ingest path for every event.
	OnAppend func(ev *segment.Event) error

	// OnAfterSeal, if non-nil, runs after a successful segment seal
	// during rotation or SealActiveAndClose: segment.Writer.Seal has
	// fsynced the footer and finalized the fixed header before this
	// hook fires. The hook receives the just-sealed segment's numeric
	// index and on-disk path. Errors propagate up through the caller;
	// the segment file is sealed and closed by Seal before this hook
	// runs, so a hook failure leaves the writer with no usable active
	// segment. Callers that want to recover should Close the writer
	// and reopen.
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
	SegmentMetrics segment.SealObserver

	// SegmentIOFaultInjector is a test-only seam forwarded to segment.Writer.
	// Nil in production.
	SegmentIOFaultInjector segment.IOFaultInjector
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
	if c.AsyncFlushWorkers < 0 {
		return fmt.Errorf("%w: AsyncFlushWorkers must be >= 0 (got %d)",
			ErrInvalidConfig, c.AsyncFlushWorkers)
	}
	if c.ReadLogRetentionBytes < 0 {
		return fmt.Errorf("%w: ReadLogRetentionBytes must be >= 0 (got %d)",
			ErrInvalidConfig, c.ReadLogRetentionBytes)
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
