// Package livestream: consumer.go owns Consumer, the firehose-to-
// segments pump. Open builds the underlying *ingest.Writer with
// the live-cursor advance hook wired in. Run subscribes to the
// upstream firehose and pushes events through ConvertEvent into
// the writer. Close flushes and tears everything down.
package livestream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/obs"
	"github.com/jcalabro/atmos/streaming"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/gt"
)

// Consumer drives the upstream firehose into a directory of
// segment files. Goroutine-safe to construct; Run is a
// single-producer loop.
type Consumer struct {
	cfg    Config
	writer *ingest.Writer

	// lastUpstream holds the highest upstream seq whose ops have ALL
	// been buffered into the active segment via writer.Append. It is
	// read by the OnAfterFlush hook to advance relay/cursor.
	// atomic.Int64 because OnAfterFlush is invoked from the writer's
	// internal goroutine (the caller of Append, but the writer holds
	// the mutex during the hook); making it atomic future-proofs us
	// for any later refactor that decouples them.
	lastUpstream atomic.Int64

	closeMu sync.Mutex
	closed  bool
}

// Open initializes the consumer's writer and validates config.
// Does not subscribe to the firehose; that happens in Run.
func Open(cfg Config) (*Consumer, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg.applyDefaults()

	c := &Consumer{cfg: cfg}

	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       cfg.SegmentsDir,
		Store:             cfg.Store,
		SeqKey:            cfg.SeqKey,
		MaxSegmentBytes:   cfg.MaxSegmentBytes,
		MaxEventsPerBlock: cfg.MaxEventsPerBlock,
		Logger:            cfg.Logger.With(slog.String("component", "livestream/ingest")),
		// Metrics intentionally nil: per-writer ingest metrics for
		// the live writer are not registered to avoid colliding with
		// the backfill writer's series. The livestream-level Metrics
		// (events received / converted, decode errors, reconnects,
		// upstream cursor) live in cfg.Metrics.
		Metrics:      nil,
		OnAfterFlush: c.onAfterFlush,
	})
	if err != nil {
		return nil, fmt.Errorf("livestream: open writer: %w", err)
	}
	c.writer = w

	return c, nil
}

// Close flushes any pending block, persists the cursor, and closes
// the underlying writer. Idempotent.
func (c *Consumer) Close() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	if c.writer == nil {
		return nil
	}
	// Flush the active segment and persist the next-seq counter.
	if err := c.writer.Close(); err != nil {
		return fmt.Errorf("livestream: close: %w", err)
	}
	// The writer.Close → segment.Close → flushLocked does not invoke
	// OnAfterFlush (that's only called during Append's full-block
	// path). Persist the final cursor explicitly.
	cur := c.lastUpstream.Load()
	if cur > 0 {
		if err := SaveUpstreamCursor(c.cfg.Store, c.cfg.CursorKey, cur); err != nil {
			return fmt.Errorf("livestream: close: save cursor: %w", err)
		}
		c.cfg.Metrics.setUpstreamCursor(cur)
	}
	return nil
}

// LastUpstreamSeq returns the highest upstream seq whose ops have
// all been buffered into the active segment. This is the in-memory
// value, NOT the persisted relay/cursor — the persisted cursor
// lags by at most one in-flight block.
//
// Used by tests and the future merge orchestrator that needs to
// know where to resume the steady-state consumer from.
func (c *Consumer) LastUpstreamSeq() int64 {
	return c.lastUpstream.Load()
}

// onAfterFlush is the ingest.Writer hook that runs after every
// block flush. Persists the highest fully-buffered upstream seq
// to relay/cursor with pebble.Sync. The placement of
// lastUpstream.Store in Run guarantees the value read here is
// always less than or equal to the latest durable event in the
// just-flushed block (DESIGN.md §3.1.1).
func (c *Consumer) onAfterFlush(ctx context.Context) error {
	cur := c.lastUpstream.Load()
	if cur == 0 {
		// Block flushed before any upstream event was fully
		// processed (only possible during very early startup if
		// the writer has pre-existing state). Skip the save —
		// nothing to persist.
		return nil
	}
	if err := SaveUpstreamCursor(c.cfg.Store, c.cfg.CursorKey, cur); err != nil {
		return err
	}
	c.cfg.Metrics.setUpstreamCursor(cur)
	return nil
}

// Run subscribes to the upstream relay's subscribeRepos firehose
// and pumps events into the underlying writer. Returns nil on
// clean context cancellation; returns the underlying error on a
// fatal write or pebble failure (so the errgroup can tear the
// process down).
//
// Reconnects with exponential backoff are handled internally by
// atmos's streaming.Client; Run does not see transient network
// errors as terminal.
func (c *Consumer) Run(ctx context.Context) error {
	c.closeMu.Lock()
	if c.closed {
		c.closeMu.Unlock()
		return ErrClosed
	}
	c.closeMu.Unlock()

	wsURL, err := deriveSubscribeReposURL(c.cfg.RelayURL)
	if err != nil {
		return fmt.Errorf("livestream: %w", err)
	}

	startCursor, err := LoadUpstreamCursor(c.cfg.Store, c.cfg.CursorKey)
	if err != nil {
		return fmt.Errorf("livestream: load start cursor: %w", err)
	}

	c.cfg.Logger.Info("livestream: subscribing",
		"url", wsURL,
		"start_cursor", startCursor,
	)

	opts := streaming.Options{
		URL:        wsURL,
		Cursor:     gt.Some(startCursor),
		SyncClient: gt.Some[*atmossync.Client](nil), // disable auto-resync; out of scope
		OnReconnect: gt.Some(func(attempt int, delay time.Duration) {
			c.cfg.Metrics.incReconnects()
			c.cfg.Logger.Warn("livestream: reconnecting",
				"attempt", attempt,
				"delay", delay,
			)
		}),
	}

	client, err := streaming.NewClient(opts)
	if err != nil {
		return fmt.Errorf("livestream: new client: %w", err)
	}
	defer func() {
		if cerr := client.Close(); cerr != nil {
			c.cfg.Logger.Warn("livestream: client close", "err", cerr)
		}
	}()

	tracer := obs.Tracer("livestream")

	for batch, err := range client.Events(ctx) {
		if err != nil {
			// Decode / sequence-gap errors flow through here;
			// atmos has already flushed the partial batch as nil
			// + err. Log and continue — the next iteration will
			// either reconnect or yield the next batch.
			c.cfg.Metrics.incDecodeErrors()
			c.cfg.Logger.Warn("livestream: stream error", "err", err)
			continue
		}

		batchCtx, span := tracer.Start(ctx, "livestream.batch")
		if perr := c.processBatch(batchCtx, batch); perr != nil {
			span.RecordError(perr)
			span.End()
			return perr
		}
		span.End()
	}

	c.cfg.Logger.Info("livestream: stopped",
		"last_upstream_seq", c.lastUpstream.Load(),
	)
	return ctx.Err()
}

// processBatch writes one batch of decoded events into the writer.
// Crucially, lastUpstream is updated only AFTER all ops of an event
// have been Append'd, so a flush triggered mid-event reads the
// previous fully-buffered upstream seq and the persisted cursor
// can never get ahead of the durable events.
func (c *Consumer) processBatch(ctx context.Context, batch []streaming.Event) error {
	indexedAt := c.cfg.now().UnixMicro()

	for _, evt := range batch {
		c.cfg.Metrics.incEventsReceived()

		segEvts, err := ConvertEvent(evt, indexedAt)
		switch {
		case errors.Is(err, ErrUnknownEventKind):
			// Forward-compat hole: a future relay variant we don't
			// know how to archive. Count it, log it, and crucially
			// LEAVE lastUpstream WHERE IT IS so a later build that
			// learns the kind can resume from this seq. Advancing
			// here would create a permanent gap in the archive.
			c.cfg.Metrics.incUnknownEvents()
			c.cfg.Logger.Warn("livestream: unknown event kind",
				"seq", evt.Seq,
			)
			continue
		case err != nil:
			c.cfg.Metrics.incDecodeErrors()
			// Bad shape from upstream is a data-integrity issue;
			// we surface it rather than silently dropping events.
			return fmt.Errorf("livestream: convert: %w", err)
		}

		for i := range segEvts {
			if err := c.writer.Append(ctx, &segEvts[i]); err != nil {
				return fmt.Errorf("livestream: append: %w", err)
			}
			c.cfg.Metrics.incEventsConverted()
		}

		// Only AFTER every op for this upstream event is buffered.
		if evt.Seq > 0 {
			c.lastUpstream.Store(evt.Seq)
		}
	}

	return nil
}
