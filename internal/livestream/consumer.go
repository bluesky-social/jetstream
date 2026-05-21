// Package livestream: consumer.go owns Consumer, the firehose-to-
// segments pump. Open builds the underlying *ingest.Writer with
// the live-cursor advance hook wired in. Run subscribes to the
// upstream firehose and pushes events through ConvertEvent into
// the writer. Close flushes and tears everything down.
package livestream

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
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
	if err := c.writer.Close(); err != nil {
		return fmt.Errorf("livestream: close: %w", err)
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
