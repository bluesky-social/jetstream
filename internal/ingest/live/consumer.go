// package live: consumer.go owns Consumer, the firehose-to-
// segments pump. Open builds the underlying *ingest.Writer with
// the live-cursor advance hook wired in. Run subscribes to the
// upstream firehose and pushes events through ConvertEvent into
// the writer. Close flushes and tears everything down.
package live

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/obs"
	"github.com/jcalabro/atmos/streaming"
	"github.com/jcalabro/gt"
)

// Consumer drives the upstream firehose into a directory of
// segment files. Goroutine-safe to construct; Run is a
// single-producer loop.
type Consumer struct {
	cfg Config

	// logger is cfg.Logger pre-attributed with
	// component=livestream/consumer for the consumer's own log
	// lines. cfg.Logger itself is left bare so child constructors
	// (ingest.Open) can set their own `component` without slog
	// stacking duplicate keys.
	logger *slog.Logger
	writer *ingest.Writer

	// lastUpstream is the highest upstream seq witnessed by
	// processBatch. Under atmos's default Parallelism>1 the seqs in
	// a single batch are in completion order, not seq order, so we
	// take a max here. This is the value persisted to relay/cursor
	// when no streaming.Client is attached yet (test paths that
	// drive processBatch directly, or the very first Close before
	// the client has yielded any events).
	//
	// In Run, we install `client` below and prefer client.Cursor()
	// over lastUpstream, because atmos's watermark is the only
	// safe-to-persist value: it stays behind any seq still being
	// verified in another worker. Persisting lastUpstream from
	// inside Run would risk skipping a still-in-flight smaller seq
	// across a crash, dropping it from the archive permanently.
	lastUpstream atomic.Int64

	// client is set once at the top of Run after NewClient succeeds,
	// and consulted by onAfterFlush and Close to read the
	// watermark-correct cursor. nil before Run starts, after a Run
	// failure that never reached client construction, and in tests
	// that exercise processBatch in isolation.
	client atomic.Pointer[streaming.Client]

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

	c := &Consumer{
		cfg:    cfg,
		logger: cfg.Logger.With(slog.String("component", "livestream/consumer")),
	}

	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       cfg.SegmentsDir,
		Store:             cfg.Store,
		SeqKey:            cfg.SeqKey,
		MaxSegmentBytes:   cfg.MaxSegmentBytes,
		MaxEventsPerBlock: cfg.MaxEventsPerBlock,

		// Bare cfg.Logger; ingest.Open sets its own
		// component=ingest/writer attribute.
		Logger: cfg.Logger,

		// Metrics intentionally nil: per-writer ingest metrics for
		// the live writer are not registered to avoid colliding with
		// the backfill writer's series. The livestream-level Metrics
		// (events received / converted, decode errors, reconnects,
		// upstream cursor) live in cfg.Metrics.
		Metrics:        nil,
		OnAfterFlush:   c.onAfterFlush,
		OnAfterSeal:    cfg.OnAfterSeal,
		SegmentMetrics: cfg.SegmentMetrics,
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
	cur := c.cursorValue()
	if cur > 0 {
		if err := SaveUpstreamCursor(c.cfg.Store, c.cfg.CursorKey, cur); err != nil {
			return fmt.Errorf("livestream: close: save cursor: %w", err)
		}
		c.cfg.Metrics.setUpstreamCursor(cur)
	}
	return nil
}

// cursorValue returns the safe-to-persist upstream cursor: atmos's
// watermark when a streaming.Client is attached, falling back to
// lastUpstream otherwise (tests + early shutdown before any events
// flow). The watermark is always <= the highest seq we've buffered,
// so persisting it can never skip a still-in-flight smaller seq.
func (c *Consumer) cursorValue() int64 {
	if cl := c.client.Load(); cl != nil {
		return cl.Cursor()
	}
	return c.lastUpstream.Load()
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
// block flush. Persists atmos's safe watermark cursor to
// relay/cursor with pebble.Sync. The watermark is the smallest
// in-flight seq minus one (or the highest yielded seq when
// nothing is in flight); persisting it can never skip a
// still-being-verified smaller seq across a crash.
func (c *Consumer) onAfterFlush(ctx context.Context) error {
	cur := c.cursorValue()
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
	closed := c.closed
	c.closeMu.Unlock()
	if closed {
		return ErrClosed
	}

	wsURL, err := deriveSubscribeReposURL(c.cfg.RelayURL)
	if err != nil {
		return fmt.Errorf("livestream: %w", err)
	}

	startCursor, err := LoadUpstreamCursor(c.cfg.Store, c.cfg.CursorKey)
	if err != nil {
		return fmt.Errorf("livestream: load start cursor: %w", err)
	}

	c.logger.InfoContext(ctx, "subscribing",
		"url", wsURL,
		"start_cursor", startCursor,
	)

	opts := streaming.Options{
		URL:       wsURL,
		Cursor:    gt.Some(startCursor),
		BatchSize: gt.Some(1),
		// Verifier is supplied by the caller via livestream.Config; the
		// streaming layer would otherwise auto-attach an in-memory verifier
		// that doesn't survive restart. cmd/jetstream constructs ours with
		// a pebble-backed StateStore + identity cache.
		Verifier: gt.Some(c.cfg.Verifier),
		OnReconnect: gt.Some(func(attempt int, delay time.Duration) {
			c.cfg.Metrics.incReconnects()
			c.logger.WarnContext(ctx, "reconnecting",
				"attempt", attempt,
				"delay", delay,
			)
		}),
	}

	client, err := streaming.NewClient(opts)
	if err != nil {
		return fmt.Errorf("livestream: new client: %w", err)
	}
	c.client.Store(client)
	defer func() {
		// On a clean ctx-cancel shutdown atmos's Events iterator has
		// already torn the socket down (consumeLoop calls conn.CloseNow
		// when its read loop exits), so this client.Close races that and
		// finds the connection already closed, returning a wrapped
		// net.ErrClosed. That's the expected steady-state shutdown path,
		// not a fault — suppress it. We still call Close because on the
		// error-return path (a fatal write/pebble failure surfaced from
		// processBatch) the iterator has NOT cancelled, so Close is what
		// releases the live socket; any non-already-closed error there is
		// genuine and worth a warning.
		cerr := client.Close()
		if cerr != nil && !errors.Is(cerr, net.ErrClosed) {
			c.logger.WarnContext(ctx, "client close", "err", cerr)
		}
	}()

	for batch, err := range client.Events(ctx) {
		if err != nil {
			// Decode / sequence-gap errors flow through here;
			// atmos has already flushed the partial batch as nil
			// + err. Log and continue — the next iteration will
			// either reconnect or yield the next batch.
			c.cfg.Metrics.incDecodeErrors()
			c.logger.WarnContext(ctx, "stream error", "err", err)
			continue
		}

		if perr := c.processBatch(ctx, batch); perr != nil {
			return perr
		}
	}

	return ctx.Err()
}

// processBatch writes one batch of decoded events into the writer.
func (c *Consumer) processBatch(ctx context.Context, batch []streaming.Event) error {
	return obs.Span(ctx, func(ctx context.Context) error {
		indexedAt := c.cfg.now().UnixMicro()

		for _, evt := range batch {
			c.cfg.Metrics.incEventsReceived()

			segEvts, err := ConvertEvent(evt, indexedAt)
			dme, isDropped := errors.AsType[*DroppedMissingBlocksError](err)
			switch {
			case errors.Is(err, ErrUnknownEventKind):
				// Forward-compat hole: a future relay variant we don't
				// know how to archive. Count and log; the cursor we
				// persist to relay/cursor comes from atmos's
				// watermark, which advances past unknown kinds either
				// way. A later build that learns this kind will
				// re-fetch from cursor and any events at-or-after the
				// last persisted watermark — under Parallelism>1 the
				// watermark trails the highest yielded seq, so most
				// near-real-time unknowns are still recoverable on
				// restart, but events that fall behind the watermark
				// before a restart are unreachable. Acceptable trade
				// for cross-DID throughput; revisit if the unknown-
				// event rate ever becomes non-trivial.
				c.cfg.Metrics.incUnknownEvents()
				c.logger.WarnContext(ctx, "unknown event kind",
					"seq", evt.Seq,
				)
				continue
			case isDropped:
				// Partial-CAR commit from a non-canonical PDS: one or
				// more create/update ops referenced CIDs whose blocks
				// were absent from the CAR diff. Bump the metric, log
				// the offending DID, and fall through to archive the
				// surviving ops (segEvts is non-nil and contains the
				// well-formed events). A misbehaving upstream must NOT
				// take the firehose down — a single such commit took
				// down a multi-hour bootstrap backfill before this
				// arm was added.
				c.cfg.Metrics.addDroppedOpsMissingBlock(len(dme.Dropped))
				// One log line per affected event keeps volume
				// bounded under a misbehaving PDS spamming the
				// firehose; per-op detail is on dme.Dropped for a
				// future flag that wants it.
				c.logger.WarnContext(ctx, "dropped ops with missing record blocks",
					"seq", evt.Seq,
					"count", len(dme.Dropped),
					"did", dme.Dropped[0].DID,
				)
			case err != nil:
				c.cfg.Metrics.incDecodeErrors()
				// Bad shape from upstream is invalid external input,
				// not local corruption. Count, log, and advance past
				// this recognized-but-malformed event so one bad repo
				// cannot wedge the firehose consumer forever. Internal
				// append/flush/pebble failures still return below.
				c.logger.WarnContext(ctx, "malformed upstream event",
					"seq", evt.Seq,
					"err", err,
				)
				c.noteUpstreamSeq(evt.Seq)
				continue
			}

			for i := range segEvts {
				if err := c.writer.Append(ctx, &segEvts[i]); err != nil {
					return fmt.Errorf("livestream: append: %w", err)
				}
				c.cfg.Metrics.incEventsConverted()

				// Forward to downstream subscribers AFTER durable append.
				// segEvts[i].Seq has been populated by Append.
				if c.cfg.OnEvent != nil {
					c.cfg.OnEvent(&segEvts[i])
				}
			}

			// Track the highest seq we've witnessed. Under
			// Parallelism>1 the seqs in this batch are in
			// completion order, so we max rather than store
			// blindly. lastUpstream is informational; the
			// cursor we persist comes from cursorValue() which
			// prefers atmos's watermark.
			c.noteUpstreamSeq(evt.Seq)
		}

		return nil
	})
}

func (c *Consumer) noteUpstreamSeq(seq int64) {
	if seq <= 0 {
		return
	}
	for {
		prev := c.lastUpstream.Load()
		if seq <= prev || c.lastUpstream.CompareAndSwap(prev, seq) {
			return
		}
	}
}

// Writer returns the live consumer's ingest writer. May be nil before
// Open completes; after Open returns successfully, this is stable
// until Close. Used by callers (cmd/jetstream) that need a writer
// reference for cursor-replay handler wiring.
func (c *Consumer) Writer() *ingest.Writer {
	return c.writer
}
