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

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/streaming"
	"github.com/jcalabro/gt"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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

	// Tombstone observation runs as the writer's OnAppend hook —
	// under the writer mutex, after seq assignment, before the block
	// can flush or the segment seal — NOT after Append returns. The
	// steady-state compactor discovers sealed segments by directory
	// scan from its own goroutine; observing post-Append would open a
	// window where a freshly sealed header's MaxSeq covers a
	// tombstone the set does not yet contain, and the pass would
	// durably advance the watermark past it, evicting it unapplied.
	var onAppend func(ev *segment.Event) error
	if cfg.Tombstones != nil {
		ts := cfg.Tombstones
		onAppend = func(ev *segment.Event) error { return ts.Observe(ev) }
	}

	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       cfg.SegmentsDir,
		Store:             cfg.Store,
		SeqKey:            cfg.SeqKey,
		MaxSegmentBytes:   cfg.MaxSegmentBytes,
		MaxEventsPerBlock: cfg.MaxEventsPerBlock,
		OnAppend:          onAppend,

		// Bare cfg.Logger; ingest.Open sets its own
		// component=ingest/writer attribute.
		Logger: cfg.Logger,

		// WriterMetrics is nil for bootstrap live_segments and shared
		// with the canonical ingest metrics in steady-state.
		Metrics:        cfg.WriterMetrics,
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
		if err := c.saveCursorAndSyncState(cur); err != nil {
			return fmt.Errorf("livestream: close: save cursor: %w", err)
		}
		c.cfg.Metrics.setUpstreamCursor(cur)
	} else if c.cfg.SyncStateStore != nil {
		// No cursor to persist (shutdown before the first yielded
		// event), but promoted verifier state can still exist — its
		// rows were appended and writer.Close fsynced them above.
		// Flush it so it isn't silently dropped.
		if err := c.cfg.SyncStateStore.Flush(); err != nil {
			return fmt.Errorf("livestream: close: flush sync state: %w", err)
		}
	}
	return nil
}

// promoteSyncState marks the verifier chain/hosting state staged for
// one upstream event as durable-eligible, after every row of that
// event's group has been appended to the writer. Until promotion, the
// state stays pending in memory and is never flushed to pebble — so a
// crash mid-group leaves durable chain state at the previous rev, the
// event is redelivered (or the next commit chain-breaks and triggers a
// fresh resync), and a durable KindSync tombstone can never sit above
// a partially-archived replacement set (compaction spec §2.2).
func (c *Consumer) promoteSyncState(segEvts []segment.Event) {
	if c.cfg.SyncStateStore == nil || len(segEvts) == 0 {
		return
	}
	did := atmos.DID(segEvts[0].DID)
	var maxRev string
	for i := range segEvts {
		if segEvts[i].Rev > maxRev {
			maxRev = segEvts[i].Rev
		}
		if segEvts[i].Kind == segment.KindAccount {
			c.cfg.SyncStateStore.PromoteHosting(atmos.DID(segEvts[i].DID), segEvts[i].UpstreamRelayCursor)
		}
	}
	if maxRev != "" {
		c.cfg.SyncStateStore.PromoteChain(did, maxRev)
	}
}

// dropStaleOrderedAsyncResync guards against atmos's async-resync
// delivery race: the synthetic resync event travels on a separate
// channel from the ordered result stream, so a post-resync #commit for
// the same DID can be yielded — and archived — BEFORE the resync's
// KindSync row. Archiving the KindSync row then would give the DID
// tombstone a seq ABOVE the newer commit's rows and compaction would
// permanently erase them. When the persisted/staged chain rev is
// already past the resync's rev, the resync is stale-ordered (or
// pipelined verification has simply run ahead — indistinguishable);
// drop the whole event. Cost of a false positive is bounded staleness
// (this resync's tombstone coverage waits for the next divergence);
// cost of archiving a stale-ordered one is permanent data destruction.
// The root fix is ordered delivery in atmos (compaction spec §12).
func (c *Consumer) dropStaleOrderedAsyncResync(ctx context.Context, evt streaming.Event, segEvts []segment.Event) (bool, error) {
	if evt.Resync != streaming.ResyncAsync || c.cfg.SyncStateStore == nil {
		return false, nil
	}
	if len(segEvts) == 0 || segEvts[0].Kind != segment.KindSync {
		return false, nil
	}
	state, err := c.cfg.SyncStateStore.LoadChain(ctx, atmos.DID(segEvts[0].DID))
	if err != nil {
		return false, fmt.Errorf("livestream: async resync ordering guard: %w", err)
	}
	if state == nil || state.Rev <= segEvts[0].Rev {
		return false, nil
	}
	c.cfg.Metrics.incStaleResyncsDropped()
	c.logger.WarnContext(ctx, "dropped stale-ordered async resync",
		"did", segEvts[0].DID,
		"resync_rev", segEvts[0].Rev,
		"chain_rev", state.Rev,
	)
	return true, nil
}

// dropReplayedAccountEvent guards against relay seq replays of #account
// events. atmos's verifier replay-drops duplicate #commit/#sync
// deliveries (rev-replay protection), but OnAccountEvent's seq guard only
// suppresses verifier STATE updates — the event still flows to the
// consumer. Without this check a relay regression (e.g. restored from
// backup) re-archives the #account row at a fresh jetstream seq, and a
// replayed account-delete landing ABOVE a later reactivate+recreate makes
// every fold (oracle reconstruct, tombstone set, compaction) erase live
// records permanently. The comparison uses the APPLIED hosting seq
// (promoted or pebble-durable, never pending): promotion happens
// synchronously after this DID's row is appended and per-DID delivery is
// seq-ordered, so seq <= applied means this exact event's row is already
// in the archive — a second delivery is a duplicate by construction.
// Pending state is excluded because a later pipelined event could stage
// it before its own rows land, and consulting it would drop a legitimate
// intermediate event.
func (c *Consumer) dropReplayedAccountEvent(ctx context.Context, segEvts []segment.Event) (bool, error) {
	if c.cfg.SyncStateStore == nil || len(segEvts) != 1 || segEvts[0].Kind != segment.KindAccount {
		return false, nil
	}
	state, err := c.cfg.SyncStateStore.LoadAppliedHosting(ctx, atmos.DID(segEvts[0].DID))
	if err != nil {
		return false, fmt.Errorf("livestream: account replay guard: %w", err)
	}
	if state == nil || segEvts[0].UpstreamRelayCursor > state.Seq {
		return false, nil
	}
	c.cfg.Metrics.incReplayedAccountEventsDropped()
	c.logger.WarnContext(ctx, "dropped replayed account event",
		"did", segEvts[0].DID,
		"seq", segEvts[0].UpstreamRelayCursor,
		"applied_seq", state.Seq,
	)
	return true, nil
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
// Used only by tests to assert how far the consumer has progressed.
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
	if err := c.saveCursorAndSyncState(cur); err != nil {
		return err
	}
	c.cfg.Metrics.setUpstreamCursor(cur)
	return nil
}

func (c *Consumer) saveCursorAndSyncState(cur int64) error {
	if cur < 0 {
		return fmt.Errorf("livestream: refuse to save negative cursor %d to %s", cur, c.cfg.CursorKey)
	}
	b := c.cfg.Store.NewBatch()
	defer func() { _ = b.Close() }()
	if err := b.Set([]byte(c.cfg.CursorKey), store.EncodeVersionedUint64LE(cursorV1, uint64(cur)), nil); err != nil {
		return fmt.Errorf("livestream: stage %s: %w", c.cfg.CursorKey, err)
	}
	if c.cfg.SyncStateStore != nil {
		if err := c.cfg.SyncStateStore.StageFlush(b); err != nil {
			return err
		}
	}
	if err := c.cfg.Store.Commit(b, store.SyncWrites); err != nil {
		return fmt.Errorf("livestream: save %s: %w", c.cfg.CursorKey, err)
	}
	if c.cfg.SyncStateStore != nil {
		c.cfg.SyncStateStore.CommitStaged()
	}
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
	if c.cfg.ReconnectBackoff != nil {
		opts.Backoff = gt.Some(*c.cfg.ReconnectBackoff)
	}
	if c.cfg.Dial != nil {
		opts.Dial = gt.Some(c.cfg.Dial)
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
			// Stream-level errors flow through here; atmos has
			// already flushed the partial batch as nil + err.
			// Classify, log, and continue — the next iteration
			// will either reconnect or yield the next batch.
			c.noteStreamError(ctx, err)
			continue
		}

		if perr := c.processBatch(ctx, batch); perr != nil {
			return perr
		}
	}

	return ctx.Err()
}

// noteStreamError records one stream-level (nil, err) yield from the
// atmos iterator. The classes carry different operator remediations, so
// each lands on its own counter:
//
//   - GapError: the relay skipped seqs — upstream data loss, nothing we
//     can do locally.
//   - UnknownFrameError: a well-formed frame this build can't represent —
//     a relay speaking a newer protocol; the fix is upgrading jetstream.
//   - StreamError: an op=-1 server error frame (e.g. FutureCursor,
//     ConsumerTooSlow), normally followed by a server-side close and an
//     atmos reconnect. A persistent FutureCursor loop means our cursor is
//     ahead of the relay (cursor corruption or a relay restored from an
//     older backup) and never self-resolves — the labeled counter is the
//     operator's signal to intervene.
//   - anything else: a garbage frame we chose to skip (decode error).
func (c *Consumer) noteStreamError(ctx context.Context, err error) {
	if gap, ok := errors.AsType[*streaming.GapError](err); ok {
		c.cfg.Metrics.noteSequenceGap(gap.Got - gap.Expected)
		c.logger.WarnContext(ctx, "upstream sequence gap",
			"expected", gap.Expected,
			"got", gap.Got,
			"missed", gap.Got-gap.Expected,
		)
		return
	}
	if uf, ok := errors.AsType[*streaming.UnknownFrameError](err); ok {
		c.cfg.Metrics.incUnknownEvents()
		c.logger.WarnContext(ctx, "unknown frame from relay",
			"t", uf.T,
			"op", uf.Op,
			"seq", uf.Seq,
		)
		return
	}
	if se, ok := errors.AsType[*streaming.StreamError](err); ok {
		c.cfg.Metrics.incStreamErrorFrames(se.Code)
		c.logger.WarnContext(ctx, "error frame from relay",
			"code", se.Code,
			"message", se.Message,
		)
		return
	}
	c.cfg.Metrics.incDecodeErrors()
	c.logger.WarnContext(ctx, "stream error", "err", err)
}

// processBatch writes one batch of decoded events into the writer.
func (c *Consumer) processBatch(ctx context.Context, batch []streaming.Event) error {
	return obs.Span(ctx, func(ctx context.Context) error {
		witnessedAt := c.cfg.now().UnixMicro()

		for _, evt := range batch {
			c.cfg.Metrics.incEventsReceived()

			segEvts, err := ConvertEvent(evt, witnessedAt)
			dme, isDropped := errors.AsType[*DroppedOpsError](err)
			ive, isInvalid := errors.AsType[*InvalidEventError](err)
			switch {
			case errors.Is(err, ErrUnknownEventKind):
				// Forward-compat hole: atmos decoded the frame but
				// yielded an event with no envelope we can archive.
				// (Wire-level unknown frame types never get this far —
				// atmos surfaces those as *UnknownFrameError on the
				// error slot, handled in noteStreamError; both paths
				// land on the same unknown_events_total counter.)
				// Count and log; the cursor we persist to relay/cursor
				// comes from atmos's watermark, which advances past
				// unknown kinds either way. A later build that learns
				// this kind will re-fetch from cursor and any events
				// at-or-after the last persisted watermark — under
				// Parallelism>1 the watermark trails the highest
				// yielded seq, so most near-real-time unknowns are
				// still recoverable on restart, but events that fall
				// behind the watermark before a restart are
				// unreachable. Acceptable trade for cross-DID
				// throughput; revisit if the unknown-event rate ever
				// becomes non-trivial.
				c.cfg.Metrics.incUnknownEvents()
				c.logger.WarnContext(ctx, "unknown event kind",
					"seq", evt.Seq,
				)
				continue
			case isInvalid:
				// Whole-event validation drop (e.g. non-TID rev): the
				// event is recognized but spec-invalid, so no later
				// build can archive it — count it and advance the
				// cursor past it. No log line: hostile upstream input
				// must not be able to drive our log volume; the
				// labeled counter is the operator signal.
				c.cfg.DropMetrics.IncDropped(ingest.DropSourceLive, ive.Reason)
				span := trace.SpanFromContext(ctx)
				if span.IsRecording() {
					span.AddEvent("dropped_invalid_event", trace.WithAttributes(
						attribute.String("reason", string(ive.Reason)),
						attribute.String("did", ive.DID),
						attribute.Int64("seq", evt.Seq),
					))
				}
				c.noteUpstreamSeq(evt.Seq)
				continue
			case isDropped:
				// Per-op drops: missing record blocks (partial-CAR
				// commit from a non-canonical PDS) or spec-invalid op
				// paths. Bump the labeled counters and fall through to
				// archive the surviving ops (segEvts is non-nil and
				// contains the well-formed events). A misbehaving
				// upstream must NOT take the firehose down — a single
				// partial-CAR commit took down a multi-hour bootstrap
				// backfill before this arm was added.
				missingBlocks := 0
				for reason, n := range dme.CountByReason() {
					c.cfg.DropMetrics.AddDropped(ingest.DropSourceLive, reason, n)
					if reason == ingest.DropReasonMissingBlock {
						missingBlocks = n
					}
				}
				// One log line per affected event — for missing-block
				// drops only — keeps volume bounded under a misbehaving
				// PDS spamming the firehose; per-op detail is on
				// dme.Dropped for a future flag that wants it.
				// Validation drops are counter-only by contract.
				if missingBlocks > 0 {
					c.logger.WarnContext(ctx, "dropped ops with missing record blocks",
						"seq", evt.Seq,
						"count", missingBlocks,
						"did", dme.Dropped[0].DID,
					)
				}
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

			if stale, err := c.dropStaleOrderedAsyncResync(ctx, evt, segEvts); err != nil {
				return err
			} else if stale {
				continue
			}

			if replayed, err := c.dropReplayedAccountEvent(ctx, segEvts); err != nil {
				return err
			} else if replayed {
				c.noteUpstreamSeq(evt.Seq)
				continue
			}

			for i := range segEvts {
				if err := segment.ValidateEvent(segEvts[i]); err != nil {
					if errors.Is(err, segment.ErrFieldTooLong) {
						// Spec-valid but unrepresentable (e.g. a legal
						// 256–512 byte rkey exceeds our 255-byte
						// column): distinct reason so operators can
						// tell "garbage" from "we chose not to
						// represent". The log stays — these are rare
						// and each one is a deliberate archival gap.
						c.cfg.DropMetrics.IncDropped(ingest.DropSourceLive, ingest.DropReasonFieldTooLong)
						c.logger.WarnContext(ctx, "dropped unarchivable upstream event",
							"seq", evt.Seq,
							"kind", segEvts[i].Kind,
							"did_len", len(segEvts[i].DID),
							"collection_len", len(segEvts[i].Collection),
							"rkey_len", len(segEvts[i].Rkey),
							"rev_len", len(segEvts[i].Rev),
							"payload_len", len(segEvts[i].Payload),
							"err", err,
						)
						continue
					}
					return fmt.Errorf("livestream: invalid segment event: %w", err)
				}
				// Append runs the tombstone Observe hook internally
				// (ingest.Config.OnAppend) before any flush/seal.
				if err := c.writer.Append(ctx, &segEvts[i]); err != nil {
					return fmt.Errorf("livestream: append: %w", err)
				}
				c.maybeTriggerCompaction()

				// Forward to downstream subscribers AFTER durable append.
				// segEvts[i].Seq has been populated by Append.
				if c.cfg.OnEvent != nil {
					c.cfg.OnEvent(&segEvts[i])
				}
			}

			c.promoteSyncState(segEvts)

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

func (c *Consumer) maybeTriggerCompaction() {
	if c.cfg.TombstoneCap <= 0 || c.cfg.CompactionTrigger == nil || c.cfg.Tombstones == nil {
		return
	}
	if c.cfg.Tombstones.Len() < c.cfg.TombstoneCap {
		return
	}
	select {
	case c.cfg.CompactionTrigger <- struct{}{}:
	default:
		if c.cfg.OnCompactionTriggerCoalesced != nil {
			c.cfg.OnCompactionTriggerCoalesced()
		}
	}
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
