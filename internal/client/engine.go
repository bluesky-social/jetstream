package client

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
)

// LiveFrame is one buffered live event: its seq and the raw JSON frame bytes.
// Mirrors the root jetstream.LiveFrame; the root adapts its public LiveBuffer
// to the engine's Buffer interface to avoid an import cycle.
type LiveFrame struct {
	Seq  uint64
	Data []byte
}

// Buffer is the engine's view of the cutover live buffer. The root package
// supplies an adapter over the user-facing jetstream.LiveBuffer.
type Buffer interface {
	Append(frames []LiveFrame) error
	Replay(ctx context.Context, from uint64) func(yield func(LiveFrame, error) bool)
	Truncate(throughSeq uint64) error
	Close() error
}

// liveRewindMargin is how far below plannedThroughSeq the live tail starts, so
// the record-stream handoff leans on at-least-once across the overlap rather
// than trusting an exact boundary. Duplicates are deduped by seq. The cost is a
// few extra live frames re-tailed, never a gap.
const liveRewindMargin = 256

// defaultMaxBatchDelay bounds how long a partially-filled batch waits before
// being flushed, so a low-volume live tail still delivers promptly rather than
// holding events until BatchSize accumulates. Backfill fills batches by count
// almost immediately, so this only governs the steady-state tail.
const defaultMaxBatchDelay = 20 * time.Millisecond

// Config is the resolved engine configuration the root package passes in.
type Config struct {
	Host     string // normalized base URL
	Request  PlanRequest
	Backfill bool // run the historical backfill path before live
	// BackfillOnly downloads and emits the sealed archive, then returns without
	// starting the live tail or cutover. A one-time dump of the matched range.
	// Only meaningful when Backfill is true.
	BackfillOnly bool
	LiveCursor   uint64 // pure-live resume cursor when !Backfill
	BatchSize    int
	// MaxBatchDelay bounds how long a partial batch waits before flushing in
	// the steady-state live tail. Zero uses defaultMaxBatchDelay.
	MaxBatchDelay time.Duration
	Concurrency   int
	Buffer        Buffer
	// XRPC drives the short XRPC negotiation calls (getTombstones,
	// planBackfill). BulkXRPC drives the large getSegment/getBlock downloads;
	// it gets bulk-transfer HTTP tuning (no short wall-clock timeout). When
	// BulkXRPC is nil the engine reuses XRPC. See design note §5.1.
	XRPC     *xrpc.Client
	BulkXRPC *xrpc.Client
	Dial     dialFunc // optional; nil uses the production websocket dialer
	// LiveBackoffMin overrides the live-tail reconnect backoff floor. Zero uses
	// the package default; tests set a tiny value to avoid real-time waits.
	LiveBackoffMin time.Duration
	Logger         *slog.Logger
}

// bulkClient returns the client for segment/block downloads, falling back to
// the negotiation client.
func (c Config) bulkClient() *xrpc.Client {
	if c.BulkXRPC != nil {
		return c.BulkXRPC
	}
	return c.XRPC
}

// Engine orchestrates the whole stream: overlay seed, backfill plan +
// download, the backfill-to-live cutover, and the steady-state live tail. It
// emits batches through Run.
type Engine struct {
	cfg        Config
	planner    *Planner
	matcher    *Matcher
	suppressor *Suppressor
	logger     *slog.Logger
}

// NewEngine builds an Engine from cfg.
func NewEngine(cfg Config) *Engine {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(discardWriter{}, nil))
	}
	return &Engine{
		cfg:        cfg,
		planner:    NewPlanner(cfg.XRPC),
		matcher:    NewMatcher(cfg.Request),
		suppressor: NewSuppressor(),
		logger:     logger,
	}
}

// Run drives the stream until ctx is cancelled or the consumer stops. It emits
// batches via emitBatch (returns false to stop) and recoverable errors via
// emitErr (returns false to stop). Run blocks until the stream ends.
func (e *Engine) Run(ctx context.Context, emitBatch func([]Event) bool, emitErr func(error) bool) {
	if e.cfg.Backfill {
		if e.cfg.BackfillOnly {
			e.runBackfillOnly(ctx, emitBatch, emitErr)
			return
		}
		e.runBackfillThenLive(ctx, emitBatch, emitErr)
		return
	}
	e.runLiveOnly(ctx, emitBatch, emitErr)
}

// runLiveOnly is the pure live-tail path (no backfill options): tail from the
// caller's saved cursor (or the current tip) with no archive negotiation.
func (e *Engine) runLiveOnly(ctx context.Context, emitBatch func([]Event) bool, emitErr func(error) bool) {
	b := newBatcher(e.cfg.BatchSize, emitBatch, emitErr)
	liveCtx, stopLive := context.WithCancel(ctx)
	defer stopLive()
	// Register onStop BEFORE starting the flusher: the flusher's first yield can
	// observe a consumer stop, and if it does so before onStop is set, the
	// batcher latches onced=true with a nil onStop and stopLive would never fire
	// (a quiet tail would then block until ctx cancel).
	b.setOnStop(stopLive)
	// Cancel the live consumer when emission stops, so a quiet tail unwinds via
	// the flusher's stop-propagating yield rather than blocking until ctx
	// cancel (see runBackfillThenLive for the same rationale).
	stopFlusher := e.startFlusher(liveCtx, b)
	defer stopFlusher()
	// LiveCursor is a pure-live resume point with 0 meaning "from the current
	// tip" (the documented WithLiveCursor contract): map 0 -> None so the
	// consumer omits the cursor, and a non-zero cursor -> Some(seq) to resume.
	var liveCursor gt.Option[uint64]
	if e.cfg.LiveCursor > 0 {
		liveCursor = gt.Some(e.cfg.LiveCursor)
	}
	consumer := newLiveConsumer(liveConfig{
		host:       e.cfg.Host,
		cursor:     liveCursor,
		dial:       e.cfg.Dial,
		logger:     e.logger,
		backoffMin: e.cfg.LiveBackoffMin,
	})
	// Route both events and errors through the batcher so the downstream yield
	// is serialized against the flusher goroutine, and an error the consumer
	// rejects stops batching (and fires onStop -> stopLive) instead of being
	// emitted concurrently and then ignored.
	_ = consumer.Run(liveCtx, func(ev *Event, _ []byte, err error) bool {
		if err != nil {
			return b.emitError(err)
		}
		return b.add(*ev)
	})
	stopFlusher()
	if !b.stopped() {
		b.flush()
	}
}

// startFlusher runs a background ticker that flushes the batcher's partial tail
// at most every MaxBatchDelay, so a low-volume live tail delivers promptly. It
// returns a stop function (idempotent) that halts the ticker and waits for it
// to exit.
func (e *Engine) startFlusher(ctx context.Context, b *batcher) func() {
	delay := e.cfg.MaxBatchDelay
	if delay <= 0 {
		delay = defaultMaxBatchDelay
	}
	done := make(chan struct{})
	var once sync.Once
	stop := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(delay)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-t.C:
				if !b.flush() {
					return // consumer stopped
				}
			}
		}
	}()
	return func() {
		once.Do(func() { close(stop) })
		<-done
	}
}

// runBackfillOnly executes a one-time dump of the sealed archive: seed the
// suppressor from the overlay, plan, then download + emit the matched range and
// return. It is a strict subset of runBackfillThenLive with the live tail,
// cutover, and steady-state phases removed — no websocket is ever dialed.
//
// Records in the active (unsealed) segment, seq in (plannedThroughSeq, M], are
// only reachable via the live tail and are therefore NOT delivered by a dump.
// That is the defining trade-off of BackfillOnly: a clean point-in-time slice
// of the sealed archive, not the full up-to-the-instant stream.
func (e *Engine) runBackfillOnly(ctx context.Context, emitBatch func([]Event) bool, emitErr func(error) bool) {
	// 1. Overlay seed. Terminal on failure: without the tombstone base the
	// suppressor cannot honor the deletion guarantee, so abort rather than emit
	// unsuppressed historical rows.
	if _, _, err := e.suppressor.SeedFromOverlay(ctx, e.cfg.XRPC); err != nil {
		emitErr(fatal(err))
		return
	}

	// 2. Plan. Terminal on failure: there is no archive transport plan to run.
	plan, err := e.planner.Plan(ctx, e.cfg.Request)
	if err != nil {
		emitErr(fatal(err))
		return
	}

	// 3. Download + emit in plan order. Rows are filtered + suppressed before
	// decode by the downloader's selector. No live buffer is consulted, so the
	// suppressor only carries the overlay-seeded tombstones (no live deletes can
	// arrive during a dump).
	b := newBatcher(e.cfg.BatchSize, emitBatch, emitErr)
	dl := NewDownloader(e.cfg.bulkClient(), e.cfg.Concurrency, newRowSelector(e.matcher, e.suppressor))
	_ = dl.Download(ctx, plan.Entries, func(res EntryResult) bool {
		if res.Err != nil {
			return b.emitError(res.Err)
		}
		for _, ev := range res.Events {
			if !b.add(ev) {
				return false
			}
		}
		return true
	})

	// 4. Flush the partial tail. Download returns on ctx cancel or consumer stop;
	// in both cases a final flush of any buffered events is correct (flushLocked
	// is a no-op once the batcher has stopped).
	if !b.stopped() {
		b.flush()
	}
}

// runBackfillThenLive executes the full archive negotiation and cutover:
//
//  1. seed the suppressor from the overlay (records M, the tombstone horizon);
//  2. plan the backfill (records plannedThroughSeq, the sealed tip);
//  3. start the live tail from plannedThroughSeq-margin into a buffering sink,
//     folding live tombstones into the suppressor as they arrive;
//  4. download + emit the backfill (filtered + suppressed), seq <= sealed tip;
//  5. flip the sink: drain buffered live frames (seq > sealed tip), then
//     forward the live tail directly in steady state.
//
// The record-stream handoff is at plannedThroughSeq, NOT at M: records in the
// active (unsealed) segment, seq in (plannedThroughSeq, M], are not
// downloadable from the archive and are delivered by the live tail instead.
func (e *Engine) runBackfillThenLive(ctx context.Context, emitBatch func([]Event) bool, emitErr func(error) bool) {
	// 1. Overlay seed. A seed failure is terminal: without the tombstone base
	// the suppressor cannot honor the deletion guarantee, so abort fatally
	// rather than emit unsuppressed historical rows.
	if _, _, err := e.suppressor.SeedFromOverlay(ctx, e.cfg.XRPC); err != nil {
		emitErr(fatal(err))
		return
	}

	// 2. Plan. A plan failure is terminal: there is no archive transport plan to
	// execute.
	plan, err := e.planner.Plan(ctx, e.cfg.Request)
	if err != nil {
		emitErr(fatal(err))
		return
	}

	// 3. Start the live tail into a buffering sink before downloading, so no
	// live event between the plan and the cutover is lost.
	liveStart := plan.PlannedThroughSeq
	if liveStart > liveRewindMargin {
		liveStart -= liveRewindMargin
	} else {
		liveStart = 0
	}
	sink := newLiveSink(e.cfg.Buffer, e.suppressor, e.matcher)
	liveCtx, stopLive := context.WithCancel(ctx)
	defer stopLive()
	consumer := newLiveConsumer(liveConfig{
		host: e.cfg.Host,
		// The cutover always means "replay from liveStart", so it is always a
		// present cursor — including Some(0) when the sealed tip is below the
		// rewind margin. Some(0) sends cursor=0 (replay from the start) rather
		// than the None "live from tip" sentinel, so the (plannedThroughSeq, tip]
		// band is not dropped. See #112.
		cursor:     gt.Some(liveStart),
		dial:       e.cfg.Dial,
		logger:     e.logger,
		backoffMin: e.cfg.LiveBackoffMin,
	})
	var liveWG sync.WaitGroup
	liveWG.Go(func() {
		_ = consumer.Run(liveCtx, sink.onLive)
	})

	b := newBatcher(e.cfg.BatchSize, emitBatch, emitErr)

	// 4. Download + emit the backfill in plan order. Rows are filtered +
	// suppressed before decode by the downloader's selector. Errors flow
	// through the batcher (b.emitError) so they stay serialized with batch
	// emission once the flusher goroutine starts in phase 5.
	dl := NewDownloader(e.cfg.bulkClient(), e.cfg.Concurrency, newRowSelector(e.matcher, e.suppressor))
	derr := dl.Download(ctx, plan.Entries, func(res EntryResult) bool {
		if res.Err != nil {
			return b.emitError(res.Err)
		}
		for _, ev := range res.Events {
			if !b.add(ev) {
				return false
			}
		}
		return true
	})
	if derr != nil { // ctx cancelled
		stopLive()
		liveWG.Wait()
		if !b.stopped() {
			b.flush()
		}
		return
	}
	if b.stopped() {
		stopLive()
		liveWG.Wait()
		return
	}

	// 5. Flip the sink: from here, buffered then live frames flow through the
	// same batcher. Drain everything strictly above the sealed tip (the
	// backfill already emitted <= plannedThroughSeq).
	//
	// When emission stops (the consumer broke the iterator), cancel the live
	// context so the live consumer exits and liveWG.Wait below returns. The
	// stop is observed two ways: an arriving live event whose b.add returns
	// false, and — crucially for a quiet tail — the periodic flusher's yield
	// returning false, which fires the batcher's onStop. Without the latter, a
	// steady-state stream with no new events never unwinds until ctx cancel.
	//
	// onStop MUST be registered before the flusher starts: otherwise the
	// flusher's first yield could latch the batcher's once-guard with a nil
	// onStop, and stopLive would never fire.
	b.setOnStop(stopLive)
	// Start the max-latency flusher so steady-state low-volume tail batches
	// deliver promptly.
	stopFlusher := e.startFlusher(ctx, b)
	defer stopFlusher()
	emitLive := func(ev Event) bool { return b.add(ev) }
	if err := sink.flipAndDrain(ctx, plan.PlannedThroughSeq, emitLive, b.emitError); err != nil {
		// A cutover replay/append failure breaks the at-least-once handoff
		// guarantee: surface it as fatal so the consumer aborts rather than
		// continues against a truncated stream.
		b.emitError(fatal(err))
		// flipAndDrain failed before installing the forward path, so the
		// batcher's onStop can never fire and a quiet live tail would never
		// observe a stop. Cancel the live consumer explicitly so liveWG.Wait
		// below returns instead of blocking until parent-ctx cancel (mirrors the
		// ctx-cancel and b.stopped early-return branches above).
		stopLive()
		liveWG.Wait()
		return
	}

	// 6. Steady state: the live consumer now forwards directly through the
	// sink to the batcher. Block until the live tail ends (ctx cancel or the
	// consumer-driven stopLive above).
	liveWG.Wait()
	stopFlusher()
	if !b.stopped() {
		b.flush()
	}
}
