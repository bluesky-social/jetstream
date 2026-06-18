package client

import (
	"context"
	"log/slog"
	"sync"

	"github.com/jcalabro/atmos/xrpc"
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

// Config is the resolved engine configuration the root package passes in.
type Config struct {
	Host          string // normalized base URL
	Request       PlanRequest
	Backfill      bool // run the historical backfill path before live
	HasLiveCursor bool
	LiveCursor    uint64 // pure-live resume cursor when !Backfill
	BatchSize     int
	Concurrency   int
	Buffer        Buffer
	XRPC          *xrpc.Client
	Dial          dialFunc // optional; nil uses the production websocket dialer
	Logger        *slog.Logger
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
		e.runBackfillThenLive(ctx, emitBatch, emitErr)
		return
	}
	e.runLiveOnly(ctx, emitBatch, emitErr)
}

// runLiveOnly is the pure live-tail path (no backfill options): tail from the
// caller's saved cursor (or the current tip) with no archive negotiation.
func (e *Engine) runLiveOnly(ctx context.Context, emitBatch func([]Event) bool, emitErr func(error) bool) {
	b := newBatcher(e.cfg.BatchSize, emitBatch)
	consumer := newLiveConsumer(liveConfig{
		host:   e.cfg.Host,
		cursor: e.cfg.LiveCursor,
		dial:   e.cfg.Dial,
		logger: e.logger,
	})
	_ = consumer.Run(ctx, func(ev *Event, _ []byte, err error) bool {
		if err != nil {
			return emitErr(err)
		}
		return b.add(*ev)
	})
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
	// 1. Overlay seed.
	if _, _, err := e.suppressor.SeedFromOverlay(ctx, e.cfg.XRPC); err != nil {
		emitErr(err)
		return
	}

	// 2. Plan.
	plan, err := e.planner.Plan(ctx, e.cfg.Request)
	if err != nil {
		emitErr(err)
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
		host:   e.cfg.Host,
		cursor: liveStart,
		dial:   e.cfg.Dial,
		logger: e.logger,
	})
	var liveWG sync.WaitGroup
	liveWG.Go(func() {
		_ = consumer.Run(liveCtx, sink.onLive)
	})

	b := newBatcher(e.cfg.BatchSize, emitBatch)

	// 4. Download + emit the backfill in plan order. Rows are filtered +
	// suppressed before decode by the downloader's selector.
	dl := NewDownloader(e.cfg.XRPC, e.cfg.Concurrency, newRowSelector(e.matcher, e.suppressor))
	derr := dl.Download(ctx, plan.Entries, func(res EntryResult) bool {
		if res.Err != nil {
			return emitErr(res.Err)
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
	emitLive := func(ev Event) bool { return b.add(ev) }
	if err := sink.flipAndDrain(ctx, plan.PlannedThroughSeq, emitLive, emitErr); err != nil {
		emitErr(err)
	}

	// 6. Steady state: the live consumer now forwards directly through the
	// sink to the batcher. Block until the live tail ends (ctx cancel).
	liveWG.Wait()
	if !b.stopped() {
		b.flush()
	}
}
