package client

import (
	"context"
	"log/slog"
	"net/http"
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
	// Replay yields buffered frames after the given exclusive lower bound. None
	// replays from the beginning (including seq 0); Some(n) yields only Seq > n.
	Replay(ctx context.Context, after gt.Option[uint64]) func(yield func(LiveFrame, error) bool)
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
	// LiveHTTPClient, when non-nil, is the *http.Client the live-tail
	// websocket dial uses for its HTTP/1.1 upgrade. nil uses the default
	// dialer. Lets a caller route the live tail through a custom transport
	// (e.g. an in-process pipe) without exposing the websocket types. Ignored
	// when Dial is set.
	LiveHTTPClient *http.Client
	// LiveBackoffMin overrides the live-tail reconnect backoff floor. Zero uses
	// the package default; tests set a tiny value to avoid real-time waits.
	LiveBackoffMin time.Duration
	Logger         *slog.Logger
	// RawRecords, when true, makes commit decode SKIP building the generic
	// Record map[string]any (decodeRecordMap — the dominant decode allocation at
	// scale). Commit.Record is left nil and Commit.RecordCBOR is populated so a
	// caller can decode it into a typed struct itself. See the root WithRawRecords
	// option and TypedEvents.
	RawRecords bool
	// RawRecordsCopied, alongside RawRecords, clones RecordCBOR (backfill path)
	// instead of aliasing the internal buffer, so it is safe to retain past the
	// batch. See WithRawRecordsCopied.
	RawRecordsCopied bool
	// RawRecordCIDs, when true alongside RawRecords, still computes Commit.CID
	// (sha256+base32 of the payload) on the backfill path. Default false in raw
	// mode: CID is real per-record work the typed fast path avoids by default.
	RawRecordCIDs bool
}

// recordDecodeMode captures how commit records are materialized, derived from
// Config. It is passed down the decode paths (backfill + live) so the gating is
// one value rather than scattered bools.
type recordDecodeMode struct {
	raw      bool // skip decodeRecordMap; leave Record nil, set RecordCBOR
	copyCBOR bool // in raw mode, clone RecordCBOR instead of aliasing the buffer
	wantCIDs bool // in raw mode, still compute Commit.CID
}

func (c Config) recordMode() recordDecodeMode {
	return recordDecodeMode{raw: c.RawRecords, copyCBOR: c.RawRecordsCopied, wantCIDs: c.RawRecordCIDs}
}

// bulkClient returns the client for segment/block downloads, falling back to
// the negotiation client.
func (c Config) bulkClient() *xrpc.Client {
	if c.BulkXRPC != nil {
		return c.BulkXRPC
	}
	return c.XRPC
}

// Engine orchestrates the whole stream: backfill plan + download, the
// backfill-to-live cutover, and the steady-state live tail. It emits batches
// through Run.
type Engine struct {
	cfg     Config
	planner *Planner
	matcher *Matcher
	logger  *slog.Logger
}

// NewEngine builds an Engine from cfg.
func NewEngine(cfg Config) *Engine {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(discardWriter{}, nil))
	}
	return &Engine{
		cfg:     cfg,
		planner: NewPlanner(cfg.XRPC),
		matcher: NewMatcher(cfg.Request),
		logger:  logger,
	}
}

// BackfillSink is the optional fast path for the backfill download phase. When
// its Transform is non-nil, the downloader runs Transform on the parallel decode
// workers (turning each block's []Event into an opaque payload) and the engine
// delivers that payload via Emit — moving the per-event conversion + batching off
// the single serial reassembler goroutine, which is the backfill scaling ceiling
// (#142). When Transform is nil the engine uses the legacy per-event batcher path
// unchanged.
//
// Emit receives the whole EntryResult (not just the payload) so the engine can
// route an error result through the error path before ever asserting the payload
// type. The live-tail phase always uses the serial batcher regardless — only the
// high-volume backfill phase takes the fast path.
type BackfillSink struct {
	// Transform converts one decoded block's events into a ready-to-deliver
	// payload, on the decode workers. nil disables the fast path.
	Transform func(entryIdx int, evs []Event) any
	// Emit delivers one non-error block payload (EntryResult.Payload) in seq
	// order; returns false to stop. Only called for non-error results.
	Emit func(EntryResult) bool
}

// Run drives the stream until ctx is cancelled or the consumer stops. It emits
// batches via emitBatch (returns false to stop) and recoverable errors via
// emitErr (returns false to stop). Run blocks until the stream ends.
//
// Run uses the legacy per-event batcher for backfill. To take the parallel
// backfill fast path, use RunWithBackfill with a non-nil BackfillSink.
func (e *Engine) Run(ctx context.Context, emitBatch func([]Event) bool, emitErr func(error) bool) {
	e.RunWithBackfill(ctx, emitBatch, emitErr, BackfillSink{})
}

// RunWithBackfill is Run with an optional backfill fast path (see BackfillSink).
// A zero BackfillSink (nil Transform) is exactly equivalent to Run.
func (e *Engine) RunWithBackfill(ctx context.Context, emitBatch func([]Event) bool, emitErr func(error) bool, bf BackfillSink) {
	if e.cfg.Backfill {
		if e.cfg.BackfillOnly {
			e.runBackfillOnly(ctx, emitBatch, emitErr, bf)
			return
		}
		e.runBackfillThenLive(ctx, emitBatch, emitErr, bf)
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
	// tip" (the documented WithLiveCursor contract): 0 -> fromTip (omit the wire
	// cursor), a non-zero cursor -> resume from it.
	consumer := newLiveConsumer(liveConfig{
		host:    e.cfg.Host,
		cursor:  e.cfg.LiveCursor,
		fromTip: e.cfg.LiveCursor == 0,
		// Pure-live resume: a saved LiveCursor means the caller already holds
		// events through it, so it is also the dedup floor. From-tip (0) leaves
		// the floor 0 so the first event delivered passes.
		dedupFloor: e.cfg.LiveCursor,
		// Forward the filters so the server prunes server-side; the inline
		// wantsLive matcher above remains the correctness backstop.
		collections: e.cfg.Request.Collections,
		dids:        e.cfg.Request.DIDs,
		dial:        e.cfg.Dial,
		httpClient:  e.cfg.LiveHTTPClient,
		logger:      e.logger,
		backoffMin:  e.cfg.LiveBackoffMin,
		mode:        e.cfg.recordMode(),
	})
	// Route both events and errors through the batcher so the downstream yield
	// is serialized against the flusher goroutine, and an error the consumer
	// rejects stops batching (and fires onStop -> stopLive) instead of being
	// emitted concurrently and then ignored.
	//
	// Apply the caller's exact DID/collection filter here: the server streams
	// ALL collections to /subscribe-v2 (the client does not forward
	// wantedCollections on the wire), so the engine must drop non-matching
	// events itself. The backfill+cutover path filters via liveSink.wantLive;
	// the pure live-only path has no sink, so it filters inline. A nil/empty
	// matcher matches everything, so an unfiltered tail is unaffected.
	_ = consumer.Run(liveCtx, func(ev *Event, _ []byte, err error) bool {
		if err != nil {
			return b.emitError(err)
		}
		if !e.wantsLive(ev) {
			return true
		}
		return b.add(*ev)
	})
	stopFlusher()
	if !b.stopped() {
		b.flush()
	}
}

// wantsLive reports whether a live-only event passes the caller's exact
// DID/collection filter. The live-only path runs no backfill and seeds no
// suppressor (there are no historical rows to suppress against), so unlike the
// cutover's liveSink.wantLive it applies only the matcher. A nil matcher
// matches everything.
func (e *Engine) wantsLive(ev *Event) bool {
	if e.matcher == nil {
		return true
	}
	se := segmentViewOf(ev)
	return e.matcher.Wants(&se)
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

// runBackfillOnly executes a one-time dump of the sealed archive: plan, then
// download + emit the matched range and return. It is a strict subset of
// runBackfillThenLive with the live tail, cutover, and steady-state phases
// removed — no websocket is ever dialed.
//
// backfillEmitFunc builds the Download emit callback shared by both backfill
// paths, and installs the fast-path transform on dl when bf provides one.
//
// Error results ALWAYS route through the batcher's emitError (which flushes any
// buffered events first, preserving error-after-data ordering) BEFORE the
// fast-path Emit is consulted — so bf.Emit is only ever called for a non-error
// result, and the payload it receives is always a real transform output. On the
// legacy path (bf.Transform == nil) events flow through the per-event batcher
// exactly as before.
//
// The returned stopped() reports whether the consumer asked to stop during the
// backfill. On the legacy path that is just b.stopped(); on the fast path the
// batcher never sees the backfill events, so a stop is observed only via Emit
// returning false and recorded here (FIX: the live phase must check THIS, not
// only b.stopped()).
func (e *Engine) backfillEmitFunc(b *batcher, bf BackfillSink, dl *Downloader) (emit func(EntryResult) bool, stopped func() bool) {
	if bf.Transform == nil {
		// Legacy path: per-event batching on the serial reassembler goroutine.
		return func(res EntryResult) bool {
			if res.Err != nil {
				return b.emitError(res.Err)
			}
			for _, ev := range res.Events {
				if !b.add(ev) {
					return false
				}
			}
			return true
		}, b.stopped
	}

	// Fast path: workers run the transform in parallel; the reassembler hands us
	// the ready payload, which we deliver via bf.Emit with no per-event work.
	dl.SetTransform(bf.Transform)
	var consumerStopped bool
	return func(res EntryResult) bool {
			if res.Err != nil {
				// Route errors (incl. a transform panic surfaced as Err) through the
				// batcher so they stay ordered after any prior events and reuse the
				// fatal/recoverable plumbing. b holds no backfill events on this path,
				// so emitError just flushes-nothing then emits the error in order.
				if !b.emitError(res.Err) {
					consumerStopped = true
					return false
				}
				return true
			}
			if !bf.Emit(res) {
				consumerStopped = true
				return false
			}
			return true
		}, func() bool {
			return consumerStopped || b.stopped()
		}
}

// Records in the active (unsealed) segment, seq in (plannedThroughSeq, M], are
// only reachable via the live tail and are therefore NOT delivered by a dump.
// That is the defining trade-off of BackfillOnly: a clean point-in-time slice
// of the sealed archive, not the full up-to-the-instant stream.
func (e *Engine) runBackfillOnly(ctx context.Context, emitBatch func([]Event) bool, emitErr func(error) bool, bf BackfillSink) {
	// 1. Plan. Terminal on failure: there is no archive transport plan to run.
	plan, err := e.planner.Plan(ctx, e.cfg.Request)
	if err != nil {
		emitErr(fatal(err))
		return
	}

	// 2. Download + emit in plan order. Rows are filtered (exact DID/collection/
	// seq) before decode by the matcher. Backfill emits every matching row with no
	// tombstone suppression — a folding consumer converges (design §5.1).
	b := newBatcher(e.cfg.BatchSize, emitBatch, emitErr)
	dl := NewDownloader(e.cfg.bulkClient(), e.cfg.Concurrency, e.matcher)
	dl.SetRecordMode(e.cfg.recordMode())
	emit, _ := e.backfillEmitFunc(b, bf, dl)
	_ = dl.Download(ctx, plan.Entries, emit)

	// 3. Flush the partial tail. On the fast path b carries no backfill events
	// (they went straight through bf.Emit), so this is a no-op there; on the
	// legacy path it flushes the final partial batch. flushLocked is a no-op once
	// the batcher has stopped.
	if !b.stopped() {
		b.flush()
	}
}

// runBackfillThenLive executes the full archive negotiation and cutover:
//
//  1. plan the backfill (records plannedThroughSeq, the sealed tip);
//  2. start the live tail from plannedThroughSeq-margin into a buffering sink;
//  3. download + emit the backfill (filtered), seq <= sealed tip;
//  4. flip the sink: drain buffered live frames (seq > sealed tip), then
//     forward the live tail directly in steady state.
//
// The record-stream handoff is at plannedThroughSeq: records in the active
// (unsealed) segment above the sealed tip are not downloadable from the archive
// and are delivered by the live tail instead. Backfill emits every matching row
// with no tombstone suppression — a folding consumer converges (design §5.1).
func (e *Engine) runBackfillThenLive(ctx context.Context, emitBatch func([]Event) bool, emitErr func(error) bool, bf BackfillSink) {
	// 1. Plan. A plan failure is terminal: there is no archive transport plan to
	// execute.
	plan, err := e.planner.Plan(ctx, e.cfg.Request)
	if err != nil {
		emitErr(fatal(err))
		return
	}

	// 2. Start the live tail into a buffering sink before downloading, so no
	// live event between the plan and the cutover is lost.
	liveStart := plan.PlannedThroughSeq
	if liveStart > liveRewindMargin {
		liveStart -= liveRewindMargin
	} else {
		liveStart = 0
	}
	// The cutover always replays from liveStart, and the dedup floor matches it.
	// An empty archive is liveStart==0: the live tail owns the whole stream and
	// the floor of 0 ("nothing delivered yet") lets the first real event (seq 1)
	// through. A non-empty archive seeds the floor at liveStart so the
	// at-least-once rewind-margin overlap is deduped. Under 1-based seqs no
	// special "covered nothing" case is needed (design §R8).
	dedupFloor := liveStart
	sink := newLiveSink(e.cfg.Buffer, e.matcher, e.cfg.recordMode())
	liveCtx, stopLive := context.WithCancel(ctx)
	defer stopLive()
	consumer := newLiveConsumer(liveConfig{
		host: e.cfg.Host,
		// The cutover always replays from liveStart (cursor=0 replays from the
		// beginning for an empty/small archive), so the (plannedThroughSeq, tip]
		// band is never dropped.
		cursor:     liveStart,
		dedupFloor: dedupFloor,
		// Forward the filters so the server prunes the live tail server-side;
		// liveSink.wantLive (the matcher) remains the backstop.
		collections: e.cfg.Request.Collections,
		dids:        e.cfg.Request.DIDs,
		dial:        e.cfg.Dial,
		httpClient:  e.cfg.LiveHTTPClient,
		logger:      e.logger,
		backoffMin:  e.cfg.LiveBackoffMin,
		mode:        e.cfg.recordMode(),
	})
	var liveWG sync.WaitGroup
	liveWG.Go(func() {
		_ = consumer.Run(liveCtx, sink.onLive)
	})

	b := newBatcher(e.cfg.BatchSize, emitBatch, emitErr)

	// 3. Download + emit the backfill in plan order. Rows are filtered (exact
	// DID/collection/seq) before decode by the matcher. Errors flow through the
	// batcher (b.emitError) so they stay serialized with batch emission once the
	// flusher goroutine starts in phase 4. On the fast path (bf.Transform != nil)
	// the per-event conversion+batching runs on the decode workers and batches are
	// delivered via bf.Emit; b carries no backfill events. All backfill batches are
	// delivered synchronously inside Download — which returns only after the
	// reassembler drains — so the cutover in phase 4 still installs the live forward
	// path strictly AFTER the last backfill batch.
	dl := NewDownloader(e.cfg.bulkClient(), e.cfg.Concurrency, e.matcher)
	dl.SetRecordMode(e.cfg.recordMode())
	emit, backfillStopped := e.backfillEmitFunc(b, bf, dl)
	derr := dl.Download(ctx, plan.Entries, emit)
	if derr != nil { // ctx cancelled
		stopLive()
		liveWG.Wait()
		if !b.stopped() {
			b.flush()
		}
		return
	}
	// Use backfillStopped() (not just b.stopped()): on the fast path the consumer
	// stop is observed via bf.Emit returning false, NOT through the batcher, so a
	// bare b.stopped() would miss it and we would needlessly dial/forward the live
	// tail after the consumer already quit.
	if backfillStopped() {
		stopLive()
		liveWG.Wait()
		return
	}

	// 4. Flip the sink: from here, buffered then live frames flow through the
	// same batcher. Drain everything strictly above the sealed tip (the
	// backfill already emitted <= plannedThroughSeq); when the backfill covered
	// nothing (empty archive) drain from the beginning so the buffered seq 0 is
	// not skipped by the strict-> replay bound.
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
	// coveredThrough is the sealed tip the backfill emitted through (0 for an
	// empty archive, which drains the buffer from the beginning). Under 1-based
	// seqs no "covered nothing" special case is needed.
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

	// 5. Steady state: the live consumer now forwards directly through the
	// sink to the batcher. Block until the live tail ends (ctx cancel or the
	// consumer-driven stopLive above).
	liveWG.Wait()
	stopFlusher()
	if !b.stopped() {
		b.flush()
	}
}
