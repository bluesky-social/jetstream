package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/jcalabro/atmos/xrpc"
)

// maxRebackfillStalls bounds consecutive re-backfill cycles that make no
// forward progress, the anti-ping-pong guard for the §14 too-old handoff
// (design §14). A re-backfill triggered by a too-old 400 normally advances the
// connect cursor (the consumer delivered live events before falling off, or the
// archive sealed new segments above the old tip); a cursor that fails to advance
// this many times in a row is a pathological loop (e.g. a misconfigured
// sub-archive lookback) and is surfaced as fatal rather than spun on forever.
const maxRebackfillStalls = 5

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
	// XRPC drives the short XRPC negotiation calls (planBackfill).
	// BulkXRPC drives the large getSegment/getBlock downloads;
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

	// rebackfillStalls counts consecutive §14 too-old re-backfill cycles whose
	// resume cursor failed to advance, bounded by maxRebackfillStalls. Touched
	// only on the single run goroutine in runBackfillThenLive.
	rebackfillStalls int

	// stats accumulates backfill-loop progress for the Stats() accessor. Written
	// on the single run goroutine (sweepSealedArchive / the re-backfill loop) and
	// read by Stats(); the mutex makes a concurrent read by a monitoring caller
	// race-free.
	statsMu sync.Mutex
	stats   Stats
}

// Stats is a point-in-time snapshot of backfill-loop progress, exposed via
// Engine.Stats for operational visibility (design §8/§10). The client library
// has no Prometheus registry, so this accessor is how a caller observes the
// residual gap a sustained-ingest backfill is closing.
type Stats struct {
	// Pages is the number of planBackfill pages downloaded across all sweeps
	// (including re-backfill cycles). Monotonically non-decreasing.
	Pages uint64
	// SealedTip is the most recently learned sealed-archive tip S (the pinned
	// pagination goal of the current sweep).
	SealedTip uint64
	// PlannedThrough is the continuation cursor the loop has reached: the highest
	// sealed seq accounted for so far. Equals SealedTip once a sweep completes.
	PlannedThrough uint64
	// ResidualGap is SealedTip - PlannedThrough: the sealed seqs still to be
	// downloaded before cutover. Zero once the sweep has consumed the archive.
	ResidualGap uint64
	// RebackfillCycles is the number of §14 too-old re-backfill cycles triggered
	// (a fell-behind/slow-handoff signal). Zero on the common path.
	RebackfillCycles uint64
}

// Stats returns a snapshot of backfill-loop progress. Safe to call from another
// goroutine while Run is in flight (e.g. a monitoring ticker).
func (e *Engine) Stats() Stats {
	e.statsMu.Lock()
	defer e.statsMu.Unlock()
	return e.stats
}

// recordPage updates the progress snapshot after a page is planned. tip is the
// pinned sealed tip; through is the continuation cursor reached.
func (e *Engine) recordPage(tip, through uint64) {
	e.statsMu.Lock()
	defer e.statsMu.Unlock()
	e.stats.Pages++
	e.stats.SealedTip = tip
	e.stats.PlannedThrough = through
	if tip > through {
		e.stats.ResidualGap = tip - through
	} else {
		e.stats.ResidualGap = 0
	}
}

// recordRebackfill bumps the re-backfill cycle counter (a §14 too-old handoff).
func (e *Engine) recordRebackfill() {
	e.statsMu.Lock()
	defer e.statsMu.Unlock()
	e.stats.RebackfillCycles++
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
	// Reset the row matcher to the configured request at the START of every run.
	// A §14 re-backfill mutates the matcher's seq floor (setAfterSeq below), and
	// the matcher is long-lived engine state (built once in NewEngine, reused for
	// the Client's lifetime). The public Client permits sequential — not
	// concurrent — Events() re-iterations, so without this reset a later run
	// would inherit the prior run's elevated floor and silently drop every row in
	// (cfg.Request.AfterSeq, priorResume]. Runs are non-concurrent (driveRun is
	// single-goroutine), so this races nothing. A nil matcher (match-all) stays
	// nil.
	if e.matcher != nil {
		e.matcher = NewMatcher(e.cfg.Request)
	}
	// rebackfillStalls counts CONSECUTIVE non-advancing §14 cycles within the
	// current run (the anti-ping-pong guard). It is long-lived engine state like
	// the matcher, so it must also reset per run: a prior run that stopped with a
	// partial stall count (e.g. cancelled after 3 non-advancing cycles) would
	// otherwise make the next run trip the fatal guard after fewer than
	// maxRebackfillStalls consecutive cycles of its own.
	e.rebackfillStalls = 0
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
	// Apply the caller's exact DID/collection filter here via wantsLive (shared
	// with the cutover tail): the server streams ALL collections to
	// /subscribe-v2 (the client does not forward wantedCollections as a hard
	// filter), so the engine must drop non-matching events itself. A nil/empty
	// matcher matches everything, so an unfiltered tail is unaffected.
	runErr := consumer.Run(liveCtx, func(ev *Event, err error) bool {
		if err != nil {
			return b.emitError(err)
		}
		if !e.wantsLive(ev) {
			return true
		}
		return b.add(*ev)
	})
	// A terminal Run error (today only errLiveCursorTooOld) returns WITHOUT having
	// routed through the batcher's error path (live.go returns it before the
	// emit-on-error report). On the pure-live path there is no archive to
	// re-enter, so the stale cursor is fatal: surface it rather than letting the
	// iterator end silently (CLAUDE.md: no silent fallbacks). A ctx cancellation
	// or an already-stopped consumer is a clean shutdown, not an error.
	if runErr != nil && liveCtx.Err() == nil && !b.stopped() {
		b.emitError(fatal(runErr))
	}
	stopFlusher()
	if !b.stopped() {
		b.flush()
	}
}

// wantsLive reports whether a live event passes the caller's exact
// DID/collection filter, applied client-side because /subscribe-v2 streams all
// collections (the client does not forward wantedCollections as a hard filter).
// There is no client-side tombstone suppression (design §5.1): every matching
// row is delivered and a folding consumer converges. Shared by the live-only
// path and the backfill cutover tail. A nil matcher matches everything.
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

// sweepSealedArchive pages planBackfill from startCursor, downloading and
// emitting every matching row in seq order until the whole sealed archive has
// been consumed (design §11/§12). It returns the pinned sealed tip S (the
// cutover cursor), whether the consumer asked to stop mid-sweep, and a terminal
// plan/context error.
//
// The mechanics that make this gap-free and progressing (design §12.1, verified
// by the manifest planner tests):
//
//   - beforeSeq is PINNED to S, the sealedTipSeq read on the FIRST page, so the
//     loop scans exactly (startCursor, S]. Segments sealed during the sweep carry
//     seqs > S and are deliberately left to the terminal /subscribe cold replay
//     (§14.1) rather than chased by a moving tip.
//   - The continuation cursor is plannedThroughSeq (exclusive lower bound for the
//     next page). A truncated page reports the MaxSeq of its last included work
//     unit (which strictly advances); a non-truncated page reports S. So
//     plannedThroughSeq >= S is the unambiguous done predicate even for a sparse
//     filter that matched zero segments in a sub-range (design §12.2).
//
// DID-level markers (#account/#identity/#sync) ride inline through every page
// whose plan touches their blocks, via the §R4-revised sentinel index — no
// snapshot, no client-side suppression. The folding consumer converges.
func (e *Engine) sweepSealedArchive(ctx context.Context, dl *Downloader, emit func(EntryResult) bool, backfillStopped func() bool, startCursor uint64) (sealedTip uint64, stopped bool, err error) {
	cursor := startCursor
	pinned := false
	for {
		req := e.cfg.Request
		req.AfterSeq = cursor
		if pinned {
			// Pin beforeSeq to the page-1 sealed tip for every subsequent page.
			req.HasBeforeSeq = true
			req.BeforeSeq = sealedTip
		}
		plan, perr := e.planner.Plan(ctx, req)
		if perr != nil {
			return sealedTip, false, perr
		}
		if !pinned {
			sealedTip = plan.SealedTipSeq
			pinned = true
		}
		if derr := dl.Download(ctx, plan.Entries, emit); derr != nil {
			return sealedTip, false, derr // ctx cancelled
		}
		// Record the page only AFTER it is downloaded + emitted: Stats.Pages is
		// documented as pages "downloaded" and ResidualGap as seqs "still to be
		// downloaded", so a monitoring goroutine must never observe a page count
		// or a shrunk ResidualGap for work not yet delivered (a plan-time record
		// would falsely report convergence on the final page before the sweep's
		// last download completes).
		e.recordPage(sealedTip, plan.PlannedThroughSeq)
		// backfillStopped() (not just b.stopped()): on the fast path the consumer
		// stop is observed via bf.Emit returning false, NOT through the batcher.
		if backfillStopped() {
			return sealedTip, true, nil
		}

		prevCursor := cursor
		cursor = plan.PlannedThroughSeq
		if cursor >= sealedTip {
			// Whole sealed archive (startCursor, S] consumed. An empty archive is
			// sealedTip==0 and terminates here on the first page (cursor 0 >= 0).
			return sealedTip, false, nil
		}
		// PlannedThroughSeq is an XRPC field from the server; planFromOutput only
		// rejects negatives and values above SealedTipSeq, not stalls. A stale,
		// buggy, or hostile server can return a continuation cursor at or below the
		// one we just sent while sealedTip stays higher, which would reissue an
		// identical request forever. Fail fatally instead of spinning — the §14
		// re-backfill loop guards the same way (maxRebackfillStalls).
		if cursor <= prevCursor {
			return sealedTip, false, fmt.Errorf("jetstream: planBackfill made no progress: afterSeq=%d plannedThroughSeq=%d sealedTipSeq=%d", prevCursor, cursor, sealedTip)
		}
	}
}

// runBackfillOnly executes a one-time dump of the sealed archive: page
// planBackfill until the sealed tip is reached, downloading + emitting the
// matched range, then return. No websocket is ever dialed.
//
// Records in the active (unsealed) segment, seq in (sealedTip, M], are only
// reachable via the live tail and are therefore NOT delivered by a dump. That is
// the defining trade-off of BackfillOnly: a clean point-in-time slice of the
// sealed archive, not the full up-to-the-instant stream.
func (e *Engine) runBackfillOnly(ctx context.Context, emitBatch func([]Event) bool, emitErr func(error) bool, bf BackfillSink) {
	b := newBatcher(e.cfg.BatchSize, emitBatch, emitErr)
	dl := NewDownloader(e.cfg.bulkClient(), e.cfg.Concurrency, e.matcher)
	dl.SetRecordMode(e.cfg.recordMode())
	emit, backfillStopped := e.backfillEmitFunc(b, bf, dl)

	_, _, err := e.sweepSealedArchive(ctx, dl, emit, backfillStopped, e.cfg.Request.AfterSeq)
	if err != nil && ctx.Err() == nil {
		// A plan failure mid-pagination is terminal: there is no archive transport
		// plan to run. A ctx-cancellation (ctx.Err() != nil) is a clean stop.
		b.emitError(fatal(err))
	}

	// Flush the partial tail. On the fast path b carries no backfill events (they
	// went straight through bf.Emit), so this is a no-op there.
	if !b.stopped() {
		b.flush()
	}
}

// runBackfillThenLive executes the paginated archive download and the bufferless
// cutover to live (design §11/§13/§14):
//
//  1. page planBackfill (pinning beforeSeq = S, the page-1 sealed tip) and
//     download + emit the whole sealed range (startCursor, S] in seq order;
//  2. connect /subscribe ONCE at cursor = S — no rewind margin, no client buffer.
//     The consumer dedups its own at-least-once overlap by seq; segments sealed
//     during the download are picked up by /subscribe's cold replay (§14.1).
//
// A pre-upgrade HTTP 400 "cursor too old" at connect (the slow-handoff or
// fell-off-live case, §14) is NOT fatal: the loop re-enters pagination from the
// last durably-processed seq (the live consumer's highest delivered seq, or S if
// it delivered nothing). Re-backfill cycles are bounded and must advance the
// cursor (anti-ping-pong); a fresh sweep re-learns the CURRENT sealed tip, which
// is >= the lookback floor, so the realistic case converges in one extra cycle.
//
// The archive download alone is the historical record; backfill emits every
// matching row with no tombstone suppression — a folding consumer converges
// (design §5.1, §R1). DID-level markers ride inline via the sentinel index.
func (e *Engine) runBackfillThenLive(ctx context.Context, emitBatch func([]Event) bool, emitErr func(error) bool, bf BackfillSink) {
	b := newBatcher(e.cfg.BatchSize, emitBatch, emitErr)
	dl := NewDownloader(e.cfg.bulkClient(), e.cfg.Concurrency, e.matcher)
	dl.SetRecordMode(e.cfg.recordMode())
	emit, backfillStopped := e.backfillEmitFunc(b, bf, dl)

	// loopCtx unwinds the whole engine when the consumer breaks the iterator. The
	// batcher's onStop cancels it, so even a quiet steady-state tail (no arriving
	// events) unwinds via the periodic flusher's stop-propagating yield rather
	// than blocking until the parent ctx is cancelled. onStop MUST be registered
	// before the flusher can drive a stopping emit (batcher contract).
	loopCtx, stopLoop := context.WithCancel(ctx)
	defer stopLoop()
	b.setOnStop(stopLoop)
	stopFlusher := e.startFlusher(loopCtx, b)
	defer stopFlusher()

	cursor := e.cfg.Request.AfterSeq
	for {
		sealedTip, stopped, serr := e.sweepSealedArchive(loopCtx, dl, emit, backfillStopped, cursor)
		if serr != nil {
			// A plan failure is terminal; a ctx cancellation is a clean stop.
			if loopCtx.Err() == nil {
				b.emitError(fatal(serr))
			}
			break
		}
		if stopped {
			break
		}

		// Cut over at the HIGHER of the freshly-learned sealed tip and the cursor
		// we already processed through. The cursor holds the last durably-delivered
		// seq (the prior cycle's resume); a live tail routinely delivers events from
		// the active, unsealed segment PAST the sealed tip, so on a §14 re-backfill
		// the re-learned sealedTip can be BELOW cursor (those live-delivered events
		// have not sealed into the archive yet). Cutting over at the lower sealedTip
		// would seed the live consumer's dedup floor below rows already delivered,
		// re-delivering (sealedTip, cursor] out of order. Worse, if that tail then
		// delivers nothing and returns too-old, resume = LastSeq() falls back to the
		// seeded floor (the low sealedTip), regressing both cursor and the matcher
		// floor below the prior resume — which silently re-enables the duplicate
		// delivery the matcher floor was guarding. max() keeps the cutover (and thus
		// the dedup floor and resume) monotonic non-decreasing, so the matcher-floor
		// invariant below (resume >= cutover >= prior floor) actually holds.
		cutover := max(sealedTip, cursor)
		resume, tooOld := e.tailLiveFromCutover(loopCtx, b, cutover)
		if !tooOld {
			// Clean stop: ctx cancelled or the consumer broke the iterator.
			break
		}

		// Flush the live tail's partially-filled batch BEFORE the next sweep. On
		// the fast path (bf.Transform set, which production uses) live cutover rows
		// go through the serial batcher b while re-backfill archive rows are
		// emitted directly via bf.Emit, bypassing b. The live rows that advanced
		// resume (seq in (cutover, resume]) may still sit in b.buf — add() only
		// auto-flushes at batch size. The next sweep emits archive rows with
		// seq > resume via bf.Emit; without this flush those newer rows reach the
		// consumer BEFORE the buffered older live rows (which would otherwise
		// flush only on the periodic flusher tick or the final flush), inverting
		// delivery order at the seam. A folding consumer still converges, but the
		// seam must stay in seq order. A flush stop means the consumer is done.
		if !b.flush() {
			break
		}

		// §14 too-old 400: re-backfill from the last durably-processed seq. Bound
		// the cycles and require the resume cursor to strictly advance past the
		// cursor this sweep started from — a non-advancing re-backfill is a
		// pathological loop, not a real fall-behind, and is surfaced as fatal.
		e.recordRebackfill()

		// Advance the row matcher's seq floor to the resume point BEFORE the next
		// sweep, so the matcher's exact filter lines up with where re-backfill
		// actually resumes.
		//
		// Scope of this fix (measured against the planner, not assumed): the
		// re-backfill plan request carries afterSeq=resume, and the server planner
		// already prunes whole segments/blocks with MaxSeq <= afterSeq
		// (manifest/plan.go segmentOverlapsSeq/blockOverlapsSeq). cursor advances
		// monotonically (resume = the live tail's highest delivered seq >= cutover
		// >= this sweep's sealed tip), so the archive at or below resume is NOT
		// re-planned or re-downloaded — there is no whole-history re-fetch to guard
		// against, and this saves zero network bytes. What it does fix is the ONE
		// work unit that STRADDLES resume: the planner's one-sided contract admits
		// the whole straddling segment/block (MaxSeq > resume but containing rows
		// <= resume), and the downloader runs the selector per row before decode
		// (downloader.go decodeFrame). Without this update the stale matcher
		// (afterSeq = the ORIGINAL request floor, e.g. 0) keeps those already-
		// delivered rows: they are re-decoded and re-emitted out of order, after
		// newer live seqs. A folding / seq-dedup consumer still converges
		// (design §13/§R7), so this is not a correctness fix; it is a bounded
		// cleanup that drops the straddling unit's redundant prefix before decode
		// and keeps the cutover seam in per-DID seq order.
		//
		// Safety: every row in (origAfter, resume] was already delivered (backfill
		// covered (origAfter, cutover]; the live tail covered (cutover, resume]),
		// and a genuinely-new event has seq > resume, so raising the floor to
		// resume can never drop an undelivered row. dl.selector IS e.matcher (same
		// pointer, NewDownloader above), and the live consumer that read e.matcher
		// has already returned, so this between-sweeps write races nothing.
		e.matcher.setAfterSeq(resume)

		if resume <= cursor {
			e.rebackfillStalls++
			if e.rebackfillStalls >= maxRebackfillStalls {
				b.emitError(fatal(fmt.Errorf(
					"jetstream: re-backfill made no progress after %d cursor-too-old cycles at seq %d",
					e.rebackfillStalls, resume)))
				break
			}
		} else {
			e.rebackfillStalls = 0
		}
		cursor = resume
	}

	stopFlusher()
	if !b.stopped() {
		b.flush()
	}
}

// tailLiveFromCutover connects /subscribe ONCE at cutover (the sealed tip) and
// tails live, forwarding each matching event to the batcher. It returns the
// highest seq durably processed and whether the connect/tail ended on a §14
// too-old 400 (signalling the caller to re-backfill). A nil/clean return
// (resume, false) means the stream ended (ctx cancel or consumer stop).
//
// The dedup floor is the cutover seq: the backfill already emitted through it,
// so the server's inclusive replay of cutover itself is deduped, and the first
// genuinely-new live event (seq > cutover) passes. No rewind margin is needed —
// the consumer's seq dedup makes the seam at-least-once with no gap (design §13).
func (e *Engine) tailLiveFromCutover(ctx context.Context, b *batcher, cutover uint64) (resume uint64, tooOld bool) {
	consumer := newLiveConsumer(liveConfig{
		host:        e.cfg.Host,
		cursor:      cutover,
		dedupFloor:  cutover,
		collections: e.cfg.Request.Collections,
		dids:        e.cfg.Request.DIDs,
		dial:        e.cfg.Dial,
		httpClient:  e.cfg.LiveHTTPClient,
		logger:      e.logger,
		backoffMin:  e.cfg.LiveBackoffMin,
		mode:        e.cfg.recordMode(),
	})
	err := consumer.Run(ctx, func(ev *Event, cerr error) bool {
		if cerr != nil {
			// A recoverable live read/reconnect error: surface it (do not swallow —
			// CLAUDE.md) but keep tailing. The consumer rejecting it stops batching.
			return b.emitError(cerr)
		}
		if !e.wantsLive(ev) {
			return true
		}
		return b.add(*ev)
	})
	return consumer.LastSeq(), errors.Is(err, errLiveCursorTooOld)
}
