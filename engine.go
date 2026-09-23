package jetstream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bluesky-social/gttp"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
)

// newEngine resolves the transport dependencies and builds the replay engine.
func newEngine(host string, cfg config) engine {
	negotiationXRPC := newXRPCClient(host, cfg, xrpc.ATProtoOpts(30*time.Second))
	bulkXRPC := newXRPCClient(host, cfg, xrpc.BulkDownloadOpts())
	if cfg.hasAPIKey {
		// AccessJwt is atmos's opaque bearer-header transport seam. API keys
		// are not parsed as JWTs and have no refresh/session semantics.
		negotiationXRPC.SetAuth(&xrpc.AuthInfo{AccessJwt: cfg.apiKey})
		bulkXRPC.SetAuth(&xrpc.AuthInfo{AccessJwt: cfg.apiKey})
	}
	// Public dictionary fetches share the negotiation transport and retry policy
	// without sharing its bearer-auth state.
	publicXRPC := &xrpc.Client{
		Host:       negotiationXRPC.Host,
		HTTPClient: negotiationXRPC.HTTPClient,
		UserAgent:  negotiationXRPC.UserAgent,
		Retry:      negotiationXRPC.Retry,
	}

	ec := engineConfig{
		Host: host,
		Request: planRequest{
			Kinds:        cfg.kinds,
			DIDs:         cfg.dids,
			Collections:  cfg.collections,
			AfterSeq:     cfg.afterSeq,
			HasBeforeSeq: cfg.hasBeforeSeq,
			BeforeSeq:    cfg.beforeSeq,
		},
		Backfill:       cfg.backfillRequested(),
		BackfillOnly:   cfg.snapshotOnly,
		LiveCursor:     cfg.liveCursor,
		BatchSize:      cfg.batchSize,
		Concurrency:    cfg.downloadConc,
		SegmentStripes: cfg.segmentStripes,
		XRPC:           negotiationXRPC,
		BulkXRPC:       bulkXRPC,
		PublicXRPC:     publicXRPC,
		// Route the live-tail websocket upgrade through the caller's HTTP
		// client too (WithHTTPClient), so an in-process transport reaches the
		// live cutover, not just the unary XRPC downloads.
		LiveHTTPClient:   cfg.httpClient,
		Logger:           cfg.logger,
		RawRecords:       cfg.rawRecords,
		RawRecordsCopied: cfg.rawRecordsCopied,
		RawRecordCIDs:    cfg.rawRecordCIDs,
		ZstdCompression:  cfg.zstdCompression,
	}
	return newReplayEngine(ec)
}

// newXRPCClient builds an xrpc.Client for host. When the caller supplied an
// HTTP client (WithHTTPClient) it overrides both workloads; otherwise each
// workload gets its own gttp client tuned by opts (short timeouts for XRPC
// negotiation, bulk-transfer tuning for downloads — design note §5.1).
func newXRPCClient(host string, cfg config, opts []gttp.Option) *xrpc.Client {
	c := &xrpc.Client{Host: host}
	// Retry policy is orthogonal to transport: apply the caller's attempt
	// cap whether or not they also supplied a custom HTTP client.
	if cfg.maxDownloadAttempts > 0 {
		c.Retry = gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(cfg.maxDownloadAttempts)})
	}
	if cfg.httpClient != nil {
		c.HTTPClient = gt.Some(cfg.httpClient)
		return c
	}
	c.HTTPClient = gt.Some(gttp.New(opts...))
	return c
}

// driveRun runs the engine with the caller's emit/error closures and backfill
// sink, wrapping it in the Close cancellation handshake: it derives a cancelable
// ctx, publishes the cancel so a concurrent Close can unwind the run, and clears
// it on return. It also rejects concurrent runs, which the public contract has
// always forbidden, rather than allowing them to race through shared matcher
// and progress state.
func (e *replayEngine) driveRun(ctx context.Context, emitBatch func([]Event) bool, emitErr func(error) bool, bf backfillSink) {
	runCtx, cancel := context.WithCancel(ctx)
	e.runMu.Lock()
	if e.closed {
		e.runMu.Unlock()
		cancel()
		return
	}
	if e.runCancel != nil {
		e.runMu.Unlock()
		cancel()
		emitErr(fatal(errors.New("jetstream: Events is already running on this Client")))
		return
	}
	e.runCancel = cancel
	e.runMu.Unlock()
	defer func() {
		e.runMu.Lock()
		e.runCancel = nil
		e.runMu.Unlock()
		cancel()
	}()
	e.runWithBackfill(runCtx, emitBatch, emitErr, bf)
}

func (e *replayEngine) run(ctx context.Context, yield func(*Batch, error) bool) {
	stopped := false

	// Build batches on parallel decode workers to keep the serial
	// reassembler cheap. Transform reads only its block and returns
	// immutable batches, or nil for an empty result.
	//
	// stopped is shared by backfill Emit and live emitBatch/emitErr. Live
	// callbacks can run on the periodic flusher, but the batcher's mutex
	// serializes them. Backfill bypasses the batcher, and live buffers
	// are flushed before each sweep, so live callbacks cannot overlap
	// backfill Emit. Transform never touches stopped. If these phases
	// overlap in future, stopped must become atomic.
	size := max(e.cfg.BatchSize, 1)
	bf := backfillSink{
		transform: func(_ int, evs []Event) any {
			if len(evs) == 0 {
				return nil // empty/filtered block: emit nothing
			}
			// Chunk the block's events into BatchSize public batches. Batches are
			// block-aligned: the final chunk of a block may be smaller than
			// BatchSize (see Batch / WithBatchSize docs). LastCursor stays correct
			// (max seq within each chunk).
			// Ceiling division written overflow-safe: len(evs) >= 1 here, and
			// (len(evs)+size-1) would wrap negative for a huge WithBatchSize,
			// panicking make (recovered into a dropped block).
			batches := make([]*Batch, 0, 1+(len(evs)-1)/size)
			for i := 0; i < len(evs); i += size {
				end := min(i+size, len(evs))
				// Three-index slice: batches share the block's backing array, and
				// Events() hands the slice to the consumer, so cap must not extend
				// into the next batch's events (an append would overwrite them).
				batches = append(batches, &Batch{events: evs[i:end:end]})
			}
			return batches
		},
		emit: func(res entryResult) bool {
			if stopped {
				return false
			}
			// res.Payload is always a []*Batch here: the engine routes error
			// results through emitErr before calling Emit, so a non-error block
			// always carries the Transform output (or nil for an empty block).
			batches, ok := res.Payload.([]*Batch)
			if !ok {
				stopped = true
				yield(nil, fatal(fmt.Errorf("jetstream: internal backfill payload has type %T, want []*Batch", res.Payload)))
				return false
			}
			for _, b := range batches {
				if !yield(b, nil) {
					stopped = true
					return false
				}
			}
			return true
		},
	}

	e.driveRun(ctx,
		func(batch []Event) bool {
			if stopped {
				return false
			}
			b := &Batch{events: batch}
			if !yield(b, nil) {
				stopped = true
				return false
			}
			return true
		},
		func(err error) bool {
			if stopped {
				return false
			}
			if !yield(nil, err) {
				stopped = true
				return false
			}
			return true
		},
		bf,
	)
}

func (e *replayEngine) close() error {
	// Cancel any in-flight run so a live tail actually stops (the documented
	// "natural way to stop a live tail"). We do NOT wait for the run to finish: a
	// consumer may call Close from inside its own Events loop, and blocking here
	// would deadlock. The bufferless cutover holds no resources to release.
	e.runMu.Lock()
	e.closed = true
	cancel := e.runCancel
	e.runMu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}
