package jetstream

import (
	"context"
	"sync"
	"time"

	iclient "github.com/bluesky-social/jetstream/internal/client"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/jcalabro/jttp"
)

// newEngine builds the real orchestration engine in internal/client and adapts
// it to the root Client's engine interface.
func newEngine(host string, cfg config) (engine, error) {
	// A backfill-only dump never starts the live tail, so it needs no cutover
	// buffer. Skip allocating the default in-memory buffer in that case; a
	// caller-supplied buffer is still honored (and closed) if one was set.
	buf := cfg.liveBuffer
	if buf == nil && !cfg.backfillOnly {
		buf = NewMemLiveBuffer()
	}
	var bufAdapter iclient.Buffer
	if buf != nil {
		bufAdapter = bufferAdapter{buf}
	}

	ec := iclient.Config{
		Host: host,
		Request: iclient.PlanRequest{
			DIDs:         cfg.dids,
			Collections:  cfg.collections,
			AfterSeq:     cfg.afterSeq,
			HasBeforeSeq: cfg.hasBeforeSeq,
			BeforeSeq:    cfg.beforeSeq,
		},
		Backfill:         cfg.backfillRequested(),
		BackfillOnly:     cfg.backfillOnly,
		LiveCursor:       cfg.liveCursor,
		BatchSize:        cfg.batchSize,
		Concurrency:      cfg.downloadConc,
		Buffer:           bufAdapter,
		XRPC:             newXRPCClient(host, cfg, xrpc.ATProtoOpts(30*time.Second)),
		BulkXRPC:         newXRPCClient(host, cfg, xrpc.BulkDownloadOpts()),
		Logger:           cfg.logger,
		RawRecords:       cfg.rawRecords,
		RawRecordsCopied: cfg.rawRecordsCopied,
		RawRecordCIDs:    cfg.rawRecordCIDs,
	}
	// ownBuf gates whether close() calls buf.Close(); only true when we created
	// the buffer ourselves. When buf is nil (backfill-only, no caller buffer)
	// it must stay false so close() never dereferences a nil buffer.
	return &realEngine{eng: iclient.NewEngine(ec), buf: buf, ownBuf: buf != nil && cfg.liveBuffer == nil, batchSize: cfg.batchSize}, nil
}

// newXRPCClient builds an xrpc.Client for host. When the caller supplied an
// HTTP client (WithHTTPClient) it overrides both workloads; otherwise each
// workload gets its own jttp client tuned by opts (short timeouts for XRPC
// negotiation, bulk-transfer tuning for downloads — design note §5.1).
func newXRPCClient(host string, cfg config, opts []jttp.Option) *xrpc.Client {
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
	c.HTTPClient = gt.Some(jttp.New(opts...))
	return c
}

// realEngine adapts internal/client.Engine to the root engine interface,
// translating the engine's events into public jetstream.Event/Batch values.
type realEngine struct {
	eng    *iclient.Engine
	buf    LiveBuffer
	ownBuf bool // close the buffer on engine close only if we created it
	// batchSize is the consumer's BatchSize, used by the backfill fast path to
	// chunk a decoded block's events into public batches ON the decode workers
	// (see run). The live path uses the internal batcher's own size.
	batchSize int

	// mu guards runCancel and closed so a concurrent Close (the documented way
	// to stop a live tail) can cancel an in-flight run without a data race.
	mu        sync.Mutex
	runCancel context.CancelFunc // cancels the active run's ctx; nil when idle
	closed    bool               // Close was called; a later run starts cancelled
}

// driveRun runs the engine with the caller's emit/error closures and backfill
// sink, wrapping it in the Close cancellation handshake: it derives a cancelable
// ctx, publishes the cancel so a concurrent Close can unwind the run, and clears
// it on return. Both the default (*Batch) run and the generic typed run share
// this; the only difference between them is the closures + sink they build.
func (e *realEngine) driveRun(ctx context.Context, emitBatch func([]iclient.Event) bool, emitErr func(error) bool, bf iclient.BackfillSink) {
	runCtx, cancel := context.WithCancel(ctx)
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		cancel()
		return
	}
	e.runCancel = cancel
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		e.runCancel = nil
		e.mu.Unlock()
		cancel()
	}()
	e.eng.RunWithBackfill(runCtx, emitBatch, emitErr, bf)
}

func (e *realEngine) run(ctx context.Context, yield func(*Batch, error) bool) {
	stopped := false

	// Backfill fast path: convert + batch each decoded block ON the parallel
	// decode workers (Transform), then deliver the ready batches in seq order
	// (Emit). This moves the per-event internal→public conversion off the single
	// serial reassembler goroutine, which was the backfill scaling ceiling (#142).
	//
	// Transform runs on N worker goroutines concurrently. It is safe there: it
	// reads only its own block's events and calls toPublicEvents (pure per-event
	// field copies, no shared state). It returns []*Batch boxed as `any` (one box
	// per ~4096-event block, negligible) so internal/client never names the public
	// types. A nil return means an empty/filtered block (nothing to emit).
	//
	// Emit and the live emitBatch/emitErr closures below all run on a single
	// goroutine at any time and in disjoint phases (backfill fully precedes the
	// live cutover — Download returns before flipAndDrain), so the shared `stopped`
	// flag is never touched concurrently. Transform never touches it.
	size := e.batchSize
	if size < 1 {
		size = 1
	}
	bf := iclient.BackfillSink{
		Transform: func(_ int, evs []iclient.Event) any {
			if len(evs) == 0 {
				return nil // empty/filtered block: emit nothing
			}
			// Chunk the block's events into BatchSize public batches. Batches are
			// block-aligned: the final chunk of a block may be smaller than
			// BatchSize (see Batch / WithBatchSize docs). LastCursor stays correct
			// (max seq within each chunk).
			batches := make([]*Batch, 0, (len(evs)+size-1)/size)
			for i := 0; i < len(evs); i += size {
				end := min(i+size, len(evs))
				batches = append(batches, &Batch{events: toPublicEvents(evs[i:end])})
			}
			return batches
		},
		Emit: func(res iclient.EntryResult) bool {
			if stopped {
				return false
			}
			// res.Payload is always a []*Batch here: the engine routes error
			// results through emitErr before calling Emit, so a non-error block
			// always carries the Transform output (or nil for an empty block).
			batches, _ := res.Payload.([]*Batch)
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
		func(batch []iclient.Event) bool {
			if stopped {
				return false
			}
			b := &Batch{events: toPublicEvents(batch)}
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

func (e *realEngine) close() error {
	// Cancel any in-flight run first so a live tail actually stops (the
	// documented "natural way to stop a live tail"). We do NOT wait for the run
	// to finish: a consumer may call Close from inside its own Events loop, and
	// blocking here would deadlock. The buffer's own methods are close-safe
	// (they return errBufferClosed rather than panicking), so a run still
	// unwinding when we close the buffer below cannot corrupt or crash.
	e.mu.Lock()
	e.closed = true
	cancel := e.runCancel
	e.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if e.ownBuf {
		return e.buf.Close()
	}
	return nil
}

// bufferAdapter bridges the public LiveBuffer to the engine's Buffer interface
// (the two differ only in the LiveFrame type, kept distinct to avoid an import
// cycle between the root package and internal/client).
type bufferAdapter struct{ b LiveBuffer }

func (a bufferAdapter) Append(frames []iclient.LiveFrame) error {
	pub := make([]LiveFrame, len(frames))
	for i, f := range frames {
		pub[i] = LiveFrame{Seq: f.Seq, Data: f.Data}
	}
	return a.b.Append(pub)
}

func (a bufferAdapter) Replay(ctx context.Context, after gt.Option[uint64]) func(yield func(iclient.LiveFrame, error) bool) {
	return func(yield func(iclient.LiveFrame, error) bool) {
		for f, err := range a.b.Replay(ctx, after) {
			if !yield(iclient.LiveFrame{Seq: f.Seq, Data: f.Data}, err) {
				return
			}
		}
	}
}

func (a bufferAdapter) Truncate(throughSeq uint64) error { return a.b.Truncate(throughSeq) }
func (a bufferAdapter) Close() error                     { return a.b.Close() }

// toPublicEvents converts a block (or batch) of internal events to public
// events. Commit rows dominate, and each public *Commit was one heap allocation
// on top of the internal one; here the whole input's commits draw from a single
// []Commit slab and each public event references &slab[i], collapsing N
// allocations to one. The slab is sized to len(evs) and never grown, so the
// &slab[i] pointers stay valid; it is a fresh per-call allocation dropped to GC
// with the batch and never pooled, so a consumer holding an earlier batch is
// unaffected. Non-commit kinds (identity/account/sync) are comparatively rare
// and keep their individual allocations.
func toPublicEvents(evs []iclient.Event) []Event {
	if len(evs) == 0 {
		return nil
	}
	commits := make([]Commit, len(evs))
	out := make([]Event, len(evs))
	ci := 0
	for i := range evs {
		var commit *Commit
		if evs[i].Kind == iclient.KindCommit && evs[i].Commit != nil {
			commit = &commits[ci]
			ci++
		}
		out[i] = toPublicEventInto(evs[i], commit)
	}
	return out
}

// toPublicEventInto converts one internal event to a public event. For a commit
// row it fills the caller-provided *commit (a slab slot; see toPublicEvents)
// rather than allocating; commit must be non-nil exactly when ev is a commit
// with a non-nil internal Commit. Other kinds allocate their own shape.
func toPublicEventInto(ev iclient.Event, commit *Commit) Event {
	out := Event{
		DID:    ev.DID,
		Seq:    ev.Seq,
		TimeUS: ev.TimeUS,
		Kind:   Kind(ev.Kind),
	}
	switch ev.Kind {
	case iclient.KindCommit:
		if ev.Commit != nil {
			*commit = Commit{
				Operation:  Operation(ev.Commit.Operation),
				Collection: ev.Commit.Collection,
				Rkey:       ev.Commit.Rkey,
				Rev:        ev.Commit.Rev,
				CID:        ev.Commit.CID,
				Record:     ev.Commit.Record,
				RecordCBOR: ev.Commit.RecordCBOR,
			}
			out.Commit = commit
		}
	case iclient.KindIdentity:
		if ev.Identity != nil {
			out.Identity = &Identity{DID: ev.Identity.DID, Handle: ev.Identity.Handle, Seq: ev.Identity.Seq, Time: ev.Identity.Time}
		}
	case iclient.KindAccount:
		if ev.Account != nil {
			out.Account = &Account{DID: ev.Account.DID, Active: ev.Account.Active, Status: ev.Account.Status, Seq: ev.Account.Seq, Time: ev.Account.Time}
		}
	case iclient.KindSync:
		if ev.Sync != nil {
			out.Sync = &Sync{DID: ev.Sync.DID, Rev: ev.Sync.Rev, Seq: ev.Sync.Seq, Time: ev.Sync.Time}
		}
	}
	return out
}
