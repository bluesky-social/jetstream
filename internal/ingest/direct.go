package ingest

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/internal/seqspace"
	"github.com/bluesky-social/jetstream/segment"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// SegmentSealer tracks a namespace's active segment for direct mode and
// seals it (design §10.8). maintainer.Segment implements it; the direct
// writer's committer goroutine is its only caller, so seals serialize with
// block commits.
type SegmentSealer interface {
	// Committed records a block the session committed, from ref's upload
	// of frame. A commit that landed anywhere but where the sealer expects
	// is corruption.
	Committed(c catalog.BlockCommit, ref catalog.ObjectRef, frame []byte) error
	// RotateIfFull seals once the active segment reaches the rotation
	// threshold.
	RotateIfFull(ctx context.Context) error
	// Seal seals the active segment if it holds any block.
	Seal(ctx context.Context) error
}

// DirectConfig configures direct mode (design §10.6). Session, Uploader, and
// Sealer are required; zero values take the defaults.
type DirectConfig struct {
	// Session is the leader session every commit runs in.
	Session *catalog.Session
	// Uploader stores block frames.
	Uploader ObjectUploader
	// Sealer owns the namespace's active segment. It must have been opened
	// in the same session, for the writer's namespace.
	Sealer SegmentSealer
	// UploadConcurrency bounds the writer's block uploads in flight.
	UploadConcurrency int
	// MaxPendingBlocks bounds the blocks frozen and not yet committed. An
	// append waits for room before it appends anything; one AppendBatch
	// may still freeze several blocks past it. Zero means twice
	// UploadConcurrency.
	MaxPendingBlocks int
	// Crash is the test-only crash seam injector. Nil in production.
	Crash crashpoint.Injector
	// OnFailure is called once with the error that ended the writer. The
	// session has already ended. It may run in the committer goroutine, so
	// it must not call Writer methods itself; Close waits for that
	// goroutine.
	OnFailure func(error)
}

func (c *Config) validateDirect() error {
	d := c.Direct
	ns := cmp.Or(c.Namespace, catalog.Main)
	switch {
	case c.Hot != nil:
		return fmt.Errorf("%w: Hot and Direct are exclusive", ErrInvalidConfig)
	case d.Session == nil || d.Uploader == nil || d.Sealer == nil:
		return fmt.Errorf("%w: Direct.Session, Direct.Uploader, and Direct.Sealer are required", ErrInvalidConfig)
	case c.Logger == nil:
		return fmt.Errorf("%w: Logger is required", ErrInvalidConfig)
	case !ns.Valid():
		return fmt.Errorf("%w: unknown namespace %q", ErrInvalidConfig, ns)
	case c.SeqKey != "" && c.SeqKey != catalog.SeqKey(ns):
		return fmt.Errorf("%w: direct mode in %q writes only %q", ErrInvalidConfig, ns, catalog.SeqKey(ns))
	case c.ReserveClientVisibleSeqs || c.UnreservedSeqsUnobservable:
		// A direct seq is never visible before it commits (design §10.1).
		return fmt.Errorf("%w: direct mode has no sequence lease", ErrInvalidConfig)
	case c.AsyncFlushWorkers != 0 || c.Catalog != nil:
		return fmt.Errorf("%w: direct mode takes neither AsyncFlushWorkers nor Catalog", ErrInvalidConfig)
	case c.MaxEventsPerBlock < 0 || c.ReadLogRetentionBytes < 0:
		return fmt.Errorf("%w: MaxEventsPerBlock and ReadLogRetentionBytes must be >= 0", ErrInvalidConfig)
	case d.UploadConcurrency < 0 || d.MaxPendingBlocks < 0:
		return fmt.Errorf("%w: Direct limits must be >= 0", ErrInvalidConfig)
	}
	if _, err := segment.NewBlockBuilder(c.MaxEventsPerBlock); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	return nil
}

func (d *DirectConfig) applyDefaults() {
	d.UploadConcurrency = cmp.Or(d.UploadConcurrency, DefaultUploadConcurrency)
	d.MaxPendingBlocks = cmp.Or(d.MaxPendingBlocks, 2*d.UploadConcurrency)
}

// directWriter is the Writer in direct mode (design §10.6): local mode's
// block flush pointed at the object store and the catalog. Appends assign
// seqs and fill one open block. A full block, or one a Flush, DrainDurability,
// ForceRotate, or Close cuts, is frozen: a goroutine encodes and uploads it,
// several at a time. One committer goroutine commits frozen blocks strictly
// in seq order, one CommitBlock each carrying the DurableBatchHook's output,
// then releases what waits on the block and applies the rotation rule.
type directWriter struct {
	cfg     *Config
	direct  *DirectConfig
	readLog *ReadableLog
	logger  *slog.Logger

	// ctx is canceled when the writer fails or finishes closing, which
	// abandons in-flight uploads.
	ctx    context.Context
	cancel context.CancelFunc

	uploads  chan struct{} // upload concurrency semaphore
	encoders sync.WaitGroup
	wake     chan struct{} // committer: queue grew
	done     chan struct{} // committer exited

	mu      sync.Mutex
	nextSeq uint64
	block   *directBlock
	queue   []directItem
	pending int // blocks frozen and not yet committed
	waiters int
	changed chan struct{} // closed and replaced when pending drops
	closed  bool
	err     error // sticky failure
}

type directBlock struct {
	first  uint64
	events []segment.Event

	// Set at freeze.
	prepareValue any
	ready        chan struct{}

	// Set by prepare before ready closes.
	frame []byte
	info  segment.BlockInfo
	ref   catalog.ObjectRef
	err   error
}

func (b *directBlock) last() uint64 { return b.first + uint64(len(b.events)) - 1 }

// directItem is one committer queue entry. Exactly one field is set.
type directItem struct {
	block   *directBlock
	meta    *hotMeta
	seal    bool
	barrier chan error
	stop    bool
}

func openDirect(cfg Config) (*Writer, error) {
	cfg.Namespace = cmp.Or(cfg.Namespace, catalog.Main)
	cfg.SeqKey = catalog.SeqKey(cfg.Namespace)
	cfg.applyDefaults()
	dc := *cfg.Direct
	dc.applyDefaults()
	cfg.Direct = &dc
	cfg.Logger = cfg.Logger.With(slog.String("component", "ingest/writer"), slog.String("mode", "direct"),
		slog.String("namespace", string(cfg.Namespace)))

	next, err := readDirectState(context.Background(), dc.Session.DB(), cfg.Namespace)
	if err != nil {
		return nil, err
	}
	w := &Writer{cfg: cfg, nextSeq: next, durableNextSeq: next}
	w.gaps, _ = seqspace.NewGaps(nil)
	// No client reads a direct writer (pods serve nothing until steady
	// state), so this log only holds events until they commit.
	w.readLog = newReadableLog(next, cfg.ReadLogRetentionBytes, nil)
	ctx, cancel := context.WithCancel(context.Background())
	d := &directWriter{
		cfg:     &w.cfg,
		direct:  w.cfg.Direct,
		readLog: w.readLog,
		logger:  w.cfg.Logger,
		ctx:     ctx,
		cancel:  cancel,
		uploads: make(chan struct{}, dc.UploadConcurrency),
		wake:    make(chan struct{}, 1),
		done:    make(chan struct{}),
		nextSeq: next,
		changed: make(chan struct{}),
	}
	w.direct = d
	cfg.Metrics.setNextSeq(next)
	go d.commitLoop()
	w.cfg.Logger.Info("opened direct writer", "next_seq", next, "epoch", dc.Session.Epoch())
	return w, nil
}

// readDirectState reads the namespace's seq key at session start (design
// §10.2). It is the committed value: whatever an earlier session assigned
// but never committed is reassigned. Main must hold no hot batches: direct
// mode precedes hot mode, and a block committed behind hot batches would
// break the seq tiling.
func readDirectState(ctx context.Context, db catalog.DB, ns catalog.Namespace) (uint64, error) {
	key := catalog.SeqKey(ns)
	tx, err := db.BeginRead(ctx)
	if err != nil {
		return 0, fmt.Errorf("ingest: read %s: %w", key, err)
	}
	defer func() { _ = tx.Close(ctx) }()
	vals, err := tx.MetaGet(ctx, [][]byte{[]byte(key)})
	if err != nil {
		return 0, fmt.Errorf("ingest: read %s: %w", key, err)
	}
	v, found := vals[key]
	next, err := catalog.DecodeSeq(key, v, found)
	if err != nil {
		return 0, err
	}
	if ns == catalog.Main {
		rows, err := tx.HotBatches(ctx, math.MaxUint64)
		if err != nil {
			return 0, fmt.Errorf("ingest: read hot batches: %w", err)
		}
		if len(rows) > 0 {
			return 0, catalog.Corruptf(catalog.SourceHotBatch, "direct mode opened over hot batches [%d,%d]",
				rows[0].FirstSeq, rows[len(rows)-1].LastSeq)
		}
	}
	return next, nil
}

func (d *directWriter) append(ctx context.Context, ev *segment.Event) error {
	return d.appendN(ctx, 1, func(int) *segment.Event { return ev })
}

func (d *directWriter) appendBatch(ctx context.Context, events []segment.Event) error {
	return d.appendN(ctx, len(events), func(i int) *segment.Event { return &events[i] })
}

// appendN waits for room below MaxPendingBlocks, then appends all n events
// under one hold of mu, so a batch's seqs are contiguous as in local mode.
func (d *directWriter) appendN(ctx context.Context, n int, at func(int) *segment.Event) (err error) {
	d.mu.Lock()
	defer d.endIfFailed(&err)
	defer d.mu.Unlock()
	for d.pending >= d.direct.MaxPendingBlocks {
		if err := d.usableLocked(); err != nil {
			d.cfg.Metrics.incAppendErrors()
			return err
		}
		if err := d.waitLocked(ctx); err != nil {
			return err
		}
	}
	for i := range n {
		if err := d.appendLocked(at(i)); err != nil {
			return err
		}
	}
	return nil
}

func (d *directWriter) appendLocked(ev *segment.Event) error {
	if err := d.usableLocked(); err != nil {
		d.cfg.Metrics.incAppendErrors()
		return err
	}
	if err := segment.ValidateEvent(*ev); err != nil {
		d.cfg.Metrics.incAppendErrors()
		return fmt.Errorf("ingest: append: %w", err)
	}
	if d.block == nil {
		d.block = &directBlock{first: d.nextSeq, events: make([]segment.Event, 0, d.cfg.MaxEventsPerBlock)}
	}
	blk := d.block

	candidate := *ev
	candidate.Seq = d.nextSeq
	// As in hot mode, the hook runs before the event is buffered and a hook
	// error fails the writer, so an event whose Append failed never commits.
	if d.cfg.OnAppend != nil {
		prev := ev.Seq
		ev.Seq = candidate.Seq
		if err := d.cfg.OnAppend(ev); err != nil {
			ev.Seq = prev
			d.cfg.Metrics.incAppendErrors()
			err = fmt.Errorf("ingest: on_append: %w", err)
			d.failLocked(err)
			return &hookFailure{err: err}
		}
	}
	entry := catalog.NewLogEntry(&candidate)
	blk.events = append(blk.events, *entry.Event())
	ev.Seq = candidate.Seq
	d.nextSeq++
	d.cfg.Metrics.incEventsAppended()
	d.cfg.Metrics.setNextSeq(d.nextSeq)
	d.readLog.appendEntry(entry)

	if len(blk.events) >= d.cfg.MaxEventsPerBlock {
		d.freezeLocked()
	}
	return nil
}

func (d *directWriter) usableLocked() error {
	if d.closed {
		return ErrClosed
	}
	return d.err
}

// freezeLocked freezes the open block (design §10.6 step 1). The prepare
// value is sampled here so it is tied to exactly this block's events.
func (d *directWriter) freezeLocked() {
	b := d.block
	if b == nil {
		return
	}
	d.block = nil
	if d.cfg.DurableBatchPrepareValue != nil {
		b.prepareValue = d.cfg.DurableBatchPrepareValue()
	}
	b.ready = make(chan struct{})
	d.pending++
	d.enqueueLocked(directItem{block: b})
	d.encoders.Add(1)
	go d.prepare(b)
}

func (d *directWriter) enqueueLocked(it directItem) {
	d.queue = append(d.queue, it)
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

// prepare encodes a frozen block and uploads it (design §10.6 step 2).
func (d *directWriter) prepare(b *directBlock) {
	defer d.encoders.Done()
	defer close(b.ready)
	bb, err := segment.NewBlockBuilder(d.cfg.MaxEventsPerBlock)
	if err != nil {
		b.err = err
		return
	}
	for i := range b.events {
		if _, err := bb.Append(b.events[i]); err != nil {
			b.err = fmt.Errorf("ingest: encode block at seq %d: %w", b.events[i].Seq, err)
			return
		}
	}
	b.frame, b.info = bb.Encode()
	if err := d.crash(crashpoint.AfterDirectBlockCutBeforeUpload); err != nil {
		b.err = err
		return
	}
	select {
	case d.uploads <- struct{}{}:
	case <-d.ctx.Done():
		b.err = d.ctx.Err()
		return
	}
	refs, err := d.direct.Uploader.Upload(d.ctx, d.direct.Session, [][]byte{b.frame})
	<-d.uploads
	switch {
	case err != nil:
		b.err = fmt.Errorf("ingest: upload block [%d,%d]: %w", b.first, b.last(), err)
	case len(refs) != 1:
		b.err = fmt.Errorf("ingest: upload block [%d,%d]: got %d refs", b.first, b.last(), len(refs))
	default:
		b.ref = refs[0]
	}
}

func (d *directWriter) commitLoop() {
	defer close(d.done)
	for {
		it := d.dequeue()
		switch {
		case it.stop:
			return
		case it.barrier != nil:
			it.barrier <- d.failure()
		case d.failure() != nil:
			// Nothing after a failure commits: the next session starts at
			// the committed seq key.
		case it.block != nil:
			if err := d.commitBlock(it.block); err != nil {
				d.fail(err)
			}
		case it.meta != nil:
			if err := d.commitMeta(it.meta); err != nil {
				d.fail(err)
			}
		case it.seal:
			if err := d.direct.Sealer.Seal(d.ctx); err != nil {
				d.fail(err)
			}
		}
	}
}

func (d *directWriter) dequeue() directItem {
	for {
		d.mu.Lock()
		if len(d.queue) > 0 {
			it := d.queue[0]
			d.queue[0] = directItem{}
			d.queue = d.queue[1:]
			d.mu.Unlock()
			return it
		}
		d.mu.Unlock()
		<-d.wake
	}
}

func (d *directWriter) hook() DurableBatchHook {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.cfg.OnDurableBatch
}

// commitBlock is design §10.6 steps 3-5 for one block: the block
// transaction with the hook's metadata, then the hook's callbacks and the
// durable watermark, then the rotation rule. The rule also runs before the
// commit: an earlier session may have committed the block that crossed the
// threshold and ended before its seal.
func (d *directWriter) commitBlock(b *directBlock) error {
	<-b.ready
	if b.err != nil {
		return b.err
	}
	if err := d.crash(crashpoint.AfterDirectBlockUploadBeforeCommit); err != nil {
		return err
	}
	return obs.Span(d.ctx, func(ctx context.Context) error {
		trace.SpanFromContext(ctx).SetAttributes(
			attribute.String("namespace", string(d.cfg.Namespace)),
			attribute.Int64("first_seq", int64(b.first)),
			attribute.Int("events", len(b.events)))
		if err := d.direct.Sealer.RotateIfFull(ctx); err != nil {
			return err
		}
		ops := metastore.NewOpBatch(nil)
		var afterCommit func()
		// err is the commit's outcome, which is all afterDone reports: a
		// failure after the commit landed fails the writer, not the batch.
		var err error
		if hook := d.hook(); hook != nil {
			var afterDone func(error)
			afterCommit, afterDone, err = hook(ctx, ops, b.last()+1, false, b.prepareValue)
			if afterDone != nil {
				defer func() { afterDone(err) }()
			}
			if err != nil {
				return fmt.Errorf("ingest: on_durable_batch: %w", err)
			}
		}
		var res catalog.BlockCommit
		res, err = d.direct.Session.CommitBlock(ctx, catalog.Block{
			Namespace: d.cfg.Namespace,
			Info:      b.info,
			Object:    b.ref,
			Meta:      ops.Ops(),
		})
		if err != nil {
			return fmt.Errorf("ingest: commit block [%d,%d]: %w", b.first, b.last(), err)
		}
		trace.SpanFromContext(ctx).SetAttributes(attribute.Int64("revision", int64(res.Revision)))
		if cerr := d.crash(crashpoint.AfterDirectBlockCommitBeforeAck); cerr != nil {
			return cerr
		}
		if cerr := d.direct.Sealer.Committed(res, b.ref, b.frame); cerr != nil {
			return cerr
		}
		d.committed(b)
		if afterCommit != nil {
			afterCommit()
		}
		d.cfg.Metrics.incBlocksFlushed()
		return d.direct.Sealer.RotateIfFull(ctx)
	})
}

// committed advances the durable watermark past b and releases its room
// under MaxPendingBlocks.
func (d *directWriter) committed(b *directBlock) {
	d.readLog.advanceDurable(b.last() + 1)
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pending--
	d.signalLocked()
}

// commitMeta commits the hook's output with no events. There is nothing to
// commit, and so no transaction, when the hook stages nothing.
func (d *directWriter) commitMeta(m *hotMeta) error {
	hook := d.hook()
	if hook == nil {
		return nil
	}
	ops := metastore.NewOpBatch(nil)
	afterCommit, afterDone, err := hook(d.ctx, ops, m.nextSeq, true, m.prepareValue)
	if err != nil {
		return fmt.Errorf("ingest: on_durable_batch: %w", err)
	}
	if ops.Len() > 0 {
		_, err = d.direct.Session.CommitMeta(d.ctx, ops.Ops())
	}
	if afterDone != nil {
		defer afterDone(err)
	}
	if err != nil {
		return fmt.Errorf("ingest: commit durable metadata: %w", err)
	}
	if afterCommit != nil {
		afterCommit()
	}
	return nil
}

func (d *directWriter) crash(p crashpoint.Point) error {
	if d.direct.Crash == nil {
		return nil
	}
	return d.direct.Crash.SimulateCrash(d.ctx, p)
}

func (d *directWriter) failure() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.err
}

// fail ends the writer: nothing later commits, every waiter gets err, and
// the session ends (design §9.1) if the failure did not already end it.
func (d *directWriter) fail(err error) {
	d.mu.Lock()
	first := d.failLocked(err)
	d.mu.Unlock()
	if first {
		d.failed(err)
	}
}

// failLocked records err as the writer's failure and reports whether it is
// the first; the caller then runs failed once mu is released.
func (d *directWriter) failLocked(err error) bool {
	if d.err != nil {
		return false
	}
	d.err = err
	d.signalLocked()
	return true
}

// endIfFailed runs failed for an append whose hook failed, after mu is
// released.
func (d *directWriter) endIfFailed(errp *error) {
	var f *hookFailure
	if errors.As(*errp, &f) {
		*errp = f.err
		d.failed(f.err)
	}
}

// failed ends the session and reports a failure failLocked recorded first.
func (d *directWriter) failed(err error) {
	d.cancel()
	if serr := d.direct.Session.Err(); serr == nil {
		_ = d.direct.Session.End("direct writer", err)
	}
	d.logger.Error("direct writer failed", "err", err)
	if d.direct.OnFailure != nil {
		d.direct.OnFailure(err)
	}
}

// waitLocked releases mu until the next signal or ctx's end.
func (d *directWriter) waitLocked(ctx context.Context) error {
	ch := d.changed
	d.waiters++
	d.mu.Unlock()
	var err error
	select {
	case <-ch:
	case <-ctx.Done():
		err = ctx.Err()
	}
	d.mu.Lock()
	d.waiters--
	return err
}

func (d *directWriter) signalLocked() {
	if d.waiters == 0 {
		return
	}
	close(d.changed)
	d.changed = make(chan struct{})
}

// barrier queues a marker behind everything already queued and waits for
// the committer to reach it: every block frozen before it has committed, or
// the writer failed.
func (d *directWriter) barrier(ctx context.Context, prep func()) error {
	ch := make(chan error, 1)
	d.mu.Lock()
	if err := d.usableLocked(); err != nil {
		d.mu.Unlock()
		return err
	}
	prep()
	d.enqueueLocked(directItem{barrier: ch})
	d.mu.Unlock()
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *directWriter) flush(ctx context.Context) error {
	return d.barrier(ctx, d.freezeLocked)
}

func (d *directWriter) drainDurability(ctx context.Context) error {
	return d.barrier(ctx, func() {
		d.freezeLocked()
		d.enqueueMetaLocked()
	})
}

func (d *directWriter) enqueueMetaLocked() {
	if d.cfg.OnDurableBatch == nil {
		return
	}
	m := &hotMeta{nextSeq: d.nextSeq}
	if d.cfg.DurableBatchPrepareValue != nil {
		m.prepareValue = d.cfg.DurableBatchPrepareValue()
	}
	d.enqueueLocked(directItem{meta: m})
}

// forceRotate commits the open block and seals the active segment if it
// holds any block, as local mode's ForceRotate does.
func (d *directWriter) forceRotate(ctx context.Context) error {
	return d.barrier(ctx, func() {
		d.freezeLocked()
		d.enqueueLocked(directItem{seal: true})
	})
}

// close commits everything appended so far and stops the writer. The active
// segment stays active: the next direct writer, or the hot writer after
// merge, continues it.
func (d *directWriter) close() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	if d.err == nil {
		d.freezeLocked()
		d.enqueueMetaLocked()
	}
	d.closed = true
	d.signalLocked()
	d.enqueueLocked(directItem{stop: true})
	d.mu.Unlock()

	<-d.done
	d.cancel()
	d.encoders.Wait()
	return d.failure()
}

func (d *directWriter) sealActiveAndClose() error {
	if err := d.forceRotate(context.Background()); err != nil && !errors.Is(err, ErrClosed) {
		_ = d.close()
		return err
	}
	return d.close()
}

func (d *directWriter) setHook(hook DurableBatchHook) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.cfg.OnDurableBatch = hook
}

func (d *directWriter) next() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.nextSeq
}
