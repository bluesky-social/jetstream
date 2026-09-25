package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/internal/seqspace"
	"github.com/bluesky-social/jetstream/segment"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Hot mode defaults (design §10.3, §18).
const (
	DefaultHotBatchMaxEvents = 256
	DefaultHotBatchMaxBytes  = 256 << 10
	DefaultHotBatchMaxAge    = 15 * time.Millisecond
	DefaultBlockMaxAge       = 30 * time.Second
	DefaultUploadConcurrency = 8
)

// Class is an append's admission class (design §10.5). Every hot batch holds
// events of one class, so a class change cuts the batch.
type Class uint8

const (
	// ClassLive is the firehose consumer. Untagged appends are live.
	ClassLive Class = iota
	// ClassBulk is failed-repo retries, sync 1.1 resyncs, PDS recovery, and
	// any other producer that appends more than one repo's worth of events.
	ClassBulk
)

func (c Class) String() string {
	if c == ClassBulk {
		return "bulk"
	}
	return "live"
}

type classKey struct{}

// WithClass tags the appends made with ctx as class c. The class travels in
// the context so producers that share a Writer need no new Append variants.
func WithClass(ctx context.Context, c Class) context.Context {
	return context.WithValue(ctx, classKey{}, c)
}

// ClassOf returns ctx's admission class: ClassLive unless tagged.
func ClassOf(ctx context.Context) Class {
	c, _ := ctx.Value(classKey{}).(Class)
	return c
}

// ObjectUploader stores pointer batch frames (design §7.3).
// protocol.Uploader implements it.
type ObjectUploader interface {
	// Upload returns one reference per object. A fresh upload is Pending:
	// the hot batch transaction makes it available. Any error has already
	// ended s.
	Upload(ctx context.Context, s *catalog.Session, objs [][]byte) ([]catalog.ObjectRef, error)
}

// HotBatchInfo describes one committed hot batch of a closed block.
type HotBatchInfo struct {
	FirstSeq, LastSeq uint64
	Class             Class
	// ObjectID is the pointer batch's object after commit resolved it, or
	// zero for an inline batch.
	ObjectID uint64
}

// ClosedBlock is a closed open block, handed to the maintainer for fold
// (design §10.7) once its last batch has committed.
type ClosedBlock struct {
	FirstSeq, LastSeq uint64
	// Events are the block's events in seq order. The maintainer owns them.
	Events  []segment.Event
	Batches []HotBatchInfo
	// OpenedAt is when the block's first event was appended.
	OpenedAt time.Time
}

// BlockSink is the maintainer side of hot mode.
type BlockSink interface {
	// BlockClosed receives each closed block, in seq order, from the
	// committer goroutine after the block's last batch commits. It must not
	// block on the Writer or do unbounded work.
	BlockClosed(ClosedBlock)
	// Rotate seals the active segment once every block already handed to
	// BlockClosed is folded (design §10.8). ForceRotate calls it.
	Rotate(ctx context.Context) error
}

// HotConfig configures hot mode. Session is required; zero values take the
// defaults above.
type HotConfig struct {
	// Session is the leader session every commit runs in.
	Session *catalog.Session
	// Uploader stores pointer batches. Nil commits every batch inline.
	Uploader ObjectUploader
	// Sink receives closed blocks. Nil drops them.
	Sink BlockSink

	BatchMaxEvents int
	BatchMaxBytes  int64
	BatchMaxAge    time.Duration
	BlockMaxAge    time.Duration
	// UploadConcurrency bounds the writer's pointer batch uploads in flight.
	UploadConcurrency int

	// OnCommit runs in the committer goroutine after each commit and after
	// the acks it releases: the local follower's doorbell. It must not
	// block.
	OnCommit func(rev uint64)
	// OnFailure is called once with the error that ended the writer. The
	// session has already ended; the owner closes the writer and starts a
	// new session. It may run in the committer goroutine, so it must not
	// call Writer methods itself; Close waits for that goroutine.
	OnFailure func(error)
}

func (c *Config) validateHot() error {
	h := c.Hot
	switch {
	case h.Session == nil:
		return fmt.Errorf("%w: Hot.Session is required", ErrInvalidConfig)
	case c.Logger == nil:
		return fmt.Errorf("%w: Logger is required", ErrInvalidConfig)
	case c.SeqKey != "" && c.SeqKey != catalog.MainSeqKey:
		return fmt.Errorf("%w: hot mode writes only %q", ErrInvalidConfig, catalog.MainSeqKey)
	case c.Namespace != "" && c.Namespace != catalog.Main:
		return fmt.Errorf("%w: hot mode writes only namespace %q", ErrInvalidConfig, catalog.Main)
	case c.ReserveClientVisibleSeqs || c.UnreservedSeqsUnobservable:
		// A hot seq is never visible before it commits (design §10.1).
		return fmt.Errorf("%w: hot mode has no sequence lease", ErrInvalidConfig)
	case c.AsyncFlushWorkers != 0 || c.Catalog != nil:
		return fmt.Errorf("%w: hot mode takes neither AsyncFlushWorkers nor Catalog", ErrInvalidConfig)
	case c.MaxEventsPerBlock < 0 || c.ReadLogRetentionBytes < 0:
		return fmt.Errorf("%w: MaxEventsPerBlock and ReadLogRetentionBytes must be >= 0", ErrInvalidConfig)
	case h.BatchMaxEvents < 0 || h.BatchMaxBytes < 0 || h.BatchMaxAge < 0 || h.BlockMaxAge < 0 || h.UploadConcurrency < 0:
		return fmt.Errorf("%w: Hot limits must be >= 0", ErrInvalidConfig)
	}
	if _, err := segment.NewBlockBuilder(c.MaxEventsPerBlock); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	return nil
}

func (h *HotConfig) applyDefaults() {
	if h.BatchMaxEvents == 0 {
		h.BatchMaxEvents = DefaultHotBatchMaxEvents
	}
	if h.BatchMaxBytes == 0 {
		h.BatchMaxBytes = DefaultHotBatchMaxBytes
	}
	if h.BatchMaxAge == 0 {
		h.BatchMaxAge = DefaultHotBatchMaxAge
	}
	if h.BlockMaxAge == 0 {
		h.BlockMaxAge = DefaultBlockMaxAge
	}
	if h.UploadConcurrency == 0 {
		h.UploadConcurrency = DefaultUploadConcurrency
	}
}

// hotWriter is the Writer in hot mode (design §10.3, §10.4). Appends assign
// seqs and fill one open block, cut into batches. A cut batch is frozen: its
// events are detached under the lock, and a goroutine encodes (and for a
// pointer batch uploads) the frame. One committer goroutine commits frozen
// batches strictly in seq order and releases everything that waits on
// durability after the commit.
//
// Time is read with time.Now and timers, so tests drive it with a synctest
// bubble instead of an injected clock.
type hotWriter struct {
	cfg     *Config
	hot     *HotConfig
	readLog *ReadableLog
	logger  *slog.Logger

	// ctx is canceled when the writer fails or finishes closing, which
	// abandons in-flight uploads.
	ctx    context.Context
	cancel context.CancelFunc

	uploads  chan struct{} // upload concurrency semaphore
	encoders sync.WaitGroup
	wake     chan struct{} // committer: queue grew
	agerWake chan struct{} // ager: a deadline moved earlier
	stopAger chan struct{}
	done     chan struct{} // committer exited
	agerDone chan struct{}

	mu      sync.Mutex
	nextSeq uint64
	block   *hotBlock
	batch   *hotBatch
	queue   []hotItem
	closed  bool
	err     error // sticky failure
}

// hotBlock is the open block. events has capacity for a full block, so the
// batches' subslices of it stay valid as it grows.
type hotBlock struct {
	first    uint64
	events   []segment.Event
	batches  []*hotBatch
	openedAt time.Time
}

type hotBatch struct {
	first    uint64
	start    int // index of the first event in block.events
	n        int
	raw      int64
	class    Class
	openedAt time.Time

	// Set at freeze.
	events       []segment.Event
	pointer      bool
	prepareValue any
	ready        chan struct{}

	// Set by prepare before ready closes.
	frame []byte
	info  segment.BlockInfo
	ref   catalog.ObjectRef
	err   error

	// Set by the committer.
	objectID uint64
}

func (b *hotBatch) last() uint64 { return b.first + uint64(b.n) - 1 }

// hotItem is one committer queue entry. Exactly one field is set.
type hotItem struct {
	batch   *hotBatch
	meta    *hotMeta
	block   *hotBlock
	barrier chan error
	stop    bool
}

// hotMeta is a metadata-only commit of the DurableBatchHook's output, for
// DrainDurability and Close (force=true in the local writer's terms).
type hotMeta struct {
	nextSeq      uint64
	prepareValue any
}

func openHot(cfg Config) (*Writer, error) {
	cfg.applyDefaults()
	hc := *cfg.Hot
	hc.applyDefaults()
	cfg.Hot = &hc
	cfg.Logger = cfg.Logger.With(slog.String("component", "ingest/writer"), slog.String("mode", "hot"))

	next, err := readHotSeq(context.Background(), hc.Session.DB())
	if err != nil {
		return nil, err
	}

	w := &Writer{cfg: cfg, nextSeq: next, durableNextSeq: next}
	w.gaps, _ = seqspace.NewGaps(nil)
	w.readLog = newReadableLog(next, cfg.ReadLogRetentionBytes, cfg.Metrics)
	ctx, cancel := context.WithCancel(context.Background())
	h := &hotWriter{
		cfg:      &w.cfg,
		hot:      w.cfg.Hot,
		readLog:  w.readLog,
		logger:   w.cfg.Logger,
		ctx:      ctx,
		cancel:   cancel,
		uploads:  make(chan struct{}, hc.UploadConcurrency),
		wake:     make(chan struct{}, 1),
		agerWake: make(chan struct{}, 1),
		stopAger: make(chan struct{}),
		done:     make(chan struct{}),
		agerDone: make(chan struct{}),
		nextSeq:  next,
	}
	w.hot = h
	cfg.Metrics.setNextSeq(next)
	go h.commitLoop()
	go h.ageLoop()
	w.cfg.Logger.Info("opened hot writer", "next_seq", next, "epoch", hc.Session.Epoch())
	return w, nil
}

// readHotSeq reads seq/next at session start (design §10.2). It is the
// committed value: whatever an earlier session assigned but never committed
// is reassigned.
func readHotSeq(ctx context.Context, db catalog.DB) (uint64, error) {
	tx, err := db.BeginRead(ctx)
	if err != nil {
		return 0, fmt.Errorf("ingest: read %s: %w", catalog.MainSeqKey, err)
	}
	defer func() { _ = tx.Close(ctx) }()
	vals, err := tx.MetaGet(ctx, [][]byte{[]byte(catalog.MainSeqKey)})
	if err != nil {
		return 0, fmt.Errorf("ingest: read %s: %w", catalog.MainSeqKey, err)
	}
	v, found := vals[catalog.MainSeqKey]
	return catalog.DecodeSeq(catalog.MainSeqKey, v, found)
}

// rawEventBytes is the batch byte cut's measure of an event: its variable
// fields plus the fixed columns.
func rawEventBytes(ev *segment.Event) int64 {
	return int64(len(ev.DID)+len(ev.Collection)+len(ev.Rkey)+len(ev.Rev)+len(ev.Payload)) + 33
}

func (h *hotWriter) append(ctx context.Context, ev *segment.Event) error {
	class := ClassOf(ctx)
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.appendLocked(class, ev)
}

func (h *hotWriter) appendBatch(ctx context.Context, events []segment.Event) error {
	class := ClassOf(ctx)
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := range events {
		if err := h.appendLocked(class, &events[i]); err != nil {
			return err
		}
	}
	return nil
}

func (h *hotWriter) appendLocked(class Class, ev *segment.Event) error {
	if err := h.usableLocked(); err != nil {
		h.cfg.Metrics.incAppendErrors()
		return err
	}
	if err := segment.ValidateEvent(*ev); err != nil {
		h.cfg.Metrics.incAppendErrors()
		return fmt.Errorf("ingest: append: %w", err)
	}
	if h.batch != nil && h.batch.class != class {
		h.freezeLocked()
	}
	now := time.Now()
	if h.block == nil {
		h.block = &hotBlock{
			first:    h.nextSeq,
			events:   make([]segment.Event, 0, h.cfg.MaxEventsPerBlock),
			openedAt: now,
		}
		h.kickAger()
	}
	blk := h.block
	if h.batch == nil {
		h.batch = &hotBatch{first: h.nextSeq, start: len(blk.events), class: class, openedAt: now}
		blk.batches = append(blk.batches, h.batch)
		h.kickAger()
	}

	candidate := *ev
	candidate.Seq = h.nextSeq
	// One copy serves the read log and the open block.
	entry := catalog.NewLogEntry(&candidate)
	blk.events = append(blk.events, *entry.Event())
	b := h.batch
	b.n++
	b.raw += rawEventBytes(&candidate)
	ev.Seq = candidate.Seq
	h.nextSeq++
	h.cfg.Metrics.incEventsAppended()
	h.cfg.Metrics.setNextSeq(h.nextSeq)
	h.readLog.appendEntry(entry)

	if h.cfg.OnAppend != nil {
		if err := h.cfg.OnAppend(ev); err != nil {
			return fmt.Errorf("ingest: on_append: %w", err)
		}
	}

	switch {
	case len(blk.events) >= h.cfg.MaxEventsPerBlock:
		h.closeBlockLocked()
	case b.n >= h.hot.BatchMaxEvents || b.raw >= h.hot.BatchMaxBytes:
		h.freezeLocked()
	}
	return nil
}

func (h *hotWriter) usableLocked() error {
	if h.closed {
		return ErrClosed
	}
	return h.err
}

// freezeLocked cuts the open batch (design §10.3). Encoding happens off the
// lock; the prepare value is sampled here so it is tied to exactly this
// batch's events.
func (h *hotWriter) freezeLocked() {
	b := h.batch
	if b == nil {
		return
	}
	h.batch = nil
	b.events = h.block.events[b.start : b.start+b.n : b.start+b.n]
	b.pointer = b.class == ClassBulk && h.hot.Uploader != nil
	if h.cfg.DurableBatchPrepareValue != nil {
		b.prepareValue = h.cfg.DurableBatchPrepareValue()
	}
	b.ready = make(chan struct{})
	h.enqueueLocked(hotItem{batch: b})
	h.encoders.Add(1)
	go h.prepare(b)
}

// closeBlockLocked cuts the open batch and closes the open block. The block
// reaches the sink after its last batch commits.
func (h *hotWriter) closeBlockLocked() {
	if h.block == nil {
		return
	}
	h.freezeLocked()
	h.enqueueLocked(hotItem{block: h.block})
	h.block = nil
}

func (h *hotWriter) enqueueLocked(it hotItem) {
	h.queue = append(h.queue, it)
	select {
	case h.wake <- struct{}{}:
	default:
	}
}

func (h *hotWriter) kickAger() {
	select {
	case h.agerWake <- struct{}{}:
	default:
	}
}

// prepare encodes a frozen batch and, for a pointer batch, uploads it.
func (h *hotWriter) prepare(b *hotBatch) {
	defer h.encoders.Done()
	defer close(b.ready)
	bb, err := segment.NewBlockBuilder(h.cfg.MaxEventsPerBlock)
	if err != nil {
		b.err = err
		return
	}
	for i := range b.events {
		if _, err := bb.Append(b.events[i]); err != nil {
			b.err = fmt.Errorf("ingest: encode hot batch at seq %d: %w", b.events[i].Seq, err)
			return
		}
	}
	b.frame, b.info = bb.Encode()
	if !b.pointer {
		return
	}
	select {
	case h.uploads <- struct{}{}:
	case <-h.ctx.Done():
		b.err = h.ctx.Err()
		return
	}
	refs, err := h.hot.Uploader.Upload(h.ctx, h.hot.Session, [][]byte{b.frame})
	<-h.uploads
	switch {
	case err != nil:
		b.err = fmt.Errorf("ingest: upload hot batch [%d,%d]: %w", b.first, b.last(), err)
	case len(refs) != 1:
		b.err = fmt.Errorf("ingest: upload hot batch [%d,%d]: got %d refs", b.first, b.last(), len(refs))
	default:
		b.ref = refs[0]
	}
}

func (h *hotWriter) commitLoop() {
	defer close(h.done)
	for {
		it := h.dequeue()
		switch {
		case it.stop:
			return
		case it.barrier != nil:
			it.barrier <- h.failure()
		case h.failure() != nil:
			// Nothing after a failure commits, and a block whose batches did
			// not all commit must not be folded: the next session rebuilds
			// from what did.
		case it.batch != nil:
			if err := h.commitBatch(it.batch); err != nil {
				h.fail(err)
			}
		case it.meta != nil:
			if err := h.commitMeta(it.meta); err != nil {
				h.fail(err)
			}
		case it.block != nil:
			h.deliver(it.block)
		}
	}
}

func (h *hotWriter) dequeue() hotItem {
	for {
		h.mu.Lock()
		if len(h.queue) > 0 {
			it := h.queue[0]
			h.queue[0] = hotItem{}
			h.queue = h.queue[1:]
			h.mu.Unlock()
			return it
		}
		h.mu.Unlock()
		<-h.wake
	}
}

func (h *hotWriter) hook() DurableBatchHook {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.cfg.OnDurableBatch
}

// commitBatch is design §10.4: one transaction, then the hook's callbacks,
// the durable watermark, and the doorbell.
func (h *hotWriter) commitBatch(b *hotBatch) error {
	<-b.ready
	if b.err != nil {
		return b.err
	}
	return obs.Span(h.ctx, func(ctx context.Context) error {
		trace.SpanFromContext(ctx).SetAttributes(
			attribute.Int64("first_seq", int64(b.first)),
			attribute.Int("events", b.n),
			attribute.String("class", b.class.String()),
			attribute.Bool("pointer", b.pointer),
		)
		next := b.last() + 1
		ops := metastore.NewOpBatch(nil)
		var afterCommit func()
		var afterDone func(error)
		if hook := h.hook(); hook != nil {
			var err error
			afterCommit, afterDone, err = hook(ctx, ops, next, false, b.prepareValue)
			if err != nil {
				return fmt.Errorf("ingest: on_durable_batch: %w", err)
			}
		}
		hb := catalog.HotBatch{
			FirstSeq:       b.first,
			LastSeq:        b.last(),
			MinWitnessedUS: b.info.MinWitnessedAt,
			MaxWitnessedUS: b.info.MaxWitnessedAt,
			Meta:           ops.Ops(),
		}
		if b.pointer {
			hb.Object = b.ref
		} else {
			hb.Frame = b.frame
		}
		res, err := h.hot.Session.CommitHotBatch(ctx, hb)
		if afterDone != nil {
			defer afterDone(err)
		}
		if err != nil {
			return fmt.Errorf("ingest: commit hot batch [%d,%d]: %w", b.first, b.last(), err)
		}
		trace.SpanFromContext(ctx).SetAttributes(attribute.Int64("revision", int64(res.Revision)))
		b.objectID = res.ObjectID
		h.readLog.advanceDurable(next)
		if afterCommit != nil {
			afterCommit()
		}
		h.cfg.Metrics.observeHotBatch(b.class, b.pointer, b.n)
		if h.hot.OnCommit != nil {
			h.hot.OnCommit(res.Revision)
		}
		return nil
	})
}

// commitMeta commits the hook's output with no events. There is nothing to
// commit, and so no transaction, when the hook stages nothing.
func (h *hotWriter) commitMeta(m *hotMeta) error {
	hook := h.hook()
	if hook == nil {
		return nil
	}
	ops := metastore.NewOpBatch(nil)
	afterCommit, afterDone, err := hook(h.ctx, ops, m.nextSeq, true, m.prepareValue)
	if err != nil {
		return fmt.Errorf("ingest: on_durable_batch: %w", err)
	}
	var rev uint64
	if ops.Len() > 0 {
		rev, err = h.hot.Session.CommitMeta(h.ctx, ops.Ops())
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
	if rev != 0 && h.hot.OnCommit != nil {
		h.hot.OnCommit(rev)
	}
	return nil
}

func (h *hotWriter) deliver(blk *hotBlock) {
	if h.hot.Sink == nil {
		return
	}
	cb := ClosedBlock{
		FirstSeq: blk.first,
		LastSeq:  blk.first + uint64(len(blk.events)) - 1,
		Events:   blk.events,
		Batches:  make([]HotBatchInfo, len(blk.batches)),
		OpenedAt: blk.openedAt,
	}
	for i, b := range blk.batches {
		cb.Batches[i] = HotBatchInfo{FirstSeq: b.first, LastSeq: b.last(), Class: b.class, ObjectID: b.objectID}
	}
	h.hot.Sink.BlockClosed(cb)
}

func (h *hotWriter) failure() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.err
}

// fail ends the writer: nothing later commits, every waiter gets err, and
// the session ends (design §9.1) if the failure did not already end it.
func (h *hotWriter) fail(err error) {
	h.mu.Lock()
	if h.err != nil {
		h.mu.Unlock()
		return
	}
	h.err = err
	h.mu.Unlock()
	h.cancel()
	if serr := h.hot.Session.Err(); serr == nil {
		_ = h.hot.Session.End("hot writer", err)
	}
	h.logger.Error("hot writer failed", "err", err)
	if h.hot.OnFailure != nil {
		h.hot.OnFailure(err)
	}
}

// ageLoop cuts batches and closes blocks by age.
func (h *hotWriter) ageLoop() {
	defer close(h.agerDone)
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	for {
		h.mu.Lock()
		now := time.Now()
		deadline, ok := h.expireLocked(now)
		h.mu.Unlock()
		if ok {
			timer.Reset(deadline.Sub(now))
		}
		select {
		case <-h.stopAger:
			timer.Stop()
			return
		case <-h.agerWake:
		case <-timer.C:
		}
		timer.Stop()
	}
}

// expireLocked applies every age cut due at now and returns the next
// deadline, if any.
func (h *hotWriter) expireLocked(now time.Time) (time.Time, bool) {
	if h.closed || h.err != nil {
		return time.Time{}, false
	}
	if h.block != nil && !now.Before(h.block.openedAt.Add(h.hot.BlockMaxAge)) {
		h.closeBlockLocked()
	}
	if h.batch != nil && !now.Before(h.batch.openedAt.Add(h.hot.BatchMaxAge)) {
		h.freezeLocked()
	}
	var deadline time.Time
	ok := false
	if h.block != nil {
		deadline, ok = h.block.openedAt.Add(h.hot.BlockMaxAge), true
	}
	if h.batch != nil {
		if d := h.batch.openedAt.Add(h.hot.BatchMaxAge); !ok || d.Before(deadline) {
			deadline, ok = d, true
		}
	}
	return deadline, ok
}

// barrier queues a marker behind everything already queued and waits for
// the committer to reach it: every batch frozen before it has committed, or
// the writer failed.
func (h *hotWriter) barrier(ctx context.Context, prep func()) error {
	ch := make(chan error, 1)
	h.mu.Lock()
	if err := h.usableLocked(); err != nil {
		h.mu.Unlock()
		return err
	}
	prep()
	h.enqueueLocked(hotItem{barrier: ch})
	h.mu.Unlock()
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *hotWriter) flush(ctx context.Context) error {
	return h.barrier(ctx, h.freezeLocked)
}

func (h *hotWriter) drainDurability(ctx context.Context) error {
	return h.barrier(ctx, func() {
		h.freezeLocked()
		h.enqueueMetaLocked()
	})
}

func (h *hotWriter) enqueueMetaLocked() {
	if h.cfg.OnDurableBatch == nil {
		return
	}
	m := &hotMeta{nextSeq: h.nextSeq}
	if h.cfg.DurableBatchPrepareValue != nil {
		m.prepareValue = h.cfg.DurableBatchPrepareValue()
	}
	h.enqueueLocked(hotItem{meta: m})
}

func (h *hotWriter) forceRotate(ctx context.Context) error {
	if err := h.barrier(ctx, h.closeBlockLocked); err != nil {
		return err
	}
	if h.hot.Sink == nil {
		return nil
	}
	return h.hot.Sink.Rotate(ctx)
}

// close commits everything appended so far and stops the writer. The open
// block is not closed: the next session rebuilds it from its committed
// batches (design §10.9).
func (h *hotWriter) close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	if h.err == nil {
		h.freezeLocked()
		h.enqueueMetaLocked()
	}
	h.closed = true
	h.enqueueLocked(hotItem{stop: true})
	h.mu.Unlock()

	<-h.done
	close(h.stopAger)
	<-h.agerDone
	h.cancel()
	h.encoders.Wait()
	return h.failure()
}

func (h *hotWriter) sealActiveAndClose() error {
	if err := h.forceRotate(context.Background()); err != nil && !errors.Is(err, ErrClosed) {
		_ = h.close()
		return err
	}
	return h.close()
}

func (h *hotWriter) setHook(hook DurableBatchHook) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cfg.OnDurableBatch = hook
}

func (h *hotWriter) next() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.nextSeq
}
