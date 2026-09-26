package ingest

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/internal/seqspace"
	"github.com/bluesky-social/jetstream/segment"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Hot mode defaults (design §10.3, §10.5, §18).
const (
	DefaultHotBatchMaxEvents     = 256
	DefaultHotBatchMaxBytes      = 256 << 10
	DefaultHotBatchMaxAge        = 15 * time.Millisecond
	DefaultBlockMaxAge           = 30 * time.Second
	DefaultUploadConcurrency     = 8
	DefaultMaxCommitBatches      = 32
	DefaultInlineBytesPerSec     = 4 << 20
	DefaultOverflowBatchMaxEvent = 1024
	DefaultOverflowBatchMaxBytes = 1 << 20
	DefaultOverflowBatchMaxAge   = time.Second
	DefaultBulkChunkMaxEvents    = 4096
	DefaultBulkPendingBytes      = 64 << 20
	DefaultHotPendingBytes       = 256 << 20
	DefaultMaxUnfoldedEvents     = 65536
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
	// Folded must be called once the block's fold commits. It releases the
	// block's events from the unfolded cap (design §10.5 rule 9). It is safe
	// to call from any goroutine, and more than once.
	Folded func()
}

// OpenBlock is an open block that session start rebuilt from an earlier
// session's hot batches (design §10.9 step 6): committed events that neither
// filled a block nor reached the block max age. The hot writer continues it.
type OpenBlock struct {
	// Events are the block's events in seq order. The writer owns them.
	Events []segment.Event
	// Batches are the hot batches that hold Events, in seq order. The
	// catalog does not record a batch's class, so rebuilt batches report
	// ClassLive.
	Batches []HotBatchInfo
	// OpenedAt starts the block's age cut.
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
	// Resume is the open block the maintainer's session-start rebuild
	// returned (design §10.9). It must hold exactly the hot batches in the
	// catalog: the writer refuses to open over hot batches that were not
	// rebuilt, because it would never fold them.
	Resume *OpenBlock

	BatchMaxEvents int
	BatchMaxBytes  int64
	BatchMaxAge    time.Duration
	BlockMaxAge    time.Duration
	// UploadConcurrency bounds the writer's pointer batch uploads in flight.
	// The process-wide PUT bound (design §10.5 rule 7) is the blob store's.
	UploadConcurrency int
	// MaxCommitBatches bounds the consecutive ready batches one transaction
	// commits (group commit), and so the transaction's size. 1 commits each
	// batch alone.
	MaxCommitBatches int

	// Admission control (design §10.5). InlineBytesPerSec is the live
	// inline token bucket's rate over encoded frame bytes; the burst is one
	// second's worth. A negative rate disables the bucket: every live batch
	// commits inline. The token bucket and overflow apply only with an
	// Uploader.
	InlineBytesPerSec int64
	// OverflowMaxEvents, OverflowMaxBytes, and OverflowMaxAge cut a live
	// overflow batch (rule 4).
	OverflowMaxEvents int
	OverflowMaxBytes  int64
	OverflowMaxAge    time.Duration
	// BulkChunkMaxEvents caps a bulk chunk and a bulk batch (rules 1, 5).
	BulkChunkMaxEvents int
	// BulkPendingBytes is the bulk permit pool (rule 6), PendingBytes the
	// total frozen-uncommitted cap (rule 8), and MaxUnfoldedEvents the
	// committed-but-unfolded cap (rule 9).
	BulkPendingBytes  int64
	PendingBytes      int64
	MaxUnfoldedEvents int64

	// OnCommit runs in the committer goroutine after each commit and after
	// the acks it releases: the local follower's doorbell. It must not
	// block.
	OnCommit func(rev uint64)
	// Crash is the test-only crash seam injector. Nil in production.
	Crash crashpoint.Injector
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
	case h.BatchMaxEvents < 0 || h.BatchMaxBytes < 0 || h.BatchMaxAge < 0 || h.BlockMaxAge < 0 || h.UploadConcurrency < 0 || h.MaxCommitBatches < 0,
		h.OverflowMaxEvents < 0 || h.OverflowMaxBytes < 0 || h.OverflowMaxAge < 0 || h.BulkChunkMaxEvents < 0,
		h.BulkPendingBytes < 0 || h.PendingBytes < 0 || h.MaxUnfoldedEvents < 0:
		return fmt.Errorf("%w: Hot limits must be >= 0", ErrInvalidConfig)
	case h.MaxUnfoldedEvents != 0 && h.MaxUnfoldedEvents < int64(cmp.Or(c.MaxEventsPerBlock, defaultMaxEventsPerBlock)):
		// Below one block, appends would stall until every block's age cut.
		return fmt.Errorf("%w: Hot.MaxUnfoldedEvents must hold at least one block", ErrInvalidConfig)
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
	if h.MaxCommitBatches == 0 {
		h.MaxCommitBatches = DefaultMaxCommitBatches
	}
	if h.InlineBytesPerSec == 0 {
		h.InlineBytesPerSec = DefaultInlineBytesPerSec
	}
	if h.OverflowMaxEvents == 0 {
		h.OverflowMaxEvents = DefaultOverflowBatchMaxEvent
	}
	if h.OverflowMaxBytes == 0 {
		h.OverflowMaxBytes = DefaultOverflowBatchMaxBytes
	}
	if h.OverflowMaxAge == 0 {
		h.OverflowMaxAge = DefaultOverflowBatchMaxAge
	}
	if h.BulkChunkMaxEvents == 0 {
		h.BulkChunkMaxEvents = DefaultBulkChunkMaxEvents
	}
	if h.BulkPendingBytes == 0 {
		h.BulkPendingBytes = DefaultBulkPendingBytes
	}
	if h.PendingBytes == 0 {
		h.PendingBytes = DefaultHotPendingBytes
	}
	if h.MaxUnfoldedEvents == 0 {
		h.MaxUnfoldedEvents = DefaultMaxUnfoldedEvents
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

	// liveWaiting counts live appenders waiting for mu. Bulk appenders
	// yield to them between chunks (design §10.5 rule 1).
	liveWaiting atomic.Int64
	bucket      *tokenBucket // nil: every live batch commits inline

	mu      sync.Mutex
	nextSeq uint64
	block   *hotBlock
	batch   *hotBatch
	queue   []hotItem
	closed  bool
	err     error // sticky failure

	// Admission state (admission.go), all under mu.
	waiters int
	changed chan struct{} // closed and replaced when admission may have changed
	// pending is frozen-uncommitted raw bytes by class (rule 8).
	pending [2]int64
	// bulkPermits is the bulk permits held (rule 6); bulkCredit is the part
	// of it taken for the chunk being appended, not yet attached to a batch.
	bulkPermits, bulkCredit int64
	// bulkFrozen is the bulk batches frozen and not yet committed (rule 7).
	bulkFrozen int
	// committedNext and foldedNext bound the committed-but-unfolded events
	// (rule 9).
	committedNext, foldedNext uint64
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

	// overflow marks a live batch the token bucket could not pay for: it
	// keeps growing to the overflow limits and freezes as a pointer batch.
	overflow bool

	// Set at freeze.
	events       []segment.Event
	pointer      bool
	prepareValue any
	ready        chan struct{}
	// tokens is the inline estimate taken from the bucket, settled against
	// the frame size once encoded. permit is the bulk permits the batch
	// holds until it commits.
	tokens, permit int64

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

	next, rows, err := readHotState(context.Background(), hc.Session.DB())
	if err != nil {
		return nil, err
	}
	block, err := resumeBlock(hc.Resume, rows, next, cfg.MaxEventsPerBlock)
	if err != nil {
		return nil, err
	}
	unfoldedFrom := next
	if block != nil {
		unfoldedFrom = block.first
	}

	w := &Writer{cfg: cfg, nextSeq: next, durableNextSeq: next}
	w.gaps, _ = seqspace.NewGaps(nil)
	// Subscribers read the catalog follower's log, which owns the read-log
	// gauges; this one only holds events until they commit.
	w.readLog = newReadableLog(next, cfg.ReadLogRetentionBytes, nil)
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
		block:    block,

		changed:       make(chan struct{}),
		committedNext: next,
		foldedNext:    unfoldedFrom,
	}
	if hc.Uploader != nil && hc.InlineBytesPerSec > 0 {
		h.bucket = newTokenBucket(hc.InlineBytesPerSec, cfg.Metrics)
	}
	cfg.Metrics.setHotUnfolded(next - unfoldedFrom)
	w.hot = h
	cfg.Metrics.setNextSeq(next)
	go h.commitLoop()
	go h.ageLoop()
	w.cfg.Logger.Info("opened hot writer", "next_seq", next, "epoch", hc.Session.Epoch())
	return w, nil
}

// readHotState reads seq/next at session start (design §10.2) and the hot
// batches still unfolded (§10.5 rule 9). seq/next is the committed value:
// whatever an earlier session assigned but never committed is reassigned.
func readHotState(ctx context.Context, db catalog.DB) (next uint64, rows []catalog.HotBatchRow, err error) {
	tx, err := db.BeginRead(ctx)
	if err != nil {
		return 0, nil, fmt.Errorf("ingest: read %s: %w", catalog.MainSeqKey, err)
	}
	defer func() { _ = tx.Close(ctx) }()
	vals, err := tx.MetaGet(ctx, [][]byte{[]byte(catalog.MainSeqKey)})
	if err != nil {
		return 0, nil, fmt.Errorf("ingest: read %s: %w", catalog.MainSeqKey, err)
	}
	v, found := vals[catalog.MainSeqKey]
	if next, err = catalog.DecodeSeq(catalog.MainSeqKey, v, found); err != nil {
		return 0, nil, err
	}
	if rows, err = tx.HotBatches(ctx, math.MaxUint64); err != nil {
		return 0, nil, fmt.Errorf("ingest: read hot batches: %w", err)
	}
	if len(rows) > 0 && rows[len(rows)-1].LastSeq+1 != next {
		return 0, nil, catalog.Corruptf(catalog.SourceHotBatch, "hot batches end at seq %d, but %s is %d",
			rows[len(rows)-1].LastSeq, catalog.MainSeqKey, next)
	}
	return next, rows, nil
}

// resumeBlock turns r into the open block after checking that it holds
// exactly the hot batches in rows, which end at next.
func resumeBlock(r *OpenBlock, rows []catalog.HotBatchRow, next uint64, maxEvents int) (*hotBlock, error) {
	switch {
	case r == nil && len(rows) == 0:
		return nil, nil
	case r == nil:
		return nil, fmt.Errorf("%w: hot batches [%d,%d] were not rebuilt into Hot.Resume (design §10.9)",
			ErrInvalidConfig, rows[0].FirstSeq, rows[len(rows)-1].LastSeq)
	case len(r.Batches) != len(rows):
		return nil, fmt.Errorf("%w: Hot.Resume has %d batches; the catalog has %d hot batches", ErrInvalidConfig, len(r.Batches), len(rows))
	case len(r.Events) == 0 || len(r.Events) >= maxEvents:
		// A full block should have been folded.
		return nil, fmt.Errorf("%w: Hot.Resume has %d events; want 1 to %d", ErrInvalidConfig, len(r.Events), maxEvents-1)
	}
	first := rows[0].FirstSeq
	if uint64(len(r.Events)) != next-first {
		return nil, fmt.Errorf("%w: Hot.Resume has %d events; the hot batches hold [%d,%d)", ErrInvalidConfig, len(r.Events), first, next)
	}
	blk := &hotBlock{
		first:    first,
		events:   make([]segment.Event, 0, maxEvents),
		openedAt: r.OpenedAt,
	}
	for i := range r.Events {
		if want := first + uint64(i); r.Events[i].Seq != want {
			return nil, fmt.Errorf("%w: Hot.Resume has seq %d where %d belongs", ErrInvalidConfig, r.Events[i].Seq, want)
		}
	}
	blk.events = append(blk.events, r.Events...)
	want := first
	for i, b := range r.Batches {
		row := rows[i]
		if b.FirstSeq != row.FirstSeq || b.LastSeq != row.LastSeq || b.ObjectID != row.ObjectID {
			return nil, fmt.Errorf("%w: Hot.Resume batch %d is [%d,%d] object %d; the catalog has [%d,%d] object %d",
				ErrInvalidConfig, i, b.FirstSeq, b.LastSeq, b.ObjectID, row.FirstSeq, row.LastSeq, row.ObjectID)
		}
		if b.FirstSeq != want || b.LastSeq < b.FirstSeq || b.LastSeq >= next {
			return nil, catalog.Corruptf(catalog.SourceHotBatch, "hot batch [%d,%d] does not continue at seq %d", b.FirstSeq, b.LastSeq, want)
		}
		want = b.LastSeq + 1
		start, n := int(b.FirstSeq-first), int(b.LastSeq-b.FirstSeq+1)
		blk.batches = append(blk.batches, &hotBatch{
			first:    b.FirstSeq,
			start:    start,
			n:        n,
			class:    b.Class,
			events:   blk.events[start : start+n : start+n],
			objectID: b.ObjectID,
		})
	}
	return blk, nil
}

// rawEventBytes is the batch byte cut's measure of an event: its variable
// fields plus the fixed columns.
func rawEventBytes(ev *segment.Event) int64 {
	return int64(len(ev.DID)+len(ev.Collection)+len(ev.Rkey)+len(ev.Rev)+len(ev.Payload)) + 33
}

func (h *hotWriter) append(ctx context.Context, ev *segment.Event) error {
	if ClassOf(ctx) == ClassBulk {
		evs := []segment.Event{*ev}
		err := h.appendBulk(ctx, evs)
		ev.Seq = evs[0].Seq
		return err
	}
	return h.appendLive(ctx, 1, func(int) *segment.Event { return ev })
}

func (h *hotWriter) appendBatch(ctx context.Context, events []segment.Event) error {
	if ClassOf(ctx) == ClassBulk {
		return h.appendBulk(ctx, events)
	}
	return h.appendLive(ctx, len(events), func(i int) *segment.Event { return &events[i] })
}

// appendLive appends n events under one admission.
func (h *hotWriter) appendLive(ctx context.Context, n int, at func(int) *segment.Event) error {
	h.liveWaiting.Add(1)
	h.mu.Lock()
	h.liveWaiting.Add(-1)
	defer h.mu.Unlock()
	// A bulk appender may be yielding to this one.
	defer h.signalLocked()
	if _, err := h.admitLocked(ctx, ClassLive, nil); err != nil {
		return err
	}
	for i := range n {
		if err := h.appendLocked(ClassLive, at(i)); err != nil {
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
		h.cutLocked(true)
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
	case b.class == ClassBulk:
		if b.n >= h.hot.BulkChunkMaxEvents {
			h.cutLocked(false)
		}
	case b.overflow:
		if b.n >= h.hot.OverflowMaxEvents || b.raw >= h.hot.OverflowMaxBytes {
			h.cutLocked(false)
		}
	case b.n >= h.hot.BatchMaxEvents || b.raw >= h.hot.BatchMaxBytes:
		h.cutLocked(false)
	}
	return nil
}

func (h *hotWriter) usableLocked() error {
	if h.closed {
		return ErrClosed
	}
	return h.err
}

// cutLocked cuts the open batch and picks its storage (design §10.5 rules
// 3-5). A bulk batch and a live overflow batch are pointer batches. A live
// batch commits inline if the token bucket pays for it. Otherwise a batch
// that reached an ordinary cut (events, bytes, age) enters overflow and keeps
// growing to the overflow limits, and one that must freeze now (class change,
// block close, a barrier) or is already at those limits becomes a pointer
// batch.
func (h *hotWriter) cutLocked(forced bool) {
	b := h.batch
	if b == nil {
		return
	}
	pointer := false
	switch {
	case h.hot.Uploader == nil:
	case b.class == ClassBulk || b.overflow:
		pointer = true
	case h.bucket == nil:
	case h.bucket.take(b.raw):
		b.tokens = b.raw
	case !forced && b.n < h.hot.OverflowMaxEvents && b.raw < h.hot.OverflowMaxBytes:
		b.overflow = true
		h.kickAger()
		return
	default:
		// Includes a batch already at the overflow limits (they may be
		// below the ordinary ones): growing it would pass them.
		pointer = true
	}
	h.freezeLocked(pointer)
}

// freezeLocked freezes the open batch (design §10.3). Encoding happens off
// the lock; the prepare value is sampled here so it is tied to exactly this
// batch's events. Only cutLocked calls it.
func (h *hotWriter) freezeLocked(pointer bool) {
	b := h.batch
	h.batch = nil
	b.events = h.block.events[b.start : b.start+b.n : b.start+b.n]
	b.pointer = pointer
	if h.cfg.DurableBatchPrepareValue != nil {
		b.prepareValue = h.cfg.DurableBatchPrepareValue()
	}
	if b.class == ClassBulk {
		b.permit += h.bulkCredit
		h.bulkCredit = 0
		h.bulkFrozen++
	}
	h.addPendingLocked(b.class, b.raw)
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
	h.cutLocked(true)
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
	if b.tokens > 0 {
		h.bucket.settle(b.tokens - int64(len(b.frame)))
	}
	if !b.pointer {
		return
	}
	if err := h.crash(crashpoint.AfterHotBatchCutBeforeUpload); err != nil {
		b.err = err
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
			if err := h.commitBatches(h.group(it.batch)); err != nil {
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

// group waits for b to be ready and takes the consecutive batches behind it
// that are ready too, to commit in one transaction (group commit, design
// §10.5). It never waits for a later batch: the group is what the committer
// can commit now. A batch whose prepare failed ends the group, so the ones
// ahead of it still commit.
func (h *hotWriter) group(b *hotBatch) []*hotBatch {
	<-b.ready
	g := []*hotBatch{b}
	if b.err != nil {
		return g
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for len(g) < h.hot.MaxCommitBatches && len(h.queue) > 0 {
		next := h.queue[0].batch
		if next == nil {
			break
		}
		select {
		case <-next.ready:
		default:
			return g
		}
		if next.err != nil {
			break
		}
		g = append(g, next)
		h.queue[0] = hotItem{}
		h.queue = h.queue[1:]
	}
	return g
}

func (h *hotWriter) hook() DurableBatchHook {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.cfg.OnDurableBatch
}

// commitBatches is design §10.4 for a group of consecutive ready batches:
// one transaction, then, batch by batch in seq order, the hook's callbacks
// and the durable watermark, then the doorbell. The hook runs once per
// batch, each into its own op batch, so every prepare sample reaches it in
// order as it does with one batch per transaction.
func (h *hotWriter) commitBatches(bs []*hotBatch) error {
	for _, b := range bs {
		if b.err != nil {
			return b.err
		}
	}
	for _, b := range bs {
		if b.pointer {
			if err := h.crash(crashpoint.AfterHotBatchUploadBeforeCommit); err != nil {
				return err
			}
		}
	}
	first, last := bs[0], bs[len(bs)-1]
	return obs.Span(h.ctx, func(ctx context.Context) error {
		events := int(last.last() - first.first + 1)
		trace.SpanFromContext(ctx).SetAttributes(
			attribute.Int64("first_seq", int64(first.first)),
			attribute.Int("events", events),
			attribute.Int("batches", len(bs)),
		)
		hbs := make([]catalog.HotBatch, len(bs))
		afterCommit := make([]func(), len(bs))
		var afterDone []func(error)
		var err error
		defer func() {
			for _, done := range afterDone {
				done(err)
			}
		}()
		hook := h.hook()
		for i, b := range bs {
			ops := metastore.NewOpBatch(nil)
			if hook != nil {
				var done func(error)
				var herr error
				afterCommit[i], done, herr = hook(ctx, ops, b.last()+1, false, b.prepareValue)
				if done != nil {
					afterDone = append(afterDone, done)
				}
				if herr != nil {
					err = fmt.Errorf("ingest: on_durable_batch: %w", herr)
					return err
				}
			}
			hbs[i] = catalog.HotBatch{
				FirstSeq:       b.first,
				LastSeq:        b.last(),
				MinWitnessedUS: b.info.MinWitnessedAt,
				MaxWitnessedUS: b.info.MaxWitnessedAt,
				Meta:           ops.Ops(),
			}
			if b.pointer {
				hbs[i].Object = b.ref
			} else {
				hbs[i].Frame = b.frame
			}
		}
		var res []catalog.HotBatchCommit
		if res, err = h.hot.Session.CommitHotBatches(ctx, hbs); err != nil {
			return fmt.Errorf("ingest: commit hot batch [%d,%d]: %w", first.first, last.last(), err)
		}
		rev := res[0].Revision
		trace.SpanFromContext(ctx).SetAttributes(attribute.Int64("revision", int64(rev)))
		if cerr := h.crash(crashpoint.AfterHotBatchCommitBeforeAck); cerr != nil {
			return cerr
		}
		for i, b := range bs {
			b.objectID = res[i].ObjectID
			h.committed(b)
			h.readLog.advanceDurable(b.last() + 1)
			if afterCommit[i] != nil {
				afterCommit[i]()
			}
			h.cfg.Metrics.observeHotBatch(b.class, b.pointer, b.n)
		}
		h.cfg.Metrics.observeHotCommit(len(bs))
		if h.hot.OnCommit != nil {
			h.hot.OnCommit(rev)
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
	last := blk.first + uint64(len(blk.events)) - 1
	if h.hot.Sink == nil {
		h.folded(last)
		return
	}
	cb := ClosedBlock{
		FirstSeq: blk.first,
		LastSeq:  blk.first + uint64(len(blk.events)) - 1,
		Events:   blk.events,
		Batches:  make([]HotBatchInfo, len(blk.batches)),
		OpenedAt: blk.openedAt,
		Folded:   func() { h.folded(last) },
	}
	for i, b := range blk.batches {
		cb.Batches[i] = HotBatchInfo{FirstSeq: b.first, LastSeq: b.last(), Class: b.class, ObjectID: b.objectID}
	}
	h.hot.Sink.BlockClosed(cb)
}

func (h *hotWriter) crash(p crashpoint.Point) error {
	if h.hot.Crash == nil {
		return nil
	}
	return h.hot.Crash.SimulateCrash(h.ctx, p)
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
	h.signalLocked()
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
	if h.batch != nil && !now.Before(h.batchDeadlineLocked()) {
		h.cutLocked(false)
	}
	var deadline time.Time
	ok := false
	if h.block != nil {
		deadline, ok = h.block.openedAt.Add(h.hot.BlockMaxAge), true
	}
	if h.batch != nil {
		if d := h.batchDeadlineLocked(); !ok || d.Before(deadline) {
			deadline, ok = d, true
		}
	}
	return deadline, ok
}

func (h *hotWriter) batchDeadlineLocked() time.Time {
	if h.batch.overflow {
		return h.batch.openedAt.Add(h.hot.OverflowMaxAge)
	}
	return h.batch.openedAt.Add(h.hot.BatchMaxAge)
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
	return h.barrier(ctx, func() { h.cutLocked(true) })
}

func (h *hotWriter) drainDurability(ctx context.Context) error {
	return h.barrier(ctx, func() {
		h.cutLocked(true)
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
		h.cutLocked(true)
		h.enqueueMetaLocked()
	}
	h.closed = true
	h.signalLocked()
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
