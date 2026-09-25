package maintainer

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/objstore"
	"github.com/bluesky-social/jetstream/internal/objstore/objcache"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/segment"
)

const (
	// DefaultMaxSegmentBytes matches local mode's rotation threshold.
	DefaultMaxSegmentBytes int64 = 256 << 20
	// DefaultReadConcurrency bounds the block reads a seal keeps in flight.
	DefaultReadConcurrency = 8
)

// ErrClosed is returned by requests made after Close, or still queued when
// Close ran.
var ErrClosed = errors.New("maintainer: closed")

// Config configures a Maintainer.
type Config struct {
	// Session is the leader session every fold and seal commits in.
	Session *catalog.Session
	// Uploader stores folded blocks and footers (design §7.3).
	Uploader ingest.ObjectUploader
	// Objects reads active blocks back for a seal: protocol.Reader.
	Objects objstore.Store
	// Cache receives every folded block and footer, so a seal usually reads
	// its blocks from memory. It should be the cache Objects reads through.
	// Nil disables it.
	Cache *objcache.Cache

	// MaxEventsPerBlock must match the hot writer's. Zero means
	// segment.DefaultMaxEventsPerBlock.
	MaxEventsPerBlock int
	// MaxSegmentBytes is the rotation threshold on the active segment's
	// framed bytes, Σ(8 + compressed_length), the same quantity local mode
	// compares. Zero means DefaultMaxSegmentBytes.
	MaxSegmentBytes int64
	// ReadConcurrency bounds a seal's block reads in flight. Zero means
	// DefaultReadConcurrency.
	ReadConcurrency int

	Logger  *slog.Logger
	Metrics *Metrics
	// OnFailure is called once with the error that stopped the maintainer.
	// The session has already ended. It runs on the maintainer goroutine and
	// must not call Close; the owner closes the writer (which releases any
	// append blocked on the unfolded cap) and then the maintainer.
	OnFailure func(error)
}

// Maintainer folds and seals main's active segment for one leader session.
// It implements ingest.BlockSink.
type Maintainer struct {
	cfg       Config
	ctx       context.Context
	maxEvents int
	bb        *segment.BlockBuilder // owned by the run goroutine

	mu     sync.Mutex
	queue  []item
	queued int // blocks in queue
	closed bool
	err    error
	wake   chan struct{}
	done   chan struct{}

	// Owned by the run goroutine.
	segment uint64
	blocks  []catalog.SealBlock
	framed  int64
}

var _ ingest.BlockSink = (*Maintainer)(nil)

// item is one queued unit of work: a block to fold, or a request that
// replies on done once everything queued before it has run.
type item struct {
	block *ingest.ClosedBlock
	seal  bool
	done  chan error
}

// Open loads main's active segment and its blocks and starts the maintainer
// goroutine. ctx bounds every fold and seal: cancelling it fails the
// maintainer, so pass the session's context, not a request's.
func Open(ctx context.Context, cfg Config) (*Maintainer, error) {
	switch {
	case cfg.Session == nil || cfg.Uploader == nil || cfg.Objects == nil:
		return nil, errors.New("maintainer: Session, Uploader, and Objects are required")
	case cfg.Logger == nil:
		return nil, errors.New("maintainer: Logger is required")
	case cfg.MaxSegmentBytes < 0 || cfg.ReadConcurrency < 0:
		return nil, errors.New("maintainer: MaxSegmentBytes and ReadConcurrency must not be negative")
	}
	cfg.MaxSegmentBytes = cmp.Or(cfg.MaxSegmentBytes, DefaultMaxSegmentBytes)
	cfg.ReadConcurrency = cmp.Or(cfg.ReadConcurrency, DefaultReadConcurrency)
	bb, err := segment.NewBlockBuilder(cfg.MaxEventsPerBlock)
	if err != nil {
		return nil, fmt.Errorf("maintainer: %w", err)
	}
	m := &Maintainer{
		cfg:       cfg,
		ctx:       ctx,
		maxEvents: bb.Cap(),
		bb:        bb,
		wake:      make(chan struct{}, 1),
		done:      make(chan struct{}),
	}
	if err := m.load(ctx); err != nil {
		return nil, err
	}
	m.cfg.Metrics.setActiveBytes(m.framed)
	go m.run()
	return m, nil
}

// load reads main's active segment and its blocks in one snapshot.
func (m *Maintainer) load(ctx context.Context) error {
	rtx, err := m.cfg.Session.DB().BeginRead(ctx)
	if err != nil {
		return fmt.Errorf("maintainer: load active segment: %w", err)
	}
	defer func() { _ = rtx.Close(ctx) }()
	segs, err := rtx.SegmentsSince(ctx, 0)
	if err != nil {
		return fmt.Errorf("maintainer: load active segment: %w", err)
	}
	found := false
	for _, s := range segs {
		if s.Namespace == catalog.Main && s.State == catalog.Active {
			if found {
				return catalog.Corruptf(catalog.SourceInvariant, "main has active segments %d and %d", m.segment, s.Index)
			}
			m.segment, found = s.Index, true
		}
	}
	if !found {
		return catalog.Corruptf(catalog.SourceInvariant, "main has no active segment")
	}
	rows, err := rtx.ActiveBlocksSince(ctx, 0)
	if err != nil {
		return fmt.Errorf("maintainer: load active blocks: %w", err)
	}
	for _, r := range rows {
		if r.Namespace != catalog.Main || r.Segment != m.segment {
			continue
		}
		if r.Ordinal != len(m.blocks) {
			return catalog.Corruptf(catalog.SourceInvariant, "main segment %d block %d is at ordinal %d",
				m.segment, len(m.blocks), r.Ordinal)
		}
		m.blocks = append(m.blocks, catalog.SealBlock{ObjectID: r.ObjectID, CompressedLength: r.CompressedLength})
		m.framed += 8 + r.CompressedLength
	}
	return nil
}

// BlockClosed implements ingest.BlockSink. It queues the block and returns
// at once. After a failure or Close the block is dropped: its hot batches
// are committed, so the next session rebuilds it.
func (m *Maintainer) BlockClosed(b ingest.ClosedBlock) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.err != nil {
		return
	}
	m.queue = append(m.queue, item{block: &b})
	m.queued++
	m.cfg.Metrics.setQueued(m.queued)
	m.kick()
}

// Rotate implements ingest.BlockSink: once every block queued before it is
// folded, it seals the active segment if it holds any block.
func (m *Maintainer) Rotate(ctx context.Context) error {
	return m.request(ctx, true)
}

// Sync waits until every block queued before it is folded, and any seal
// the rotation rule called for has committed.
func (m *Maintainer) Sync(ctx context.Context) error {
	return m.request(ctx, false)
}

func (m *Maintainer) request(ctx context.Context, seal bool) error {
	done := make(chan error, 1)
	m.mu.Lock()
	switch {
	case m.err != nil:
		err := m.err
		m.mu.Unlock()
		return err
	case m.closed:
		m.mu.Unlock()
		return ErrClosed
	}
	m.queue = append(m.queue, item{seal: seal, done: done})
	m.kick()
	m.mu.Unlock()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close stops the maintainer after the fold or seal in progress, if any.
// Queued blocks are left for the next session's rebuild, and queued
// requests return ErrClosed. It returns the error that failed the
// maintainer, if any.
func (m *Maintainer) Close() error {
	m.mu.Lock()
	m.closed = true
	m.kick()
	m.mu.Unlock()
	<-m.done
	return m.failure()
}

// Err returns the error that failed the maintainer, or nil.
func (m *Maintainer) Err() error { return m.failure() }

func (m *Maintainer) failure() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.err
}

func (m *Maintainer) kick() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func (m *Maintainer) run() {
	defer close(m.done)
	for {
		it, ok := m.dequeue()
		if !ok {
			return
		}
		err := m.failure()
		if err == nil {
			if err = m.handle(it); err != nil {
				m.fail(err)
				err = m.failure()
			}
		}
		if it.done != nil {
			it.done <- err
		}
	}
}

// dequeue returns the next item, or false once closed. Close abandons what
// is still queued.
func (m *Maintainer) dequeue() (item, bool) {
	for {
		m.mu.Lock()
		if m.closed {
			for _, it := range m.queue {
				if it.done != nil {
					it.done <- ErrClosed
				}
			}
			m.queue, m.queued = nil, 0
			m.cfg.Metrics.setQueued(0)
			m.mu.Unlock()
			return item{}, false
		}
		if len(m.queue) > 0 {
			it := m.queue[0]
			m.queue[0] = item{}
			m.queue = m.queue[1:]
			if it.block != nil {
				m.queued--
				m.cfg.Metrics.setQueued(m.queued)
			}
			m.mu.Unlock()
			return it, true
		}
		m.mu.Unlock()
		<-m.wake
	}
}

// handle applies the rotation rule after every fold, and before one too:
// an earlier session may have committed the fold that crossed the threshold
// and ended before its seal.
func (m *Maintainer) handle(it item) error {
	if it.block != nil {
		if err := m.rotateIfFull(); err != nil {
			return err
		}
		if err := m.fold(m.ctx, it.block); err != nil {
			return err
		}
		return m.rotateIfFull()
	}
	if it.seal && len(m.blocks) > 0 {
		return m.seal(m.ctx)
	}
	return m.rotateIfFull()
}

func (m *Maintainer) rotateIfFull() error {
	if len(m.blocks) > 0 && m.framed >= m.cfg.MaxSegmentBytes {
		return m.seal(m.ctx)
	}
	return nil
}

// fail stops the maintainer and ends the session if the failure did not
// already end it.
func (m *Maintainer) fail(err error) {
	m.mu.Lock()
	if m.err != nil {
		m.mu.Unlock()
		return
	}
	m.err = err
	m.mu.Unlock()
	if m.cfg.Session.Err() == nil {
		_ = m.cfg.Session.End("maintainer", err)
	}
	m.cfg.Logger.Error("maintainer failed", "err", err)
	if m.cfg.OnFailure != nil {
		m.cfg.OnFailure(err)
	}
}

// fold is §10.7: encode the block from memory, upload it, and replace its
// hot batches with one active block.
func (m *Maintainer) fold(ctx context.Context, b *ingest.ClosedBlock) error {
	return obs.Span(ctx, func(ctx context.Context) error {
		start := time.Now()
		trace.SpanFromContext(ctx).SetAttributes(
			attribute.Int64("first_seq", int64(b.FirstSeq)),
			attribute.Int64("last_seq", int64(b.LastSeq)))
		frame, info, err := m.encode(b)
		if err != nil {
			return err
		}
		refs, err := m.cfg.Uploader.Upload(ctx, m.cfg.Session, [][]byte{frame})
		if err != nil {
			return fmt.Errorf("maintainer: upload block [%d,%d]: %w", b.FirstSeq, b.LastSeq, err)
		}
		if len(refs) != 1 {
			return fmt.Errorf("maintainer: upload block [%d,%d]: got %d refs", b.FirstSeq, b.LastSeq, len(refs))
		}
		res, err := m.cfg.Session.Fold(ctx, catalog.Block{Namespace: catalog.Main, Info: info, Object: refs[0]})
		if err != nil {
			return fmt.Errorf("maintainer: fold block [%d,%d]: %w", b.FirstSeq, b.LastSeq, err)
		}
		// The fold appended to whatever the catalog holds as active; the
		// seal's block list is built from memory, so the two must agree.
		if res.Segment != m.segment || res.Ordinal != len(m.blocks) {
			return catalog.Corruptf(catalog.SourceInvariant, "fold of [%d,%d] landed at segment %d ordinal %d; maintainer expected %d/%d",
				b.FirstSeq, b.LastSeq, res.Segment, res.Ordinal, m.segment, len(m.blocks))
		}
		m.cfg.Cache.Add(refs[0].SHA256, frame)
		m.blocks = append(m.blocks, catalog.SealBlock{ObjectID: res.ObjectID, CompressedLength: int64(len(frame))})
		m.framed += 8 + int64(len(frame))
		m.cfg.Metrics.setActiveBytes(m.framed)
		if b.Folded != nil {
			b.Folded()
		}
		m.cfg.Metrics.observeFold(!refs[0].Pending, time.Since(start))
		return nil
	})
}

// encode builds the block's frame with the encoder the hot writer uses for
// pointer batches, so a batch that covered the whole block dedups.
func (m *Maintainer) encode(b *ingest.ClosedBlock) ([]byte, segment.BlockInfo, error) {
	n := uint64(len(b.Events))
	if n == 0 || b.FirstSeq == 0 || b.LastSeq-b.FirstSeq+1 != n {
		return nil, segment.BlockInfo{}, fmt.Errorf("maintainer: closed block [%d,%d] has %d events", b.FirstSeq, b.LastSeq, n)
	}
	for i := range b.Events {
		if want := b.FirstSeq + uint64(i); b.Events[i].Seq != want {
			return nil, segment.BlockInfo{}, fmt.Errorf("maintainer: closed block [%d,%d] has seq %d at %d", b.FirstSeq, b.LastSeq, b.Events[i].Seq, want)
		}
		if _, err := m.bb.Append(b.Events[i]); err != nil {
			return nil, segment.BlockInfo{}, fmt.Errorf("maintainer: encode block [%d,%d]: %w", b.FirstSeq, b.LastSeq, err)
		}
	}
	frame, info := m.bb.Encode()
	return frame, info, nil
}

// seal is §10.8: build the header and footer over the active blocks, upload
// the footer, and commit the generation.
func (m *Maintainer) seal(ctx context.Context) error {
	return obs.Span(ctx, func(ctx context.Context) error {
		start := time.Now()
		trace.SpanFromContext(ctx).SetAttributes(
			attribute.Int64("segment", int64(m.segment)),
			attribute.Int("blocks", len(m.blocks)))
		src := newFrameSource(ctx, m.cfg.Objects, m.blocks, m.cfg.ReadConcurrency)
		header, footer, h, err := segment.BuildSealed(src)
		src.close()
		if err != nil {
			return fmt.Errorf("maintainer: build seal of segment %d: %w", m.segment, err)
		}
		if int(h.BlockCount) != len(m.blocks) {
			return catalog.Corruptf(catalog.SourceSeal, "main segment %d footer indexes %d of %d blocks",
				m.segment, h.BlockCount, len(m.blocks))
		}
		refs, err := m.cfg.Uploader.Upload(ctx, m.cfg.Session, [][]byte{footer})
		if err != nil {
			return fmt.Errorf("maintainer: upload footer of segment %d: %w", m.segment, err)
		}
		if len(refs) != 1 {
			return fmt.Errorf("maintainer: upload footer of segment %d: got %d refs", m.segment, len(refs))
		}
		_, err = m.cfg.Session.Seal(ctx, catalog.Seal{
			Namespace: catalog.Main,
			Segment:   m.segment,
			Header:    header,
			Footer:    refs[0],
			Blocks:    m.blocks,
		})
		if err != nil {
			return fmt.Errorf("maintainer: seal segment %d: %w", m.segment, err)
		}
		m.cfg.Cache.Add(refs[0].SHA256, footer)
		m.segment++
		m.blocks = nil
		m.framed = 0
		m.cfg.Metrics.setActiveBytes(0)
		m.cfg.Metrics.observeSeal(time.Since(start))
		return nil
	})
}

// frameSource yields the active blocks' frames in order for BuildSealed,
// keeping up to window reads in flight ahead of it.
type frameSource struct {
	ctx     context.Context
	cancel  context.CancelFunc
	objects objstore.Store
	blocks  []catalog.SealBlock
	window  int
	slots   []chan fetched
	next    int
	started int
	wg      sync.WaitGroup
}

type fetched struct {
	data []byte
	err  error
}

func newFrameSource(ctx context.Context, objects objstore.Store, blocks []catalog.SealBlock, window int) *frameSource {
	ctx, cancel := context.WithCancel(ctx)
	return &frameSource{
		ctx:     ctx,
		cancel:  cancel,
		objects: objects,
		blocks:  blocks,
		window:  window,
		slots:   make([]chan fetched, len(blocks)),
	}
}

func (s *frameSource) NextFrame() ([]byte, error) {
	if s.next == len(s.blocks) {
		return nil, io.EOF
	}
	for s.started < len(s.blocks) && s.started < s.next+s.window {
		i := s.started
		s.started++
		ch := make(chan fetched, 1)
		s.slots[i] = ch
		s.wg.Go(func() {
			data, err := s.objects.Get(s.ctx, s.blocks[i].ObjectID)
			ch <- fetched{data, err}
		})
	}
	i := s.next
	r := <-s.slots[i]
	s.slots[i] = nil
	s.next++
	b := s.blocks[i]
	if r.err != nil {
		return nil, fmt.Errorf("maintainer: read block %d (object %d): %w", i, b.ObjectID, r.err)
	}
	if int64(len(r.data)) != b.CompressedLength {
		return nil, catalog.Corruptf(catalog.SourceRead, "block %d (object %d) is %d bytes; catalog says %d",
			i, b.ObjectID, len(r.data), b.CompressedLength)
	}
	return r.data, nil
}

// close cancels the reads still in flight and waits for them.
func (s *frameSource) close() {
	s.cancel()
	s.wg.Wait()
}
