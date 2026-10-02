package maintainer

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/crashpoint"
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
	// Crash is the test-only crash seam injector. Nil in production.
	Crash crashpoint.Injector
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

	// seg is main's active segment, owned by the run goroutine.
	seg *Segment
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
	m.seg, err = OpenSegment(ctx, SegmentConfig{
		Session:         cfg.Session,
		Namespace:       catalog.Main,
		Uploader:        cfg.Uploader,
		Objects:         cfg.Objects,
		Cache:           cfg.Cache,
		MaxSegmentBytes: cfg.MaxSegmentBytes,
		ReadConcurrency: cfg.ReadConcurrency,
		Metrics:         cfg.Metrics,
		Crash:           cfg.Crash,
	})
	if err != nil {
		return nil, err
	}
	go m.run()
	return m, nil
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
	if it.seal {
		return m.seg.Seal(m.ctx)
	}
	return m.rotateIfFull()
}

func (m *Maintainer) rotateIfFull() error {
	return m.seg.RotateIfFull(m.ctx)
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
		if err := m.crash(ctx, crashpoint.AfterFoldUploadBeforeCommit); err != nil {
			return err
		}
		res, err := m.cfg.Session.Fold(ctx, catalog.Block{Namespace: catalog.Main, Info: info, Object: refs[0]})
		if err != nil {
			return fmt.Errorf("maintainer: fold block [%d,%d]: %w", b.FirstSeq, b.LastSeq, err)
		}
		if err := m.seg.Committed(res, refs[0], frame); err != nil {
			return fmt.Errorf("maintainer: fold of [%d,%d]: %w", b.FirstSeq, b.LastSeq, err)
		}
		if b.Folded != nil {
			b.Folded()
		}
		m.cfg.Metrics.observeFold(!refs[0].Pending, time.Since(start))
		return nil
	})
}

func (m *Maintainer) crash(ctx context.Context, p crashpoint.Point) error {
	if m.cfg.Crash == nil {
		return nil
	}
	return m.cfg.Crash.SimulateCrash(ctx, p)
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
