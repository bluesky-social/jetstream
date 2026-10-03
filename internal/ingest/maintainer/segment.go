package maintainer

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
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

// SegmentConfig configures a Segment.
type SegmentConfig struct {
	// Session is the leader session every seal commits in.
	Session *catalog.Session
	// Namespace is the segment's namespace. Empty means catalog.Main.
	Namespace catalog.Namespace
	// Uploader stores footers (design §7.3).
	Uploader ingest.ObjectUploader
	// Objects reads back, for a seal, the active blocks an earlier session
	// committed: protocol.Reader.
	Objects objstore.Store
	// Cache receives every committed block and footer. Nil disables it.
	Cache *objcache.Cache
	// MaxSegmentBytes is the rotation threshold on the framed bytes. Zero
	// means DefaultMaxSegmentBytes.
	MaxSegmentBytes int64
	// ReadConcurrency bounds a seal's block reads in flight. Zero means
	// DefaultReadConcurrency.
	ReadConcurrency int
	// Metrics are main's; pass nil for bootstrap_live, whose seals would
	// otherwise move main's active-segment gauge.
	Metrics *Metrics
	// Crash is the test-only crash seam injector. Nil in production.
	Crash crashpoint.Injector
}

// Segment is one namespace's active segment as a leader session sees it:
// its index, its committed blocks, and their framed size, loaded once at
// session start and kept in step with every block the session commits. It
// applies the rotation rule and seals (design §10.8). The maintainer uses
// it for main in hot mode, and the direct writer for either namespace in
// direct mode (§10.6), so both seal the same way.
//
// It indexes each block the session commits for the footer as the block
// commits, off the owner's goroutine. A seal then decodes only the blocks
// an earlier session committed, rather than all of a 256MiB segment while
// every commit waits behind it. The indexes hold each block's distinct DIDs
// until the seal, about 200MiB for a full segment of live traffic: no more
// than a seal holds at once anyway, only for longer.
//
// A Segment is not safe for concurrent use. One goroutine owns it: the
// maintainer's, or the direct writer's committer. Its methods do not end
// the session on failure; the owner does.
type Segment struct {
	cfg SegmentConfig

	index  uint64
	blocks []catalog.SealBlock
	framed int64
	// indexes are the footer indexes of the blocks this session committed,
	// the last len(indexes) of blocks.
	indexes []*blockIndex
}

// blockIndex is one committed block's footer index, computed in the
// background.
type blockIndex struct {
	done chan struct{}
	// Set before done closes.
	ix  *segment.BlockIndex
	err error
}

func indexBlock(frame []byte) *blockIndex {
	b := &blockIndex{done: make(chan struct{})}
	go func() {
		defer close(b.done)
		b.ix, b.err = segment.IndexBlock(frame)
	}()
	return b
}

var _ ingest.SegmentSealer = (*Segment)(nil)

// OpenSegment loads the namespace's active segment and its blocks.
func OpenSegment(ctx context.Context, cfg SegmentConfig) (*Segment, error) {
	switch {
	case cfg.Session == nil || cfg.Uploader == nil || cfg.Objects == nil:
		return nil, errors.New("maintainer: Session, Uploader, and Objects are required")
	case cfg.MaxSegmentBytes < 0 || cfg.ReadConcurrency < 0:
		return nil, errors.New("maintainer: MaxSegmentBytes and ReadConcurrency must not be negative")
	}
	cfg.Namespace = cmp.Or(cfg.Namespace, catalog.Main)
	if !cfg.Namespace.Valid() {
		return nil, fmt.Errorf("maintainer: unknown namespace %q", cfg.Namespace)
	}
	cfg.MaxSegmentBytes = cmp.Or(cfg.MaxSegmentBytes, DefaultMaxSegmentBytes)
	cfg.ReadConcurrency = cmp.Or(cfg.ReadConcurrency, DefaultReadConcurrency)
	s := &Segment{cfg: cfg}
	if err := s.load(ctx); err != nil {
		return nil, err
	}
	s.cfg.Metrics.setActiveBytes(s.framed)
	return s, nil
}

// load reads the active segment and its blocks in one snapshot.
func (s *Segment) load(ctx context.Context) error {
	ns := s.cfg.Namespace
	rtx, err := s.cfg.Session.DB().BeginRead(ctx)
	if err != nil {
		return fmt.Errorf("maintainer: load %s active segment: %w", ns, err)
	}
	defer func() { _ = rtx.Close(ctx) }()
	segs, err := rtx.SegmentsSince(ctx, 0)
	if err != nil {
		return fmt.Errorf("maintainer: load %s active segment: %w", ns, err)
	}
	found := false
	for _, seg := range segs {
		if seg.Namespace == ns && seg.State == catalog.Active {
			if found {
				return catalog.Corruptf(catalog.SourceInvariant, "%s has active segments %d and %d", ns, s.index, seg.Index)
			}
			s.index, found = seg.Index, true
		}
	}
	if !found {
		return catalog.Corruptf(catalog.SourceInvariant, "%s has no active segment", ns)
	}
	rows, err := rtx.ActiveBlocksSince(ctx, 0)
	if err != nil {
		return fmt.Errorf("maintainer: load %s active blocks: %w", ns, err)
	}
	for _, r := range rows {
		if r.Namespace != ns || r.Segment != s.index {
			continue
		}
		if r.Ordinal != len(s.blocks) {
			return catalog.Corruptf(catalog.SourceInvariant, "%s segment %d block %d is at ordinal %d",
				ns, s.index, len(s.blocks), r.Ordinal)
		}
		s.blocks = append(s.blocks, catalog.SealBlock{ObjectID: r.ObjectID, CompressedLength: r.CompressedLength})
		s.framed += 8 + r.CompressedLength
	}
	return nil
}

// Index is the active segment's index.
func (s *Segment) Index() uint64 { return s.index }

// Blocks is the number of blocks in the active segment.
func (s *Segment) Blocks() int { return len(s.blocks) }

// FramedBytes is the active segment's framed bytes, Σ(8 + compressed_length).
func (s *Segment) FramedBytes() int64 { return s.framed }

// Committed implements ingest.SegmentSealer: it records a block the session
// committed to the active segment, from ref's upload of frame. The commit
// appended to whatever the catalog holds as active; a seal's block list is
// built from memory, so the two must agree. The Segment keeps frame, which
// the caller must not modify.
func (s *Segment) Committed(c catalog.BlockCommit, ref catalog.ObjectRef, frame []byte) error {
	if c.Segment != s.index || c.Ordinal != len(s.blocks) {
		return catalog.Corruptf(catalog.SourceInvariant, "%s block landed at segment %d ordinal %d; expected %d/%d",
			s.cfg.Namespace, c.Segment, c.Ordinal, s.index, len(s.blocks))
	}
	s.cfg.Cache.Add(ref.SHA256, frame)
	s.blocks = append(s.blocks, catalog.SealBlock{ObjectID: c.ObjectID, CompressedLength: int64(len(frame))})
	s.indexes = append(s.indexes, indexBlock(frame))
	s.framed += 8 + int64(len(frame))
	s.cfg.Metrics.setActiveBytes(s.framed)
	return nil
}

// RotateIfFull implements ingest.SegmentSealer: it seals once the framed
// bytes reach MaxSegmentBytes, local mode's rotation rule.
func (s *Segment) RotateIfFull(ctx context.Context) error {
	if len(s.blocks) > 0 && s.framed >= s.cfg.MaxSegmentBytes {
		return s.Seal(ctx)
	}
	return nil
}

// Seal implements ingest.SegmentSealer. It is §10.8: build the header and
// footer over the active blocks, upload the footer, and commit the
// generation. It does nothing when the active segment has no blocks.
func (s *Segment) Seal(ctx context.Context) error {
	if len(s.blocks) == 0 {
		return nil
	}
	ns := s.cfg.Namespace
	return obs.Span(ctx, func(ctx context.Context) error {
		start := time.Now()
		readBack := len(s.blocks) - len(s.indexes)
		trace.SpanFromContext(ctx).SetAttributes(
			attribute.String("namespace", string(ns)),
			attribute.Int64("segment", int64(s.index)),
			attribute.Int("blocks", len(s.blocks)),
			attribute.Int("read_back", readBack))
		header, footer, h, err := s.build(ctx, readBack)
		if err != nil {
			return fmt.Errorf("maintainer: build seal of %s segment %d: %w", ns, s.index, err)
		}
		if int(h.BlockCount) != len(s.blocks) {
			return catalog.Corruptf(catalog.SourceSeal, "%s segment %d footer indexes %d of %d blocks",
				ns, s.index, h.BlockCount, len(s.blocks))
		}
		refs, err := s.cfg.Uploader.Upload(ctx, s.cfg.Session, [][]byte{footer})
		if err != nil {
			return fmt.Errorf("maintainer: upload footer of %s segment %d: %w", ns, s.index, err)
		}
		if len(refs) != 1 {
			return fmt.Errorf("maintainer: upload footer of %s segment %d: got %d refs", ns, s.index, len(refs))
		}
		if s.cfg.Crash != nil {
			if err := s.cfg.Crash.SimulateCrash(ctx, crashpoint.AfterSealFooterUploadBeforeCommit); err != nil {
				return err
			}
		}
		_, err = s.cfg.Session.Seal(ctx, catalog.Seal{
			Namespace: ns,
			Segment:   s.index,
			Header:    header,
			Footer:    refs[0],
			Blocks:    s.blocks,
		})
		if err != nil {
			return fmt.Errorf("maintainer: seal %s segment %d: %w", ns, s.index, err)
		}
		s.cfg.Cache.Add(refs[0].SHA256, footer)
		s.index++
		s.blocks = nil
		s.indexes = nil
		s.framed = 0
		s.cfg.Metrics.setActiveBytes(0)
		s.cfg.Metrics.observeSeal(time.Since(start))
		return nil
	})
}

// build computes the sealed header and footer: from the first readBack
// blocks, which an earlier session committed, read back from the object
// store, then from the indexes of the blocks this session committed.
func (s *Segment) build(ctx context.Context, readBack int) (header, footer []byte, h segment.Header, err error) {
	b := segment.NewSealBuilder()
	if readBack > 0 {
		src := newFrameSource(ctx, s.cfg.Objects, s.blocks[:readBack], s.cfg.ReadConcurrency)
		defer src.close()
		for {
			frame, err := src.NextFrame()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, nil, segment.Header{}, err
			}
			if err := b.AddFrame(frame); err != nil {
				return nil, nil, segment.Header{}, err
			}
		}
	}
	for i, bi := range s.indexes {
		<-bi.done
		if bi.err != nil {
			n := readBack + i
			return nil, nil, segment.Header{}, fmt.Errorf("maintainer: index block %d (object %d): %w", n, s.blocks[n].ObjectID, bi.err)
		}
		if err := b.Add(bi.ix); err != nil {
			return nil, nil, segment.Header{}, err
		}
	}
	return b.Finish()
}

// frameSource yields active blocks' frames in order for a seal, keeping up
// to window reads in flight ahead of it.
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
