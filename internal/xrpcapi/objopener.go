package xrpcapi

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/objstore"
	"github.com/bluesky-social/jetstream/segment"
)

// GenerationSource resolves a sealed segment index to its current
// generation's parts. The disaggregated catalog follower implements it.
type GenerationSource interface {
	GenerationParts(ctx context.Context, idx uint64) (catalog.GenerationParts, bool, error)
}

// ObjectOpener opens sealed segments from the object store (design §11.5):
// each file is virtual, assembled from one pinned generation's header row,
// block objects, and footer object, so no pod keeps a local copy. A
// compaction that replaces the generation after OpenSegment does not change
// what an open file reads: it holds that generation's object IDs, and GC
// keeps their bytes until GC_DELAY after the swap.
type ObjectOpener struct {
	Gens    GenerationSource
	Objects objstore.Store
}

var _ SegmentOpener = ObjectOpener{}

// OpenSegment implements SegmentOpener. The file reads through ctx, so the
// request's deadline bounds every object fetch it makes.
func (o ObjectOpener) OpenSegment(ctx context.Context, idx uint64) (SegmentFile, error) {
	p, ok, err := o.Gens.GenerationParts(ctx, idx)
	if err != nil {
		return nil, fmt.Errorf("xrpcapi: resolve segment %d: %w", idx, err)
	}
	if !ok {
		return nil, ErrSegmentNotFound
	}
	return newObjectSegmentFile(ctx, o.Objects, idx, p)
}

// objectSegmentFile is one generation laid out as its segment file: the
// header, then per block an 8-byte little-endian length and the block
// object, then the footer object.
type objectSegmentFile struct {
	ctx     context.Context
	objects objstore.Store
	hdr     segment.Header
	modTime time.Time
	size    int64
	spans   []span
	// blocks indexes spans by block: blocks[i] is block i's object span.
	blocks []int
}

// span is one contiguous piece of the virtual file: bytes held in memory
// (the header and the length prefixes), or an object read from the store.
type span struct {
	off int64
	n   int64
	mem []byte
	id  uint64
}

func newObjectSegmentFile(ctx context.Context, objects objstore.Store, idx uint64, p catalog.GenerationParts) (*objectSegmentFile, error) {
	if len(p.Header) != segment.ReservedHeaderBytes {
		return nil, fmt.Errorf("xrpcapi: header of segment %d generation %d is %d bytes", idx, p.Generation, len(p.Header))
	}
	hdr, err := segment.ReadSealedHeader(bytes.NewReader(p.Header))
	if err != nil {
		return nil, fmt.Errorf("xrpcapi: header of segment %d generation %d: %w", idx, p.Generation, err)
	}
	f := &objectSegmentFile{
		ctx: ctx, objects: objects, hdr: hdr, modTime: p.CreatedAt,
		spans:  make([]span, 0, 2+2*len(p.Blocks)),
		blocks: make([]int, len(p.Blocks)),
	}
	off := int64(0)
	add := func(s span) {
		s.off = off
		f.spans = append(f.spans, s)
		off += s.n
	}
	add(span{n: int64(len(p.Header)), mem: p.Header})
	for i, b := range p.Blocks {
		if b.Length <= 0 {
			return nil, fmt.Errorf("xrpcapi: segment %d generation %d block %d is %d bytes", idx, p.Generation, i, b.Length)
		}
		add(span{n: 8, mem: binary.LittleEndian.AppendUint64(nil, uint64(b.Length))})
		f.blocks[i] = len(f.spans)
		add(span{n: b.Length, id: b.ID})
	}
	if uint64(off) != hdr.FooterOffset || uint64(len(p.Blocks)) != uint64(hdr.BlockCount) {
		return nil, fmt.Errorf("xrpcapi: segment %d generation %d: %d blocks end at %d, header says %d blocks and footer_offset %d",
			idx, p.Generation, len(p.Blocks), off, hdr.BlockCount, hdr.FooterOffset)
	}
	if p.Footer.Length <= 0 {
		return nil, fmt.Errorf("xrpcapi: segment %d generation %d footer is %d bytes", idx, p.Generation, p.Footer.Length)
	}
	add(span{n: p.Footer.Length, id: p.Footer.ID})
	f.size = off
	return f, nil
}

func (f *objectSegmentFile) Size() int64            { return f.size }
func (f *objectSegmentFile) Header() segment.Header { return f.hdr }
func (f *objectSegmentFile) ModTime() time.Time     { return f.modTime }
func (f *objectSegmentFile) Close() error           { return nil }

// find returns the index of the span holding off, which must be in the file.
func (f *objectSegmentFile) find(off int64) int {
	return sort.Search(len(f.spans), func(i int) bool { return f.spans[i].off+f.spans[i].n > off })
}

// read returns up to n bytes at off from the single span holding off,
// without copying a memory span or a cached object. It fails once the
// request's context ends, even for bytes it could serve from memory, so the
// response cutoff holds for a client reading cached objects.
func (f *objectSegmentFile) read(off, n int64) ([]byte, error) {
	if err := f.ctx.Err(); err != nil {
		return nil, err
	}
	s := f.spans[f.find(off)]
	rel := off - s.off
	n = min(n, s.n-rel)
	if s.mem != nil {
		return s.mem[rel : rel+n], nil
	}
	return f.objects.GetRange(f.ctx, s.id, rel, n)
}

// ReadAt implements io.ReaderAt, splitting reads at span boundaries.
func (f *objectSegmentFile) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("xrpcapi: negative offset")
	}
	done := 0
	for done < len(p) && off < f.size {
		b, err := f.read(off, int64(len(p)-done))
		if err != nil {
			return done, err
		}
		done += copy(p[done:], b)
		off += int64(len(b))
	}
	if done < len(p) {
		return done, io.EOF
	}
	return done, nil
}

// Content returns a sequential reader for http.ServeContent. It reads
// ahead because ServeContent copies in 32 KiB pieces, and a ranged object
// GET per piece would multiply requests to the store. The window grows as a
// read stays sequential, so a small Range request does not fetch megabytes
// it never sends.
func (f *objectSegmentFile) Content() io.ReadSeeker {
	return &objectContent{f: f}
}

const (
	minReadAhead = 256 << 10
	maxReadAhead = 8 << 20
)

type objectContent struct {
	f      *objectSegmentFile
	pos    int64
	buf    []byte
	bufOff int64
	window int64
}

func (c *objectContent) Read(p []byte) (int, error) {
	if c.pos >= c.f.size {
		return 0, io.EOF
	}
	if c.pos < c.bufOff || c.pos >= c.bufOff+int64(len(c.buf)) {
		c.window = min(max(2*c.window, minReadAhead), maxReadAhead)
		b, err := c.f.read(c.pos, max(c.window, int64(len(p))))
		if err != nil {
			return 0, err
		}
		c.buf, c.bufOff = b, c.pos
	}
	n := copy(p, c.buf[c.pos-c.bufOff:])
	c.pos += int64(n)
	return n, nil
}

func (c *objectContent) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += c.pos
	case io.SeekEnd:
		offset += c.f.size
	default:
		return 0, errors.New("xrpcapi: invalid whence")
	}
	if offset < 0 {
		return 0, errors.New("xrpcapi: negative position")
	}
	if offset != c.pos {
		c.window = 0
	}
	c.pos = offset
	return offset, nil
}

// blockFramer is implemented by a SegmentFile that holds each block frame
// as its own object. getBlock serves it directly: HEAD reads nothing, and
// GET one whole object, verified and cacheable, with no footer lookup.
type blockFramer interface {
	BlockFrameSection(i int) *io.SectionReader
	BlockFrame(i int) ([]byte, error)
}

var _ blockFramer = (*objectSegmentFile)(nil)

// BlockFrameSection returns block i's frame as a section of the file; i
// must be in range.
func (f *objectSegmentFile) BlockFrameSection(i int) *io.SectionReader {
	s := f.spans[f.blocks[i]]
	return io.NewSectionReader(f, s.off, s.n)
}

// BlockFrame returns block i's whole, verified frame; i must be in range.
func (f *objectSegmentFile) BlockFrame(i int) ([]byte, error) {
	return f.objects.Get(f.ctx, f.spans[f.blocks[i]].id)
}
