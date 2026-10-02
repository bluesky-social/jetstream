// Package catalog is the storage-neutral description of the archive: which
// segments exist in each namespace, and where each durable block's bytes live
// (design §11.2, §19.1).
//
// Core read paths (cold reader, cursor resolution, repo export, merge) address
// data through BlockRef and a Fetcher, so they never learn whether a block is
// a byte range of a local segment file or an object in S3. The local
// implementation is catalog/local, which wraps the segment files the ingest
// writer produces. The PostgreSQL catalog and its mirror land in stage 2; the
// writer-side transaction methods of design §19.1 arrive with them (S2.6).
package catalog

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"sort"

	"github.com/bluesky-social/jetstream/segment"
)

// Namespace names an independent sequence of segments. Seqs are monotonic
// within a namespace, not across namespaces.
type Namespace string

const (
	// Main is the archive served to clients: segments/ in local mode.
	Main Namespace = "main"
	// BootstrapLive holds live events captured during bootstrap until merge
	// folds them into Main: backfill/live_segments/ in local mode.
	BootstrapLive Namespace = "bootstrap_live"
)

// Namespaces lists every namespace, in a stable order.
var Namespaces = []Namespace{Main, BootstrapLive}

// Valid reports whether ns is a known namespace.
func (ns Namespace) Valid() bool {
	return ns == Main || ns == BootstrapLive
}

// SegmentState is a segment's lifecycle state.
type SegmentState uint8

const (
	// Active segments accept appends. Only their flushed blocks are visible.
	Active SegmentState = iota + 1
	// Sealed segments are immutable apart from compaction, which publishes a
	// new generation.
	Sealed
)

func (s SegmentState) String() string {
	switch s {
	case Active:
		return "active"
	case Sealed:
		return "sealed"
	default:
		return fmt.Sprintf("SegmentState(%d)", uint8(s))
	}
}

// SegmentView is one segment's current generation as seen by a CatalogView.
// Values are immutable once published; Blocks must not be modified.
type SegmentView struct {
	Namespace Namespace
	Index     uint64
	State     SegmentState

	// Generation identifies the bytes this view describes. A fetch through a
	// ref built from an older generation fails with ErrStaleRef. Local mode
	// uses the sealed header checksum, and zero for an active segment (whose
	// header checksum is still zero on disk).
	Generation uint64

	// Header is the sealed header. Zero for an active segment.
	Header segment.Header

	// Blocks is the durable block index: the footer's for a sealed segment,
	// the flushed blocks for an active one. Offsets are file offsets of each
	// block's 8-byte length prefix, as in segment.BlockInfo.
	Blocks []segment.BlockInfo

	// Size is the sealed file size in bytes (footer offset plus footer
	// length). Zero for an active segment.
	Size int64
}

// MinSeq returns the smallest seq the segment's blocks cover, or 0 if it has
// no blocks.
func (v SegmentView) MinSeq() uint64 {
	if len(v.Blocks) == 0 {
		return 0
	}
	return v.Blocks[0].MinSeq
}

// MaxSeq returns the largest seq the segment's blocks cover, or 0 if it has
// no blocks.
func (v SegmentView) MaxSeq() uint64 {
	if len(v.Blocks) == 0 {
		return 0
	}
	return v.Blocks[len(v.Blocks)-1].MaxSeq
}

// BlockRef addresses one durable block (design §11.2). MinSeq..MaxSeq and the
// witnessed range are the block's historical envelope: compaction may drop
// rows inside it but never widens or shifts it.
type BlockRef struct {
	MinSeq, MaxSeq                 uint64
	MinWitnessedUS, MaxWitnessedUS int64

	// Namespace, Segment, and Block are the block's position. They stay
	// stable across compaction, which preserves block topology.
	Namespace Namespace
	Segment   uint64
	Block     int

	// Generation is the segment generation the ref was built from (see
	// SegmentView.Generation). Together with the position it names the
	// block's bytes exactly, which is what a decoded-block cache keys on.
	Generation uint64

	// Loc says where the block's zstd frame lives. Fetchers type-switch on
	// it; a Fetcher that does not understand a Locator returns an error.
	Loc Locator
}

// Locator is where a block's frame lives. The set of implementations is
// closed: FileBlock (local mode), ObjectBlock, and InlineBlock.
type Locator interface {
	locator()
}

// FileBlock is a block inside a local segment file. With the ref's
// position it is a (path, block index) handle pinned to the ref's
// Generation: the fetcher checks the file's header checksum against it
// before trusting Offset and Length, because a compaction rewrite moves
// blocks within the file.
type FileBlock struct {
	Path string
	// Offset is the file offset of the block's 8-byte length prefix.
	Offset uint64
	// Length is the zstd frame length, excluding the prefix.
	Length uint32
}

// ObjectBlock is a block stored as one object in the object store: a sealed
// block, an active block, or a pointer hot batch.
type ObjectBlock struct {
	ObjectID uint64
}

// InlineBlock is a hot batch whose frame is held inline in the catalog.
type InlineBlock struct {
	Frame []byte
}

func (FileBlock) locator()   {}
func (ObjectBlock) locator() {}
func (InlineBlock) locator() {}

// ErrStaleRef means a ref's generation is gone (compaction, fold, or a seal
// of the active segment it pointed into). The caller should take a fresh
// view and ask RefsFrom again from the seq it had reached.
var ErrStaleRef = errors.New("catalog: stale block ref")

// Fetcher returns a block's zstd frame, without the 8-byte length prefix of
// the segment file layout, ready for segment.DecodeBlockFrame.
type Fetcher interface {
	Fetch(ctx context.Context, ref BlockRef) ([]byte, error)
}

// DecodeRef fetches and decodes one block.
func DecodeRef(ctx context.Context, f Fetcher, ref BlockRef) ([]segment.Event, error) {
	frame, err := f.Fetch(ctx, ref)
	if err != nil {
		return nil, err
	}
	return segment.DecodeBlockFrame(frame)
}

// CatalogView is an immutable snapshot of the catalog. Reads through one view
// are mutually consistent; a later view may add, seal, or replace segments.
type CatalogView interface {
	// Revision increases whenever the set of segments or a segment's
	// generation changes. Local mode does not bump it for active-block
	// flushes, which are sampled when the view is taken.
	Revision() uint64
	// Segments returns ns's segments in index order: sealed segments, then
	// at most one active segment.
	Segments(ns Namespace) []SegmentView
	// RefsFrom yields every ref in ns from the first block whose MaxSeq is
	// at least seq, up to the newest durable block, in seq order. It is lazy
	// so a reader that stops after a batch does not pay for the rest of the
	// archive.
	RefsFrom(ns Namespace, seq uint64) iter.Seq[BlockRef]
	// TipSeq is one past the newest seq any ref in ns covers, or 0 when ns
	// holds no blocks.
	TipSeq(ns Namespace) uint64
}

// Catalog is the reader-facing handle on the archive. Refresh brings the next
// Snapshot up to date with changes the catalog was not told about directly.
type Catalog interface {
	Snapshot() CatalogView
	Refresh(ctx context.Context) error
}

// ActiveSource reports the durable part of a namespace's active segment. The
// ingest writer implements it, reporting the segment and its flushed blocks
// under the writer lock so the two are coherent.
type ActiveSource interface {
	ActiveSegment() (SegmentView, bool)
}

// ErrOverlap means two segments' seq envelopes overlap or are out of index
// order, so no ref ordering could be exact.
var ErrOverlap = errors.New("catalog: segment seq envelopes overlap")

// SegmentList is one namespace's segments in index order, validated to have
// increasing, non-overlapping block seq envelopes. It is immutable: Put and
// Delete return a new list and leave the receiver, and any view built from
// it, untouched. Backends keep their sealed segments in a SegmentList so a
// snapshot shares it instead of re-validating the whole archive.
type SegmentList struct {
	segs []SegmentView
	// nonEmpty holds the positions in segs of segments with at least one
	// block. Their envelopes are strictly increasing, which is what
	// RefsFrom's binary search needs; an empty segment has no envelope.
	nonEmpty []int
}

// NewSegmentList validates segs and returns them as a list. It takes
// ownership of segs.
func NewSegmentList(segs []SegmentView) (SegmentList, error) {
	for i := range segs {
		if err := checkBlocks(segs[i]); err != nil {
			return SegmentList{}, err
		}
	}
	return buildList(segs)
}

// checkBlocks checks that v's own block envelopes are increasing.
func checkBlocks(v SegmentView) error {
	for i, b := range v.Blocks {
		if b.MaxSeq < b.MinSeq || (i > 0 && b.MinSeq <= v.Blocks[i-1].MaxSeq) {
			return fmt.Errorf("%w: %s segment %d block %d [%d,%d]", ErrOverlap, v.Namespace, v.Index, i, b.MinSeq, b.MaxSeq)
		}
	}
	return nil
}

// buildList checks index order and segment envelopes, given segments whose
// blocks already passed checkBlocks.
func buildList(segs []SegmentView) (SegmentList, error) {
	var nonEmpty []int
	for i := range segs {
		if i > 0 && segs[i].Index <= segs[i-1].Index {
			return SegmentList{}, fmt.Errorf("%w: %s segment %d follows %d", ErrOverlap, segs[i].Namespace, segs[i].Index, segs[i-1].Index)
		}
		if len(segs[i].Blocks) == 0 {
			continue
		}
		if n := len(nonEmpty); n > 0 {
			if prev := segs[nonEmpty[n-1]]; segs[i].MinSeq() <= prev.MaxSeq() {
				return SegmentList{}, fmt.Errorf("%w: %s segment %d starts at seq %d, segment %d ends at %d",
					ErrOverlap, segs[i].Namespace, segs[i].Index, segs[i].MinSeq(), prev.Index, prev.MaxSeq())
			}
		}
		nonEmpty = append(nonEmpty, i)
	}
	return SegmentList{segs: segs, nonEmpty: nonEmpty}, nil
}

// Segments returns the list's segments. The slice is shared and must not be
// modified.
func (l SegmentList) Segments() []SegmentView { return l.segs }

// Len returns the number of segments.
func (l SegmentList) Len() int { return len(l.segs) }

// Last returns the segment with the highest index.
func (l SegmentList) Last() (SegmentView, bool) {
	if len(l.segs) == 0 {
		return SegmentView{}, false
	}
	return l.segs[len(l.segs)-1], true
}

func (l SegmentList) find(idx uint64) (int, bool) {
	return sort.Find(len(l.segs), func(i int) int { return cmp.Compare(idx, l.segs[i].Index) })
}

// Get returns segment idx.
func (l SegmentList) Get(idx uint64) (SegmentView, bool) {
	if i, ok := l.find(idx); ok {
		return l.segs[i], true
	}
	return SegmentView{}, false
}

// Put returns a list with v inserted, or replacing the segment with v's
// index. It checks v's blocks and the envelopes around it, not the whole
// list again.
func (l SegmentList) Put(v SegmentView) (SegmentList, error) {
	if err := checkBlocks(v); err != nil {
		return SegmentList{}, err
	}
	i, found := l.find(v.Index)
	segs := make([]SegmentView, 0, len(l.segs)+1)
	segs = append(segs, l.segs[:i]...)
	segs = append(segs, v)
	if found {
		i++
	}
	segs = append(segs, l.segs[i:]...)
	return buildList(segs)
}

// Delete returns a list without segment idx.
func (l SegmentList) Delete(idx uint64) SegmentList {
	i, found := l.find(idx)
	if !found {
		return l
	}
	out, err := buildList(slices.Delete(slices.Clone(l.segs), i, i+1))
	if err != nil {
		// Removing a segment cannot create an overlap.
		panic(fmt.Sprintf("catalog: %v", err))
	}
	return out
}

// View is the plain CatalogView implementation shared by catalog backends:
// per namespace, a sealed SegmentList and optionally the active segment
// after it, plus a locator function.
type View struct {
	rev    uint64
	ns     map[Namespace]nsView
	locate func(SegmentView, int) Locator
}

type nsView struct {
	sealed  SegmentList
	tail    SegmentView
	hasTail bool
}

// NewView builds a view from per-namespace segment lists and optional tail
// segments (each namespace's active segment). A tail must have a higher
// index than its list's last segment and blocks above the list's last
// block. Building a view validates only the tails, so its cost does not grow
// with the archive. locate builds the Locator for block i of a segment.
func NewView(rev uint64, lists map[Namespace]SegmentList, tails map[Namespace]SegmentView, locate func(SegmentView, int) Locator) (*View, error) {
	out := make(map[Namespace]nsView, len(lists)+len(tails))
	for ns, l := range lists {
		out[ns] = nsView{sealed: l}
	}
	for ns, tail := range tails {
		if err := checkBlocks(tail); err != nil {
			return nil, err
		}
		nv := out[ns]
		if last, ok := nv.sealed.Last(); ok && tail.Index <= last.Index {
			return nil, fmt.Errorf("%w: %s tail segment %d follows %d", ErrOverlap, ns, tail.Index, last.Index)
		}
		if n := len(nv.sealed.nonEmpty); n > 0 && len(tail.Blocks) > 0 {
			if prev := nv.sealed.segs[nv.sealed.nonEmpty[n-1]]; tail.MinSeq() <= prev.MaxSeq() {
				return nil, fmt.Errorf("%w: %s tail segment %d starts at seq %d, segment %d ends at %d",
					ErrOverlap, ns, tail.Index, tail.MinSeq(), prev.Index, prev.MaxSeq())
			}
		}
		nv.tail, nv.hasTail = tail, true
		out[ns] = nv
	}
	return &View{rev: rev, ns: out, locate: locate}, nil
}

func (v *View) Revision() uint64 { return v.rev }

func (v *View) Segments(ns Namespace) []SegmentView {
	nv := v.ns[ns]
	out := make([]SegmentView, 0, nv.sealed.Len()+1)
	out = append(out, nv.sealed.segs...)
	if nv.hasTail {
		out = append(out, nv.tail)
	}
	return out
}

func (v *View) RefsFrom(ns Namespace, seq uint64) iter.Seq[BlockRef] {
	return func(yield func(BlockRef) bool) {
		nv := v.ns[ns]
		list, idx := nv.sealed.segs, nv.sealed.nonEmpty
		start := sort.Search(len(idx), func(i int) bool { return list[idx[i]].MaxSeq() >= seq })
		for _, pos := range idx[start:] {
			if !v.yieldFrom(list[pos], seq, yield) {
				return
			}
		}
		if nv.hasTail {
			v.yieldFrom(nv.tail, seq, yield)
		}
	}
}

// yieldFrom yields s's refs from the first block reaching seq, and reports
// whether the consumer wants more.
func (v *View) yieldFrom(s SegmentView, seq uint64, yield func(BlockRef) bool) bool {
	first := sort.Search(len(s.Blocks), func(i int) bool { return s.Blocks[i].MaxSeq >= seq })
	for i := first; i < len(s.Blocks); i++ {
		b := s.Blocks[i]
		ref := BlockRef{
			MinSeq:         b.MinSeq,
			MaxSeq:         b.MaxSeq,
			MinWitnessedUS: b.MinWitnessedAt,
			MaxWitnessedUS: b.MaxWitnessedAt,
			Namespace:      s.Namespace,
			Segment:        s.Index,
			Generation:     s.Generation,
			Block:          i,
			Loc:            v.locate(s, i),
		}
		if !yield(ref) {
			return false
		}
	}
	return true
}

func (v *View) TipSeq(ns Namespace) uint64 {
	nv := v.ns[ns]
	if nv.hasTail && len(nv.tail.Blocks) > 0 {
		return nv.tail.MaxSeq() + 1
	}
	if n := len(nv.sealed.nonEmpty); n > 0 {
		return nv.sealed.segs[nv.sealed.nonEmpty[n-1]].MaxSeq() + 1
	}
	return 0
}
