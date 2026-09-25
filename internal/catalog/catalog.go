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
	"context"
	"errors"
	"fmt"
	"iter"
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

	// Segment and Block are the block's position in its namespace. They stay
	// stable across compaction, which preserves block topology.
	Segment uint64
	Block   int

	// Loc says where the block's zstd frame lives. Fetchers type-switch on
	// it; a Fetcher that does not understand a Locator returns an error.
	Loc Locator
}

// Locator is where a block's frame lives. The set of implementations is
// closed: FileBlock (local mode), ObjectBlock, and InlineBlock.
type Locator interface {
	locator()
}

// FileBlock is a block inside a local segment file. It is a (path, block
// index) handle pinned to a generation: the fetcher checks the file's header
// checksum against Generation before trusting Offset and Length, because a
// compaction rewrite moves blocks within the file.
type FileBlock struct {
	Path       string
	Generation uint64
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

// View is the plain CatalogView implementation shared by catalog backends:
// per-namespace segment lists plus a locator function.
type View struct {
	rev    uint64
	segs   map[Namespace][]SegmentView
	locate func(SegmentView, int) Locator
	// nonEmpty holds, per namespace, the positions in segs of segments with
	// at least one block. Their envelopes are strictly increasing, which is
	// what RefsFrom's binary search needs; an empty segment has no envelope.
	nonEmpty map[Namespace][]int
}

// NewView builds a view from per-namespace segments, which must be in index
// order with increasing, non-overlapping block seq envelopes. locate builds
// the Locator for block i of a segment.
func NewView(rev uint64, segs map[Namespace][]SegmentView, locate func(SegmentView, int) Locator) (*View, error) {
	nonEmpty := make(map[Namespace][]int, len(segs))
	for ns, list := range segs {
		var prevMax uint64
		var havePrev bool
		for i := range list {
			if i > 0 && list[i].Index <= list[i-1].Index {
				return nil, fmt.Errorf("%w: %s segment %d follows %d", ErrOverlap, ns, list[i].Index, list[i-1].Index)
			}
			for _, b := range list[i].Blocks {
				if b.MaxSeq < b.MinSeq || (havePrev && b.MinSeq <= prevMax) {
					return nil, fmt.Errorf("%w: %s segment %d block [%d,%d] after seq %d",
						ErrOverlap, ns, list[i].Index, b.MinSeq, b.MaxSeq, prevMax)
				}
				prevMax, havePrev = b.MaxSeq, true
			}
			if len(list[i].Blocks) > 0 {
				nonEmpty[ns] = append(nonEmpty[ns], i)
			}
		}
	}
	return &View{rev: rev, segs: segs, locate: locate, nonEmpty: nonEmpty}, nil
}

func (v *View) Revision() uint64 { return v.rev }

func (v *View) Segments(ns Namespace) []SegmentView {
	list := v.segs[ns]
	out := make([]SegmentView, len(list))
	copy(out, list)
	return out
}

func (v *View) RefsFrom(ns Namespace, seq uint64) iter.Seq[BlockRef] {
	return func(yield func(BlockRef) bool) {
		list, idx := v.segs[ns], v.nonEmpty[ns]
		start := sort.Search(len(idx), func(i int) bool { return list[idx[i]].MaxSeq() >= seq })
		for _, pos := range idx[start:] {
			s := list[pos]
			first := sort.Search(len(s.Blocks), func(i int) bool { return s.Blocks[i].MaxSeq >= seq })
			for i := first; i < len(s.Blocks); i++ {
				b := s.Blocks[i]
				ref := BlockRef{
					MinSeq:         b.MinSeq,
					MaxSeq:         b.MaxSeq,
					MinWitnessedUS: b.MinWitnessedAt,
					MaxWitnessedUS: b.MaxWitnessedAt,
					Segment:        s.Index,
					Block:          i,
					Loc:            v.locate(s, i),
				}
				if !yield(ref) {
					return
				}
			}
		}
	}
}

func (v *View) TipSeq(ns Namespace) uint64 {
	idx := v.nonEmpty[ns]
	if len(idx) == 0 {
		return 0
	}
	return v.segs[ns][idx[len(idx)-1]].MaxSeq() + 1
}
