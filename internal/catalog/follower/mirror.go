package follower

import (
	"maps"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/segment"
)

// mirror is one immutable in-memory copy of the catalog (design §11.2).
// A tick builds the next mirror from the previous one plus the rows that
// changed, sharing everything that did not; readers load the current one
// with a single atomic pointer read.
type mirror struct {
	rev uint64
	// refreshed is when the tick that last confirmed this mirror current
	// started: the snapshot it read is at least that new.
	refreshed time.Time

	phase      lifecycle.Phase
	deadline   time.Time
	deadlineOK bool

	// sealed is each namespace's sealed segments. SegmentView.Generation is
	// the segment_generations ID.
	sealed map[catalog.Namespace]catalog.SegmentList
	// gens holds the current generation of every sealed segment by ID.
	gens map[uint64]*generation
	// active is each namespace's active segment and its blocks.
	active map[catalog.Namespace]activeSegment
	// hot is Main's hot batches in first_seq order, with inline frames.
	hot []catalog.HotBatchRow
	// tailLocs is the Locator of each block of each namespace's tail view
	// segment: active blocks, then (Main only) hot batches.
	tailLocs map[catalog.Namespace][]catalog.Locator

	objects objIndex
	view    *catalog.View
}

// activeSegment is one namespace's active segment row and blocks, in
// ordinal order.
type activeSegment struct {
	index  uint64
	blocks []catalog.ActiveBlockRow
}

// generation is one sealed segment generation: everything getSegment and
// getBlock serve, and the footer's block index.
type generation struct {
	id        uint64
	ns        catalog.Namespace
	idx       uint64
	headerRaw []byte
	header    segment.Header
	createdAt time.Time
	footerID  uint64
	footerLen int64
	blockIDs  []uint64
	// blockLens are the blocks' compressed (object) lengths.
	blockLens []int64
	blocks    []segment.BlockInfo
	size      int64
}

func (g *generation) view() catalog.SegmentView {
	return catalog.SegmentView{
		Namespace:  g.ns,
		Index:      g.idx,
		State:      catalog.Sealed,
		Generation: g.id,
		Header:     g.header,
		Blocks:     g.blocks,
		Size:       g.size,
	}
}

// locate implements the View's locator. Sealed segments map to their
// generation's block objects; the tail's blocks were located when the view
// was built.
func (m *mirror) locate(s catalog.SegmentView, i int) catalog.Locator {
	if s.State == catalog.Sealed {
		return catalog.ObjectBlock{ObjectID: m.gens[s.Generation].blockIDs[i]}
	}
	return m.tailLocs[s.Namespace][i]
}

// objIndex maps object IDs to rows for every object the mirror references,
// and possibly some it no longer does. It is a large immutable base shared
// across mirrors plus a small delta a tick copies on write, so a tick costs
// O(new rows) until the delta outgrows maxObjDelta and the next tick
// rebuilds the base from the references alone. A stale row is harmless:
// the object reader re-reads an object's row from the catalog before
// trusting a failed read (§7.5).
type objIndex struct {
	base  map[uint64]catalog.ObjectRow
	delta map[uint64]catalog.ObjectRow
}

const maxObjDelta = 8192

func (x objIndex) get(id uint64) (catalog.ObjectRow, bool) {
	if r, ok := x.delta[id]; ok {
		return r, true
	}
	r, ok := x.base[id]
	return r, ok
}

// with returns an index that also holds rows.
func (x objIndex) with(rows map[uint64]catalog.ObjectRow) objIndex {
	if len(rows) == 0 {
		return x
	}
	delta := make(map[uint64]catalog.ObjectRow, len(x.delta)+len(rows))
	maps.Copy(delta, x.delta)
	maps.Copy(delta, rows)
	return objIndex{base: x.base, delta: delta}
}

// compacted returns an index holding only the rows of ids, when the delta
// has grown past maxObjDelta.
func (x objIndex) compacted(ids func(yield func(uint64) bool)) objIndex {
	if len(x.delta) <= maxObjDelta {
		return x
	}
	base := make(map[uint64]catalog.ObjectRow, len(x.base))
	for id := range ids {
		if r, ok := x.get(id); ok {
			base[id] = r
		}
	}
	return objIndex{base: base}
}

// referencedObjects yields every object ID m references, with repeats.
func (m *mirror) referencedObjects(yield func(uint64) bool) {
	for _, g := range m.gens {
		if !yield(g.footerID) {
			return
		}
		for _, id := range g.blockIDs {
			if !yield(id) {
				return
			}
		}
	}
	for _, a := range m.active {
		for _, b := range a.blocks {
			if !yield(b.ObjectID) {
				return
			}
		}
	}
	for _, h := range m.hot {
		if !h.Inline && !yield(h.ObjectID) {
			return
		}
	}
}
