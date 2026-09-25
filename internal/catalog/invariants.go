package catalog

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/bluesky-social/jetstream/segment"
)

// Snapshot is a whole-catalog dump, read in one ReadTx, that
// CheckInvariants runs over. It holds the objects the catalog references,
// not every object row.
type Snapshot struct {
	Archive          ArchiveRow
	Segments         []SegmentRow
	Generations      map[uint64]GenerationRow
	GenerationBlocks map[uint64][]GenerationBlockRow
	ActiveBlocks     []ActiveBlockRow
	HotBatches       []HotBatchRow
	Objects          map[uint64]ObjectRow
	// Meta holds the metadata keys the invariants read (SnapshotMetaKeys).
	Meta map[string][]byte
}

// RelayCursorKey is the live consumer's durable relay cursor. The catalog
// does not decode it; invariant 7 is checked by a caller-supplied function.
const RelayCursorKey = "relay/cursor"

// SnapshotMetaKeys are the metadata keys LoadSnapshot reads.
var SnapshotMetaKeys = []string{MainSeqKey, BootstrapLiveSeqKey, RelayCursorKey}

// LoadSnapshot reads the whole catalog through rtx. It loads frames of no
// hot batch.
func LoadSnapshot(ctx context.Context, rtx ReadTx) (*Snapshot, error) {
	return LoadSnapshotFrames(ctx, rtx, ^uint64(0))
}

// LoadSnapshotFrames is LoadSnapshot, loading the inline frames of the hot
// batches whose first seq is at least framesFrom.
func LoadSnapshotFrames(ctx context.Context, rtx ReadTx, framesFrom uint64) (*Snapshot, error) {
	s := &Snapshot{Generations: map[uint64]GenerationRow{}, GenerationBlocks: map[uint64][]GenerationBlockRow{}}
	var err error
	if s.Archive, err = rtx.Archive(ctx); err != nil {
		return nil, err
	}
	if s.Segments, err = rtx.SegmentsSince(ctx, 0); err != nil {
		return nil, err
	}
	var genIDs []uint64
	for _, seg := range s.Segments {
		if seg.GenerationID != 0 {
			genIDs = append(genIDs, seg.GenerationID)
		}
	}
	gens, err := rtx.Generations(ctx, genIDs)
	if err != nil {
		return nil, err
	}
	for _, g := range gens {
		s.Generations[g.ID] = g
	}
	gbs, err := rtx.GenerationBlocks(ctx, genIDs)
	if err != nil {
		return nil, err
	}
	for _, gb := range gbs {
		s.GenerationBlocks[gb.GenerationID] = append(s.GenerationBlocks[gb.GenerationID], gb)
	}
	if s.ActiveBlocks, err = rtx.ActiveBlocksSince(ctx, 0); err != nil {
		return nil, err
	}
	if s.HotBatches, err = rtx.HotBatches(ctx, framesFrom); err != nil {
		return nil, err
	}
	objs, err := rtx.Objects(ctx, s.referencedObjects())
	if err != nil {
		return nil, err
	}
	s.Objects = make(map[uint64]ObjectRow, len(objs))
	for _, o := range objs {
		s.Objects[o.ID] = o
	}
	keys := make([][]byte, len(SnapshotMetaKeys))
	for i, k := range SnapshotMetaKeys {
		keys[i] = []byte(k)
	}
	if s.Meta, err = rtx.MetaGet(ctx, keys); err != nil {
		return nil, err
	}
	return s, nil
}

// referencedObjects lists every object ID a catalog row references, sorted
// and deduplicated.
func (s *Snapshot) referencedObjects() []uint64 {
	var ids []uint64
	for _, g := range s.Generations {
		ids = append(ids, g.FooterObjectID)
	}
	for _, gbs := range s.GenerationBlocks {
		for _, gb := range gbs {
			ids = append(ids, gb.ObjectID)
		}
	}
	for _, b := range s.ActiveBlocks {
		ids = append(ids, b.ObjectID)
	}
	for _, h := range s.HotBatches {
		if h.ObjectID != 0 {
			ids = append(ids, h.ObjectID)
		}
	}
	slices.Sort(ids)
	return slices.Compact(ids)
}

// InvariantOptions tunes CheckInvariants.
type InvariantOptions struct {
	// MaxEventsPerBlock bounds a hot batch (invariant 6). Zero means
	// segment.DefaultMaxEventsPerBlock.
	MaxEventsPerBlock int
	// RelayCursor checks invariant 7 against the stored relay/cursor value
	// (nil when absent). The catalog cannot know which upstream seqs are
	// fully committed, because the segment format does not keep an event's
	// upstream seq; a test model can (storagefake.RelayWatch). Nil skips it.
	RelayCursor func(s *Snapshot, value []byte) error
	// Cheap skips the checks that decode every generation header, for the
	// leader's session-start check on a large archive.
	Cheap bool
}

// CheckInvariants checks the design §9.3 invariants over s. It returns a
// CorruptionError naming the first broken invariant.
func CheckInvariants(s *Snapshot, opts InvariantOptions) error {
	if opts.MaxEventsPerBlock == 0 {
		opts.MaxEventsPerBlock = segment.DefaultMaxEventsPerBlock
	}
	if err := checkObjects(s); err != nil {
		return err
	}
	for _, ns := range Namespaces {
		if err := checkNamespace(s, ns, opts); err != nil {
			return err
		}
	}
	if opts.RelayCursor != nil {
		if err := opts.RelayCursor(s, s.Meta[RelayCursorKey]); err != nil {
			return Corruptf(SourceInvariant, "invariant 7: %v", err)
		}
	}
	return nil
}

// checkObjects is invariant 4: every referenced object is available.
func checkObjects(s *Snapshot) error {
	for _, id := range s.referencedObjects() {
		o, ok := s.Objects[id]
		if !ok {
			return Corruptf(SourceInvariant, "invariant 4: referenced object %d has no row", id)
		}
		if o.State != ObjectAvailable {
			return Corruptf(SourceInvariant, "invariant 4: referenced object %d is %s", id, o.State)
		}
	}
	return nil
}

// checkNamespace checks invariants 1, 2, 3, 5, and 6 for one namespace.
func checkNamespace(s *Snapshot, ns Namespace, opts InvariantOptions) error {
	var segs []SegmentRow
	for _, seg := range s.Segments {
		if seg.Namespace == ns {
			segs = append(segs, seg)
		}
	}
	slices.SortFunc(segs, func(a, b SegmentRow) int { return cmp.Compare(a.Index, b.Index) })
	var blocks []ActiveBlockRow
	for _, b := range s.ActiveBlocks {
		if b.Namespace == ns {
			blocks = append(blocks, b)
		}
	}
	slices.SortFunc(blocks, func(a, b ActiveBlockRow) int {
		return cmp.Or(cmp.Compare(a.Segment, b.Segment), cmp.Compare(a.Ordinal, b.Ordinal))
	})
	key := SeqKey(ns)
	val, found := s.Meta[key]
	next, err := DecodeSeq(key, val, found)
	if err != nil {
		return err
	}

	// Invariant 2: indexes contiguous from 0, exactly one active segment,
	// and it is the last.
	for i, seg := range segs {
		if seg.Index != uint64(i) {
			return Corruptf(SourceInvariant, "invariant 2: %s segment at position %d has index %d", ns, i, seg.Index)
		}
		last := i == len(segs)-1
		if (seg.State == Active) != last {
			return Corruptf(SourceInvariant, "invariant 2: %s segment %d is %s at position %d of %d", ns, seg.Index, seg.State, i, len(segs))
		}
	}
	if len(segs) == 0 {
		if len(blocks) > 0 || next != 1 || (ns == Main && len(s.HotBatches) > 0) {
			return Corruptf(SourceInvariant, "invariant 2: %s has no segments but has blocks, hot batches, or %s=%d", ns, key, next)
		}
		return nil
	}

	// Invariant 1, sealed part: generations tile [1, ...) in index order.
	expect := uint64(1)
	for _, seg := range segs {
		if seg.State != Sealed {
			continue
		}
		g, ok := s.Generations[seg.GenerationID]
		if !ok || g.Namespace != ns || g.Segment != seg.Index {
			return Corruptf(SourceInvariant, "invariant 2: %s segment %d names generation %d, which is not its own", ns, seg.Index, seg.GenerationID)
		}
		gbs := s.GenerationBlocks[g.ID]
		for i, gb := range gbs {
			if gb.Ordinal != i {
				return Corruptf(SourceInvariant, "invariant 3: %s segment %d generation %d block ordinal %d at position %d", ns, seg.Index, g.ID, gb.Ordinal, i)
			}
		}
		if opts.Cheap {
			continue
		}
		hdr, err := segment.ReadSealedHeader(bytes.NewReader(g.Header))
		if err != nil {
			return Corruptf(SourceGeneration, "%s segment %d generation %d header: %v", ns, seg.Index, g.ID, err)
		}
		if int(hdr.BlockCount) != len(gbs) {
			return Corruptf(SourceInvariant, "invariant 1: %s segment %d header has %d blocks; catalog has %d", ns, seg.Index, hdr.BlockCount, len(gbs))
		}
		if hdr.EventCount == 0 {
			continue
		}
		if hdr.MinSeq != expect {
			return Corruptf(SourceInvariant, "invariant 1: %s segment %d starts at seq %d, want %d", ns, seg.Index, hdr.MinSeq, expect)
		}
		expect = hdr.MaxSeq + 1
	}
	if opts.Cheap && len(segs) > 1 {
		// Without headers, resume from the first active or hot seq.
		expect = 0
	}

	// Invariants 1 and 3, active part.
	active := segs[len(segs)-1]
	for i, b := range blocks {
		if b.Segment != active.Index {
			return Corruptf(SourceInvariant, "invariant 3: %s active block in segment %d; the active segment is %d", ns, b.Segment, active.Index)
		}
		if b.Ordinal != i {
			return Corruptf(SourceInvariant, "invariant 3: %s segment %d block ordinal %d at position %d", ns, b.Segment, b.Ordinal, i)
		}
		if expect == 0 {
			expect = b.MinSeq
		}
		if b.MinSeq != expect || b.MaxSeq < b.MinSeq || b.MaxSeq-b.MinSeq+1 != uint64(b.EventCount) {
			return Corruptf(SourceInvariant, "invariant 1: %s segment %d block %d covers [%d,%d] with %d events, want start %d",
				ns, b.Segment, b.Ordinal, b.MinSeq, b.MaxSeq, b.EventCount, expect)
		}
		expect = b.MaxSeq + 1
	}
	return checkHot(s, ns, expect, next, opts)
}

// checkHot finishes invariant 1 with invariants 5 and 6: hot batches (main
// only) continue exactly where the blocks end and reach the seq key. A zero
// expect means "wherever the first hot batch starts" (Cheap mode).
func checkHot(s *Snapshot, ns Namespace, expect, next uint64, opts InvariantOptions) error {
	if ns != Main {
		if expect != 0 && expect != next {
			return Corruptf(SourceInvariant, "invariant 1: %s blocks end before seq %d; %s is %d", ns, expect, SeqKey(ns), next)
		}
		return nil
	}
	for i, h := range s.HotBatches {
		if i > 0 && h.FirstSeq <= s.HotBatches[i-1].FirstSeq {
			return errors.New("catalog: snapshot hot batches are not in first_seq order")
		}
		if expect == 0 {
			expect = h.FirstSeq
		}
		if h.FirstSeq != expect || h.LastSeq < h.FirstSeq || h.LastSeq-h.FirstSeq+1 != uint64(h.EventCount) {
			return Corruptf(SourceInvariant, "invariant 5: hot batch [%d,%d] with %d events, want start %d",
				h.FirstSeq, h.LastSeq, h.EventCount, expect)
		}
		if int(h.EventCount) > opts.MaxEventsPerBlock {
			return Corruptf(SourceInvariant, "invariant 6: hot batch [%d,%d] has %d events, more than a block (%d)",
				h.FirstSeq, h.LastSeq, h.EventCount, opts.MaxEventsPerBlock)
		}
		if (h.ObjectID != 0) == h.Inline {
			// A schema CHECK, restated so a backend that forgets it fails.
			return Corruptf(SourceInvariant, "hot batch %d has both or neither of frame and object", h.FirstSeq)
		}
		expect = h.LastSeq + 1
	}
	if expect != 0 && expect != next {
		return Corruptf(SourceInvariant, "invariant 1: main is covered up to seq %d; %s is %d", expect, MainSeqKey, next)
	}
	return nil
}

// String renders a snapshot summary for failure messages.
func (s *Snapshot) String() string {
	return fmt.Sprintf("catalog rev %d: %d segments, %d active blocks, %d hot batches, meta %v",
		s.Archive.CatalogRevision, len(s.Segments), len(s.ActiveBlocks), len(s.HotBatches), s.Meta)
}
