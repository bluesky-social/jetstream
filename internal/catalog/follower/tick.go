package follower

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"slices"
	"time"

	"github.com/jcalabro/gt"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/internal/objstore"
	"github.com/bluesky-social/jetstream/internal/objstore/protocol"
	"github.com/bluesky-social/jetstream/segment"
)

// changes is what one tick's read transaction returned (design §11.1 steps
// 1 to 5).
type changes struct {
	archive    catalog.ArchiveRow
	full       bool
	phase      lifecycle.Phase
	deadline   time.Time
	deadlineOK bool
	segments   []catalog.SegmentRow
	gens       []catalog.GenerationRow
	genBlocks  map[uint64][]catalog.GenerationBlockRow
	activeRows []catalog.ActiveBlockRow
	activeKeys []catalog.ActiveBlockKey
	hot        []catalog.HotBatchRow
	objects    map[uint64]catalog.ObjectRow
}

// read runs the tick's one read transaction. It returns nil changes when
// the catalog revision equals old's.
func (f *Follower) read(ctx context.Context, old *mirror) (*changes, error) {
	rtx, err := f.cfg.DB.BeginRead(ctx)
	if err != nil {
		return nil, fmt.Errorf("follower: begin read: %w", err)
	}
	c, err := f.readTx(ctx, rtx, old)
	if cerr := rtx.Close(ctx); err == nil && cerr != nil {
		err = fmt.Errorf("follower: close read: %w", cerr)
	}
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (f *Follower) readTx(ctx context.Context, rtx catalog.ReadTx, old *mirror) (*changes, error) {
	arch, err := rtx.Archive(ctx)
	if err != nil {
		return nil, err
	}
	if arch.ArchiveID != f.cfg.ArchiveID {
		return nil, &fatalError{fmt.Errorf("follower: catalog archive_id %s is not the configured %s",
			objstore.FormatUUID(arch.ArchiveID), objstore.FormatUUID(f.cfg.ArchiveID))}
	}
	if old != nil {
		switch {
		case arch.CatalogRevision == old.rev:
			return nil, nil
		case arch.CatalogRevision < old.rev:
			return nil, catalog.Corruptf(catalog.SourceInvariant, "catalog_revision went from %d back to %d", old.rev, arch.CatalogRevision)
		}
	}
	c := &changes{archive: arch, genBlocks: map[uint64][]catalog.GenerationBlockRow{}}

	meta, err := rtx.MetaGet(ctx, [][]byte{[]byte(lifecycle.PhaseKey), []byte(catalog.CompactionDeadlineKey)})
	if err != nil {
		return nil, err
	}
	if v, ok := meta[lifecycle.PhaseKey]; ok {
		if c.phase, err = lifecycle.ParsePhase(v); err != nil {
			return nil, catalog.Corruptf(catalog.SourceMeta, "%v", err)
		}
	}
	if v, ok := meta[catalog.CompactionDeadlineKey]; ok {
		// A bad value only costs the Cache-Control hint, so it is not worth
		// taking every pod down over.
		if c.deadline, c.deadlineOK, err = catalog.DecodeCompactionDeadline(v); err != nil {
			f.log.Warn("ignoring malformed compaction deadline", slog.Any("error", err))
		}
	}

	// A phase change reloads every segment row: DeleteNamespace, which
	// commits with phase=steady_state, leaves no row behind to be seen as
	// a change.
	since := uint64(0)
	c.full = old == nil || old.phase != c.phase
	if !c.full {
		since = old.rev
	}
	if c.segments, err = rtx.SegmentsSince(ctx, since); err != nil {
		return nil, err
	}
	var genIDs []uint64
	for _, s := range c.segments {
		if s.State == catalog.Sealed && (old == nil || old.gens[s.GenerationID] == nil) {
			genIDs = append(genIDs, s.GenerationID)
		}
	}
	if len(genIDs) > 0 {
		if c.gens, err = rtx.Generations(ctx, genIDs); err != nil {
			return nil, err
		}
		gbs, err := rtx.GenerationBlocks(ctx, genIDs)
		if err != nil {
			return nil, err
		}
		for _, gb := range gbs {
			c.genBlocks[gb.GenerationID] = append(c.genBlocks[gb.GenerationID], gb)
		}
	}
	if c.activeRows, err = rtx.ActiveBlocksSince(ctx, since); err != nil {
		return nil, err
	}
	if c.activeKeys, err = rtx.ActiveBlockKeys(ctx); err != nil {
		return nil, err
	}
	// Every hot batch not in old was committed at or above old's tip, so
	// that is where frames start. Older inline frames are already in old.
	var framesFrom uint64
	if old != nil {
		framesFrom = old.view.TipSeq(catalog.Main)
	}
	if c.hot, err = rtx.HotBatches(ctx, framesFrom); err != nil {
		return nil, err
	}

	var want []uint64
	need := func(id uint64) {
		if old == nil {
			want = append(want, id)
		} else if _, ok := old.objects.get(id); !ok {
			want = append(want, id)
		}
	}
	for _, g := range c.gens {
		need(g.FooterObjectID)
	}
	for _, gbs := range c.genBlocks {
		for _, gb := range gbs {
			need(gb.ObjectID)
		}
	}
	for _, b := range c.activeRows {
		need(b.ObjectID)
	}
	for _, h := range c.hot {
		if !h.Inline {
			need(h.ObjectID)
		}
	}
	c.objects = map[uint64]catalog.ObjectRow{}
	if len(want) > 0 {
		slices.Sort(want)
		objs, err := rtx.Objects(ctx, slices.Compact(want))
		if err != nil {
			return nil, err
		}
		for _, o := range objs {
			c.objects[o.ID] = o
		}
	}
	return c, nil
}

// tickRows is the RowSource of one tick's object reads: the rows the tick
// just loaded, then the previous mirror's, then the catalog. It never waits
// on the follower, which is mid-tick.
type tickRows struct {
	objects objIndex
	db      protocol.DBRows
}

func (t tickRows) Object(ctx context.Context, id uint64, refresh bool) (catalog.ObjectRow, bool, error) {
	if !refresh {
		if r, ok := t.objects.get(id); ok {
			return r, true, nil
		}
	}
	return t.db.Object(ctx, id, refresh)
}

// loadedGen is a generation built this tick, with its footer bytes for the
// manifest.
type loadedGen struct {
	gen    *generation
	footer []byte
}

// build makes the next mirror from old and c. Reads go through rd, whose
// rows cover c.
func (f *Follower) build(ctx context.Context, old *mirror, c *changes, rd *protocol.Reader, objects objIndex) (*mirror, []loadedGen, error) {
	loaded, err := gt.ConcurrentN(ctx, c.gens, f.cfg.ReadConcurrency, func(row catalog.GenerationRow) (loadedGen, error) {
		return loadGeneration(ctx, rd, row, c.genBlocks[row.ID], objects)
	})
	if err != nil {
		return nil, nil, err
	}
	newGens := make(map[uint64]*generation, len(loaded))
	for _, l := range loaded {
		newGens[l.gen.id] = l.gen
	}

	m := &mirror{
		rev:        c.archive.CatalogRevision,
		phase:      c.phase,
		deadline:   c.deadline,
		deadlineOK: c.deadlineOK,
		objects:    objects,
	}
	if err := m.applySegments(old, c, newGens); err != nil {
		return nil, nil, err
	}
	if err := m.applyActiveBlocks(old, c); err != nil {
		return nil, nil, err
	}
	if err := m.applyHot(old, c); err != nil {
		return nil, nil, err
	}
	tails, err := m.tails(objects)
	if err != nil {
		return nil, nil, err
	}
	if m.view, err = catalog.NewView(m.rev, m.sealed, tails, m.locate); err != nil {
		return nil, nil, catalog.Corruptf(catalog.SourceInvariant, "revision %d: %v", m.rev, err)
	}
	m.objects = m.objects.compacted(m.referencedObjects)
	return m, loaded, nil
}

// loadGeneration fetches row's footer and checks that the header, footer,
// and generation_blocks describe one segment file.
func loadGeneration(ctx context.Context, rd *protocol.Reader, row catalog.GenerationRow, gbs []catalog.GenerationBlockRow, objects objIndex) (loadedGen, error) {
	bad := func(format string, args ...any) error {
		return catalog.Corruptf(catalog.SourceGeneration, "%s segment %d generation %d: %s",
			row.Namespace, row.Segment, row.ID, fmt.Sprintf(format, args...))
	}
	hdr, err := segment.ReadSealedHeader(bytes.NewReader(row.Header))
	if err != nil {
		return loadedGen{}, bad("header: %v", err)
	}
	footer, err := rd.Get(ctx, row.FooterObjectID)
	if err != nil {
		return loadedGen{}, fmt.Errorf("follower: fetch footer of %s segment %d generation %d: %w", row.Namespace, row.Segment, row.ID, err)
	}
	r, err := segment.OpenReaderParts(row.Header, footer, func(i int) ([]byte, error) {
		return nil, fmt.Errorf("block %d read while loading a footer", i)
	}, segment.ReaderOptions{Name: fmt.Sprintf("%s/%d@%d", row.Namespace, row.Segment, row.ID)})
	if err != nil {
		return loadedGen{}, bad("footer: %v", err)
	}
	blocks := slices.Clone(r.Blocks())
	_ = r.Close()

	if len(blocks) != len(gbs) || uint64(hdr.BlockCount) != uint64(len(gbs)) {
		return loadedGen{}, bad("footer lists %d blocks, header %d, generation_blocks %d", len(blocks), hdr.BlockCount, len(gbs))
	}
	g := &generation{
		id:        row.ID,
		ns:        row.Namespace,
		idx:       row.Segment,
		headerRaw: row.Header,
		header:    hdr,
		createdAt: row.CreatedAt,
		footerID:  row.FooterObjectID,
		footerLen: int64(len(footer)),
		blockIDs:  make([]uint64, len(gbs)),
		blockLens: make([]int64, len(gbs)),
		blocks:    blocks,
		size:      int64(hdr.FooterOffset) + int64(len(footer)),
	}
	off := uint64(segment.ReservedHeaderBytes)
	for i, gb := range gbs {
		if gb.Ordinal != i {
			return loadedGen{}, bad("generation block %d has ordinal %d", i, gb.Ordinal)
		}
		if blocks[i].Offset != off || int64(blocks[i].CompressedSize) != gb.CompressedLength {
			return loadedGen{}, bad("block %d at offset %d with %d bytes; generation_blocks put it at %d with %d",
				i, blocks[i].Offset, blocks[i].CompressedSize, off, gb.CompressedLength)
		}
		if o, ok := objects.get(gb.ObjectID); ok && o.Length != gb.CompressedLength {
			return loadedGen{}, bad("block %d object %d is %d bytes, want %d", i, gb.ObjectID, o.Length, gb.CompressedLength)
		}
		g.blockIDs[i], g.blockLens[i] = gb.ObjectID, gb.CompressedLength
		off += 8 + uint64(gb.CompressedLength)
	}
	if hdr.FooterOffset != off {
		return loadedGen{}, bad("footer_offset %d, blocks end at %d", hdr.FooterOffset, off)
	}
	return loadedGen{gen: g, footer: footer}, nil
}

// applySegments sets m's sealed lists, generations, and active segment
// indexes from old and c.
func (m *mirror) applySegments(old *mirror, c *changes, newGens map[uint64]*generation) error {
	genFor := func(row catalog.SegmentRow) (*generation, error) {
		g := newGens[row.GenerationID]
		if g == nil && old != nil {
			g = old.gens[row.GenerationID]
		}
		if g == nil || g.ns != row.Namespace || g.idx != row.Index {
			return nil, catalog.Corruptf(catalog.SourceInvariant, "%s segment %d names generation %d, which was not loaded for it",
				row.Namespace, row.Index, row.GenerationID)
		}
		return g, nil
	}

	if c.full {
		m.gens = make(map[uint64]*generation)
		m.active = make(map[catalog.Namespace]activeSegment)
		views := map[catalog.Namespace][]catalog.SegmentView{}
		for _, row := range c.segments {
			if row.State == catalog.Active {
				m.active[row.Namespace] = activeSegment{index: row.Index}
				continue
			}
			g, err := genFor(row)
			if err != nil {
				return err
			}
			m.gens[g.id] = g
			views[row.Namespace] = append(views[row.Namespace], g.view())
		}
		m.sealed = make(map[catalog.Namespace]catalog.SegmentList, len(views))
		for ns, vs := range views {
			l, err := catalog.NewSegmentList(vs)
			if err != nil {
				return catalog.Corruptf(catalog.SourceInvariant, "revision %d: %v", m.rev, err)
			}
			m.sealed[ns] = l
		}
		return nil
	}

	m.gens, m.sealed, m.active = old.gens, old.sealed, old.active
	cloned := false
	for _, row := range c.segments {
		if !cloned {
			m.gens, m.sealed, m.active = maps.Clone(old.gens), maps.Clone(old.sealed), maps.Clone(old.active)
			cloned = true
		}
		if row.State == catalog.Active {
			m.active[row.Namespace] = activeSegment{index: row.Index}
			continue
		}
		if a, ok := m.active[row.Namespace]; ok && a.index == row.Index {
			delete(m.active, row.Namespace)
		}
		cur, ok := m.sealed[row.Namespace].Get(row.Index)
		if ok && cur.Generation == row.GenerationID {
			continue
		}
		g, err := genFor(row)
		if err != nil {
			return err
		}
		if ok {
			delete(m.gens, cur.Generation)
		}
		m.gens[g.id] = g
		l, err := m.sealed[row.Namespace].Put(g.view())
		if err != nil {
			return catalog.Corruptf(catalog.SourceInvariant, "revision %d: %v", m.rev, err)
		}
		m.sealed[row.Namespace] = l
	}
	return nil
}

// applyActiveBlocks sets each active segment's blocks: the current key set,
// with rows from c or, unchanged, from old.
func (m *mirror) applyActiveBlocks(old *mirror, c *changes) error {
	fresh := make(map[catalog.ActiveBlockKey]catalog.ActiveBlockRow, len(c.activeRows))
	for _, r := range c.activeRows {
		fresh[r.Key()] = r
	}
	blocks := map[catalog.Namespace][]catalog.ActiveBlockRow{}
	for _, k := range c.activeKeys {
		a, ok := m.active[k.Namespace]
		if !ok || a.index != k.Segment {
			return catalog.Corruptf(catalog.SourceInvariant, "active block %s/%d/%d is not in the active segment", k.Namespace, k.Segment, k.Ordinal)
		}
		r, ok := fresh[k]
		if !ok && !c.full && old != nil {
			if oa, found := old.active[k.Namespace]; found && oa.index == k.Segment && k.Ordinal < len(oa.blocks) {
				r, ok = oa.blocks[k.Ordinal], true
			}
		}
		if !ok {
			return catalog.Corruptf(catalog.SourceInvariant, "active block %s/%d/%d has no row", k.Namespace, k.Segment, k.Ordinal)
		}
		if r.Ordinal != len(blocks[k.Namespace]) {
			return catalog.Corruptf(catalog.SourceInvariant, "%s segment %d active block ordinal %d follows %d blocks",
				k.Namespace, k.Segment, r.Ordinal, len(blocks[k.Namespace]))
		}
		blocks[k.Namespace] = append(blocks[k.Namespace], r)
	}
	if old != nil && !c.full && len(c.segments) == 0 && len(c.activeRows) == 0 && sameActiveKeys(old, blocks) {
		return nil
	}
	active := make(map[catalog.Namespace]activeSegment, len(m.active))
	for ns, a := range m.active {
		active[ns] = activeSegment{index: a.index, blocks: blocks[ns]}
	}
	m.active = active
	return nil
}

func sameActiveKeys(old *mirror, blocks map[catalog.Namespace][]catalog.ActiveBlockRow) bool {
	for ns, a := range old.active {
		if len(a.blocks) != len(blocks[ns]) {
			return false
		}
	}
	return true
}

// applyHot sets Main's hot batches, reusing old's inline frames.
func (m *mirror) applyHot(old *mirror, c *changes) error {
	var oldHot map[uint64]catalog.HotBatchRow
	if old != nil {
		oldHot = make(map[uint64]catalog.HotBatchRow, len(old.hot))
		for _, h := range old.hot {
			oldHot[h.FirstSeq] = h
		}
	}
	m.hot = make([]catalog.HotBatchRow, len(c.hot))
	for i, h := range c.hot {
		if h.LastSeq < h.FirstSeq || uint64(h.EventCount) != h.LastSeq-h.FirstSeq+1 {
			return catalog.Corruptf(catalog.SourceHotBatch, "hot batch [%d,%d] has %d events", h.FirstSeq, h.LastSeq, h.EventCount)
		}
		if i > 0 && h.FirstSeq != c.hot[i-1].LastSeq+1 {
			return catalog.Corruptf(catalog.SourceHotBatch, "hot batch [%d,%d] follows [%d,%d]", h.FirstSeq, h.LastSeq, c.hot[i-1].FirstSeq, c.hot[i-1].LastSeq)
		}
		if h.Inline && h.Frame == nil {
			o, ok := oldHot[h.FirstSeq]
			if !ok || !o.Inline || o.LastSeq != h.LastSeq || o.Frame == nil {
				return catalog.Corruptf(catalog.SourceHotBatch, "inline hot batch [%d,%d] below the last tip has no frame in the mirror", h.FirstSeq, h.LastSeq)
			}
			h.Frame = o.Frame
		}
		if !h.Inline && h.ObjectID == 0 {
			return catalog.Corruptf(catalog.SourceHotBatch, "pointer hot batch [%d,%d] has no object", h.FirstSeq, h.LastSeq)
		}
		m.hot[i] = h
	}
	return nil
}

// tails builds each namespace's tail view segment (active blocks, then
// Main's hot batches as pseudo-blocks) and records its locators.
func (m *mirror) tails(objects objIndex) (map[catalog.Namespace]catalog.SegmentView, error) {
	tails := make(map[catalog.Namespace]catalog.SegmentView, len(m.active)+1)
	m.tailLocs = make(map[catalog.Namespace][]catalog.Locator, len(m.active)+1)
	add := func(ns catalog.Namespace, idx uint64) catalog.SegmentView {
		if v, ok := tails[ns]; ok {
			return v
		}
		return catalog.SegmentView{Namespace: ns, Index: idx, State: catalog.Active}
	}
	// Offsets are those of a segment file holding these blocks in order;
	// nothing reads them, but they keep BlockInfo meaningful.
	off := map[catalog.Namespace]uint64{}
	push := func(v *catalog.SegmentView, loc catalog.Locator, b segment.BlockInfo) error {
		if b.MaxSeq < b.MinSeq || uint64(b.EventCount) != b.MaxSeq-b.MinSeq+1 {
			return catalog.Corruptf(catalog.SourceInvariant, "%s tail block [%d,%d] has %d events", v.Namespace, b.MinSeq, b.MaxSeq, b.EventCount)
		}
		o, ok := off[v.Namespace]
		if !ok {
			o = uint64(segment.ReservedHeaderBytes)
		}
		b.Offset = o
		off[v.Namespace] = o + 8 + uint64(b.CompressedSize)
		v.Blocks = append(v.Blocks, b)
		m.tailLocs[v.Namespace] = append(m.tailLocs[v.Namespace], loc)
		return nil
	}
	for ns, a := range m.active {
		v := add(ns, a.index)
		for _, r := range a.blocks {
			if r.CompressedLength < 0 || r.CompressedLength > math.MaxUint32 || r.UncompressedLength < 0 || r.UncompressedLength > math.MaxUint32 {
				return nil, catalog.Corruptf(catalog.SourceInvariant, "active block %s/%d/%d has lengths %d/%d",
					ns, r.Segment, r.Ordinal, r.CompressedLength, r.UncompressedLength)
			}
			if err := push(&v, catalog.ObjectBlock{ObjectID: r.ObjectID}, segment.BlockInfo{
				CompressedSize:   uint32(r.CompressedLength),
				UncompressedSize: uint32(r.UncompressedLength),
				EventCount:       r.EventCount,
				MinSeq:           r.MinSeq,
				MaxSeq:           r.MaxSeq,
				MinWitnessedAt:   r.MinWitnessedUS,
				MaxWitnessedAt:   r.MaxWitnessedUS,
			}); err != nil {
				return nil, err
			}
		}
		tails[ns] = v
	}
	if len(m.hot) > 0 {
		idx := uint64(0)
		if last, ok := m.sealed[catalog.Main].Last(); ok {
			idx = last.Index + 1
		}
		v := add(catalog.Main, idx)
		for _, h := range m.hot {
			var loc catalog.Locator
			var n int64
			if h.Inline {
				loc, n = catalog.InlineBlock{Frame: h.Frame}, int64(len(h.Frame))
			} else {
				loc = catalog.ObjectBlock{ObjectID: h.ObjectID}
				if o, ok := objects.get(h.ObjectID); ok {
					n = o.Length
				}
			}
			if n > math.MaxUint32 {
				return nil, catalog.Corruptf(catalog.SourceHotBatch, "hot batch [%d,%d] is %d bytes", h.FirstSeq, h.LastSeq, n)
			}
			if err := push(&v, loc, segment.BlockInfo{
				CompressedSize: uint32(n),
				EventCount:     h.EventCount,
				MinSeq:         h.FirstSeq,
				MaxSeq:         h.LastSeq,
				MinWitnessedAt: h.MinWitnessedUS,
				MaxWitnessedAt: h.MaxWitnessedUS,
			}); err != nil {
				return nil, err
			}
		}
		tails[catalog.Main] = v
	}
	return tails, nil
}

// feed returns the events [log tip, next's Main tip) in seq order: blocks
// folded (or sealed) since the last tick first, read through RefsFrom, then
// the hot batches (design §11.1). Every seq appears exactly once.
func (f *Follower) feed(ctx context.Context, next *mirror, from uint64, rd *protocol.Reader) ([]segment.Event, error) {
	tip := next.view.TipSeq(catalog.Main)
	if tip <= from {
		if tip != 0 && tip < from {
			return nil, catalog.Corruptf(catalog.SourceInvariant, "main tip %d is below the readable log tip %d", tip, from)
		}
		return nil, nil
	}
	fetch := tickFetcher{rd: rd}
	var out []segment.Event
	want := from
	for ref := range next.view.RefsFrom(catalog.Main, from) {
		events, err := catalog.DecodeRef(ctx, fetch, ref)
		if err != nil {
			if _, ok := catalog.IsCorruption(err); ok {
				return nil, err
			}
			if _, inline := ref.Loc.(catalog.InlineBlock); inline || errors.Is(err, segment.ErrCorruptSegment) {
				return nil, catalog.Corruptf(catalog.SourceHotBatch, "decode main block [%d,%d]: %v", ref.MinSeq, ref.MaxSeq, err)
			}
			return nil, fmt.Errorf("follower: read main block [%d,%d]: %w", ref.MinSeq, ref.MaxSeq, err)
		}
		for i := range events {
			ev := &events[i]
			if ev.Seq < want {
				continue
			}
			if ev.Seq != want || ev.Seq > ref.MaxSeq {
				return nil, catalog.Corruptf(catalog.SourceHotBatch, "main block [%d,%d] holds seq %d where %d was due", ref.MinSeq, ref.MaxSeq, ev.Seq, want)
			}
			out = append(out, *ev)
			want++
		}
		if want != ref.MaxSeq+1 {
			return nil, catalog.Corruptf(catalog.SourceHotBatch, "main block [%d,%d] ends at seq %d", ref.MinSeq, ref.MaxSeq, want)
		}
	}
	if want != tip {
		return nil, catalog.Corruptf(catalog.SourceInvariant, "main refs from %d end at %d, before tip %d", from, want, tip)
	}
	return out, nil
}

// tickFetcher reads a not-yet-published mirror's blocks.
type tickFetcher struct{ rd *protocol.Reader }

func (t tickFetcher) Fetch(ctx context.Context, ref catalog.BlockRef) ([]byte, error) {
	switch loc := ref.Loc.(type) {
	case catalog.InlineBlock:
		return loc.Frame, nil
	case catalog.ObjectBlock:
		return t.rd.Get(ctx, loc.ObjectID)
	default:
		return nil, fmt.Errorf("follower: unsupported locator %T", ref.Loc)
	}
}

// remoteSegments lists Main's sealed segments for the manifest's initial
// load, taking footers from loaded or fetching them.
func (f *Follower) remoteSegments(ctx context.Context, m *mirror, loaded []loadedGen, rd *protocol.Reader) ([]manifest.RemoteSegment, error) {
	footers := make(map[uint64][]byte, len(loaded))
	for _, l := range loaded {
		footers[l.gen.id] = l.footer
	}
	segs := m.sealed[catalog.Main].Segments()
	return gt.ConcurrentN(ctx, segs, f.cfg.ReadConcurrency, func(v catalog.SegmentView) (manifest.RemoteSegment, error) {
		g := m.gens[v.Generation]
		footer, ok := footers[g.id]
		if !ok {
			var err error
			if footer, err = rd.Get(ctx, g.footerID); err != nil {
				return manifest.RemoteSegment{}, fmt.Errorf("follower: fetch footer of main segment %d: %w", g.idx, err)
			}
		}
		return manifest.RemoteSegment{Idx: g.idx, Parts: manifest.SegmentParts{
			Generation: g.header.Checksum,
			Header:     g.headerRaw,
			Footer:     footer,
			CreatedAt:  g.createdAt,
			Size:       g.size,
		}}, nil
	})
}
