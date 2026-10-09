package catalog

import (
	"context"
	"slices"
)

// MaxChangedGenerations bounds the sealed segments one DB.ReadChanges loads
// generations for. A tick after a long outage, or one that lands on a big
// compaction, can see more; ReadChanges then reports Overflow instead of
// running statements whose size has no bound, and the follower falls back to
// the chunked per-statement read.
const MaxChangedGenerations = 256

// ChangesQuery is one incremental follower read (design §11.1 steps 1 to 5)
// against a mirror at revision Since.
type ChangesQuery struct {
	// Since is the mirror's catalog revision: rows with a revision above it
	// changed.
	Since uint64
	// FramesFrom is the mirror's tip: inline hot batch frames load from
	// here, as ReadTx.HotBatches.
	FramesFrom uint64
	// MetaKeys are the metadata_kv keys to read.
	MetaKeys [][]byte
}

// Changes is what DB.ReadChanges read, all from one snapshot. When
// Archive.CatalogRevision <= Since nothing changed, and every field after
// Meta may be empty.
type Changes struct {
	Archive ArchiveRow
	// Meta holds the values of the MetaKeys that exist.
	Meta map[string][]byte
	// Overflow reports more than MaxChangedGenerations changed sealed
	// segments. Only Archive and Meta are then valid.
	Overflow bool
	// Segments are the segment rows with revision > Since, as
	// ReadTx.SegmentsSince.
	Segments []SegmentRow
	// Generations and GenerationBlocks are those of the sealed segments in
	// Segments, ordered as ReadTx.Generations and ReadTx.GenerationBlocks.
	Generations      []GenerationRow
	GenerationBlocks []GenerationBlockRow
	// ActiveBlocks are the active block rows with revision > Since;
	// ActiveBlockKeys is every active block's key.
	ActiveBlocks    []ActiveBlockRow
	ActiveBlockKeys []ActiveBlockKey
	// HotBatches is every hot batch, as ReadTx.HotBatches(FramesFrom).
	HotBatches []HotBatchRow
	// Objects are the rows of every object Generations (footers),
	// GenerationBlocks, ActiveBlocks, and the pointer HotBatches reference,
	// in object_id order.
	Objects []ObjectRow
}

// ReadChangesTx is DB.ReadChanges as a sequence of ReadTx statements. It is
// the reference a backend's one-round-trip version must match, and the
// implementation of a backend whose statements cost nothing.
func ReadChangesTx(ctx context.Context, rtx ReadTx, q ChangesQuery) (Changes, error) {
	var c Changes
	var err error
	if c.Archive, err = rtx.Archive(ctx); err != nil {
		return Changes{}, err
	}
	if c.Meta, err = rtx.MetaGet(ctx, q.MetaKeys); err != nil {
		return Changes{}, err
	}
	if c.Archive.CatalogRevision <= q.Since {
		return c, nil
	}
	if c.Segments, err = rtx.SegmentsSince(ctx, q.Since); err != nil {
		return Changes{}, err
	}
	var genIDs []uint64
	for _, s := range c.Segments {
		if s.State == Sealed {
			genIDs = append(genIDs, s.GenerationID)
		}
	}
	genIDs = slices.Compact(slices.Sorted(slices.Values(genIDs)))
	if len(genIDs) > MaxChangedGenerations {
		return Changes{Archive: c.Archive, Meta: c.Meta, Overflow: true}, nil
	}
	if len(genIDs) > 0 {
		if c.Generations, err = rtx.Generations(ctx, genIDs); err != nil {
			return Changes{}, err
		}
		if c.GenerationBlocks, err = rtx.GenerationBlocks(ctx, genIDs); err != nil {
			return Changes{}, err
		}
	}
	if c.ActiveBlocks, err = rtx.ActiveBlocksSince(ctx, q.Since); err != nil {
		return Changes{}, err
	}
	if c.ActiveBlockKeys, err = rtx.ActiveBlockKeys(ctx); err != nil {
		return Changes{}, err
	}
	if c.HotBatches, err = rtx.HotBatches(ctx, q.FramesFrom); err != nil {
		return Changes{}, err
	}
	var ids []uint64
	for _, g := range c.Generations {
		ids = append(ids, g.FooterObjectID)
	}
	for _, gb := range c.GenerationBlocks {
		ids = append(ids, gb.ObjectID)
	}
	for _, b := range c.ActiveBlocks {
		ids = append(ids, b.ObjectID)
	}
	for _, h := range c.HotBatches {
		if !h.Inline {
			ids = append(ids, h.ObjectID)
		}
	}
	if len(ids) > 0 {
		if c.Objects, err = rtx.Objects(ctx, slices.Compact(slices.Sorted(slices.Values(ids)))); err != nil {
			return Changes{}, err
		}
	}
	return c, nil
}
