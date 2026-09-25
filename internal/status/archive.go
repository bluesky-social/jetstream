package status

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/segment"
)

// Archive is the segment catalog the collector reads segment data through,
// so /status needs no directory of segment files to walk.
type Archive interface {
	Snapshot() catalog.CatalogView
	Fetcher() catalog.Fetcher
	// SealedMetadata returns a Reader over a sealed segment's footer, for
	// the collection index the view does not carry. It reports
	// catalog.ErrStaleRef when the segment was rewritten since the view.
	SealedMetadata(v catalog.SegmentView) (*segment.Reader, error)
}

// treeFromView folds ns's segments in view into a tree labeled dir. With
// activeOnly it folds only the active segment, for a namespace whose sealed
// segments already come from the manifest.
//
// Per-segment failures are tolerated like InspectAll's per-file ones: the
// namespace's newest segment fails silently (it can be sealing or rotating
// under us), older ones become warnings, and a segment rewritten since the
// view (catalog.ErrStaleRef) is skipped as a race with compaction.
func treeFromView(ctx context.Context, a Archive, view catalog.CatalogView, ns catalog.Namespace, dir string, activeOnly bool, collections map[string]*CollectionAggregate) (TreeAggregate, []string, error) {
	tree := TreeAggregate{Dir: dir}
	var warnings []string
	segs := view.Segments(ns)
	for i, v := range segs {
		if activeOnly && v.State != catalog.Active {
			continue
		}
		if err := ctx.Err(); err != nil {
			return TreeAggregate{}, nil, err
		}
		var (
			ins *segment.Inspection
			err error
		)
		if v.State == catalog.Sealed {
			ins, err = inspectSealedView(a, v)
		} else {
			ins, err = inspectActiveView(ctx, view, a.Fetcher(), v)
		}
		if err != nil {
			if ctx.Err() != nil {
				return TreeAggregate{}, nil, ctx.Err()
			}
			if i != len(segs)-1 && !errors.Is(err, catalog.ErrStaleRef) {
				warnings = append(warnings, fmt.Sprintf("%s segment %d: %v", ns, v.Index, err))
			}
			continue
		}

		tree.DiskBytes += ins.FileSize
		if ins.Sealed {
			tree.SealedCount++
		} else {
			tree.ActiveCount++
		}
		// Segments come in index order, so the last one folded is the
		// newest.
		tree.LatestSegment = &SegmentSummary{
			Index:           v.Index,
			Sealed:          ins.Sealed,
			EventCount:      ins.TotalEvents,
			UniqueDIDCount:  ins.UniqueDIDCount,
			BlockCount:      uint32(len(ins.Blocks)),
			CollectionCount: len(ins.Collections),
			MinSeq:          ins.MinSeq,
			MaxSeq:          ins.MaxSeq,
			MinWitnessedAt:  microsToTime(ins.MinWitnessedAt),
			MaxWitnessedAt:  microsToTime(ins.MaxWitnessedAt),
			SizeBytes:       ins.FileSize,
		}
		foldInspection(&tree, ins, collections)
	}
	return tree, warnings, nil
}

// inspectSealedView is segment.Inspect for a sealed segment in a view: the
// header and block index come from the view, the collection index from the
// footer. The checksum is not recomputed; the catalog verified it on load.
func inspectSealedView(a Archive, v catalog.SegmentView) (*segment.Inspection, error) {
	r, err := a.SealedMetadata(v)
	if err != nil {
		return nil, err
	}
	defer func() { _ = r.Close() }()

	blocks := r.Blocks()
	perBlock := make([][]uint32, len(blocks))
	for i := range blocks {
		ids, err := r.BlockCollections(i)
		if err != nil {
			return nil, err
		}
		perBlock[i] = ids
	}
	h := v.Header
	return &segment.Inspection{
		FileSize:              v.Size,
		Sealed:                true,
		Header:                h,
		Blocks:                blocks,
		Collections:           r.Collections(),
		BlockCollections:      perBlock,
		CollectionEventCounts: r.CollectionEventCounts(),
		TotalEvents:           uint64(h.EventCount),
		UniqueDIDCount:        h.UniqueDIDCount,
		MinSeq:                h.MinSeq,
		MaxSeq:                h.MaxSeq,
		MinWitnessedAt:        h.MinWitnessedAt,
		MaxWitnessedAt:        h.MaxWitnessedAt,
		ChecksumValid:         true,
	}, nil
}

// inspectActiveView is segment.Inspect for an active segment in a view. An
// active segment has no footer yet, so the collection table and unique DIDs
// come from decoding its durable blocks, the same walk segment.Inspect does
// over an active file: collections in first-seen order, DID-marker
// sentinels interned but not counted, empty DIDs ignored.
func inspectActiveView(ctx context.Context, view catalog.CatalogView, f catalog.Fetcher, v catalog.SegmentView) (*segment.Inspection, error) {
	ins := &segment.Inspection{
		FileSize:         segment.ReservedHeaderBytes,
		Blocks:           v.Blocks,
		BlockCollections: make([][]uint32, len(v.Blocks)),
	}
	if len(v.Blocks) == 0 {
		return ins, nil
	}
	last := v.Blocks[len(v.Blocks)-1]
	// Each block is framed by an 8-byte length prefix at its offset.
	ins.FileSize = int64(last.Offset) + 8 + int64(last.CompressedSize)
	ins.MinSeq, ins.MaxSeq = v.MinSeq(), v.MaxSeq()
	ins.MinWitnessedAt, ins.MaxWitnessedAt = v.Blocks[0].MinWitnessedAt, v.Blocks[0].MaxWitnessedAt
	for _, b := range v.Blocks {
		ins.TotalEvents += uint64(b.EventCount)
		ins.MinWitnessedAt = min(ins.MinWitnessedAt, b.MinWitnessedAt)
		ins.MaxWitnessedAt = max(ins.MaxWitnessedAt, b.MaxWitnessedAt)
	}

	ids := make(map[string]uint32)
	dids := make(map[string]struct{})
	intern := func(name string, count bool, block map[uint32]struct{}) {
		id, ok := ids[name]
		if !ok {
			id = uint32(len(ins.Collections))
			ids[name] = id
			ins.Collections = append(ins.Collections, strings.Clone(name))
			ins.CollectionEventCounts = append(ins.CollectionEventCounts, 0)
		}
		if count {
			ins.CollectionEventCounts[id]++
		}
		block[id] = struct{}{}
	}
	for ref := range view.RefsFrom(v.Namespace, v.MinSeq()) {
		if ref.Segment != v.Index {
			break
		}
		if ref.Block >= len(ins.BlockCollections) {
			return nil, fmt.Errorf("status: %s segment %d: ref for block %d of %d", v.Namespace, v.Index, ref.Block, len(ins.BlockCollections))
		}
		events, err := catalog.DecodeRef(ctx, f, ref)
		if err != nil {
			return nil, err
		}
		block := make(map[uint32]struct{})
		for i := range events {
			ev := &events[i]
			if ev.DID != "" {
				if _, ok := dids[ev.DID]; !ok {
					dids[strings.Clone(ev.DID)] = struct{}{}
				}
			}
			if ev.Collection != "" {
				intern(ev.Collection, true, block)
			}
			if s := didMarkerSentinel(ev.Kind); s != "" {
				intern(s, false, block)
			}
		}
		blockIDs := make([]uint32, 0, len(block))
		for id := range block {
			blockIDs = append(blockIDs, id)
		}
		slices.Sort(blockIDs)
		ins.BlockCollections[ref.Block] = blockIDs
	}
	ins.UniqueDIDCount = uint32(len(dids))
	return ins, nil
}

// didMarkerSentinel mirrors the segment writer's interning of DID-level
// events under a reserved collection name.
func didMarkerSentinel(k segment.Kind) string {
	switch k {
	case segment.KindAccount:
		return segment.SentinelCollectionAccount
	case segment.KindIdentity:
		return segment.SentinelCollectionIdentity
	case segment.KindSync:
		return segment.SentinelCollectionSync
	default:
		return ""
	}
}
