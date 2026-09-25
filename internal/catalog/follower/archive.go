package follower

import (
	"context"
	"fmt"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/objstore"
	"github.com/bluesky-social/jetstream/internal/seqspace"
	"github.com/bluesky-social/jetstream/segment"
)

// This file is the follower's surface for the read endpoints (design
// §11.4-§11.6): the seq state cursor resolution reads in place of a writer,
// the generation parts getSegment and getBlock serve, and the footer
// readers status and repo export use.

// NextSeq returns one past the mirror's newest Main seq. Seqs start at 1,
// so an empty archive's next seq is 1, as a writer's is.
func (f *Follower) NextSeq() uint64 {
	return max(f.Snapshot().TipSeq(catalog.Main), 1)
}

// SeqGaps returns nil: disaggregated seqs are allocated by the committer
// without vacancies.
func (f *Follower) SeqGaps() *seqspace.Gaps { return nil }

// ActiveTimeFloorSeq returns the first seq of the first tail block (active
// blocks, then hot batches) witnessed at or after timeUS, or NextSeq when
// none is. Like the writer's, the answer is block-granular, and the caller
// rechecks the manifest for a seal that raced it.
func (f *Follower) ActiveTimeFloorSeq(timeUS int64) uint64 {
	v := f.Snapshot()
	segs := v.Segments(catalog.Main)
	if n := len(segs); n > 0 && segs[n-1].State != catalog.Sealed {
		for _, b := range segs[n-1].Blocks {
			if b.MaxWitnessedAt >= timeUS {
				return b.MinSeq
			}
		}
	}
	return max(v.TipSeq(catalog.Main), 1)
}

// LogFloor returns the readable log's floor, or ok=false before the first
// steady-state mirror. The cold reader serves seqs below it.
func (f *Follower) LogFloor() (floor uint64, ok bool) {
	if l := f.Log(); l != nil {
		return l.FloorSeq(), true
	}
	return 0, false
}

// SyncSeq runs a synchronous tick when seq is at or past the mirror's tip
// (design §11.6): a client that saw seq on another pod, or through the
// leader, must not be told it is in the future or be planned short of it.
func (f *Follower) SyncSeq(ctx context.Context, seq uint64) error {
	if seq < f.NextSeq() {
		return nil
	}
	return f.Refresh(ctx)
}

// GenerationParts returns the current generation of Main's sealed segment
// idx, or found=false if there is none. An index past the mirror's last
// sealed segment runs a synchronous tick first: the client may have learned
// of the segment from a fresher pod.
func (f *Follower) GenerationParts(ctx context.Context, idx uint64) (catalog.GenerationParts, bool, error) {
	if p, ok := f.generationParts(idx); ok {
		return p, true, nil
	}
	if m := f.cur.Load(); m != nil {
		if last, ok := m.sealed[catalog.Main].Last(); ok && idx <= last.Index {
			return catalog.GenerationParts{}, false, nil
		}
	}
	if err := f.Refresh(ctx); err != nil {
		return catalog.GenerationParts{}, false, err
	}
	p, ok := f.generationParts(idx)
	return p, ok, nil
}

func (f *Follower) generationParts(idx uint64) (catalog.GenerationParts, bool) {
	m := f.cur.Load()
	if m == nil {
		return catalog.GenerationParts{}, false
	}
	v, ok := m.sealed[catalog.Main].Get(idx)
	if !ok {
		return catalog.GenerationParts{}, false
	}
	g := m.gens[v.Generation]
	blocks := make([]catalog.ObjectPart, len(g.blockIDs))
	for i, id := range g.blockIDs {
		blocks[i] = catalog.ObjectPart{ID: id, Length: g.blockLens[i]}
	}
	return catalog.GenerationParts{
		Generation: g.id,
		Header:     g.headerRaw,
		CreatedAt:  g.createdAt,
		Blocks:     blocks,
		Footer:     catalog.ObjectPart{ID: g.footerID, Length: g.footerLen},
	}, true
}

// Objects returns the pod's object reader, which verifies and caches whole
// objects and serves ranges.
func (f *Follower) Objects() objstore.Store { return f.rd }

// Fetcher returns f as the catalog's Fetcher, for readers that take the
// catalog and its fetcher separately.
func (f *Follower) Fetcher() catalog.Fetcher { return f }

// SealedMetadata returns a Reader over sealed segment v's header and footer,
// for readers that need footer sections the view does not carry. Its
// DecodeBlock reads through Fetch, pinned to v's generation. A generation
// the current mirror no longer holds is catalog.ErrStaleRef.
func (f *Follower) SealedMetadata(v catalog.SegmentView) (*segment.Reader, error) {
	if v.State != catalog.Sealed {
		return nil, fmt.Errorf("follower: metadata of %s segment %d in %q", v.State, v.Index, v.Namespace)
	}
	m := f.cur.Load()
	if m == nil {
		return nil, catalog.ErrStaleRef
	}
	g, ok := m.gens[v.Generation]
	if !ok {
		return nil, fmt.Errorf("%w: %s segment %d generation %d", catalog.ErrStaleRef, v.Namespace, v.Index, v.Generation)
	}
	ctx := context.Background()
	footer, err := f.rd.Get(ctx, g.footerID)
	if err != nil {
		return nil, fmt.Errorf("follower: footer of %s segment %d: %w", g.ns, g.idx, err)
	}
	name := fmt.Sprintf("%s/%d@%d", g.ns, g.idx, g.id)
	return segment.OpenReaderParts(g.headerRaw, footer, func(i int) ([]byte, error) {
		if i < 0 || i >= len(g.blocks) {
			return nil, fmt.Errorf("follower: %s has no block %d", name, i)
		}
		b := g.blocks[i]
		return f.Fetch(ctx, catalog.BlockRef{
			MinSeq: b.MinSeq, MaxSeq: b.MaxSeq,
			MinWitnessedUS: b.MinWitnessedAt, MaxWitnessedUS: b.MaxWitnessedAt,
			Namespace: g.ns, Segment: g.idx, Block: i, Generation: g.id,
			Loc: catalog.ObjectBlock{ObjectID: g.blockIDs[i]},
		})
	}, segment.ReaderOptions{Name: name})
}
