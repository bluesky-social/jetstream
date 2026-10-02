package orchestrator

import (
	"context"
	"fmt"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/tombstone"
	"github.com/bluesky-social/jetstream/segment"
	"golang.org/x/sync/semaphore"
)

// compactionCatalog is the archive a compaction pass reads: the follower in
// disaggregated mode, the local catalog otherwise.
type compactionCatalog interface {
	catalog.Catalog
	Fetcher() catalog.Fetcher
}

func (o *Orchestrator) compactionCatalog() compactionCatalog {
	if d := o.cfg.Disaggregated; d != nil {
		return d.Catalog
	}
	return o.segments()
}

// compactionSegmentName names a Main segment in logs and errors.
func (o *Orchestrator) compactionSegmentName(idx uint64) string {
	if o.cfg.Disaggregated != nil {
		return fmt.Sprintf("%s/%d", catalog.Main, idx)
	}
	return o.segments().Path(catalog.Main, idx)
}

// setCompactionSchedule publishes the Cache-Control deadline (design §12.7):
// the next pass, the running pass's start, or zero for unknown. In
// disaggregated mode it commits the value for the followers too, in a fenced
// transaction, so a failure ends the session.
func (o *Orchestrator) setCompactionSchedule(ctx context.Context, at time.Time) error {
	o.cfg.CompactionSchedule.setNextCompactionAt(at)
	d := o.cfg.Disaggregated
	if d == nil {
		return nil
	}
	op := metastore.Op{Kind: metastore.OpSet, Key: []byte(catalog.CompactionDeadlineKey), Value: catalog.EncodeCompactionDeadline(at)}
	if _, err := d.Session.CommitMeta(ctx, []metastore.Op{op}); err != nil {
		return fmt.Errorf("orchestrator: compaction: publish deadline: %w", err)
	}
	return nil
}

// advanceCompactionWatermark moves compaction/seq from prior to next. In
// disaggregated mode it is the §12.1 compare-and-set: only the leader writes
// the key, so a stored value other than prior is corruption.
func (o *Orchestrator) advanceCompactionWatermark(ctx context.Context, prior uint64, priorOK bool, next uint64) error {
	d := o.cfg.Disaggregated
	if d == nil {
		return saveCompactionWatermark(o.cfg.Store, next)
	}
	var old []byte
	if priorOK {
		old = metastore.EncodeVersionedUint64LE(compactionWatermarkV1, prior)
	}
	if _, err := d.Session.CompareAndSetMeta(ctx, compactionWatermarkKey, old, metastore.EncodeVersionedUint64LE(compactionWatermarkV1, next)); err != nil {
		return fmt.Errorf("orchestrator: compaction: advance watermark to %d: %w", next, err)
	}
	return nil
}

// compactionReserve returns the shared decode budget's Reserve for one
// chunk's workers, or nil when the budget is unbounded. A block larger than
// the whole budget waits for all of it.
func compactionReserve(ctx context.Context, budget int64) func(int64) (func(), error) {
	if budget <= 0 {
		return nil
	}
	sem := semaphore.NewWeighted(budget)
	return func(n int64) (func(), error) {
		n = min(max(n, 1), budget)
		if err := sem.Acquire(ctx, n); err != nil {
			return nil, err
		}
		return func() { sem.Release(n) }, nil
	}
}

// maxTombstoneSeq is the highest seq in snap. No row at or above it can be
// dropped.
func maxTombstoneSeq(snap tombstone.Snapshot) uint64 {
	var top uint64
	for _, seq := range snap.Records {
		top = max(top, seq)
	}
	for _, ts := range snap.DIDs {
		top = max(top, ts.Seq)
	}
	return top
}

// rewriteSegmentDisaggregated is the §12.2 segment rewrite on the shared
// catalog: a sparse rewrite of the current generation, the upload of its new
// blocks and footer, and the publish.
//
// The rewrite reads under gctx, which a sibling worker's failure cancels.
// Upload and publish run under the pass's ctx instead: a failed catalog
// transaction ends the session, and a sibling's error must fail only the
// pass.
func (o *Orchestrator) rewriteSegmentDisaggregated(ctx, gctx context.Context, f sealedCompactionSegment, snap tombstone.Snapshot, rule *segment.Tombstones, reserve func(int64) (func(), error)) (compactionRewriteResult, error) {
	d := o.cfg.Disaggregated
	out := compactionRewriteResult{file: f, droppedByReason: map[string]uint64{}}
	parts, found, err := d.Catalog.GenerationParts(gctx, f.Idx)
	if err != nil {
		return out, fmt.Errorf("orchestrator: compaction: generation of %s: %w", f.Path, err)
	}
	if !found {
		return out, fmt.Errorf("orchestrator: compaction: sealed segment %s left the catalog mid-pass", f.Path)
	}
	out.blocks = len(parts.Blocks)
	footer, err := d.Objects.Get(gctx, parts.Footer.ID)
	if err != nil {
		return out, fmt.Errorf("orchestrator: compaction: fetch %s footer: %w", f.Path, err)
	}
	fetch := func(i int) ([]byte, error) {
		if i < 0 || i >= len(parts.Blocks) {
			return nil, fmt.Errorf("orchestrator: compaction: %s has no block %d", f.Path, i)
		}
		return d.Objects.Get(gctx, parts.Blocks[i].ID)
	}
	res, err := segment.SparseRewrite(parts.Header, footer, fetch, rule, segment.SparseOptions{
		Name:    f.Path,
		Reserve: reserve,
		OnDrop: func(ev *segment.Event, didLevel bool) {
			out.droppedByReason[snap.DropReason(ev, didLevel)]++
		},
	})
	out.blocksFetched = res.BlocksFetched
	if err != nil {
		return out, fmt.Errorf("orchestrator: compaction: rewrite %s: %w", f.Path, err)
	}
	if !res.Rewritten {
		return out, nil
	}

	objs := make([][]byte, 0, len(res.Frames)+1)
	for _, fr := range res.Frames {
		objs = append(objs, fr.Frame)
	}
	objs = append(objs, res.Footer)
	refs, err := d.Uploader.Upload(ctx, d.Session, objs)
	if err != nil {
		return out, fmt.Errorf("orchestrator: compaction: upload %s: %w", f.Path, err)
	}
	if err := o.simulateCrash(ctx, crashpoint.AfterCompactionUploadBeforePublish); err != nil {
		return out, err
	}
	blocks := make([]catalog.PublishBlock, len(parts.Blocks))
	for i, b := range parts.Blocks {
		blocks[i] = catalog.PublishBlock{Object: catalog.ObjectRef{ID: b.ID}, Reused: true, CompressedLength: b.Length}
	}
	for k, fr := range res.Frames {
		blocks[fr.Block] = catalog.PublishBlock{Object: refs[k], CompressedLength: int64(len(fr.Frame))}
	}
	if _, err := d.Session.PublishGeneration(ctx, catalog.Publish{
		Segment: f.Idx,
		Source:  parts.Generation,
		Header:  res.HeaderBytes,
		Footer:  refs[len(refs)-1],
		Blocks:  blocks,
	}); err != nil {
		return out, fmt.Errorf("orchestrator: compaction: publish %s: %w", f.Path, err)
	}
	out.result = segment.RewriteResult{Rewritten: true, RowsDropped: res.RowsDropped, BlocksTouched: uint32(len(res.Frames))}
	out.size = int64(res.Header.FooterOffset) + int64(len(res.Footer))
	return out, nil
}
