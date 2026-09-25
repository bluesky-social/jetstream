package storagefake

import (
	"bytes"
	"context"
	"slices"

	"github.com/bluesky-social/jetstream/internal/catalog"
)

// readTx is a REPEATABLE READ READ ONLY transaction: it holds one committed
// state for its whole life.
type readTx struct {
	db     *DB
	s      *state
	seam   bool // yield to the scheduler; false for the fake's own checks
	closed bool
}

var _ catalog.ReadTx = (*readTx)(nil)

func (r *readTx) stmt(ctx context.Context, name string) error {
	if r.closed {
		return ErrTxDone
	}
	if r.seam {
		if err := r.db.yield(ctx, "read/"+name); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (r *readTx) Archive(ctx context.Context) (catalog.ArchiveRow, error) {
	if err := r.stmt(ctx, "archive"); err != nil {
		return catalog.ArchiveRow{}, err
	}
	return r.s.archive, nil
}

func (r *readTx) SegmentsSince(ctx context.Context, rev uint64) ([]catalog.SegmentRow, error) {
	if err := r.stmt(ctx, "segments"); err != nil {
		return nil, err
	}
	var out []catalog.SegmentRow
	for _, seg := range r.s.segments.values() {
		if seg.Revision > rev {
			out = append(out, seg)
		}
	}
	return out, nil
}

func (r *readTx) Generations(ctx context.Context, ids []uint64) ([]catalog.GenerationRow, error) {
	if err := r.stmt(ctx, "generations"); err != nil {
		return nil, err
	}
	var out []catalog.GenerationRow
	for _, id := range sortedUnique(ids) {
		if g, ok := r.s.generations.get(id); ok {
			g.Header = bytes.Clone(g.Header)
			out = append(out, g)
		}
	}
	return out, nil
}

func (r *readTx) GenerationBlocks(ctx context.Context, ids []uint64) ([]catalog.GenerationBlockRow, error) {
	if err := r.stmt(ctx, "generation_blocks"); err != nil {
		return nil, err
	}
	want := map[uint64]bool{}
	for _, id := range ids {
		want[id] = true
	}
	var out []catalog.GenerationBlockRow
	for _, gb := range r.s.genBlocks.values() {
		if want[gb.GenerationID] {
			out = append(out, gb)
		}
	}
	return out, nil
}

func (r *readTx) ActiveBlocksSince(ctx context.Context, rev uint64) ([]catalog.ActiveBlockRow, error) {
	if err := r.stmt(ctx, "active_blocks"); err != nil {
		return nil, err
	}
	var out []catalog.ActiveBlockRow
	for _, b := range r.s.activeBlocks.values() {
		if b.Revision > rev {
			out = append(out, b)
		}
	}
	return out, nil
}

func (r *readTx) ActiveBlockKeys(ctx context.Context) ([]catalog.ActiveBlockKey, error) {
	if err := r.stmt(ctx, "active_block_keys"); err != nil {
		return nil, err
	}
	vals := r.s.activeBlocks.values()
	out := make([]catalog.ActiveBlockKey, len(vals))
	for i, b := range vals {
		out[i] = b.Key()
	}
	return out, nil
}

func (r *readTx) HotBatches(ctx context.Context, framesFrom uint64) ([]catalog.HotBatchRow, error) {
	if err := r.stmt(ctx, "hot_batches"); err != nil {
		return nil, err
	}
	vals := r.s.hotBatches.values()
	for i := range vals {
		if vals[i].Inline && vals[i].FirstSeq >= framesFrom {
			vals[i].Frame = bytes.Clone(vals[i].Frame)
		} else {
			vals[i].Frame = nil
		}
	}
	return vals, nil
}

func (r *readTx) Objects(ctx context.Context, ids []uint64) ([]catalog.ObjectRow, error) {
	if err := r.stmt(ctx, "objects"); err != nil {
		return nil, err
	}
	var out []catalog.ObjectRow
	for _, id := range sortedUnique(ids) {
		if o, ok := r.s.objects.get(id); ok {
			out = append(out, o)
		}
	}
	return out, nil
}

func (r *readTx) MetaGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	if err := r.stmt(ctx, "meta_get"); err != nil {
		return nil, err
	}
	out := map[string][]byte{}
	for _, k := range keys {
		if v, ok := r.s.meta.get(string(k)); ok {
			out[string(k)] = bytes.Clone(v)
		}
	}
	return out, nil
}

func (r *readTx) Close(context.Context) error {
	r.closed = true
	return nil
}

func sortedUnique(ids []uint64) []uint64 {
	out := slices.Clone(ids)
	slices.Sort(out)
	return slices.Compact(out)
}
