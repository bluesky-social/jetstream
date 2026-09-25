package storagefake

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/metastore"
)

// tx is a READ COMMITTED leader transaction. Every writer holds the archive
// row lock from its first write (normally the fence) to its end, so a
// writer's view is the state committed when it took the lock plus its own
// writes: nothing else can commit meanwhile. Before that first write it
// reads the latest committed state.
//
// PostgreSQL would only row-lock the rows an unfenced write touches; here
// any write takes the archive lock. No script can observe the difference,
// and it keeps the fake's serialization argument trivial.
type tx struct {
	db       *DB
	cl       *Client
	kind     catalog.TxKind
	connLost *Fault

	locked  bool
	s       *state // mutable; set once locked
	notify  []uint64
	stmts   int
	done    bool
	aborted error
}

var _ catalog.Tx = (*tx)(nil)

// stmt starts one statement. write statements take the archive row lock.
func (t *tx) stmt(ctx context.Context, name string, write bool) error {
	if t.done {
		return ErrTxDone
	}
	if t.aborted != nil {
		return fmt.Errorf("%w: %w", ErrTxAborted, t.aborted)
	}
	if err := t.db.yield(ctx, t.cl, "stmt/"+name); err != nil {
		if errors.Is(err, ErrKilled) {
			t.release()
		}
		return t.abort(err)
	}
	if err := ctx.Err(); err != nil {
		return t.abort(err)
	}
	t.stmts++
	if f := t.connLost; f != nil && f.Statement == t.stmts {
		f.fired.Add(1)
		t.connLost = nil
		t.release()
		return t.abort(fmt.Errorf("%s: %w", name, ErrConnLost))
	}
	if write && !t.locked {
		if err := t.db.lockArchive(ctx); err != nil {
			return t.abort(err)
		}
		t.locked = true
		t.s = t.db.current().child()
	}
	return nil
}

// abort puts the transaction in the aborted state. Locks stay held until
// Rollback, as in PostgreSQL, unless the connection died.
func (t *tx) abort(err error) error {
	if t.aborted == nil {
		t.aborted = err
	}
	return err
}

func (t *tx) release() {
	if t.locked {
		t.locked = false
		t.s = nil
		t.db.unlockArchive()
	}
}

func (t *tx) view() *state {
	if t.s != nil {
		return t.s
	}
	return t.db.current()
}

func (t *tx) now() time.Time { return t.db.now() }

func (t *tx) FenceBump(ctx context.Context, epoch uint64) (uint64, bool, error) {
	held := t.locked
	if err := t.stmt(ctx, "fence", true); err != nil {
		return 0, false, err
	}
	if t.s.archive.WriterEpoch != epoch {
		// No row matched, so the UPDATE locked nothing.
		if !held {
			t.release()
		}
		return 0, false, nil
	}
	t.s.archive.CatalogRevision++
	return t.s.archive.CatalogRevision, true, nil
}

func (t *tx) MetaGetForUpdate(ctx context.Context, key []byte) ([]byte, bool, error) {
	if err := t.stmt(ctx, "meta_get_for_update", true); err != nil {
		return nil, false, err
	}
	v, ok := t.s.meta.get(string(key))
	return cloneBytes(v), ok, nil
}

func (t *tx) ApplyMeta(ctx context.Context, ops []metastore.Op) error {
	if err := t.stmt(ctx, "apply_meta", true); err != nil {
		return err
	}
	applyMeta(t.s.meta, ops)
	return nil
}

func applyMeta(m *layer[string, []byte], ops []metastore.Op) {
	for _, op := range ops {
		switch op.Kind {
		case metastore.OpSet:
			v := bytes.Clone(op.Value)
			if v == nil {
				v = []byte{} // value is NOT NULL; an empty value stays present
			}
			m.set(string(op.Key), v)
		case metastore.OpDelete:
			m.del(string(op.Key))
		case metastore.OpDeleteRange:
			ks := m.keys()
			lo, _ := slices.BinarySearch(ks, string(op.Key))
			for _, k := range ks[lo:] {
				if k >= string(op.End) {
					break
				}
				m.del(k)
			}
		}
	}
}

func (t *tx) FindAvailableObject(ctx context.Context, sha [32]byte, maxUnrefAge time.Duration) (catalog.ObjectRow, bool, error) {
	if err := t.stmt(ctx, "find_available_object", false); err != nil {
		return catalog.ObjectRow{}, false, err
	}
	s := t.view()
	id, ok := s.objectsBySHA.get(string(sha[:]))
	if !ok {
		return catalog.ObjectRow{}, false, nil
	}
	row, _ := s.objects.get(id)
	if maxUnrefAge > 0 && !row.UnreferencedAt.IsZero() && !row.UnreferencedAt.After(t.now().Add(-maxUnrefAge)) {
		return catalog.ObjectRow{}, false, nil
	}
	return row, true, nil
}

func (t *tx) InsertObjects(ctx context.Context, objs []catalog.NewObject) ([]uint64, error) {
	if err := t.stmt(ctx, "insert_objects", true); err != nil {
		return nil, err
	}
	ids := make([]uint64, len(objs))
	now := t.now()
	for i, o := range objs {
		if o.Length <= 0 {
			return nil, t.abort(violation("objects_byte_length_check", "byte_length %d", o.Length))
		}
		// The sequence advances even if the statement fails, as nextval does.
		t.db.mu.Lock()
		id := t.db.nextObj
		t.db.nextObj++
		t.db.mu.Unlock()
		if _, dup := t.s.objectKeys.get(string(o.Key[:])); dup {
			return nil, t.abort(violation("objects_key_key", "key %x exists", o.Key))
		}
		t.s.objectKeys.set(string(o.Key[:]), id)
		t.s.objects.set(id, catalog.ObjectRow{
			ID: id, Key: o.Key, SHA256: o.SHA256, Length: o.Length,
			State: catalog.ObjectUploading, CreatedAt: now,
		})
		ids[i] = id
	}
	return ids, nil
}

func (t *tx) SetObjectAvailable(ctx context.Context, id uint64) (bool, error) {
	if err := t.stmt(ctx, "set_object_available", true); err != nil {
		return false, err
	}
	row, ok := t.s.objects.get(id)
	if !ok || row.State != catalog.ObjectUploading {
		return false, nil
	}
	sha := string(row.SHA256[:])
	if other, dup := t.s.objectsBySHA.get(sha); dup {
		return false, t.abort(violation("objects_sha256_available", "object %d already has sha256 %x", other, row.SHA256))
	}
	row.State = catalog.ObjectAvailable
	t.s.objects.set(id, row)
	t.s.objectsBySHA.set(sha, id)
	return true, nil
}

func (t *tx) RefCheck(ctx context.Context, ids []uint64) ([]uint64, error) {
	if err := t.stmt(ctx, "ref_check", true); err != nil {
		return nil, err
	}
	var missing []uint64
	for _, id := range ids {
		row, ok := t.s.objects.get(id)
		if !ok || row.State != catalog.ObjectAvailable {
			missing = append(missing, id)
			continue
		}
		if !row.UnreferencedAt.IsZero() {
			row.UnreferencedAt = time.Time{}
			t.s.objects.set(id, row)
		}
	}
	return missing, nil
}

func (t *tx) objectFK(table string, id uint64) error {
	if _, ok := t.s.objects.get(id); !ok {
		return t.abort(violation(table+"_object_id_fkey", "object %d does not exist", id))
	}
	return nil
}

func (t *tx) InsertHotBatch(ctx context.Context, row catalog.HotBatchRow) error {
	if err := t.stmt(ctx, "insert_hot_batch", true); err != nil {
		return err
	}
	if err := t.bigints("hot_batches", row.FirstSeq, row.LastSeq, row.Epoch, row.Revision, row.ObjectID); err != nil {
		return err
	}
	switch {
	case row.EventCount == 0 || row.EventCount > math.MaxInt32:
		return t.abort(violation("hot_batches_event_count_check", "event_count %d", row.EventCount))
	case row.LastSeq-row.FirstSeq+1 != uint64(row.EventCount):
		return t.abort(violation("hot_batches_check", "[%d,%d] with event_count %d", row.FirstSeq, row.LastSeq, row.EventCount))
	case (row.Frame == nil) == (row.ObjectID == 0):
		return t.abort(violation("hot_batches_check1", "batch %d needs exactly one of frame and object_id", row.FirstSeq))
	}
	if _, dup := t.s.hotBatches.get(row.FirstSeq); dup {
		return t.abort(violation("hot_batches_pkey", "first_seq %d exists", row.FirstSeq))
	}
	if row.ObjectID != 0 {
		if err := t.objectFK("hot_batches", row.ObjectID); err != nil {
			return err
		}
	}
	row.Frame = cloneBytes(row.Frame)
	row.Inline = row.Frame != nil
	row.CommittedAt = t.now()
	t.s.hotBatches.set(row.FirstSeq, row)
	return nil
}

func (t *tx) DeleteHotBatches(ctx context.Context, lo, hi uint64) ([]catalog.HotBatchSpan, error) {
	if err := t.stmt(ctx, "delete_hot_batches", true); err != nil {
		return nil, err
	}
	var out []catalog.HotBatchSpan
	ks := t.s.hotBatches.keys()
	i, _ := slices.BinarySearch(ks, lo)
	for _, k := range ks[i:] {
		if k > hi {
			break
		}
		row, _ := t.s.hotBatches.get(k)
		out = append(out, catalog.HotBatchSpan{FirstSeq: row.FirstSeq, LastSeq: row.LastSeq, EventCount: row.EventCount, ObjectID: row.ObjectID})
		t.s.hotBatches.del(k)
	}
	return out, nil
}

func (t *tx) ActiveSegment(ctx context.Context, ns catalog.Namespace) (catalog.SegmentRow, bool, error) {
	if err := t.stmt(ctx, "active_segment", true); err != nil {
		return catalog.SegmentRow{}, false, err
	}
	idx, ok := t.s.oneActive.get(string(ns))
	if !ok {
		return catalog.SegmentRow{}, false, nil
	}
	row, ok := t.s.segments.get(segKey(ns, idx))
	return row, ok, nil
}

// blocksOf returns a segment's active blocks in ordinal order.
func blocksOf(s *state, ns catalog.Namespace, idx uint64) []catalog.ActiveBlockRow {
	prefix := blockPrefix(ns, idx)
	ks := s.activeBlocks.keys()
	i, _ := slices.BinarySearch(ks, prefix)
	var out []catalog.ActiveBlockRow
	for _, k := range ks[i:] {
		if !strings.HasPrefix(k, prefix) {
			break
		}
		row, _ := s.activeBlocks.get(k)
		out = append(out, row)
	}
	return out
}

func (t *tx) LastActiveBlock(ctx context.Context, ns catalog.Namespace, idx uint64) (catalog.ActiveBlockRow, bool, error) {
	if err := t.stmt(ctx, "last_active_block", false); err != nil {
		return catalog.ActiveBlockRow{}, false, err
	}
	rows := blocksOf(t.view(), ns, idx)
	if len(rows) == 0 {
		return catalog.ActiveBlockRow{}, false, nil
	}
	return rows[len(rows)-1], true, nil
}

func (t *tx) ActiveBlocksForUpdate(ctx context.Context, ns catalog.Namespace, idx uint64) ([]catalog.ActiveBlockRow, error) {
	if err := t.stmt(ctx, "active_blocks_for_update", true); err != nil {
		return nil, err
	}
	return blocksOf(t.s, ns, idx), nil
}

func (t *tx) segmentFK(table string, ns catalog.Namespace, idx uint64) error {
	if _, ok := t.s.segments.get(segKey(ns, idx)); !ok {
		return t.abort(violation(table+"_namespace_segment_index_fkey", "segment (%s, %d) does not exist", ns, idx))
	}
	return nil
}

func (t *tx) InsertActiveBlock(ctx context.Context, row catalog.ActiveBlockRow) error {
	if err := t.stmt(ctx, "insert_active_block", true); err != nil {
		return err
	}
	if err := t.bigints("active_segment_blocks", row.Segment, row.ObjectID, row.MinSeq, row.MaxSeq, row.Revision); err != nil {
		return err
	}
	if row.EventCount == 0 || row.EventCount > math.MaxInt32 {
		return t.abort(violation("active_segment_blocks_event_count_check", "event_count %d", row.EventCount))
	}
	if row.Ordinal < 0 || row.Ordinal > math.MaxInt32 {
		return t.abort(violation("active_segment_blocks_ordinal", "ordinal %d out of integer range", row.Ordinal))
	}
	k := blockKey(row.Namespace, row.Segment, row.Ordinal)
	if _, dup := t.s.activeBlocks.get(k); dup {
		return t.abort(violation("active_segment_blocks_pkey", "(%s, %d, %d) exists", row.Namespace, row.Segment, row.Ordinal))
	}
	if err := t.objectFK("active_segment_blocks", row.ObjectID); err != nil {
		return err
	}
	if err := t.segmentFK("active_segment_blocks", row.Namespace, row.Segment); err != nil {
		return err
	}
	t.s.activeBlocks.set(k, row)
	return nil
}

func (t *tx) DeleteActiveBlocks(ctx context.Context, ns catalog.Namespace, idx uint64) (int, error) {
	if err := t.stmt(ctx, "delete_active_blocks", true); err != nil {
		return 0, err
	}
	rows := blocksOf(t.s, ns, idx)
	for _, r := range rows {
		t.s.activeBlocks.del(blockKey(ns, idx, r.Ordinal))
	}
	return len(rows), nil
}

func (t *tx) InsertGeneration(ctx context.Context, row catalog.GenerationRow) (uint64, error) {
	if err := t.stmt(ctx, "insert_generation", true); err != nil {
		return 0, err
	}
	if err := t.bigints("segment_generations", row.Segment, row.FooterObjectID, row.Revision); err != nil {
		return 0, err
	}
	t.db.mu.Lock()
	id := t.db.nextGen
	t.db.nextGen++
	t.db.mu.Unlock()
	if len(row.Header) != 256 {
		return 0, t.abort(violation("segment_generations_header_check", "header is %d bytes", len(row.Header)))
	}
	if err := t.objectFK("segment_generations_footer", row.FooterObjectID); err != nil {
		return 0, err
	}
	if err := t.segmentFK("segment_generations", row.Namespace, row.Segment); err != nil {
		return 0, err
	}
	row.ID = id
	row.Header = bytes.Clone(row.Header)
	row.CreatedAt = t.now()
	t.s.generations.set(id, row)
	return id, nil
}

func (t *tx) InsertGenerationBlocks(ctx context.Context, rows []catalog.GenerationBlockRow) error {
	if err := t.stmt(ctx, "insert_generation_blocks", true); err != nil {
		return err
	}
	for _, r := range rows {
		if r.Ordinal < 0 || r.Ordinal > math.MaxInt32 {
			return t.abort(violation("generation_blocks_ordinal", "ordinal %d out of integer range", r.Ordinal))
		}
		if _, ok := t.s.generations.get(r.GenerationID); !ok {
			return t.abort(violation("generation_blocks_generation_id_fkey", "generation %d does not exist", r.GenerationID))
		}
		if err := t.objectFK("generation_blocks", r.ObjectID); err != nil {
			return err
		}
		k := genBlockKey(r.GenerationID, r.Ordinal)
		if _, dup := t.s.genBlocks.get(k); dup {
			return t.abort(violation("generation_blocks_pkey", "(%d, %d) exists", r.GenerationID, r.Ordinal))
		}
		t.s.genBlocks.set(k, r)
	}
	return nil
}

func (t *tx) SealSegment(ctx context.Context, ns catalog.Namespace, idx, gen, revision uint64) (bool, error) {
	if err := t.stmt(ctx, "seal_segment", true); err != nil {
		return false, err
	}
	if gen == 0 {
		// Zero stands for NULL in SegmentRow; sequences start at 1.
		return false, t.abort(violation("segments_check", "sealed segment needs a generation"))
	}
	k := segKey(ns, idx)
	row, ok := t.s.segments.get(k)
	if !ok || row.State != catalog.Active {
		return false, nil
	}
	row.State, row.GenerationID, row.Revision = catalog.Sealed, gen, revision
	t.s.segments.set(k, row)
	t.s.oneActive.del(string(ns))
	return true, nil
}

func (t *tx) InsertSegment(ctx context.Context, row catalog.SegmentRow) error {
	if err := t.stmt(ctx, "insert_segment", true); err != nil {
		return err
	}
	if err := t.bigints("segments", row.Index, row.GenerationID, row.Revision); err != nil {
		return err
	}
	switch {
	case !row.Namespace.Valid():
		return t.abort(violation("segments_namespace_check", "namespace %q", row.Namespace))
	case row.State != catalog.Active && row.State != catalog.Sealed:
		return t.abort(violation("segments_state_check", "state %v", row.State))
	case (row.State == catalog.Active) != (row.GenerationID == 0):
		return t.abort(violation("segments_check", "%s segment with generation %d", row.State, row.GenerationID))
	}
	k := segKey(row.Namespace, row.Index)
	if _, dup := t.s.segments.get(k); dup {
		return t.abort(violation("segments_pkey", "(%s, %d) exists", row.Namespace, row.Index))
	}
	if row.State == catalog.Active {
		if other, dup := t.s.oneActive.get(string(row.Namespace)); dup {
			return t.abort(violation("segments_one_active", "%s segment %d is already active", row.Namespace, other))
		}
		t.s.oneActive.set(string(row.Namespace), row.Index)
	}
	t.s.segments.set(k, row)
	return nil
}

func (t *tx) DeleteNamespace(ctx context.Context, ns catalog.Namespace) error {
	if err := t.stmt(ctx, "delete_namespace", true); err != nil {
		return err
	}
	for id, g := range t.s.generations.all() {
		if g.Namespace != ns {
			continue
		}
		prefix := genBlockPrefix(id)
		for _, k := range t.s.genBlocks.keys() {
			if strings.HasPrefix(k, prefix) {
				t.s.genBlocks.del(k)
			}
		}
		t.s.generations.del(id)
	}
	for k, b := range t.s.activeBlocks.all() {
		if b.Namespace == ns {
			t.s.activeBlocks.del(k)
		}
	}
	for k, seg := range t.s.segments.all() {
		if seg.Namespace == ns {
			t.s.segments.del(k)
		}
	}
	t.s.oneActive.del(string(ns))
	return nil
}

func (t *tx) Notify(ctx context.Context, revision uint64) error {
	// NOTIFY serializes committers on a global lock in PostgreSQL too.
	if err := t.stmt(ctx, "notify", true); err != nil {
		return err
	}
	t.notify = append(t.notify, revision)
	return nil
}

func (t *tx) Commit(ctx context.Context) error {
	if t.done {
		return ErrTxDone
	}
	if t.aborted != nil {
		t.finish()
		return fmt.Errorf("storagefake: commit rolled back: %w", t.aborted)
	}
	if err := t.db.yield(ctx, t.cl, "commit/"+string(t.kind)); err != nil {
		t.finish()
		return err
	}
	if f := t.connLost; f != nil && f.Statement == 0 {
		f.fired.Add(1)
		t.finish()
		return fmt.Errorf("commit: %w", ErrConnLost)
	}
	if !t.locked {
		t.finish()
		return nil
	}
	f := t.db.commitFault(t.kind)
	if f != nil && f.Kind == FaultCommitFails {
		t.finish()
		return fmt.Errorf("storagefake: injected commit failure (%s)", f)
	}
	t.db.publish(t.s, t.notify, f != nil && f.Kind == FaultNotifyLost)
	t.finish()
	if f != nil && f.Kind == FaultCommitLost {
		return fmt.Errorf("commit: %w", ErrCommitLost)
	}
	return nil
}

func (t *tx) Rollback(context.Context) error {
	if !t.done {
		t.finish()
	}
	return nil
}

func (t *tx) finish() {
	t.release()
	t.done = true
}

// bigints rejects values a PostgreSQL bigint cannot hold.
func (t *tx) bigints(table string, vs ...uint64) error {
	for _, v := range vs {
		if v > math.MaxInt64 {
			return t.abort(violation(table+"_bigint", "value %d out of bigint range", v))
		}
	}
	return nil
}
