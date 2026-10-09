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

	// queuing is set while a queued statement runs. results holds what
	// queued statements return, delivered as pgstore delivers them: with
	// the next statement that is not queued, or Commit.
	queuing bool
	results []func()
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
	if !t.queuing {
		t.deliver()
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

// errFenced aborts a transaction whose fence matched nothing.
var errFenced = errors.New("storagefake: the fence matched no row")

// queue runs a queued statement. It reports the statement's failure the
// way pgstore does: not from the call, which pgstore sends nowhere, but
// from the next statement or Commit (catalog.Tx). The transaction is
// already aborted, so either one returns it.
func (t *tx) queue(stmt func() error) error {
	t.queuing = true
	err := stmt()
	t.queuing = false
	if err != nil {
		_ = t.abort(err)
	}
	return nil
}

// deliver hands queued statements' results to the script.
func (t *tx) deliver() {
	for _, f := range t.results {
		f()
	}
	t.results = nil
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

func (t *tx) FenceBump(ctx context.Context, epoch uint64, reads ...catalog.Read) (uint64, bool, error) {
	held := t.locked
	if err := t.stmt(ctx, "fence", true); err != nil {
		return 0, false, err
	}
	if t.s.archive.WriterEpoch != epoch {
		// No row matched, so the UPDATE locked nothing, and pgstore's fence
		// then fails the statement so that nothing pipelined after it runs.
		if !held {
			t.release()
		}
		_ = t.abort(errFenced)
		return 0, false, nil
	}
	t.s.archive.CatalogRevision++
	rev := t.s.archive.CatalogRevision
	if err := t.Read(ctx, reads...); err != nil {
		return 0, false, err
	}
	return rev, true, nil
}

// FenceBumpAt runs as pgstore pipelines it: queued, so its failure aborts
// the transaction and reaches the script from Commit.
func (t *tx) FenceBumpAt(ctx context.Context, epoch, rev uint64, checks ...catalog.MetaCheck) error {
	return t.queue(func() error {
		held := t.locked
		if err := t.stmt(ctx, "fence_at", true); err != nil {
			return err
		}
		if t.s.archive.WriterEpoch != epoch || t.s.archive.CatalogRevision+1 != rev {
			// The UPDATE matched nothing, so it locked nothing.
			if !held {
				t.release()
			}
			return fmt.Errorf("storagefake: fence at revision %d: %w", rev, catalog.ErrPrecondition)
		}
		t.s.archive.CatalogRevision++
		for _, c := range checks {
			if err := t.stmt(ctx, "meta_check", false); err != nil {
				return err
			}
			if v, ok := t.s.meta.get(string(c.Key)); !ok || !bytes.Equal(v, c.Value) {
				return fmt.Errorf("storagefake: %q is not %q: %w", c.Key, c.Value, catalog.ErrPrecondition)
			}
		}
		return nil
	})
}

// Read runs each read as its own statement, which is what PostgreSQL does
// with a pipeline: other transactions can run between them, and only locks
// keep them out.
func (t *tx) Read(ctx context.Context, reads ...catalog.Read) error {
	for _, r := range reads {
		if err := t.read(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

func (t *tx) read(ctx context.Context, r catalog.Read) error {
	switch r := r.(type) {
	case *catalog.MetaRead:
		if err := t.stmt(ctx, "meta_get_for_update", true); err != nil {
			return err
		}
		v, ok := t.s.meta.get(string(r.Key))
		r.Value, r.Found = cloneBytes(v), ok
	case *catalog.ActiveSegmentRead:
		if err := t.stmt(ctx, "active_segment", true); err != nil {
			return err
		}
		r.Row, r.Found = catalog.SegmentRow{}, false
		if idx, ok := t.s.oneActive.get(string(r.Namespace)); ok {
			r.Row, r.Found = t.s.segments.get(segKey(r.Namespace, idx))
		}
	case *catalog.LastActiveBlockRead:
		if err := t.stmt(ctx, "last_active_block", false); err != nil {
			return err
		}
		s := t.view()
		r.Row, r.Found = catalog.ActiveBlockRow{}, false
		if idx, ok := s.oneActive.get(string(r.Namespace)); ok {
			if rows := blocksOf(s, r.Namespace, idx); len(rows) > 0 {
				r.Row, r.Found = rows[len(rows)-1], true
			}
		}
	case *catalog.AvailableObjectsRead:
		if err := t.stmt(ctx, "available_objects", true); err != nil {
			return err
		}
		r.Rows = map[[32]byte]catalog.ObjectRow{}
		for _, sha := range r.SHA256 {
			id, ok := t.s.objectsBySHA.get(string(sha[:]))
			if !ok {
				continue
			}
			row, _ := t.s.objects.get(id)
			if r.MaxUnrefAge > 0 && !row.UnreferencedAt.IsZero() && !row.UnreferencedAt.After(t.now().Add(-r.MaxUnrefAge)) {
				continue
			}
			r.Rows[sha] = row
		}
	case *catalog.ObjectsRead:
		if err := t.stmt(ctx, "objects_for_update", true); err != nil {
			return err
		}
		r.Rows = map[uint64]catalog.ObjectRow{}
		for _, id := range r.IDs {
			if row, ok := t.s.objects.get(id); ok {
				r.Rows[id] = row
			}
		}
	default:
		return t.abort(fmt.Errorf("storagefake: unknown read %T", r))
	}
	return nil
}

func (t *tx) ApplyMeta(ctx context.Context, ops []metastore.Op) error {
	return t.queue(func() error { return t.applyMeta(ctx, ops) })
}

func (t *tx) applyMeta(ctx context.Context, ops []metastore.Op) error {
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

func (t *tx) InsertObjects(ctx context.Context, objs []catalog.NewObject, ids []uint64) error {
	return t.queue(func() error { return t.insertObjects(ctx, objs, ids) })
}

func (t *tx) insertObjects(ctx context.Context, objs []catalog.NewObject, ids []uint64) error {
	if err := t.stmt(ctx, "insert_objects", true); err != nil {
		return err
	}
	if len(ids) != len(objs) {
		return t.abort(fmt.Errorf("storagefake: %d ids for %d objects", len(ids), len(objs)))
	}
	got := make([]uint64, len(objs))
	now := t.now()
	for i, o := range objs {
		if o.Length <= 0 {
			return t.abort(violation("objects_byte_length_check", "byte_length %d", o.Length))
		}
		// The sequence advances even if the statement fails, as nextval does.
		t.db.mu.Lock()
		id := t.db.nextObj
		t.db.nextObj++
		t.db.mu.Unlock()
		if _, dup := t.s.objectKeys.get(string(o.Key[:])); dup {
			return t.abort(violation("objects_key_key", "key %x exists", o.Key))
		}
		t.s.objectKeys.set(string(o.Key[:]), id)
		t.s.objects.set(id, catalog.ObjectRow{
			ID: id, Key: o.Key, SHA256: o.SHA256, Length: o.Length,
			State: catalog.ObjectUploading, CreatedAt: now,
		})
		got[i] = id
	}
	t.results = append(t.results, func() { copy(ids, got) })
	return nil
}

func (t *tx) SetObjectsAvailable(ctx context.Context, ids []uint64) error {
	return t.queue(func() error { return t.setObjectsAvailable(ctx, ids) })
}

func (t *tx) setObjectsAvailable(ctx context.Context, ids []uint64) error {
	if err := t.stmt(ctx, "set_objects_available", true); err != nil {
		return err
	}
	for _, id := range ids {
		row, ok := t.s.objects.get(id)
		if !ok || row.State != catalog.ObjectUploading {
			continue
		}
		sha := string(row.SHA256[:])
		if other, dup := t.s.objectsBySHA.get(sha); dup {
			return t.abort(violation("objects_sha256_available", "object %d already has sha256 %x", other, row.SHA256))
		}
		row.State = catalog.ObjectAvailable
		t.s.objects.set(id, row)
		t.s.objectsBySHA.set(sha, id)
	}
	return nil
}

func (t *tx) ClearUnreferenced(ctx context.Context, ids []uint64) error {
	return t.queue(func() error { return t.clearUnreferenced(ctx, ids) })
}

func (t *tx) clearUnreferenced(ctx context.Context, ids []uint64) error {
	if err := t.stmt(ctx, "clear_unreferenced", true); err != nil {
		return err
	}
	for _, id := range ids {
		row, ok := t.s.objects.get(id)
		if ok && row.State == catalog.ObjectAvailable && !row.UnreferencedAt.IsZero() {
			row.UnreferencedAt = time.Time{}
			t.s.objects.set(id, row)
		}
	}
	return nil
}

func (t *tx) objectFK(table string, id uint64) error {
	if _, ok := t.s.objects.get(id); !ok {
		return t.abort(violation(table+"_object_id_fkey", "object %d does not exist", id))
	}
	return nil
}

func (t *tx) InsertHotBatch(ctx context.Context, row catalog.HotBatchRow) error {
	return t.queue(func() error { return t.insertHotBatch(ctx, row) })
}

func (t *tx) insertHotBatch(ctx context.Context, row catalog.HotBatchRow) error {
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
	return t.queue(func() error { return t.insertActiveBlock(ctx, row) })
}

func (t *tx) insertActiveBlock(ctx context.Context, row catalog.ActiveBlockRow) error {
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
	return t.queue(func() error { return t.insertGenerationBlocks(ctx, rows) })
}

func (t *tx) insertGenerationBlocks(ctx context.Context, rows []catalog.GenerationBlockRow) error {
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
	return t.queue(func() error { return t.insertSegment(ctx, row) })
}

func (t *tx) insertSegment(ctx context.Context, row catalog.SegmentRow) error {
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
	return t.queue(func() error { return t.deleteNamespace(ctx, ns) })
}

func (t *tx) deleteNamespace(ctx context.Context, ns catalog.Namespace) error {
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

func (t *tx) SegmentForUpdate(ctx context.Context, ns catalog.Namespace, idx uint64) (catalog.SegmentRow, bool, error) {
	if err := t.stmt(ctx, "segment_for_update", true); err != nil {
		return catalog.SegmentRow{}, false, err
	}
	row, ok := t.s.segments.get(segKey(ns, idx))
	return row, ok, nil
}

func (t *tx) Generation(ctx context.Context, id uint64) (catalog.GenerationRow, bool, error) {
	if err := t.stmt(ctx, "generation", false); err != nil {
		return catalog.GenerationRow{}, false, err
	}
	g, ok := t.view().generations.get(id)
	g.Header = bytes.Clone(g.Header)
	return g, ok, nil
}

// genBlocksOf returns a generation's blocks in ordinal order.
func genBlocksOf(s *state, gen uint64) []catalog.GenerationBlockRow {
	prefix := genBlockPrefix(gen)
	ks := s.genBlocks.keys()
	i, _ := slices.BinarySearch(ks, prefix)
	var out []catalog.GenerationBlockRow
	for _, k := range ks[i:] {
		if !strings.HasPrefix(k, prefix) {
			break
		}
		row, _ := s.genBlocks.get(k)
		out = append(out, row)
	}
	return out
}

func (t *tx) BlocksOfGeneration(ctx context.Context, id uint64) ([]catalog.GenerationBlockRow, error) {
	if err := t.stmt(ctx, "blocks_of_generation", false); err != nil {
		return nil, err
	}
	return genBlocksOf(t.view(), id), nil
}

func (t *tx) SetSegmentGeneration(ctx context.Context, ns catalog.Namespace, idx, gen, revision uint64) (bool, error) {
	if err := t.stmt(ctx, "set_segment_generation", true); err != nil {
		return false, err
	}
	if err := t.bigints("segments", gen, revision); err != nil {
		return false, err
	}
	if gen == 0 {
		return false, t.abort(violation("segments_check", "sealed segment needs a generation"))
	}
	k := segKey(ns, idx)
	row, ok := t.s.segments.get(k)
	if !ok || row.State != catalog.Sealed {
		return false, nil
	}
	row.GenerationID, row.Revision = gen, revision
	t.s.segments.set(k, row)
	return true, nil
}

func (t *tx) DeleteGeneration(ctx context.Context, id uint64) (bool, error) {
	if err := t.stmt(ctx, "delete_generation", true); err != nil {
		return false, err
	}
	if _, ok := t.s.generations.get(id); !ok {
		return false, nil
	}
	for _, gb := range genBlocksOf(t.s, id) {
		t.s.genBlocks.del(genBlockKey(id, gb.Ordinal))
	}
	t.s.generations.del(id)
	return true, nil
}

// referenced returns every object ID some row references: the four §13
// NOT EXISTS checks, evaluated once for the whole statement.
func referenced(s *state) map[uint64]bool {
	refs := map[uint64]bool{}
	for _, h := range s.hotBatches.all() {
		if h.ObjectID != 0 {
			refs[h.ObjectID] = true
		}
	}
	for _, a := range s.activeBlocks.all() {
		refs[a.ObjectID] = true
	}
	for _, gb := range s.genBlocks.all() {
		refs[gb.ObjectID] = true
	}
	for _, g := range s.generations.all() {
		refs[g.FooterObjectID] = true
	}
	return refs
}

func (t *tx) MarkUnreferenced(ctx context.Context, after uint64, limit int) (catalog.MarkPage, error) {
	if err := t.stmt(ctx, "mark_unreferenced", true); err != nil {
		return catalog.MarkPage{}, err
	}
	var page catalog.MarkPage
	refs := referenced(t.s)
	now := t.now()
	ks := t.s.objects.keys()
	i, _ := slices.BinarySearch(ks, after+1)
	for _, id := range ks[i:] {
		if page.Scanned >= limit {
			break
		}
		row, _ := t.s.objects.get(id)
		if row.State != catalog.ObjectAvailable || !row.UnreferencedAt.IsZero() {
			continue
		}
		page.Scanned++
		page.Last = id
		if refs[id] {
			continue
		}
		row.UnreferencedAt = now
		t.s.objects.set(id, row)
		page.Marked++
	}
	return page, nil
}

func (t *tx) ClaimObjects(ctx context.Context, gcDelay, orphanAge time.Duration, limit int) ([]catalog.ObjectRow, error) {
	if err := t.stmt(ctx, "claim_objects", true); err != nil {
		return nil, err
	}
	now := t.now()
	var out []catalog.ObjectRow
	for _, id := range t.s.objects.keys() {
		if len(out) >= limit {
			break
		}
		row, _ := t.s.objects.get(id)
		switch {
		case row.State == catalog.ObjectAvailable && !row.UnreferencedAt.IsZero() && row.UnreferencedAt.Before(now.Add(-gcDelay)):
			// Leaving 'available' leaves the partial unique index too.
			t.s.objectsBySHA.del(string(row.SHA256[:]))
		case row.State == catalog.ObjectUploading && row.CreatedAt.Before(now.Add(-orphanAge)):
		default:
			continue
		}
		out = append(out, row)
		row.State = catalog.ObjectDeleting
		t.s.objects.set(id, row)
	}
	return out, nil
}

func (t *tx) DeletingObjects(ctx context.Context, limit int) ([]catalog.ObjectRow, error) {
	if err := t.stmt(ctx, "deleting_objects", false); err != nil {
		return nil, err
	}
	s := t.view()
	var out []catalog.ObjectRow
	for _, id := range s.objects.keys() {
		if len(out) >= limit {
			break
		}
		if row, _ := s.objects.get(id); row.State == catalog.ObjectDeleting {
			out = append(out, row)
		}
	}
	return out, nil
}

func (t *tx) ReferencedObjects(ctx context.Context, ids []uint64) ([]uint64, error) {
	if err := t.stmt(ctx, "referenced_objects", false); err != nil {
		return nil, err
	}
	refs := referenced(t.view())
	var out []uint64
	for _, id := range sortedUnique(ids) {
		if refs[id] {
			out = append(out, id)
		}
	}
	return out, nil
}

func (t *tx) ForgetObjects(ctx context.Context, ids []uint64) (int, error) {
	if err := t.stmt(ctx, "forget_objects", true); err != nil {
		return 0, err
	}
	refs := referenced(t.s)
	n := 0
	for _, id := range sortedUnique(ids) {
		row, ok := t.s.objects.get(id)
		if !ok || row.State != catalog.ObjectDeleting {
			continue
		}
		if refs[id] {
			// Every referencing foreign key is ON DELETE NO ACTION.
			return 0, t.abort(violation("objects_referenced", "object %d is still referenced", id))
		}
		t.s.objects.del(id)
		t.s.objectKeys.del(string(row.Key[:]))
		n++
	}
	return n, nil
}

func (t *tx) Notify(ctx context.Context, revision uint64) error {
	return t.queue(func() error { return t.notifyStmt(ctx, revision) })
}

func (t *tx) notifyStmt(ctx context.Context, revision uint64) error {
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
	t.deliver()
	if !t.locked {
		t.finish()
		return nil
	}
	f := t.db.commitFault(t.cl, t.kind)
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
