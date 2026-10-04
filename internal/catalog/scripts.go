package catalog

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bluesky-social/jetstream/internal/leader"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/segment"
)

// Metadata keys the catalog scripts own.
const (
	// MainSeqKey is the next seq to assign in Main.
	MainSeqKey = "seq/next"
	// BootstrapLiveSeqKey is the next seq to assign in BootstrapLive.
	BootstrapLiveSeqKey = "live_segments/seq/next"
)

// maxBatchEvents mirrors the segment block decoder's event cap: no batch
// larger than one block can be encoded, let alone decoded.
const maxBatchEvents = 1 << 18

// SeqKey returns ns's seq counter key.
func SeqKey(ns Namespace) string {
	if ns == BootstrapLive {
		return BootstrapLiveSeqKey
	}
	return MainSeqKey
}

// EncodeSeq encodes a seq counter value (8-byte little-endian, the local
// writer's encoding).
func EncodeSeq(v uint64) []byte {
	return binary.LittleEndian.AppendUint64(nil, v)
}

// DecodeSeq decodes a stored seq counter. An absent key is 1: seqs start at
// 1, as in the local writer. A stored 0 is corruption: every catalog write
// stores a counter past a committed seq. (The local writer floors a stored 0
// to 1 only because a pre-seed pebble build could have written one.)
func DecodeSeq(key string, val []byte, found bool) (uint64, error) {
	if !found {
		return 1, nil
	}
	if len(val) != 8 {
		return 0, Corruptf(SourceMeta, "%s has length %d, want 8", key, len(val))
	}
	n := binary.LittleEndian.Uint64(val)
	if n == 0 {
		return 0, Corruptf(SourceMeta, "%s is 0", key)
	}
	return n, nil
}

// SessionConfig configures a Session.
type SessionConfig struct {
	DB    DB
	Epoch uint64
	// Metrics and LeaderMetrics may be nil.
	Metrics       *Metrics
	LeaderMetrics *leader.Metrics
}

// Session runs the catalog transaction scripts for one leader session
// (design §6.5, §9, §10). Every script is one transaction that runs the
// epoch fence first. The first script to fail ends the Session: every later
// call returns ErrSessionEnded, so nothing is retried inside the session
// that saw the failure (§9.1).
//
// A Session is safe for concurrent use. The fence's row lock serializes the
// transactions in the database.
type Session struct {
	db      DB
	epoch   uint64
	metrics *Metrics
	lm      *leader.Metrics

	mu  sync.Mutex
	err error

	// uploads groups concurrent BeginUploads calls into one transaction.
	uploads struct {
		sync.Mutex
		busy  bool
		queue []*uploadCall
	}
}

// NewSession returns a Session for epoch.
func NewSession(cfg SessionConfig) *Session {
	return &Session{db: cfg.DB, epoch: cfg.Epoch, metrics: cfg.Metrics, lm: cfg.LeaderMetrics}
}

// Epoch returns the session's writer epoch.
func (s *Session) Epoch() uint64 { return s.epoch }

// DB returns the database the session writes through.
func (s *Session) DB() DB { return s.db }

// Err returns the error that ended the session, or nil.
func (s *Session) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *Session) fail(err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = err
		s.metrics.ObserveCorruption(err)
	}
	return err
}

func (s *Session) ended() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		return nil
	}
	if _, ok := IsCorruption(s.err); ok {
		return s.err
	}
	return fmt.Errorf("%w: an earlier transaction failed: %w", ErrSessionEnded, s.err)
}

// run is the shape every leader write transaction takes: fence first, with
// reads in its round trip, then the script body, NOTIFY, commit. Any error
// rolls back and ends the session; a failed COMMIT is an unknown result and
// is never retried.
//
// The archive row lock is held from the fence to COMMIT, and every leader
// transaction waits for it, so a script passes every read it can name up
// front as reads rather than spend a round trip under the lock on each.
func (s *Session) run(ctx context.Context, kind TxKind, reads []Read, body func(tx Tx, rev uint64) error) (uint64, error) {
	if err := s.ended(); err != nil {
		return 0, err
	}
	tx, err := s.db.Begin(ctx, kind)
	if err != nil {
		return 0, s.fail(sessionEnded(string(kind)+": begin", err))
	}
	rev, err := s.runBody(ctx, kind, tx, reads, body)
	if err == nil {
		if err = tx.Commit(ctx); err != nil {
			err = sessionEnded(string(kind)+": commit result unknown", err)
		}
	}
	if err != nil {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return 0, s.fail(err)
	}
	return rev, nil
}

func (s *Session) runBody(ctx context.Context, kind TxKind, tx Tx, reads []Read, body func(tx Tx, rev uint64) error) (uint64, error) {
	rev, ok, err := tx.FenceBump(ctx, s.epoch, reads...)
	if err != nil {
		return 0, sessionEnded(string(kind)+": fence", err)
	}
	if !ok {
		s.lm.FenceFailure()
		return 0, fmt.Errorf("%s: epoch %d: %w", kind, s.epoch, ErrFenced)
	}
	if err := body(tx, rev); err != nil {
		return 0, sessionEnded(string(kind), err)
	}
	if err := tx.Notify(ctx, rev); err != nil {
		return 0, sessionEnded(string(kind)+": notify", err)
	}
	return rev, nil
}

// seqRead reads and locks ns's seq key, for checkSeq.
func seqRead(ns Namespace) *MetaRead {
	return &MetaRead{Key: []byte(SeqKey(ns))}
}

// checkSeq checks the seq key r read equals first (§10.2). A mismatch means
// the leader's in-memory seq state diverged from what committed, which is
// corruption.
func checkSeq(r *MetaRead, first uint64) error {
	key := string(r.Key)
	stored, err := DecodeSeq(key, r.Value, r.Found)
	if err != nil {
		return err
	}
	if stored != first {
		return Corruptf(SourceSeq, "%s is %d, commit starts at %d", key, stored, first)
	}
	return nil
}

// ObjectRef names an object a transaction is about to reference.
type ObjectRef struct {
	ID     uint64
	SHA256 [32]byte
	// Pending marks an uploading row that the referencing transaction makes
	// available first, folding §7.3 step 6 into it. If another upload of
	// the same bytes already won, the transaction references the winner.
	Pending bool
}

// objectReads are the reads that resolving refs takes, sent with the fence:
// each ref's own row, and for a pending ref the available row with its
// bytes. Both are row-locked, and every writer of objects holds the fence
// besides, so the writes queued after the checks change exactly the rows
// the checks saw.
type objectReads struct {
	refs  []ObjectRef
	rows  ObjectsRead
	avail AvailableObjectsRead
}

func newObjectReads(refs ...ObjectRef) *objectReads {
	r := &objectReads{refs: refs}
	for _, ref := range refs {
		r.rows.IDs = append(r.rows.IDs, ref.ID)
		if ref.Pending {
			r.avail.SHA256 = append(r.avail.SHA256, ref.SHA256)
		}
	}
	return r
}

func (r *objectReads) reads() []Read {
	switch {
	case len(r.refs) == 0:
		return nil
	case len(r.avail.SHA256) == 0:
		return []Read{&r.rows}
	}
	return []Read{&r.rows, &r.avail}
}

// markAvailable is §7.3 step 6 for each pending ref, in order: its row
// becomes available, unless an available object with the same bytes
// already won, which the ref then names instead. It queues the change and
// returns the object ID each ref names, and the state every object read
// will be in once the change applies.
func (r *objectReads) markAvailable(ctx context.Context, tx Tx) ([]uint64, map[uint64]ObjectState, error) {
	states := make(map[uint64]ObjectState, len(r.rows.Rows)+len(r.avail.Rows))
	for id, row := range r.rows.Rows {
		states[id] = row.State
	}
	winners := make(map[[32]byte]uint64, len(r.avail.Rows))
	for sha, row := range r.avail.Rows {
		winners[sha] = row.ID
		states[row.ID] = row.State
	}
	ids := make([]uint64, len(r.refs))
	var promote []uint64
	for i, ref := range r.refs {
		ids[i] = ref.ID
		if !ref.Pending {
			continue
		}
		if id, ok := winners[ref.SHA256]; ok {
			ids[i] = id
			continue
		}
		if states[ref.ID] != ObjectUploading {
			// GC may have claimed an upload that stalled past the orphan age
			// (§7.3's accepted leak). A fresh session retries the upload.
			return nil, nil, fmt.Errorf("object %d is no longer uploading", ref.ID)
		}
		// A later ref to the same bytes references this one, as it would
		// have found it available.
		states[ref.ID] = ObjectAvailable
		winners[ref.SHA256] = ref.ID
		promote = append(promote, ref.ID)
	}
	if len(promote) > 0 {
		if err := tx.SetObjectsAvailable(ctx, promote); err != nil {
			return nil, nil, err
		}
	}
	return ids, states, nil
}

// resolve makes the pending refs available (markAvailable), runs the §7.4
// reference check on every object the refs name, and returns their IDs in
// ref order.
func (r *objectReads) resolve(ctx context.Context, tx Tx) ([]uint64, error) {
	if len(r.refs) == 0 {
		return nil, nil
	}
	ids, states, err := r.markAvailable(ctx, tx)
	if err != nil {
		return nil, err
	}
	var missing []uint64
	for _, id := range ids {
		if states[id] != ObjectAvailable {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		return nil, Corruptf(SourceRef, "objects %v are not available", missing)
	}
	return ids, tx.ClearUnreferenced(ctx, ids)
}

// HotBatch is one frozen hot batch to commit (§10.4). Exactly one of Frame
// and Object is set.
type HotBatch struct {
	FirstSeq, LastSeq              uint64
	MinWitnessedUS, MaxWitnessedUS int64
	Frame                          []byte
	Object                         ObjectRef
	// Meta is what the DurableBatchHook staged. The script adds the seq key
	// update itself.
	Meta []metastore.Op
}

// HotBatchCommit is the result of CommitHotBatch.
type HotBatchCommit struct {
	Revision uint64
	// ObjectID is the object the row references, after resolving a pending
	// object. Zero for an inline batch.
	ObjectID uint64
}

// CommitHotBatch is the §10.4 batch transaction.
func (s *Session) CommitHotBatch(ctx context.Context, b HotBatch) (HotBatchCommit, error) {
	out, err := s.CommitHotBatches(ctx, []HotBatch{b})
	if err != nil {
		return HotBatchCommit{}, err
	}
	return out[0], nil
}

// CommitHotBatches commits consecutive hot batches in one §10.4 transaction
// (group commit, §10.5): every row shares the revision, the hooks' ops apply
// in batch order, and seq/next moves once, past the last batch. Every leader
// transaction serializes on the fence row, so on a PostgreSQL whose commits
// wait for a WAL flush, one transaction per batch caps the whole leader near
// 1/flush-latency transactions a second (§22.2).
func (s *Session) CommitHotBatches(ctx context.Context, bs []HotBatch) ([]HotBatchCommit, error) {
	if len(bs) == 0 {
		return nil, s.fail(errors.New("catalog: commit of no hot batches"))
	}
	var meta []metastore.Op
	for i, b := range bs {
		if err := b.validate(); err != nil {
			return nil, s.fail(err)
		}
		if i > 0 && b.FirstSeq != bs[i-1].LastSeq+1 {
			return nil, s.fail(fmt.Errorf("catalog: hot batch [%d,%d] does not follow [%d,%d]",
				b.FirstSeq, b.LastSeq, bs[i-1].FirstSeq, bs[i-1].LastSeq))
		}
		meta = append(meta, b.Meta...)
	}
	var refs []ObjectRef
	for _, b := range bs {
		if b.Frame == nil {
			refs = append(refs, b.Object)
		}
	}
	seq, objs := seqRead(Main), newObjectReads(refs...)
	out := make([]HotBatchCommit, len(bs))
	rev, err := s.run(ctx, TxHotBatch, append([]Read{seq}, objs.reads()...), func(tx Tx, rev uint64) error {
		if err := checkSeq(seq, bs[0].FirstSeq); err != nil {
			return err
		}
		ids, err := objs.resolve(ctx, tx)
		if err != nil {
			return err
		}
		for i, b := range bs {
			row := HotBatchRow{
				FirstSeq:       b.FirstSeq,
				LastSeq:        b.LastSeq,
				EventCount:     uint32(b.LastSeq - b.FirstSeq + 1),
				MinWitnessedUS: b.MinWitnessedUS,
				MaxWitnessedUS: b.MaxWitnessedUS,
				Epoch:          s.epoch,
				Revision:       rev,
				Frame:          b.Frame,
			}
			if b.Frame == nil {
				row.ObjectID, out[i].ObjectID = ids[0], ids[0]
				ids = ids[1:]
			}
			if err := tx.InsertHotBatch(ctx, row); err != nil {
				return err
			}
		}
		return tx.ApplyMeta(ctx, withSeq(Main, bs[len(bs)-1].LastSeq+1, meta))
	})
	if err != nil {
		return nil, err
	}
	for i := range out {
		out[i].Revision = rev
	}
	return out, nil
}

func (b HotBatch) validate() error {
	switch {
	case b.FirstSeq == 0 || b.LastSeq < b.FirstSeq:
		return fmt.Errorf("catalog: hot batch has bad seq range [%d,%d]", b.FirstSeq, b.LastSeq)
	case b.LastSeq-b.FirstSeq >= maxBatchEvents:
		return fmt.Errorf("catalog: hot batch [%d,%d] exceeds the block event limit", b.FirstSeq, b.LastSeq)
	case (b.Frame == nil) == (b.Object.ID == 0):
		return errors.New("catalog: hot batch needs exactly one of an inline frame and an object")
	}
	return nil
}

// withSeq returns the seq key update followed by the hook's ops, the order
// the local writer stages them in.
func withSeq(ns Namespace, next uint64, meta []metastore.Op) []metastore.Op {
	ops := make([]metastore.Op, 0, len(meta)+1)
	ops = append(ops, metastore.Op{Kind: metastore.OpSet, Key: []byte(SeqKey(ns)), Value: EncodeSeq(next)})
	return append(ops, meta...)
}

// Block is one encoded block to commit as an active block, either directly
// (CommitBlock, CommitBlocks) or by folding hot batches (Fold).
type Block struct {
	Namespace Namespace
	// Info carries the block's bounds and sizes. Offset is ignored.
	Info   segment.BlockInfo
	Object ObjectRef
	// Meta is the DurableBatchHook output for a direct commit. Fold takes
	// none.
	Meta []metastore.Op
}

// BlockCommit is the result of CommitBlock, CommitBlocks, and Fold.
type BlockCommit struct {
	Revision uint64
	Segment  uint64
	Ordinal  int
	ObjectID uint64
}

func (b Block) validate() error {
	i := b.Info
	switch {
	case !b.Namespace.Valid():
		return fmt.Errorf("catalog: block has unknown namespace %q", b.Namespace)
	case i.EventCount == 0 || i.MinSeq == 0 || i.MaxSeq < i.MinSeq:
		return fmt.Errorf("catalog: block has bad bounds [%d,%d] count %d", i.MinSeq, i.MaxSeq, i.EventCount)
	case i.MaxSeq-i.MinSeq+1 != uint64(i.EventCount):
		return fmt.Errorf("catalog: block [%d,%d] has %d events; seqs are gap-free here", i.MinSeq, i.MaxSeq, i.EventCount)
	case i.CompressedSize == 0:
		return errors.New("catalog: block has no compressed bytes")
	case b.Object.ID == 0:
		return errors.New("catalog: block has no object")
	}
	return nil
}

// CommitBlock is the §10.6 direct-mode block transaction.
func (s *Session) CommitBlock(ctx context.Context, b Block) (BlockCommit, error) {
	out, err := s.CommitBlocks(ctx, []Block{b})
	if err != nil {
		return BlockCommit{}, err
	}
	return out[0], nil
}

// CommitBlocks commits consecutive direct-mode blocks of one namespace in
// one §10.6 transaction (group commit): they take the next ordinals of the
// active segment, every row shares the revision, the blocks' Meta apply in
// block order, and the seq key moves once, past the last block. Every leader
// transaction serializes on the fence row, so one transaction per block
// caps a backfill at about one block per transaction latency (pop2,
// 2026-10-04). The caller keeps the blocks inside the active segment:
// the transaction does not apply the rotation rule.
func (s *Session) CommitBlocks(ctx context.Context, bs []Block) ([]BlockCommit, error) {
	if len(bs) == 0 {
		return nil, s.fail(errors.New("catalog: commit of no blocks"))
	}
	var meta []metastore.Op
	for i, b := range bs {
		if err := b.validate(); err != nil {
			return nil, s.fail(err)
		}
		if i > 0 {
			prev := bs[i-1]
			if b.Namespace != prev.Namespace {
				return nil, s.fail(fmt.Errorf("catalog: block group spans namespaces %q and %q", prev.Namespace, b.Namespace))
			}
			if b.Info.MinSeq != prev.Info.MaxSeq+1 {
				return nil, s.fail(fmt.Errorf("catalog: block [%d,%d] does not follow [%d,%d]",
					b.Info.MinSeq, b.Info.MaxSeq, prev.Info.MinSeq, prev.Info.MaxSeq))
			}
		}
		meta = append(meta, b.Meta...)
	}
	first, last := bs[0], bs[len(bs)-1]
	out := make([]BlockCommit, len(bs))
	seq, br := seqRead(first.Namespace), newBlockReads(bs...)
	rev, err := s.run(ctx, TxBlock, append([]Read{seq}, br.reads()...), func(tx Tx, rev uint64) error {
		if err := checkSeq(seq, first.Info.MinSeq); err != nil {
			return err
		}
		if err := insertActiveBlocks(ctx, tx, rev, bs, br, out); err != nil {
			return err
		}
		return tx.ApplyMeta(ctx, withSeq(first.Namespace, last.Info.MaxSeq+1, meta))
	})
	if err != nil {
		return nil, err
	}
	for i := range out {
		out[i].Revision = rev
	}
	return out, nil
}

// Fold is the §10.7 fold transaction: it replaces the hot batches covering
// exactly the block's seq range with one active block in Main.
func (s *Session) Fold(ctx context.Context, b Block) (BlockCommit, error) {
	if err := b.validate(); err != nil {
		return BlockCommit{}, s.fail(err)
	}
	if b.Namespace != Main || len(b.Meta) > 0 {
		return BlockCommit{}, s.fail(errors.New("catalog: fold is main-only and carries no metadata"))
	}
	out := make([]BlockCommit, 1)
	br := newBlockReads(b)
	rev, err := s.run(ctx, TxFold, br.reads(), func(tx Tx, rev uint64) error {
		spans, err := tx.DeleteHotBatches(ctx, b.Info.MinSeq, b.Info.MaxSeq)
		if err != nil {
			return err
		}
		if err := checkCoverage(spans, b.Info.MinSeq, b.Info.MaxSeq); err != nil {
			return err
		}
		return insertActiveBlocks(ctx, tx, rev, []Block{b}, br, out)
	})
	out[0].Revision = rev
	return out[0], err
}

// checkCoverage checks that the deleted hot batches tile [lo, hi] exactly.
// Anything else means the fold would lose or duplicate committed events.
func checkCoverage(spans []HotBatchSpan, lo, hi uint64) error {
	next := lo
	for _, sp := range spans {
		if sp.FirstSeq != next || sp.LastSeq < sp.FirstSeq || sp.LastSeq > hi ||
			sp.LastSeq-sp.FirstSeq+1 != uint64(sp.EventCount) {
			return Corruptf(SourceFold, "hot batch [%d,%d] does not continue coverage at %d of [%d,%d]",
				sp.FirstSeq, sp.LastSeq, next, lo, hi)
		}
		next = sp.LastSeq + 1
	}
	if next != hi+1 {
		return Corruptf(SourceFold, "hot batches cover [%d,%d) of [%d,%d]", lo, next, lo, hi)
	}
	return nil
}

// blockReads are the reads insertActiveBlocks takes, sent with the fence.
type blockReads struct {
	seg  ActiveSegmentRead
	last LastActiveBlockRead
	obj  *objectReads
}

// newBlockReads reads for blocks of one namespace.
func newBlockReads(bs ...Block) *blockReads {
	refs := make([]ObjectRef, len(bs))
	for i, b := range bs {
		refs[i] = b.Object
	}
	return &blockReads{
		seg:  ActiveSegmentRead{Namespace: bs[0].Namespace},
		last: LastActiveBlockRead{Namespace: bs[0].Namespace},
		obj:  newObjectReads(refs...),
	}
}

func (r *blockReads) reads() []Read {
	return append([]Read{&r.seg, &r.last}, r.obj.reads()...)
}

// insertActiveBlocks appends consecutive blocks of one namespace to its
// active segment, filling out. The first must continue the segment's last
// block. The first block of a segment is checked by the seq key (direct
// mode) or the hot batch coverage (fold), together with CheckInvariants.
func insertActiveBlocks(ctx context.Context, tx Tx, rev uint64, bs []Block, r *blockReads, out []BlockCommit) error {
	ns := bs[0].Namespace
	if !r.seg.Found {
		return Corruptf(SourceInvariant, "namespace %s has no active segment", ns)
	}
	seg := r.seg.Row
	ordinal := 0
	if r.last.Found {
		last := r.last.Row
		if last.Segment != seg.Index {
			return Corruptf(SourceInvariant, "%s last active block is in segment %d; the active segment is %d",
				ns, last.Segment, seg.Index)
		}
		if last.MaxSeq+1 != bs[0].Info.MinSeq {
			return Corruptf(SourceInvariant, "%s segment %d block %d ends at %d; new block starts at %d",
				ns, seg.Index, last.Ordinal, last.MaxSeq, bs[0].Info.MinSeq)
		}
		ordinal = last.Ordinal + 1
	}
	ids, err := r.obj.resolve(ctx, tx)
	if err != nil {
		return err
	}
	for i, b := range bs {
		row := ActiveBlockRow{
			Namespace:          ns,
			Segment:            seg.Index,
			Ordinal:            ordinal + i,
			ObjectID:           ids[i],
			EventCount:         b.Info.EventCount,
			MinSeq:             b.Info.MinSeq,
			MaxSeq:             b.Info.MaxSeq,
			MinWitnessedUS:     b.Info.MinWitnessedAt,
			MaxWitnessedUS:     b.Info.MaxWitnessedAt,
			CompressedLength:   int64(b.Info.CompressedSize),
			UncompressedLength: int64(b.Info.UncompressedSize),
			Revision:           rev,
		}
		if err := tx.InsertActiveBlock(ctx, row); err != nil {
			return err
		}
		out[i] = BlockCommit{Segment: seg.Index, Ordinal: row.Ordinal, ObjectID: row.ObjectID}
	}
	return nil
}

// Seal describes a sealed generation built from an active segment's blocks
// (§10.8).
type Seal struct {
	Namespace Namespace
	Segment   uint64
	// Header is the finalized 256-byte header from segment.BuildSealed.
	Header []byte
	Footer ObjectRef
	// Blocks is the block list the footer was built from, in order.
	Blocks []SealBlock
}

// SealBlock is one block of a Seal.
type SealBlock struct {
	ObjectID         uint64
	CompressedLength int64
}

// SealCommit is the result of Seal.
type SealCommit struct {
	Revision       uint64
	GenerationID   uint64
	FooterObjectID uint64
}

// Seal is the §10.8 seal transaction.
func (s *Session) Seal(ctx context.Context, sl Seal) (SealCommit, error) {
	hdr, err := segment.ReadSealedHeader(bytes.NewReader(sl.Header))
	if err != nil {
		return SealCommit{}, s.fail(fmt.Errorf("catalog: seal %s segment %d: %w", sl.Namespace, sl.Segment, err))
	}
	if !sl.Namespace.Valid() || sl.Footer.ID == 0 {
		return SealCommit{}, s.fail(fmt.Errorf("catalog: seal %s segment %d: bad namespace or footer", sl.Namespace, sl.Segment))
	}
	// The footer first, then the blocks: ids[0] is the footer's.
	refs := []ObjectRef{sl.Footer}
	for _, b := range sl.Blocks {
		refs = append(refs, ObjectRef{ID: b.ObjectID})
	}
	seg, objs := &ActiveSegmentRead{Namespace: sl.Namespace}, newObjectReads(refs...)
	var out SealCommit
	rev, err := s.run(ctx, TxSeal, append([]Read{seg}, objs.reads()...), func(tx Tx, rev uint64) error {
		if !seg.Found || seg.Row.Index != sl.Segment {
			return Corruptf(SourceSeal, "%s segment %d is not the active segment", sl.Namespace, sl.Segment)
		}
		rows, err := tx.ActiveBlocksForUpdate(ctx, sl.Namespace, sl.Segment)
		if err != nil {
			return err
		}
		// The check makes the blocks' refs name exactly the rows' objects.
		if err := checkSealList(sl, hdr, rows); err != nil {
			return err
		}
		ids, err := objs.resolve(ctx, tx)
		if err != nil {
			return err
		}
		footerID := ids[0]
		gen, err := tx.InsertGeneration(ctx, GenerationRow{
			Namespace:      sl.Namespace,
			Segment:        sl.Segment,
			Header:         sl.Header,
			FooterObjectID: footerID,
			Revision:       rev,
		})
		if err != nil {
			return err
		}
		gbs := make([]GenerationBlockRow, len(rows))
		for i, r := range rows {
			gbs[i] = GenerationBlockRow{GenerationID: gen, Ordinal: i, ObjectID: r.ObjectID, CompressedLength: r.CompressedLength}
		}
		if err := tx.InsertGenerationBlocks(ctx, gbs); err != nil {
			return err
		}
		if _, err := tx.DeleteActiveBlocks(ctx, sl.Namespace, sl.Segment); err != nil {
			return err
		}
		ok, err := tx.SealSegment(ctx, sl.Namespace, sl.Segment, gen, rev)
		if err != nil {
			return err
		}
		if !ok {
			return Corruptf(SourceSeal, "%s segment %d stopped being active mid-seal", sl.Namespace, sl.Segment)
		}
		if err := tx.InsertSegment(ctx, SegmentRow{Namespace: sl.Namespace, Index: sl.Segment + 1, State: Active, Revision: rev}); err != nil {
			return err
		}
		out = SealCommit{GenerationID: gen, FooterObjectID: footerID}
		return nil
	})
	out.Revision = rev
	return out, err
}

// checkSealList checks the committed active blocks are exactly the blocks
// the footer was built from, and that the header agrees with them. A
// difference means the sealed file would not describe the catalog's blocks.
func checkSealList(sl Seal, hdr segment.Header, rows []ActiveBlockRow) error {
	if len(rows) != len(sl.Blocks) {
		return Corruptf(SourceSeal, "%s segment %d has %d active blocks; footer lists %d",
			sl.Namespace, sl.Segment, len(rows), len(sl.Blocks))
	}
	var events uint64
	for i, r := range rows {
		b := sl.Blocks[i]
		if r.Ordinal != i || r.ObjectID != b.ObjectID || r.CompressedLength != b.CompressedLength {
			return Corruptf(SourceSeal, "%s segment %d block %d is object %d (%d bytes) at ordinal %d; footer has object %d (%d bytes)",
				sl.Namespace, sl.Segment, i, r.ObjectID, r.CompressedLength, r.Ordinal, b.ObjectID, b.CompressedLength)
		}
		events += uint64(r.EventCount)
	}
	if uint64(hdr.BlockCount) != uint64(len(rows)) || uint64(hdr.EventCount) != events {
		return Corruptf(SourceSeal, "%s segment %d header has %d blocks / %d events; catalog has %d / %d",
			sl.Namespace, sl.Segment, hdr.BlockCount, hdr.EventCount, len(rows), events)
	}
	if len(rows) > 0 && (hdr.MinSeq != rows[0].MinSeq || hdr.MaxSeq != rows[len(rows)-1].MaxSeq) {
		return Corruptf(SourceSeal, "%s segment %d header covers [%d,%d]; blocks cover [%d,%d]",
			sl.Namespace, sl.Segment, hdr.MinSeq, hdr.MaxSeq, rows[0].MinSeq, rows[len(rows)-1].MaxSeq)
	}
	return nil
}

// Publish describes a compacted generation of a sealed Main segment
// (§12.2): the output of segment.SparseRewrite with its new objects
// uploaded.
type Publish struct {
	Segment uint64
	// Source is the generation the rewrite read. It must still be the
	// segment's current generation.
	Source uint64
	// Header is the rewritten 256-byte header.
	Header []byte
	Footer ObjectRef
	// Blocks is the new generation's block list, in ordinal order: one
	// entry per source block.
	Blocks []PublishBlock
}

// PublishBlock is one block of a Publish.
type PublishBlock struct {
	// Object is the re-encoded block's object, or, when Reused, the source
	// generation's object at the same ordinal.
	Object           ObjectRef
	Reused           bool
	CompressedLength int64
}

// PublishCommit is the result of PublishGeneration.
type PublishCommit struct {
	Revision       uint64
	GenerationID   uint64
	FooterObjectID uint64
	// ObjectIDs are the new generation's block objects in ordinal order,
	// after resolving pending objects.
	ObjectIDs []uint64
}

// PublishGeneration is the §12.2 publish transaction: it replaces a sealed
// Main segment's current generation with a compacted one. Only the leader
// compacts, so a source generation that is no longer current, a reused block
// that is not the source's, or a header whose envelope differs from the
// source's is corruption.
func (s *Session) PublishGeneration(ctx context.Context, p Publish) (PublishCommit, error) {
	hdr, err := segment.ReadSealedHeader(bytes.NewReader(p.Header))
	if err != nil {
		return PublishCommit{}, s.fail(fmt.Errorf("catalog: publish segment %d: %w", p.Segment, err))
	}
	switch {
	case p.Source == 0 || p.Footer.ID == 0:
		return PublishCommit{}, s.fail(fmt.Errorf("catalog: publish segment %d: no source generation or footer", p.Segment))
	case int(hdr.BlockCount) != len(p.Blocks):
		return PublishCommit{}, s.fail(fmt.Errorf("catalog: publish segment %d: header has %d blocks; publish lists %d",
			p.Segment, hdr.BlockCount, len(p.Blocks)))
	}
	for i, b := range p.Blocks {
		if b.Object.ID == 0 || b.CompressedLength <= 0 || (b.Reused && b.Object.Pending) {
			return PublishCommit{}, s.fail(fmt.Errorf("catalog: publish segment %d: bad block %d", p.Segment, i))
		}
	}
	// The blocks in ordinal order, then the footer. A reused block is not
	// pending, so resolving it is its reference check.
	refs := make([]ObjectRef, 0, len(p.Blocks)+1)
	for _, b := range p.Blocks {
		refs = append(refs, b.Object)
	}
	objs := newObjectReads(append(refs, p.Footer)...)
	var out PublishCommit
	rev, err := s.run(ctx, TxCompaction, objs.reads(), func(tx Tx, rev uint64) error {
		seg, found, err := tx.SegmentForUpdate(ctx, Main, p.Segment)
		if err != nil {
			return err
		}
		if !found || seg.State != Sealed || seg.GenerationID != p.Source {
			return Corruptf(SourceCompaction, "main segment %d is not sealed at generation %d (found=%t, %v)",
				p.Segment, p.Source, found, seg)
		}
		src, found, err := tx.Generation(ctx, p.Source)
		if err != nil {
			return err
		}
		if !found {
			return Corruptf(SourceCompaction, "main segment %d names missing generation %d", p.Segment, p.Source)
		}
		srcBlocks, err := tx.BlocksOfGeneration(ctx, p.Source)
		if err != nil {
			return err
		}
		if err := checkPublish(p, hdr, src, srcBlocks); err != nil {
			return err
		}
		ids, err := objs.resolve(ctx, tx)
		if err != nil {
			return err
		}
		out.ObjectIDs, ids = ids[:len(p.Blocks)], ids[len(p.Blocks):]
		footerID := ids[0]
		gen, err := tx.InsertGeneration(ctx, GenerationRow{
			Namespace:      Main,
			Segment:        p.Segment,
			Header:         p.Header,
			FooterObjectID: footerID,
			Revision:       rev,
		})
		if err != nil {
			return err
		}
		gbs := make([]GenerationBlockRow, len(p.Blocks))
		for i, b := range p.Blocks {
			gbs[i] = GenerationBlockRow{GenerationID: gen, Ordinal: i, ObjectID: out.ObjectIDs[i], CompressedLength: b.CompressedLength}
		}
		if err := tx.InsertGenerationBlocks(ctx, gbs); err != nil {
			return err
		}
		ok, err := tx.SetSegmentGeneration(ctx, Main, p.Segment, gen, rev)
		if err != nil {
			return err
		}
		if !ok {
			return Corruptf(SourceCompaction, "main segment %d stopped being sealed mid-publish", p.Segment)
		}
		if ok, err = tx.DeleteGeneration(ctx, p.Source); err != nil {
			return err
		}
		if !ok {
			return Corruptf(SourceCompaction, "generation %d vanished mid-publish", p.Source)
		}
		out.GenerationID, out.FooterObjectID = gen, footerID
		return nil
	})
	out.Revision = rev
	return out, err
}

// checkPublish checks a compacted generation against its source. A rewrite
// keeps every block and the seq and witnessed envelope, drops at least one
// row, and reuses only the source's own objects at their own ordinals.
func checkPublish(p Publish, hdr segment.Header, src GenerationRow, srcBlocks []GenerationBlockRow) error {
	srcHdr, err := segment.ReadSealedHeader(bytes.NewReader(src.Header))
	if err != nil {
		return Corruptf(SourceGeneration, "main segment %d generation %d header: %v", p.Segment, src.ID, err)
	}
	switch {
	case src.Namespace != Main || src.Segment != p.Segment:
		return Corruptf(SourceCompaction, "generation %d belongs to %s segment %d, not main segment %d",
			src.ID, src.Namespace, src.Segment, p.Segment)
	case int(srcHdr.BlockCount) != len(srcBlocks) || len(srcBlocks) != len(p.Blocks):
		return Corruptf(SourceCompaction, "main segment %d: source has %d blocks (header %d); rewrite has %d",
			p.Segment, len(srcBlocks), srcHdr.BlockCount, len(p.Blocks))
	case hdr.MinSeq != srcHdr.MinSeq || hdr.MaxSeq != srcHdr.MaxSeq ||
		hdr.MinWitnessedAt != srcHdr.MinWitnessedAt || hdr.MaxWitnessedAt != srcHdr.MaxWitnessedAt:
		return Corruptf(SourceCompaction, "main segment %d: rewrite covers seqs [%d,%d] witnessed [%d,%d]; source covers [%d,%d] [%d,%d]",
			p.Segment, hdr.MinSeq, hdr.MaxSeq, hdr.MinWitnessedAt, hdr.MaxWitnessedAt,
			srcHdr.MinSeq, srcHdr.MaxSeq, srcHdr.MinWitnessedAt, srcHdr.MaxWitnessedAt)
	case hdr.EventCount >= srcHdr.EventCount:
		return Corruptf(SourceCompaction, "main segment %d: rewrite has %d events; source has %d",
			p.Segment, hdr.EventCount, srcHdr.EventCount)
	}
	for i, b := range p.Blocks {
		sb := srcBlocks[i]
		if sb.Ordinal != i {
			return Corruptf(SourceCompaction, "main segment %d generation %d block ordinal %d at position %d", p.Segment, src.ID, sb.Ordinal, i)
		}
		if b.Reused && (b.Object.ID != sb.ObjectID || b.CompressedLength != sb.CompressedLength) {
			return Corruptf(SourceCompaction, "main segment %d block %d reuses object %d (%d bytes); source has object %d (%d bytes)",
				p.Segment, i, b.Object.ID, b.CompressedLength, sb.ObjectID, sb.CompressedLength)
		}
	}
	return nil
}

// CompareAndSetMeta sets key to value in one fenced transaction if its
// stored value is prior, where a nil prior means absent. Any other stored
// value is corruption: only the leader writes the keys it guards, so the
// leader's in-memory prior cannot legitimately be stale. It backs the
// §12.1 compaction/seq advance.
func (s *Session) CompareAndSetMeta(ctx context.Context, key string, prior, value []byte) (uint64, error) {
	if value == nil {
		return 0, s.fail(fmt.Errorf("catalog: compare-and-set of %s to nil", key))
	}
	stored := &MetaRead{Key: []byte(key)}
	return s.run(ctx, TxMetadata, []Read{stored}, func(tx Tx, rev uint64) error {
		if stored.Found != (prior != nil) || !bytes.Equal(stored.Value, prior) {
			return Corruptf(SourceCompaction, "%s is %x (found=%t); expected %x (found=%t)",
				key, stored.Value, stored.Found, prior, prior != nil)
		}
		return tx.ApplyMeta(ctx, []metastore.Op{{Kind: metastore.OpSet, Key: []byte(key), Value: value}})
	})
}

// GCMark is one page of the §13 mark, in one fenced transaction.
func (s *Session) GCMark(ctx context.Context, after uint64, limit int) (MarkPage, error) {
	var page MarkPage
	_, err := s.run(ctx, TxGC, nil, func(tx Tx, rev uint64) error {
		var err error
		page, err = tx.MarkUnreferenced(ctx, after, limit)
		return err
	})
	return page, err
}

// GCClaim is the §13 claim, in one fenced transaction. It returns the
// objects the caller deletes next. Deleting rows left by a GC run that did
// not finish come first, as they are; only when there are none does it
// claim fresh objects. A fresh claim re-runs the reference checks: §7.4
// clears unreferenced_at on every reference, so a claimed object that is
// referenced is corruption.
func (s *Session) GCClaim(ctx context.Context, gcDelay, orphanAge time.Duration, limit int) ([]ObjectRow, error) {
	var out []ObjectRow
	_, err := s.run(ctx, TxGC, nil, func(tx Tx, rev uint64) error {
		rows, err := tx.DeletingObjects(ctx, limit)
		if err != nil || len(rows) > 0 {
			out = rows
			return err
		}
		if rows, err = tx.ClaimObjects(ctx, gcDelay, orphanAge, limit); err != nil {
			return err
		}
		ids := make([]uint64, len(rows))
		for i, r := range rows {
			ids[i] = r.ID
		}
		refs, err := tx.ReferencedObjects(ctx, ids)
		if err != nil {
			return err
		}
		if len(refs) > 0 {
			return Corruptf(SourceGC, "claimed objects %v are referenced", refs)
		}
		out = rows
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GCForget is the §13 forget, in one fenced transaction: it deletes the
// rows of deleted objects that are still deleting, and returns how many.
func (s *Session) GCForget(ctx context.Context, ids []uint64) (int, error) {
	var n int
	_, err := s.run(ctx, TxGC, nil, func(tx Tx, rev uint64) error {
		var err error
		n, err = tx.ForgetObjects(ctx, ids)
		return err
	})
	return n, err
}

// InitNamespace creates ns's first active segment if the namespace has no
// active segment, and applies meta in the same transaction either way.
func (s *Session) InitNamespace(ctx context.Context, ns Namespace, meta []metastore.Op) (uint64, error) {
	if !ns.Valid() {
		return 0, s.fail(fmt.Errorf("catalog: unknown namespace %q", ns))
	}
	active := &ActiveSegmentRead{Namespace: ns}
	return s.run(ctx, TxNamespace, []Read{active}, func(tx Tx, rev uint64) error {
		if !active.Found {
			if err := tx.InsertSegment(ctx, SegmentRow{Namespace: ns, Index: 0, State: Active, Revision: rev}); err != nil {
				return err
			}
		}
		if len(meta) == 0 {
			return nil
		}
		return tx.ApplyMeta(ctx, meta)
	})
}

// DeleteNamespace deletes every catalog row of ns and applies meta in the
// same transaction (merge's final step, design §10.10). Main can never be
// deleted.
func (s *Session) DeleteNamespace(ctx context.Context, ns Namespace, meta []metastore.Op) (uint64, error) {
	if ns != BootstrapLive {
		return 0, s.fail(fmt.Errorf("catalog: refusing to delete namespace %q", ns))
	}
	return s.run(ctx, TxNamespace, nil, func(tx Tx, rev uint64) error {
		if err := tx.DeleteNamespace(ctx, ns); err != nil {
			return err
		}
		if len(meta) == 0 {
			return nil
		}
		return tx.ApplyMeta(ctx, meta)
	})
}

// CommitMeta applies metadata ops in one fenced transaction. It backs the
// leader's metastore.Store commits (design §6.4: every metadata write is a
// leader write transaction).
func (s *Session) CommitMeta(ctx context.Context, ops []metastore.Op) (uint64, error) {
	return s.run(ctx, TxMetadata, nil, func(tx Tx, rev uint64) error {
		return tx.ApplyMeta(ctx, ops)
	})
}

// UploadRequest asks BeginUploads for an object slot.
type UploadRequest struct {
	// Key is the fresh object key the caller will PUT under.
	Key    [16]byte
	SHA256 [32]byte
	Length int64
}

// UploadSlot is BeginUploads' answer for one request.
type UploadSlot struct {
	ObjectID uint64
	// Dedup means an available object with the same bytes exists: the
	// caller skips the upload and references ObjectID directly.
	Dedup bool
}

// BeginUploads is §7.3 steps 2 and 3 for a set of objects in one fenced
// transaction: dedup against available objects whose unreferenced age is
// under maxUnrefAge (gc_delay/2), and insert uploading rows for the rest.
// The committed rows make orphaned uploads visible to GC.
//
// Concurrent calls share a transaction (group commit, §10.5): a call that
// arrives while another's transaction is in flight queues, and the next
// transaction serves the whole queue. Every leader transaction serializes on
// the fence row, and each pointer batch needs one of these, so on a
// PostgreSQL whose commits wait for a WAL flush they would otherwise take
// as much of the leader's commit capacity as the hot batches (§22.2). A
// failure ends the session, so the calls sharing it share its fate either
// way.
func (s *Session) BeginUploads(ctx context.Context, reqs []UploadRequest, maxUnrefAge time.Duration) ([]UploadSlot, time.Time, error) {
	for _, r := range reqs {
		if r.Length <= 0 {
			return nil, time.Time{}, s.fail(fmt.Errorf("catalog: upload of %d bytes", r.Length))
		}
	}
	c := &uploadCall{reqs: reqs, maxUnrefAge: maxUnrefAge, lead: make(chan struct{}), done: make(chan struct{})}
	u := &s.uploads
	u.Lock()
	u.queue = append(u.queue, c)
	if !u.busy {
		u.busy = true
		close(c.lead)
	}
	u.Unlock()
	select {
	case <-c.done:
	case <-c.lead:
		s.leadUploads(ctx)
	}
	<-c.done
	return c.slots, c.committed, c.err
}

// uploadCall is one BeginUploads call waiting for a transaction. lead is
// closed when the call must run the next transaction itself.
type uploadCall struct {
	reqs        []UploadRequest
	maxUnrefAge time.Duration
	lead, done  chan struct{}

	slots     []UploadSlot
	committed time.Time
	err       error
}

// leadUploads runs one transaction for every queued call, then hands the
// lead to the first call that queued meanwhile, so no call waits for more
// than the transaction in flight and its own.
func (s *Session) leadUploads(ctx context.Context) {
	u := &s.uploads
	u.Lock()
	calls := u.queue
	u.queue = nil
	u.Unlock()

	slots, committed, err := s.beginUploads(ctx, calls)
	for i, c := range calls {
		if err == nil {
			c.slots, c.committed = slots[i], committed
		}
		c.err = err
		close(c.done)
	}

	u.Lock()
	if len(u.queue) > 0 {
		close(u.queue[0].lead)
	} else {
		u.busy = false
	}
	u.Unlock()
}

func (s *Session) beginUploads(ctx context.Context, calls []*uploadCall) ([][]UploadSlot, time.Time, error) {
	// One dedup lookup per distinct age, in the fence's round trip. The
	// calls almost always share one.
	lookups := map[time.Duration]*AvailableObjectsRead{}
	var reads []Read
	for _, c := range calls {
		l := lookups[c.maxUnrefAge]
		if l == nil {
			l = &AvailableObjectsRead{MaxUnrefAge: c.maxUnrefAge}
			lookups[c.maxUnrefAge] = l
			reads = append(reads, l)
		}
		for _, r := range c.reqs {
			l.SHA256 = append(l.SHA256, r.SHA256)
		}
	}
	slots := make([][]UploadSlot, len(calls))
	var (
		at  [][2]int
		ids []uint64
	)
	_, err := s.run(ctx, TxObjects, reads, func(tx Tx, rev uint64) error {
		var fresh []NewObject
		for i, c := range calls {
			slots[i] = make([]UploadSlot, len(c.reqs))
			found := lookups[c.maxUnrefAge].Rows
			for j, r := range c.reqs {
				if row, ok := found[r.SHA256]; ok {
					slots[i][j] = UploadSlot{ObjectID: row.ID, Dedup: true}
					continue
				}
				fresh = append(fresh, NewObject(r))
				at = append(at, [2]int{i, j})
			}
		}
		if len(fresh) == 0 {
			return nil
		}
		// The IDs arrive with COMMIT's round trip.
		ids = make([]uint64, len(fresh))
		return tx.InsertObjects(ctx, fresh, ids)
	})
	if err != nil {
		return nil, time.Time{}, err
	}
	for k, ij := range at {
		if ids[k] == 0 {
			// The rows committed, but as orphaned uploads GC collects.
			return nil, time.Time{}, s.fail(sessionEnded(string(TxObjects), fmt.Errorf("object row %d of %d has no ID", k, len(ids))))
		}
		slots[ij[0]][ij[1]] = UploadSlot{ObjectID: ids[k]}
	}
	// The monotonic reading taken after commit is the start of the
	// orphan-age window for the skip-PUT rule.
	return slots, time.Now(), nil
}

// MarkAvailable is §7.3 step 6 as its own transaction. It returns the
// object ID callers must reference: ref.ID, or the row of another upload of
// the same bytes that won first.
func (s *Session) MarkAvailable(ctx context.Context, ref ObjectRef) (uint64, error) {
	ref.Pending = true
	objs := newObjectReads(ref)
	var id uint64
	_, err := s.run(ctx, TxObjects, objs.reads(), func(tx Tx, rev uint64) error {
		ids, _, err := objs.markAvailable(ctx, tx)
		if err != nil {
			return err
		}
		id = ids[0]
		return nil
	})
	return id, err
}

// End ends the session for a failure outside any transaction that the design
// still treats as session-ending, such as an upload that could not complete
// (§7.3: "every caller in this document ends the session"). It returns the
// session-ending error; corruption stays fatal. If the session had already
// ended, the first error is kept.
func (s *Session) End(op string, err error) error {
	return s.fail(sessionEnded(op, err))
}
