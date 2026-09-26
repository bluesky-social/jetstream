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

// run is the shape every leader write transaction takes: fence first, the
// script body, NOTIFY, commit. Any error rolls back and ends the session;
// a failed COMMIT is an unknown result and is never retried.
func (s *Session) run(ctx context.Context, kind TxKind, body func(tx Tx, rev uint64) error) (uint64, error) {
	if err := s.ended(); err != nil {
		return 0, err
	}
	tx, err := s.db.Begin(ctx, kind)
	if err != nil {
		return 0, s.fail(sessionEnded(string(kind)+": begin", err))
	}
	rev, err := s.runBody(ctx, kind, tx, body)
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

func (s *Session) runBody(ctx context.Context, kind TxKind, tx Tx, body func(tx Tx, rev uint64) error) (uint64, error) {
	rev, ok, err := tx.FenceBump(ctx, s.epoch)
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

// checkSeq locks ns's seq key and checks it equals first (§10.2). A
// mismatch means the leader's in-memory seq state diverged from what
// committed, which is corruption.
func checkSeq(ctx context.Context, tx Tx, ns Namespace, first uint64) error {
	key := SeqKey(ns)
	val, found, err := tx.MetaGetForUpdate(ctx, []byte(key))
	if err != nil {
		return err
	}
	stored, err := DecodeSeq(key, val, found)
	if err != nil {
		return err
	}
	if stored != first {
		return Corruptf(SourceSeq, "%s is %d, commit starts at %d", key, stored, first)
	}
	return nil
}

// refCheck runs the §7.4 reference check for every ID.
func refCheck(ctx context.Context, tx Tx, ids ...uint64) error {
	missing, err := tx.RefCheck(ctx, ids)
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		return Corruptf(SourceRef, "objects %v are not available", missing)
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

// resolve makes a pending object available and reference-checks the
// object the transaction will point at, returning its ID.
func resolve(ctx context.Context, tx Tx, ref ObjectRef) (uint64, error) {
	id := ref.ID
	if ref.Pending {
		var err error
		if id, err = markAvailable(ctx, tx, ref.ID, ref.SHA256); err != nil {
			return 0, err
		}
	}
	if err := refCheck(ctx, tx, id); err != nil {
		return 0, err
	}
	return id, nil
}

// markAvailable is §7.3 step 6. Every write runs under the fence's row
// lock, so the winner lookup and the update cannot race another upload.
func markAvailable(ctx context.Context, tx Tx, id uint64, sha [32]byte) (uint64, error) {
	winner, found, err := tx.FindAvailableObject(ctx, sha, 0)
	if err != nil {
		return 0, err
	}
	if found {
		return winner.ID, nil
	}
	ok, err := tx.SetObjectAvailable(ctx, id)
	if err != nil {
		return 0, err
	}
	if !ok {
		// GC may have claimed an upload that stalled past the orphan age
		// (§7.3's accepted leak). A fresh session retries the upload.
		return 0, fmt.Errorf("object %d is no longer uploading", id)
	}
	return id, nil
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
	out := make([]HotBatchCommit, len(bs))
	rev, err := s.run(ctx, TxHotBatch, func(tx Tx, rev uint64) error {
		if err := checkSeq(ctx, tx, Main, bs[0].FirstSeq); err != nil {
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
				id, err := resolve(ctx, tx, b.Object)
				if err != nil {
					return err
				}
				row.ObjectID, out[i].ObjectID = id, id
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
// (CommitBlock) or by folding hot batches (Fold).
type Block struct {
	Namespace Namespace
	// Info carries the block's bounds and sizes. Offset is ignored.
	Info   segment.BlockInfo
	Object ObjectRef
	// Meta is the DurableBatchHook output for a direct commit. Fold takes
	// none.
	Meta []metastore.Op
}

// BlockCommit is the result of CommitBlock and Fold.
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
	if err := b.validate(); err != nil {
		return BlockCommit{}, s.fail(err)
	}
	var out BlockCommit
	rev, err := s.run(ctx, TxBlock, func(tx Tx, rev uint64) error {
		if err := checkSeq(ctx, tx, b.Namespace, b.Info.MinSeq); err != nil {
			return err
		}
		if err := insertActiveBlock(ctx, tx, rev, b, &out); err != nil {
			return err
		}
		return tx.ApplyMeta(ctx, withSeq(b.Namespace, b.Info.MaxSeq+1, b.Meta))
	})
	out.Revision = rev
	return out, err
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
	var out BlockCommit
	rev, err := s.run(ctx, TxFold, func(tx Tx, rev uint64) error {
		spans, err := tx.DeleteHotBatches(ctx, b.Info.MinSeq, b.Info.MaxSeq)
		if err != nil {
			return err
		}
		if err := checkCoverage(spans, b.Info.MinSeq, b.Info.MaxSeq); err != nil {
			return err
		}
		return insertActiveBlock(ctx, tx, rev, b, &out)
	})
	out.Revision = rev
	return out, err
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

// insertActiveBlock appends b to its namespace's active segment. The block
// must continue the segment's last block. The first block of a segment is
// checked by the seq key (direct mode) or the hot batch coverage (fold),
// together with CheckInvariants.
func insertActiveBlock(ctx context.Context, tx Tx, rev uint64, b Block, out *BlockCommit) error {
	seg, found, err := tx.ActiveSegment(ctx, b.Namespace)
	if err != nil {
		return err
	}
	if !found {
		return Corruptf(SourceInvariant, "namespace %s has no active segment", b.Namespace)
	}
	ordinal := 0
	last, found, err := tx.LastActiveBlock(ctx, b.Namespace, seg.Index)
	if err != nil {
		return err
	}
	if found {
		if last.MaxSeq+1 != b.Info.MinSeq {
			return Corruptf(SourceInvariant, "%s segment %d block %d ends at %d; new block starts at %d",
				b.Namespace, seg.Index, last.Ordinal, last.MaxSeq, b.Info.MinSeq)
		}
		ordinal = last.Ordinal + 1
	}
	id, err := resolve(ctx, tx, b.Object)
	if err != nil {
		return err
	}
	row := ActiveBlockRow{
		Namespace:          b.Namespace,
		Segment:            seg.Index,
		Ordinal:            ordinal,
		ObjectID:           id,
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
	*out = BlockCommit{Segment: seg.Index, Ordinal: ordinal, ObjectID: id}
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
	var out SealCommit
	rev, err := s.run(ctx, TxSeal, func(tx Tx, rev uint64) error {
		seg, found, err := tx.ActiveSegment(ctx, sl.Namespace)
		if err != nil {
			return err
		}
		if !found || seg.Index != sl.Segment {
			return Corruptf(SourceSeal, "%s segment %d is not the active segment", sl.Namespace, sl.Segment)
		}
		rows, err := tx.ActiveBlocksForUpdate(ctx, sl.Namespace, sl.Segment)
		if err != nil {
			return err
		}
		if err := checkSealList(sl, hdr, rows); err != nil {
			return err
		}
		footerID, err := resolve(ctx, tx, sl.Footer)
		if err != nil {
			return err
		}
		ids := make([]uint64, len(rows))
		for i, r := range rows {
			ids[i] = r.ObjectID
		}
		if err := refCheck(ctx, tx, ids...); err != nil {
			return err
		}
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

// ErrNotImplemented is returned by scripts whose stage has not landed.
var ErrNotImplemented = errors.New("catalog: not implemented")

// PublishGeneration publishes a compacted generation (§12.2). It lands with
// compaction in stage 4; disaggregated mode refuses to enable compaction
// until then (plan D5).
func (s *Session) PublishGeneration(context.Context) error {
	return fmt.Errorf("catalog: PublishGeneration: %w until stage 4", ErrNotImplemented)
}

// InitNamespace creates ns's first active segment if the namespace has no
// active segment, and applies meta in the same transaction either way.
func (s *Session) InitNamespace(ctx context.Context, ns Namespace, meta []metastore.Op) (uint64, error) {
	if !ns.Valid() {
		return 0, s.fail(fmt.Errorf("catalog: unknown namespace %q", ns))
	}
	return s.run(ctx, TxNamespace, func(tx Tx, rev uint64) error {
		_, found, err := tx.ActiveSegment(ctx, ns)
		if err != nil {
			return err
		}
		if !found {
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
	return s.run(ctx, TxNamespace, func(tx Tx, rev uint64) error {
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
	return s.run(ctx, TxMetadata, func(tx Tx, rev uint64) error {
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
	slots := make([][]UploadSlot, len(calls))
	_, err := s.run(ctx, TxObjects, func(tx Tx, rev uint64) error {
		var fresh []NewObject
		var at [][2]int
		for i, c := range calls {
			slots[i] = make([]UploadSlot, len(c.reqs))
			for j, r := range c.reqs {
				row, found, err := tx.FindAvailableObject(ctx, r.SHA256, c.maxUnrefAge)
				if err != nil {
					return err
				}
				if found {
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
		ids, err := tx.InsertObjects(ctx, fresh)
		if err != nil {
			return err
		}
		if len(ids) != len(fresh) {
			return fmt.Errorf("inserted %d object rows, want %d", len(ids), len(fresh))
		}
		for k, ij := range at {
			slots[ij[0]][ij[1]] = UploadSlot{ObjectID: ids[k]}
		}
		return nil
	})
	if err != nil {
		return nil, time.Time{}, err
	}
	// The monotonic reading taken after commit is the start of the
	// orphan-age window for the skip-PUT rule.
	return slots, time.Now(), nil
}

// MarkAvailable is §7.3 step 6 as its own transaction. It returns the
// object ID callers must reference: ref.ID, or the row of another upload of
// the same bytes that won first.
func (s *Session) MarkAvailable(ctx context.Context, ref ObjectRef) (uint64, error) {
	var id uint64
	_, err := s.run(ctx, TxObjects, func(tx Tx, rev uint64) error {
		var err error
		id, err = markAvailable(ctx, tx, ref.ID, ref.SHA256)
		return err
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
