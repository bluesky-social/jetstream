package catalog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/seqspace"
	"github.com/bluesky-social/jetstream/segment"
)

// This file holds the transaction scripts that import a local archive into
// the catalog (specs/notes/2026-10-09-local-to-disagg-migration.md §6.4).
// They are built from the same Tx primitives as the leader's own scripts,
// and every one but SetMigrationState refuses unless migration/state says
// a migration is importing, so none can run against a catalog that
// disaggregated mode built or that a finished migration handed off.
//
// Imports keep the seqs, segment indexes, and segment bytes of the source.
// They are the only scripts that accept a registered seq vacancy, and only
// one the source registered, above what the catalog already holds.

// migrationRead reads and locks MigrationStateKey.
func migrationRead() *MetaRead { return &MetaRead{Key: []byte(MigrationStateKey)} }

// storedMigrationState decodes what r read. The empty state is an absent
// key.
func storedMigrationState(r *MetaRead) (MigrationState, error) {
	if !r.Found {
		return "", nil
	}
	st, err := ParseMigrationState(r.Value)
	if err != nil {
		return "", Corruptf(SourceMeta, "%v", err)
	}
	return st, nil
}

// ErrNotImporting means an import script ran while migration/state did not
// allow it: the migration was aborted or handed off, or the catalog was
// never a migration's.
var ErrNotImporting = errors.New("catalog: the catalog is not importing a local archive")

func checkImporting(r *MetaRead) error {
	st, err := storedMigrationState(r)
	if err != nil {
		return err
	}
	if !st.Importing() {
		return fmt.Errorf("%w (migration/state %q)", ErrNotImporting, st)
	}
	return nil
}

// importReads are the reads every block import takes with its fence: the
// migration state, Main's seq key and vacancies, and Main's active segment
// and its last block.
type importReads struct {
	state, seq, vac *MetaRead
	seg             ActiveSegmentRead
	last            LastActiveBlockRead
}

func newImportReads() *importReads {
	return &importReads{
		state: migrationRead(),
		seq:   seqRead(Main),
		vac:   &MetaRead{Key: []byte(VacanciesKey)},
		seg:   ActiveSegmentRead{Namespace: Main},
		last:  LastActiveBlockRead{Namespace: Main},
	}
}

func (r *importReads) reads() []Read {
	return []Read{r.state, r.seq, r.vac, &r.seg, &r.last}
}

// frontier checks the migration state and returns Main's seq key, the
// stored vacancy set, and that set with incoming merged in.
func (r *importReads) frontier(incoming *seqspace.Gaps) (next uint64, stored, gaps *seqspace.Gaps, err error) {
	if err := checkImporting(r.state); err != nil {
		return 0, nil, nil, err
	}
	if next, err = DecodeSeq(MainSeqKey, r.seq.Value, r.seq.Found); err != nil {
		return 0, nil, nil, err
	}
	if stored, err = DecodeVacancies(r.vac.Value, r.vac.Found); err != nil {
		return 0, nil, nil, err
	}
	if !r.seg.Found {
		return 0, nil, nil, Corruptf(SourceInvariant, "main has no active segment")
	}
	if r.last.Found && (r.last.Row.Segment != r.seg.Row.Index || r.last.Row.MaxSeq+1 != next) {
		return 0, nil, nil, Corruptf(SourceInvariant, "main's last active block %d/%d ends at %d; %s is %d (active segment %d)",
			r.last.Row.Segment, r.last.Row.Ordinal, r.last.Row.MaxSeq, MainSeqKey, next, r.seg.Row.Index)
	}
	gaps, err = mergeVacancies(stored, incoming, next)
	return next, stored, gaps, err
}

// mergeVacancies returns the vacancy set after an import that brings the
// source's whole registry, incoming (nil brings nothing). The registry only
// grows, and only above what the catalog holds: every stored vacancy must
// still be in it, and anything new must lie at or above next.
func mergeVacancies(stored, incoming *seqspace.Gaps, next uint64) (*seqspace.Gaps, error) {
	if incoming == nil {
		return stored, nil
	}
	for _, g := range stored.Ranges() {
		if end, ok := incoming.EndContaining(g.Start); !ok || end < g.End {
			return nil, Corruptf(SourceMigration, "the source no longer registers vacancy [%d,%d)", g.Start, g.End)
		}
	}
	for _, g := range incoming.Ranges() {
		if g.Start >= next {
			continue
		}
		if end, ok := stored.EndContaining(g.Start); !ok || end < min(g.End, next) {
			return nil, Corruptf(SourceMigration, "the source registers vacancy [%d,%d) below the imported frontier %d", g.Start, g.End, next)
		}
	}
	return incoming, nil
}

// walkEnvelopes checks that blocks continue the seq space from next, each
// starting where the one before ended or across exactly one registered
// vacancy, and that no vacancy overlaps a block. Compaction keeps every
// block's envelope, so this holds for compacted and empty blocks too. It
// returns the seq after the last block.
func walkEnvelopes(blocks []segment.BlockInfo, next uint64, gaps *seqspace.Gaps) (uint64, error) {
	ranges := make([]seqspace.BlockRange, len(blocks))
	for i, b := range blocks {
		if b.MinSeq != next && !Bridges(gaps, next, b.MinSeq) {
			return 0, Corruptf(SourceMigration, "block [%d,%d] does not continue the archive at seq %d, and no vacancy is exactly [%d,%d)",
				b.MinSeq, b.MaxSeq, next, next, b.MinSeq)
		}
		next = b.MaxSeq + 1
		ranges[i] = seqspace.BlockRange{Min: b.MinSeq, Max: b.MaxSeq}
	}
	if err := gaps.ValidateVacant(ranges); err != nil {
		return 0, Corruptf(SourceMigration, "%v", err)
	}
	return next, nil
}

// vacancyOps are the metadata writes an import ends with: the new seq key,
// and the vacancies below it if they changed. A vacancy at or past the new
// frontier waits for the import that ships the block after it, so the
// catalog never registers one that no block bounds. walkEnvelopes already
// refused any vacancy that overlaps a block, so none straddles next.
func vacancyOps(next uint64, stored, gaps *seqspace.Gaps) ([]metastore.Op, error) {
	ops := []metastore.Op{{Kind: metastore.OpSet, Key: []byte(MainSeqKey), Value: EncodeSeq(next)}}
	var below []seqspace.Gap
	for _, g := range gaps.Ranges() {
		if g.Start < next {
			below = append(below, g)
		}
	}
	if slices.Equal(stored.Ranges(), below) {
		return ops, nil
	}
	kept, err := seqspace.NewGaps(below)
	if err != nil {
		return nil, Corruptf(SourceMigration, "vacancies below %d: %v", next, err)
	}
	return append(ops, metastore.Op{Kind: metastore.OpSet, Key: []byte(VacanciesKey), Value: EncodeVacancies(kept)}), nil
}

// ImportSegment is one sealed local segment to import as a generation.
type ImportSegment struct {
	// Index is the segment's index, which must be Main's active segment.
	Index uint64
	// Header is the file's 256-byte header.
	Header []byte
	// Footer is the object holding the file's bytes from footer_offset to
	// EOF.
	Footer ObjectRef
	// Blocks are the footer's blocks, in order: each block's index entry
	// and the object holding its frame.
	Blocks []ImportBlock
	// Vacancies is the source's whole vacancy registry. Nil leaves the
	// stored set as it is.
	Vacancies *seqspace.Gaps
}

// ImportBlock is one block of an ImportSegment.
type ImportBlock struct {
	Info   segment.BlockInfo
	Object ObjectRef
}

// validate checks the segment's shape: its header, and block index
// entries that tile [header, footer_offset) in order with sane bounds.
func (is ImportSegment) validate() (segment.Header, error) {
	hdr, err := segment.ReadSealedHeader(bytes.NewReader(is.Header))
	if err != nil {
		return segment.Header{}, fmt.Errorf("catalog: import segment %d: %w", is.Index, err)
	}
	if is.Footer.ID == 0 {
		return segment.Header{}, fmt.Errorf("catalog: import segment %d: no footer object", is.Index)
	}
	if int(hdr.BlockCount) != len(is.Blocks) {
		return segment.Header{}, fmt.Errorf("catalog: import segment %d: header has %d blocks; import lists %d", is.Index, hdr.BlockCount, len(is.Blocks))
	}
	off := uint64(segment.ReservedHeaderBytes)
	var events uint64
	for i, b := range is.Blocks {
		info := b.Info
		switch {
		case b.Object.ID == 0:
			return segment.Header{}, fmt.Errorf("catalog: import segment %d block %d: no object", is.Index, i)
		case info.Offset != off || info.CompressedSize == 0:
			return segment.Header{}, fmt.Errorf("catalog: import segment %d block %d: at offset %d with %d bytes; expected offset %d",
				is.Index, i, info.Offset, info.CompressedSize, off)
		case info.MinSeq == 0 || info.MaxSeq < info.MinSeq || uint64(info.EventCount) > info.MaxSeq-info.MinSeq+1:
			return segment.Header{}, fmt.Errorf("catalog: import segment %d block %d: bounds [%d,%d] with %d events",
				is.Index, i, info.MinSeq, info.MaxSeq, info.EventCount)
		}
		off += 8 + uint64(info.CompressedSize)
		events += uint64(info.EventCount)
	}
	if hdr.FooterOffset != off || uint64(hdr.EventCount) != events {
		return segment.Header{}, fmt.Errorf("catalog: import segment %d: header has footer at %d and %d events; blocks end at %d with %d",
			is.Index, hdr.FooterOffset, hdr.EventCount, off, events)
	}
	if n := len(is.Blocks); n > 0 && (hdr.MinSeq != is.Blocks[0].Info.MinSeq || hdr.MaxSeq != is.Blocks[n-1].Info.MaxSeq) {
		return segment.Header{}, fmt.Errorf("catalog: import segment %d: header covers [%d,%d]; blocks cover [%d,%d]",
			is.Index, hdr.MinSeq, hdr.MaxSeq, is.Blocks[0].Info.MinSeq, is.Blocks[n-1].Info.MaxSeq)
	}
	return hdr, nil
}

// ImportSealedSegment imports a sealed local segment as the active Main
// segment's one generation, seals it, and opens the next segment, in one
// transaction. The segment must be Main's active segment and hold no active
// blocks. Unlike Seal it takes a compacted generation: blocks with fewer
// events than their envelope, empty blocks, and an empty segment.
func (s *Session) ImportSealedSegment(ctx context.Context, is ImportSegment) (SealCommit, error) {
	if _, err := is.validate(); err != nil {
		return SealCommit{}, s.fail(err)
	}
	infos := make([]segment.BlockInfo, len(is.Blocks))
	// The blocks in order, then the footer, as PublishGeneration orders them.
	refs := make([]ObjectRef, 0, len(is.Blocks)+1)
	for i, b := range is.Blocks {
		infos[i] = b.Info
		refs = append(refs, b.Object)
	}
	ir, objs := newImportReads(), newObjectReads(append(refs, is.Footer)...)
	var out SealCommit
	rev, err := s.run(ctx, TxSeal, append(ir.reads(), objs.reads()...), func(tx Tx, rev uint64) error {
		next, stored, gaps, err := ir.frontier(is.Vacancies)
		if err != nil {
			return err
		}
		if ir.seg.Row.Index != is.Index || ir.last.Found {
			return Corruptf(SourceMigration, "import of segment %d: main's active segment is %d (has blocks: %t)",
				is.Index, ir.seg.Row.Index, ir.last.Found)
		}
		if len(infos) > 0 {
			if next, err = walkEnvelopes(infos, next, gaps); err != nil {
				return fmt.Errorf("import of segment %d: %w", is.Index, err)
			}
		}
		ids, err := objs.resolve(ctx, tx)
		if err != nil {
			return err
		}
		blockIDs, footerID := ids[:len(is.Blocks)], ids[len(is.Blocks)]
		gen, err := tx.InsertGeneration(ctx, GenerationRow{
			Namespace:      Main,
			Segment:        is.Index,
			Header:         is.Header,
			FooterObjectID: footerID,
			Revision:       rev,
		})
		if err != nil {
			return err
		}
		if len(is.Blocks) > 0 {
			gbs := make([]GenerationBlockRow, len(is.Blocks))
			for i, b := range is.Blocks {
				gbs[i] = GenerationBlockRow{GenerationID: gen, Ordinal: i, ObjectID: blockIDs[i], CompressedLength: int64(b.Info.CompressedSize)}
			}
			if err := tx.InsertGenerationBlocks(ctx, gbs); err != nil {
				return err
			}
		}
		ok, err := tx.SealSegment(ctx, Main, is.Index, gen, rev)
		if err != nil {
			return err
		}
		if !ok {
			return Corruptf(SourceMigration, "main segment %d stopped being active mid-import", is.Index)
		}
		if err := tx.InsertSegment(ctx, SegmentRow{Namespace: Main, Index: is.Index + 1, State: Active, Revision: rev}); err != nil {
			return err
		}
		out = SealCommit{GenerationID: gen, FooterObjectID: footerID}
		ops, err := vacancyOps(next, stored, gaps)
		if err != nil {
			return err
		}
		return tx.ApplyMeta(ctx, ops)
	})
	out.Revision = rev
	return out, err
}

// ImportBlocks is a run of the source's durable active blocks to import.
type ImportBlocks struct {
	// Blocks are consecutive Main blocks of the active segment. Each is
	// dense, as a local active block is: compaction never touches the
	// active segment. Their Meta must be empty.
	Blocks []Block
	// Vacancies is the source's whole vacancy registry. Nil leaves the
	// stored set as it is.
	Vacancies *seqspace.Gaps
}

// ImportActiveBlocks appends blocks to Main's active segment in one
// transaction. It is CommitBlocks except that a block may start past the
// seq key across exactly one registered vacancy, which the same import may
// register.
func (s *Session) ImportActiveBlocks(ctx context.Context, ib ImportBlocks) ([]BlockCommit, error) {
	if len(ib.Blocks) == 0 {
		return nil, s.fail(errors.New("catalog: import of no blocks"))
	}
	infos := make([]segment.BlockInfo, len(ib.Blocks))
	refs := make([]ObjectRef, len(ib.Blocks))
	for i, b := range ib.Blocks {
		if err := b.validate(); err != nil {
			return nil, s.fail(err)
		}
		if b.Namespace != Main || len(b.Meta) > 0 {
			return nil, s.fail(errors.New("catalog: imported blocks are main's and carry no metadata"))
		}
		infos[i], refs[i] = b.Info, b.Object
	}
	ir, objs := newImportReads(), newObjectReads(refs...)
	out := make([]BlockCommit, len(ib.Blocks))
	rev, err := s.run(ctx, TxBlock, append(ir.reads(), objs.reads()...), func(tx Tx, rev uint64) error {
		next, stored, gaps, err := ir.frontier(ib.Vacancies)
		if err != nil {
			return err
		}
		if next, err = walkEnvelopes(infos, next, gaps); err != nil {
			return fmt.Errorf("import of active blocks: %w", err)
		}
		ids, err := objs.resolve(ctx, tx)
		if err != nil {
			return err
		}
		ordinal := 0
		if ir.last.Found {
			ordinal = ir.last.Row.Ordinal + 1
		}
		seg := ir.seg.Row.Index
		for i, b := range ib.Blocks {
			row := ActiveBlockRow{
				Namespace:          Main,
				Segment:            seg,
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
			out[i] = BlockCommit{Segment: seg, Ordinal: row.Ordinal, ObjectID: row.ObjectID}
		}
		ops, err := vacancyOps(next, stored, gaps)
		if err != nil {
			return err
		}
		return tx.ApplyMeta(ctx, ops)
	})
	if err != nil {
		return nil, err
	}
	for i := range out {
		out[i].Revision = rev
	}
	return out, nil
}

// ImportSeal seals Main's active segment with the header and footer of the
// source's sealed file, after its blocks were imported as active blocks.
// It is Seal, refused unless the catalog is importing. Seal's checks hold
// as they are: the footer lists exactly the active blocks, and the header's
// event count is theirs, vacancies between them included.
func (s *Session) ImportSeal(ctx context.Context, sl Seal) (SealCommit, error) {
	if sl.Namespace != Main {
		return SealCommit{}, s.fail(fmt.Errorf("catalog: import seal of %s segment %d", sl.Namespace, sl.Segment))
	}
	return s.seal(ctx, sl, migrationRead())
}

// protectedKeys are the metadata keys only catalog scripts write. ImportMeta
// refuses to touch them: the import scripts keep the seq key and vacancies
// in step with the blocks, and SetMigrationState owns the migration keys.
var protectedKeys = []string{MainSeqKey, BootstrapLiveSeqKey, VacanciesKey, MigrationStateKey, MigrationHandoffSeqKey}

// ErrProtectedKey means a metadata import would write a key only catalog
// scripts may write.
var ErrProtectedKey = errors.New("catalog: metadata import touches a catalog-owned key")

func checkUnprotected(ops []metastore.Op) error {
	for _, op := range ops {
		for _, k := range protectedKeys {
			key := []byte(k)
			hit := bytes.Equal(op.Key, key)
			if op.Kind == metastore.OpDeleteRange {
				hit = bytes.Compare(op.Key, key) <= 0 && bytes.Compare(key, op.End) < 0
			}
			if hit {
				return fmt.Errorf("%w: %s %q", ErrProtectedKey, op.Kind, k)
			}
		}
	}
	return nil
}

// ImportMeta applies ops, copied from the source's metadata, in one
// transaction. It refuses unless the catalog is importing, and refuses ops
// on catalog-owned keys.
func (s *Session) ImportMeta(ctx context.Context, ops []metastore.Op) (uint64, error) {
	if err := checkUnprotected(ops); err != nil {
		return 0, s.fail(err)
	}
	state := migrationRead()
	return s.run(ctx, TxMetadata, []Read{state}, func(tx Tx, rev uint64) error {
		if err := checkImporting(state); err != nil {
			return err
		}
		if len(ops) == 0 {
			return nil
		}
		return tx.ApplyMeta(ctx, ops)
	})
}

// ErrMigrationState means SetMigrationState found a state other than the
// one it was to move from.
var ErrMigrationState = errors.New("catalog: migration state is not the expected one")

// SetMigrationState moves migration/state from from to to, and applies
// meta, in one transaction. The empty from is an absent key. The move must
// be a ValidMigrationTransition, and meta may not write a catalog-owned key,
// except migration/handoff_seq on the move to done.
// A stored state other than from fails with ErrMigrationState, which ends
// the session like any failed script: the caller re-reads the state in a
// new one.
func (s *Session) SetMigrationState(ctx context.Context, from, to MigrationState, meta []metastore.Op) (uint64, error) {
	if !ValidMigrationTransition(from, to) {
		return 0, s.fail(fmt.Errorf("catalog: migration state cannot move from %q to %q", from, to))
	}
	for _, op := range meta {
		// The handoff seq is the done transaction's own record; every other
		// catalog-owned key belongs to the import scripts.
		if op.Kind == metastore.OpSet && to == MigrationDone && bytes.Equal(op.Key, []byte(MigrationHandoffSeqKey)) {
			continue
		}
		if op.Kind == metastore.OpDeleteRange {
			return 0, s.fail(errors.New("catalog: a migration state change carries only point writes"))
		}
		if err := checkUnprotected([]metastore.Op{op}); err != nil {
			return 0, s.fail(err)
		}
	}
	state := migrationRead()
	return s.run(ctx, TxMetadata, []Read{state}, func(tx Tx, rev uint64) error {
		stored, err := storedMigrationState(state)
		if err != nil {
			return err
		}
		if stored != from {
			return fmt.Errorf("%w: it is %q, expected %q", ErrMigrationState, stored, from)
		}
		ops := append(slices.Clone(meta), metastore.Op{Kind: metastore.OpSet, Key: []byte(MigrationStateKey), Value: []byte(to)})
		return tx.ApplyMeta(ctx, ops)
	})
}

// ErrNotEmpty means InitMigration found a catalog that already holds an
// archive other than a just-started migration.
var ErrNotEmpty = errors.New("catalog: the catalog already holds an archive")

// InitMigration starts a migration on a catalog `storage init` just
// created: it creates Main's segment 0 and sets migration/state to seeding,
// in one transaction, so no pod can find the catalog without a phase and
// without a migration state. It creates no bootstrap_live segment, as a
// migrated archive is past merge. Run again on a catalog it already set up,
// it does nothing.
func (s *Session) InitMigration(ctx context.Context) (uint64, error) {
	state, phase := migrationRead(), &MetaRead{Key: []byte(lifecycle.PhaseKey)}
	mainSeg, liveSeg := &ActiveSegmentRead{Namespace: Main}, &ActiveSegmentRead{Namespace: BootstrapLive}
	seq := seqRead(Main)
	return s.run(ctx, TxNamespace, []Read{state, phase, mainSeg, liveSeg, seq}, func(tx Tx, rev uint64) error {
		stored, err := storedMigrationState(state)
		if err != nil {
			return err
		}
		switch {
		case stored == MigrationSeeding && !phase.Found && !liveSeg.Found && mainSeg.Found && mainSeg.Row.Index == 0 && !seq.Found:
			return nil
		case stored != "" || phase.Found || mainSeg.Found || liveSeg.Found || seq.Found:
			return fmt.Errorf("%w (migration/state %q, phase found %t, main active %t, bootstrap_live active %t)",
				ErrNotEmpty, stored, phase.Found, mainSeg.Found, liveSeg.Found)
		}
		if err := tx.InsertSegment(ctx, SegmentRow{Namespace: Main, Index: 0, State: Active, Revision: rev}); err != nil {
			return err
		}
		return tx.ApplyMeta(ctx, []metastore.Op{{Kind: metastore.OpSet, Key: []byte(MigrationStateKey), Value: []byte(MigrationSeeding)}})
	})
}

// ReadMigrationState reads migration/state in one fenced transaction: what
// it returns cannot change while this session holds the lease. A
// disaggregated leader runs it first, before any write (§6.2).
func (s *Session) ReadMigrationState(ctx context.Context) (MigrationState, error) {
	state := migrationRead()
	var out MigrationState
	_, err := s.run(ctx, TxMetadata, []Read{state}, func(tx Tx, rev uint64) error {
		var err error
		out, err = storedMigrationState(state)
		return err
	})
	return out, err
}
