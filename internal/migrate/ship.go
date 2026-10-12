package migrate

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/seqspace"
	"github.com/bluesky-social/jetstream/segment"
)

// maxSealedPerShip bounds the sealed segments one ship call imports, so
// the main loop gets back to operator requests during a long seed.
const maxSealedPerShip = 16

// tracker is what the catalog holds of main, kept from the session's own
// commits: the migrator is the only writer.
type tracker struct {
	// seg is main's active segment, and active the blocks shipped into it.
	seg    uint64
	active []shippedBlock
	// next is main's seq key: every seq below it is shipped.
	next uint64
}

type shippedBlock struct {
	info segment.BlockInfo
	obj  uint64
}

// loadSnapshot reads the whole catalog in one snapshot.
func loadSnapshot(ctx context.Context, db catalog.DB) (*catalog.Snapshot, error) {
	rtx, err := db.BeginRead(ctx)
	if err != nil {
		return nil, fmt.Errorf("migrate: read catalog: %w", err)
	}
	snap, err := catalog.LoadSnapshot(ctx, rtx)
	_ = rtx.Close(ctx)
	if err != nil {
		return nil, fmt.Errorf("migrate: read catalog: %w", err)
	}
	return snap, nil
}

// loadTracker reads the catalog in one snapshot.
func loadTracker(ctx context.Context, db catalog.DB) (*tracker, *catalog.Snapshot, error) {
	snap, err := loadSnapshot(ctx, db)
	if err != nil {
		return nil, nil, err
	}
	tr := &tracker{}
	found := false
	for _, s := range snap.Segments {
		if s.Namespace == catalog.Main && s.State == catalog.Active {
			tr.seg, found = s.Index, true
		}
	}
	if !found {
		return nil, nil, catalog.Corruptf(catalog.SourceMigration, "main has no active segment")
	}
	for _, b := range snap.ActiveBlocks {
		if b.Namespace != catalog.Main {
			continue
		}
		if b.Segment != tr.seg || b.Ordinal != len(tr.active) {
			return nil, nil, catalog.Corruptf(catalog.SourceMigration, "main active block %d/%d out of order", b.Segment, b.Ordinal)
		}
		tr.active = append(tr.active, shippedBlock{info: blockInfoOf(b), obj: b.ObjectID})
	}
	v, ok := snap.Meta[catalog.MainSeqKey]
	if tr.next, err = catalog.DecodeSeq(catalog.MainSeqKey, v, ok); err != nil {
		return nil, nil, err
	}
	return tr, snap, nil
}

func blockInfoOf(b catalog.ActiveBlockRow) segment.BlockInfo {
	return segment.BlockInfo{
		CompressedSize: uint32(b.CompressedLength), UncompressedSize: uint32(b.UncompressedLength),
		EventCount: b.EventCount, MinSeq: b.MinSeq, MaxSeq: b.MaxSeq,
		MinWitnessedAt: b.MinWitnessedUS, MaxWitnessedAt: b.MaxWitnessedUS,
	}
}

// fatalError is a migration failure a new session would only repeat: the
// source and the catalog disagree about what was shipped. The migrator
// stops and leaves it to the operator.
type fatalError struct{ err error }

func (e fatalError) Error() string      { return e.err.Error() }
func (e fatalError) Unwrap() error      { return e.err }
func (e fatalError) SessionFatal() bool { return true }

func fatalf(format string, args ...any) error {
	return fatalError{fmt.Errorf("migrate: "+format, args...)}
}

// reconcile checks that what the catalog holds is still what the source
// holds (migration plan §6.3): each imported generation against the local
// file's (reconcileSealed), and each shipped active block's bytes against
// the local file's. Compaction is paused, so a mismatch means something
// rewrote the source behind the migration's back.
func (m *Migrator) reconcile(ctx context.Context, tr *tracker, snap *catalog.Snapshot, local uint64) error {
	segs, err := m.reconcileSealed(snap)
	if err != nil {
		return err
	}
	if len(tr.active) > 0 {
		if tr.seg >= uint64(len(segs)) {
			return fatalf("reconcile: the catalog holds blocks of segment %d, which the source does not have", tr.seg)
		}
		lv := segs[tr.seg]
		if err := checkPrefix(lv, tr.active); err != nil {
			return err
		}
		f, err := openSegment(m.cfg.FS, m.cfg.Catalog.Path(catalog.Main, lv.Index), m.thr)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		for i, b := range tr.active {
			frame, err := f.frame(ctx, lv.Blocks[i])
			if err != nil {
				return err
			}
			o, ok := snap.Objects[b.obj]
			if !ok || o.SHA256 != sha256.Sum256(frame) {
				return fatalf("reconcile: segment %d block %d in the catalog is not the source's", tr.seg, i)
			}
		}
	}
	if tr.next > local {
		return fatalf("reconcile: the catalog's seq/next %d is past the source's %d", tr.next, local)
	}
	return nil
}

// reconcileSealed checks each imported generation's header checksum, which
// covers the header and footer, against the one the source's catalog holds
// for that segment. It reads no files. It returns the source's main
// segments.
func (m *Migrator) reconcileSealed(snap *catalog.Snapshot) ([]catalog.SegmentView, error) {
	segs := m.cfg.Catalog.Snapshot().Segments(catalog.Main)
	for _, s := range snap.Segments {
		if s.Namespace != catalog.Main || s.State != catalog.Sealed {
			continue
		}
		g, ok := snap.Generations[s.GenerationID]
		if !ok {
			return nil, catalog.Corruptf(catalog.SourceMigration, "main segment %d has no generation %d", s.Index, s.GenerationID)
		}
		hdr, err := segment.ReadSealedHeader(byteReaderAt(g.Header))
		if err != nil {
			return nil, catalog.Corruptf(catalog.SourceMigration, "main segment %d header: %v", s.Index, err)
		}
		if s.Index >= uint64(len(segs)) || segs[s.Index].State != catalog.Sealed || segs[s.Index].Generation != hdr.Checksum {
			return nil, fatalf("reconcile: the catalog's segment %d (checksum %x) is not the source's; was the source compacted during the migration?", s.Index, hdr.Checksum)
		}
	}
	return segs, nil
}

// checkPrefix checks that the source's segment still begins with the
// blocks the catalog holds of it.
func checkPrefix(lv catalog.SegmentView, shipped []shippedBlock) error {
	if len(lv.Blocks) < len(shipped) {
		return fatalf("segment %d has %d blocks in the source; the catalog holds %d", lv.Index, len(lv.Blocks), len(shipped))
	}
	for i, b := range shipped {
		if !sameBlock(lv.Blocks[i], b.info) {
			return fatalf("segment %d block %d in the source is %+v; the catalog holds %+v", lv.Index, i, lv.Blocks[i], b.info)
		}
	}
	return nil
}

// progress is one look at the source: its segments, its durable seq/next
// read after them, and its vacancy registry read after that. A block the
// view holds is shipped only once it is below next, so only blocks whose
// metadata batch committed leave the source.
type progress struct {
	segs []catalog.SegmentView
	next uint64
	gaps *seqspace.Gaps
}

func (m *Migrator) look(ctx context.Context) (progress, error) {
	view := m.cfg.Catalog.Snapshot()
	next, err := localNext(ctx, m.cfg.Meta)
	if err != nil {
		return progress{}, err
	}
	gaps, err := ingest.LoadSeqGaps(m.cfg.Meta)
	if err != nil {
		return progress{}, fmt.Errorf("migrate: %w", err)
	}
	return progress{segs: view.Segments(catalog.Main), next: next, gaps: gaps}, nil
}

// ship moves the catalog toward the source: sealed segments first, a
// pipelined batch of them per call, then at most one transaction of active
// blocks, so a shadow pod never decodes more than that in one poll. With
// all set it ships everything below the source's seq/next. It reports
// whether no block below it is left to ship; the seq keys can still differ
// by a vacancy no block bounds yet.
func (m *Migrator) ship(ctx context.Context, sess *catalog.Session, tr *tracker, all bool) (bool, error) {
	p, err := m.look(ctx)
	if err != nil {
		return false, err
	}
	m.cfg.Metrics.setSegments(len(p.segs), int(tr.seg)+1)
	defer func() { m.cfg.Metrics.setSeqs(p.next, tr.next) }()
	m.setProgress(p.next, tr, len(p.segs))
	for {
		if tr.seg >= uint64(len(p.segs)) {
			return true, nil
		}
		lv := p.segs[tr.seg]
		if lv.Index != tr.seg {
			return false, fatalf("the source's main segment at position %d is %d", tr.seg, lv.Index)
		}
		if err := checkPrefix(lv, tr.active); err != nil {
			return false, err
		}
		if lv.State == catalog.Sealed && lv.MaxSeq() < p.next {
			if len(tr.active) == 0 {
				if err := m.importSealedRun(ctx, sess, tr, p); err != nil {
					return false, err
				}
				if !all && tr.seg < uint64(len(p.segs)) && p.segs[tr.seg].State == catalog.Sealed {
					return false, nil
				}
				continue
			}
			for len(tr.active) < len(lv.Blocks) {
				if err := m.shipActive(ctx, sess, tr, lv, p); err != nil {
					return false, err
				}
			}
			if err := m.importSeal(ctx, sess, tr, lv); err != nil {
				return false, err
			}
			continue
		}
		pending := 0
		for _, b := range lv.Blocks[len(tr.active):] {
			if b.MaxSeq >= p.next {
				break
			}
			pending++
		}
		if pending == 0 {
			return true, nil
		}
		if err := m.shipActive(ctx, sess, tr, lv, p); err != nil {
			return false, err
		}
		if !all {
			return false, nil
		}
	}
}

// prepared is a sealed segment read and uploaded, ready to import.
type prepared struct {
	is    catalog.ImportSegment
	bytes int64
	err   error
}

// importSealedRun imports the sealed segments from tr.seg on, up to the
// first one not yet below the source's seq/next: up to SegmentConcurrency
// read and upload at once, and they import strictly in index order.
func (m *Migrator) importSealedRun(ctx context.Context, sess *catalog.Session, tr *tracker, p progress) error {
	var run []catalog.SegmentView
	for _, lv := range p.segs[tr.seg:] {
		if lv.State != catalog.Sealed || lv.MaxSeq() >= p.next || len(run) == maxSealedPerShip {
			break
		}
		run = append(run, lv)
	}
	pctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	defer wg.Wait()
	results := make([]chan prepared, len(run))
	started := 0
	start := func() {
		i := started
		started++
		results[i] = make(chan prepared, 1)
		wg.Go(func() {
			is, n, err := m.prepSealed(pctx, sess, run[i], p.gaps)
			results[i] <- prepared{is: is, bytes: n, err: err}
		})
	}
	for i, lv := range run {
		for started < len(run) && started < i+m.cfg.SegmentConcurrency {
			start()
		}
		r := <-results[i]
		if r.err != nil {
			return r.err
		}
		if _, err := sess.ImportSealedSegment(ctx, r.is); err != nil {
			return fmt.Errorf("migrate: import segment %d: %w", lv.Index, err)
		}
		if err := m.crash(ctx, crashpoint.AfterMigrationSegmentImport); err != nil {
			return err
		}
		tr.seg, tr.active = lv.Index+1, nil
		if len(lv.Blocks) > 0 {
			tr.next = lv.MaxSeq() + 1
		}
		m.cfg.Metrics.imported(1, len(r.is.Blocks), r.bytes)
		m.setProgress(p.next, tr, len(p.segs))
	}
	return nil
}

// prepSealed reads a sealed segment and uploads its blocks and footer, in
// chunks so only a chunk of frames is in memory at once.
func (m *Migrator) prepSealed(ctx context.Context, sess *catalog.Session, lv catalog.SegmentView, gaps *seqspace.Gaps) (catalog.ImportSegment, int64, error) {
	f, err := openSegment(m.cfg.FS, m.cfg.Catalog.Path(catalog.Main, lv.Index), m.thr)
	if err != nil {
		return catalog.ImportSegment{}, 0, err
	}
	defer func() { _ = f.Close() }()
	parts, err := f.sealed(ctx, lv.Generation)
	if err != nil {
		return catalog.ImportSegment{}, 0, err
	}
	if len(parts.blocks) != len(lv.Blocks) {
		return catalog.ImportSegment{}, 0, fmt.Errorf("%w: segment %d has %d blocks; the catalog saw %d", errLocalChanged, lv.Index, len(parts.blocks), len(lv.Blocks))
	}
	is := catalog.ImportSegment{Index: lv.Index, Header: parts.header, Vacancies: gaps}
	var total int64
	var frames [][]byte
	var chunk int
	upload := func() error {
		if len(frames) == 0 {
			return nil
		}
		refs, err := m.uploader.Upload(ctx, sess, frames)
		if err != nil {
			return fmt.Errorf("migrate: upload segment %d: %w", lv.Index, err)
		}
		for _, ref := range refs {
			is.Blocks = append(is.Blocks, catalog.ImportBlock{Info: parts.blocks[len(is.Blocks)], Object: ref})
		}
		frames, chunk = frames[:0], 0
		return nil
	}
	for _, b := range parts.blocks {
		frame, err := f.frame(ctx, b)
		if err != nil {
			return catalog.ImportSegment{}, 0, err
		}
		frames = append(frames, frame)
		chunk += len(frame)
		total += int64(len(frame))
		if chunk >= uploadChunkBytes {
			if err := upload(); err != nil {
				return catalog.ImportSegment{}, 0, err
			}
		}
	}
	if err := upload(); err != nil {
		return catalog.ImportSegment{}, 0, err
	}
	refs, err := m.uploader.Upload(ctx, sess, [][]byte{parts.footer})
	if err != nil {
		return catalog.ImportSegment{}, 0, fmt.Errorf("migrate: upload segment %d footer: %w", lv.Index, err)
	}
	is.Footer = refs[0]
	if err := m.crash(ctx, crashpoint.AfterMigrationSegmentUpload); err != nil {
		return catalog.ImportSegment{}, 0, err
	}
	return is, total + int64(len(parts.header)+len(parts.footer)), nil
}

// shipActive ships the next durable blocks of lv after those the catalog
// holds, up to TailMaxBlocksPerTxn, in one transaction with the vacancy
// registry.
func (m *Migrator) shipActive(ctx context.Context, sess *catalog.Session, tr *tracker, lv catalog.SegmentView, p progress) error {
	var infos []segment.BlockInfo
	for _, b := range lv.Blocks[len(tr.active):] {
		if b.MaxSeq >= p.next || len(infos) == m.cfg.TailMaxBlocksPerTxn {
			break
		}
		infos = append(infos, b)
	}
	if len(infos) == 0 {
		return nil
	}
	f, err := openSegment(m.cfg.FS, m.cfg.Catalog.Path(catalog.Main, lv.Index), m.thr)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	frames := make([][]byte, len(infos))
	var n int64
	for i, b := range infos {
		if frames[i], err = f.frame(ctx, b); err != nil {
			return err
		}
		n += int64(len(frames[i]))
	}
	refs, err := m.uploader.Upload(ctx, sess, frames)
	if err != nil {
		return fmt.Errorf("migrate: upload segment %d blocks: %w", lv.Index, err)
	}
	bs := make([]catalog.Block, len(infos))
	for i, b := range infos {
		bs[i] = catalog.Block{Namespace: catalog.Main, Info: b, Object: refs[i]}
	}
	if err := m.crash(ctx, crashpoint.AfterMigrationActiveUpload); err != nil {
		return err
	}
	commits, err := sess.ImportActiveBlocks(ctx, catalog.ImportBlocks{Blocks: bs, Vacancies: p.gaps})
	if err != nil {
		return fmt.Errorf("migrate: ship segment %d blocks: %w", lv.Index, err)
	}
	for i, c := range commits {
		tr.active = append(tr.active, shippedBlock{info: infos[i], obj: c.ObjectID})
	}
	tr.next = infos[len(infos)-1].MaxSeq + 1
	m.cfg.Metrics.imported(0, len(infos), n)
	return nil
}

// importSeal seals the catalog's active segment, whose blocks are all
// shipped, with the source's own header and footer.
func (m *Migrator) importSeal(ctx context.Context, sess *catalog.Session, tr *tracker, lv catalog.SegmentView) error {
	f, err := openSegment(m.cfg.FS, m.cfg.Catalog.Path(catalog.Main, lv.Index), m.thr)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	parts, err := f.sealed(ctx, lv.Generation)
	if err != nil {
		return err
	}
	if len(parts.blocks) != len(tr.active) {
		return fatalf("segment %d sealed with %d blocks; the catalog holds %d", lv.Index, len(parts.blocks), len(tr.active))
	}
	sl := catalog.Seal{Namespace: catalog.Main, Segment: lv.Index, Header: parts.header}
	for i, b := range tr.active {
		if !sameBlock(parts.blocks[i], b.info) {
			return fatalf("segment %d block %d sealed as %+v; the catalog holds %+v", lv.Index, i, parts.blocks[i], b.info)
		}
		sl.Blocks = append(sl.Blocks, catalog.SealBlock{ObjectID: b.obj, CompressedLength: int64(b.info.CompressedSize)})
	}
	refs, err := m.uploader.Upload(ctx, sess, [][]byte{parts.footer})
	if err != nil {
		return fmt.Errorf("migrate: upload segment %d footer: %w", lv.Index, err)
	}
	sl.Footer = refs[0]
	if _, err := sess.ImportSeal(ctx, sl); err != nil {
		return fmt.Errorf("migrate: seal segment %d: %w", lv.Index, err)
	}
	tr.seg, tr.active = lv.Index+1, nil
	m.cfg.Metrics.imported(1, 0, int64(len(parts.footer)))
	return nil
}
