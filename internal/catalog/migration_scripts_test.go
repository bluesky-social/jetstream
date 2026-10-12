package catalog_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/leader"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/seqspace"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// newMigrationHarness is newHarness on a catalog `storage init
// --migrate-from-local` set up: main segment 0 and migration/state seeding.
func newMigrationHarness(t *testing.T, b backend) *harness {
	t.Helper()
	db, newLock := b.open(t)
	h := &harness{t: t, db: &checkedDB{DB: db}, newLock: newLock, lm: leader.NewMetrics(prometheus.NewRegistry())}
	h.lock = newLock()
	require.NoError(t, h.lock.Acquire(t.Context(), time.Hour))
	h.s = catalog.NewSession(catalog.SessionConfig{DB: h.db, Epoch: h.lock.Epoch(), LeaderMetrics: h.lm})
	_, err := h.s.InitMigration(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() {
		if !t.Failed() && !h.violationOK {
			require.NoError(t, h.db.Violation(), "catalog invariants")
		}
	})
	return h
}

// localSegment is a sealed local segment file split into the parts an
// import uploads.
type localSegment struct {
	header, footer []byte
	frames         [][]byte
	infos          []segment.BlockInfo
	hdr            segment.Header
}

func migrationEvent(seq uint64) segment.Event {
	return segment.Event{
		Seq: seq, WitnessedAt: int64(seq), Kind: segment.KindCreate,
		DID: "did:plc:test", Collection: "app.bsky.feed.post", Rkey: fmt.Sprint(seq), Rev: "r",
		Payload: []byte{0xa0},
	}
}

// writeLocalSegment writes a sealed segment whose blocks hold the inclusive
// seq ranges given, then, if drop is set, compacts away the rows it picks,
// as local compaction does.
func writeLocalSegment(t *testing.T, blocks [][2]uint64, drop func(seq uint64) bool) localSegment {
	t.Helper()
	path := filepath.Join(t.TempDir(), "seg.jss")
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 1 << 16})
	require.NoError(t, err)
	for _, b := range blocks {
		for seq := b[0]; seq <= b[1]; seq++ {
			_, err := w.Append(migrationEvent(seq))
			require.NoError(t, err)
		}
		require.NoError(t, w.Flush())
	}
	_, err = w.Seal()
	require.NoError(t, err)
	if drop != nil {
		res, err := segment.Rewrite(path, func(ev *segment.Event) segment.RowDecision {
			if drop(ev.Seq) {
				return segment.RowDrop
			}
			return segment.RowKeep
		}, segment.RewriteOptions{})
		require.NoError(t, err)
		require.True(t, res.Rewritten)
	}
	return splitLocalSegment(t, path)
}

func splitLocalSegment(t *testing.T, path string) localSegment {
	t.Helper()
	r, err := segment.Open(segment.ReaderConfig{Path: path})
	require.NoError(t, err)
	hdr, infos := r.Header(), r.Blocks()
	require.NoError(t, r.Close())
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	ls := localSegment{header: data[:segment.ReservedHeaderBytes], footer: data[hdr.FooterOffset:], infos: infos, hdr: hdr}
	for _, b := range infos {
		ls.frames = append(ls.frames, data[b.Offset+8:b.Offset+8+uint64(b.CompressedSize)])
	}
	return ls
}

// importOf uploads ls's parts and returns its import.
func (h *harness) importOf(idx uint64, ls localSegment, gaps *seqspace.Gaps) catalog.ImportSegment {
	h.t.Helper()
	is := catalog.ImportSegment{Index: idx, Header: ls.header, Footer: h.object(ls.footer), Vacancies: gaps}
	for i, f := range ls.frames {
		is.Blocks = append(is.Blocks, catalog.ImportBlock{Info: ls.infos[i], Object: h.object(f)})
	}
	return is
}

// activeOf uploads ls's blocks [from, to) as active block imports.
func (h *harness) activeOf(ls localSegment, from, to int) []catalog.Block {
	h.t.Helper()
	var out []catalog.Block
	for i := from; i < to; i++ {
		out = append(out, catalog.Block{Namespace: catalog.Main, Info: ls.infos[i], Object: h.object(ls.frames[i])})
	}
	return out
}

func gapsOf(t *testing.T, gs ...seqspace.Gap) *seqspace.Gaps {
	t.Helper()
	g, err := seqspace.NewGaps(gs)
	require.NoError(t, err)
	return g
}

func (h *harness) seqNext() uint64 {
	h.t.Helper()
	v, ok := h.meta(catalog.MainSeqKey)
	n, err := catalog.DecodeSeq(catalog.MainSeqKey, v, ok)
	require.NoError(h.t, err)
	return n
}

func (h *harness) vacancies() []seqspace.Gap {
	h.t.Helper()
	v, ok := h.meta(catalog.VacanciesKey)
	g, err := catalog.DecodeVacancies(v, ok)
	require.NoError(h.t, err)
	return g.Ranges()
}

func TestVacanciesRoundTrip(t *testing.T) {
	t.Parallel()
	g := gapsOf(t, seqspace.Gap{Start: 5, End: 9}, seqspace.Gap{Start: 1, End: 2}, seqspace.Gap{Start: 100, End: 4196})
	got, err := catalog.DecodeVacancies(catalog.EncodeVacancies(g), true)
	require.NoError(t, err)
	require.Equal(t, g.Ranges(), got.Ranges())
	empty, err := catalog.DecodeVacancies(nil, false)
	require.NoError(t, err)
	require.Zero(t, empty.Count())
	for _, bad := range [][]byte{{}, {2, 0, 0, 0, 0}, {1, 0, 0, 0, 1}, append(catalog.EncodeVacancies(g), 0)} {
		_, err := catalog.DecodeVacancies(bad, true)
		src, ok := catalog.IsCorruption(err)
		require.True(t, ok, "%x: %v", bad, err)
		require.Equal(t, catalog.SourceMeta, src)
	}
	// Out of order or adjacent ranges are not a normalized set.
	raw := catalog.EncodeVacancies(gapsOf(t, seqspace.Gap{Start: 1, End: 2}, seqspace.Gap{Start: 5, End: 9}))
	swapped := append(append([]byte{}, raw[:5]...), append(raw[21:37], raw[5:21]...)...)
	_, err = catalog.DecodeVacancies(swapped, true)
	require.Error(t, err)
}

func TestMigrationTransitions(t *testing.T) {
	t.Parallel()
	all := []catalog.MigrationState{"", catalog.MigrationSeeding, catalog.MigrationTailing, catalog.MigrationHandingOff,
		catalog.MigrationDone, catalog.MigrationAborted, catalog.MigrationReverted}
	valid := map[[2]catalog.MigrationState]bool{
		{"", catalog.MigrationSeeding}:                          true,
		{catalog.MigrationSeeding, catalog.MigrationTailing}:    true,
		{catalog.MigrationTailing, catalog.MigrationHandingOff}: true,
		{catalog.MigrationHandingOff, catalog.MigrationTailing}: true,
		{catalog.MigrationHandingOff, catalog.MigrationDone}:    true,
		{catalog.MigrationSeeding, catalog.MigrationAborted}:    true,
		{catalog.MigrationTailing, catalog.MigrationAborted}:    true,
		{catalog.MigrationHandingOff, catalog.MigrationAborted}: true,
		{catalog.MigrationDone, catalog.MigrationReverted}:      true,
	}
	for _, from := range all {
		for _, to := range all {
			require.Equal(t, valid[[2]catalog.MigrationState{from, to}], catalog.ValidMigrationTransition(from, to), "%q -> %q", from, to)
		}
		require.Equal(t, from != "" && from != catalog.MigrationDone, from.BlocksLeader(), "%q", from)
	}
}

// The seed's shape: sealed segments imported whole (one dense, one
// compacted down to an empty block, one empty), then the source's active
// segment imported block by block and sealed from the source's own footer.
// Every commit passes CheckInvariants, and the generations are the source
// files' bytes.
func TestMigration_ImportHappyPath(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newMigrationHarness(t, be)
		ctx := t.Context()

		seg0 := writeLocalSegment(t, [][2]uint64{{1, 4}, {5, 9}}, nil)
		c0, err := h.s.ImportSealedSegment(ctx, h.importOf(0, seg0, nil))
		require.NoError(t, err)
		require.Equal(t, uint64(10), h.seqNext())

		// Compaction kept every block's envelope while dropping rows: block
		// 1 is empty now, and the header counts fewer events than seqs.
		seg1 := writeLocalSegment(t, [][2]uint64{{10, 12}, {13, 15}, {16, 20}}, func(seq uint64) bool {
			return seq == 11 || (seq >= 13 && seq <= 15)
		})
		require.Equal(t, uint32(0), seg1.infos[1].EventCount)
		require.Less(t, seg1.hdr.EventCount, uint32(11))
		_, err = h.s.ImportSealedSegment(ctx, h.importOf(1, seg1, nil))
		require.NoError(t, err)
		require.Equal(t, uint64(21), h.seqNext())

		// The source's active segment: its first blocks ship while it is
		// active, the rest with its seal.
		seg2 := writeLocalSegment(t, [][2]uint64{{21, 22}, {23, 30}, {31, 31}}, nil)
		bc, err := h.s.ImportActiveBlocks(ctx, catalog.ImportBlocks{Blocks: h.activeOf(seg2, 0, 2)})
		require.NoError(t, err)
		require.Equal(t, 0, bc[0].Ordinal)
		require.Equal(t, uint64(2), bc[1].Segment)
		bc2, err := h.s.ImportActiveBlocks(ctx, catalog.ImportBlocks{Blocks: h.activeOf(seg2, 2, 3)})
		require.NoError(t, err)
		require.Equal(t, 2, bc2[0].Ordinal)
		require.Equal(t, uint64(32), h.seqNext())
		sl := catalog.Seal{Namespace: catalog.Main, Segment: 2, Header: seg2.header, Footer: h.object(seg2.footer)}
		for i, c := range append(bc, bc2...) {
			sl.Blocks = append(sl.Blocks, catalog.SealBlock{ObjectID: c.ObjectID, CompressedLength: int64(seg2.infos[i].CompressedSize)})
		}
		_, err = h.s.ImportSeal(ctx, sl)
		require.NoError(t, err)

		snap, err := h.snapshot()
		require.NoError(t, err)
		require.Len(t, snap.Segments, 4)
		require.Equal(t, catalog.Active, snap.Segments[3].State)
		require.Empty(t, snap.ActiveBlocks)
		g := snap.Generations[c0.GenerationID]
		require.Equal(t, seg0.header, g.Header, "the header is the source's, ETag included")
		require.NoError(t, catalog.CheckInvariants(snap, catalog.InvariantOptions{}))
		require.Empty(t, h.vacancies())
	})
}

// Vacancies the source registered after unclean restarts import in every
// position a local archive can hold one: at seq 1, between two blocks of a
// sealed segment, between segments, and between active blocks. The
// invariants accept exactly those.
func TestMigration_ImportVacancies(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newMigrationHarness(t, be)
		ctx := t.Context()
		all := gapsOf(t,
			seqspace.Gap{Start: 1, End: 4},
			seqspace.Gap{Start: 8, End: 20},
			seqspace.Gap{Start: 25, End: 30},
			seqspace.Gap{Start: 33, End: 40})

		// The source registers all four, but segment 0 bounds only the
		// first two; the rest wait for the blocks after them.
		seg0 := writeLocalSegment(t, [][2]uint64{{4, 7}, {20, 22}}, nil)
		_, err := h.s.ImportSealedSegment(ctx, h.importOf(0, seg0, all))
		require.NoError(t, err)
		require.Equal(t, uint64(23), h.seqNext())
		require.Equal(t, all.Ranges()[:2], h.vacancies())

		seg1 := writeLocalSegment(t, [][2]uint64{{23, 24}}, nil)
		_, err = h.s.ImportSealedSegment(ctx, h.importOf(1, seg1, all))
		require.NoError(t, err)

		seg2 := writeLocalSegment(t, [][2]uint64{{30, 32}, {40, 41}}, nil)
		_, err = h.s.ImportActiveBlocks(ctx, catalog.ImportBlocks{Blocks: h.activeOf(seg2, 0, 2), Vacancies: all})
		require.NoError(t, err)
		require.Equal(t, uint64(42), h.seqNext())
		require.Equal(t, all.Ranges(), h.vacancies())

		snap, err := h.snapshot()
		require.NoError(t, err)
		require.NoError(t, catalog.CheckInvariants(snap, catalog.InvariantOptions{}))
		require.NoError(t, catalog.CheckInvariants(snap, catalog.InvariantOptions{Cheap: true}))

		// A registry that also claims seqs a block holds, or seqs past the
		// frontier, is corrupt: replay would skip real events through it.
		for _, extra := range []seqspace.Gap{{Start: 31, End: 32}, {Start: 22, End: 23}, {Start: 4, End: 5}, {Start: 42, End: 50}} {
			bad, err := all.Add(extra)
			require.NoError(t, err)
			snap.Meta[catalog.VacanciesKey] = catalog.EncodeVacancies(bad)
			err = catalog.CheckInvariants(snap, catalog.InvariantOptions{})
			_, corrupt := catalog.IsCorruption(err)
			require.True(t, corrupt, "vacancy %v: %v", extra, err)
		}

		// Without the registry the same catalog is corrupt.
		delete(snap.Meta, catalog.VacanciesKey)
		err = catalog.CheckInvariants(snap, catalog.InvariantOptions{})
		_, corrupt := catalog.IsCorruption(err)
		require.True(t, corrupt, "%v", err)
	})
}

func TestMigration_ImportRefusesHoles(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		ctx := t.Context()
		for _, tc := range []struct {
			name string
			run  func(h *harness) error
		}{
			{"unregistered hole between segments", func(h *harness) error {
				_, err := h.s.ImportSealedSegment(ctx, h.importOf(0, writeLocalSegment(t, [][2]uint64{{3, 5}}, nil), nil))
				return err
			}},
			{"unregistered hole inside a sealed segment", func(h *harness) error {
				_, err := h.s.ImportSealedSegment(ctx, h.importOf(0, writeLocalSegment(t, [][2]uint64{{1, 5}, {7, 9}}, nil), nil))
				return err
			}},
			{"vacancy short of the next block", func(h *harness) error {
				_, err := h.s.ImportSealedSegment(ctx, h.importOf(0, writeLocalSegment(t, [][2]uint64{{5, 6}}, nil),
					gapsOf(t, seqspace.Gap{Start: 1, End: 4})))
				return err
			}},
			{"vacancy overlapping a block", func(h *harness) error {
				_, err := h.s.ImportSealedSegment(ctx, h.importOf(0, writeLocalSegment(t, [][2]uint64{{1, 9}}, nil),
					gapsOf(t, seqspace.Gap{Start: 3, End: 5})))
				return err
			}},
			{"unregistered hole before active blocks", func(h *harness) error {
				_, err := h.s.ImportActiveBlocks(ctx, catalog.ImportBlocks{Blocks: h.activeOf(writeLocalSegment(t, [][2]uint64{{2, 3}}, nil), 0, 1)})
				return err
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				h := newMigrationHarness(t, be)
				h.t = t
				requireCorruption(t, tc.run(h), catalog.SourceMigration)
				require.Equal(t, uint64(1), h.seqNext(), "nothing committed")
			})
		}
	})
}

// The registry only grows, and only above the imported frontier: a source
// that forgot a vacancy, or registers one inside history the catalog
// already holds, is refused.
func TestMigration_VacanciesOnlyGrowAboveFrontier(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newMigrationHarness(t, be)
		ctx := t.Context()
		seg0 := writeLocalSegment(t, [][2]uint64{{1, 4}, {9, 10}}, nil)
		_, err := h.s.ImportSealedSegment(ctx, h.importOf(0, seg0, gapsOf(t, seqspace.Gap{Start: 5, End: 9})))
		require.NoError(t, err)

		seg1 := writeLocalSegment(t, [][2]uint64{{11, 12}}, nil)
		_, err = h.s.ImportSealedSegment(ctx, h.importOf(1, seg1, gapsOf(t)))
		requireCorruption(t, err, catalog.SourceMigration)

		h.newSession()
		_, err = h.s.ImportSealedSegment(ctx, h.importOf(1, seg1, gapsOf(t, seqspace.Gap{Start: 2, End: 3}, seqspace.Gap{Start: 5, End: 9})))
		requireCorruption(t, err, catalog.SourceMigration)

		// The same registry again, and one that only adds above the
		// frontier, both import.
		h.newSession()
		_, err = h.s.ImportSealedSegment(ctx, h.importOf(1, seg1, gapsOf(t, seqspace.Gap{Start: 5, End: 9}, seqspace.Gap{Start: 13, End: 20})))
		require.NoError(t, err)
		require.Equal(t, uint64(13), h.seqNext())
	})
}

// No import runs outside the importing states, and the state machine moves
// only along its edges.
func TestMigration_StateGuards(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newMigrationHarness(t, be)
		ctx := t.Context()

		_, err := h.s.SetMigrationState(ctx, catalog.MigrationTailing, catalog.MigrationHandingOff, nil)
		require.ErrorIs(t, err, catalog.ErrMigrationState)
		require.ErrorIs(t, err, catalog.ErrSessionEnded)

		h.newSession()
		_, err = h.s.SetMigrationState(ctx, catalog.MigrationSeeding, catalog.MigrationDone, nil)
		require.Error(t, err, "seeding cannot jump to done")

		h.newSession()
		_, err = h.s.SetMigrationState(ctx, catalog.MigrationSeeding, catalog.MigrationTailing, []metastore.Op{
			{Kind: metastore.OpSet, Key: []byte(lifecycle.PhaseKey), Value: []byte(lifecycle.PhaseSteadyState)},
		})
		require.NoError(t, err)
		st, err := h.s.ReadMigrationState(ctx)
		require.NoError(t, err)
		require.Equal(t, catalog.MigrationTailing, st)
		v, ok := h.meta(lifecycle.PhaseKey)
		require.True(t, ok)
		require.Equal(t, string(lifecycle.PhaseSteadyState), string(v))

		_, err = h.s.ImportMeta(ctx, []metastore.Op{{Kind: metastore.OpSet, Key: []byte("repo/did:plc:a"), Value: []byte("x")}})
		require.NoError(t, err)
		for _, op := range []metastore.Op{
			{Kind: metastore.OpSet, Key: []byte(catalog.MainSeqKey), Value: catalog.EncodeSeq(9)},
			{Kind: metastore.OpDelete, Key: []byte(catalog.VacanciesKey)},
			{Kind: metastore.OpDeleteRange, Key: []byte("migration/"), End: []byte("migration0")},
		} {
			h.newSession()
			_, err = h.s.ImportMeta(ctx, []metastore.Op{op})
			require.ErrorIs(t, err, catalog.ErrProtectedKey, "%v", op)
		}

		// A state change cannot carry a write the import scripts own, and
		// only done may record the handoff seq.
		for _, op := range []metastore.Op{
			{Kind: metastore.OpSet, Key: []byte(catalog.MainSeqKey), Value: catalog.EncodeSeq(1)},
			{Kind: metastore.OpSet, Key: []byte(catalog.BootstrapLiveSeqKey), Value: catalog.EncodeSeq(1)},
			{Kind: metastore.OpDelete, Key: []byte(catalog.VacanciesKey)},
			{Kind: metastore.OpSet, Key: []byte(catalog.MigrationHandoffSeqKey), Value: catalog.EncodeSeq(1)},
		} {
			h.newSession()
			_, err = h.s.SetMigrationState(ctx, catalog.MigrationTailing, catalog.MigrationHandingOff, []metastore.Op{op})
			require.ErrorIs(t, err, catalog.ErrProtectedKey, "%v", op)
		}

		h.newSession()
		_, err = h.s.SetMigrationState(ctx, catalog.MigrationTailing, catalog.MigrationAborted, nil)
		require.NoError(t, err)
		seg0 := writeLocalSegment(t, [][2]uint64{{1, 2}}, nil)
		_, err = h.s.ImportSealedSegment(ctx, h.importOf(0, seg0, nil))
		require.ErrorIs(t, err, catalog.ErrNotImporting)
		h.newSession()
		_, err = h.s.ImportMeta(ctx, nil)
		require.ErrorIs(t, err, catalog.ErrNotImporting)
		h.newSession()
		_, err = h.s.ImportActiveBlocks(ctx, catalog.ImportBlocks{Blocks: h.activeOf(seg0, 0, 1)})
		require.ErrorIs(t, err, catalog.ErrNotImporting)
	})
}

// InitMigration runs only on an empty catalog, and again on one it set up.
func TestMigration_InitMigration(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newMigrationHarness(t, be)
		ctx := t.Context()
		_, err := h.s.InitMigration(ctx)
		require.NoError(t, err, "idempotent")
		st, err := h.s.ReadMigrationState(ctx)
		require.NoError(t, err)
		require.Equal(t, catalog.MigrationSeeding, st)
		snap, err := h.snapshot()
		require.NoError(t, err)
		require.Len(t, snap.Segments, 1)
		require.Equal(t, catalog.Main, snap.Segments[0].Namespace)

		// A catalog disaggregated mode built is never a migration's.
		plain := newHarness(t, be)
		_, err = plain.s.InitMigration(ctx)
		require.ErrorIs(t, err, catalog.ErrNotEmpty)
		st, err = func() (catalog.MigrationState, error) {
			plain.newSession()
			return plain.s.ReadMigrationState(ctx)
		}()
		require.NoError(t, err)
		require.Equal(t, catalog.MigrationState(""), st)
	})
}

// A fully compacted run of blocks encodes to identical frames, which the
// import stores once and references from every position.
func TestMigration_ImportDedupsIdenticalFrames(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newMigrationHarness(t, be)
		seg := writeLocalSegment(t, [][2]uint64{{1, 3}, {4, 6}, {7, 9}}, func(seq uint64) bool { return seq < 7 })
		require.Equal(t, seg.frames[0], seg.frames[1])
		c, err := h.s.ImportSealedSegment(t.Context(), h.importOf(0, seg, nil))
		require.NoError(t, err)
		snap, err := h.snapshot()
		require.NoError(t, err)
		gbs := snap.GenerationBlocks[c.GenerationID]
		require.Len(t, gbs, 3)
		require.Equal(t, gbs[0].ObjectID, gbs[1].ObjectID)
		require.NotEqual(t, gbs[1].ObjectID, gbs[2].ObjectID)
	})
}
