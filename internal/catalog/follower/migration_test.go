package follower_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/objstore/memblob"
	"github.com/bluesky-social/jetstream/internal/objstore/protocol"
	"github.com/bluesky-social/jetstream/internal/seqspace"
	"github.com/bluesky-social/jetstream/internal/storagefake"
	"github.com/bluesky-social/jetstream/segment"
)

// newMigrationRig is a rig whose catalog a migration started: main segment
// 0 and migration/state seeding, written by InitMigration.
func newMigrationRig(t *testing.T) *rig {
	t.Helper()
	db := storagefake.New(storagefake.Config{ArchiveID: testArchiveID})
	lease := db.NewLease()
	require.NoError(t, lease.Acquire(t.Context(), time.Hour))
	r := &rig{t: t, db: db, blob: memblob.New()}
	r.s = catalog.NewSession(catalog.SessionConfig{DB: db, Epoch: lease.Epoch()})
	_, err := r.s.InitMigration(t.Context())
	require.NoError(t, err)
	r.up, err = protocol.NewUploader(protocol.UploaderConfig{
		Blob: r.blob, ArchiveID: testArchiveID, GCDelay: 6 * time.Hour, OrphanAge: time.Hour,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if !t.Failed() {
			require.NoError(t, db.Violation(), "catalog invariants")
		}
	})
	return r
}

// addEvents appends events for seqs [lo, hi] to the rig's stream, leaving
// zero events at any seq it skipped (a vacancy).
func (r *rig) addEvents(lo, hi uint64) {
	r.t.Helper()
	for uint64(len(r.events))+1 < lo {
		r.events = append(r.events, segment.Event{})
	}
	now := time.Now().UnixMicro()
	for seq := lo; seq <= hi; seq++ {
		r.events = append(r.events, segment.Event{
			Seq: seq, WitnessedAt: now, Kind: segment.KindCreate,
			DID: fmt.Sprintf("did:plc:%d", seq%7), Collection: "app.bsky.feed.post",
			Rkey: fmt.Sprint(seq), Rev: "r", Payload: []byte{0xa1, 0x61, 0x61, byte(seq % 24)},
		})
	}
}

// importActive ships blocks [lo, hi] each, as a migrator does with the
// source's active segment, with the source's whole vacancy registry.
func (r *rig) importActive(gaps *seqspace.Gaps, ranges ...[2]uint64) {
	r.t.Helper()
	var bs []catalog.Block
	for _, rg := range ranges {
		r.addEvents(rg[0], rg[1])
		frame, info := r.frame(rg[0], rg[1])
		ref, err := r.up.Put(r.t.Context(), r.s, frame)
		require.NoError(r.t, err)
		bs = append(bs, catalog.Block{Namespace: catalog.Main, Info: info, Object: ref})
		r.active = append(r.active, activeBlock{frame: frame, info: info})
	}
	cs, err := r.s.ImportActiveBlocks(r.t.Context(), catalog.ImportBlocks{Blocks: bs, Vacancies: gaps})
	require.NoError(r.t, err)
	for i, c := range cs {
		r.active[len(r.active)-len(cs)+i].id = c.ObjectID
	}
}

// importSeal seals the active segment from its blocks' frames, as
// ImportSeal does with the source's own footer.
func (r *rig) importSeal() {
	r.t.Helper()
	frames := make([][]byte, len(r.active))
	sl := catalog.Seal{Namespace: catalog.Main, Segment: r.seg}
	for i, b := range r.active {
		frames[i] = b.frame
		sl.Blocks = append(sl.Blocks, catalog.SealBlock{ObjectID: b.id, CompressedLength: int64(b.info.CompressedSize)})
	}
	hdr, footer, _, err := segment.BuildSealed(segment.SliceFrameSource(frames))
	require.NoError(r.t, err)
	sl.Header = hdr
	sl.Footer, err = r.up.Put(r.t.Context(), r.s, footer)
	require.NoError(r.t, err)
	_, err = r.s.ImportSeal(r.t.Context(), sl)
	require.NoError(r.t, err)
	r.active = nil
	r.seg++
}

func (r *rig) setMigration(from, to catalog.MigrationState, meta ...metastore.Op) {
	r.t.Helper()
	_, err := r.s.SetMigrationState(r.t.Context(), from, to, meta)
	require.NoError(r.t, err)
}

// requireVacantStream is requireStream for a stream with vacancies: the
// rig's zero events stand for the seqs nobody holds.
func (r *rig) requireVacantStream(got []segment.Event, from uint64) {
	r.t.Helper()
	var want []segment.Event
	for _, ev := range r.events[from-1:] {
		if ev.Seq != 0 {
			want = append(want, ev)
		}
	}
	require.Len(r.t, got, len(want))
	for i := range want {
		require.Equal(r.t, want[i].Seq, got[i].Seq, "seq at %d", i)
		require.Equal(r.t, want[i].Payload, got[i].Payload, "payload of seq %d", want[i].Seq)
	}
}

// A follower of a migrated catalog reports the migration state, serves the
// imported vacancies, and its readable log and cold reads cross every one:
// between segments, between active blocks, and at the active tail before a
// seal.
func TestFollower_MigratedCatalogVacancies(t *testing.T) {
	t.Parallel()
	r := newMigrationRig(t)
	ctx := t.Context()
	f, _, _ := r.follower(followerOpts{})
	require.NoError(t, f.Refresh(ctx))
	st, ok := f.MigrationState()
	require.True(t, ok)
	require.Equal(t, catalog.MigrationSeeding, st)
	require.Error(t, f.Ready(ctx), "no phase while seeding")

	// The seed: a vacancy at seq 1, then one between blocks.
	g1 := gapsOf(t, seqspace.Gap{Start: 1, End: 3}, seqspace.Gap{Start: 8, End: 12})
	r.importActive(g1, [2]uint64{3, 7}, [2]uint64{12, 15})
	r.importSeal()
	r.setMigration(catalog.MigrationSeeding, catalog.MigrationTailing,
		metastore.Op{Kind: metastore.OpSet, Key: []byte(lifecycle.PhaseKey), Value: []byte(lifecycle.PhaseSteadyState)})
	require.NoError(t, f.Refresh(ctx))
	require.NoError(t, f.Ready(ctx))
	st, _ = f.MigrationState()
	require.Equal(t, catalog.MigrationTailing, st)
	require.Equal(t, g1.Ranges(), f.SeqGaps().Ranges())
	require.Equal(t, uint64(16), f.NextSeq())

	// Tailing: a vacancy between segments, and one between active blocks,
	// each shipped with the first block after it.
	g2 := gapsOf(t, append(g1.Ranges(), seqspace.Gap{Start: 16, End: 4112}, seqspace.Gap{Start: 4114, End: 4200})...)
	r.importActive(g2, [2]uint64{4112, 4113})
	require.NoError(t, f.Refresh(ctx))
	r.importActive(g2, [2]uint64{4200, 4210}, [2]uint64{4211, 4211})
	require.NoError(t, f.Refresh(ctx))
	r.importSeal()
	require.NoError(t, f.Refresh(ctx))
	require.Equal(t, g2.Ranges(), f.SeqGaps().Ranges())

	// The log skipped the vacancy past its old tip, so its floor moved
	// over it: seqs below the floor are the cold reader's.
	require.Equal(t, uint64(4112), f.Log().FloorSeq())
	r.requireVacantStream(logEvents(t, f, 4112), 4112)
	r.requireVacantStream(coldEvents(t, f, 1), 1)

	r.setMigration(catalog.MigrationTailing, catalog.MigrationHandingOff)
	r.setMigration(catalog.MigrationHandingOff, catalog.MigrationDone)
	require.NoError(t, f.Refresh(ctx))
	st, _ = f.MigrationState()
	require.Equal(t, catalog.MigrationDone, st)
	require.False(t, st.BlocksLeader())
}

func gapsOf(t *testing.T, gs ...seqspace.Gap) *seqspace.Gaps {
	t.Helper()
	g, err := seqspace.NewGaps(gs)
	require.NoError(t, err)
	return g
}
