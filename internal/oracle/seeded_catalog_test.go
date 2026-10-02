package oracle

import (
	"bytes"
	"encoding/binary"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/catalog/follower"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/ingest/live"
	"github.com/bluesky-social/jetstream/internal/ingest/maintainer"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/objstore/memblob"
	"github.com/bluesky-social/jetstream/internal/objstore/protocol"
	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/bluesky-social/jetstream/internal/storagefake"
	"github.com/bluesky-social/jetstream/segment"
)

const (
	seedTestAccounts   = 6
	seedTestLive       = 40
	seedTestBlock      = 8
	seedTestMaxSegment = 2 << 10
)

type seedRig struct {
	t      *testing.T
	w      *world.World
	db     *storagefake.DB
	blob   *memblob.Blob
	s      *catalog.Session
	up     *protocol.Uploader
	reader *protocol.Reader
	seeded *SeededCatalog
}

func newSeedRig(t *testing.T, seed uint64) *seedRig {
	t.Helper()
	w := newRestartWorld(t, Config{
		Seed:              seed,
		Accounts:          seedTestAccounts,
		MinInitialRecords: 2,
		MaxInitialRecords: 5,
		LiveEventsSteady:  seedTestLive,
	})
	t.Cleanup(func() { require.NoError(t, w.Close()) })

	db := storagefake.New(storagefake.Config{
		Invariants:  catalog.InvariantOptions{MaxEventsPerBlock: seedTestBlock},
		OnViolation: func(rev uint64, err error) { t.Errorf("catalog invariant at revision %d: %v", rev, err) },
	})
	lease := db.NewLease()
	require.NoError(t, lease.Acquire(t.Context(), time.Hour))
	r := &seedRig{t: t, w: w, db: db, blob: memblob.New()}
	r.s = catalog.NewSession(catalog.SessionConfig{DB: db, Epoch: lease.Epoch()})
	archive := db.Archive().ArchiveID
	var err error
	r.up, err = protocol.NewUploader(protocol.UploaderConfig{Blob: r.blob, ArchiveID: archive, GCDelay: time.Hour, OrphanAge: time.Hour})
	require.NoError(t, err)
	r.reader, err = protocol.NewReader(protocol.ReaderConfig{Rows: protocol.DBRows{DB: db}, Blob: r.blob, ArchiveID: archive})
	require.NoError(t, err)

	r.seeded, err = SeedCatalog(t.Context(), SeedCatalogConfig{
		World:             w,
		LiveEvents:        seedTestLive,
		Session:           r.s,
		Uploader:          r.up,
		MaxEventsPerBlock: seedTestBlock,
		MaxSegmentBytes:   seedTestMaxSegment,
	})
	require.NoError(t, err)
	return r
}

func (r *seedRig) snapshot() *catalog.Snapshot {
	r.t.Helper()
	snap, err := r.db.Snapshot()
	require.NoError(r.t, err)
	return snap
}

// meta dumps every metadata row.
func (r *seedRig) meta() map[string][]byte {
	r.t.Helper()
	it, err := r.db.MetaStore(nil).NewIter(r.t.Context(), nil, nil)
	require.NoError(r.t, err)
	out := map[string][]byte{}
	for it.Next() {
		out[string(it.Key())] = bytes.Clone(it.Value())
	}
	require.NoError(r.t, it.Err())
	require.NoError(r.t, it.Close())
	return out
}

// archived reads main from seq 1 through a ready follower, the read path
// every hot-mode pod serves from.
func (r *seedRig) archived() []segment.Event {
	r.t.Helper()
	ctx := r.t.Context()
	f, err := follower.New(follower.Config{
		DB: r.db, Blob: r.blob, ArchiveID: r.db.Archive().ArchiveID,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:         follower.NewMetrics(prometheus.NewRegistry()),
		ReadConcurrency: 4,
	})
	require.NoError(r.t, err)
	require.NoError(r.t, f.Refresh(ctx))
	require.NoError(r.t, f.Ready(ctx), "a seeded catalog is servable")
	var out []segment.Event
	for ref := range f.Snapshot().RefsFrom(catalog.Main, 1) {
		evs, err := catalog.DecodeRef(ctx, f, ref)
		require.NoError(r.t, err)
		for _, ev := range evs {
			ev.Payload = bytes.Clone(ev.Payload)
			out = append(out, ev)
		}
	}
	return out
}

// persisted strips what the block format does not keep.
func persisted(evs []segment.Event) []segment.Event {
	out := make([]segment.Event, len(evs))
	for i, ev := range evs {
		ev.UpstreamRelayCursor = 0
		if len(ev.Payload) == 0 {
			ev.Payload = nil
		}
		out[i] = ev
	}
	return out
}

func TestSeedCatalog_SteadyState(t *testing.T) {
	t.Parallel()
	r := newSeedRig(t, 17)
	sc := r.seeded
	ctx := t.Context()

	snap := r.snapshot()
	require.NoError(t, r.db.Violation())
	require.NoError(t, catalog.CheckInvariants(snap, catalog.InvariantOptions{MaxEventsPerBlock: seedTestBlock}))
	require.GreaterOrEqual(t, sc.SealedSegments, 2, "sealed segments")
	require.Positive(t, sc.ActiveBlocks, "active blocks")
	require.Len(t, snap.ActiveBlocks, sc.ActiveBlocks)
	require.Empty(t, snap.HotBatches)
	require.Positive(t, sc.BackfillEvents)
	require.Greater(t, len(sc.Events), sc.BackfillEvents, "live rows")

	store := r.db.MetaStore(nil)
	phase, err := lifecycle.ReadPhase(ctx, store)
	require.NoError(t, err)
	require.Equal(t, lifecycle.PhaseSteadyState, phase)
	cursor, err := live.LoadUpstreamCursor(store, catalog.RelayCursorKey)
	require.NoError(t, err)
	require.Equal(t, r.w.CurrentSeq(), cursor)
	require.Equal(t, sc.RelayCursor, cursor)
	require.Equal(t, sc.LiveStartCursor+seedTestLive, sc.RelayCursor)
	next, err := catalog.DecodeSeq(catalog.MainSeqKey, snap.Meta[catalog.MainSeqKey], true)
	require.NoError(t, err)
	require.Equal(t, sc.NextSeq, next)
	require.Equal(t, uint64(len(sc.Events))+1, next)

	counts, ok, err := backfill.LoadCounts(store)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(seedTestAccounts), counts.Total)
	require.Equal(t, uint64(seedTestAccounts), counts.Complete)
	require.Equal(t, seedTestAccounts, sc.Repos)

	// The archive is what the world produced: its final state, and, for
	// the live rows, the event log the firehose implies.
	got := r.archived()
	require.Equal(t, persisted(sc.Events), persisted(got))
	observed := observedEventsFromSegments(got)
	require.NoError(t, CheckInvariants(observed))
	model, err := Reconstruct(observed)
	require.NoError(t, err)
	truth, err := GroundTruthFromWorld(r.w)
	require.NoError(t, err)
	require.NoError(t, Compare(truth, model))

	liveRows := make([]segment.Event, 0, len(sc.Events)-sc.BackfillEvents)
	for _, ev := range sc.Events[sc.BackfillEvents:] {
		ev.Seq = uint64(ev.UpstreamRelayCursor)
		liveRows = append(liveRows, ev)
	}
	want, err := ExpectedEventLogFromFirehose(r.w, sc.LiveStartCursor, seedTestLive)
	require.NoError(t, err)
	require.NoError(t, CompareEventLogs(want, NormalizeEventLog(observedEventsFromSegments(liveRows))))

	// Each sealed generation is byte-identical to local mode's segment file
	// for the same rows.
	seq := 0
	for _, seg := range snap.Segments {
		if seg.Namespace != catalog.Main || seg.State != catalog.Sealed {
			continue
		}
		row := snap.Generations[seg.GenerationID]
		file := bytes.Clone(row.Header)
		for _, gb := range snap.GenerationBlocks[seg.GenerationID] {
			frame, err := r.reader.Get(ctx, gb.ObjectID)
			require.NoError(t, err)
			file = binary.LittleEndian.AppendUint64(file, uint64(len(frame)))
			file = append(file, frame...)
		}
		footer, err := r.reader.Get(ctx, row.FooterObjectID)
		require.NoError(t, err)
		file = append(file, footer...)
		hdr, err := segment.ReadSealedHeader(bytes.NewReader(row.Header))
		require.NoError(t, err)
		require.Equal(t, uint64(seq+1), hdr.MinSeq)
		n := int(hdr.EventCount)
		require.Equal(t, localSegment(t, sc.Events[seq:seq+n]), file, "segment %d", seg.Index)
		seq += n
	}
}

// localSegment writes evs through local mode's segment writer and returns
// the sealed file.
func localSegment(t *testing.T, evs []segment.Event) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), "seg")
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: seedTestBlock})
	require.NoError(t, err)
	for _, ev := range evs {
		full, err := w.Append(ev)
		require.NoError(t, err)
		if full {
			require.NoError(t, w.Flush())
		}
	}
	_, err = w.Seal()
	require.NoError(t, err)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}

func TestSeedCatalog_Deterministic(t *testing.T) {
	t.Parallel()
	a, b := newSeedRig(t, 29), newSeedRig(t, 29)
	require.Equal(t, a.seeded, b.seeded)
	require.Equal(t, a.meta(), b.meta())
}

// A hot-mode leader session opens on a seeded catalog like on one it wrote
// itself: Rebuild passes its session-start check with nothing to resume,
// and the writer continues main at NextSeq.
func TestSeedCatalog_HotSessionContinues(t *testing.T) {
	t.Parallel()
	r := newSeedRig(t, 41)
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	m, err := maintainer.Open(ctx, maintainer.Config{
		Session:           r.s,
		Uploader:          r.up,
		Objects:           r.reader,
		MaxEventsPerBlock: seedTestBlock,
		MaxSegmentBytes:   seedTestMaxSegment,
		Logger:            logger,
	})
	require.NoError(t, err)
	open, err := m.Rebuild(ctx, maintainer.RebuildConfig{BlockMaxAge: time.Hour})
	require.NoError(t, err)
	require.Nil(t, open, "a seeded catalog has no hot batches")
	w, err := ingest.Open(ingest.Config{
		MaxEventsPerBlock: seedTestBlock,
		Hot: &ingest.HotConfig{
			Session:           r.s,
			Uploader:          r.up,
			Sink:              m,
			BatchMaxEvents:    3,
			BlockMaxAge:       time.Hour,
			MaxUnfoldedEvents: 2 * seedTestBlock,
		},
		Logger: logger,
	})
	require.NoError(t, err)

	var added []segment.Event
	for len(added) < 3*seedTestBlock {
		frame, err := r.w.GenerateOneForTest(ctx)
		require.NoError(t, err)
		evt, err := decodeOracleFirehoseFrame(frame)
		require.NoError(t, err)
		rows, err := expectedSegmentEventsFromFirehoseEvent(r.w, evt)
		require.NoError(t, err)
		for i := range rows {
			rows[i].WitnessedAt = seedEpoch.Add(time.Hour).UnixMicro()
			require.NoError(t, w.Append(ctx, &rows[i]))
			added = append(added, rows[i])
		}
	}
	require.NoError(t, w.ForceRotate(ctx))
	require.NoError(t, w.Close())
	require.NoError(t, m.Close())
	require.Equal(t, r.seeded.NextSeq, added[0].Seq)

	require.NoError(t, r.db.Violation())
	require.NoError(t, catalog.CheckInvariants(r.snapshot(), catalog.InvariantOptions{MaxEventsPerBlock: seedTestBlock}))
	got := r.archived()
	require.Equal(t, persisted(slices.Concat(r.seeded.Events, added)), persisted(got))
}
