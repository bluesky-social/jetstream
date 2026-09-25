package maintainer_test

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/maintainer"
	"github.com/bluesky-social/jetstream/internal/storagefake"
	"github.com/bluesky-social/jetstream/segment"
)

// Hot batch rows left by sessions with different block boundaries rebuild
// correctly (design §10.9). Each seed runs a random history of sessions:
// batch sizes, classes, and storage vary; some sessions never fold, some
// stop folding partway, some end on a commit fault in the writer or the
// maintainer. Every rebuild must fold exactly the greedy groups that can no
// longer grow and hand back the rest, and a final session must seal an
// archive holding exactly the committed events.
func TestRebuild_PriorSessionShapes(t *testing.T) {
	t.Parallel()
	for seed := range 40 {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			t.Parallel()
			rng := rand.New(rand.NewPCG(uint64(seed), 11))
			e := newEnv(t, rng.IntN(2) == 0)
			var model []segment.Event // model[seq-1] is the event last assigned seq
			for range 3 + rng.IntN(5) {
				e.priorSession(rng, &model)
				e.restart()
			}
			e.finalSession(t)

			next := e.committedNext()
			snap := e.snapshot()
			requireNoActiveState(t, snap)
			var got []segment.Event
			for _, g := range e.generations(snap) {
				got = append(got, verify(t, g)...)
			}
			require.Len(t, got, int(next-1))
			for i := range got {
				requireSameEvent(t, model[i], got[i])
			}
		})
	}
}

// priorSession runs one random session and records every seq it assigned.
func (e *env) priorSession(rng *rand.Rand, model *[]segment.Event) {
	t := e.t
	faultsBefore := len(e.db.Unfired())
	if rng.IntN(3) == 0 {
		kinds := []catalog.TxKind{catalog.TxHotBatch, catalog.TxFold, catalog.TxSeal}
		e.db.InjectFaults(&storagefake.Fault{
			Kind:    []storagefake.FaultKind{storagefake.FaultCommitFails, storagefake.FaultCommitLost}[rng.IntN(2)],
			TxKind:  kinds[rng.IntN(len(kinds))],
			Ordinal: 1 + rng.IntN(3),
		})
		faultsBefore++
	}
	// A fault may end the session anywhere; anything else is a bug.
	failed := func(err error) {
		t.Helper()
		if err != nil {
			require.Less(t, len(e.db.Unfired()), faultsBefore, "error with no fault fired: %v", err)
		}
	}

	var w atomic.Pointer[ingest.Writer]
	m := e.maintainer(maintainer.Config{
		MaxSegmentBytes: []int64{1500, 4000, 1 << 20}[rng.IntN(3)],
		// The owner's job: release appends blocked on the unfolded cap.
		OnFailure: func(error) {
			if w := w.Load(); w != nil {
				go func() { _ = w.Close() }()
			}
		},
	})
	age := []time.Duration{time.Nanosecond, time.Hour}[rng.IntN(2)]
	before := e.snapshot()
	open, err := m.Rebuild(t.Context(), maintainer.RebuildConfig{BlockMaxAge: age})
	if err != nil {
		failed(err)
		failed(m.Close())
		return
	}
	e.checkRebuild(before, open, age)

	var sink ingest.BlockSink = m
	switch rng.IntN(3) {
	case 0:
		// The maintainer never sees a block: every closed block stays hot.
		sink = &dropSink{Maintainer: m}
	case 1:
		// Folding stops partway, as when a maintainer dies.
		sink = &dropSink{Maintainer: m, forward: int64(rng.IntN(4))}
	}
	hc := ingest.HotConfig{
		Resume:             open,
		BatchMaxEvents:     1 + rng.IntN(7),
		BulkChunkMaxEvents: 1 + rng.IntN(blockEvents),
	}
	if rng.IntN(2) == 0 {
		hc.Uploader = e.up
		hc.InlineBytesPerSec = []int64{-1, 1}[rng.IntN(2)]
	}
	wr := e.writer(sink, hc)
	w.Store(wr)
	ctx := t.Context()
	for n := rng.IntN(5 * blockEvents); n > 0; {
		evs := testEvents(rng, min(n, 1+rng.IntN(6)))
		n -= len(evs)
		actx := ctx
		if rng.IntN(3) == 0 {
			actx = ingest.WithClass(ctx, ingest.ClassBulk)
		}
		err := wr.AppendBatch(actx, evs)
		for _, ev := range evs {
			if ev.Seq != 0 {
				for uint64(len(*model)) < ev.Seq {
					*model = append(*model, segment.Event{})
				}
				(*model)[ev.Seq-1] = ev
			}
		}
		if err == nil {
			switch rng.IntN(10) {
			case 0:
				err = wr.Flush(ctx)
			case 1:
				err = wr.ForceRotate(ctx)
			}
		}
		if err != nil {
			failed(err)
			break
		}
	}
	failed(wr.Close())
	failed(m.Sync(ctx))
	failed(m.Close())
}

// checkRebuild checks one rebuild against the hot batches before it: every
// greedy group but the returned open block is now a block, and exactly the
// open block's batches are still hot.
func (e *env) checkRebuild(before *catalog.Snapshot, open *ingest.OpenBlock, age time.Duration) {
	t := e.t
	var groups [][]catalog.HotBatchRow
	n := 0
	for _, h := range before.HotBatches {
		if len(groups) == 0 || n+int(h.EventCount) > blockEvents {
			groups = append(groups, nil)
			n = 0
		}
		groups[len(groups)-1] = append(groups[len(groups)-1], h)
		n += int(h.EventCount)
	}
	after := e.snapshot()
	folded := groups
	if open != nil {
		require.Equal(t, time.Hour, age, "an aged last group folds")
		require.NotEmpty(t, groups)
		last := groups[len(groups)-1]
		folded = groups[:len(groups)-1]
		require.Less(t, len(open.Events), blockEvents, "a full last group folds")
		require.Len(t, open.Batches, len(last))
		require.Len(t, after.HotBatches, len(last))
		for i, h := range last {
			want := ingest.HotBatchInfo{FirstSeq: h.FirstSeq, LastSeq: h.LastSeq, ObjectID: h.ObjectID}
			require.Equal(t, want, open.Batches[i])
			require.Equal(t, h.FirstSeq, after.HotBatches[i].FirstSeq)
		}
		for i, ev := range open.Events {
			require.Equal(t, last[0].FirstSeq+uint64(i), ev.Seq)
		}
	} else {
		require.Empty(t, after.HotBatches)
		if age == time.Hour && len(groups) > 0 {
			last := groups[len(groups)-1]
			require.Equal(t, uint64(blockEvents), last[len(last)-1].LastSeq-last[0].FirstSeq+1,
				"a young last group folds only when full")
		}
	}
	ranges := e.blockRanges(after)
	for _, g := range folded {
		r := [2]uint64{g[0].FirstSeq, g[len(g)-1].LastSeq}
		require.Contains(t, ranges, r, "group [%d,%d] folded as one block", r[0], r[1])
	}
}

// blockRanges returns the seq range of every main block, sealed or active.
func (e *env) blockRanges(snap *catalog.Snapshot) [][2]uint64 {
	var out [][2]uint64
	for _, g := range e.generations(snap) {
		r, err := segment.OpenReaderParts(g.header, g.footer, func(i int) ([]byte, error) { return g.frames[i], nil }, segment.ReaderOptions{})
		require.NoError(e.t, err)
		for _, b := range r.Blocks() {
			out = append(out, [2]uint64{b.MinSeq, b.MaxSeq})
		}
		require.NoError(e.t, r.Close())
	}
	for _, b := range snap.ActiveBlocks {
		if b.Namespace == catalog.Main {
			out = append(out, [2]uint64{b.MinSeq, b.MaxSeq})
		}
	}
	return out
}

// An earlier session committed the fold that crossed the rotation threshold
// and ended before its seal. The rebuild seals that segment before folding
// anything onto it, so the generation matches local mode's.
func TestRebuild_SealsLeftoverFullSegment(t *testing.T) {
	t.Parallel()
	e := newEnv(t, false)
	evs := testEvents(rand.New(rand.NewPCG(5, 5)), 3*blockEvents)

	// Session 1 commits three blocks of hot batches and folds none.
	m := e.maintainer(maintainer.Config{})
	w := e.writer(&dropSink{Maintainer: m}, ingest.HotConfig{})
	appendAll(t, t.Context(), w, evs)
	require.NoError(t, w.Close())
	require.NoError(t, m.Close())

	// Session 2's rebuild folds two blocks, crossing the threshold, and
	// its seal fails.
	e.restart()
	e.db.InjectFaults(&storagefake.Fault{Kind: storagefake.FaultCommitFails, TxKind: catalog.TxSeal, Ordinal: 1})
	m = e.maintainer(maintainer.Config{MaxSegmentBytes: 1})
	_, err := m.Rebuild(t.Context(), maintainer.RebuildConfig{})
	require.ErrorIs(t, err, catalog.ErrSessionEnded)
	require.ErrorIs(t, m.Close(), catalog.ErrSessionEnded)
	snap := e.snapshot()
	require.Len(t, snap.ActiveBlocks, 1, "the first fold crossed the threshold")
	require.Equal(t, uint64(blockEvents+1), snap.HotBatches[0].FirstSeq)

	// Session 3 seals the leftover segment first, then folds.
	e.restart()
	m = e.maintainer(maintainer.Config{MaxSegmentBytes: 1})
	open, err := m.Rebuild(t.Context(), maintainer.RebuildConfig{})
	require.NoError(t, err)
	require.Nil(t, open)
	require.NoError(t, m.Close())
	snap = e.snapshot()
	requireNoActiveState(t, snap)
	gens := e.generations(snap)
	require.Len(t, gens, 3, "every block crosses the threshold on its own")
	for i, g := range gens {
		require.Equal(t, localSeal(t, evs[i*blockEvents:(i+1)*blockEvents]), g.file(), "generation %d", i)
	}
}

// Rebuild checks the catalog invariants first, and a violation ends the
// session before anything folds.
func TestRebuild_ChecksInvariants(t *testing.T) {
	t.Parallel()
	e := newEnv(t, false)
	m := e.maintainer(maintainer.Config{})
	w := e.writer(&dropSink{Maintainer: m}, ingest.HotConfig{})
	appendAll(t, t.Context(), w, testEvents(rand.New(rand.NewPCG(6, 6)), blockEvents))
	require.NoError(t, w.Close())
	require.NoError(t, m.Close())

	e.restart()
	m = e.maintainer(maintainer.Config{})
	_, err := m.Rebuild(t.Context(), maintainer.RebuildConfig{
		RelayCursor: func([]byte) error { return errors.New("cursor ahead of the commits") },
	})
	var corrupt *catalog.CorruptionError
	require.ErrorAs(t, err, &corrupt)
	require.ErrorAs(t, e.s.Err(), &corrupt, "the session ended")
	require.ErrorAs(t, m.Close(), &corrupt)
	require.Empty(t, e.snapshot().ActiveBlocks)
}

// finalSession rebuilds, folds the open block, and seals everything. A fault
// armed by an earlier session may still end it; each fires once, so a later
// attempt succeeds.
func (e *env) finalSession(t *testing.T) {
	for range 5 {
		unfired := len(e.db.Unfired())
		err := func() error {
			m := e.maintainer(maintainer.Config{})
			open, err := m.Rebuild(t.Context(), maintainer.RebuildConfig{BlockMaxAge: time.Hour})
			if err != nil {
				return errors.Join(err, m.Close())
			}
			w := e.writer(m, ingest.HotConfig{Resume: open})
			err = w.ForceRotate(t.Context())
			return errors.Join(err, w.Close(), m.Close())
		}()
		if err == nil {
			return
		}
		require.Less(t, len(e.db.Unfired()), unfired, "error with no fault fired: %v", err)
		e.restart()
	}
	t.Fatal("the final session never ran clean")
}

func (e *env) committedNext() uint64 {
	e.t.Helper()
	snap := e.snapshot()
	next, err := catalog.DecodeSeq(catalog.MainSeqKey, snap.Meta[catalog.MainSeqKey], snap.Meta[catalog.MainSeqKey] != nil)
	require.NoError(e.t, err)
	return next
}

// dropSink forwards the first forward closed blocks to the maintainer, then
// drops the rest while releasing them from the writer's unfolded cap, so
// their hot batches outlive the session.
type dropSink struct {
	*maintainer.Maintainer
	forward int64
	seen    atomic.Int64
}

func (d *dropSink) BlockClosed(b ingest.ClosedBlock) {
	if d.seen.Add(1) <= d.forward {
		d.Maintainer.BlockClosed(b)
		return
	}
	b.Folded()
}

func requireSameEvent(t *testing.T, want, got segment.Event) {
	t.Helper()
	require.Equal(t, want.Seq, got.Seq)
	require.Equal(t, want.DID, got.DID)
	require.Equal(t, want.Collection, got.Collection)
	require.Equal(t, want.Rkey, got.Rkey)
	require.Equal(t, want.WitnessedAt, got.WitnessedAt)
	require.True(t, slices.Equal(want.Payload, got.Payload), "seq %d payload", want.Seq)
}
