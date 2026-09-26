package protocol_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/objstore"
	"github.com/bluesky-social/jetstream/internal/objstore/memblob"
	"github.com/bluesky-social/jetstream/internal/objstore/protocol"
)

var errCrash = errors.New("simulated crash")

// crashAt fails the run at one crashpoint.
type crashAt crashpoint.Point

func (c crashAt) SimulateCrash(_ context.Context, p crashpoint.Point) error {
	if p == crashpoint.Point(c) {
		return errCrash
	}
	return nil
}

func (h *harness) collector(t *testing.T, cfg protocol.GCConfig) *protocol.Collector {
	t.Helper()
	cfg.Blob, cfg.ArchiveID, cfg.Delay, cfg.OrphanAge = h.blob, archiveID, gcDelay, orphanAge
	c, err := protocol.NewCollector(cfg)
	require.NoError(t, err)
	return c
}

// newSession replaces the harness's session with a new one for the same
// epoch, as a restarted leader session would be.
func (h *harness) newSession(t *testing.T) {
	t.Helper()
	h.session = catalog.NewSession(catalog.SessionConfig{DB: h.db, Epoch: h.lease.Epoch(), Metrics: h.catalogM})
}

// takeover hands the lease to a new holder and returns the old session.
func (h *harness) takeover(t *testing.T) *catalog.Session {
	t.Helper()
	old := h.session
	require.NoError(t, h.lease.Release(t.Context()))
	h.lease = h.db.NewLease()
	require.NoError(t, h.lease.Acquire(t.Context(), time.Hour))
	h.newSession(t)
	return old
}

func (h *harness) states(t *testing.T) map[catalog.ObjectState]int64 {
	t.Helper()
	r, err := h.db.BeginRead(t.Context())
	require.NoError(t, err)
	defer func() { _ = r.Close(context.Background()) }()
	got, err := r.ObjectStates(t.Context())
	require.NoError(t, err)
	return got
}

// gcWorld is a harness holding one referenced object, one available but
// unreferenced object, and one orphaned upload.
type gcWorld struct {
	*harness
	referenced, unreferenced, orphan uint64
}

func newGCWorld(t *testing.T, o options) *gcWorld {
	t.Helper()
	h := newHarness(t, o)
	w := &gcWorld{harness: h}
	refs, err := h.up.Upload(t.Context(), h.session, [][]byte{payload(0)})
	require.NoError(t, err)
	c, err := h.session.CommitHotBatch(t.Context(), catalog.HotBatch{FirstSeq: 1, LastSeq: 1, Object: refs[0]})
	require.NoError(t, err)
	w.referenced = c.ObjectID
	w.unreferenced = h.put(t, payload(1))
	orphans, err := h.up.Upload(t.Context(), h.session, [][]byte{payload(2)})
	require.NoError(t, err)
	w.orphan = orphans[0].ID
	return w
}

func (w *gcWorld) key(t *testing.T, id uint64) string {
	t.Helper()
	return objstore.Key(archiveID, w.object(t, id).Key)
}

func TestGC(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		w := newGCWorld(t, options{})
		reg := prometheus.NewRegistry()
		m := protocol.NewGCMetrics(reg)
		c := w.collector(t, protocol.GCConfig{Metrics: m})
		keep := w.key(t, w.referenced)
		require.Len(t, w.keys(), 3)

		res, err := c.Run(t.Context(), w.session, w.db)
		require.NoError(t, err)
		require.Equal(t, protocol.GCResult{Marked: 1}, res, "nothing has aged past the delays")
		require.EqualValues(t, 2, testutil.ToFloat64(m.Objects.WithLabelValues("available")))
		require.EqualValues(t, 1, testutil.ToFloat64(m.Objects.WithLabelValues("uploading")))

		time.Sleep(orphanAge + time.Second)
		res, err = c.Run(t.Context(), w.session, w.db)
		require.NoError(t, err)
		require.Equal(t, protocol.GCResult{Claimed: 1, Deleted: 1}, res, "the orphan ages out first")

		time.Sleep(gcDelay)
		res, err = c.Run(t.Context(), w.session, w.db)
		require.NoError(t, err)
		require.Equal(t, protocol.GCResult{Claimed: 1, Deleted: 1}, res)
		require.Equal(t, []string{keep}, w.keys())
		require.Equal(t, map[catalog.ObjectState]int64{catalog.ObjectAvailable: 1}, w.states(t))
		got, err := w.rd.Get(t.Context(), w.referenced)
		require.NoError(t, err)
		require.Equal(t, payload(0), got)

		require.EqualValues(t, 2, testutil.ToFloat64(m.Deleted))
		require.EqualValues(t, 3, testutil.ToFloat64(m.Runs.WithLabelValues("ok")))
		require.Equal(t, 1, testutil.CollectAndCount(m.RunDuration))
		for state, want := range map[string]float64{"available": 1, "uploading": 0, "deleting": 0} {
			require.Equal(t, want, testutil.ToFloat64(m.Objects.WithLabelValues(state)), state)
		}
		require.NoError(t, w.session.Err())
	})
}

// Pages and batches smaller than the work loop until it is done.
func TestGCPages(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, options{})
		for i := range 7 {
			h.put(t, payload(i))
		}
		c := h.collector(t, protocol.GCConfig{MarkPage: 2, ClaimLimit: 3, Concurrency: 2})
		res, err := c.Run(t.Context(), h.session, nil)
		require.NoError(t, err)
		require.Equal(t, 7, res.Marked)
		time.Sleep(gcDelay + time.Second)
		res, err = c.Run(t.Context(), h.session, nil)
		require.NoError(t, err)
		require.Equal(t, protocol.GCResult{Claimed: 7, Deleted: 7}, res)
		require.Empty(t, h.keys())
		require.Empty(t, h.states(t))
	})
}

// A run that stops at any step leaves the catalog for the next run, even in
// a new session, to finish.
func TestGCResume(t *testing.T) {
	t.Parallel()
	for _, p := range []crashpoint.Point{
		crashpoint.AfterGCMarkBeforeClaim,
		crashpoint.AfterGCClaimBeforeDelete,
		crashpoint.AfterGCDeleteBeforeForget,
	} {
		t.Run(string(p), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				w := newGCWorld(t, options{})
				keep := w.key(t, w.referenced)
				_, err := w.collector(t, protocol.GCConfig{}).Run(t.Context(), w.session, nil)
				require.NoError(t, err, "mark the unreferenced object")
				time.Sleep(gcDelay + time.Second)

				_, err = w.collector(t, protocol.GCConfig{Crash: crashAt(p)}).Run(t.Context(), w.session, nil)
				require.ErrorIs(t, err, errCrash)
				require.NoError(t, w.session.Err(), "a crashpoint is not a catalog failure")
				states := w.states(t)
				switch p {
				case crashpoint.AfterGCMarkBeforeClaim:
					require.Zero(t, states[catalog.ObjectDeleting])
					require.Len(t, w.keys(), 3)
				case crashpoint.AfterGCClaimBeforeDelete:
					require.EqualValues(t, 2, states[catalog.ObjectDeleting])
					require.Len(t, w.keys(), 3)
				case crashpoint.AfterGCDeleteBeforeForget:
					require.EqualValues(t, 2, states[catalog.ObjectDeleting])
					require.Equal(t, []string{keep}, w.keys())
				}

				w.newSession(t)
				res, err := w.collector(t, protocol.GCConfig{}).Run(t.Context(), w.session, nil)
				require.NoError(t, err)
				require.Equal(t, 2, res.Deleted)
				require.Equal(t, []string{keep}, w.keys())
				require.Equal(t, map[catalog.ObjectState]int64{catalog.ObjectAvailable: 1}, w.states(t))
			})
		})
	}
}

// A failed delete fails the run but not the session. Its batch stays
// deleting until a later run deletes and forgets it; an unknown-result
// delete that landed deletes again as a missing key.
func TestGCDeleteFailure(t *testing.T) {
	t.Parallel()
	for _, kind := range []memblob.FaultKind{memblob.FaultError, memblob.FaultErrorAfter} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				fault := &switchable{fault: always(memblob.OpDelete, kind)}
				w := newGCWorld(t, options{blob: memblob.New(memblob.WithFaultInjector(fault))})
				keep := w.key(t, w.referenced)
				m := protocol.NewGCMetrics(prometheus.NewRegistry())
				c := w.collector(t, protocol.GCConfig{Metrics: m})
				time.Sleep(orphanAge + time.Second)

				fault.on.Store(true)
				_, err := c.Run(t.Context(), w.session, w.db)
				require.Error(t, err)
				_, corrupt := catalog.IsCorruption(err)
				require.False(t, corrupt)
				require.NoError(t, w.session.Err())
				require.EqualValues(t, 1, w.states(t)[catalog.ObjectDeleting])
				require.EqualValues(t, 1, testutil.ToFloat64(m.DeleteFailures))
				require.EqualValues(t, 1, testutil.ToFloat64(m.Runs.WithLabelValues("error")))
				require.EqualValues(t, 1, testutil.ToFloat64(m.Objects.WithLabelValues("deleting")))

				fault.on.Store(false)
				time.Sleep(gcDelay + time.Second)
				res, err := c.Run(t.Context(), w.session, w.db)
				require.NoError(t, err)
				require.Equal(t, protocol.GCResult{Claimed: 2, Deleted: 2}, res, "the leftover batch, then the fresh claim")
				require.Equal(t, []string{keep}, w.keys())
				require.Zero(t, testutil.ToFloat64(m.Objects.WithLabelValues("deleting")))
			})
		})
	}
}

// A GC transaction that fails ends the session, and the run reports it.
func TestGCEndedSession(t *testing.T) {
	t.Parallel()
	h := newHarness(t, options{})
	h.put(t, payload(0))
	stale := h.takeover(t)
	_, err := h.collector(t, protocol.GCConfig{}).Run(t.Context(), stale, nil)
	require.ErrorIs(t, err, catalog.ErrFenced)
	require.Error(t, stale.Err())
}

func TestNewCollectorValidates(t *testing.T) {
	t.Parallel()
	for _, cfg := range []protocol.GCConfig{
		{Delay: time.Hour, OrphanAge: time.Hour},
		{Blob: memblob.New(), OrphanAge: time.Hour},
		{Blob: memblob.New(), Delay: time.Hour},
	} {
		_, err := protocol.NewCollector(cfg)
		require.Error(t, err)
	}
}
