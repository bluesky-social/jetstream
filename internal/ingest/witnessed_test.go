package ingest

import (
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"path/filepath"
	"testing"
	"testing/synctest"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// witnessed_at stays monotonic with seq when stamps run backwards, including
// across a reopen onto flushed active blocks and onto a sealed tail
// (docs/README.md §3.1).
func TestWriter_WitnessedMonotonic(t *testing.T) {
	t.Parallel()
	m := NewMetrics(prometheus.NewRegistry())
	cfg := Config{
		SegmentsDir:       filepath.Join(t.TempDir(), "segments"),
		Store:             newTestStore(t),
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:           m,
		MaxEventsPerBlock: 2,
		MaxSegmentBytes:   1 << 30,
	}
	appendAt := func(w *Writer, us int64) int64 {
		ev := segment.Event{WitnessedAt: us, Kind: segment.KindCreate, DID: "did:plc:a"}
		require.NoError(t, w.Append(t.Context(), &ev))
		return ev.WitnessedAt
	}

	w, err := Open(cfg)
	require.NoError(t, err)
	require.Equal(t, int64(1000), appendAt(w, 1000))
	require.Equal(t, int64(1000), appendAt(w, 900), "a late stamp is raised to the floor")
	require.NoError(t, w.Close())

	w, err = Open(cfg)
	require.NoError(t, err)
	require.Equal(t, int64(1000), appendAt(w, 500), "the floor survives a reopen onto flushed blocks")
	require.Equal(t, int64(2000), appendAt(w, 2000))
	require.NoError(t, w.SealActiveAndClose())

	w, err = Open(cfg)
	require.NoError(t, err)
	require.Equal(t, int64(2000), appendAt(w, 10), "the floor survives a reopen onto a sealed tail")
	require.NoError(t, w.Close())
	require.Equal(t, 3.0, testutil.ToFloat64(m.WitnessedClamped))

	var got []int64
	files, err := SegmentFiles(cfg.SegmentsDir)
	require.NoError(t, err)
	note := func(evs []segment.Event) {
		for _, ev := range evs {
			got = append(got, ev.WitnessedAt)
		}
	}
	for _, f := range files {
		r, err := segment.Open(segment.ReaderConfig{Path: f.Path})
		if errors.Is(err, segment.ErrActiveSegment) {
			require.NoError(t, segment.WalkActive(f.Path, func(evs []segment.Event) error {
				note(evs)
				return nil
			}))
			continue
		}
		require.NoError(t, err)
		for i := range int(r.Header().BlockCount) {
			evs, err := r.DecodeBlock(i)
			require.NoError(t, err)
			note(evs)
		}
		require.NoError(t, r.Close())
	}
	require.Equal(t, []int64{1000, 1000, 1000, 2000, 2000}, got, "stored stamps are the clamped ones")
}

// A hot session's floor comes from the previous session's hot batches, read
// from the database rather than the follower.
func TestHot_WitnessedMonotonicAcrossSessions(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		env := newHotEnv(t)
		rng := rand.New(rand.NewPCG(9, 9))
		m := NewMetrics(prometheus.NewRegistry())
		appendAt := func(w *Writer, us int64) int64 {
			ev := testEvent(rng, 0)
			ev.WitnessedAt = us
			require.NoError(t, w.Append(t.Context(), &ev))
			return ev.WitnessedAt
		}
		w := env.open(Config{MaxEventsPerBlock: 16, Metrics: m})
		require.Equal(t, int64(1000), appendAt(w, 1000))
		require.Equal(t, int64(1000), appendAt(w, 900))
		require.NoError(t, w.Close())

		env.newSession()
		w = env.open(Config{MaxEventsPerBlock: 16, Metrics: m, Hot: &HotConfig{Sink: &recSink{}, Resume: env.resume()}})
		require.Equal(t, int64(1000), appendAt(w, 500), "a new session starts at the durable floor")
		require.NoError(t, w.Close())
		require.Equal(t, 2.0, testutil.ToFloat64(m.WitnessedClamped))

		var got []int64
		for _, row := range env.rows() {
			for _, ev := range env.frameEvents(row) {
				got = append(got, ev.WitnessedAt)
			}
		}
		require.Equal(t, []int64{1000, 1000, 1000}, got)
	})
}
