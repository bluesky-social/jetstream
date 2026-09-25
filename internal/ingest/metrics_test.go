package ingest

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// TestNewMetrics_RegistersCounters confirms NewMetrics registers the
// expected counter and gauge series against the supplied registry
// and that every helper round-trips through testutil.ToFloat64.
func TestNewMetrics_RegistersCounters(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	require.NotNil(t, m)

	m.incEventsAppended()
	m.incBlocksFlushed()
	m.incSegmentsRotated()
	m.incAppendErrors()
	m.setActiveSegBytes(123)
	m.setNextSeq(456)
	m.setSeqLease(500, 456, 2, 40)
	m.setSeqReservationHeadroom(510, 456)
	m.incSeqGapRegistered(12)

	require.InDelta(t, 1.0, testutil.ToFloat64(m.EventsAppended), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.BlocksFlushed), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.SegmentsRotated), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.AppendErrors), 0)
	require.InDelta(t, 123.0, testutil.ToFloat64(m.ActiveSegBytes), 0)
	require.InDelta(t, 456.0, testutil.ToFloat64(m.NextSeq), 0)
	require.InDelta(t, 500.0, testutil.ToFloat64(m.SeqReservedEnd), 0)
	require.InDelta(t, 54.0, testutil.ToFloat64(m.SeqReservationHeadroom), 0)
	require.InDelta(t, 2.0, testutil.ToFloat64(m.SeqGapCount), 0)
	require.InDelta(t, 40.0, testutil.ToFloat64(m.SeqGapWidth), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(m.SeqGapsRegistered), 0)
	require.InDelta(t, 12.0, testutil.ToFloat64(m.SeqGapValuesRegistered), 0)
	requireNoDebugMetricFields(t, m)
	requireNoDebugMetrics(t, reg)
}

// TestNewMetrics_NilSafe pins that every inc/set helper tolerates a
// nil receiver. The tests in writer_test.go pass nil to skip
// registration.
func TestNewMetrics_NilSafe(t *testing.T) {
	t.Parallel()
	var m *Metrics
	require.NotPanics(t, func() {
		m.incEventsAppended()
		m.incBlocksFlushed()
		m.incSegmentsRotated()
		m.incAppendErrors()
		m.setActiveSegBytes(1)
		m.setNextSeq(1)
		m.setSeqLease(5, 1, 1, 4)
		m.setSeqReservationHeadroom(5, 1)
		m.incSeqGapRegistered(4)
	})
}

func requireNoDebugMetrics(t *testing.T, reg *prometheus.Registry) {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		require.NotContains(t, mf.GetName(), "_debug_")
	}
}

func requireNoDebugMetricFields(t *testing.T, m *Metrics) {
	t.Helper()
	typ := reflect.TypeOf(*m)
	for i := range typ.NumField() {
		require.NotContains(t, typ.Field(i).Name, "Debug")
	}
}

// TestPendingGaugeIsPartialBlockSawtooth pins what the dashboard expression
// next_seq - readable_log_durable_seq measures for the steady-state writer: the
// number of events in the unflushed partial block. It climbs by one per append,
// reads MaxEventsPerBlock while the full block is being fsynced and committed,
// and drops to zero once that commit lands. Nothing else moves the durable
// watermark in steady state, so a low reading is a scrape that landed just
// after a block cut, not a second flush path (design §22, S1.14).
func TestPendingGaugeIsPartialBlockSawtooth(t *testing.T) {
	t.Parallel()
	const perBlock = 8
	m := NewMetrics(prometheus.NewRegistry())
	pending := func() uint64 {
		return uint64(testutil.ToFloat64(m.NextSeq) - testutil.ToFloat64(m.ReadLogDurableSeq))
	}

	var atCommit []uint64
	w, err := Open(Config{
		SegmentsDir:              filepath.Join(t.TempDir(), "segments"),
		Store:                    newTestStore(t),
		SeqKey:                   seqNextKey,
		ReserveClientVisibleSeqs: true,
		MaxEventsPerBlock:        perBlock,
		MaxSegmentBytes:          1 << 30,
		Logger:                   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:                  m,
		OnDurableBatch: func(_ context.Context, _ *pebble.Batch, _ uint64, _ bool, _ any) (func(), func(error), error) {
			atCommit = append(atCommit, pending())
			return nil, nil, nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	require.Zero(t, pending())

	for i := range 3*perBlock + 3 {
		require.NoError(t, w.Append(t.Context(), &segment.Event{Kind: segment.KindCreate, DID: "did:plc:pending"}))
		want := uint64((i + 1) % perBlock)
		require.Equal(t, want, pending(), "append %d", i)
		require.Equal(t, w.NextSeq()-w.ReadLog().DurableSeq(), pending(), "gauges must track the readable log")
	}
	require.Equal(t, []uint64{perBlock, perBlock, perBlock}, atCommit,
		"the metadata commit sees a full block pending; durable advances only after it")
}
