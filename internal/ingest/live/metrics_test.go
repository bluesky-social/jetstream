package live

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/jcalabro/atmos/streaming"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// TestNewMetrics_RegistersAllSeries pins that the constructor
// registers every series on the provided registry exactly once.
// We catch double-registration via reg.Register, which returns
// AlreadyRegisteredError on collision.
func TestNewMetrics_RegistersAllSeries(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	require.NotNil(t, m)

	gathered, err := reg.Gather()
	require.NoError(t, err)
	names := make(map[string]struct{}, len(gathered))
	for _, mf := range gathered {
		names[mf.GetName()] = struct{}{}
	}
	for _, want := range []string{
		"jetstream_livestream_events_received_total",
		"jetstream_livestream_reconnects_total",
		"jetstream_livestream_decode_errors_total",
		"jetstream_livestream_sequence_gaps_total",
		"jetstream_livestream_sequence_gap_missed_seqs_total",
		"jetstream_livestream_unknown_events_total",
		"jetstream_livestream_stale_resyncs_dropped_total",
		"jetstream_livestream_replayed_account_events_dropped_total",
		"jetstream_livestream_upstream_cursor",
	} {
		_, ok := names[want]
		require.True(t, ok, "missing metric %s", want)
	}
	_, ok := names["jetstream_livestream_events_converted_total"]
	require.False(t, ok, "events converted duplicates jetstream_ingest_events_appended_total")
	// Folded into the shared jetstream_ingest_dropped_events_total
	// family (#197); pin the old names dead so a revert can't silently
	// double-count drops across two shapes.
	for _, gone := range []string{
		"jetstream_livestream_dropped_ops_missing_block_total",
		"jetstream_livestream_dropped_events_total",
	} {
		_, ok := names[gone]
		require.False(t, ok, "metric %s was folded into jetstream_ingest_dropped_events_total", gone)
	}

	// Re-registering the same collectors must collide.
	require.Panics(t, func() { _ = NewMetrics(reg) })
}

// TestNoteStreamError_ClassifiesGapsApartFromDecodeErrors pins the
// operator contract: relay data loss (GapError) and garbage frames
// (everything else) land on different counters, and the gap-width
// counter accumulates the number of seqs the relay skipped.
func TestNoteStreamError_ClassifiesGapsApartFromDecodeErrors(t *testing.T) {
	t.Parallel()
	metrics := NewMetrics(prometheus.NewRegistry())
	c := &Consumer{
		cfg:    Config{Metrics: metrics},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	c.noteStreamError(t.Context(), &streaming.GapError{Expected: 10, Got: 15})
	c.noteStreamError(t.Context(), &streaming.GapError{Expected: 20, Got: 21})
	c.noteStreamError(t.Context(), fmt.Errorf("wrapped: %w", &streaming.DecodeError{Err: errors.New("bad cbor")}))
	c.noteStreamError(t.Context(), errors.New("some other stream error"))

	require.InDelta(t, 2.0, testutil.ToFloat64(metrics.SequenceGaps), 0,
		"each GapError yield must count one gap")
	require.InDelta(t, 6.0, testutil.ToFloat64(metrics.SequenceGapMissedSeqs), 0,
		"gap widths must accumulate: (15-10) + (21-20)")
	require.InDelta(t, 2.0, testutil.ToFloat64(metrics.DecodeErrors), 0,
		"non-gap stream errors must stay on decode_errors_total")
}
