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

	// StreamErrorFrames is a labeled vec: no series exists until the
	// first observation, so touch one label to make Gather see it.
	m.incStreamErrorFrames("FutureCursor")

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
		"jetstream_livestream_stream_error_frames_total",
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

// TestNoteStreamError_ClassifiesStreamErrors pins the operator
// contract for the iterator's error slot: each error class lands on
// its own counter, because each carries a different remediation —
// relay data loss (GapError), a relay speaking a newer protocol
// (UnknownFrameError → upgrade jetstream), a server error frame
// (StreamError, labeled by code; a FutureCursor loop needs an
// operator), and garbage frames (everything else → decode errors).
func TestNoteStreamError_ClassifiesStreamErrors(t *testing.T) {
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
	c.noteStreamError(t.Context(), &streaming.UnknownFrameError{T: "#futureThing", Op: 1, Seq: 30})
	c.noteStreamError(t.Context(), fmt.Errorf("wrapped: %w", &streaming.UnknownFrameError{T: "#other", Op: 2}))
	c.noteStreamError(t.Context(), &streaming.StreamError{Code: "FutureCursor", Message: "cursor in the future"})
	c.noteStreamError(t.Context(), &streaming.StreamError{Code: "FutureCursor"})
	c.noteStreamError(t.Context(), &streaming.StreamError{Code: "ConsumerTooSlow"})

	require.InDelta(t, 2.0, testutil.ToFloat64(metrics.SequenceGaps), 0,
		"each GapError yield must count one gap")
	require.InDelta(t, 6.0, testutil.ToFloat64(metrics.SequenceGapMissedSeqs), 0,
		"gap widths must accumulate: (15-10) + (21-20)")
	require.InDelta(t, 2.0, testutil.ToFloat64(metrics.DecodeErrors), 0,
		"only unclassified stream errors land on decode_errors_total")
	require.InDelta(t, 2.0, testutil.ToFloat64(metrics.UnknownEvents), 0,
		"unknown frames (direct or wrapped) land on unknown_events_total")
	require.InDelta(t, 2.0, testutil.ToFloat64(metrics.StreamErrorFrames.WithLabelValues("FutureCursor")), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(metrics.StreamErrorFrames.WithLabelValues("ConsumerTooSlow")), 0)
}
