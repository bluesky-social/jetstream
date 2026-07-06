package subscribe

import (
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

var testTailWriters sync.Map

func newReadLogTail(t *testing.T, retentionBytes int64, cold coldReader) (*Tail, *ingest.Writer) {
	t.Helper()
	st, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	w, err := ingest.Open(ingest.Config{
		SegmentsDir:           filepath.Join(t.TempDir(), "segments"),
		Store:                 st,
		Logger:                slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:               ingest.NewMetrics(prometheus.NewRegistry()),
		MaxEventsPerBlock:     1024,
		ReadLogRetentionBytes: retentionBytes,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	if cold == nil {
		cold = noCold
	}
	tl := newTail(tailConfig{
		cold:    cold,
		nextSeq: w.NextSeq,
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	tl.SetReadLogSource(func() *ingest.ReadableLog { return w.ReadLog() })
	testTailWriters.Store(tl, w)
	return tl, w
}

func writerForTail(t *testing.T, tl *Tail) *ingest.Writer {
	t.Helper()
	if got, ok := testTailWriters.Load(tl); ok {
		return got.(*ingest.Writer)
	}
	st, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:           filepath.Join(t.TempDir(), "segments"),
		Store:                 st,
		Logger:                slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:               ingest.NewMetrics(prometheus.NewRegistry()),
		MaxEventsPerBlock:     1024,
		ReadLogRetentionBytes: 1 << 20,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	tl.SetReadLogSource(func() *ingest.ReadableLog { return w.ReadLog() })
	testTailWriters.Store(tl, w)
	return w
}

func appendToWriter(t *testing.T, w *ingest.Writer, ev *segment.Event) {
	t.Helper()
	require.NoError(t, w.Append(t.Context(), ev))
}
