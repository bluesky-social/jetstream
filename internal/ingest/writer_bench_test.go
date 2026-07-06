package ingest

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/prometheus/client_golang/prometheus"
)

// benchEvent approximates a median bsky post commit event: ~200 byte
// dag-json payload plus realistic key column widths.
func benchEvent(payload []byte) segment.Event {
	return segment.Event{
		Kind:       segment.KindCreate,
		DID:        "did:plc:ewvi7nxzyoun6zhxrhs64oiz",
		Collection: "app.bsky.feed.post",
		Rkey:       "3ke6kg3wk3e22",
		Rev:        "3ke6kg3wk3e22",
		Payload:    payload,
	}
}

func newBenchWriter(b *testing.B, root string, asyncWorkers int) *Writer {
	b.Helper()
	segDir := filepath.Join(root, "segments")
	st, err := store.Open(filepath.Join(root, "meta"), nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = st.Close() })
	w, err := Open(Config{
		SegmentsDir:       segDir,
		Store:             st,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:           NewMetrics(prometheus.NewRegistry()),
		MaxSegmentBytes:   1 << 40, // no rotation during the bench
		AsyncFlushWorkers: asyncWorkers,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = w.Close() })
	return w
}

// BenchmarkWriterBackfillShape drives the writer the way backfill does:
// AppendBatch of 1024-event batches from concurrent producers. Reports
// events/sec. Vary async workers to quantify what the async flush
// pipeline buys over inline (sync) flush.
//
// BENCH_DIR overrides the writer root (e.g. a real NVMe path); default
// b.TempDir() (often tmpfs, which removes fsync cost and isolates the
// compression-serialization effect).
func BenchmarkWriterBackfillShape(b *testing.B) {
	const batchSize = 1024
	payload := make([]byte, 200)
	for i := range payload {
		payload[i] = byte(i * 7) // mildly compressible, not trivially so
	}

	for _, mode := range []struct {
		name  string
		async int
		procs int
	}{
		{"sync/p1", 0, 1},
		{"sync/p8", 0, 8},
		{"async1/p8", 1, 8},
		{"async4/p8", 4, 8},
		{"async8/p8", 8, 8},
	} {
		b.Run(mode.name, func(b *testing.B) {
			root := os.Getenv("BENCH_DIR")
			if root == "" {
				root = b.TempDir()
			} else {
				var err error
				root, err = os.MkdirTemp(root, "jsbench")
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = os.RemoveAll(root) })
			}
			w := newBenchWriter(b, root, mode.async)

			b.SetParallelism(mode.procs)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				batch := make([]segment.Event, batchSize)
				for pb.Next() {
					for i := range batch {
						batch[i] = benchEvent(payload)
					}
					if err := w.AppendBatch(context.Background(), batch); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.StopTimer()
			evs := float64(b.N) * batchSize
			b.ReportMetric(evs/b.Elapsed().Seconds(), "events/sec")
		})
	}
}

// BenchmarkWriterLiveShape drives the writer the way the steady-state
// live consumer does: single goroutine, per-event Append, with an
// installed ordered sink (nil-cost check when absent). Reports
// events/sec and captures the block-boundary flush stall that a sync
// appender pays inline.
func BenchmarkWriterLiveShape(b *testing.B) {
	payload := make([]byte, 200)
	for i := range payload {
		payload[i] = byte(i * 7)
	}

	for _, mode := range []struct {
		name  string
		async int
		sink  bool
	}{
		{"sync/nosink", 0, false},
		{"sync/sink", 0, true},
		// No async+sink mode: SetOrderedEventSink rejects async writers (#249).
		{"async4/nosink", 4, false},
	} {
		b.Run(mode.name, func(b *testing.B) {
			root := os.Getenv("BENCH_DIR")
			if root == "" {
				root = b.TempDir()
			} else {
				var err error
				root, err = os.MkdirTemp(root, "jsbench")
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = os.RemoveAll(root) })
			}
			w := newBenchWriter(b, root, mode.async)
			var sunk uint64
			if mode.sink {
				w.SetOrderedEventSink(func(ev *segment.Event) { sunk += ev.Seq & 1 })
			}

			b.ResetTimer()
			for b.Loop() {
				ev := benchEvent(payload)
				if err := w.Append(context.Background(), &ev); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "events/sec")
		})
	}
}
