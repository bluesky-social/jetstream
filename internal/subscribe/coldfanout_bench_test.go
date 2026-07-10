package subscribe

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/cbor"
)

// BenchmarkColdFanout is the local regression signal for #295: c concurrent
// subscribers replay the same sealed history through the ColdReader and
// consume every event's wire form. Before the fix, zstd fanout was ~11x
// slower than uncompressed at c=10 (per-subscriber entries re-compressed
// every event through one serialized encoder); after it, both modes share
// one encode/compress per event and should sit within the same order of
// magnitude. Reported frames/s is aggregate across all subscribers.
func BenchmarkColdFanout(b *testing.B) {
	for _, mode := range []string{"uncompressed", "zstd"} {
		for _, c := range []int{1, 10} {
			b.Run(fmt.Sprintf("%s/c=%d", mode, c), func(b *testing.B) {
				benchColdFanout(b, mode == "zstd", c)
			})
		}
	}
}

func benchColdFanout(b *testing.B, compress bool, subscribers int) {
	const events = 4096
	rd := newBenchColdReader(b, events)

	b.ReportAllocs()
	b.ResetTimer()
	var frames atomic.Int64
	for b.Loop() {
		// Invalidate so each iteration replays genuinely cold: the block is
		// re-decoded (single-flight) and fresh shared entries are built, so
		// the per-event encode+compress runs exactly once per iteration and
		// fans out to all subscribers — the #295 replay-storm shape. Without
		// this, iterations after the first serve fully-memoized bytes and
		// the compression cost vanishes from the measurement.
		rd.InvalidateSegment(0)
		var wg sync.WaitGroup
		for range subscribers {
			wg.Go(func() {
				cursor := uint64(0)
				for cursor < events {
					batch, next, err := rd.Read(context.Background(), cursor, 512)
					if err != nil {
						b.Error(err)
						return
					}
					if next <= cursor {
						b.Error("cold read did not advance")
						return
					}
					for _, e := range batch {
						var body []byte
						var eerr error
						if compress {
							body, eerr = e.Compressed()
						} else {
							body, eerr = e.Encoded()
						}
						if eerr != nil {
							b.Error(eerr)
							return
						}
						_ = body
						frames.Add(1)
					}
					cursor = next
				}
			})
		}
		wg.Wait()
	}
	b.StopTimer()
	b.ReportMetric(float64(frames.Load())/b.Elapsed().Seconds(), "frames/s")
}

// newBenchColdReader builds a ColdReader over one sealed segment holding
// eventCount realistic-shaped commit events, with the readable-log floor
// above the segment so every read is cold.
func newBenchColdReader(b *testing.B, eventCount int) *ColdReader {
	b.Helper()
	dir := b.TempDir()
	segDir := filepath.Join(dir, "segments")
	if err := os.MkdirAll(segDir, 0o755); err != nil {
		b.Fatal(err)
	}

	w, err := segment.New(segment.Config{
		Path:              filepath.Join(segDir, "seg_0000000000.jss"),
		MaxEventsPerBlock: 4096,
	})
	if err != nil {
		b.Fatal(err)
	}
	for i := range eventCount {
		// Payload is real DAG-CBOR shaped like a small post record, so the
		// encoder's record decode and the compressor see realistic structure.
		payload, err := cbor.Marshal(map[string]any{
			"$type":     "app.bsky.feed.post",
			"text":      fmt.Sprintf("benchmark post number %d with some repeated filler text", i),
			"createdAt": fmt.Sprintf("2026-07-09T12:00:00.%06dZ", i),
			"langs":     []any{"en"},
		})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := w.Append(segment.Event{
			Seq: uint64(i), WitnessedAt: int64(1_000 + i), Kind: segment.KindCreate,
			DID:        fmt.Sprintf("did:plc:bench%04d", i%64),
			Collection: "app.bsky.feed.post",
			Rkey:       fmt.Sprintf("rkey%08d", i), Rev: "aaa",
			Payload: payload,
		}); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := w.Seal(); err != nil {
		b.Fatal(err)
	}
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}

	m := mustOpenColdReaderManifest(b, segDir)
	st, iw := openColdReaderWriterAtTip(b, dir, uint64(eventCount))
	b.Cleanup(func() { _ = iw.Close(); _ = st.Close() })

	var writerPtr atomic.Pointer[ingest.Writer]
	writerPtr.Store(iw)
	return NewColdReader(ColdReaderConfig{
		Manifest: m, WriterRef: &writerPtr, BlockCacheBytes: 64 << 20,
	})
}
