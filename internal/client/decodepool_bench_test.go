package client

import (
	"bytes"
	"os"
	"strconv"
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/require"
)

// buildLikeBlockFrame builds a real sealed-segment block frame of n like events,
// returning the raw zstd frame bytes exactly as getBlock/getSegment would serve.
// This drives the decode benchmarks against production-shaped input (the
// decompress + columnar-decode + CBOR-decode path) rather than a synthetic blob.
func buildLikeBlockFrame(tb testing.TB, n int) []byte {
	tb.Helper()
	dir := tb.TempDir()
	path := dir + "/seg_bench.jss"
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: n})
	require.NoError(tb, err)
	rec := map[string]any{
		"$type":     "app.bsky.feed.like",
		"createdAt": "2024-11-20T15:27:04.328Z",
		"subject": map[string]any{
			"cid": "bafyreiangzeq6wkzgdywr6x6mfzue6plymwjcn5yc45kx27migmeahilme",
			"uri": "at://did:plc:onwgs7pxf2cgtm5z4bh5mml3/app.bsky.feed.post/3lbetkiuwwc2a",
		},
	}
	payload, err := cbor.Marshal(rec)
	require.NoError(tb, err)
	for i := range n {
		_, err := w.Append(segment.Event{
			Seq:        uint64(i + 1),
			IndexedAt:  int64(1_730_000_000_000_000 + i),
			Kind:       segment.KindCreate,
			DID:        "did:plc:atu3zhe7rhhq5ujaqe7yjpnn",
			Collection: "app.bsky.feed.like",
			Rkey:       "3lbfb65ejz" + strconv.Itoa(i%10) + "2c",
			Rev:        "3mfvujdqdjt2t",
			Payload:    payload,
		})
		require.NoError(tb, err)
	}
	_, err = w.Seal()
	require.NoError(tb, err)
	raw, err := os.ReadFile(path)
	require.NoError(tb, err)
	hdr, err := segment.ReadSealedHeader(bytes.NewReader(raw))
	require.NoError(tb, err)
	require.Equal(tb, uint32(1), hdr.BlockCount, "bench expects a single block")
	frame, err := segment.ReadBlockFrame(bytes.NewReader(raw), hdr, 0)
	require.NoError(tb, err)
	return frame
}

// BenchmarkDecodeFrameBlock measures the downloader's per-block decode (zstd
// decompress + columnar decode + per-event CBOR→map) on a full 4096-event like
// block — the production block size and dominant collection. This is the hot
// loop parallelized across the decode pool, so its ns/op and B/op set the
// ceiling on single-core decode throughput.
func BenchmarkDecodeFrameBlock(b *testing.B) {
	frame := buildLikeBlockFrame(b, 4096)
	d := NewDownloader(nil, 1, nil)
	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	for b.Loop() {
		events, err := d.decodeFrame(frame, "seg_bench.jss", 0)
		if err != nil || len(events) != 4096 {
			b.Fatalf("decode: %v (n=%d)", err, len(events))
		}
	}
}
