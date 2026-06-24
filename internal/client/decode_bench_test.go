package client

import (
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/require"
)

// benchLikePayload is a representative app.bsky.feed.like record — the dominant
// collection in the archive (~68% of events) and therefore the record shape that
// governs backfill decode throughput. A like is a small object with a nested
// subject holding two CID-link strings.
func benchLikePayload(tb testing.TB) []byte {
	tb.Helper()
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
	return payload
}

// benchPostPayload is a heavier app.bsky.feed.post record: more string fields, a
// langs array, and a nested facet-like structure, to exercise the converter on a
// fatter object than a like.
func benchPostPayload(tb testing.TB) []byte {
	tb.Helper()
	rec := map[string]any{
		"$type":     "app.bsky.feed.post",
		"text":      "this is a representative post with a moderate amount of text content in it",
		"createdAt": "2024-11-20T15:27:04.328Z",
		"langs":     []any{"en", "es"},
		"reply": map[string]any{
			"root":   map[string]any{"cid": "bafyreiangzeq6wkzgdywr6x6mfzue6plymwjcn5yc45kx27migmeahilme", "uri": "at://did:plc:abc/app.bsky.feed.post/1"},
			"parent": map[string]any{"cid": "bafyreid65l3je655doo5fvwxdef4ar2drd6tku6fo7pwwnv7wxjk5tz4fm", "uri": "at://did:plc:def/app.bsky.feed.post/2"},
		},
	}
	payload, err := cbor.Marshal(rec)
	require.NoError(tb, err)
	return payload
}

func BenchmarkDecodeRecordMapLike(b *testing.B) {
	payload := benchLikePayload(b)
	b.ReportAllocs()
	for b.Loop() {
		m, err := decodeRecordMap(payload)
		if err != nil || m == nil {
			b.Fatalf("decode: %v", err)
		}
	}
}

func BenchmarkDecodeRecordMapPost(b *testing.B) {
	payload := benchPostPayload(b)
	b.ReportAllocs()
	for b.Loop() {
		m, err := decodeRecordMap(payload)
		if err != nil || m == nil {
			b.Fatalf("decode: %v", err)
		}
	}
}

// BenchmarkDecodeSegmentEventLike measures the full per-event decode path used by
// the downloader (decodeCommit: record map + CID compute + RecordCBOR clone), on
// the dominant like shape.
func BenchmarkDecodeSegmentEventLike(b *testing.B) {
	ev := &segment.Event{
		Seq:        1,
		IndexedAt:  1_730_000_000_000_000,
		Kind:       segment.KindCreate,
		DID:        "did:plc:atu3zhe7rhhq5ujaqe7yjpnn",
		Collection: "app.bsky.feed.like",
		Rkey:       "3lbfb65ejz62c",
		Rev:        "3mfvujdqdjt2t",
		Payload:    benchLikePayload(b),
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := decodeSegmentEvent(ev); err != nil {
			b.Fatalf("decode: %v", err)
		}
	}
}
