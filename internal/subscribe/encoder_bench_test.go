package subscribe

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
)

// mustDagCBOR encodes a JSON-shaped value to canonical DAG-CBOR.
func mustDagCBOR(b *testing.B, v any) []byte {
	b.Helper()
	jbytes, err := json.Marshal(v)
	if err != nil {
		b.Fatal(err)
	}
	val, err := cbor.FromJSON(jbytes)
	if err != nil {
		b.Fatal(err)
	}
	var buf bytes.Buffer
	if err := cbor.NewEncoder(&buf).WriteValue(val); err != nil {
		b.Fatal(err)
	}
	return buf.Bytes()
}

// BenchmarkEncodeV2 measures the hot-path frame encode. It runs once per
// event (amortized across all subscribers via Entry memoization), so its
// cost bounds ingest-side fan-out overhead, not per-subscriber cost.
func BenchmarkEncodeV2(b *testing.B) {
	// A realistic ~580 B post record (the live-corpus mean event size).
	record := map[string]any{
		"$type":     "app.bsky.feed.post",
		"createdAt": "2026-08-10T12:00:00.000Z",
		"langs":     []any{"en"},
		"text":      "benchmarking the proposal-0015 frame encoder with a post of roughly representative length for the live firehose corpus",
	}
	payload := mustDagCBOR(b, record)

	commit := &segment.Event{
		Seq:         123456,
		WitnessedAt: 1_700_000_000_123_456,
		Kind:        segment.KindCreate,
		DID:         "did:plc:abcdefghijklmnopqrstuvwx",
		Collection:  "app.bsky.feed.post",
		Rkey:        "3kx2abcdefghi",
		Rev:         "3kx2abcdefgha",
		Payload:     payload,
	}

	ident := &comatproto.SyncSubscribeRepos_Identity{
		DID: "did:plc:abcdefghijklmnopqrstuvwx", Seq: 99, Time: "2026-05-25T00:00:00Z",
	}
	identPayload, err := ident.MarshalCBOR()
	if err != nil {
		b.Fatal(err)
	}
	identity := &segment.Event{
		Seq:         123457,
		WitnessedAt: 1_700_000_000_123_457,
		Kind:        segment.KindIdentity,
		DID:         "did:plc:abcdefghijklmnopqrstuvwx",
		Payload:     identPayload,
	}

	b.Run("commit", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := EncodeV2(commit); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("identity", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := EncodeV2(identity); err != nil {
				b.Fatal(err)
			}
		}
	})
}
