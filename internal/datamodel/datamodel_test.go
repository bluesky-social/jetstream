package datamodel_test

import (
	"encoding/json"
	"testing"

	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream/internal/datamodel"
	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/bluesky-social/jetstream/segment"
)

func marshal(t testing.TB, v any) []byte {
	t.Helper()
	b, err := cbor.Marshal(v)
	require.NoError(t, err)
	return b
}

// blake3Link is {"l": <CIDv1 dag-cbor, multihash 0x1e>}, the shape the
// test bed found archived (specs/notes/2026-09-28-disaggregated-testbed-findings.md
// finding 4).
func blake3Link() []byte {
	b := []byte{0xa1, 0x61, 'l', 0xd8, 0x2a, 0x58, 37, 0x00, 0x01, 0x71, 0x1e, 0x20}
	return append(b, make([]byte, 32)...)
}

func goodRecords(t testing.TB) [][]byte {
	sha := cbor.ComputeCID(cbor.CodecDagCBOR, []byte{0xa0})
	return [][]byte{
		{0xa0},
		marshal(t, map[string]any{"$type": "app.bsky.feed.like", "subject": map[string]any{"cid": "x", "uri": "at://y"}}),
		marshal(t, map[string]any{"n": int64(-7), "b": []byte{1, 2}, "l": sha, "a": []any{nil, true, "s", map[string]any{}}}),
	}
}

func badRecords(t testing.TB) map[string][]byte {
	return map[string][]byte{
		"float":         marshal(t, map[string]any{"$type": "net.anisota.x", "v": 1.5}),
		"nested float":  marshal(t, map[string]any{"a": []any{map[string]any{"v": 0.25}}}),
		"blake3 cid":    blake3Link(),
		"string record": marshal(t, `{"$type":"app.bsky.feed.post","text":"hi"}`),
		"array record":  marshal(t, []any{int64(1)}),
		"trailing":      {0xa0, 0xa0},
		"empty":         nil,
		"truncated":     {0xa1, 0x61},
	}
}

func TestCheckRecord(t *testing.T) {
	t.Parallel()
	for _, p := range goodRecords(t) {
		require.NoError(t, datamodel.CheckRecord(p), "%x", p)
	}
	for name, p := range badRecords(t) {
		err := datamodel.CheckRecord(p)
		require.ErrorIs(t, err, datamodel.ErrInvalid, name)
	}
}

// FuzzCheckRecordServable pins the gate's promise: a record it accepts is
// served by both websocket encoders as a JSON object, and v2 serves exactly
// the records the gate accepts.
func FuzzCheckRecordServable(f *testing.F) {
	for _, p := range goodRecords(f) {
		f.Add(p)
	}
	for _, p := range badRecords(f) {
		f.Add(p)
	}
	f.Fuzz(func(t *testing.T, payload []byte) {
		ev := &segment.Event{
			Seq: 1, WitnessedAt: 1, Kind: segment.KindCreate,
			DID: "did:plc:a", Collection: "app.bsky.feed.like", Rkey: "rk", Rev: "rev",
			Payload: payload,
		}
		gateErr := datamodel.CheckRecord(payload)
		v2, v2Err := subscribe.EncodeV2(ev)
		require.Equal(t, gateErr == nil, v2Err == nil, "gate=%v v2=%v payload=%x", gateErr, v2Err, payload)
		if gateErr != nil {
			return
		}
		v1, err := subscribe.Encode(ev)
		require.NoError(t, err, "v1 Encode rejected a gate-accepted record %x", payload)
		var f1 struct {
			Commit struct {
				Record json.RawMessage `json:"record"`
			} `json:"commit"`
		}
		require.NoError(t, json.Unmarshal(v1, &f1))
		requireObject(t, f1.Commit.Record)

		var f2 struct {
			Payload struct {
				Record json.RawMessage `json:"record"`
			} `json:"payload"`
		}
		require.NoError(t, json.Unmarshal(v2, &f2))
		requireObject(t, f2.Payload.Record)
	})
}

func requireObject(t *testing.T, raw json.RawMessage) {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(raw, &m), "record %s", raw)
	require.NotNil(t, m, "record %s", raw)
}
