package subscribe

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/api/jetstream"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/require"
)

// loadGolden reads testdata/golden_v1.jsonl into one map per line.
func loadGolden(t *testing.T) []map[string]any {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", "golden_v1.jsonl"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	var out []map[string]any
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var m map[string]any
		require.NoError(t, json.Unmarshal(line, &m))
		out = append(out, m)
	}
	require.NoError(t, sc.Err())
	return out
}

// recordCBOR encodes a v1 JSON record (already in the atproto JSON shape
// with $link / $bytes sentinels) back to canonical DAG-CBOR. We do this
// by routing through cbor.FromJSON to get a CBOR data-model value, then
// running cbor.NewEncoder().WriteValue.
func recordCBOR(t *testing.T, jsonRecord any) []byte {
	t.Helper()
	jbytes, err := json.Marshal(jsonRecord)
	require.NoError(t, err)
	val, err := cbor.FromJSON(jbytes)
	require.NoError(t, err)

	var buf bytes.Buffer
	enc := cbor.NewEncoder(&buf)
	require.NoError(t, enc.WriteValue(val))
	return buf.Bytes()
}

func commitKindFromOp(op string) segment.Kind {
	switch op {
	case "create":
		return segment.KindCreate
	case "update":
		return segment.KindUpdate
	case "delete":
		return segment.KindDelete
	default:
		return 0
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.MarshalIndent(v, "", "  ")
	require.NoError(t, err)
	return string(b)
}

func TestEncode_CommitGoldenRoundTrips(t *testing.T) {
	t.Parallel()
	golden := loadGolden(t)

	for i, want := range golden {
		kind, ok := want["kind"].(string)
		if !ok || kind != "commit" {
			continue
		}
		commitAny, ok := want["commit"].(map[string]any)
		require.True(t, ok, "want[\"commit\"] not a map")
		opAny, ok := commitAny["operation"].(string)
		require.True(t, ok, "operation not a string")
		collAny, ok := commitAny["collection"].(string)
		require.True(t, ok, "collection not a string")

		t.Run(fmt.Sprintf("%d_%s_%s", i, opAny, collAny), func(t *testing.T) {
			t.Parallel()

			commit, ok := want["commit"].(map[string]any)
			require.True(t, ok, "commit not a map")
			op, ok := commit["operation"].(string)
			require.True(t, ok, "operation not a string")
			timeUS, ok := want["time_us"].(float64)
			require.True(t, ok, "time_us not a float64")
			did, ok := want["did"].(string)
			require.True(t, ok, "did not a string")
			collection, ok := commit["collection"].(string)
			require.True(t, ok, "collection not a string")
			rkey, ok := commit["rkey"].(string)
			require.True(t, ok, "rkey not a string")
			rev, ok := commit["rev"].(string)
			require.True(t, ok, "rev not a string")

			segEvt := &segment.Event{
				WitnessedAt: int64(timeUS),
				Kind:        commitKindFromOp(op),
				DID:         did,
				Collection:  collection,
				Rkey:        rkey,
				Rev:         rev,
			}
			if op != "delete" {
				segEvt.Payload = recordCBOR(t, commit["record"])
			}

			gotJSON, err := Encode(segEvt)
			require.NoError(t, err)

			var got map[string]any
			require.NoError(t, json.Unmarshal(gotJSON, &got))

			require.True(t, reflect.DeepEqual(got, want),
				"mismatch\nwant: %s\n got: %s",
				mustJSON(t, want), mustJSON(t, got))
		})
	}
}

func TestEncode_IdentityGoldenRoundTrip(t *testing.T) {
	t.Parallel()
	for _, want := range loadGolden(t) {
		if want["kind"] != "identity" {
			continue
		}
		want := want
		t.Run("identity", func(t *testing.T) {
			t.Parallel()

			id, ok := want["identity"].(map[string]any)
			require.True(t, ok, "identity not a map")

			// Build the segment.Event the same way live.convertIdentity
			// does: marshal a typed Identity to CBOR.
			didStr, ok := id["did"].(string)
			require.True(t, ok, "did not a string")
			seqFloat, ok := id["seq"].(float64)
			require.True(t, ok, "seq not a float64")
			timeStr, ok := id["time"].(string)
			require.True(t, ok, "time not a string")

			ident := &comatproto.SyncSubscribeRepos_Identity{
				DID:  didStr,
				Seq:  int64(seqFloat),
				Time: timeStr,
			}
			payload, err := ident.MarshalCBOR()
			require.NoError(t, err)

			timeUS, ok := want["time_us"].(float64)
			require.True(t, ok, "time_us not a float64")
			did, ok := want["did"].(string)
			require.True(t, ok, "did not a string")

			segEvt := &segment.Event{
				WitnessedAt: int64(timeUS),
				Kind:        segment.KindIdentity,
				DID:         did,
				Payload:     payload,
			}

			gotJSON, err := Encode(segEvt)
			require.NoError(t, err)

			var got map[string]any
			require.NoError(t, json.Unmarshal(gotJSON, &got))
			require.True(t, reflect.DeepEqual(got, want),
				"identity mismatch\nwant: %s\n got: %s",
				mustJSON(t, want), mustJSON(t, got))
		})
	}
}

func TestEncode_AccountGoldenRoundTrip(t *testing.T) {
	t.Parallel()
	for _, want := range loadGolden(t) {
		if want["kind"] != "account" {
			continue
		}
		want := want
		t.Run("account", func(t *testing.T) {
			t.Parallel()

			acct, ok := want["account"].(map[string]any)
			require.True(t, ok, "account not a map")

			didStr, ok := acct["did"].(string)
			require.True(t, ok, "did not a string")
			seqFloat, ok := acct["seq"].(float64)
			require.True(t, ok, "seq not a float64")
			timeStr, ok := acct["time"].(string)
			require.True(t, ok, "time not a string")
			active, ok := acct["active"].(bool)
			require.True(t, ok, "active not a bool")

			a := &comatproto.SyncSubscribeRepos_Account{
				DID:    didStr,
				Seq:    int64(seqFloat),
				Time:   timeStr,
				Active: active,
			}
			payload, err := a.MarshalCBOR()
			require.NoError(t, err)

			timeUS, ok := want["time_us"].(float64)
			require.True(t, ok, "time_us not a float64")
			did, ok := want["did"].(string)
			require.True(t, ok, "did not a string")

			segEvt := &segment.Event{
				WitnessedAt: int64(timeUS),
				Kind:        segment.KindAccount,
				DID:         did,
				Payload:     payload,
			}

			gotJSON, err := Encode(segEvt)
			require.NoError(t, err)

			var got map[string]any
			require.NoError(t, json.Unmarshal(gotJSON, &got))
			require.True(t, reflect.DeepEqual(got, want),
				"account mismatch\nwant: %s\n got: %s",
				mustJSON(t, want), mustJSON(t, got))
		})
	}
}

func TestEncode_SyncReturnsSkipSentinel(t *testing.T) {
	t.Parallel()
	_, err := Encode(&segment.Event{Kind: segment.KindSync, DID: "did:plc:x"})
	require.ErrorIs(t, err, errSkipEvent)
}

func TestEncode_CreateResyncUsesCreateWireOperation(t *testing.T) {
	t.Parallel()

	body, err := Encode(&segment.Event{
		Seq:         123,
		WitnessedAt: 1_700_000_000_000_000,
		Kind:        segment.KindCreateResync,
		DID:         "did:plc:x",
		Collection:  "app.bsky.feed.post",
		Rkey:        "r1",
		Rev:         "rev1",
		Payload:     []byte{0xa0},
	})
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(body, &got))
	commit, ok := got["commit"].(map[string]any)
	require.True(t, ok, "commit payload should be present")
	require.Equal(t, "create", commit["operation"])
}

func TestEncode_UnknownKindReturnsError(t *testing.T) {
	t.Parallel()
	_, err := Encode(&segment.Event{Kind: segment.Kind(99)})
	require.Error(t, err)
	require.NotErrorIs(t, err, errSkipEvent)
}

func TestEncode_CursorFieldOnCommit(t *testing.T) {
	t.Parallel()
	// Empty CBOR map (0xa0) is sufficient — the encoder will decode
	// and re-encode it as JSON; the test only asserts the envelope's
	// cursor field, not the record contents.
	evt := &segment.Event{
		Seq:         12345,
		WitnessedAt: 1_700_000_000_000_000,
		Kind:        segment.KindCreate,
		DID:         "did:plc:test",
		Collection:  "app.bsky.feed.post",
		Rkey:        "abc",
		Rev:         "rev1",
		Payload:     []byte{0xa0},
	}
	body, err := Encode(evt)
	require.NoError(t, err)
	require.Contains(t, string(body), `"cursor":12345`)
}

func TestEncode_CursorFieldOnIdentity(t *testing.T) {
	t.Parallel()
	ident := &comatproto.SyncSubscribeRepos_Identity{
		DID: "did:plc:test",
		Seq: 99,
	}
	payload, err := ident.MarshalCBOR()
	require.NoError(t, err)
	evt := &segment.Event{
		Seq:         12345,
		WitnessedAt: 1_700_000_000_000_000,
		Kind:        segment.KindIdentity,
		DID:         "did:plc:test",
		Payload:     payload,
	}
	body, err := Encode(evt)
	require.NoError(t, err)
	require.Contains(t, string(body), `"cursor":12345`)
}

func TestEncode_CursorFieldOnAccount(t *testing.T) {
	t.Parallel()
	acct := &comatproto.SyncSubscribeRepos_Account{
		DID:    "did:plc:test",
		Active: true,
		Seq:    77,
	}
	payload, err := acct.MarshalCBOR()
	require.NoError(t, err)
	evt := &segment.Event{
		Seq:         12345,
		WitnessedAt: 1_700_000_000_000_000,
		Kind:        segment.KindAccount,
		DID:         "did:plc:test",
		Payload:     payload,
	}
	body, err := Encode(evt)
	require.NoError(t, err)
	require.Contains(t, string(body), `"cursor":12345`)
}

// unwrapV2Frame asserts body is a proposal-0015 message frame
// ({"$type":"message","payload":{...}}) and returns the payload object.
func unwrapV2Frame(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var frame map[string]any
	require.NoError(t, json.Unmarshal(body, &frame))
	require.Equal(t, "message", frame["$type"], "frame envelope $type")
	require.Len(t, frame, 2, "message frame must be exactly {$type, payload}")
	payload, ok := frame["payload"].(map[string]any)
	require.True(t, ok, "payload not an object in %s", body)
	return payload
}

// decodeV2Union decodes the frame's payload through the lexgen-generated
// message union — the exact path a standard-lexicon-tooling consumer
// takes. This is the cross-stack conformance check: the wire our encoder
// produces must be consumable without any jetstream-specific decode code.
func decodeV2Union(t *testing.T, body []byte) *jetstream.JetstreamSubscribe_Message {
	t.Helper()
	var frame struct {
		Type    string          `json:"$type"`
		Payload json.RawMessage `json:"payload"`
	}
	require.NoError(t, json.Unmarshal(body, &frame))
	require.Equal(t, "message", frame.Type)
	var msg jetstream.JetstreamSubscribe_Message
	require.NoError(t, msg.UnmarshalJSON(frame.Payload))
	return &msg
}

func TestEncodeV2_CommitFrameCarriesRecordCBOR(t *testing.T) {
	t.Parallel()
	payload := []byte{0xa0}
	evt := &segment.Event{
		Seq:                 12345,
		WitnessedAt:         1_700_000_000_000_000,
		UpstreamRelayCursor: 98765,
		Kind:                segment.KindCreate,
		DID:                 "did:plc:test",
		Collection:          "app.bsky.feed.post",
		Rkey:                "abc",
		Rev:                 "rev1",
		Payload:             payload,
	}

	body, err := EncodeV2(evt)
	require.NoError(t, err)

	got := unwrapV2Frame(t, body)
	require.Equal(t, "network.bsky.jetstream.subscribe#commit", got["$type"])
	require.Equal(t, float64(12345), got["seq"])
	require.Equal(t, "did:plc:test", got["did"])
	require.Equal(t, "2023-11-14T22:13:20.000000Z", got["time"],
		"time must be the canonical six-fractional-digit UTC rendering of WitnessedAt")
	require.Equal(t, "create", got["operation"])
	require.Equal(t, "app.bsky.feed.post", got["collection"])
	require.Equal(t, "abc", got["rkey"])
	require.Equal(t, "rev1", got["rev"])
	require.NotContains(t, got, "upstream_relay_cursor", "internal relay cursor must not leak onto the wire")
	require.NotContains(t, got, "kind", "the kind discriminator is the $type; no kind field on the new wire")
	require.NotContains(t, got, "cursor", "the duplicated cursor field died with the old wire; seq is the cursor")

	recordCbor, ok := got["recordCbor"].(map[string]any)
	require.True(t, ok, "recordCbor must be the atproto data-model $bytes object")
	require.Equal(t, base64.RawStdEncoding.EncodeToString(payload), recordCbor["$bytes"])

	// Cross-stack: the payload decodes through the generated union.
	msg := decodeV2Union(t, body)
	commit := msg.JetstreamSubscribe_Commit.Val()
	require.Equal(t, int64(12345), commit.Seq)
	require.Equal(t, payload, commit.RecordCbor)
	require.Equal(t, "app.bsky.feed.post", commit.Collection)
}

func TestEncodeV2_CommitDeleteOmitsRecordPayloads(t *testing.T) {
	t.Parallel()
	evt := &segment.Event{
		Seq:         9,
		WitnessedAt: 123,
		Kind:        segment.KindDelete,
		DID:         "did:plc:test",
		Collection:  "app.bsky.feed.post",
		Rkey:        "abc",
		Rev:         "rev1",
	}

	body, err := EncodeV2(evt)
	require.NoError(t, err)
	got := unwrapV2Frame(t, body)
	require.Equal(t, "network.bsky.jetstream.subscribe#commit", got["$type"])
	require.Equal(t, "delete", got["operation"])
	require.Equal(t, float64(9), got["seq"])
	require.NotContains(t, got, "record")
	require.NotContains(t, got, "cid")
	require.NotContains(t, got, "recordCbor")
}

func TestEncodeV2_IdentityAndAccountWrapUpstreamEvents(t *testing.T) {
	t.Parallel()
	ident := &comatproto.SyncSubscribeRepos_Identity{
		DID: "did:plc:test", Seq: 99, Time: "2026-05-25T00:00:00Z",
	}
	identPayload, err := ident.MarshalCBOR()
	require.NoError(t, err)
	acct := &comatproto.SyncSubscribeRepos_Account{
		DID: "did:plc:test", Active: true, Seq: 100, Time: "2026-05-25T00:00:01Z",
	}
	acctPayload, err := acct.MarshalCBOR()
	require.NoError(t, err)

	for _, tc := range []struct {
		name        string
		kind        segment.Kind
		payload     []byte
		upstreamSeq float64
	}{
		{"identity", segment.KindIdentity, identPayload, 99},
		{"account", segment.KindAccount, acctPayload, 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			body, err := EncodeV2(&segment.Event{
				Seq:         123,
				WitnessedAt: 1_700_000_000_000_000,
				Kind:        tc.kind,
				DID:         "did:plc:test",
				Payload:     tc.payload,
			})
			require.NoError(t, err)
			got := unwrapV2Frame(t, body)
			require.Equal(t, "network.bsky.jetstream.subscribe#"+tc.name, got["$type"])
			require.Equal(t, float64(123), got["seq"], "envelope seq is jetstream's")
			require.Equal(t, "did:plc:test", got["did"])

			upstream, ok := got[tc.name].(map[string]any)
			require.True(t, ok, "wrapped upstream event not a map")
			require.Equal(t, tc.upstreamSeq, upstream["seq"],
				"wrapped event keeps the upstream relay's seq, distinct from jetstream's")
		})
	}
}

func TestEncodeV2_SyncEmitsArchivedEvent(t *testing.T) {
	t.Parallel()
	sync := &comatproto.SyncSubscribeRepos_Sync{
		DID:    "did:plc:test",
		Rev:    "rev-sync",
		Seq:    444,
		Time:   "2026-05-25T00:00:00Z",
		Blocks: []byte{0x01, 0x02, 0x03},
	}
	payload, err := sync.MarshalCBOR()
	require.NoError(t, err)
	body, err := EncodeV2(&segment.Event{
		Seq:         77,
		WitnessedAt: 1_700_000_000_000_000,
		Kind:        segment.KindSync,
		DID:         "did:plc:test",
		Rev:         "rev-sync",
		Payload:     payload,
	})
	require.NoError(t, err)

	got := unwrapV2Frame(t, body)
	require.Equal(t, "network.bsky.jetstream.subscribe#sync", got["$type"])
	require.Equal(t, float64(77), got["seq"])

	syncJSON, ok := got["sync"].(map[string]any)
	require.True(t, ok, "sync not a map")
	require.Equal(t, "did:plc:test", syncJSON["did"])
	require.Equal(t, "rev-sync", syncJSON["rev"])
	blocks, ok := syncJSON["blocks"].(map[string]any)
	require.True(t, ok, "blocks must be the $bytes object")
	require.Equal(t, base64.RawStdEncoding.EncodeToString(sync.Blocks), blocks["$bytes"])

	msg := decodeV2Union(t, body)
	require.Equal(t, sync.Blocks, msg.JetstreamSubscribe_Sync.Val().Sync.Blocks)
}

func TestEncodeV2Error_FrameShape(t *testing.T) {
	t.Parallel()
	frame := EncodeV2Error("ConsumerTooSlow", "reader below floor rate")
	var got map[string]any
	require.NoError(t, json.Unmarshal(frame, &got))
	require.Equal(t, map[string]any{
		"$type":   "error",
		"error":   "ConsumerTooSlow",
		"message": "reader below floor rate",
	}, got)

	// message is optional and omitted when empty, per the proposal.
	frame = EncodeV2Error("ConsumerTooSlow", "")
	got = map[string]any{}
	require.NoError(t, json.Unmarshal(frame, &got))
	require.Equal(t, map[string]any{"$type": "error", "error": "ConsumerTooSlow"}, got)
}

func TestEncodeV2Info_FrameShape(t *testing.T) {
	t.Parallel()
	body, err := EncodeV2Info("OutdatedCursor", "starting at seq 42")
	require.NoError(t, err)
	got := unwrapV2Frame(t, body)
	require.Equal(t, "network.bsky.jetstream.subscribe#info", got["$type"])
	require.Equal(t, "OutdatedCursor", got["name"])
	require.Equal(t, "starting at seq 42", got["message"])
	require.NotContains(t, got, "seq", "info frames carry no seq")

	msg := decodeV2Union(t, body)
	require.True(t, msg.JetstreamSubscribe_Info.HasVal())
	require.Equal(t, "OutdatedCursor", msg.JetstreamSubscribe_Info.Val().Name)
}

func TestEncodeV2_UnknownKindReturnsError(t *testing.T) {
	t.Parallel()
	_, err := EncodeV2(&segment.Event{Kind: segment.Kind(99)})
	require.Error(t, err)
	require.NotErrorIs(t, err, errSkipEvent)
}

// TestEncode_TimeUSResolvesDisplayValue is the M2 behavioral guarantee: the
// wire time_us is the imported IndexedAt when one was set, otherwise it falls
// back to WitnessedAt. This must hold across every encoder entry point (v1 +
// extended) and every kind, because time_us lives on the shared envelope. A
// #sync event has no v1 form, so it is exercised only on the extended path.
func TestEncode_TimeUSResolvesDisplayValue(t *testing.T) {
	t.Parallel()

	const witnessed = int64(1_700_000_000_000_000)
	const imported = int64(1_600_000_000_000_000)

	ident := &comatproto.SyncSubscribeRepos_Identity{DID: "did:plc:x", Seq: 1, Time: "2026-05-25T00:00:00Z"}
	identPayload, err := ident.MarshalCBOR()
	require.NoError(t, err)
	acct := &comatproto.SyncSubscribeRepos_Account{DID: "did:plc:x", Active: true, Seq: 2, Time: "2026-05-25T00:00:01Z"}
	acctPayload, err := acct.MarshalCBOR()
	require.NoError(t, err)
	sync := &comatproto.SyncSubscribeRepos_Sync{DID: "did:plc:x", Rev: "r", Seq: 3, Time: "2026-05-25T00:00:02Z", Blocks: []byte{0x01}}
	syncPayload, err := sync.MarshalCBOR()
	require.NoError(t, err)

	// event returns a fresh segment.Event of the given kind with the given
	// witnessed/indexed columns; the payload is picked to match the kind so
	// every encoder path decodes cleanly.
	event := func(kind segment.Kind, w, idx int64) *segment.Event {
		e := &segment.Event{
			Seq:         42,
			WitnessedAt: w,
			IndexedAt:   idx,
			Kind:        kind,
			DID:         "did:plc:x",
		}
		switch kind {
		case segment.KindIdentity:
			e.Payload = identPayload
		case segment.KindAccount:
			e.Payload = acctPayload
		case segment.KindSync:
			e.Payload = syncPayload
		default: // commit kinds
			e.Collection = "app.bsky.feed.post"
			e.Rkey = "abc"
			e.Rev = "rev1"
			e.Payload = []byte{0xa0}
		}
		return e
	}

	timeUSOf := func(t *testing.T, body []byte) int64 {
		t.Helper()
		var m map[string]any
		require.NoError(t, json.Unmarshal(body, &m))
		f, ok := m["time_us"].(float64)
		require.True(t, ok, "time_us not a float64 in %s", body)
		return int64(f)
	}

	// v2 frames carry the display time as the canonical datetime string;
	// recover the µs value to assert the same display-resolution rule.
	v2TimeUSOf := func(t *testing.T, body []byte) int64 {
		t.Helper()
		payload := unwrapV2Frame(t, body)
		s, ok := payload["time"].(string)
		require.True(t, ok, "time not a string in %s", body)
		ts, err := time.Parse(wireTimeLayout, s)
		require.NoError(t, err)
		return ts.UnixMicro()
	}

	// v1 Encode: commit, identity, account (sync has no v1 form).
	for _, kind := range []segment.Kind{segment.KindCreate, segment.KindIdentity, segment.KindAccount} {
		t.Run("v1_unimported_"+string(rune('0'+int(kind))), func(t *testing.T) {
			t.Parallel()
			body, err := Encode(event(kind, witnessed, 0))
			require.NoError(t, err)
			require.Equal(t, witnessed, timeUSOf(t, body), "unimported must fall back to witnessed")
		})
		t.Run("v1_imported_"+string(rune('0'+int(kind))), func(t *testing.T) {
			t.Parallel()
			body, err := Encode(event(kind, witnessed, imported))
			require.NoError(t, err)
			require.Equal(t, imported, timeUSOf(t, body), "imported display value must win")
		})
	}

	// v2 path covers all kinds including #sync.
	for _, kind := range []segment.Kind{segment.KindCreate, segment.KindIdentity, segment.KindAccount, segment.KindSync} {
		t.Run("v2_unimported_"+string(rune('0'+int(kind))), func(t *testing.T) {
			t.Parallel()
			body, err := EncodeV2(event(kind, witnessed, 0))
			require.NoError(t, err)
			require.Equal(t, witnessed, v2TimeUSOf(t, body), "unimported must fall back to witnessed")
		})
		t.Run("v2_imported_"+string(rune('0'+int(kind))), func(t *testing.T) {
			t.Parallel()
			body, err := EncodeV2(event(kind, witnessed, imported))
			require.NoError(t, err)
			require.Equal(t, imported, v2TimeUSOf(t, body), "imported display value must win")
		})
	}
}

// TestWireTime_MicrosecondRoundTrip pins the canonical rendering: every
// unix-µs value formats to exactly six fractional digits UTC and parses
// back to the identical µs value (goldens, the zstd dictionary, and
// client-side µs recovery rely on one byte-stable rendering per instant).
func TestWireTime_MicrosecondRoundTrip(t *testing.T) {
	t.Parallel()
	for _, us := range []int64{
		0,
		1,
		999_999,
		1_700_000_000_000_000,
		1_700_000_000_123_456,
		1_700_000_000_120_000,
	} {
		s := wireTime(us)
		require.Regexp(t, `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$`, s,
			"wire time must always carry exactly six fractional digits")
		ts, err := time.Parse(wireTimeLayout, s)
		require.NoError(t, err)
		require.Equal(t, us, ts.UnixMicro(), "µs must round-trip losslessly through %q", s)
	}
}

func TestEncode_CursorOmittedWhenZero(t *testing.T) {
	t.Parallel()
	evt := &segment.Event{
		Seq:         0,
		WitnessedAt: 1_700_000_000_000_000,
		Kind:        segment.KindCreate,
		DID:         "did:plc:test",
		Collection:  "app.bsky.feed.post",
		Rkey:        "abc",
		Rev:         "rev1",
		Payload:     []byte{0xa0},
	}
	body, err := Encode(evt)
	require.NoError(t, err)
	require.NotContains(t, string(body), `"cursor":0`,
		"omitempty in atmos.JetstreamEvent must keep cursor:0 off the wire")
}
