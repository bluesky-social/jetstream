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
		i, want := i, want
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

func TestEncodeExtended_CommitSupersetWithRecordCBOR(t *testing.T) {
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

	simpleBody, err := Encode(evt)
	require.NoError(t, err)
	extendedBody, err := EncodeExtended(evt)
	require.NoError(t, err)

	var simple, extended map[string]any
	require.NoError(t, json.Unmarshal(simpleBody, &simple))
	require.NoError(t, json.Unmarshal(extendedBody, &extended))

	for _, key := range []string{"did", "time_us", "cursor", "kind"} {
		require.Equal(t, simple[key], extended[key], "extended must preserve simple top-level field %q", key)
	}
	require.Equal(t, float64(12345), extended["seq"])
	require.Equal(t, float64(98765), extended["upstream_relay_cursor"])

	simpleCommit, ok := simple["commit"].(map[string]any)
	require.True(t, ok, "simple commit not a map")
	extendedCommit, ok := extended["commit"].(map[string]any)
	require.True(t, ok, "extended commit not a map")
	for _, key := range []string{"rev", "operation", "collection", "rkey", "cid", "record"} {
		require.Equal(t, simpleCommit[key], extendedCommit[key], "extended must preserve simple commit field %q", key)
	}
	require.Equal(t, base64.StdEncoding.EncodeToString(payload), extendedCommit["record_cbor"])
}

func TestEncodeExtended_CommitDeleteOmitsRecordPayloads(t *testing.T) {
	t.Parallel()
	evt := &segment.Event{
		Seq:                 9,
		WitnessedAt:         123,
		UpstreamRelayCursor: 456,
		Kind:                segment.KindDelete,
		DID:                 "did:plc:test",
		Collection:          "app.bsky.feed.post",
		Rkey:                "abc",
		Rev:                 "rev1",
	}

	body, err := EncodeExtended(evt)
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, "commit", got["kind"])
	require.Equal(t, float64(9), got["seq"])
	require.Equal(t, float64(456), got["upstream_relay_cursor"])

	commit, ok := got["commit"].(map[string]any)
	require.True(t, ok, "commit not a map")
	require.Equal(t, "delete", commit["operation"])
	require.NotContains(t, commit, "record")
	require.NotContains(t, commit, "cid")
	require.NotContains(t, commit, "record_cbor")
}

func TestEncodeExtended_IdentityAndAccountCarryCursors(t *testing.T) {
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
		name    string
		kind    segment.Kind
		payload []byte
		wantKey string
	}{
		{"identity", segment.KindIdentity, identPayload, "identity"},
		{"account", segment.KindAccount, acctPayload, "account"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			body, err := EncodeExtended(&segment.Event{
				Seq:                 123,
				WitnessedAt:         1_700_000_000_000_000,
				UpstreamRelayCursor: 321,
				Kind:                tc.kind,
				DID:                 "did:plc:test",
				Payload:             tc.payload,
			})
			require.NoError(t, err)
			var got map[string]any
			require.NoError(t, json.Unmarshal(body, &got))
			require.Equal(t, tc.name, got["kind"])
			require.Equal(t, float64(123), got["cursor"])
			require.Equal(t, float64(123), got["seq"])
			require.Equal(t, float64(321), got["upstream_relay_cursor"])
			require.Contains(t, got, tc.wantKey)
		})
	}
}

func TestEncodeExtended_SyncEmitsArchivedEvent(t *testing.T) {
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
	body, err := EncodeExtended(&segment.Event{
		Seq:                 77,
		WitnessedAt:         1_700_000_000_000_000,
		UpstreamRelayCursor: 444,
		Kind:                segment.KindSync,
		DID:                 "did:plc:test",
		Rev:                 "rev-sync",
		Payload:             payload,
	})
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, "sync", got["kind"])
	require.Equal(t, float64(77), got["seq"])
	require.Equal(t, float64(444), got["upstream_relay_cursor"])
	require.Contains(t, got, "sync")

	syncJSON, ok := got["sync"].(map[string]any)
	require.True(t, ok, "sync not a map")
	require.Equal(t, "did:plc:test", syncJSON["did"])
	require.Equal(t, "rev-sync", syncJSON["rev"])
	require.Equal(t, base64.StdEncoding.EncodeToString(sync.Blocks), syncJSON["blocks"])
}

func TestEncodeExtended_UnknownKindReturnsError(t *testing.T) {
	t.Parallel()
	_, err := EncodeExtended(&segment.Event{Kind: segment.Kind(99)})
	require.Error(t, err)
	require.NotErrorIs(t, err, errSkipEvent)
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
