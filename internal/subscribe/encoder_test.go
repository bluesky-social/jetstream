package subscribe

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/bluesky-social/jetstream-v2/segment"
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
				IndexedAt:  int64(timeUS),
				Kind:       commitKindFromOp(op),
				DID:        did,
				Collection: collection,
				Rkey:       rkey,
				Rev:        rev,
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
				IndexedAt: int64(timeUS),
				Kind:      segment.KindIdentity,
				DID:       did,
				Payload:   payload,
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
				IndexedAt: int64(timeUS),
				Kind:      segment.KindAccount,
				DID:       did,
				Payload:   payload,
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
		Seq:        12345,
		IndexedAt:  1_700_000_000_000_000,
		Kind:       segment.KindCreate,
		DID:        "did:plc:test",
		Collection: "app.bsky.feed.post",
		Rkey:       "abc",
		Rev:        "rev1",
		Payload:    []byte{0xa0},
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
		Seq:       12345,
		IndexedAt: 1_700_000_000_000_000,
		Kind:      segment.KindIdentity,
		DID:       "did:plc:test",
		Payload:   payload,
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
		Seq:       12345,
		IndexedAt: 1_700_000_000_000_000,
		Kind:      segment.KindAccount,
		DID:       "did:plc:test",
		Payload:   payload,
	}
	body, err := Encode(evt)
	require.NoError(t, err)
	require.Contains(t, string(body), `"cursor":12345`)
}

func TestEncode_CursorOmittedWhenZero(t *testing.T) {
	t.Parallel()
	evt := &segment.Event{
		Seq:        0,
		IndexedAt:  1_700_000_000_000_000,
		Kind:       segment.KindCreate,
		DID:        "did:plc:test",
		Collection: "app.bsky.feed.post",
		Rkey:       "abc",
		Rev:        "rev1",
		Payload:    []byte{0xa0},
	}
	body, err := Encode(evt)
	require.NoError(t, err)
	require.NotContains(t, string(body), `"cursor":0`,
		"omitempty in atmos.JetstreamEvent must keep cursor:0 off the wire")
}
