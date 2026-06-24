package client

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/require"
)

// oldDecodeRecordMap is the prior JSON-text round-trip implementation, kept in
// the test as the reference oracle for the direct converter that replaced it
// (#142 lever 1). decodeRecordMap must stay deep-equal to this for every record
// shape, or existing consumers of the "record" field would see a behavior
// change. If the library's JSON sentinel mapping ever changes, this reference
// changes with it and the differential test still pins equivalence.
func oldDecodeRecordMap(t *testing.T, payload []byte) (map[string]any, bool) {
	t.Helper()
	val, err := cbor.NewDecoder(bytes.NewReader(payload)).ReadValue()
	if err != nil {
		return nil, false
	}
	jsonBytes, err := cbor.ToJSON(val)
	if err != nil {
		return nil, false
	}
	var m map[string]any
	if err := json.Unmarshal(jsonBytes, &m); err != nil || m == nil {
		return nil, false
	}
	return m, true
}

// TestDecodeRecordMapMatchesJSONRoundTrip is the equivalence oracle for #142
// lever 1: the direct CBOR->map converter must produce a result deep-equal to
// the old cbor.ToJSON + json.Unmarshal round-trip across representative and
// edge-case record shapes (nested maps, arrays, CID links, byte strings,
// integers, floats, bools, null, empty containers, unicode/escaped strings).
func TestDecodeRecordMapMatchesJSONRoundTrip(t *testing.T) {
	t.Parallel()
	cidA := mustCID(t)
	cases := map[string]map[string]any{
		"like": {
			"$type":     "app.bsky.feed.like",
			"createdAt": "2024-11-20T15:27:04.328Z",
			"subject":   map[string]any{"cid": "bafy", "uri": "at://x/y/z"},
		},
		"scalars": {
			"$type":  "x.test",
			"int":    int64(42),
			"negint": int64(-7),
			"float":  3.5,
			"bool":   true,
			"nil":    nil,
			"str":    "héllo \"world\"\n\t/slash",
		},
		"containers": {
			"$type":     "x.test",
			"arr":       []any{int64(1), "two", false, nil, []any{int64(3)}},
			"emptyArr":  []any{},
			"emptyMap":  map[string]any{},
			"nestedMap": map[string]any{"a": map[string]any{"b": map[string]any{"c": int64(1)}}},
		},
		"binaryAndLink": {
			"$type": "x.test",
			"blob":  []byte{0x00, 0x01, 0xfe, 0xff, 0x10, 0x20},
			"empty": []byte{},
			"link":  cidA,
		},
	}
	for name, rec := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			payload, err := cbor.Marshal(rec)
			require.NoError(t, err)

			want, ok := oldDecodeRecordMap(t, payload)
			require.True(t, ok, "reference path must decode the fixture")

			got, err := decodeRecordMap(payload)
			require.NoError(t, err)
			require.Equal(t, want, got, "direct converter must match the JSON round-trip exactly")

			// The JSON serialization must also be identical — the public client
			// marshals Event.Commit.Record, so byte-equality is the real contract.
			wantJSON, err := json.Marshal(want)
			require.NoError(t, err)
			gotJSON, err := json.Marshal(got)
			require.NoError(t, err)
			require.JSONEq(t, string(wantJSON), string(gotJSON))
		})
	}
}

// FuzzDecodeRecordMapEquivalence drives arbitrary bytes through the direct
// converter and pins the two halves of its contract:
//
//   - WHENEVER decodeRecordMap accepts a payload, the result must be deep-equal
//     to the canonical cbor.ToJSON + json.Unmarshal conversion of that same
//     payload — i.e. the value handed to the consumer faithfully represents the
//     record the way the server's /subscribe does. This is the equivalence that
//     actually matters.
//   - It is deliberately STRICTER on what it accepts: a valid record is always a
//     CBOR map, so a non-map top-level value (scalar/array/bytes/CID/null) is
//     rejected. We do NOT require "old accepts => new accepts"; the old round
//     trip inconsistently accepted top-level bytes/CID, which are not records.
func FuzzDecodeRecordMapEquivalence(f *testing.F) {
	for _, rec := range []map[string]any{
		{"$type": "app.bsky.feed.like", "subject": map[string]any{"cid": "x"}},
		{"a": int64(1), "b": []any{int64(2), int64(3)}},
		{"bytes": []byte{1, 2, 3}},
	} {
		if p, err := cbor.Marshal(rec); err == nil {
			f.Add(p)
		}
	}
	f.Fuzz(func(t *testing.T, payload []byte) {
		got, gotErr := decodeRecordMap(payload)
		if gotErr != nil {
			return // rejection is always allowed; the accept path is what we pin
		}
		// Accepted: the value must equal the canonical conversion of the SAME
		// payload exactly. Recompute the reference independently of the converter.
		r := bytes.NewReader(payload)
		val, err := cbor.NewDecoder(r).ReadValue()
		require.NoError(t, err, "converter accepted a payload that does not CBOR-decode")
		require.Zero(t, r.Len(), "converter accepted a payload with trailing bytes")
		jsonBytes, err := cbor.ToJSON(val)
		require.NoError(t, err)
		var ref map[string]any
		require.NoError(t, json.Unmarshal(jsonBytes, &ref))
		require.NotNil(t, ref, "converter accepted a payload the canonical path treats as non-object")
		require.Equal(t, ref, got, "accepted value must equal the canonical CBOR->JSON conversion")
	})
}

func mustCID(t *testing.T) cbor.CID {
	t.Helper()
	c := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("hello"))
	return c
}

// TestDecodeRecordMapRejectsTrailingBytes guards the strict single-item decode
// contract: a payload that is a valid CBOR map followed by junk must be
// rejected, because the decoded map would otherwise diverge from the CID and
// RecordCBOR computed over the full payload.
func TestDecodeRecordMapRejectsTrailingBytes(t *testing.T) {
	t.Parallel()
	valid, err := cbor.Marshal(map[string]any{"$type": "app.bsky.feed.post", "text": "hi"})
	require.NoError(t, err)

	payload := append(append([]byte{}, valid...), 0xff, 0x00) // valid map + junk
	_, err = decodeRecordMap(payload)
	require.Error(t, err, "trailing bytes after a record must be rejected")
	require.ErrorContains(t, err, "trailing")
}

// TestDecodeRecordMapRejectsNull guards against CBOR null being accepted as a
// commit record: json.Unmarshal of null leaves the map nil with no error, which
// would surface a non-delete commit with a nil Record.
func TestDecodeRecordMapRejectsNull(t *testing.T) {
	t.Parallel()
	// 0xf6 is the DAG-CBOR encoding of null.
	_, err := decodeRecordMap([]byte{0xf6})
	require.Error(t, err, "a null record must be rejected")
	require.ErrorContains(t, err, "not an object")
}

// TestDecodeCommitNullRecordFailsClosed asserts the end-to-end path: a non-delete
// commit row whose payload is CBOR null must fail decode rather than yield a
// commit with a nil Record.
func TestDecodeCommitNullRecordFailsClosed(t *testing.T) {
	t.Parallel()
	ev := &segment.Event{
		Seq:        1,
		Kind:       segment.KindCreate,
		DID:        "did:plc:a",
		Collection: "app.bsky.feed.post",
		Rkey:       "r1",
		Rev:        "rev1",
		Payload:    []byte{0xf6}, // CBOR null
	}
	_, err := decodeCommit(ev)
	require.Error(t, err, "a create commit with a null record must fail decode")
}
