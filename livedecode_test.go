package jetstream

import (
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/require"
)

// wireTime is the canonical six-fractional-digit UTC rendering the server
// emits; 1 µs after the epoch keeps fixtures short and µs-exact.
const testWireTime = "1970-01-01T00:00:00.000001Z"

// liveCommitFrame builds an xrpc.v1.json #commit message frame.
func liveCommitFrame(t *testing.T, seq uint64, did, op, coll, rkey string, withRecord bool) []byte {
	t.Helper()
	s := strconv.FormatUint(seq, 10)
	frame := `{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#commit"` +
		`,"seq":` + s + `,"did":"` + did + `","time":"` + testWireTime + `"` +
		`,"rev":"r","operation":"` + op + `","collection":"` + coll + `","rkey":"` + rkey + `"`
	if withRecord {
		frame += `,"cid":"bafytest","record":{"$type":"` + coll + `","text":"hi"}`
	}
	frame += `}}`
	return []byte(frame)
}

// liveIdentityFrame builds an xrpc.v1.json #identity message frame.
func liveIdentityFrame(seq uint64, did, handle string) []byte {
	s := strconv.FormatUint(seq, 10)
	return []byte(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#identity"` +
		`,"seq":` + s + `,"did":"` + did + `","time":"` + testWireTime + `"` +
		`,"identity":{"did":"` + did + `","handle":"` + handle + `","seq":` + s + `,"time":"2026-05-25T00:00:00Z"}}}`)
}

// liveAccountFrame builds an xrpc.v1.json #account message frame. A deleted
// account (active=false, status="deleted") doubles as a DID-level tombstone.
func liveAccountFrame(seq uint64, did string, active bool, status string) []byte {
	s := strconv.FormatUint(seq, 10)
	act := "true"
	if !active {
		act = "false"
	}
	return []byte(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#account"` +
		`,"seq":` + s + `,"did":"` + did + `","time":"` + testWireTime + `"` +
		`,"account":{"did":"` + did + `","active":` + act + `,"status":"` + status + `","seq":` + s + `,"time":"2026-05-25T00:00:00Z"}}}`)
}

func TestDecodeLiveFrameCommit(t *testing.T) {
	t.Parallel()
	ev, info, err := decodeLiveFrame(liveCommitFrame(t, 42, "did:plc:a", "create", "app.bsky.feed.post", "r1", true), recordDecodeMode{})
	require.NoError(t, err)
	require.Nil(t, info)
	require.Equal(t, KindCommit, ev.Kind)
	require.EqualValues(t, 42, ev.Seq)
	require.Equal(t, "did:plc:a", ev.DID)
	require.EqualValues(t, 1, ev.TimeUS, "the canonical datetime parses back to unix-µs")
	require.Equal(t, OpCreate, ev.Commit.Operation)
	require.Equal(t, "app.bsky.feed.post", ev.Commit.Collection)
	require.Equal(t, "hi", ev.Commit.Record["text"])
	require.NotEmpty(t, ev.Commit.RecordCBOR)
}

func TestDecodeLiveFrameDelete(t *testing.T) {
	t.Parallel()
	ev, _, err := decodeLiveFrame(liveCommitFrame(t, 7, "did:plc:a", "delete", "app.bsky.feed.post", "r1", false), recordDecodeMode{})
	require.NoError(t, err)
	require.Equal(t, OpDelete, ev.Commit.Operation)
	require.Nil(t, ev.Commit.Record)
	require.Nil(t, ev.Commit.RecordCBOR)
}

func TestDecodeLiveFrameCreateMissingRecord(t *testing.T) {
	t.Parallel()
	_, _, err := decodeLiveFrame(liveCommitFrame(t, 1, "did:plc:a", "create", "app.bsky.feed.post", "r", false), recordDecodeMode{})
	require.ErrorContains(t, err, "missing record")
}

func TestDecodeLiveFrameCanonicalizesRecordJSON(t *testing.T) {
	t.Parallel()
	const link = "bafyreidykglsf47zgclpbxyaqaextkc4pw4hoyqx7kod6orhhixr5q5j2i"
	frame := []byte(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#commit","seq":1,"did":"did:plc:a","time":"` + testWireTime + `","rev":"r","operation":"create","collection":"app.bsky.feed.post","rkey":"r","record":{"z":{"$bytes":"AQI="},"max":9223372036854775807,"a":{"$link":"` + link + `"},"min":-9223372036854775808,"items":[0,true,null]}}}`)
	ev, _, err := decodeLiveFrame(frame, recordDecodeMode{raw: true})
	require.NoError(t, err)
	require.Nil(t, ev.Commit.Record, "raw mode must still skip retaining the generic map")
	require.NotEmpty(t, ev.Commit.RecordCBOR)

	cid, err := cbor.ParseCIDString(link)
	require.NoError(t, err)
	expected, err := cbor.Marshal(map[string]any{
		"a": cid, "items": []any{int64(0), true, nil},
		"max": int64(9223372036854775807), "min": int64(-9223372036854775808),
		"z": []byte{1, 2},
	})
	require.NoError(t, err)
	require.Equal(t, expected, ev.Commit.RecordCBOR)
}

func TestDecodeLiveRecordRejectsInvalidAtprotoJSON(t *testing.T) {
	t.Parallel()
	for _, input := range []string{
		`{"n":1.5}`,
		`{"n":9223372036854775808}`,
		`{"n":-9223372036854775809}`,
		`{"n":1,"n":2}`,
		`{} {}`,
	} {
		_, _, err := decodeLiveRecord([]byte(input))
		require.Error(t, err, input)
	}
}

func TestDecodeLiveFrameAccountIdentitySync(t *testing.T) {
	t.Parallel()
	ev, _, err := decodeLiveFrame(liveAccountFrame(5, "did:plc:a", false, "deleted"), recordDecodeMode{})
	require.NoError(t, err)
	require.Equal(t, KindAccount, ev.Kind)
	require.False(t, ev.Account.Active)
	require.Equal(t, "deleted", ev.Account.Status)
	require.EqualValues(t, 5, ev.Account.Seq, "wrapped upstream event keeps the relay's seq")

	ev, _, err = decodeLiveFrame(liveIdentityFrame(6, "did:plc:a", "alice.test"), recordDecodeMode{})
	require.NoError(t, err)
	require.Equal(t, KindIdentity, ev.Kind)
	require.Equal(t, "alice.test", ev.Identity.Handle)

	sync := []byte(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#sync"` +
		`,"seq":8,"did":"did:plc:a","time":"` + testWireTime + `"` +
		`,"sync":{"did":"did:plc:a","rev":"rev1","seq":800,"time":"2026-05-25T00:00:00Z","blocks":{"$bytes":"AQI"}}}}`)
	ev, _, err = decodeLiveFrame(sync, recordDecodeMode{})
	require.NoError(t, err)
	require.Equal(t, KindSync, ev.Kind)
	require.EqualValues(t, 8, ev.Seq, "envelope seq is jetstream's")
	require.Equal(t, "rev1", ev.Sync.Rev)
	require.EqualValues(t, 800, ev.Sync.Seq, "wrapped upstream event keeps the relay's seq")
}

func TestDecodeLiveFrameUnknownPayloadTypeSkips(t *testing.T) {
	t.Parallel()
	// A well-formed message frame whose payload $type this build does not
	// know is a newer server's addition: skip, never error.
	for _, typ := range []string{
		"network.bsky.jetstream.subscribeEvents#futureKind",
		"com.example.other#thing",
	} {
		_, info, err := decodeLiveFrame([]byte(`{"$type":"message","payload":{"$type":"`+typ+`","seq":1}}`), recordDecodeMode{})
		require.ErrorIs(t, err, errSkipFrame, "payload $type %q must be skipped, not errored", typ)
		require.Nil(t, info)
	}
}

func TestDecodeLiveFrameUnknownEnvelopeTypeSkips(t *testing.T) {
	t.Parallel()
	// A frame whose envelope $type is neither message nor error is a newer
	// protocol revision: skip.
	_, _, err := decodeLiveFrame([]byte(`{"$type":"snapshot","data":{}}`), recordDecodeMode{})
	require.ErrorIs(t, err, errSkipFrame)
}

func TestDecodeLiveFrameInfoFrame(t *testing.T) {
	t.Parallel()
	frame := []byte(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#info"` +
		`,"name":"OutdatedCursor","message":"starting at seq 200"}}`)
	_, info, err := decodeLiveFrame(frame, recordDecodeMode{})
	require.ErrorIs(t, err, errSkipFrame, "info frames are advisories, not events")
	require.NotNil(t, info)
	require.Equal(t, "OutdatedCursor", info.Name)
	require.Equal(t, "starting at seq 200", info.Message)
}

func TestDecodeLiveFrameErrorFrame(t *testing.T) {
	t.Parallel()
	_, _, err := decodeLiveFrame([]byte(`{"$type":"error","error":"ConsumerTooSlow","message":"too far behind"}`), recordDecodeMode{})
	require.NotErrorIs(t, err, errSkipFrame)
	var streamErr *liveStreamError
	require.ErrorAs(t, err, &streamErr)
	require.Equal(t, "ConsumerTooSlow", streamErr.Code)
	require.Equal(t, "too far behind", streamErr.Message)

	// An error frame with no code is malformed, not a typed stream error.
	_, _, err = decodeLiveFrame([]byte(`{"$type":"error","message":"?"}`), recordDecodeMode{})
	require.Error(t, err)
	require.NotErrorIs(t, err, errSkipFrame)
	require.NotErrorAs(t, err, &streamErr)
}

func TestDecodeLiveFrameMalformed(t *testing.T) {
	t.Parallel()
	for name, frame := range map[string]string{
		"not json":        `{`,
		"missing payload": `{"$type":"message"}`,
		"bad time":        `{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#sync","seq":1,"did":"did:plc:a","time":"not-a-time","sync":{"did":"did:plc:a","rev":"r","seq":1,"time":"t","blocks":{"$bytes":"AQI"}}}}`,
		"negative seq":    `{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#sync","seq":-1,"did":"did:plc:a","time":"` + testWireTime + `","sync":{"did":"did:plc:a","rev":"r","seq":1,"time":"t","blocks":{"$bytes":"AQI"}}}}`,
		// A frame with NO envelope $type is malformed (e.g. a pre-lexicon
		// /subscribe-v2 or v1 server), not a newer protocol revision — it
		// must error so a wrong endpoint doesn't look healthy while
		// silently delivering nothing.
		"missing envelope type": `{"did":"did:plc:a","time_us":1,"cursor":1,"kind":"commit"}`,
		// Missing REQUIRED wrapped upstream payloads: the generated union
		// decoder does not enforce lexicon `required`, so the decode layer
		// must — otherwise a zero-valued wrapped event is emitted and
		// advances the dedup cursor.
		"identity missing payload": `{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#identity","seq":1,"did":"did:plc:a","time":"` + testWireTime + `"}}`,
		"account missing payload":  `{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#account","seq":1,"did":"did:plc:a","time":"` + testWireTime + `"}}`,
		"sync missing payload":     `{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#sync","seq":1,"did":"did:plc:a","time":"` + testWireTime + `"}}`,
		// A payload with no $type discriminator is malformed, not a future
		// message kind (which would carry a nonempty unknown $type).
		"payload missing type": `{"$type":"message","payload":{"seq":1,"did":"did:plc:a"}}`,
		// A commit missing its required record identifiers cannot be keyed
		// by a folding consumer.
		"commit missing collection": `{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#commit","seq":1,"did":"did:plc:a","time":"` + testWireTime + `","rev":"r","operation":"delete","rkey":"k"}}`,
		// seq 0 means the required seq field was absent (the wire is 1-based).
		"commit zero seq": `{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#commit","did":"did:plc:a","time":"` + testWireTime + `","rev":"r","operation":"delete","collection":"c","rkey":"k"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, _, err := decodeLiveFrame([]byte(frame), recordDecodeMode{})
			require.Error(t, err)
			require.NotErrorIs(t, err, errSkipFrame)
		})
	}
}

// TestDecodeLiveFrameBoundsUntrustedDiagnostics pins the byte bounds on
// server-supplied diagnostic strings: an error frame's code/message and an
// #info's name/message can be as large as the read limit, and must be
// truncated (rune-aligned) before they reach error strings or logs.
func TestDecodeLiveFrameBoundsUntrustedDiagnostics(t *testing.T) {
	t.Parallel()

	huge := strings.Repeat("é", 4096) // multibyte to exercise rune alignment

	_, _, err := decodeLiveFrame([]byte(`{"$type":"error","error":"`+huge+`","message":"`+huge+`"}`), recordDecodeMode{})
	var streamErr *liveStreamError
	require.ErrorAs(t, err, &streamErr)
	require.LessOrEqual(t, len(streamErr.Code), maxLiveDiagNameBytes+len("…"))
	require.LessOrEqual(t, len(streamErr.Message), maxLiveDiagMessageBytes+len("…"))
	require.True(t, utf8.ValidString(streamErr.Code), "truncation must be rune-aligned")
	require.True(t, utf8.ValidString(streamErr.Message), "truncation must be rune-aligned")

	_, info, err := decodeLiveFrame([]byte(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#info","name":"`+huge+`","message":"`+huge+`"}}`), recordDecodeMode{})
	require.ErrorIs(t, err, errSkipFrame)
	require.NotNil(t, info)
	require.LessOrEqual(t, len(info.Name), maxLiveDiagNameBytes+len("…"))
	require.LessOrEqual(t, len(info.Message), maxLiveDiagMessageBytes+len("…"))
	require.True(t, utf8.ValidString(info.Name), "truncation must be rune-aligned")
	require.True(t, utf8.ValidString(info.Message), "truncation must be rune-aligned")
}
