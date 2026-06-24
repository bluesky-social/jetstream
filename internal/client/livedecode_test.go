package client

import (
	"encoding/base64"
	"strconv"
	"testing"

	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/require"
)

func liveCommitFrame(t *testing.T, seq uint64, did, op, coll, rkey string, withRecord bool) []byte {
	t.Helper()
	s := strconv.FormatUint(seq, 10)
	frame := `{"did":"` + did + `","time_us":1,"cursor":` + s + `,"seq":` + s +
		`,"kind":"commit","commit":{"rev":"r","operation":"` + op + `","collection":"` + coll + `","rkey":"` + rkey + `"`
	if withRecord {
		rec := map[string]any{"$type": coll, "text": "hi"}
		payload, err := cbor.Marshal(rec)
		require.NoError(t, err)
		frame += `,"cid":"bafytest","record_cbor":"` + base64.StdEncoding.EncodeToString(payload) + `"`
	}
	frame += `}}`
	return []byte(frame)
}

// liveIdentityFrame builds an extended-wire #identity frame.
func liveIdentityFrame(seq uint64, did, handle string) []byte {
	s := strconv.FormatUint(seq, 10)
	return []byte(`{"did":"` + did + `","time_us":1,"cursor":` + s + `,"seq":` + s +
		`,"kind":"identity","identity":{"did":"` + did + `","handle":"` + handle + `","seq":` + s + `,"time":"t"}}`)
}

// liveAccountFrame builds an extended-wire #account frame. A deleted account
// (active=false, status="deleted") doubles as a DID-level tombstone.
func liveAccountFrame(seq uint64, did string, active bool, status string) []byte {
	s := strconv.FormatUint(seq, 10)
	act := "true"
	if !active {
		act = "false"
	}
	return []byte(`{"did":"` + did + `","time_us":1,"cursor":` + s + `,"seq":` + s +
		`,"kind":"account","account":{"did":"` + did + `","active":` + act +
		`,"status":"` + status + `","seq":` + s + `,"time":"t"}}`)
}

func TestDecodeLiveFrameCommit(t *testing.T) {
	t.Parallel()
	ev, err := decodeLiveFrame(liveCommitFrame(t, 42, "did:plc:a", "create", "app.bsky.feed.post", "r1", true))
	require.NoError(t, err)
	require.Equal(t, KindCommit, ev.Kind)
	require.EqualValues(t, 42, ev.Seq)
	require.Equal(t, "did:plc:a", ev.DID)
	require.Equal(t, OpCreate, ev.Commit.Operation)
	require.Equal(t, "app.bsky.feed.post", ev.Commit.Collection)
	require.Equal(t, "hi", ev.Commit.Record["text"])
	require.NotEmpty(t, ev.Commit.RecordCBOR)
}

func TestDecodeLiveFrameDelete(t *testing.T) {
	t.Parallel()
	ev, err := decodeLiveFrame(liveCommitFrame(t, 7, "did:plc:a", "delete", "app.bsky.feed.post", "r1", false))
	require.NoError(t, err)
	require.Equal(t, OpDelete, ev.Commit.Operation)
	require.Nil(t, ev.Commit.Record)
	require.Nil(t, ev.Commit.RecordCBOR)
}

func TestDecodeLiveFrameCreateMissingCBOR(t *testing.T) {
	t.Parallel()
	// A create without record_cbor (i.e. not extended mode) must error.
	_, err := decodeLiveFrame(liveCommitFrame(t, 1, "did:plc:a", "create", "c", "r", false))
	require.ErrorContains(t, err, "record_cbor")
}

func TestDecodeLiveFrameAccountIdentitySync(t *testing.T) {
	t.Parallel()
	acct := []byte(`{"did":"did:plc:a","time_us":1,"seq":5,"kind":"account","account":{"did":"did:plc:a","active":false,"status":"deleted","seq":5,"time":"t"}}`)
	ev, err := decodeLiveFrame(acct)
	require.NoError(t, err)
	require.Equal(t, KindAccount, ev.Kind)
	require.False(t, ev.Account.Active)
	require.Equal(t, "deleted", ev.Account.Status)

	id := []byte(`{"did":"did:plc:a","time_us":1,"seq":6,"kind":"identity","identity":{"did":"did:plc:a","handle":"alice.test","seq":6,"time":"t"}}`)
	ev, err = decodeLiveFrame(id)
	require.NoError(t, err)
	require.Equal(t, KindIdentity, ev.Kind)
	require.Equal(t, "alice.test", ev.Identity.Handle)

	sync := []byte(`{"did":"did:plc:a","time_us":1,"seq":8,"kind":"sync","sync":{"did":"did:plc:a","rev":"rev1","seq":8,"time":"t"}}`)
	ev, err = decodeLiveFrame(sync)
	require.NoError(t, err)
	require.Equal(t, KindSync, ev.Kind)
	require.Equal(t, "rev1", ev.Sync.Rev)
}

func TestDecodeLiveFrameUnknownKindSkips(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"heartbeat", "segment_sealed", "segment_compacted", "future_thing"} {
		_, err := decodeLiveFrame([]byte(`{"kind":"` + kind + `","seq":1}`))
		require.ErrorIs(t, err, errSkipFrame, "kind %q must be skipped, not errored", kind)
	}
}

func TestDecodeLiveFrameErrorFrame(t *testing.T) {
	t.Parallel()
	_, err := decodeLiveFrame([]byte(`{"error":"FutureCursor","message":"cursor in the future"}`))
	require.ErrorContains(t, err, "FutureCursor")
	require.NotErrorIs(t, err, errSkipFrame)
}

func TestDecodeLiveFrameSeqFallbackToCursor(t *testing.T) {
	t.Parallel()
	// A frame with only cursor (no seq) still yields a usable seq.
	ev, err := decodeLiveFrame([]byte(`{"did":"did:plc:a","cursor":99,"kind":"account","account":{"did":"did:plc:a","active":true,"seq":99,"time":"t"}}`))
	require.NoError(t, err)
	require.EqualValues(t, 99, ev.Seq)
}
