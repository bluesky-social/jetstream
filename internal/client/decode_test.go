package client

import (
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/require"
)

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
