package subscribe

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/bluesky-social/jetstream-v2/segment"
)

// FuzzEncode asserts that Encode either produces valid JSON or returns
// an error — never panics, never emits invalid JSON. The fuzzer can't
// produce the inner CBOR payload meaningfully (it's a tightly framed
// structure), but it can stress every other field plus the kind dispatch.
func FuzzEncode(f *testing.F) {
	// Seed with a few hand-crafted shapes.
	seeds := []struct {
		kind                       uint8
		did, collection, rkey, rev string
		payload                    []byte
	}{
		{uint8(segment.KindCreate), "did:plc:a", "app.bsky.feed.like", "rk1", "rev1", []byte{0xa0}}, // empty CBOR map
		{uint8(segment.KindDelete), "did:plc:a", "app.bsky.feed.like", "rk1", "rev1", nil},
		{uint8(segment.KindIdentity), "did:plc:a", "", "", "", []byte{0xa0}},
		{uint8(segment.KindSync), "did:plc:a", "", "", "", nil},
		{99, "did:plc:a", "", "", "", nil}, // unknown kind
	}
	for _, s := range seeds {
		f.Add(s.kind, s.did, s.collection, s.rkey, s.rev, s.payload)
	}

	f.Fuzz(func(t *testing.T, kind uint8, did, collection, rkey, rev string, payload []byte) {
		evt := &segment.Event{
			Kind:       segment.Kind(kind),
			DID:        did,
			Collection: collection,
			Rkey:       rkey,
			Rev:        rev,
			Payload:    payload,
			IndexedAt:  1,
		}

		out, err := Encode(evt)
		if err != nil {
			if errors.Is(err, errSkipEvent) {
				if out != nil {
					t.Fatalf("errSkipEvent must not return body, got %d bytes", len(out))
				}
				return
			}
			// Any other error path is fine; just must not have panicked.
			return
		}

		// On success, the output must be valid JSON.
		var v any
		if err := json.Unmarshal(out, &v); err != nil {
			t.Fatalf("Encode produced invalid JSON: %v\nbytes: %s", err, out)
		}
	})
}
