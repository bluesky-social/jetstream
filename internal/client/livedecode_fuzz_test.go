package client

import (
	"encoding/base64"
	"testing"

	"github.com/jcalabro/atmos/cbor"
)

// FuzzDecodeLiveFrame asserts the live-frame decoder never panics on arbitrary
// bytes: live frames are untrusted server input (AGENTS.md treats all upstream
// data as hostile). Any input must yield an event, errSkipFrame, or an error —
// never a crash.
func FuzzDecodeLiveFrame(f *testing.F) {
	// Seed with valid and adversarial shapes.
	rec, _ := cbor.Marshal(map[string]any{"$type": "app.bsky.feed.post", "text": "hi"})
	b64 := base64.StdEncoding.EncodeToString(rec)
	f.Add([]byte(`{"did":"did:plc:a","seq":1,"kind":"commit","commit":{"operation":"create","collection":"c","rkey":"r","record_cbor":"` + b64 + `"}}`))
	f.Add([]byte(`{"kind":"heartbeat"}`))
	f.Add([]byte(`{"kind":"commit","commit":{"operation":"create","record_cbor":"not-base64!!"}}`))
	f.Add([]byte(`{"kind":"commit","commit":{"operation":"create","record_cbor":""}}`))
	f.Add([]byte(`{"error":"FutureCursor","message":"x"}`))
	f.Add([]byte(`{"kind":"account"}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`not json`))
	f.Add([]byte(``))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must not panic. The return values are all acceptable; we only assert
		// the absence of a crash and basic invariants on success.
		ev, err := decodeLiveFrame(data)
		if err != nil {
			return
		}
		// On success, Kind must be one of the known kinds and the matching
		// payload pointer set (decodeLiveFrame returns errSkipFrame otherwise).
		switch ev.Kind {
		case KindCommit:
			if ev.Commit == nil {
				t.Fatalf("commit event with nil Commit: %q", data)
			}
		case KindIdentity:
			if ev.Identity == nil {
				t.Fatalf("identity event with nil Identity: %q", data)
			}
		case KindAccount:
			if ev.Account == nil {
				t.Fatalf("account event with nil Account: %q", data)
			}
		case KindSync:
			if ev.Sync == nil {
				t.Fatalf("sync event with nil Sync: %q", data)
			}
		default:
			t.Fatalf("success with unexpected kind %q: %q", ev.Kind, data)
		}
	})
}
