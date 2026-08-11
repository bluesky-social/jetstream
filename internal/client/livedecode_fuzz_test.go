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
	b64 := base64.RawStdEncoding.EncodeToString(rec)
	const t0 = "1970-01-01T00:00:00.000001Z"
	f.Add([]byte(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#commit","seq":1,"did":"did:plc:a","time":"` + t0 + `","rev":"r","operation":"create","collection":"c","rkey":"r","recordCbor":{"$bytes":"` + b64 + `"}}}`))
	f.Add([]byte(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#info","name":"OutdatedCursor"}}`))
	f.Add([]byte(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#futureKind","seq":9}}`))
	f.Add([]byte(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#commit","operation":"create","recordCbor":{"$bytes":"!!"}}}`))
	f.Add([]byte(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#sync","seq":-1,"did":"d","time":"` + t0 + `"}}`))
	f.Add([]byte(`{"$type":"error","error":"FutureCursor","message":"x"}`))
	f.Add([]byte(`{"$type":"error"}`))
	f.Add([]byte(`{"$type":"message"}`))
	f.Add([]byte(`{"$type":"snapshot"}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`not json`))
	f.Add([]byte(``))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must not panic. The return values are all acceptable; we only assert
		// the absence of a crash and basic invariants on success.
		ev, info, err := decodeLiveFrame(data, recordDecodeMode{})
		if err != nil {
			// An info advisory only rides on errSkipFrame.
			if info != nil && err != errSkipFrame { //nolint:errorlint // identity check is the contract
				t.Fatalf("info returned with non-skip error %v: %q", err, data)
			}
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
