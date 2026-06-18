package subscribe

import (
	"encoding/json"
	"net/url"
	"testing"

	"github.com/bluesky-social/jetstream/segment"
)

// FuzzParseQuery feeds arbitrary input to ParseQuery and asserts that
// the parser never panics, and on success Wants is also panic-free for
// arbitrary events. The corpus is seeded with both happy and adversarial
// inputs so the engine has an interesting starting point.
func FuzzParseQuery(f *testing.F) {
	seeds := []string{
		"",
		"wantedCollections=app.bsky.feed.post",
		"wantedCollections=app.bsky.graph.*&wantedCollections=app.bsky.feed.like",
		"wantedDids=did:plc:eygmaihciaxprqvxpfvl6flk",
		"maxMessageSizeBytes=1000000",
		"maxMessageSizeBytes=-1",
		"maxMessageSizeBytes=abc",
		"wantedCollections=not%20a%20valid%20nsid",
		"wantedDids=garbage",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, raw string) {
		q, err := url.ParseQuery(raw)
		if err != nil {
			// url.ParseQuery rejected the input — that's fine; fuzz the
			// parser by skipping (we're not fuzzing url.ParseQuery itself).
			return
		}
		filter, err := ParseQuery(q)
		if err != nil {
			return
		}
		// On a successful parse, Wants must never panic on an arbitrary event.
		evt := &segment.Event{
			Kind:       segment.KindCreate,
			DID:        "did:plc:fuzz",
			Collection: "app.bsky.feed.post",
		}
		_ = filter.Wants(evt)
		// Also exercise identity/account paths.
		evt.Kind = segment.KindIdentity
		_ = filter.Wants(evt)
		evt.Kind = segment.KindAccount
		_ = filter.Wants(evt)
	})
}

// FuzzParseUpdatePayload feeds arbitrary JSON payloads (possibly
// malformed) to the inner parse path and asserts panic-freedom.
func FuzzParseUpdatePayload(f *testing.F) {
	seeds := []string{
		`{}`,
		`{"wantedCollections":["app.bsky.feed.post"]}`,
		`{"wantedDids":["did:plc:eygmaihciaxprqvxpfvl6flk"]}`,
		`{"maxMessageSizeBytes":1000000}`,
		`{"maxMessageSizeBytes":-99}`,
		`{"wantedCollections":[]}`,
		`{"wantedCollections":["app.bsky.fo*"]}`,
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, raw string) {
		var p UpdatePayload
		if err := json.Unmarshal([]byte(raw), &p); err != nil {
			return
		}
		// Cap the input size so a fuzz-generated giant array doesn't
		// dominate runtime — ParseQuery/ParseUpdatePayload's own caps
		// reject these, but skipping early keeps the fuzz loop fast.
		if len(p.WantedCollections) > MaxWantedCollections+5 ||
			len(p.WantedDIDs) > MaxWantedDIDs+5 {
			return
		}
		filter, err := ParseUpdatePayload(p)
		if err != nil {
			return
		}
		evt := &segment.Event{
			Kind:       segment.KindCreate,
			DID:        "did:plc:fuzz",
			Collection: "app.bsky.feed.post",
		}
		_ = filter.Wants(evt)
	})
}
