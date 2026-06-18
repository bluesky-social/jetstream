// Package subscribe — filter.go owns the v1-compatible subscriber filter:
// query-string and options_update parsing, plus the Wants(evt) predicate.
//
// V1 wire compatibility is the point. Where v2's house style ("crash loud,
// no silent fallbacks" — CLAUDE.md) would diverge from the v1 README's
// stated contract, this file deliberately matches v1 and documents the
// rationale inline. Search for "V1 PARITY" to find every such spot.
package subscribe

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
)

// Caps from the v1 README (see https://github.com/bluesky-social/jetstream).
const (
	// MaxWantedCollections is the hard cap on collection patterns per
	// subscriber. Combined with the per-subscriber atomic.Pointer[Filter]
	// in handler.go, this bounds memory growth from a hostile client.
	MaxWantedCollections = 100

	// MaxWantedDIDs is the hard cap on DID filter entries per subscriber.
	MaxWantedDIDs = 10_000

	// MaxSubscriberMessageBytes caps the size of a SubscriberSourcedMessage
	// frame. V1 PARITY: v1 documents this as 10MB. Decimal, not 10 MiB.
	MaxSubscriberMessageBytes = 10_000_000
)

// Filter is a per-connection set of subscriber preferences. A nil
// *Filter is treated as match-all (defensive — Wants returns true);
// a zero-value Filter (returned by ParseQuery on empty input) also
// matches every event with no size cap. Filters are treated as
// immutable once published — handler.go swaps them via atomic.Pointer
// rather than mutating in place.
type Filter struct {
	wantedDIDs          map[string]struct{} // nil = match-all
	wantedCollections   *wantedCollections  // nil = match-all
	maxMessageSizeBytes uint32              // 0 = no cap
}

// wantedCollections splits the user's preferences into the two shapes
// we'll dispatch on at match time: full-path map lookup (fast,
// expected-common case) and prefix scan (rare, slower).
type wantedCollections struct {
	fullPaths map[string]struct{}
	prefixes  []string // each entry ends in "." (e.g. "app.bsky.graph.")
}

// MaxMessageSizeBytes returns the per-frame size cap, or 0 for "no cap".
// Lives outside Wants because the predicate doesn't have access to the
// encoded byte length; the handler enforces post-encode.
func (f *Filter) MaxMessageSizeBytes() uint32 {
	if f == nil {
		return 0
	}
	return f.maxMessageSizeBytes
}

// ParseQuery turns a /subscribe query string into a validated *Filter.
// Returns an ErrInvalidOptions-wrapped error on any validation failure
// so callers can errors.Is and the handler can return HTTP 400 with a
// useful body. Empty input yields a match-all filter.
func ParseQuery(q url.Values) (*Filter, error) {
	wantedCol, err := parseWantedCollections(q["wantedCollections"])
	if err != nil {
		return nil, err
	}

	wantedDIDs, err := parseWantedDIDs(q["wantedDids"])
	if err != nil {
		return nil, err
	}

	return &Filter{
		wantedDIDs:          wantedDIDs,
		wantedCollections:   wantedCol,
		maxMessageSizeBytes: parseMaxMsgSizeQuery(q.Get("maxMessageSizeBytes")),
	}, nil
}

func parseWantedCollections(values []string) (*wantedCollections, error) {
	if len(values) == 0 {
		return nil, nil
	}

	// Hard upper bound to keep a hostile client from making us allocate
	// arbitrarily large dedupe structures before the v1-compat post-dedupe
	// cap below kicks in. Anything well above MaxWantedCollections is a
	// signal of abuse, not a sloppy duplicate list.
	if len(values) > MaxWantedCollections*100 {
		return nil, fmt.Errorf("%w: too many wanted collections: %d",
			ErrInvalidOptions, len(values))
	}

	wc := &wantedCollections{
		fullPaths: make(map[string]struct{}),
	}
	seenPrefixes := make(map[string]struct{})
	for _, raw := range values {
		if strings.HasSuffix(raw, ".*") {
			// V1 PARITY: the prefix form is accepted with no further
			// validation. The v1 README says "The prefix before the .*
			// must pass NSID validation" but v1's actual code (server/
			// subscriber.go) does no such validation — it just trims
			// the "*" and stores the prefix. Matching that lets
			// documented patterns like "app.bsky.*" (only 2 segments
			// before the wildcard, would fail strict NSID validation)
			// work as v1 clients expect. Patterns like "app.bsky.fo*"
			// (no "." before the "*") fall through to the NSID branch
			// below and are rejected, also matching v1.
			head := strings.TrimSuffix(raw, "*")
			if _, dup := seenPrefixes[head]; dup {
				continue
			}
			seenPrefixes[head] = struct{}{}
			wc.prefixes = append(wc.prefixes, head)
			if len(wc.fullPaths)+len(wc.prefixes) > MaxWantedCollections {
				return nil, fmt.Errorf("%w: too many wanted collections: > %d",
					ErrInvalidOptions, MaxWantedCollections)
			}
			continue
		}
		nsid, err := atmos.ParseNSID(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid collection: %s",
				ErrInvalidOptions, raw)
		}
		wc.fullPaths[string(nsid)] = struct{}{}
		if len(wc.fullPaths)+len(wc.prefixes) > MaxWantedCollections {
			return nil, fmt.Errorf("%w: too many wanted collections: > %d",
				ErrInvalidOptions, MaxWantedCollections)
		}
	}
	return wc, nil
}

func parseWantedDIDs(values []string) (map[string]struct{}, error) {
	if len(values) == 0 {
		return nil, nil
	}

	// V1 PARITY: v1 dedupes via a map first and only then checks the
	// 10_000 cap. A client that sends a sloppy list with duplicates is
	// accepted as long as the unique count fits. We mirror that.
	//
	// Cheap upper bound to bound dedupe-map growth from a hostile
	// client; anything multiples above MaxWantedDIDs is abuse, not a
	// sloppy list.
	if len(values) > MaxWantedDIDs*100 {
		return nil, fmt.Errorf("%w: too many wanted DIDs: %d",
			ErrInvalidOptions, len(values))
	}

	out := make(map[string]struct{}, len(values))
	for _, raw := range values {
		did, err := atmos.ParseDID(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid DID: %s",
				ErrInvalidOptions, raw)
		}
		out[string(did)] = struct{}{}
	}
	if len(out) > MaxWantedDIDs {
		return nil, fmt.Errorf("%w: too many wanted DIDs: %d > %d",
			ErrInvalidOptions, len(out), MaxWantedDIDs)
	}
	return out, nil
}

// parseMaxMsgSizeQuery parses the maxMessageSizeBytes query value.
//
// V1 PARITY (deliberate): empty, malformed, negative, and overflowing
// values silently resolve to 0 ("no cap"). This matches jetstream v1's
// ParseMaxMessageSizeBytes behavior. The v1 README documents this as
// the contract:
//
//	"Zero means no limit, negative values are treated as zero.
//	 (Default '0' or empty = no maximum size)"
//
// CLAUDE.md prefers crashing loud over silent fallbacks, but the v1
// wire contract IS the contract: existing clients send "0", "" and
// (occasionally) garbage and rely on this exact coercion. Changing it
// would silently break clients that depend on the v1 README's stated
// behavior. TestParseMaxMsgSize_V1Compat locks this down — touch with
// care.
func parseMaxMsgSizeQuery(s string) uint32 {
	if s == "" {
		return 0
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0 // V1 PARITY: silent coerce
	}
	if n < 0 {
		return 0 // V1 PARITY: documented behavior
	}
	if uint64(n) > uint64(^uint32(0)) {
		return 0 // V1 PARITY: overflow coerces to 0 too
	}
	return uint32(n)
}

// parseRequireHello returns true iff the requireHello query parameter is
// exactly the literal string "true". V1 PARITY: jetstream v1 uses
//
//	c.Request().URL.Query().Get("requireHello") == "true"
//
// so any other value — including "True", "1", or "yes" — is treated as
// false. Existing v1 clients send "true" or omit the param; we must not
// "be liberal in what we accept" or we change wire semantics for them.
// TestParseRequireHello locks this down — touch with care.
func parseRequireHello(values url.Values) bool {
	return values.Get("requireHello") == "true"
}

// SubscriberSourcedMessage is the envelope for any client→server message
// over the websocket. v1 README §"Subscriber Sourced messages".
type SubscriberSourcedMessage struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// UpdatePayload is the body of an "options_update" message. V1 PARITY:
// MaxMessageSizeBytes is encoded as a JSON integer; clampMaxMsgSize
// silently coerces negative values to 0.
type UpdatePayload struct {
	WantedCollections   []string `json:"wantedCollections"`
	WantedDIDs          []string `json:"wantedDids"`
	MaxMessageSizeBytes int      `json:"maxMessageSizeBytes"`
}

// SubMessageTypeOptionsUpdate is the only Type value the handler acts on
// today. v1 logs and ignores unknown Types; we match that.
const SubMessageTypeOptionsUpdate = "options_update"

// ParseUpdatePayload validates a JSON UpdatePayload and returns a fresh
// *Filter. Same validation rules as ParseQuery; produces the same
// ErrInvalidOptions-wrapped errors so the handler's terminate-on-error
// path is symmetric across init-time and mid-stream parsing.
func ParseUpdatePayload(p UpdatePayload) (*Filter, error) {
	wantedCol, err := parseWantedCollections(p.WantedCollections)
	if err != nil {
		return nil, err
	}
	wantedDIDs, err := parseWantedDIDs(p.WantedDIDs)
	if err != nil {
		return nil, err
	}
	return &Filter{
		wantedDIDs:          wantedDIDs,
		wantedCollections:   wantedCol,
		maxMessageSizeBytes: clampMaxMsgSize(p.MaxMessageSizeBytes),
	}, nil
}

// clampMaxMsgSize coerces a JSON-decoded MaxMessageSizeBytes value to uint32.
//
// V1 PARITY (deliberate): negative values silently resolve to 0 ("no cap").
// This matches jetstream v1's documented behavior. The v1 README states:
// "Zero means no limit, negative values are treated as zero."
//
// CLAUDE.md prefers crashing loud over silent fallbacks, but the v1
// wire contract IS the contract. TestParseMaxMsgSize_V1Compat locks this
// down — touch with care.
func clampMaxMsgSize(n int) uint32 {
	if n < 0 {
		return 0
	}
	return uint32(n)
}

// Wants reports whether the subscriber should receive evt. The rules
// (V1 PARITY):
//
//   - A nil *Filter or an empty Filter ("match-all" from ParseQuery on
//     no query params) matches every event.
//   - wantedDIDs applies to all event kinds. If non-empty and evt.DID
//     is not in the set, drop.
//   - wantedCollections applies ONLY to commit events
//     (KindCreate / KindUpdate / KindDelete / KindCreateResync). Identity and Account
//     events bypass the collection filter — v1 README:
//     "Regardless of desired collections, all subscribers receive
//     Account and Identity events." Sync events are filtered upstream
//     by encoder.go (errSkipEvent).
//
// Wants does NOT enforce maxMessageSizeBytes; the handler enforces
// against the encoded byte length post-Encode.
func (f *Filter) Wants(evt *segment.Event) bool {
	if f == nil {
		return true
	}
	if f.wantedDIDs != nil {
		if _, ok := f.wantedDIDs[evt.DID]; !ok {
			return false
		}
	}
	if f.wantedCollections == nil {
		return true
	}
	// Collection filter applies only to commit events.
	if !isCommitKind(evt.Kind) {
		return true
	}
	// V1 PARITY: a commit with an empty collection bypasses the filter.
	// v1's WantsCollection short-circuits on collection == "" to
	// preserve any commit lacking a collection (e.g., a future kind or
	// a malformed source) rather than silently dropping it.
	if evt.Collection == "" {
		return true
	}
	if _, ok := f.wantedCollections.fullPaths[evt.Collection]; ok {
		return true
	}
	for _, prefix := range f.wantedCollections.prefixes {
		if strings.HasPrefix(evt.Collection, prefix) {
			return true
		}
	}
	return false
}

func isCommitKind(k segment.Kind) bool {
	return k.IsCommit()
}
