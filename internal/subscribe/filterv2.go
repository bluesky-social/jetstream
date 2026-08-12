// Package subscribe — filterv2.go owns the network.bsky.jetstream.subscribeEvents
// (v2) subscriber filter: the kinds/dids/collections query parameters and
// the Wants(evt) predicate.
//
// The three axes are independent predicates ANDed together (design note
// specs/notes/2026-08-10-subscribe-v2-proposal-0015-design.md §5):
//
//	deliver(evt) =
//	      (kinds unset       OR evt.kind ∈ kinds)
//	  AND (dids unset        OR evt.did ∈ dids)
//	  AND (evt.kind ≠ commit OR collections unset
//	                         OR matches(evt.collection, collections))
//
// Unset means match-all on that axis, so no parameters is "one big
// stream". The collections axis constrains only commit events — the only
// kind that has a collection — and never drops other kinds: excluding
// account/identity/sync is the kinds axis's job. This replaces v1's
// implicit "collections ⇒ plus everyone's account/identity events"
// coupling with explicit composition (kinds=commit&collections=X for a
// commits-only collection stream).
//
// Unlike the v1 filter, validation here is crash-loud (pre-upgrade HTTP
// 400): unknown kinds values, a collections filter that could never apply
// (kinds set and excluding commit), and the legacy wanted* parameter
// names are all rejected with messages naming the fix. There is no
// mid-stream filter update — v2 is server-push only.
package subscribe

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
)

// kindMask is a bitmask over the four subscriber-selectable event-kind
// classes. The classes are exactly the message union's $type fragments.
type kindMask uint8

const (
	kindMaskCommit kindMask = 1 << iota
	kindMaskIdentity
	kindMaskAccount
	kindMaskSync

	// kindMaskAll is the unset ("match-all") representation.
	kindMaskAll = kindMaskCommit | kindMaskIdentity | kindMaskAccount | kindMaskSync
)

// kindClassOf maps a segment kind to its wire kind class. Returns 0 for
// kinds this build cannot classify (a future segment.Kind): such events
// pass an unset kinds axis (match-all keeps its meaning) and are dropped
// by any explicit selection (they cannot have been selected).
func kindClassOf(k segment.Kind) kindMask {
	switch {
	case k.IsCommit():
		return kindMaskCommit
	case k == segment.KindIdentity:
		return kindMaskIdentity
	case k == segment.KindAccount:
		return kindMaskAccount
	case k == segment.KindSync:
		return kindMaskSync
	}
	return 0
}

// FilterV2 is a v2 connection's immutable filter. Construct via
// ParseQueryV2 only — the zero value's kinds mask would drop everything.
// A nil *FilterV2 is defensively match-all, mirroring *Filter.
type FilterV2 struct {
	// kindsSet distinguishes an explicit kinds selection from an unset
	// axis. The distinction matters for a future kind class this build
	// cannot name (kindClassOf returns 0): unset means match-all and
	// passes it, while an explicit selection — even one enumerating
	// every kind this build knows — drops it, because the client could
	// not have selected a kind that didn't exist when it connected.
	kindsSet            bool
	kinds               kindMask
	dids                map[string]struct{} // nil = match-all
	collections         *wantedCollections  // nil = match-all
	maxMessageSizeBytes uint32              // 0 = no cap
}

// MaxMessageSizeBytes returns the per-frame size cap, or 0 for "no cap".
// Enforced by the handler on the whole uncompressed frame (envelope
// included) post-encode.
func (f *FilterV2) MaxMessageSizeBytes() uint32 {
	if f == nil {
		return 0
	}
	return f.maxMessageSizeBytes
}

// Wants reports whether the subscriber should receive evt: the
// conjunction of the three independently-evaluated axis predicates.
func (f *FilterV2) Wants(evt *segment.Event) bool {
	if f == nil {
		return true
	}

	// Kinds axis. Unset matches everything, including a future kind
	// class this build can't name (class 0); an explicit selection drops
	// class-0 kinds (see kindsSet).
	if f.kindsSet && f.kinds&kindClassOf(evt.Kind) == 0 {
		return false
	}

	// DIDs axis: applies to every event kind.
	if f.dids != nil {
		if _, ok := f.dids[evt.DID]; !ok {
			return false
		}
	}

	// Collections axis: constrains commit events only; other kinds are
	// unaffected by construction (excluding them is the kinds axis's
	// job). A commit with an empty collection bypasses the filter —
	// never silently drop data the filter can't classify (the ingest
	// gate makes this unreachable for spec-clean archives; this is
	// defense-in-depth, same policy as v1).
	if f.collections == nil || !evt.Kind.IsCommit() || evt.Collection == "" {
		return true
	}
	return f.collections.matches(evt.Collection)
}

// matches reports whether collection matches any full path or prefix.
func (wc *wantedCollections) matches(collection string) bool {
	if _, ok := wc.fullPaths[collection]; ok {
		return true
	}
	for _, prefix := range wc.prefixes {
		if len(collection) >= len(prefix) && collection[:len(prefix)] == prefix {
			return true
		}
	}
	return false
}

// ParseQueryV2 turns a network.bsky.jetstream.subscribeEvents query string into
// a validated *FilterV2. Any validation failure returns an
// ErrInvalidOptions-wrapped error, which the handler maps to a
// pre-upgrade HTTP 400 InvalidRequest envelope.
func ParseQueryV2(q url.Values) (*FilterV2, error) {
	// The legacy v1 parameter names are named 400s, not silently ignored:
	// a migrating consumer whose filter param were dropped would receive
	// the full firehose without noticing — the classic silent-fallback
	// trap. Other unknown parameters stay ignored per XRPC convention;
	// only these carry a tombstone.
	if _, ok := q["wantedDids"]; ok {
		return nil, fmt.Errorf("%w: wantedDids is the legacy /subscribe parameter; use dids", ErrInvalidOptions)
	}
	if _, ok := q["wantedCollections"]; ok {
		return nil, fmt.Errorf("%w: wantedCollections is the legacy /subscribe parameter; use collections", ErrInvalidOptions)
	}
	if _, ok := q["requireHello"]; ok {
		return nil, fmt.Errorf("%w: requireHello is not supported on this endpoint; it is server-push only (subscriber-sourced messages were removed)", ErrInvalidOptions)
	}

	if n := len(q["kinds"]); n > 4 {
		return nil, fmt.Errorf("%w: too many kinds values: %d > 4", ErrInvalidOptions, n)
	}
	kinds, kindsSet, err := parseKinds(q["kinds"])
	if err != nil {
		return nil, err
	}

	// The lexicon declares maxLength on the dids and collections arrays
	// (10000 and 100); enforce it on the RAW repeated-param count, before
	// the shared v1 parsers' dedupe-then-cap leniency. A client generated
	// from the lexicon never sends more, and v2 has no legacy consumers
	// whose sloppy duplicate lists need forgiving.
	if n := len(q["dids"]); n > MaxWantedDIDs {
		return nil, fmt.Errorf("%w: too many dids values: %d > %d", ErrInvalidOptions, n, MaxWantedDIDs)
	}
	if n := len(q["collections"]); n > MaxWantedCollections {
		return nil, fmt.Errorf("%w: too many collections values: %d > %d", ErrInvalidOptions, n, MaxWantedCollections)
	}

	dids, err := parseWantedDIDs(q["dids"])
	if err != nil {
		return nil, err
	}

	// Unlike v1 (which is deliberately lax for wire parity), a wildcard's
	// head must be a valid NSID prefix — the same probe idea as
	// planSnapshot's classifyCollectionPattern: appending a known-valid
	// name label reuses atmos.ParseNSID as the single source of truth for
	// NSID grammar. A malformed head like "not_an_nsid.*" would otherwise
	// upgrade fine and silently match nothing — the lexicon promises a 400
	// instead. The probe label is the shortest valid one ("x") so a long
	// head near the 317-byte NSID cap isn't falsely rejected by the probe's
	// own added length.
	for _, raw := range q["collections"] {
		if head, ok := strings.CutSuffix(raw, ".*"); ok {
			if _, perr := atmos.ParseNSID(head + ".x"); perr != nil {
				return nil, fmt.Errorf("%w: invalid collection wildcard: %s", ErrInvalidOptions, raw)
			}
		}
	}
	collections, err := parseWantedCollections(q["collections"])
	if err != nil {
		return nil, err
	}

	// A collections filter that could never apply is a client bug; tell
	// the developer immediately rather than serve a stream they
	// misunderstand. (kinds unset composes fine: commits are in scope.)
	if collections != nil && kindsSet && kinds&kindMaskCommit == 0 {
		return nil, fmt.Errorf("%w: collections filter can never apply: kinds excludes commit", ErrInvalidOptions)
	}
	maxMessageSizeBytes, err := parseMaxMessageSizeV2(q)
	if err != nil {
		return nil, err
	}

	return &FilterV2{
		kindsSet:            kindsSet,
		kinds:               kinds,
		dids:                dids,
		collections:         collections,
		maxMessageSizeBytes: maxMessageSizeBytes,
	}, nil
}

func parseMaxMessageSizeV2(q url.Values) (uint32, error) {
	values, ok := q["maxMessageSizeBytes"]
	if !ok {
		return 0, nil
	}
	if len(values) != 1 || values[0] == "" {
		return 0, fmt.Errorf("%w: maxMessageSizeBytes must be one unsigned 32-bit integer", ErrInvalidOptions)
	}
	n, err := strconv.ParseUint(values[0], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid maxMessageSizeBytes %q", ErrInvalidOptions, values[0])
	}
	return uint32(n), nil
}

// parseKinds parses repeated kinds values into a mask. No values means
// match-all (set=false). Values are the lexicon enum — anything else is
// a 400, per the lexicon: rejected "rather than silently never matching".
func parseKinds(values []string) (mask kindMask, set bool, err error) {
	if len(values) == 0 {
		return kindMaskAll, false, nil
	}
	// The lexicon declares maxLength: 4 (the enum has exactly 4 values);
	// enforce it on the raw repeated-param count like dids/collections.
	if len(values) > 4 {
		return 0, false, fmt.Errorf("%w: too many kinds values: %d > 4", ErrInvalidOptions, len(values))
	}
	for _, v := range values {
		switch v {
		case "commit":
			mask |= kindMaskCommit
		case "identity":
			mask |= kindMaskIdentity
		case "account":
			mask |= kindMaskAccount
		case "sync":
			mask |= kindMaskSync
		default:
			return 0, false, fmt.Errorf("%w: unknown kind %q (valid: commit, identity, account, sync)", ErrInvalidOptions, v)
		}
	}
	return mask, true, nil
}
