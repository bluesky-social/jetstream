# Subscribe v1-Compatible Filtering Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port jetstream v1's `/subscribe` filtering contract — `wantedCollections`, `wantedDids`, `maxMessageSizeBytes`, and the `options_update` subscriber-sourced-message — to jetstream v2's existing websocket handler, with strict v1 wire-compatibility.

**Architecture:** A new `internal/subscribe/filter.go` owns a `*Filter` value type, query/payload parsers, and a `Wants(evt) bool` predicate. `internal/subscribe/handler.go` parses query params on connect, holds an `atomic.Pointer[Filter]` per connection, parses subscriber-sourced messages in the reader goroutine, and applies the filter (plus post-encode max-size enforcement) in the writer loop. The broadcaster stays a protocol-agnostic fan-out.

**Tech Stack:** Go stdlib (`net/http`, `net/url`, `encoding/json`, `sync/atomic`), `github.com/coder/websocket`, `github.com/jcalabro/atmos` (NSID/DID syntax helpers), `github.com/prometheus/client_golang`, `github.com/stretchr/testify` for tests, `gotestsum` (via `just test`).

**Spec:** `docs/superpowers/specs/2026-05-27-subscribe-v1-filtering-design.md`

---

## Task overview

1. `errors.go` — add `ErrInvalidOptions`.
2. `filter.go` — `Filter` type, `ParseQuery`, NSID/DID/cap validation, prefix handling.
3. `filter.go` — `ParseUpdatePayload` + `UpdatePayload` + `SubscriberSourcedMessage` types.
4. `filter.go` — `Wants` predicate (DID + collection + kind rules).
5. `filter.go` — `maxMessageSizeBytes` parsing helpers + v1-parity tests.
6. `filter_fuzz_test.go` — fuzz `ParseQuery` and `ParseUpdatePayload`.
7. `metrics.go` — three new counters + one labeled, plus `metrics_test.go` extension.
8. `handler.go` — parse on connect, per-conn `atomic.Pointer[Filter]`, writer-loop filter + max-size; integration tests.
9. `handler.go` — reader goroutine parses `SubscriberSourcedMessage`, applies `options_update`, terminates on error; integration tests.
10. `doc.go` — rewrite to drop the "filtering deferred" disclaimer and document v1-compat behaviors.

Run `just lint test` after each task. Commit at the end of each task.

---

## Task 1: Add `ErrInvalidOptions`

**Files:**
- Modify: `internal/subscribe/errors.go`

- [ ] **Step 1.1: Add the exported error**

Replace the contents of `internal/subscribe/errors.go` with:

```go
package subscribe

import "errors"

// errSkipEvent signals that the encoder intentionally produced no
// frame for this event (e.g. #sync events, which Jetstream v1 never
// emitted on the wire). The handler's writer loop treats this as
// "advance the channel; keep the connection alive."
//
//nolint:unused
var errSkipEvent = errors.New("subscribe: skip event")

// ErrInvalidOptions wraps validation failures from ParseQuery and
// ParseUpdatePayload. Callers (the handler, plus tests outside this
// package) errors.Is against it to distinguish bad-input failures from
// other errors.
var ErrInvalidOptions = errors.New("subscribe: invalid options")
```

- [ ] **Step 1.2: Build to confirm no compile errors**

Run: `just lint`
Expected: PASS (no lint or build errors).

- [ ] **Step 1.3: Commit**

```bash
git add internal/subscribe/errors.go
git commit -m "subscribe: add ErrInvalidOptions sentinel"
```

---

## Task 2: `Filter` type and `ParseQuery`

This is the largest task. We do strict TDD: tests first, then implementation, then green.

**Files:**
- Create: `internal/subscribe/filter.go`
- Create: `internal/subscribe/filter_test.go`

- [ ] **Step 2.1: Write the failing tests for `ParseQuery`**

Create `internal/subscribe/filter_test.go` with:

```go
package subscribe

import (
	"errors"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseQuery_Empty_MatchAll(t *testing.T) {
	t.Parallel()
	f, err := ParseQuery(url.Values{})
	require.NoError(t, err)
	require.NotNil(t, f)
	// Match-all means both filters are nil and max-size is 0.
	require.Nil(t, f.wantedDIDs)
	require.Nil(t, f.wantedCollections)
	require.Equal(t, uint32(0), f.MaxMessageSizeBytes())
}

func TestParseQuery_SingleCollection(t *testing.T) {
	t.Parallel()
	q := url.Values{"wantedCollections": []string{"app.bsky.feed.post"}}
	f, err := ParseQuery(q)
	require.NoError(t, err)
	require.NotNil(t, f.wantedCollections)
	_, ok := f.wantedCollections.fullPaths["app.bsky.feed.post"]
	require.True(t, ok)
	require.Empty(t, f.wantedCollections.prefixes)
}

func TestParseQuery_PrefixCollection(t *testing.T) {
	t.Parallel()
	q := url.Values{"wantedCollections": []string{"app.bsky.graph.*"}}
	f, err := ParseQuery(q)
	require.NoError(t, err)
	require.NotNil(t, f.wantedCollections)
	require.Empty(t, f.wantedCollections.fullPaths)
	require.Equal(t, []string{"app.bsky.graph."}, f.wantedCollections.prefixes)
}

func TestParseQuery_MixedFullAndPrefix(t *testing.T) {
	t.Parallel()
	q := url.Values{"wantedCollections": []string{
		"app.bsky.feed.post",
		"app.bsky.graph.*",
		"app.bsky.feed.like",
	}}
	f, err := ParseQuery(q)
	require.NoError(t, err)
	require.Len(t, f.wantedCollections.fullPaths, 2)
	require.Equal(t, []string{"app.bsky.graph."}, f.wantedCollections.prefixes)
}

func TestParseQuery_DIDs(t *testing.T) {
	t.Parallel()
	q := url.Values{"wantedDids": []string{
		"did:plc:eygmaihciaxprqvxpfvl6flk",
		"did:plc:rfov6bpyztcnedeyyzgfq42k",
	}}
	f, err := ParseQuery(q)
	require.NoError(t, err)
	require.Len(t, f.wantedDIDs, 2)
	_, ok := f.wantedDIDs["did:plc:eygmaihciaxprqvxpfvl6flk"]
	require.True(t, ok)
}

func TestParseQuery_BadCollection(t *testing.T) {
	t.Parallel()
	q := url.Values{"wantedCollections": []string{"not a valid nsid"}}
	_, err := ParseQuery(q)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidOptions))
}

func TestParseQuery_PrefixNotAtNSIDBoundary(t *testing.T) {
	t.Parallel()
	// "app.bsky.fo*" is not a valid prefix — the suffix before .* must
	// itself parse as a valid NSID.
	q := url.Values{"wantedCollections": []string{"app.bsky.fo*"}}
	_, err := ParseQuery(q)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidOptions))
}

func TestParseQuery_BadDID(t *testing.T) {
	t.Parallel()
	q := url.Values{"wantedDids": []string{"not-a-did"}}
	_, err := ParseQuery(q)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidOptions))
}

func TestParseQuery_TooManyCollections(t *testing.T) {
	t.Parallel()
	cols := make([]string, MaxWantedCollections+1)
	for i := range cols {
		// Generate a unique valid NSID per slot.
		cols[i] = "app.bsky.feed.post" // duplicates are fine; v1 counts entries before dedupe
	}
	q := url.Values{"wantedCollections": cols}
	_, err := ParseQuery(q)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidOptions))
	require.True(t, strings.Contains(err.Error(), "too many"))
}

func TestParseQuery_TooManyDIDs(t *testing.T) {
	t.Parallel()
	dids := make([]string, MaxWantedDIDs+1)
	for i := range dids {
		dids[i] = "did:plc:eygmaihciaxprqvxpfvl6flk"
	}
	q := url.Values{"wantedDids": dids}
	_, err := ParseQuery(q)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidOptions))
	require.True(t, strings.Contains(err.Error(), "too many"))
}
```

- [ ] **Step 2.2: Run the tests and confirm they fail to compile**

Run: `just test ./internal/subscribe`
Expected: build failure — `ParseQuery`, `MaxWantedCollections`, `MaxWantedDIDs`, etc. not defined.

- [ ] **Step 2.3: Write the implementation**

Create `internal/subscribe/filter.go` with:

```go
// Package subscribe — filter.go owns the v1-compatible subscriber filter:
// query-string and options_update parsing, plus the Wants(evt) predicate.
//
// V1 wire compatibility is the point. Where v2's house style ("crash loud,
// no silent fallbacks" — CLAUDE.md) would diverge from the v1 README's
// stated contract, this file deliberately matches v1 and documents the
// rationale inline. Search for "V1 PARITY" to find every such spot.
package subscribe

import (
	"fmt"
	"net/url"
	"strings"

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
// *Filter means "match nothing"; a zero-value (returned by an empty
// ParseQuery) means "match all events with no size cap." Filters are
// treated as immutable once published — handler.go swaps them via
// atomic.Pointer rather than mutating in place.
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

	maxSize, err := parseMaxMsgSizeQuery(q.Get("maxMessageSizeBytes"))
	if err != nil {
		return nil, err
	}

	return &Filter{
		wantedDIDs:          wantedDIDs,
		wantedCollections:   wantedCol,
		maxMessageSizeBytes: maxSize,
	}, nil
}

func parseWantedCollections(values []string) (*wantedCollections, error) {
	if len(values) == 0 {
		return nil, nil
	}
	if len(values) > MaxWantedCollections {
		return nil, fmt.Errorf("%w: too many wanted collections: %d > %d",
			ErrInvalidOptions, len(values), MaxWantedCollections)
	}

	wc := &wantedCollections{
		fullPaths: make(map[string]struct{}),
	}
	for _, raw := range values {
		if strings.HasSuffix(raw, ".*") {
			// Prefix form. The leading portion (without the trailing "*")
			// must itself parse as a valid NSID. v1 enforces this so that
			// patterns like "app.bsky.fo*" are rejected at parse time.
			head := strings.TrimSuffix(raw, "*")
			// head still ends with "."; strip it for NSID validation.
			if _, err := atmos.ParseNSID(strings.TrimSuffix(head, ".")); err != nil {
				return nil, fmt.Errorf("%w: invalid collection: %s",
					ErrInvalidOptions, raw)
			}
			wc.prefixes = append(wc.prefixes, head)
			continue
		}
		nsid, err := atmos.ParseNSID(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid collection: %s",
				ErrInvalidOptions, raw)
		}
		wc.fullPaths[string(nsid)] = struct{}{}
	}
	return wc, nil
}

func parseWantedDIDs(values []string) (map[string]struct{}, error) {
	if len(values) == 0 {
		return nil, nil
	}
	if len(values) > MaxWantedDIDs {
		return nil, fmt.Errorf("%w: too many wanted DIDs: %d > %d",
			ErrInvalidOptions, len(values), MaxWantedDIDs)
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
	return out, nil
}
```

- [ ] **Step 2.4: Stub `parseMaxMsgSizeQuery` so the package compiles**

Append to `internal/subscribe/filter.go`:

```go
// parseMaxMsgSizeQuery is implemented properly in Task 5; stubbed here
// so Task 2 compiles. The stub returns 0, nil for any input — matches
// the v1 default ("0 or empty = no cap") for the ParseQuery happy paths
// covered in this task. Task 5 replaces this with the full v1-parity
// implementation and adds TestParseMaxMsgSize_V1Compat.
func parseMaxMsgSizeQuery(s string) (uint32, error) {
	_ = s
	return 0, nil
}
```

- [ ] **Step 2.5: Run the tests and confirm they pass**

Run: `just test ./internal/subscribe`
Expected: PASS for every `TestParseQuery_*` test added in Step 2.1.

- [ ] **Step 2.6: Run lint**

Run: `just lint`
Expected: PASS.

- [ ] **Step 2.7: Commit**

```bash
git add internal/subscribe/filter.go internal/subscribe/filter_test.go
git commit -m "subscribe: add Filter type and ParseQuery"
```

---

## Task 3: `ParseUpdatePayload` + wire-message types

**Files:**
- Modify: `internal/subscribe/filter.go`
- Modify: `internal/subscribe/filter_test.go`

- [ ] **Step 3.1: Write failing tests for `ParseUpdatePayload` and the wire types**

Append to `internal/subscribe/filter_test.go`:

```go
func TestParseUpdatePayload_Empty_MatchAll(t *testing.T) {
	t.Parallel()
	f, err := ParseUpdatePayload(UpdatePayload{})
	require.NoError(t, err)
	require.Nil(t, f.wantedDIDs)
	require.Nil(t, f.wantedCollections)
	require.Equal(t, uint32(0), f.MaxMessageSizeBytes())
}

func TestParseUpdatePayload_HappyPath(t *testing.T) {
	t.Parallel()
	p := UpdatePayload{
		WantedCollections: []string{"app.bsky.feed.post", "app.bsky.graph.*"},
		WantedDIDs:        []string{"did:plc:eygmaihciaxprqvxpfvl6flk"},
		MaxMessageSizeBytes: 1_000_000,
	}
	f, err := ParseUpdatePayload(p)
	require.NoError(t, err)
	require.Len(t, f.wantedCollections.fullPaths, 1)
	require.Equal(t, []string{"app.bsky.graph."}, f.wantedCollections.prefixes)
	require.Len(t, f.wantedDIDs, 1)
	require.Equal(t, uint32(1_000_000), f.MaxMessageSizeBytes())
}

func TestParseUpdatePayload_BadCollection(t *testing.T) {
	t.Parallel()
	_, err := ParseUpdatePayload(UpdatePayload{
		WantedCollections: []string{"not a valid nsid"},
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidOptions))
}

func TestParseUpdatePayload_BadDID(t *testing.T) {
	t.Parallel()
	_, err := ParseUpdatePayload(UpdatePayload{
		WantedDIDs: []string{"not-a-did"},
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidOptions))
}

func TestParseUpdatePayload_TooManyDIDs(t *testing.T) {
	t.Parallel()
	dids := make([]string, MaxWantedDIDs+1)
	for i := range dids {
		dids[i] = "did:plc:eygmaihciaxprqvxpfvl6flk"
	}
	_, err := ParseUpdatePayload(UpdatePayload{WantedDIDs: dids})
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidOptions))
}

func TestSubscriberSourcedMessage_RoundTrip(t *testing.T) {
	t.Parallel()
	// Confirm the JSON shape matches the v1 wire contract.
	const raw = `{"type":"options_update","payload":{"wantedCollections":["app.bsky.feed.post"],"wantedDids":["did:plc:eygmaihciaxprqvxpfvl6flk"],"maxMessageSizeBytes":1000000}}`
	var msg SubscriberSourcedMessage
	require.NoError(t, json.Unmarshal([]byte(raw), &msg))
	require.Equal(t, SubMessageTypeOptionsUpdate, msg.Type)

	var p UpdatePayload
	require.NoError(t, json.Unmarshal(msg.Payload, &p))
	require.Equal(t, []string{"app.bsky.feed.post"}, p.WantedCollections)
	require.Equal(t, []string{"did:plc:eygmaihciaxprqvxpfvl6flk"}, p.WantedDIDs)
	require.Equal(t, 1_000_000, p.MaxMessageSizeBytes)
}
```

Add to the imports at the top of the test file: `"encoding/json"`.

- [ ] **Step 3.2: Run tests and confirm they fail to compile**

Run: `just test ./internal/subscribe`
Expected: build failure — `ParseUpdatePayload`, `UpdatePayload`, `SubscriberSourcedMessage`, `SubMessageTypeOptionsUpdate` not defined.

- [ ] **Step 3.3: Add the wire types and `ParseUpdatePayload`**

Append to `internal/subscribe/filter.go`. First, change the `import` block to include `"encoding/json"`:

```go
import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/jcalabro/atmos"
)
```

Then append after the existing functions:

```go
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

// clampMaxMsgSize is implemented properly in Task 5; stubbed for Task 3.
// V1 PARITY: passes positive values through, will coerce negatives to 0
// once Task 5 lands. Test TestParseUpdatePayload_HappyPath uses 1e6,
// which is positive on both stub and final.
func clampMaxMsgSize(n int) uint32 {
	if n < 0 {
		return 0
	}
	return uint32(n)
}
```

- [ ] **Step 3.4: Run tests and confirm they pass**

Run: `just test ./internal/subscribe`
Expected: PASS for all `TestParseUpdatePayload_*` and `TestSubscriberSourcedMessage_RoundTrip`.

- [ ] **Step 3.5: Run lint**

Run: `just lint`
Expected: PASS.

- [ ] **Step 3.6: Commit**

```bash
git add internal/subscribe/filter.go internal/subscribe/filter_test.go
git commit -m "subscribe: add ParseUpdatePayload and SubscriberSourcedMessage types"
```

---

## Task 4: `Wants` predicate

**Files:**
- Modify: `internal/subscribe/filter.go`
- Modify: `internal/subscribe/filter_test.go`

- [ ] **Step 4.1: Write failing tests for the predicate**

Append to `internal/subscribe/filter_test.go`:

```go
import (
	// (already present from earlier tasks; ensure segment is imported below)
)

// Helper: build an event with the given kind/did/collection.
func ev(kind segment.Kind, did, collection string) *segment.Event {
	return &segment.Event{Kind: kind, DID: did, Collection: collection}
}
```

Add `"github.com/bluesky-social/jetstream-v2/segment"` to the test file's import block. Then append:

```go
func TestWants_NilFilterMatchesAll(t *testing.T) {
	t.Parallel()
	var f *Filter
	require.True(t, f.Wants(ev(segment.KindCreate, "did:plc:abc", "app.bsky.feed.post")))
	require.True(t, f.Wants(ev(segment.KindIdentity, "did:plc:abc", "")))
}

func TestWants_EmptyFilterMatchesAll(t *testing.T) {
	t.Parallel()
	f, err := ParseQuery(url.Values{})
	require.NoError(t, err)
	require.True(t, f.Wants(ev(segment.KindCreate, "did:plc:abc", "app.bsky.feed.post")))
	require.True(t, f.Wants(ev(segment.KindIdentity, "did:plc:xyz", "")))
}

func TestWants_CollectionFullPath(t *testing.T) {
	t.Parallel()
	f, err := ParseQuery(url.Values{
		"wantedCollections": []string{"app.bsky.feed.post"},
	})
	require.NoError(t, err)
	require.True(t, f.Wants(ev(segment.KindCreate, "did:plc:abc", "app.bsky.feed.post")))
	require.False(t, f.Wants(ev(segment.KindCreate, "did:plc:abc", "app.bsky.feed.like")))
}

func TestWants_CollectionPrefix(t *testing.T) {
	t.Parallel()
	f, err := ParseQuery(url.Values{
		"wantedCollections": []string{"app.bsky.graph.*"},
	})
	require.NoError(t, err)
	require.True(t, f.Wants(ev(segment.KindCreate, "did:plc:abc", "app.bsky.graph.follow")))
	require.True(t, f.Wants(ev(segment.KindCreate, "did:plc:abc", "app.bsky.graph.list")))
	require.False(t, f.Wants(ev(segment.KindCreate, "did:plc:abc", "app.bsky.feed.post")))
}

func TestWants_DIDFilter(t *testing.T) {
	t.Parallel()
	f, err := ParseQuery(url.Values{
		"wantedDids": []string{"did:plc:want"},
	})
	require.NoError(t, err)
	require.True(t, f.Wants(ev(segment.KindCreate, "did:plc:want", "app.bsky.feed.post")))
	require.False(t, f.Wants(ev(segment.KindCreate, "did:plc:other", "app.bsky.feed.post")))
}

// V1 PARITY: identity and account events bypass wantedCollections.
// They DO still respect wantedDids. The v1 README documents this:
// "Regardless of desired collections, all subscribers receive Account
// and Identity events."
func TestWants_IdentityBypassesCollectionFilter(t *testing.T) {
	t.Parallel()
	f, err := ParseQuery(url.Values{
		"wantedCollections": []string{"app.bsky.feed.post"},
	})
	require.NoError(t, err)
	require.True(t, f.Wants(ev(segment.KindIdentity, "did:plc:any", "")))
	require.True(t, f.Wants(ev(segment.KindAccount, "did:plc:any", "")))
}

func TestWants_IdentityRespectsDIDFilter(t *testing.T) {
	t.Parallel()
	f, err := ParseQuery(url.Values{
		"wantedDids": []string{"did:plc:want"},
	})
	require.NoError(t, err)
	require.True(t, f.Wants(ev(segment.KindIdentity, "did:plc:want", "")))
	require.False(t, f.Wants(ev(segment.KindIdentity, "did:plc:other", "")))
	require.True(t, f.Wants(ev(segment.KindAccount, "did:plc:want", "")))
	require.False(t, f.Wants(ev(segment.KindAccount, "did:plc:other", "")))
}

func TestWants_BothFiltersMustMatchForCommits(t *testing.T) {
	t.Parallel()
	f, err := ParseQuery(url.Values{
		"wantedCollections": []string{"app.bsky.feed.post"},
		"wantedDids":        []string{"did:plc:want"},
	})
	require.NoError(t, err)
	// Both match → delivered.
	require.True(t, f.Wants(ev(segment.KindCreate, "did:plc:want", "app.bsky.feed.post")))
	// DID matches, collection doesn't → dropped.
	require.False(t, f.Wants(ev(segment.KindCreate, "did:plc:want", "app.bsky.feed.like")))
	// Collection matches, DID doesn't → dropped.
	require.False(t, f.Wants(ev(segment.KindCreate, "did:plc:other", "app.bsky.feed.post")))
}

func TestWants_DoesNotEnforceMaxMessageSize(t *testing.T) {
	t.Parallel()
	// Wants reports membership; size enforcement lives in the handler
	// after Encode (the predicate doesn't see the encoded byte length).
	q := url.Values{"maxMessageSizeBytes": []string{"100"}}
	f, err := ParseQuery(q)
	require.NoError(t, err)
	// A huge payload still passes Wants.
	huge := &segment.Event{
		Kind:       segment.KindCreate,
		DID:        "did:plc:abc",
		Collection: "app.bsky.feed.post",
		Payload:    make([]byte, 1_000_000),
	}
	require.True(t, f.Wants(huge))
}
```

- [ ] **Step 4.2: Run tests and confirm they fail to compile**

Run: `just test ./internal/subscribe`
Expected: build failure — `(*Filter).Wants` not defined.

- [ ] **Step 4.3: Implement `Wants`**

Append to `internal/subscribe/filter.go`:

```go
// Wants reports whether the subscriber should receive evt. The rules
// (V1 PARITY):
//
//   - A nil *Filter or an empty Filter ("match-all" from ParseQuery on
//     no query params) matches every event.
//   - wantedDIDs applies to all event kinds. If non-empty and evt.DID
//     is not in the set, drop.
//   - wantedCollections applies ONLY to commit events
//     (KindCreate / KindUpdate / KindDelete). Identity and Account
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
	return k == segment.KindCreate || k == segment.KindUpdate || k == segment.KindDelete
}
```

Add `"github.com/bluesky-social/jetstream-v2/segment"` to `filter.go`'s import block.

- [ ] **Step 4.4: Run tests and confirm they pass**

Run: `just test ./internal/subscribe`
Expected: PASS for all `TestWants_*` plus all earlier filter tests still green.

- [ ] **Step 4.5: Run lint**

Run: `just lint`
Expected: PASS.

- [ ] **Step 4.6: Commit**

```bash
git add internal/subscribe/filter.go internal/subscribe/filter_test.go
git commit -m "subscribe: add Filter.Wants predicate with v1-parity rules"
```

---

## Task 5: `maxMessageSizeBytes` v1-parity parsing + lock-down test

This task replaces the Task 2/3 stubs with the real v1-parity parsing and pins the contract down with a table-driven test. **Read `docs/superpowers/specs/2026-05-27-subscribe-v1-filtering-design.md` §"`maxMessageSizeBytes` parsing" before starting** — the v1-parity decision and rationale are documented there.

**Files:**
- Modify: `internal/subscribe/filter.go`
- Modify: `internal/subscribe/filter_test.go`

- [ ] **Step 5.1: Write the v1-parity table test**

Append to `internal/subscribe/filter_test.go`:

```go
// TestParseMaxMsgSize_V1Compat locks down the silent-coercion behavior.
// V1 PARITY (deliberate divergence from CLAUDE.md's "no silent fallbacks"
// rule): the v1 README states "Zero means no limit, negative values are
// treated as zero." Existing v1 clients send "0", "" and (occasionally)
// garbage and rely on this exact coercion. See the design doc for the
// full rationale.
func TestParseMaxMsgSize_V1Compat(t *testing.T) {
	t.Parallel()

	t.Run("query", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			in   string
			want uint32
		}{
			{"", 0},
			{"0", 0},
			{"-1", 0},
			{"abc", 0},
			{"1000000", 1_000_000},
			{"99999999999", 0}, // overflow → 0
		}
		for _, tc := range cases {
			tc := tc
			t.Run(tc.in, func(t *testing.T) {
				t.Parallel()
				got, err := parseMaxMsgSizeQuery(tc.in)
				require.NoError(t, err, "parseMaxMsgSizeQuery must NEVER error: v1 parity")
				require.Equal(t, tc.want, got)
			})
		}
	})

	t.Run("payload", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			in   int
			want uint32
		}{
			{0, 0},
			{-1, 0},
			{1_000_000, 1_000_000},
		}
		for _, tc := range cases {
			tc := tc
			t.Run("", func(t *testing.T) {
				t.Parallel()
				got := clampMaxMsgSize(tc.in)
				require.Equal(t, tc.want, got)
			})
		}
	})
}
```

- [ ] **Step 5.2: Run the test and confirm it fails**

Run: `just test ./internal/subscribe -run TestParseMaxMsgSize_V1Compat`
Expected: FAIL — the Task 2 stub for `parseMaxMsgSizeQuery` returns 0 for "1000000", but the test expects 1_000_000.

- [ ] **Step 5.3: Replace the stub with the real implementation**

In `internal/subscribe/filter.go`, replace the existing `parseMaxMsgSizeQuery` stub with:

```go
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
//
// Returns an error type only because the rest of the parsers in this
// file return (T, error); this function never actually errors today.
func parseMaxMsgSizeQuery(s string) (uint32, error) {
	if s == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, nil // V1 PARITY: silent coerce
	}
	if n < 0 {
		return 0, nil // V1 PARITY: documented behavior
	}
	if uint64(n) > uint64(^uint32(0)) {
		return 0, nil // V1 PARITY: overflow coerces to 0 too
	}
	return uint32(n), nil
}
```

Add `"strconv"` to `filter.go`'s import block.

The existing `clampMaxMsgSize` from Task 3 is already correct; leave it alone. Re-read its doc comment and verify it still says "V1 PARITY"; if not, update it to match the explanation above.

- [ ] **Step 5.4: Run the V1 compat test plus the broader filter suite**

Run: `just test ./internal/subscribe`
Expected: PASS, including `TestParseMaxMsgSize_V1Compat`.

- [ ] **Step 5.5: Run lint**

Run: `just lint`
Expected: PASS.

- [ ] **Step 5.6: Commit**

```bash
git add internal/subscribe/filter.go internal/subscribe/filter_test.go
git commit -m "subscribe: v1-parity maxMessageSizeBytes parsing"
```

---

## Task 6: Fuzz tests for `ParseQuery` and `ParseUpdatePayload`

AGENTS.md: "Fuzz tests and property based tests are valuable for many things, notably handling untrusted user input." The query string and websocket payload are untrusted.

**Files:**
- Create: `internal/subscribe/filter_fuzz_test.go`

- [ ] **Step 6.1: Create the fuzz file**

Create `internal/subscribe/filter_fuzz_test.go`:

```go
package subscribe

import (
	"encoding/json"
	"net/url"
	"strings"
	"testing"

	"github.com/bluesky-social/jetstream-v2/segment"
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
	_ = strings.HasPrefix // import retained for future seeds; safe to drop if linter complains
}
```

If the linter complains about unused `strings` import, delete the last line (`_ = strings.HasPrefix`) and remove `"strings"` from the import block.

- [ ] **Step 6.2: Run a brief fuzz pass to confirm no panics**

Run: `just fuzz 5s ./internal/subscribe`
Expected: each fuzz target runs for ~5s and reports `PASS`. No panics, no crashes.

- [ ] **Step 6.3: Run the unit test suite (the seed corpus runs as regular tests)**

Run: `just test ./internal/subscribe`
Expected: PASS — fuzz targets execute their seed corpus during normal `go test` runs.

- [ ] **Step 6.4: Commit**

```bash
git add internal/subscribe/filter_fuzz_test.go
git commit -m "subscribe: fuzz ParseQuery and ParseUpdatePayload"
```

---

## Task 7: New metrics for filter and options_update

**Files:**
- Modify: `internal/subscribe/metrics.go`
- Modify: `internal/subscribe/metrics_test.go`

- [ ] **Step 7.1: Extend `metrics.go` with the new series**

In `internal/subscribe/metrics.go`, add four new fields to the `Metrics` struct (preserving the existing fields exactly):

```go
type Metrics struct {
	Subscribers       prometheus.Gauge
	CleanDisconnects  prometheus.Counter
	SlowDrops         prometheus.Counter
	EventsPublished   prometheus.Counter
	EventsSent        prometheus.Counter
	EventsSkippedSync prometheus.Counter
	EncodeErrors      prometheus.Counter
	QueueDepth        prometheus.Histogram

	// Added in 2026-05-27 v1-filtering port:
	EventsFiltered       prometheus.Counter
	EventsOversize       prometheus.Counter
	OptionsUpdates       prometheus.Counter
	OptionsUpdateErrors  *prometheus.CounterVec
}
```

In `NewMetrics`, register the new series. Inside the existing `m := &Metrics{...}` literal, append (after `QueueDepth`):

```go
		EventsFiltered: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "events_filtered_total",
			Help: "Events the per-subscriber Filter dropped before encoding (Wants returned false).",
		}),
		EventsOversize: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "events_oversize_total",
			Help: "Encoded frames dropped because their size exceeded the subscriber's maxMessageSizeBytes.",
		}),
		OptionsUpdates: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "options_updates_total",
			Help: "Number of successful options_update messages applied to a connected subscriber.",
		}),
		OptionsUpdateErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: metricsNamespace, Subsystem: metricsSubsystem,
				Name: "options_update_errors_total",
				Help: "Subscriber-sourced messages rejected. Reason label is one of: oversize, bad_envelope_json, bad_payload_json, invalid_options.",
			},
			[]string{"reason"},
		),
```

Update the `reg.MustRegister(...)` call at the end of `NewMetrics` to include the new collectors:

```go
	reg.MustRegister(
		m.Subscribers, m.CleanDisconnects, m.SlowDrops,
		m.EventsPublished, m.EventsSent, m.EventsSkippedSync,
		m.EncodeErrors, m.QueueDepth,
		m.EventsFiltered, m.EventsOversize,
		m.OptionsUpdates, m.OptionsUpdateErrors,
	)
```

Append four nil-safe increment helpers at the bottom of the file:

```go
func (m *Metrics) incEventsFiltered() {
	if m != nil {
		m.EventsFiltered.Inc()
	}
}
func (m *Metrics) incEventsOversize() {
	if m != nil {
		m.EventsOversize.Inc()
	}
}
func (m *Metrics) incOptionsUpdates() {
	if m != nil {
		m.OptionsUpdates.Inc()
	}
}
// Reasons for incOptionsUpdateError. Defined as constants so callers can't
// drift the label cardinality.
const (
	OptionsUpdateErrorReasonOversize        = "oversize"
	OptionsUpdateErrorReasonBadEnvelopeJSON = "bad_envelope_json"
	OptionsUpdateErrorReasonBadPayloadJSON  = "bad_payload_json"
	OptionsUpdateErrorReasonInvalidOptions  = "invalid_options"
)

func (m *Metrics) incOptionsUpdateError(reason string) {
	if m != nil {
		m.OptionsUpdateErrors.WithLabelValues(reason).Inc()
	}
}
```

- [ ] **Step 7.2: Extend `metrics_test.go`**

In `internal/subscribe/metrics_test.go`, add the new helpers to the existing two tests so they're exercised under both real-Metrics and nil-Metrics paths.

In `TestNewMetrics_RegistersAllSeries`, append before the closing `}`:

```go
	m.incEventsFiltered()
	m.incEventsOversize()
	m.incOptionsUpdates()
	m.incOptionsUpdateError(OptionsUpdateErrorReasonOversize)
	m.incOptionsUpdateError(OptionsUpdateErrorReasonBadEnvelopeJSON)
	m.incOptionsUpdateError(OptionsUpdateErrorReasonBadPayloadJSON)
	m.incOptionsUpdateError(OptionsUpdateErrorReasonInvalidOptions)
```

In `TestMetrics_NilReceiverIsNoop`, append the same calls before the closing `}`:

```go
	m.incEventsFiltered()
	m.incEventsOversize()
	m.incOptionsUpdates()
	m.incOptionsUpdateError(OptionsUpdateErrorReasonOversize)
```

- [ ] **Step 7.3: Run the metrics tests**

Run: `just test ./internal/subscribe -run Metrics`
Expected: PASS for both tests.

- [ ] **Step 7.4: Run lint**

Run: `just lint`
Expected: PASS.

- [ ] **Step 7.5: Commit**

```bash
git add internal/subscribe/metrics.go internal/subscribe/metrics_test.go
git commit -m "subscribe: add filter / options_update metrics"
```

---

## Task 8: Handler — parse on connect + writer-loop filter + max-size

This task wires the filter into the handler's connect path and writer loop. It does NOT yet touch the reader goroutine — that's Task 9. Splitting keeps each task small and the integration tests focused.

**Files:**
- Modify: `internal/subscribe/handler.go`
- Modify: `internal/subscribe/handler_test.go`

- [ ] **Step 8.1: Write failing integration tests for connect-time behavior**

Append to `internal/subscribe/handler_test.go`. These tests use the existing `newSteadyStateStore` helper at the top of the file.

```go
func TestHandler_Filter_RejectsInvalidQuery(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	// Illegal prefix (must be at NSID boundary).
	resp, err := http.Get(srv.URL + "?wantedCollections=app.bsky.fo*")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	require.Contains(t, string(body), "invalid collection")
}

func TestHandler_Filter_TooManyDIDs(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	// Build a too-many-DIDs query string by hand to avoid url.Values allocation noise.
	var sb strings.Builder
	for i := 0; i <= MaxWantedDIDs; i++ {
		if i > 0 {
			sb.WriteByte('&')
		}
		sb.WriteString("wantedDids=did:plc:eygmaihciaxprqvxpfvl6flk")
	}
	resp, err := http.Get(srv.URL + "?" + sb.String())
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestHandler_Filter_WantedCollections_DeliversMatching(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?wantedCollections=app.bsky.feed.post"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	time.Sleep(50 * time.Millisecond)

	// Build a record-bearing commit. Encoder needs DAG-CBOR Payload + a CID;
	// the simplest valid CBOR is an empty map: 0xa0 = empty map.
	publishCommit(t, b, "did:plc:abc", "app.bsky.feed.like", 1)
	publishCommit(t, b, "did:plc:abc", "app.bsky.feed.post", 2)

	frame := readOneFrame(t, ctx, conn)
	var got map[string]any
	require.NoError(t, json.Unmarshal(frame, &got))
	require.Equal(t, "commit", got["kind"])
	commit := got["commit"].(map[string]any)
	require.Equal(t, "app.bsky.feed.post", commit["collection"])
}

func TestHandler_Filter_WantedCollections_PrefixMatch(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?wantedCollections=app.bsky.graph.*"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()
	time.Sleep(50 * time.Millisecond)

	publishCommit(t, b, "did:plc:abc", "app.bsky.feed.post", 1)
	publishCommit(t, b, "did:plc:abc", "app.bsky.graph.follow", 2)

	frame := readOneFrame(t, ctx, conn)
	var got map[string]any
	require.NoError(t, json.Unmarshal(frame, &got))
	commit := got["commit"].(map[string]any)
	require.Equal(t, "app.bsky.graph.follow", commit["collection"])
}

func TestHandler_Filter_WantedDIDs_DeliversMatching(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?wantedDids=did:plc:want"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()
	time.Sleep(50 * time.Millisecond)

	publishCommit(t, b, "did:plc:other", "app.bsky.feed.post", 1)
	publishCommit(t, b, "did:plc:want", "app.bsky.feed.post", 2)

	frame := readOneFrame(t, ctx, conn)
	var got map[string]any
	require.NoError(t, json.Unmarshal(frame, &got))
	require.Equal(t, "did:plc:want", got["did"])
}

func TestHandler_Filter_IdentityBypassesCollectionFilter(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?wantedCollections=app.bsky.feed.post"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()
	time.Sleep(50 * time.Millisecond)

	publishIdentity(t, b, "did:plc:any", 1)

	frame := readOneFrame(t, ctx, conn)
	var got map[string]any
	require.NoError(t, json.Unmarshal(frame, &got))
	require.Equal(t, "identity", got["kind"])
}

func TestHandler_Filter_IdentityRespectsDIDFilter(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?wantedDids=did:plc:want"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()
	time.Sleep(50 * time.Millisecond)

	publishIdentity(t, b, "did:plc:other", 1)
	publishIdentity(t, b, "did:plc:want", 2)

	frame := readOneFrame(t, ctx, conn)
	var got map[string]any
	require.NoError(t, json.Unmarshal(frame, &got))
	require.Equal(t, "did:plc:want", got["did"])
}

func TestHandler_Filter_MaxMessageSize_DropsOversize(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	// 200 bytes is enough for a small commit envelope but not for a giant one.
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?maxMessageSizeBytes=200"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()
	time.Sleep(50 * time.Millisecond)

	// Identity events are tiny; one should fit. Use them as the
	// "delivered" half of this test rather than constructing oversize
	// commits (which require valid CBOR + CID).
	publishOversizeCommit(t, b, "did:plc:big", "app.bsky.feed.post", 1)
	publishIdentity(t, b, "did:plc:small", 2)

	frame := readOneFrame(t, ctx, conn)
	var got map[string]any
	require.NoError(t, json.Unmarshal(frame, &got))
	// We should see the identity (small) but never the oversize commit.
	require.Equal(t, "identity", got["kind"])
}

// V1 PARITY regression guard — empty maxMessageSizeBytes coerces to "no cap".
func TestHandler_Filter_MaxMessageSize_EmptyMeansNoCap(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?maxMessageSizeBytes="

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err, "empty maxMessageSizeBytes must NOT reject the connection")
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()
}

// V1 PARITY regression guard — negative maxMessageSizeBytes coerces to "no cap".
func TestHandler_Filter_MaxMessageSize_NegativeMeansNoCap(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?maxMessageSizeBytes=-1"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err, "negative maxMessageSizeBytes must NOT reject the connection")
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()
}
```

Add to `handler_test.go`'s import block as needed: `"github.com/bluesky-social/jetstream-v2/segment"`. Several imports (`net/http`, `strings`) may already be there.

- [ ] **Step 8.2: Add the test helpers (publish helpers + readOneFrame)**

Append to `internal/subscribe/handler_test.go` (above or below the new tests; near the existing `newSteadyStateStore`):

```go
// readOneFrame reads one text frame with a 1s deadline. Centralizes the
// pattern so tests stay terse.
func readOneFrame(t *testing.T, ctx context.Context, conn *websocket.Conn) []byte {
	t.Helper()
	rctx, rcancel := context.WithTimeout(ctx, 1*time.Second)
	defer rcancel()
	_, frame, err := conn.Read(rctx)
	require.NoError(t, err)
	return frame
}

// publishIdentity publishes a minimal identity event the encoder can render.
func publishIdentity(t *testing.T, b *Broadcaster, did string, indexedAt int64) {
	t.Helper()
	id := &comatproto.SyncSubscribeRepos_Identity{
		DID: did, Seq: indexedAt, Time: "2026-05-27T00:00:00Z",
	}
	payload, err := id.MarshalCBOR()
	require.NoError(t, err)
	b.Publish(&segment.Event{
		IndexedAt: indexedAt, Kind: segment.KindIdentity,
		DID: did, Payload: payload,
	})
}

// publishCommit publishes a minimal create commit. The Payload is a
// DAG-CBOR-encoded empty map (0xa0), which the encoder will turn into "{}".
func publishCommit(t *testing.T, b *Broadcaster, did, collection string, indexedAt int64) {
	t.Helper()
	b.Publish(&segment.Event{
		IndexedAt:  indexedAt,
		Kind:       segment.KindCreate,
		DID:        did,
		Collection: collection,
		Rkey:       "abcd1234",
		Rev:        "3lXrev",
		Payload:    []byte{0xa0}, // CBOR empty map
	})
}

// publishOversizeCommit publishes a commit with a payload large enough
// that the encoded JSON envelope will exceed any modest maxMessageSizeBytes.
func publishOversizeCommit(t *testing.T, b *Broadcaster, did, collection string, indexedAt int64) {
	t.Helper()
	// CBOR map of 1 entry: key "x" → byte string of 4096 bytes.
	// 0xa1 = map(1); 0x61 = text(1); 0x78 ... = bytes header.
	big := bytes.NewBuffer(nil)
	big.WriteByte(0xa1)             // map(1)
	big.WriteByte(0x61)             // text(1)
	big.WriteByte('x')              // "x"
	big.WriteByte(0x59)             // bytes, 2-byte length follows
	big.WriteByte(0x10)             // 0x1000 = 4096
	big.WriteByte(0x00)
	big.Write(make([]byte, 0x1000)) // 4096 zero bytes
	b.Publish(&segment.Event{
		IndexedAt:  indexedAt,
		Kind:       segment.KindCreate,
		DID:        did,
		Collection: collection,
		Rkey:       "abcd1234",
		Rev:        "3lXrev",
		Payload:    big.Bytes(),
	})
}
```

Add imports at the top of `handler_test.go` if missing: `"bytes"`, `"github.com/bluesky-social/jetstream-v2/segment"`.

- [ ] **Step 8.3: Run the tests and confirm they fail**

Run: `just test ./internal/subscribe -run TestHandler_Filter`
Expected: FAIL — handler doesn't yet honor any filter; oversize/identity-bypass/etc. are wrong.

- [ ] **Step 8.4: Modify `handler.go` — parse on connect, add atomic.Pointer, filter writer loop**

Replace the body of `serve` in `internal/subscribe/handler.go`. Add `"sync/atomic"`, `"net/url"` (already present indirectly via `r.URL`), and remove no longer-relevant imports if applicable. The new shape:

```go
func serve(
	w http.ResponseWriter,
	r *http.Request,
	broadcaster *Broadcaster,
	store *store.Store,
	logger *slog.Logger,
	m *Metrics,
) {
	if !lifecycle.IsSteadyState(store) {
		http.Error(w, "service not ready: bootstrap in progress", http.StatusServiceUnavailable)
		return
	}

	// Parse subscriber filter BEFORE upgrading. v1 contract: a bad
	// query yields HTTP 400 with a useful body, not a websocket close.
	initialFilter, perr := ParseQuery(r.URL.Query())
	if perr != nil {
		http.Error(w, perr.Error(), http.StatusBadRequest)
		return
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns:  []string{"*"},
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		// Accept already wrote the error response.
		return
	}
	defer func() { _ = conn.CloseNow() }()

	// Per-connection filter pointer. Updates from options_update (Task 9)
	// will Store a fresh *Filter; the writer loop Loads on each event.
	// Treated as immutable once published — atomic pointer not RWMutex.
	var filterPtr atomic.Pointer[Filter]
	filterPtr.Store(initialFilter)

	subCh, doneCh, unsubscribe := broadcaster.Subscribe()
	defer unsubscribe()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// Reader goroutine. Task 9 will replace this body with full
	// SubscriberSourcedMessage parsing; for now we keep the existing
	// drain-and-discard behavior.
	go func() {
		defer cancel()
		for {
			_, _, rerr := conn.Reader(ctx)
			if rerr != nil {
				return
			}
		}
	}()

	pingTicker := time.NewTicker(pingInterval)
	defer pingTicker.Stop()

	logger.Info("subscriber connected", "remote_addr", r.RemoteAddr)
	defer logger.Info("subscriber disconnected", "remote_addr", r.RemoteAddr)

	for {
		select {
		case <-ctx.Done():
			return
		case <-doneCh:
			return
		case <-pingTicker.C:
			pingCtx, pcancel := context.WithTimeout(ctx, frameWriteTimeout)
			perr := conn.Ping(pingCtx)
			pcancel()
			if perr != nil {
				return
			}
		case evt := <-subCh:
			f := filterPtr.Load()
			if !f.Wants(evt) {
				m.incEventsFiltered()
				continue
			}
			body, eerr := Encode(evt)
			if errors.Is(eerr, errSkipEvent) {
				m.incEventsSkippedSync()
				continue
			}
			if eerr != nil {
				m.incEncodeErrors()
				logger.Warn("encode error",
					"err", eerr,
					"kind", int(evt.Kind),
					"did", evt.DID,
				)
				continue
			}
			if max := f.MaxMessageSizeBytes(); max > 0 && uint32(len(body)) > max {
				m.incEventsOversize()
				continue
			}
			writeCtx, wcancel := context.WithTimeout(ctx, frameWriteTimeout)
			werr := conn.Write(writeCtx, websocket.MessageText, body)
			wcancel()
			if werr != nil {
				return
			}
			m.incEventsSent()
		}
	}
}
```

Add `"sync/atomic"` to handler.go's import block.

- [ ] **Step 8.5: Run the new integration tests and the existing ones**

Run: `just test ./internal/subscribe`
Expected: PASS — every `TestHandler_Filter_*`, `TestHandler_HappyPath_DeliversIdentityEvent`, `TestHandler_SyncEventNotEmitted`, `TestHandler_RejectsWhenNotSteadyState`, all green.

- [ ] **Step 8.6: Run lint**

Run: `just lint`
Expected: PASS.

- [ ] **Step 8.7: Run with the race detector**

Run: `just test-race ./internal/subscribe`
Expected: PASS, no data races (the atomic.Pointer is the only shared state between reader and writer goroutines).

- [ ] **Step 8.8: Commit**

```bash
git add internal/subscribe/handler.go internal/subscribe/handler_test.go
git commit -m "subscribe: parse filter on connect, apply in writer loop"
```

---

## Task 9: Handler — `options_update` reader

This task replaces the drain-and-discard reader with a full SubscriberSourcedMessage parser. On a valid `options_update` it swaps the per-connection `*Filter`. On any error it terminates the connection with a websocket close and a useful reason string.

**Files:**
- Modify: `internal/subscribe/handler.go`
- Modify: `internal/subscribe/handler_test.go`

- [ ] **Step 9.1: Write failing tests for the dynamic filter path**

Append to `internal/subscribe/handler_test.go`:

```go
func TestHandler_OptionsUpdate_ChangesFilter(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()
	time.Sleep(50 * time.Millisecond)

	// Narrow to likes only.
	update := SubscriberSourcedMessage{
		Type: SubMessageTypeOptionsUpdate,
		Payload: jsonMust(t, UpdatePayload{
			WantedCollections: []string{"app.bsky.feed.like"},
		}),
	}
	require.NoError(t, conn.Write(ctx, websocket.MessageText, jsonMust(t, update)))

	// Give the reader goroutine a moment to apply the update.
	time.Sleep(50 * time.Millisecond)

	publishCommit(t, b, "did:plc:abc", "app.bsky.feed.post", 1)
	publishCommit(t, b, "did:plc:abc", "app.bsky.feed.like", 2)

	frame := readOneFrame(t, ctx, conn)
	var got map[string]any
	require.NoError(t, json.Unmarshal(frame, &got))
	commit := got["commit"].(map[string]any)
	require.Equal(t, "app.bsky.feed.like", commit["collection"])
}

func TestHandler_OptionsUpdate_InvalidPayloadDisconnects(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close(websocket.StatusInternalError, "test done") }()
	time.Sleep(50 * time.Millisecond)

	// Send malformed JSON envelope.
	require.NoError(t, conn.Write(ctx, websocket.MessageText, []byte("not json")))

	// The next read should observe a close.
	rctx, rcancel := context.WithTimeout(ctx, 2*time.Second)
	defer rcancel()
	_, _, err = conn.Read(rctx)
	require.Error(t, err, "expected close after malformed envelope")
}

func TestHandler_OptionsUpdate_BadNSIDDisconnects(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close(websocket.StatusInternalError, "test done") }()
	time.Sleep(50 * time.Millisecond)

	update := SubscriberSourcedMessage{
		Type: SubMessageTypeOptionsUpdate,
		Payload: jsonMust(t, UpdatePayload{
			WantedCollections: []string{"app.bsky.fo*"}, // illegal prefix
		}),
	}
	require.NoError(t, conn.Write(ctx, websocket.MessageText, jsonMust(t, update)))

	rctx, rcancel := context.WithTimeout(ctx, 2*time.Second)
	defer rcancel()
	_, _, err = conn.Read(rctx)
	require.Error(t, err, "expected close after bad NSID in options_update")
}

func TestHandler_OptionsUpdate_OversizePayload(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	// coder/websocket has a default per-message read limit (32 KiB) on
	// the SERVER side too — set it high enough to actually receive the
	// oversize message and exercise our 10MB cap. See conn.SetReadLimit.
	defer func() { _ = conn.Close(websocket.StatusInternalError, "test done") }()
	time.Sleep(50 * time.Millisecond)

	// Send a payload just over MaxSubscriberMessageBytes.
	big := make([]byte, MaxSubscriberMessageBytes+1)
	for i := range big {
		big[i] = ' '
	}
	require.NoError(t, conn.Write(ctx, websocket.MessageText, big))

	rctx, rcancel := context.WithTimeout(ctx, 2*time.Second)
	defer rcancel()
	_, _, err = conn.Read(rctx)
	require.Error(t, err, "expected close after oversize subscriber message")
}

func TestHandler_OptionsUpdate_UnknownTypeIgnored(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()
	time.Sleep(50 * time.Millisecond)

	// V1 PARITY: unknown message types are logged and ignored, not fatal.
	unknown := SubscriberSourcedMessage{
		Type:    "unknown_type",
		Payload: json.RawMessage(`null`),
	}
	require.NoError(t, conn.Write(ctx, websocket.MessageText, jsonMust(t, unknown)))

	// Subsequent events must still flow.
	time.Sleep(50 * time.Millisecond)
	publishIdentity(t, b, "did:plc:still-alive", 1)
	frame := readOneFrame(t, ctx, conn)
	var got map[string]any
	require.NoError(t, json.Unmarshal(frame, &got))
	require.Equal(t, "identity", got["kind"])
}

// Small helper used by the options_update tests.
func jsonMust[T any](t *testing.T, v T) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}
```

- [ ] **Step 9.2: Run tests and confirm they fail**

Run: `just test ./internal/subscribe -run TestHandler_OptionsUpdate`
Expected: FAIL — the reader goroutine is still drain-and-discard, so options_update messages don't update the filter and don't disconnect on bad input.

- [ ] **Step 9.3: Raise the websocket read limit so our 10MB cap can run**

`coder/websocket` enforces a default per-message read limit of 32 KiB on the server side. To exercise our `MaxSubscriberMessageBytes = 10_000_000` cap, we need to raise the library limit above ours so the handler (not the library) rejects oversize messages with the right close code and reason.

In `internal/subscribe/handler.go`, immediately after the `websocket.Accept` call (before `defer conn.CloseNow()`), add:

```go
	// Raise coder/websocket's default 32 KiB read limit so that
	// MaxSubscriberMessageBytes (10MB, v1 parity) is the binding cap
	// and the handler returns a useful close reason on oversize.
	conn.SetReadLimit(int64(MaxSubscriberMessageBytes) + 1024)
```

The +1024 slack lets the reader observe at least one byte beyond the cap so the `len(payload) > MaxSubscriberMessageBytes` check fires inside our code path rather than at the library's strict boundary.

- [ ] **Step 9.4: Replace the reader goroutine in `handler.go`**

In `internal/subscribe/handler.go`, replace the existing reader goroutine (the small block under `// Reader goroutine. Task 9 will replace this body...`) with the full parser:

```go
	// Reader goroutine: parses SubscriberSourcedMessage frames. On any
	// validation failure we send a websocket close with the reason and
	// cancel the connection context, which tears down the writer loop.
	go func() {
		defer cancel()
		for {
			msgType, payload, rerr := conn.Read(ctx)
			if rerr != nil {
				return
			}
			if msgType != websocket.MessageText {
				// V1 ignores binary frames silently; match that.
				continue
			}
			if len(payload) > MaxSubscriberMessageBytes {
				m.incOptionsUpdateError(OptionsUpdateErrorReasonOversize)
				_ = conn.Close(websocket.StatusMessageTooBig,
					"subscriber message exceeds 10MB cap")
				return
			}
			var msg SubscriberSourcedMessage
			if err := json.Unmarshal(payload, &msg); err != nil {
				m.incOptionsUpdateError(OptionsUpdateErrorReasonBadEnvelopeJSON)
				_ = conn.Close(websocket.StatusInvalidFramePayloadData,
					"bad SubscriberSourcedMessage envelope")
				return
			}
			switch msg.Type {
			case SubMessageTypeOptionsUpdate:
				var update UpdatePayload
				if err := json.Unmarshal(msg.Payload, &update); err != nil {
					m.incOptionsUpdateError(OptionsUpdateErrorReasonBadPayloadJSON)
					_ = conn.Close(websocket.StatusInvalidFramePayloadData,
						"bad options_update payload")
					return
				}
				newFilter, err := ParseUpdatePayload(update)
				if err != nil {
					m.incOptionsUpdateError(OptionsUpdateErrorReasonInvalidOptions)
					// Truncate the reason to fit the websocket close-frame
					// 123-byte cap (RFC 6455 §5.5.1).
					reason := truncateCloseReason(err.Error())
					_ = conn.Close(websocket.StatusPolicyViolation, reason)
					return
				}
				filterPtr.Store(newFilter)
				m.incOptionsUpdates()
			default:
				// V1 PARITY: unknown types log a warning and are ignored.
				logger.Warn("unknown subscriber message type", "type", msg.Type)
			}
		}
	}()
```

Add `"encoding/json"` to handler.go's imports if not already present. Then add a small helper at the bottom of handler.go:

```go
// truncateCloseReason fits a reason string into the 123-byte limit
// imposed on websocket close-frame reason text (RFC 6455 §5.5.1). Any
// truncation appends "..." to make the cut visible to clients.
func truncateCloseReason(s string) string {
	const max = 123
	if len(s) <= max {
		return s
	}
	if max < 3 {
		return s[:max]
	}
	return s[:max-3] + "..."
}
```

- [ ] **Step 9.5: Run the new tests plus the full subscribe suite**

Run: `just test ./internal/subscribe`
Expected: PASS — every test, including `TestHandler_OptionsUpdate_*`, `TestHandler_Filter_*`, and the pre-existing happy-path tests.

- [ ] **Step 9.6: Run the race detector**

Run: `just test-race ./internal/subscribe`
Expected: PASS, no data races. The `filterPtr` (atomic.Pointer) and the `cancel` shared between goroutines are the only cross-goroutine state; both are concurrency-safe.

- [ ] **Step 9.7: Run lint**

Run: `just lint`
Expected: PASS.

- [ ] **Step 9.8: Commit**

```bash
git add internal/subscribe/handler.go internal/subscribe/handler_test.go
git commit -m "subscribe: handle options_update from clients"
```

---

## Task 10: Update package documentation

**Files:**
- Modify: `internal/subscribe/doc.go`

- [ ] **Step 10.1: Rewrite `doc.go` with the v1-compat paragraph**

Replace the contents of `internal/subscribe/doc.go` with:

```go
// Package subscribe owns the live websocket fan-out behind the public
// /subscribe endpoint, plus the v1-compatible filtering and
// subscriber-sourced-message protocol that clients depend on.
//
// The package has four concerns, each in its own file:
//
//   - broadcaster.go: a single-publisher / many-subscriber pub/sub with
//     bounded per-subscriber channels. Slow subscribers are dropped, never
//     blocked; the firehose pipeline stays uncoupled from any one client.
//
//   - encoder.go: a pure function family that turns a segment.Event into
//     the Jetstream v1-compatible JSON wire format.
//
//   - filter.go: the per-connection Filter — wantedCollections,
//     wantedDids, maxMessageSizeBytes — plus parsers for the query-string
//     and options_update wire formats.
//
//   - handler.go: an http.Handler that upgrades to a websocket, parses
//     the initial filter, registers with the broadcaster, and pumps
//     filtered+encoded events to the client. The reader goroutine
//     accepts SubscriberSourcedMessage frames and applies options_update
//     by swapping a per-connection atomic.Pointer[Filter].
//
// V1 wire compatibility is the explicit design point. Where v2's house
// style ("crash loud, no silent fallbacks" — CLAUDE.md) would diverge
// from the v1 README's stated contract, this package deliberately
// matches v1. The places we do that are:
//
//   - maxMessageSizeBytes silently coerces empty/malformed/negative
//     values to 0 ("no cap"). v1 README: "Zero means no limit, negative
//     values are treated as zero." Locked down by
//     TestParseMaxMsgSize_V1Compat.
//
//   - Identity and Account events bypass wantedCollections — they are
//     always delivered, regardless of the subscriber's collection
//     filter. v1 README: "Regardless of desired collections, all
//     subscribers receive Account and Identity events." Locked down by
//     TestWants_IdentityBypassesCollectionFilter.
//
//   - #sync events are deliberately not emitted. v1 didn't emit them
//     either; the v2 archive path is authoritative for #sync.
//     Implemented in encoder.go via errSkipEvent.
//
//   - Unknown SubscriberSourcedMessage.Type values are logged and
//     ignored, not fatal. v1 has the same policy. Locked down by
//     TestHandler_OptionsUpdate_UnknownTypeIgnored.
//
// Out of scope for this v1-compat surface: cursor replay, zstd
// compression, requireHello. We accept those query params
// (silently ignored for cursor; absent for requireHello) so that v1
// clients that send them aren't rejected. Future v2-native endpoints
// will live alongside this package or in a sibling.
package subscribe
```

- [ ] **Step 10.2: Run lint to confirm the doc compiles**

Run: `just lint`
Expected: PASS.

- [ ] **Step 10.3: Final full-suite check**

Run: `just`
Expected: PASS — both lint and `just test` green.

- [ ] **Step 10.4: Commit**

```bash
git add internal/subscribe/doc.go
git commit -m "subscribe: document v1-compat behaviors and package shape"
```

---

## Verification

After completing every task above, the result is:

- A `/subscribe` endpoint that honors `wantedCollections`, `wantedDids`, and `maxMessageSizeBytes` per the v1 README.
- An `options_update` SubscriberSourcedMessage that lets clients change those filters mid-stream.
- Strict v1 wire-contract parity for the silent-coercion, identity-bypass, sync-skip, and unknown-type-ignored behaviors — each with a regression-guard test and a `V1 PARITY` code comment.
- Four new metrics under the `jetstream_subscribe_*` namespace, with a labeled error counter.
- Test coverage spanning unit (Filter rules, parser cap rules, V1 parity table), fuzz (ParseQuery + ParseUpdatePayload panic-freedom), and integration (every documented filter behavior driven through the real websocket handler).

End-to-end smoke check (optional, for confidence):

```sh
just simulator
# In another terminal:
just run serve
# In a third terminal:
websocat 'ws://localhost:8080/subscribe?wantedCollections=app.bsky.feed.post' | head -3
```

You should see only `app.bsky.feed.post` commits (plus any identity/account events).
