package subscribe

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/bluesky-social/jetstream-v2/segment"
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
	// "app.bsky.fo*" doesn't end in ".*" — it falls through to the
	// NSID branch where "*" is not a valid NSID character. v1 also
	// rejects this case, just via a different code path.
	q := url.Values{"wantedCollections": []string{"app.bsky.fo*"}}
	_, err := ParseQuery(q)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidOptions))
}

// V1 PARITY: a top-level vendor prefix like "app.bsky.*" is accepted.
// The v1 README's stated "head must pass NSID validation" rule is not
// what v1 actually implements — v1's code accepts any "<head>.*" with
// no head validation. Matching v1's behavior (not its docs) is the
// only way to keep clients that already send these patterns working.
func TestParseQuery_PrefixCollection_TwoSegment(t *testing.T) {
	t.Parallel()
	q := url.Values{"wantedCollections": []string{"app.bsky.*"}}
	f, err := ParseQuery(q)
	require.NoError(t, err)
	require.Equal(t, []string{"app.bsky."}, f.wantedCollections.prefixes)
}

// Prefix dedupe: duplicate ".*" patterns collapse to one entry rather
// than wasting an MaxWantedCollections slot per duplicate.
func TestParseQuery_PrefixCollection_DeduplicatesDuplicates(t *testing.T) {
	t.Parallel()
	q := url.Values{"wantedCollections": []string{
		"app.bsky.graph.*",
		"app.bsky.graph.*",
	}}
	f, err := ParseQuery(q)
	require.NoError(t, err)
	require.Equal(t, []string{"app.bsky.graph."}, f.wantedCollections.prefixes)
}

func TestParseQuery_BadDID(t *testing.T) {
	t.Parallel()
	q := url.Values{"wantedDids": []string{"not-a-did"}}
	_, err := ParseQuery(q)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidOptions))
}

// TestParseQuery_TooManyCollections — the cap fires post-dedupe. Use
// MaxWantedCollections+1 unique prefix patterns so the unique count
// exceeds the cap regardless of dedupe.
func TestParseQuery_TooManyCollections(t *testing.T) {
	t.Parallel()
	cols := make([]string, MaxWantedCollections+1)
	for i := range cols {
		// Each "p<i>.x.y.*" is a unique prefix that survives dedupe.
		cols[i] = fmt.Sprintf("p%d.x.y.*", i)
	}
	q := url.Values{"wantedCollections": cols}
	_, err := ParseQuery(q)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidOptions))
	require.True(t, strings.Contains(err.Error(), "too many"))
}

// V1 PARITY: a sloppy list with duplicate collections/prefixes that
// dedupes under the cap is accepted, not rejected. v1 builds the
// dedupe set first (via the FullPaths map) and only then compares to
// the 100 cap; our parser does the same now for both shapes.
func TestParseQuery_DuplicateCollectionsBelowCap_Accepted(t *testing.T) {
	t.Parallel()
	cols := make([]string, MaxWantedCollections+50)
	for i := range cols {
		cols[i] = "app.bsky.feed.post"
	}
	q := url.Values{"wantedCollections": cols}
	f, err := ParseQuery(q)
	require.NoError(t, err)
	require.Len(t, f.wantedCollections.fullPaths, 1)
}

func TestParseQuery_TooManyDIDs(t *testing.T) {
	t.Parallel()
	// Distinct DIDs so dedupe doesn't shrink the set under the cap.
	dids := make([]string, MaxWantedDIDs+1)
	for i := range dids {
		dids[i] = fmt.Sprintf("did:web:host%d.example.com", i)
	}
	q := url.Values{"wantedDids": dids}
	_, err := ParseQuery(q)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidOptions))
	require.True(t, strings.Contains(err.Error(), "too many"))
}

// V1 PARITY: a list that dedupes under the 10_000 cap is accepted.
// v1 builds the DID map first and only checks len(map) > 10_000.
func TestParseQuery_DuplicateDIDsBelowCap_Accepted(t *testing.T) {
	t.Parallel()
	dids := make([]string, MaxWantedDIDs+50)
	for i := range dids {
		dids[i] = "did:plc:eygmaihciaxprqvxpfvl6flk"
	}
	q := url.Values{"wantedDids": dids}
	f, err := ParseQuery(q)
	require.NoError(t, err)
	require.Len(t, f.wantedDIDs, 1)
}

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
		WantedCollections:   []string{"app.bsky.feed.post", "app.bsky.graph.*"},
		WantedDIDs:          []string{"did:plc:eygmaihciaxprqvxpfvl6flk"},
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
	// Unique DIDs so the post-dedupe cap fires.
	dids := make([]string, MaxWantedDIDs+1)
	for i := range dids {
		dids[i] = fmt.Sprintf("did:web:host%d.example.com", i)
	}
	_, err := ParseUpdatePayload(UpdatePayload{WantedDIDs: dids})
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidOptions))
}

// V1 PARITY: empty arrays in options_update mean "disable that filter".
// Locks down the README §Subscriber Sourced messages contract:
// "If either array is empty, the relevant filter will be disabled
// (i.e. sending empty wantedDids will mean a client gets messages
// for all DIDs again)."
func TestParseUpdatePayload_EmptyArraysDisableFilters(t *testing.T) {
	t.Parallel()
	f, err := ParseUpdatePayload(UpdatePayload{
		WantedCollections: []string{},
		WantedDIDs:        []string{},
	})
	require.NoError(t, err)
	require.Nil(t, f.wantedCollections, "empty array must clear collection filter")
	require.Nil(t, f.wantedDIDs, "empty array must clear DID filter")
	// And the resulting filter must accept everything.
	require.True(t, f.Wants(ev(segment.KindCreate, "did:plc:any", "app.bsky.feed.post")))
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

// Helper: build an event with the given kind/did/collection.
func ev(kind segment.Kind, did, collection string) *segment.Event {
	return &segment.Event{Kind: kind, DID: did, Collection: collection}
}

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
				got := parseMaxMsgSizeQuery(tc.in)
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
