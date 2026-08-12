package subscribe

import (
	"net/url"
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func mustParseV2(t *testing.T, rawQuery string) *FilterV2 {
	t.Helper()
	q, err := url.ParseQuery(rawQuery)
	require.NoError(t, err)
	f, err := ParseQueryV2(q)
	require.NoError(t, err)
	return f
}

func v2Evt(kind segment.Kind, did, collection string) *segment.Event {
	return &segment.Event{Kind: kind, DID: did, Collection: collection}
}

// TestFilterV2_CompositionTable is the truth table from the design note's
// composition examples (§5): every row of the consumer-facing story.
func TestFilterV2_CompositionTable(t *testing.T) {
	t.Parallel()

	// The event population: X's like commit, X's follow commit, Y's like
	// commit, X's identity, X's account, Y's identity, X's sync.
	xLike := v2Evt(segment.KindCreate, "did:plc:x", "app.bsky.feed.like")
	xFollow := v2Evt(segment.KindCreate, "did:plc:x", "app.bsky.graph.follow")
	yLike := v2Evt(segment.KindCreate, "did:plc:y", "app.bsky.feed.like")
	xIdent := v2Evt(segment.KindIdentity, "did:plc:x", "")
	xAcct := v2Evt(segment.KindAccount, "did:plc:x", "")
	yIdent := v2Evt(segment.KindIdentity, "did:plc:y", "")
	xSync := v2Evt(segment.KindSync, "did:plc:x", "")
	all := []*segment.Event{xLike, xFollow, yLike, xIdent, xAcct, yIdent, xSync}

	for _, tc := range []struct {
		name  string
		query string
		want  []*segment.Event
	}{
		{"one big stream", "", all},
		{"kinds=commit", "kinds=commit", []*segment.Event{xLike, xFollow, yLike}},
		{"collections only keeps everyone's DID-level events", "collections=app.bsky.feed.like",
			[]*segment.Event{xLike, yLike, xIdent, xAcct, yIdent, xSync}},
		{"kinds=commit&collections: the commits-only stream", "kinds=commit&collections=app.bsky.feed.like",
			[]*segment.Event{xLike, yLike}},
		{"dids: every event about X", "dids=did:plc:x",
			[]*segment.Event{xLike, xFollow, xIdent, xAcct, xSync}},
		{"dids+kinds: X's account/identity only", "dids=did:plc:x&kinds=account&kinds=identity",
			[]*segment.Event{xIdent, xAcct}},
		{"dids+collections: X's follows + X's DID-level events", "dids=did:plc:x&collections=app.bsky.graph.follow",
			[]*segment.Event{xFollow, xIdent, xAcct, xSync}},
		{"kinds=sync", "kinds=sync", []*segment.Event{xSync}},
		{"collection prefix pattern", "kinds=commit&collections=app.bsky.graph.*",
			[]*segment.Event{xFollow}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := mustParseV2(t, tc.query)
			var got []*segment.Event
			for _, e := range all {
				if f.Wants(e) {
					got = append(got, e)
				}
			}
			require.Equal(t, tc.want, got)
		})
	}
}

// TestFilterV2_Orthogonality is the property pin: for any event and any
// filter, Wants equals the conjunction of the three independently-
// evaluated axis predicates. This pins the composition law itself, not
// just examples — a future change that couples two axes (v1's implicit
// collections⇒kind coupling, say) fails here on some combination.
func TestFilterV2_Orthogonality(t *testing.T) {
	t.Parallel()

	kinds := []segment.Kind{
		segment.KindCreate, segment.KindUpdate, segment.KindDelete,
		segment.KindCreateResync, segment.KindIdentity, segment.KindAccount,
		segment.KindSync, segment.Kind(99), // 99: a future kind this build can't classify
	}
	dids := []string{"did:plc:x", "did:plc:y"}
	collections := []string{"app.bsky.feed.like", "app.bsky.graph.follow", ""}

	kindsAxes := []string{"", "kinds=commit", "kinds=identity&kinds=sync", "kinds=commit&kinds=account"}
	didAxes := []string{"", "dids=did:plc:x"}
	colAxes := []string{"", "collections=app.bsky.feed.like", "collections=app.bsky.graph.*"}

	kindSel := func(query string, k segment.Kind) bool {
		f := mustParseV2(t, query)
		return !f.kindsSet || f.kinds&kindClassOf(k) != 0
	}
	didSel := func(query, did string) bool {
		f := mustParseV2(t, query)
		if f.dids == nil {
			return true
		}
		_, ok := f.dids[did]
		return ok
	}
	colSel := func(query string, k segment.Kind, col string) bool {
		f := mustParseV2(t, query)
		if f.collections == nil || !k.IsCommit() || col == "" {
			return true
		}
		return f.collections.matches(col)
	}

	for _, ka := range kindsAxes {
		for _, da := range didAxes {
			for _, ca := range colAxes {
				// Skip the combination ParseQueryV2 rejects as inert.
				if ca != "" && ka != "" && !mustParseV2(t, ka).hasCommit() {
					continue
				}
				query := joinQuery(ka, da, ca)
				f := mustParseV2(t, query)
				for _, k := range kinds {
					for _, did := range dids {
						for _, col := range collections {
							evt := v2Evt(k, did, col)
							want := kindSel(ka, k) && didSel(da, did) && colSel(ca, k, col)
							require.Equal(t, want, f.Wants(evt),
								"query=%q kind=%d did=%s col=%q", query, k, did, col)
						}
					}
				}
			}
		}
	}
}

func (f *FilterV2) hasCommit() bool { return f.kinds&kindMaskCommit != 0 }

func joinQuery(parts ...string) string {
	out := ""
	for _, p := range parts {
		if p == "" {
			continue
		}
		if out != "" {
			out += "&"
		}
		out += p
	}
	return out
}

func TestParseQueryV2_Validation(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		query   string
		wantErr string
	}{
		{"unknown kind", "kinds=likes", `unknown kind "likes"`},
		{"empty kind value", "kinds=", `unknown kind ""`},
		{"case-variant kind", "kinds=Commit", `unknown kind "Commit"`},
		{"inert collections: kinds excludes commit", "kinds=identity&collections=app.bsky.feed.like",
			"collections filter can never apply"},
		{"legacy wantedDids tombstone", "wantedDids=did:plc:x", "use dids"},
		{"legacy wantedCollections tombstone", "wantedCollections=app.bsky.feed.like", "use collections"},
		{"requireHello tombstone", "requireHello=true", "server-push only"},
		{"invalid DID", "dids=not-a-did", "invalid DID"},
		{"invalid collection", "collections=not_an_nsid", "invalid collection"},
		{"too many kinds", "kinds=commit&kinds=commit&kinds=commit&kinds=commit&kinds=commit", "too many kinds"},
		{"empty max message size", "maxMessageSizeBytes=", "maxMessageSizeBytes"},
		{"malformed max message size", "maxMessageSizeBytes=garbage", "maxMessageSizeBytes"},
		{"negative max message size", "maxMessageSizeBytes=-1", "maxMessageSizeBytes"},
		{"overflowing max message size", "maxMessageSizeBytes=4294967296", "maxMessageSizeBytes"},
		{"repeated max message size", "maxMessageSizeBytes=1&maxMessageSizeBytes=2", "maxMessageSizeBytes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			q, err := url.ParseQuery(tc.query)
			require.NoError(t, err)
			_, perr := ParseQueryV2(q)
			require.Error(t, perr)
			require.ErrorIs(t, perr, ErrInvalidOptions)
			require.Contains(t, perr.Error(), tc.wantErr)
		})
	}
}

func TestParseQueryV2_ValidCombinations(t *testing.T) {
	t.Parallel()

	// collections with kinds UNSET composes fine (commits are in scope).
	f := mustParseV2(t, "collections=app.bsky.feed.like")
	require.True(t, f.Wants(v2Evt(segment.KindIdentity, "did:plc:x", "")))

	// collections + kinds INCLUDING commit is fine.
	f = mustParseV2(t, "kinds=commit&kinds=identity&collections=app.bsky.feed.like")
	require.True(t, f.Wants(v2Evt(segment.KindIdentity, "did:plc:x", "")))
	require.False(t, f.Wants(v2Evt(segment.KindAccount, "did:plc:x", "")))

	// Duplicate kinds values dedupe via the mask.
	f = mustParseV2(t, "kinds=commit&kinds=commit&kinds=commit")
	require.True(t, f.Wants(v2Evt(segment.KindCreate, "did:plc:x", "app.bsky.feed.like")))
	require.False(t, f.Wants(v2Evt(segment.KindIdentity, "did:plc:x", "")))

	// V2 rejects malformed values rather than silently disabling the cap.
	f = mustParseV2(t, "maxMessageSizeBytes=1024")
	require.Equal(t, uint32(1024), f.MaxMessageSizeBytes())
}

func TestFilterV2_NilAndEdgeSemantics(t *testing.T) {
	t.Parallel()

	// A nil filter is defensively match-all.
	var nilFilter *FilterV2
	require.True(t, nilFilter.Wants(v2Evt(segment.KindCreate, "did:plc:x", "app.bsky.feed.like")))
	require.Equal(t, uint32(0), nilFilter.MaxMessageSizeBytes())

	// An empty-collection commit bypasses the collections filter (never
	// silently drop data the filter can't classify).
	f := mustParseV2(t, "collections=app.bsky.feed.like")
	require.True(t, f.Wants(v2Evt(segment.KindCreate, "did:plc:x", "")))

	// A future kind class: unset kinds axis passes it, any explicit
	// selection drops it (it cannot have been selected).
	future := v2Evt(segment.Kind(99), "did:plc:x", "")
	require.True(t, mustParseV2(t, "").Wants(future))
	require.False(t, mustParseV2(t, "kinds=commit&kinds=identity&kinds=account&kinds=sync").Wants(future))

	// dids applies to every kind, including sync.
	f = mustParseV2(t, "dids=did:plc:x")
	require.True(t, f.Wants(v2Evt(segment.KindSync, "did:plc:x", "")))
	require.False(t, f.Wants(v2Evt(segment.KindSync, "did:plc:y", "")))
}

func TestParseKinds_TooManyValues(t *testing.T) {
	t.Parallel()
	// The lexicon declares maxLength: 4 on kinds; the 5th raw value is a
	// 400 even when every value is a duplicate the mask would dedupe.
	values := []string{"commit", "commit", "commit", "commit", "commit"}
	_, _, err := parseKinds(values)
	require.Error(t, err)
	require.Contains(t, err.Error(), "too many kinds")
}

func TestParseQueryV2_LexiconMaxLengths(t *testing.T) {
	t.Parallel()

	// dids: the raw repeated-param count is capped at the lexicon's
	// maxLength (10000) BEFORE the shared v1 dedupe-then-cap parser runs —
	// duplicates don't buy a v2 client any slack.
	q := url.Values{}
	for range MaxWantedDIDs + 1 {
		q.Add("dids", "did:web:same.example.com")
	}
	_, err := ParseQueryV2(q)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidOptions)
	require.Contains(t, err.Error(), "too many dids")

	// collections: same raw-count rule at maxLength 100.
	q = url.Values{}
	for range MaxWantedCollections + 1 {
		q.Add("collections", "app.bsky.feed.like")
	}
	_, err = ParseQueryV2(q)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidOptions)
	require.Contains(t, err.Error(), "too many collections")

	// At exactly the cap, duplicates still parse fine.
	q = url.Values{}
	for range MaxWantedCollections {
		q.Add("collections", "app.bsky.feed.like")
	}
	_, err = ParseQueryV2(q)
	require.NoError(t, err)
}

func TestParseQueryV2_WildcardHeadValidation(t *testing.T) {
	t.Parallel()

	// v2 rejects a wildcard whose head is not a valid NSID prefix (v1
	// deliberately accepts these for wire parity and silently never
	// matches; the v2 lexicon promises a 400 instead).
	for _, bad := range []string{"not_an_nsid.*", "single.*", "-bad.example.*"} {
		q, err := url.ParseQuery("collections=" + bad)
		require.NoError(t, err)
		_, perr := ParseQueryV2(q)
		require.Error(t, perr, "wildcard %q must be rejected", bad)
		require.ErrorIs(t, perr, ErrInvalidOptions)
		require.Contains(t, perr.Error(), "invalid collection wildcard")
	}

	// Valid two-segment authority heads keep working (the documented
	// "app.bsky.*" shape).
	f := mustParseV2(t, "collections=app.bsky.*")
	require.True(t, f.Wants(v2Evt(segment.KindCreate, "did:plc:x", "app.bsky.feed.like")))
	require.False(t, f.Wants(v2Evt(segment.KindCreate, "did:plc:x", "com.example.thing")))
}
