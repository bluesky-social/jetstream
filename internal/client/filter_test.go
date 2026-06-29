package client

import (
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func commitRow(seq uint64, did, collection string) segment.Event {
	return segment.Event{Seq: seq, Kind: segment.KindCreate, DID: did, Collection: collection, Rkey: "r"}
}

func TestMatcherNilMatchesAll(t *testing.T) {
	t.Parallel()
	var m *Matcher
	ev := commitRow(5, "did:plc:a", "app.bsky.feed.post")
	keep := m.Wants(&ev)
	require.True(t, keep)
}

func TestMatcherSeqWindow(t *testing.T) {
	t.Parallel()
	m := NewMatcher(PlanRequest{AfterSeq: 10, HasBeforeSeq: true, BeforeSeq: 20})
	for _, tc := range []struct {
		seq  uint64
		want bool
	}{
		{10, false}, // exclusive lower
		{11, true},
		{20, true}, // inclusive upper
		{21, false},
	} {
		ev := commitRow(tc.seq, "did:plc:a", "c")
		require.Equalf(t, tc.want, m.Wants(&ev), "seq %d", tc.seq)
	}
}

// TestMatcherAfterSeqZeroIncludesFirstEvent: seqs start at 1, and
// WithAfterSeq(0) means "from the start of the archive". afterSeq=0 must impose
// NO lower bound so the genuine first event (seq 1) is included, matching the
// server (which omits the wire field entirely when afterSeq is 0).
func TestMatcherAfterSeqZeroIncludesFirstEvent(t *testing.T) {
	t.Parallel()
	m := NewMatcher(PlanRequest{AfterSeq: 0})
	// Seqs start at 1; afterSeq=0 imposes no lower bound, so the first-ever
	// event (seq 1) is included.
	ev1 := commitRow(1, "did:plc:a", "app.bsky.feed.post")
	require.True(t, m.Wants(&ev1), "afterSeq=0 must include the first event at seq 1")
	ev2 := commitRow(2, "did:plc:a", "app.bsky.feed.post")
	require.True(t, m.Wants(&ev2), "afterSeq=0 must include seq 2")
}

// TestMatcherAfterSeqPositiveStaysExclusive guards that a non-zero afterSeq
// keeps its resume-after semantics (seq > afterSeq), so a client resuming from
// a saved cursor never re-receives the event it last saw.
func TestMatcherAfterSeqPositiveStaysExclusive(t *testing.T) {
	t.Parallel()
	m := NewMatcher(PlanRequest{AfterSeq: 1})
	ev0 := commitRow(0, "did:plc:a", "c")
	require.False(t, m.Wants(&ev0), "afterSeq=1 excludes seq 0")
	ev1 := commitRow(1, "did:plc:a", "c")
	require.False(t, m.Wants(&ev1), "afterSeq=1 excludes seq 1 (exclusive)")
	ev2 := commitRow(2, "did:plc:a", "c")
	require.True(t, m.Wants(&ev2), "afterSeq=1 includes seq 2")
}

func TestMatcherDIDFilterAllKinds(t *testing.T) {
	t.Parallel()
	m := NewMatcher(PlanRequest{DIDs: []string{"did:plc:keep"}})

	keep := commitRow(1, "did:plc:keep", "app.bsky.feed.post")
	require.True(t, m.Wants(&keep))

	drop := commitRow(2, "did:plc:other", "app.bsky.feed.post")
	require.False(t, m.Wants(&drop))

	// DID filter applies to non-commit kinds too.
	acct := segment.Event{Seq: 3, Kind: segment.KindAccount, DID: "did:plc:other"}
	require.False(t, m.Wants(&acct))
	acctKeep := segment.Event{Seq: 4, Kind: segment.KindAccount, DID: "did:plc:keep"}
	require.True(t, m.Wants(&acctKeep))
}

func TestMatcherCollectionExactAndWildcard(t *testing.T) {
	t.Parallel()
	m := NewMatcher(PlanRequest{Collections: []string{"app.bsky.feed.post", "app.bsky.graph.*"}})

	for _, tc := range []struct {
		coll string
		want bool
	}{
		{"app.bsky.feed.post", true},    // exact
		{"app.bsky.feed.like", false},   // exact miss
		{"app.bsky.graph.follow", true}, // wildcard prefix
		{"app.bsky.graph.block", true},  // wildcard prefix
		{"app.bsky.graphology", false},  // must not match the dot-trimmed prefix loosely
		{"app.bsky.feed", false},        // partial
	} {
		ev := commitRow(1, "did:plc:a", tc.coll)
		require.Equalf(t, tc.want, m.Wants(&ev), "collection %q", tc.coll)
	}
}

// TestMatcherCollectionFilterDropsIdentityAccount guards the v2 Go-client
// presentation contract (#142): with a collection filter set, identity and
// account events — which carry no collection — are NOT delivered, because a
// collection-scoped subscriber did not ask for them. Sync still passes (it is
// an internal resync signal, not gated by the collection filter). Account
// tombstone folding happens upstream of this matcher, so dropping account here
// does not weaken record suppression.
func TestMatcherCollectionFilterDeliversDIDLevelEvents(t *testing.T) {
	t.Parallel()
	m := NewMatcher(PlanRequest{Collections: []string{"app.bsky.feed.post"}})

	// #account, #identity, #sync carry no collection and always bypass the
	// collection filter — the consumer's only signal to purge a dead account.
	for _, k := range []segment.Kind{segment.KindIdentity, segment.KindAccount, segment.KindSync} {
		ev := segment.Event{Seq: 1, Kind: k, DID: "did:plc:a"}
		require.Truef(t, m.Wants(&ev), "kind %d must bypass the collection filter", k)
	}

	// A commit whose collection does not match is still dropped.
	miss := commitRow(2, "did:plc:a", "app.bsky.feed.like")
	require.False(t, m.Wants(&miss), "non-matching commit must be dropped")

	// A commit with an empty collection bypasses the filter (v1 parity).
	emptyColl := segment.Event{Seq: 3, Kind: segment.KindCreate, DID: "did:plc:a", Collection: ""}
	require.True(t, m.Wants(&emptyColl))
}

// TestMatcherNoCollectionFilterDeliversIdentityAccount guards the other side of
// the contract: with NO collection filter, identity and account events are
// delivered (subject to any DID filter) — "give me the whole stream".
func TestMatcherNoCollectionFilterDeliversIdentityAccount(t *testing.T) {
	t.Parallel()
	m := NewMatcher(PlanRequest{}) // no filters at all

	for _, k := range []segment.Kind{segment.KindIdentity, segment.KindAccount, segment.KindSync} {
		ev := segment.Event{Seq: 1, Kind: k, DID: "did:plc:a"}
		require.Truef(t, m.Wants(&ev), "kind %d must be delivered with no collection filter", k)
	}

	// A DID-only filter still delivers identity/account for the matching DID.
	md := NewMatcher(PlanRequest{DIDs: []string{"did:plc:keep"}})
	keep := segment.Event{Seq: 1, Kind: segment.KindIdentity, DID: "did:plc:keep"}
	require.True(t, md.Wants(&keep), "identity for a matching DID must be delivered")
	drop := segment.Event{Seq: 2, Kind: segment.KindAccount, DID: "did:plc:other"}
	require.False(t, md.Wants(&drop), "account for a non-matching DID must be dropped")
}

func TestMatcherWildcardBoundary(t *testing.T) {
	t.Parallel()
	// "app.bsky.graph.*" trims to prefix "app.bsky.graph." (keeps the dot),
	// so a sibling namespace sharing the textual prefix must not match.
	m := NewMatcher(PlanRequest{Collections: []string{"app.bsky.graph.*"}})
	hit := commitRow(1, "did:plc:a", "app.bsky.graph.follow")
	miss := commitRow(2, "did:plc:a", "app.bsky.graphient.thing")
	require.True(t, m.Wants(&hit))
	require.False(t, m.Wants(&miss))
}
