package client

import (
	"strings"

	"github.com/bluesky-social/jetstream/segment"
)

// Matcher applies the caller's exact DID/collection/seq filters to decoded
// segment rows. The backfill planner is a one-sided transport hint (no false
// negatives, possible false positives via DID blooms and per-block collection
// summaries), so the client MUST re-apply exact filtering after decode.
//
// The presentation contract is the v2 Go-client contract (NOT v1 wire parity):
// a subscriber that asked for specific collections wants only those record
// types, so account and identity events — which carry no collection — are
// dropped when a collection filter is set. With no collection filter they flow
// (subject to the DID filter). Concretely:
//
//   - DID filter applies to all event kinds.
//   - With a collection filter set: only commit events whose collection matches
//     are delivered; identity and account events are dropped (they carry no
//     collection). A commit with an empty collection still bypasses the filter.
//   - With no collection filter: identity and account events are delivered
//     (subject to the DID filter), matching "give me the whole stream".
//   - Sync events bypass the collection filter regardless: they are an internal
//     resync signal, never surfaced as a user record.
//
// IMPORTANT: this filter governs what is DELIVERED to the consumer, not what
// the engine observes internally. Account-deletion tombstones are folded into
// the Suppressor BEFORE this matcher runs (liveSink.onLive observes the
// tombstone, then calls wantLive), so dropping an account event here never
// weakens the client's record-deletion guarantee.
//
// The seq window is the client's exact (afterSeq, beforeSeq] bound, applied on
// top of the planner's coarse per-segment/block seq pruning.
type Matcher struct {
	dids         map[string]struct{} // nil = match all DIDs
	fullPaths    map[string]struct{} // exact collection NSIDs
	prefixes     []string            // wildcard collection prefixes ("app.bsky.feed.")
	afterSeq     uint64              // exclusive lower bound
	hasBeforeSeq bool
	beforeSeq    uint64 // inclusive upper bound
}

// NewMatcher builds a Matcher from resolved filters. Empty dids/collections
// mean match-all for that dimension. Collection entries are either exact NSIDs
// or namespace wildcards ending in ".*" (e.g. "app.bsky.feed.*").
func NewMatcher(req PlanRequest) *Matcher {
	m := &Matcher{
		afterSeq:     req.AfterSeq,
		hasBeforeSeq: req.HasBeforeSeq,
		beforeSeq:    req.BeforeSeq,
	}
	if len(req.DIDs) > 0 {
		m.dids = make(map[string]struct{}, len(req.DIDs))
		for _, d := range req.DIDs {
			m.dids[d] = struct{}{}
		}
	}
	if len(req.Collections) > 0 {
		m.fullPaths = make(map[string]struct{})
		for _, c := range req.Collections {
			if strings.HasSuffix(c, ".*") {
				// Trim only the trailing "*", keeping the dot, so
				// "app.bsky.feed.*" matches "app.bsky.feed."-prefixed NSIDs.
				m.prefixes = append(m.prefixes, strings.TrimSuffix(c, "*"))
				continue
			}
			m.fullPaths[c] = struct{}{}
		}
	}
	return m
}

// Wants reports whether ev passes the exact filters. A nil Matcher matches
// everything.
func (m *Matcher) Wants(ev *segment.Event) bool {
	if m == nil {
		return true
	}
	if !m.wantsSeq(ev.Seq) {
		return false
	}
	if m.dids != nil {
		if _, ok := m.dids[ev.DID]; !ok {
			return false
		}
	}
	if !m.hasCollectionFilter() {
		return true
	}
	// Sync is an internal resync signal, never gated by the collection filter.
	if ev.Kind == segment.KindSync {
		return true
	}
	// With a collection filter set, identity and account events (which carry no
	// collection) are not what a collection-scoped subscriber asked for. Drop
	// them from delivery. Tombstone folding for account-deletes already happened
	// upstream (see the type doc), so this does not affect record suppression.
	if !ev.Kind.IsCommit() {
		return false
	}
	// A commit lacking a collection bypasses the filter (v1 parity).
	if ev.Collection == "" {
		return true
	}
	if _, ok := m.fullPaths[ev.Collection]; ok {
		return true
	}
	for _, prefix := range m.prefixes {
		if strings.HasPrefix(ev.Collection, prefix) {
			return true
		}
	}
	return false
}

func (m *Matcher) wantsSeq(seq uint64) bool {
	// afterSeq is a RESUME-AFTER bound (seq > afterSeq), but only when one was
	// actually requested. afterSeq==0 means "from the start of the archive"
	// (WithAfterSeq(0)), and jetstream's seq space is 0-based — the first-ever
	// event is seq 0 — so a bare seq <= afterSeq check would drop that first
	// event. Gate on afterSeq>0 so 0 imposes no lower bound, matching the server
	// (which omits the wire field and applies no bound when afterSeq is 0). See
	// #111.
	if m.afterSeq > 0 && seq <= m.afterSeq {
		return false
	}
	if m.hasBeforeSeq && seq > m.beforeSeq {
		return false
	}
	return true
}

func (m *Matcher) hasCollectionFilter() bool {
	return len(m.fullPaths) > 0 || len(m.prefixes) > 0
}
