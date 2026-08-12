package jetstream

import (
	"strings"

	"github.com/bluesky-social/jetstream/segment"
)

// matcher applies the caller's exact kind/DID/collection/seq filters to decoded
// segment rows. The snapshot planner is a one-sided transport hint (no false
// negatives, possible false positives via DID blooms and per-block collection
// summaries), so the client MUST re-apply exact filtering after decode.
//
// The presentation contract matches the server's /subscribe wire policy:
//
//   - Kind and DID filters apply independently to all events.
//   - With a collection filter set: only commit events whose collection matches
//     are delivered. A commit with an empty collection still bypasses the
//     filter. #account, #identity, and #sync — the DID-level events, which carry
//     no collection — always bypass the collection filter (subject to the DID
//     filter), because they are the consumer's only signal to purge a dead
//     account's records; hiding them would create a permanently stale view.
//   - With no collection filter: every kind is delivered (subject to the DID
//     filter), matching "give me the whole stream".
//
// The seq window is the client's exact (afterSeq, beforeSeq] bound, applied on
// top of the planner's coarse per-segment/block seq pruning.
type matcher struct {
	kinds        map[Kind]struct{}   // nil = match all kinds
	dids         map[string]struct{} // nil = match all DIDs
	fullPaths    map[string]struct{} // exact collection NSIDs
	prefixes     []string            // wildcard collection prefixes ("app.bsky.feed.")
	afterSeq     uint64              // exclusive lower bound
	hasBeforeSeq bool
	beforeSeq    uint64 // inclusive upper bound
}

// newMatcher builds a matcher from resolved filters. Empty kinds/dids/
// collections mean match-all for that dimension. Collection entries are exact NSIDs
// or namespace wildcards ending in ".*" (e.g. "app.bsky.feed.*").
func newMatcher(req planRequest) *matcher {
	m := &matcher{
		afterSeq:     req.AfterSeq,
		hasBeforeSeq: req.HasBeforeSeq,
		beforeSeq:    req.BeforeSeq,
	}
	if len(req.Kinds) > 0 {
		m.kinds = make(map[Kind]struct{}, len(req.Kinds))
		for _, kind := range req.Kinds {
			m.kinds[kind] = struct{}{}
		}
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

// wantsSegment reports whether a stored row passes the exact filters.
func (m *matcher) wantsSegment(ev *segment.Event) bool {
	return m.wants(ev.Seq, ev.DID, publicKind(ev.Kind), ev.Collection)
}

// wantsEvent reports whether a decoded live event passes the exact filters.
func (m *matcher) wantsEvent(ev *Event) bool {
	isCommit := ev.Kind == KindCommit
	collection := ""
	if isCommit && ev.Commit != nil {
		collection = ev.Commit.Collection
	}
	return m.wants(ev.Seq, ev.DID, ev.Kind, collection)
}

func (m *matcher) wants(seq uint64, did string, kind Kind, collection string) bool {
	if m == nil {
		return true
	}
	if !m.wantsSeq(seq) {
		return false
	}
	if m.kinds != nil {
		if _, ok := m.kinds[kind]; !ok {
			return false
		}
	}
	if m.dids != nil {
		if _, ok := m.dids[did]; !ok {
			return false
		}
	}
	if !m.hasCollectionFilter() {
		return true
	}
	// DID-level events (#account, #identity, #sync) carry no collection and
	// always bypass the collection filter, subject to the DID filter applied
	// above. They are the consumer's only signal to purge a dead account's
	// records, so hiding them under a collection filter would create a
	// permanently stale view (see the type doc).
	if kind != KindCommit {
		return true
	}
	// A commit lacking a collection bypasses the filter (v1 parity).
	if collection == "" {
		return true
	}
	if _, ok := m.fullPaths[collection]; ok {
		return true
	}
	for _, prefix := range m.prefixes {
		if strings.HasPrefix(collection, prefix) {
			return true
		}
	}
	return false
}

func publicKind(kind segment.Kind) Kind {
	if kind.IsCommit() {
		return KindCommit
	}
	switch kind {
	case segment.KindIdentity:
		return KindIdentity
	case segment.KindAccount:
		return KindAccount
	case segment.KindSync:
		return KindSync
	default:
		return ""
	}
}

// setAfterSeq raises the matcher's exclusive lower bound to afterSeq. It is used
// on a §14 re-backfill: the sweep re-runs planSnapshot from the last
// durably-processed seq (the live tail's highest delivered seq), and the matcher
// must track that resume point so the one work unit that STRADDLES it (admitted
// whole under the planner's one-sided contract) has its already-delivered rows
// dropped before decode rather than re-emitted out of order. Only the seq floor
// moves; the kind/DID/collection filters and the (user-supplied) beforeSeq bound are
// untouched. The bound only ever moves FORWARD across a backfill loop's life
// (resume >= cutover >= the prior floor), so this never widens the window. See
// the call site in runBackfillThenLive for the full scope/safety argument.
func (m *matcher) setAfterSeq(afterSeq uint64) {
	m.afterSeq = afterSeq
}

func (m *matcher) wantsSeq(seq uint64) bool {
	// afterSeq is a RESUME-AFTER bound (seq > afterSeq), but only when one was
	// actually requested. afterSeq==0 means "from the start of the archive"
	// (WithAfterSeq(0)). Seqs start at 1, so afterSeq==0 imposes no lower bound
	// and the first-ever event (seq 1) is included, matching the server (which
	// omits the wire field and applies no bound when afterSeq is 0). The
	// afterSeq>0 gate keeps that 0-imposes-nothing behavior.
	if m.afterSeq > 0 && seq <= m.afterSeq {
		return false
	}
	if m.hasBeforeSeq && seq > m.beforeSeq {
		return false
	}
	return true
}

func (m *matcher) hasCollectionFilter() bool {
	return len(m.fullPaths) > 0 || len(m.prefixes) > 0
}
