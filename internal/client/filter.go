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
// The presentation contract matches the server's /subscribe wire policy:
//
//   - DID filter applies to all event kinds.
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
	// DID-level events (#account, #identity, #sync) carry no collection and
	// always bypass the collection filter, subject to the DID filter applied
	// above. They are the consumer's only signal to purge a dead account's
	// records, so hiding them under a collection filter would create a
	// permanently stale view (see the type doc).
	if !ev.Kind.IsCommit() {
		return true
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

// Keep makes *Matcher satisfy the downloader's RowSelector: a row is kept iff it
// passes the exact filters. The drop reason is "filtered" for a filter miss.
// Backfill no longer suppresses tombstoned rows — every matching row is emitted
// and a folding consumer converges (design §5.1) — so the matcher is the whole
// keep/drop decision.
func (m *Matcher) Keep(ev *segment.Event) (bool, string) {
	if m.Wants(ev) {
		return true, ""
	}
	return false, "filtered"
}

func (m *Matcher) wantsSeq(seq uint64) bool {
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

func (m *Matcher) hasCollectionFilter() bool {
	return len(m.fullPaths) > 0 || len(m.prefixes) > 0
}
