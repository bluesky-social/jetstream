package client

import "github.com/bluesky-social/jetstream/segment"

// rowSelector combines exact filtering (Matcher) with tombstone suppression
// (Suppressor) into the single keep/drop predicate the downloader applies
// before decode. A row is kept only if it passes the filter AND is not
// suppressed. Either component may be nil (that dimension keeps everything).
type rowSelector struct {
	matcher    *Matcher
	suppressor *Suppressor
}

// newRowSelector builds the combined selector. Either argument may be nil.
func newRowSelector(m *Matcher, s *Suppressor) *rowSelector {
	return &rowSelector{matcher: m, suppressor: s}
}

// Keep reports whether ev should be emitted. Filtering is checked first (it is
// cheaper and prunes most rows), then suppression. The drop reason is
// "filtered" for an exact-filter miss or the tombstone reason for a
// suppressed row.
func (s *rowSelector) Keep(ev *segment.Event) (bool, string) {
	if s.matcher != nil && !s.matcher.Wants(ev) {
		return false, "filtered"
	}
	if s.suppressor != nil {
		if drop, reason := s.suppressor.ShouldDrop(ev); drop {
			return false, reason
		}
	}
	return true, ""
}
