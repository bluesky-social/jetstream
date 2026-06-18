// Package orchestrator: merge_filter.go owns the per-event keep/drop
// predicate that decides whether a live_segments survivor should be
// promoted into the steady-state segments tree, plus the per-DID
// repo/<did> lookup cache that backs it. Spec:
// docs/superpowers/specs/2026-05-27-merge-phase-design.md §4.3–§4.4.
package orchestrator

import (
	"errors"
	"fmt"

	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
)

// isCommitKind reports whether k is one of the three commit-shaped
// event kinds (DESIGN.md §3.2). Only these carry a per-event Rev
// that maps to the repo MST and can therefore be filtered against
// repo/<did>.Backfill.Rev.
func isCommitKind(k segment.Kind) bool {
	return k.IsCommit()
}

func isBackfillRevFilteredKind(k segment.Kind) bool {
	return isCommitKind(k) || k == segment.KindSync
}

// shouldKeep returns true unless the event is a commit-shaped row
// whose data is already covered by the backfill writer's authoritative
// per-DID write. Rev-stamped KindSync rows are included because their
// resync replacement records are superseded by the same backfill rev.
//
// Cross-component dependency: this predicate's correctness leans on
// internal/ingest/backfill/handler.go stamping commit.Rev (the head
// rev of the repo at download time) onto every synthetic Create
// event. If that handler ever switches to per-record commit revs,
// this predicate's BackfillRev comparison stops being a coherent
// watermark for the whole repo and would need to be reworked.
func shouldKeep(ev *segment.Event, st *backfill.RepoStatus) bool {
	if !isBackfillRevFilteredKind(ev.Kind) {
		return true
	}
	if st == nil {
		return true
	}
	if st.Backfill.Status != backfill.StatusComplete {
		return true
	}
	if st.Backfill.Rev == "" || ev.Rev == "" {
		return true
	}
	// TIDs are designed to sort lexicographically (atproto rev spec).
	return ev.Rev > st.Backfill.Rev
}

// repoStatusLookup memoizes per-DID repo/<did> reads for a single
// merge run. Pebble I/O failures (other than ErrNotFound) latch a
// sticky error on the cache; subsequent lookups return it.
type repoStatusLookup struct {
	store     *store.Store
	cache     map[string]*backfill.RepoStatus
	stickyErr error
	onLookup  func() // called on first-time-seen DIDs (for metrics); nil-safe
}

// newRepoStatusLookup builds an empty cache. onLookup is invoked once
// per first-time-seen DID so callers can wire a metric.
func newRepoStatusLookup(s *store.Store, onLookup func()) *repoStatusLookup {
	return &repoStatusLookup{
		store:    s,
		cache:    make(map[string]*backfill.RepoStatus),
		onLookup: onLookup,
	}
}

// get returns the cached or freshly-read RepoStatus for did.
// Missing rows cache a nil so repeated misses don't re-hit pebble.
// Returns the sticky error on every call once one has been latched.
//
// The returned pointer aliases the cache: mutating its fields would
// mutate the value future get calls see. Merge-phase callers must
// not mutate the returned pointer; commitSourceComplete builds a
// fresh RepoStatus value from the cached one and calls set after
// each pebble write so the cache tracks the latest Rev.
func (l *repoStatusLookup) get(did string) (*backfill.RepoStatus, error) {
	if l.stickyErr != nil {
		return nil, l.stickyErr
	}
	if rs, ok := l.cache[did]; ok {
		return rs, nil
	}
	if l.onLookup != nil {
		l.onLookup()
	}

	val, closer, err := l.store.Get(backfill.RepoKey(did))
	if errors.Is(err, store.ErrNotFound) {
		l.cache[did] = nil
		return nil, nil
	}
	if err != nil {
		l.stickyErr = fmt.Errorf("orchestrator: merge: lookup repo/%s: %w", did, err)
		return nil, l.stickyErr
	}
	defer func() { _ = closer.Close() }()

	rs, err := backfill.DecodeRepoStatus(val)
	if err != nil {
		l.stickyErr = fmt.Errorf("orchestrator: merge: decode repo/%s: %w", did, err)
		return nil, l.stickyErr
	}
	l.cache[did] = rs
	return rs, nil
}

// set replaces the cached entry. Called by commitSourceComplete
// (Task 8) after a successful pebble batch so subsequent sources
// see the updated Rev without a fresh pebble read.
func (l *repoStatusLookup) set(did string, rs *backfill.RepoStatus) {
	l.cache[did] = rs
}
