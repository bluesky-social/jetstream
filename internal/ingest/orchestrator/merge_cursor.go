// merge_cursor.go owns the merge/next_source_idx
// pebble cursor and the atomic per-source commit batch that advances
// the cursor alongside per-DID repo/<did>.Rev refreshes. Spec:
// specs/notes/2026-05-27-merge-phase-design.md §4.5–§4.6.

package orchestrator

import (
	"fmt"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/store"
)

const (
	mergeNextSourceIdxKey      = "merge/next_source_idx"
	mergeCursorV1         byte = 0x01
)

// loadMergeCursor returns the persisted next-source index, or 0 if
// absent.
func loadMergeCursor(s *store.Store) (uint64, error) {
	v, _, err := s.GetVersionedUint64LE(mergeNextSourceIdxKey, mergeCursorV1)
	if err != nil {
		return 0, fmt.Errorf("orchestrator: merge: load cursor: %w", err)
	}
	return v, nil
}

// deleteMergeCursor removes the merge cursor key. Called from the
// terminal cleanup of runMerge once all source segments are drained
// and the live_segments tree has been removed.
func deleteMergeCursor(s *store.Store) error {
	if err := s.Delete([]byte(mergeNextSourceIdxKey), store.SyncWrites); err != nil {
		return fmt.Errorf("orchestrator: merge: delete cursor: %w", err)
	}
	return nil
}

// commitSourceComplete atomically commits, in one pebble batch with
// Sync, the cursor advance to nextIdx and a per-DID repo/<did>.Rev
// + UpdatedAt refresh for every entry in perDIDLastRev. The cache
// is updated in-place on commit success so subsequent sources see
// the new Revs without a fresh pebble read.
//
// Per the spec §4.6: only top-level Rev / UpdatedAt are mutated.
// Backfill.* is preserved byte-for-byte — Backfill.Rev is the
// immutable signal of where initial backfill stopped.
//
// now is parameterized for deterministic tests; production callers
// should pass time.Now().UTC().
func commitSourceComplete(
	s *store.Store,
	cache *repoStatusLookup,
	nextIdx uint64,
	perDIDLastRev map[string]string,
	now time.Time,
) error {
	batch := s.NewBatch()
	defer func() { _ = batch.Close() }()

	cursorVal := store.EncodeVersionedUint64LE(mergeCursorV1, nextIdx)
	if err := batch.Set([]byte(mergeNextSourceIdxKey), cursorVal, nil); err != nil {
		return fmt.Errorf("orchestrator: merge: stage cursor: %w", err)
	}

	// Build the updated RepoStatus rows in-memory first so we can
	// mirror them into the cache atomically with the commit.
	pendingCacheUpdates := make(map[string]*backfill.RepoStatus, len(perDIDLastRev))
	for did, rev := range perDIDLastRev {
		rs, err := cache.get(did)
		if err != nil {
			return err
		}
		if rs == nil {
			// No pre-existing repo/<did> row. Writing a fresh row here
			// would produce Backfill.Status="" (zero value, not in the
			// Status enum) — corrupting the row's contract with
			// backfill.Store.Lookup.
			//
			// The post-merge discovery step (§4.7) writes a proper
			// StatusFailed row for these DIDs via maybeWriteDiscoveredRow.
			// Steady-state retry then re-downloads the repo and
			// OnComplete writes the authoritative Rev.
			//
			// Skip the Rev refresh; the cursor still advances.
			continue
		}
		next := *rs
		next.Rev = rev
		next.UpdatedAt = now.UTC()
		enc, err := backfill.EncodeRepoStatus(&next)
		if err != nil {
			return fmt.Errorf("orchestrator: merge: encode repo/%s: %w", did, err)
		}
		if err := batch.Set(backfill.RepoKey(did), enc, nil); err != nil {
			return fmt.Errorf("orchestrator: merge: stage repo/%s: %w", did, err)
		}
		updated := next
		pendingCacheUpdates[did] = &updated
	}

	if err := s.Commit(batch, store.SyncWrites); err != nil {
		return fmt.Errorf("orchestrator: merge: commit batch: %w", err)
	}
	for did, rs := range pendingCacheUpdates {
		cache.set(did, rs)
	}
	return nil
}
