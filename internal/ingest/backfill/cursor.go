// Package backfill: cursor.go persists the relay's listRepos resume
// cursor in pebble so a process restart can skip listRepos pages
// already fully processed in a prior Run.
//
// The cursor is opaque per the atproto spec — treat it as a string
// of bytes the relay handed us, valid only against the same relay.
// Cross-relay cursors are undefined behavior; operators changing
// --relay-url between runs should clear this key (or rebuild the
// data dir).
//
// # Persistence semantics
//
// SaveListReposCursor uses pebble.Sync, same as the per-DID write
// path. atmos calls our save callback after every listRepos page
// boundary, so the cost is one fsync per ~1000 DIDs (the protocol's
// page cap). Cheap relative to repo download.
//
// # Known durability hole
//
// atmos fires OnPageComplete after a page's eligible jobs are
// queued onto the worker channel — workers may still be downloading.
// On a process kill mid-page-flush, those workers' DIDs stay at
// StateDiscovered. The next Run starts at the saved cursor (page
// N+1) and never re-walks page N, so those DIDs are stuck until a
// future cursor-less Run rediscovers them.
//
// This is acceptable for now: a future "rewalk" subcommand can
// clear the cursor to force a full re-enumeration. In practice the
// hole only bites if every subsequent Run also dies in the same
// way, which would have bigger problems than orphaned DIDs.
package backfill

import (
	"errors"
	"fmt"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/cockroachdb/pebble"
)

// listReposCursorKey is the pebble key for the persisted listRepos
// resume cursor. Singleton — operators changing relays accept the
// cross-relay opaque-cursor risk.
const listReposCursorKey = "relay/list_repos_cursor"

// LoadListReposCursor reads the persisted cursor from pebble. Returns
// "" if no cursor has been saved (a fresh data dir, or the final
// post-drain page that wrote ""). Errors only on pebble I/O failure.
func LoadListReposCursor(db *store.Store) (string, error) {
	val, closer, err := db.Get([]byte(listReposCursorKey))
	if errors.Is(err, pebble.ErrNotFound) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("backfill: load list_repos_cursor: %w", err)
	}
	defer func() { _ = closer.Close() }()

	// Copy out before closing the buffer — pebble's docs require it.
	out := string(val)
	return out, nil
}

// SaveListReposCursor durably persists the cursor for resume. Used as
// the body of atmos's OnPageComplete callback; the synchronous fsync
// guarantees a crash after the page completes can't lose the advance.
func SaveListReposCursor(db *store.Store, cursor string) error {
	if err := db.Set([]byte(listReposCursorKey), []byte(cursor), store.SyncWrites); err != nil {
		return fmt.Errorf("backfill: save list_repos_cursor: %w", err)
	}
	return nil
}
