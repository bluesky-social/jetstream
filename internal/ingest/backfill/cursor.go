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
// Run checkpoints this cursor from atmos's OnBatchComplete callback. atmos
// fires that callback after the batch has been fully reconciled and
// every eligible repo in the batch has reached a terminal state for
// this run. Before checkpointing, Run drains writer durability so
// queued repo completions and their segment data are durably visible.
// The checkpoint uses pebble.Sync, so a successful callback means the
// cursor advance is durable too.
package backfill

import (
	"errors"
	"fmt"

	"github.com/bluesky-social/jetstream/internal/store"
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
	if errors.Is(err, store.ErrNotFound) {
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

// SaveListReposCursor durably persists the cursor for resume via
// pebble.Sync. This is now a test/seed helper only: Run's production
// checkpoint path uses SaveListReposCheckpoint, which writes the relay
// and bootstrap cursors atomically after the writer durability drain.
// Tests that only need to seed a starting cursor still call this.
func SaveListReposCursor(db *store.Store, cursor string) error {
	if err := db.Set([]byte(listReposCursorKey), []byte(cursor), store.SyncWrites); err != nil {
		return fmt.Errorf("backfill: save list_repos_cursor: %w", err)
	}
	return nil
}

// SaveListReposCheckpoint atomically persists the relay resume cursor
// and, when non-empty, the bootstrap-last cursor. These two keys are
// one logical checkpoint during Run: the relay cursor must not advance
// independently of the bootstrap cursor the merge phase will use.
func SaveListReposCheckpoint(db *store.Store, relayCursor, bootstrapCursor string) error {
	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()

	if err := batch.Set([]byte(listReposCursorKey), []byte(relayCursor), nil); err != nil {
		return fmt.Errorf("backfill: stage list_repos_cursor: %w", err)
	}
	if bootstrapCursor != "" {
		if err := batch.Set([]byte(bootstrapLastListReposCursorKey), []byte(bootstrapCursor), nil); err != nil {
			return fmt.Errorf("backfill: stage bootstrap_last_listrepos_cursor: %w", err)
		}
	}
	if err := db.Commit(batch, store.SyncWrites); err != nil {
		return fmt.Errorf("backfill: save list_repos checkpoint: %w", err)
	}
	return nil
}

// bootstrapLastListReposCursorKey is the pebble key carrying the
// last *non-empty* listRepos cursor saved during the bootstrap
// phase. The merge phase reads this to resume listRepos against
// the relay and discover DIDs born during the bootstrap window
// (merge-phase spec §4.7,
// docs/superpowers/specs/2026-05-27-merge-phase-design.md).
//
// We need a separate key from listReposCursorKey because the
// existing cursor is allowed (correctly) to drain to "" when
// listRepos completes — that's how the resume path knows to start
// from the beginning on the next Run. The merge phase needs the
// last meaningful cursor, not the post-drain empty value.
const bootstrapLastListReposCursorKey = "bootstrap/last_listrepos_cursor"

// LoadBootstrapLastListReposCursor returns the saved bootstrap-phase
// last-non-empty listRepos cursor, or "" if absent (debug short-
// circuit runs that never paged past page 1, or a fresh data dir).
func LoadBootstrapLastListReposCursor(db *store.Store) (string, error) {
	val, closer, err := db.Get([]byte(bootstrapLastListReposCursorKey))
	if errors.Is(err, store.ErrNotFound) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("backfill: load bootstrap_last_listrepos_cursor: %w", err)
	}
	defer func() { _ = closer.Close() }()
	return string(val), nil
}

// MaybeSaveBootstrapLastListReposCursor writes cursor under
// bootstrapLastListReposCursorKey via pebble.Sync iff cursor != "".
// The empty-cursor short-circuit is the entire point: atmos's final
// OnBatchComplete fires with the relay's "I'm done" empty value, and
// we must not overwrite the last meaningful cursor with it.
func MaybeSaveBootstrapLastListReposCursor(db *store.Store, cursor string) error {
	if cursor == "" {
		return nil
	}
	if err := db.Set([]byte(bootstrapLastListReposCursorKey), []byte(cursor), store.SyncWrites); err != nil {
		return fmt.Errorf("backfill: save bootstrap_last_listrepos_cursor: %w", err)
	}
	return nil
}

// DeleteBootstrapLastListReposCursor removes the key. Called by the
// merge phase's terminal cleanup once discovery has succeeded so
// the keyspace is clean once we reach steady state.
func DeleteBootstrapLastListReposCursor(db *store.Store) error {
	if err := db.Delete([]byte(bootstrapLastListReposCursorKey), store.SyncWrites); err != nil {
		return fmt.Errorf("backfill: delete bootstrap_last_listrepos_cursor: %w", err)
	}
	return nil
}
