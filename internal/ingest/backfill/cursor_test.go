package backfill

import (
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/stretchr/testify/require"
)

// newCursorTestStore returns a fresh pebble-backed *store.Store in a
// t.TempDir(). Mirrors newTestStore in store_test.go but lives here
// so these tests don't depend on store_test.go's helper layout.
func newCursorTestStore(t *testing.T) *store.Store {
	t.Helper()
	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestLoadListReposCursor_Empty pins the first-time-startup contract:
// no row yet, Load returns "" without error so Run can pass "" through
// to atmos as "start from the beginning."
func TestLoadListReposCursor_Empty(t *testing.T) {
	t.Parallel()
	db := newCursorTestStore(t)

	got, err := LoadListReposCursor(db)
	require.NoError(t, err)
	require.Equal(t, "", got)
}

// TestSaveLoadListReposCursor_RoundTrip is the basic persistence
// invariant: whatever bytes the relay handed us, we hand back.
func TestSaveLoadListReposCursor_RoundTrip(t *testing.T) {
	t.Parallel()
	db := newCursorTestStore(t)

	require.NoError(t, SaveListReposCursor(db, "opaque-cursor-token-xyz"))

	got, err := LoadListReposCursor(db)
	require.NoError(t, err)
	require.Equal(t, "opaque-cursor-token-xyz", got)
}

// TestSaveListReposCursor_Overwrites confirms cursor advance is
// monotonic-by-overwrite — each page's NextCursor replaces the prior.
// We never accumulate cursors; a single global key holds the latest.
func TestSaveListReposCursor_Overwrites(t *testing.T) {
	t.Parallel()
	db := newCursorTestStore(t)

	require.NoError(t, SaveListReposCursor(db, "first"))
	require.NoError(t, SaveListReposCursor(db, "second"))

	got, err := LoadListReposCursor(db)
	require.NoError(t, err)
	require.Equal(t, "second", got)
}

// TestSaveListReposCursor_EmptyValue covers the post-drain state:
// atmos fires OnBatchComplete("") after the final batch. We must
// accept the empty string as a valid value, not treat it as a
// missing-row error. Load afterwards returns "" — the same as
// fresh-startup, which is the right semantic (next Run starts from
// the beginning, since there's nothing left to skip).
func TestSaveListReposCursor_EmptyValue(t *testing.T) {
	t.Parallel()
	db := newCursorTestStore(t)

	require.NoError(t, SaveListReposCursor(db, "first"))
	require.NoError(t, SaveListReposCursor(db, ""))

	got, err := LoadListReposCursor(db)
	require.NoError(t, err)
	require.Equal(t, "", got)
}

func TestBootstrapLastListReposCursor_RoundTrip(t *testing.T) {
	t.Parallel()
	db := newCursorTestStore(t)

	got, err := LoadBootstrapLastListReposCursor(db)
	require.NoError(t, err)
	require.Equal(t, "", got)

	require.NoError(t, MaybeSaveBootstrapLastListReposCursor(db, "page-2-cursor"))

	got, err = LoadBootstrapLastListReposCursor(db)
	require.NoError(t, err)
	require.Equal(t, "page-2-cursor", got)
}

func TestBootstrapLastListReposCursor_IgnoresEmpty(t *testing.T) {
	t.Parallel()
	db := newCursorTestStore(t)

	require.NoError(t, MaybeSaveBootstrapLastListReposCursor(db, "page-1-cursor"))
	// The relay's final page returns NextCursor="". We must NOT
	// overwrite our last-known-non-empty cursor with that.
	require.NoError(t, MaybeSaveBootstrapLastListReposCursor(db, ""))

	got, err := LoadBootstrapLastListReposCursor(db)
	require.NoError(t, err)
	require.Equal(t, "page-1-cursor", got)
}

func TestBootstrapLastListReposCursor_Delete(t *testing.T) {
	t.Parallel()
	db := newCursorTestStore(t)
	require.NoError(t, MaybeSaveBootstrapLastListReposCursor(db, "x"))

	require.NoError(t, DeleteBootstrapLastListReposCursor(db))

	got, err := LoadBootstrapLastListReposCursor(db)
	require.NoError(t, err)
	require.Equal(t, "", got)
}

func TestSaveListReposCheckpoint_WritesBothKeys(t *testing.T) {
	t.Parallel()
	db := newCursorTestStore(t)

	require.NoError(t, SaveListReposCheckpoint(db, "relay-page-2", "bootstrap-page-2"))

	relay, err := LoadListReposCursor(db)
	require.NoError(t, err)
	require.Equal(t, "relay-page-2", relay)

	bootstrap, err := LoadBootstrapLastListReposCursor(db)
	require.NoError(t, err)
	require.Equal(t, "bootstrap-page-2", bootstrap)
}

func TestSaveListReposCheckpoint_IgnoresEmptyBootstrapCursor(t *testing.T) {
	t.Parallel()
	db := newCursorTestStore(t)

	require.NoError(t, MaybeSaveBootstrapLastListReposCursor(db, "existing-bootstrap"))

	require.NoError(t, SaveListReposCheckpoint(db, "relay-page-3", ""))

	relay, err := LoadListReposCursor(db)
	require.NoError(t, err)
	require.Equal(t, "relay-page-3", relay)

	bootstrap, err := LoadBootstrapLastListReposCursor(db)
	require.NoError(t, err)
	require.Equal(t, "existing-bootstrap", bootstrap)
}

func TestSaveListReposCheckpoint_SavesEmptyRelayCursor(t *testing.T) {
	t.Parallel()
	db := newCursorTestStore(t)

	require.NoError(t, SaveListReposCursor(db, "stale-relay"))
	require.NoError(t, MaybeSaveBootstrapLastListReposCursor(db, "existing-bootstrap"))

	require.NoError(t, SaveListReposCheckpoint(db, "", ""))

	relay, err := LoadListReposCursor(db)
	require.NoError(t, err)
	require.Equal(t, "", relay)

	bootstrap, err := LoadBootstrapLastListReposCursor(db)
	require.NoError(t, err)
	require.Equal(t, "existing-bootstrap", bootstrap)
}
