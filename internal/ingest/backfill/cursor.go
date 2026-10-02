package backfill

import (
	"context"
	"errors"
	"fmt"

	"github.com/bluesky-social/jetstream/internal/metastore"
)

// These keys belong to the retired relay-global enumeration scheme. They are
// retained only as migration sentinels; all new checkpoints live in
// pdshost/<hostname> rows.
const (
	listReposCursorKey              = "relay/list_repos_cursor"
	bootstrapLastListReposCursorKey = "bootstrap/last_listrepos_cursor"
)

// RejectRetiredCursors prevents an old relay-global bootstrap from being
// resumed under the per-PDS cursor scheme. There is no safe translation for
// opaque relay cursors; operators must finish with the old binary or start a
// fresh bootstrap data directory.
func RejectRetiredCursors(db metastore.Store) error {
	for _, key := range []string{listReposCursorKey, bootstrapLastListReposCursorKey} {
		val, err := db.Get(context.Background(), []byte(key))
		if errors.Is(err, metastore.ErrNotFound) {
			continue
		}
		if err != nil {
			return fmt.Errorf("backfill: check retired cursor %s: %w", key, err)
		}
		nonEmpty := len(val) > 0
		if nonEmpty {
			return fmt.Errorf("backfill: old-scheme bootstrap in progress (%s is non-empty); finish on the old binary or restart bootstrap from a fresh data dir", key)
		}
	}
	return nil
}
