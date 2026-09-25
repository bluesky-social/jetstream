package backfill

import (
	"context"
	"fmt"

	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/jcalabro/atmos"
)

func saveHandleIndex(db metastore.Store, handle string, did atmos.DID) error {
	key, ok := normalizeHandleIndexKey(handle)
	if !ok {
		return nil
	}
	if err := db.Set(context.Background(), key, []byte(did)); err != nil {
		return fmt.Errorf("backfill: save handle index %q: %w", handle, err)
	}
	return nil
}

func deleteHandleIndex(db metastore.Store, handle string) error {
	key, ok := normalizeHandleIndexKey(handle)
	if !ok {
		return nil
	}
	if err := db.Delete(context.Background(), key); err != nil {
		return fmt.Errorf("backfill: delete handle index %q: %w", handle, err)
	}
	return nil
}
