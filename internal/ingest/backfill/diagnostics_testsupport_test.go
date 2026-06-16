package backfill

import (
	"fmt"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/jcalabro/atmos"
)

func hostKey(host string) ([]byte, error) {
	_, key, err := normalizeHostStatusKey(host)
	return key, err
}

func saveHandleIndex(db *store.Store, handle string, did atmos.DID) error {
	key, ok := normalizeHandleIndexKey(handle)
	if !ok {
		return nil
	}
	if err := db.Set(key, []byte(did), store.SyncWrites); err != nil {
		return fmt.Errorf("backfill: save handle index %q: %w", handle, err)
	}
	return nil
}

func deleteHandleIndex(db *store.Store, handle string) error {
	key, ok := normalizeHandleIndexKey(handle)
	if !ok {
		return nil
	}
	if err := db.Delete(key, store.SyncWrites); err != nil {
		return fmt.Errorf("backfill: delete handle index %q: %w", handle, err)
	}
	return nil
}
